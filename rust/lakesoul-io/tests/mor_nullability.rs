// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Consumer-visible MOR schemas and native merge semantics. Native sums and
//! joins are NULL-poisoning; they are not SQL aggregates that ignore NULLs.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfig, FileScanConfigBuilder,
};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_datasource::memory::MemorySourceConfig;
use futures::{StreamExt, TryStreamExt};
use lakesoul_io::config::LakeSoulIOConfig;
use lakesoul_io::physical_plan::merge::MergeParquetExec;
use lakesoul_io::reader::LakeSoulReader;
use object_store::ObjectMeta;
use parquet::arrow::ArrowWriter;

const NUMERIC_OPERATORS: [&str; 4] = ["UseLast", "UseLastNotNull", "SumAll", "SumLast"];
const JOINED_OPERATORS: [&str; 4] = [
    "JoinedAllByComma",
    "JoinedAllBySemicolon",
    "JoinedLastByComma",
    "JoinedLastBySemicolon",
];

type NumericRow = (i64, Option<i64>);

fn schema(value_type: DataType, pk_nullable: bool, value_nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, pk_nullable),
        Field::new("v", value_type, value_nullable),
    ]))
}

fn numeric_batch(schema: &SchemaRef, rows: &[NumericRow]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter(rows.iter().map(|row| row.1))),
        ],
    )
    .expect("valid numeric fixture")
}

fn string_batch(schema: &SchemaRef, rows: &[(i64, Option<&str>)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter(rows.iter().map(|row| row.1))),
        ],
    )
    .expect("valid string fixture")
}

fn omitted_batch(pk_nullable: bool, ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            pk_nullable,
        )])),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("valid omitted-column fixture")
}

fn memory_input(schema: &SchemaRef, batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&[batches], Arc::clone(schema), None)
        .expect("one memory partition per merge input")
}

fn config(schema: &SchemaRef, files: Vec<String>, operator: &str) -> LakeSoulIOConfig {
    LakeSoulIOConfig::builder()
        .with_schema(Arc::clone(schema))
        .with_files(files)
        .with_primary_keys(vec!["id".to_string()])
        .with_merge_op("v".to_string(), operator.to_string())
        .with_batch_size(2)
        .with_thread_num(1)
        .build()
}

fn memory_plan(
    requested: &SchemaRef,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    operator: &str,
) -> MergeParquetExec {
    // UseLast derives one compaction flag per configured input file. These
    // labels identify memory streams; no file is scanned or compact-path bypass used.
    let files = (0..inputs.len())
        .map(|index| format!("memory-input-{index}.parquet"))
        .collect();
    MergeParquetExec::new_with_inputs(
        Arc::clone(requested),
        inputs,
        config(requested, files, operator),
        Arc::new(HashMap::new()),
    )
    .expect("construct native merge plan")
}

async fn collect_checked(
    mut stream: SendableRecordBatchStream,
    expected_schema: &SchemaRef,
) -> Vec<RecordBatch> {
    assert_eq!(stream.schema(), *expected_schema, "stream schema");
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        let batch = batch.expect("native merge must execute successfully");
        assert_eq!(batch.schema(), *expected_schema, "emitted batch schema");
        assert_eq!(stream.schema(), *expected_schema, "stable stream schema");
        batches.push(batch);
    }
    batches
}

async fn execute_checked(
    plan: &MergeParquetExec,
    expected_schema: &SchemaRef,
) -> Vec<RecordBatch> {
    assert_eq!(plan.schema(), *expected_schema, "execution plan schema");
    let context =
        SessionContext::new_with_config(SessionConfig::default().with_batch_size(2));
    let stream = plan.execute(0, context.task_ctx()).expect("execute MOR");
    collect_checked(stream, expected_schema).await
}

fn numeric_rows(batches: &[RecordBatch]) -> Vec<NumericRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!ids.is_null(row), "merged primary keys must be non-NULL");
            rows.push((
                ids.value(row),
                (!values.is_null(row)).then(|| values.value(row)),
            ));
        }
    }
    rows
}

fn string_rows(batches: &[RecordBatch]) -> Vec<(i64, Option<String>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!ids.is_null(row), "merged primary keys must be non-NULL");
            rows.push((
                ids.value(row),
                (!values.is_null(row)).then(|| values.value(row).to_string()),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn use_last_and_use_last_not_null_keep_distinct_null_semantics() {
    let physical = schema(DataType::Int64, false, true);
    for declared_nullable in [false, true] {
        let requested = schema(DataType::Int64, false, declared_nullable);
        let expected_schema = schema(DataType::Int64, false, true);
        for operator in ["UseLast", "UseLastNotNull"] {
            let old = vec![
                numeric_batch(&physical, &[(1, Some(10)), (1, Some(11)), (2, Some(20))]),
                numeric_batch(&physical, &[]),
                numeric_batch(
                    &physical,
                    &[(2, None), (3, None), (4, Some(40)), (5, None)],
                ),
            ];
            let new = vec![
                numeric_batch(&physical, &[(1, Some(100)), (2, None), (3, None)]),
                numeric_batch(&physical, &[(4, None), (5, Some(50)), (6, Some(60))]),
            ];
            let plan = memory_plan(
                &requested,
                vec![memory_input(&physical, old), memory_input(&physical, new)],
                operator,
            );
            let expected = match operator {
                "UseLast" => vec![
                    (1, Some(100)),
                    (2, None),
                    (3, None),
                    (4, None),
                    (5, Some(50)),
                    (6, Some(60)),
                ],
                "UseLastNotNull" => vec![
                    (1, Some(100)),
                    (2, Some(20)),
                    (3, None),
                    (4, Some(40)),
                    (5, Some(50)),
                    (6, Some(60)),
                ],
                _ => unreachable!(),
            };
            let batches = execute_checked(&plan, &expected_schema).await;
            assert_eq!(numeric_rows(&batches), expected, "{operator}");
        }
    }
}

#[tokio::test]
async fn sums_select_all_rows_or_last_per_stream_across_batch_boundaries() {
    let physical = schema(DataType::Int64, false, true);
    for declared_nullable in [false, true] {
        let requested = schema(DataType::Int64, false, declared_nullable);
        let expected_schema = schema(DataType::Int64, false, true);
        for operator in ["SumAll", "SumLast"] {
            for stream_count in [1, 2] {
                let old = vec![
                    numeric_batch(&physical, &[(1, Some(2))]),
                    numeric_batch(&physical, &[(1, Some(3)), (2, None)]),
                    numeric_batch(&physical, &[]),
                    numeric_batch(&physical, &[(2, Some(7)), (3, Some(11)), (4, None)]),
                    numeric_batch(&physical, &[(4, None), (5, Some(13))]),
                ];
                let mut inputs = vec![memory_input(&physical, old)];
                if stream_count == 2 {
                    let new = vec![
                        numeric_batch(&physical, &[(1, Some(5))]),
                        numeric_batch(&physical, &[(1, Some(7)), (2, Some(10))]),
                        numeric_batch(
                            &physical,
                            &[(2, Some(20)), (3, None), (4, None), (5, Some(17))],
                        ),
                    ];
                    inputs.push(memory_input(&physical, new));
                }
                // All includes earlier NULLs. Last skips superseded versions,
                // but a selected NULL in any stream still poisons the result.
                let expected = match (operator, stream_count) {
                    ("SumAll", 1) => vec![
                        (1, Some(5)),
                        (2, None),
                        (3, Some(11)),
                        (4, None),
                        (5, Some(13)),
                    ],
                    ("SumLast", 1) => vec![
                        (1, Some(3)),
                        (2, Some(7)),
                        (3, Some(11)),
                        (4, None),
                        (5, Some(13)),
                    ],
                    ("SumAll", 2) => vec![
                        (1, Some(17)),
                        (2, None),
                        (3, None),
                        (4, None),
                        (5, Some(30)),
                    ],
                    ("SumLast", 2) => vec![
                        (1, Some(10)),
                        (2, Some(27)),
                        (3, None),
                        (4, None),
                        (5, Some(30)),
                    ],
                    _ => unreachable!(),
                };
                let plan = memory_plan(&requested, inputs, operator);
                let batches = execute_checked(&plan, &expected_schema).await;
                assert_eq!(
                    numeric_rows(&batches),
                    expected,
                    "{operator}, {stream_count} streams"
                );
            }
        }
    }
}

#[tokio::test]
async fn non_null_values_preserve_declared_and_physical_value_nullability() {
    for declared_nullable in [false, true] {
        for physical_nullable in [false, true] {
            let requested = schema(DataType::Int64, false, declared_nullable);
            let physical = schema(DataType::Int64, true, physical_nullable);
            let expected_schema = schema(
                DataType::Int64,
                false,
                declared_nullable || physical_nullable,
            );
            for operator in NUMERIC_OPERATORS {
                let inputs = vec![
                    memory_input(
                        &physical,
                        vec![
                            numeric_batch(&physical, &[(1, Some(2))]),
                            numeric_batch(&physical, &[(1, Some(3)), (2, Some(10))]),
                        ],
                    ),
                    memory_input(
                        &physical,
                        vec![numeric_batch(&physical, &[(1, Some(5)), (2, Some(20))])],
                    ),
                ];
                let expected = match operator {
                    "UseLast" | "UseLastNotNull" => vec![(1, Some(5)), (2, Some(20))],
                    "SumAll" => vec![(1, Some(10)), (2, Some(30))],
                    "SumLast" => vec![(1, Some(8)), (2, Some(30))],
                    _ => unreachable!(),
                };
                let plan = memory_plan(&requested, inputs, operator);
                let batches = execute_checked(&plan, &expected_schema).await;
                assert_eq!(numeric_rows(&batches), expected, "{operator}");
            }
        }
    }
}

#[tokio::test]
async fn omitted_values_preserve_contributions_but_new_keys_without_values_are_null() {
    let full = schema(DataType::Int64, false, false);
    for declared_nullable in [false, true] {
        let requested = schema(DataType::Int64, false, declared_nullable);
        let expected_schema = schema(DataType::Int64, false, true);
        for operator in NUMERIC_OPERATORS {
            let partial = omitted_batch(false, &[1, 3]);
            let plan = memory_plan(
                &requested,
                vec![
                    memory_input(
                        &full,
                        vec![numeric_batch(&full, &[(1, Some(10)), (2, Some(20))])],
                    ),
                    memory_input(&partial.schema(), vec![partial]),
                ],
                operator,
            );
            let batches = execute_checked(&plan, &expected_schema).await;
            assert_eq!(
                numeric_rows(&batches),
                vec![(1, Some(10)), (2, Some(20)), (3, None)],
                "{operator}"
            );
        }
    }
}

#[tokio::test]
async fn omitted_values_do_not_turn_explicit_null_updates_into_absent_updates() {
    let full = schema(DataType::Int64, false, false);
    let nullable = schema(DataType::Int64, false, true);
    let expected_schema = schema(DataType::Int64, false, true);
    for operator in NUMERIC_OPERATORS {
        let partial = omitted_batch(false, &[1, 2, 3, 4, 5]);
        let plan = memory_plan(
            &full,
            vec![
                memory_input(
                    &full,
                    vec![numeric_batch(&full, &[(1, Some(10)), (2, Some(20))])],
                ),
                memory_input(
                    &nullable,
                    vec![numeric_batch(&nullable, &[(1, None), (4, None)])],
                ),
                memory_input(&partial.schema(), vec![partial]),
            ],
            operator,
        );
        let expected_first = if operator == "UseLastNotNull" {
            Some(10)
        } else {
            None
        };
        let batches = execute_checked(&plan, &expected_schema).await;
        assert_eq!(
            numeric_rows(&batches),
            vec![
                (1, expected_first),
                (2, Some(20)),
                (3, None),
                (4, None),
                (5, None)
            ],
            "{operator}"
        );
    }
}

#[tokio::test]
async fn empty_inputs_produce_no_merged_rows_for_every_operator() {
    for operator in NUMERIC_OPERATORS.into_iter().chain(JOINED_OPERATORS) {
        let value_type = if operator.starts_with("Joined") {
            DataType::Utf8
        } else {
            DataType::Int64
        };
        for declared_nullable in [false, true] {
            let requested = schema(value_type.clone(), false, declared_nullable);
            let plan = memory_plan(&requested, vec![], operator);
            assert!(
                execute_checked(&plan, &requested)
                    .await
                    .iter()
                    .all(|batch| batch.num_rows() == 0)
            );
            for physical_nullable in [false, true] {
                let physical = schema(value_type.clone(), true, physical_nullable);
                let expected_schema = schema(
                    value_type.clone(),
                    false,
                    declared_nullable || physical_nullable,
                );
                let plan = memory_plan(
                    &requested,
                    vec![
                        memory_input(&physical, vec![]),
                        memory_input(
                            &physical,
                            vec![RecordBatch::new_empty(Arc::clone(&physical))],
                        ),
                    ],
                    operator,
                );
                assert!(
                    execute_checked(&plan, &expected_schema)
                        .await
                        .iter()
                        .all(|batch| batch.num_rows() == 0)
                );
            }
        }
    }
}

#[tokio::test]
async fn joined_operators_poison_selected_nulls_and_leave_missing_contributions_null() {
    let physical = schema(DataType::Utf8, false, true);
    for operator in JOINED_OPERATORS {
        for declared_nullable in [false, true] {
            let requested = schema(DataType::Utf8, false, declared_nullable);
            let expected_schema = schema(DataType::Utf8, false, true);
            let partial = omitted_batch(false, &[1, 6]);
            let plan = memory_plan(
                &requested,
                vec![
                    memory_input(
                        &physical,
                        vec![
                            string_batch(
                                &physical,
                                &[
                                    (1, Some("solo")),
                                    (2, Some("old")),
                                    (3, None),
                                    (4, None),
                                ],
                            ),
                            string_batch(&physical, &[(4, Some("recovered")), (5, None)]),
                        ],
                    ),
                    memory_input(
                        &physical,
                        vec![string_batch(
                            &physical,
                            &[(2, None), (3, None), (5, Some("new"))],
                        )],
                    ),
                    memory_input(&partial.schema(), vec![partial]),
                ],
                operator,
            );
            // Last selects "recovered" instead of the earlier NULL in the
            // same stream. For key 5 the other stream's selected NULL poisons
            // even Last. The lone non-NULL All contribution avoids testing delimiters.
            let recovered = operator
                .starts_with("JoinedLast")
                .then(|| "recovered".to_string());
            let batches = execute_checked(&plan, &expected_schema).await;
            assert_eq!(
                string_rows(&batches),
                vec![
                    (1, Some("solo".to_string())),
                    (2, None),
                    (3, None),
                    (4, recovered),
                    (5, None),
                    (6, None),
                ],
                "{operator}"
            );
        }
    }
}

#[tokio::test]
async fn joined_last_non_null_contributions_are_last_per_stream() {
    let physical = schema(DataType::Utf8, true, false);
    let requested = schema(DataType::Utf8, false, false);
    for (operator, expected) in [
        ("JoinedLastByComma", "b,c"),
        ("JoinedLastBySemicolon", "b;c"),
    ] {
        let plan = memory_plan(
            &requested,
            vec![
                memory_input(
                    &physical,
                    vec![
                        string_batch(&physical, &[(1, Some("a"))]),
                        string_batch(&physical, &[(1, Some("b"))]),
                    ],
                ),
                memory_input(&physical, vec![string_batch(&physical, &[(1, Some("c"))])]),
            ],
            operator,
        );
        let batches = execute_checked(&plan, &requested).await;
        assert_eq!(
            string_rows(&batches),
            vec![(1, Some(expected.to_string()))],
            "{operator}"
        );
    }
}

fn metadata_schema(pk_nullable: bool, value_nullable: bool) -> SchemaRef {
    Arc::new(Schema::new_with_metadata(
        vec![
            Field::new("id", DataType::Int64, pk_nullable).with_metadata(HashMap::from(
                [("logical_role".to_string(), "primary_key".to_string())],
            )),
            Field::new("v", DataType::Int64, value_nullable).with_metadata(
                HashMap::from([("logical_role".to_string(), "value".to_string())]),
            ),
        ],
        HashMap::from([("logical_schema".to_string(), "mor_metadata".to_string())]),
    ))
}

#[tokio::test]
async fn nullable_physical_keys_preserve_declared_key_and_output_metadata() {
    let physical = schema(DataType::Int64, true, true);
    for declared_pk_nullable in [false, true] {
        let requested = metadata_schema(declared_pk_nullable, false);
        let expected_schema = metadata_schema(declared_pk_nullable, true);
        for operator in NUMERIC_OPERATORS {
            let partial = omitted_batch(true, &[1, 3]);
            let plan = memory_plan(
                &requested,
                vec![
                    memory_input(
                        &physical,
                        vec![numeric_batch(&physical, &[(1, Some(10)), (2, None)])],
                    ),
                    memory_input(&partial.schema(), vec![partial]),
                ],
                operator,
            );
            let batches = execute_checked(&plan, &expected_schema).await;
            assert_eq!(
                numeric_rows(&batches),
                vec![(1, Some(10)), (2, None), (3, None)],
                "{operator}"
            );
        }
    }
}

#[tokio::test]
async fn null_physical_primary_keys_are_rejected_for_non_null_declared_keys() {
    let requested = schema(DataType::Int64, false, false);
    let physical = schema(DataType::Int64, true, false);
    for operator in NUMERIC_OPERATORS {
        let invalid = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(Int64Array::from(vec![None, Some(1)])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
            ],
        )
        .expect("nullable physical schema permits the invalid stored key");
        let plan = memory_plan(
            &requested,
            vec![memory_input(&physical, vec![invalid])],
            operator,
        );
        assert_eq!(plan.schema(), requested);
        let context = SessionContext::new();
        let stream = plan
            .execute(0, context.task_ctx())
            .expect("execution starts before reading input");
        assert_eq!(stream.schema(), requested);
        let error = stream
            .try_collect::<Vec<_>>()
            .await
            .expect_err("NULL key must fail at runtime")
            .to_string();
        assert!(
            error.contains("id") && error.to_ascii_lowercase().contains("null"),
            "{operator}: {error}"
        );
    }
}

#[tokio::test]
async fn null_composite_primary_key_components_are_rejected_at_runtime() {
    let requested = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("shard", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let physical = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("shard", DataType::Int64, true),
        Field::new("v", DataType::Int64, false),
    ]));
    for null_column in ["id", "shard"] {
        for operator in ["UseLast", "SumAll"] {
            let ids = if null_column == "id" {
                vec![None, Some(1)]
            } else {
                vec![Some(1), Some(1)]
            };
            let shards = if null_column == "shard" {
                vec![None, Some(1)]
            } else {
                vec![Some(1), Some(1)]
            };
            let invalid = RecordBatch::try_new(
                Arc::clone(&physical),
                vec![
                    Arc::new(Int64Array::from(ids)) as ArrayRef,
                    Arc::new(Int64Array::from(shards)) as ArrayRef,
                    Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                ],
            )
            .unwrap();
            let io_config = LakeSoulIOConfig::builder()
                .with_schema(Arc::clone(&requested))
                .with_file("memory-input-0.parquet")
                .with_primary_keys(vec!["id".to_string(), "shard".to_string()])
                .with_merge_op("v".to_string(), operator.to_string())
                .build();
            let plan = MergeParquetExec::new_with_inputs(
                Arc::clone(&requested),
                vec![memory_input(&physical, vec![invalid])],
                io_config,
                Arc::new(HashMap::new()),
            )
            .unwrap();
            assert_eq!(plan.schema(), requested);
            let stream = plan.execute(0, SessionContext::new().task_ctx()).unwrap();
            assert_eq!(stream.schema(), requested);
            let error = stream
                .try_collect::<Vec<_>>()
                .await
                .expect_err("NULL composite key must fail")
                .to_string();
            assert!(
                error.contains(null_column)
                    && error.to_ascii_lowercase().contains("null"),
                "{operator}: {error}"
            );
        }
    }
}

fn parquet_config(dir: &Path, name: &str, batch: &RecordBatch) -> FileScanConfig {
    let path = dir.join(name);
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&path).expect("create fixture parquet"),
        batch.schema(),
        None,
    )
    .expect("fixture parquet writer");
    writer.write(batch).expect("write fixture batch");
    writer.close().expect("close fixture parquet");
    let source = ParquetFormat::default()
        .file_source(TableSchema::builder(batch.schema()).build());
    let meta = ObjectMeta {
        location: object_store::path::Path::from(path.to_str().unwrap()),
        last_modified: chrono::DateTime::<chrono::Utc>::from(
            std::time::SystemTime::UNIX_EPOCH,
        ),
        size: std::fs::metadata(&path).unwrap().len(),
        e_tag: None,
        version: None,
    };
    FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
        .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new_from_meta(
            meta,
        )])])
        .build()
}

#[tokio::test]
async fn parquet_constructor_and_native_reader_keep_partial_update_nullability() {
    let dir = tempfile::tempdir().unwrap();
    let physical = schema(DataType::Int64, true, true);
    let requested = schema(DataType::Int64, false, false);
    let expected_schema = schema(DataType::Int64, false, true);
    let configs = vec![
        parquet_config(
            dir.path(),
            "old.parquet",
            &numeric_batch(&physical, &[(1, Some(10)), (2, Some(20)), (4, None)]),
        ),
        parquet_config(dir.path(), "partial.parquet", &omitted_batch(true, &[1, 3])),
        parquet_config(
            dir.path(),
            "null-update.parquet",
            &numeric_batch(&physical, &[(2, None)]),
        ),
    ];
    let paths: Vec<String> = ["old.parquet", "partial.parquet", "null-update.parquet"]
        .into_iter()
        .map(|name| dir.path().join(name).to_str().unwrap().to_string())
        .collect();
    for operator in ["UseLast", "UseLastNotNull"] {
        let io_config = config(&requested, paths.clone(), operator);
        let plan = MergeParquetExec::new(
            Arc::clone(&requested),
            configs.clone(),
            io_config.clone(),
        )
        .unwrap();
        let expected_second = if operator == "UseLastNotNull" {
            Some(20)
        } else {
            None
        };
        let expected = vec![(1, Some(10)), (2, expected_second), (3, None), (4, None)];
        let batches = execute_checked(&plan, &expected_schema).await;
        assert_eq!(
            numeric_rows(&batches),
            expected,
            "parquet constructor: {operator}"
        );

        // This also crosses file-schema inference and native reader projection,
        // rather than exercising only a hand-constructed physical merge node.
        let mut reader = LakeSoulReader::new(io_config).unwrap();
        reader.start().await.unwrap();
        let batches = collect_checked(
            reader.stream().expect("started reader stream"),
            &expected_schema,
        )
        .await;
        assert_eq!(
            numeric_rows(&batches),
            expected,
            "native reader: {operator}"
        );
    }
}

#[tokio::test]
async fn injected_partition_values_remain_non_null_with_partial_value_updates() {
    let requested = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
        Field::new("region", DataType::Utf8, false),
    ]));
    let physical = schema(DataType::Int64, true, false);
    for operator in ["UseLast", "UseLastNotNull"] {
        let partial = omitted_batch(true, &[1, 2]);
        let io_config = LakeSoulIOConfig::builder()
            .with_schema(Arc::clone(&requested))
            .with_files(vec![
                "memory-input-0.parquet".to_string(),
                "memory-input-1.parquet".to_string(),
            ])
            .with_primary_keys(vec!["id".to_string()])
            .with_merge_op("v".to_string(), operator.to_string())
            .with_default_column_value("region", "west")
            .build();
        let plan = MergeParquetExec::new_with_inputs(
            Arc::clone(&requested),
            vec![
                memory_input(&physical, vec![numeric_batch(&physical, &[(1, Some(10))])]),
                memory_input(&partial.schema(), vec![partial]),
            ],
            io_config,
            Arc::new(HashMap::from([("region".to_string(), "west".to_string())])),
        )
        .unwrap();
        let batches = execute_checked(&plan, &expected_schema).await;
        assert_eq!(
            numeric_rows(&batches),
            vec![(1, Some(10)), (2, None)],
            "{operator}"
        );
        for batch in &batches {
            let regions = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                regions.iter().collect::<Vec<_>>(),
                vec![Some("west"); batch.num_rows()]
            );
        }
    }
}
