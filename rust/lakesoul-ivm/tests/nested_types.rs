// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! Nested column types (`Struct`, `List`, `Map`) in the maintained views: the
//! values are opaque payloads the refresh copies through, and expressions may
//! read single fields out of them.

use std::sync::Arc;

use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_array::{
    Array, ArrayRef, Int64Array, ListArray, MapArray, RecordBatch, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, JoinKeyExprs,
    row_mv_schema_for, sum_count_groups_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn point_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Utf8, true),
    ]))
}

fn list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
}

fn map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        )),
        false,
    )
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("s", point_type(), true),
        Field::new("l", list_type(), true),
        Field::new("m", map_type(), true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

/// One source row; `None` means a NULL struct/list/map, `Some(vec![])` an
/// empty list/map.
struct Row<'a> {
    k: i64,
    g: &'a str,
    s: Option<(i64, Option<&'a str>)>,
    l: Option<Vec<i64>>,
    m: Option<Vec<(&'a str, Option<i64>)>>,
    op: &'a str,
}

fn source_batch(rows: &[Row<'_>]) -> RecordBatch {
    let points = StructArray::new(
        Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]),
        vec![
            Arc::new(Int64Array::from_iter(
                rows.iter().map(|row| row.s.map(|point| point.0)),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter(
                rows.iter().map(|row| row.s.and_then(|point| point.1)),
            )),
        ],
        Some(NullBuffer::from_iter(
            rows.iter().map(|row| row.s.is_some()),
        )),
    );

    let mut offsets = vec![0i32];
    let mut values = Vec::new();
    for row in rows {
        for value in row.l.as_deref().unwrap_or_default() {
            values.push(*value);
        }
        offsets.push(values.len() as i32);
    }
    let list_field = Arc::new(Field::new("item", DataType::Int64, true));
    let lists = ListArray::new(
        list_field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(Int64Array::from_iter_values(values)),
        Some(NullBuffer::from_iter(
            rows.iter().map(|row| row.l.is_some()),
        )),
    );

    let mut offsets = vec![0i32];
    let mut keys = Vec::new();
    let mut map_values = Vec::new();
    for row in rows {
        for (key, value) in row.m.as_deref().unwrap_or_default() {
            keys.push(*key);
            map_values.push(*value);
        }
        offsets.push(keys.len() as i32);
    }
    // The entries struct itself is never null; a NULL map value is a null in
    // the value child.
    let entries = StructArray::new(
        Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]),
        vec![
            Arc::new(StringArray::from_iter_values(keys)) as ArrayRef,
            Arc::new(Int64Array::from_iter(map_values)) as ArrayRef,
        ],
        None,
    );
    let map_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ])),
        false,
    ));
    let maps = MapArray::new(
        map_field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        Some(NullBuffer::from_iter(
            rows.iter().map(|row| row.m.is_some()),
        )),
        false,
    );

    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.k))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.g))),
            Arc::new(points),
            Arc::new(lists),
            Arc::new(maps),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.op))),
        ],
    )
    .unwrap()
}

async fn create_source(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    suffix: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_src_{suffix}"),
                table_path(dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

/// The string form of one column of every row, ordered by `k`.
async fn read_column(runtime: &IvmRuntime, mv: &IvmTable, column: &str) -> Vec<String> {
    read_column_by(runtime, mv, column, "k").await
}

/// The string form of one column of every row, ordered by `order_by`.
async fn read_column_by(
    runtime: &IvmRuntime,
    mv: &IvmTable,
    column: &str,
    order_by: &str,
) -> Vec<String> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql(&format!("select {column} from mv order by {order_by}"))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let array = batch.column(0);
        for row in 0..batch.num_rows() {
            rows.push(arrow::util::display::array_value_to_string(array, row).unwrap());
        }
    }
    rows
}

/// A row view copies nested columns through unchanged, including NULL, empty
/// and nested-NULL values.
#[test_log::test(tokio::test)]
async fn a_row_view_passes_nested_columns_through() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let output_columns = ["k", "s", "l", "m"].map(String::from);
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_row_{suffix}"),
                table_path(&dir, "mv"),
                row_mv_schema_for(&source.schema, &output_columns).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                Row {
                    k: 1,
                    g: "a",
                    s: Some((10, Some("x"))),
                    l: Some(vec![1, 2]),
                    m: Some(vec![("p", Some(7))]),
                    op: "insert",
                },
                Row {
                    k: 2,
                    g: "b",
                    s: None,
                    l: None,
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 3,
                    g: "c",
                    s: Some((30, None)),
                    l: Some(vec![]),
                    m: Some(vec![]),
                    op: "insert",
                },
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_row_{suffix} SELECT k, s, l, m \
         FROM ivm_nested_src_{suffix}"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_column(runtime, &mv, "s").await,
        vec![
            "{a: 10, b: x}".to_string(),
            "".to_string(),
            "{a: 30, b: }".to_string()
        ]
    );
    assert_eq!(
        read_column(runtime, &mv, "l").await,
        vec!["[1, 2]".to_string(), "".to_string(), "[]".to_string()]
    );
    assert_eq!(
        read_column(runtime, &mv, "m").await,
        vec!["{p: 7}".to_string(), "".to_string(), "{}".to_string()]
    );

    // An updated struct, a new NULL and a delete flow through incrementally.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                Row {
                    k: 1,
                    g: "a",
                    s: Some((11, Some("y"))),
                    l: Some(vec![3]),
                    m: Some(vec![("q", None)]),
                    op: "insert",
                },
                Row {
                    k: 2,
                    g: "b",
                    s: None,
                    l: None,
                    m: None,
                    op: "delete",
                },
            ]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_column(runtime, &mv, "s").await,
        vec!["{a: 11, b: y}".to_string(), "{a: 30, b: }".to_string()]
    );
    assert_eq!(
        read_column(runtime, &mv, "l").await,
        vec!["[3]".to_string(), "[]".to_string()]
    );
    assert_eq!(
        read_column(runtime, &mv, "m").await,
        vec!["{q: }".to_string(), "{}".to_string()]
    );
}

/// A struct field is readable inside expressions: the aggregate group value
/// comes from a nested payload.
#[test_log::test(tokio::test)]
async fn a_struct_field_is_readable_in_expressions() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_sum_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_groups_mv_schema_for(
                    &source.schema,
                    &["g".to_string()],
                    &[],
                    None,
                    Some("s['a']"),
                    false,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                Row {
                    k: 1,
                    g: "a",
                    s: Some((10, Some("x"))),
                    l: Some(vec![1, 2]),
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 2,
                    g: "a",
                    s: Some((5, Some("y"))),
                    l: Some(vec![3]),
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 3,
                    g: "b",
                    s: Some((7, None)),
                    l: Some(vec![4]),
                    m: None,
                    op: "insert",
                },
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_sum_{suffix} \
         SELECT g, SUM(s['a']) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_nested_src_{suffix} GROUP BY g"
    );
    let outcome = executor.execute(&statement).await;
    let execution = match outcome {
        Ok(execution) => execution,
        Err(error) => panic!("the struct field aggregate was rejected: {error}"),
    };
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let batches = context
        .sql("select g, sum_v from mv order by g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((groups.value(row).to_string(), sums.value(row)));
        }
    }
    assert_eq!(rows, vec![("a".to_string(), 15), ("b".to_string(), 7)]);
}

/// `array_agg` over a struct column materializes a list of structs: the
/// aggregate result type is nested and round-trips through the view spec.
#[test_log::test(tokio::test)]
async fn array_agg_over_a_struct_column_materializes_a_list_of_structs() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let column = lakesoul_ivm::array_agg_output_column(Some("s"));
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_agg_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::array_agg_mv_schema_for(
                    &source.schema,
                    &["g".to_string()],
                    "s",
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()])
            .with_allow_fallback(false),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                Row {
                    k: 1,
                    g: "a",
                    s: Some((10, Some("x"))),
                    l: None,
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 2,
                    g: "a",
                    s: Some((5, Some("y"))),
                    l: None,
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 3,
                    g: "b",
                    s: Some((7, None)),
                    l: None,
                    m: None,
                    op: "insert",
                },
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_agg_{suffix} \
         SELECT g, array_agg(s ORDER BY k) AS {column} \
         FROM ivm_nested_src_{suffix} GROUP BY g"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_column_by(runtime, &mv, &column, "g").await,
        vec![
            "[{a: 10, b: x}, {a: 5, b: y}]".to_string(),
            "[{a: 7, b: }]".to_string(),
        ]
    );

    // An updated struct and a delete recompute the affected group's list.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                Row {
                    k: 1,
                    g: "a",
                    s: Some((11, Some("z"))),
                    l: None,
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 2,
                    g: "a",
                    s: Some((5, Some("y"))),
                    l: None,
                    m: None,
                    op: "delete",
                },
            ]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_column_by(runtime, &mv, &column, "g").await,
        vec!["[{a: 11, b: z}]".to_string(), "[{a: 7, b: }]".to_string(),]
    );
}

/// A pair join whose payloads are nested columns.
#[test_log::test(tokio::test)]
async fn a_join_carries_nested_payloads() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = create_source(&runtime, &dir, &suffix).await;
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_right_{suffix}"),
                table_path(&dir, "right"),
                source_schema(),
            )
            .with_primary_keys(vec!["g".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_join_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::lookup_join_view_schema_for(
                    &source_schema(),
                    &source_schema(),
                    &["k".to_string()],
                    &["g".to_string()],
                    &["g".to_string()],
                    &JoinKeyExprs::default(),
                    "s",
                    "l",
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    left.append_batch(
        runtime.client(),
        source_batch(&[
            Row {
                k: 1,
                g: "a",
                s: Some((10, Some("x"))),
                l: None,
                m: None,
                op: "insert",
            },
            Row {
                k: 2,
                g: "b",
                s: Some((20, None)),
                l: None,
                m: None,
                op: "insert",
            },
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            source_batch(&[Row {
                k: 1,
                g: "a",
                s: None,
                l: Some(vec![4, 5]),
                m: None,
                op: "insert",
            }]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_join_{suffix} SELECT a.s, b.l \
         FROM ivm_nested_src_{suffix} a LEFT JOIN ivm_nested_right_{suffix} b ON b.g = a.g"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_column_by(runtime, &mv, "left_value", "g").await,
        vec!["{a: 10, b: x}".to_string(), "{a: 20, b: }".to_string()]
    );
    assert_eq!(
        read_column_by(runtime, &mv, "right_value", "g").await,
        vec!["[4, 5]".to_string(), "".to_string()]
    );

    // A new right payload updates the pair; deleting the left row drops it.
    right
        .append_batch(
            runtime.client(),
            source_batch(&[Row {
                k: 1,
                g: "a",
                s: None,
                l: Some(vec![6]),
                m: None,
                op: "insert",
            }]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_column_by(runtime, &mv, "right_value", "g").await,
        vec!["[6]".to_string(), "".to_string()]
    );
}

/// A union over nested columns: both branches carry the same nested types.
#[test_log::test(tokio::test)]
async fn a_union_carries_nested_columns() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = create_source(&runtime, &dir, &suffix).await;
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_union_right_{suffix}"),
                table_path(&dir, "right"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let output_columns = ["k", "s", "m"].map(String::from);
    let output_schema =
        lakesoul_ivm::union_output_schema_for(&source_schema(), &output_columns, &[])
            .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_union_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::union_all_mv_schema_for(&output_schema).unwrap(),
            )
            .with_primary_keys(vec![
                lakesoul_ivm::IVM_SOURCE_COLUMN.to_string(),
                "k".to_string(),
            ]),
        )
        .await
        .unwrap();
    left.append_batch(
        runtime.client(),
        source_batch(&[Row {
            k: 1,
            g: "a",
            s: Some((10, Some("x"))),
            l: None,
            m: Some(vec![("p", Some(1))]),
            op: "insert",
        }]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            source_batch(&[Row {
                k: 1,
                g: "b",
                s: Some((20, None)),
                l: None,
                m: None,
                op: "insert",
            }]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_union_{suffix} SELECT k, s, m \
         FROM ivm_nested_src_{suffix} \
         UNION ALL SELECT k, s, m FROM ivm_nested_union_right_{suffix}"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    let mut structs = read_column(runtime, &mv, "s").await;
    structs.sort();
    assert_eq!(
        structs,
        vec!["{a: 10, b: x}".to_string(), "{a: 20, b: }".to_string()]
    );
    let mut maps = read_column(runtime, &mv, "m").await;
    maps.sort();
    assert_eq!(maps, vec!["".to_string(), "{p: 1}".to_string()]);
}

/// A value window over a nested column: `LAG` carries the struct through.
#[test_log::test(tokio::test)]
async fn a_window_carries_a_nested_value() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv =
        runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_nested_window_{suffix}"),
                    table_path(&dir, "mv"),
                    lakesoul_ivm::window_columns_mv_schema_for(
                        &source.schema,
                        &["g".to_string()],
                        &["k".to_string()],
                        &[lakesoul_ivm::WindowColumn::new(
                            lakesoul_ivm::WindowFunction::Lag,
                        )
                        .with_value("s")
                        .with_column("prev_s")],
                    )
                    .unwrap(),
                )
                .with_primary_keys(vec!["g".to_string(), "k".to_string()]),
            )
            .await
            .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                Row {
                    k: 1,
                    g: "a",
                    s: Some((10, Some("x"))),
                    l: None,
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 2,
                    g: "a",
                    s: Some((20, None)),
                    l: None,
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 3,
                    g: "b",
                    s: Some((30, Some("z"))),
                    l: None,
                    m: None,
                    op: "insert",
                },
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_window_{suffix} \
         SELECT k, g, lag(s) OVER (PARTITION BY g ORDER BY k) AS prev_s \
         FROM ivm_nested_src_{suffix}"
    );
    let outcome = executor.execute(&statement).await;
    match outcome {
        Ok(execution) => assert_eq!(execution.action.label(), "bootstrap"),
        Err(error) => panic!("the nested-value window was rejected: {error}"),
    }
    let runtime = executor.runtime();
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let batches = context
        .sql("select prev_s from mv order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let array = batch.column(0);
        for row in 0..batch.num_rows() {
            rows.push(arrow::util::display::array_value_to_string(array, row).unwrap());
        }
    }
    assert_eq!(
        rows,
        vec!["".to_string(), "{a: 10, b: x}".to_string(), "".to_string()]
    );
}

/// A `SUM`/`COUNT` view grouped by a struct column: the group key is a nested
/// value, so the state table and the MV are keyed by it.
#[test_log::test(tokio::test)]
async fn a_count_view_groups_by_a_struct_key() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_groups_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::sum_count_mv_schema_for(
                    &source.schema,
                    &["s".to_string()],
                    None,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["s".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                Row {
                    k: 1,
                    g: "a",
                    s: Some((10, Some("x"))),
                    l: None,
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 2,
                    g: "b",
                    s: Some((10, Some("x"))),
                    l: None,
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 3,
                    g: "c",
                    s: Some((20, None)),
                    l: None,
                    m: None,
                    op: "insert",
                },
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_groups_{suffix} \
         SELECT s, COUNT(*) AS count_v FROM ivm_nested_src_{suffix} GROUP BY s"
    );
    let outcome = executor.execute(&statement).await;
    match outcome {
        Ok(execution) => assert_eq!(execution.action.label(), "bootstrap"),
        Err(error) => panic!("the nested group key was rejected: {error}"),
    }
    let runtime = executor.runtime();
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let batches = context
        .sql("select s, count_v from mv order by count_v desc")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let structs = batch.column(0);
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                arrow::util::display::array_value_to_string(structs, row).unwrap(),
                counts.value(row),
            ));
        }
    }
    assert_eq!(
        rows,
        vec![
            ("{a: 10, b: x}".to_string(), 2),
            ("{a: 20, b: }".to_string(), 1)
        ]
    );

    // Moving one row to another struct group retracts it from the old group.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[Row {
                k: 2,
                g: "b",
                s: Some((20, None)),
                l: None,
                m: None,
                op: "insert",
            }]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    let batches = context
        .sql("select s, count_v from mv order by count_v desc")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let structs = batch.column(0);
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                arrow::util::display::array_value_to_string(structs, row).unwrap(),
                counts.value(row),
            ));
        }
    }
    assert_eq!(
        rows,
        vec![
            ("{a: 20, b: }".to_string(), 2),
            ("{a: 10, b: x}".to_string(), 1)
        ]
    );
}

/// A `COUNT` view grouped by a map column: NULL and empty maps are distinct
/// groups.
#[test_log::test(tokio::test)]
async fn a_count_view_groups_by_a_map_key() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_map_groups_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::sum_count_mv_schema_for(
                    &source.schema,
                    &["m".to_string()],
                    None,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["m".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                Row {
                    k: 1,
                    g: "a",
                    s: None,
                    l: None,
                    m: Some(vec![("p", Some(1))]),
                    op: "insert",
                },
                Row {
                    k: 2,
                    g: "b",
                    s: None,
                    l: None,
                    m: Some(vec![("p", Some(1))]),
                    op: "insert",
                },
                Row {
                    k: 3,
                    g: "c",
                    s: None,
                    l: None,
                    m: Some(vec![("p", None)]),
                    op: "insert",
                },
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_map_groups_{suffix} \
         SELECT m, COUNT(*) AS count_v FROM ivm_nested_src_{suffix} GROUP BY m"
    );
    let outcome = executor.execute(&statement).await;
    match outcome {
        Ok(execution) => assert_eq!(execution.action.label(), "bootstrap"),
        Err(error) => panic!("the map group key was rejected: {error}"),
    }
    let runtime = executor.runtime();
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let batches = context
        .sql("select m, count_v from mv order by count_v desc")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let maps = batch.column(0);
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                arrow::util::display::array_value_to_string(maps, row).unwrap(),
                counts.value(row),
            ));
        }
    }
    assert_eq!(
        rows,
        vec![("{p: 1}".to_string(), 2), ("{p: }".to_string(), 1)]
    );
}

/// A `COUNT` view grouped by a list column, including NULL and empty lists as
/// distinct groups.
#[test_log::test(tokio::test)]
async fn a_count_view_groups_by_a_list_key() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_list_groups_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::sum_count_mv_schema_for(
                    &source.schema,
                    &["l".to_string()],
                    None,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["l".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                Row {
                    k: 1,
                    g: "a",
                    s: None,
                    l: Some(vec![1, 2]),
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 2,
                    g: "b",
                    s: None,
                    l: Some(vec![1, 2]),
                    m: None,
                    op: "insert",
                },
                Row {
                    k: 3,
                    g: "c",
                    s: None,
                    l: Some(vec![]),
                    m: None,
                    op: "insert",
                },
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_list_groups_{suffix} \
         SELECT l, COUNT(*) AS count_v FROM ivm_nested_src_{suffix} GROUP BY l"
    );
    let outcome = executor.execute(&statement).await;
    match outcome {
        Ok(execution) => assert_eq!(execution.action.label(), "bootstrap"),
        Err(error) => panic!("the list group key was rejected: {error}"),
    }
    let runtime = executor.runtime();
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let batches = context
        .sql("select l, count_v from mv order by count_v desc")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let lists = batch.column(0);
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                arrow::util::display::array_value_to_string(lists, row).unwrap(),
                counts.value(row),
            ));
        }
    }
    assert_eq!(rows, vec![("[1, 2]".to_string(), 2), ("[]".to_string(), 1)]);
}

/// A tiny deterministic generator for the differential rounds.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) % bound
    }
}

/// Randomised nested-payload mutations checked against a full recompute: the
/// view groups by a struct key and sums a scalar column.
#[test_log::test(tokio::test)]
async fn nested_payloads_match_a_full_recompute() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nested_oracle_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::sum_count_mv_schema_for(
                    &source.schema,
                    &["s".to_string()],
                    Some("k"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["s".to_string()])
            .with_allow_fallback(false),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nested_oracle_{suffix} \
         SELECT s, SUM(k) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_nested_src_{suffix} GROUP BY s"
    );
    let mut rng = Lcg(0x5eed);
    let mut next_k = 1i64;
    let mut live = Vec::new();
    for round in 0..24 {
        let runtime = executor.runtime();
        let point = |rng: &mut Lcg| {
            Some((
                rng.below(4) as i64,
                if rng.below(2) == 0 { Some("x") } else { None },
            ))
        };
        let mut rows = Vec::new();
        match if live.is_empty() { 0 } else { rng.below(3) } {
            0 => {
                let k = next_k;
                next_k += 1;
                live.push(k);
                rows.push(Row {
                    k,
                    g: "g",
                    s: point(&mut rng),
                    l: None,
                    m: None,
                    op: "insert",
                });
            }
            1 => {
                let k = live[rng.below(live.len() as u64) as usize];
                rows.push(Row {
                    k,
                    g: "g",
                    s: point(&mut rng),
                    l: None,
                    m: None,
                    op: "insert",
                });
            }
            _ => {
                let index = rng.below(live.len() as u64) as usize;
                let k = live.swap_remove(index);
                rows.push(Row {
                    k,
                    g: "g",
                    s: point(&mut rng),
                    l: None,
                    m: None,
                    op: "delete",
                });
            }
        }
        source
            .append_batch(runtime.client(), source_batch(&rows))
            .await
            .unwrap();
        let execution = executor.execute(&statement).await.unwrap();
        let action = execution.action.label();
        assert!(
            action == "bootstrap" || action == "incremental",
            "round {round}: {action}"
        );

        // The full recompute: the same statement over the current source state.
        let context = SessionContext::new();
        context
            .register_table("src", Arc::new(runtime.table_provider(&source)))
            .unwrap();
        let reference = context
            .sql(
                "SELECT s, SUM(k) AS sum_v, COUNT(*) AS count_v \
                 FROM src GROUP BY s",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut expected = Vec::new();
        for batch in &reference {
            let structs = batch.column(0);
            let sums = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                expected.push(format!(
                    "{}|{}|{}",
                    arrow::util::display::array_value_to_string(structs, row).unwrap(),
                    sums.value(row),
                    counts.value(row)
                ));
            }
        }
        expected.sort();

        let context = SessionContext::new();
        context
            .register_table("mv", Arc::new(runtime.table_provider(&mv)))
            .unwrap();
        let batches = context
            .sql("SELECT s, sum_v, count_v FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut actual = Vec::new();
        for batch in &batches {
            let structs = batch.column(0);
            let sums = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                actual.push(format!(
                    "{}|{}|{}",
                    arrow::util::display::array_value_to_string(structs, row).unwrap(),
                    sums.value(row),
                    counts.value(row)
                ));
            }
        }
        actual.sort();
        assert_eq!(actual, expected, "round {round}");
    }
}

/// A nested **join key**: the struct column matches by value, and the pair
/// join materializes it as the join key column of the MV.
#[test_log::test(tokio::test)]
async fn a_join_on_a_struct_key_matches_by_value() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nkey_l_{suffix}"),
                table_path(&dir, "left"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nkey_r_{suffix}"),
                table_path(&dir, "right"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nkey_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::left_join_view_schema_for(
                    &source_schema(),
                    &source_schema(),
                    &["k".to_string()],
                    &["k".to_string()],
                    &["s".to_string()],
                    "l",
                    "m",
                )
                .unwrap(),
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &["k".to_string()],
                &["k".to_string()],
            ))
            .with_allow_fallback(false),
        )
        .await
        .unwrap();
    left.append_batch(
        runtime.client(),
        source_batch(&[
            Row {
                k: 1,
                g: "a",
                s: Some((10, Some("x"))),
                l: Some(vec![1]),
                m: None,
                op: "insert",
            },
            Row {
                k: 2,
                g: "b",
                s: Some((20, None)),
                l: Some(vec![2]),
                m: None,
                op: "insert",
            },
            Row {
                k: 3,
                g: "c",
                s: None,
                l: Some(vec![3]),
                m: None,
                op: "insert",
            },
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            source_batch(&[Row {
                k: 7,
                g: "r",
                s: Some((10, Some("x"))),
                l: None,
                m: Some(vec![("p", Some(1))]),
                op: "insert",
            }]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_nkey_mv_{suffix} SELECT a.l, b.m \
         FROM ivm_nkey_l_{suffix} a LEFT JOIN ivm_nkey_r_{suffix} b ON b.s = a.s"
    );
    let outcome = executor.execute(&statement).await;
    match outcome {
        Ok(execution) => assert_eq!(execution.action.label(), "bootstrap"),
        Err(error) => panic!("the nested join key was rejected: {error}"),
    }
    let runtime = executor.runtime();
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let batches = context
        .sql("select right_value from mv order by __left_pk_k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let array = batch.column(0);
        for row in 0..batch.num_rows() {
            rows.push(arrow::util::display::array_value_to_string(array, row).unwrap());
        }
    }
    assert_eq!(
        rows,
        vec!["{p: 1}".to_string(), "".to_string(), "".to_string()]
    );

    // A matching struct inserted on the right completes the unmatched pair.
    right
        .append_batch(
            runtime.client(),
            source_batch(&[Row {
                k: 8,
                g: "r",
                s: Some((20, None)),
                l: None,
                m: Some(vec![("q", Some(2))]),
                op: "insert",
            }]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    let batches = context
        .sql("select right_value from mv order by __left_pk_k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let array = batch.column(0);
        for row in 0..batch.num_rows() {
            rows.push(arrow::util::display::array_value_to_string(array, row).unwrap());
        }
    }
    assert_eq!(
        rows,
        vec!["{p: 1}".to_string(), "{q: 2}".to_string(), "".to_string()]
    );
}

/// A **list** join key: lists match element-wise, NULL never matches, and the
/// pair join materializes the key column.
#[test_log::test(tokio::test)]
async fn a_join_on_a_list_key_matches_by_value() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lkey_l_{suffix}"),
                table_path(&dir, "left"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lkey_r_{suffix}"),
                table_path(&dir, "right"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lkey_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::left_join_view_schema_for(
                    &source_schema(),
                    &source_schema(),
                    &["k".to_string()],
                    &["k".to_string()],
                    &["l".to_string()],
                    "s",
                    "m",
                )
                .unwrap(),
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &["k".to_string()],
                &["k".to_string()],
            ))
            .with_allow_fallback(false),
        )
        .await
        .unwrap();
    left.append_batch(
        runtime.client(),
        source_batch(&[
            Row {
                k: 1,
                g: "a",
                s: Some((10, None)),
                l: Some(vec![1, 2]),
                m: None,
                op: "insert",
            },
            Row {
                k: 2,
                g: "b",
                s: Some((20, None)),
                l: Some(vec![]),
                m: None,
                op: "insert",
            },
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            source_batch(&[Row {
                k: 7,
                g: "r",
                s: None,
                l: Some(vec![1, 2]),
                m: Some(vec![("p", Some(1))]),
                op: "insert",
            }]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_lkey_mv_{suffix} SELECT a.s, b.m \
         FROM ivm_lkey_l_{suffix} a LEFT JOIN ivm_lkey_r_{suffix} b ON b.l = a.l"
    );
    let outcome = executor.execute(&statement).await;
    match outcome {
        Ok(execution) => assert_eq!(execution.action.label(), "bootstrap"),
        Err(error) => panic!("the nested join key was rejected: {error}"),
    }
    let runtime = executor.runtime();
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let batches = context
        .sql("select right_value from mv order by __left_pk_k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let array = batch.column(0);
        for row in 0..batch.num_rows() {
            rows.push(arrow::util::display::array_value_to_string(array, row).unwrap());
        }
    }
    assert_eq!(rows, vec!["{p: 1}".to_string(), "".to_string()]);

    // The empty list matches an empty list on the right.
    right
        .append_batch(
            runtime.client(),
            source_batch(&[Row {
                k: 8,
                g: "r",
                s: None,
                l: Some(vec![]),
                m: Some(vec![("q", Some(2))]),
                op: "insert",
            }]),
        )
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    let batches = context
        .sql("select right_value from mv order by __left_pk_k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let array = batch.column(0);
        for row in 0..batch.num_rows() {
            rows.push(arrow::util::display::array_value_to_string(array, row).unwrap());
        }
    }
    assert_eq!(rows, vec!["{p: 1}".to_string(), "{q: 2}".to_string()]);
}

/// The sources of the identity tests: the struct `s` and the list `l` are
/// **non-nullable** so either can be a primary key (a row identity key must
/// be non-null).
fn pk_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("s", point_type(), false),
        Field::new("l", list_type(), false),
        Field::new("m", map_type(), true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

/// One `pk_schema()` row: `(k, g, s, l, m, op)`.
type PkRow<'a> = (
    i64,
    &'a str,
    (i64, Option<&'a str>),
    Vec<i64>,
    Option<Vec<(&'a str, Option<i64>)>>,
    &'a str,
);

fn pk_batch(rows: &[PkRow<'_>]) -> RecordBatch {
    let points = StructArray::new(
        Fields::from(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, true),
        ]),
        vec![
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| (row.2).0),
            )) as ArrayRef,
            Arc::new(StringArray::from_iter(rows.iter().map(|row| (row.2).1))),
        ],
        None,
    );
    let mut offsets = vec![0i32];
    let mut values = Vec::new();
    for row in rows {
        values.extend(row.3.iter());
        offsets.push(values.len() as i32);
    }
    let lists = ListArray::new(
        Arc::new(Field::new("item", DataType::Int64, true)),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(Int64Array::from_iter_values(values)),
        None,
    );
    let mut offsets = vec![0i32];
    let mut keys = Vec::new();
    let mut map_values = Vec::new();
    for row in rows {
        for (key, value) in row.4.as_deref().unwrap_or_default() {
            keys.push(*key);
            map_values.push(*value);
        }
        offsets.push(keys.len() as i32);
    }
    let entries = StructArray::new(
        Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, true),
        ]),
        vec![
            Arc::new(StringArray::from_iter_values(keys)) as ArrayRef,
            Arc::new(Int64Array::from_iter(map_values)) as ArrayRef,
        ],
        None,
    );
    let maps = MapArray::new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, true),
            ])),
            false,
        )),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        Some(NullBuffer::from_iter(
            rows.iter().map(|row| row.4.is_some()),
        )),
        false,
    );
    RecordBatch::try_new(
        pk_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(points),
            Arc::new(lists),
            Arc::new(maps),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.5))),
        ],
    )
    .unwrap()
}

/// The derived left-join step's schema over struct-primary-key sources: one
/// payload per side, widened to the subquery's names, with the non-null row
/// identity appended by the rewrite.
fn struct_pk_step_schema() -> SchemaRef {
    lakesoul_ivm::wide_left_join_view_schema_with_identity(
        &source_schema(),
        &source_schema(),
        &["s".to_string()],
        &["s".to_string()],
        &["g".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "k".to_string(),
                name: "k".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "m".to_string(),
                name: "m".to_string(),
            },
        ],
    )
    .unwrap()
}

/// A derived outer-join step over a source whose **primary key is a list**
/// cannot encode its row identity: the step is rejected with a clear reason
/// before it is created.
#[test_log::test(tokio::test)]
async fn a_list_primary_key_rejects_the_derived_identity() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lpk_l_{suffix}"),
                table_path(&dir, "left"),
                pk_schema(),
            )
            .with_primary_keys(vec!["l".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lpk_r_{suffix}"),
                table_path(&dir, "right"),
                pk_schema(),
            )
            .with_primary_keys(vec!["l".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lpk_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::sum_count_groups_mv_schema_for(
                    &struct_pk_step_schema(),
                    &["k".to_string(), "m".to_string()],
                    &[],
                    None,
                    None,
                    false,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_allow_fallback(false),
        )
        .await
        .unwrap();
    left.append_batch(
        runtime.client(),
        pk_batch(&[(1, "a", (10, None), vec![1], None, "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            pk_batch(&[(1, "a", (70, None), vec![2], None, "insert")]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_lpk_mv_{suffix} SELECT x.k, x.m, COUNT(*) AS count_v \
         FROM (SELECT b.k, c.m FROM ivm_lpk_l_{suffix} b \
               LEFT JOIN ivm_lpk_r_{suffix} c ON c.g = b.g) x \
               GROUP BY x.k, x.m"
    );
    let error = executor.execute(&statement).await.unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("cannot encode") && message.contains("primary key"),
        "unexpected error: {message}"
    );
    // The rejection happens before the step or the target is registered.
    assert!(
        executor
            .runtime()
            .metadata()
            .get_view_spec(&mv.table_id)
            .await
            .unwrap()
            .is_none()
    );
}

/// The `(k, payload, count)` rows of a count-only MV, ordered by `k`.
async fn read_group_counts(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(i64, String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select k, m, count_v from mv order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payloads = batch.column(1);
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row),
                arrow::util::display::array_value_to_string(payloads, row).unwrap(),
                counts.value(row),
            ));
        }
    }
    rows
}

/// A source pair whose **primary key is a struct** through a derived pair left
/// join (the join key `g` does not determine the right side, so the step is a
/// pair join and materializes a row identity): each side's identity is encoded
/// below the join, so the leaf-expression pushdown never sees a struct field
/// access above it.
#[test_log::test(tokio::test)]
async fn a_struct_primary_key_materializes_a_pair_step_identity() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ppk_l_{suffix}"),
                table_path(&dir, "left"),
                pk_schema(),
            )
            .with_primary_keys(vec!["s".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ppk_r_{suffix}"),
                table_path(&dir, "right"),
                pk_schema(),
            )
            .with_primary_keys(vec!["s".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ppk_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::sum_count_groups_mv_schema_for(
                    &struct_pk_step_schema(),
                    &["k".to_string(), "m".to_string()],
                    &[],
                    None,
                    None,
                    false,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_allow_fallback(false),
        )
        .await
        .unwrap();
    left.append_batch(
        runtime.client(),
        pk_batch(&[
            (1, "a", (10, Some("x")), vec![1], None, "insert"),
            (2, "b", (20, None), vec![2], None, "insert"),
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            pk_batch(&[(
                1,
                "a",
                (70, Some("y")),
                vec![7],
                Some(vec![("p", Some(1))]),
                "insert",
            )]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_ppk_mv_{suffix} SELECT x.k, x.m, COUNT(*) AS count_v FROM (SELECT b.k, c.m FROM ivm_ppk_l_{suffix} b LEFT JOIN ivm_ppk_r_{suffix} c ON c.g = b.g) x GROUP BY x.k, x.m"
    );
    let outcome = executor.execute(&statement).await;
    match outcome {
        Ok(execution) => assert_eq!(execution.action.label(), "bootstrap"),
        Err(error) => panic!("the struct primary key was rejected: {error}"),
    }
    let runtime = executor.runtime();
    assert_eq!(
        read_group_counts(runtime, &mv).await,
        vec![(1, "{p: 1}".to_string(), 1), (2, String::new(), 1)]
    );

    // A second right row whose struct key is new and whose join key matches
    // the left row behind `k = 1` adds another pair to that group.
    right
        .append_batch(
            runtime.client(),
            pk_batch(&[(
                4,
                "a",
                (80, None),
                vec![8],
                Some(vec![("p", Some(1))]),
                "insert",
            )]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_group_counts(runtime, &mv).await,
        vec![(1, "{p: 1}".to_string(), 2), (2, String::new(), 1)]
    );
}

/// A source pair whose **left primary key is a struct** through a derived
/// lookup step: the right side is keyed by the join key, so the step is a 1:1
/// lookup that materializes the join key once and carries the left struct key
/// by name.  The consumer reads the subplan's output columns (`k`, `m`); the
/// engine bridges them to the step's compact payload columns internally, and
/// maintains the groups as the right payload changes and disappears.
#[test_log::test(tokio::test)]
async fn a_struct_primary_key_materializes_a_lookup_step() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pkl_l_{suffix}"),
                table_path(&dir, "left"),
                pk_schema(),
            )
            .with_primary_keys(vec!["s".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    // The right side is keyed by the join key, which makes the step a lookup.
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pkl_r_{suffix}"),
                table_path(&dir, "right"),
                pk_schema(),
            )
            .with_primary_keys(vec!["g".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pkl_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::sum_count_groups_mv_schema_for(
                    &pk_schema(),
                    &["k".to_string(), "m".to_string()],
                    &[],
                    None,
                    None,
                    false,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_allow_fallback(false),
        )
        .await
        .unwrap();
    left.append_batch(
        runtime.client(),
        pk_batch(&[
            (1, "a", (10, Some("x")), vec![1], None, "insert"),
            (2, "b", (20, None), vec![2], None, "insert"),
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            pk_batch(&[(
                1,
                "a",
                (70, Some("y")),
                vec![7],
                Some(vec![("p", Some(1))]),
                "insert",
            )]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_pkl_mv_{suffix} SELECT x.k, x.m, COUNT(*) AS count_v FROM (SELECT b.k, c.m FROM ivm_pkl_l_{suffix} b LEFT JOIN ivm_pkl_r_{suffix} c ON c.g = b.g) x GROUP BY x.k, x.m"
    );
    let outcome = executor.execute(&statement).await;
    match outcome {
        Ok(execution) => assert_eq!(execution.action.label(), "bootstrap"),
        Err(error) => panic!("the struct primary key was rejected: {error}"),
    }
    let runtime = executor.runtime();
    assert_eq!(
        read_group_counts(runtime, &mv).await,
        vec![(1, "{p: 1}".to_string(), 1), (2, String::new(), 1)]
    );

    // The right payload changes, then disappears: the group follows it.
    right
        .append_batch(
            runtime.client(),
            pk_batch(&[(
                1,
                "a",
                (70, Some("y")),
                vec![7],
                Some(vec![("t", Some(3))]),
                "insert",
            )]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_group_counts(runtime, &mv).await,
        vec![(1, "{t: 3}".to_string(), 1), (2, String::new(), 1)]
    );

    right
        .append_batch(
            runtime.client(),
            pk_batch(&[(
                1,
                "a",
                (70, Some("y")),
                vec![7],
                Some(vec![("t", Some(3))]),
                "delete",
            )]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_group_counts(runtime, &mv).await,
        vec![(1, String::new(), 1), (2, String::new(), 1)]
    );
}
