// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! SQL aggregates consume latest MOR rows, not native merge contributions.
//!
//! The reference tables below contain manually derived latest values. In
//! particular, an absent update field preserves an existing value, an explicit
//! NULL overwrites it, and a new key with no contribution has a NULL value.
//! Stored NOT NULL value declarations therefore cannot justify non-null MOR
//! read schemas or rewriting COUNT(value) to COUNT(*). Append-only tables are
//! deliberately checked separately and retain their NOT NULL read schema.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::ScalarValue;
use datafusion::datasource::MemTable;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetSource};
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::SessionContext;
use datafusion_distributed::{
    DistributedExec, DistributedLeafExec, NetworkBoundaryExt, Stage, display_plan_ascii,
};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::physical_plan::MergeParquetExec;
use lakesoul_io::writer::async_writer::{
    AsyncBatchWriter, AsyncSendableMutableLakeSoulWriter,
};
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

use crate::Result;
use crate::catalog::create_io_config_builder;
use crate::cli::CoreArgs;
use crate::distributed::codec::roundtrip;
use crate::lakesoul_table::LakeSoulTable;
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use crate::tests::create_table_with_file_format;
use crate::tests::distributed_tests::{
    distributed_factory_sized, run_distributed_test, spawn_workers,
};

// Vortex and VortexCompact share the same scan and distributed codec; only
// their writer compression differs (LakeSoulFormatRegistry::file_format).
const FORMATS: [PhysicalFormat; 2] =
    [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact];
const AGGREGATES: &str = "SUM(v) AS sum_v, AVG(v) AS avg_v, \
    COUNT(v) AS count_v, COUNT(*) AS count_rows, MIN(v) AS min_v, MAX(v) AS max_v";
type Row = (i32, i32, Option<i32>, Option<i32>);

fn schema(value_nullable: bool) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, value_nullable),
        Field::new("evolved", DataType::Int32, value_nullable),
    ]))
}

fn rows_batch(value_nullable: bool, rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        schema(value_nullable),
        vec![
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int32Array::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )) as ArrayRef,
        ],
    )
    .unwrap()
}

fn unique_table(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}

async fn create_fixture(
    client: MetaDataClientRef,
    format: PhysicalFormat,
    stored_nullable: bool,
    primary_key: bool,
) -> Result<LakeSoulTable> {
    let name = unique_table("mor_nullability");
    create_table_with_file_format(
        client.clone(),
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(schema(stored_nullable))
            .with_primary_keys(if primary_key {
                vec!["id".to_string()]
            } else {
                vec![]
            })
            .with_range_partitions(vec!["part".to_string()])
            .build(),
        format,
    )
    .await?;
    let table =
        LakeSoulTable::for_namespace_and_name("default", &name, Some(client)).await?;
    assert_eq!(
        table.schema(),
        schema(stored_nullable),
        "stored declaration changed"
    );
    Ok(table)
}

/// Write and commit the actual partial-update storage schema, without
/// manufacturing NULL contributions for absent columns. Each awaited commit
/// establishes deterministic version order.
pub(super) async fn write_storage_batch(
    client: MetaDataClientRef,
    table: &LakeSoulTable,
    format: PhysicalFormat,
    batch: RecordBatch,
) -> Result<()> {
    let input_schema = batch.schema();
    let config = create_io_config_builder(
        client,
        Some(table.table_name()),
        false,
        table.table_namespace(),
        Default::default(),
        Default::default(),
    )
    .await?
    .with_schema(input_schema.clone())
    .with_physical_format(format)
    .build();
    let mut writer = AsyncSendableMutableLakeSoulWriter::from_io_config(config).await?;
    writer.write_record_batch(batch).await?;
    let outputs = Box::new(writer).flush_and_close().await?;
    table.commit_flush_result(outputs).await
}

fn initial_rows() -> Vec<Row> {
    vec![
        (0, 1, Some(10), Some(100)),
        (0, 2, Some(20), Some(200)),
        (1, 3, Some(30), Some(300)),
        (1, 4, Some(40), Some(400)),
        (2, 5, Some(50), Some(500)),
        (2, 6, Some(60), Some(600)),
    ]
}

async fn apply_partial_updates(
    client: MetaDataClientRef,
    table: &LakeSoulTable,
    format: PhysicalFormat,
) -> Result<()> {
    // All update fields are nullable in storage. `evolved` is omitted,
    // including for new id=7, even when its table declaration is NOT NULL.
    let update_schema = Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, true),
    ]));
    let updates = RecordBatch::try_new(
        update_schema,
        vec![
            Arc::new(Int32Array::from(vec![0, 0, 1, 1, 2, 2])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 7])) as ArrayRef,
            Arc::new(Int32Array::from(vec![
                Some(100),
                None,
                None,
                None,
                Some(500),
                Some(700),
            ])) as ArrayRef,
        ],
    )?;
    write_storage_batch(client.clone(), table, format, updates).await?;
    // The newest id=1 update has no value contribution: v=100 must survive.
    // New ids 8 and 9 have never contributed either value, so both are NULL.
    let keys = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("id", DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![0, 0, 1])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1, 8, 9])) as ArrayRef,
        ],
    )?;
    write_storage_batch(client, table, format, keys).await
}

/// Independently derived from the three commits, not read back from LakeSoul.
/// Group 0 is mixed NULL; group 1 is all NULL; group 2 is entirely non-NULL.
/// Thus global SUM(v)=1360, AVG(v)=340, COUNT(v)=4, COUNT(*)=9,
/// MIN(v)=60, MAX(v)=700; evolved has six contributions summing to 2100.
fn latest_rows() -> Vec<Row> {
    vec![
        (0, 1, Some(100), Some(100)),
        (0, 2, None, Some(200)),
        (0, 8, None, None),
        (1, 3, None, Some(300)),
        (1, 4, None, Some(400)),
        (1, 9, None, None),
        (2, 5, Some(500), Some(500)),
        (2, 6, Some(60), Some(600)),
        (2, 7, Some(700), None),
    ]
}

fn register_reference(
    ctx: &SessionContext,
    value_nullable: bool,
    rows: &[Row],
) -> Result<String> {
    let name = unique_table("latest_reference");
    let batch = rows_batch(value_nullable, rows);
    ctx.register_table(
        &name,
        Arc::new(MemTable::try_new(batch.schema(), vec![vec![batch]])?),
    )?;
    Ok(name)
}

fn query_cases() -> Vec<String> {
    vec![
        format!("SELECT {AGGREGATES} FROM $TABLE"),
        format!("SELECT part, {AGGREGATES} FROM $TABLE GROUP BY part ORDER BY part"),
        format!("SELECT {AGGREGATES} FROM $TABLE WHERE part = 2"),
        format!("SELECT {AGGREGATES} FROM $TABLE WHERE part = 0"),
        format!("SELECT {AGGREGATES} FROM $TABLE WHERE part = 1"),
        format!("SELECT {AGGREGATES} FROM $TABLE WHERE v < 0"),
        format!(
            "SELECT part, {AGGREGATES} FROM $TABLE WHERE v < 0 GROUP BY part ORDER BY part"
        ),
        "SELECT v, evolved FROM $TABLE ORDER BY part, id".to_string(),
        "SELECT v, evolved FROM $TABLE WHERE id IN (1, 2, 7, 8, 99) ORDER BY id"
            .to_string(),
        format!("SELECT {AGGREGATES} FROM $TABLE WHERE id IN (1, 2, 7, 8, 99)"),
        "SELECT id, v FROM $TABLE WHERE v IN (10, 20, 30, 40, 50) ORDER BY id"
            .to_string(),
        "SELECT id, v FROM $TABLE WHERE v IS NULL ORDER BY id".to_string(),
        "SELECT id, v FROM $TABLE WHERE v > 50 ORDER BY id".to_string(),
        "SELECT SUM(evolved) AS sum_evolved, AVG(evolved) AS avg_evolved, \
            COUNT(evolved) AS count_evolved, COUNT(*) AS count_rows, \
            MIN(evolved) AS min_evolved, MAX(evolved) AS max_evolved FROM $TABLE"
            .to_string(),
        "SELECT part, id, v FROM $TABLE \
            WHERE v > (SELECT AVG(v) FROM $TABLE) ORDER BY part, id"
            .to_string(),
        "SELECT (SELECT SUM(v) FROM $TABLE WHERE part = 1) AS all_null_total".to_string(),
    ]
}

/// DistributedLeafExec hides its per-task variants from children(). Include
/// them and its coordinator original when checking the schemas sent on wire.
fn plan_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut pending = vec![plan.clone()];
    let mut nodes = vec![];
    while let Some(node) = pending.pop() {
        pending.extend(node.children().into_iter().rev().cloned());
        if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
            pending.extend(leaf.variants().iter().rev().cloned());
            pending.push(leaf.original().clone());
        }
        nodes.push(node);
    }
    nodes
}

fn assert_read_fields(schema: &Schema, value_nullable: bool, label: &str) {
    for field in schema.fields() {
        match field.name().as_str() {
            "part" | "id" => assert!(
                !field.is_nullable(),
                "{label}: key/partition field {field:?}"
            ),
            "v" | "evolved" => {
                assert_eq!(field.is_nullable(), value_nullable, "{label}: {field:?}")
            }
            _ => {}
        }
    }
}

/// Record the real physical AggregateExec input/output as well as merge
/// output. Codec round trips must preserve these contracts, not just values.
fn aggregate_merge_schemas(
    plan: &Arc<dyn ExecutionPlan>,
    value_nullable: bool,
    label: &str,
) -> Vec<(String, Vec<SchemaRef>)> {
    plan_nodes(plan)
        .into_iter()
        .filter_map(|node| {
            if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
                let schemas = vec![
                    aggregate.input_schema(),
                    aggregate.input().schema(),
                    node.schema(),
                ];
                for schema in &schemas {
                    assert_read_fields(schema, value_nullable, label);
                }
                Some((format!("AggregateExec:{:?}", aggregate.mode()), schemas))
            } else if node.is::<MergeParquetExec>() {
                assert_read_fields(&node.schema(), value_nullable, label);
                Some(("MergeParquetExec".to_string(), vec![node.schema()]))
            } else {
                None
            }
        })
        .collect()
}

fn scalar_rows(batches: &[RecordBatch]) -> Vec<Vec<ScalarValue>> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                batch
                    .columns()
                    .iter()
                    .map(|array| ScalarValue::try_from_array(array, row).unwrap())
                    .collect()
            })
        })
        .collect()
}

fn assert_value_filter_above_merge(plan: &Arc<dyn ExecutionPlan>, sql: &str) {
    let nodes = plan_nodes(plan);
    assert!(
        nodes.iter().any(|node| {
            node.downcast_ref::<FilterExec>().is_some_and(|filter| {
                filter.predicate().to_string().contains("v@")
                    && plan_nodes(filter.input())
                        .iter()
                        .any(|input| input.is::<MergeParquetExec>())
            })
        }),
        "value filter must remain above MOR: {sql}\n{}",
        display_plan_ascii(plan.as_ref(), false)
    );
    for node in nodes {
        if let Some(scan) = node.downcast_ref::<DataSourceExec>()
            && let Some(config) = scan.data_source().downcast_ref::<FileScanConfig>()
            && let Some(filter) = config.file_source().filter()
        {
            assert!(
                !filter.to_string().contains("v@"),
                "value predicate reached a file source: {sql}: {filter}"
            );
        }
    }
}

async fn assert_query(
    ctx: &SessionContext,
    reference_ctx: &SessionContext,
    table: &str,
    reference: &str,
    template: &str,
    value_nullable: bool,
    require_dispatch: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let sql = template.replace("$TABLE", table);
    let reference_sql = template.replace("$TABLE", reference);
    let expected_df = reference_ctx.sql(&reference_sql).await?;
    let expected_schema = expected_df.schema().as_arrow().clone();
    let expected = expected_df.collect().await?;
    let dataframe = ctx.sql(&sql).await?;
    assert_eq!(
        dataframe.schema().as_arrow().fields(),
        expected_schema.fields(),
        "logical output: {sql}"
    );
    let logical_schema = dataframe.schema().as_arrow().clone();
    let plan = dataframe.create_physical_plan().await?;
    assert_eq!(
        plan.schema().fields(),
        logical_schema.fields(),
        "physical output: {sql}"
    );
    let snapshots = aggregate_merge_schemas(&plan, value_nullable, &sql);
    // A local plan encodes the same AggregateExec and LakeSoul extension
    // nodes as worker stages; distributed execution below exercises actual
    // specialization, transport, decoding and consumer-visible batch schema.
    if !plan.is::<DistributedExec>() {
        let decoded = roundtrip::exec(plan.clone())?;
        assert_eq!(decoded.schema(), plan.schema(), "wire output schema: {sql}");
        assert_eq!(
            aggregate_merge_schemas(&decoded, value_nullable, &sql),
            snapshots,
            "wire aggregate/merge schemas: {sql}"
        );
    }
    if require_dispatch {
        assert!(
            plan.is::<DistributedExec>(),
            "query must have real distributed stages: {sql}\n{}",
            display_plan_ascii(plan.as_ref(), false)
        );
    }
    let batches = collect(plan.clone(), ctx.task_ctx()).await?;
    for batch in &batches {
        assert_eq!(
            batch.schema().fields(),
            logical_schema.fields(),
            "consumer batch schema: {sql}"
        );
    }
    assert_eq!(
        scalar_rows(&batches),
        scalar_rows(&expected),
        "manual latest-row reference: {sql}"
    );
    if require_dispatch {
        let channels = plan
            .metrics()
            .and_then(|metrics| metrics.sum_by_name("remote_coordinator_channels"))
            .map(|value| value.as_usize())
            .unwrap_or(0);
        assert!(channels > 0, "no worker stage was dispatched: {sql}");
    }
    Ok(plan)
}

async fn assert_fixture_queries(
    factory: &LakeSoulSessionFactory,
    reference_ctx: &SessionContext,
    table: &LakeSoulTable,
    rows: &[Row],
    value_nullable: bool,
    templates: &[String],
    forced_splitting: bool,
) -> Result<()> {
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;
    let reference = register_reference(reference_ctx, value_nullable, rows)?;
    let read = ctx.table(table.table_name()).await?;
    assert_eq!(
        read.schema().as_arrow().fields(),
        schema(value_nullable).fields(),
        "logical read must describe possible merged NULLs"
    );
    for template in templates {
        // A MOR work unit is one range partition and is indivisible, so a
        // query restricted to a single partition, an empty scan, or a scan
        // under the default byte heuristic legitimately stays on the
        // coordinator. Only the factory that forces one byte per partition,
        // with two workers and several partitions, must dispatch a stage.
        let require_dispatch = forced_splitting
            && !rows.is_empty()
            && (template.contains("SUM(") || template.contains("AVG("))
            && !template.contains("part = ")
            && !template.starts_with("SELECT (SELECT");
        let plan = assert_query(
            &ctx,
            reference_ctx,
            table.table_name(),
            &reference,
            template,
            value_nullable,
            require_dispatch,
        )
        .await?;
        if !rows.is_empty() && template.starts_with("SELECT SUM(v)") {
            assert!(
                plan_nodes(&plan)
                    .iter()
                    .any(|node| node.is::<AggregateExec>()),
                "fixture must exercise physical AggregateExec input/output"
            );
        }
        if template.contains("WHERE id IN (") {
            let parquet_pk_filters: Vec<bool> = plan_nodes(&plan)
                .iter()
                .filter_map(|node| {
                    let scan = node.downcast_ref::<DataSourceExec>()?;
                    let config = scan.data_source().downcast_ref::<FileScanConfig>()?;
                    config.file_source().is::<ParquetSource>().then(|| {
                        config
                            .file_source()
                            .filter()
                            .is_some_and(|filter| filter.to_string().contains("id@"))
                    })
                })
                .collect();
            if !parquet_pk_filters.is_empty() {
                assert!(
                    parquet_pk_filters.into_iter().any(|pushed| pushed),
                    "PK predicate must reach the Parquet scan: {template}"
                );
            }
        }
        if !rows.is_empty()
            && value_nullable
            && (template.contains("WHERE v IN (10,")
                || template.contains("WHERE v IS NULL")
                || template.contains("WHERE v < 0"))
        {
            assert_value_filter_above_merge(&plan, template);
        }
    }
    Ok(())
}

async fn assert_sql_created_provider(
    client: MetaDataClientRef,
    factory: &LakeSoulSessionFactory,
    reference_ctx: &SessionContext,
    format: PhysicalFormat,
) -> Result<()> {
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;
    let name = unique_table("mor_created_nullability");
    let path = std::env::current_dir()?.join("default").join(&name);
    ctx.sql(&format!(
        "CREATE EXTERNAL TABLE lakesoul.default.{name} (
            part INT NOT NULL, id INT NOT NULL, v INT NOT NULL,
            evolved INT NOT NULL, PRIMARY KEY (id)
         ) STORED AS LAKESOUL PARTITIONED BY (part)
         LOCATION '{}' OPTIONS ('file_format' '{}', 'hashBucketNum' '4')",
        path.display(),
        format.name()
    ))
    .await?
    .collect()
    .await?;
    // Inspect the provider returned by CREATE, before a fresh catalog load
    // can hide a discrepancy between newly-created and loaded providers.
    assert_eq!(
        ctx.table(&name).await?.schema().as_arrow().fields(),
        schema(true).fields()
    );
    let table =
        LakeSoulTable::for_namespace_and_name("default", &name, Some(client.clone()))
            .await?;
    assert_eq!(table.schema(), schema(false));
    let initial = rows_batch(true, &initial_rows());
    let old_schema = Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        // Nullable storage metadata, but every actual primary key is non-NULL.
        Field::new("id", DataType::Int32, true),
        Field::new("v", DataType::Int32, true),
    ]));
    let old_file = RecordBatch::try_new(old_schema, initial.columns()[..3].to_vec())?;
    // `evolved` has no file contribution at all, as with old files after a
    // schema addition. Its original NOT NULL declaration is not a default.
    write_storage_batch(client.clone(), &table, format, old_file).await?;
    apply_partial_updates(client, &table, format).await?;
    let expected: Vec<Row> = latest_rows()
        .into_iter()
        .map(|(part, id, v, _)| (part, id, v, None))
        .collect();
    let reference = register_reference(reference_ctx, true, &expected)?;
    for template in [
        format!("SELECT {AGGREGATES} FROM $TABLE"),
        "SELECT id, v, evolved FROM $TABLE ORDER BY part, id".to_string(),
        "SELECT COUNT(evolved) AS count_evolved, SUM(evolved) AS sum_evolved,
            AVG(evolved) AS avg_evolved, MIN(evolved) AS min_evolved,
            MAX(evolved) AS max_evolved FROM $TABLE"
            .to_string(),
    ] {
        assert_query(
            &ctx,
            reference_ctx,
            &name,
            &reference,
            &template,
            true,
            false,
        )
        .await?;
    }
    Ok(())
}

fn assert_split_stage(plan: &Arc<dyn ExecutionPlan>) {
    assert!(
        plan_nodes(plan).iter().any(|node| {
            node.as_network_boundary().is_some_and(|boundary| {
                matches!(boundary.input_stage(), Stage::Local(stage) if stage.tasks >= 2)
            })
        }),
        "forced splitting must use multiple worker tasks:\n{}",
        display_plan_ascii(plan.as_ref(), false)
    );
    let channels = plan
        .metrics()
        .and_then(|metrics| metrics.sum_by_name("remote_coordinator_channels"))
        .map(|value| value.as_usize())
        .unwrap_or(0);
    assert!(
        channels >= 2,
        "forced split did not dispatch multiple worker tasks"
    );
}

#[test]
fn sql_mor_value_aggregates_and_nullability_survive_distribution() {
    run_distributed_test(
        sql_mor_value_aggregates_and_nullability_survive_distribution_inner(),
    );
}

async fn sql_mor_value_aggregates_and_nullability_survive_distribution_inner()
-> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let single = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?
        .with_target_partitions(4);
    // The oracle is an independent, ordinary DataFusion session, never a
    // second LakeSoul scan and never a distributed reference-table scan.
    let reference_ctx = SessionContext::new();
    let (worker_urls, mut workers) = spawn_workers(2).await;
    let result = async {
        // The second element marks the factory that forces several scan tasks
        // (one byte per partition); the default sizing leaves small scans on
        // the coordinator, so only it can require a dispatched stage.
        let mut factories = vec![(single, false)];
        for bytes_per_partition in [None, Some(1)] {
            let forced_splitting = bytes_per_partition.is_some();
            factories.push((
                distributed_factory_sized(
                    client.clone(),
                    worker_urls.clone(),
                    4,
                    bytes_per_partition,
                    false,
                )?,
                forced_splitting,
            ));
        }
        for format in FORMATS {
            for stored_nullable in [false, true] {
                let table = create_fixture(client.clone(), format, stored_nullable, true).await?;
                let initial = initial_rows();
                // Non-NULL data still has nullable physical storage fields.
                write_storage_batch(client.clone(), &table, format, rows_batch(true, &initial)).await?;
                let initial_queries = vec![
                    format!("SELECT {AGGREGATES} FROM $TABLE"),
                    "SELECT v, evolved FROM $TABLE ORDER BY part, id".to_string(),
                ];
                for (factory, forced_splitting) in &factories {
                    assert_fixture_queries(factory, &reference_ctx, &table, &initial, true, &initial_queries, *forced_splitting).await?;
                }
                apply_partial_updates(client.clone(), &table, format).await?;
                let latest = latest_rows();
                for (factory, forced_splitting) in &factories {
                    assert_fixture_queries(factory, &reference_ctx, &table, &latest, true, &query_cases(), *forced_splitting).await?;
                }
                // A grouped aggregate crosses scan and aggregate stages over
                // three MOR work units; forced splitting cannot duplicate a
                // merged key or split its history among consumers.
                let forced = &factories[2].0;
                let ctx = forced.create_session(&LakeSoulSessionOptions::default())?;
                let reference = register_reference(&reference_ctx, true, &latest)?;
                let grouped = format!("SELECT part, {AGGREGATES} FROM $TABLE GROUP BY part ORDER BY part");
                let plan = assert_query(&ctx, &reference_ctx, table.table_name(), &reference, &grouped, true, true).await?;
                assert_split_stage(&plan);
                let stored = LakeSoulTable::for_namespace_and_name("default", table.table_name(), Some(client.clone())).await?;
                assert_eq!(stored.schema(), schema(stored_nullable), "reads must not rewrite the stored declaration");

                let empty = create_fixture(client.clone(), format, stored_nullable, true).await?;
                let empty_queries = vec![
                    format!("SELECT {AGGREGATES} FROM $TABLE"),
                    format!("SELECT part, {AGGREGATES} FROM $TABLE GROUP BY part ORDER BY part"),
                    "SELECT v, evolved FROM $TABLE ORDER BY part, id".to_string(),
                ];
                for (factory, forced_splitting) in &factories {
                    assert_fixture_queries(factory, &reference_ctx, &empty, &[], true, &empty_queries, *forced_splitting).await?;
                }
            }
            assert_sql_created_provider(client.clone(), &factories[0].0, &reference_ctx, format).await?;
            // Append-only rows do not have MOR's absent-field/overwrite
            // semantics. Keep both their physical and logical values NOT NULL.
            let append = create_fixture(client.clone(), format, false, false).await?;
            let rows = initial_rows();
            write_storage_batch(client.clone(), &append, format, rows_batch(false, &rows)).await?;
            let append_queries = vec![
                format!("SELECT {AGGREGATES} FROM $TABLE"),
                "SELECT v, evolved FROM $TABLE ORDER BY part, id".to_string(),
            ];
            for (factory, forced_splitting) in &factories {
                assert_fixture_queries(factory, &reference_ctx, &append, &rows, false, &append_queries, *forced_splitting).await?;
            }
        }
        Ok(())
    }.await;
    workers.abort_all();
    while workers.join_next().await.is_some() {}
    result
}
