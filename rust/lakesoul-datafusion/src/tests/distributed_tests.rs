// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Distributed execution over real LakeSoul tables.
//!
//! Each integration test boots three in-process gRPC workers sharing the
//! coordinator's object-store configuration, creates append-only LakeSoul
//! tables through the metadata catalog, writes parquet files via the normal
//! upsert path, and runs scan/filter/aggregate/join SQL on a distributed
//! session. Results are asserted against a single-node session, and
//! `EXPLAIN ANALYZE` output is asserted to contain distributed stages and
//! network metrics rather than merely observing speedups.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{ArrayRef, Int32Array};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use crate::distributed::{DistributedOptions, LakeSoulWorkerOptions, WorkerDiscovery};
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use crate::tests::{assert_batches_eq, create_table};
use crate::{Result, cli::CoreArgs};

const WORKER_COUNT: usize = 3;

/// Global suffix keeping per-test table names unique (metadata is only
/// cleaned once per test binary run).
static TABLE_SUFFIX: AtomicU64 = AtomicU64::new(0);

/// Spawns `WORKER_COUNT` in-process LakeSoul workers on free ports and
/// returns their `http://127.0.0.1:<port>` URLs.
async fn spawn_workers() -> (Vec<String>, JoinSet<()>) {
    let mut urls = Vec::new();
    let mut join_set = JoinSet::new();
    for _ in 0..WORKER_COUNT {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        urls.push(format!("http://127.0.0.1:{port}"));
        let options = LakeSoulWorkerOptions {
            core_args: CoreArgs::default(),
        };
        join_set.spawn(async move {
            crate::distributed::spawn_lakesoul_worker(&options, listener)
                .await
                .unwrap();
        });
    }
    (urls, join_set)
}

fn distributed_factory(
    client: MetaDataClientRef,
    worker_urls: Vec<String>,
    fallback_to_local: bool,
) -> Result<LakeSoulSessionFactory> {
    Ok(
        LakeSoulSessionFactory::new(client, &CoreArgs::default())?.with_distributed(
            DistributedOptions {
                discovery: WorkerDiscovery::Static(worker_urls),
                fallback_to_local,
                target_partitions: WORKER_COUNT,
                bytes_per_partition: Some(1),
            },
        ),
    )
}

fn single_node_factory(client: MetaDataClientRef) -> Result<LakeSoulSessionFactory> {
    LakeSoulSessionFactory::new(client, &CoreArgs::default())
}

fn t1_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]))
}

fn t2_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("c", DataType::Int32, false),
    ]))
}

fn batch(schema: SchemaRef, part: Vec<i32>, value: Vec<i32>) -> RecordBatch {
    let part: ArrayRef = Arc::new(Int32Array::from(part));
    let value: ArrayRef = Arc::new(Int32Array::from(value));
    RecordBatch::try_new(schema, vec![part, value]).unwrap()
}

/// Creates two append-only (no primary key) range-partitioned tables and
/// writes one file per partition value; three partitions fan out to three
/// workers with `bytes_per_partition = 1`. Returns the table names.
async fn seed_tables(client: MetaDataClientRef) -> Result<(String, String)> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let t1_name = format!("distributed_t1_{suffix}");
    let t2_name = format!("distributed_t2_{suffix}");

    create_table(
        client.clone(),
        &t1_name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(t1_schema())
            .with_range_partitions(vec!["part".to_string()])
            .build(),
    )
    .await?;
    let t1 = crate::lakesoul_table::LakeSoulTable::for_name(&t1_name).await?;
    t1.execute_upsert(batch(
        t1_schema(),
        vec![0, 0, 0, 1, 1, 1, 2, 2, 2],
        vec![1, 2, 3, 11, 12, 13, 21, 22, 23],
    ))
    .await?;

    create_table(
        client.clone(),
        &t2_name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(t2_schema())
            .with_range_partitions(vec!["part".to_string()])
            .build(),
    )
    .await?;
    let t2 = crate::lakesoul_table::LakeSoulTable::for_name(&t2_name).await?;
    t2.execute_upsert(batch(
        t2_schema(),
        vec![0, 0, 0, 1, 1, 1, 2, 2, 2],
        vec![2, 2, 2, 13, 13, 13, 23, 23, 23],
    ))
    .await?;
    Ok((t1_name, t2_name))
}

/// Runs every SQL query on the distributed and the single-node session and
/// asserts identical (sorted) results.
async fn assert_matches_single_node(
    distributed: &LakeSoulSessionFactory,
    single_node: &LakeSoulSessionFactory,
    queries: &[(String, Vec<&str>)],
) -> Result<()> {
    let dist_ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    let local_ctx = single_node.create_session(&LakeSoulSessionOptions::default())?;
    for (sql, expected) in queries {
        let dist_result = dist_ctx.sql(sql).await?.collect().await?;
        let local_result = local_ctx.sql(sql).await?.collect().await?;
        assert_batches_eq(&format!("{sql} (distributed)"), expected, &dist_result);
        assert_batches_eq(&format!("{sql} (single-node)"), expected, &local_result);
    }
    Ok(())
}

const FILTER_RESULT: &[&str] = &[
    "+------+----+",
    "| part | b  |",
    "+------+----+",
    "| 1    | 11 |",
    "| 1    | 12 |",
    "| 1    | 13 |",
    "| 2    | 21 |",
    "| 2    | 22 |",
    "| 2    | 23 |",
    "+------+----+",
];

#[test]
fn test_distributed_scan_filter_aggregate_join() {
    Runtime::new()
        .unwrap()
        .block_on(test_distributed_scan_filter_aggregate_join_inner())
        .unwrap();
}

async fn test_distributed_scan_filter_aggregate_join_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let (t1, t2) = seed_tables(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers().await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let single_node = single_node_factory(client.clone())?;

    let queries: Vec<(String, Vec<&str>)> = vec![
        (
            format!("SELECT part, b FROM {t1} WHERE b > 5 ORDER BY part, b"),
            FILTER_RESULT.to_vec(),
        ),
        (
            format!(
                "SELECT part, count(*) AS cnt, sum(b) AS total \
                 FROM {t1} GROUP BY part ORDER BY part"
            ),
            vec![
                "+------+-----+-------+",
                "| part | cnt | total |",
                "+------+-----+-------+",
                "| 0    | 3   | 6     |",
                "| 1    | 3   | 36    |",
                "| 2    | 3   | 66    |",
                "+------+-----+-------+",
            ],
        ),
        (
            format!(
                "SELECT t1.part, t1.b, t2.c FROM {t1} t1 JOIN {t2} t2 \
                 ON t1.part = t2.part AND t1.b = t2.c ORDER BY t1.part, t1.b"
            ),
            vec![
                "+------+----+----+",
                "| part | b  | c  |",
                "+------+----+----+",
                "| 0    | 2  | 2  |",
                "| 0    | 2  | 2  |",
                "| 0    | 2  | 2  |",
                "| 1    | 13 | 13 |",
                "| 1    | 13 | 13 |",
                "| 1    | 13 | 13 |",
                "| 2    | 23 | 23 |",
                "| 2    | 23 | 23 |",
                "| 2    | 23 | 23 |",
                "+------+----+----+",
            ],
        ),
    ];
    assert_matches_single_node(&distributed, &single_node, &queries).await?;

    workers.abort_all();
    Ok(())
}

#[test]
fn test_explain_analyze_shows_distributed_stages() {
    Runtime::new()
        .unwrap()
        .block_on(test_explain_analyze_shows_distributed_stages_inner())
        .unwrap();
}

async fn test_explain_analyze_shows_distributed_stages_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let (t1, _t2) = seed_tables(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers().await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;

    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    let sql = format!("EXPLAIN ANALYZE SELECT part, count(*) FROM {t1} GROUP BY part");
    let plan = ctx.sql(&sql).await?.collect().await?;
    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
        .unwrap()
        .to_string();

    assert!(
        formatted.contains("DistributedExec"),
        "expected DistributedExec stages in plan:\n{formatted}"
    );
    assert!(
        formatted.contains("NetworkShuffleExec"),
        "expected network shuffles in plan:\n{formatted}"
    );
    assert!(
        formatted.contains("Plan with Metrics"),
        "expected distributed metrics output in plan:\n{formatted}"
    );
    assert!(
        formatted.contains("metrics=[output_rows="),
        "expected node metrics in plan:\n{formatted}"
    );
    assert!(
        formatted.contains("tasks=3"),
        "expected multi-task distributed stage in plan:\n{formatted}"
    );

    workers.abort_all();
    Ok(())
}

#[test]
fn test_production_fails_fast_without_workers() {
    Runtime::new()
        .unwrap()
        .block_on(test_production_fails_fast_without_workers_inner())
        .unwrap();
}

async fn test_production_fails_fast_without_workers_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let (t1, _t2) = seed_tables(client.clone()).await?;

    let distributed = distributed_factory(client.clone(), Vec::new(), false)?;
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;

    let sql = format!("SELECT part, b FROM {t1} WHERE b > 5");
    let dataframe = ctx.sql(&sql).await.expect("logical planning must succeed");
    let err = dataframe
        .collect()
        .await
        .expect_err("production must fast-fail with no ready workers");
    assert!(
        err.to_string().contains("no ready LakeSoul workers"),
        "unexpected error: {err}"
    );

    // The table is still readable single-node for comparison.
    let single_node = single_node_factory(client)?;
    let batches = single_node
        .create_session(&LakeSoulSessionOptions::default())?
        .sql(&format!(
            "SELECT part, b FROM {t1} WHERE b > 5 ORDER BY part, b"
        ))
        .await?
        .collect()
        .await?;
    assert_batches_eq(
        "SELECT part, b FROM distributed_t1 WHERE b > 5 ORDER BY part, b",
        FILTER_RESULT,
        &batches,
    );
    Ok(())
}

#[test]
fn test_dev_fallback_runs_single_node() {
    Runtime::new()
        .unwrap()
        .block_on(test_dev_fallback_runs_single_node_inner())
        .unwrap();
}

async fn test_dev_fallback_runs_single_node_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let (t1, _t2) = seed_tables(client.clone()).await?;

    let distributed = distributed_factory(client.clone(), Vec::new(), true)?;
    let single_node = single_node_factory(client.clone())?;

    let queries: Vec<(String, Vec<&str>)> = vec![(
        format!("SELECT part, b FROM {t1} WHERE b > 5 ORDER BY part, b"),
        FILTER_RESULT.to_vec(),
    )];
    assert_matches_single_node(&distributed, &single_node, &queries).await
}

#[test]
fn worker_urls_reject_malformed_entries() {
    use crate::distributed::StaticWorkerResolver;

    let resolver = StaticWorkerResolver::new(vec!["not a url".to_string()]);
    assert!(resolver.is_err());
}
