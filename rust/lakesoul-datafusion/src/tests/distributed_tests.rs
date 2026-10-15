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
//!
//! # Merge-on-read comparison matrix
//!
//! Covered here: append-only scans, primary-key tables with several versions
//! of the same keys across files, a scan that is a single merge work unit and
//! one that is several (distributed union), range partitions, hash buckets,
//! CDC delete tombstones, filter pushdown, projections, `ORDER BY`/`LIMIT`,
//! hash `GROUP BY` after the scan, queries running while upserts commit, and
//! a plan-only matrix that pins which of the four table shapes
//! (primary key × range partition) the distributed planner distributes at all
//! (`test_distribution_matrix_by_table_shape`).
//!
//! Beyond matching a single-node session, the correctness tests prove remote
//! execution and exact semantics, not just results:
//!
//! - a CDC table with range partitions whose merge runs on the workers and
//!   whose non-key filters must not resurrect stale versions or drop deleted
//!   keys, projecting value columns (plus one exact full-projection query)
//!   (`test_remote_cdc_merge_with_non_key_filter`);
//! - several hash buckets behind a composite primary key, with the same key
//!   updated across three files, run with more and with fewer workers than
//!   merge work units (`test_multi_bucket_composite_pk_merge_on_workers`);
//! - ordered Top-K / `LIMIT` / `OFFSET` over several merge work units,
//!   compared in output order (the shared `assert_batches_eq` sorts rows,
//!   which cannot catch a wrong global order)
//!   (`test_remote_topk_limit_offset_keeps_output_order`);
//! - a query keeping the file set it captured at planning time across commits
//!   landing while its workers run, with deterministic barriers
//!   (`test_a_query_keeps_the_file_set_it_captured`);
//! - the failure tests below also assert that a failed query drains its tasks
//!   from the workers and that the system recovers once the fault is removed.
//!
//! # Failure paths
//!
//! Covered: a worker that cannot build a session for a task fails the query
//! within a bounded time, instead of hanging or of the coordinator answering
//! without it — also with `fallback_to_local = true`, where a failed execution
//! must surface as an error rather than a silent local rerun. The test pins
//! the injection from the worker side - it asserts the worker that cannot
//! serve was really handed a task - so a query that failed for some other
//! reason cannot pass for the injected failure. Failed queries drain their
//! tasks from the healthy workers within bounded time, and a fault injected
//! through the object store recovers once it is removed
//! (`test_a_failing_read_cleans_up_tasks_and_recovers`). The transfer-level
//! break itself is the library's own `tests/error_propagation.rs` and a read
//! that breaks after progress is
//! `test_a_read_failing_after_progress_fails_distributed_query`. The
//! large-table cancellation test additionally cancels queries with
//! aggregating, joining and sorting head stages by dropping the stream right
//! after the workers' task counters show a dispatch, before it is ever
//! polled.
//!
//! Not covered by this suite, deliberately:
//!
//! - *a worker lost partway through a query*: a worker that only starts
//!   rejecting after the first task it serves cannot be injected
//!   deterministically, because whether it is asked for a second task at all is
//!   the library's assignment. Measured over five runs of a 32-file shuffling
//!   query with four workers, one asked it exactly once, and the assignment
//!   races the query's own failure, so widening the table moves the flake
//!   instead of removing it;
//! - *metadata becoming unavailable during a running query*: the object store
//!   has a seam (`RuntimeEnv::register_object_store`, which
//!   `test_a_read_failing_after_progress_fails_distributed_query` uses) while
//!   the metadata client has none, so nothing can interrupt it mid-query;
//! - *compacted + uncompacted files in one partition*: this build has no
//!   Rust-side compaction entry point (the compaction service lives in the
//!   Spark integration), so the mix cannot be produced here;
//! - *schema evolution and default-column filling across files*: writing a
//!   narrower batch into a wider table needs a narrower declared schema,
//!   which the upsert path does not offer, so this suite cannot produce the
//!   state. The append-only scan leaf over such files is pinned by
//!   `lakesoul-io`'s `append_only_leaf` test, and the nullability half of the
//!   merge path — a logically non-null column kept non-null while merging
//!   files that lack it — by `catalog`/`merge` unit tests instead.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array, Int64Array};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::{ExecutionPlan, collect, execute_stream};
use datafusion::prelude::SessionContext;
use datafusion_distributed::grpc::{GetTaskProgressRequest, ObservabilityServiceClient};
use datafusion_distributed::{
    DistributedExec, DistributedExt, DistributedLeafExec, Worker, display_plan_ascii,
};
use futures::StreamExt;
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::physical_plan::MergeParquetExec;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;
use vortex_datafusion::VortexSource;

use crate::distributed::{
    DistributedOptions, LakeSoulWorkerOptions, StaticWorkerResolver, WorkerDiscovery,
};
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use crate::tests::{
    assert_batches_eq, assert_batches_eq_ordered, cdc_batch, create_cdc_table,
    create_cdc_table_with_range_partitions, create_table_with_file_format,
};
use crate::{Result, cli::CoreArgs};

const WORKER_COUNT: usize = 3;

/// Global suffix keeping per-test table names unique (metadata is only
/// cleaned once per test binary run).
static TABLE_SUFFIX: AtomicU64 = AtomicU64::new(0);

/// Serializes the metadata-heavy distributed tests.
///
/// These tests create independent metadata clients and connection pools, so
/// running them all in parallel can exhaust the local PostgreSQL connection
/// budget (SQLSTATE 53300) and make unrelated tests fail.
static DISTRIBUTED_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

fn run_distributed_test<F>(future: F)
where
    F: std::future::Future<Output = Result<()>>,
{
    let _serialized = DISTRIBUTED_TEST_LOCK.lock();
    Runtime::new().unwrap().block_on(future).unwrap();
}

/// Creates a table pinned to Parquet.
///
/// Parquet scan leaves are serialized by `datafusion-proto` itself, which is
/// what the general distributed tests below exercise; the vortex tests at the
/// end of this module cover the scan leaf that needs the registered vortex
/// codec instead.
async fn create_distributed_table(
    client: MetaDataClientRef,
    table_name: &str,
    config: LakeSoulIOConfig,
) -> Result<()> {
    create_table_with_file_format(client, table_name, config, PhysicalFormat::Parquet)
        .await
}

/// Spawns `workers` in-process LakeSoul workers on free ports and returns
/// their `http://127.0.0.1:<port>` URLs.
async fn spawn_workers(workers: usize) -> (Vec<String>, JoinSet<()>) {
    let mut urls = Vec::new();
    let mut join_set = JoinSet::new();
    for _ in 0..workers {
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
    distributed_factory_with(client, worker_urls, WORKER_COUNT, fallback_to_local)
}

/// Like [`distributed_factory`], with an explicit stage task count.
fn distributed_factory_with(
    client: MetaDataClientRef,
    worker_urls: Vec<String>,
    target_partitions: usize,
    fallback_to_local: bool,
) -> Result<LakeSoulSessionFactory> {
    Ok(
        LakeSoulSessionFactory::new(client, &CoreArgs::default())?.with_distributed(
            DistributedOptions {
                discovery: WorkerDiscovery::Static(worker_urls),
                fallback_to_local,
                target_partitions,
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

    create_distributed_table(
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

    create_distributed_table(
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

/// `id, v` primary-key schema for merge-on-read tests.
fn pk_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

/// Writes rows through the normal upsert path, producing one file per call.
async fn upsert_pk(table: &str, ids: Vec<i32>, values: Vec<i32>) -> Result<()> {
    let batch = RecordBatch::try_new(
        pk_schema(),
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(Int32Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap();
    crate::lakesoul_table::LakeSoulTable::for_name(table)
        .await?
        .execute_upsert(batch)
        .await
}

#[test]
fn test_merge_on_read_matches_single_node() {
    run_distributed_test(test_merge_on_read_matches_single_node_inner());
}

/// Creates the two merge-on-read test tables used by the distributed tests:
/// one single-work-unit table (`id` primary key, no range partitions) and one
/// three-work-unit table (one range partition per `range` value, bucketed by
/// the `hash` primary key; a range column is part of the merge key, not of the
/// bucket key, so it is not a primary key column). Each upsert produces one
/// file, so both tables have several versions of the same keys across files.
async fn seed_pk_tables(client: MetaDataClientRef) -> Result<(String, String)> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);

    // One work unit: `id` is the primary key and therefore the bucket key.
    let single_unit = format!("distributed_pk_one_{suffix}");
    create_distributed_table(
        client.clone(),
        &single_unit,
        LakeSoulIOConfigBuilder::new()
            .with_schema(pk_schema())
            .with_primary_keys(vec!["id".to_string()])
            .build(),
    )
    .await?;
    upsert_pk(&single_unit, vec![1, 2, 3], vec![10, 20, 30]).await?;
    upsert_pk(&single_unit, vec![2, 3, 4], vec![200, 300, 400]).await?;

    // Three work units: one range partition per `range` value, bucketed by the
    // `hash` primary key. A range column is part of the merge key, not of the
    // bucket key, so it is not a primary key column.
    let multi_unit = format!("distributed_pk_many_{suffix}");
    let schema = Arc::new(Schema::new(vec![
        Field::new("range", DataType::Int32, false),
        Field::new("hash", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]));
    create_distributed_table(
        client.clone(),
        &multi_unit,
        LakeSoulIOConfigBuilder::new()
            .with_schema(Arc::clone(&schema))
            .with_primary_keys(vec!["hash".to_string()])
            .with_range_partitions(vec!["range".to_string()])
            .build(),
    )
    .await?;
    let table = crate::lakesoul_table::LakeSoulTable::for_name(&multi_unit).await?;
    for (ranges, hashes, values) in [
        (
            vec![0, 0, 1, 1, 2],
            vec![0, 1, 0, 1, 0],
            vec![1, 2, 3, 4, 5],
        ),
        (vec![0, 1], vec![1, 0], vec![22, 33]),
    ] {
        table
            .execute_upsert(
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(ranges)) as ArrayRef,
                        Arc::new(Int32Array::from(hashes)) as ArrayRef,
                        Arc::new(Int32Array::from(values)) as ArrayRef,
                    ],
                )
                .unwrap(),
            )
            .await?;
    }
    Ok((single_unit, multi_unit))
}

async fn test_merge_on_read_matches_single_node_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let (single_unit, multi_unit) = seed_pk_tables(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let single_node = single_node_factory(client.clone())?;

    #[rustfmt::skip]
    let queries: Vec<(String, Vec<&str>)> = vec![
        // Single work unit: merging, not concatenating, is what makes these
        // numbers right.
        (
            format!("SELECT count(*) AS cnt FROM {single_unit}"),
            vec![
                "+-----+",
                "| cnt |",
                "+-----+",
                "| 4   |",
                "+-----+"
            ],
        ),
        (
            format!("SELECT sum(v) AS total FROM {single_unit}"),
            vec![
                "+-------+",
                "| total |",
                "+-------+",
                "| 910   |",
                "+-------+",
            ],
        ),
        (
            format!("SELECT id, v FROM {single_unit} ORDER BY id"),
            vec![
                "+----+-----+",
                "| id | v   |",
                "+----+-----+",
                "| 1  | 10  |",
                "| 2  | 200 |",
                "| 3  | 300 |",
                "| 4  | 400 |",
                "+----+-----+",
            ],
        ),
        // A hash `GROUP BY` forces a repartition after the scan: if a merge
        // work unit were executed once per consumer task, these counts would
        // multiply.
        (
            format!(
                "SELECT id, count(*) AS c FROM {single_unit} GROUP BY id ORDER BY id"
            ),
            vec![
                "+----+---+",
                "| id | c |",
                "+----+---+",
                "| 1  | 1 |",
                "| 2  | 1 |",
                "| 3  | 1 |",
                "| 4  | 1 |",
                "+----+---+",
            ],
        ),
        // Several work units behind a distributed union.
        (
            format!("SELECT count(*) AS cnt FROM {multi_unit}"),
            vec!["+-----+", "| cnt |", "+-----+", "| 5   |", "+-----+"],
        ),
        (
            format!("SELECT sum(v) AS total FROM {multi_unit}"),
            vec![
                "+-------+",
                "| total |",
                "+-------+",
                "| 65    |",
                "+-------+",
            ],
        ),
        (
            format!("SELECT range, hash, v FROM {multi_unit} ORDER BY range, hash"),
            vec![
                "+-------+------+----+",
                "| range | hash | v  |",
                "+-------+------+----+",
                "| 0     | 0    | 1  |",
                "| 0     | 1    | 22 |",
                "| 1     | 0    | 33 |",
                "| 1     | 1    | 4  |",
                "| 2     | 0    | 5  |",
                "+-------+------+----+",
            ],
        ),
        (
            format!(
                "SELECT hash, count(*) AS c FROM {multi_unit} GROUP BY hash ORDER BY hash"
            ),
            vec![
                "+------+---+",
                "| hash | c |",
                "+------+---+",
                "| 0    | 3 |",
                "| 1    | 2 |",
                "+------+---+",
            ],
        ),
    ];
    assert_matches_single_node(&distributed, &single_node, &queries).await?;

    // "Exactly once" is asserted through the results above: every query is
    // compared against the single-node session, so a work unit executed twice
    // would multiply the counts and sums, and one split across tasks would
    // corrupt the merge instead of returning the version that wins.
    workers.abort_all();
    Ok(())
}

/// Regression test for merge work-unit atomicity when workers outnumber the
/// merge work units.
///
/// With 5 workers for 3 range partitions, the distributed planner has more
/// stage tasks than merge work units. The children-isolator union then
/// allocates several tasks to one `MergeParquetExec` (it inherits
/// `Desired(N)` from its file scans) and scales the leaves up by slicing each
/// version file into byte ranges — so the same primary key's versions merge in
/// different tasks and the scan returns stale or duplicate rows. Every merge
/// work unit must stay pinned to one task instead, in both plan shapes: the
/// union case and the single-work-unit scan without a union.
#[test]
fn test_merge_work_units_pinned_to_one_task() {
    run_distributed_test(test_merge_work_units_pinned_to_one_task_inner());
}

async fn test_merge_work_units_pinned_to_one_task_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let (single_unit, multi_unit) = seed_pk_tables(client.clone()).await?;

    // 5 workers for 3 merge work units: more stage tasks than work units, so
    // the planner has surplus slots it must not spend on splitting a merge.
    let (worker_urls, mut workers) = spawn_workers(5).await;
    let distributed = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?
        .with_distributed(DistributedOptions {
            discovery: WorkerDiscovery::Static(worker_urls),
            fallback_to_local: false,
            target_partitions: 5,
            bytes_per_partition: Some(1),
        });
    let single_node = single_node_factory(client.clone())?;

    // The same result-equality assertions as `test_merge_on_read_matches_single_node`:
    // distributed results must equal the single-node session for both plan
    // shapes. `assert_matches_single_node` only compares results; the pinning
    // of merge work units to one task is checked by the explicit `EXPLAIN
    // ANALYZE` loop below.
    let queries: Vec<(String, Vec<&str>)> = vec![
        // Single work unit without a scan union.
        (
            format!("SELECT count(*) AS cnt FROM {single_unit}"),
            vec!["+-----+", "| cnt |", "+-----+", "| 4   |", "+-----+"],
        ),
        (
            format!(
                "SELECT id, count(*) AS c FROM {single_unit} GROUP BY id ORDER BY id"
            ),
            vec![
                "+----+---+",
                "| id | c |",
                "+----+---+",
                "| 1  | 1 |",
                "| 2  | 1 |",
                "| 3  | 1 |",
                "| 4  | 1 |",
                "+----+---+",
            ],
        ),
        // Several work units behind a distributed union.
        (
            format!("SELECT count(*) AS cnt FROM {multi_unit}"),
            vec!["+-----+", "| cnt |", "+-----+", "| 5   |", "+-----+"],
        ),
        (
            format!("SELECT sum(v) AS total FROM {multi_unit}"),
            vec![
                "+-------+",
                "| total |",
                "+-------+",
                "| 65    |",
                "+-------+",
            ],
        ),
        (
            format!(
                "SELECT hash, count(*) AS c FROM {multi_unit} GROUP BY hash ORDER BY hash"
            ),
            vec![
                "+------+---+",
                "| hash | c |",
                "+------+---+",
                "| 0    | 3 |",
                "| 1    | 2 |",
                "+------+---+",
            ],
        ),
    ];
    assert_matches_single_node(&distributed, &single_node, &queries).await?;

    // And the plan must pin every merge work unit to one task in both shapes.
    let dist_ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    for (sql, _) in &queries {
        let explain_sql = format!("EXPLAIN ANALYZE {sql}");
        let plan = dist_ctx.sql(&explain_sql).await?.collect().await?;
        let explain = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        assert_merge_work_units_pinned(&explain, sql);
    }

    workers.abort_all();
    Ok(())
}

/// CDC tombstones and pushdown over a merge-on-read table: a delete of a key
/// written in an earlier file, an update, and a key only the latest file has,
/// read through filters, projections and a limited ordering.
#[test]
fn test_cdc_deletes_and_pushdown_match_single_node() {
    run_distributed_test(test_cdc_deletes_and_pushdown_match_single_node_inner());
}

async fn test_cdc_deletes_and_pushdown_match_single_node_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let table = format!("distributed_cdc_{suffix}");
    create_cdc_table(client.clone(), &table, PhysicalFormat::Parquet).await?;
    let lakehouse = crate::lakesoul_table::LakeSoulTable::for_name(&table).await?;
    for batch in [
        cdc_batch(&[1, 2, 3], &[10, 20, 30], &["insert", "insert", "insert"]),
        cdc_batch(&[2], &[200], &["insert"]),
        cdc_batch(&[1], &[10], &["delete"]),
        cdc_batch(&[4], &[40], &["insert"]),
    ] {
        lakehouse.execute_upsert(batch).await?;
    }

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let single_node = single_node_factory(client.clone())?;

    let queries: Vec<(String, Vec<&str>)> = vec![
        (
            format!("SELECT count(*) AS cnt FROM {table}"),
            vec!["+-----+", "| cnt |", "+-----+", "| 3   |", "+-----+"],
        ),
        (
            format!("SELECT id, score FROM {table} ORDER BY id"),
            vec![
                "+----+-------+",
                "| id | score |",
                "+----+-------+",
                "| 2  | 200   |",
                "| 3  | 30    |",
                "| 4  | 40    |",
                "+----+-------+",
            ],
        ),
        (
            format!("SELECT sum(score) AS total FROM {table}"),
            vec![
                "+-------+",
                "| total |",
                "+-------+",
                "| 270   |",
                "+-------+",
            ],
        ),
        (
            format!("SELECT id FROM {table} WHERE id > 2 ORDER BY id"),
            vec!["+----+", "| id |", "+----+", "| 3  |", "| 4  |", "+----+"],
        ),
        (
            format!("SELECT score FROM {table} ORDER BY score DESC LIMIT 2"),
            vec![
                "+-------+",
                "| score |",
                "+-------+",
                "| 200   |",
                "| 40    |",
                "+-------+",
            ],
        ),
    ];
    assert_matches_single_node(&distributed, &single_node, &queries).await?;
    workers.abort_all();
    Ok(())
}

/// Queries running while writes commit must always see a consistent snapshot:
/// because every merge work unit runs exactly once and whole, a primary key
/// can never appear twice and no version can be lost. Keys are written as
/// `id -> 10 * id`, so a consistent snapshot is fully determined by its row
/// count and sum.
#[test]
fn test_queries_during_concurrent_upserts_stay_consistent() {
    run_distributed_test(test_queries_during_concurrent_upserts_stay_consistent_inner());
}

async fn test_queries_during_concurrent_upserts_stay_consistent_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let table = format!("distributed_concurrent_{suffix}");
    create_distributed_table(
        client.clone(),
        &table,
        LakeSoulIOConfigBuilder::new()
            .with_schema(pk_schema())
            .with_primary_keys(vec!["id".to_string()])
            .build(),
    )
    .await?;
    upsert_pk(&table, vec![1, 2, 3], vec![10, 20, 30]).await?;

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    let sql = format!(
        "SELECT count(*) AS c, count(DISTINCT id) AS d, sum(v) AS s FROM {table}"
    );

    let mut samples = 0usize;
    let mut last_count: Option<i64> = None;
    // Returns the sampled (count, distinct, sum) triple.
    let check = |batches: &[RecordBatch],
                 samples: &mut usize,
                 last_count: &mut Option<i64>|
     -> (i64, i64, i64) {
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) is Int64")
            .value(0);
        let distinct = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(DISTINCT) is Int64")
            .value(0);
        let sum = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("sum is Int64")
            .value(0);
        assert_eq!(count, distinct, "a primary key appeared twice");
        // The seeded keys are 1..=3 (v = 10 * id) and the writer commits
        // 10, 11, ... one at a time, so a consistent snapshot is the seed plus
        // a prefix of the write sequence: the sum pins down exactly which
        // prefix was read, catching both lost and stale versions.
        let written = count - 3;
        let expected: i64 = 60 + (10..10 + written).map(|id| id * 10).sum::<i64>();
        assert_eq!(
            sum, expected,
            "snapshot is not a consistent prefix (count={count})"
        );
        // Visibility must never move backwards while writes commit.
        if let Some(previous) = *last_count {
            assert!(
                count >= previous,
                "sampled count moved backwards: {previous} -> {count}"
            );
        }
        *last_count = Some(count);
        *samples += 1;
        (count, distinct, sum)
    };

    // One sample before the writer starts, so a fast writer cannot make the
    // loop body vacuous.
    check(
        &ctx.sql(&sql).await?.collect().await?,
        &mut samples,
        &mut last_count,
    );

    let writer = tokio::spawn(async move {
        for key in 10..40 {
            upsert_pk(&table, vec![key], vec![key * 10]).await.unwrap();
        }
    });
    while !writer.is_finished() {
        check(
            &ctx.sql(&sql).await?.collect().await?,
            &mut samples,
            &mut last_count,
        );
        tokio::task::yield_now().await;
    }
    writer.await.expect("writer task");

    // The final state must observe every committed write, not a stale
    // snapshot of the initial one: the 3 seed rows plus all 30 written keys.
    let (count, distinct, sum) = check(
        &ctx.sql(&sql).await?.collect().await?,
        &mut samples,
        &mut last_count,
    );
    assert_eq!(count, 33, "final query must see every committed write");
    assert_eq!(distinct, 33);
    let expected: i64 = 60 + (10..40).map(|id| i64::from(id * 10)).sum::<i64>();
    assert_eq!(sum, expected, "final query must see every committed write");
    workers.abort_all();
    Ok(())
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

/// Like [`assert_matches_single_node`], but compares the results in their
/// original output order instead of sorting the rows first: an `ORDER BY`
/// that is a total order must produce exactly the expected sequence, which a
/// sorted comparison cannot observe.
async fn assert_matches_single_node_ordered(
    distributed: &LakeSoulSessionFactory,
    single_node: &LakeSoulSessionFactory,
    queries: &[(String, Vec<&str>)],
) -> Result<()> {
    let dist_ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    let local_ctx = single_node.create_session(&LakeSoulSessionOptions::default())?;
    for (sql, expected) in queries {
        let dist_result = dist_ctx.sql(sql).await?.collect().await?;
        let local_result = local_ctx.sql(sql).await?.collect().await?;
        assert_batches_eq_ordered(
            &format!("{sql} (distributed)"),
            expected,
            &dist_result,
        );
        assert_batches_eq_ordered(
            &format!("{sql} (single-node)"),
            expected,
            &local_result,
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Distributed-execution proof helpers
// ---------------------------------------------------------------------------

/// How long a query may take from `sql()` through planning to the end of
/// execution before a fault or cancellation test gives up.
const EXECUTION_TIMEOUT: Duration = Duration::from_secs(60);

/// Runs `sql` end to end — `ctx.sql` (planning), scheduling and `collect`
/// (execution) — under one timeout, so a query that hangs anywhere surfaces
/// as the timeout instead of the test hanging because only `collect` was
/// wrapped.
async fn collect_within_timeout(
    ctx: &SessionContext,
    sql: &str,
    limit: Duration,
) -> Result<Vec<RecordBatch>> {
    let ctx = ctx.clone();
    let sql = sql.to_string();
    let fut = async move { ctx.sql(&sql).await?.collect().await };
    let batches = tokio::time::timeout(limit, fut).await.map_err(|_| {
        DataFusionError::Execution(format!("query did not finish within {limit:?}"))
    })??;
    Ok(batches)
}

/// Runs `sql` end to end under [`EXECUTION_TIMEOUT`] and asserts that it
/// fails: the timeout itself is a hang and panics (a failure test must pin
/// that the query *fails* in bounded time, not merely that something
/// eventually errored), and a success means the injected failure was masked.
///
/// Whether the error is the injected one is proven by the caller's counters
/// (the worker recorded being asked), not by error text.
async fn expect_query_failure_within(ctx: &SessionContext, sql: &str) -> Result<()> {
    let ctx = ctx.clone();
    let fut = {
        let sql = sql.to_string();
        async move { ctx.sql(&sql).await?.collect().await }
    };
    match tokio::time::timeout(EXECUTION_TIMEOUT, fut).await {
        Err(_) => {
            panic!("query hung instead of failing within {EXECUTION_TIMEOUT:?}: {sql}")
        }
        Ok(Ok(batches)) => {
            panic!("the query must fail, not succeed with: {batches:?}")
        }
        Ok(Err(_)) => Ok(()),
    }
}

/// A worker session builder that counts every task handed to its worker and
/// then builds a normal one, so a test can prove a query really reached a
/// worker instead of running on the coordinator.
///
/// Reduced parity, deliberate: it registers only the LakeSoul distributed
/// codec, not the production worker's full setup (every user codec, the
/// vector-search UDF, identity headers). That is exactly what the
/// parquet-only tests using it need; a test that needs production parity
/// builds its worker through `crate::distributed::lakesoul_worker` instead.
struct CountingSessionBuilder {
    received: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl datafusion_distributed::WorkerSessionBuilder for CountingSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: datafusion_distributed::WorkerQueryContext,
    ) -> datafusion::common::Result<datafusion::execution::SessionState> {
        self.received.fetch_add(1, Ordering::SeqCst);
        let mut builder = ctx.builder;
        builder = builder.with_distributed_user_codec(crate::distributed::LakeSoulCodec);
        Ok(builder.build())
    }
}

/// Like `spawn_workers`, but every worker counts the tasks it is handed, so a
/// test can prove the workers actually received them.
async fn spawn_workers_with_task_recording(
    workers: usize,
) -> (Vec<String>, Vec<Arc<AtomicUsize>>, JoinSet<()>) {
    let mut urls = Vec::new();
    let mut receipts = Vec::new();
    let mut join_set = JoinSet::new();
    for _ in 0..workers {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        urls.push(format!(
            "http://127.0.0.1:{}",
            listener.local_addr().unwrap().port()
        ));
        let received = Arc::new(AtomicUsize::new(0));
        let worker = Worker::from_session_builder(CountingSessionBuilder {
            received: Arc::clone(&received),
        })
        .with_runtime_env(
            crate::session::build_worker_runtime_env(&CoreArgs::default()).unwrap(),
        )
        .with_version(crate::distributed::DISTRIBUTED_PROTOCOL_VERSION);
        join_set.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(worker.into_worker_server())
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        receipts.push(received);
    }
    (urls, receipts, join_set)
}

/// Total tasks the recording workers have been handed so far.
fn recorded_tasks(receipts: &[Arc<AtomicUsize>]) -> usize {
    receipts.iter().map(|c| c.load(Ordering::SeqCst)).sum()
}

/// A store that pauses each worker's *first* file read at a gate: the read
/// resumes when the watched channel turns `true`, so a test can commit a new
/// version while the workers hold dispatched, blocked tasks.
#[derive(Debug)]
struct GatedStore {
    inner: Arc<dyn object_store::ObjectStore>,
    gate: tokio::sync::watch::Receiver<bool>,
    /// Whether this worker instance already reached the gate.
    reached: std::sync::atomic::AtomicBool,
    /// Reads that have parked at the gate, incremented when a first read
    /// arrives (the waiting happens after this counter is bumped, so a test
    /// that waits for `arrived > 0` knows the read is blocked mid-flight,
    /// not merely dispatched).
    arrived: Arc<AtomicUsize>,
}

impl std::fmt::Display for GatedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gated-store")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for GatedStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        // Only the first read per worker instance waits at the gate. The
        // receiver is cloned because `borrow_and_update`/`changed` need
        // `&mut`; a fresh clone returns the current value from
        // `borrow_and_update`, so an already-open gate never blocks.
        if !self.reached.swap(true, Ordering::SeqCst) {
            self.arrived.fetch_add(1, Ordering::SeqCst);
            let mut gate = self.gate.clone();
            while !*gate.borrow_and_update() {
                gate.changed()
                    .await
                    .map_err(|_| object_store::Error::Generic {
                        store: "gated-store",
                        source: "the gate was dropped before it opened".into(),
                    })?;
            }
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Like `spawn_workers_with_task_recording`, but each worker's object store
/// pauses its first file read at `gate` (opened when the channel sends
/// `true`), so a test can commit a version while the workers hold dispatched,
/// blocked tasks.
/// Like [`spawn_workers_with_handles`], but each worker's object store is a
/// [`GatedStore`] sharing `gate`, and instead of session-builder receipts the
/// function returns each worker's gate-arrival counter: a test can wait until
/// every worker's first read is parked at the (still closed) gate and know
/// the workers are blocked mid-read, not merely handed a task.
async fn spawn_gated_workers(
    workers: usize,
    gate: tokio::sync::watch::Receiver<bool>,
) -> (Vec<String>, Vec<Arc<AtomicUsize>>, Vec<Worker>, JoinSet<()>) {
    let mut urls = Vec::new();
    let mut arrived = Vec::new();
    let mut handles = Vec::new();
    let mut join_set = JoinSet::new();
    for _ in 0..workers {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        urls.push(format!(
            "http://127.0.0.1:{}",
            listener.local_addr().unwrap().port()
        ));
        let arrived_at_gate = Arc::new(AtomicUsize::new(0));
        let runtime =
            crate::session::build_worker_runtime_env(&CoreArgs::default()).unwrap();
        runtime.register_object_store(
            &url::Url::parse("file://").unwrap(),
            Arc::new(GatedStore {
                inner: Arc::new(object_store::local::LocalFileSystem::new()),
                gate: gate.clone(),
                reached: std::sync::atomic::AtomicBool::new(false),
                arrived: Arc::clone(&arrived_at_gate),
            }),
        );
        let options = LakeSoulWorkerOptions {
            core_args: CoreArgs::default(),
        };
        let worker = crate::distributed::lakesoul_worker(&options)
            .unwrap()
            .with_runtime_env(runtime)
            .with_version(crate::distributed::DISTRIBUTED_PROTOCOL_VERSION);
        handles.push(worker.clone());
        let observability = worker.with_observability_service(Arc::new(
            StaticWorkerResolver::new(Vec::new()).unwrap(),
        ));
        join_set.spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(worker.into_worker_server())
                .add_service(observability)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await;
        });
        arrived.push(arrived_at_gate);
    }
    (urls, arrived, handles, join_set)
}

/// Runs one SQL query on the distributed session and asserts in one place
/// that it really executed on the workers:
///
/// 1. the physical plan has distributed stages (`DistributedExec`), not the
///    coordinator-only plan a distributed session produces for shapes it
///    does not distribute;
/// 2. while the query ran, at least one recording worker was handed a task;
/// 3. the result equals `expected` — in output order when `ordered`, as a
///    row multiset otherwise (the shared `assert_batches_eq` sorts rows
///    first, which cannot catch an `ORDER BY` error).
async fn assert_query_runs_on_workers(
    distributed: &LakeSoulSessionFactory,
    receipts: &[Arc<AtomicUsize>],
    sql: &str,
    expected: &[&str],
    ordered: bool,
) -> Result<()> {
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let explain = display_plan_ascii(plan.as_ref(), false);
    assert!(
        plan.is::<DistributedExec>(),
        "the plan must have distributed stages:\n{explain}"
    );
    let before = recorded_tasks(receipts);
    let batches = collect_within_timeout(&ctx, sql, EXECUTION_TIMEOUT).await?;
    assert!(
        recorded_tasks(receipts) > before,
        "no recording worker was handed a task while the query ran; \
         it executed on the coordinator:\n{explain}"
    );
    if ordered {
        assert_batches_eq_ordered(sql, expected, &batches);
    } else {
        assert_batches_eq(sql, expected, &batches);
    }
    Ok(())
}

/// Polls until every counter is at least one — every worker's first read is
/// parked at its gate — or panics at the deadline.
///
/// Waiting on the gate's own arrival counter (not a session-builder or
/// dispatch count) is what makes a test's "the workers hold blocked tasks"
/// claim real: a dispatched task that never started reading would not block.
async fn wait_until_all_reached(counters: &[Arc<AtomicUsize>], label: &str) {
    for _ in 0..600 {
        if counters.iter().all(|c| c.load(Ordering::SeqCst) > 0) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let reached: Vec<usize> = counters.iter().map(|c| c.load(Ordering::SeqCst)).collect();
    panic!("not every {label} parked a read at the gate within 30 s: {reached:?}");
}

/// Polls until the worker runs no task, within 5 s: a query's worker tasks
/// must be stopped promptly after it ends or fails, not left for their TTL.
async fn wait_until_no_tasks(handle: &Worker, label: &str) {
    for _ in 0..100 {
        if handle.tasks_running().await == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "the tasks of {label} must be stopped promptly, {} still running",
        handle.tasks_running().await
    );
}

async fn initialized_tasks(
    observer: &mut ObservabilityServiceClient<tonic::transport::Channel>,
) -> Result<usize> {
    Ok(observer
        .get_task_progress(GetTaskProgressRequest {})
        .await?
        .into_inner()
        .tasks
        .len())
}

/// Waits for initialized plans, not empty cache entries, to drain. The
/// deadline includes the observation RPCs, and RPC errors fail the test.
async fn wait_until_no_initialized_tasks(
    observer: &mut ObservabilityServiceClient<tonic::transport::Channel>,
    label: &str,
) -> Result<()> {
    let mut remaining = 0;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            remaining = initialized_tasks(observer).await?;
            if remaining == 0 {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| {
        DataFusionError::Execution(format!(
            "{label} must release its initialized plans within 5 s; \
             {remaining} task(s) still hold a plan"
        ))
    })?
}

/// Runs a `count(*)` on `ctx` under the execution timeout and asserts it
/// returns `expected`: workers stay usable after a query ends, is cancelled
/// or fails.
async fn assert_count(
    ctx: &SessionContext,
    table: &str,
    expected: i64,
    why: &str,
) -> Result<()> {
    let batches = collect_within_timeout(
        ctx,
        &format!("SELECT count(*) AS c FROM {table}"),
        EXECUTION_TIMEOUT,
    )
    .await?;
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) yields int64")
            .value(0),
        expected,
        "{why}"
    );
    Ok(())
}

/// Asserts the distributed plan pins every merge work unit to a single task.
///
/// A merge split across `tasks=N` sees only a byte slice of every version
/// file, so the same primary key's versions land in different tasks and the
/// merge emits stale or duplicate rows. The task map of the distributed plan
/// (`EXPLAIN ANALYZE`) is the observable surface of that contract:
///
/// - With a `DistributedUnionExec` (several merge work units), every union
///   task slot must reference a merge child with an inner task count of 1 —
///   the planner displays multi-task allocations as `c0(0/2)` markers.
/// - Without a union (single work unit), the stage containing the
///   `MergeParquetExec` must run in one task, or the plan must execute
///   entirely in the coordinator's head stage.
///
/// Only merge-on-read (primary-key) tables are covered: append-only tables
/// distribute their scan legitimately.
fn assert_merge_work_units_pinned(explain: &str, sql: &str) {
    if explain
        .lines()
        .any(|line| line.contains("DistributedUnionExec"))
    {
        for line in explain
            .lines()
            .filter(|line| line.contains("DistributedUnionExec"))
        {
            // Only the task map precedes `metrics=[...]`; multi-task
            // allocations are the sole source of parentheses there.
            let task_map = line.split("metrics=").next().unwrap_or(line);
            assert!(
                !task_map.contains('('),
                "merge work unit split across union task slots for {sql}: {task_map}"
            );
        }
    } else {
        let mut current_tasks = None;
        for line in explain.lines() {
            if let Some(rest) = line.split("tasks=").nth(1)
                && let Some(tasks) = rest
                    .split(',')
                    .next()
                    .and_then(|value| value.trim().parse::<usize>().ok())
            {
                current_tasks = Some(tasks);
            }
            if line.contains("MergeParquetExec") {
                // `None` means the plan runs entirely in the head stage on
                // the coordinator (one task), which satisfies the contract.
                assert!(
                    current_tasks.is_none_or(|tasks| tasks == 1),
                    "merge work unit split across distributed tasks for {sql}; \
                     every stage containing MergeParquetExec must run in one task. \
                     EXPLAIN:\n{explain}"
                );
            }
        }
    }
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
    run_distributed_test(test_distributed_scan_filter_aggregate_join_inner());
}

async fn test_distributed_scan_filter_aggregate_join_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let (t1, t2) = seed_tables(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
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
    run_distributed_test(test_explain_analyze_shows_distributed_stages_inner());
}

async fn test_explain_analyze_shows_distributed_stages_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let (t1, _t2) = seed_tables(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
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

/// A query is refused without a ready worker only when the distributed
/// planner cannot plan it at all.
///
/// While no worker is ready the distributed planner plans the query
/// single-node by itself — it caps every stage at one task, so no network
/// boundary survives — and that plan is not a failure: it runs on the
/// coordinator in both modes. `fallback_to_local` only decides what happens
/// when the distributed planner fails, which is pinned by the gate's unit
/// tests.
#[test]
fn test_no_ready_workers_runs_on_the_coordinator() {
    run_distributed_test(test_no_ready_workers_runs_on_the_coordinator_inner());
}

async fn test_no_ready_workers_runs_on_the_coordinator_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let (t1, _t2) = seed_tables(client.clone()).await?;

    let sql = format!("SELECT part, b FROM {t1} WHERE b > 5 ORDER BY part, b");
    let queries = vec![(sql.clone(), FILTER_RESULT.to_vec())];
    let single_node = single_node_factory(client.clone())?;

    for fallback_to_local in [false, true] {
        let distributed =
            distributed_factory(client.clone(), Vec::new(), fallback_to_local)?;
        assert_matches_single_node(&distributed, &single_node, &queries).await?;

        // The plan must not pretend to have run on a worker: with no ready
        // worker the query is a single-node plan, not a distributed stage.
        let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
        let plan = ctx
            .sql(&format!("EXPLAIN ANALYZE {sql}"))
            .await?
            .collect()
            .await?;
        let explain = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
            .unwrap()
            .to_string();
        assert!(
            !explain.contains("DistributedExec"),
            "a plan with no ready worker must not claim a distributed stage \
             (fallback_to_local={fallback_to_local}):\n{explain}"
        );
    }
    Ok(())
}

#[test]
fn worker_urls_reject_malformed_entries() {
    use crate::distributed::StaticWorkerResolver;

    let resolver = StaticWorkerResolver::new(vec!["not a url".to_string()]);
    assert!(resolver.is_err());
}

/// An append-only work unit is a plain file scan, so its files fan out over
/// the stage's tasks.
///
/// The table below has no range partition: all nine files of the table form a
/// single work unit. Reading it must still use the workers — the scan leaf is
/// the `DataSourceExec` the merge operator would have wrapped, and the
/// distributed planner splits its one file group by file and byte range —
/// whereas a work unit pinned to a merge operator runs entirely on the
/// coordinator (or in one worker task) however many files it holds.
#[test]
fn test_append_only_scan_fans_out_over_tasks() {
    run_distributed_test(test_append_only_scan_fans_out_over_tasks_inner());
}

async fn test_append_only_scan_fans_out_over_tasks_inner() -> Result<()> {
    const FILES: usize = 9;
    let client = Arc::new(MetaDataClient::from_env().await?);
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_flat_{suffix}");

    create_distributed_table(
        client.clone(),
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(t1_schema())
            .build(),
    )
    .await?;
    let table = crate::lakesoul_table::LakeSoulTable::for_name(&name).await?;
    for round in 0..FILES as i32 {
        table
            .execute_upsert(batch(
                t1_schema(),
                vec![0, 0, 0, 1, 1, 1, 2, 2, 2],
                vec![
                    round * 100 + 1,
                    round * 100 + 2,
                    round * 100 + 3,
                    round * 100 + 11,
                    round * 100 + 12,
                    round * 100 + 13,
                    round * 100 + 21,
                    round * 100 + 22,
                    round * 100 + 23,
                ],
            ))
            .await?;
    }

    // One task per file is reachable: nine workers, and one byte per requested
    // partition, so the desired task count exceeds the number of files.
    let (worker_urls, mut workers) = spawn_workers(FILES).await;
    let distributed = LakeSoulSessionFactory::new(client.clone(), &CoreArgs::default())?
        .with_distributed(DistributedOptions {
            discovery: WorkerDiscovery::Static(worker_urls),
            fallback_to_local: false,
            target_partitions: FILES,
            bytes_per_partition: Some(1),
        });
    let single_node = single_node_factory(client.clone())?;

    // Projections of a partition column only, of a file column, and of both:
    // the scan leaf reads the file columns and takes the partition columns as
    // constants of the work unit.
    let queries: Vec<(String, Vec<&str>)> = vec![
        (
            format!("SELECT count(*) AS cnt FROM {name}"),
            vec!["+-----+", "| cnt |", "+-----+", "| 81  |", "+-----+"],
        ),
        (
            format!(
                "SELECT part, count(*) AS cnt, sum(b) AS total \
                 FROM {name} GROUP BY part ORDER BY part"
            ),
            vec![
                "+------+-----+-------+",
                "| part | cnt | total |",
                "+------+-----+-------+",
                "| 0    | 27  | 10854 |",
                "| 1    | 27  | 11124 |",
                "| 2    | 27  | 11394 |",
                "+------+-----+-------+",
            ],
        ),
        (
            format!("SELECT b FROM {name} WHERE part = 2 AND b < 30 ORDER BY b"),
            vec![
                "+----+", "| b  |", "+----+", "| 21 |", "| 22 |", "| 23 |", "+----+",
            ],
        ),
    ];
    assert_matches_single_node(&distributed, &single_node, &queries).await?;

    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    let sql = format!("EXPLAIN ANALYZE SELECT part, count(*) FROM {name} GROUP BY part");
    let plan = ctx.sql(&sql).await?.collect().await?;
    let explain = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
        .unwrap()
        .to_string();

    assert!(
        !explain.contains("MergeParquetExec"),
        "append-only scan must not need a merge operator:\n{explain}"
    );
    let stage_tasks = explain
        .lines()
        .find(|line| line.contains("Stage 1 ──"))
        .and_then(|line| line.split("tasks=").nth(1))
        .and_then(|rest| rest.split(',').next())
        .and_then(|tasks| tasks.trim().parse::<usize>().ok())
        .unwrap_or_else(|| panic!("no scan stage in plan:\n{explain}"));
    assert!(
        stage_tasks > 1,
        "single-work-unit append-only scan ran in {stage_tasks} task(s); \
         its {FILES} files must fan out over the stage's tasks:\n{explain}"
    );

    workers.abort_all();
    Ok(())
}

/// One row of the distribution matrix: a query over one LakeSoul table shape
/// and the physical plan it must produce.
struct ShapeCase {
    /// Table shape under test, used in assertion messages.
    shape: &'static str,
    sql: String,
    /// Whether the distributed planner must wrap the plan in `DistributedExec`.
    distributed: bool,
    /// How many merge-on-read work units (`MergeParquetExec` nodes) the plan
    /// must contain.
    merge_units: usize,
}

/// One fresh table per shape of the distribution matrix.
struct ShapeTables {
    flat_append: String,
    flat_pk: String,
    partitioned_append: String,
    partitioned_pk: String,
    single_partition_pk: String,
    empty_pk: String,
}

/// `part, id, v` — `part` is the range partition of the partitioned shapes,
/// `id` the primary key of the primary-key shapes, `v` a value column.
fn shape_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

/// Three rows per range-partition value; `round` makes every key appear in a
/// second file so primary-key tables need a real merge-on-read.
fn shape_batch(round: i32, parts: &[i32]) -> RecordBatch {
    let mut batch_parts = vec![];
    let mut ids = vec![];
    let mut values = vec![];
    for &part in parts {
        for row in 0..3 {
            batch_parts.push(part);
            ids.push(part * 10 + row);
            values.push(round * 100 + part * 10 + row);
        }
    }
    RecordBatch::try_new(
        shape_schema(),
        vec![
            Arc::new(Int32Array::from(batch_parts)) as ArrayRef,
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(Int32Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Creates the six matrix tables; all but `empty_pk` get two versions of the
/// same keys written through the normal upsert path.
///
/// For the partitioned shapes a single upsert covers all three range
/// partitions, so each of them holds one work unit, i.e. one `MergeParquetExec`
/// or one scan leaf per partition value.
async fn seed_shape_matrix(client: MetaDataClientRef) -> Result<ShapeTables> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let tables = ShapeTables {
        flat_append: format!("distributed_shape_flat_append_{suffix}"),
        flat_pk: format!("distributed_shape_flat_pk_{suffix}"),
        partitioned_append: format!("distributed_shape_part_append_{suffix}"),
        partitioned_pk: format!("distributed_shape_part_pk_{suffix}"),
        single_partition_pk: format!("distributed_shape_single_part_pk_{suffix}"),
        empty_pk: format!("distributed_shape_empty_pk_{suffix}"),
    };

    let shapes: [(&str, &[&str], &[&str]); 6] = [
        (&tables.flat_append, &[], &[]),
        (&tables.flat_pk, &["id"], &[]),
        (&tables.partitioned_append, &[], &["part"]),
        (&tables.partitioned_pk, &["id"], &["part"]),
        (&tables.single_partition_pk, &["id"], &["part"]),
        (&tables.empty_pk, &["id"], &[]),
    ];
    for (name, primary_keys, range_partitions) in shapes {
        create_distributed_table(
            client.clone(),
            name,
            LakeSoulIOConfigBuilder::new()
                .with_schema(shape_schema())
                .with_primary_keys(primary_keys.iter().map(|k| k.to_string()).collect())
                .with_range_partitions(
                    range_partitions.iter().map(|p| p.to_string()).collect(),
                )
                .build(),
        )
        .await?;
    }

    for name in [
        &tables.flat_append,
        &tables.flat_pk,
        &tables.partitioned_append,
        &tables.partitioned_pk,
    ] {
        let table = crate::lakesoul_table::LakeSoulTable::for_name(name).await?;
        table.execute_upsert(shape_batch(0, &[0, 1, 2])).await?;
        table.execute_upsert(shape_batch(1, &[0, 1, 2])).await?;
    }

    // A range-partitioned primary-key table with a single populated range
    // partition: one merge work unit, which must stay on the coordinator.
    let table =
        crate::lakesoul_table::LakeSoulTable::for_name(&tables.single_partition_pk)
            .await?;
    table.execute_upsert(shape_batch(0, &[0])).await?;
    table.execute_upsert(shape_batch(1, &[0])).await?;

    Ok(tables)
}

/// One identical `GROUP BY` per shape: the only variable is the scan leaf the
/// table shape produces.
fn shape_matrix_cases(tables: &ShapeTables) -> Vec<ShapeCase> {
    let group_by =
        |table: &str| format!("SELECT id, count(*) AS c FROM {table} GROUP BY id");
    vec![
        ShapeCase {
            shape: "append-only, no range partition",
            sql: group_by(&tables.flat_append),
            distributed: true,
            merge_units: 0,
        },
        ShapeCase {
            shape: "primary key, no range partition",
            sql: group_by(&tables.flat_pk),
            distributed: false,
            merge_units: 1,
        },
        ShapeCase {
            shape: "append-only, range partitioned",
            sql: group_by(&tables.partitioned_append),
            distributed: true,
            merge_units: 0,
        },
        ShapeCase {
            shape: "primary key, range partitioned",
            sql: group_by(&tables.partitioned_pk),
            distributed: true,
            merge_units: 3,
        },
        // A range predicate reaches `scan` for metadata pruning, so the
        // three work units collapse to the one with `part = 0` and the plan
        // stays on the coordinator. The unsafe value predicate must not
        // prevent that pruning (conjunctions are classified per conjunct).
        ShapeCase {
            shape: "primary key, range partitioned, range filter",
            sql: format!(
                "SELECT id, count(*) AS c FROM {} WHERE part = 0 GROUP BY id",
                tables.partitioned_pk
            ),
            distributed: false,
            merge_units: 1,
        },
        ShapeCase {
            shape: "primary key, range partitioned, range filter and value filter",
            sql: format!(
                "SELECT id, count(*) AS c FROM {} WHERE part = 0 AND v > 0 GROUP BY id",
                tables.partitioned_pk
            ),
            distributed: false,
            merge_units: 1,
        },
        ShapeCase {
            shape: "primary key, range partitioned, single populated partition",
            sql: group_by(&tables.single_partition_pk),
            distributed: false,
            merge_units: 1,
        },
        ShapeCase {
            shape: "primary key, empty table",
            sql: group_by(&tables.empty_pk),
            distributed: false,
            merge_units: 0,
        },
    ]
}

/// Counts merge-on-read work units in a physical plan.
///
/// Network boundaries expose the plan of their local stage as a child, so a
/// single walk reaches the merge operators of distributed stages as well.
fn count_merge_work_units(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut count = 0;
    plan.apply(|node| {
        if node.is::<MergeParquetExec>() {
            count += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking a physical plan cannot fail");
    count
}

/// Plans `case.sql` without executing it and asserts the distribution decision
/// and the scan leaf it was made on.
async fn assert_plan_shape(ctx: &SessionContext, case: &ShapeCase) -> Result<()> {
    let plan = ctx.sql(&case.sql).await?.create_physical_plan().await?;
    let distributed = plan.is::<DistributedExec>();
    let explain = display_plan_ascii(plan.as_ref(), false);
    assert_eq!(
        distributed, case.distributed,
        "{}: distributed={distributed}, expected {}\n{explain}",
        case.shape, case.distributed,
    );
    assert_eq!(
        count_merge_work_units(&plan),
        case.merge_units,
        "{}: unexpected merge work units\n{explain}",
        case.shape,
    );
    Ok(())
}

/// Plans one query per LakeSoul table shape and pins whether the distributed
/// planner distributes it, without executing anything.
///
/// The shapes differ in the scan leaf they produce: an append-only work unit is
/// a plain file scan the distributed planner splits over tasks, a primary-key
/// work unit is a merge operator pinned to one task, and range partitions add
/// a union of work units per partition value. The assertions are structural —
/// `DistributedExec` or not, and how many merge work units the plan holds — so
/// they do not depend on file names, stage ids or plan formatting.
#[test]
fn test_distribution_matrix_by_table_shape() {
    run_distributed_test(test_distribution_matrix_by_table_shape_inner());
}

async fn test_distribution_matrix_by_table_shape_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let tables = seed_shape_matrix(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    for case in shape_matrix_cases(&tables) {
        assert_plan_shape(&ctx, &case).await?;
    }

    workers.abort_all();
    Ok(())
}

/// A single worker cannot host a second task: every stage is capped at one
/// task, all network boundaries are elided, and even the shapes that distribute
/// over three workers are planned for the coordinator.
///
/// This is the planner's own decision, not a fallback for a planning failure:
/// the availability policy gate is only consulted when the distributed planner
/// errors.
#[test]
fn test_single_worker_plans_every_shape_on_the_coordinator() {
    run_distributed_test(test_single_worker_plans_every_shape_on_the_coordinator_inner());
}

async fn test_single_worker_plans_every_shape_on_the_coordinator_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let tables = seed_shape_matrix(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers(1).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    for case in shape_matrix_cases(&tables) {
        let case = ShapeCase {
            distributed: false,
            ..case
        };
        assert_plan_shape(&ctx, &case).await?;
    }

    workers.abort_all();
    Ok(())
}

/// Whether any distributed stage hands a worker a plan that scans a vortex
/// file.
///
/// This is the part of the plan a worker must decode: the task variants live
/// inside a `DistributedLeafExec`, which exposes them through `variants()`
/// rather than as children.
fn worker_variants_scan_vortex(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut found = false;
    plan.apply(|node| {
        if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>() {
            for variant in leaf.variants() {
                if contains_vortex_scan(variant) {
                    found = true;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking a physical plan cannot fail");
    found
}

/// Whether `plan` reads through a [`VortexSource`] anywhere.
fn contains_vortex_scan(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut found = false;
    plan.apply(|node| {
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>()
            && config.file_source().is::<VortexSource>()
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walking a physical plan cannot fail");
    found
}

/// A distributed query over vortex tables runs on the workers.
///
/// A vortex scan leaf has no `try_to_proto` hook, so it crosses the wire
/// through the vortex codec that `user_codecs` registers on the coordinator
/// *and* every worker. Both shapes that send a scan to a worker are covered:
/// an append-only table whose scan fans out over the stage's tasks, and a
/// primary-key table whose three merge work units merge in the workers.
///
/// The results are asserted against a single-node session, the plan against
/// `DistributedExec`, and the worker-side task variants against a vortex scan
/// leaf — with `fallback_to_local = false`, so a coordinator-only fallback
/// cannot make this pass.
#[test]
fn test_vortex_tables_run_on_the_workers() {
    run_distributed_test(test_vortex_tables_run_on_the_workers_inner());
}

async fn test_vortex_tables_run_on_the_workers_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let append = seed_vortex_append_table(client.clone()).await?;
    let partitioned_pk = seed_vortex_pk_table(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let single_node = single_node_factory(client.clone())?;

    #[rustfmt::skip]
    let queries: Vec<(String, Vec<&str>)> = vec![
        (
            format!(
                "SELECT part, count(*) AS cnt FROM {append} GROUP BY part ORDER BY part"
            ),
            VORTEX_APPEND_COUNTS.to_vec(),
        ),
        // Three range partitions, three keys each, two file versions per key:
        // only a real merge-on-read yields 3 rows per partition.
        (
            format!(
                "SELECT part, count(*) AS cnt FROM {partitioned_pk} \
                 GROUP BY part ORDER BY part"
            ),
            vec![
                "+------+-----+",
                "| part | cnt |",
                "+------+-----+",
                "| 0    | 3   |",
                "| 1    | 3   |",
                "| 2    | 3   |",
                "+------+-----+",
            ],
        ),
    ];
    assert_matches_single_node(&distributed, &single_node, &queries).await?;

    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    for (sql, _) in &queries {
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        let explain = display_plan_ascii(plan.as_ref(), false);
        assert!(plan.is::<DistributedExec>(), "not distributed:\n{explain}");
        assert!(
            worker_variants_scan_vortex(&plan),
            "no worker task was handed a vortex scan:\n{explain}"
        );
    }

    workers.abort_all();
    Ok(())
}

/// Creates the append-only table the vortex tests read: no primary key,
/// no range partition, two files of `t1_schema()` rows.
async fn seed_vortex_append_table(client: MetaDataClientRef) -> Result<String> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_vortex_append_{suffix}");
    create_table_with_file_format(
        client,
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(t1_schema())
            .build(),
        PhysicalFormat::Vortex,
    )
    .await?;
    let table = crate::lakesoul_table::LakeSoulTable::for_name(&name).await?;
    table
        .execute_upsert(batch(
            t1_schema(),
            vec![0, 0, 0, 1, 1, 1, 2, 2, 2],
            vec![1, 2, 3, 11, 12, 13, 21, 22, 23],
        ))
        .await?;
    table
        .execute_upsert(batch(
            t1_schema(),
            vec![0, 0, 0, 1, 1, 1, 2, 2, 2],
            vec![4, 5, 6, 14, 15, 16, 24, 25, 26],
        ))
        .await?;
    Ok(name)
}

/// Rows per range partition of `seed_vortex_append_table`'s table: append-only,
/// so both rounds stay visible.
const VORTEX_APPEND_COUNTS: &[&str] = &[
    "+------+-----+",
    "| part | cnt |",
    "+------+-----+",
    "| 0    | 6   |",
    "| 1    | 6   |",
    "| 2    | 6   |",
    "+------+-----+",
];

/// Creates a range-partitioned primary-key vortex table with two versions of
/// every key: three merge work units, the shape the distributed planner
/// distributes as a union of merge operators.
async fn seed_vortex_pk_table(client: MetaDataClientRef) -> Result<String> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_vortex_pk_{suffix}");
    create_table_with_file_format(
        client,
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(shape_schema())
            .with_primary_keys(vec!["id".to_string()])
            .with_range_partitions(vec!["part".to_string()])
            .build(),
        PhysicalFormat::Vortex,
    )
    .await?;
    let table = crate::lakesoul_table::LakeSoulTable::for_name(&name).await?;
    table.execute_upsert(shape_batch(0, &[0, 1, 2])).await?;
    table.execute_upsert(shape_batch(1, &[0, 1, 2])).await?;
    Ok(name)
}

/// A vortex table whose plan the distributed planner keeps single-node still
/// runs.
///
/// A primary-key table without range partitions has one merge work unit, which
/// is pinned to one task, and the boundary above it is elided
/// (`test_distribution_matrix_by_table_shape`), so the merge operator reads
/// vortex files on the coordinator. The gate checks worker stages only, so such
/// a plan is never refused for the file format it reads.
#[test]
fn test_vortex_table_runs_when_no_stage_is_sent() {
    run_distributed_test(test_vortex_table_runs_when_no_stage_is_sent_inner());
}

async fn test_vortex_table_runs_when_no_stage_is_sent_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_vortex_local_{suffix}");
    create_table_with_file_format(
        client.clone(),
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(pk_schema())
            .with_primary_keys(vec!["id".to_string()])
            .build(),
        PhysicalFormat::Vortex,
    )
    .await?;
    upsert_pk(&name, vec![1, 2, 3], vec![10, 20, 30]).await?;
    upsert_pk(&name, vec![2, 3, 4], vec![200, 300, 400]).await?;

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;

    let sql = format!("SELECT id, v FROM {name} ORDER BY id");
    let plan = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await?
        .collect()
        .await?;
    let explain = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
        .unwrap()
        .to_string();
    assert!(
        !explain.contains("DistributedExec"),
        "this shape must stay on the coordinator:\n{explain}"
    );

    let batches = ctx.sql(&sql).await?.collect().await?;
    assert_batches_eq(
        &sql,
        &[
            "+----+-----+",
            "| id | v   |",
            "+----+-----+",
            "| 1  | 10  |",
            "| 2  | 200 |",
            "| 3  | 300 |",
            "| 4  | 400 |",
            "+----+-----+",
        ],
        &batches,
    );

    workers.abort_all();
    Ok(())
}

/// Like `spawn_workers`, but returns each `Worker` handle and exposes its
/// observability service, so tests can distinguish initialized tasks from
/// empty cache entries (`tasks_running` needs the dev-only `integration` feature).
async fn spawn_workers_with_handles(
    workers: usize,
) -> (Vec<String>, Vec<Worker>, JoinSet<()>) {
    let mut urls = Vec::new();
    let mut handles = Vec::new();
    let mut join_set = JoinSet::new();
    for _ in 0..workers {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}");
        urls.push(url.clone());
        let options = LakeSoulWorkerOptions {
            core_args: CoreArgs::default(),
        };
        let worker = crate::distributed::lakesoul_worker(&options).unwrap();
        let observability = worker.with_observability_service(Arc::new(
            crate::distributed::StaticWorkerResolver::new(vec![url])
                .expect("worker url is well formed"),
        ));
        handles.push(worker.clone());
        join_set.spawn(async move {
            let incoming = TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(worker.into_worker_server())
                .add_service(observability)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
    }
    (urls, handles, join_set)
}

/// Streams `rows` of `(part, b)` into an append-only range-partitioned table
/// through the normal upsert path, one call per file, and returns the table
/// name and the row count written.
async fn seed_large_table(
    client: MetaDataClientRef,
    rows_per_call: usize,
    calls: usize,
) -> Result<(String, usize)> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_cancel_t_{suffix}");
    create_distributed_table(
        client.clone(),
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(t1_schema())
            .with_range_partitions(vec!["part".to_string()])
            .build(),
    )
    .await?;
    let table = crate::lakesoul_table::LakeSoulTable::for_name(&name).await?;
    for _ in 0..calls {
        let rows = rows_per_call;
        let part: Vec<i32> = (0..rows).map(|i| (i % 3) as i32).collect();
        let b: Vec<i32> = (0..rows).map(|i| i as i32).collect();
        table.execute_upsert(batch(t1_schema(), part, b)).await?;
    }
    Ok((name, rows_per_call * calls))
}

#[test]
fn test_dropping_a_running_distributed_stream_stops_the_workers() {
    run_distributed_test(
        test_dropping_a_running_distributed_stream_stops_the_workers_inner(),
    );
}

/// A distributed stream dropped mid-execution must stop the worker tasks
/// promptly — for a streaming head stage and for a blocked one.
///
/// This is the mechanism the PG adapter's statement cancellation relies on:
/// `CancellableRows` reports `57014` and drops the wrapped DataFusion stream,
/// and the coordinator turns that drop into the end of the coordinator→worker
/// channels (see `postgres_lakesoul::cancel`). Verified here end to end over
/// real gRPC workers:
///
/// - First a bare scan, so the coordinator's head stage streams batches
///   continuously and the dropped stream is observed by the stage pump at its
///   next batch. (An aggregating or sorted head stage emits nothing until the
///   scan is over, and would only observe the drop then.)
/// - Then head stages that aggregate, join and sort. Not polling the stream
///   proves nothing by itself: the head stage is pumped in the background, so
///   a single-batch result can land in its output buffer and the query can
///   finish normally while the tasks drain on their own. Instead, each query
///   runs on fresh gated workers whose first file read is parked at a gate
///   that never opens — the arrival counters show the reads are blocked
///   mid-flight — so the input provably cannot finish and only the drop's
///   cancellation can drain the tasks, checked while the gate is still
///   closed. (A planner that blocks its head stage until the input ends is
///   covered by the same mechanism a fortiori.)
/// - Each dispatched stage task holds one worker task entry, invalidated when
///   the worker sees the coordinator channel's EOS that the drop triggers. A
///   leaked cancellation cannot normalize them otherwise: a worker task left
///   running is backpressured — the cancelled query never reads another
///   batch — so it never completes on its own and the entry survives its
///   ten-minute TTL.
/// - Therefore, after each drop, no worker may still hold an initialized task
///   within seconds, and the workers must stay usable.
///
/// `GetTaskProgress` only reports initialized entries holding a plan.
/// `Worker::tasks_running()` instead counts every cache entry: after EOS
/// cleanup, a late `ExecuteTask` can recreate an empty entry that waits for a
/// plan and lingers until the ten-minute TTI. Such an entry has no execution
/// plan, so it must not be mistaken for a leaked initialized task.
async fn test_dropping_a_running_distributed_stream_stops_the_workers_inner() -> Result<()>
{
    let client = Arc::new(MetaDataClient::from_env().await?);
    // Four million rows: the scan outlasts the first batch by far, and its
    // buffered batches far exceed every boundary buffer, so a task left
    // running after the drop stays backpressured instead of finishing.
    let (t, expected_rows) = seed_large_table(client.clone(), 2_000_000, 2).await?;

    let (worker_urls, handles, mut workers) =
        spawn_workers_with_handles(WORKER_COUNT).await;
    let mut observers = Vec::with_capacity(handles.len());
    for url in &worker_urls {
        observers.push(ObservabilityServiceClient::connect(url.clone()).await?);
    }
    let factory = distributed_factory(client.clone(), worker_urls, false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    let mut stream = ctx
        .sql(&format!("SELECT part, b FROM {t}"))
        .await?
        .execute_stream()
        .await?;
    // The first batch proves execution started; the same observation used
    // after cancellation must see initialized plans before the drop.
    drop(stream.next().await.unwrap()?);
    let mut dispatched = 0;
    for observer in &mut observers {
        dispatched += initialized_tasks(observer).await?;
    }
    assert!(
        dispatched > 0,
        "the scan stage must be dispatched while the first batch arrives, got {dispatched}"
    );

    // The cancellation mechanism under test: drop the stream while the scan
    // is still running.
    drop(stream);

    for (index, observer) in observers.iter_mut().enumerate() {
        wait_until_no_initialized_tasks(observer, &format!("scan worker {index}"))
            .await?;
    }

    // The workers are still usable afterwards: a fresh distributed query
    // over the same table returns the full count.
    assert_count(
        &ctx,
        &t,
        expected_rows as i64,
        "a query after the cancelled one must see every row",
    )
    .await?;

    // The same cancellation for head stages that do more than stream a scan:
    // a `GROUP BY`, a join feeding a count, and a sorted Top-K with an
    // `OFFSET`. Not polling the stream proves nothing by itself: the head
    // stage is pumped in the background, so a single-batch result can land in
    // its output buffer and the query can finish normally on its own — the
    // tasks then drain without any cancellation. Instead, each query runs on
    // fresh gated workers whose first file read is parked at a gate that
    // never opens: the input provably cannot finish, so only the drop's
    // cancellation can release the initialized plans — checked while the
    // gate is still closed.
    let small = format!(
        "distributed_cancel_small_{}",
        TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst)
    );
    create_distributed_table(
        client.clone(),
        &small,
        LakeSoulIOConfigBuilder::new()
            .with_schema(t1_schema())
            .with_range_partitions(vec!["part".to_string()])
            .build(),
    )
    .await?;
    crate::lakesoul_table::LakeSoulTable::for_name(&small)
        .await?
        .execute_upsert(batch(t1_schema(), vec![0, 1, 2], vec![0, 1, 2]))
        .await?;

    for sql in [
        format!("SELECT part, count(*) AS c FROM {t} GROUP BY part"),
        format!("SELECT count(*) AS c FROM {t} a JOIN {small} b ON a.part = b.part"),
        format!("SELECT part, b FROM {t} ORDER BY b DESC LIMIT 5 OFFSET 2"),
    ] {
        let (_gate_tx, gate_rx) = tokio::sync::watch::channel(false);
        let (worker_urls, arrived, _handles, mut workers) =
            spawn_gated_workers(WORKER_COUNT, gate_rx).await;
        let mut observers = Vec::with_capacity(worker_urls.len());
        for url in &worker_urls {
            observers.push(ObservabilityServiceClient::connect(url.clone()).await?);
        }
        let factory = distributed_factory(client.clone(), worker_urls, false)?;
        let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;
        let stream = ctx.sql(&sql).await?.execute_stream().await?;
        // Three scan tasks over three workers are assigned to consecutive
        // worker URLs modulo the worker count, so each worker holds one task;
        // each parks its first read at the still closed gate. Otherwise the
        // helper panics at the deadline.
        wait_until_all_reached(&arrived, "head-stage cancellation worker").await;
        for (index, observer) in observers.iter_mut().enumerate() {
            assert!(
                initialized_tasks(observer).await? > 0,
                "head-stage worker {index} must hold a plan while its read is gated"
            );
        }
        // The mechanism under test: drop the stream while every worker's task
        // is parked mid-read, the gate still closed — no task can finish
        // naturally, so reaching zero proves the drop cancelled them.
        drop(stream);
        for (index, observer) in observers.iter_mut().enumerate() {
            wait_until_no_initialized_tasks(
                observer,
                &format!("head-stage worker {index}"),
            )
            .await?;
        }
        // Recovery on the very workers whose tasks were just cancelled: the
        // gate stays closed, but each worker's first read already passed it,
        // so a fresh query must return the full count.
        assert_count(
            &ctx,
            &t,
            expected_rows as i64,
            "a query after the cancelled one must see every row",
        )
        .await?;
        workers.abort_all();
    }

    // The workers are still usable afterwards: a fresh distributed query
    // over the same table returns the full count.
    assert_count(
        &ctx,
        &t,
        expected_rows as i64,
        "a query after the cancelled ones must see every row",
    )
    .await?;

    workers.abort_all();
    Ok(())
}
/// A worker store that serves one read and then fails subsequent reads.
///
/// `fail_enabled` is the fault's kill switch: cleared, the store goes back to
/// serving reads normally, so a test can verify that the system recovers once
/// the fault is removed instead of pinning every worker's session to a
/// permanently broken store.
#[derive(Debug)]
struct FailAfterFirstRead {
    inner: Arc<dyn object_store::ObjectStore>,
    first_served: tokio::sync::Mutex<bool>,
    served: Arc<AtomicUsize>,
    failed: Arc<std::sync::atomic::AtomicBool>,
    fail_enabled: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Display for FailAfterFirstRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fail-after-first-read")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for FailAfterFirstRead {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let mut first_served = self.first_served.lock().await;
        if *first_served && self.fail_enabled.load(Ordering::SeqCst) {
            self.failed.store(true, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "fail-after-first-read",
                source: "injected failure after successful read".into(),
            });
        }
        let result = self.inner.get_opts(location, options).await;
        if result.is_ok() {
            *first_served = true;
            self.served.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[test]
fn test_a_read_failing_after_progress_fails_distributed_query() {
    run_distributed_test(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let (table, _) = seed_large_table(Arc::clone(&client), 4, 32).await?;
        let served = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fail_enabled = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut fault_workers = JoinSet::new();
        let mut urls = Vec::new();
        for _ in 0..WORKER_COUNT {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
            urls.push(format!("http://{}", listener.local_addr()?));
            let runtime = crate::session::build_worker_runtime_env(&CoreArgs::default())?;
            runtime.register_object_store(
                &url::Url::parse("file://")?,
                Arc::new(FailAfterFirstRead {
                    inner: Arc::new(object_store::local::LocalFileSystem::new()),
                    first_served: tokio::sync::Mutex::new(false),
                    served: Arc::clone(&served),
                    failed: Arc::clone(&failed),
                    fail_enabled: Arc::clone(&fail_enabled),
                }),
            );
            let worker = crate::distributed::lakesoul_worker(
                &crate::distributed::LakeSoulWorkerOptions {
                    core_args: CoreArgs::default(),
                },
            )?
            .with_runtime_env(runtime);
            fault_workers.spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(worker.into_worker_server())
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                        listener,
                    ))
                    .await;
            });
        }
        let factory = distributed_factory(Arc::clone(&client), urls, false)?;
        let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;
        expect_query_failure_within(&ctx, &format!("select count(*) from {table}"))
            .await?;
        assert!(
            served.load(Ordering::SeqCst) > 0,
            "a read must succeed first"
        );
        assert!(
            failed.load(Ordering::SeqCst),
            "the injected failure must occur"
        );
        fault_workers.abort_all();
        Ok(())
    });
}

struct RecordingQueryIdBuilder {
    seen: Arc<parking_lot::Mutex<Vec<Option<u64>>>>,
}

#[async_trait::async_trait]
impl datafusion_distributed::WorkerSessionBuilder for RecordingQueryIdBuilder {
    async fn build_session_state(
        &self,
        ctx: datafusion_distributed::WorkerQueryContext,
    ) -> datafusion::common::Result<datafusion::execution::SessionState> {
        self.seen
            .lock()
            .push(crate::distributed::headers::query_id_from_headers(
                &ctx.headers,
            ));
        let mut builder = ctx.builder;
        builder = builder.with_distributed_user_codec(crate::distributed::LakeSoulCodec);
        Ok(builder.build())
    }
}

/// The statement ID of one statement must not leak into the next one's worker
/// tasks, and every worker task must carry its own statement's ID.
///
/// The workers are the point of the test, so it needs `WORKER_COUNT` of them:
/// with a single worker the distributed planner caps every stage at one task,
/// no network boundary survives, and the statement runs on the coordinator
/// without a worker session ever being built.
#[test]
fn test_two_statements_on_one_session_deliver_distinct_ids_to_workers() {
    run_distributed_test(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let (table, _) = seed_large_table(Arc::clone(&client), 8, 8).await?;
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut urls = Vec::new();
        let mut workers = JoinSet::new();
        // A single available worker makes the distributed planner elide every
        // network boundary and run the plan on the coordinator. Start the same
        // worker count as the distributed session so requests actually reach
        // the recording builders.
        for _ in 0..WORKER_COUNT {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
            urls.push(format!("http://{}", listener.local_addr()?));
            let runtime = crate::session::build_worker_runtime_env(&CoreArgs::default())?;
            let worker = datafusion_distributed::Worker::from_session_builder(
                RecordingQueryIdBuilder {
                    seen: Arc::clone(&seen),
                },
            )
            .with_runtime_env(runtime)
            .with_version(crate::distributed::DISTRIBUTED_PROTOCOL_VERSION);
            workers.spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(worker.into_worker_server())
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                        listener,
                    ))
                    .await;
            });
        }
        let factory = distributed_factory(Arc::clone(&client), urls, false)?;
        let context =
            Arc::new(factory.create_session(&LakeSoulSessionOptions::default())?);
        let actor =
            crate::distributed::headers::SessionActor::new("alice", "", "default");

        for query_id in [501_u64, 502] {
            seen.lock().clear();
            let context_for_query = Arc::clone(&context);
            let sql = format!("select count(*) from {table}");
            crate::distributed::headers::with_query_id(
                &context,
                &actor,
                query_id,
                async move { context_for_query.sql(&sql).await?.collect().await },
            )
            .await?;
            let delivered = seen.lock().clone();
            assert!(!delivered.is_empty(), "query {query_id} reached no worker");
            assert!(
                delivered.iter().all(|id| *id == Some(query_id)),
                "worker task headers did not carry {query_id}: {delivered:?}"
            );
        }
        // Poll a labelled statement and cancel it while it is pending. Dropping
        // the future must restore identity-only headers before the next query.
        tokio::select! {
            biased;
            _ = crate::distributed::headers::with_query_id(
                &context,
                &actor,
                503,
                std::future::pending::<()>(),
            ) => panic!("pending statement unexpectedly completed"),
            _ = tokio::task::yield_now() => {}
        }
        seen.lock().clear();
        context
            .sql(&format!("select count(*) from {table}"))
            .await?
            .collect()
            .await?;
        let after_cancel = seen.lock().clone();
        assert!(
            !after_cancel.is_empty(),
            "the query after cancellation reached no worker"
        );
        assert!(
            after_cancel.iter().all(Option::is_none),
            "cancelled statement ID leaked into subsequent worker tasks: {after_cancel:?}"
        );
        workers.abort_all();
        Ok(())
    });
}

// ---------------------------------------------------------------------------
// Failure paths
// ---------------------------------------------------------------------------

/// Seeds a table of `files` files with `rows` rows each, one range partition
/// per file.
///
/// Each `execute_upsert` writes one file, so this is the fixture a fault test
/// needs: with far more files than workers, the scan stage is split into far
/// more tasks than there are workers, every registered worker is given some,
/// and one of them failing is what the query must report.
async fn seed_wide_table(
    client: MetaDataClientRef,
    files: usize,
    rows: usize,
) -> Result<String> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_wide_{suffix}");
    create_distributed_table(
        client,
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(t1_schema())
            .with_range_partitions(vec!["part".to_string()])
            .build(),
    )
    .await?;
    let table = crate::lakesoul_table::LakeSoulTable::for_name(&name).await?;
    for file in 0..files {
        // One partition per file: a scan has as many merge work units as it has
        // files, more than the tests that rely on that have workers.
        let parts: Vec<i32> = (0..rows).map(|_| (file % files.max(1)) as i32).collect();
        let values: Vec<i32> = (0..rows).map(|row| (file * rows + row) as i32).collect();
        table
            .execute_upsert(batch(t1_schema(), parts, values))
            .await?;
    }
    Ok(name)
}

/// A worker that cannot build a session for the tasks it is given.
///
/// The counter it keeps is the same self-proof as the dying worker's: the test
/// asserts it was asked for a task, so a query that failed for some other
/// reason cannot pass for the injected failure.
struct FailingSessionBuilder {
    asked: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl datafusion_distributed::WorkerSessionBuilder for FailingSessionBuilder {
    async fn build_session_state(
        &self,
        _ctx: datafusion_distributed::WorkerQueryContext,
    ) -> datafusion::common::Result<datafusion::execution::SessionState> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Err(datafusion::common::DataFusionError::Execution(
            "this worker cannot serve tasks".to_string(),
        ))
    }
}

/// Replaces one worker of the set with a worker that fails every task.
///
/// The `Worker` handles of all workers are returned too, so a test can check
/// that the query's tasks are drained from the workers after the failure.
async fn spawn_workers_with_one_failing()
-> (Vec<String>, Vec<Worker>, JoinSet<()>, Arc<AtomicUsize>) {
    let (mut urls, mut handles, mut workers) = spawn_workers_with_handles(2).await;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
        .await
        .unwrap();
    urls.push(format!(
        "http://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    ));
    let asked = Arc::new(AtomicUsize::new(0));
    let worker = Worker::from_session_builder(FailingSessionBuilder {
        asked: Arc::clone(&asked),
    })
    // The runtime and version a real worker advertises, or discovery keeps it
    // out of the set and the query never reaches it.
    .with_runtime_env(
        crate::session::build_worker_runtime_env(&CoreArgs::default()).unwrap(),
    )
    .with_version(crate::distributed::DISTRIBUTED_PROTOCOL_VERSION);
    handles.push(worker.clone());
    workers.spawn(async move {
        let _ = tonic::transport::Server::builder()
            .add_service(worker.into_worker_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (urls, handles, workers, asked)
}

/// A worker that cannot serve a task fails the query, and the query does not
/// hang: the plan's design point is that a worker failure fails the query, with
/// no retry on the machines that are left.
///
/// The table is deliberately much wider than the worker set: the scan stage is
/// then split into far more tasks than there are workers, so the one that
/// cannot serve is certain to be given some, instead of hoping that assignment
/// happens to pick it.
#[test]
fn test_a_worker_that_cannot_serve_fails_the_query() {
    run_distributed_test(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let wide = seed_wide_table(Arc::clone(&client), 32, 32).await?;
        let (urls, handles, mut workers, asked) = spawn_workers_with_one_failing().await;
        let factory = distributed_factory(Arc::clone(&client), urls, false)?;
        let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

        expect_query_failure_within(&ctx, &format!("select count(*) from {wide}"))
            .await?;
        assert!(
            asked.load(Ordering::SeqCst) > 0,
            "the worker that cannot serve has to be given a task for this test to \
             be about its failure, asked {}",
            asked.load(Ordering::SeqCst)
        );
        // The healthy workers must drain the query's tasks. The worker that
        // fails at session build never started its task, so nothing on the
        // worker observes the query's end to invalidate its task entry —
        // the same leaked-entry mechanism the cancellation test
        // (`test_dropping_a_running_distributed_stream_stops_the_workers`)
        // documents: such an entry is what the TTL is for, and it holds no
        // running execution.
        for handle in handles[..handles.len() - 1].iter() {
            wait_until_no_tasks(handle, "healthy worker").await;
        }

        workers.abort_all();
        Ok(())
    });
}

// ---------------------------------------------------------------------------
// Remote merge-on-read correctness
// ---------------------------------------------------------------------------

/// `(part, id, score, op)` — the CDC shape of `create_cdc_table` plus a range
/// partition, so queries over the whole table distribute as a union of merge
/// work units.
fn partitioned_cdc_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
        Field::new("op", DataType::Utf8, true),
    ]))
}

fn partitioned_cdc_batch(
    schema: &SchemaRef,
    parts: &[i32],
    ids: &[i32],
    scores: &[i32],
    ops: &[&str],
) -> RecordBatch {
    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(Int32Array::from(parts.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(scores.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(ops.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Remote merge-on-read of a CDC table whose rows live in several range
/// partitions, driven by non-key filters and value-focused projections.
///
/// `create_cdc_table` has no range partition, so every query over it has a
/// single merge work unit and stays on the coordinator
/// (`test_distribution_matrix_by_table_shape`); this fixture has three, so
/// the CDC merge runs as a distributed union on the workers. The non-key
/// filter `score >= 10` is deliberately not a primary-key or range-partition
/// predicate, so the table provider classifies it `Unsupported`: DataFusion
/// evaluates it in a filter above the scan and never hands it to the merge.
/// This query would resurrect stale versions if the classification ever
/// changed and value predicates were pushed below the merge — applied per
/// file, such a predicate keeps a stale insert whose old value satisfies it
/// and drops the file whose newer value does not.
///
/// Per key of the fixture:
/// - key 1: inserted with a satisfying value, updated to a non-satisfying
///   one — the update must win and its old value must not survive the filter;
/// - key 2: deleted with a tombstone whose value differs from the row it
///   deletes — the delete must win;
/// - key 3: deleted, then reinserted with a new value — the reinsert wins;
/// - key 4: never touched, keeps the filtered result non-empty.
///
/// Most queries project `score` or aggregates only; the full-projection query
/// pins the exact surviving rows. The merge's hidden required columns
/// (primary key, CDC change column) must neither leak into the results nor
/// vanish in the distributed plan: dropping them would fail the worker-side
/// merge and surface as wrong results.
#[test]
fn test_remote_cdc_merge_with_non_key_filter() {
    run_distributed_test(test_remote_cdc_merge_with_non_key_filter_inner());
}

/// One upsert call of the partitioned CDC fixture: partition values, keys,
/// scores, change ops.
type CdcWrite<'a> = (&'a [i32], &'a [i32], &'a [i32], &'a [&'a str]);

async fn test_remote_cdc_merge_with_non_key_filter_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let table = format!("distributed_cdc_part_{suffix}");
    create_cdc_table_with_range_partitions(
        client.clone(),
        &table,
        PhysicalFormat::Parquet,
        &["part"],
    )
    .await?;
    let lakehouse = crate::lakesoul_table::LakeSoulTable::for_name(&table).await?;
    let schema = partitioned_cdc_schema();
    let writes: [CdcWrite; 5] = [
        (
            &[0, 1, 2, 0],
            &[1, 2, 3, 4],
            &[10, 20, 30, 40],
            &["insert", "insert", "insert", "insert"],
        ),
        (&[0], &[1], &[5], &["insert"]),
        (&[1], &[2], &[99], &["delete"]),
        (&[2], &[3], &[30], &["delete"]),
        (&[2], &[3], &[31], &["insert"]),
    ];
    for (parts, ids, scores, ops) in writes {
        lakehouse
            .execute_upsert(partitioned_cdc_batch(&schema, parts, ids, scores, ops))
            .await?;
    }

    let (worker_urls, receipts, mut workers) =
        spawn_workers_with_task_recording(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;

    #[rustfmt::skip]
    let queries: [(String, Vec<&str>, bool); 4] = [
        // Full projection in a total order: the update of key 1 wins (no
        // resurrection of score 10 through the stale insert's file), key 2
        // is gone, key 3 carries the reinserted value.
        (
            format!("SELECT part, id, score FROM {table} ORDER BY part, id"),
            vec![
                "+------+----+-------+",
                "| part | id | score |",
                "+------+----+-------+",
                "| 0    | 1  | 5     |",
                "| 0    | 4  | 40    |",
                "| 2    | 3  | 31    |",
                "+------+----+-------+",
            ],
            true,
        ),
        (
            // A stale or resurrected version (10, 20, 30) would appear here.
            format!("SELECT score FROM {table} WHERE score >= 10 ORDER BY score"),
            vec![
                "+-------+",
                "| score |",
                "+-------+",
                "| 31    |",
                "| 40    |",
                "+-------+",
            ],
            true,
        ),
        (
            format!("SELECT count(*) AS c, sum(score) AS s FROM {table}"),
            vec![
                "+---+----+",
                "| c | s  |",
                "+---+----+",
                "| 3 | 76 |",
                "+---+----+",
            ],
            false,
        ),
        (
            format!("SELECT count(*) AS c FROM {table} WHERE score >= 10"),
            vec!["+---+", "| c |", "+---+", "| 2 |", "+---+"],
            false,
        ),
    ];
    for (sql, expected, ordered) in &queries {
        assert_query_runs_on_workers(&distributed, &receipts, sql, expected, *ordered)
            .await?;
    }

    // Three populated partitions: three merge work units behind a union, and
    // none of them may be split across tasks.
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    assert_merge_units_pinned(&ctx, &queries[0].0, 3).await?;

    workers.abort_all();
    Ok(())
}

/// `part, k1, k2, v` — composite primary key `(k1, k2)`, range partitioned by
/// `part`, four hash buckets.
fn bucket_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("k1", DataType::Int32, false),
        Field::new("k2", DataType::Int32, false),
        Field::new("v", DataType::Int32, false),
    ]))
}

fn bucket_batch(rows: &[(i32, i32, i32, i32)]) -> RecordBatch {
    let mut parts = Vec::new();
    let mut k1s = Vec::new();
    let mut k2s = Vec::new();
    let mut values = Vec::new();
    for &(part, k1, k2, v) in rows {
        parts.push(part);
        k1s.push(k1);
        k2s.push(k2);
        values.push(v);
    }
    RecordBatch::try_new(
        bucket_schema(),
        vec![
            Arc::new(Int32Array::from(parts)) as ArrayRef,
            Arc::new(Int32Array::from(k1s)) as ArrayRef,
            Arc::new(Int32Array::from(k2s)) as ArrayRef,
            Arc::new(Int32Array::from(values)) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Creates the four-bucket composite-key table and writes each partition's
/// first key three times, once per file, so the latest file decides.
async fn seed_bucketed_table(client: MetaDataClientRef) -> Result<String> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_bucketed_{suffix}");
    create_distributed_table(
        client.clone(),
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(bucket_schema())
            .with_primary_keys(vec!["k1".to_string(), "k2".to_string()])
            .with_range_partitions(vec!["part".to_string()])
            .with_hash_bucket_num("4")
            .build(),
    )
    .await?;
    let table = crate::lakesoul_table::LakeSoulTable::for_name(&name).await?;
    table
        .execute_upsert(bucket_batch(&[
            (0, 1, 0, 10),
            (1, 2, 0, 11),
            (2, 3, 0, 12),
            (0, 1, 1, 1),
            (1, 2, 1, 2),
            (2, 3, 1, 3),
        ]))
        .await?;
    table
        .execute_upsert(bucket_batch(&[(0, 1, 0, 20), (1, 2, 0, 21), (2, 3, 0, 22)]))
        .await?;
    table
        .execute_upsert(bucket_batch(&[(0, 1, 0, 30), (1, 2, 0, 31), (2, 3, 0, 32)]))
        .await?;
    Ok(name)
}

/// Several hash buckets behind a composite primary key, merged on the workers.
///
/// The plain primary-key fixtures rely on the default single bucket, so their
/// files carry one bucket each and a merge work unit is one bucket's files;
/// `hash_bucket_num = 4` gives every range partition's merge work unit files
/// from several buckets, and the same composite key `(k1, k2)` is updated
/// across three files. The worker-side merge must read every bucket's files
/// for the key and emit only the latest version, with no duplicate rows.
///
/// The merge work units run with more workers than units (5 workers, 3 units)
/// and with fewer (2 workers, 3 units): the unit count, not the worker count,
/// is what must stay pinned to one task each.
#[test]
fn test_multi_bucket_composite_pk_merge_on_workers() {
    run_distributed_test(test_multi_bucket_composite_pk_merge_on_workers_inner());
}

async fn test_multi_bucket_composite_pk_merge_on_workers_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let name = seed_bucketed_table(client.clone()).await?;
    let single_node = single_node_factory(client.clone())?;

    let queries: Vec<(String, Vec<&str>, bool)> = vec![
        // A stale version (10+p, 20+p) or a duplicated key would corrupt this
        // exact sequence.
        (
            format!("SELECT part, k1, k2, v FROM {name} ORDER BY part, k1, k2"),
            vec![
                "+------+----+----+----+",
                "| part | k1 | k2 | v  |",
                "+------+----+----+----+",
                "| 0    | 1  | 0  | 30 |",
                "| 0    | 1  | 1  | 1  |",
                "| 1    | 2  | 0  | 31 |",
                "| 1    | 2  | 1  | 2  |",
                "| 2    | 3  | 0  | 32 |",
                "| 2    | 3  | 1  | 3  |",
                "+------+----+----+----+",
            ],
            true,
        ),
        (
            format!(
                "SELECT count(*) AS c, count(DISTINCT k1) AS d, sum(v) AS s FROM {name}"
            ),
            vec![
                "+---+---+----+",
                "| c | d | s  |",
                "+---+---+----+",
                "| 6 | 3 | 99 |",
                "+---+---+----+",
            ],
            false,
        ),
        // A non-key filter whose stale versions (10+p, 20+p) also satisfy it.
        (
            format!("SELECT v FROM {name} WHERE v > 2 ORDER BY v"),
            vec![
                "+----+", "| v  |", "+----+", "| 3  |", "| 30 |", "| 31 |", "| 32 |",
                "+----+",
            ],
            true,
        ),
    ];

    // More workers than merge work units: five workers, three units.
    {
        let (worker_urls, receipts, mut workers) =
            spawn_workers_with_task_recording(5).await;
        let distributed =
            distributed_factory_with(client.clone(), worker_urls, 5, false)?;
        for (sql, expected, ordered) in &queries {
            assert_query_runs_on_workers(
                &distributed,
                &receipts,
                sql,
                expected,
                *ordered,
            )
            .await?;
        }
        let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
        assert_merge_units_pinned(&ctx, &queries[0].0, 3).await?;
        workers.abort_all();
    }

    // Fewer workers than merge work units: two workers, three units.
    {
        let (worker_urls, mut workers) = spawn_workers(2).await;
        let distributed =
            distributed_factory_with(client.clone(), worker_urls, 2, false)?;
        for (sql, expected, ordered) in &queries {
            let pair = (sql.clone(), expected.clone());
            if *ordered {
                assert_matches_single_node_ordered(&distributed, &single_node, &[pair])
                    .await?;
            } else {
                assert_matches_single_node(&distributed, &single_node, &[pair]).await?;
            }
        }
        let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
        assert_merge_units_pinned(&ctx, &queries[0].0, 3).await?;
        workers.abort_all();
    }

    Ok(())
}

/// Upserts `(part, id, v)` rows into a table with `shape_schema()`.
async fn upsert_shape_rows(table: &str, rows: Vec<(i32, i32, i32)>) -> Result<()> {
    let mut parts = Vec::new();
    let mut ids = Vec::new();
    let mut values = Vec::new();
    for (part, id, v) in rows {
        parts.push(part);
        ids.push(id);
        values.push(v);
    }
    crate::lakesoul_table::LakeSoulTable::for_name(table)
        .await?
        .execute_upsert(
            RecordBatch::try_new(
                shape_schema(),
                vec![
                    Arc::new(Int32Array::from(parts)) as ArrayRef,
                    Arc::new(Int32Array::from(ids)) as ArrayRef,
                    Arc::new(Int32Array::from(values)) as ArrayRef,
                ],
            )
            .unwrap(),
        )
        .await
}

/// `(part, id, v)`: the top values concentrate in partition 2, `v` repeats
/// across partitions with `id` as the final tie-breaker, and the superseded
/// versions (50 for key 1, 500 for key 21) are values that must not appear —
/// 50 stays live only for the never-updated key 11.
async fn seed_topk_table(client: MetaDataClientRef) -> Result<String> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_topk_{suffix}");
    create_distributed_table(
        client.clone(),
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(shape_schema())
            .with_primary_keys(vec!["id".to_string()])
            .with_range_partitions(vec!["part".to_string()])
            .build(),
    )
    .await?;
    upsert_shape_rows(&name, vec![(0, 1, 50), (1, 11, 50), (2, 21, 500)]).await?;
    upsert_shape_rows(
        &name,
        vec![
            (0, 1, 5),
            (0, 2, 5),
            (0, 3, 7),
            (1, 12, 8),
            (1, 13, 9),
            (2, 21, 900),
            (2, 22, 900),
            (2, 23, 901),
        ],
    )
    .await?;
    Ok(name)
}

/// Ordered Top-K, `LIMIT` and `OFFSET` over a plan whose scan is a union of
/// three merge work units, compared in output order.
///
/// The shared `assert_batches_eq` sorts result rows before comparing, so the
/// module's other `ORDER BY` queries cannot catch a wrong global order. This
/// fixture makes the order observable: the top values concentrate in one
/// partition, `v` repeats across partitions so the `ORDER BY` needs the
/// primary key as the final tie-breaker (the ties are cut mid-sequence by
/// `LIMIT`/`OFFSET`), and the stale value 500 must not appear at all.
///
/// The order of the merge work units' output is not pinned — the union
/// concatenates them in whatever order its tasks finish — which is exactly
/// what the global sort at the coordinator must normalize.
#[test]
fn test_remote_topk_limit_offset_keeps_output_order() {
    run_distributed_test(test_remote_topk_limit_offset_keeps_output_order_inner());
}

async fn test_remote_topk_limit_offset_keeps_output_order_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let name = seed_topk_table(client.clone()).await?;

    let (worker_urls, receipts, mut workers) =
        spawn_workers_with_task_recording(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;

    #[rustfmt::skip]
    let queries: [(String, Vec<&str>, bool); 3] = [
        (
            format!("SELECT id, v FROM {name} ORDER BY v DESC, id ASC LIMIT 5"),
            vec![
                "+----+-----+",
                "| id | v   |",
                "+----+-----+",
                "| 23 | 901 |",
                "| 21 | 900 |",
                "| 22 | 900 |",
                "| 11 | 50  |",
                "| 13 | 9   |",
                "+----+-----+",
            ],
            true,
        ),
        (
            format!(
                "SELECT id, v FROM {name} ORDER BY v DESC, id ASC LIMIT 5 OFFSET 2"
            ),
            vec![
                "+----+-----+",
                "| id | v   |",
                "+----+-----+",
                "| 22 | 900 |",
                "| 11 | 50  |",
                "| 13 | 9   |",
                "| 12 | 8   |",
                "| 3  | 7   |",
                "+----+-----+",
            ],
            true,
        ),
        (
            format!("SELECT id, v FROM {name} ORDER BY v ASC, id ASC LIMIT 4"),
            vec![
                "+----+---+",
                "| id | v |",
                "+----+---+",
                "| 1  | 5 |",
                "| 2  | 5 |",
                "| 3  | 7 |",
                "| 12 | 8 |",
                "+----+---+",
            ],
            true,
        ),
    ];
    for (sql, expected, ordered) in &queries {
        assert_query_runs_on_workers(&distributed, &receipts, sql, expected, *ordered)
            .await?;
    }

    // Three merge work units feed the sort; none may be split across tasks.
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;
    for (sql, _, _) in &queries {
        assert_merge_units_pinned(&ctx, sql, 3).await?;
    }

    workers.abort_all();
    Ok(())
}

/// Upserts one version of a `(part, id, v)` primary-key table with
/// `partitions` range partitions, one key per partition: the whole table is
/// one version. Many partitions keep a streaming merge query running for a
/// while after its first batch, so a commit can be ordered against it
/// deterministically.
async fn seed_snapshot_table(
    client: MetaDataClientRef,
    partitions: usize,
) -> Result<String> {
    let suffix = TABLE_SUFFIX.fetch_add(1, Ordering::SeqCst);
    let name = format!("distributed_snapshot_{suffix}");
    create_distributed_table(
        client.clone(),
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(shape_schema())
            .with_primary_keys(vec!["id".to_string()])
            .with_range_partitions(vec!["part".to_string()])
            .build(),
    )
    .await?;
    upsert_shape_rows(&name, snapshot_rows(100, partitions)).await?;
    Ok(name)
}

/// The expected rows of one snapshot version: `(p, p, version + p)`.
fn snapshot_rows(version: i32, partitions: usize) -> Vec<(i32, i32, i32)> {
    (0..partitions)
        .map(|p| (p as i32, p as i32, version + p as i32))
        .collect()
}

/// Rows of `(part, id, v)` batches as triplets, in stream order.
fn shape_rows(batches: &[RecordBatch]) -> Vec<(i32, i32, i32)> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                let column = |index: usize| {
                    batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("shape columns are int32")
                        .value(row)
                };
                (column(0), column(1), column(2))
            })
        })
        .collect()
}

/// A distributed query keeps the file set it captured at planning time,
/// whatever commits land while it runs.
///
/// The overlaps are deterministic — no sleeps, no loop-timing luck:
///
/// - barrier 1: the physical plan is created, *then* a new version commits,
///   *then* the captured plan executes — it must merge the captured files
///   only, never the committed one;
/// - barrier 2: the plan executes on gated workers, whose first file read
///   blocks until the test says so — the gate's arrival counters show every
///   worker's read is parked mid-flight, not merely dispatched — then another
///   version commits, and only then is the gate opened and the same stream
///   read to the end — still exactly the previously committed version.
///
/// `execute_upsert` commits one version of the whole table, so each barrier's
/// expectation is exactly that version's file set: its rows, no duplicate
/// keys, no stale values, no lost rows. Consistency is per committed version;
/// a single cross-partition upsert is one version, not an atomic write
/// observed partway through.
#[test]
fn test_a_query_keeps_the_file_set_it_captured() {
    run_distributed_test(test_a_query_keeps_the_file_set_it_captured_inner());
}

async fn test_a_query_keeps_the_file_set_it_captured_inner() -> Result<()> {
    // 64 merge work units over 3 workers: every worker holds several tasks,
    // and a running query has work left after any first batch.
    const PARTITIONS: usize = 64;
    let client = Arc::new(MetaDataClient::from_env().await?);
    let name = seed_snapshot_table(client.clone(), PARTITIONS).await?;
    let sql = format!("SELECT part, id, v FROM {name}");

    // Barrier 1: capture version 100+p, commit 200+p, execute the captured
    // plan — on recording workers, so the run is pinned to the workers.
    {
        let (worker_urls, receipts, mut workers) =
            spawn_workers_with_task_recording(WORKER_COUNT).await;
        let distributed = distributed_factory(client.clone(), worker_urls, false)?;
        let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;

        let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
        let explain = display_plan_ascii(plan.as_ref(), false);
        assert!(
            plan.is::<DistributedExec>(),
            "the plan must have distributed stages:\n{explain}"
        );
        upsert_shape_rows(&name, snapshot_rows(200, PARTITIONS)).await?;
        let rows = shape_rows(&collect(plan, ctx.task_ctx().clone()).await?);
        assert_eq!(
            sorted_rows(rows),
            snapshot_rows(100, PARTITIONS),
            "the query must keep the file set it captured at planning time"
        );
        assert!(
            recorded_tasks(&receipts) > 0,
            "this test is about the workers' snapshot, not a coordinator-only run"
        );
        workers.abort_all();
    }

    // Barrier 2: capture the 200+p file set, execute on gated workers — wait
    // until every worker's first read is actually parked at the gate (the
    // gate's arrival counter, not a session-builder receipt, which only
    // proves the plan was handed over) — then commit 300+p, open the gate,
    // and read the same stream to the end.
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let (worker_urls, arrived, _handles, mut workers) =
        spawn_gated_workers(WORKER_COUNT, gate_rx).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;

    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let explain = display_plan_ascii(plan.as_ref(), false);
    assert!(
        plan.is::<DistributedExec>(),
        "the plan must have distributed stages:\n{explain}"
    );
    let mut stream = execute_stream(plan, ctx.task_ctx().clone())?;
    wait_until_all_reached(&arrived, "snapshot worker").await;
    // The commit lands while the query is running and blocked, mid-merge.
    upsert_shape_rows(&name, snapshot_rows(300, PARTITIONS)).await?;
    gate_tx.send(true).map_err(|_| {
        DataFusionError::Execution("the gate receiver was dropped".to_string())
    })?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    let rows = shape_rows(&batches);
    assert_eq!(
        sorted_rows(rows),
        snapshot_rows(200, PARTITIONS),
        "a commit landing mid-execution must not enter a running query, and \
         the running query must neither lose nor duplicate rows"
    );

    // A query planned after the commits sees the latest version.
    let rows = shape_rows(&ctx.sql(&sql).await?.collect().await?);
    assert_eq!(
        sorted_rows(rows),
        snapshot_rows(300, PARTITIONS),
        "a query planned after the commits must see every committed version"
    );

    workers.abort_all();
    Ok(())
}

/// Rows of `(part, id, v)` batches, in key order.
fn sorted_rows(mut rows: Vec<(i32, i32, i32)>) -> Vec<(i32, i32, i32)> {
    rows.sort_unstable();
    rows
}

/// Plans `sql` on `ctx` and asserts it has exactly `merge_units` merge work
/// units, none of which is split across distributed tasks
/// (`assert_merge_work_units_pinned` on the `EXPLAIN ANALYZE` output), and
/// that the planner distributed the plan at all: a coordinator-only plan
/// would otherwise pass the pinning check vacuously.
///
/// The `EXPLAIN ANALYZE` text must show the merge operators; if a formatting
/// change ever drops them, the check must fail loudly instead of passing
/// silently.
async fn assert_merge_units_pinned(
    ctx: &SessionContext,
    sql: &str,
    merge_units: usize,
) -> Result<()> {
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let explain = display_plan_ascii(plan.as_ref(), false);
    assert!(
        plan.is::<DistributedExec>(),
        "the plan must have distributed stages:\n{explain}"
    );
    assert_eq!(
        count_merge_work_units(&plan),
        merge_units,
        "unexpected merge work units for {sql}:\n{explain}"
    );
    let batches =
        collect_within_timeout(ctx, &format!("EXPLAIN ANALYZE {sql}"), EXECUTION_TIMEOUT)
            .await?;
    let explain_analyze =
        datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
    assert!(
        explain_analyze.contains("MergeParquetExec"),
        "the analyzed plan must show the merge operators for the pinning check \
         to be about them:\n{explain_analyze}"
    );
    assert_merge_work_units_pinned(&explain_analyze, sql);
    Ok(())
}

// ---------------------------------------------------------------------------
// Failure cleanup and recovery
// ---------------------------------------------------------------------------

/// A read that starts failing mid-query fails the distributed query, drains
/// the tasks from the workers that are still healthy, and — once the fault is
/// removed — leaves the workers and the coordinator usable.
///
/// Runs with `fallback_to_local = false`, so the failed execution cannot hide
/// behind a local rerun: it surfaces as the query's error.
#[test]
fn test_a_failing_read_cleans_up_tasks_and_recovers() {
    run_distributed_test(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table = seed_wide_table(Arc::clone(&client), 16, 8).await?;
        let served = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fail_enabled = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let mut fault_workers = JoinSet::new();
        let mut urls = Vec::new();
        let mut handles = Vec::new();
        for _ in 0..WORKER_COUNT {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
            urls.push(format!("http://{}", listener.local_addr()?));
            let runtime = crate::session::build_worker_runtime_env(&CoreArgs::default())?;
            runtime.register_object_store(
                &url::Url::parse("file://")?,
                Arc::new(FailAfterFirstRead {
                    inner: Arc::new(object_store::local::LocalFileSystem::new()),
                    first_served: tokio::sync::Mutex::new(false),
                    served: Arc::clone(&served),
                    failed: Arc::clone(&failed),
                    fail_enabled: Arc::clone(&fail_enabled),
                }),
            );
            let worker = crate::distributed::lakesoul_worker(
                &crate::distributed::LakeSoulWorkerOptions {
                    core_args: CoreArgs::default(),
                },
            )?
            .with_runtime_env(runtime);
            handles.push(worker.clone());
            fault_workers.spawn(async move {
                let _ = tonic::transport::Server::builder()
                    .add_service(worker.into_worker_server())
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                        listener,
                    ))
                    .await;
            });
        }
        let factory = distributed_factory(Arc::clone(&client), urls, false)?;
        let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

        expect_query_failure_within(&ctx, &format!("select count(*) from {table}"))
            .await?;
        assert!(
            served.load(Ordering::SeqCst) > 0,
            "a read must succeed first"
        );
        assert!(
            failed.load(Ordering::SeqCst),
            "the injected failure must occur"
        );

        // The failure must drain the query's tasks from the healthy workers,
        // not leave them running until their TTL.
        for (index, handle) in handles.iter().enumerate() {
            wait_until_no_tasks(handle, &format!("recovery worker {index}")).await;
        }

        // Once the fault is removed, the workers serve a full query again.
        fail_enabled.store(false, Ordering::SeqCst);
        let count = collect_within_timeout(
            &ctx,
            &format!("select count(*) from {table}"),
            EXECUTION_TIMEOUT,
        )
        .await
        .expect("the query must succeed once the fault is removed");
        assert_eq!(
            count[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("count(*) yields int64")
                .value(0),
            16 * 8,
            "the recovered query must see every row"
        );

        fault_workers.abort_all();
        Ok(())
    });
}

/// A worker that cannot build a session for its tasks fails the query even
/// with `fallback_to_local = true`.
///
/// `fallback_to_local` decides what happens when the distributed *planner*
/// fails (see the gate's unit tests). A task that fails during execution on a
/// worker must surface as the query's error, never as a silent local rerun on
/// the coordinator: a rerun would turn a partial failure into a success and
/// report a result computed from scratch, hiding the broken worker.
#[test]
fn test_a_session_builder_failure_is_not_rerun_locally() {
    run_distributed_test(async {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let wide = seed_wide_table(Arc::clone(&client), 32, 32).await?;
        let (urls, handles, mut workers, asked) = spawn_workers_with_one_failing().await;
        let factory = distributed_factory(Arc::clone(&client), urls, true)?;
        let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

        expect_query_failure_within(&ctx, &format!("select count(*) from {wide}"))
            .await?;
        assert!(
            asked.load(Ordering::SeqCst) > 0,
            "the worker that cannot serve has to be given a task for this test to \
             be about its failure, asked {}",
            asked.load(Ordering::SeqCst)
        );
        // Same reasoning as the failing-worker test: only the workers that
        // served a task must drain; the session-build failure's entry has no
        // running execution behind it and relies on the TTL.
        for handle in handles[..handles.len() - 1].iter() {
            wait_until_no_tasks(handle, "healthy worker").await;
        }

        workers.abort_all();
        Ok(())
    });
}
