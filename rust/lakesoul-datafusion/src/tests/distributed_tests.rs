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
//! Not covered by this suite, deliberately:
//!
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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::array::{ArrayRef, Int32Array, Int64Array};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion_distributed::{DistributedExec, Worker, display_plan_ascii};
use futures::StreamExt;
use lakesoul_io::config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::physical_plan::MergeParquetExec;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;
use tokio_stream::wrappers::TcpListenerStream;

use crate::distributed::{DistributedOptions, LakeSoulWorkerOptions, WorkerDiscovery};
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use crate::tests::{
    assert_batches_eq, cdc_batch, create_cdc_table, create_table_with_file_format,
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
/// The distributed codec ships Parquet scan leaves to workers; a vortex scan
/// leaf has no wire form yet (`VortexSource` has no `try_to_proto` and the
/// encoding context `datafusion-proto` would need is not reachable from a
/// `PhysicalExtensionCodec`), so distributed execution currently runs on
/// Parquet tables only. A query over a vortex table that would use the workers
/// is refused while planning (the gate in `distributed::planner`); one that
/// stays on the coordinator still runs (see the vortex tests at the end of this
/// module).
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
    // shapes, and `assert_matches_single_node` additionally asserts that every
    // `EXPLAIN` stage containing a `MergeParquetExec` runs in one task.
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

/// Creates the append-only table the vortex wire tests read: no primary key,
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

/// A vortex scan that a query would send to a worker is refused while
/// planning.
///
/// No worker can decode a vortex file source, and the coordinator only notices
/// while sending the stage — its encoding error surfaces as a worker-side
/// failure (the worker waits for a plan that never arrives) long after planning
/// has returned. The gate therefore fails the query at planning time, where
/// `fallback_to_local` can still act on it (see the next test).
///
/// Both shapes that send a scan to a worker are covered: an append-only table
/// whose scan fans out over the stage's tasks, and a primary-key table whose
/// three merge work units merge in the workers.
#[test]
fn test_vortex_table_is_refused_by_the_distributed_planner() {
    run_distributed_test(test_vortex_table_is_refused_by_the_distributed_planner_inner());
}

async fn test_vortex_table_is_refused_by_the_distributed_planner_inner() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let append = seed_vortex_append_table(client.clone()).await?;
    let partitioned_pk = seed_vortex_pk_table(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, false)?;
    let ctx = distributed.create_session(&LakeSoulSessionOptions::default())?;

    for (shape, sql) in [
        (
            "append-only scan",
            format!(
                "SELECT part, count(*) AS cnt FROM {append} GROUP BY part ORDER BY part"
            ),
        ),
        (
            "primary-key work units",
            format!("SELECT id, count(*) AS c FROM {partitioned_pk} GROUP BY id"),
        ),
    ] {
        let err = ctx
            .sql(&sql)
            .await?
            .collect()
            .await
            .expect_err("a vortex stage cannot be sent to a worker");
        let message = err.find_root().to_string();
        assert!(
            message.contains("wire-encodable scan leaf"),
            "{shape}: expected the gate to refuse the plan while planning: {message}"
        );
        assert!(message.contains(".vortex"), "{shape}: {message}");
    }

    workers.abort_all();
    Ok(())
}

/// The development fallback turns the same refusal into a coordinator-only
/// query: the gate plans it with the plain LakeSoul planner, which reads vortex
/// files in place.
#[test]
fn test_vortex_table_runs_on_the_coordinator_with_the_fallback() {
    run_distributed_test(
        test_vortex_table_runs_on_the_coordinator_with_the_fallback_inner(),
    );
}

async fn test_vortex_table_runs_on_the_coordinator_with_the_fallback_inner() -> Result<()>
{
    let client = Arc::new(MetaDataClient::from_env().await?);
    let name = seed_vortex_append_table(client.clone()).await?;

    let (worker_urls, mut workers) = spawn_workers(WORKER_COUNT).await;
    let distributed = distributed_factory(client.clone(), worker_urls, true)?;
    let single_node = single_node_factory(client.clone())?;
    let sql =
        format!("SELECT part, count(*) AS cnt FROM {name} GROUP BY part ORDER BY part");
    let queries = vec![(sql.clone(), VORTEX_APPEND_COUNTS.to_vec())];
    assert_matches_single_node(&distributed, &single_node, &queries).await?;

    // The plan is the local planner's: no stage is sent, so nothing about it
    // depends on a worker.
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
        "the fallback must run the query on the coordinator:\n{explain}"
    );

    workers.abort_all();
    Ok(())
}

/// Only stages are checked, so a vortex table whose plan the distributed
/// planner keeps single-node runs without the fallback.
///
/// A primary-key table without range partitions has one merge work unit, which
/// is pinned to one task, and the boundary above it is elided
/// (`test_distribution_matrix_by_table_shape`), so the merge operator reads
/// vortex files on the coordinator. Refusing that plan would fail a query no
/// worker was ever asked to run.
#[test]
fn test_vortex_table_runs_without_the_fallback_when_no_stage_is_sent() {
    run_distributed_test(
        test_vortex_table_runs_without_the_fallback_when_no_stage_is_sent_inner(),
    );
}

async fn test_vortex_table_runs_without_the_fallback_when_no_stage_is_sent_inner()
-> Result<()> {
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
        "this shape must stay on the coordinator, or the gate would refuse \
         it:\n{explain}"
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

/// Like `spawn_workers`, but each worker's `Worker` handle is returned too, so
/// a test can observe task lifecycle through `Worker::tasks_running` (which
/// needs the `integration` feature, enabled for this crate's dev deps).
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
        urls.push(format!("http://127.0.0.1:{port}"));
        let options = LakeSoulWorkerOptions {
            core_args: CoreArgs::default(),
        };
        let worker = crate::distributed::lakesoul_worker(&options).unwrap();
        handles.push(worker.clone());
        join_set.spawn(async move {
            let incoming = TcpListenerStream::new(listener);
            tonic::transport::Server::builder()
                .add_service(worker.into_worker_server())
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
/// promptly.
///
/// This is the mechanism the PG adapter's statement cancellation relies on:
/// `CancellableRows` reports `57014` and drops the wrapped DataFusion stream,
/// and the coordinator turns that drop into the end of the coordinator→worker
/// channels (see `postgres_lakesoul::cancel`). Verified here end to end over
/// real gRPC workers:
///
/// - The query is a bare scan, so the coordinator's head stage streams
///   batches continuously and the dropped stream is observed by the stage
///   pump at its next batch. (An aggregating or sorted head stage emits
///   nothing until the scan is over, and would only observe the drop then.)
/// - Each dispatched stage task holds one worker task entry, invalidated when
///   the worker sees the coordinator channel's EOS that the drop triggers. A
///   leaked cancellation cannot normalize them otherwise: a worker task left
///   running is backpressured — the cancelled query never reads another
///   batch — so it never completes on its own and the entry survives its
///   ten-minute TTL.
/// - Therefore, after the drop, every worker's `tasks_running()` must reach
///   zero within seconds, and the workers must stay usable.
async fn test_dropping_a_running_distributed_stream_stops_the_workers_inner() -> Result<()>
{
    let client = Arc::new(MetaDataClient::from_env().await?);
    // Four million rows: the scan outlasts the first batch by far, and its
    // buffered batches far exceed every boundary buffer, so a task left
    // running after the drop stays backpressured instead of finishing.
    let (t, expected_rows) = seed_large_table(client.clone(), 2_000_000, 2).await?;

    let (worker_urls, handles, mut workers) =
        spawn_workers_with_handles(WORKER_COUNT).await;
    let factory = distributed_factory(client, worker_urls, false)?;
    let ctx = factory.create_session(&LakeSoulSessionOptions::default())?;

    let mut stream = ctx
        .sql(&format!("SELECT part, b FROM {t}"))
        .await?
        .execute_stream()
        .await?;
    // The first batch can only arrive after the plan was dispatched, so the
    // workers hold un-finalized task entries right now.
    drop(stream.next().await.unwrap()?);
    let dispatched: usize =
        futures::future::join_all(handles.iter().map(Worker::tasks_running))
            .await
            .into_iter()
            .sum();
    assert!(
        dispatched > 0,
        "the scan stage must be dispatched while the first batch arrives, got {dispatched}"
    );

    // The cancellation mechanism under test: drop the stream while the scan
    // is still running.
    drop(stream);

    for handle in &handles {
        let mut drained = false;
        for _ in 0..100 {
            if handle.tasks_running().await == 0 {
                drained = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            drained,
            "the dropped stream must stop the worker tasks; {} still running",
            handle.tasks_running().await
        );
    }

    // The workers are still usable afterwards: a fresh distributed query
    // over the same table returns the full count.
    let count = ctx
        .sql(&format!("SELECT count(*) AS c FROM {t}"))
        .await?
        .collect()
        .await?;
    assert_eq!(count.len(), 1);
    assert_eq!(
        count[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count(*) yields int64")
            .value(0),
        expected_rows as i64,
        "a query after the cancelled one must see every row"
    );

    workers.abort_all();
    Ok(())
}
