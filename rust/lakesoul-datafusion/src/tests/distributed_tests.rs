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
//! hash `GROUP BY` after the scan, and queries running while upserts commit.
//!
//! Not covered by this suite, deliberately:
//!
//! - *compacted + uncompacted files in one partition*: this build has no
//!   Rust-side compaction entry point (the compaction service lives in the
//!   Spark integration), so the mix cannot be produced here;
//! - *schema evolution and default-column filling across files*: writing a
//!   narrower batch into a wider table needs a narrower declared schema,
//!   which the upsert path does not offer. The nullability half of that path
//!   — a logically non-null column kept non-null while merging files that
//!   lack it — is pinned by `catalog`/`merge` unit tests instead.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{ArrayRef, Int32Array, Int64Array};
use arrow::record_batch::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

use crate::distributed::{DistributedOptions, LakeSoulWorkerOptions, WorkerDiscovery};
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use crate::tests::{assert_batches_eq, cdc_batch, create_cdc_table, create_table};
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
    create_table(
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
    create_table(
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
    create_cdc_table(client.clone(), &table).await?;
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
    create_table(
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

#[test]
fn test_production_fails_fast_without_workers() {
    run_distributed_test(test_production_fails_fast_without_workers_inner());
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
    run_distributed_test(test_dev_fallback_runs_single_node_inner());
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
