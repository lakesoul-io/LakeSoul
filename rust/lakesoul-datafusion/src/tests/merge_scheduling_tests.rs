// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! How many merge-on-read merges a local scan keeps open at once.
//!
//! Every open merge holds a non-spillable `LakeSoulMerge[lane]` reservation
//! from the moment its first unit starts until its stream is dropped. The
//! counter below is fed by the memory pool's consumer registrations, so it
//! measures merges that actually executed, not a partition count read off
//! the plan.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation, UnboundedMemoryPool,
};
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::physical_plan::MergeParquetExec;
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

use crate::Result;
use crate::cli::CoreArgs;
use crate::lakesoul_table::LakeSoulTable;
use crate::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use crate::tests::create_table_with_file_format;
use crate::tests::mor_nullability_tests::write_storage_batch;

/// The production-shaped pkmonth case: one range partition per month of a
/// seven-year table, scanned with two target partitions.
const RANGES: i32 = 84;
const BUCKETS: usize = 2;
const TARGET_PARTITIONS: usize = 2;
/// Keys per range partition, all written by the first commit.
const KEYS: i32 = 6;

/// Counts live `LakeSoulMerge` consumers on top of an unbounded pool.
#[derive(Debug, Default)]
struct MergeTrackingPool {
    inner: UnboundedMemoryPool,
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl MergeTrackingPool {
    fn is_merge(consumer: &MemoryConsumer) -> bool {
        consumer.name().starts_with("LakeSoulMerge")
    }

    fn active(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }

    fn peak(&self) -> usize {
        self.peak.load(Ordering::SeqCst)
    }

    fn reset_peak(&self) {
        self.peak.store(self.active(), Ordering::SeqCst);
    }

    /// Waits for the merges of a dropped stream to unwind: the tasks of
    /// `RepartitionExec`/`CoalescePartitionsExec` release theirs once
    /// aborted, which happens on the runtime, not inside `drop`.
    async fn wait_until_idle(&self) {
        for _ in 0..500 {
            if self.active() == 0 && self.reserved() == 0 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!(
            "merges still open after the stream was dropped: active={}, reserved={}",
            self.active(),
            self.reserved()
        );
    }
}

impl fmt::Display for MergeTrackingPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "merge_tracking(active={})", self.active())
    }
}

impl MemoryPool for MergeTrackingPool {
    fn name(&self) -> &str {
        "merge_tracking"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        if Self::is_merge(consumer) {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
        }
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        if Self::is_merge(consumer) {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion::error::Result<()> {
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

fn full_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("part", DataType::Int32, false),
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, true),
        Field::new("w", DataType::Int32, true),
    ]))
}

fn full_batch(rows: &[(i32, i32, i32, i32)]) -> RecordBatch {
    let column = |f: fn(&(i32, i32, i32, i32)) -> i32| {
        Arc::new(Int32Array::from(rows.iter().map(f).collect::<Vec<_>>())) as ArrayRef
    };
    RecordBatch::try_new(
        full_schema(),
        vec![
            column(|row| row.0),
            column(|row| row.1),
            column(|row| row.2),
            column(|row| row.3),
        ],
    )
    .unwrap()
}

/// Writes three versions into every range partition:
///
/// 1. keys `0..KEYS` with `v = 100 * part + id`, `w = 1`;
/// 2. an overlapping upsert of keys `2..KEYS` with `v += 10_000`, `w = 2`;
/// 3. a partial update storing only `(part, id, v)` for keys 0 and 5 with
///    `v = -1`: `w` keeps the value of the latest version that wrote it.
pub(crate) async fn seed_ranges(
    client: MetaDataClientRef,
    ranges: i32,
) -> Result<String> {
    let name = format!("merge_scheduling_{}", uuid::Uuid::new_v4().simple());
    create_table_with_file_format(
        client.clone(),
        &name,
        LakeSoulIOConfigBuilder::new()
            .with_schema(full_schema())
            .with_primary_keys(vec!["id".to_string()])
            .with_range_partitions(vec!["part".to_string()])
            .with_hash_bucket_num(BUCKETS.to_string())
            .build(),
        PhysicalFormat::Parquet,
    )
    .await?;
    let table =
        LakeSoulTable::for_namespace_and_name("default", &name, Some(client.clone()))
            .await?;
    let first: Vec<_> = (0..ranges)
        .flat_map(|part| (0..KEYS).map(move |id| (part, id, 100 * part + id, 1)))
        .collect();
    table.execute_upsert(full_batch(&first)).await?;
    let second: Vec<_> = (0..ranges)
        .flat_map(|part| (2..KEYS).map(move |id| (part, id, 100 * part + id + 10_000, 2)))
        .collect();
    table.execute_upsert(full_batch(&second)).await?;
    let partial_parts: Vec<i32> = (0..ranges).flat_map(|part| [part, part]).collect();
    let partial_ids: Vec<i32> = (0..ranges).flat_map(|_| [0, KEYS - 1]).collect();
    let partial = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("part", DataType::Int32, false),
            Field::new("id", DataType::Int32, false),
            Field::new("v", DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(partial_parts)) as ArrayRef,
            Arc::new(Int32Array::from(partial_ids)) as ArrayRef,
            Arc::new(Int32Array::from(vec![-1; 2 * ranges as usize])) as ArrayRef,
        ],
    )?;
    write_storage_batch(client, &table, PhysicalFormat::Parquet, partial).await?;
    Ok(name)
}

/// The latest `(v, w)` per key, derived from the three commits above.
fn expected_row(part: i32, id: i32) -> (i32, i32) {
    match id {
        0 => (-1, 1),
        1 => (100 * part + 1, 1),
        id if id == KEYS - 1 => (-1, 2),
        id => (100 * part + id + 10_000, 2),
    }
}

/// `part -> (count, sum(v), sum(w))`.
pub(crate) fn expected_groups(ranges: i32) -> BTreeMap<i32, (i64, i64, i64)> {
    (0..ranges)
        .map(|part| {
            let (sum_v, sum_w) = (0..KEYS).fold((0i64, 0i64), |(v, w), id| {
                let (row_v, row_w) = expected_row(part, id);
                (v + row_v as i64, w + row_w as i64)
            });
            (part, (KEYS as i64, sum_v, sum_w))
        })
        .collect()
}

fn tracking_session(
    client: MetaDataClientRef,
    pool: &Arc<MergeTrackingPool>,
    target_partitions: usize,
) -> Result<Arc<SessionContext>> {
    LakeSoulSessionFactory::new_with_memory_pool(
        client,
        &CoreArgs::default(),
        Arc::clone(pool) as Arc<dyn MemoryPool>,
    )?
    .with_target_partitions(target_partitions)
    .create_session(&LakeSoulSessionOptions::default())
}

/// `part -> (count, sum(v), sum(w))` of `table`, each range emitted once.
pub(crate) async fn grouped_result(
    ctx: &SessionContext,
    table: &str,
) -> Result<BTreeMap<i32, (i64, i64, i64)>> {
    let batches = ctx
        .sql(&format!(
            "SELECT part, count(*) AS c, sum(v) AS sv, sum(w) AS sw \
             FROM {table} GROUP BY part"
        ))
        .await?
        .collect()
        .await?;
    let mut groups = BTreeMap::new();
    for batch in &batches {
        let column = |index: usize| batch.column(index).as_ref();
        let parts = column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        let counts = column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let sum_v = column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let sum_w = column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        for row in 0..batch.num_rows() {
            assert!(!sum_v.is_null(row) && !sum_w.is_null(row));
            let previous = groups.insert(
                parts.value(row),
                (counts.value(row), sum_v.value(row), sum_w.value(row)),
            );
            assert!(
                previous.is_none(),
                "range {} emitted twice",
                parts.value(row)
            );
        }
    }
    Ok(groups)
}

/// `(lanes, work units)` of every merge in a physical plan, in plan order.
fn merge_layout(plan: &Arc<dyn ExecutionPlan>) -> Vec<(usize, usize)> {
    let mut merges = Vec::new();
    plan.apply(|node| {
        if let Some(merge) = node.downcast_ref::<MergeParquetExec>() {
            assert!(
                merge.output_ordering().is_none(),
                "a merge of independent units must not advertise an ordering"
            );
            let lanes = match merge.output_partitioning() {
                Partitioning::UnknownPartitioning(lanes) => *lanes,
                other => panic!("merge lanes must be UnknownPartitioning, got {other:?}"),
            };
            merges.push((lanes, merge.work_units().len()));
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    merges
}

/// 84 range partitions under `target_partitions = 2` open at most two merges
/// at a time, and still return every range's latest versions exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_ranges_open_at_most_target_partitions_merges() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table = seed_ranges(client.clone(), RANGES).await?;
    let pool = Arc::new(MergeTrackingPool::default());
    let ctx = tracking_session(client, &pool, TARGET_PARTITIONS)?;

    assert_eq!(grouped_result(&ctx, &table).await?, expected_groups(RANGES));
    assert!(pool.peak() >= 1, "the scan must have executed merges");
    assert!(
        pool.peak() <= TARGET_PARTITIONS,
        "{} merges were open at once for {RANGES} ranges and target_partitions = \
         {TARGET_PARTITIONS}",
        pool.peak()
    );
    pool.wait_until_idle().await;

    let plan = ctx
        .sql(&format!("SELECT * FROM {table}"))
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(
        merge_layout(&plan),
        vec![(TARGET_PARTITIONS, RANGES as usize * BUCKETS)],
        "every range's bucket units belong to one local merge with \
         target_partitions lanes"
    );
    Ok(())
}

/// A full row scan returns every latest version once, with the range column
/// injected from each unit's own range rather than its merge neighbours.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_range_merge_keeps_versions_and_range_defaults() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let ranges = 5;
    let table = seed_ranges(client.clone(), ranges).await?;
    let pool = Arc::new(MergeTrackingPool::default());
    let ctx = tracking_session(client, &pool, TARGET_PARTITIONS)?;

    let batches = ctx
        .sql(&format!("SELECT part, id, v, w FROM {table}"))
        .await?
        .collect()
        .await?;
    let mut rows = BTreeMap::new();
    for batch in &batches {
        let int = |index: usize| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .clone()
        };
        let (parts, ids, v, w) = (int(0), int(1), int(2), int(3));
        for row in 0..batch.num_rows() {
            let key = (parts.value(row), ids.value(row));
            assert!(
                rows.insert(key, (v.value(row), w.value(row))).is_none(),
                "key {key:?} emitted twice"
            );
        }
    }
    let expected: BTreeMap<_, _> = (0..ranges)
        .flat_map(|part| (0..KEYS).map(move |id| ((part, id), expected_row(part, id))))
        .collect();
    assert_eq!(rows, expected);
    assert!(pool.peak() <= TARGET_PARTITIONS);
    pool.wait_until_idle().await;
    Ok(())
}

/// Dropping a scan after its first batch releases every open merge. Whether a
/// merge is still open at the drop depends on the coalescing producers'
/// schedule; the lane-level test in `lakesoul-io` pins release mid-unit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_a_scan_early_releases_its_merges() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table = seed_ranges(client.clone(), RANGES).await?;
    let pool = Arc::new(MergeTrackingPool::default());
    let ctx = tracking_session(client, &pool, TARGET_PARTITIONS)?;

    let mut stream = ctx
        .sql(&format!("SELECT * FROM {table}"))
        .await?
        .execute_stream()
        .await?;
    let first = stream.next().await.expect("the scan yields rows")?;
    assert!(first.num_rows() > 0);
    assert!(pool.peak() >= 1, "the scan must have executed a merge");
    drop(stream);
    pool.wait_until_idle().await;
    assert!(pool.peak() <= TARGET_PARTITIONS);

    // The session stays usable after the cancelled scan.
    pool.reset_peak();
    assert_eq!(grouped_result(&ctx, &table).await?, expected_groups(RANGES));
    assert!(pool.peak() <= TARGET_PARTITIONS);
    Ok(())
}

/// A range predicate that matches no range partition plans no merge and opens
/// none; one that matches a single range keeps all its bucket units.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pruned_ranges_plan_only_their_units() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let ranges = 3;
    let table = seed_ranges(client.clone(), ranges).await?;
    let pool = Arc::new(MergeTrackingPool::default());
    let ctx = tracking_session(client, &pool, TARGET_PARTITIONS)?;

    let none = format!("SELECT * FROM {table} WHERE part = {ranges}");
    let plan = ctx.sql(&none).await?.create_physical_plan().await?;
    assert!(merge_layout(&plan).is_empty());
    let batches = ctx.sql(&none).await?.collect().await?;
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    assert_eq!(pool.peak(), 0, "no merge may open for a pruned scan");

    let one = format!("SELECT * FROM {table} WHERE part = 1");
    let plan = ctx.sql(&one).await?.create_physical_plan().await?;
    assert_eq!(merge_layout(&plan), vec![(BUCKETS, BUCKETS)]);
    let mut expected = expected_groups(ranges);
    expected.retain(|part, _| *part == 1);
    // A filtered table expression: `grouped_result` appends `GROUP BY part`.
    let filtered = grouped_result(&ctx, &format!("{table} WHERE part = 1")).await?;
    assert_eq!(filtered, expected);
    pool.wait_until_idle().await;
    Ok(())
}
