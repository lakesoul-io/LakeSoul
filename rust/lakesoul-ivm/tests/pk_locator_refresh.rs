// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end measurement of the pk locator on IVM refresh reads.
//!
//! All tables are vortex, so the locator applies to the `old` as-of read, the
//! MV state reads and the MIN value-state reads.  Run the two modes in fresh
//! processes and compare the printed timings plus the `LAKESOUL_PK_PROFILE`
//! lines (rows read, files probed, index build/hit time):
//!
//! ```sh
//! # locator on (shares the local disk cache)
//! LAKESOUL_CACHE=1 LAKESOUL_PK_PROFILE=1 \
//!   cargo test --profile test-fast -p lakesoul-ivm -j 1 --test pk_locator_refresh \
//!     -- --ignored --nocapture
//! # baseline (plain scans)
//! PK_BENCH_LOCATOR=off LAKESOUL_PK_PROFILE=1 cargo test ... (same command)
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, MinMaxKind, MinMaxView,
    PhysicalFormat, SumCountView, min_max_mv_schema_for, sum_count_mv_schema_for,
    value_count_state_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn enable_locator_cache() {
    if std::env::var("PK_BENCH_LOCATOR").as_deref() == Ok("off") {
        return;
    }
    static ENABLE: Once = Once::new();
    ENABLE.call_once(|| {
        // SAFETY: set once before the first locator use in this test process.
        unsafe { std::env::set_var("LAKESOUL_CACHE", "1") };
    });
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, String, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.1.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn count_files(path: &std::path::Path) -> usize {
    std::fs::read_dir(path)
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| {
                    entry
                        .file_type()
                        .map(|kind| kind.is_file())
                        .unwrap_or(false)
                })
                .count()
        })
        .unwrap_or(0)
}

async fn sql_state(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> HashMap<String, (i64, i64)> {
    let context = SessionContext::new();
    let batches = source.read_current(runtime.client()).await.unwrap();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(source.schema.clone(), vec![batches]).unwrap());
    context.register_table("src", table).unwrap();
    let frame = context
        .sql(&format!(
            "select g, sum(amount) as s, min(amount) as m from src \
             where \"{CHANGE_COLUMN}\" <> 'delete' group by g"
        ))
        .await
        .unwrap();
    let mut state = HashMap::new();
    for batch in frame.collect().await.unwrap() {
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
        let mins = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            state.insert(
                groups.value(row).to_string(),
                (sums.value(row), mins.value(row)),
            );
        }
    }
    state
}

async fn read_column(
    runtime: &IvmRuntime,
    mv: &IvmTable,
    column: &str,
) -> HashMap<String, i64> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(schema.index_of(column).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                state.insert(groups.value(row).to_string(), values.value(row));
            }
        }
    }
    state
}

#[test_log::test(tokio::test)]
#[ignore]
async fn pk_locator_refresh_measurement() {
    enable_locator_cache();
    let batches = env_usize("PK_BENCH_BATCHES", 8);
    let rows_per_batch = env_usize("PK_BENCH_ROWS", 8_192);
    let groups = env_usize("PK_BENCH_GROUPS", 64) as i64;
    let warm_rounds = env_usize("PK_BENCH_WARM_ROUNDS", 3);

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pkbench_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    let group_keys = vec!["g".to_string()];
    let sum_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pkbench_sum_{suffix}"),
                table_path(&dir, "sum_mv"),
                sum_count_mv_schema_for(&source.schema, &group_keys, Some("amount"))
                    .unwrap(),
            )
            .with_primary_keys(group_keys.clone())
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    let sum_view = SumCountView::new_with_group_keys(
        format!("pkbench_sum_{suffix}"),
        source.clone(),
        sum_mv.clone(),
        group_keys.clone(),
        Some("amount".to_string()),
    );
    let min_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pkbench_min_{suffix}"),
                table_path(&dir, "min_mv"),
                min_max_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    "amount",
                    MinMaxKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone())
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    let min_state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pkbench_min_state_{suffix}"),
                table_path(&dir, "min_state"),
                value_count_state_schema_for(&source.schema, &group_keys, "amount")
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string(), "value".to_string()])
            .with_bucket_columns(group_keys.clone())
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    let min_view = MinMaxView::new(
        format!("pkbench_min_{suffix}"),
        source.clone(),
        min_mv.clone(),
        min_state.clone(),
        "g",
        "amount",
        MinMaxKind::Min,
    );

    // Initial data: unique keys spread over the groups, one file per batch.
    let setup_start = Instant::now();
    let mut next_key = 0i64;
    for _ in 0..batches {
        let rows = (0..rows_per_batch)
            .map(|index| {
                let key = next_key;
                next_key += 1;
                let group = key % groups;
                (
                    key,
                    format!("g{group:03}"),
                    (index as i64 + 1) * 10,
                    "insert",
                )
            })
            .collect::<Vec<_>>();
        source
            .append_batch(runtime.client(), batch(&rows))
            .await
            .unwrap();
    }
    let setup = setup_start.elapsed();
    let total_rows = next_key as usize;
    let src_files_initial = count_files(&dir.path().join("src"));

    // Cold refresh: every touched file builds its index.
    let cold_start = Instant::now();
    runtime.refresh_sum_count(&sum_view).await.unwrap().unwrap();
    let cold_sum = cold_start.elapsed();
    let cold_start = Instant::now();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
    let cold_min = cold_start.elapsed();

    // Warm rounds: update a handful of keys, then refresh again.  Per-round
    // durations are reported as median/min because metadata commits dominate
    // the mean and are noisy under serializable transactions.
    let mut warm_sum_rounds = Vec::with_capacity(warm_rounds);
    let mut warm_min_rounds = Vec::with_capacity(warm_rounds);
    for round in 0..warm_rounds {
        let updates = (0..8)
            .map(|index| {
                let key = ((round * 8 + index) as i64 * 7 + 1) % next_key;
                let group = key % groups;
                (key, format!("g{group:03}"), 100_000 + key, "insert")
            })
            .collect::<Vec<_>>();
        source
            .append_batch(runtime.client(), batch(&updates))
            .await
            .unwrap();
        let start = Instant::now();
        runtime.refresh_sum_count(&sum_view).await.unwrap().unwrap();
        warm_sum_rounds.push(start.elapsed());
        let start = Instant::now();
        runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
        warm_min_rounds.push(start.elapsed());
    }

    // Correctness is required in both modes.
    let expected = sql_state(&runtime, &source).await;
    assert_eq!(
        read_column(&runtime, &sum_mv, "sum_v").await,
        expected
            .iter()
            .map(|(group, (sum, _))| (group.clone(), *sum))
            .collect::<HashMap<_, _>>()
    );
    assert_eq!(
        read_column(&runtime, &min_mv, "value").await,
        expected
            .iter()
            .map(|(group, (_, min))| (group.clone(), *min))
            .collect::<HashMap<_, _>>()
    );

    fn median(mut values: Vec<Duration>) -> Duration {
        values.sort_unstable();
        values[values.len() / 2]
    }

    let locator = std::env::var("PK_BENCH_LOCATOR").unwrap_or_else(|_| "on".into());
    let (hits, misses) = lakesoul_io::pk_locator::cache_stats();
    eprintln!(
        "[pk-locator-refresh] locator={locator} rows={total_rows} batches={batches} \
         groups={groups} src_files_initial={src_files_initial} src_files={} \
         sum_files={} min_state_files={} \
         setup={setup:?} cold_sum={cold_sum:?} cold_min={cold_min:?} \
         warm_sum_median={:?} warm_sum_min={:?} warm_min_median={:?} warm_min_min={:?} \
         ({warm_rounds} rounds) index_cache=(hits={hits}, misses={misses})",
        count_files(&dir.path().join("src")),
        count_files(&dir.path().join("sum_mv")),
        count_files(&dir.path().join("min_state")),
        median(warm_sum_rounds.clone()),
        warm_sum_rounds.iter().min().copied().unwrap_or_default(),
        median(warm_min_rounds.clone()),
        warm_min_rounds.iter().min().copied().unwrap_or_default(),
    );
}
