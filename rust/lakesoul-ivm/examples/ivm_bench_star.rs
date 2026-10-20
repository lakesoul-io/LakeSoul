// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! P0.1 benchmark: a 10M-row star schema refreshed incrementally and then
//! aggregated.
//!
//! ```text
//! fact(k, fk1, fk2, fk3, v)  ──┬── LEFT JOIN dim1 ON dim1.k = fact.fk1
//! dim1(k, a, b)                ├── LEFT JOIN dim2 ON dim2.k = fact.fk2
//! dim2(k, a, b)                └── LEFT JOIN dim3 ON dim3.k = fact.fk3
//! dim3(k, a, b)                        │
//!                                      ▼
//!                          join MV (k, v, a1, a2, a3)
//!                                      │
//!                                      ▼
//!                     agg MV (a1, a2, SUM(v), COUNT(v))
//! ```
//!
//! The fact delta is 1%–5% per round, the dimensions are ~10k rows and change
//! rarely, and the aggregate has ~10k groups.  The tool records the join-step
//! and aggregate-step latency, the process peak RSS, and the state file
//! counts; with `--verify` it compares the incremental aggregate with a full
//! rebuild of both views.
//!
//! ```sh
//! # local storage, 1M rows, 1% fact changes, 3 measured rounds
//! cargo run --release -p lakesoul-ivm --example ivm_bench_star -- \
//!     --rows 1000000 --change-ratio 0.01 --rounds 3 --storage local
//! ```
//!
//! The S3 mode expects `AWS_*` and `LAKESOUL_PG_*` like the other benchmark;
//! the bucket must exist.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_COUNT_COLUMN, IVM_SUM_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    JoinKeyExprs, LookupChainColumn, LookupChainStep, LookupChainView, PhysicalFormat,
    SumCountView, lookup_chain_mv_schema_for, sum_count_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Storage {
    Local,
    S3,
}

struct Args {
    rows: usize,
    dim_rows: usize,
    change_ratio: f64,
    dim_change_ratio: f64,
    rounds: usize,
    warmup: usize,
    batch: usize,
    seed: u64,
    storage: Storage,
    bucket: String,
    prefix: String,
    buckets: u32,
    format: PhysicalFormat,
    hot_dim: Option<usize>,
    verify: bool,
    keep: bool,
    out: Option<String>,
    compact_every: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            rows: 1_000_000,
            dim_rows: 10_000,
            change_ratio: 0.01,
            dim_change_ratio: 0.0,
            rounds: 3,
            warmup: 1,
            batch: 65_536,
            seed: 42,
            storage: Storage::S3,
            bucket: "lakesoul-test-bucket".to_string(),
            prefix: "ivm-bench-star".to_string(),
            buckets: 4,
            format: PhysicalFormat::Parquet,
            hot_dim: None,
            verify: false,
            keep: false,
            out: None,
            compact_every: 0,
        }
    }
}

fn value_of(args: &[String], index: &mut usize) -> String {
    *index += 1;
    args.get(*index)
        .unwrap_or_else(|| panic!("missing value after {}", args[*index - 1]))
        .clone()
}

fn number<T: std::str::FromStr>(value: &str) -> T
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .unwrap_or_else(|error| panic!("{value} is not a number: {error}"))
}

fn print_help() {
    println!(
        "\
ivm_bench_star: 10M star-schema incremental join + aggregation benchmark

Usage: ivm_bench_star [OPTIONS]

  --rows <N>                 fact rows to seed (default 1000000)
  --dim-rows <N>             rows per dimension (default 10000)
  --change-ratio <F>         fact change fraction per round (default 0.01)
  --dim-change-ratio <F>     dimension change fraction per round (default 0)
  --rounds <N>               measured rounds (default 3)
  --warmup <N>               warmup rounds, not measured (default 1)
  --batch <N>                source write batch size (default 65536)
  --seed <N>                 deterministic workload seed (default 42)
  --buckets <N>              hash buckets per table (default 4)
  --format <parquet|vortex|vortex-compact>
                             data file format (default parquet)
  --hot-dim <N>              map fact FKs into N dimension keys (default:
                             all dimension keys; smaller N increases fanout)
  --storage <local|s3>       table storage (default s3)
  --bucket <NAME>            S3 bucket (default lakesoul-test-bucket)
  --prefix <PREFIX>          S3 key prefix (default ivm-bench-star)
  --verify                   compare the incremental result with a full rebuild
  --keep                     keep the tables when the run finishes
  --compact-every <N>        compact the MVs out of band every N rounds
                             (the time is printed but excluded from the
                             refresh timings; 0 disables, default 0)
  --out <FILE>               write the per-round CSV there
  -h, --help

Environment: LAKESOUL_PG_URL, LAKESOUL_PG_USERNAME, LAKESOUL_PG_PASSWORD and,
for S3, AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION.
"
    );
}

fn parse_args() -> Args {
    let mut args = Args::default();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < argv.len() {
        match argv[index].as_str() {
            "--rows" => args.rows = number(&value_of(&argv, &mut index)),
            "--dim-rows" => args.dim_rows = number(&value_of(&argv, &mut index)),
            "--change-ratio" => args.change_ratio = number(&value_of(&argv, &mut index)),
            "--dim-change-ratio" => {
                args.dim_change_ratio = number(&value_of(&argv, &mut index))
            }
            "--rounds" => args.rounds = number(&value_of(&argv, &mut index)),
            "--warmup" => args.warmup = number(&value_of(&argv, &mut index)),
            "--batch" => args.batch = number(&value_of(&argv, &mut index)),
            "--seed" => args.seed = number(&value_of(&argv, &mut index)),
            "--buckets" => args.buckets = number(&value_of(&argv, &mut index)),
            "--format" => {
                args.format = match value_of(&argv, &mut index).as_str() {
                    "parquet" => PhysicalFormat::Parquet,
                    "vortex" => PhysicalFormat::Vortex,
                    "vortex-compact" => PhysicalFormat::VortexCompact,
                    other => {
                        panic!(
                            "--format must be parquet, vortex or vortex-compact, not {other}"
                        )
                    }
                }
            }
            "--hot-dim" => args.hot_dim = Some(number(&value_of(&argv, &mut index))),
            "--storage" => {
                args.storage = match value_of(&argv, &mut index).as_str() {
                    "local" => Storage::Local,
                    "s3" => Storage::S3,
                    other => panic!("--storage must be local or s3, not {other}"),
                }
            }
            "--bucket" => args.bucket = value_of(&argv, &mut index),
            "--prefix" => args.prefix = value_of(&argv, &mut index),
            "--verify" => args.verify = true,
            "--keep" => args.keep = true,
            "--compact-every" => {
                args.compact_every = number(&value_of(&argv, &mut index))
            }
            "--out" => args.out = Some(value_of(&argv, &mut index)),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => panic!("unknown argument {other}; use --help"),
        }
        index += 1;
    }
    args
}

// ---------------------------------------------------------------------------
// Workload
// ---------------------------------------------------------------------------

/// A small deterministic generator, so runs are reproducible.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(6364136223846793005).wrapping_add(1))
    }
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn value(&mut self) -> i64 {
        (self.next() % 1_000) as i64
    }
    fn index(&mut self, len: usize) -> usize {
        (self.next() % len.max(1) as u64) as usize
    }
}

fn fact_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("fk1", DataType::Int64, false),
        Field::new("fk2", DataType::Int64, false),
        Field::new("fk3", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn dim_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn fact_row(
    key: i64,
    dim_rows: usize,
    hot_dim: Option<usize>,
    value: i64,
    op: &str,
) -> (i64, i64, i64, i64, i64, String) {
    let fanout = hot_dim.unwrap_or(dim_rows).max(1) as i64;
    (
        key,
        key % fanout,
        (key.wrapping_mul(7).wrapping_add(3)) % fanout,
        (key.wrapping_mul(13).wrapping_add(5)) % fanout,
        value,
        op.to_string(),
    )
}

type FactRow = (i64, i64, i64, i64, i64, String);

fn fact_batch(rows: &[FactRow]) -> RecordBatch {
    RecordBatch::try_new(
        fact_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.3))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.4))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.5.as_str()),
            )),
        ],
    )
    .unwrap()
}

fn dim_batch(rows: &[(i64, i64, i64, String)]) -> RecordBatch {
    RecordBatch::try_new(
        dim_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|r| r.3.as_str()),
            )),
        ],
    )
    .unwrap()
}

fn dim_attr(key: i64) -> (i64, i64) {
    (key % 100, (key / 100) % 100)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn peak_rss_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find(|line| line.starts_with("VmHWM:"))
                .and_then(|line| line.split_whitespace().nth(1))
                .and_then(|value| value.parse::<u64>().ok())
        })
        .map(|kb| kb / 1024)
        .unwrap_or(0)
}

async fn table_files(runtime: &IvmRuntime, table: &IvmTable) -> usize {
    let mut count = 0;
    if let Ok(partitions) = runtime
        .client()
        .get_all_partition_info(&table.table_id)
        .await
    {
        for partition in partitions {
            if let Ok(files) = runtime
                .client()
                .get_data_files_of_single_partition(&partition)
                .await
            {
                count += files.len();
            }
        }
    }
    count
}

async fn append_fact(
    runtime: &IvmRuntime,
    table: &IvmTable,
    rows: &[FactRow],
    batch: usize,
) {
    for chunk in rows.chunks(batch.max(1)) {
        table
            .append_batch(runtime.client(), fact_batch(chunk))
            .await
            .unwrap();
    }
}

async fn append_dim(
    runtime: &IvmRuntime,
    table: &IvmTable,
    rows: &[(i64, i64, i64, String)],
    batch: usize,
) {
    for chunk in rows.chunks(batch.max(1)) {
        table
            .append_batch(runtime.client(), dim_batch(chunk))
            .await
            .unwrap();
    }
}

/// A stable checksum of the aggregate MV's logical rows.
async fn agg_checksum(runtime: &IvmRuntime, mv: &IvmTable) -> (u64, i128, i128) {
    let context = SessionContext::new();
    context
        .register_table("agg", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql(&format!(
            "select a1, a2, {IVM_SUM_COLUMN}, {IVM_COUNT_COLUMN} from agg"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = 0u64;
    let mut sum = 0i128;
    let mut count = 0i128;
    for batch in &batches {
        let a1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let a2 = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows += 1;
            sum += (a1.value(index) as i128) * 1_000_003
                + (a2.value(index) as i128)
                + (s.value(index) as i128) * 31
                + (c.value(index) as i128) * 17;
            count += c.value(index) as i128;
        }
    }
    (rows, sum, count)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let args = parse_args();
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let run = format!("{suffix}_star");
    let mut _dir = None;
    let base = match args.storage {
        Storage::Local => {
            let dir = tempdir().unwrap();
            let path = format!("file://{}", dir.path().join(&run).display());
            _dir = Some(dir);
            path
        }
        Storage::S3 => format!("s3://{}/{}/{}", args.bucket, args.prefix, run),
    };

    let table = |name: &str, schema: SchemaRef, keys: Vec<String>| {
        let mut options =
            IvmTableOptions::new(name.to_string(), format!("{base}/{name}"), schema)
                .with_primary_keys(keys)
                .with_cdc_column(CHANGE_COLUMN)
                .with_file_format(args.format);
        options.hash_bucket_num = args.buckets.to_string();
        options
    };

    let fact = runtime
        .create_table(table(
            &format!("ivm_star_{suffix}_fact"),
            fact_schema(),
            vec!["k".to_string()],
        ))
        .await
        .unwrap();
    let mut dims = Vec::new();
    for index in 0..3 {
        dims.push(
            runtime
                .create_table(table(
                    &format!("ivm_star_{suffix}_dim{index}"),
                    dim_schema(),
                    vec!["k".to_string()],
                ))
                .await
                .unwrap(),
        );
    }

    // Seed the dimensions: `a` and `b` have 100 values each, so grouping by
    // two dimension attributes yields about 10k groups.
    let dim_rows = (0..args.dim_rows as i64)
        .map(|key| {
            let (a, b) = dim_attr(key);
            (key, a, b, "insert".to_string())
        })
        .collect::<Vec<_>>();
    for dim in &dims {
        append_dim(&runtime, dim, &dim_rows, args.batch).await;
    }

    // Seed the fact table and keep the live keys for updates/deletes.
    let mut live = Vec::with_capacity(args.rows);
    let mut rng = Lcg::new(args.seed);
    for start in (0..args.rows).step_by(args.batch.max(1)) {
        let end = (start + args.batch.max(1)).min(args.rows);
        let rows = (start..end)
            .map(|key| {
                let key = key as i64;
                let value = rng.value();
                fact_row(key, args.dim_rows, args.hot_dim, value, "insert")
            })
            .collect::<Vec<_>>();
        fact.append_batch(runtime.client(), fact_batch(&rows))
            .await
            .unwrap();
        for row in &rows {
            live.push(row.0);
        }
    }

    // The chain view: fact LEFT JOIN dim0 ON dim0.k = fact.fk1 ...
    let steps = (0..3)
        .map(|index| LookupChainStep {
            source: index + 1,
            left: true,
            keys: vec![format!("fk{}", index + 1)],
            right_keys: vec!["k".to_string()],
            key_sources: Vec::new(),
            unique: true,
            key_exprs: JoinKeyExprs::default(),
        })
        .collect::<Vec<_>>();
    let columns = vec![
        LookupChainColumn {
            source: 0,
            column: "k".to_string(),
            name: "k".to_string(),
        },
        LookupChainColumn {
            source: 0,
            column: "v".to_string(),
            name: "v".to_string(),
        },
        LookupChainColumn {
            source: 1,
            column: "a".to_string(),
            name: "a1".to_string(),
        },
        LookupChainColumn {
            source: 2,
            column: "a".to_string(),
            name: "a2".to_string(),
        },
        LookupChainColumn {
            source: 3,
            column: "a".to_string(),
            name: "a3".to_string(),
        },
    ];
    let mut schemas = vec![fact.schema.clone()];
    schemas.extend(dims.iter().map(|dim| dim.schema.clone()));
    let mut join_options = IvmTableOptions::new(
        format!("ivm_star_{suffix}_join"),
        format!("{base}/join"),
        lookup_chain_mv_schema_for(&schemas, &steps, &columns).unwrap(),
    )
    .with_primary_keys(vec!["k".to_string()])
    .with_file_format(args.format);
    join_options.hash_bucket_num = args.buckets.to_string();
    let join_mv = runtime.create_table(join_options).await.unwrap();
    let chain = LookupChainView::new(
        format!("star_chain_{suffix}"),
        std::iter::once((fact.clone(), None))
            .chain(dims.iter().cloned().map(|dim| (dim, None)))
            .collect(),
        steps,
        columns,
        join_mv.clone(),
    );

    let group_keys = vec!["a1".to_string(), "a2".to_string()];
    let mut agg_options = IvmTableOptions::new(
        format!("ivm_star_{suffix}_agg"),
        format!("{base}/agg"),
        sum_count_mv_schema_for(&join_mv.schema, &group_keys, Some("v")).unwrap(),
    )
    .with_primary_keys(group_keys.clone())
    .with_file_format(args.format);
    agg_options.hash_bucket_num = args.buckets.to_string();
    let agg_mv = runtime.create_table(agg_options).await.unwrap();
    let agg = SumCountView::new_with_group_keys(
        format!("star_agg_{suffix}"),
        join_mv.clone(),
        agg_mv.clone(),
        group_keys,
        Some("v".to_string()),
    );

    let started = Instant::now();
    let bootstrap_chain = Instant::now();
    runtime.refresh_lookup_chain(&chain).await.unwrap();
    let bootstrap_chain_ms = bootstrap_chain.elapsed().as_secs_f64() * 1_000.0;
    let bootstrap_agg = Instant::now();
    runtime.refresh_sum_count(&agg).await.unwrap();
    let bootstrap_agg_ms = bootstrap_agg.elapsed().as_secs_f64() * 1_000.0;
    println!(
        "bootstrapped star ({} fact rows, {} dim rows, {} buckets): join={:.0}ms agg={:.0}ms total={:.1}s peak_rss={}MB",
        args.rows,
        args.dim_rows,
        args.buckets,
        bootstrap_chain_ms,
        bootstrap_agg_ms,
        started.elapsed().as_secs_f64(),
        peak_rss_mb()
    );

    let mut csv = Vec::new();
    let mut next_key = args.rows as i64;
    for round in 0..(args.warmup + args.rounds) {
        let fact_changes = ((args.rows as f64) * args.change_ratio).round() as usize;
        let fact_changes = fact_changes.max(1);
        let mut rows: Vec<FactRow> = Vec::with_capacity(fact_changes);
        for change in 0..fact_changes {
            match change % 3 {
                0 if !live.is_empty() => {
                    // Update a live fact.
                    let index = rng.index(live.len());
                    let key = live[index];
                    let value = rng.value();
                    rows.push(fact_row(
                        key,
                        args.dim_rows,
                        args.hot_dim,
                        value,
                        "insert",
                    ));
                }
                1 if !live.is_empty() => {
                    // Delete a live fact.
                    let index = rng.index(live.len());
                    let key = live.swap_remove(index);
                    rows.push(fact_row(key, args.dim_rows, args.hot_dim, 0, "delete"));
                }
                _ => {
                    // Insert a new fact.
                    let key = next_key;
                    next_key += 1;
                    live.push(key);
                    let value = rng.value();
                    rows.push(fact_row(
                        key,
                        args.dim_rows,
                        args.hot_dim,
                        value,
                        "insert",
                    ));
                }
            }
        }

        let dim_changes =
            ((args.dim_rows as f64) * args.dim_change_ratio).round() as usize;
        let mut dim_updates: Vec<(i64, i64, i64, String)> =
            Vec::with_capacity(dim_changes);
        for _ in 0..dim_changes {
            let key = rng.index(args.dim_rows) as i64;
            let (_, b) = dim_attr(key);
            let a = rng.value() % 100;
            dim_updates.push((key, a, b, "insert".to_string()));
        }

        let write_started = Instant::now();
        append_fact(&runtime, &fact, &rows, args.batch).await;
        if !dim_updates.is_empty() {
            // Rotate the updated dimension so each step is exercised.
            append_dim(
                &runtime,
                &dims[round % dims.len()],
                &dim_updates,
                args.batch,
            )
            .await;
        }
        let write_ms = write_started.elapsed().as_secs_f64() * 1_000.0;

        let chain_started = Instant::now();
        runtime.refresh_lookup_chain(&chain).await.unwrap();
        let chain_ms = chain_started.elapsed().as_secs_f64() * 1_000.0;
        let agg_started = Instant::now();
        runtime.refresh_sum_count(&agg).await.unwrap();
        let agg_ms = agg_started.elapsed().as_secs_f64() * 1_000.0;
        let total_ms = chain_ms + agg_ms;
        let rss = peak_rss_mb();
        let measured = round >= args.warmup;
        if measured {
            println!(
                "round {round} changes={} dim_changes={} write={:.0}ms join={:.0}ms agg={:.0}ms total={:.0}ms peak_rss={}MB",
                rows.len(),
                dim_updates.len(),
                write_ms,
                chain_ms,
                agg_ms,
                total_ms,
                rss
            );
            csv.push(format!(
                "{round},{},{},{:.1},{:.1},{:.1},{:.1},{}",
                rows.len(),
                dim_updates.len(),
                write_ms,
                chain_ms,
                agg_ms,
                total_ms,
                rss
            ));
        }
        // The production compaction is external and runs between refreshes;
        // the test harness plays that role here and keeps it out of the
        // measured refresh timings.
        if args.compact_every > 0 && round > 0 && round % args.compact_every == 0 {
            let compaction_started = Instant::now();
            join_mv.compact(runtime.client()).await.unwrap();
            agg_mv.compact(runtime.client()).await.unwrap();
            eprintln!(
                "compaction after round {round}: {:.0}ms (excluded from refresh timings)",
                compaction_started.elapsed().as_secs_f64() * 1_000.0
            );
        }
    }

    let incremental_checksum = if args.verify {
        Some(agg_checksum(&runtime, &agg_mv).await)
    } else {
        None
    };

    // The full baseline: rebuild both views from the current sources.
    let full_chain_started = Instant::now();
    runtime.rebuild_lookup_chain(&chain).await.unwrap();
    let full_chain_ms = full_chain_started.elapsed().as_secs_f64() * 1_000.0;
    let full_agg_started = Instant::now();
    runtime.rebuild_sum_count(&agg).await.unwrap();
    let full_agg_ms = full_agg_started.elapsed().as_secs_f64() * 1_000.0;

    if let Some(before) = incremental_checksum {
        let after = agg_checksum(&runtime, &agg_mv).await;
        println!(
            "verification: incremental rows={} sum={} count={}; full rows={} sum={} count={}",
            before.0, before.1, before.2, after.0, after.1, after.2
        );
        assert_eq!(before, after, "the incremental and full aggregate differ");
    }

    let join_files = table_files(&runtime, &join_mv).await;
    let agg_files = table_files(&runtime, &agg_mv).await;
    let fact_files = table_files(&runtime, &fact).await;
    println!(
        "full rebuild: join={:.0}ms agg={:.0}ms total={:.1}s peak_rss={}MB",
        full_chain_ms,
        full_agg_ms,
        (full_chain_ms + full_agg_ms) / 1_000.0,
        peak_rss_mb()
    );
    println!(
        "files: fact={} join_mv={} agg_mv={}",
        fact_files, join_files, agg_files
    );

    if let Some(path) = &args.out
        && let Ok(file) = File::create(path)
    {
        let mut writer = BufWriter::new(file);
        writeln!(
            writer,
            "round,fact_changes,dim_changes,write_ms,join_ms,agg_ms,total_ms,peak_rss_mb"
        )
        .ok();
        for line in &csv {
            writeln!(writer, "{line}").ok();
        }
    }

    if args.keep {
        println!("kept the tables under {base}");
    } else {
        for table in std::iter::once(&fact)
            .chain(dims.iter())
            .chain(std::iter::once(&join_mv))
            .chain(std::iter::once(&agg_mv))
        {
            let _ = runtime
                .client()
                .drop_table(&table.table_name, &table.namespace)
                .await;
        }
    }
}
