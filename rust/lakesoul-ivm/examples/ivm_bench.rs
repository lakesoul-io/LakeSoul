// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A refresh benchmark: incremental maintenance against a full recompute of
//! the same statement.
//!
//! Every measured round appends the same change batch to the source, then
//! times two independent views:
//!
//! * the **incremental** view (`INSERT INTO <mv> SELECT ...`), whose action
//!   must be `incremental`;
//! * the **full** view (`INSERT OVERWRITE <mv> SELECT ...`), the same
//!   statement recomputed from the current source state (action `overwrite`).
//!
//! The script writes one CSV row per view and round and prints p50/p95
//! summaries, so the speedup and its scaling can be compared across table
//! sizes, change ratios and shapes.  See `BENCH.md` for the methodology.
//!
//! # Storage
//!
//! `--storage s3` (the default) expects a reachable S3 endpoint (for the local
//! dev setup, RustFS at `127.0.0.1:9000`) configured through the environment
//! the IO layer reads:
//!
//! ```text
//! export AWS_ENDPOINT=http://127.0.0.1:9000
//! export AWS_ACCESS_KEY_ID=rustfsadmin
//! export AWS_SECRET_ACCESS_KEY=rustfsadmin
//! export AWS_REGION=us-east-1
//! export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
//! export LAKESOUL_PG_USERNAME=lakesoul_test
//! export LAKESOUL_PG_PASSWORD=lakesoul_test
//! cargo run --release -p lakesoul-ivm --example ivm_bench -- \
//!     --shape both --rows 100000 --change-ratio 0.01 --rounds 15
//! ```
//!
//! The bucket must exist; the one-time creation for RustFS is documented in
//! `BENCH.md`.  `--storage local` uses a temporary directory instead.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, Result, row_mv_schema_for,
    sum_count_mv_schema_for,
};

const CHANGE_COLUMN: &str = "op";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    Row,
    SumCount,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Row => "row",
            Shape::SumCount => "sum_count",
        }
    }

    fn parse(value: &str) -> Option<Vec<Shape>> {
        match value {
            "row" => Some(vec![Shape::Row]),
            "sum_count" => Some(vec![Shape::SumCount]),
            "both" => Some(vec![Shape::Row, Shape::SumCount]),
            _ => None,
        }
    }

    fn mv_schema(self, source: &Schema) -> Result<SchemaRef> {
        match self {
            Shape::Row => row_mv_schema_for(
                source,
                &["k".to_string(), "g".to_string(), "v".to_string()],
            ),
            Shape::SumCount => {
                sum_count_mv_schema_for(source, &["g".to_string()], Some("v"))
            }
        }
    }

    fn primary_keys(self) -> Vec<String> {
        match self {
            Shape::Row => vec!["k".to_string()],
            Shape::SumCount => vec!["g".to_string()],
        }
    }

    fn select(self, source: &str) -> String {
        match self {
            Shape::Row => format!("SELECT k, g, v FROM {source} WHERE v > 5"),
            Shape::SumCount => {
                format!("SELECT g, SUM(v), COUNT(v) FROM {source} GROUP BY g")
            }
        }
    }

    /// The visible output columns, for the end-to-end comparison.
    fn output_columns(self) -> &'static str {
        match self {
            Shape::Row => "k, g, v",
            Shape::SumCount => "g, sum_v, count_v",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Storage {
    Local,
    S3,
}

struct Args {
    shapes: Vec<Shape>,
    rows: usize,
    change_ratio: Option<f64>,
    change_rows: Option<usize>,
    rounds: usize,
    warmup: usize,
    seed: u64,
    batch: usize,
    groups: usize,
    storage: Storage,
    bucket: String,
    prefix: String,
    out: Option<String>,
    verify_every: usize,
    keep: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            shapes: vec![Shape::Row, Shape::SumCount],
            rows: 100_000,
            change_ratio: Some(0.01),
            change_rows: None,
            rounds: 15,
            warmup: 2,
            seed: 42,
            batch: 8192,
            groups: 1_000,
            storage: Storage::S3,
            bucket: "lakesoul-test-bucket".to_string(),
            prefix: "ivm-bench".to_string(),
            out: None,
            verify_every: 0,
            keep: false,
        }
    }
}

impl Args {
    fn parse() -> Self {
        let mut args = Args::default();
        let mut iter = std::env::args().skip(1);
        while let Some(flag) = iter.next() {
            let mut value = || {
                iter.next().unwrap_or_else(|| {
                    panic!("{flag} needs a value");
                })
            };
            match flag.as_str() {
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                "--shape" => {
                    let parsed = Shape::parse(&value()).unwrap_or_else(|| {
                        panic!("--shape must be row, sum_count or both")
                    });
                    args.shapes = parsed;
                }
                "--rows" => args.rows = parse_num(&value()),
                "--change-ratio" => {
                    args.change_ratio = Some(parse_float(&value()));
                    args.change_rows = None;
                }
                "--change-rows" => {
                    args.change_rows = Some(parse_num(&value()));
                    args.change_ratio = None;
                }
                "--rounds" => args.rounds = parse_num(&value()),
                "--warmup" => args.warmup = parse_num(&value()),
                "--seed" => args.seed = parse_num(&value()) as u64,
                "--batch" => args.batch = parse_num(&value()),
                "--groups" => args.groups = parse_num(&value()),
                "--storage" => {
                    args.storage = match value().as_str() {
                        "local" => Storage::Local,
                        "s3" => Storage::S3,
                        other => panic!("--storage must be local or s3, got {other}"),
                    }
                }
                "--bucket" => args.bucket = value(),
                "--prefix" => args.prefix = value(),
                "--out" => args.out = Some(value()),
                "--verify-every" => args.verify_every = parse_num(&value()),
                "--keep" => args.keep = true,
                other => panic!("unknown argument {other} (use --help)"),
            }
        }
        assert!(args.rows > 0, "--rows must be positive");
        assert!(args.rounds > 0, "--rounds must be positive");
        assert!(args.batch > 0, "--batch must be positive");
        assert!(args.groups > 0, "--groups must be positive");
        args
    }

    fn change_rows(&self) -> usize {
        if let Some(rows) = self.change_rows {
            return rows.max(1);
        }
        let ratio = self.change_ratio.unwrap_or(0.01);
        ((self.rows as f64) * ratio).round().max(1.0) as usize
    }
}

fn parse_num(value: &str) -> usize {
    value
        .parse::<usize>()
        .unwrap_or_else(|_| panic!("{value} is not a number"))
}

fn parse_float(value: &str) -> f64 {
    value
        .parse::<f64>()
        .unwrap_or_else(|_| panic!("{value} is not a number"))
}

fn print_help() {
    println!(
        "\
ivm_bench: incremental refresh vs a full recompute of the same statement

Usage: ivm_bench [OPTIONS]

  --shape <row|sum_count|both>  views to measure (default both)
  --rows <N>                    source rows to seed (default 100000)
  --change-ratio <F>            changed fraction per round (default 0.01)
  --change-rows <N>             absolute changed rows (overrides the ratio)
  --rounds <N>                  measured rounds (default 15)
  --warmup <N>                  warmup rounds, not measured (default 2)
  --seed <N>                    deterministic workload seed (default 42)
  --batch <N>                   source write batch size (default 8192)
  --groups <N>                  distinct groups in the source (default 1000)
  --storage <local|s3>          table storage (default s3)
  --bucket <NAME>               S3 bucket (default lakesoul-test-bucket)
  --prefix <PREFIX>             S3 key prefix (default ivm-bench)
  --out <FILE>                  write the CSV there instead of stdout
  --verify-every <N>            compare the two views every N rounds (0: end)
  --keep                        keep the tables when the run finishes
  -h, --help

Environment: LAKESOUL_PG_URL, LAKESOUL_PG_USERNAME, LAKESOUL_PG_PASSWORD and,
for S3, AWS_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION.
"
    );
}

/// One measured round of one shape.
struct RoundResult {
    shape: &'static str,
    round: usize,
    change_rows: usize,
    source_write_ms: f64,
    incr_ms: f64,
    full_ms: f64,
    incr_action: &'static str,
    full_action: &'static str,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    warn_missing_env(&args);

    let runtime = IvmRuntime::from_env().await?;
    runtime.init_schema().await?;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let local_dir = match args.storage {
        Storage::Local => Some(tempfile::tempdir().map_err(|error| {
            rootcause::report!("creating the local temp dir: {error}")
        })?),
        Storage::S3 => None,
    };
    let root = match (&args.storage, &local_dir) {
        (Storage::Local, Some(dir)) => {
            format!("file://{}/{}", dir.path().display(), suffix)
        }
        (Storage::S3, _) => {
            format!("s3://{}/{}/{}", args.bucket, args.prefix, suffix)
        }
        (Storage::Local, None) => unreachable!("local storage needs a temp dir"),
    };

    let source_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_bench_{suffix}_src"),
                format!("{root}/src"),
                source_schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_file_format(PhysicalFormat::Parquet),
        )
        .await?;

    let mut rng = Lcg::new(args.seed);
    println!(
        "storage={} source={} rows={} groups={} change_rows={} rounds={} warmup={}",
        match args.storage {
            Storage::Local => "local",
            Storage::S3 => "s3",
        },
        source.table_name,
        args.rows,
        args.groups,
        args.change_rows(),
        args.rounds,
        args.warmup,
    );

    // Seed the source table in batches.
    let mut live: Vec<(i64, String, i64)> = Vec::with_capacity(args.rows);
    let load_start = Instant::now();
    let mut loaded = 0usize;
    let mut next_key = 1i64;
    while loaded < args.rows {
        let count = args.batch.min(args.rows - loaded);
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let key = next_key;
            next_key += 1;
            let group = format!("g{}", key % args.groups as i64);
            let value = rng.value();
            rows.push((key, group.clone(), value, "insert"));
            live.push((key, group, value));
        }
        source
            .append_batch(runtime.client(), batch(&source_schema, &rows))
            .await?;
        loaded += count;
    }
    println!("loaded {loaded} rows in {:.1} ms", ms(load_start.elapsed()));

    // Create the incremental and the full-recompute view of every shape.
    let mut views: Vec<(Shape, IvmTable, IvmTable)> = Vec::new();
    for shape in &args.shapes {
        let schema = shape.mv_schema(&source_schema)?;
        let incremental = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_bench_{suffix}_{}_inc", shape.name()),
                    format!("{root}/{}_inc", shape.name()),
                    schema.clone(),
                )
                .with_primary_keys(shape.primary_keys())
                .with_file_format(PhysicalFormat::Parquet),
            )
            .await?;
        let full = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_bench_{suffix}_{}_full", shape.name()),
                    format!("{root}/{}_full", shape.name()),
                    schema,
                )
                .with_primary_keys(shape.primary_keys())
                .with_file_format(PhysicalFormat::Parquet),
            )
            .await?;
        views.push((*shape, incremental, full));
    }

    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();

    // Bootstrap both views from the current source state.
    for (shape, incremental, full) in &views {
        let select = shape.select(&source.table_name);
        let bootstrap = executor
            .execute(&format!("INSERT INTO {} {select}", incremental.table_name))
            .await?;
        assert_eq!(bootstrap.action.label(), "bootstrap", "{select}");
        let bootstrap_full = executor
            .execute(&format!("INSERT INTO {} {select}", full.table_name))
            .await?;
        assert_eq!(bootstrap_full.action.label(), "bootstrap", "{select}");
        println!(
            "bootstrapped {} (epochs {} / {})",
            shape.name(),
            bootstrap.epoch.unwrap_or_default(),
            bootstrap_full.epoch.unwrap_or_default()
        );
    }

    let change_rows = args.change_rows();
    let mut results: Vec<RoundResult> = Vec::new();
    let total_rounds = args.warmup + args.rounds;
    for round in 0..total_rounds {
        let changes =
            mutate(&mut live, &mut next_key, change_rows, &mut rng, args.groups);
        let write_start = Instant::now();
        source
            .append_batch(runtime.client(), batch(&source_schema, &changes))
            .await?;
        let source_write_ms = ms(write_start.elapsed());

        for (shape, incremental, full) in &views {
            let select = shape.select(&source.table_name);
            let incr_start = Instant::now();
            let incremental_execution = executor
                .execute(&format!("INSERT INTO {} {select}", incremental.table_name))
                .await?;
            let incr_ms = ms(incr_start.elapsed());
            let full_start = Instant::now();
            let full_execution = executor
                .execute(&format!("INSERT OVERWRITE {} {select}", full.table_name))
                .await?;
            let full_ms = ms(full_start.elapsed());
            assert_eq!(
                incremental_execution.action.label(),
                "incremental",
                "round {round} {} did not refresh incrementally",
                shape.name()
            );
            assert_eq!(
                full_execution.action.label(),
                "overwrite",
                "round {round} {} did not recompute in full",
                shape.name()
            );
            if round >= args.warmup {
                results.push(RoundResult {
                    shape: shape.name(),
                    round,
                    change_rows: changes.len(),
                    source_write_ms,
                    incr_ms,
                    full_ms,
                    incr_action: incremental_execution.action.label(),
                    full_action: full_execution.action.label(),
                });
            }
        }

        let verify = args.verify_every > 0 && (round + 1) % args.verify_every == 0
            || (args.verify_every == 0 && round + 1 == total_rounds);
        if verify {
            verify_views(runtime, &views).await?;
        }
        if round < args.warmup {
            continue;
        }
        for (shape, _, _) in &views {
            let Some(result) = results
                .iter()
                .find(|result| result.round == round && result.shape == shape.name())
            else {
                continue;
            };
            println!(
                "round {} shape={} change_rows={} incr={} full={} speedup={:.1}x",
                round,
                shape.name(),
                result.change_rows,
                format_ms(result.incr_ms),
                format_ms(result.full_ms),
                speedup(result.incr_ms, result.full_ms),
            );
        }
    }

    write_csv(&args, &results)?;
    summarize(&args, &results);
    if !args.keep {
        for (_, incremental, full) in &views {
            runtime
                .client()
                .drop_table(&incremental.table_name, &incremental.namespace)
                .await
                .ok();
            runtime
                .client()
                .drop_table(&full.table_name, &full.namespace)
                .await
                .ok();
        }
        runtime
            .client()
            .drop_table(&source.table_name, &source.namespace)
            .await
            .ok();
        println!("dropped the benchmark tables (pass --keep to keep them)");
    }
    Ok(())
}

fn warn_missing_env(args: &Args) {
    if matches!(args.storage, Storage::S3) {
        for key in ["AWS_ENDPOINT", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"] {
            if std::env::var(key).is_err() {
                eprintln!(
                    "warning: {key} is not set; S3 storage needs the endpoint and \
                     credentials the IO layer reads (see BENCH.md)"
                );
            }
        }
    }
}

fn speedup(incr_ms: f64, full_ms: f64) -> f64 {
    if incr_ms <= 0.0 {
        f64::INFINITY
    } else {
        full_ms / incr_ms
    }
}

fn format_ms(value: f64) -> String {
    format!("{value:.1}ms")
}

fn write_csv(args: &Args, results: &[RoundResult]) -> Result<()> {
    let mut csv = String::from(
        "shape,round,change_rows,source_write_ms,incr_ms,full_ms,speedup,\
         incr_action,full_action\n",
    );
    for result in results {
        csv.push_str(&format!(
            "{},{},{},{:.3},{:.3},{:.3},{:.2},{},{}\n",
            result.shape,
            result.round,
            result.change_rows,
            result.source_write_ms,
            result.incr_ms,
            result.full_ms,
            speedup(result.incr_ms, result.full_ms),
            result.incr_action,
            result.full_action,
        ));
    }
    match &args.out {
        Some(path) => {
            std::fs::write(path, csv)
                .map_err(|error| rootcause::report!("writing {path}: {error}"))?;
            println!("csv written to {path}");
        }
        None => print!("{csv}"),
    }
    Ok(())
}

fn summarize(args: &Args, results: &[RoundResult]) {
    println!();
    println!(
        "shape      rows      groups    change    incr_p50   full_p50   speedup  \
         incr_p95   full_p95"
    );
    for shape in &args.shapes {
        let incr = percentile(
            &results
                .iter()
                .filter(|result| result.shape == shape.name())
                .map(|result| result.incr_ms)
                .collect::<Vec<_>>(),
            0.50,
        );
        let full = percentile(
            &results
                .iter()
                .filter(|result| result.shape == shape.name())
                .map(|result| result.full_ms)
                .collect::<Vec<_>>(),
            0.50,
        );
        let incr_p95 = percentile(
            &results
                .iter()
                .filter(|result| result.shape == shape.name())
                .map(|result| result.incr_ms)
                .collect::<Vec<_>>(),
            0.95,
        );
        let full_p95 = percentile(
            &results
                .iter()
                .filter(|result| result.shape == shape.name())
                .map(|result| result.full_ms)
                .collect::<Vec<_>>(),
            0.95,
        );
        println!(
            "{:<10} {:<9} {:<9} {:<9} {:<10} {:<10} {:<8} {:<10} {:<10}",
            shape.name(),
            args.rows,
            args.groups,
            args.change_rows(),
            format_ms(incr),
            format_ms(full),
            format!("{:.1}x", speedup(incr, full)),
            format_ms(incr_p95),
            format_ms(full_p95),
        );
    }
}

fn percentile(values: &[f64], percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let index = ((sorted.len() as f64) * percentile).ceil() as usize;
    sorted[index.saturating_sub(1).min(sorted.len() - 1)]
}

async fn verify_views(
    runtime: &IvmRuntime,
    views: &[(Shape, IvmTable, IvmTable)],
) -> Result<()> {
    for (shape, incremental, full) in views {
        // A fresh session per pass, so the registrations do not collide.
        let context = SessionContext::new();
        let incremental_rows =
            read_visible(runtime, &context, incremental, shape).await?;
        let full_rows = read_visible(runtime, &context, full, shape).await?;
        assert_eq!(
            incremental_rows,
            full_rows,
            "{}: the incremental and full views differ",
            shape.name()
        );
    }
    Ok(())
}

async fn read_visible(
    runtime: &IvmRuntime,
    context: &SessionContext,
    table: &IvmTable,
    shape: &Shape,
) -> Result<Vec<Vec<String>>> {
    let provider = runtime.table_provider(table);
    context
        .register_table(table.table_name.as_str(), Arc::new(provider))
        .map_err(|error| {
            rootcause::report!("registering {}: {error}", table.table_name)
        })?;
    let frame = context
        .sql(&format!(
            "SELECT {} FROM {}",
            shape.output_columns(),
            table.table_name
        ))
        .await
        .map_err(|error| rootcause::report!("reading {}: {error}", table.table_name))?;
    let batches = frame
        .collect()
        .await
        .map_err(|error| rootcause::report!("reading {}: {error}", table.table_name))?;
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for column in batch.columns() {
                values.push(
                    arrow::util::display::array_value_to_string(column, row).map_err(
                        |error| rootcause::report!("displaying a row: {error}"),
                    )?,
                );
            }
            rows.push(values);
        }
    }
    rows.sort();
    Ok(rows)
}

/// The source rows: `(k, g, v, change marker)`.
fn batch(schema: &SchemaRef, rows: &[(i64, String, i64, &str)]) -> RecordBatch {
    let key = Int64Array::from(rows.iter().map(|row| row.0).collect::<Vec<_>>());
    let group =
        StringArray::from(rows.iter().map(|row| row.1.clone()).collect::<Vec<_>>());
    let value = Int64Array::from(rows.iter().map(|row| row.2).collect::<Vec<_>>());
    let change = StringArray::from(rows.iter().map(|row| row.3).collect::<Vec<_>>());
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(key),
            Arc::new(group),
            Arc::new(value),
            Arc::new(change),
        ],
    )
    .expect("source batch")
}

/// One round of changes: inserts, updates and deletes in equal parts.
fn mutate(
    live: &mut Vec<(i64, String, i64)>,
    next_key: &mut i64,
    change_rows: usize,
    rng: &mut Lcg,
    groups: usize,
) -> Vec<(i64, String, i64, &'static str)> {
    let mut rows = Vec::with_capacity(change_rows);
    for index in 0..change_rows {
        let operation = if live.is_empty() { 0 } else { index % 3 };
        match operation {
            0 => {
                let key = *next_key;
                *next_key += 1;
                let group = format!("g{}", key % groups as i64);
                let value = rng.value();
                live.push((key, group.clone(), value));
                rows.push((key, group, value, "insert"));
            }
            1 => {
                let position = rng.index(live.len());
                let (key, group, _) = live[position].clone();
                let value = rng.value();
                live[position].2 = value;
                rows.push((key, group, value, "insert"));
            }
            _ => {
                let position = rng.index(live.len());
                let (key, group, value) = live.swap_remove(position);
                rows.push((key, group, value, "delete"));
            }
        }
    }
    rows
}

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

    /// A random index below `len` (`len` is non-empty in every caller).
    fn index(&mut self, len: usize) -> usize {
        (self.next() % len.max(1) as u64) as usize
    }
}

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}
