// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A refresh benchmark: incremental maintenance against a full recompute of
//! the same statement.
//!
//! Every measured round appends the same change batch to the sources, then
//! times two independent views:
//!
//! * the **incremental** view (`INSERT INTO <mv> SELECT ...`), whose action
//!   must be `incremental`;
//! * the **full** view (`INSERT OVERWRITE <mv> SELECT ...`), the same
//!   statement recomputed from the current source state (action `overwrite`).
//!
//! The script writes one CSV row per view and round and prints p50/p95
//! summaries plus the refresh-scoped IO counters (files read, rows scanned,
//! rows written, bytes written), so the speedup and its scaling can be
//! compared across table sizes, change ratios and shapes.  See `BENCH.md` for
//! the methodology.
//!
//! Shapes: `row`, `sum_count`, `lookup` (a left join to a keyed dimension),
//! `multi_join` (a three-way inner chain) and `top_k` (a partitioned
//! `row_number`).  `both` runs the first two and `all` runs every shape,
//! each with its own tables.
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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, JoinKeyExprs, MultiJoinColumn,
    Result, lookup_join_view_schema_for, multi_join_mv_schema_for,
    multi_join_primary_keys, row_mv_schema_for, sum_count_mv_schema_for,
    top_k_mv_schema_for,
};
use metrics::{
    Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit,
};

const CHANGE_COLUMN: &str = "op";

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    Row,
    SumCount,
    Lookup,
    MultiJoin,
    TopK,
    Chain,
    Partitioned,
    Skew,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Row => "row",
            Shape::SumCount => "sum_count",
            Shape::Lookup => "lookup",
            Shape::MultiJoin => "multi_join",
            Shape::TopK => "top_k",
            Shape::Chain => "chain",
            Shape::Partitioned => "partitioned",
            Shape::Skew => "skew",
        }
    }

    fn parse(value: &str) -> Option<Vec<Shape>> {
        match value {
            "row" => Some(vec![Shape::Row]),
            "sum_count" => Some(vec![Shape::SumCount]),
            "lookup" => Some(vec![Shape::Lookup]),
            "multi_join" => Some(vec![Shape::MultiJoin]),
            "top_k" => Some(vec![Shape::TopK]),
            "chain" => Some(vec![Shape::Chain]),
            "partitioned" => Some(vec![Shape::Partitioned]),
            "skew" => Some(vec![Shape::Skew]),
            "both" => Some(vec![Shape::Row, Shape::SumCount]),
            "all" => Some(vec![
                Shape::Row,
                Shape::SumCount,
                Shape::Lookup,
                Shape::MultiJoin,
                Shape::TopK,
                Shape::Chain,
                Shape::Partitioned,
                Shape::Skew,
            ]),
            _ => None,
        }
    }

    /// The source tables the shape reads, in chain order.
    fn source_count(self) -> usize {
        match self {
            Shape::Row
            | Shape::SumCount
            | Shape::TopK
            | Shape::Partitioned
            | Shape::Skew => 1,
            Shape::Lookup | Shape::Chain => 2,
            Shape::MultiJoin => 3,
        }
    }

    /// The materialized-view steps: the chain refreshes a lookup view and then
    /// a row view over it; every other shape is one view.
    fn steps(self) -> usize {
        match self {
            Shape::Chain => 2,
            _ => 1,
        }
    }

    /// The range partition columns of the view (and of the source).
    fn range_partitions(self) -> &'static [&'static str] {
        match self {
            Shape::Partitioned => &["g"],
            _ => &[],
        }
    }

    /// The MV layout of one step.  `previous` is the schema of the step the
    /// chain view reads.
    fn mv_schema(
        self,
        schemas: &[SchemaRef],
        step: usize,
        previous: Option<&SchemaRef>,
    ) -> Result<SchemaRef> {
        if step == 1 {
            let previous =
                previous.expect("a chain step needs the previous step's schema");
            return row_mv_schema_for(
                previous,
                &[
                    "k".to_string(),
                    "left_value".to_string(),
                    "right_value".to_string(),
                ],
            );
        }
        let schema = &schemas[0];
        match self {
            Shape::Row => row_mv_schema_for(
                schema,
                &["k".to_string(), "g".to_string(), "v".to_string()],
            ),
            Shape::SumCount | Shape::Partitioned | Shape::Skew => {
                sum_count_mv_schema_for(schema, &["g".to_string()], Some("v"))
            }
            Shape::Lookup | Shape::Chain => lookup_join_view_schema_for(
                schema,
                &schemas[1],
                &["k".to_string()],
                &["k".to_string()],
                &["k".to_string()],
                &JoinKeyExprs::default(),
                "v",
                "v",
            ),
            Shape::MultiJoin => multi_join_mv_schema_for(
                schemas,
                &vec![vec!["k".to_string()]; 3],
                &Shape::multi_join_columns(),
            ),
            Shape::TopK => top_k_mv_schema_for(
                schema,
                &["k".to_string()],
                &["k".to_string(), "g".to_string(), "v".to_string()],
            ),
        }
    }

    fn mv_primary_keys(self, step: usize) -> Vec<String> {
        if step == 1 {
            return vec!["k".to_string()];
        }
        match self {
            Shape::Row | Shape::Lookup | Shape::Chain | Shape::TopK => {
                vec!["k".to_string()]
            }
            Shape::SumCount | Shape::Partitioned | Shape::Skew => {
                vec!["g".to_string()]
            }
            Shape::MultiJoin => multi_join_primary_keys(&vec![vec!["k".to_string()]; 3]),
        }
    }

    fn multi_join_columns() -> Vec<MultiJoinColumn> {
        vec![
            MultiJoinColumn {
                source: 0,
                column: "k".to_string(),
                name: "k".to_string(),
            },
            MultiJoinColumn {
                source: 0,
                column: "g".to_string(),
                name: "ag".to_string(),
            },
            MultiJoinColumn {
                source: 1,
                column: "v".to_string(),
                name: "bv".to_string(),
            },
            MultiJoinColumn {
                source: 2,
                column: "v".to_string(),
                name: "cv".to_string(),
            },
        ]
    }

    /// The SELECT producing one step, over the source table names (step 0) or
    /// the previous step's view (step 1).
    fn select(self, step: usize, names: &[String]) -> String {
        if step == 1 {
            return format!("SELECT k, left_value, right_value FROM {}", names[0]);
        }
        match self {
            Shape::Row => format!("SELECT k, g, v FROM {} WHERE v > 5", names[0]),
            Shape::SumCount | Shape::Partitioned | Shape::Skew => {
                format!("SELECT g, SUM(v), COUNT(v) FROM {} GROUP BY g", names[0])
            }
            Shape::Lookup | Shape::Chain => format!(
                "SELECT a.k, a.v, b.v FROM {} a LEFT JOIN {} b ON b.k = a.k",
                names[0], names[1]
            ),
            Shape::MultiJoin => format!(
                "SELECT a.k, a.g AS ag, b.v AS bv, c.v AS cv FROM {} a \
                 JOIN {} b ON a.k = b.k JOIN {} c ON b.k = c.k",
                names[0], names[1], names[2]
            ),
            Shape::TopK => format!(
                "SELECT k, g, v FROM (SELECT k, g, v, \
                 row_number() OVER (PARTITION BY g ORDER BY v DESC) AS rn \
                 FROM {}) t WHERE rn <= 5",
                names[0]
            ),
        }
    }

    /// The visible MV output columns of the final step.
    fn output_columns(self) -> &'static str {
        match self {
            Shape::Row | Shape::TopK => "k, g, v",
            Shape::SumCount | Shape::Partitioned | Shape::Skew => "g, sum_v, count_v",
            Shape::Lookup | Shape::Chain => "k, left_value, right_value",
            Shape::MultiJoin => "k, ag, bv, cv, \"__pk0_k\", \"__pk1_k\", \"__pk2_k\"",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Storage {
    Local,
    S3,
}

#[derive(Clone, Copy)]
struct Mix {
    insert: usize,
    update: usize,
    delete: usize,
}

impl Default for Mix {
    fn default() -> Self {
        Self {
            insert: 1,
            update: 1,
            delete: 1,
        }
    }
}

impl Mix {
    fn parse(value: &str) -> Self {
        let parts = value
            .split(',')
            .map(|part| {
                part.trim().parse::<usize>().unwrap_or_else(|_| {
                    panic!("--mix wants insert,update,delete: {value}")
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(parts.len(), 3, "--mix wants insert,update,delete");
        assert!(
            parts.iter().any(|weight| *weight > 0),
            "--mix needs a positive weight"
        );
        Self {
            insert: parts[0],
            update: parts[1],
            delete: parts[2],
        }
    }

    fn pick(&self, roll: usize) -> Operation {
        let total = self.insert + self.update + self.delete;
        let slot = roll % total;
        if slot < self.insert {
            Operation::Insert
        } else if slot < self.insert + self.update {
            Operation::Update
        } else {
            Operation::Delete
        }
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Insert,
    Update,
    Delete,
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
    dim_rows: Option<usize>,
    mix: Mix,
    /// The fraction of mutations aimed at the non-fact side(s).
    secondary_change: f64,
    partitions: usize,
    hot_partition: usize,
    skew: f64,
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
            dim_rows: None,
            mix: Mix::default(),
            secondary_change: 0.2,
            partitions: 10,
            hot_partition: 0,
            skew: 0.8,
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
                        panic!("--shape must be row, sum_count, lookup, multi_join, top_k, both or all")
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
                "--dim-rows" => args.dim_rows = Some(parse_num(&value())),
                "--mix" => args.mix = Mix::parse(&value()),
                "--secondary-change" => {
                    args.secondary_change = parse_float(&value()).clamp(0.0, 1.0)
                }
                "--partitions" => {
                    args.partitions = parse_num(&value()).max(1);
                }
                "--hot-partition" => args.hot_partition = parse_num(&value()),
                "--skew" => args.skew = parse_float(&value()).clamp(0.0, 1.0),
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

    fn dim_rows(&self) -> usize {
        self.dim_rows.unwrap_or_else(|| (self.rows / 2).max(1))
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

  --shape <SHAPE>               row | sum_count | lookup | multi_join | top_k |
                                both (row+sum_count) | all (default both)
  --rows <N>                    fact rows to seed (default 100000)
  --change-ratio <F>            changed fraction per round (default 0.01)
  --change-rows <N>             absolute changed rows (overrides the ratio)
  --rounds <N>                  measured rounds (default 15)
  --warmup <N>                  warmup rounds, not measured (default 2)
  --seed <N>                    deterministic workload seed (default 42)
  --batch <N>                   source write batch size (default 8192)
  --groups <N>                  distinct groups in the fact (default 1000)
  --dim-rows <N>                dimension rows for lookup (default rows/2)
  --mix <I,U,D>                 insert,update,delete weights (default 1,1,1)
  --secondary-change <F>        mutations aimed at the non-fact side (default 0.2)
  --partitions <N>              range partitions for the partitioned shape (default 10)
  --hot-partition <N>           the partition all partitioned changes hit (default 0)
  --skew <F>                    hot-group share for the skew shape (default 0.8)
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
    incr: IoDelta,
    full: IoDelta,
}

/// The IO counters a refresh moved, read from the installed recorder.
#[derive(Clone, Copy, Default)]
struct IoDelta {
    files_read: u64,
    rows_scanned: u64,
    rows_written: u64,
    mv_bytes_written: u64,
    object_store_bytes: u64,
}

/// A row of a source table (`k`, `g`, `v`).
#[derive(Clone)]
struct LiveRow {
    k: i64,
    g: String,
    v: i64,
}

/// One per-round mutation of one source.
struct Mutation {
    source: usize,
    row: (i64, String, i64, &'static str),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    warn_missing_env(&args);
    // Capture the IO counters the refresh engine and the object store emit, so
    // every round reports what it actually read and wrote.
    let recorder = Arc::new(BenchMetrics::default());
    let _ = metrics::set_global_recorder(BenchRecorder {
        state: recorder.clone(),
    });

    let local_dir = match args.storage {
        Storage::Local => Some(tempfile::tempdir().map_err(|error| {
            rootcause::report!("creating the local temp dir: {error}")
        })?),
        Storage::S3 => None,
    };

    let mut results = Vec::new();
    for shape in &args.shapes {
        let mut shape_results =
            run_shape(*shape, &args, &recorder, local_dir.as_ref()).await?;
        results.append(&mut shape_results);
    }

    write_csv(&args, &results)?;
    summarize(&args, &results);
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

/// Run one shape end to end and return its measured rounds.
async fn run_shape(
    shape: Shape,
    args: &Args,
    recorder: &Arc<BenchMetrics>,
    local_dir: Option<&tempfile::TempDir>,
) -> Result<Vec<RoundResult>> {
    let runtime = IvmRuntime::from_env().await?;
    runtime.init_schema().await?;
    let suffix = format!(
        "{}_{}",
        uuid::Uuid::new_v4().simple(),
        shape.name().replace('_', "")
    );
    let root = match (&args.storage, local_dir) {
        (Storage::Local, Some(dir)) => {
            format!("file://{}/{}", dir.path().display(), suffix)
        }
        (Storage::S3, _) => format!("s3://{}/{}/{}", args.bucket, args.prefix, suffix),
        (Storage::Local, None) => unreachable!("local storage needs a temp dir"),
    };

    let source_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let source_count = shape.source_count();
    let range_partitions = shape
        .range_partitions()
        .iter()
        .map(|column| column.to_string())
        .collect::<Vec<_>>();
    let mut schemas = Vec::with_capacity(source_count);
    let mut tables = Vec::with_capacity(source_count);
    let mut lives: Vec<Vec<LiveRow>> = vec![Vec::new(); source_count];
    let mut next_keys = vec![1i64; source_count];
    let mut rng = Lcg::new(args.seed ^ shape.name().len() as u64);
    let load_start = Instant::now();
    for source in 0..source_count {
        let name = format!("ivm_bench_{suffix}_s{source}");
        let mut options = IvmTableOptions::new(
            name,
            format!("{root}/s{source}"),
            source_schema.clone(),
        )
        .with_primary_keys(vec!["k".to_string()])
        .with_cdc_column(CHANGE_COLUMN)
        .with_file_format(PhysicalFormat::Parquet);
        if !range_partitions.is_empty() {
            options = options.with_range_partition_columns(range_partitions.clone());
        }
        let table = runtime.create_table(options).await?;
        let rows = seed_rows(shape, source, args);
        let mut batch_rows = Vec::with_capacity(rows.min(args.batch));
        let mut written = 0;
        let mut key = 1i64;
        while written < rows {
            let count = args.batch.min(rows - written);
            for _ in 0..count {
                let group = initial_group(shape, key, args, &mut rng);
                let value = rng.value();
                batch_rows.push((key, group.clone(), value, "insert"));
                lives[source].push(LiveRow {
                    k: key,
                    g: group,
                    v: value,
                });
                key += 1;
            }
            table
                .append_batch(runtime.client(), batch(&source_schema, &batch_rows))
                .await?;
            batch_rows.clear();
            written += count;
        }
        next_keys[source] = key;
        schemas.push(source_schema.clone());
        tables.push(table);
    }
    println!(
        "shape={} storage={} rows={} groups={} change_rows={} rounds={} warmup={} \
         loaded_in={:.1}ms",
        shape.name(),
        match args.storage {
            Storage::Local => "local",
            Storage::S3 => "s3",
        },
        args.rows,
        args.groups,
        args.change_rows(),
        args.rounds,
        args.warmup,
        ms(load_start.elapsed()),
    );

    // One MV layout per step: step 0 reads the sources, a chain's step 1 reads
    // step 0's view.
    let mut step_schemas: Vec<SchemaRef> = Vec::new();
    for step in 0..shape.steps() {
        let schema = shape.mv_schema(&schemas, step, step_schemas.last())?;
        step_schemas.push(schema);
    }
    let mut incremental_views: Vec<IvmTable> = Vec::new();
    let mut full_views: Vec<IvmTable> = Vec::new();
    for (step, schema) in step_schemas.iter().enumerate() {
        let tag = if shape.steps() == 1 {
            String::new()
        } else {
            format!("_s{step}")
        };
        for (side, suffix_name) in [("mv", "inc"), ("full", "full")] {
            let mut options = IvmTableOptions::new(
                format!("ivm_bench_{suffix}_{suffix_name}{tag}"),
                format!("{root}/{suffix_name}{tag}"),
                schema.clone(),
            )
            .with_primary_keys(shape.mv_primary_keys(step))
            .with_file_format(PhysicalFormat::Parquet);
            if !range_partitions.is_empty() {
                options = options.with_range_partition_columns(range_partitions.clone());
            }
            let table = runtime.create_table(options).await?;
            match side {
                "mv" => incremental_views.push(table),
                _ => full_views.push(table),
            }
        }
    }

    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let source_names = tables
        .iter()
        .map(|table| table.table_name.clone())
        .collect::<Vec<_>>();
    let mut incremental_selects = Vec::with_capacity(shape.steps());
    let mut full_selects = Vec::with_capacity(shape.steps());
    for step in 0..shape.steps() {
        let incremental_names = if step == 0 {
            source_names.clone()
        } else {
            vec![incremental_views[step - 1].table_name.clone()]
        };
        let full_names = if step == 0 {
            source_names.clone()
        } else {
            vec![full_views[step - 1].table_name.clone()]
        };
        incremental_selects.push(shape.select(step, &incremental_names));
        full_selects.push(shape.select(step, &full_names));
    }

    // Bootstrap both chains step by step.
    for step in 0..shape.steps() {
        let incremental = executor
            .execute(&format!(
                "INSERT INTO {} {}",
                incremental_views[step].table_name, incremental_selects[step]
            ))
            .await?;
        assert_eq!(incremental.action.label(), "bootstrap");
        let full = executor
            .execute(&format!(
                "INSERT INTO {} {}",
                full_views[step].table_name, full_selects[step]
            ))
            .await?;
        assert_eq!(full.action.label(), "bootstrap");
    }
    println!("bootstrapped {} ({} step(s))", shape.name(), shape.steps());

    let change_rows = args.change_rows();
    let mut results: Vec<RoundResult> = Vec::new();
    let total_rounds = args.warmup + args.rounds;
    for round in 0..total_rounds {
        let mutations = plan_mutations(
            shape,
            &mut lives,
            &mut next_keys,
            change_rows,
            args,
            &mut rng,
        );
        let write_start = Instant::now();
        let mut per_source: Vec<Vec<(i64, String, i64, &'static str)>> =
            vec![Vec::new(); source_count];
        let mut applied = 0usize;
        for mutation in mutations {
            per_source[mutation.source].push(mutation.row);
        }
        for (source, rows) in per_source.iter_mut().enumerate() {
            if rows.is_empty() {
                continue;
            }
            tables[source]
                .append_batch(runtime.client(), batch(&source_schema, rows))
                .await?;
            applied += rows.len();
        }
        let source_write_ms = ms(write_start.elapsed());

        let before = recorder.snapshot();
        let incr_start = Instant::now();
        for step in 0..shape.steps() {
            let execution = executor
                .execute(&format!(
                    "INSERT INTO {} {}",
                    incremental_views[step].table_name, incremental_selects[step]
                ))
                .await?;
            assert_eq!(
                execution.action.label(),
                "incremental",
                "round {round} {} step {step} did not refresh incrementally",
                shape.name()
            );
        }
        let incr_ms = ms(incr_start.elapsed());
        let incr_io = io_delta(&before, &recorder.snapshot());
        let before = recorder.snapshot();
        let full_start = Instant::now();
        for step in 0..shape.steps() {
            let execution = executor
                .execute(&format!(
                    "INSERT OVERWRITE {} {}",
                    full_views[step].table_name, full_selects[step]
                ))
                .await?;
            assert_eq!(
                execution.action.label(),
                "overwrite",
                "round {round} {} step {step} did not recompute in full",
                shape.name()
            );
        }
        let full_ms = ms(full_start.elapsed());
        let full_io = io_delta(&before, &recorder.snapshot());
        if round >= args.warmup {
            results.push(RoundResult {
                shape: shape.name(),
                round,
                change_rows: applied,
                source_write_ms,
                incr_ms,
                full_ms,
                incr_action: "incremental",
                full_action: "overwrite",
                incr: incr_io,
                full: full_io,
            });
        }

        let verify = args.verify_every > 0 && (round + 1) % args.verify_every == 0
            || (args.verify_every == 0 && round + 1 == total_rounds);
        if verify {
            let last = shape.steps() - 1;
            verify_views(runtime, shape, &incremental_views[last], &full_views[last])
                .await?;
        }
        if round >= args.warmup
            && let Some(result) = results.iter().find(|result| result.round == round)
        {
            println!(
                "round {} shape={} change_rows={} incr={} full={} speedup={:.1}x \
                 incr_rows={} incr_written={}",
                round,
                shape.name(),
                result.change_rows,
                format_ms(result.incr_ms),
                format_ms(result.full_ms),
                speedup(result.incr_ms, result.full_ms),
                result.incr.rows_scanned,
                result.incr.rows_written,
            );
        }
    }

    if !args.keep {
        for table in &tables {
            drop_table(runtime, table).await;
        }
        for table in incremental_views.iter().chain(full_views.iter()) {
            drop_table(runtime, table).await;
        }
    } else {
        println!("kept the tables under {root}");
    }
    Ok(results)
}

/// The initial group of a seeded row.
fn initial_group(shape: Shape, key: i64, args: &Args, rng: &mut Lcg) -> String {
    match shape {
        Shape::Partitioned => format!("g{}", key % args.partitions as i64),
        Shape::Skew => {
            if rng.fraction() < args.skew {
                "g_hot".to_string()
            } else {
                format!("g{}", rng.index(args.groups))
            }
        }
        _ => format!("g{}", key % args.groups as i64),
    }
}

/// The group a newly inserted row takes.
fn insert_group(shape: Shape, key: i64, args: &Args, rng: &mut Lcg) -> String {
    match shape {
        Shape::Partitioned => {
            format!("g{}", args.hot_partition % args.partitions)
        }
        Shape::Skew => {
            if rng.fraction() < args.skew {
                "g_hot".to_string()
            } else {
                format!("g{}", rng.index(args.groups))
            }
        }
        _ => format!("g{}", key % args.groups as i64),
    }
}

/// The live row an update/delete targets.  The partitioned shape keeps every
/// change in the hot partition.
fn pick_live(shape: Shape, live: &[LiveRow], args: &Args, rng: &mut Lcg) -> usize {
    if shape == Shape::Partitioned {
        let hot = format!("g{}", args.hot_partition % args.partitions);
        for _ in 0..32 {
            let position = rng.index(live.len());
            if live[position].g == hot {
                return position;
            }
        }
    }
    rng.index(live.len())
}

async fn drop_table(runtime: &IvmRuntime, table: &IvmTable) {
    runtime
        .client()
        .drop_table(&table.table_name, &table.namespace)
        .await
        .ok();
}

/// The number of rows seeded into one source of a shape.
fn seed_rows(shape: Shape, source: usize, args: &Args) -> usize {
    if shape == Shape::Lookup && source == 1 {
        args.dim_rows()
    } else {
        args.rows
    }
}

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

/// Plan one round of mutations over the sources.
///
/// Inserts into a multi-join replicate to every source (a new key only joins
/// when all sides have it); the other shapes insert into one source.
fn plan_mutations(
    shape: Shape,
    lives: &mut [Vec<LiveRow>],
    next_keys: &mut [i64],
    change_rows: usize,
    args: &Args,
    rng: &mut Lcg,
) -> Vec<Mutation> {
    let mut mutations = Vec::with_capacity(change_rows);
    for index in 0..change_rows {
        let target = if lives.len() == 1 {
            0
        } else if rng.fraction() < args.secondary_change {
            let secondary = lives.len() - 1;
            1 + rng.index(secondary)
        } else {
            0
        };
        let operation = args.mix.pick(rng.index(1000) + index);
        let operation = match (shape, target, operation) {
            // A dimension row with a new key never matches the fact side.
            (Shape::Lookup, 1, Operation::Insert) => Operation::Update,
            (_, _, Operation::Insert) if shape == Shape::MultiJoin => {
                // Replicate the new key to every source.
                let key = next_keys[0];
                for source in 0..lives.len() {
                    next_keys[source] += 1;
                    let value = rng.value();
                    let group = insert_group(shape, key, args, rng);
                    lives[source].push(LiveRow {
                        k: key,
                        g: group.clone(),
                        v: value,
                    });
                    mutations.push(Mutation {
                        source,
                        row: (key, group, value, "insert"),
                    });
                }
                continue;
            }
            (_, _, Operation::Insert) => Operation::Insert,
            (_, _, Operation::Update) if lives[target].is_empty() => Operation::Insert,
            (_, _, Operation::Delete) if lives[target].is_empty() => Operation::Insert,
            (_, _, operation) => operation,
        };
        match operation {
            Operation::Insert => {
                let key = next_keys[target];
                next_keys[target] += 1;
                let group = insert_group(shape, key, args, rng);
                let value = rng.value();
                lives[target].push(LiveRow {
                    k: key,
                    g: group.clone(),
                    v: value,
                });
                mutations.push(Mutation {
                    source: target,
                    row: (key, group, value, "insert"),
                });
            }
            Operation::Update => {
                let position = pick_live(shape, &lives[target], args, rng);
                let value = rng.value();
                lives[target][position].v = value;
                let live = &lives[target][position];
                mutations.push(Mutation {
                    source: target,
                    row: (live.k, live.g.clone(), value, "insert"),
                });
            }
            Operation::Delete => {
                let position = pick_live(shape, &lives[target], args, rng);
                let live = lives[target].swap_remove(position);
                mutations.push(Mutation {
                    source: target,
                    row: (live.k, live.g, live.v, "delete"),
                });
            }
        }
    }
    mutations
}

async fn verify_views(
    runtime: &IvmRuntime,
    shape: Shape,
    incremental: &IvmTable,
    full: &IvmTable,
) -> Result<()> {
    let context = SessionContext::new();
    let incremental_rows = read_visible(runtime, &context, incremental, shape).await?;
    let full_rows = read_visible(runtime, &context, full, shape).await?;
    assert_eq!(
        incremental_rows,
        full_rows,
        "{}: the incremental and full views differ",
        shape.name()
    );
    Ok(())
}

async fn read_visible(
    runtime: &IvmRuntime,
    context: &SessionContext,
    table: &IvmTable,
    shape: Shape,
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

fn write_csv(args: &Args, results: &[RoundResult]) -> Result<()> {
    let mut csv = String::from(
        "shape,round,change_rows,source_write_ms,incr_ms,full_ms,speedup,\
         incr_files,incr_rows_scanned,incr_rows_written,incr_mv_bytes,\
         incr_io_bytes,full_files,full_rows_scanned,full_rows_written,\
         full_mv_bytes,full_io_bytes,incr_action,full_action\n",
    );
    for result in results {
        csv.push_str(&format!(
            "{},{},{},{:.3},{:.3},{:.3},{:.2},{},{},{},{},{},{},{},{},{},{},\
             {},{}\n",
            result.shape,
            result.round,
            result.change_rows,
            result.source_write_ms,
            result.incr_ms,
            result.full_ms,
            speedup(result.incr_ms, result.full_ms),
            result.incr.files_read,
            result.incr.rows_scanned,
            result.incr.rows_written,
            result.incr.mv_bytes_written,
            result.incr.object_store_bytes,
            result.full.files_read,
            result.full.rows_scanned,
            result.full.rows_written,
            result.full.mv_bytes_written,
            result.full.object_store_bytes,
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
         incr_p95   full_p95   incr_rows  incr_written full_rows  full_written"
    );
    for shape in &args.shapes {
        let pick = |shape: &Shape| {
            results
                .iter()
                .filter(|result| result.shape == shape.name())
                .collect::<Vec<_>>()
        };
        let values = pick(shape);
        if values.is_empty() {
            continue;
        }
        let collect = |field: fn(&RoundResult) -> f64| {
            values
                .iter()
                .map(|result| field(result))
                .collect::<Vec<_>>()
        };
        let incr = percentile(&collect(|result| result.incr_ms), 0.50);
        let full = percentile(&collect(|result| result.full_ms), 0.50);
        let incr_p95 = percentile(&collect(|result| result.incr_ms), 0.95);
        let full_p95 = percentile(&collect(|result| result.full_ms), 0.95);
        let mean = |pick: fn(&RoundResult) -> u64| {
            values.iter().map(|result| pick(result)).sum::<u64>() / values.len() as u64
        };
        println!(
            "{:<10} {:<9} {:<9} {:<9} {:<10} {:<10} {:<8} {:<10} {:<10} \
             {:<10} {:<12} {:<10} {:<12}",
            shape.name(),
            args.rows,
            args.groups,
            args.change_rows(),
            format_ms(incr),
            format_ms(full),
            format!("{:.1}x", speedup(incr, full)),
            format_ms(incr_p95),
            format_ms(full_p95),
            mean(|result| result.incr.rows_scanned),
            mean(|result| result.incr.rows_written),
            mean(|result| result.full.rows_scanned),
            mean(|result| result.full.rows_written),
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

fn ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
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

    fn index(&mut self, len: usize) -> usize {
        (self.next() % len.max(1) as u64) as usize
    }

    /// A uniform value in `[0, 1)`.
    fn fraction(&mut self) -> f64 {
        (self.next() % 1_000) as f64 / 1_000.0
    }
}

/// The counters the global recorder collected, keyed by `name{label=value}`.
#[derive(Default)]
struct BenchMetrics {
    counters: Mutex<BTreeMap<String, u64>>,
}

impl BenchMetrics {
    fn snapshot(&self) -> BTreeMap<String, u64> {
        self.counters.lock().unwrap().clone()
    }
}

fn metric_key(key: &Key) -> String {
    let mut labels = key
        .labels()
        .map(|label| format!("{}={}", label.key(), label.value()))
        .collect::<Vec<_>>();
    labels.sort();
    format!("{}{{{}}}", key.name(), labels.join(","))
}

struct BenchCounter {
    state: Arc<BenchMetrics>,
    key: String,
}

impl metrics::CounterFn for BenchCounter {
    fn increment(&self, value: u64) {
        *self
            .state
            .counters
            .lock()
            .unwrap()
            .entry(self.key.clone())
            .or_default() += value;
    }

    fn absolute(&self, value: u64) {
        self.state
            .counters
            .lock()
            .unwrap()
            .insert(self.key.clone(), value);
    }
}

struct BenchRecorder {
    state: Arc<BenchMetrics>,
}

impl Recorder for BenchRecorder {
    fn describe_counter(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _key: KeyName,
        _unit: Option<Unit>,
        _description: SharedString,
    ) {
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(BenchCounter {
            state: self.state.clone(),
            key: metric_key(key),
        }))
    }

    fn register_gauge(&self, _key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

/// The IO counters one `execute` moved.
fn io_delta(before: &BTreeMap<String, u64>, after: &BTreeMap<String, u64>) -> IoDelta {
    IoDelta {
        files_read: counter_delta(before, after, "lakesoul_ivm_refresh_files_read"),
        rows_scanned: counter_delta(before, after, "lakesoul_ivm_refresh_rows_scanned"),
        rows_written: counter_delta(before, after, "lakesoul_ivm_mv_rows_written"),
        mv_bytes_written: counter_delta(before, after, "lakesoul_ivm_mv_bytes_written"),
        object_store_bytes: counter_delta(
            before,
            after,
            "lakesoul_object_store_bytes_total",
        ),
    }
}

fn counter_delta(
    before: &BTreeMap<String, u64>,
    after: &BTreeMap<String, u64>,
    name: &str,
) -> u64 {
    let prefix = format!("{name}{{");
    after
        .iter()
        .filter(|(key, _)| key.starts_with(&prefix))
        .map(|(key, value)| value.saturating_sub(*before.get(key).unwrap_or(&0)))
        .sum()
}
