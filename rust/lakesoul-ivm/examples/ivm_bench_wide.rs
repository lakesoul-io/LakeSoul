// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Wide CDC source benchmark: does pruning the before-state projection help?
//!
//! A `SUM/COUNT` view over a keyed CDC table with many payload columns reads
//! the before-state of the changed keys on every refresh.  Only a few columns
//! (primary key, group key, value, change column) are needed; this benchmark
//! makes the payload wide so the read cost difference is visible.
//!
//! ```sh
//! LAKESOUL_CACHE=1 cargo run --release -p lakesoul-ivm --example ivm_bench_wide -- \
//!   --rows 1000000 --wide 8 --change-ratio 0.01 --rounds 3 --format vortex
//! # baseline without the projection
//! LAKESOUL_IVM_NO_PROJECTION=1 ... (same command)
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmTableOptions, PhysicalFormat, SumCountView, sum_count_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

struct Args {
    rows: usize,
    wide: usize,
    groups: i64,
    change_ratio: f64,
    rounds: usize,
    warmup: usize,
    batch: usize,
    format: PhysicalFormat,
    seed: u64,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            rows: 1_000_000,
            wide: 8,
            groups: 1_000,
            change_ratio: 0.01,
            rounds: 3,
            warmup: 1,
            batch: 262_144,
            format: PhysicalFormat::Vortex,
            seed: 42,
        }
    }
}

fn parse_args() -> Args {
    let mut args = Args::default();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < argv.len() {
        let value = |index: &mut usize| -> String {
            *index += 1;
            argv.get(*index)
                .unwrap_or_else(|| panic!("missing value after {}", argv[*index - 1]))
                .clone()
        };
        match argv[index].as_str() {
            "--rows" => args.rows = value(&mut index).parse().unwrap(),
            "--wide" => args.wide = value(&mut index).parse().unwrap(),
            "--groups" => args.groups = value(&mut index).parse().unwrap(),
            "--change-ratio" => args.change_ratio = value(&mut index).parse().unwrap(),
            "--rounds" => args.rounds = value(&mut index).parse().unwrap(),
            "--warmup" => args.warmup = value(&mut index).parse().unwrap(),
            "--batch" => args.batch = value(&mut index).parse().unwrap(),
            "--seed" => args.seed = value(&mut index).parse().unwrap(),
            "--format" => {
                args.format = match value(&mut index).as_str() {
                    "parquet" => PhysicalFormat::Parquet,
                    "vortex" => PhysicalFormat::Vortex,
                    "vortex-compact" => PhysicalFormat::VortexCompact,
                    other => panic!("unknown format {other}"),
                }
            }
            "-h" | "--help" => {
                println!(
                    "ivm_bench_wide [--rows N] [--wide N] [--groups N] \
                     [--change-ratio F] [--rounds N] [--warmup N] \
                     [--batch N] [--format parquet|vortex|vortex-compact]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument {other}"),
        }
        index += 1;
    }
    args
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn index(&mut self, len: usize) -> usize {
        (self.next() % len.max(1) as u64) as usize
    }
}

fn source_schema(wide: usize) -> SchemaRef {
    let mut fields = vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
    ];
    for column in 0..wide {
        fields.push(Field::new(format!("p{column}"), DataType::Int64, false));
    }
    fields.push(Field::new(CHANGE_COLUMN, DataType::Utf8, false));
    Arc::new(Schema::new(fields))
}

/// One source row: `(key, group, value, change)`.
type SourceRow = (i64, String, i64, &'static str);

fn batch(schema: &SchemaRef, rows: &[SourceRow], wide: usize) -> RecordBatch {
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.1.as_str()),
        )),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
    ];
    for column in 0..wide {
        columns.push(Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.0 + column as i64 + row.2),
        )));
    }
    columns.push(Arc::new(StringArray::from_iter_values(
        rows.iter().map(|row| row.3),
    )));
    RecordBatch::try_new(schema.clone(), columns).expect("source batch")
}

async fn append(
    runtime: &IvmRuntime,
    table: &lakesoul_ivm::IvmTable,
    rows: &[SourceRow],
    batch_rows: usize,
    wide: usize,
) {
    for chunk in rows.chunks(batch_rows) {
        table
            .append_batch(runtime.client(), batch(&table.schema, chunk, wide))
            .await
            .unwrap();
    }
}

/// The MV state as `(group, sum, count)`, sorted.
async fn mv_state(
    runtime: &IvmRuntime,
    mv: &lakesoul_ivm::IvmTable,
) -> Vec<(String, i64, i64)> {
    let context = SessionContext::new();
    let batches = mv.read_current(runtime.client()).await.unwrap();
    let provider: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(mv.schema.clone(), vec![batches]).unwrap());
    context.register_table("mv", provider).unwrap();
    let frame = context
        .sql("select g, sum_v, count_v from mv")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in frame {
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
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                groups.value(row).to_string(),
                sums.value(row),
                counts.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let path = |name: &str| format!("file://{}", dir.path().join(name).display());

    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_wide_src_{suffix}"),
                path("src"),
                source_schema(args.wide),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_file_format(args.format),
        )
        .await
        .unwrap();
    let group_keys = vec!["g".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_wide_mv_{suffix}"),
                path("mv"),
                sum_count_mv_schema_for(&source.schema, &group_keys, Some("v")).unwrap(),
            )
            .with_primary_keys(group_keys.clone())
            .with_file_format(args.format),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("wide_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        Some("v".to_string()),
    );

    let mut rng = Lcg(args.seed);
    let mut live: Vec<i64> = Vec::with_capacity(args.rows);
    let mut next_key = args.rows as i64;
    let seed_rows = (0..args.rows)
        .map(|key| {
            live.push(key as i64);
            (
                key as i64,
                format!("g{}", key as i64 % args.groups),
                (key % 1000) as i64,
                "insert",
            )
        })
        .collect::<Vec<_>>();
    let started = Instant::now();
    append(&runtime, &source, &seed_rows, args.batch, args.wide).await;
    runtime.refresh_sum_count(&view).await.unwrap();
    println!(
        "bootstrapped {} rows x {} payload columns in {:.1}s",
        args.rows,
        args.wide,
        started.elapsed().as_secs_f64()
    );

    let changes = ((args.rows as f64) * args.change_ratio).round() as usize;
    for round in 0..(args.warmup + args.rounds) {
        let mut rows: Vec<SourceRow> = Vec::with_capacity(changes);
        for change in 0..changes {
            match change % 3 {
                0 => {
                    let key = live[rng.index(live.len())];
                    let group = format!("g{}", key % args.groups);
                    rows.push((key, group, (rng.next() % 1000) as i64, "insert"));
                }
                1 => {
                    let index = rng.index(live.len());
                    let key = live.swap_remove(index);
                    let group = format!("g{}", key % args.groups);
                    rows.push((key, group, 0, "delete"));
                }
                _ => {
                    let key = next_key;
                    next_key += 1;
                    live.push(key);
                    rows.push((
                        key,
                        format!("g{}", key % args.groups),
                        (rng.next() % 1000) as i64,
                        "insert",
                    ));
                }
            }
        }
        let write_started = Instant::now();
        append(&runtime, &source, &rows, args.batch, args.wide).await;
        let write_ms = write_started.elapsed().as_secs_f64() * 1_000.0;
        let started = Instant::now();
        runtime.refresh_sum_count(&view).await.unwrap();
        let refresh_ms = started.elapsed().as_secs_f64() * 1_000.0;
        if round >= args.warmup {
            println!(
                "round {} changes={} write={:.0}ms refresh={:.1}ms state_rows={}",
                round,
                rows.len(),
                write_ms,
                refresh_ms,
                mv_state(&runtime, &mv)
                    .await
                    .iter()
                    .map(|row| row.2)
                    .sum::<i64>()
            );
        }
    }

    // Correctness: the MV must equal the aggregate over the live source rows.
    let expected = {
        let context = SessionContext::new();
        let batches = source.read_current(runtime.client()).await.unwrap();
        let provider: Arc<dyn datafusion::catalog::TableProvider> =
            Arc::new(MemTable::try_new(source.schema.clone(), vec![batches]).unwrap());
        context.register_table("src", provider).unwrap();
        let batches = context
            .sql("select g, sum(v) as s, count(v) as c from src where op <> 'delete' group by g")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
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
            let counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((
                    groups.value(row).to_string(),
                    sums.value(row),
                    counts.value(row),
                ));
            }
        }
        rows.sort();
        rows
    };
    let actual = mv_state(&runtime, &mv).await;
    assert_eq!(
        actual, expected,
        "incremental state differs from the full aggregate"
    );
    println!("verification ok ({} groups)", actual.len());
}
