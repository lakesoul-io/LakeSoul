// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
//
// Benchmarks for V2 partial merge with different schema streams.
//
// Run with:
//   LAKESOUL_IO_USE_V2_MERGE=true RUST_LOG=info cargo run --profile bench --bench partial_merge
//   Or select one: BENCH_LABEL=overlap LAKESOUL_IO_USE_V2_MERGE=true RUST_LOG=info cargo run --profile bench --bench partial_merge

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Int64Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::reader::LakeSoulReader;
use lakesoul_io::writer::create_writer_with_io_config;
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};
use tracing_subscriber;

const TOTAL_ROWS: usize = 10_000_000;
const FILES: usize = 3;
const BATCH_SIZE: usize = 4096;
const BENCH_DIR: &str = "/tmp/lakesoul_bench_partial";

fn bench_dir() -> PathBuf {
    PathBuf::from(BENCH_DIR)
}

/// Full merged schema — all columns that exist across all streams.
fn merged_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("col_a", DataType::Utf8, true),
        Field::new("col_b", DataType::Utf8, true),
        Field::new("col_c", DataType::Utf8, true),
        Field::new("col_d", DataType::Utf8, true),
        Field::new("col_e", DataType::Utf8, true),
        Field::new("col_f", DataType::Utf8, true),
        Field::new("col_g", DataType::Utf8, true),
        Field::new("col_h", DataType::Utf8, true),
    ]))
}

/// Schema for a single stream — only the columns it actually has.
fn stream_schema(col_names: &[&str]) -> SchemaRef {
    let mut fields: Vec<Field> = vec![Field::new("id", DataType::Int64, false)];
    for &name in col_names {
        fields.push(Field::new(name, DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

fn gen_str(rng: &mut StdRng) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    const LEN: usize = 8;
    (0..LEN)
        .map(|_| CHARSET[rng.next_u32() as usize % CHARSET.len()] as char)
        .collect()
}

fn make_batch(
    schema: &SchemaRef,
    num_rows: usize,
    ids_start: i64,
    rng: &mut StdRng,
) -> RecordBatch {
    let columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "id" {
                let ids: Vec<i64> = (ids_start..ids_start + num_rows as i64).collect();
                Arc::new(Int64Array::from(ids)) as ArrayRef
            } else {
                let mut builder = StringBuilder::with_capacity(num_rows, 32);
                for _ in 0..num_rows {
                    builder.append_value(gen_str(rng));
                }
                Arc::new(builder.finish()) as ArrayRef
            }
        })
        .collect();
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

async fn generate_and_write(
    prefix: &str,
    col_names: &[&str],
    rows_per_stream: usize,
    rng: &mut StdRng,
) -> Vec<String> {
    let schema = stream_schema(col_names);
    std::fs::create_dir_all(bench_dir()).unwrap();

    // Determine if files already exist
    let mut all_exist = true;
    let mut paths = Vec::new();
    for i in 0..FILES {
        let path = bench_dir().join(format!("{}_{}.parquet", prefix, i));
        all_exist &= path.exists();
        paths.push(path);
    }

    let mut files = Vec::new();
    for (i, path) in paths.into_iter().enumerate() {
        let p = path.to_string_lossy().to_string();
        if all_exist {
            files.push(p);
            continue;
        }
        if path.exists() {
            files.push(p);
            continue;
        }

        let start_id = (i * rows_per_stream / FILES) as i64;
        let end_id = ((i + 1) * rows_per_stream / FILES) as i64;

        let _ = std::fs::remove_file(&path);

        let config = LakeSoulIOConfigBuilder::new()
            .with_schema(schema.clone())
            .with_files(vec![p.clone()])
            .with_primary_keys(vec!["id".to_string()])
            .build();
        let mut writer = create_writer_with_io_config(config).await.unwrap();

        let mut cursor = start_id;
        while cursor < end_id {
            let chunk_size = BATCH_SIZE.min((end_id - cursor) as usize);
            let batch = make_batch(&schema, chunk_size, cursor, rng);
            writer.write_record_batch(batch).await.unwrap();
            cursor += chunk_size as i64;
        }
        writer.flush_and_close().await.unwrap();
        files.push(p);
    }
    files
}

async fn prepare_overlapping_files() -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(42);
    let rows_per_stream = TOTAL_ROWS / 3;

    let s1_cols = ["col_a", "col_b", "col_c", "col_d", "col_e"];
    let s2_cols = ["col_c", "col_d", "col_e", "col_f", "col_g"];
    let s3_cols = ["col_a", "col_b", "col_f", "col_g", "col_h"];

    let mut files = Vec::new();
    for (prefix, cols) in &[
        ("overlap_s1", &s1_cols[..]),
        ("overlap_s2", &s2_cols[..]),
        ("overlap_s3", &s3_cols[..]),
    ] {
        files.extend(generate_and_write(prefix, cols, rows_per_stream, &mut rng).await);
    }
    files
}

async fn prepare_nonoverlap_files() -> Vec<String> {
    let mut rng = StdRng::seed_from_u64(42);
    let rows_per_stream = TOTAL_ROWS / 3;

    let mut files = Vec::new();
    for (prefix, cols) in &[
        ("nonoverlap_s1", &["col_a", "col_b", "col_c"][..]),
        ("nonoverlap_s2", &["col_d", "col_e", "col_f"][..]),
        ("nonoverlap_s3", &["col_g", "col_h"][..]),
    ] {
        files.extend(generate_and_write(prefix, cols, rows_per_stream, &mut rng).await);
    }
    files
}

async fn run_merge_bench(files: Vec<String>, label: &str) {
    let schema = merged_schema();
    let conf = LakeSoulIOConfigBuilder::new()
        .with_schema(schema)
        .with_primary_keys(vec!["id".to_string()])
        .with_files(files)
        .with_batch_size(8192)
        .with_thread_num(2)
        .build();

    let start = Instant::now();
    let mut reader = LakeSoulReader::new(conf).unwrap();
    reader.start().await.unwrap();
    let mut total_rows = 0usize;
    while let Some(rb) = reader.next_rb().await {
        let rb = rb.unwrap();
        total_rows += rb.num_rows();
    }
    let elapsed = start.elapsed();
    println!(
        "[{}] rows={} elapsed={:?} throughput={:.1} rows/s",
        label,
        total_rows,
        elapsed,
        total_rows as f64 / elapsed.as_secs_f64()
    );
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let label = std::env::var("BENCH_LABEL").unwrap_or_else(|_| "all".to_string());

    if label == "all" || label == "overlap" {
        let files = prepare_overlapping_files().await;
        run_merge_bench(files, "overlap").await;
    }

    if label == "all" || label == "nonoverlap" {
        let files = prepare_nonoverlap_files().await;
        run_merge_bench(files, "nonoverlap").await;
    }
}
