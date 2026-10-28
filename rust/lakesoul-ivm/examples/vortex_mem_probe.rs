// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Isolate IVM internal-table memory: write, full scan, and 1% key lookup.
//!
//! Run each phase in its own process so `VmHWM` is attributable:
//! `cargo run --release -p lakesoul-ivm --example vortex_mem_probe -- \
//!    --phase write --format vortex --rows 1000000`

use std::sync::Arc;

use arrow_array::{
    Decimal128Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use lakesoul_ivm::{IvmRuntime, IvmTableOptions, PhysicalFormat};
use rootcause::report;

fn schema() -> SchemaRef {
    let mut fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("src_sys", DataType::Utf8, false),
        Field::new("order_no", DataType::Utf8, false),
        Field::new("order_item_id", DataType::Utf8, false),
        Field::new("store_id", DataType::Int64, true),
        Field::new("sku_id", DataType::Utf8, true),
        Field::new("goods_id", DataType::Utf8, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("item_level", DataType::Utf8, true),
        Field::new("item_type", DataType::Utf8, true),
        Field::new("num", DataType::Decimal128(18, 6), true),
        Field::new("goods_amount", DataType::Decimal128(18, 6), true),
        Field::new("receivable_amount", DataType::Decimal128(18, 6), true),
        Field::new(
            "event_date",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "record_date",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "process_date",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
    ];
    fields.push(Field::new("v1", DataType::Int64, true));
    fields.push(Field::new("v2", DataType::Int64, true));
    Arc::new(Schema::new(fields))
}

fn batch(start: usize, len: usize) -> RecordBatch {
    let ids = Int64Array::from_iter_values((start..start + len).map(|i| i as i64));
    let src = StringArray::from_iter_values((0..len).map(|_| "qimai"));
    let orders = StringArray::from_iter_values(
        (start..start + len).map(|i| format!("O{}", i / 3)),
    );
    let items =
        StringArray::from_iter_values((start..start + len).map(|i| format!("I{i}")));
    let stores = Int64Array::from_iter_values((0..len).map(|i| (i % 100) as i64));
    let skus = StringArray::from_iter_values((0..len).map(|i| format!("S{}", i % 50)));
    let goods = StringArray::from_iter_values((0..len).map(|i| format!("G{}", i % 20)));
    let names = StringArray::from_iter_values(
        (0..len).map(|i| format!("name-{i}-{}", "x".repeat(40))),
    );
    let levels = StringArray::from_iter_values((0..len).map(|_| "1"));
    let types = StringArray::from_iter_values((0..len).map(|_| "0"));
    let nums =
        Decimal128Array::from_iter_values((0..len).map(|i| ((i % 1000) as i128) * 1000))
            .with_precision_and_scale(18, 6)
            .unwrap();
    let amounts =
        Decimal128Array::from_iter_values((0..len).map(|i| ((i % 5000) as i128) * 1000))
            .with_precision_and_scale(18, 6)
            .unwrap();
    let receivables =
        Decimal128Array::from_iter_values((0..len).map(|i| ((i % 3000) as i128) * 1000))
            .with_precision_and_scale(18, 6)
            .unwrap();
    let ts = TimestampMillisecondArray::from_iter_values(
        (0..len).map(|i| 1_700_000_000_000 + i as i64),
    );
    let v1 = Int64Array::from_iter_values((0..len).map(|i| (i % 7) as i64));
    let v2 = Int64Array::from_iter_values((0..len).map(|i| (i % 11) as i64));
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(ids),
            Arc::new(src),
            Arc::new(orders),
            Arc::new(items),
            Arc::new(stores),
            Arc::new(skus),
            Arc::new(goods),
            Arc::new(names),
            Arc::new(levels),
            Arc::new(types),
            Arc::new(nums),
            Arc::new(amounts),
            Arc::new(receivables),
            Arc::new(ts.clone()),
            Arc::new(ts.clone()),
            Arc::new(ts),
            Arc::new(v1),
            Arc::new(v2),
        ],
    )
    .expect("probe batch")
}

fn arg(name: &str, default: &str) -> String {
    let argv: Vec<String> = std::env::args().collect();
    argv.iter()
        .position(|value| value == name)
        .and_then(|index| argv.get(index + 1).cloned())
        .unwrap_or_else(|| default.to_string())
}

fn hwm_mb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| {
            value
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse::<u64>()
                .ok()
        })
        .unwrap_or(0)
        / 1024
}

#[tokio::main]
async fn main() -> rootcause::Result<()> {
    let phase = arg("--phase", "read");
    let format = match arg("--format", "vortex").as_str() {
        "parquet" => PhysicalFormat::Parquet,
        "vortex" => PhysicalFormat::Vortex,
        "vortex-compact" => PhysicalFormat::VortexCompact,
        other => return Err(report!("unknown format {other}")),
    };
    let rows: usize = arg("--rows", "1000000").parse()?;
    let buckets: usize = arg("--buckets", "8").parse()?;
    let batch_rows: usize = arg("--batch", "8192").parse()?;
    let name = format!("vortex_mem_probe_{}", format.name());
    let path = format!("file:///tmp/opencode/probe_{}", format.name());
    let runtime = IvmRuntime::from_env().await?;
    runtime.init_schema().await?;

    if phase == "write" {
        if let Ok(existing) = runtime.open_table(&name, "default").await {
            let _ = runtime
                .client()
                .drop_table(&existing.table_name, &existing.namespace)
                .await;
        }
        let _ = std::fs::remove_dir_all(path.trim_start_matches("file://"));
        let mut options = IvmTableOptions::new(name.clone(), path.clone(), schema())
            .with_primary_keys(vec!["id".to_string()])
            .with_bucket_columns(vec!["id".to_string()])
            .with_file_format(format);
        options.hash_bucket_num = buckets.to_string();
        let table = runtime.create_table(options).await?;
        let started = std::time::Instant::now();
        let total_batches = rows.div_ceil(batch_rows);
        let stream = futures::stream::iter((0..total_batches).map(move |index| {
            let start = index * batch_rows;
            let len = batch_rows.min(rows - start);
            Ok(batch(start, len))
        }));
        table.append_stream(runtime.client(), stream).await?;
        println!(
            "phase=write format={} rows={} elapsed_ms={} hwm_mb={}",
            format.name(),
            rows,
            started.elapsed().as_millis(),
            hwm_mb()
        );
        return Ok(());
    }

    let table = runtime.open_table(&name, "default").await?;
    let started = std::time::Instant::now();
    if phase == "append" {
        let append_rows: usize = arg("--append-rows", "10000").parse()?;
        let repeat: usize = arg("--repeat", "1").parse()?;
        for round in 0..repeat {
            let total_batches = append_rows.div_ceil(batch_rows);
            let stream = futures::stream::iter((0..total_batches).map(move |index| {
                let start = round * append_rows + index * batch_rows;
                let len = batch_rows.min(append_rows - index * batch_rows);
                Ok(batch(start, len))
            }));
            table.append_stream(runtime.client(), stream).await?;
        }
        println!(
            "phase=append format={} rows={} repeat={} elapsed_ms={} hwm_mb={}",
            format.name(),
            append_rows * repeat,
            repeat,
            started.elapsed().as_millis(),
            hwm_mb()
        );
        return Ok(());
    }
    if phase == "read" {
        let batches = table.read_current(runtime.client()).await?;
        let counted: usize = batches.iter().map(RecordBatch::num_rows).sum();
        println!(
            "phase=read format={} rows={} batches={} elapsed_ms={} hwm_mb={}",
            format.name(),
            counted,
            batches.len(),
            started.elapsed().as_millis(),
            hwm_mb()
        );
        return Ok(());
    }

    if phase == "lookup" {
        let fraction: usize = arg("--percent", "1").parse()?;
        let files = table.current_files(runtime.client()).await?;
        let step = (100 / fraction.max(1)).max(1);
        let keys: Vec<i64> = (0..rows).step_by(step).map(|value| value as i64).collect();
        let filter = datafusion::prelude::col("id").in_list(
            keys.iter()
                .map(|key| datafusion::prelude::lit(*key))
                .collect(),
            false,
        );
        let repeat: usize = arg("--repeat", "1").parse()?;
        let mut counted = 0usize;
        for _ in 0..repeat {
            let batches = table
                .read_files_filtered(files.clone(), vec![filter.clone()])
                .await?;
            counted += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        }
        println!(
            "phase=lookup format={} keys={} rows={} repeat={} elapsed_ms={} hwm_mb={}",
            format.name(),
            keys.len(),
            counted,
            repeat,
            started.elapsed().as_millis(),
            hwm_mb()
        );
        return Ok(());
    }

    Err(report!("unknown phase {phase}"))
}
