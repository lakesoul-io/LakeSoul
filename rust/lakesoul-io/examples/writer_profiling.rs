// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::SchemaRef;
use datafusion_execution::memory_pool::human_readable_size;
use datafusion_session::Session;
use lakesoul_io::{
    config::LakeSoulIOConfig, session::LakeSoulIOSession, utils::random_str,
    writer::SyncSendableMutableLakeSoulWriter,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rootcause::Report;
use tokio::{runtime::Builder, time::Instant};
use walkdir::WalkDir;

fn _create_batches(
    num_batch: usize,
    num_columns: usize,
    num_rows: usize,
    str_len: usize,
) -> Vec<RecordBatch> {
    let mut v = vec![];
    for _ in 0..num_batch {
        let iter = (0..num_columns)
            .map(|i| {
                (
                    format!("col_{}", i),
                    Arc::new(StringArray::from(
                        (0..num_rows)
                            .map(|_| random_str(str_len))
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                    true,
                )
            })
            .collect::<Vec<_>>();
        let b = RecordBatch::try_from_iter_with_nullable(iter).unwrap();
        v.push(b);
    }
    v
}

pub fn get_parquet_files(dir_path: &str) -> Vec<PathBuf> {
    WalkDir::new(dir_path)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "parquet"))
        .map(|e| e.path().to_owned())
        .collect()
}

pub fn get_schema_from_first_file(files: &[PathBuf]) -> SchemaRef {
    if files.is_empty() {
        panic!("No parquet files found");
    }
    let first_file = File::open(&files[0]).expect("Open first file failed");
    let first_builder = ParquetRecordBatchReaderBuilder::try_new(first_file)
        .expect("Create first builder failed");
    first_builder.schema().clone()
}

fn get_dir_size(path: &str) -> u64 {
    WalkDir::new(path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .sum()
}

#[cfg(feature = "jemalloc")]
fn inner() -> Result<(), Report> {
    // 1. 获取所有 Parquet 文件路径
    // let files = get_parquet_files("/data/lakesoul/tpch_sf10/lineitem");
    let files = vec![PathBuf::from("/data/lakesoul/tpch_sf1/orders.parquet")];

    // 2. 获取 Schema
    let schema = get_schema_from_first_file(&files);

    let temp_dir = tempfile::tempdir()?;
    let prefix = temp_dir.path().to_str().unwrap().to_string();

    println!("prefix: {}", prefix);

    let path = temp_dir
        // .path()
        .keep()
        .join("test.parquet")
        .into_os_string()
        .into_string()
        .unwrap();

    // 模拟 Flink 1GB Slot 环境配置
    // Rust 可用内存 ~300-400MB
    // 设置 DataFusion Pool = 200MB, 触发 Spill
    let io_config = LakeSoulIOConfig::builder()
        .with_prefix(prefix)
        // .with_files(vec![path.clone()])
        .with_batch_size(8192)
        .set_dynamic_partition(true)
        .with_hash_bucket_num("32") // 关键：单线程写入，防止内存倍增 (1GB环境下建议为1)
        .with_option("df_mem_limit", "100MB") // 200MB Limit
        // .with_option("max_spill_file_size_bytes", "104857600") // 100MB
        .with_receiver_capacity(2) // 关键：限制 Channel 积压
        .with_schema(schema)
        .with_range_partitions(vec!["o_orderdate".to_string()])
        // 必须设置 PK 才会触发 Sort -> Spill
        .with_primary_keys(vec!["o_orderkey".to_string()])
        // .with_aux_sort_column("col2".to_string())
        .build();

    let io_session = Arc::new(LakeSoulIOSession::try_new(io_config)?);
    let pool = io_session.runtime_env().memory_pool.clone();
    let runtime = Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2) // 2
        .build()
        .unwrap();
    let mut writer =
        SyncSendableMutableLakeSoulWriter::try_new(io_session.clone(), runtime)?;

    let e = tikv_jemalloc_ctl::epoch::mib().unwrap();
    let allocated = tikv_jemalloc_ctl::stats::allocated::mib().unwrap();
    let resident = tikv_jemalloc_ctl::stats::resident::mib().unwrap();
    let active = tikv_jemalloc_ctl::stats::active::mib().unwrap();

    let start = Instant::now();
    let mut total_rows = 0;
    let mut batch_idx = 0;
    println!("Start writing loop...");

    for file_path in &files {
        let file = File::open(file_path).expect("Open file failed");
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("Create builder failed")
            .with_batch_size(8192);
        let reader = builder.build().expect("Build reader failed");

        for batch_result in reader {
            let b = batch_result.expect("Read batch failed");
            let once_start = Instant::now();

            writer.write_batch(b.clone())?;

            total_rows += b.num_rows();
            batch_idx += 1;

            e.advance().unwrap();

            // 监控 Spill 目录大小 (LakeSoul 默认 spill 到 /tmp/lakesoul/spill/...)
            let spill_size = get_dir_size("/tmp/lakesoul/spill/dev");

            println!(
                "write batch {}\npool reserve: {}\nspill size: {}\ncost: {} ms\nAllocated: {}\nRsident: {}\nActive: {}\n",
                batch_idx,
                human_readable_size(pool.reserved()),
                human_readable_size(spill_size as usize),
                once_start.elapsed().as_millis(),
                human_readable_size(allocated.read().unwrap()),
                human_readable_size(resident.read().unwrap()),
                human_readable_size(active.read().unwrap()),
            );
        }
    }

    let flush_start = Instant::now();
    writer.flush_and_close()?;
    println!("flush cost: {} ms", flush_start.elapsed().as_millis());
    println!(
        "total_batches={}, total_rows={}, total_cost_mills={}\nAllocated: {}\nRsident: {}\nActive: {}\n",
        batch_idx,
        total_rows,
        start.elapsed().as_millis(),
        human_readable_size(allocated.read().unwrap()),
        human_readable_size(resident.read().unwrap()),
        human_readable_size(active.read().unwrap()),
    );
    Ok(())
}

#[cfg(not(feature = "jemalloc"))]
fn inner() -> Result<(), Report> {
    Ok(())
}

fn main() -> Result<(), Report> {
    //     #[cfg(feature = "dhat-heap")]
    //     let _profiler = dhat::Profiler::new_heap();
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    inner()?;

    Ok(())
}
