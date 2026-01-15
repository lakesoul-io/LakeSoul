// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use lakesoul_io::{
    config::LakeSoulIOConfig, utils::random_str,
    writer::SyncSendableMutableLakeSoulWriter,
};
use rootcause::Report;
use tokio::{runtime::Builder, time::Instant};

// #[cfg(feature = "dhat-heap")]
// #[global_allocator]
// static ALLOC: dhat::Alloc = dhat::Alloc;

fn create_batch(num_columns: usize, num_rows: usize, str_len: usize) -> RecordBatch {
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
    RecordBatch::try_from_iter_with_nullable(iter).unwrap()
}

fn main() -> Result<(), Report> {
    //     #[cfg(feature = "dhat-heap")]
    //     let _profiler = dhat::Profiler::new_heap();

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let num_batch = 1000;
    let num_rows = 1000;
    let num_columns = 20;
    let str_len = 4;

    let to_write = create_batch(num_columns, num_rows, str_len);
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir
        .path()
        // .keep()
        .join("test.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let io_config = LakeSoulIOConfig::builder()
        .with_files(vec![path.clone()])
        .with_thread_num(2)
        .with_batch_size(num_rows)
        .with_option("mem_limit", "256m")
        // .with_max_row_group_size(2000)
        // .with_max_row_group_num_values(4_00_000)
        .with_schema(to_write.schema())
        // .with_primary_keys(vec!["col_2".to_string()])
        .with_primary_keys(
            (0..num_columns - 1)
                .map(|i| format!("col_{}", i))
                .collect::<Vec<String>>(),
        )
        // .with_aux_sort_column("col2".to_string())
        .build();

    let mut writer =
        SyncSendableMutableLakeSoulWriter::from_io_config(io_config, runtime)?;

    let e = tikv_jemalloc_ctl::epoch::mib().unwrap();
    let allocated = tikv_jemalloc_ctl::stats::allocated::mib().unwrap();
    let resident = tikv_jemalloc_ctl::stats::resident::mib().unwrap();
    let active = tikv_jemalloc_ctl::stats::active::mib().unwrap();

    let start = Instant::now();
    for i in 0..num_batch {
        e.advance().unwrap();
        let once_start = Instant::now();
        writer.write_batch(create_batch(num_columns, num_rows, str_len))?;
        println!(
            "write batch {} cost: {} ms\nAllocated: {} MB\nRsident: {} MB\nActive: {} MB\n",
            i,
            once_start.elapsed().as_millis(),
            allocated.read().unwrap() / 1024 / 1024,
            resident.read().unwrap() / 1024 / 1024,
            active.read().unwrap() / 1024 / 1024,
        );
    }
    let flush_start = Instant::now();
    writer.flush_and_close()?;
    println!("write path: {}", path);
    println!("flush cost: {} ms", flush_start.elapsed().as_millis());
    println!(
        "num_batch={}, num_columns={}, num_rows={}, str_len={}, total_cost_mills={}\nAllocated: {} MB\nRsident: {} MB\nActive: {} MB\n",
        num_batch,
        num_columns,
        num_rows,
        str_len,
        start.elapsed().as_millis(),
        allocated.read().unwrap() / 1024 / 1024,
        resident.read().unwrap() / 1024 / 1024,
        active.read().unwrap() / 1024 / 1024,
    );

    // let file = File::open(path.clone())?;
    // let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 100_000).unwrap();

    //     // let actual_batch = record_batch_reader
    //     //     .next()
    //     //     .expect("No batch found")
    //     //     .expect("Unable to get batch");

    //     // assert_eq!(to_write.schema(), actual_batch.schema());
    //     // assert_eq!(num_columns, actual_batch.num_columns());
    //     // assert_eq!(num_rows * num_batch, actual_batch.num_rows());
    Ok(())
}
