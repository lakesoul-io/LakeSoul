use std::{fs::File, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use datafusion_common::Result;
use lakesoul_io::{lakesoul_io_config::LakeSoulIOConfigBuilder, lakesoul_writer::SyncSendableMutableLakeSoulWriter};
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use rand::distributions::DistString;
use tokio::{runtime::Builder, time::Instant};

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn create_batch(num_columns: usize, num_rows: usize, str_len: usize) -> RecordBatch {
    let mut rng = rand::thread_rng();
    let iter = (0..num_columns)
        .into_iter()
        .map(|i| {
            (
                format!("col_{}", i),
                Arc::new(StringArray::from(
                    (0..num_rows)
                        .into_iter()
                        .map(|_| rand::distributions::Alphanumeric.sample_string(&mut rng, str_len))
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                true,
            )
        })
        .collect::<Vec<_>>();
    RecordBatch::try_from_iter_with_nullable(iter).unwrap()
}

fn main() -> Result<()> {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let num_batch = 1000;
    let num_rows = 1000;
    let num_columns = 20;
    let str_len = 4;

    let to_write = create_batch(num_columns, num_rows, str_len);
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir
        .into_path()
        .join("test.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    dbg!(&path);
    let writer_conf = LakeSoulIOConfigBuilder::new()
        .with_files(vec![path.clone()])
        .with_thread_num(2)
        .with_batch_size(num_rows)
        // .with_max_row_group_size(2000)
        // .with_max_row_group_num_values(4_00_000)
        .with_schema(to_write.schema())
        // .with_primary_keys(vec!["col_2".to_string()])
        .with_primary_keys(
            (0..num_columns - 1)
                .into_iter()
                .map(|i| format!("col_{}", i))
                .collect::<Vec<String>>(),
        )
        // .with_aux_sort_column("col2".to_string())
        .build();

    let writer = SyncSendableMutableLakeSoulWriter::try_new(writer_conf, runtime)?;

    let start = Instant::now();
    for _ in 0..num_batch {
        let once_start = Instant::now();
        writer.write_batch(create_batch(num_columns, num_rows, str_len))?;
        // println!("write batch once cost: {}", once_start.elapsed().as_millis());
    }
    let flush_start = Instant::now();
    writer.flush_and_close()?;
    println!("flush cost: {}", flush_start.elapsed().as_millis());
    println!(
        "num_batch={}, num_columns={}, num_rows={}, str_len={}, cost_mills={}",
        num_batch,
        num_columns,
        num_rows,
        str_len,
        start.elapsed().as_millis()
    );

    // let file = File::open(path.clone())?;
    // let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 100_000).unwrap();

    // let actual_batch = record_batch_reader
    //     .next()
    //     .expect("No batch found")
    //     .expect("Unable to get batch");

    // assert_eq!(to_write.schema(), actual_batch.schema());
    // assert_eq!(num_columns, actual_batch.num_columns());
    // assert_eq!(num_rows * num_batch, actual_batch.num_rows());
    Ok(())
}
