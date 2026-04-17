// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Test for writer in memory constrained environment
//! Original code from ['apache/datafusion'](https://github.com/apache/datafusion)

use std::{pin::Pin, sync::Arc};

use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::DataFusionError;
use datafusion_execution::{SendableRecordBatchStream, memory_pool::units::KB};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use lakesoul_io::{
    byte_size,
    config::{LakeSoulIOConfig, OPTION_KEY_POOL_SIZE},
    writer::async_writer::AsyncBatchWriter,
};
use rootcause::Report;
use tempfile::env::temp_dir;
use tokio_stream::StreamExt;

struct RunTestWithLimitedMemoryArgs {
    record_batch_size: usize,
    pool_size: usize,
    number_of_record_batches: usize,
    get_size_of_record_batch_to_generate:
        Pin<Box<dyn Fn(usize) -> usize + Send + 'static>>,
}

async fn run_sort_test_with_limited_memory(
    mut args: RunTestWithLimitedMemoryArgs,
) -> Result<usize, Report> {
    let get_size_of_record_batch_to_generate = std::mem::replace(
        &mut args.get_size_of_record_batch_to_generate,
        Box::pin(move |_| unreachable!("should not be called after take")),
    );
    let scan_schema = Arc::new(Schema::new(vec![
        Field::new("col_0", DataType::UInt64, true),
        Field::new("col_1", DataType::Utf8, true),
        Field::new("col_2", DataType::Utf8, true),
    ]));
    let writer = {
        let prefix = temp_dir();
        let io_config = LakeSoulIOConfig::builder()
            .with_batch_size(args.record_batch_size)
            .with_option(OPTION_KEY_POOL_SIZE, args.pool_size)
            .with_primary_key("col_0")
            .set_dynamic_partition(true)
            .with_hash_bucket_num("2")
            .with_thread_num(8)
            .with_range_partition("col_2".to_string())
            .with_prefix(format!("{}", prefix.as_path().display()))
            .with_schema(scan_schema.clone())
            .with_object_store_option("fs.s3a.path.style.access", "false")
            .with_object_store_option("fs.defaultFS", "file://")
            .build();
        cfg_if::cfg_if! {
            if #[cfg(feature = "test-utils")] {
                let mut io_session = lakesoul_io::session::LakeSoulIOSession::try_new(io_config)?;
                io_session.set_logged_pool();
                lakesoul_io::writer::create_writer(Arc::new(io_session)).await?
            } else {
                lakesoul_io::writer::create_writer_with_io_config(io_config).await?
            }
        }
    };

    let schema = Arc::clone(&scan_schema);
    let batch_stream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter((0..args.number_of_record_batches as u64).map(
            move |index| -> Result<RecordBatch, DataFusionError> {
                // bytes
                let c = ('a'..='z').nth((index as usize) % 26).unwrap();
                let total_batch_size =
                    get_size_of_record_batch_to_generate(index as usize);
                let range_array_size = c.len_utf8() * args.record_batch_size;

                let uint64_array_size = size_of::<u64>() * args.record_batch_size;
                let string_array_size = total_batch_size
                    .saturating_sub(uint64_array_size.saturating_add(range_array_size));

                let string_item_size = string_array_size / args.record_batch_size;
                let string_array = Arc::new(StringArray::from_iter_values(
                    (0..args.record_batch_size)
                        .map(|_| c.to_string().repeat(string_item_size)),
                ));

                let range_array = Arc::new(StringArray::from_iter_values(
                    (0..args.record_batch_size).map(|_| c.to_string()),
                ));

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(UInt64Array::from_iter_values(
                            (index * args.record_batch_size as u64)
                                ..(index * args.record_batch_size as u64)
                                    + args.record_batch_size as u64,
                        )),
                        string_array,
                        range_array,
                    ],
                )
                .map_err(|err| err.into())
            },
        )),
    ));

    run_test(args, batch_stream, writer).await
}

/// Consume the stream and change the amount of memory used while consuming it based on the [`MemoryBehavior`] provided
async fn consume_stream_and_simulate_other_running_memory_consumers(
    args: RunTestWithLimitedMemoryArgs,
    mut input_stream: SendableRecordBatchStream,
    mut writer: Box<dyn AsyncBatchWriter + Send>,
) -> Result<usize, Report> {
    let mut number_of_rows = 0;
    let record_batch_size = args.record_batch_size as u64;

    while let Some(batch) = input_stream.next().await {
        let batch = batch?;
        let batch_rows = batch.num_rows();

        writer.write_record_batch(batch).await?;

        number_of_rows += batch_rows;
    }

    assert_eq!(
        number_of_rows,
        args.number_of_record_batches * record_batch_size as usize
    );

    let _output = writer.flush().await?;
    let metrics_set = writer.metrics().unwrap().clone();
    writer.close().await?;
    let spill_count = metrics_set.spill_count().unwrap();
    Ok(spill_count)
}

async fn run_test(
    args: RunTestWithLimitedMemoryArgs,
    batch_stream: SendableRecordBatchStream,
    writer: Box<dyn AsyncBatchWriter + Send>,
) -> Result<usize, Report> {
    let number_of_record_batches = args.number_of_record_batches;

    let spill_count = consume_stream_and_simulate_other_running_memory_consumers(
        args,
        batch_stream,
        writer,
    )
    .await?;

    assert!(
        spill_count > 0,
        "Expected spill, but did not, number of record batches: {number_of_record_batches}",
    );

    Ok(spill_count)
}

#[tokio::test]
async fn sort_with_limited_memory_test() -> Result<(), Report> {
    let record_batch_size = 8192;
    let pool_size = byte_size!("2mb");
    let generate_batch_size = pool_size / 16;

    // Basic test with a lot of groups that cannot all fit in memory and 1 record batch
    // from each spill file is too much memory
    let spill_count = run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        record_batch_size,
        pool_size,
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| generate_batch_size),
    })
    .await?;

    let total_spill_files_size = spill_count * generate_batch_size;
    assert!(
        total_spill_files_size > pool_size,
        "Total spill files size {total_spill_files_size} should be greater than pool size {pool_size}",
    );

    Ok(())
}

#[tokio::test]
async fn sort_with_different_sizes_of_record_batch_test() -> Result<(), Report> {
    let record_batch_size = 8192;
    let pool_size = byte_size!("2mb");
    // Basic test with a lot of groups that cannot all fit in memory and 1 record batch
    // from each spill file is too much memory
    run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        record_batch_size,
        pool_size,
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |i| {
            if i % 25 == 1 {
                pool_size / 6
            } else {
                16 * KB as usize
            }
        }),
    })
    .await?;

    Ok(())
}

#[tokio::test]
#[test_log::test]
async fn sort_with_large_record_batch_test() -> Result<(), Report> {
    let record_batch_size = 8192;
    let pool_size = byte_size!("10mb");
    let generate_batch_size = pool_size / 5;

    // Basic test with a lot of groups that cannot all fit in memory and 1 record batch
    // from each spill file is too much memory
    let spill_count = run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        record_batch_size,
        pool_size,
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| generate_batch_size),
    })
    .await?;

    let total_spill_files_size = spill_count * generate_batch_size;
    assert!(
        total_spill_files_size > pool_size,
        "Total spill files size {total_spill_files_size} should be greater than pool size {pool_size}",
    );

    Ok(())
}
