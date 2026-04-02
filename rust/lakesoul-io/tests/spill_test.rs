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

use std::{path::PathBuf, pin::Pin, sync::Arc};

use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use datafusion::prelude::SessionConfig;
use datafusion_common::DataFusionError;
use datafusion_execution::{
    SendableRecordBatchStream, TaskContext,
    memory_pool::{
        FairSpillPool, GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
        units::{KB, MB},
    },
    runtime_env::RuntimeEnvBuilder,
};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion_physical_plan::{
    ExecutionPlan, metrics::MetricValue, sorts::sort::SortExec,
    stream::RecordBatchStreamAdapter,
};
use datafusion_session::Session;
use lakesoul_io::{
    byte_size,
    config::{LakeSoulIOConfig, LakeSoulIOConfigBuilder, OPTION_KEY_POOL_SIZE},
    utils::{lakesoul_file_name, random_str},
    writer::{async_writer::AsyncBatchWriter, create_writer_with_io_config},
};
use rootcause::{Report, report};
use tempfile::env::temp_dir;
use tokio_stream::StreamExt;

mod common;

#[derive(Default)]
enum MemoryBehavior {
    #[default]
    AsIs,
    TakeAllMemoryAtTheBeginning,
    TakeAllMemoryAndReleaseEveryNthBatch(usize),
}

struct RunTestWithLimitedMemoryArgs {
    record_batch_size: usize,
    pool_size: usize,
    number_of_record_batches: usize,
    get_size_of_record_batch_to_generate:
        Pin<Box<dyn Fn(usize) -> usize + Send + 'static>>,
    memory_behavior: MemoryBehavior,
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
    ]));
    let writer = {
        // let dir = temp_dir();
        let prefix = "/data/jiax_space/LakeSoul";
        // let writer_id = random_str(16);
        // let file_name = lakesoul_file_name(&writer_id, 0);
        let io_config = LakeSoulIOConfig::builder()
            .with_batch_size(args.record_batch_size)
            .with_option(OPTION_KEY_POOL_SIZE, args.pool_size)
            .with_primary_key("col_0")
            .set_dynamic_partition(true)
            .with_hash_bucket_num("1")
            .with_prefix(prefix.to_string())
            .with_schema(scan_schema.clone())
            .with_object_store_option("fs.s3a.path.style.access", "false")
            .with_object_store_option("fs.defaultFS", "file://")
            .build();
        create_writer_with_io_config(io_config).await?
    };

    let schema = Arc::clone(&scan_schema);
    let batch_stream = Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter((0..args.number_of_record_batches as u64).map(
            move |index| -> Result<RecordBatch, DataFusionError> {
                // bytes
                let mut string_array_size =
                    get_size_of_record_batch_to_generate(index as usize);
                let uint64_array_size =
                    size_of::<u64>() * args.record_batch_size as usize;

                string_array_size = string_array_size.saturating_sub(uint64_array_size);

                let string_item_size =
                    string_array_size / args.record_batch_size as usize;
                let string_array = Arc::new(StringArray::from_iter_values(
                    (0..args.record_batch_size).map(|_| "a".repeat(string_item_size)),
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
                    ],
                )
                .map_err(|err| err.into())
            },
        )),
    ));

    run_test(args, batch_stream, writer).await
}

fn grow_memory_as_much_as_possible(
    memory_step: usize,
    memory_reservation: &mut MemoryReservation,
) -> Result<bool, Report> {
    let mut was_able_to_grow = false;
    while memory_reservation.try_grow(memory_step).is_ok() {
        was_able_to_grow = true;
    }

    Ok(was_able_to_grow)
}

/// Consume the stream and change the amount of memory used while consuming it based on the [`MemoryBehavior`] provided
async fn consume_stream_and_simulate_other_running_memory_consumers(
    args: RunTestWithLimitedMemoryArgs,
    mut input_stream: SendableRecordBatchStream,
    mut writer: Box<dyn AsyncBatchWriter + Send>,
) -> Result<(), Report> {
    let mut number_of_rows = 0;
    let record_batch_size = args.record_batch_size as u64;

    // same memory pool
    // todo
    let memory_pool = writer.io_session().task_ctx().memory_pool().clone();
    let memory_consumer = MemoryConsumer::new("mock_memory_consumer");
    let mut memory_reservation = memory_consumer.register(&memory_pool);

    let mut index = 0;
    let mut memory_took = false;

    while let Some(batch) = input_stream.next().await {
        match args.memory_behavior {
            MemoryBehavior::AsIs => {
                // Do nothing
            }
            MemoryBehavior::TakeAllMemoryAtTheBeginning => {
                if !memory_took {
                    memory_took = true;
                    grow_memory_as_much_as_possible(10, &mut memory_reservation)?;
                }
            }
            MemoryBehavior::TakeAllMemoryAndReleaseEveryNthBatch(n) => {
                if !memory_took {
                    memory_took = true;
                    grow_memory_as_much_as_possible(
                        args.pool_size,
                        &mut memory_reservation,
                    )?;
                } else if index % n == 0 {
                    // release memory
                    memory_reservation.free();
                }
            }
        }

        let batch = batch?;
        let batch_rows = batch.num_rows();

        writer.write_record_batch(batch).await?;

        number_of_rows += batch_rows;

        index += 1;
    }

    assert_eq!(
        number_of_rows,
        args.number_of_record_batches * record_batch_size as usize
    );

    let _output = writer.flush_and_close().await?;
    // println!("{:?}", output);
    Ok(())
}

pub(crate) fn assert_spill_count_metric(
    expect_spill: bool,
    plan_that_spills: Arc<dyn ExecutionPlan>,
) -> usize {
    if let Some(metrics_set) = plan_that_spills.metrics() {
        let mut spill_count = 0;

        // Inspect metrics for SpillCount
        for metric in metrics_set.iter() {
            if let MetricValue::SpillCount(count) = metric.value() {
                spill_count = count.value();
                break;
            }
        }

        if expect_spill && spill_count == 0 {
            panic!("Expected spill but SpillCount metric not found or SpillCount was 0.");
        } else if !expect_spill && spill_count > 0 {
            panic!(
                "Expected no spill but found SpillCount metric with value greater than 0."
            );
        }

        spill_count
    } else {
        panic!("No metrics returned from the operator; cannot verify spilling.");
    }
}

async fn run_test(
    args: RunTestWithLimitedMemoryArgs,
    batch_stream: SendableRecordBatchStream,
    writer: Box<dyn AsyncBatchWriter + Send>,
) -> Result<usize, Report> {
    let _number_of_record_batches = args.number_of_record_batches;

    consume_stream_and_simulate_other_running_memory_consumers(
        args,
        batch_stream,
        writer,
    )
    .await?;

    // let spill_count = assert_spill_count_metric(true, plan);

    // assert!(
    //     spill_count > 0,
    //     "Expected spill, but did not, number of record batches: {number_of_record_batches}",
    // );

    // Ok(spill_count)
    Ok(0)
}

#[tokio::test]
#[test_log::test]
async fn sort_with_limited_memory_test() -> Result<(), Report> {
    let record_batch_size = 8192;
    // let mem_limit = "100mb";
    let pool_size = byte_size!("2mb");

    let generate_batch_size = pool_size / 16;
    println!("{generate_batch_size}");

    // Basic test with a lot of groups that cannot all fit in memory and 1 record batch
    // from each spill file is too much memory
    let spill_count = run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        record_batch_size,
        pool_size,
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| generate_batch_size),
        memory_behavior: MemoryBehavior::default(),
    })
    .await?;

    println!("spill count: {}", spill_count);

    // let total_spill_files_size = spill_count * generate_batch_size;
    // assert!(
    //     total_spill_files_size > pool_size,
    //     "Total spill files size {total_spill_files_size} should be greater than pool size {pool_size}",
    // );

    Ok(())
}

// #[tokio::test]
// async fn sort_with_different_sizes_of_record_batch_test() -> Result<(), Report> {
//     let record_batch_size = 8192;
//     let pool_size = 2 * MB as usize;
//     let task_ctx = {
//         let memory_pool = Arc::new(FairSpillPool::new(pool_size));
//         TaskContext::default()
//             .with_session_config(
//                 SessionConfig::new()
//                     .with_batch_size(record_batch_size)
//                     .with_sort_spill_reservation_bytes(1),
//             )
//             .with_runtime(Arc::new(
//                 RuntimeEnvBuilder::new()
//                     .with_memory_pool(memory_pool)
//                     .build()
//                     .unwrap(),
//             ))
//     };
//     let generate_batch_size = pool_size / 16;
//     println!("{generate_batch_size}");

//     run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
//         pool_size,
//         task_ctx: Arc::new(task_ctx),
//         number_of_record_batches: 100,
//         get_size_of_record_batch_to_generate: Box::pin(move |i| {
//             if i % 25 == 1 {
//                 pool_size / 6
//             } else {
//                 16 * KB as usize
//             }
//         }),
//         memory_behavior: Default::default(),
//     })
//     .await?;
//     Ok(())
// }

// #[tokio::test]
// async fn sort_with_different_memory_reservation_test() -> Result<(), Report> {
//     let record_batch_size = 8192;
//     let pool_size = 2 * MB as usize;
//     let task_ctx = {
//         let memory_pool = Arc::new(FairSpillPool::new(pool_size));
//         TaskContext::default()
//             .with_session_config(
//                 SessionConfig::new()
//                     .with_batch_size(record_batch_size)
//                     .with_sort_spill_reservation_bytes(1),
//             )
//             .with_runtime(Arc::new(
//                 RuntimeEnvBuilder::new()
//                     .with_memory_pool(memory_pool)
//                     .build()?,
//             ))
//     };

//     run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
//         pool_size,
//         task_ctx: Arc::new(task_ctx),
//         number_of_record_batches: 100,
//         get_size_of_record_batch_to_generate: Box::pin(move |i| {
//             if i % 25 == 1 {
//                 pool_size / 6
//             } else {
//                 16 * KB as usize
//             }
//         }),
//         memory_behavior: MemoryBehavior::TakeAllMemoryAndReleaseEveryNthBatch(10),
//     })
//     .await?;

//     Ok(())
// }

// #[tokio::test]
// async fn sort_with_taking_all_memory() -> Result<(), Report> {
//     let record_batch_size = 8192;
//     let pool_size = 2 * MB as usize;
//     let task_ctx = {
//         let memory_pool = Arc::new(FairSpillPool::new(pool_size));
//         TaskContext::default()
//             .with_session_config(
//                 SessionConfig::new()
//                     .with_batch_size(record_batch_size)
//                     .with_sort_spill_reservation_bytes(1),
//             )
//             .with_runtime(Arc::new(
//                 RuntimeEnvBuilder::new()
//                     .with_memory_pool(memory_pool)
//                     .build()?,
//             ))
//     };

//     run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
//         pool_size,
//         task_ctx: Arc::new(task_ctx),
//         number_of_record_batches: 100,
//         get_size_of_record_batch_to_generate: Box::pin(move |i| {
//             if i % 25 == 1 {
//                 pool_size / 6
//             } else {
//                 16 * KB as usize
//             }
//         }),
//         memory_behavior: MemoryBehavior::TakeAllMemoryAtTheBeginning,
//     })
//     .await?;

//     Ok(())
// }

// #[tokio::test]
// async fn sort_with_large_record_batch() -> Result<(), Report> {
//     let record_batch_size = 8192;
//     let pool_size = 2 * MB as usize;
//     let task_ctx = {
//         let memory_pool = Arc::new(FairSpillPool::new(pool_size));
//         TaskContext::default()
//             .with_session_config(
//                 SessionConfig::new()
//                     .with_batch_size(record_batch_size)
//                     .with_sort_spill_reservation_bytes(1),
//             )
//             .with_runtime(Arc::new(
//                 RuntimeEnvBuilder::new()
//                     .with_memory_pool(memory_pool)
//                     .build()?,
//             ))
//     };

//     // Test that the merge degree of multi level merge sort cannot be fixed size when there is not enough memory
//     run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
//         pool_size,
//         task_ctx: Arc::new(task_ctx),
//         number_of_record_batches: 100,
//         get_size_of_record_batch_to_generate: Box::pin(move |_| pool_size / 6),
//         memory_behavior: Default::default(),
//     })
//     .await?;

//     Ok(())
// }
