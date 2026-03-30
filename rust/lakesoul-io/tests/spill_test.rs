use std::{pin::Pin, sync::Arc};

use arrow_array::{RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SortOptions};
use datafusion::prelude::SessionConfig;
use datafusion_execution::{
    SendableRecordBatchStream, TaskContext,
    memory_pool::{FairSpillPool, MemoryConsumer, MemoryReservation, units::MB},
    runtime_env::RuntimeEnvBuilder,
};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion_physical_plan::{
    ExecutionPlan, metrics::MetricValue, sorts::sort::SortExec,
    stream::RecordBatchStreamAdapter,
};
use rootcause::Report;
use tokio_stream::StreamExt;

use crate::common::OnceExec;

mod common;

#[derive(Default)]
enum MemoryBehavior {
    #[default]
    AsIs,
    TakeAllMemoryAtTheBeginning,
    TakeAllMemoryAndReleaseEveryNthBatch(usize),
}

struct RunTestWithLimitedMemoryArgs {
    pool_size: usize,
    task_ctx: Arc<TaskContext>,
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
    let record_batch_size = args.task_ctx.session_config().batch_size() as u64;
    let schema = Arc::clone(&scan_schema);
    let plan = Arc::new(OnceExec::new(Box::pin(RecordBatchStreamAdapter::new(
        Arc::clone(&schema),
        futures::stream::iter((0..args.number_of_record_batches as u64).map(
            move |index| {
                let mut record_batch_memory_size =
                    get_size_of_record_batch_to_generate(index as usize);
                record_batch_memory_size = record_batch_memory_size
                    .saturating_sub(size_of::<u64>() * record_batch_size as usize);

                let string_item_size =
                    record_batch_memory_size / record_batch_size as usize;
                let string_array = Arc::new(StringArray::from_iter_values(
                    (0..record_batch_size).map(|_| "a".repeat(string_item_size)),
                ));

                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(UInt64Array::from_iter_values(
                            (index * record_batch_size)
                                ..(index * record_batch_size) + record_batch_size,
                        )),
                        string_array,
                    ],
                )
                .map_err(|err| err.into())
            },
        )),
    ))));
    let sort_exec = Arc::new(SortExec::new(
        LexOrdering::new(vec![PhysicalSortExpr {
            expr: col("col_0", &scan_schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }])
        .unwrap(),
        plan,
    ));
    let result = sort_exec.execute(0, Arc::clone(&args.task_ctx))?;

    run_test(args, sort_exec, result).await
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
    mut result_stream: SendableRecordBatchStream,
) -> Result<(), Report> {
    let mut number_of_rows = 0;
    let record_batch_size = args.task_ctx.session_config().batch_size() as u64;

    let memory_pool = args.task_ctx.memory_pool();
    let memory_consumer = MemoryConsumer::new("mock_memory_consumer");
    let mut memory_reservation = memory_consumer.register(memory_pool);

    let mut index = 0;
    let mut memory_took = false;

    while let Some(batch) = result_stream.next().await {
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
        number_of_rows += batch.num_rows();

        index += 1;
    }

    assert_eq!(
        number_of_rows,
        args.number_of_record_batches * record_batch_size as usize
    );

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
    plan: Arc<dyn ExecutionPlan>,
    result_stream: SendableRecordBatchStream,
) -> Result<usize, Report> {
    let number_of_record_batches = args.number_of_record_batches;

    consume_stream_and_simulate_other_running_memory_consumers(args, result_stream)
        .await?;

    let spill_count = assert_spill_count_metric(true, plan);

    assert!(
        spill_count > 0,
        "Expected spill, but did not, number of record batches: {number_of_record_batches}",
    );

    Ok(spill_count)
}

#[tokio::test]
async fn xxxxxxtest() -> Result<(), Report> {
    let record_batch_size = 8192;
    let pool_size = 2 * MB as usize;
    let task_ctx = {
        let memory_pool = Arc::new(FairSpillPool::new(pool_size));
        TaskContext::default()
            .with_session_config(
                SessionConfig::new()
                    .with_batch_size(record_batch_size)
                    .with_sort_spill_reservation_bytes(1),
            )
            .with_runtime(Arc::new(
                RuntimeEnvBuilder::new()
                    .with_memory_pool(memory_pool)
                    .build()
                    .unwrap(),
            ))
    };
    let generate_batch_size = pool_size / 16;
    println!("{generate_batch_size}");

    // Basic test with a lot of groups that cannot all fit in memory and 1 record batch
    // from each spill file is too much memory
    let spill_count = run_sort_test_with_limited_memory(RunTestWithLimitedMemoryArgs {
        pool_size,
        task_ctx: Arc::new(task_ctx),
        number_of_record_batches: 100,
        get_size_of_record_batch_to_generate: Box::pin(move |_| generate_batch_size),
        memory_behavior: Default::default(),
    })
    .await?;

    let total_spill_files_size = spill_count * generate_batch_size;
    assert!(
        total_spill_files_size > pool_size,
        "Total spill files size {total_spill_files_size} should be greater than pool size {pool_size}",
    );

    Ok(())
}
