// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the partitioning writer, which repartitions the record batches by primary keys and range partitions before writing.

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::DataFusionError;
use datafusion_common_runtime::{JoinSet, SpawnedTask};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{
    LexOrdering, PhysicalSortExpr,
    expressions::{Column, col},
};
use datafusion_physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, Partitioning, PhysicalExpr,
    projection::ProjectionExec, sorts::sort::SortExec, stream::RecordBatchReceiverStream,
};
use datafusion_session::Session;
use futures::{StreamExt, TryStreamExt};
use rootcause::{Report, bail, report};
use tokio::sync::mpsc::Sender;

use crate::{
    Result,
    config::{IOSchema, LakeSoulIOConfig, LakeSoulIOConfigBuilder},
    helpers::transform::uniform_schema,
    helpers::{
        columnar_values_to_partition_desc, columnar_values_to_sub_path,
        get_batch_memory_size, get_columnar_values,
    },
    physical_plan::repartition::RepartitionByRangeAndHashExec,
    physical_plan::self_incremental_index_column::SelfIncrementalIndexColumnExec,
    session::LakeSoulIOSession,
    utils::random_str,
};

use super::{AsyncBatchWriter, FlushOutput, MultiPartAsyncWriter, ReceiverStreamExec};

/// Wrap the async writer with a RepartitionExec to
/// dynamic repartitioning the batches before write to async writer
pub struct PartitioningAsyncWriter {
    /// The schema of the partitioning writer.
    schema: SchemaRef,
    /// The sender to async multi-part file writer.
    sorter_sender: Sender<Result<RecordBatch, DataFusionError>>,
    /// The join handle of the partitioning execution plan.
    spawned_task: Option<SpawnedTask<Result<Vec<FlushOutput>>>>,
    /// The external error of the partitioning execution plan.
    err: Option<Report>,
    /// The buffered size of the partitioning writer.
    buffered_size: u64,
}

type NestedFlushOutputResult = Result<JoinSet<Result<Vec<FlushOutput>>>>;

impl PartitioningAsyncWriter {
    pub fn try_new(io_session: Arc<LakeSoulIOSession>) -> Result<Self> {
        // TODO: need clone?
        let io_config = io_session.io_config();
        let schema = io_config.target_schema.0.clone();
        let receiver_stream_builder = RecordBatchReceiverStream::builder(
            schema.clone(),
            io_config.receiver_capacity,
        );
        let tx = receiver_stream_builder.tx();
        let recv_exec = ReceiverStreamExec::new(receiver_stream_builder, schema.clone());
        let partitioning_exec =
            PartitioningAsyncWriter::create_partitioning_exec(recv_exec, io_config)?;

        // launch one async task per *input* partition

        let mut writer_io_config = io_config.clone();

        if !io_config.aux_sort_cols.is_empty() {
            let schema = io_config.target_schema.0.clone();
            // O(nm), n = number of target schema fields, m = number of aux sort cols
            let proj_indices = schema
                .fields
                .iter()
                .filter(|f| !io_config.aux_sort_cols.contains(f.name()))
                .map(|f| {
                    schema
                        .index_of(f.name().as_str())
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                })
                .collect::<Result<Vec<usize>, DataFusionError>>()?;
            let writer_schema = Arc::new(schema.project(&proj_indices)?);
            writer_io_config.target_schema = IOSchema(uniform_schema(writer_schema));
        }

        let range_partitions = io_config.range_partitions.clone();

        let mut spawned_tasks = vec![];

        let write_id = random_str(16);
        // this task_ctx can be shared
        let task_ctx = io_session.task_ctx();
        for i in 0..partitioning_exec.output_partitioning().partition_count() {
            let sink_task = SpawnedTask::spawn(Self::pull_and_sink(
                partitioning_exec.clone(),
                i,
                LakeSoulIOConfigBuilder::from(writer_io_config.clone()),
                Arc::new(range_partitions.clone()), // TODO clone and arc
                write_id.clone(),
                task_ctx.clone(),
            ));

            // In a separate task, wait for each input to be done
            // (and pass along any errors, including panic!s)
            spawned_tasks.push(sink_task);
        }

        let spawned_task = SpawnedTask::spawn(Self::await_and_summary(spawned_tasks));

        Ok(Self {
            schema,
            sorter_sender: tx,
            spawned_task: Some(spawned_task),
            err: None,
            buffered_size: 0,
        })
    }

    fn create_partitioning_exec(
        input: ReceiverStreamExec,
        io_config: &LakeSoulIOConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut aux_sort_cols = io_config.aux_sort_cols.clone();
        let input: Arc<dyn ExecutionPlan> = if io_config.stable_sort() {
            aux_sort_cols.push("__self_incremental_index__".to_string());
            info!(
                "input schema of self incremental index exec: {:?}",
                input.schema()
            );
            Arc::new(SelfIncrementalIndexColumnExec::new(Arc::new(input)))
        } else {
            Arc::new(input)
        };
        let input_schema = input.schema();
        let sort_exprs: Vec<PhysicalSortExpr> = io_config
            .range_partitions
            .iter()
            .chain(io_config.primary_keys.iter())
            // add aux sort cols to sort expr
            .chain(aux_sort_cols.iter())
            .map(|sort_column| {
                let col =
                    Column::new_with_schema(sort_column.as_str(), input_schema.as_ref())?;
                Ok(PhysicalSortExpr {
                    expr: Arc::new(col),
                    options: SortOptions::default(),
                })
            })
            .collect::<Result<Vec<PhysicalSortExpr>>>()?;

        let sort_exec = if let Some(ordering) = LexOrdering::new(sort_exprs) {
            Arc::new(SortExec::new(ordering, input))
        } else {
            return Ok(input);
        };
        // see if we need to prune aux sort cols
        let sort_exec: Arc<dyn ExecutionPlan> = if aux_sort_cols.is_empty() {
            sort_exec
        } else {
            // O(nm), n = number of target schema fields, m = number of aux sort cols
            let proj_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = io_config
                .target_schema
                .0
                .fields
                .iter()
                .filter_map(|f| {
                    if aux_sort_cols.contains(f.name()) {
                        // exclude aux sort cols
                        None
                    } else {
                        Some(
                            col(f.name().as_str(), &io_config.target_schema.0)
                                .map(|e| (e, f.name().clone())),
                        )
                    }
                })
                .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>, DataFusionError>>(
                )?;
            Arc::new(ProjectionExec::try_new(proj_expr, sort_exec)?)
        };

        let exec_plan = if io_config.primary_keys.is_empty()
            && io_config.range_partitions.is_empty()
        {
            sort_exec
        } else {
            let sorted_schema = sort_exec.schema();

            let range_partitioning_expr: Vec<Arc<dyn PhysicalExpr>> = io_config
                .range_partitions
                .iter()
                .map(|col| {
                    let idx = sorted_schema.index_of(col.as_str())?;
                    Ok(Arc::new(Column::new(col.as_str(), idx)) as Arc<dyn PhysicalExpr>)
                })
                .collect::<Result<Vec<_>>>()?;

            let hash_partitioning_expr: Vec<Arc<dyn PhysicalExpr>> = io_config
                .primary_keys
                .iter()
                .map(|col| {
                    let idx = sorted_schema.index_of(col.as_str())?;
                    Ok(Arc::new(Column::new(col.as_str(), idx)) as Arc<dyn PhysicalExpr>)
                })
                .collect::<Result<Vec<_>>>()?;
            let hash_partitioning = Partitioning::Hash(
                hash_partitioning_expr,
                io_config.get_hash_bucket_num()?,
            );

            Arc::new(RepartitionByRangeAndHashExec::try_new(
                sort_exec,
                range_partitioning_expr,
                hash_partitioning,
            )?)
        };

        Ok(exec_plan)
    }

    async fn pull_and_sink(
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        io_config_builder: LakeSoulIOConfigBuilder,
        range_partitions: Arc<Vec<String>>,
        write_id: String,
        task_ctx: Arc<TaskContext>,
    ) -> Result<JoinSet<Result<Vec<FlushOutput>>>> {
        let mut data = input.execute(partition, task_ctx.clone())?;
        // O(nm), n = number of data fields, m = number of range partitions
        let schema_projection_excluding_range = data
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(
                |(idx, field)| match range_partitions.contains(field.name()) {
                    true => None,
                    false => Some(idx),
                },
            )
            .collect::<Vec<_>>();

        let mut err = None;

        let mut partitioned_writer = HashMap::<String, Box<MultiPartAsyncWriter>>::new();
        let mut flush_join_set = JoinSet::new();
        // let mut partitioned_flush_result_locked = partitioned_flush_result.lock().await;
        while let Some(batch_result) = data.next().await {
            match batch_result {
                Ok(batch) => {
                    debug!("write record_batch with {} rows", batch.num_rows());
                    let columnar_values =
                        get_columnar_values(&batch, range_partitions.clone())?;
                    let partition_desc =
                        columnar_values_to_partition_desc(&columnar_values);
                    let partition_sub_path =
                        columnar_values_to_sub_path(&columnar_values);
                    let batch_excluding_range =
                        batch.project(&schema_projection_excluding_range)?;

                    let file_absolute_path = format!(
                        "{}{}part-{}_{:0>4}.parquet",
                        io_config_builder.prefix(),
                        partition_sub_path,
                        write_id,
                        partition
                    );

                    if !partitioned_writer.contains_key(&partition_desc) {
                        // TODO only files diff
                        let config = io_config_builder
                            .clone()
                            .with_files(vec![file_absolute_path])
                            .build();

                        let writer = MultiPartAsyncWriter::try_new_with_context(
                            &config,
                            task_ctx.clone(),
                        )
                        .await?;
                        partitioned_writer
                            .insert(partition_desc.clone(), Box::new(writer));
                    }

                    if let Some(async_writer) =
                        partitioned_writer.get_mut(&partition_desc)
                    {
                        async_writer
                            .write_record_batch(batch_excluding_range)
                            .await?;
                    }
                }
                // received abort signal
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        if let Some(e) = err {
            error!("partitioned writer write error: {:?}", e);
            for (_, writer) in partitioned_writer.into_iter() {
                match writer.abort_and_close().await {
                    Ok(_) => match e {
                        DataFusionError::Internal(ref err_msg)
                            if err_msg == "external abort" =>
                        {
                            debug!("External abort signal received")
                        }
                        _ => return Err(e.into()),
                    },
                    Err(abort_err) => return Err(abort_err),
                }
            }
            Ok(flush_join_set)
        } else {
            for (partition_desc, writer) in partitioned_writer.into_iter() {
                flush_join_set.spawn(async move {
                    let writer_flush_results = writer.flush_and_close().await?;
                    info!(
                        "Flushed writer {:?}",
                        writer_flush_results
                            .iter()
                            .map(|o| {
                                (
                                    o.file_path.clone(),
                                    o.object_meta.size,
                                    o.file_meta.file_metadata().num_rows(),
                                    o.file_meta.num_row_groups(),
                                )
                            })
                            .collect::<Vec<_>>()
                    );
                    Ok(writer_flush_results
                        .into_iter()
                        .map(|mut output| {
                            output.partition_desc = partition_desc.clone();
                            output
                        })
                        .collect::<Vec<_>>())
                });
            }
            Ok(flush_join_set)
        }
    }

    async fn await_and_summary(
        tasks: Vec<SpawnedTask<NestedFlushOutputResult>>,
    ) -> Result<Vec<FlushOutput>> {
        let output = futures::stream::iter(tasks)
            .then(|task| async {
                let join_set = task.await??;
                Ok::<JoinSet<_>, Report>(join_set)
            })
            .map_ok(|mut js| {
                futures::stream::poll_fn(move |cx| match js.poll_join_next(cx) {
                    std::task::Poll::Ready(Some(Ok(res))) => {
                        std::task::Poll::Ready(Some(res))
                    }
                    std::task::Poll::Ready(Some(Err(e))) => {
                        std::task::Poll::Ready(Some(Err(report!(e).into_dynamic())))
                    }
                    std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                    std::task::Poll::Pending => std::task::Poll::Pending,
                })
            })
            .try_flatten_unordered(Some(8)) // io_bound task may increase this
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        Ok(output)
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for PartitioningAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(err) = &self.err {
            bail!("Already failed by {}", err)
        }

        let memory_size = get_batch_memory_size(&batch)? as u64;
        let send_result = self.sorter_sender.send(Ok(batch)).await;
        self.buffered_size += memory_size;
        match send_result {
            Ok(_) => Ok(()),
            // channel has been closed, indicating error happened during sort write
            Err(e) => {
                error!("Error sending record batch to sorter: {:?}", e);
                if let Some(task) = self.spawned_task.take() {
                    let result = task
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    self.err = result.err();
                    Err(report!("by {:?}", self.err))
                } else {
                    self.err = Some(e.into());
                    Err(report!("by {:?}", self.err))
                }
            }
        }
    }

    async fn flush_and_close(self: Box<Self>) -> Result<Vec<FlushOutput>> {
        if let Some(join_handle) = self.spawned_task {
            let sender = self.sorter_sender;
            drop(sender);
            join_handle.await?
        } else {
            bail!("aborted, cannot flush")
        }
    }

    async fn abort_and_close(self: Box<Self>) -> Result<()> {
        if let Some(join_handle) = self.spawned_task {
            let sender = self.sorter_sender;
            // send abort signal to the task
            sender
                .send(Err(DataFusionError::Internal("external abort".to_string())))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            drop(sender);
            let _ = join_handle
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(())
        } else {
            // previous error has already aborted writer
            Ok(())
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn buffered_size(&self) -> u64 {
        self.buffered_size
    }
}
