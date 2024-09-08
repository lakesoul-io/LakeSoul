// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::{SchemaRef, SortOptions};
use datafusion::{
    execution::TaskContext,
    physical_expr::{
        expressions::{col, Column},
        PhysicalSortExpr,
    },
    physical_plan::{
        projection::ProjectionExec,
        sorts::sort::SortExec,
        stream::RecordBatchReceiverStream,
        ExecutionPlan, Partitioning, PhysicalExpr,
    },
};
use datafusion_common::{DataFusionError, Result};

use rand::distributions::DistString;
use tokio::{
    sync::mpsc::Sender,
    task::JoinHandle,
};
use tokio_stream::StreamExt;
use tracing::debug;

use crate::{
    helpers::{columnar_values_to_partition_desc, columnar_values_to_sub_path, get_batch_memory_size, get_columnar_values},
    lakesoul_io_config::{create_session_context, LakeSoulIOConfig, LakeSoulIOConfigBuilder},
    repartition::RepartitionByRangeAndHashExec,
};

use super::{AsyncBatchWriter, WriterFlushResult, MultiPartAsyncWriter, ReceiverStreamExec};

// type PartitionedWriterInfo = Arc<Mutex<HashMap<String, Vec<WriterFlushResult>>>>;

/// Wrap the above async writer with a RepartitionExec to
/// dynamic repartitioning the batches before write to async writer
pub struct PartitioningAsyncWriter {
    schema: SchemaRef,
    sorter_sender: Sender<Result<RecordBatch>>,
    _partitioning_exec: Arc<dyn ExecutionPlan>,
    join_handle: Option<JoinHandle<WriterFlushResult>>,
    err: Option<DataFusionError>,
    buffered_size: u64,
}

impl PartitioningAsyncWriter {
    pub fn try_new(config: LakeSoulIOConfig) -> Result<Self> {
        let mut config = config.clone();
        let task_context = create_session_context(&mut config)?.task_ctx();

        let schema = config.target_schema.0.clone();
        let receiver_stream_builder = RecordBatchReceiverStream::builder(schema.clone(), 8);
        let tx = receiver_stream_builder.tx();
        let recv_exec = ReceiverStreamExec::new(receiver_stream_builder, schema.clone());

        let partitioning_exec = PartitioningAsyncWriter::get_partitioning_exec(recv_exec, config.clone())?;

        // launch one async task per *input* partition
        let mut join_handles = vec![];

        let write_id = rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

        // let partitioned_file_path_and_row_count = Arc::new(Mutex::new(HashMap::<String, (Vec<String>, u64)>::new()));
        for i in 0..partitioning_exec.output_partitioning().partition_count() {
            let sink_task = tokio::spawn(Self::pull_and_sink(
                partitioning_exec.clone(),
                i,
                task_context.clone(),
                config.clone().into(),
                Arc::new(config.range_partitions.clone()),
                write_id.clone()
            ));
            // // In a separate task, wait for each input to be done
            // // (and pass along any errors, including panic!s)
            join_handles.push(sink_task);
        }

        let join_handle = tokio::spawn(Self::await_and_summary(
            join_handles,
            // partitioned_file_path_and_row_count,
        ));

        Ok(Self {
            schema,
            sorter_sender: tx,
            _partitioning_exec: partitioning_exec,
            join_handle: Some(join_handle),
            err: None,
            buffered_size: 0,
        })
    }

    fn get_partitioning_exec(input: ReceiverStreamExec, config: LakeSoulIOConfig) -> Result<Arc<dyn ExecutionPlan>> {
        let sort_exprs: Vec<PhysicalSortExpr> = config
            .range_partitions
            .iter()
            .chain(config.primary_keys.iter())
            // add aux sort cols to sort expr
            .chain(config.aux_sort_cols.iter())
            .map(|pk| {
                let col = Column::new_with_schema(pk.as_str(), &config.target_schema.0)?;
                Ok(PhysicalSortExpr {
                    expr: Arc::new(col),
                    options: SortOptions::default(),
                })
            })
            .collect::<Result<Vec<PhysicalSortExpr>>>()?;
        if sort_exprs.is_empty() {
            return Ok(Arc::new(input));
        }

        let sort_exec = Arc::new(SortExec::new(sort_exprs, Arc::new(input)));

        // see if we need to prune aux sort cols
        let sort_exec: Arc<dyn ExecutionPlan> = if config.aux_sort_cols.is_empty() {
            sort_exec
        } else {
            // O(nm), n = number of target schema fields, m = number of aux sort cols
            let proj_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = config
                .target_schema
                .0
                .fields
                .iter()
                .filter_map(|f| {
                    if config.aux_sort_cols.contains(f.name()) {
                        // exclude aux sort cols
                        None
                    } else {
                        Some(col(f.name().as_str(), &config.target_schema.0).map(|e| (e, f.name().clone())))
                    }
                })
                .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>>>()?;
            Arc::new(ProjectionExec::try_new(proj_expr, sort_exec)?)
        };

        let exec_plan = if config.primary_keys.is_empty() && config.range_partitions.is_empty() {
            sort_exec
        } else {
            let sorted_schema = sort_exec.schema();

            let range_partitioning_expr: Vec<Arc<dyn PhysicalExpr>> = config
                .range_partitions
                .iter()
                .map(|col| {
                    let idx = sorted_schema.index_of(col.as_str())?;
                    Ok(Arc::new(Column::new(col.as_str(), idx)) as Arc<dyn PhysicalExpr>)
                })
                .collect::<Result<Vec<_>>>()?;

            let hash_partitioning_expr: Vec<Arc<dyn PhysicalExpr>> = config
                .primary_keys
                .iter()
                .map(|col| {
                    let idx = sorted_schema.index_of(col.as_str())?;
                    Ok(Arc::new(Column::new(col.as_str(), idx)) as Arc<dyn PhysicalExpr>)
                })
                .collect::<Result<Vec<_>>>()?;
            let hash_partitioning = Partitioning::Hash(hash_partitioning_expr, config.hash_bucket_num);

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
        context: Arc<TaskContext>,
        config_builder: LakeSoulIOConfigBuilder,
        range_partitions: Arc<Vec<String>>,
        write_id: String,
        // partitioned_flush_result: PartitionedWriterInfo,
    ) -> Result<Vec<JoinHandle<WriterFlushResult>>> {
        let mut data = input.execute(partition, context.clone())?;
        // O(nm), n = number of data fields, m = number of range partitions
        let schema_projection_excluding_range = data
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| match range_partitions.contains(field.name()) {
                true => None,
                false => Some(idx),
            })
            .collect::<Vec<_>>();

        let mut err = None;


        let mut partitioned_writer = HashMap::<String, Box<MultiPartAsyncWriter>>::new();
        let mut flush_join_handle_list = Vec::new();
        // let mut partitioned_flush_result_locked = partitioned_flush_result.lock().await;
        while let Some(batch_result) = data.next().await {
            match batch_result {
                Ok(batch) => {
                    debug!("write record_batch with {} rows", batch.num_rows());
                    let columnar_values = get_columnar_values(&batch, range_partitions.clone())?;
                    let partition_desc = columnar_values_to_partition_desc(&columnar_values);
                    let partition_sub_path = columnar_values_to_sub_path(&columnar_values);
                    let batch_excluding_range = batch.project(&schema_projection_excluding_range)?;

                    let file_absolute_path = format!(
                        "{}{}part-{}_{:0>4}.parquet",
                        config_builder.prefix(),
                        partition_sub_path,
                        write_id,
                        partition
                    );

                    if !partitioned_writer.contains_key(&partition_desc) {
                        let mut config = config_builder.clone().with_files(vec![file_absolute_path]).build();

                        let writer = MultiPartAsyncWriter::try_new_with_context(&mut config, context.clone()).await?;
                        partitioned_writer.insert(partition_desc.clone(), Box::new(writer));
                    }

                    if let Some(async_writer) = partitioned_writer.get_mut(&partition_desc) {
                        // row_count += batch_excluding_range.num_rows();
                        async_writer.write_record_batch(batch_excluding_range).await?;
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
            for (_, writer) in partitioned_writer.into_iter() {
                match writer.abort_and_close().await {
                    Ok(_) => match e {
                        DataFusionError::Internal(ref err_msg) if err_msg == "external abort" => (),
                        _ => return Err(e),
                    },
                    Err(abort_err) => {
                        return Err(DataFusionError::Internal(format!(
                            "Abort failed {:?}, previous error {:?}",
                            abort_err, e
                        )))
                    }
                }
            }
            Ok(flush_join_handle_list)
        } else {
            
            for (partition_desc, writer) in partitioned_writer.into_iter() {
                
                let flush_result = tokio::spawn(async move {
                    let writer_flush_results =writer.flush_and_close().await?;
                    Ok(
                        writer_flush_results.into_iter().map(
                            |(_, path, file_metadata)| 
                            {
                                (partition_desc.clone(), path, file_metadata)
                            }
                        ).collect::<Vec<_>>()
                    )
                });
                flush_join_handle_list.push(flush_result);
            }
            Ok(flush_join_handle_list)
        }
    }

    async fn await_and_summary(
        join_handles: Vec<JoinHandle<Result<Vec<JoinHandle<WriterFlushResult>>>>>,
        // partitioned_file_path_and_row_count: PartitionedWriterInfo,
    ) -> WriterFlushResult {
        let mut flatten_results = Vec::new();
        let results = futures::future::join_all(join_handles).await;
        for result in results {
            match result {
                Ok(Ok(part_join_handles)) => {
                    let part_results = futures::future::join_all(part_join_handles).await;
                    for part_result in part_results {
                        match part_result {
                            Ok(Ok(flatten_part_result)) => {
                                flatten_results.extend(flatten_part_result);
                            }
                            Ok(Err(e)) => return Err(DataFusionError::Execution(format!("{}", e))),
                            Err(e) => return Err(DataFusionError::Execution(format!("{}", e))),
                        }
                    }
                }
                Ok(Err(e)) => return Err(DataFusionError::Execution(format!("{}", e))),
                Err(e) => return Err(DataFusionError::Execution(format!("{}", e))),
            }
        }
        Ok(flatten_results)
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for PartitioningAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // arrow_cast::pretty::print_batches(&[batch.clone()]);
        if let Some(err) = &self.err {
            return Err(DataFusionError::Internal(format!(
                "PartitioningAsyncWriter already failed with error {:?}",
                err
            )));
        }

        let memory_size = get_batch_memory_size(&batch)? as u64;
        let send_result = self.sorter_sender.send(Ok(batch)).await;
        self.buffered_size += memory_size;
        match send_result {
            Ok(_) => Ok(()),
            // channel has been closed, indicating error happened during sort write
            Err(e) => {
                if let Some(join_handle) = self.join_handle.take() {
                    let result = join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    self.err = result.err();
                    Err(DataFusionError::Internal(format!(
                        "Write to PartitioningAsyncWriter failed: {:?}",
                        self.err
                    )))
                } else {
                    self.err = Some(DataFusionError::External(Box::new(e)));
                    Err(DataFusionError::Internal(format!(
                        "Write to PartitioningAsyncWriter failed: {:?}",
                        self.err
                    )))
                }
            }
        }
    }

    async fn flush_and_close(self: Box<Self>) -> WriterFlushResult {
        if let Some(join_handle) = self.join_handle {
            let sender = self.sorter_sender;
            drop(sender);
            join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            Err(DataFusionError::Internal(
                "PartitioningAsyncWriter has been aborted, cannot flush".to_string(),
            ))
        }
    }

    async fn abort_and_close(self: Box<Self>) -> Result<()> {
        if let Some(join_handle) = self.join_handle {
            let sender = self.sorter_sender;
            // send abort signal to the task
            sender
                .send(Err(DataFusionError::Internal("external abort".to_string())))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            drop(sender);
            let _ = join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?;
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
