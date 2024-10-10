// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{SchemaRef, SortOptions};
use datafusion::{
    physical_expr::{
        expressions::{col, Column},
        PhysicalSortExpr,
    },
    physical_plan::{
        projection::ProjectionExec, sorts::sort::SortExec, stream::RecordBatchReceiverStream, ExecutionPlan,
        PhysicalExpr,
    },
};
use datafusion_common::{DataFusionError, Result};
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use tokio_stream::StreamExt;

use crate::{helpers::get_batch_memory_size, lakesoul_io_config::LakeSoulIOConfig};

use super::{AsyncBatchWriter, MultiPartAsyncWriter, ReceiverStreamExec, WriterFlushResult};

/// Wrap the above async writer with a SortExec to
/// sort the batches before write to async writer
pub struct SortAsyncWriter {
    schema: SchemaRef,
    sorter_sender: Sender<Result<RecordBatch>>,
    _sort_exec: Arc<dyn ExecutionPlan>,
    join_handle: Option<JoinHandle<Result<WriterFlushResult>>>,
    err: Option<DataFusionError>,
    buffered_size: u64,
}

impl SortAsyncWriter {
    pub fn try_new(
        async_writer: MultiPartAsyncWriter,
        config: LakeSoulIOConfig,
        // runtime: Arc<Runtime>,
    ) -> Result<Self> {
        // let _ = runtime.enter();
        let schema = config.target_schema.0.clone();
        let receiver_stream_builder = RecordBatchReceiverStream::builder(schema.clone(), 8);
        let tx = receiver_stream_builder.tx();
        let recv_exec = ReceiverStreamExec::new(receiver_stream_builder, schema.clone());

        let sort_exprs: Vec<PhysicalSortExpr> = config
            .primary_keys
            .iter()
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
        let sort_exec = Arc::new(SortExec::new(sort_exprs, Arc::new(recv_exec)));

        // see if we need to prune aux sort cols
        let exec_plan: Arc<dyn ExecutionPlan> = if config.aux_sort_cols.is_empty() {
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

        let mut sorted_stream = exec_plan.execute(0, async_writer.task_ctx())?;

        let mut async_writer = Box::new(async_writer);
        let join_handle = tokio::task::spawn(async move {
            let mut err = None;
            while let Some(batch) = sorted_stream.next().await {
                match batch {
                    Ok(batch) => {
                        async_writer.write_record_batch(batch).await?;
                    }
                    // received abort signal
                    Err(e) => {
                        err = Some(e);
                        break;
                    }
                }
            }
            if let Some(e) = err {
                let result = async_writer.abort_and_close().await;
                match result {
                    Ok(_) => match e {
                        DataFusionError::Internal(ref err_msg) if err_msg == "external abort" => Ok(vec![]),
                        _ => Err(e),
                    },
                    Err(abort_err) => Err(DataFusionError::Internal(format!(
                        "Abort failed {:?}, previous error {:?}",
                        abort_err, e
                    ))),
                }
            } else {
                async_writer.flush_and_close().await
            }
        });

        Ok(SortAsyncWriter {
            schema,
            sorter_sender: tx,
            _sort_exec: exec_plan,
            join_handle: Some(join_handle),
            err: None,
            buffered_size: 0,
        })
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for SortAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(err) = &self.err {
            return Err(DataFusionError::Internal(format!(
                "SortAsyncWriter already failed with error {:?}",
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
                        "Write to SortAsyncWriter failed: {:?}",
                        self.err
                    )))
                } else {
                    self.err = Some(DataFusionError::External(Box::new(e)));
                    Err(DataFusionError::Internal(format!(
                        "Write to SortAsyncWriter failed: {:?}",
                        self.err
                    )))
                }
            }
        }
    }

    async fn flush_and_close(self: Box<Self>) -> Result<WriterFlushResult> {
        if let Some(join_handle) = self.join_handle {
            let sender = self.sorter_sender;
            drop(sender);
            join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            Err(DataFusionError::Internal(
                "SortAsyncWriter has been aborted, cannot flush".to_string(),
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
