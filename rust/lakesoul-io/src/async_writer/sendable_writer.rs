// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of async version of [`crate::sync_writer::SyncSendableMutableLakeSoulWriter`].

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

use crate::{
    async_writer::{AsyncBatchWriter, WriterFlushResult},
    helpers::get_batch_memory_size,
    lakesoul_io_config::LakeSoulIOConfig,
    lakesoul_writer::{SendableWriter, create_writer},
};

pub struct AsyncSendableMutableLakeSoulWriter {
    schema: SchemaRef,
    config: LakeSoulIOConfig,
    in_progress: Option<Arc<Mutex<SendableWriter>>>,
    flush_results: WriterFlushResult,
}

impl AsyncSendableMutableLakeSoulWriter {
    pub async fn try_new(mut config: LakeSoulIOConfig) -> Result<Self> {
        let writer = create_writer(config.clone()).await?;
        let schema = writer.schema();

        if let Some(mem_limit) = config.mem_limit() {
            if config.use_dynamic_partition {
                config.max_file_size = Some((mem_limit as f64 * 0.15) as u64);
            } else if !config.primary_keys.is_empty() && !config.keep_ordering() {
                config.max_file_size = Some((mem_limit as f64 * 0.2) as u64);
            }
        }

        Ok(Self {
            in_progress: Some(Arc::new(Mutex::new(writer))),
            schema,
            config,
            flush_results: vec![],
        })
    }

    #[async_recursion::async_recursion]
    async fn write_batch_async(
        &mut self,
        record_batch: RecordBatch,
        do_spill: bool,
    ) -> Result<()> {
        debug!(record_batch_row=?record_batch.num_rows(), do_spill=?do_spill, "write_batch_async");
        let config = self.config.clone();

        if let Some(max_file_size) = self.config.max_file_size {
            let in_progress_writer = match &mut self.in_progress {
                Some(writer) => writer,
                x => x.insert(Arc::new(Mutex::new(create_writer(config).await?))),
            };
            let mut guard = in_progress_writer.lock().await;

            let batch_memory_size = get_batch_memory_size(&record_batch)? as u64;
            let batch_rows = record_batch.num_rows() as u64;

            if !do_spill && guard.buffered_size() + batch_memory_size > max_file_size {
                let to_write = (batch_rows * (max_file_size - guard.buffered_size()))
                    / batch_memory_size;
                if to_write + 1 < batch_rows {
                    let to_write = to_write as usize + 1;
                    let a = record_batch.slice(0, to_write);
                    let b =
                        record_batch.slice(to_write, record_batch.num_rows() - to_write);
                    drop(guard);
                    self.write_batch_async(a, true).await?;
                    return self.write_batch_async(b, false).await;
                }
            }

            let rb_schema = record_batch.schema();
            guard.write_record_batch(record_batch).await.map_err(|e| {
                DataFusionError::Internal(format!(
                    "err={}, config={:?}, batch_schema={:?}",
                    e, self.config, rb_schema
                ))
            })?;

            if do_spill {
                debug!("spilling writer with size: {}", guard.buffered_size());
                drop(guard);
                if let Some(writer) = self.in_progress.take() {
                    let inner_writer = Arc::try_unwrap(writer).map_err(|_| {
                        DataFusionError::Internal(
                            "Cannot get ownership of inner writer".to_string(),
                        )
                    })?;
                    let writer = inner_writer.into_inner();
                    let results = writer.flush_and_close().await.map_err(|e| {
                        DataFusionError::Internal(format!(
                            "err={}, config={:?}, batch_schema={:?}",
                            e, self.config, rb_schema
                        ))
                    })?;
                    self.flush_results.extend(results);
                }
            }
            Ok(())
        } else if let Some(inner_writer) = &self.in_progress {
            let inner_writer = inner_writer.clone();
            let mut writer = inner_writer.lock().await;
            writer.write_record_batch(record_batch).await
        } else {
            Err(DataFusionError::Internal(
                "Invalid state of inner writer".to_string(),
            ))
        }
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for AsyncSendableMutableLakeSoulWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.write_batch_async(batch, false).await
    }

    async fn flush_and_close(self: Box<Self>) -> Result<WriterFlushResult> {
        debug!("flush_and_close: {:?}", self.flush_results);
        if let Some(inner_writer) = self.in_progress {
            let inner_writer = Arc::try_unwrap(inner_writer).map_err(|_| {
                DataFusionError::Internal(
                    "Cannot get ownership of inner writer".to_string(),
                )
            })?;
            let writer = inner_writer.into_inner();
            debug!("writer schema: {:?}", writer.schema());
            let mut results = writer.flush_and_close().await.map_err(|e| {
                DataFusionError::Internal(format!("err={}, config={:?}", e, self.config))
            })?;
            results.extend(self.flush_results);
            Ok(results)
        } else {
            Ok(self.flush_results)
        }
    }

    async fn abort_and_close(self: Box<Self>) -> Result<()> {
        if let Some(inner_writer) = self.in_progress {
            let inner_writer = Arc::try_unwrap(inner_writer).map_err(|_| {
                DataFusionError::Internal(
                    "Cannot get ownership of inner writer".to_string(),
                )
            })?;
            let writer = inner_writer.into_inner();
            writer.abort_and_close().await
        } else {
            Ok(())
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn buffered_size(&self) -> u64 {
        if let Some(writer) = &self.in_progress {
            futures::executor::block_on(async { writer.lock().await.buffered_size() })
        } else {
            0
        }
    }
}
