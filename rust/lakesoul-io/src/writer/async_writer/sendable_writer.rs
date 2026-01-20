// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of async version of [`crate::sync_writer::SyncSendableMutableLakeSoulWriter`].

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::DataFusionError;
use rootcause::report;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

use crate::{
    Result,
    config::LakeSoulIOConfig,
    helpers::get_batch_memory_size,
    session::LakeSoulIOSession,
    writer::{
        SendableWriter,
        async_writer::{AsyncBatchWriter, FlushOutput},
        create_writer,
    },
};

pub struct AsyncSendableMutableLakeSoulWriter {
    schema: SchemaRef,
    io_session: Arc<LakeSoulIOSession>,
    in_progress: Option<Arc<Mutex<SendableWriter>>>,
    flush_results: Vec<FlushOutput>,
}

impl AsyncSendableMutableLakeSoulWriter {
    pub async fn from_io_config(io_config: LakeSoulIOConfig) -> Result<Self> {
        let io_session = Arc::new(LakeSoulIOSession::try_new(io_config)?);
        Self::try_new(io_session).await
    }
    pub async fn try_new(io_session: Arc<LakeSoulIOSession>) -> Result<Self> {
        let mut io_config = io_session.io_config().clone();
        let writer = create_writer(io_session.clone()).await?;
        let schema = writer.schema();

        if let Some(mem_limit) = io_config.mem_limit() {
            io_config.max_file_size = Some(mem_limit as u64);
            info!("Set max file size to {:?}", io_config.max_file_size);
        }
        let io_session = Arc::new(io_session.with_io_config(io_config));

        Ok(Self {
            in_progress: Some(Arc::new(Mutex::new(writer))),
            schema,
            io_session,
            flush_results: vec![],
        })
    }

    #[instrument(skip(self, record_batch))]
    async fn write_batch_async(
        &mut self,
        record_batch: RecordBatch,
        do_spill: bool,
    ) -> Result<()> {
        debug!(record_batch_row=?record_batch.num_rows(), "write_batch_async");

        if let Some(max_file_size) = self.io_session.io_config().max_file_size {
            let in_progress_writer = match &mut self.in_progress {
                Some(writer) => writer,
                x => x.insert(Arc::new(Mutex::new(
                    create_writer(self.io_session.clone()).await?,
                ))),
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
                    Box::pin(self.write_batch_async(a, true)).await?;
                    return Box::pin(self.write_batch_async(b, false)).await;
                }
            }

            guard.write_record_batch(record_batch).await?;

            if do_spill {
                info!(
                    "Flushing writer with cumulated size: {}",
                    guard.buffered_size()
                );
                drop(guard);
                if let Some(writer) = self.in_progress.take() {
                    let inner_writer = Arc::try_unwrap(writer)
                        .map_err(|_| report!("Cannot get ownership of inner writer"))?;
                    let writer = inner_writer.into_inner();
                    let results = writer.flush_and_close().await?;
                    self.flush_results.extend(results);
                }
            }
            Ok(())
        } else if let Some(inner_writer) = &self.in_progress {
            let inner_writer = inner_writer.clone();
            let mut writer = inner_writer.lock().await;
            writer.write_record_batch(record_batch).await
        } else {
            Err(report!("Invalid state of inner writer"))
        }
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for AsyncSendableMutableLakeSoulWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.write_batch_async(batch, false).await
    }

    async fn flush_and_close(self: Box<Self>) -> Result<Vec<FlushOutput>> {
        debug!("{:?}", self.flush_results);
        if let Some(inner_writer) = self.in_progress {
            let inner_writer = Arc::try_unwrap(inner_writer).map_err(|_| {
                DataFusionError::Internal(
                    "Cannot get ownership of inner writer".to_string(),
                )
            })?;
            let writer = inner_writer.into_inner();
            debug!("writer schema: {:?}", writer.schema());
            let mut results = writer.flush_and_close().await?;
            results.extend(self.flush_results);
            Ok(results)
        } else {
            Ok(self.flush_results)
        }
    }

    async fn abort_and_close(self: Box<Self>) -> Result<()> {
        if let Some(inner_writer) = self.in_progress {
            let inner_writer = Arc::try_unwrap(inner_writer)
                .map_err(|_| report!("Cannot get ownership of inner writer"))?;
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
