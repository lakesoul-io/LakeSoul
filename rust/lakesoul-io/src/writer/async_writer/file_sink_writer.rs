// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{any::Any, collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::DataFusionError;
use datafusion_common_runtime::SpawnedTask;
use datafusion_datasource::file_sink_config::FileSink;
use datafusion_datasource_parquet::ParquetSink;
use datafusion_execution::{
    TaskContext, memory_pool::UnboundedMemoryPool, runtime_env::RuntimeEnvBuilder,
};
use datafusion_physical_plan::{metrics::MetricsSet, stream::RecordBatchReceiverStream};
use datafusion_session::Session;
use object_store::{ObjectStoreExt, path::Path};
use rootcause::{bail, report};
use tokio::sync::mpsc::Sender;

use crate::{
    Result,
    constant::DEFAULT_PARTITION_DESC,
    file_format::{PhysicalFormat, vortex::VortexSink},
    helpers::{
        FileExistCols, get_batch_memory_size,
        transform::{uniform_record_batch, uniform_schema},
    },
    session::LakeSoulIOSession,
};

use super::{AsyncBatchWriter, FlushOutput};

pub struct FileSinkWriter {
    physical_format: PhysicalFormat,
    sink: Arc<dyn FileSink>,
    schema: SchemaRef,
    sink_schema: SchemaRef,
    io_session: Arc<LakeSoulIOSession>,
    sender: Option<Sender<Result<RecordBatch, DataFusionError>>>,
    task: Option<SpawnedTask<Result<u64>>>,
    buffered_size: u64,
    flush_results: Option<Vec<FlushOutput>>,
}

impl FileSinkWriter {
    pub fn try_new(
        sink: Arc<dyn FileSink>,
        physical_format: PhysicalFormat,
        io_session: Arc<LakeSoulIOSession>,
    ) -> Result<Self> {
        let schema = uniform_schema(io_session.io_config().target_schema.0.clone());
        let sink_schema = Arc::clone(sink.schema());
        let receiver_stream_builder = RecordBatchReceiverStream::builder(
            Arc::clone(&sink_schema),
            io_session.io_config().receiver_capacity,
        );
        let sender = receiver_stream_builder.tx();
        let data = receiver_stream_builder.build();
        let task_ctx = Self::sink_task_ctx(io_session.task_ctx())?;
        let sink_for_task = Arc::clone(&sink);

        let task = SpawnedTask::spawn(async move {
            match FileSink::write_all(sink_for_task.as_ref(), data, &task_ctx).await {
                Ok(row_count) => Ok(row_count),
                Err(DataFusionError::Internal(msg)) if msg == "external abort" => Ok(0),
                Err(e) => Err(e.into()),
            }
        });

        Ok(Self {
            physical_format,
            sink,
            schema,
            sink_schema,
            io_session,
            sender: Some(sender),
            task: Some(task),
            buffered_size: 0,
            flush_results: None,
        })
    }

    // use other memory pool(unbounded) for sink task
    fn sink_task_ctx(task_ctx: Arc<TaskContext>) -> Result<Arc<TaskContext>> {
        let runtime_env = task_ctx.runtime_env();
        let sink_runtime_env = RuntimeEnvBuilder::from_runtime_env(runtime_env.as_ref())
            .with_memory_pool(Arc::new(UnboundedMemoryPool::default()))
            .build_arc()?;

        Ok(Arc::new(TaskContext::new(
            task_ctx.task_id(),
            task_ctx.session_id(),
            task_ctx.session_config().clone(),
            task_ctx.scalar_functions().clone(),
            task_ctx.higher_order_functions().clone(),
            task_ctx.aggregate_functions().clone(),
            task_ctx.window_functions().clone(),
            sink_runtime_env,
        )))
    }

    fn project_to_sink_schema(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if batch.schema().fields() == self.sink_schema.fields() {
            return Ok(batch);
        }

        let input_schema = batch.schema();
        let projection = self
            .sink_schema
            .fields()
            .iter()
            .map(|field| {
                input_schema.index_of(field.name()).map_err(|_| {
                    report!(
                        "Failed to find writer column {} in input batch",
                        field.name()
                    )
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(batch.project(&projection)?)
    }

    fn downcast_sink<T: FileSink + 'static>(&self) -> Option<&T> {
        let sink = self.sink.as_ref() as &dyn Any;
        sink.downcast_ref::<T>()
    }

    async fn collect_flush_outputs(&self) -> Result<Vec<FlushOutput>> {
        match self.physical_format {
            PhysicalFormat::Parquet => {
                let sink = self
                    .downcast_sink::<ParquetSink>()
                    .ok_or(report!("downcast ParquetSink failed"))?;
                self.collect_parquet_outputs(sink).await
            }
            PhysicalFormat::Vortex => {
                let sink = self
                    .downcast_sink::<VortexSink>()
                    .ok_or(report!("downcast VortexSink failed"))?;
                self.collect_vortex_outputs(sink).await
            }
        }
    }

    async fn collect_parquet_outputs(
        &self,
        sink: &ParquetSink,
    ) -> Result<Vec<FlushOutput>> {
        let mut written = sink.written().into_iter().collect::<Vec<_>>();
        written.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));

        let object_store = self
            .io_session
            .task_ctx()
            .runtime_env()
            .object_store(&self.sink.config().object_store_url)?;
        let single_file_path = self.single_file_path();

        futures::future::try_join_all(written.into_iter().map(|(path, metadata)| {
            let object_store = Arc::clone(&object_store);
            let file_path = single_file_path
                .clone()
                .unwrap_or_else(|| self.path_to_url_string(&path));
            async move {
                let object_meta = object_store.head(&path).await?;
                let file_exist_cols = metadata.get_file_exists_cols();
                let other_info = HashMap::from([(
                    String::from("num_row_groups"),
                    metadata.num_row_groups().to_string(),
                )]);
                Ok(FlushOutput {
                    partition_desc: DEFAULT_PARTITION_DESC.to_string(),
                    file_path,
                    object_meta,
                    file_exist_cols,
                    row_count: metadata.file_metadata().num_rows() as usize,
                    other_info,
                })
            }
        }))
        .await
    }

    async fn collect_vortex_outputs(
        &self,
        sink: &VortexSink,
    ) -> Result<Vec<FlushOutput>> {
        let mut written = sink.written().into_iter().collect::<Vec<_>>();
        written.sort_by(|(left, _), (right, _)| left.as_ref().cmp(right.as_ref()));

        let object_store = self
            .io_session
            .task_ctx()
            .runtime_env()
            .object_store(&self.sink.config().object_store_url)?;
        let single_file_path = self.single_file_path();

        futures::future::try_join_all(written.into_iter().map(|(path, footer)| {
            let object_store = Arc::clone(&object_store);
            let file_path = single_file_path
                .clone()
                .unwrap_or_else(|| self.path_to_url_string(&path));
            async move {
                let object_meta = object_store.head(&path).await?;
                let file_exist_cols = footer.get_file_exists_cols();
                let other_info = HashMap::from([(
                    String::from("physical_format"),
                    String::from("vortex"),
                )]);
                Ok(FlushOutput {
                    partition_desc: DEFAULT_PARTITION_DESC.to_string(),
                    file_path,
                    object_meta,
                    file_exist_cols,
                    row_count: footer.row_count() as usize,
                    other_info,
                })
            }
        }))
        .await
    }

    fn single_file_path(&self) -> Option<String> {
        (self.sink.config().table_paths.len() == 1)
            .then(|| self.sink.config().original_url.clone())
            .filter(|path| !path.is_empty())
    }

    fn path_to_url_string(&self, path: &Path) -> String {
        let object_store_url = self
            .sink
            .config()
            .object_store_url
            .as_str()
            .trim_end_matches('/');
        if object_store_url == "file:" || object_store_url == "file://" {
            format!("file://{}", path)
        } else {
            format!("{}/{}", object_store_url, path)
        }
    }

    async fn wait_for_task(&mut self) -> Result<()> {
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }

    async fn abort_output_path(&self) {
        let Some(path) = self
            .sink
            .config()
            .table_paths
            .first()
            .map(|url| url.prefix())
        else {
            return;
        };
        let Ok(object_store) = self
            .io_session
            .task_ctx()
            .runtime_env()
            .object_store(&self.sink.config().object_store_url)
        else {
            return;
        };
        if let Err(e) = object_store.delete(path).await {
            debug!("Failed to delete aborted writer output {}: {}", path, e);
        }
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for FileSinkWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if self.flush_results.is_some() {
            bail!("FileSinkWriter already flushed")
        }

        let batch = uniform_record_batch(self.project_to_sink_schema(batch)?)?;
        self.buffered_size += get_batch_memory_size(&batch)? as u64;
        let sender = self
            .sender
            .as_ref()
            .ok_or(report!("FileSinkWriter is closed"))?;
        match sender.send(Ok(batch)).await {
            Ok(_) => Ok(()),
            Err(send_err) => {
                if let Some(task) = self.task.take() {
                    task.await??;
                }
                Err(report!("FileSinkWriter write failed: {send_err}"))
            }
        }
    }

    async fn flush(&mut self) -> Result<Vec<FlushOutput>> {
        if let Some(results) = &self.flush_results {
            return Ok(results.clone());
        }

        let sender = self
            .sender
            .take()
            .ok_or(report!("already flushed or aborted"))?;
        drop(sender);
        self.wait_for_task().await?;
        let results = self.collect_flush_outputs().await?;
        self.flush_results = Some(results.clone());
        Ok(results)
    }

    async fn close(self: Box<Self>) -> Result<()> {
        Ok(())
    }

    async fn flush_and_close(mut self: Box<Self>) -> Result<Vec<FlushOutput>> {
        let output = self.flush().await?;
        self.close().await?;
        Ok(output)
    }

    async fn abort(&mut self) -> Result<()> {
        if let Some(sender) = self.sender.take() {
            sender
                .send(Err(DataFusionError::Internal("external abort".to_string())))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            drop(sender);
        }

        if let Some(task) = self.task.take() {
            let _ = task.await?;
        }
        self.abort_output_path().await;
        Ok(())
    }

    async fn abort_and_close(mut self: Box<Self>) -> Result<()> {
        self.abort().await?;
        self.close().await
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn buffered_size(&self) -> u64 {
        self.buffered_size
    }

    fn io_session(&self) -> &Arc<LakeSoulIOSession> {
        &self.io_session
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.sink.metrics()
    }
}
