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

use crate::blob::{self, BlobPolicy, PackBuffer};
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
    /// Blob columns of this leaf file, keyed by column name.
    blob_columns: HashMap<String, BlobPolicy>,
    /// Pack sinks per blob column; each rolls to a new pack once the policy's
    /// ``pack_target_bytes`` is reached.
    pack_sinks: HashMap<String, PackSinkState>,
}

/// Pack sink for one blob column.
///
/// Values are appended to the current pack; once the next value would push it
/// over ``pack_target_bytes`` the current pack is finished and a new one is
/// started (``target_bytes == 0`` keeps a single pack).
struct PackSinkState {
    column: String,
    table_dir: String,
    target_bytes: u64,
    current_path: String,
    current: PackBuffer,
    finished: Vec<(String, Vec<u8>)>,
}

impl PackSinkState {
    fn new(column: &str, table_dir: &str, target_bytes: u64) -> Self {
        Self {
            column: column.to_string(),
            table_dir: table_dir.to_string(),
            target_bytes,
            current_path: Self::next_path(table_dir, column),
            current: PackBuffer::default(),
            finished: Vec::new(),
        }
    }

    fn next_path(table_dir: &str, column: &str) -> String {
        format!(
            "{table_dir}/_blob/{column}/{}.blob",
            uuid::Uuid::new_v4().simple()
        )
    }

    /// Finished packs plus the in-progress one, in write order.
    fn take_packs(&mut self) -> Vec<(String, Vec<u8>)> {
        let mut packs = std::mem::take(&mut self.finished);
        if !self.current.is_empty() {
            let path = std::mem::replace(
                &mut self.current_path,
                Self::next_path(&self.table_dir, &self.column),
            );
            packs.push((path, std::mem::take(&mut self.current).into_data()));
        }
        packs
    }
}

impl blob::BlobPackSink for PackSinkState {
    fn append(&mut self, value: &[u8]) -> Result<blob::PackLocation> {
        if self.target_bytes > 0
            && !self.current.is_empty()
            && self.current.len() as u64 + value.len() as u64 > self.target_bytes
        {
            let path = std::mem::replace(
                &mut self.current_path,
                Self::next_path(&self.table_dir, &self.column),
            );
            self.finished
                .push((path, std::mem::take(&mut self.current).into_data()));
        }
        let (offset, length, crc) = self.current.append(value)?;
        Ok(blob::PackLocation {
            pack_path: self.current_path.clone(),
            offset,
            length,
            crc,
        })
    }
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

        let blob_columns = blob::parse_blob_policies(io_session.io_config().options())?;
        let pack_sinks = if blob_columns.is_empty() {
            HashMap::new()
        } else {
            let file_url = (sink.config().table_paths.len() == 1)
                .then(|| sink.config().original_url.clone())
                .filter(|path| !path.is_empty())
                .ok_or_else(|| {
                    report!("blob columns need a single-file writer to place pack files")
                })?;
            let table_dir = file_url
                .rsplit_once('/')
                .map(|(dir, _)| dir)
                .unwrap_or_default();
            blob_columns
                .iter()
                .map(|(column, policy)| {
                    (
                        column.clone(),
                        PackSinkState::new(column, table_dir, policy.pack_target_bytes),
                    )
                })
                .collect()
        };

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
            blob_columns,
            pack_sinks,
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
            PhysicalFormat::Vortex | PhysicalFormat::VortexCompact => {
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
        let physical_format = self.physical_format.to_string();

        futures::future::try_join_all(written.into_iter().map(|(path, metadata)| {
            let object_store = Arc::clone(&object_store);
            let physical_format = physical_format.clone();
            let file_path = single_file_path
                .clone()
                .unwrap_or_else(|| self.path_to_url_string(&path));
            async move {
                let object_meta = object_store.head(&path).await?;
                let file_exist_cols = metadata.get_file_exists_cols();
                let other_info = HashMap::from([
                    (
                        String::from("num_row_groups"),
                        metadata.num_row_groups().to_string(),
                    ),
                    (String::from("physical_format"), physical_format),
                ]);
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
        let physical_format = self.physical_format.to_string();

        futures::future::try_join_all(written.into_iter().map(|(path, footer)| {
            let object_store = Arc::clone(&object_store);
            let file_path = single_file_path
                .clone()
                .unwrap_or_else(|| self.path_to_url_string(&path));
            let physical_format = physical_format.clone();
            async move {
                let object_meta = object_store.head(&path).await?;
                let file_exist_cols = footer.get_file_exists_cols();
                let other_info =
                    HashMap::from([(String::from("physical_format"), physical_format)]);
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

    /// Strip the scheme (and, for object stores, the bucket) from a URL so the
    /// result is an object-store path. Local paths keep their leading slash.
    fn url_to_object_path(url: &str) -> String {
        if let Some((scheme, rest)) = url.split_once("://") {
            return if scheme == "file" {
                rest.to_string()
            } else {
                rest.split_once('/')
                    .map(|(_, path)| path.to_string())
                    .unwrap_or_default()
            };
        }
        // Hadoop-style single-slash URLs such as ``file:/tmp/x`` or
        // ``s3a:/bucket/key``.
        if let Some((scheme, rest)) = url.split_once(":/") {
            if scheme == "file" {
                // ``split_once(":/")`` consumes the separating slash.
                return format!("/{rest}");
            }
            let rest = rest.trim_start_matches('/');
            return rest
                .split_once('/')
                .map(|(_, path)| path.to_string())
                .unwrap_or_default();
        }
        url.to_string()
    }

    /// Serialize the `.blobref` sidecar payload.
    fn blobref_payload(packs: &[String]) -> Result<Vec<u8>> {
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "packs": packs,
        }))
        .map_err(|error| report!("failed to serialize blobref: {error}"))
    }

    /// Replace every blob column with its tagged encoding, spilling values over
    /// the policy threshold into the per-column pack buffer.
    fn encode_blob_columns(&mut self, mut batch: RecordBatch) -> Result<RecordBatch> {
        if self.blob_columns.is_empty() {
            return Ok(batch);
        }
        let columns: Vec<(String, BlobPolicy)> = self
            .blob_columns
            .iter()
            .map(|(column, policy)| (column.clone(), policy.clone()))
            .collect();
        for (column, policy) in columns {
            let Ok(index) = batch.schema().index_of(&column) else {
                continue;
            };
            let Some(sink) = self.pack_sinks.get_mut(&column) else {
                continue;
            };
            let array = batch.column(index).clone();
            let encoded = blob::encode_column_with_sink(array.as_ref(), &policy, sink)?;
            let mut columns: Vec<arrow_array::ArrayRef> = batch.columns().to_vec();
            columns[index] = encoded;
            batch = RecordBatch::try_new(batch.schema(), columns)
                .map_err(|error| report!("failed to rebuild blob batch: {error}"))?;
        }
        Ok(batch)
    }

    /// Upload the accumulated pack files and the `.blobref` sidecar.
    async fn write_blob_packs(&mut self) -> Result<()> {
        if self.blob_columns.is_empty() {
            return Ok(());
        }
        let Some(data_path) = self
            .sink
            .config()
            .table_paths
            .first()
            .map(|url| url.prefix().clone())
        else {
            return Ok(());
        };
        let object_store = self
            .io_session
            .task_ctx()
            .runtime_env()
            .object_store(&self.sink.config().object_store_url)?;
        let sinks = std::mem::take(&mut self.pack_sinks);
        let mut written: Vec<String> = Vec::new();
        for (_column, mut sink) in sinks {
            for (pack_path, data) in sink.take_packs() {
                object_store
                    .put(
                        &Path::from(Self::url_to_object_path(&pack_path)),
                        data.into(),
                    )
                    .await?;
                debug!("wrote blob pack {}", pack_path);
                written.push(pack_path);
            }
        }
        written.sort();
        written.dedup();
        let payload = Self::blobref_payload(&written)?;
        let sidecar = Path::from(format!("{data_path}.blobref"));
        object_store.put(&sidecar, payload.into()).await?;
        debug!("wrote blob reference {}", sidecar);
        Ok(())
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
        let batch = self.encode_blob_columns(batch)?;
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
        self.write_blob_packs().await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::BlobPackSink;

    #[test]
    fn blobref_payload_escapes_paths() {
        let packs = vec![
            "s3://bucket/t\"a\\b/_blob/c/u.blob".to_string(),
            "s3://bucket/t/_blob/c/u2.blob".to_string(),
        ];
        let payload = FileSinkWriter::blobref_payload(&packs).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(value["version"], 1);
        assert_eq!(value["packs"][0], packs[0]);
        assert_eq!(value["packs"][1], packs[1]);
    }

    #[test]
    fn url_to_object_path_handles_hadoop_urls() {
        assert_eq!(
            FileSinkWriter::url_to_object_path("file:/tmp/a/b.blob"),
            "/tmp/a/b.blob"
        );
        assert_eq!(
            FileSinkWriter::url_to_object_path("file:///tmp/a/b.blob"),
            "/tmp/a/b.blob"
        );
        assert_eq!(
            FileSinkWriter::url_to_object_path("s3://bucket/a/b.blob"),
            "a/b.blob"
        );
        assert_eq!(
            FileSinkWriter::url_to_object_path("s3a:/bucket/a/b.blob"),
            "a/b.blob"
        );
    }

    #[test]
    fn pack_sink_rolls_at_the_target() {
        let mut sink = PackSinkState::new("frame", "s3://bucket/table", 10);
        let first = sink.append(b"aaaa").unwrap();
        let second = sink.append(b"bbbb").unwrap();
        let third = sink.append(b"cccc").unwrap();

        assert_eq!(first.offset, 0);
        assert_eq!(second.offset, 4);
        assert_eq!(first.pack_path, second.pack_path);
        assert_ne!(third.pack_path, second.pack_path);
        assert_eq!(third.offset, 0);
        assert!(
            first
                .pack_path
                .starts_with("s3://bucket/table/_blob/frame/")
        );

        let packs = sink.take_packs();
        assert_eq!(packs.len(), 2);
        assert_eq!(packs[0].1, b"aaaabbbb");
        assert_eq!(packs[1].1, b"cccc");
    }

    #[test]
    fn pack_sink_without_target_keeps_one_pack() {
        let mut sink = PackSinkState::new("frame", "s3://bucket/table", 0);
        sink.append(b"aaaa").unwrap();
        sink.append(b"bbbb").unwrap();
        sink.append(b"cccc").unwrap();

        let packs = sink.take_packs();
        assert_eq!(packs.len(), 1);
        assert_eq!(packs[0].1, b"aaaabbbbcccc");
    }
}
