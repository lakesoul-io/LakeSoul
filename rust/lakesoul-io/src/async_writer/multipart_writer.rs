// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the multipart writer.

use std::{collections::VecDeque, sync::Arc};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use atomic_refcell::AtomicRefCell;
use bytes::Bytes;
use datafusion::{
    datasource::listing::ListingTableUrl,
    execution::{object_store::ObjectStoreUrl, TaskContext},
};
use datafusion_common::{project_schema, DataFusionError, Result};
use log::debug;
use object_store::{path::Path, ObjectStore, WriteMultipart};
use parquet::basic::ZstdLevel;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use url::Url;

use crate::{
    constant::TBD_PARTITION_DESC,
    helpers::get_batch_memory_size,
    lakesoul_io_config::{create_session_context, LakeSoulIOConfig},
    transform::{uniform_record_batch, uniform_schema},
};

use super::{AsyncBatchWriter, InMemBuf, WriterFlushResult};

/// An async writer using object_store's multi-part upload feature for cloud storage.
/// This writer uses a `VecDeque<u8>` as `std::io::Write` for arrow-rs's ArrowWriter.
/// Everytime when a new RowGroup is flushed, the length of the VecDeque would grow.
/// At this time, we pass the VecDeque as `bytes::Buf` to `AsyncWriteExt::write_buf` provided
/// by object_store, which would drain and copy the content of the VecDeque so that we could reuse it.
/// The `CloudMultiPartUpload` itself would try to concurrently upload parts, and
/// all parts will be committed to cloud storage by shutdown the `AsyncWrite` object.
pub struct MultiPartAsyncWriter {
    /// The in-memory buffer of the multi-part async writer.
    in_mem_buf: InMemBuf,
    /// The task context of the multi-part async writer.
    task_context: Arc<TaskContext>,
    /// The schema of the multi-part async writer.
    schema: SchemaRef,
    /// The multi-part writer of [`object_store::WriteMultipart`] that is used to upload the data to the object store asynchronously.
    writer: WriteMultipart,
    /// The [`ArrowWriter`] of the multi-part async writer.
    arrow_writer: ArrowWriter<InMemBuf>,
    /// The io config of the multi-part async writer.
    _config: LakeSoulIOConfig,
    /// The object store of the multi-part async writer.
    object_store: Arc<dyn ObjectStore>,
    /// The path of the multi-part async writer.
    _path: Path,
    /// The absolute path of the multi-part async writer.
    absolute_path: String,
    /// The number of rows of the multi-part async writer.
    num_rows: u64,
    buffered_size: u64,
}

impl MultiPartAsyncWriter {
    pub async fn try_new_with_context(config: &mut LakeSoulIOConfig, task_context: Arc<TaskContext>) -> Result<Self> {
        if config.files.is_empty() {
            return Err(DataFusionError::Internal(
                "wrong number of file names provided for writer".to_string(),
            ));
        }
        let file_name = &config
            .files
            .last()
            .ok_or(DataFusionError::Internal("wrong file name".to_string()))?;

        // local style path should have already been handled in create_session_context,
        // so we don't have to deal with ParseError::RelativeUrlWithoutBase here
        let (object_store, path) = match Url::parse(file_name.as_str()) {
            Ok(url) => Ok((
                task_context
                    .runtime_env()
                    .object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?)?,
                Path::from_url_path(url.path())?,
            )),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }?;

        // get underlying multipart uploader
        let multipart_upload = object_store.put_multipart(&path).await?;
        let write_multi_part = WriteMultipart::new_with_chunk_size(multipart_upload, 128 * 1024 * 1024);

        let in_mem_buf = InMemBuf(Arc::new(AtomicRefCell::new(VecDeque::<u8>::with_capacity(
            16 * 1024, // 16kb
        ))));
        let schema = uniform_schema(config.target_schema.0.clone());

        // O(nm), n = number of fields, m = number of range partitions
        let schema_projection_excluding_range = schema
            .fields()
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| match config.range_partitions.contains(field.name()) {
                true => None,
                false => Some(idx),
            })
            .collect::<Vec<_>>();
        let writer_schema = project_schema(&schema, Some(&schema_projection_excluding_range))?;

        let max_row_group_size = if config.max_row_group_size * schema.fields().len() > config.max_row_group_num_values
        {
            config
                .batch_size
                .max(config.max_row_group_num_values / schema.fields().len())
        } else {
            config.max_row_group_size
        };
        let arrow_writer = ArrowWriter::try_new(
            in_mem_buf.clone(),
            writer_schema,
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(max_row_group_size)
                    .set_write_batch_size(config.batch_size)
                    .set_compression(Compression::ZSTD(ZstdLevel::default()))
                    .set_dictionary_enabled(false)
                    .build(),
            ),
        )?;

        Ok(MultiPartAsyncWriter {
            in_mem_buf,
            task_context,
            schema,
            writer: write_multi_part,
            // multi_part_id: multipart_id,
            arrow_writer,
            _config: config.clone(),
            object_store,
            _path: path,
            absolute_path: file_name.to_string(),
            num_rows: 0,
            buffered_size: 0,
        })
    }

    pub async fn try_new(mut config: LakeSoulIOConfig) -> Result<Self> {
        let task_context = create_session_context(&mut config)?.task_ctx();
        Self::try_new_with_context(&mut config, task_context).await
    }

    async fn write_batch(
        batch: RecordBatch,
        arrow_writer: &mut ArrowWriter<InMemBuf>,
        in_mem_buf: &mut InMemBuf,
        writer: &mut WriteMultipart,
    ) -> Result<()> {
        arrow_writer.write(&batch)?;
        let mut v = in_mem_buf
            .0
            .try_borrow_mut()
            .map_err(|e| DataFusionError::Internal(format!("{:?}", e)))?;
        if v.len() > 0 {
            MultiPartAsyncWriter::write_part(writer, &mut v).await
        } else {
            Ok(())
        }
    }

    pub async fn write_part(writer: &mut WriteMultipart, in_mem_buf: &mut VecDeque<u8>) -> Result<()> {
        let bytes = Bytes::from(in_mem_buf.drain(..).collect::<Vec<u8>>());
        writer.put(bytes);
        Ok(())
    }

    pub fn nun_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn absolute_path(&self) -> String {
        self.absolute_path.clone()
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_context.clone()
    }
}

#[async_trait::async_trait]
impl AsyncBatchWriter for MultiPartAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = uniform_record_batch(batch)?;
        self.num_rows += batch.num_rows() as u64;
        self.buffered_size += get_batch_memory_size(&batch)? as u64;
        MultiPartAsyncWriter::write_batch(batch, &mut self.arrow_writer, &mut self.in_mem_buf, &mut self.writer).await
    }

    async fn flush_and_close(self: Box<Self>) -> Result<WriterFlushResult> {
        debug!("MultiPartAsyncWriter::flush_and_close: {:?}", self.arrow_writer);
        // close arrow writer to flush remaining rows
        let mut this = *self;
        let arrow_writer = this.arrow_writer;
        let file_path = this.absolute_path.clone();
        let metadata = arrow_writer.close()?;
        let mut v = this
            .in_mem_buf
            .0
            .try_borrow_mut()
            .map_err(|e| DataFusionError::Internal(format!("{:?}", e)))?;
        if v.len() > 0 {
            MultiPartAsyncWriter::write_part(&mut this.writer, &mut v).await?;
        }
        // shutdown multi-part async writer to complete the upload
        this.writer.finish().await?;
        let path =
            Path::from_url_path(<ListingTableUrl as AsRef<Url>>::as_ref(&ListingTableUrl::parse(&file_path)?).path())?;
        let object_meta = this.object_store.head(&path).await?;
        Ok(vec![(TBD_PARTITION_DESC.to_string(), file_path, object_meta, metadata)])
    }

    async fn abort_and_close(self: Box<Self>) -> Result<()> {
        let this = *self;
        this.writer.abort().await.map_err(DataFusionError::ObjectStore)?;
        Ok(())
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn buffered_size(&self) -> u64 {
        self.buffered_size
    }
}
