/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::lakesoul_io_config::{create_session_context, LakeSoulIOConfig};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use datafusion_common::DataFusionError;
use datafusion_common::DataFusionError::Internal;
use object_store::path::Path;
use object_store::MultipartId;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use url::{ParseError, Url};

/// An async writer using object_store's multi-part upload feature for cloud storage.
/// This writer uses a `VecDeque<u8>` as `std::io::Write` for arrow-rs's ArrowWriter.
/// Everytime when a new RowGroup is flushed, the length of the VecDeque would grow.
/// At this time, we pass the VecDeque as `bytes::Buf` to `AsyncWriteExt::write_buf` provided
/// by object_store, which would drain and copy the content of the VecDeque so that we could reuse it.
/// The CloudMultiPartUpload itself would try to concurrently upload parts, and
/// all parts will be committed to cloud storage by shutdown the AsyncWriter.
pub struct MultiPartAsyncWriter {
    in_mem_buf: InMemBuf,
    sess_ctx: SessionContext,
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    multi_part_id: MultipartId,
    arrow_writer: ArrowWriter<InMemBuf>,
    config: LakeSoulIOConfig,
}

#[derive(Clone)]
struct InMemBuf(Arc<VecDeque<u8>>);

impl Write for InMemBuf {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        unsafe {
            Arc::get_mut_unchecked(&mut self.0).extend(buf);
            Ok(buf.len())
        }
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        unsafe {
            Arc::get_mut_unchecked(&mut self.0).extend(buf);
            Ok(())
        }
    }
}

impl MultiPartAsyncWriter {
    pub async fn new(config: LakeSoulIOConfig) -> Result<Self> {
        if config.files.len() != 1 {
            return Err(Internal("wrong number of file names provided for writer".to_string()));
        }
        let sess_ctx = create_session_context(&config)?;
        let file_name = &config.files[0];
        let (object_store, path) = match Url::parse(file_name.as_str()) {
            Ok(url) => Ok((sess_ctx
                .runtime_env()
                .object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?)?,
                Path::from(url.path()))
            ),
            Err(ParseError::RelativeUrlWithoutBase) => Ok((sess_ctx
                .runtime_env()
                .object_store(ObjectStoreUrl::local_filesystem())?,
                Path::from(file_name.as_str()))
            ),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }?;
        let (multipart_id, async_writer) = object_store.put_multipart(&path).await?;
        let in_mem_buf = InMemBuf(Arc::new(VecDeque::<u8>::with_capacity(64 * 1024 * 1024)));
        let write_schema: Schema = serde_json::from_str(&config.schema_json).unwrap();
        Ok(MultiPartAsyncWriter {
            in_mem_buf: in_mem_buf.clone(),
            sess_ctx,
            writer: async_writer,
            multi_part_id: multipart_id,
            arrow_writer: ArrowWriter::try_new(
                in_mem_buf,
                Arc::new(write_schema),
                Some(
                    WriterProperties::builder()
                        .set_max_row_group_size(config.max_row_group_size)
                        .set_write_batch_size(config.batch_size)
                        .build(),
                ),
            )?,
            config,
        })
    }

    pub async fn write_record_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.arrow_writer.write(batch)?;
        if self.in_mem_buf.0.len() > 0 {
            MultiPartAsyncWriter::write_part(&mut self.writer, &mut self.in_mem_buf.0).await
        } else {
            Ok(())
        }
    }

    pub async fn write_part(
        writer: &mut Box<dyn AsyncWrite + Unpin + Send>,
        in_mem_buf: &mut Arc<VecDeque<u8>>,
    ) -> Result<()> {
        unsafe {
            writer.write_all_buf(Arc::get_mut_unchecked(in_mem_buf).into()).await?;
            Ok(())
        }
    }

    pub async fn flush_and_shutdown(mut self) -> Result<()> {
        let arrow_writer = self.arrow_writer;
        arrow_writer.close()?;
        if self.in_mem_buf.0.len() > 0 {
            MultiPartAsyncWriter::write_part(&mut self.writer, &mut self.in_mem_buf.0).await?;
        }
        self.writer.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::lakesoul_io_config::LakeSoulIOConfigBuilder;
    use crate::lakesoul_writer::MultiPartAsyncWriter;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::record_batch::RecordBatch;
    use arrow_schema::Schema;
    use datafusion::error::Result;
    use std::borrow::Borrow;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_parquet_async_write() -> Result<()> {
        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col)])?;
        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["file:/tmp/test.parquet".to_string()])
            .with_thread_num(2)
            .with_batch_size(256)
            .with_schema_json(serde_json::to_string::<Schema>(to_write.schema().borrow()).unwrap())
            .build();
        let mut async_writer = MultiPartAsyncWriter::new(writer_conf).await?;
        async_writer.write_record_batch(&to_write).await?;
        async_writer.flush_and_shutdown().await?;
        Ok(())
    }
}
