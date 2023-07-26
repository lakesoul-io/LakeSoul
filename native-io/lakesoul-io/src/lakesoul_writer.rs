// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::lakesoul_io_config::{create_session_context, IOSchema, LakeSoulIOConfig};
use crate::transform::{uniform_record_batch, uniform_schema};

use arrow::compute::SortOptions;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use atomic_refcell::AtomicRefCell;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::expressions::{col, Column};
use datafusion::physical_expr::{PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::{RecordBatchReceiverStream, RecordBatchReceiverStreamBuilder};
use datafusion::physical_plan::{ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics};
use datafusion::prelude::SessionContext;
use datafusion_common::DataFusionError;
use datafusion_common::DataFusionError::Internal;
use object_store::path::Path;
use object_store::{MultipartId, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::any::Any;
use std::borrow::Borrow;
use std::collections::VecDeque;
use std::fmt::{Debug, Formatter};
use std::io::ErrorKind::ResourceBusy;
use std::io::Write;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use url::Url;

#[async_trait]
pub trait AsyncBatchWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()>;

    async fn flush_and_close(self: Box<Self>) -> Result<()>;

    async fn abort_and_close(self: Box<Self>) -> Result<()>;
}

/// An async writer using object_store's multi-part upload feature for cloud storage.
/// This writer uses a `VecDeque<u8>` as `std::io::Write` for arrow-rs's ArrowWriter.
/// Everytime when a new RowGroup is flushed, the length of the VecDeque would grow.
/// At this time, we pass the VecDeque as `bytes::Buf` to `AsyncWriteExt::write_buf` provided
/// by object_store, which would drain and copy the content of the VecDeque so that we could reuse it.
/// The `CloudMultiPartUpload` itself would try to concurrently upload parts, and
/// all parts will be committed to cloud storage by shutdown the `AsyncWrite` object.
pub struct MultiPartAsyncWriter {
    in_mem_buf: InMemBuf,
    sess_ctx: SessionContext,
    schema: SchemaRef,
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    multi_part_id: MultipartId,
    arrow_writer: ArrowWriter<InMemBuf>,
    _config: LakeSoulIOConfig,
    object_store: Arc<dyn ObjectStore>,
    path: Path,
}

/// Wrap the above async writer with a SortExec to
/// sort the batches before write to async writer
pub struct SortAsyncWriter {
    sorter_sender: Sender<Result<RecordBatch>>,
    _sort_exec: Arc<dyn ExecutionPlan>,
    join_handle: JoinHandle<Result<()>>,
}

/// A VecDeque which is both std::io::Write and bytes::Buf
#[derive(Clone)]
struct InMemBuf(Arc<AtomicRefCell<VecDeque<u8>>>);

impl Write for InMemBuf {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut v = self
            .0
            .try_borrow_mut()
            .map_err(|_| std::io::Error::from(ResourceBusy))?;
        v.extend(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let mut v = self
            .0
            .try_borrow_mut()
            .map_err(|_| std::io::Error::from(ResourceBusy))?;
        v.extend(buf);
        Ok(())
    }
}

pub struct ReceiverStreamExec {
    receiver_stream_builder: AtomicRefCell<Option<RecordBatchReceiverStreamBuilder>>,
    schema: SchemaRef,
}

impl ReceiverStreamExec {
    pub fn new(receiver_stream_builder: RecordBatchReceiverStreamBuilder, schema: SchemaRef) -> Self {
        Self {
            receiver_stream_builder: AtomicRefCell::new(Some(receiver_stream_builder)),
            schema,
        }
    }
}

impl Debug for ReceiverStreamExec {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl ExecutionPlan for ReceiverStreamExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(self: Arc<Self>, _children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let builder = self.receiver_stream_builder.borrow_mut().take().unwrap();
        Ok(builder.build())
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl MultiPartAsyncWriter {
    pub async fn try_new(mut config: LakeSoulIOConfig) -> Result<Self> {
        if config.files.len() != 1 {
            return Err(Internal("wrong number of file names provided for writer".to_string()));
        }
        let sess_ctx = create_session_context(&mut config)?;
        let file_name = &config.files[0];

        // local style path should have already been handled in create_session_context,
        // so we don't have to deal with ParseError::RelativeUrlWithoutBase here
        let (object_store, path) = match Url::parse(file_name.as_str()) {
            Ok(url) => Ok((
                sess_ctx
                    .runtime_env()
                    .object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?)?,
                Path::from(url.path()),
            )),
            Err(e) => Err(DataFusionError::External(Box::new(e))),
        }?;

        let (multipart_id, async_writer) = object_store.put_multipart(&path).await?;
        let in_mem_buf = InMemBuf(Arc::new(AtomicRefCell::new(VecDeque::<u8>::with_capacity(
            16 * 1024 * 1024,
        ))));
        let schema: SchemaRef = config.schema.0.clone();

        let arrow_writer = ArrowWriter::try_new(
            in_mem_buf.clone(),
            uniform_schema(schema.clone()),
            Some(
                WriterProperties::builder()
                    .set_max_row_group_size(config.max_row_group_size)
                    .set_write_batch_size(config.batch_size)
                    .set_compression(Compression::SNAPPY)
                    .build(),
            ),
        )?;

        Ok(MultiPartAsyncWriter {
            in_mem_buf,
            sess_ctx,
            schema: uniform_schema(schema),
            writer: async_writer,
            multi_part_id: multipart_id,
            arrow_writer,
            _config: config,
            object_store,
            path,
        })
    }

    async fn write_batch(
        batch: RecordBatch,
        arrow_writer: &mut ArrowWriter<InMemBuf>,
        in_mem_buf: &mut InMemBuf,
        writer: &mut Box<dyn AsyncWrite + Unpin + Send>,
    ) -> Result<()> {
        arrow_writer.write(&batch)?;
        let mut v = in_mem_buf
            .0
            .try_borrow_mut()
            .map_err(|e| Internal(format!("{:?}", e)))?;
        if v.len() > 0 {
            MultiPartAsyncWriter::write_part(writer, &mut v).await
        } else {
            Ok(())
        }
    }

    pub async fn write_part(
        writer: &mut Box<dyn AsyncWrite + Unpin + Send>,
        in_mem_buf: &mut VecDeque<u8>,
    ) -> Result<()> {
        writer.write_all_buf(in_mem_buf).await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncBatchWriter for MultiPartAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = uniform_record_batch(batch)?;
        MultiPartAsyncWriter::write_batch(batch, &mut self.arrow_writer, &mut self.in_mem_buf, &mut self.writer).await
    }

    async fn flush_and_close(self: Box<Self>) -> Result<()> {
        // close arrow writer to flush remaining rows
        let mut this = *self;
        let arrow_writer = this.arrow_writer;
        arrow_writer.close()?;
        let mut v = this
            .in_mem_buf
            .0
            .try_borrow_mut()
            .map_err(|e| Internal(format!("{:?}", e)))?;
        if v.len() > 0 {
            MultiPartAsyncWriter::write_part(&mut this.writer, &mut v).await?;
        }
        // shutdown multi part async writer to complete the upload
        this.writer.flush().await?;
        this.writer.shutdown().await?;
        Ok(())
    }

    async fn abort_and_close(self: Box<Self>) -> Result<()> {
        let this = *self;
        this.object_store
            .abort_multipart(&this.path, &this.multi_part_id)
            .await
            .map_err(DataFusionError::ObjectStore)
    }
}

impl SortAsyncWriter {
    pub fn try_new(
        async_writer: MultiPartAsyncWriter,
        config: LakeSoulIOConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let _ = runtime.enter();
        let schema = config.schema.0.clone();
        let receiver_stream_builder = RecordBatchReceiverStream::builder(schema.clone(), 8);
        let tx = receiver_stream_builder.tx();
        let recv_exec = ReceiverStreamExec::new(receiver_stream_builder, schema);

        let sort_exprs: Vec<PhysicalSortExpr> = config
            .primary_keys
            .iter()
            // add aux sort cols to sort expr
            .chain(config.aux_sort_cols.iter())
            .map(|pk| {
                let col = Column::new_with_schema(pk.as_str(), &config.schema.0)?;
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
            let proj_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = config
                .schema
                .0
                .fields
                .iter()
                .filter_map(|f| {
                    if config.aux_sort_cols.contains(f.name()) {
                        // exclude aux sort cols
                        None
                    } else {
                        Some(col(f.name().as_str(), &config.schema.0).map(|e| (e, f.name().clone())))
                    }
                })
                .collect::<Result<Vec<(Arc<dyn PhysicalExpr>, String)>>>()?;
            Arc::new(ProjectionExec::try_new(proj_expr, sort_exec)?)
        };

        let mut sorted_stream = exec_plan.execute(0, async_writer.sess_ctx.task_ctx())?;

        let mut async_writer = Box::new(async_writer);
        let join_handle = tokio::task::spawn(async move {
            while let Some(batch) = sorted_stream.next().await {
                match batch {
                    Ok(batch) => {
                        async_writer.write_record_batch(batch).await?;
                    }
                    // received abort singal
                    Err(_) => return async_writer.abort_and_close().await,
                }
            }
            async_writer.flush_and_close().await?;
            Ok(())
        });

        Ok(SortAsyncWriter {
            sorter_sender: tx,
            _sort_exec: exec_plan,
            join_handle,
        })
    }
}

#[async_trait]
impl AsyncBatchWriter for SortAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.sorter_sender
            .send(Ok(batch))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn flush_and_close(self: Box<Self>) -> Result<()> {
        let sender = self.sorter_sender;
        drop(sender);
        self.join_handle
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    }

    async fn abort_and_close(self: Box<Self>) -> Result<()> {
        let sender = self.sorter_sender;
        // send abort signal to the task
        sender
            .send(Err(Internal("abort".to_string())))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        drop(sender);
        self.join_handle
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
    }
}

type SendableWriter = Box<dyn AsyncBatchWriter + Send>;

pub struct SyncSendableMutableLakeSoulWriter {
    inner: Arc<Mutex<SendableWriter>>,
    runtime: Arc<Runtime>,
    schema: SchemaRef,
}

impl SyncSendableMutableLakeSoulWriter {
    pub fn try_new(config: LakeSoulIOConfig, runtime: Runtime) -> Result<Self> {
        let runtime = Arc::new(runtime);
        runtime.clone().block_on(async move {
            // if aux sort cols exist, we need to adjust the schema of final writer
            // to exclude all aux sort cols
            let writer_schema: SchemaRef = if !config.aux_sort_cols.is_empty() {
                let schema = config.schema.0.clone();
                let proj_indices = schema
                    .fields
                    .iter()
                    .filter(|f| !config.aux_sort_cols.contains(f.name()))
                    .map(|f| schema.index_of(f.name().as_str()).map_err(DataFusionError::ArrowError))
                    .collect::<Result<Vec<usize>>>()?;
                Arc::new(schema.project(proj_indices.borrow())?)
            } else {
                config.schema.0.clone()
            };

            let mut writer_config = config.clone();
            writer_config.schema = IOSchema(uniform_schema(writer_schema));
            let writer = MultiPartAsyncWriter::try_new(writer_config).await?;

            let schema = writer.schema.clone();
            let writer: Box<dyn AsyncBatchWriter + Send> = if !config.primary_keys.is_empty() {
                Box::new(SortAsyncWriter::try_new(writer, config, runtime.clone())?)
            } else {
                Box::new(writer)
            };

            Ok(SyncSendableMutableLakeSoulWriter {
                inner: Arc::new(Mutex::new(writer)),
                runtime,
                schema, // this should be the final written schema
            })
        })
    }

    // blocking method for writer record batch.
    // since the underlying multipart upload would accumulate buffers
    // and upload concurrently in background, we only need blocking method here
    // for ffi callers
    pub fn write_batch(&self, record_batch: RecordBatch) -> Result<()> {
        let inner_writer = self.inner.clone();
        let runtime = self.runtime.clone();
        runtime.block_on(async move {
            let mut writer = inner_writer.lock().await;
            writer.write_record_batch(record_batch).await
        })
    }

    pub fn flush_and_close(self) -> Result<()> {
        let inner_writer = match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(_) => return Err(Internal("Cannot get ownership of inner writer".to_string())),
        };
        let runtime = self.runtime;
        runtime.block_on(async move {
            let writer = inner_writer.into_inner();
            writer.flush_and_close().await
        })
    }

    pub fn abort_and_close(self) -> Result<()> {
        let inner_writer = match Arc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(_) => return Err(Internal("Cannot get ownership of inner writer".to_string())),
        };
        let runtime = self.runtime;
        runtime.block_on(async move {
            let writer = inner_writer.into_inner();
            writer.abort_and_close().await
        })
    }

    pub fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::lakesoul_io_config::LakeSoulIOConfigBuilder;
    use crate::lakesoul_reader::LakeSoulReader;
    use crate::lakesoul_writer::{
        AsyncBatchWriter, MultiPartAsyncWriter, SortAsyncWriter, SyncSendableMutableLakeSoulWriter,
    };
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::record_batch::RecordBatch;
    use datafusion::error::Result;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    use std::fs::File;
    use std::sync::Arc;
    use arrow_array::Array;
    use tokio::runtime::Builder;

    #[test]
    fn test_parquet_async_write() -> Result<()> {
        let runtime = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        runtime.clone().block_on(async move {
            let col = Arc::new(Int64Array::from_iter_values([3, 2, 1])) as ArrayRef;
            let to_write = RecordBatch::try_from_iter([("col", col)])?;
            let temp_dir = tempfile::tempdir()?;
            let path = temp_dir
                .into_path()
                .join("test.parquet")
                .into_os_string()
                .into_string()
                .unwrap();
            let writer_conf = LakeSoulIOConfigBuilder::new()
                .with_files(vec![path.clone()])
                .with_thread_num(2)
                .with_batch_size(256)
                .with_max_row_group_size(2)
                .with_schema(to_write.schema())
                .build();
            let mut async_writer = MultiPartAsyncWriter::try_new(writer_conf).await?;
            async_writer.write_record_batch(to_write.clone()).await?;
            Box::new(async_writer).flush_and_close().await?;

            let file = File::open(path.clone())?;
            let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 1024).unwrap();

            let actual_batch = record_batch_reader
                .next()
                .expect("No batch found")
                .expect("Unable to get batch");

            assert_eq!(to_write.schema(), actual_batch.schema());
            assert_eq!(to_write.num_columns(), actual_batch.num_columns());
            assert_eq!(to_write.num_rows(), actual_batch.num_rows());
            for i in 0..to_write.num_columns() {
                let expected_data = to_write.column(i).to_data();
                let actual_data = actual_batch.column(i).to_data();

                assert_eq!(expected_data, actual_data);
            }

            let writer_conf = LakeSoulIOConfigBuilder::new()
                .with_files(vec![path.clone()])
                .with_thread_num(2)
                .with_batch_size(256)
                .with_max_row_group_size(2)
                .with_schema(to_write.schema())
                .with_primary_keys(vec!["col".to_string()])
                .build();

            let async_writer = MultiPartAsyncWriter::try_new(writer_conf.clone()).await?;
            let mut async_writer = SortAsyncWriter::try_new(async_writer, writer_conf, runtime.clone())?;
            async_writer.write_record_batch(to_write.clone()).await?;
            Box::new(async_writer).flush_and_close().await?;

            let file = File::open(path)?;
            let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 1024).unwrap();

            let actual_batch = record_batch_reader
                .next()
                .expect("No batch found")
                .expect("Unable to get batch");

            let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
            let to_read = RecordBatch::try_from_iter([("col", col)])?;
            assert_eq!(to_read.schema(), actual_batch.schema());
            assert_eq!(to_read.num_columns(), actual_batch.num_columns());
            assert_eq!(to_read.num_rows(), actual_batch.num_rows());
            for i in 0..to_read.num_columns() {
                let expected_data = to_read.column(i).to_data();
                let actual_data = actual_batch.column(i).to_data();

                assert_eq!(expected_data, actual_data);
            }
            Ok(())
        })
    }

    #[test]
    fn test_parquet_async_write_with_aux_sort() -> Result<()> {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let col = Arc::new(Int64Array::from_iter_values([3, 2, 3])) as ArrayRef;
        let col1 = Arc::new(Int64Array::from_iter_values([5, 3, 2])) as ArrayRef;
        let col2 = Arc::new(Int64Array::from_iter_values([3, 2, 1])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col), ("col1", col1), ("col2", col2)])?;
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir
            .into_path()
            .join("test.parquet")
            .into_os_string()
            .into_string()
            .unwrap();
        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![path.clone()])
            .with_thread_num(2)
            .with_batch_size(256)
            .with_max_row_group_size(2)
            .with_schema(to_write.schema())
            .with_primary_keys(vec!["col".to_string()])
            .with_aux_sort_column("col2".to_string())
            .build();

        let writer = SyncSendableMutableLakeSoulWriter::try_new(writer_conf, runtime)?;
        writer.write_batch(to_write.clone())?;
        writer.flush_and_close()?;

        let file = File::open(path.clone())?;
        let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 1024).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");
        let col = Arc::new(Int64Array::from_iter_values([2, 3, 3])) as ArrayRef;
        let col1 = Arc::new(Int64Array::from_iter_values([3, 2, 5])) as ArrayRef;
        let to_read = RecordBatch::try_from_iter([("col", col), ("col1", col1)])?;

        assert_eq!(to_read.schema(), actual_batch.schema());
        assert_eq!(to_read.num_columns(), actual_batch.num_columns());
        assert_eq!(to_read.num_rows(), actual_batch.num_rows());
        for i in 0..to_read.num_columns() {
            let expected_data = to_read.column(i).to_data();
            let actual_data = actual_batch.column(i).to_data();

            assert_eq!(expected_data, actual_data);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_s3_read_write() -> Result<()> {
        let common_conf_builder = LakeSoulIOConfigBuilder::new()
            .with_thread_num(2)
            .with_batch_size(8192)
            .with_max_row_group_size(250000)
            .with_object_store_option("fs.s3a.access.key".to_string(), "minioadmin1".to_string())
            .with_object_store_option("fs.s3a.secret.key".to_string(), "minioadmin1".to_string())
            .with_object_store_option("fs.s3a.endpoint".to_string(), "http://localhost:9000".to_string());

        let read_conf = common_conf_builder
            .clone()
            .with_files(vec![
                "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet".to_string()
            ])
            .build();
        let mut reader = LakeSoulReader::new(read_conf)?;
        reader.start().await?;

        let schema = reader.schema.clone().unwrap();

        let write_conf = common_conf_builder
            .clone()
            .with_files(vec![
                "s3://lakesoul-test-bucket/data/native-io-test/large_file_written.parquet".to_string(),
            ])
            .with_schema(schema)
            .build();
        let mut async_writer = MultiPartAsyncWriter::try_new(write_conf).await?;

        while let Some(rb) = reader.next_rb().await {
            let rb = rb?;
            async_writer.write_record_batch(rb).await?;
        }

        Box::new(async_writer).flush_and_close().await?;
        drop(reader);

        Ok(())
    }

    #[test]
    fn test_s3_read_sort_write() -> Result<()> {
        let runtime = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        runtime.clone().block_on(async move {
            let common_conf_builder = LakeSoulIOConfigBuilder::new()
                .with_thread_num(2)
                .with_batch_size(8192)
                .with_max_row_group_size(250000)
                .with_object_store_option("fs.s3a.access.key".to_string(), "minioadmin1".to_string())
                .with_object_store_option("fs.s3a.secret.key".to_string(), "minioadmin1".to_string())
                .with_object_store_option("fs.s3a.endpoint".to_string(), "http://localhost:9000".to_string());

            let read_conf = common_conf_builder
                .clone()
                .with_files(vec![
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet".to_string()
                ])
                .build();
            let mut reader = LakeSoulReader::new(read_conf)?;
            reader.start().await?;

            let schema = reader.schema.clone().unwrap();

            let write_conf = common_conf_builder
                .clone()
                .with_files(vec![
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file_written_sorted.parquet".to_string(),
                ])
                .with_schema(schema)
                .with_primary_keys(vec!["str0".to_string(), "str1".to_string(), "int1".to_string()])
                .build();
            let async_writer = MultiPartAsyncWriter::try_new(write_conf.clone()).await?;
            let mut async_writer = SortAsyncWriter::try_new(async_writer, write_conf, runtime.clone())?;

            while let Some(rb) = reader.next_rb().await {
                let rb = rb?;
                async_writer.write_record_batch(rb).await?;
            }

            Box::new(async_writer).flush_and_close().await?;
            drop(reader);

            Ok(())
        })
    }
}
