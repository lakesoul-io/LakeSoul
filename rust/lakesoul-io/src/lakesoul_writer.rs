// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use crate::helpers::{columnar_values_to_partition_desc, columnar_values_to_sub_path, get_columnar_values};
use crate::lakesoul_io_config::{create_session_context, IOSchema, LakeSoulIOConfig, LakeSoulIOConfigBuilder};
use crate::repartition::RepartitionByRangeAndHashExec;
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
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream};
use datafusion_common::DataFusionError::Internal;
use datafusion_common::{project_schema, DataFusionError};
use object_store::path::Path;
use object_store::{MultipartId, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rand::distributions::DistString;
use std::any::Any;
use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::io::ErrorKind::AddrInUse;
use std::io::Write;
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tracing::debug;
use url::Url;

#[async_trait]
pub trait AsyncBatchWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()>;

    async fn flush_and_close(self: Box<Self>) -> Result<Vec<u8>>;

    async fn abort_and_close(self: Box<Self>) -> Result<Vec<u8>>;

    fn schema(&self) -> SchemaRef;
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
    task_context: Arc<TaskContext>,
    schema: SchemaRef,
    writer: Box<dyn AsyncWrite + Unpin + Send>,
    multi_part_id: MultipartId,
    arrow_writer: ArrowWriter<InMemBuf>,
    _config: LakeSoulIOConfig,
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    absolute_path: String,
    num_rows: u64,
}

/// Wrap the above async writer with a SortExec to
/// sort the batches before write to async writer
pub struct SortAsyncWriter {
    schema: SchemaRef,
    sorter_sender: Sender<Result<RecordBatch>>,
    _sort_exec: Arc<dyn ExecutionPlan>,
    join_handle: Option<JoinHandle<Result<Vec<u8>>>>,
    err: Option<DataFusionError>,
}

/// Wrap the above async writer with a RepartitionExec to
/// dynamic repartitioning the batches before write to async writer
pub struct PartitioningAsyncWriter {
    schema: SchemaRef,
    sorter_sender: Sender<Result<RecordBatch>>,
    _partitioning_exec: Arc<dyn ExecutionPlan>,
    join_handle: Option<JoinHandle<Result<Vec<u8>>>>,
    err: Option<DataFusionError>,
}

/// A VecDeque which is both std::io::Write and bytes::Buf
#[derive(Clone)]
struct InMemBuf(Arc<AtomicRefCell<VecDeque<u8>>>);

impl Write for InMemBuf {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut v = self.0.try_borrow_mut().map_err(|_| std::io::Error::from(AddrInUse))?;
        v.extend(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let mut v = self.0.try_borrow_mut().map_err(|_| std::io::Error::from(AddrInUse))?;
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

impl DisplayAs for ReceiverStreamExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ReceiverStreamExec")
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
        let builder = self
            .receiver_stream_builder
            .borrow_mut()
            .take()
            .ok_or(DataFusionError::Internal("empty receiver stream".to_string()))?;
        Ok(builder.build())
    }
}

impl MultiPartAsyncWriter {
    pub async fn try_new_with_context(config: &mut LakeSoulIOConfig, task_context: Arc<TaskContext>) -> Result<Self> {
        if config.files.is_empty() {
            return Err(Internal("wrong number of file names provided for writer".to_string()));
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
        let (multipart_id, async_writer) = object_store.put_multipart(&path).await?;
        let in_mem_buf = InMemBuf(Arc::new(AtomicRefCell::new(VecDeque::<u8>::with_capacity(
            16 * 1024 * 1024, // 16kb
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

        let arrow_writer = ArrowWriter::try_new(
            in_mem_buf.clone(),
            writer_schema,
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
            task_context,
            schema,
            writer: async_writer,
            multi_part_id: multipart_id,
            arrow_writer,
            _config: config.clone(),
            object_store,
            path,
            absolute_path: file_name.to_string(),
            num_rows: 0,
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
        // underlying writer
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

    pub fn nun_rows(&self) -> u64 {
        self.num_rows
    }

    pub fn path(&self) -> Path {
        self.path.clone()
    }

    pub fn absolute_path(&self) -> String {
        self.absolute_path.clone()
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_context.clone()
    }
}

#[async_trait]
impl AsyncBatchWriter for MultiPartAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = uniform_record_batch(batch)?;
        self.num_rows += batch.num_rows() as u64;
        MultiPartAsyncWriter::write_batch(batch, &mut self.arrow_writer, &mut self.in_mem_buf, &mut self.writer).await
    }

    async fn flush_and_close(self: Box<Self>) -> Result<Vec<u8>> {
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
        // shutdown multi-part async writer to complete the upload
        this.writer.flush().await?;
        this.writer.shutdown().await?;
        Ok(vec![])
    }

    async fn abort_and_close(self: Box<Self>) -> Result<Vec<u8>> {
        let this = *self;
        this.object_store
            .abort_multipart(&this.path, &this.multi_part_id)
            .await
            .map_err(DataFusionError::ObjectStore)?;
        Ok(vec![])
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl SortAsyncWriter {
    pub fn try_new(
        async_writer: MultiPartAsyncWriter,
        config: LakeSoulIOConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Self> {
        let _ = runtime.enter();
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
                        Internal(ref err_msg) if err_msg == "external abort" => Ok(vec![]),
                        _ => Err(e),
                    },
                    Err(abort_err) => Err(Internal(format!(
                        "Abort failed {:?}, previous error {:?}",
                        abort_err, e
                    ))),
                }
            } else {
                async_writer.flush_and_close().await?;
                Ok(vec![])
            }
        });

        Ok(SortAsyncWriter {
            schema,
            sorter_sender: tx,
            _sort_exec: exec_plan,
            join_handle: Some(join_handle),
            err: None,
        })
    }
}

#[async_trait]
impl AsyncBatchWriter for SortAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if let Some(err) = &self.err {
            return Err(Internal(format!("SortAsyncWriter already failed with error {:?}", err)));
        }
        let send_result = self.sorter_sender.send(Ok(batch)).await;
        match send_result {
            Ok(_) => Ok(()),
            // channel has been closed, indicating error happened during sort write
            Err(e) => {
                if let Some(join_handle) = self.join_handle.take() {
                    let result = join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    self.err = result.err();
                    Err(Internal(format!("Write to SortAsyncWriter failed: {:?}", self.err)))
                } else {
                    self.err = Some(DataFusionError::External(Box::new(e)));
                    Err(Internal(format!("Write to SortAsyncWriter failed: {:?}", self.err)))
                }
            }
        }
    }

    async fn flush_and_close(self: Box<Self>) -> Result<Vec<u8>> {
        if let Some(join_handle) = self.join_handle {
            let sender = self.sorter_sender;
            drop(sender);
            join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            Err(Internal("SortAsyncWriter has been aborted, cannot flush".to_string()))
        }
    }

    async fn abort_and_close(self: Box<Self>) -> Result<Vec<u8>> {
        if let Some(join_handle) = self.join_handle {
            let sender = self.sorter_sender;
            // send abort signal to the task
            sender
                .send(Err(Internal("external abort".to_string())))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            drop(sender);
            join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            // previous error has already aborted writer
            Ok(vec![])
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

type PartitionedWriterInfo = Arc<Mutex<HashMap<String, (Vec<String>, u64)>>>;

impl PartitioningAsyncWriter {
    pub fn try_new(task_context: Arc<TaskContext>, config: LakeSoulIOConfig, runtime: Arc<Runtime>) -> Result<Self> {
        let _ = runtime.enter();
        let schema = config.target_schema.0.clone();
        let receiver_stream_builder = RecordBatchReceiverStream::builder(schema.clone(), 8);
        let tx = receiver_stream_builder.tx();
        let recv_exec = ReceiverStreamExec::new(receiver_stream_builder, schema.clone());

        let partitioning_exec = PartitioningAsyncWriter::get_partitioning_exec(recv_exec, config.clone())?;

        // launch one async task per *input* partition
        let mut join_handles = vec![];

        let write_id = rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16);

        let partitioned_file_path_and_row_count = Arc::new(Mutex::new(HashMap::<String, (Vec<String>, u64)>::new()));
        for i in 0..partitioning_exec.output_partitioning().partition_count() {
            let sink_task = tokio::spawn(Self::pull_and_sink(
                partitioning_exec.clone(),
                i,
                task_context.clone(),
                config.clone().into(),
                Arc::new(config.range_partitions.clone()),
                write_id.clone(),
                partitioned_file_path_and_row_count.clone(),
            ));
            // // In a separate task, wait for each input to be done
            // // (and pass along any errors, including panic!s)
            join_handles.push(sink_task);
        }

        let join_handle = tokio::spawn(Self::await_and_summary(
            join_handles,
            partitioned_file_path_and_row_count,
        ));

        Ok(Self {
            schema,
            sorter_sender: tx,
            _partitioning_exec: partitioning_exec,
            join_handle: Some(join_handle),
            err: None,
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
        partitioned_file_path_and_row_count: PartitionedWriterInfo,
    ) -> Result<u64> {
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

        let mut row_count = 0;

        let mut partitioned_writer = HashMap::<String, Box<MultiPartAsyncWriter>>::new();
        let mut partitioned_file_path_and_row_count_locked = partitioned_file_path_and_row_count.lock().await;
        while let Some(batch_result) = data.next().await {
            match batch_result {
                Ok(batch) => {
                    debug!("write record_batch with {} rows", batch.num_rows());
                    let columnar_values = get_columnar_values(&batch, range_partitions.clone())?;
                    let partition_desc = columnar_values_to_partition_desc(&columnar_values);
                    let batch_excluding_range = batch.project(&schema_projection_excluding_range)?;
                    let file_absolute_path = format!(
                        "{}{}part-{}_{:0>4}.parquet",
                        config_builder.prefix(),
                        columnar_values_to_sub_path(&columnar_values),
                        write_id,
                        partition
                    );

                    if !partitioned_writer.contains_key(&partition_desc) {
                        let mut config = config_builder.clone().with_files(vec![file_absolute_path]).build();

                        let writer = MultiPartAsyncWriter::try_new_with_context(&mut config, context.clone()).await?;
                        partitioned_writer.insert(partition_desc.clone(), Box::new(writer));
                    }

                    if let Some(async_writer) = partitioned_writer.get_mut(&partition_desc) {
                        row_count += batch_excluding_range.num_rows();
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
                        Internal(ref err_msg) if err_msg == "external abort" => (),
                        _ => return Err(e),
                    },
                    Err(abort_err) => {
                        return Err(Internal(format!(
                            "Abort failed {:?}, previous error {:?}",
                            abort_err, e
                        )))
                    }
                }
            }
            Ok(row_count as u64)
        } else {
            for (partition_desc, writer) in partitioned_writer.into_iter() {
                let file_absolute_path = writer.absolute_path();
                let num_rows = writer.nun_rows();
                if let Some(file_path_and_row_count) =
                    partitioned_file_path_and_row_count_locked.get_mut(&partition_desc)
                {
                    file_path_and_row_count.0.push(file_absolute_path);
                    file_path_and_row_count.1 += num_rows;
                } else {
                    partitioned_file_path_and_row_count_locked
                        .insert(partition_desc.clone(), (vec![file_absolute_path], num_rows));
                }
                writer.flush_and_close().await?;
            }
            Ok(row_count as u64)
        }
    }

    async fn await_and_summary(
        join_handles: Vec<JoinHandle<Result<u64>>>,
        partitioned_file_path_and_row_count: PartitionedWriterInfo,
    ) -> Result<Vec<u8>> {
        let _ =
            futures::future::join_all(join_handles)
                .await
                .iter()
                .try_fold(0u64, |counter, result| match &result {
                    Ok(Ok(count)) => Ok(counter + count),
                    Ok(Err(e)) => Err(DataFusionError::Execution(format!("{}", e))),
                    Err(e) => Err(DataFusionError::Execution(format!("{}", e))),
                })?;
        let partitioned_file_path_and_row_count = partitioned_file_path_and_row_count.lock().await;

        let mut summary = format!("{}", partitioned_file_path_and_row_count.len());
        for (partition_desc, (files, _)) in partitioned_file_path_and_row_count.iter() {
            summary += "\x01";
            summary += partition_desc.as_str();
            summary += "\x02";
            summary += files.join("\x02").as_str();
        }
        Ok(summary.into_bytes())
    }
}

#[async_trait]
impl AsyncBatchWriter for PartitioningAsyncWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // arrow_cast::pretty::print_batches(&[batch.clone()]);
        if let Some(err) = &self.err {
            return Err(Internal(format!(
                "PartitioningAsyncWriter already failed with error {:?}",
                err
            )));
        }
        let send_result = self.sorter_sender.send(Ok(batch)).await;
        match send_result {
            Ok(_) => Ok(()),
            // channel has been closed, indicating error happened during sort write
            Err(e) => {
                if let Some(join_handle) = self.join_handle.take() {
                    let result = join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?;
                    self.err = result.err();
                    Err(Internal(format!(
                        "Write to PartitioningAsyncWriter failed: {:?}",
                        self.err
                    )))
                } else {
                    self.err = Some(DataFusionError::External(Box::new(e)));
                    Err(Internal(format!(
                        "Write to PartitioningAsyncWriter failed: {:?}",
                        self.err
                    )))
                }
            }
        }
    }

    async fn flush_and_close(self: Box<Self>) -> Result<Vec<u8>> {
        if let Some(join_handle) = self.join_handle {
            let sender = self.sorter_sender;
            drop(sender);
            join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            Err(Internal(
                "PartitioningAsyncWriter has been aborted, cannot flush".to_string(),
            ))
        }
    }

    async fn abort_and_close(self: Box<Self>) -> Result<Vec<u8>> {
        if let Some(join_handle) = self.join_handle {
            let sender = self.sorter_sender;
            // send abort signal to the task
            sender
                .send(Err(Internal("external abort".to_string())))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            drop(sender);
            join_handle.await.map_err(|e| DataFusionError::External(Box::new(e)))?
        } else {
            // previous error has already aborted writer
            Ok(vec![])
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

pub type SendableWriter = Box<dyn AsyncBatchWriter + Send>;

// inner is sort writer
// multipart writer
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
                let schema = config.target_schema.0.clone();
                // O(nm), n = number of target schema fields, m = number of aux sort cols
                let proj_indices = schema
                    .fields
                    .iter()
                    .filter(|f| !config.aux_sort_cols.contains(f.name()))
                    .map(|f| schema.index_of(f.name().as_str()).map_err(DataFusionError::ArrowError))
                    .collect::<Result<Vec<usize>>>()?;
                Arc::new(schema.project(proj_indices.borrow())?)
            } else {
                config.target_schema.0.clone()
            };

            let mut writer_config = config.clone();
            let writer: Box<dyn AsyncBatchWriter + Send> = if config.use_dynamic_partition {
                let task_context = create_session_context(&mut writer_config)?.task_ctx();
                Box::new(PartitioningAsyncWriter::try_new(task_context, config, runtime.clone())?)
            } else if !config.primary_keys.is_empty() {
                // sort primary key table

                writer_config.target_schema = IOSchema(uniform_schema(writer_schema));
                let writer = MultiPartAsyncWriter::try_new(writer_config).await?;
                Box::new(SortAsyncWriter::try_new(writer, config, runtime.clone())?)
            } else {
                // else multipart
                writer_config.target_schema = IOSchema(uniform_schema(writer_schema));
                let writer = MultiPartAsyncWriter::try_new(writer_config).await?;
                Box::new(writer)
            };
            let schema = writer.schema();

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

    pub fn flush_and_close(self) -> Result<Vec<u8>> {
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

    pub fn abort_and_close(self) -> Result<Vec<u8>> {
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
    use crate::lakesoul_writer::{AsyncBatchWriter, MultiPartAsyncWriter, SyncSendableMutableLakeSoulWriter};
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::record_batch::RecordBatch;
    use arrow_array::Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::error::Result;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    use std::fs::File;
    use std::sync::Arc;
    use tokio::runtime::Builder;

    use super::SortAsyncWriter;

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
    fn test_sort_spill_write() -> Result<()> {
        let runtime = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        runtime.clone().block_on(async move {
            let common_conf_builder = LakeSoulIOConfigBuilder::new()
                .with_thread_num(2)
                .with_batch_size(8192)
                .with_max_row_group_size(250000);
            let read_conf = common_conf_builder
                .clone()
                .with_files(vec!["large_file.snappy.parquet".to_string()])
                .with_schema(Arc::new(Schema::new(vec![
                    Arc::new(Field::new("uuid", DataType::Utf8, false)),
                    Arc::new(Field::new("ip", DataType::Utf8, false)),
                    Arc::new(Field::new("hostname", DataType::Utf8, false)),
                    Arc::new(Field::new("requests", DataType::Int64, false)),
                    Arc::new(Field::new("name", DataType::Utf8, false)),
                    Arc::new(Field::new("city", DataType::Utf8, false)),
                    Arc::new(Field::new("job", DataType::Utf8, false)),
                    Arc::new(Field::new("phonenum", DataType::Utf8, false)),
                ])))
                .build();
            let mut reader = LakeSoulReader::new(read_conf)?;
            reader.start().await?;

            let schema = reader.schema.clone().unwrap();

            let write_conf = common_conf_builder
                .clone()
                .with_files(vec!["/home/chenxu/program/data/large_file_written.parquet".to_string()])
                .with_primary_key("uuid".to_string())
                .with_schema(schema)
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
