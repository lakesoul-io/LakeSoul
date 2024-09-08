// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod multipart_writer;
pub use multipart_writer::MultiPartAsyncWriter;

mod sort_writer;
pub use sort_writer::SortAsyncWriter;

mod partitioning_writer;
pub use partitioning_writer::PartitioningAsyncWriter;

use std::{
    any::Any,
    collections::VecDeque,
    fmt::{Debug, Formatter},
    io::{ErrorKind, Write},
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use atomic_refcell::AtomicRefCell;
use datafusion::{
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        stream::RecordBatchReceiverStreamBuilder, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    },
};
use datafusion_common::{DataFusionError, Result};
use parquet::format::FileMetaData;


// The result of a flush operation with format (partition_desc, file_path, file_meta)
pub type WriterFlushResult = Result<Vec<(String, String, FileMetaData)>>;

#[async_trait::async_trait]
pub trait AsyncBatchWriter {
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()>;

    async fn flush_and_close(self: Box<Self>) -> WriterFlushResult;

    async fn abort_and_close(self: Box<Self>) -> Result<()>;

    fn schema(&self) -> SchemaRef;

    fn buffered_size(&self) -> u64 {
        0
    }
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
            .map_err(|_| std::io::Error::from(ErrorKind::AddrInUse))?;
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
            .map_err(|_| std::io::Error::from(ErrorKind::AddrInUse))?;
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
