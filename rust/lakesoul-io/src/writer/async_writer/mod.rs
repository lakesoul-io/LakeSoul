// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Module for the async writer implementation of LakeSoul.
mod multipart_writer;
use bytes::BytesMut;
use datafusion_common::DataFusionError;
pub use multipart_writer::MultiPartAsyncWriter;

mod sort_writer;
use object_store::ObjectMeta;
pub use sort_writer::SortAsyncWriter;

mod partitioning_writer;
pub use partitioning_writer::PartitioningAsyncWriter;

mod sendable_writer;
pub use sendable_writer::AsyncSendableMutableLakeSoulWriter;

use std::{
    any::Any,
    fmt::{Debug, Formatter},
    io::Write,
    sync::Arc,
};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use atomic_refcell::AtomicRefCell;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, stream::RecordBatchReceiverStreamBuilder,
};
use parquet::file::metadata::ParquetMetaData;

use crate::Result;

#[derive(Debug, Clone)]
pub struct FlushOutput {
    pub partition_desc: String,
    pub file_path: String,
    pub object_meta: ObjectMeta,
    pub file_meta: ParquetMetaData,
}

/// The trait for the async batch writer.
#[async_trait::async_trait]
pub trait AsyncBatchWriter {
    /// Write a record batch to the writer.
    async fn write_record_batch(&mut self, batch: RecordBatch) -> Result<()>;

    /// Flush the writer and close it.
    async fn flush_and_close(self: Box<Self>) -> Result<Vec<FlushOutput>>;

    /// Abort the writer and close it when an error occurs.
    async fn abort_and_close(self: Box<Self>) -> Result<()>;

    /// Get the schema of the writer.
    fn schema(&self) -> SchemaRef;

    /// Get the buffered size of rows in the writer.
    fn buffered_size(&self) -> u64 {
        0
    }
}

/// A VecDeque which is both std::io::Write and bytes::Buf
#[derive(Clone)]
struct InMemBuf(Arc<AtomicRefCell<BytesMut>>);

impl InMemBuf {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(AtomicRefCell::new(BytesMut::with_capacity(
            capacity,
        ))))
    }
}

impl Write for InMemBuf {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.borrow_mut().extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// A [`datafusion::physical_plan::execution_plan::ExecutionPlan`] implementation for the receiver stream.
pub struct ReceiverStreamExec {
    receiver_stream_builder: AtomicRefCell<Option<RecordBatchReceiverStreamBuilder>>,
    schema: SchemaRef,
    properties: PlanProperties,
}

impl ReceiverStreamExec {
    pub fn new(
        receiver_stream_builder: RecordBatchReceiverStreamBuilder,
        schema: SchemaRef,
    ) -> Self {
        Self {
            receiver_stream_builder: AtomicRefCell::new(Some(receiver_stream_builder)),
            schema: schema.clone(),
            properties: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
}

impl Debug for ReceiverStreamExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReceiverStreamExec")
    }
}

impl DisplayAs for ReceiverStreamExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ReceiverStreamExec")
    }
}

impl ExecutionPlanProperties for ReceiverStreamExec {
    fn output_partitioning(&self) -> &Partitioning {
        &self.properties.partitioning
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.properties.eq_properties
    }
}

impl ExecutionPlan for ReceiverStreamExec {
    fn name(&self) -> &str {
        "ReceiverStreamExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        unimplemented!()
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream, DataFusionError> {
        let builder = self.receiver_stream_builder.borrow_mut().take().ok_or(
            DataFusionError::Internal("empty receiver stream".to_string()),
        )?;
        Ok(builder.build())
    }
}
