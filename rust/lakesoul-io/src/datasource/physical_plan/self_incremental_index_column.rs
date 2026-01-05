// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Implementation of the self-incremental index column, which is used for the case of stable sort.

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, SchemaBuilder};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering};
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, PlanProperties};
use datafusion::physical_plan::{
    ExecutionPlanProperties, Partitioning, SendableRecordBatchStream,
};
use datafusion_common::{DataFusionError, Result};
use futures::Stream;
use futures::StreamExt;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// [`ExecutionPlan`] implementation of the self-incremental index column, which is used for the case of stable sort.
/// It appends a self-incremental index column to the input record batch for further sort.
#[derive(Debug)]
pub struct SelfIncrementalIndexColumnExec {
    input: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
    properties: PlanProperties,
}

impl SelfIncrementalIndexColumnExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let mut schema_builder = SchemaBuilder::new();
        for field in input.schema().fields() {
            schema_builder.push(field.clone());
        }
        schema_builder.push(Field::new(
            "__self_incremental_index__",
            DataType::UInt64,
            false,
        ));
        let target_schema = Arc::new(schema_builder.finish());
        let properties = PlanProperties::new(
            EquivalenceProperties::new(target_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            input,
            target_schema,
            properties,
        }
    }
}

impl DisplayAs for SelfIncrementalIndexColumnExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "SelfIncrementalIndexColumnExec")
    }
}

impl ExecutionPlanProperties for SelfIncrementalIndexColumnExec {
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

impl ExecutionPlan for SelfIncrementalIndexColumnExec {
    fn name(&self) -> &str {
        "SelfIncrementalIndexColumnExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        Ok(Box::pin(SelfIncrementalIndexStream::new(
            input,
            self.schema(),
        )))
    }
}

/// A stream that appends a self-incremental index column to each record batch
struct SelfIncrementalIndexStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    current_index: u64,
}

impl SelfIncrementalIndexStream {
    fn new(input: SendableRecordBatchStream, schema: SchemaRef) -> Self {
        Self {
            input,
            schema,
            current_index: 0,
        }
    }

    fn add_index_column(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let row_count = batch.num_rows();

        // Create the index array
        let mut index_values = Vec::with_capacity(row_count);
        for i in 0..row_count {
            index_values.push(self.current_index + i as u64);
        }
        self.current_index += row_count as u64;

        let index_array = Arc::new(UInt64Array::from(index_values)) as ArrayRef;

        // Combine the index array with existing columns
        let mut columns = batch.columns().to_vec();
        columns.push(index_array);

        // Create new record batch with the index column
        let options = RecordBatchOptions::new().with_row_count(Some(row_count));
        RecordBatch::try_new_with_options(self.schema.clone(), columns, &options).map_err(
            |e| {
                DataFusionError::ArrowError(
                    Box::new(e),
                    Some("Failed to apply self-incremental index column".to_string()),
                )
            },
        )
    }
}

impl Stream for SelfIncrementalIndexStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                info!("poll_next batch with schema: {:?}", batch.schema());
                Poll::Ready(Some(self.add_index_column(&batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for SelfIncrementalIndexStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
