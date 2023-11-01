// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0


use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::common::{Statistics, DataFusionError};

use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::{
    Expr, TableType,
};
use datafusion::physical_expr::{PhysicalSortExpr, PhysicalSortRequirement};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, Distribution, stream::RecordBatchStreamAdapter
};
use datafusion::arrow::datatypes::{Schema, SchemaRef};

use crate::lakesoul_io_config::{IOSchema, LakeSoulIOConfig};
use crate::lakesoul_writer::MultiPartAsyncWriter;
use crate::transform::uniform_schema;

#[derive(Debug, Clone)]
pub struct LakeSoulParquetSinkProvider{
    schema: SchemaRef,
    config: LakeSoulIOConfig
}

#[async_trait]
impl TableProvider for LakeSoulParquetSinkProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        _projections: Option<&Vec<usize>>,
        // filters and limit can be used here to inject some push-down operations if needed
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let msg = "Scan not implemented for LakeSoulParquetSinkProvider".to_owned();
        Err(DataFusionError::NotImplemented(msg))
    }


    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let writer_schema = self.schema();
        let mut writer_config = self.config.clone();
        writer_config.schema = IOSchema(uniform_schema(writer_schema));
        let _writer = MultiPartAsyncWriter::try_new(writer_config).await?;
        Ok(Arc::new(LakeSoulParquetSinkExec::new(input)))
    }

}

#[derive(Debug, Clone)]
struct LakeSoulParquetSinkExec {
        /// Input plan that produces the record batches to be written.
        input: Arc<dyn ExecutionPlan>,
        /// Sink to whic to write
        // sink: Arc<dyn DataSink>,
        /// Schema describing the structure of the data.
        schema: SchemaRef,
    
}

impl LakeSoulParquetSinkExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            input,
            schema: Arc::new(Schema::empty())
        }
    }
}

impl DisplayAs for LakeSoulParquetSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "LakeSoulParquetSinkExec")
    }
}

impl ExecutionPlan for LakeSoulParquetSinkExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        // Require that the InsertExec gets the data in the order the
        // input produced it (otherwise the optimizer may chose to reorder
        // the input which could result in unintended / poor UX)
        //
        // More rationale:
        // https://github.com/apache/arrow-datafusion/pull/6354#discussion_r1195284178
        vec![self
            .input
            .output_ordering()
            .map(PhysicalSortRequirement::from_sort_exprs)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            schema: self.schema.clone(),
        }))
    }

    /// Execute the plan and return a stream of `RecordBatch`es for
    /// the specified partition.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                format!("Invalid requested partition {partition}. InsertExec requires a single input partition."
                )));
        }

        // Execute each of our own input's partitions and pass them to the sink
        let input_partition_count = self.input.output_partitioning().partition_count();
        if input_partition_count != 1 {
            return Err(DataFusionError::Internal(format!(
                "Invalid input partition count {input_partition_count}. \
                         InsertExec needs only a single partition."
            )));
        }

        let data = self.input.execute(0, context)?;
        let schema = self.schema.clone();


        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, data)))
    }


    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}
