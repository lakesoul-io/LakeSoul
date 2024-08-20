// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use arrow_schema::{Schema, SchemaRef};
use datafusion::{
    execution::TaskContext,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream},
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{ExecutionMode, PlanProperties};
use datafusion_common::Result;
use crate::default_column_stream::empty_schema_stream::EmptySchemaStream;

#[derive(Debug)]
pub struct EmptySchemaScanExec {
    count: usize,
    empty_schema: SchemaRef,
    cache: PlanProperties,
}

impl EmptySchemaScanExec {
    pub fn new(count: usize) -> Self {
        let empty_schema = SchemaRef::new(Schema::empty());
        Self {
            count,
            empty_schema: empty_schema.clone(),
            cache: PlanProperties::new(
            EquivalenceProperties::new(empty_schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
            )
        }
    }
}

impl DisplayAs for EmptySchemaScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "EmptySchemaScanExec")
    }
}

impl ExecutionPlan for EmptySchemaScanExec {
    fn name(&self) -> &str {
        "EmptySchemaScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.empty_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(&self, _partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(EmptySchemaStream::new(
            _context.session_config().batch_size(),
            self.count,
        )))
    }
}
