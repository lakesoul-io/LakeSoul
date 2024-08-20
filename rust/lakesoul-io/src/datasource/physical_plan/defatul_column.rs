// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::{any::Any, collections::HashMap};

use arrow_schema::SchemaRef;
use datafusion::physical_plan::PlanProperties;
use datafusion::{
    execution::TaskContext
    ,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream},
};
use datafusion_common::{DataFusionError, Result};

use crate::default_column_stream::DefaultColumnStream;

#[derive(Debug)]
pub struct DefaultColumnExec {
    input: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
    default_column_value: Arc<HashMap<String, String>>,
    cache: PlanProperties,
}

impl DefaultColumnExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        target_schema: SchemaRef,
        default_column_value: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        let properties = input.properties().clone();
        Ok(Self {
            input,
            target_schema,
            default_column_value,
            cache: properties,
        })
    }
}

impl DisplayAs for DefaultColumnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DefaultColumnExec")
    }
}

impl ExecutionPlan for DefaultColumnExec {
    fn name(&self) -> &str {
        "DefaultColumnExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.target_schema.clone()
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

    fn execute(&self, partition: usize, context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "Invalid requested partition {partition}. InsertExec requires a single input partition."
            )));
        }

        let mut streams = Vec::with_capacity(self.cache.output_partitioning().partition_count());
        for i in 0..self.cache.output_partitioning().partition_count() {
            let stream = self.input.execute(i, context.clone())?;
            streams.push(stream);
        }
        Ok(Box::pin(DefaultColumnStream::new_from_streams_with_default(
            streams,
            self.schema(),
            self.default_column_value.clone(),
        )))
    }
}
