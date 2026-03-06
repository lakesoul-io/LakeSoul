// SPDX-FileCopyrightText: 2024 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arrow_schema::SchemaRef;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use tokio::runtime::Handle;
use tokio_stream::StreamExt;

use crate::stream::empty_schema::EmptySchemaStream;

#[derive(Debug)]
pub struct EmptySchemaExec {
    schema: SchemaRef,
    batch_size: usize,
    remaining_num_rows: usize,
    plan_props: PlanProperties,
}

impl EmptySchemaExec {
    pub fn new(schema: SchemaRef, batch_size: usize, remaining_num_rows: usize) -> Self {
        Self {
            schema: schema.clone(),
            batch_size,
            remaining_num_rows,
            plan_props: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
}

impl DisplayAs for EmptySchemaExec {
    fn fmt_as(
        &self,
        t: datafusion_physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion_physical_plan::DisplayFormatType::Default
            | datafusion_physical_plan::DisplayFormatType::Verbose => {
                write!(
                    f,
                    "EmptySchemaExec: batch_size={}, remaining_num_rows={}",
                    self.batch_size, self.remaining_num_rows
                )
            }
            datafusion_physical_plan::DisplayFormatType::TreeRender => {
                writeln!(f, "batch_size={}", self.batch_size)?;
                writeln!(f, "remaining_num_rows={}", self.remaining_num_rows)?;
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for EmptySchemaExec {
    fn name(&self) -> &str {
        "EmptySchemaExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        &self.plan_props
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<std::sync::Arc<dyn ExecutionPlan>> {
        // no children
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: std::sync::Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion_execution::SendableRecordBatchStream> {
        Ok(Box::pin(EmptySchemaStream::new(
            self.batch_size,
            self.remaining_num_rows,
        )))
    }
}

#[derive(Debug)]
pub(crate) struct EmptyScanCountExec {
    schema: SchemaRef,
    batch_size: usize,
    child: std::sync::Arc<dyn ExecutionPlan>,
    plan_props: PlanProperties,
}

impl EmptyScanCountExec {
    pub fn new(
        schema: SchemaRef,
        batch_size: usize,
        child: std::sync::Arc<dyn ExecutionPlan>,
    ) -> Self {
        Self {
            schema: schema.clone(),
            batch_size,
            child,
            plan_props: PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
}

impl DisplayAs for EmptyScanCountExec {
    fn fmt_as(
        &self,
        t: datafusion_physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion_physical_plan::DisplayFormatType::Default
            | datafusion_physical_plan::DisplayFormatType::Verbose => {
                write!(f, "EmptyScanCountExec: batch_size={} ", self.batch_size,)
            }
            datafusion_physical_plan::DisplayFormatType::TreeRender => {
                writeln!(f, "batch_size={}", self.batch_size)?;
                Ok(())
            }
        }
    }
}

impl ExecutionPlan for EmptyScanCountExec {
    fn name(&self) -> &str {
        "EmptyScanCountExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        &self.plan_props
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<std::sync::Arc<dyn ExecutionPlan>> {
        todo!("Not Supported")
    }

    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion_execution::TaskContext>,
    ) -> datafusion_common::Result<datafusion_execution::SendableRecordBatchStream> {
        assert_eq!(partition, 0);

        let mut input_stream = self.child.execute(0, context)?;
        let batch_size = self.batch_size;

        // 2. 将异步消费逻辑封装在一个 Future 中
        let count_future = async move {
            let mut count = 0;
            while let Some(batch) = input_stream.next().await {
                count += batch?.num_rows();
            }
            Ok::<usize, datafusion_common::DataFusionError>(count)
        };

        let total_count = match Handle::try_current() {
            Ok(handle) => tokio::task::block_in_place(|| handle.block_on(count_future)),
            Err(_) => futures::executor::block_on(count_future),
        }?;

        // 4. 返回结果流
        Ok(Box::pin(EmptySchemaStream::new(batch_size, total_count)))
    }
}
