use arrow_schema::SchemaRef;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};

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
