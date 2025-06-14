// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, sync::Arc};

use arrow::datatypes::Schema;
use datafusion::physical_plan::{
    DisplayAs, ExecutionPlan, stream::RecordBatchStreamAdapter,
};
use tpchgen::generators::LineItemGenerator;
use tpchgen_arrow::LineItemArrow;

#[derive(Debug)]
pub struct TpchGenExec {
    // part is one based
    pub schema: Arc<Schema>,
    pub part: i32,
    pub num_parts: i32,
    pub scale_factor: f64,
}

impl fmt::Display for TpchGenExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TpchGenExec")
    }
}

impl DisplayAs for TpchGenExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        write!(f, "TpchGenExec")
    }
}

impl ExecutionPlan for TpchGenExec {
    fn name(&self) -> &str {
        todo!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        todo!()
    }

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: std::sync::Arc<Self>,
        _children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<std::sync::Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        debug!("partition is {}", partition);
        let g = LineItemGenerator::new(0.001, (partition + 1) as i32, 8);
        let i = LineItemArrow::new(g).into_iter().map(|x| Ok(x));
        let adapter =
            RecordBatchStreamAdapter::new(self.schema.clone(), futures::stream::iter(i));
        Ok(Box::pin(adapter))
    }
}


#[cfg(test)]
mod tests {
    use datafusion::catalog::memory::MemorySourceConfig;

    #[test]
    fn t() {
        MemorySourceConfig
    }
}