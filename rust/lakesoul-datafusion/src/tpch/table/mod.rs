// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{
    any::Any,
    fmt::{Debug, Display},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::{
    catalog::{MemTable, Session, TableProvider},
    error::DataFusionError,
    physical_plan::{DisplayAs, ExecutionPlan, memory::LazyBatchGenerator},
};
use datafusion_expr::{Expr, TableType};

use crate::tpch::exec::TpchGenExec;

#[derive(Debug)]
pub struct TpchTable {
    pub schema: SchemaRef,
    pub parts: Vec<i32>,
    pub num_parts: i32,
    pub scale_factor: f64,
}

#[async_trait]
impl TableProvider for TpchTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Ok(Arc::new(TpchGenExec {
            schema: self.schema.clone(),
            part: 1,
            num_parts: self.num_parts,
            scale_factor: self.scale_factor,
        }))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::datasource::physical_plan::parquet::plan_to_parquet;
    use tpchgen::generators::{self, LineItemGenerator, NationGenerator};
    use tpchgen_arrow::{LineItemArrow, NationArrow};

    #[test]
    fn generate_test() {
        let generator = LineItemGenerator::new(0.01, 1, 1);
        let mut ag = LineItemArrow::new(generator).with_batch_size(10);
        let b = ag.next().unwrap();
        println!("{:?}", b)
    }
}
