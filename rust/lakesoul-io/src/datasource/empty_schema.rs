// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};

use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::{datasource::TableProvider, logical_expr::TableType};
use datafusion_common::Result;

use super::physical_plan::EmptySchemaScanExec;

#[derive(Clone, Debug)]
pub struct EmptySchemaProvider {
    count: usize,
    empty_schema: SchemaRef,
}

impl EmptySchemaProvider {
    pub fn new(count: usize) -> Self {
        EmptySchemaProvider {
            count,
            empty_schema: SchemaRef::new(Schema::empty()),
        }
    }
}

#[async_trait]
impl TableProvider for EmptySchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.empty_schema.clone()
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
        Ok(Arc::new(EmptySchemaScanExec::new(self.count)))
    }
}
