// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::datasource::memory::MemTable;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use datafusion_expr::{Expr, TableType};
use std::any::Any;
use std::fmt::{Debug, Display};
use std::sync::{Arc, LazyLock};
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, LineItemGeneratorIterator, OrderGenerator,
    PartGenerator, PartSuppGenerator, SupplierGenerator,
};
use tpchgen_arrow::{LineItemArrow, RecordBatchIterator};

use generator::TpchGenerator;
// use crate::tpch::table::TpchTable;

// mod stream;
// mod table;

// mod exec;

mod schemas;

mod table_funcion;
pub use table_funcion::register_tpch_udtfs;

mod source;

mod generator;

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
enum TpchTableKind {
    LineItem,
    Nation,
    Region,
    Part,
    Supplier,
    Partsupp,
    Customer,
    Orders,
}

impl TpchTableKind {
    fn schema(&self) -> Arc<Schema> {
        match *self {
            TpchTableKind::LineItem => schemas::LINEITEM_SCHEMA.clone(),
            TpchTableKind::Nation => todo!(),
            _ => {
                todo!()
            }
        }
    }

    pub fn generator(
        &self,
        scale_factor: f64,
        part: usize,
        num_parts: usize,
    ) -> impl TpchGenerator {
        match *self {
            TpchTableKind::LineItem => {
                let generator = LineItemGenerator::new(
                    scale_factor,
                    part as i32 + 1,
                    num_parts as i32,
                );
                LineItemArrow::new(generator)
            }
            _ => {
                todo!()
            }
        }
    }
}
