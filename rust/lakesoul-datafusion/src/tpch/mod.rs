// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use datafusion::arrow::datatypes::Schema;
use std::fmt::Debug;
use std::sync::Arc;
use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};
use tpchgen_arrow::{
    CustomerArrow, LineItemArrow, NationArrow, OrderArrow, PartArrow, PartSuppArrow,
    RecordBatchIterator, RegionArrow, SupplierArrow,
};

mod schemas;
mod source;
mod sql;
pub use sql::tpch_gen_sql;
mod table_funcion;
pub use table_funcion::register_tpch_udtfs;

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
enum TpchTableKind {
    LineItem,
    Nation,
    Region,
    Part,
    Supplier,
    PartSupp,
    Customer,
    Order,
}

impl TpchTableKind {
    fn schema(&self) -> Arc<Schema> {
        match *self {
            TpchTableKind::LineItem => schemas::LINEITEM_SCHEMA.clone(),
            TpchTableKind::Nation => schemas::NATION_SCHEMA.clone(),
            TpchTableKind::Region => schemas::REGION_SCHEMA.clone(),
            TpchTableKind::Part => schemas::PART_SCHEMA.clone(),
            TpchTableKind::Supplier => schemas::SUPPLIER_SCHEMA.clone(),
            TpchTableKind::PartSupp => schemas::PARTSUPP_SCHEMA.clone(),
            TpchTableKind::Customer => schemas::CUSTOMER_SCHEMA.clone(),
            TpchTableKind::Order => schemas::ORDER_SCHEMA.clone(),
        }
    }

    pub fn generator(
        &self,
        scale_factor: f64,
        part: usize,
        num_parts: usize,
    ) -> Box<dyn RecordBatchIterator> {
        match *self {
            TpchTableKind::LineItem => {
                let generator = LineItemGenerator::new(
                    scale_factor,
                    part as i32 + 1,
                    num_parts as i32,
                );
                Box::new(LineItemArrow::new(generator))
            }
            TpchTableKind::Nation => {
                let generator =
                    NationGenerator::new(scale_factor, part as i32 + 1, num_parts as i32);

                Box::new(NationArrow::new(generator))
            }
            TpchTableKind::Region => {
                let generator =
                    RegionGenerator::new(scale_factor, part as i32 + 1, num_parts as i32);

                Box::new(RegionArrow::new(generator))
            }
            TpchTableKind::Part => {
                let generator =
                    PartGenerator::new(scale_factor, part as i32 + 1, num_parts as i32);

                Box::new(PartArrow::new(generator))
            }
            TpchTableKind::Supplier => {
                let generator = SupplierGenerator::new(
                    scale_factor,
                    part as i32 + 1,
                    num_parts as i32,
                );

                Box::new(SupplierArrow::new(generator))
            }
            TpchTableKind::PartSupp => {
                let generator = PartSuppGenerator::new(
                    scale_factor,
                    part as i32 + 1,
                    num_parts as i32,
                );

                Box::new(PartSuppArrow::new(generator))
            }
            TpchTableKind::Customer => {
                let generator = CustomerGenerator::new(
                    scale_factor,
                    part as i32 + 1,
                    num_parts as i32,
                );

                Box::new(CustomerArrow::new(generator))
            }
            TpchTableKind::Order => {
                let generator =
                    OrderGenerator::new(scale_factor, part as i32 + 1, num_parts as i32);

                Box::new(OrderArrow::new(generator))
            }
        }
    }
}
