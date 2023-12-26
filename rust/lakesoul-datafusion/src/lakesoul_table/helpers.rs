// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::{datatypes::Schema, record_batch::RecordBatch};

use datafusion::{logical_expr::{Expr, col}, physical_expr::{create_physical_expr, PhysicalSortExpr}, physical_plan::Partitioning, execution::context::SessionState, physical_planner::create_physical_sort_expr, common::DFSchema, error::Result, scalar::ScalarValue};

use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
use proto::proto::entity::TableInfo;

use crate::{serialize::arrow_java::schema_from_metadata_str, catalog::{parse_table_info_partitions, LakeSoulTableProperty}};

pub(crate) fn create_io_config_builder_from_table_info(table_info: Arc<TableInfo>) -> LakeSoulIOConfigBuilder {
    let (range_partitions, hash_partitions) = parse_table_info_partitions(table_info.partitions.clone());
    let properties = serde_json::from_str::<LakeSoulTableProperty>(&table_info.properties).unwrap();
    LakeSoulIOConfigBuilder::new()
        .with_schema(schema_from_metadata_str(&table_info.table_schema))
        .with_prefix(table_info.table_path.clone())
        .with_primary_keys(
            hash_partitions
        )
        .with_range_partitions(
            range_partitions
        )
        .with_hash_bucket_num(
            properties.hash_bucket_num.unwrap_or(1)
        )
}



pub fn get_columnar_value(
    batch: &RecordBatch
) -> Vec<(String, ScalarValue)> {
    vec![]
}