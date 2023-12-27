// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use datafusion::scalar::ScalarValue;

use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
use proto::proto::entity::TableInfo;

use crate::{
    catalog::{parse_table_info_partitions, LakeSoulTableProperty},
    serialize::arrow_java::schema_from_metadata_str,
};

pub(crate) fn create_io_config_builder_from_table_info(table_info: Arc<TableInfo>) -> LakeSoulIOConfigBuilder {
    let (range_partitions, hash_partitions) = parse_table_info_partitions(table_info.partitions.clone());
    let properties = serde_json::from_str::<LakeSoulTableProperty>(&table_info.properties).unwrap();
    LakeSoulIOConfigBuilder::new()
        .with_schema(schema_from_metadata_str(&table_info.table_schema))
        .with_prefix(table_info.table_path.clone())
        .with_primary_keys(hash_partitions)
        .with_range_partitions(range_partitions)
        .with_hash_bucket_num(properties.hash_bucket_num.unwrap_or(1))
}

pub fn get_columnar_value(batch: &RecordBatch) -> Vec<(String, ScalarValue)> {
    vec![]
}
