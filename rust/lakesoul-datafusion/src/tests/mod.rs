// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{env, sync::Arc};

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use lakesoul_common::ser::arrow_java::schema_to_metadata_parts;
use lakesoul_metadata_proto::entity::TableInfo;
use rootcause::report;

use crate::{
    Result,
    catalog::{LakeSoulTableProperty, format_table_info_partitions},
};

#[cfg(test)]
use lakesoul_io::config::{
    LakeSoulIOConfig, LakeSoulIOConfigBuilder, OPTION_KEY_CDC_COLUMN,
    OPTION_KEY_STABLE_SORT,
};
use lakesoul_metadata::MetaDataClient;
#[cfg(test)]
use lakesoul_metadata::MetaDataClientRef;

#[cfg(test)]
mod hash_tests;
#[cfg(test)]
mod insert_tests;
#[cfg(test)]
mod pk_locator_tests;
#[cfg(test)]
mod upsert_tests;
#[cfg(test)]
mod vector_search_tests;
// mod compaction_tests;
// mod streaming_tests;
#[cfg(feature = "ci")]
mod integration_tests;

#[cfg(feature = "ci")]
mod benchmarks;

#[cfg(test)]
mod catalog_tests;
#[cfg(test)]
mod distributed_tests;
#[cfg(test)]
mod session_factory_tests;
#[cfg(test)]
mod vortex_catalog_tests;

// in cargo test, this executed only once
#[ctor::ctor]
fn init() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let client = Arc::new(MetaDataClient::from_env().await.unwrap());
            client.meta_cleanup().await.unwrap();
            debug!("clean metadata");
        })
}

#[track_caller]
fn assert_batches_eq(table_name: &str, expected: &[&str], results: &[RecordBatch]) {
    // let expected_lines: Vec<String> =
    //         expected.iter().map(|&s| s.into()).collect();
    let (schema, remain) = expected.split_at(3);
    let (expected, end) = remain.split_at(remain.len() - 1);
    let mut expected = Vec::from(expected);

    expected.sort();

    let expected_lines = [schema, &expected, end].concat();

    let formatted = datafusion::arrow::util::pretty::pretty_format_batches(results)
        .unwrap()
        .to_string();

    let actual_lines: Vec<&str> = formatted.trim().lines().collect();
    let (schema, remain) = actual_lines.split_at(3);
    let (result, end) = remain.split_at(remain.len() - 1);
    let mut result = Vec::from(result);

    result.sort();

    let result = [schema, &result, end].concat();

    assert_eq!(
        expected_lines, result,
        "\n\n{}\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        table_name, expected_lines, result
    );
}

/// Register a LakeSoul table in the LakeSoul metadata.
#[allow(dead_code)]
#[cfg(test)]
pub(crate) async fn create_table(
    client: MetaDataClientRef,
    table_name: &str,
    config: LakeSoulIOConfig,
) -> Result<()> {
    create_table_inner(client, table_name, config, None).await
}

/// Create a LakeSoul table that declares vector indexes through the
/// `vector_index_columns` table property, so writes auto-build the indexes
/// (same semantics as the Python SDK's `vector_index` argument).
///
/// The configuration is validated against the table schema and primary
/// keys *before* any metadata is created.
#[allow(dead_code)] // used for test
pub(crate) async fn create_table_with_vector_index(
    client: MetaDataClientRef,
    table_name: &str,
    config: LakeSoulIOConfig,
    vector_index_configs: &[crate::vector_index::VectorIndexTableConfig],
) -> Result<()> {
    crate::vector_index::validate_vector_index_configs(
        vector_index_configs,
        config.target_schema().as_ref(),
        config.primary_keys_slice(),
    )?;
    let vector_index_columns = (!vector_index_configs.is_empty())
        .then(|| crate::vector_index::vector_index_columns_to_json(vector_index_configs));
    create_table_inner(client, table_name, config, vector_index_columns).await
}

async fn create_table_inner(
    client: MetaDataClientRef,
    table_name: &str,
    config: LakeSoulIOConfig,
    vector_index_columns: Option<String>,
) -> Result<()> {
    debug!("create_table: {:?}", &table_name);
    let target_schema = config.target_schema();
    let (table_schema, table_schema_arrow_ipc, table_schema_arrow_ipc_json_hash) =
        schema_to_metadata_parts(target_schema.as_ref());

    client
        .create_table(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_name: table_name.to_string(),
            table_path: format!(
                "file://{}/default/{}",
                env::current_dir()
                    .unwrap()
                    .to_str()
                    .ok_or(report!("can not get $TMPDIR"))?,
                table_name
            ),
            table_schema,
            table_schema_arrow_ipc,
            table_schema_arrow_ipc_json_hash,
            table_namespace: "default".to_string(),
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(String::from("4")),
                vector_index_columns,
                ..Default::default()
            })?,
            partitions: format!(
                "{};{}",
                config
                    .range_partitions_slice()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(","),
                config
                    .primary_keys_slice()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            domain: "public".to_string(),
        })
        .await?;
    Ok(())
}

/// CDC-enabled primary-key schema: `op` carries `insert`/`delete`.
pub(crate) fn cdc_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
        Field::new("op", DataType::Utf8, true),
    ]))
}

pub(crate) fn cdc_batch(ids: &[i32], scores: &[i32], ops: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        cdc_schema(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(scores.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(ops.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

/// Creates the CDC table shape LakeSoul's delete tombstones need: a change
/// column plus the `use_cdc` table property.
pub(crate) async fn create_cdc_table(
    client: MetaDataClientRef,
    table_name: &str,
) -> Result<()> {
    let primary_keys = vec!["id".to_string()];
    let io_config = LakeSoulIOConfigBuilder::new()
        .with_schema(cdc_schema())
        .with_primary_keys(primary_keys.clone())
        .with_option(OPTION_KEY_CDC_COLUMN, "op")
        .with_option(OPTION_KEY_STABLE_SORT, "true")
        .build();
    let target_schema = io_config.target_schema();
    let (table_schema, table_schema_arrow_ipc, table_schema_arrow_ipc_json_hash) =
        schema_to_metadata_parts(target_schema.as_ref());

    client
        .create_table(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_name: table_name.to_string(),
            table_path: format!(
                "file://{}/default/{}",
                env::current_dir()
                    .unwrap()
                    .to_str()
                    .ok_or(report!("can not get $TMPDIR"))?,
                table_name
            ),
            table_schema,
            table_schema_arrow_ipc,
            table_schema_arrow_ipc_json_hash,
            table_namespace: "default".to_string(),
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(String::from("4")),
                cdc_change_column: Some(String::from("op")),
                use_cdc: Some(String::from("true")),
                ..Default::default()
            })?,
            partitions: format_table_info_partitions(&[], &primary_keys),
            domain: "public".to_string(),
        })
        .await?;
    Ok(())
}
