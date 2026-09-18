// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{env, sync::Arc};

use arrow::array::RecordBatch;
use lakesoul_common::ser::arrow_java::schema_to_metadata_parts;
use lakesoul_metadata_proto::entity::TableInfo;
use rootcause::report;

use crate::{Result, catalog::LakeSoulTableProperty};

#[cfg(test)]
use lakesoul_io::config::LakeSoulIOConfig;
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
mod text_search_tests;
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
    create_table_inner(client, table_name, config, None, None).await
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
    create_table_inner(client, table_name, config, vector_index_columns, None).await
}

/// Create a LakeSoul table that declares text indexes through the
/// `text_index_columns` table property, so writes auto-build the indexes.
///
/// The configuration is validated against the table schema and primary
/// keys *before* any metadata is created.
#[allow(dead_code)] // used for test
pub(crate) async fn create_table_with_text_index(
    client: MetaDataClientRef,
    table_name: &str,
    config: LakeSoulIOConfig,
    text_index_configs: &[crate::text_index::TextIndexTableConfig],
) -> Result<()> {
    crate::text_index::validate_text_index_configs(
        text_index_configs,
        config.target_schema().as_ref(),
        config.primary_keys_slice(),
    )?;
    let text_index_columns = (!text_index_configs.is_empty())
        .then(|| crate::text_index::text_index_columns_to_json(text_index_configs));
    create_table_inner(client, table_name, config, None, text_index_columns).await
}

async fn create_table_inner(
    client: MetaDataClientRef,
    table_name: &str,
    config: LakeSoulIOConfig,
    vector_index_columns: Option<String>,
    text_index_columns: Option<String>,
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
                text_index_columns,
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
