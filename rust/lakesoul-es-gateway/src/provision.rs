// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Table provisioning for declared indexes.
//!
//! Every declared index must have a LakeSoul table with the fixed document
//! schema, a text index on the content column, an optional vector index on
//! the embedding column and the internal CDC column used for deletes.
//! Declared-but-missing tables are created at startup; existing tables are
//! validated and a mismatch fails the gateway fast instead of silently
//! serving a different schema.

use anyhow::Context;
use arrow_schema::DataType;
use lakesoul_common::ser::arrow_java::{
    schema_from_table_info_metadata, schema_to_metadata_parts,
};
use lakesoul_datafusion::catalog::LakeSoulTableProperty;
use lakesoul_datafusion::text_index::{
    parse_text_index_columns, text_index_columns_to_json, validate_text_index_configs,
};
use lakesoul_datafusion::vector_index::{
    parse_vector_index_columns, validate_vector_index_configs,
    vector_index_columns_to_json,
};
use lakesoul_metadata_proto::entity::TableInfo;

use crate::schema::{CDC_COLUMN, PK_COLUMN};
use crate::state::{GatewayState, IndexRuntime};

/// Provision every declared index.
pub async fn provision_all(state: &GatewayState) -> anyhow::Result<()> {
    for runtime in state.indexes.values() {
        ensure_table(state, runtime)
            .await
            .with_context(|| format!("index '{}'", runtime.config.name))?;
        tracing::info!(
            index = %runtime.config.name,
            table = %runtime.table,
            "index provisioned"
        );
    }
    Ok(())
}

async fn ensure_table(
    state: &GatewayState,
    runtime: &IndexRuntime,
) -> anyhow::Result<()> {
    if let Some(info) = state
        .client
        .get_table_info_by_table_name(&runtime.table, &runtime.namespace)
        .await?
    {
        validate_existing(runtime, &info)?;
        return Ok(());
    }

    let text_index_columns = text_index_json(state, runtime)?;
    let vector_index_columns = vector_index_json(runtime)?;
    let properties = LakeSoulTableProperty {
        hash_bucket_num: Some(
            runtime.hash_bucket_num(&state.config.defaults).to_string(),
        ),
        cdc_change_column: Some(CDC_COLUMN.to_string()),
        use_cdc: Some("true".to_string()),
        text_index_columns: Some(text_index_columns),
        vector_index_columns,
        file_format: Some("parquet".to_string()),
        ..Default::default()
    };
    let (table_schema, table_schema_arrow_ipc, table_schema_arrow_ipc_json_hash) =
        schema_to_metadata_parts(runtime.schema.as_ref());

    let created = state
        .client
        .create_table_if_not_exists(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_name: runtime.table.clone(),
            table_path: runtime.path.clone(),
            table_schema,
            table_schema_arrow_ipc,
            table_schema_arrow_ipc_json_hash,
            table_namespace: runtime.namespace.clone(),
            properties: serde_json::to_string(&properties)?,
            partitions: format!(";{}", PK_COLUMN),
            domain: "public".to_string(),
        })
        .await?;

    if !created {
        let info = state
            .client
            .get_table_info_by_table_name(&runtime.table, &runtime.namespace)
            .await?
            .context("table disappeared during provisioning")?;
        validate_existing(runtime, &info)?;
    }
    Ok(())
}

/// Text index declaration of the content column.
fn text_index_json(
    state: &GatewayState,
    runtime: &IndexRuntime,
) -> anyhow::Result<String> {
    let defaults = &state.config.defaults;
    let raw = serde_json::json!([{
        "column": runtime.config.content_column,
        "tokenizer": runtime.tokenizer(defaults),
        "with_positions": runtime.with_positions(defaults),
        "stored": false,
    }])
    .to_string();
    let configs = parse_text_index_columns(Some(&raw))
        .map_err(|error| anyhow::anyhow!("text index config: {error}"))?;
    validate_text_index_configs(
        &configs,
        runtime.schema.as_ref(),
        &[PK_COLUMN.to_string()],
    )
    .map_err(|error| anyhow::anyhow!("text index validation: {error}"))?;
    Ok(text_index_columns_to_json(&configs))
}

/// Vector index declaration of the embedding column, when a dimension is
/// configured.  Vectors are normalized on write and queried with the IP
/// metric, which is cosine similarity for unit vectors.
fn vector_index_json(runtime: &IndexRuntime) -> anyhow::Result<Option<String>> {
    let Some(dim) = runtime.dim else {
        return Ok(None);
    };
    let raw = serde_json::json!([{
        "column": runtime.config.embedding_column,
        "dim": dim,
        "nlist": 256,
        "total_bits": 7,
        "metric": "IP",
        "rotator_type": "FhtKac",
        "seed": 42,
        "use_faster_config": true,
    }])
    .to_string();
    let configs = parse_vector_index_columns(Some(&raw))
        .map_err(|error| anyhow::anyhow!("vector index config: {error}"))?;
    validate_vector_index_configs(
        &configs,
        runtime.schema.as_ref(),
        &[PK_COLUMN.to_string()],
    )
    .map_err(|error| anyhow::anyhow!("vector index validation: {error}"))?;
    Ok(Some(vector_index_columns_to_json(&configs)))
}

/// Validate that an existing table matches the declared schema and indexes.
fn validate_existing(runtime: &IndexRuntime, info: &TableInfo) -> anyhow::Result<()> {
    let schema = schema_from_table_info_metadata(
        &info.table_schema,
        &info.table_schema_arrow_ipc,
        &info.table_schema_arrow_ipc_json_hash,
    )
    .map_err(|error| anyhow::anyhow!("failed to decode table schema: {error}"))?;

    let pk = schema
        .field_with_name(PK_COLUMN)
        .map_err(|_| anyhow::anyhow!("table is missing the '{PK_COLUMN}' primary key"))?;
    anyhow::ensure!(
        pk.data_type() == &DataType::UInt64,
        "primary key '{PK_COLUMN}' must be UInt64, got {}",
        pk.data_type()
    );

    let content = schema
        .field_with_name(&runtime.config.content_column)
        .map_err(|_| {
            anyhow::anyhow!(
                "table is missing the content column '{}'",
                runtime.config.content_column
            )
        })?;
    anyhow::ensure!(
        content.data_type() == &DataType::Utf8,
        "content column '{}' must be Utf8, got {}",
        runtime.config.content_column,
        content.data_type()
    );

    match (
        runtime.dim,
        schema.field_with_name(&runtime.config.embedding_column),
    ) {
        (Some(dim), Ok(field)) => match field.data_type() {
            DataType::FixedSizeList(_, size) if *size == dim as i32 => {}
            other => anyhow::bail!(
                "embedding column '{}' must be FixedSizeList<Float32, {dim}>, got {other}",
                runtime.config.embedding_column
            ),
        },
        (Some(_), Err(_)) => anyhow::bail!(
            "table is missing the embedding column '{}'",
            runtime.config.embedding_column
        ),
        (None, _) => {}
    }

    anyhow::ensure!(
        schema.field_with_name(CDC_COLUMN).is_ok(),
        "table is missing the internal CDC column '{CDC_COLUMN}'"
    );

    let properties: LakeSoulTableProperty =
        serde_json::from_str(&info.properties).unwrap_or_default();
    let text_configs = parse_text_index_columns(properties.text_index_columns.as_deref())
        .map_err(|error| anyhow::anyhow!("invalid text index properties: {error}"))?;
    anyhow::ensure!(
        text_configs
            .iter()
            .any(|config| config.column == runtime.config.content_column),
        "table has no text index on '{}'",
        runtime.config.content_column
    );

    if let Some(dim) = runtime.dim {
        let vector_configs =
            parse_vector_index_columns(properties.vector_index_columns.as_deref())
                .map_err(|error| {
                    anyhow::anyhow!("invalid vector index properties: {error}")
                })?;
        let declared = vector_configs
            .iter()
            .find(|config| config.column == runtime.config.embedding_column);
        let declared = declared.ok_or_else(|| {
            anyhow::anyhow!(
                "table has no vector index on '{}'",
                runtime.config.embedding_column
            )
        })?;
        anyhow::ensure!(
            declared.params.dim == dim,
            "vector index dimension mismatch on '{}': table has {}, config has {dim}",
            runtime.config.embedding_column,
            declared.params.dim
        );
    }
    Ok(())
}
