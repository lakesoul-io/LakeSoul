// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shared data-file read path for index builders.
//!
//! Index builds read the shard's data files through [`LakeSoulReader`], so
//! they see exactly the rows a query would see (merge-on-read, CDC, ...).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema;

use crate::config::LakeSoulIOConfigBuilder;
use crate::session::LakeSoulIOSession;

/// Parent directory of the first file, URL scheme preserved.
///
/// All files of a shard share the same partition directory, so the first
/// file determines the reader prefix.
pub fn table_prefix(file_paths: &[String]) -> String {
    file_paths
        .first()
        .and_then(|url| {
            let (scheme, rest) = if let Some(rest) = url.strip_prefix("file://") {
                ("file://", rest)
            } else if let Some(rest) = url.strip_prefix("s3://") {
                ("s3://", rest)
            } else if let Some(rest) = url.strip_prefix("s3a://") {
                ("s3a://", rest)
            } else {
                ("", url.as_str())
            };
            std::path::Path::new(rest.trim_end_matches('/'))
                .parent()?
                .to_str()
                .map(|s| format!("{scheme}{s}"))
        })
        .unwrap_or_default()
}

/// Reader config for a shard, with the object-store options passed through.
///
/// The simplified keys used by `create_object_store` (access_key_id,
/// endpoint, ...) are harmless here — the reader only acts on the
/// `fs.s3a.*` keys it recognises.
pub fn shard_reader_config_builder(
    file_paths: &[String],
    pk_column: &str,
    object_store_options: &HashMap<String, String>,
    default_fs: Option<&str>,
) -> LakeSoulIOConfigBuilder {
    let mut config_builder = LakeSoulIOConfigBuilder::new()
        .with_files(file_paths.to_vec())
        .with_prefix(table_prefix(file_paths))
        .with_primary_keys(vec![pk_column.to_string()]);

    for (key, value) in object_store_options {
        if key != "type" {
            config_builder =
                config_builder.with_object_store_option(key.clone(), value.clone());
        }
    }
    if let Some(default_fs) = default_fs {
        config_builder = config_builder
            .with_object_store_option("fs.defaultFS".to_string(), default_fs.to_string());
    }

    config_builder
}

/// Read `pk_column` plus the projected columns of every row of the shard's
/// data files.
///
/// The schema is inferred through LakeSoul's format registry so Parquet,
/// Vortex, and remote object stores all use the same schema path as the
/// actual reader.  Empty row groups are skipped.
pub async fn read_shard_batches(
    file_paths: &[String],
    pk_column: &str,
    projection: &[String],
    object_store_options: &HashMap<String, String>,
    default_fs: Option<&str>,
) -> crate::Result<Vec<RecordBatch>> {
    let mut results = Vec::new();
    if file_paths.is_empty() {
        return Ok(results);
    }

    // Infer through LakeSoul's format registry so Parquet, Vortex, and remote
    // object stores all use the same schema path as the actual reader.
    let inference_config = shard_reader_config_builder(
        file_paths,
        pk_column,
        object_store_options,
        default_fs,
    )
    .set_inferring_schema(true)
    .build();
    let inference_session =
        LakeSoulIOSession::try_new(inference_config).map_err(|e| {
            rootcause::report!("failed to create schema inference session: {}", e)
        })?;
    let inferred_schema = inference_session
        .get_table_schema()
        .await
        .map_err(|e| rootcause::report!("failed to infer data file schema: {}", e))?;
    let file_schema = inferred_schema.file_schema();
    let mut fields = Vec::with_capacity(projection.len() + 1);
    fields.push(
        file_schema
            .field_with_name(pk_column)
            .map_err(|e| {
                rootcause::report!("PK column '{}' not found: {}", pk_column, e)
            })?
            .clone(),
    );
    for column in projection {
        fields.push(
            file_schema
                .field_with_name(column)
                .map_err(|e| {
                    rootcause::report!("index column '{}' not found: {}", column, e)
                })?
                .clone(),
        );
    }
    let schema = Arc::new(Schema::new(fields));

    let io_config = shard_reader_config_builder(
        file_paths,
        pk_column,
        object_store_options,
        default_fs,
    )
    .with_schema(schema)
    .build();
    let mut reader = crate::reader::LakeSoulReader::new(io_config)
        .map_err(|e| rootcause::report!("failed to create reader: {}", e))?;
    reader
        .start()
        .await
        .map_err(|e| rootcause::report!("failed to start reader: {}", e))?;

    while let Some(batch_result) = reader.next_rb().await {
        let batch = batch_result.map_err(|e| rootcause::report!("read error: {}", e))?;
        if batch.num_rows() == 0 {
            continue;
        }
        results.push(batch);
    }

    Ok(results)
}
