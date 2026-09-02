// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Vector index configuration and the automatic index build for writes.
//!
//! Like the Python SDK, a table enables vector search by storing a
//! `vector_index_columns` table property — a JSON array of index
//! configurations.  After a write commits, the sink reads that property
//! and builds/updates one IVF+RaBitQ index shard per
//! `(partition, hash bucket)` from the newly written files; the Rust
//! builder performs an incremental delta update when the shard index
//! already exists.

use std::collections::HashMap;
use std::sync::Arc;

use lakesoul_io::helpers::extract_hash_bucket_id;
use lakesoul_io::vector::builder::VectorShardIndexBuilder;
use lakesoul_vector::{Metric, RotatorType, VectorIndexConfig};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use rootcause::{bail, report};

use crate::Result;

/// Property key holding the vector index configurations (JSON).
pub const VECTOR_INDEX_COLUMNS_KEY: &str = "vector_index_columns";

fn default_nlist() -> usize {
    256
}

fn default_total_bits() -> usize {
    7
}

fn default_metric() -> String {
    "L2".to_string()
}

fn default_rotator_type() -> String {
    "FhtKac".to_string()
}

fn default_seed() -> u64 {
    42
}

fn default_use_faster_config() -> bool {
    true
}

/// One entry of the `vector_index_columns` table property (JSON).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VectorIndexTableConfig {
    /// Vector column name (must exist in the table schema).
    pub column: String,
    /// Vector dimension.
    pub dim: usize,
    /// Number of IVF clusters.
    #[serde(default = "default_nlist")]
    pub nlist: usize,
    /// RaBitQ total bits (1-16).
    #[serde(default = "default_total_bits")]
    pub total_bits: usize,
    /// Distance metric: `"L2"` or `"IP"`.
    #[serde(default = "default_metric")]
    pub metric: String,
    /// Rotation: `"FhtKac"` or `"Matrix"`.
    #[serde(default = "default_rotator_type")]
    pub rotator_type: String,
    /// Random seed.
    #[serde(default = "default_seed")]
    pub seed: u64,
    /// Fast quantization mode.
    #[serde(default = "default_use_faster_config")]
    pub use_faster_config: bool,
}

impl VectorIndexTableConfig {
    /// Convert into the native vector index configuration.
    pub fn to_vector_index_config(&self) -> Result<VectorIndexConfig> {
        let metric = match self.metric.to_uppercase().as_str() {
            "L2" => Metric::L2,
            "IP" | "INNERPRODUCT" => Metric::InnerProduct,
            other => bail!("unsupported vector index metric: {other}"),
        };
        let rotator_type = match self.rotator_type.to_lowercase().as_str() {
            "fhtkac" => RotatorType::FhtKacRotator,
            "matrix" => RotatorType::MatrixRotator,
            other => bail!("unsupported vector index rotator type: {other}"),
        };
        Ok(VectorIndexConfig {
            column_name: self.column.clone(),
            dim: self.dim,
            nlist: self.nlist,
            total_bits: self.total_bits,
            metric,
            rotator_type,
            seed: self.seed,
            use_faster_config: self.use_faster_config,
        })
    }
}

/// Serialize configurations into the `vector_index_columns` property value.
pub fn vector_index_columns_to_json(configs: &[VectorIndexTableConfig]) -> String {
    serde_json::to_string(configs).unwrap_or_else(|_| "[]".to_string())
}

/// Validate that a table schema can support the configured vector indexes,
/// before any metadata is created (mirrors the Python SDK).
///
/// Requires an `UInt64`/`Int64` primary key (the index maps search results
/// to primary key values) and `FixedSizeList`/`List` of `Float32`/`Float64`
/// vector columns whose dimension matches the configured `dim`.
pub fn validate_vector_index_configs(
    configs: &[VectorIndexTableConfig],
    schema: &arrow::datatypes::Schema,
    primary_keys: &[String],
) -> Result<()> {
    use arrow::datatypes::DataType;
    if configs.is_empty() {
        return Ok(());
    }
    let Some(pk_column) = primary_keys.first() else {
        bail!(
            "a vector index requires an id column: pass primary_keys=[...] \
             when creating a table with vector_index (the index maps search \
             results to primary key values)"
        );
    };
    let Some(pk_index) = schema.index_of(pk_column).ok() else {
        bail!("vector index primary key '{pk_column}' not found in table schema");
    };
    match schema.field(pk_index).data_type() {
        DataType::UInt64 | DataType::Int64 => {}
        other => bail!(
            "vector index primary key '{pk_column}' must be UInt64 or Int64, got {other}"
        ),
    }
    for config in configs {
        let column = &config.column;
        let Some(column_index) = schema.index_of(column).ok() else {
            bail!("vector index column '{column}' not found in table schema");
        };
        let data_type = schema.field(column_index).data_type();
        let (element_type, fixed_len) = match data_type {
            DataType::FixedSizeList(field, len) => {
                (field.data_type(), Some(*len as usize))
            }
            DataType::List(field) | DataType::LargeList(field) => {
                (field.data_type(), None)
            }
            other => {
                bail!(
                    "vector index column '{column}' must be FixedSizeList or List \
                     of Float32/Float64, got {other}"
                )
            }
        };
        match element_type {
            DataType::Float32 | DataType::Float64 => {}
            other => bail!(
                "vector index column '{column}' elements must be Float32/Float64, got {other}"
            ),
        }
        if let Some(len) = fixed_len
            && config.dim != len
        {
            bail!(
                "vector index column '{column}' dim {} does not match schema \
                 FixedSizeList size {len}",
                config.dim
            );
        }
    }
    Ok(())
}

/// Parse a `vector_index_columns` property value.
///
/// The value is normally a JSON string containing a JSON array; a raw JSON
/// array is accepted as well.  Empty or missing values yield an empty list.
pub fn parse_vector_index_columns(
    raw: Option<&str>,
) -> Result<Vec<VectorIndexTableConfig>> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let raw = raw.trim();
    if raw.is_empty() || raw == "[]" {
        return Ok(Vec::new());
    }
    let value: serde_json::Value = serde_json::from_str(raw)?;
    let array = match value {
        serde_json::Value::String(s) => {
            let inner = s.trim();
            if inner.is_empty() {
                return Ok(Vec::new());
            }
            serde_json::from_str::<serde_json::Value>(inner)?
        }
        value => value,
    };
    Ok(serde_json::from_value(array)?)
}

/// Extract the vector index configurations from a table's raw properties
/// JSON (the `TableInfo.properties` column).
pub fn parse_vector_index_from_table_properties(
    properties_json: &str,
) -> Result<Vec<VectorIndexTableConfig>> {
    let properties: serde_json::Value = serde_json::from_str(properties_json)?;
    match properties.get(VECTOR_INDEX_COLUMNS_KEY) {
        Some(serde_json::Value::String(s)) => parse_vector_index_columns(Some(s)),
        Some(value) => Ok(serde_json::from_value(value.clone())?),
        None => Ok(Vec::new()),
    }
}

/// Build (or incrementally update) the vector index for newly committed
/// files of a write.
///
/// Files are grouped by `(partition_desc, hash_bucket_id)`; each group is
/// one index shard and is handed to the native builder, which derives the
/// index location from the files' directory and performs a delta update
/// when the shard index already exists.  Fails loudly when any shard of a
/// configured column fails.
///
/// `partition_files` maps a partition description to its newly written
/// (file path, row count) pairs, as produced by the write sink.
pub async fn auto_build_vector_index(
    configs: &[VectorIndexTableConfig],
    primary_keys: &[String],
    object_store_options: &HashMap<String, String>,
    partition_files: &HashMap<String, (Vec<String>, u64)>,
) -> Result<usize> {
    if configs.is_empty() || partition_files.is_empty() {
        return Ok(0);
    }
    let Some(pk_column) = primary_keys.first() else {
        bail!("a vector index requires a table with a primary key");
    };
    let Some(first_file) = partition_files
        .values()
        .find_map(|(files, _)| files.first())
    else {
        return Ok(0);
    };
    let store = store_for_files(first_file, object_store_options)?;

    let mut built = 0usize;
    for config in configs {
        let vector_config = config.to_vector_index_config()?;
        let mut failures: Vec<String> = Vec::new();
        // One shard per (partition_desc, hash_bucket_id) — files from
        // different range partitions must never share a shard.
        let mut shards: HashMap<(String, u32), Vec<String>> = HashMap::new();
        for (partition_desc, (files, _)) in partition_files {
            for file in files {
                if let Some(bucket) = extract_hash_bucket_id(file) {
                    shards
                        .entry((partition_desc.clone(), bucket))
                        .or_default()
                        .push(file.clone());
                }
            }
        }
        for ((partition_desc, bucket), bucket_files) in shards {
            let result = VectorShardIndexBuilder::new(
                store.clone(),
                vector_config.clone(),
                bucket_files,
                pk_column.clone(),
                object_store_options.clone(),
                None,
            )
            .build()
            .await;
            match result {
                Ok(()) => built += 1,
                Err(error) => failures.push(format!(
                    "partition {partition_desc:?} bucket {bucket}: {error}"
                )),
            }
        }
        if !failures.is_empty() {
            return Err(report!(
                "vector index build failed for column '{}': {}",
                config.column,
                failures.join("; ")
            ));
        }
    }
    Ok(built)
}

/// Build an object store for the vector index from the table's files.
fn store_for_files(
    first_file: &str,
    object_store_options: &HashMap<String, String>,
) -> Result<Arc<dyn ObjectStore>> {
    if first_file.starts_with("s3://") || first_file.starts_with("s3a://") {
        Ok(Arc::new(
            lakesoul_io::object_store::create_s3_store_from_options(
                object_store_options,
            )?,
        ))
    } else {
        Ok(Arc::new(LocalFileSystem::new()))
    }
}
