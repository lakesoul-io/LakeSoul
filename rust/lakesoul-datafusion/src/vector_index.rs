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
//!
//! # Rebuilds
//!
//! Incremental writes append vectors to the centroids trained at the
//! initial build, so after enough deltas the centroids no longer match the
//! (growing) data distribution.  A shard whose
//! `rebuild_mode == "auto"` config is therefore rebuilt from scratch (fresh
//! k-means over all of the shard's data files) as part of the write once a
//! **cluster** of the shard has accumulated
//! `delta_vectors / base_vectors > max_delta_ratio` — i.e. when new data
//! has landed in an existing cluster disproportionately, its centroid no
//! longer represents that cluster's contents.  [`rebuild_vector_index`]
//! exposes the same operation explicitly.

use std::collections::HashMap;
use std::sync::Arc;

use lakesoul_io::helpers::extract_hash_bucket_id;
use lakesoul_io::vector::builder::{VectorShardIndexBuilder, shard_index_prefix};
use lakesoul_vector::{
    ClusterStat, ManifestStore, Metric, RotatorType, VectorIndexConfig, cluster_stats,
};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use rootcause::{bail, report};
use tracing::info;

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

fn default_rebuild_mode() -> String {
    "auto".to_string()
}

fn default_max_delta_ratio() -> f32 {
    1.0
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
    /// Index rebuild strategy: `"auto"` (default) rebuilds a shard from
    /// scratch when any of its clusters' delta/base vector ratio exceeds
    /// `max_delta_ratio`; `"none"` only ever appends delta segments.
    #[serde(default = "default_rebuild_mode")]
    pub rebuild_mode: String,
    /// Auto-rebuild trigger: rebuild the shard when **any cluster** of it
    /// has accumulated `delta_vectors / base_vectors` above this ratio
    /// (per-cluster drift detection; a cluster with no base vectors but
    /// deltas has infinite ratio).
    #[serde(default = "default_max_delta_ratio")]
    pub max_delta_ratio: f32,
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
            rebuild_mode: self.rebuild_mode.to_lowercase(),
            max_delta_ratio: self.max_delta_ratio,
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
        let rebuild_mode = config.rebuild_mode.to_lowercase();
        if rebuild_mode != "auto" && rebuild_mode != "none" {
            bail!(
                "vector index column '{column}' rebuild_mode must be \"auto\" or \"none\", \
                 got {:?}",
                config.rebuild_mode
            );
        }
        if config.max_delta_ratio <= 0.0 || config.max_delta_ratio.is_nan() {
            bail!(
                "vector index column '{column}' max_delta_ratio must be > 0, got {}",
                config.max_delta_ratio
            );
        }
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

/// Build (or incrementally update, or rebuild) the vector index for newly
/// committed files of a write.
///
/// Files are grouped by `(partition_desc, hash_bucket_id)`; each group is
/// one index shard and is handed to the native builder, which derives the
/// index location from the files' directory and performs a delta update
/// when the shard index already exists.
///
/// When `all_active_files` (every currently active data file of the table,
/// from the metadata client) is provided and the shard's config uses
/// `rebuild_mode: "auto"`, a shard any of whose clusters has drifted past
/// `max_delta_ratio` (cluster delta vectors / cluster base vectors) is
/// **rebuilt** from all of its files instead of receiving another delta
/// segment.  Fails loudly when any shard of a configured column fails.
///
/// `partition_files` maps a partition description to its newly written
/// (file path, row count) pairs, as produced by the write sink.
pub async fn auto_build_vector_index(
    configs: &[VectorIndexTableConfig],
    primary_keys: &[String],
    object_store_options: &HashMap<String, String>,
    partition_files: &HashMap<String, (Vec<String>, u64)>,
    all_active_files: Option<&[String]>,
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
        let rebuild = vector_config.rebuild_mode == "auto";
        // Files of each shard, for a full rebuild when drift is detected.
        let shard_all_files: HashMap<String, Vec<String>> = if rebuild {
            match all_active_files {
                Some(all) => {
                    let mut map: HashMap<String, Vec<String>> = HashMap::new();
                    for file in all {
                        let prefix = shard_index_prefix(
                            std::slice::from_ref(file),
                            &config.column,
                        );
                        map.entry(prefix).or_default().push(file.clone());
                    }
                    map
                }
                None => HashMap::new(),
            }
        } else {
            HashMap::new()
        };
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
            // Decide: full rebuild (drift) or incremental delta?
            let prefix = shard_index_prefix(&bucket_files, &config.column);
            // Rebuild only when the complete shard file list is available —
            // rebuilding from just the new files would drop the existing
            // index contents.
            let full_shard_files = shard_all_files.get(&prefix).cloned();
            let should_rebuild = rebuild
                && full_shard_files
                    .as_ref()
                    .is_some_and(|files| !files.is_empty())
                && drift_exceeds_threshold(
                    &store,
                    &prefix,
                    vector_config.max_delta_ratio,
                )
                .await
                .unwrap_or(false);
            let result = if should_rebuild {
                let files = full_shard_files.expect("checked above");
                info!(
                    "Rebuilding vector index shard for column '{}' ({} files)",
                    config.column,
                    files.len()
                );
                VectorShardIndexBuilder::new(
                    store.clone(),
                    vector_config.clone(),
                    files,
                    pk_column.clone(),
                    object_store_options.clone(),
                    None,
                )
                .rebuild()
                .await
            } else {
                VectorShardIndexBuilder::new(
                    store.clone(),
                    vector_config.clone(),
                    bucket_files,
                    pk_column.clone(),
                    object_store_options.clone(),
                    None,
                )
                .build()
                .await
            };
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

/// Whether any cluster of the shard index at `prefix` has drifted past the
/// configured ratio (`delta_vectors / base_vectors` per cluster).  Only
/// meaningful when a manifest exists (returns `Ok(false)` otherwise).
async fn drift_exceeds_threshold(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    max_delta_ratio: f32,
) -> Result<bool> {
    let mstore = ManifestStore::new(store.clone(), prefix.to_string());
    let clusters: Option<Vec<ClusterStat>> = cluster_stats(&mstore)
        .await
        .map_err(|e| report!("failed to read vector index stats at '{prefix}': {e}"))?;
    Ok(clusters
        .is_some_and(|stats| stats.iter().any(|c| c.delta_ratio() > max_delta_ratio)))
}

/// Rebuild every vector index shard of a table from scratch (fresh
/// k-means over all of the table's active data files), regardless of the
/// configured `rebuild_mode`/`max_delta_ratio`.
///
/// Returns the number of rebuilt shards.  Fails loudly when any shard of a
/// configured column fails.
pub async fn rebuild_vector_index(
    client: &lakesoul_metadata::MetaDataClient,
    table_name: &str,
    namespace: &str,
    primary_keys: &[String],
    object_store_options: HashMap<String, String>,
) -> Result<usize> {
    let Some(table_info) = client
        .get_table_info_by_table_name(table_name, namespace)
        .await?
    else {
        bail!("table '{namespace}.{table_name}' not found");
    };
    let configs = parse_vector_index_from_table_properties(&table_info.properties)?;
    if configs.is_empty() {
        return Ok(0);
    }
    let Some(pk_column) = primary_keys.first() else {
        bail!("a vector index requires a table with a primary key");
    };
    let all_active_files = client
        .get_data_files_by_table_name(table_name, namespace)
        .await?;
    let Some(first_file) = all_active_files.first() else {
        return Ok(0);
    };
    let store = store_for_files(first_file, &object_store_options)?;

    let mut rebuilt = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for config in &configs {
        let vector_config = config.to_vector_index_config()?;
        // Group every active file into its shard.
        let mut shards: HashMap<String, Vec<String>> = HashMap::new();
        for file in &all_active_files {
            if extract_hash_bucket_id(file).is_some() {
                let prefix =
                    shard_index_prefix(std::slice::from_ref(file), &config.column);
                shards.entry(prefix).or_default().push(file.clone());
            }
        }
        for (prefix, files) in shards {
            let result = VectorShardIndexBuilder::new(
                store.clone(),
                vector_config.clone(),
                files,
                pk_column.clone(),
                object_store_options.clone(),
                None,
            )
            .rebuild()
            .await;
            match result {
                Ok(()) => rebuilt += 1,
                Err(error) => failures.push(format!("{prefix}: {error}")),
            }
        }
    }
    if !failures.is_empty() {
        return Err(report!(
            "vector index rebuild failed for column(s): {}",
            failures.join("; ")
        ));
    }
    Ok(rebuilt)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn schema() -> arrow::datatypes::Schema {
        use arrow::datatypes::{DataType, Field};
        arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    8,
                ),
                false,
            ),
        ])
    }

    fn config(rebuild_mode: &str, max_delta_ratio: f32) -> VectorIndexTableConfig {
        VectorIndexTableConfig {
            column: "vec".to_string(),
            dim: 8,
            nlist: 4,
            total_bits: 7,
            metric: "L2".to_string(),
            rotator_type: "FhtKac".to_string(),
            seed: 42,
            use_faster_config: true,
            rebuild_mode: rebuild_mode.to_string(),
            max_delta_ratio,
        }
    }

    #[test]
    fn parse_and_validate_rebuild_options() {
        let configs =
            parse_vector_index_columns(Some(r#"[{"column":"vec","dim":8}]"#)).unwrap();
        assert_eq!(configs[0].rebuild_mode, "auto", "default is auto");
        assert_eq!(configs[0].max_delta_ratio, 1.0);

        let configs = parse_vector_index_columns(Some(
            r#"[{"column":"vec","dim":8,"rebuild_mode":"none","max_delta_ratio":0.25}]"#,
        ))
        .unwrap();
        assert_eq!(configs[0].rebuild_mode, "none");
        assert_eq!(configs[0].max_delta_ratio, 0.25);
        validate_vector_index_configs(&configs, &schema(), &["id".to_string()]).unwrap();
    }

    #[test]
    fn validate_rejects_bad_rebuild_options() {
        let err = validate_vector_index_configs(
            &[config("sometimes", 1.0)],
            &schema(),
            &["id".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("rebuild_mode"), "{err}");

        let err = validate_vector_index_configs(
            &[config("auto", 0.0)],
            &schema(),
            &["id".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("max_delta_ratio"), "{err}");
    }
}
