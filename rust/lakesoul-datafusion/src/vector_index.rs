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
//! The kind-agnostic parts (config parsing, shard grouping, delta/rebuild
//! planning, committing and GC) live in [`crate::index`].
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
use std::time::Duration;

use lakesoul_common::IndexKind;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::index::prefix::shard_index_prefix;
use lakesoul_io::vector::builder::VectorShardIndexBuilder;
use lakesoul_metadata::index_catalog::{
    CommitMode, IndexCommitView, VectorCatalog, VectorSegmentEntry,
};
use lakesoul_vector::{Metric, RotatorType, SegmentEntry, VectorIndexConfig};
use rootcause::{bail, report};
use tracing::warn;

use crate::Result;
use crate::index::build::{
    commit_if_non_empty, group_files_by_shard, group_shard_files, plan_shard_build,
};
use crate::index::config::{
    IndexTableConfig, index_columns_to_json, parse_index_columns,
    parse_index_from_table_properties,
};
use crate::index::gc::{IndexGcOptions, IndexGcReport, gc_index_shards, gc_shard_now};
use crate::index::store_for_files;

pub use crate::index::gc::DEFAULT_GC_GRACE_SECONDS;

/// Property key holding the vector index configurations (JSON).
pub const VECTOR_INDEX_COLUMNS_KEY: &str = IndexKind::Vector.property_key();

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

/// Vector-specific parameters of one `vector_index_columns` entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VectorIndexParams {
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

/// One entry of the `vector_index_columns` table property (JSON).
pub type VectorIndexTableConfig = IndexTableConfig<VectorIndexParams>;

impl IndexTableConfig<VectorIndexParams> {
    /// Convert into the native vector index configuration.
    pub fn to_vector_index_config(&self) -> Result<VectorIndexConfig> {
        let metric = match self.params.metric.to_uppercase().as_str() {
            "L2" => Metric::L2,
            "IP" | "INNERPRODUCT" => Metric::InnerProduct,
            other => bail!("unsupported vector index metric: {other}"),
        };
        let rotator_type = match self.params.rotator_type.to_lowercase().as_str() {
            "fhtkac" => RotatorType::FhtKacRotator,
            "matrix" => RotatorType::MatrixRotator,
            other => bail!("unsupported vector index rotator type: {other}"),
        };
        Ok(VectorIndexConfig {
            column_name: self.column.clone(),
            dim: self.params.dim,
            nlist: self.params.nlist,
            total_bits: self.params.total_bits,
            metric,
            rotator_type,
            seed: self.params.seed,
            use_faster_config: self.params.use_faster_config,
            rebuild_mode: self.management.rebuild_mode.to_lowercase(),
            max_delta_ratio: self.management.max_delta_ratio,
        })
    }
}

/// Serialize configurations into the `vector_index_columns` property value.
pub fn vector_index_columns_to_json(configs: &[VectorIndexTableConfig]) -> String {
    index_columns_to_json(configs)
}

/// Parse a `vector_index_columns` property value.
pub fn parse_vector_index_columns(
    raw: Option<&str>,
) -> Result<Vec<VectorIndexTableConfig>> {
    parse_index_columns(raw)
}

/// Extract the vector index configurations from a table's raw properties
/// JSON (the `TableInfo.properties` column).
pub fn parse_vector_index_from_table_properties(
    properties_json: &str,
) -> Result<Vec<VectorIndexTableConfig>> {
    parse_index_from_table_properties(properties_json, VECTOR_INDEX_COLUMNS_KEY)
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
        config.management.validate(IndexKind::Vector, column)?;
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
            && config.params.dim != len
        {
            bail!(
                "vector index column '{column}' dim {} does not match schema \
                 FixedSizeList size {len}",
                config.params.dim
            );
        }
    }
    Ok(())
}

/// Knobs for an explicit garbage collection run.
pub type VectorIndexGcOptions = IndexGcOptions;

/// What a garbage collection run did.
pub type VectorIndexGcReport = IndexGcReport;

/// Whether an object file in a shard directory belongs to the vector index.
fn is_vector_segment_file(name: &str) -> bool {
    name.starts_with("cluster_") && name.ends_with(".seg")
}

fn to_resolved_shard(
    prefix: &str,
    view: &IndexCommitView<VectorSegmentEntry>,
) -> Result<ResolvedIndex> {
    let segments: Vec<SegmentEntry> = view
        .segments
        .iter()
        .map(|segment| SegmentEntry {
            cluster_id: segment.cluster_id,
            segment_version: segment.segment_version,
            segment_filename: segment.filename.clone(),
            num_vectors: segment.num_vectors,
            file_size: segment.file_size,
        })
        .collect();
    Ok(ResolvedIndex {
        kind: IndexKind::Vector,
        index_prefix: prefix.to_string(),
        commit_id: view.commit_id,
        generation: view.generation,
        version: view.version,
        header: view.header.clone(),
        segments: serde_json::to_value(&segments).map_err(|error| {
            report!("failed to serialize vector index segments: {}", error)
        })?,
    })
}

fn to_catalog_segments(segments: &[SegmentEntry]) -> Vec<VectorSegmentEntry> {
    segments
        .iter()
        .map(|segment| VectorSegmentEntry {
            cluster_id: segment.cluster_id,
            segment_version: segment.segment_version,
            filename: segment.segment_filename.clone(),
            num_vectors: segment.num_vectors,
            file_size: segment.file_size,
        })
        .collect()
}

/// Build (or incrementally update, or rebuild) the vector index for newly
/// committed files of a write, then optionally garbage collect.
///
/// Files are grouped by `(partition_desc, hash_bucket_id)`; each group is
/// one index shard.  The current commit of every shard is resolved from the
/// catalog ([`VectorCatalog`]): without a commit the shard is built fresh,
/// with one the new vectors are appended as delta segments, and when any
/// cluster has drifted past `max_delta_ratio` the whole shard is rebuilt
/// from all of its active data files.
pub async fn auto_build_vector_index(
    configs: &[VectorIndexTableConfig],
    primary_keys: &[String],
    object_store_options: &HashMap<String, String>,
    partition_files: &HashMap<String, (Vec<String>, u64)>,
    all_active_files: Option<&[String]>,
    catalog: &VectorCatalog,
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
        let management = &config.management;
        let auto_rebuild = vector_config.rebuild_mode == "auto";
        // Files of each shard, for a full rebuild when drift is detected.
        let shard_all_files: HashMap<String, Vec<String>> = if auto_rebuild {
            match all_active_files {
                Some(all) => group_files_by_shard(all, IndexKind::Vector, &config.column),
                None => HashMap::new(),
            }
        } else {
            HashMap::new()
        };
        let mut failures: Vec<String> = Vec::new();
        for shard in group_shard_files(partition_files) {
            let (partition_desc, bucket, bucket_files) =
                (shard.partition_desc, shard.bucket, shard.files);
            let prefix =
                shard_index_prefix(&bucket_files, IndexKind::Vector, &config.column);
            let resolved = match catalog.resolve(&prefix).await {
                Ok(view) => view,
                Err(error) => {
                    failures.push(format!(
                        "partition {partition_desc:?} bucket {bucket}: failed to resolve index: {error}"
                    ));
                    continue;
                }
            };
            let full_shard_files = shard_all_files.get(&prefix).cloned();
            let should_rebuild = auto_rebuild
                && full_shard_files
                    .as_ref()
                    .is_some_and(|files| !files.is_empty())
                && drift_exceeds_threshold(catalog, &prefix, management.max_delta_ratio)
                    .await
                    .unwrap_or(false);
            let plan = plan_shard_build(
                resolved.as_ref(),
                should_rebuild,
                full_shard_files,
                bucket_files,
            );
            let mut builder = VectorShardIndexBuilder::new(
                store.clone(),
                vector_config.clone(),
                plan.files.clone(),
                pk_column.clone(),
                object_store_options.clone(),
                None,
            );
            if let Some(view) = &plan.base {
                builder = builder.with_base(to_resolved_shard(&prefix, view)?);
            }
            let mut commit_mode = plan.mode;
            let outcome = match builder.build().await {
                Ok(outcome) => outcome,
                Err(error) if commit_mode == CommitMode::Delta => {
                    // The resolved commit points at files that no longer
                    // exist (directory removed out-of-band, restored backup,
                    // or a stale control-plane row).  Heal by rebuilding the
                    // shard from its full data files instead of failing the
                    // write.
                    warn!(
                        "incremental vector index build for '{prefix}' failed ({error}); \
                         rebuilding the shard from scratch"
                    );
                    commit_mode = CommitMode::Rebuild;
                    match VectorShardIndexBuilder::new(
                        store.clone(),
                        vector_config.clone(),
                        plan.files,
                        pk_column.clone(),
                        object_store_options.clone(),
                        None,
                    )
                    .rebuild()
                    .await
                    {
                        Ok(outcome) => outcome,
                        Err(error) => {
                            failures.push(format!(
                                "partition {partition_desc:?} bucket {bucket}: {error}"
                            ));
                            continue;
                        }
                    }
                }
                Err(error) => {
                    failures.push(format!(
                        "partition {partition_desc:?} bucket {bucket}: {error}"
                    ));
                    continue;
                }
            };
            match commit_if_non_empty(
                catalog,
                &prefix,
                &outcome.header,
                &to_catalog_segments(&outcome.new_segments),
                commit_mode,
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => continue,
                Err(error) => {
                    failures.push(format!(
                        "partition {partition_desc:?} bucket {bucket}: {error}"
                    ));
                    continue;
                }
            }
            built += 1;
            if management.gc_enabled
                && let Err(error) = gc_shard_now(
                    &store,
                    catalog,
                    &prefix,
                    Duration::from_secs(management.gc_grace_seconds),
                    management.gc_keep_generations,
                    &is_vector_segment_file,
                )
                .await
            {
                warn!("vector index gc failed for '{prefix}': {error}");
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
/// meaningful when a commit exists (returns `Ok(false)` otherwise).
async fn drift_exceeds_threshold(
    catalog: &VectorCatalog,
    prefix: &str,
    max_delta_ratio: f32,
) -> Result<bool> {
    let clusters = catalog
        .cluster_stats(prefix)
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
    let catalog = client.vector_index_catalog();

    let mut rebuilt = 0usize;
    let mut failures: Vec<String> = Vec::new();
    for config in &configs {
        let vector_config = config.to_vector_index_config()?;
        // Group every active file into its shard.
        let mut shards: Vec<(String, Vec<String>)> =
            group_files_by_shard(&all_active_files, IndexKind::Vector, &config.column)
                .into_iter()
                .collect();
        shards.sort_by(|a, b| a.0.cmp(&b.0));
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
                Ok(outcome) => {
                    match commit_if_non_empty(
                        &catalog,
                        &prefix,
                        &outcome.header,
                        &to_catalog_segments(&outcome.new_segments),
                        CommitMode::Rebuild,
                    )
                    .await
                    {
                        Ok(true) => rebuilt += 1,
                        Ok(false) => {}
                        Err(error) => failures.push(format!("{prefix}: {error}")),
                    }
                }
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

/// Garbage collect the vector index files of a table.
///
/// Drops expired leases and superseded generations from the catalog and
/// deletes their segment objects once they are older than the grace period,
/// so readers that resolved an older commit keep working.  Shards whose
/// directory is gone (dropped partitions) have their control-plane rows
/// removed as well.
pub async fn gc_vector_index(
    client: &lakesoul_metadata::MetaDataClient,
    table_name: &str,
    namespace: &str,
    object_store_options: &HashMap<String, String>,
    options: &VectorIndexGcOptions,
) -> Result<VectorIndexGcReport> {
    let Some(table_info) = client
        .get_table_info_by_table_name(table_name, namespace)
        .await?
    else {
        bail!("table '{namespace}.{table_name}' not found");
    };
    let all_active_files = client
        .get_data_files_by_table_name(table_name, namespace)
        .await?;
    let Some(first_file) = all_active_files.first() else {
        return Ok(VectorIndexGcReport::default());
    };
    let store = store_for_files(first_file, object_store_options)?;
    let catalog = client.vector_index_catalog();
    gc_index_shards(
        &store,
        &catalog,
        &table_info.table_path,
        options,
        &is_vector_segment_file,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::index::config::IndexManagementConfig;

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
            params: VectorIndexParams {
                dim: 8,
                nlist: 4,
                total_bits: 7,
                metric: "L2".to_string(),
                rotator_type: "FhtKac".to_string(),
                seed: 42,
                use_faster_config: true,
            },
            management: IndexManagementConfig {
                rebuild_mode: rebuild_mode.to_string(),
                max_delta_ratio,
                ..Default::default()
            },
        }
    }

    #[test]
    fn parse_and_validate_rebuild_options() {
        let configs =
            parse_vector_index_columns(Some(r#"[{"column":"vec","dim":8}]"#)).unwrap();
        assert_eq!(
            configs[0].management.rebuild_mode, "auto",
            "default is auto"
        );
        assert_eq!(configs[0].management.max_delta_ratio, 1.0);

        let configs = parse_vector_index_columns(Some(
            r#"[{"column":"vec","dim":8,"rebuild_mode":"none","max_delta_ratio":0.25}]"#,
        ))
        .unwrap();
        assert_eq!(configs[0].management.rebuild_mode, "none");
        assert_eq!(configs[0].management.max_delta_ratio, 0.25);
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
