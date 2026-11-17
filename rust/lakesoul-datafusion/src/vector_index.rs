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

use lakesoul_common::misc::SecretMap;
use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use uuid::Uuid;

use lakesoul_common::IndexKind;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::index::prefix::shard_index_prefix;
use lakesoul_io::vector::builder::VectorShardIndexBuilder;
use lakesoul_metadata::index_catalog::{
    CommitMode, IndexCommitView, VectorCatalog, VectorSegmentEntry,
    normalize_index_prefix,
};
use lakesoul_metadata_proto::entity::TableInfo;
use lakesoul_vector::vector_commit::{commit_vector_build, delete_vector_keys};
use lakesoul_vector::{IndexKey, Metric, RotatorType, SegmentEntry, VectorIndexConfig};
use rootcause::{bail, report};
use tracing::warn;

use crate::Result;
use crate::index::build::{group_files_by_shard, group_shard_files, plan_shard_build};
use crate::index::config::{
    IndexTableConfig, index_columns_to_json, parse_index_columns,
    parse_index_from_table_properties,
};
use crate::index::gc::{
    IndexGcOptions, IndexGcReport, gc_index_shards, gc_shard_now, should_run_write_gc,
};
use crate::index::store_for_files;

pub use crate::index::gc::DEFAULT_GC_GRACE_SECONDS;

/// Property key holding the vector index configurations (JSON).
pub const VECTOR_INDEX_COLUMNS_KEY: &str = IndexKind::Vector.property_key();

/// Monotonic table-property generation bumped by every drop/re-declare.
/// Snapshot caches include it in their key, so a stale plan cannot reuse an
/// index from before a drop even when its file set is unchanged.
pub const VECTOR_INDEX_EPOCH_KEY: &str = "vector_index_epoch";

/// Whether the declared column currently has a usable index.  A drop with
/// `keep_declaration=true` sets it to false so the optimizer falls back to a
/// regular scan until a rebuild/redeclare sets it back to true.
pub const VECTOR_INDEX_AVAILABLE_KEY: &str = "vector_index_available";

/// Return whether the table declaration points at a usable index.
pub fn vector_index_available(properties: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(properties)
        .ok()
        .and_then(|value| value.get(VECTOR_INDEX_AVAILABLE_KEY).cloned())
        .and_then(|value| value.as_bool())
        .unwrap_or(true)
}

static UNAVAILABLE_TABLES: LazyLock<Mutex<HashSet<(String, String)>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Record whether a table's declared vector index is currently usable.
pub fn set_vector_index_available(namespace: &str, table_name: &str, available: bool) {
    if let Ok(mut tables) = UNAVAILABLE_TABLES.lock() {
        let key = (namespace.to_string(), table_name.to_string());
        if available {
            tables.remove(&key);
        } else {
            tables.insert(key);
        }
    }
}

/// Whether a table has a declaration but no usable index.
pub fn is_vector_index_unavailable(namespace: &str, table_name: &str) -> bool {
    UNAVAILABLE_TABLES
        .lock()
        .map(|tables| tables.contains(&(namespace.to_string(), table_name.to_string())))
        .unwrap_or(false)
}

/// Read the vector-index epoch from a raw table-properties JSON string.
pub fn vector_index_epoch(properties: &str) -> u64 {
    serde_json::from_str::<serde_json::Value>(properties)
        .ok()
        .and_then(|value| value.get(VECTOR_INDEX_EPOCH_KEY).cloned())
        .and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_u64(),
            serde_json::Value::String(raw) => raw.parse().ok(),
            _ => None,
        })
        .unwrap_or(0)
}

fn bump_epoch(object: &mut serde_json::Map<String, serde_json::Value>) -> u64 {
    let next = object
        .get(VECTOR_INDEX_EPOCH_KEY)
        .and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_u64(),
            serde_json::Value::String(raw) => raw.parse().ok(),
            _ => None,
        })
        .unwrap_or(0)
        + 1;
    object.insert(
        VECTOR_INDEX_EPOCH_KEY.to_string(),
        serde_json::Value::Number(next.into()),
    );
    next
}

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
/// Requires at least one non-null primary key column(s) — the index maps
/// search results back to primary-key values, and any key type the table can
/// store is accepted as long as its `arrow-row` encoding is available (also
/// composite keys) — and `FixedSizeList`/`List` of
/// `Float16`/`Float32`/`Float64` vector columns whose dimension matches the
/// configured `dim`.
pub fn validate_vector_index_configs(
    configs: &[VectorIndexTableConfig],
    schema: &arrow::datatypes::Schema,
    primary_keys: &[String],
) -> Result<()> {
    use arrow::datatypes::DataType;
    if configs.is_empty() {
        return Ok(());
    }
    if primary_keys.is_empty() {
        bail!(
            "a vector index requires an id column: pass primary_keys=[...] \
             when creating a table with vector_index (the index maps search \
             results to primary key values)"
        );
    }
    // The index stores the arrow-Row encoding of the primary-key columns, so
    // build the encoder here to reject key types it cannot represent.
    let layout = lakesoul_io::index::key::KeyLayout::from_schema(schema, primary_keys)
        .map_err(|e| report!("invalid vector index primary key: {e}"))?;
    lakesoul_io::index::key::KeyCodec::new(layout)
        .map_err(|e| report!("invalid vector index primary key: {e}"))?;
    for config in configs {
        let column = &config.column;
        config.management.validate(IndexKind::Vector, column)?;
        // Only 1/3/7-bit RaBitQ widths have an estimator kernel; anything
        // else would panic deep inside a search, so reject it here (table
        // creation and the Python SDK both run this validation).
        if !lakesoul_vector::rabitq::is_supported_total_bits(config.params.total_bits) {
            bail!(
                "vector index column '{column}': total_bits must be 1, 3 or 7                  (got {})",
                config.params.total_bits
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
                     of Float16/Float32/Float64, got {other}"
                )
            }
        };
        match element_type {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {}
            other => bail!(
                "vector index column '{column}' elements must be \
                 Float16/Float32/Float64, got {other}"
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

/// Options for `drop_vector_index`.
#[derive(Debug, Clone)]
pub struct DropVectorIndexOptions {
    /// Columns to drop; `None` means every column declared in
    /// `vector_index_columns`.  An empty vector is rejected.
    pub columns: Option<Vec<String>>,
    /// Drop only one partition's shards.  Requires `keep_declaration = true`
    /// because other partitions still use the declaration.
    pub partition_desc: Option<String>,
    /// Keep the `vector_index_columns` entry so a later write rebuilds it.
    pub keep_declaration: bool,
    /// Delete the `_vector_index/...` objects as well as the control plane.
    pub purge_files: bool,
    /// Only report what would be dropped.
    pub dry_run: bool,
    /// Do not fail when the index (or the column declaration) is absent.
    pub if_exists: bool,
    /// Protection period before deleting index objects.
    ///
    /// The control plane and caches are invalidated immediately; objects are
    /// kept for this long so readers that already resolved the old commit can
    /// finish.  `Duration::ZERO` deletes immediately.
    pub grace: std::time::Duration,
    /// Wait synchronously for object cleanup.  With `false`, the call returns
    /// after the control plane is removed and a background task finishes the
    /// object purge/GC; `drop_id` identifies the job.
    pub wait_for_cleanup: bool,
}

impl Default for DropVectorIndexOptions {
    fn default() -> Self {
        Self {
            columns: None,
            partition_desc: None,
            keep_declaration: false,
            purge_files: true,
            dry_run: false,
            if_exists: true,
            grace: std::time::Duration::ZERO,
            wait_for_cleanup: true,
        }
    }
}

/// Result of `drop_vector_index`.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct DropVectorIndexReport {
    pub table_name: String,
    pub namespace: String,
    pub columns: Vec<String>,
    pub partitions: Vec<String>,
    pub shards_dropped: usize,
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
    pub declaration_removed: bool,
    pub dry_run: bool,
    /// Table `vector_index_epoch` after the property update.
    pub epoch: u64,
    /// State machine id for deferred/background cleanup.
    pub drop_id: Option<String>,
    /// `completed`, `tombstone_retained`, or `deferred`.
    pub cleanup: String,
}

fn target_columns(
    configured: &BTreeSet<String>,
    requested: Option<Vec<String>>,
) -> Result<Vec<String>> {
    match requested {
        Some(columns) if columns.is_empty() => {
            bail!("columns must not be empty when specified")
        }
        Some(columns) => {
            let mut seen = BTreeSet::new();
            Ok(columns
                .into_iter()
                .filter(|column| seen.insert(column.clone()))
                .collect())
        }
        None => Ok(configured.iter().cloned().collect()),
    }
}

fn properties_with_vector_configs(
    properties: &str,
    configs: &[VectorIndexTableConfig],
) -> Result<String> {
    let mut properties: serde_json::Value = if properties.trim().is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str(properties)?
    };
    let Some(object) = properties.as_object_mut() else {
        bail!("table properties are not a JSON object")
    };
    let existing = parse_vector_index_columns(
        object
            .get(VECTOR_INDEX_COLUMNS_KEY)
            .and_then(serde_json::Value::as_str),
    )?;
    let replace: BTreeSet<&str> = configs
        .iter()
        .map(|config| config.column.as_str())
        .collect();
    let mut merged: Vec<VectorIndexTableConfig> = existing
        .into_iter()
        .filter(|config| !replace.contains(config.column.as_str()))
        .collect();
    merged.extend(configs.iter().cloned());
    object.insert(
        VECTOR_INDEX_COLUMNS_KEY.to_string(),
        serde_json::Value::String(vector_index_columns_to_json(&merged)),
    );
    bump_epoch(object);
    object.insert(
        VECTOR_INDEX_AVAILABLE_KEY.to_string(),
        serde_json::Value::Bool(true),
    );
    Ok(serde_json::to_string(&properties)?)
}

fn properties_without_columns(
    properties: &str,
    columns: &BTreeSet<String>,
) -> Result<Option<(String, u64)>> {
    let mut properties: serde_json::Value = if properties.trim().is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str(properties)?
    };
    let Some(object) = properties.as_object_mut() else {
        bail!("table properties are not a JSON object")
    };
    let Some(raw) = object.get(VECTOR_INDEX_COLUMNS_KEY) else {
        return Ok(None);
    };
    let mut configs: Vec<serde_json::Value> = match raw {
        serde_json::Value::String(raw) => serde_json::from_str(raw)?,
        value => serde_json::from_value(value.clone())?,
    };
    let before = configs.len();
    configs.retain(|config| {
        !config
            .get("column")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|column| columns.contains(column))
    });
    if configs.len() == before {
        return Ok(None);
    }
    if configs.is_empty() {
        object.remove(VECTOR_INDEX_COLUMNS_KEY);
    } else {
        object.insert(
            VECTOR_INDEX_COLUMNS_KEY.to_string(),
            serde_json::Value::String(serde_json::to_string(&configs)?),
        );
    }
    let epoch = bump_epoch(object);
    Ok(Some((serde_json::to_string(&properties)?, epoch)))
}

async fn remove_declarations(
    client: &lakesoul_metadata::MetaDataClient,
    table_info: &TableInfo,
    columns: &BTreeSet<String>,
) -> Result<(bool, u64)> {
    let table_id = table_info.table_id.clone();
    let mut expected = table_info.properties.clone();
    for _ in 0..3 {
        let Some((properties, epoch)) = properties_without_columns(&expected, columns)?
        else {
            return Ok((false, vector_index_epoch(&expected)));
        };
        if client
            .update_table_properties_if_unchanged(&table_id, &expected, &properties)
            .await?
        {
            return Ok((true, epoch));
        }
        let Some(fresh) = client.get_table_info_by_table_id(&table_id).await? else {
            bail!("table '{table_id}' disappeared while dropping its vector index");
        };
        expected = fresh.properties;
    }
    bail!(
        "table '{table_id}' properties kept changing while dropping the \
         vector index; retry the drop"
    )
}

async fn bump_vector_index_epoch_and_state(
    client: &lakesoul_metadata::MetaDataClient,
    table_info: &TableInfo,
    available: Option<bool>,
) -> Result<u64> {
    let table_id = table_info.table_id.clone();
    let mut expected = table_info.properties.clone();
    for _ in 0..3 {
        let mut properties: serde_json::Value = if expected.trim().is_empty() {
            serde_json::Value::Object(serde_json::Map::new())
        } else {
            serde_json::from_str(&expected)?
        };
        let Some(object) = properties.as_object_mut() else {
            bail!("table properties are not a JSON object")
        };
        let epoch = bump_epoch(object);
        if let Some(available) = available {
            object.insert(
                VECTOR_INDEX_AVAILABLE_KEY.to_string(),
                serde_json::Value::Bool(available),
            );
        }
        let new_properties = serde_json::to_string(&properties)?;
        if client
            .update_table_properties_if_unchanged(&table_id, &expected, &new_properties)
            .await?
        {
            return Ok(epoch);
        }
        let Some(fresh) = client.get_table_info_by_table_id(&table_id).await? else {
            bail!("table '{table_id}' disappeared while bumping vector index epoch");
        };
        expected = fresh.properties;
    }
    bail!("table '{table_id}' properties kept changing while bumping vector epoch")
}

/// Re-declare a vector index after a drop that removed its declaration.
///
/// Writes the merged `vector_index_columns` property with CAS and clears the
/// drop tombstones for the declared columns.  The caller can then build or
/// rebuild the index.
pub async fn declare_vector_index(
    client: &lakesoul_metadata::MetaDataClient,
    table_name: &str,
    namespace: &str,
    configs: Vec<VectorIndexTableConfig>,
) -> Result<()> {
    if configs.is_empty() {
        bail!("at least one vector index config is required");
    }
    let Some(mut table_info) = client
        .get_table_info_by_table_name(table_name, namespace)
        .await?
    else {
        bail!("table '{namespace}.{table_name}' not found");
    };
    let table_id = table_info.table_id.clone();
    let mut declared = false;
    for _ in 0..3 {
        let properties =
            properties_with_vector_configs(&table_info.properties, &configs)?;
        if client
            .update_table_properties_if_unchanged(
                &table_id,
                &table_info.properties,
                &properties,
            )
            .await?
        {
            declared = true;
            break;
        }
        table_info = client
            .get_table_info_by_table_id(&table_id)
            .await?
            .ok_or_else(|| {
                report!("table '{table_id}' disappeared while declaring vector index")
            })?;
    }
    if !declared {
        bail!(
            "table '{table_id}' properties kept changing while declaring the \
             vector index; retry"
        );
    }
    crate::catalog::metadata_cache::invalidate_table(namespace, table_name);
    let catalog = client.vector_index_catalog();
    let columns: BTreeSet<&str> = configs
        .iter()
        .map(|config| config.column.as_str())
        .collect();
    let index_root = format!(
        "{}/_vector_index/",
        normalize_index_prefix(&table_info.table_path).trim_end_matches('/')
    );
    for prefix in catalog.list_dropped_under(&table_info.table_path).await? {
        let Some(rest) = prefix.strip_prefix(index_root.as_str()) else {
            continue;
        };
        let column = rest.split('/').next().unwrap_or_default();
        if columns.contains(column) {
            catalog.clear_dropped(&prefix).await?;
        }
    }
    Ok(())
}

async fn delete_prefix_objects(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<(u64, u64)> {
    use futures::StreamExt;
    let path = object_store::path::Path::from(prefix.trim_end_matches('/'));
    let exact = path.as_ref().trim_end_matches('/').to_string();
    let child_prefix = format!("{exact}/");
    let mut stream = store.list(Some(&path));
    let mut objects = 0u64;
    let mut bytes = 0u64;
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        // Object-store list is a raw string prefix match.  Do not let a
        // shard `_vector_index/vec` delete a sibling `_vector_index/vec_v2`.
        let location = meta.location.as_ref();
        if location != exact && !location.starts_with(&child_prefix) {
            continue;
        }
        bytes = bytes.saturating_add(meta.size);
        let mut last_error = None;
        for attempt in 0..3 {
            match store.delete(&meta.location).await {
                Ok(()) => {
                    last_error = None;
                    break;
                }
                Err(error) => {
                    last_error = Some(error);
                    if attempt < 2 {
                        tokio::time::sleep(std::time::Duration::from_millis(
                            100 * (attempt + 1) as u64,
                        ))
                        .await;
                    }
                }
            }
        }
        if let Some(error) = last_error {
            return Err(error.into());
        }
        objects += 1;
    }
    Ok((objects, bytes))
}

async fn delete_dropped_prefix_objects(
    store: &Arc<dyn ObjectStore>,
    catalog: &VectorCatalog,
    prefix: &str,
) -> Result<(u64, u64)> {
    // Logical drop already blocks new unprotected readers. Keep the prefix
    // lock across the sweep as well, so rebuild cannot clear the tombstone
    // and publish new files between the lease check and object deletion.
    Ok(catalog
        .with_drop_cleanup(prefix, || async {
            delete_prefix_objects(store, prefix).await.map_err(|error| {
                lakesoul_metadata::LakeSoulMetaDataError::Internal(format!(
                    "physical index cleanup failed: {error}"
                ))
            })
        })
        .await?)
}

/// Drop a vector index for a table.
///
/// This is the DataFusion/Rust entry point: it creates the object store from
/// the table path and the supplied object-store options, then performs the
/// drop.  The Python binding uses [`drop_vector_index_with_store`] because
/// its `store_config` is already parsed.
pub async fn drop_vector_index(
    client: &lakesoul_metadata::MetaDataClient,
    table_name: &str,
    namespace: &str,
    object_store_options: &HashMap<String, String>,
    options: DropVectorIndexOptions,
) -> Result<DropVectorIndexReport> {
    let Some(table_info) = client
        .get_table_info_by_table_name(table_name, namespace)
        .await?
    else {
        bail!("table '{namespace}.{table_name}' not found");
    };
    let mut store_options = object_store_options.clone();
    if (table_info.table_path.starts_with("s3://")
        || table_info.table_path.starts_with("s3a://"))
        && !store_options.contains_key("fs.s3a.bucket")
        && let Some(rest) = table_info
            .table_path
            .split_once("://")
            .map(|(_, rest)| rest)
        && let Some((bucket, _)) = rest.split_once('/')
    {
        store_options.insert("fs.s3a.bucket".to_string(), bucket.to_string());
    }
    let store = store_for_files(&table_info.table_path, &store_options)?;
    drop_vector_index_inner(client, store, table_info, options).await
}

/// Same as [`drop_vector_index`], with a caller-supplied object store.
pub async fn drop_vector_index_with_store(
    client: &lakesoul_metadata::MetaDataClient,
    store: Arc<dyn ObjectStore>,
    table_name: &str,
    namespace: &str,
    options: DropVectorIndexOptions,
) -> Result<DropVectorIndexReport> {
    let Some(table_info) = client
        .get_table_info_by_table_name(table_name, namespace)
        .await?
    else {
        bail!("table '{namespace}.{table_name}' not found");
    };
    drop_vector_index_inner(client, store, table_info, options).await
}

async fn drop_vector_index_inner(
    client: &lakesoul_metadata::MetaDataClient,
    store: Arc<dyn ObjectStore>,
    table_info: TableInfo,
    options: DropVectorIndexOptions,
) -> Result<DropVectorIndexReport> {
    if options.partition_desc.is_some() && !options.keep_declaration {
        bail!(
            "partition-scoped drop requires keep_declaration=true: other              partitions still rely on the vector_index_columns declaration"
        );
    }

    let properties_json = if table_info.properties.trim().is_empty() {
        "{}"
    } else {
        table_info.properties.as_str()
    };
    let configs = parse_vector_index_from_table_properties(properties_json)?;
    let configured: BTreeSet<String> =
        configs.iter().map(|config| config.column.clone()).collect();
    let mut columns = target_columns(&configured, options.columns.clone())?;
    if let Some(requested) = options.columns.as_ref() {
        let missing: Vec<&String> = requested
            .iter()
            .filter(|column| !configured.contains(*column))
            .collect();
        if !missing.is_empty() {
            if !options.if_exists {
                bail!(
                    "column(s) {} are not vector-indexed",
                    missing
                        .iter()
                        .map(|column| format!("'{column}'"))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            columns.retain(|column| configured.contains(column));
        }
    }
    let column_set: BTreeSet<String> = columns.iter().cloned().collect();

    let mut report = DropVectorIndexReport {
        table_name: table_info.table_name.clone(),
        namespace: table_info.table_namespace.clone(),
        columns: columns.clone(),
        epoch: vector_index_epoch(&table_info.properties),
        ..Default::default()
    };

    if columns.is_empty() {
        if options.if_exists {
            return Ok(report);
        }
        bail!("table has no vector_index_columns configured");
    }

    let table_prefix = normalize_index_prefix(&table_info.table_path);
    let table_prefix = table_prefix.trim_end_matches('/');
    const INDEX_MARKER: &str = "/_vector_index/";
    let catalog = client.vector_index_catalog();
    let mut shards: Vec<String> = Vec::new();
    let mut partitions = BTreeSet::new();
    for prefix in catalog.list_shards_under(&table_info.table_path).await? {
        // Unpartitioned tables store indexes at
        //   {table_path}/_vector_index/{column}/{partition}/{bucket}
        // while partitioned tables store them under each partition directory:
        //   {table_path}/dt=.../_vector_index/{column}/{partition}/{bucket}
        // Parse the marker instead of assuming the table root, and compare a
        // requested partition against the table directory (not the -5 index
        // partition that every data partition uses).
        let Some(marker) = prefix.find(INDEX_MARKER) else {
            continue;
        };
        let before_marker = &prefix[..marker];
        let Some(table_partition) = before_marker.strip_prefix(table_prefix) else {
            continue;
        };
        let table_partition = table_partition.trim_start_matches('/');

        let mut parts = prefix[marker + INDEX_MARKER.len()..].split('/');
        let column = parts.next().unwrap_or_default();
        if !column_set.contains(column) {
            continue;
        }
        let index_partition = parts.next().unwrap_or_default();
        let matches = match options.partition_desc.as_deref() {
            None => true,
            Some(wanted) if table_partition.is_empty() => index_partition == wanted,
            Some(wanted) => {
                table_partition == wanted || table_partition.replace('/', ",") == wanted
            }
        };
        if !matches {
            continue;
        }
        let partition = if table_partition.is_empty() {
            index_partition
        } else {
            table_partition
        };
        partitions.insert(partition.to_string());
        shards.push(prefix);
    }

    report.partitions = partitions.into_iter().collect::<Vec<_>>();
    report.shards_dropped = shards.len();
    report.dry_run = options.dry_run;
    if options.dry_run {
        return Ok(report);
    }

    let drop_id = Uuid::new_v4();
    report.drop_id = Some(drop_id.to_string());
    report.cleanup = "deferred".to_string();

    // Phase 1a: install every tombstone before touching the declaration.
    // A concurrent build is blocked immediately, and a failed property
    // update can roll the tombstones back safely.
    for prefix in &shards {
        catalog
            .mark_dropped_for_job(prefix, "drop_vector_index", drop_id, "tombstoned")
            .await?;
    }

    // Phase 1b: update the declaration and bump the epoch.  The epoch is
    // part of the snapshot-cache key; the tombstone is the build guard.
    let declaration_result = if !options.keep_declaration {
        remove_declarations(client, &table_info, &column_set)
            .await
            .map(|(removed, epoch)| {
                report.declaration_removed = removed;
                report.epoch = epoch;
            })
    } else {
        bump_vector_index_epoch_and_state(client, &table_info, Some(false))
            .await
            .map(|epoch| report.epoch = epoch)
    };
    if let Err(error) = declaration_result {
        for prefix in &shards {
            let _ = catalog.clear_dropped(prefix).await;
        }
        return Err(error);
    }
    set_vector_index_available(
        &table_info.table_namespace,
        &table_info.table_name,
        options.keep_declaration,
    );

    crate::catalog::metadata_cache::invalidate_table(
        &table_info.table_namespace,
        &table_info.table_name,
    );

    // Phase 1c: remove the control plane and process-local caches.
    for prefix in &shards {
        catalog.delete_shard(prefix).await?;
        let _ = catalog
            .set_drop_state(prefix, "control_plane_deleted")
            .await;
        lakesoul_io::index::cache::invalidate(IndexKind::Vector, prefix).await;
        lakesoul_io::vector::search::invalidate_lazy_shard(prefix);
        crate::datasource::file_format::vector_search_exec::invalidate_snapshot_prefix(
            prefix,
        );
    }

    // No shard means no object cleanup; still report the correct terminal
    // state instead of a misleading "deferred" job.
    if shards.is_empty() || !options.purge_files {
        report.cleanup = if options.keep_declaration {
            "completed".to_string()
        } else {
            "tombstone_retained".to_string()
        };
        return Ok(report);
    }

    // Phase 2: object cleanup.  Deferred mode returns immediately with a
    // persistent job id; a background task (or the GC API after a crash)
    // finishes the purge.
    if !options.wait_for_cleanup {
        for prefix in &shards {
            let _ = catalog.set_drop_state(prefix, "objects_pending").await;
        }
        spawn_drop_cleanup(
            Arc::clone(&store),
            catalog.clone(),
            shards.clone(),
            options.grace,
            options.keep_declaration,
            drop_id,
        );
        return Ok(report);
    }

    if options.purge_files && !options.grace.is_zero() {
        tokio::time::sleep(options.grace).await;
    }
    let mut failures = Vec::new();
    for prefix in &shards {
        if options.purge_files {
            match delete_dropped_prefix_objects(&store, &catalog, prefix).await {
                Ok((objects, bytes)) => {
                    report.objects_deleted =
                        report.objects_deleted.saturating_add(objects);
                    report.bytes_deleted = report.bytes_deleted.saturating_add(bytes);
                }
                Err(error) => {
                    failures.push(format!("{prefix}: {error}"));
                    let _ = catalog.set_drop_state(prefix, "objects_failed").await;
                    continue;
                }
            }
        }
        // When the declaration is removed the tombstone is the only guard
        // against a long-running writer that still carries the old table
        // property.  Keep it; an explicit rebuild clears it.  When the
        // declaration is kept, the user asked for a rebuildable index, so
        // release the prefix.
        if options.keep_declaration {
            catalog.clear_dropped(prefix).await?;
        } else {
            let _ = catalog.set_drop_state(prefix, "tombstone_retained").await;
        }
    }
    if !failures.is_empty() {
        bail!(
            "vector index control plane was removed but object cleanup failed \
             for: {}; rerun drop or use the drop-GC API",
            failures.join("; ")
        );
    }
    report.cleanup = if options.keep_declaration {
        "completed".to_string()
    } else {
        "tombstone_retained".to_string()
    };

    Ok(report)
}

fn spawn_drop_cleanup(
    store: Arc<dyn ObjectStore>,
    catalog: VectorCatalog,
    prefixes: Vec<String>,
    grace: Duration,
    keep_declaration: bool,
    drop_id: Uuid,
) {
    lakesoul_io::session::GLOBAL_RUNTIME.spawn(async move {
        if !grace.is_zero() {
            tokio::time::sleep(grace).await;
        }
        for prefix in prefixes {
            match delete_dropped_prefix_objects(&store, &catalog, &prefix).await {
                Ok(_) => {
                    if keep_declaration {
                        let _ = catalog.clear_dropped(&prefix).await;
                    } else {
                        let _ = catalog
                            .set_drop_state(&prefix, "tombstone_retained")
                            .await;
                    }
                }
                Err(error) => {
                    warn!(
                        drop_id = %drop_id,
                        prefix,
                        %error,
                        "deferred vector-index drop cleanup failed; tombstone retained for GC"
                    );
                    let _ = catalog.set_drop_state(&prefix, "objects_failed").await;
                }
            }
        }
    });
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
            data_files: segment.data_files.clone(),
            birth_version: segment.birth_version,
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
        mutation_head: view.mutation_head.clone(),
        mutation_records: view.mutation_records,
    })
}

fn to_catalog_segments(
    segments: &[SegmentEntry],
    data_files: &[String],
) -> Vec<VectorSegmentEntry> {
    segments
        .iter()
        .map(|segment| VectorSegmentEntry {
            cluster_id: segment.cluster_id,
            segment_version: segment.segment_version,
            filename: segment.segment_filename.clone(),
            num_vectors: segment.num_vectors,
            file_size: segment.file_size,
            data_files: data_files.to_vec(),
            birth_version: segment.birth_version,
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
    if primary_keys.is_empty() {
        bail!("a vector index requires a table with a primary key");
    }
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
            let resolve_started = std::time::Instant::now();
            let resolved = match catalog.resolve(&prefix).await {
                Ok(view) => view,
                Err(error) => {
                    failures.push(format!(
                        "partition {partition_desc:?} bucket {bucket}: failed to resolve index: {error}"
                    ));
                    continue;
                }
            };
            let resolve_ms = resolve_started.elapsed().as_secs_f64() * 1000.0;
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
                primary_keys.to_vec(),
                object_store_options.clone().into(),
                None,
            );
            // The files this build reads, recorded on the new segments as
            // their coverage.
            let covered_files = plan.files.clone();
            if let Some(view) = &plan.base {
                builder = builder.with_base(to_resolved_shard(&prefix, view)?);
            }
            let mut commit_mode = plan.mode;
            let build_started = std::time::Instant::now();
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
                        primary_keys.to_vec(),
                        object_store_options.clone().into(),
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
            let build_ms = build_started.elapsed().as_secs_f64() * 1000.0;
            let commit_started = std::time::Instant::now();
            let commit_segments =
                to_catalog_segments(&outcome.new_segments, &covered_files);
            match commit_vector_build(
                &store,
                catalog,
                &prefix,
                &outcome.header,
                commit_segments,
                outcome.upserted_keys,
                commit_mode,
            )
            .await
            {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(error) => {
                    failures.push(format!(
                        "partition {partition_desc:?} bucket {bucket}: {error}"
                    ));
                    continue;
                }
            }
            let commit_ms = commit_started.elapsed().as_secs_f64() * 1000.0;
            built += 1;
            let mut gc_ms = 0.0;
            if management.gc_enabled && should_run_write_gc(management.gc_grace_seconds) {
                let gc_started = std::time::Instant::now();
                if let Err(error) = gc_shard_now(
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
                gc_ms = gc_started.elapsed().as_secs_f64() * 1000.0;
            }
            debug!(
                prefix = %prefix,
                rebuilt = should_rebuild,
                resolve_ms,
                build_ms,
                commit_ms,
                gc_ms,
                "vector index shard updated"
            );
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

/// Durably delete index keys from one shard.
///
/// Deleted rows reappear in search results if they are still present in the
/// table's data files, so callers must delete/update the rows first (or run
/// [`rebuild_vector_index`] afterwards to physically drop them).
///
/// Returns whether a commit was published.
pub async fn delete_vector_index_keys(
    store: &Arc<dyn ObjectStore>,
    catalog: &VectorCatalog,
    index_prefix: &str,
    keys: &[IndexKey],
) -> Result<bool> {
    delete_vector_keys(store, catalog, index_prefix, keys)
        .await
        .map(|outcome| outcome.is_some())
        .map_err(|error| {
            report!("failed to delete vector index keys at '{index_prefix}': {error}")
        })
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
    object_store_options: SecretMap,
) -> Result<usize> {
    let Some(table_info) = client
        .get_table_info_by_table_name(table_name, namespace)
        .await?
    else {
        bail!("table '{namespace}.{table_name}' not found");
    };
    let properties_json = if table_info.properties.trim().is_empty() {
        "{}"
    } else {
        table_info.properties.as_str()
    };
    let configs = parse_vector_index_from_table_properties(properties_json)?;
    if configs.is_empty() {
        return Ok(0);
    }
    // A rebuild changes the index generation; bump the epoch so snapshot
    // caches keyed by the table epoch cannot serve the old generation.
    let _ = bump_vector_index_epoch_and_state(client, &table_info, Some(true)).await?;
    set_vector_index_available(namespace, table_name, true);
    if primary_keys.is_empty() {
        bail!("a vector index requires a table with a primary key");
    }
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
            // An explicit rebuild claims the shard: clear a pending drop
            // tombstone before publishing a new generation.
            catalog.clear_dropped(&prefix).await?;
            let covered_files = files.clone();
            let result = VectorShardIndexBuilder::new(
                store.clone(),
                vector_config.clone(),
                files,
                primary_keys.to_vec(),
                object_store_options.clone(),
                None,
            )
            .rebuild()
            .await;
            match result {
                Ok(outcome) => {
                    let commit_segments =
                        to_catalog_segments(&outcome.new_segments, &covered_files);
                    match commit_vector_build(
                        &store,
                        &catalog,
                        &prefix,
                        &outcome.header,
                        commit_segments,
                        Vec::new(),
                        CommitMode::Rebuild,
                    )
                    .await
                    {
                        Ok(Some(_)) => rebuilt += 1,
                        Ok(None) => {}
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

/// What a tombstone object sweep did.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct DropGcReport {
    pub table_name: String,
    pub namespace: String,
    pub tombstones: usize,
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
}

/// Sweep objects left behind by a failed or deferred vector-index drop.
///
/// Tombstones are deliberately retained: they prevent a stale writer from
/// recreating a dropped index.  An explicit rebuild claims and clears them.
pub async fn gc_dropped_vector_index_objects(
    client: &lakesoul_metadata::MetaDataClient,
    table_name: &str,
    namespace: &str,
    object_store_options: &HashMap<String, String>,
) -> Result<DropGcReport> {
    let Some(table_info) = client
        .get_table_info_by_table_name(table_name, namespace)
        .await?
    else {
        bail!("table '{namespace}.{table_name}' not found");
    };
    let mut store_options = object_store_options.clone();
    if (table_info.table_path.starts_with("s3://")
        || table_info.table_path.starts_with("s3a://"))
        && !store_options.contains_key("fs.s3a.bucket")
        && let Some(rest) = table_info
            .table_path
            .split_once("://")
            .map(|(_, rest)| rest)
        && let Some((bucket, _)) = rest.split_once('/')
    {
        store_options.insert("fs.s3a.bucket".to_string(), bucket.to_string());
    }
    let store = store_for_files(&table_info.table_path, &store_options)?;
    let catalog = client.vector_index_catalog();
    let prefixes = catalog.list_dropped_under(&table_info.table_path).await?;
    let mut report = DropGcReport {
        table_name: table_info.table_name.clone(),
        namespace: table_info.table_namespace.clone(),
        tombstones: prefixes.len(),
        ..Default::default()
    };
    for prefix in prefixes {
        let (objects, bytes) =
            delete_dropped_prefix_objects(&store, &catalog, &prefix).await?;
        report.objects_deleted = report.objects_deleted.saturating_add(objects);
        report.bytes_deleted = report.bytes_deleted.saturating_add(bytes);
        // Terminal for the drop state machine; the tombstone remains the
        // stale-writer guard until an explicit rebuild/declare clears it.
        let _ = catalog.set_drop_state(&prefix, "tombstone_retained").await;
    }
    Ok(report)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_lease_drop_defers_physical_objects_until_release() {
        let client = lakesoul_metadata::MetaDataClient::from_env().await.unwrap();
        let catalog = client.vector_index_catalog();
        let prefix = format!("read-lease-drop-{}", Uuid::new_v4());
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let object =
            object_store::path::Path::from(format!("{prefix}/cluster_0000_v0000.seg"));
        store
            .put(&object, bytes::Bytes::from_static(b"old").into())
            .await
            .unwrap();
        let segment = VectorSegmentEntry {
            cluster_id: 0,
            segment_version: 0,
            filename: "cluster_0000_v0000.seg".into(),
            num_vectors: 1,
            file_size: 3,
            data_files: Vec::new(),
            birth_version: 0,
        };
        let view = catalog
            .commit(&prefix, b"header", &[segment], CommitMode::Rebuild)
            .await
            .unwrap();
        let lease = catalog
            .acquire_pinned_read_views(
                &[(prefix.clone(), view.commit_id)],
                "drop-object-test",
            )
            .await
            .unwrap();
        catalog.mark_dropped(&prefix, "test").await.unwrap();
        catalog.delete_shard(&prefix).await.unwrap();
        let cleanup_store = store.clone();
        let cleanup_catalog = catalog.clone();
        let cleanup_prefix = prefix.clone();
        let mut cleanup = tokio::spawn(async move {
            delete_dropped_prefix_objects(
                &cleanup_store,
                &cleanup_catalog,
                &cleanup_prefix,
            )
            .await
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut cleanup)
                .await
                .is_err()
        );
        assert!(store.get(&object).await.is_ok());
        lease.release().await.unwrap();
        let report = tokio::time::timeout(Duration::from_secs(2), cleanup)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(report, (1, 3));
        assert!(store.get(&object).await.is_err());
        catalog.clear_dropped(&prefix).await.unwrap();
    }

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

    #[test]
    fn validate_accepts_float16_vector_columns() {
        use arrow::datatypes::{DataType, Field};
        for element in [DataType::Float16, DataType::Float32, DataType::Float64] {
            let fixed = arrow::datatypes::Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new(
                    "vec",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", element.clone(), true)),
                        8,
                    ),
                    false,
                ),
            ]);
            validate_vector_index_configs(
                &[config("auto", 1.0)],
                &fixed,
                &["id".to_string()],
            )
            .unwrap_or_else(|e| panic!("FixedSizeList<{element}>: {e}"));

            // The SQL scenario's variable-length `List` column shape.
            let list = arrow::datatypes::Schema::new(vec![
                Field::new("id", DataType::UInt64, false),
                Field::new(
                    "vec",
                    DataType::List(Arc::new(Field::new("item", element.clone(), true))),
                    false,
                ),
            ]);
            validate_vector_index_configs(
                &[config("auto", 1.0)],
                &list,
                &["id".to_string()],
            )
            .unwrap_or_else(|e| panic!("List<{element}>: {e}"));
        }

        let boolean = arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new(
                "vec",
                DataType::List(Arc::new(Field::new("item", DataType::Boolean, true))),
                false,
            ),
        ]);
        let err = validate_vector_index_configs(
            &[config("auto", 1.0)],
            &boolean,
            &["id".to_string()],
        )
        .unwrap_err();
        assert!(err.to_string().contains("Float16/Float32/Float64"), "{err}");
    }
}
