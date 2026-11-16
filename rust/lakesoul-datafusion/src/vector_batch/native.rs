// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Snapshot-bound batch engine shared by SQL, pgwire and Python. Windows use
//! one immutable metadata view and a renewable read lease, never newer shards.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use arrow::datatypes::{DataType, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionState;
use futures::{StreamExt, TryStreamExt};
use lakesoul_common::IndexKind;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::index::key::{KeyCodec, KeyLayout};
use lakesoul_io::pk_locator::{PrefixMatch, VectorBlockSource};
use lakesoul_io::vector::search::{LoadedShard, VectorHit, load_typed_shard};
use lakesoul_metadata::error::LakeSoulMetaDataError;
use lakesoul_metadata::index_catalog::{IndexReadLeaseGuard, VectorCatalog};
use lakesoul_vector::rabitq::segment::IndexHeader;
use lakesoul_vector::{IndexKey, SegmentEntry};
use object_store::ObjectStore;
use object_store::path::Path;
use rootcause::compat::boxed_error::IntoBoxedError;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;

use crate::datasource::table_provider::LakeSoulTableProvider;

pub const DEFAULT_WORK_MEMORY: usize = 128 * 1024 * 1024;
pub const MAX_BATCH_QUERIES: usize = 65_536;
pub const MAX_BATCH_INPUT_BYTES: usize = 64 * 1024 * 1024;
pub const DEFAULT_QUERY_WINDOW: usize = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeBatchOptions {
    pub top_k: usize,
    pub nprobe: usize,
    pub exact: bool,
    pub exact_overfetch: usize,
    /// Zero selects the bounded process default.
    pub parallelism: usize,
    /// Concurrent object-source opens; always bounded by the server cap.
    pub io_concurrency: usize,
    /// Transient memory; L1/L2 caches have separate budgets.
    pub memory_bytes: usize,
}

impl Default for NativeBatchOptions {
    fn default() -> Self {
        Self {
            top_k: 10,
            nprobe: 64,
            exact: false,
            exact_overfetch: 10,
            parallelism: 0,
            io_concurrency: 8,
            memory_bytes: DEFAULT_WORK_MEMORY,
        }
    }
}

impl NativeBatchOptions {
    pub fn validate(&self) -> Result<()> {
        if self.top_k == 0 || self.nprobe == 0 || self.exact_overfetch == 0 {
            return Err(DataFusionError::Plan(
                "top_k, nprobe and exact_overfetch must be positive".into(),
            ));
        }
        if self.memory_bytes == 0 || self.memory_bytes > DEFAULT_WORK_MEMORY {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "native batch work memory must be between 1 and {DEFAULT_WORK_MEMORY} bytes"
            )));
        }
        if !(1..=64).contains(&self.io_concurrency) {
            return Err(DataFusionError::Plan(
                "batch io_concurrency must be between 1 and 64".into(),
            ));
        }
        self.candidates()?;
        Ok(())
    }
    pub fn candidates(&self) -> Result<usize> {
        self.top_k
            .checked_mul(if self.exact { self.exact_overfetch } else { 1 })
            .ok_or_else(|| DataFusionError::Plan("batch candidate count overflow".into()))
    }
    pub fn effective_parallelism(&self) -> usize {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        if self.parallelism == 0 {
            cores
        } else {
            self.parallelism.min(cores)
        }
        .max(1)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NativeBatchHit {
    pub id: i64,
    pub distance: f64,
}

/// Serializable physical work; stores and credentials never enter this type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeBatchShardDescriptor {
    pub index_prefix: String,
    pub commit_id: i64,
    pub generation: u64,
    pub version: u64,
    pub header: Vec<u8>,
    pub segments: Vec<SegmentEntry>,
    pub files: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NativeBatchSnapshotDescriptor {
    pub table_id: String,
    pub table_path: String,
    pub column: String,
    pub epoch: u64,
    pub dim: usize,
    pub metric: String,
    pub schema: Schema,
    pub partitions: Vec<(String, i32)>,
    pub shards: Vec<NativeBatchShardDescriptor>,
    /// Internal parent request token; never supplied through SQL arguments.
    #[serde(default)]
    pub read_lease_id: Option<String>,
}

#[derive(Debug, Default)]
pub struct NativeBatchMetrics {
    pub windows: AtomicU64,
    pub queries: AtomicU64,
    pub shard_probes: AtomicU64,
    pub rerank_blocks: AtomicU64,
    pub cluster_loads: AtomicU64,
    pub shared_block_reuses: AtomicU64,
    pub peak_workers: AtomicU64,
}

pub struct NativeBatchSnapshot {
    descriptor: NativeBatchSnapshotDescriptor,
    store: Arc<dyn ObjectStore>,
    lease: Arc<IndexReadLeaseGuard>,
    key_kind: u64,
    loaded: Vec<OnceCell<LoadedShard>>,
    pub metrics: NativeBatchMetrics,
}
impl std::fmt::Debug for NativeBatchSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeBatchSnapshot")
            .field("table_id", &self.descriptor.table_id)
            .field("epoch", &self.descriptor.epoch)
            .field("shards", &self.descriptor.shards.len())
            .finish()
    }
}
fn metadata_error(error: LakeSoulMetaDataError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
fn cancelled(cancel: &AtomicBool) -> Result<()> {
    if cancel.load(Ordering::Acquire) {
        Err(DataFusionError::Execution(
            "native vector batch cancelled".into(),
        ))
    } else {
        Ok(())
    }
}
async fn cancellation_requested(cancel: &AtomicBool) {
    while !cancel.load(Ordering::Acquire) {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}
fn normalized_metric(metric: &str) -> Result<&'static str> {
    match metric.to_ascii_uppercase().as_str() {
        "L2" => Ok("L2"),
        "IP" | "INNERPRODUCT" => Ok("IP"),
        _ => Err(DataFusionError::Plan(format!(
            "unsupported batch metric {metric:?}"
        ))),
    }
}
fn object_identity(file: &str) -> String {
    if let Ok(url) = url::Url::parse(file) {
        format!("{}{}", url.host_str().unwrap_or(""), url.path())
    } else {
        file.to_string()
    }
}
/// Equality, not subset coverage: extra superseded files could expose stale
/// rows on an index-only path. Keep partition directories in the identity.
fn validate_coverage(active: &[String], segments: &[SegmentEntry]) -> Result<()> {
    let active: HashSet<_> = active.iter().map(|f| object_identity(f)).collect();
    let covered: HashSet<_> = segments
        .iter()
        .flat_map(|s| &s.data_files)
        .map(|f| object_identity(f))
        .collect();
    if active.is_empty() || covered != active {
        return Err(DataFusionError::NotImplemented(
            "native SQL batch needs an index whose recorded file set exactly matches the snapshot; rebuild the index".into()));
    }
    Ok(())
}

impl NativeBatchSnapshot {
    pub async fn prepare(
        provider: &LakeSoulTableProvider,
        state: &SessionState,
        column: &str,
        metric: &str,
    ) -> Result<Arc<Self>> {
        if provider.read_snapshot.is_some()
            || crate::write_mode::LakeSoulFixedSnapshot::from_session(state).is_some()
        {
            return Err(DataFusionError::NotImplemented(
                "native vector batch does not support time-travel views".into(),
            ));
        }
        if provider.primary_keys != ["id"]
            || provider.logical_schema.field_with_name("id")?.data_type()
                != &DataType::Int64
        {
            return Err(DataFusionError::NotImplemented(
                "native SQL batch requires one Int64 primary key named id".into(),
            ));
        }
        if !provider.io_config.cdc_column().is_empty() {
            return Err(DataFusionError::NotImplemented(
                "native SQL batch requires a table without a CDC change column".into(),
            ));
        }
        let path = provider
            .listing_table_paths
            .first()
            .ok_or_else(|| DataFusionError::Plan("target table has no path".into()))?;
        let store = state.runtime_env().object_store(path.object_store())?;
        let catalog = provider.client.vector_index_catalog();
        for attempt in 0..3 {
            let metadata = provider
                .client
                .vector_batch_metadata(&provider.table_info.table_id)
                .await
                .map_err(metadata_error)?
                .ok_or_else(|| {
                    DataFusionError::Execution("batch target no longer exists".into())
                })?;
            if metadata.table_info.table_schema != provider.table_info.table_schema
                || metadata.table_info.table_schema_arrow_ipc != provider.table_info.table_schema_arrow_ipc
                || metadata.table_info.table_schema_arrow_ipc_json_hash != provider.table_info.table_schema_arrow_ipc_json_hash
                || metadata.table_info.table_path != provider.table_info.table_path
                || metadata.table_info.partitions != provider.table_info.partitions
            {
                return Err(DataFusionError::Execution("target schema/location changed after planning; retry batch statement".into()));
            }
            let fresh_properties: crate::catalog::LakeSoulTableProperty = serde_json::from_str(
                &metadata.table_info.properties,
            ).map_err(|error| DataFusionError::Execution(format!("invalid table properties: {error}")))?;
            if fresh_properties.cdc_change_column.as_deref().is_some_and(|column| !column.is_empty()) {
                return Err(DataFusionError::NotImplemented("native SQL batch requires a snapshot without CDC".into()));
            }
            if !crate::vector_index::vector_index_available(
                &metadata.table_info.properties,
            ) {
                return Err(DataFusionError::Execution(
                    "target vector index is unavailable".into(),
                ));
            }
            let configs = crate::vector_index::parse_vector_index_from_table_properties(
                &metadata.table_info.properties,
            )
            .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
            let config =
                configs.iter().find(|c| c.column == column).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "column {column:?} has no vector index"
                    ))
                })?;
            let index_metric = normalized_metric(&config.params.metric)?;
            if !metric.eq_ignore_ascii_case("auto")
                && normalized_metric(metric)? != index_metric
            {
                return Err(DataFusionError::Plan(
                    "batch metric does not match target index".into(),
                ));
            }
            let files: Vec<String> =
                metadata.data_files.iter().map(|f| f.path.clone()).collect();
            let mut groups: Vec<_> = crate::index::build::group_files_by_shard(
                &files,
                IndexKind::Vector,
                column,
            )
            .into_iter()
            .collect();
            groups.sort_by(|a, b| a.0.cmp(&b.0));
            if groups.iter().map(|(_, files)| files.len()).sum::<usize>() != files.len() {
                return Err(DataFusionError::NotImplemented(
                    "batch files need hash bucket identities".into(),
                ));
            }
            let mut shards = Vec::with_capacity(groups.len());
            for (prefix, files) in groups {
                let view = metadata.index_views.get(&prefix).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "missing batch index shard {prefix:?}"
                    ))
                })?;
                if view.mutation_head.is_some() || view.mutation_records != 0 {
                    return Err(DataFusionError::NotImplemented(
                        "native SQL batch needs a snapshot without index mutations"
                            .into(),
                    ));
                }
                let header = IndexHeader::deserialize(&view.header).map_err(|e| {
                    DataFusionError::Execution(format!("batch header: {e}"))
                })?;
                if header.dim != config.params.dim
                    || (header.metric == lakesoul_vector::Metric::L2)
                        != (index_metric == "L2")
                {
                    return Err(DataFusionError::Execution(
                        "index header differs from vector declaration".into(),
                    ));
                }
                let segments = super::segment_entries(view);
                validate_coverage(&files, &segments)?;
                shards.push(NativeBatchShardDescriptor {
                    index_prefix: prefix,
                    commit_id: view.commit_id,
                    generation: view.generation,
                    version: view.version,
                    header: view.header.clone(),
                    segments,
                    files,
                });
            }
            let pins: Vec<_> = shards
                .iter()
                .map(|s| (s.index_prefix.clone(), s.commit_id))
                .collect();
            let lease = match catalog
                .acquire_snapshot_read_views(&metadata, &pins, "native-vector-batch")
                .await
            {
                Ok(lease) => lease,
                Err(LakeSoulMetaDataError::CommitConflict(_)) if attempt < 2 => continue,
                Err(error) => return Err(metadata_error(error)),
            };
            return Self::with_lease(
                NativeBatchSnapshotDescriptor {
                    table_id: metadata.table_info.table_id.clone(),
                    table_path: metadata.table_info.table_path.clone(),
                    column: column.into(),
                    epoch: crate::vector_index::vector_index_epoch(
                        &metadata.table_info.properties,
                    ),
                    dim: config.params.dim,
                    metric: index_metric.into(),
                    schema: provider.file_schema.as_ref().clone(),
                    partitions: metadata
                        .partitions
                        .iter()
                        .map(|p| (p.partition_desc.clone(), p.version))
                        .collect(),
                    read_lease_id: Some(lease.lease_id.to_string()),
                    shards,
                },
                store,
                lease,
            );
        }
        unreachable!("bounded snapshot retry loop returns")
    }
    fn with_lease(
        descriptor: NativeBatchSnapshotDescriptor,
        store: Arc<dyn ObjectStore>,
        lease: IndexReadLeaseGuard,
    ) -> Result<Arc<Self>> {
        let layout = KeyLayout::from_schema(&descriptor.schema, &["id".into()])
            .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
        let key_kind = layout.kind_hash();
        let loaded = descriptor.shards.iter().map(|_| OnceCell::new()).collect();
        Ok(Arc::new(Self {
            descriptor,
            store,
            lease: Arc::new(lease),
            key_kind,
            loaded,
            metrics: NativeBatchMetrics::default(),
        }))
    }
    pub fn ensure_valid(&self) -> Result<()> {
        self.lease.ensure_valid().map_err(metadata_error)
    }
    pub async fn cancelled(&self) {
        self.lease.cancelled().await
    }
    pub fn dim(&self) -> usize {
        self.descriptor.dim
    }
    pub fn metric(&self) -> &str {
        &self.descriptor.metric
    }
    pub fn descriptor(&self) -> &NativeBatchSnapshotDescriptor {
        &self.descriptor
    }
    pub fn is_empty(&self) -> bool {
        self.descriptor.shards.is_empty()
    }
    pub async fn from_descriptor(
        descriptor: NativeBatchSnapshotDescriptor,
        store: Arc<dyn ObjectStore>,
        catalog: &VectorCatalog,
    ) -> Result<Arc<Self>> {
        let pins: Vec<_> = descriptor
            .shards
            .iter()
            .map(|s| (s.index_prefix.clone(), s.commit_id))
            .collect();
        let parent = descriptor.read_lease_id.as_deref().ok_or_else(|| {
            DataFusionError::Execution(
                "distributed batch snapshot has no parent read lease".into(),
            )
        })?;
        let parent = uuid::Uuid::parse_str(parent).map_err(|_| {
            DataFusionError::Execution(
                "invalid distributed batch read lease token".into(),
            )
        })?;
        let lease = catalog
            .acquire_protected_read_views(&pins, parent, "native-vector-worker")
            .await
            .map_err(metadata_error)?;
        Self::with_lease(descriptor, store, lease)
    }
    /// Conservative bound: inputs/outputs are reserved separately by SQL.
    pub fn estimated_work_bytes(
        &self,
        queries: usize,
        options: &NativeBatchOptions,
    ) -> Result<usize> {
        let overflow = || {
            DataFusionError::ResourcesExhausted("batch memory estimate overflow".into())
        };
        let per_query = self
            .dim()
            .checked_mul(40)
            .and_then(|n| {
                options
                    .candidates()
                    .ok()?
                    .checked_mul(self.descriptor.shards.len().max(1))?
                    .checked_mul(256)?
                    .checked_add(n)
            })
            .ok_or_else(overflow)?;
        queries
            .checked_mul(per_query)
            .and_then(|n| n.checked_add(1024 * 1024))
            .ok_or_else(overflow)
    }
    async fn shard(&self, index: usize, cancel: Arc<AtomicBool>) -> Result<LoadedShard> {
        let descriptor = self.descriptor.shards[index].clone();
        let store = Arc::clone(&self.store);
        let lease = Arc::clone(&self.lease);
        let key_kind = self.key_kind;
        self.loaded[index]
            .get_or_try_init(|| async move {
                tokio::task::spawn_blocking(move || {
                    lakesoul_io::session::GLOBAL_RUNTIME.block_on(async move {
                        // Keep protection until the actual loader has stopped,
                        // even if its consumer drops the waiting future.
                        let _lease = lease;
                        tokio::select! {
                            result = load_typed_shard(
                                &store, &descriptor.index_prefix, descriptor.commit_id,
                                descriptor.version, &descriptor.header, &descriptor.segments,
                                None, 0, key_kind,
                            ) => result.map_err(|e| DataFusionError::External(e.into_boxed_error()))?
                                .ok_or_else(|| DataFusionError::Execution("pinned batch shard cannot load".into())),
                            _ = cancellation_requested(&cancel) => Err(DataFusionError::Execution("batch shard load cancelled".into())),
                            _ = _lease.cancelled() => {
                                cancel.store(true,Ordering::Release);
                                Err(DataFusionError::Execution("batch read lease lost during shard load".into()))
                            }
                        }
                    })
                }).await.map_err(|error| DataFusionError::Execution(format!("batch shard load task: {error}")))?
            }).await.cloned()
    }
    pub async fn search(
        &self,
        queries: &[Vec<f32>],
        options: &NativeBatchOptions,
        cancel: Arc<AtomicBool>,
    ) -> Result<Vec<Vec<NativeBatchHit>>> {
        options.validate()?;
        self.lease.ensure_valid().map_err(metadata_error)?;
        cancelled(&cancel)?;
        if queries.len() > MAX_BATCH_QUERIES
            || queries
                .iter()
                .any(|q| q.len() != self.dim() || q.iter().any(|v| !v.is_finite()))
        {
            return Err(DataFusionError::Plan(
                "invalid batch query dimensions/values/count".into(),
            ));
        }
        if queries.is_empty() {
            return Ok(Vec::new());
        }
        let work = self.estimated_work_bytes(queries.len(), options)?;
        if work > options.memory_bytes {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "batch window needs {work} bytes; budget {}: reduce query_window",
                options.memory_bytes
            )));
        }
        self.metrics.windows.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .queries
            .fetch_add(queries.len() as u64, Ordering::Relaxed);
        let mut per_query: Vec<Vec<(usize, VectorHit)>> = vec![Vec::new(); queries.len()];
        for i in 0..self.descriptor.shards.len() {
            cancelled(&cancel)?;
            let shard = self.shard(i, Arc::clone(&cancel)).await?;
            let input = queries.to_vec();
            let opts = options.clone();
            let stop = Arc::clone(&cancel);
            let lease = Arc::clone(&self.lease);
            let mut task = tokio::task::spawn_blocking(move || {
                let _lease = lease;
                let refs: Vec<_> = input.iter().map(Vec::as_slice).collect();
                shard
                    .search_batch_with_options(
                        &refs,
                        opts.candidates().unwrap(),
                        opts.nprobe,
                        &lakesoul_vector::rabitq::ivf::BatchSearchOptions {
                            query_tile_size: 16,
                            parallelism: opts.effective_parallelism(),
                            cancellation: Some(stop),
                        },
                    )
                    .map_err(|e| DataFusionError::External(e.into_boxed_error()))
            });
            let (hits, diagnostics) = tokio::select! {
                result = &mut task => result.map_err(|e| DataFusionError::Execution(format!("batch probe task: {e}")))??,
                _ = self.lease.cancelled() => {
                    cancel.store(true,Ordering::Release);
                    let _ = task.await;
                    return Err(DataFusionError::Execution("batch read lease lost".into()));
                }
            };
            self.metrics
                .cluster_loads
                .fetch_add(diagnostics.lazy_cluster_loads as u64, Ordering::Relaxed);
            self.metrics
                .shared_block_reuses
                .fetch_add(diagnostics.shared_block_reuses as u64, Ordering::Relaxed);
            self.metrics
                .peak_workers
                .fetch_max(diagnostics.workers as u64, Ordering::Relaxed);
            self.metrics.shard_probes.fetch_add(1, Ordering::Relaxed);
            if hits.len() != queries.len() {
                return Err(DataFusionError::Internal(
                    "batch kernel query count mismatch".into(),
                ));
            }
            for (out, values) in per_query.iter_mut().zip(hits) {
                out.extend(values.into_iter().map(|hit| (i, hit)));
            }
        }
        self.lease.ensure_valid().map_err(metadata_error)?;
        if options.exact {
            self.exact_rerank(queries, per_query, options, cancel).await
        } else {
            let layout = KeyLayout::from_schema(&self.descriptor.schema, &["id".into()])
                .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
            let codec = KeyCodec::new(layout)
                .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
            let rows = per_query
                .into_iter()
                .map(|hits| {
                    finalize_hits(&codec, hits, options.top_k, self.metric() == "L2")
                })
                .collect::<Result<Vec<_>>>()?;
            cancelled(&cancel)?;
            self.ensure_valid()?;
            Ok(rows)
        }
    }
    async fn exact_rerank(
        &self,
        queries: &[Vec<f32>],
        hits: Vec<Vec<(usize, VectorHit)>>,
        options: &NativeBatchOptions,
        cancel: Arc<AtomicBool>,
    ) -> Result<Vec<Vec<NativeBatchHit>>> {
        cancelled(&cancel)?;
        let schema = Arc::new(self.descriptor.schema.clone());
        let declared_field = schema.field_with_name(&self.descriptor.column)?;
        let field = match declared_field.data_type() {
            DataType::List(item) | DataType::LargeList(item) => Arc::new(
                declared_field.clone().with_data_type(DataType::FixedSizeList(
                    item.clone(), i32::try_from(self.dim()).map_err(|_| DataFusionError::Plan("index dimension exceeds Arrow list limit".into()))?,
                )),
            ),
            _ => Arc::new(declared_field.clone()),
        };
        let store = Arc::clone(&self.store);
        let mut ranks: BTreeMap<(usize, String), u64> = BTreeMap::new();
        for (shard_index, shard) in self.descriptor.shards.iter().enumerate() {
            for file in &shard.files {
                let rank = shard
                    .segments
                    .iter()
                    .filter(|s| {
                        s.data_files
                            .iter()
                            .any(|f| object_identity(f) == object_identity(file))
                    })
                    .map(|s| s.birth_version)
                    .max()
                    .unwrap_or(0);
                ranks.insert((shard_index, file.clone()), rank);
            }
        }
        let mut files: Vec<_> = ranks.into_iter().collect();
        files.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let ascending = self.metric() == "L2";
        let input = queries.to_vec();
        let stop = Arc::clone(&cancel);
        let lease = Arc::clone(&self.lease);
        let k = options.top_k;
        let parallelism = options.effective_parallelism();
        let io_concurrency = options.io_concurrency;
        let mut task = tokio::task::spawn_blocking(
            move || -> Result<(Vec<Vec<NativeBatchHit>>, u64)> {
                let _lease = lease;
                let layout = KeyLayout::from_schema(schema.as_ref(), &["id".into()])
                    .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
                let codec = KeyCodec::new(layout)
                    .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
                let mut candidates: Vec<_> =
                    hits.into_iter().map(deduplicate_hits).collect();
                let mut references: BTreeMap<(usize, IndexKey), Vec<(usize, usize)>> =
                    BTreeMap::new();
                for (q, hits) in candidates.iter().enumerate() {
                    for (candidate, (shard, hit)) in hits.iter().enumerate() {
                        references
                            .entry((*shard, hit.key.clone()))
                            .or_default()
                            .push((q, candidate));
                    }
                }
                let keys: Vec<_> = references.keys().cloned().collect();
                let mut owners: HashMap<(usize, IndexKey), u64> =
                    HashMap::with_capacity(keys.len());
                let mut blocks = 0u64;
                // Open and retain only a bounded group of sources. A many-file
                // table must not pin every file's key mapping for the whole window.
                for file_group in files.chunks(io_concurrency) {
                    cancelled(&stop)?;
                    let opened: Vec<_> = lakesoul_io::session::GLOBAL_RUNTIME.block_on(async {
                    let opens = futures::stream::iter(file_group.iter().cloned()).map(|((shard_index,file),rank)| {
                        let store = Arc::clone(&store);
                        let field = Arc::clone(&field);
                        async move {
                            let location = file_location(&file)?;
                            let format = PhysicalFormat::from_extension(location.as_ref())
                                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                            let source = VectorBlockSource::open_cached(store,&location,format,
                                &["id".into()],&[DataType::Int64],&field).await.ok_or_else(||
                                    DataFusionError::NotImplemented(format!("cannot open exact batch source {file:?}")))?;
                            Ok::<_,DataFusionError>((shard_index,source,rank))
                        }
                    }).buffered(io_concurrency).try_collect::<Vec<_>>();
                    tokio::select! {
                        result = opens => result,
                        _ = cancellation_requested(&stop) => Err(DataFusionError::Execution("batch source open cancelled".into())),
                        _ = _lease.cancelled() => {
                            stop.store(true,Ordering::Release);
                            Err(DataFusionError::Execution("batch read lease lost opening sources".into()))
                        }
                    }
                })?;
                    let mut jobs = Vec::new();
                    for (shard_index, source, rank) in opened {
                        cancelled(&stop)?;
                        let mut requests = Vec::new();
                        let shard_keys: Vec<_> = keys
                            .iter()
                            .filter(|(shard, _)| *shard == shard_index)
                            .collect();
                        let prefixes: Vec<_> =
                            shard_keys.iter().map(|(_, key)| key.as_bytes()).collect();
                        for (key, matched) in
                            shard_keys.into_iter().zip(source.lookup_matches(&prefixes))
                        {
                            match matched {
                                PrefixMatch::Missing => {}
                                PrefixMatch::Multiple if !owners.contains_key(key) => {
                                    return Err(DataFusionError::Execution(
                                        "ambiguous visible key in exact batch".into(),
                                    ));
                                }
                                PrefixMatch::Multiple => {}
                                PrefixMatch::Unique(row) => {
                                    if let Some(old_rank) = owners.get(key) {
                                        if *old_rank == rank {
                                            return Err(DataFusionError::Execution("equal-rank files both own an exact batch key".into()));
                                        }
                                        continue; // A newer immutable file owns it.
                                    }
                                    owners.insert(key.clone(), rank);
                                    for &(query, candidate) in &references[key] {
                                        requests.push((query, candidate, row));
                                    }
                                }
                            }
                        }
                        if !requests.is_empty() {
                            jobs.push((source, requests));
                        }
                    }
                    let scored = lakesoul_vector::rabitq::ivf::batch::map_batch_jobs(
                        jobs,
                        parallelism,
                        |(source, requests)| -> Result<_> {
                            cancelled(&stop)?;
                            let (scores, count) = source
                                .score_batch_rows(&requests, &input, ascending, &stop)
                                .ok_or_else(|| {
                                    DataFusionError::Execution(
                                        "exact batch block scoring failed or cancelled"
                                            .into(),
                                    )
                                })?;
                            Ok((requests, scores, count))
                        },
                    );
                    for scored in scored {
                        let (requests, scores, count) = scored?;
                        blocks += count;
                        for ((q, c, _), score) in requests.iter().zip(scores) {
                            candidates[*q][*c].1.score =
                                if ascending { score } else { -score };
                        }
                    }
                }
                if owners.len() != keys.len() {
                    return Err(DataFusionError::Execution(
                        "ANN key missing from exact batch snapshot".into(),
                    ));
                }
                let rows = candidates
                    .into_iter()
                    .map(|hits| finalize_hits(&codec, hits, k, ascending))
                    .collect::<Result<Vec<_>>>()?;
                Ok((rows, blocks))
            },
        );
        let (rows, blocks) = tokio::select! {
            result=&mut task => result.map_err(|e| DataFusionError::Execution(format!("exact batch task: {e}")))??,
            _=self.lease.cancelled() => {
                cancel.store(true,Ordering::Release);
                let _=task.await;
                return Err(DataFusionError::Execution("batch read lease lost during rerank".into()));
            }
        };
        self.metrics
            .rerank_blocks
            .fetch_add(blocks, Ordering::Relaxed);
        cancelled(&cancel)?;
        self.ensure_valid()?;
        Ok(rows)
    }
}
fn file_location(file: &str) -> Result<Path> {
    if let Ok(url) = url::Url::parse(file) {
        Path::from_url_path(url.path())
            .map_err(|e| DataFusionError::Execution(e.to_string()))
    } else {
        Ok(Path::from(file))
    }
}
fn deduplicate_hits(hits: Vec<(usize, VectorHit)>) -> Vec<(usize, VectorHit)> {
    let mut newest: BTreeMap<(usize, IndexKey), VectorHit> = BTreeMap::new();
    for (shard, hit) in hits {
        newest
            .entry((shard, hit.key.clone()))
            .and_modify(|old| {
                if (hit.birth_version, hit.version) > (old.birth_version, old.version) {
                    *old = hit.clone();
                }
            })
            .or_insert(hit);
    }
    newest
        .into_iter()
        .map(|((shard, _), hit)| (shard, hit))
        .collect()
}
fn finalize_hits(
    codec: &KeyCodec,
    hits: Vec<(usize, VectorHit)>,
    k: usize,
    l2: bool,
) -> Result<Vec<NativeBatchHit>> {
    let hits = deduplicate_hits(hits);
    let keys: Vec<_> = hits.iter().map(|(_, h)| &h.key).collect();
    let ids = super::decode_ids_refs(codec, &keys)?;
    let mut rows: Vec<_> = hits
        .into_iter()
        .zip(ids)
        .filter_map(|((_, hit), id)| {
            if !hit.score.is_finite() {
                return None;
            }
            let distance = if l2 {
                (f64::from(hit.score).max(0.0)).sqrt()
            } else {
                -f64::from(hit.score)
            };
            distance
                .is_finite()
                .then_some(NativeBatchHit { id, distance })
        })
        .collect();
    rows.sort_by(|a, b| a.distance.total_cmp(&b.distance).then(a.id.cmp(&b.id)));
    rows.truncate(k);
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn work_parameters_cannot_overflow_or_disable_budget() {
        assert!(
            NativeBatchOptions {
                top_k: usize::MAX,
                exact: true,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            NativeBatchOptions {
                memory_bytes: 0,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(NativeBatchOptions::default().validate().is_ok());
    }
    #[test]
    fn newest_version_wins_instead_of_stale_score() {
        let key = IndexKey::new([1, 2, 3]);
        let hits = deduplicate_hits(vec![
            (
                0,
                VectorHit {
                    key: key.clone(),
                    score: 0.1,
                    version: 1,
                    birth_version: 2,
                },
            ),
            (
                0,
                VectorHit {
                    key,
                    score: 1.5,
                    version: 0,
                    birth_version: 3,
                },
            ),
        ]);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1.score, 1.5);
    }
}
