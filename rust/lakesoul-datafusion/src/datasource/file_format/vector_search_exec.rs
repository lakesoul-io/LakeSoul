// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Physical execution plan that reads the vector-index candidates for a
//! table.
//!
//! The plan is produced by [`LakeSoulTableProvider::scan`] when the logical
//! plan carries the vector-search marker (see
//! [`VectorSearchPushdownRule`](crate::planner::vector_search_rule::VectorSearchPushdownRule)).
//! It reads each partition/bucket through the native [`LakeSoulReader`]
//! configured with the `vector_search_*` options, so the reader runs the
//! ANN search against that bucket's IVF+RaBitQ index and returns only the
//! candidate rows.  The `Sort` + `Limit` nodes above the scan then compute
//! the exact global top-k over the (small) candidate set.

use lakesoul_common::misc::SecretMap;
use std::fmt::Formatter;
use std::sync::{Arc, LazyLock, Mutex};

use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    stream::RecordBatchStreamAdapter,
};
use object_store::path::Path as StorePath;
use rootcause::compat::boxed_error::IntoBoxedError;

use lakesoul_common::IndexKind;
use lakesoul_io::config::{
    LakeSoulIOConfig, LakeSoulIOConfigBuilder, OPTION_KEY_VECTOR_SEARCH_COLUMN,
    OPTION_KEY_VECTOR_SEARCH_METRIC, OPTION_KEY_VECTOR_SEARCH_NPROBE,
    OPTION_KEY_VECTOR_SEARCH_ORDER_BY, OPTION_KEY_VECTOR_SEARCH_PARALLELISM,
    OPTION_KEY_VECTOR_SEARCH_QUERY, OPTION_KEY_VECTOR_SEARCH_REQUESTS,
    OPTION_KEY_VECTOR_SEARCH_TOP_K,
};
use lakesoul_io::execution_trace::instrument_record_batch_stream;
use lakesoul_io::index::IndexLease;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::index::key::{KeyCodec, KeyLayout};
use lakesoul_io::reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
use lakesoul_io::vector::search::{
    LoadedShard, VectorHit, cached_typed_shard, load_typed_shard,
};
use lakesoul_metadata::index_catalog::{
    IndexCommitView, VectorCatalog, VectorSegmentEntry,
};
use lakesoul_vector::{IndexKey, SegmentEntry};

use crate::udf::vector_search_marker::{LakeSoulVectorSearchConfig, VectorSearchRequest};

/// One resolved shard of a snapshot, ready to be probed.
struct CachedSnapshotShard {
    prefix: String,
    /// Data files of the scan group this shard was resolved for; the coverage
    /// proof is re-checked against them on every reuse.
    files: Vec<String>,
    /// Distinct file names the commit covers, so the per-query proof does not
    /// rebuild the same set from a thousand segment entries.
    covered: Vec<String>,
    view: Arc<IndexCommitView<VectorSegmentEntry>>,
    shard: LoadedShard,
}

/// Distinct data-file names covered by a shard commit.
fn covered_file_names(view: &IndexCommitView<VectorSegmentEntry>) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut covered = Vec::new();
    for segment in &view.segments {
        for file in &segment.data_files {
            let name = file.rsplit('/').next().unwrap_or(file).to_string();
            if seen.insert(name.clone()) {
                covered.push(name);
            }
        }
    }
    covered
}

/// Index shards of the exact file set a vector scan was planned from.
///
/// Data files are immutable, so a file set always describes the same rows: a
/// query planned against the set a previous query already resolved can reuse
/// its resolved commits and loaded shards.  Correctness does not rest on that
/// argument alone — the per-query coverage proof re-runs against the cached
/// commits and falls back to the reader path when it fails — and a data commit
/// changes the file set, which misses the cache and resolves afresh.
struct CachedSnapshot {
    key: String,
    shards: Vec<CachedSnapshotShard>,
    bytes: usize,
}

/// How many snapshots stay pinned, and the memory they may hold.
///
/// The entries only add `Arc` references to shards the process-wide index cache
/// already owns, so the budget is a guard for the case where that cache evicts.
const SNAPSHOT_CACHE_ENTRIES: usize = 2;
const SNAPSHOT_CACHE_BYTES: usize = 512 * 1024 * 1024;

static SNAPSHOT_CACHE: LazyLock<Mutex<Vec<Arc<CachedSnapshot>>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Whether the snapshot shard cache is enabled (`LAKESOUL_VECTOR_SNAPSHOT_CACHE=0`
/// turns it off for A/B runs).
fn snapshot_cache_enabled() -> bool {
    !std::env::var("LAKESOUL_VECTOR_SNAPSHOT_CACHE").is_ok_and(|value| value == "0")
}

fn snapshot_cache_get(key: &str) -> Option<Arc<CachedSnapshot>> {
    let cache = SNAPSHOT_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.iter().find(|entry| entry.key == key).map(Arc::clone)
}

fn snapshot_cache_put(entry: Arc<CachedSnapshot>) {
    if entry.bytes > SNAPSHOT_CACHE_BYTES {
        return;
    }
    let mut cache = SNAPSHOT_CACHE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    cache.retain(|existing| existing.key != entry.key);
    cache.insert(0, entry);
    cache.truncate(SNAPSHOT_CACHE_ENTRIES);
    // The entries pin shards the index cache may otherwise evict, so keep the
    // total footprint inside the budget as well.
    let mut total = cache.iter().map(|entry| entry.bytes).sum::<usize>();
    while cache.len() > 1 && total > SNAPSHOT_CACHE_BYTES {
        if let Some(dropped) = cache.pop() {
            total -= dropped.bytes;
        }
    }
}

/// Execution plan for the vector-index candidate scan.
#[derive(Debug)]
pub struct LakeSoulVectorSearchExec {
    /// Output schema: file columns followed by partition columns.
    schema: SchemaRef,
    /// File columns (partition columns excluded).
    file_schema: SchemaRef,
    /// (name, type) of the range partition columns.
    partition_cols: Vec<(String, DataType)>,
    /// One group of files (one partition/bucket) per partition of the scan.
    file_groups: Vec<Vec<PartitionedFile>>,
    /// Values of the partition columns for each file group (aligned with
    /// `partition_cols`).
    partition_values: Vec<Vec<ScalarValue>>,
    /// Object store URL used to reconstruct reader file URIs.
    object_store_url: ObjectStoreUrl,
    /// Primary key columns (the vector index returns primary key ids).
    primary_keys: Vec<String>,
    /// Object store configuration options (e.g. S3 credentials).
    object_store_options: SecretMap,
    /// CDC change column; when set, delete tombstones are dropped after the
    /// merge-on-read merge.
    cdc_column: String,
    /// Vector search parameters.
    vector_search: VectorSearchRequest,
    /// `LIMIT` of the plan above, when it has one: the scan may then keep only
    /// the rows that limit can return and defer the payload columns of the
    /// rest (`vector_search.deferred_payload`).
    fetch_limit: Option<usize>,
    /// Catalog used to resolve index commits and hold reader leases.
    catalog: VectorCatalog,
    /// Runtime metrics.
    metrics: ExecutionPlanMetricsSet,
    /// Plan properties.
    properties: Arc<PlanProperties>,
}

/// Path arithmetic of one bucket group, computed once per query.
struct GroupFiles {
    file_uris: Vec<String>,
    table_prefix: String,
    /// `(index_prefix, hash_bucket)` of every searched index column.
    index_prefixes: Vec<(String, u32)>,
}

/// Current commits of one scan's shards, resolved with one catalog call.
struct ResolvedShards {
    views: std::collections::HashMap<
        String,
        Option<Arc<IndexCommitView<VectorSegmentEntry>>>,
    >,
}

impl ResolvedShards {
    fn view(&self, index_prefix: &str) -> Option<&IndexCommitView<VectorSegmentEntry>> {
        self.views
            .get(index_prefix)
            .and_then(Option::as_ref)
            .map(Arc::as_ref)
    }
}

impl LakeSoulVectorSearchExec {
    /// Create a new vector-search scan plan.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        schema: SchemaRef,
        file_schema: SchemaRef,
        partition_cols: Vec<(String, DataType)>,
        file_groups: Vec<Vec<PartitionedFile>>,
        partition_values: Vec<Vec<ScalarValue>>,
        object_store_url: ObjectStoreUrl,
        primary_keys: Vec<String>,
        object_store_options: SecretMap,
        cdc_column: String,
        vector_search: VectorSearchRequest,
        fetch_limit: Option<usize>,
        catalog: VectorCatalog,
    ) -> DFResult<Self> {
        Ok(Self {
            schema: Arc::clone(&schema),
            file_schema,
            partition_cols,
            file_groups,
            partition_values,
            object_store_url,
            primary_keys,
            object_store_options,
            cdc_column,
            vector_search,
            fetch_limit,
            catalog,
            metrics: ExecutionPlanMetricsSet::new(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }

    /// File URIs, table prefix and index shard prefixes of one bucket group.
    ///
    /// Pure path arithmetic: the caller resolves all groups' shards in one
    /// catalog round trip before building their configs.
    fn group_files(&self, files: &[PartitionedFile]) -> DFResult<GroupFiles> {
        let file_uris = files
            .iter()
            .map(|f| file_uri(&self.object_store_url, &f.object_meta.location))
            .collect::<Vec<_>>();
        let first = file_uris.first().cloned().ok_or_else(|| {
            DataFusionError::Internal("empty vector-search file group".into())
        })?;
        let table_prefix = derive_prefix(&first);
        // Every searched column contributes its shard prefixes; a prefix is
        // resolved (and leased) once even when it appears twice.
        let mut index_prefixes = Vec::new();
        for column in &self.vector_search.columns {
            index_prefixes.extend(lakesoul_io::index::prefix::derive_index_prefixes(
                &file_uris,
                &table_prefix,
                IndexKind::Vector,
                &column.column,
            ));
        }
        Ok(GroupFiles {
            file_uris,
            table_prefix,
            index_prefixes,
        })
    }

    /// Build the native reader configuration for one file group.
    ///
    /// The shard commits were resolved (and leased, unless already cached)
    /// for every group before this call; the native reader then searches the
    /// injected commit without touching the catalog itself, so this is pure
    /// configuration work.
    fn reader_config(
        &self,
        views: &ResolvedShards,
        leases_by_prefix: &std::collections::HashMap<String, Vec<Arc<IndexLease>>>,
        files: &GroupFiles,
        partition_values: &[ScalarValue],
        nprobe: usize,
        parallelism: usize,
        read_schema: &SchemaRef,
        filters: Vec<Expr>,
    ) -> DFResult<LakeSoulIOConfig> {
        let GroupFiles {
            file_uris,
            table_prefix,
            index_prefixes,
        } = files;
        let file_uris = file_uris.clone();
        if file_uris.is_empty() {
            return Err(DataFusionError::Internal(
                "empty vector-search file group".into(),
            ));
        }
        let table_prefix = table_prefix.clone();
        let mut seen_prefixes: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        let mut resolved_shards = Vec::with_capacity(index_prefixes.len());
        let mut leases = Vec::with_capacity(index_prefixes.len());
        for (index_prefix, _bucket) in index_prefixes {
            if !seen_prefixes.insert(index_prefix.as_str()) {
                continue;
            }
            let Some(view) = views.view(index_prefix.as_str()) else {
                continue;
            };
            if let Some(prefix_leases) = leases_by_prefix.get(index_prefix.as_str()) {
                leases.extend(prefix_leases.iter().cloned());
            }
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
            resolved_shards.push(ResolvedIndex {
                kind: IndexKind::Vector,
                index_prefix: index_prefix.to_string(),
                commit_id: view.commit_id,
                generation: view.generation,
                version: view.version,
                header: view.header.clone(),
                segments: serde_json::to_value(&segments).map_err(|error| {
                    DataFusionError::External(
                        rootcause::report!(
                            "failed to serialize vector index segments: {}",
                            error
                        )
                        .into_boxed_error(),
                    )
                })?,
                mutation_head: view.mutation_head.clone(),
                mutation_records: view.mutation_records,
            });
        }

        let mut builder = LakeSoulIOConfigBuilder::default()
            .with_files(file_uris)
            .with_primary_keys(self.primary_keys.clone())
            .with_schema(Arc::clone(read_schema))
            .with_prefix(table_prefix)
            // Phase 2 narrows the second read to the rows ranking kept.
            .with_filters(filters)
            // Push the candidate pk filter into the file scans so the merge
            // only sees the (few) matching rows instead of every row of every
            // file.  DataFusion still re-applies the filter above for
            // correctness (our pushdown is best-effort/Inexact).
            .with_option(lakesoul_io::config::OPTION_KEY_FILE_FILTER_PUSHDOWN, "true");
        if !self.cdc_column.is_empty() {
            // CDC delete tombstones must be dropped after the merge, so the
            // native reader applies `cdc_column != 'delete'`.
            builder = builder.with_option(
                lakesoul_io::config::OPTION_KEY_CDC_COLUMN,
                self.cdc_column.clone(),
            );
        }

        if !self.partition_cols.is_empty() {
            let partition_schema = Arc::new(Schema::new(
                self.partition_cols
                    .iter()
                    .map(|(name, ty)| {
                        arrow::datatypes::Field::new(name, ty.clone(), true)
                    })
                    .collect::<Vec<_>>(),
            ));
            builder = builder.with_partition_schema(partition_schema);
            for ((name, _), value) in self.partition_cols.iter().zip(partition_values) {
                builder = builder
                    .with_default_column_value(name.clone(), scalar_to_string(value));
            }
        }

        let mut has_path_style_config = false;
        for (k, v) in &self.object_store_options {
            if k == "fs.s3a.path.style.access" {
                has_path_style_config = true;
            }
            builder = builder.with_object_store_option(k.clone(), v.clone());
        }
        if !has_path_style_config {
            builder = builder.with_object_store_option(
                "fs.s3a.path.style.access".to_string(),
                "true".to_string(),
            );
        }

        // Exact mode fetches extra candidates per bucket so the global sort
        // above has margin against the approximate within-shard ranking;
        // index-only mode needs each bucket's top-k only (with a small margin
        // when a filter above the scan can drop candidates) because the
        // ordering comes from the index itself.  Either way the physical
        // `Limit` trims back to top_k.
        let candidates_per_bucket = self.vector_search.candidates_per_bucket();
        let primary_metric = self
            .vector_search
            .columns
            .first()
            .map(|column| column.metric.clone())
            .unwrap_or_else(|| "L2".to_string());
        builder = builder
            .with_resolved_index_shards(resolved_shards)
            .with_index_leases(leases)
            .with_option(
                OPTION_KEY_VECTOR_SEARCH_TOP_K,
                candidates_per_bucket.to_string(),
            )
            .with_option(OPTION_KEY_VECTOR_SEARCH_NPROBE, nprobe.to_string())
            .with_option(
                OPTION_KEY_VECTOR_SEARCH_PARALLELISM,
                parallelism.max(1).to_string(),
            )
            .with_option(OPTION_KEY_VECTOR_SEARCH_METRIC, primary_metric);
        if self.vector_search.is_single() {
            // Keep the single-column shorthand: it is the documented reader
            // option pair and the fastest path through the option parser.
            let column = &self.vector_search.columns[0];
            builder = builder
                .with_option(OPTION_KEY_VECTOR_SEARCH_COLUMN, column.column.clone())
                .with_option(OPTION_KEY_VECTOR_SEARCH_QUERY, column.query_csv.clone());
        } else {
            // Several columns: the reader intersects the per-column candidate
            // sets and fuses the distances (index-only ordering) or narrows
            // the scan and lets the exact Sort above re-rank the candidates.
            let requests: Vec<serde_json::Value> = self
                .vector_search
                .columns
                .iter()
                .map(|column| {
                    serde_json::json!({
                        "column": column.column,
                        "query": column.query_csv,
                        "top_k": candidates_per_bucket,
                        "weight": column.weight,
                        "metric": column.metric,
                    })
                })
                .collect();
            builder = builder.with_option(
                OPTION_KEY_VECTOR_SEARCH_REQUESTS,
                serde_json::Value::Array(requests).to_string(),
            );
        }
        if self.vector_search.order_by {
            // Index-only ordering: the reader appends the index distance per
            // candidate row and the vector column drops out of the scan.
            builder = builder
                .with_option(OPTION_KEY_VECTOR_SEARCH_ORDER_BY, "true".to_string());
        }
        Ok(builder.build())
    }

    /// The columns the ranking needs: the primary keys, the searched vector
    /// columns and the CDC column the merge reads.
    fn ranking_columns(&self) -> std::collections::HashSet<String> {
        let mut names: std::collections::HashSet<String> =
            self.primary_keys.iter().cloned().collect();
        for column in &self.vector_search.columns {
            names.insert(column.column.clone());
        }
        if !self.cdc_column.is_empty() {
            names.insert(self.cdc_column.clone());
        }
        names
    }

    /// Projected columns that are not needed to rank a candidate.
    fn payload_columns(&self) -> Vec<String> {
        let ranking = self.ranking_columns();
        self.file_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .filter(|name| !ranking.contains(name))
            .collect()
    }

    /// The file schema reduced to the ranking columns, in schema order.
    fn ranking_schema(&self) -> SchemaRef {
        let ranking = self.ranking_columns();
        let fields: Vec<arrow::datatypes::Field> = self
            .file_schema
            .fields()
            .iter()
            .filter(|field| ranking.contains(field.name()))
            .map(|field| field.as_ref().clone())
            .collect();
        Arc::new(Schema::new_with_metadata(
            fields,
            self.file_schema.metadata().clone(),
        ))
    }

    /// Rows the scan keeps before fetching payload: the plan's limit (or the
    /// limit the pushdown rule matched) plus a margin for ties and float
    /// differences between the ranking here and the sort above.
    fn keep_rows(&self) -> usize {
        self.fetch_limit
            .unwrap_or(0)
            .max(self.vector_search.top_k)
            .saturating_mul(2)
            .saturating_add(16)
    }

    /// True when the payload columns can be fetched after ranking: a limit is
    /// known, nothing filters rows between the scan and the sort, the payload
    /// is not empty and the session has not turned it off.
    fn defers_payload(
        &self,
        session_config: &datafusion::execution::config::SessionConfig,
    ) -> bool {
        let enabled = session_config
            .options()
            .extensions
            .get::<LakeSoulVectorSearchConfig>()
            .map(|options| options.deferred_payload)
            .unwrap_or(true);
        if !(enabled && self.vector_search.narrow && !self.payload_columns().is_empty()) {
            return false;
        }
        if self.keep_rows() == 0 {
            return false;
        }
        if self.vector_search.order_by {
            // Index-only ordering ranks on the index distance the reader
            // appends, whatever the projected schema holds.
            return true;
        }
        // The exact ranking happens in this process and must agree with the
        // sort above; anything it cannot compute exactly keeps the
        // single-read path.
        let metric = self.vector_search.columns[0].metric.as_str();
        if !(metric.eq_ignore_ascii_case("L2") || metric.eq_ignore_ascii_case("IP")) {
            return false;
        }
        if self
            .vector_search
            .columns
            .iter()
            .any(|column| !column.metric.eq_ignore_ascii_case(metric))
        {
            return false;
        }
        self.vector_search.columns.iter().all(|column| {
            self.file_schema
                .field_with_name(&column.column)
                .map(|field| is_float_list(field.data_type()))
                .unwrap_or(false)
        })
    }

    /// Build one reader config per bucket group.
    ///
    /// The shard commits of every bucket are resolved with a single catalog
    /// round trip: resolving one shard per group used to cost one round trip
    /// per bucket and dominated this phase of a query.
    fn build_configs(
        &self,
        read_schema: &SchemaRef,
        filters: Vec<datafusion::logical_expr::Expr>,
        nprobe: usize,
        parallelism: usize,
    ) -> DFResult<Vec<LakeSoulIOConfig>> {
        let groups = self
            .file_groups
            .iter()
            .map(|group| self.group_files(group))
            .collect::<DFResult<Vec<_>>>()?;
        let mut prefixes: Vec<String> = groups
            .iter()
            .flat_map(|group| {
                group
                    .index_prefixes
                    .iter()
                    .map(|(prefix, _)| prefix.clone())
            })
            .collect();
        prefixes.sort_unstable();
        prefixes.dedup();
        let t_handoff = std::time::Instant::now();
        let mut handoff_ms = 0.0_f64;
        let mut resolve_ms = 0.0_f64;
        let (views, leases_by_prefix) = tokio::task::block_in_place(|| {
            handoff_ms = t_handoff.elapsed().as_secs_f64() * 1000.0;
            lakesoul_io::session::GLOBAL_RUNTIME.block_on(async {
                let t_resolve = std::time::Instant::now();
                let views = self.catalog.resolve_cached_many(&prefixes).await.map_err(
                    |error| {
                        DataFusionError::External(
                            rootcause::report!(
                                "failed to resolve vector index shards: {}",
                                error
                            )
                            .into_boxed_error(),
                        )
                    },
                )?;
                resolve_ms = t_resolve.elapsed().as_secs_f64() * 1000.0;
                // A lease is only needed while the index is being loaded; a
                // cache hit reads no file at all.  Leases are taken in the
                // same trip as the resolution so the whole phase costs one
                // runtime hand-off instead of one per bucket.
                let mut leases: std::collections::HashMap<String, Vec<Arc<IndexLease>>> =
                    std::collections::HashMap::new();
                for prefix in &prefixes {
                    let Some(view) = views.get(prefix).and_then(Option::as_ref) else {
                        continue;
                    };
                    if lakesoul_io::index::cache::is_loaded(
                        IndexKind::Vector,
                        prefix.trim_end_matches('/'),
                        view.commit_id,
                    )
                    .await
                    {
                        continue;
                    }
                    let lease = self
                        .catalog
                        .acquire_lease(prefix, lease_ttl(), &lease_owner())
                        .await
                        .map_err(|error| {
                            DataFusionError::External(
                                rootcause::report!(
                                    "failed to lease vector index at '{}': {}",
                                    prefix,
                                    error
                                )
                                .into_boxed_error(),
                            )
                        })?;
                    if let Some(handle) = lease {
                        leases
                            .entry(prefix.clone())
                            .or_default()
                            .push(Arc::new(IndexLease::new(handle)));
                    }
                }
                Ok::<_, DataFusionError>((ResolvedShards { views }, leases))
            })
        })?;
        let block_ms = t_handoff.elapsed().as_secs_f64() * 1000.0;
        let t_configs = std::time::Instant::now();
        let mut configs = Vec::with_capacity(self.file_groups.len());
        for ((_group, values), files) in self
            .file_groups
            .iter()
            .zip(&self.partition_values)
            .zip(groups)
        {
            let config = self.reader_config(
                &views,
                &leases_by_prefix,
                &files,
                values,
                nprobe,
                parallelism,
                read_schema,
                filters.clone(),
            )?;
            configs.push(config);
        }
        if std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok() {
            debug!(
                handoff_ms,
                resolve_ms,
                block_ms,
                configs_ms = t_configs.elapsed().as_secs_f64() * 1000.0,
                groups = configs.len(),
                "vector build_configs profile"
            );
        }
        Ok(configs)
    }

    /// True when the output of this scan is exactly primary-key columns of
    /// the index-only ordering.
    ///
    /// Anything else (a payload projection, an exact re-rank, a CDC change
    /// column, range partitions) needs the data files and keeps the reader
    /// path.
    fn index_only_eligible(&self) -> bool {
        // Escape hatch for A/B runs against the reader path
        // (`LAKESOUL_VECTOR_INDEX_ONLY=0`).
        if std::env::var("LAKESOUL_VECTOR_INDEX_ONLY").is_ok_and(|value| value == "0") {
            return false;
        }
        if !self.vector_search.order_by
            || !self.vector_search.is_single()
            || !self.cdc_column.is_empty()
            || !self.partition_cols.is_empty()
        {
            return false;
        }
        let primary_keys: std::collections::HashSet<&str> =
            self.primary_keys.iter().map(String::as_str).collect();
        self.schema
            .fields()
            .iter()
            .all(|field| primary_keys.contains(field.name().as_str()))
    }

    /// Identity of the snapshot this scan reads.
    ///
    /// The file groups are the rows the plan can return, and the searched
    /// columns plus the ordering decide which shards serve them; data files are
    /// immutable, so two scans with this key read the same rows from the same
    /// shards.
    fn snapshot_key(&self, expected_key_kind: u64) -> String {
        let mut key = String::with_capacity(256);
        key.push_str(self.object_store_url.as_str());
        key.push('\u{0}');
        key.push_str(&format!("{expected_key_kind:016x}"));
        key.push('\u{0}');
        key.push(if self.vector_search.order_by {
            '1'
        } else {
            '0'
        });
        for column in &self.vector_search.columns {
            key.push('\u{0}');
            key.push_str(&column.column);
            key.push('\u{2}');
            key.push_str(&column.metric);
        }
        for group in &self.file_groups {
            key.push('\u{1}');
            for file in group {
                key.push('\u{0}');
                key.push_str(file.object_meta.location.as_ref());
            }
        }
        key
    }

    /// Re-run the coverage proof on a cached snapshot.
    ///
    /// The cache never bypasses the proof: a cached commit is only served while
    /// it still covers exactly the files the plan reads.
    fn snapshot_covered(
        &self,
        snapshot: &CachedSnapshot,
        expected_prefixes: &[String],
    ) -> bool {
        if snapshot.shards.len() != expected_prefixes.len() {
            return false;
        }
        if snapshot
            .shards
            .iter()
            .zip(expected_prefixes)
            .any(|(slot, prefix)| &slot.prefix != prefix)
        {
            return false;
        }
        snapshot.shards.iter().all(|slot| {
            let covered: Vec<&str> = slot.covered.iter().map(String::as_str).collect();
            crate::vector_batch::can_skip_verification(
                &self.cdc_column,
                slot.view.mutation_head.is_none(),
                slot.view.mutation_records,
                &slot.files,
                &covered,
            )
        })
    }

    /// Answer a primary-key-only projection from the index keys alone.
    ///
    /// When the plan ranks on the index distance (`rerank = 'index'`), the
    /// output holds nothing but primary-key columns, and every shard's commit
    /// provably covers its snapshot (no mutation chain, no CDC change column,
    /// no active file outside the commit), the candidate rows *are* the keys
    /// the index returned: no data file has to be opened, scanned or merged.
    /// This is the single-query counterpart of the snapshot batch entry and
    /// it removes the per-bucket reader setup (a DataFusion session and a
    /// physical plan per bucket) from the query path.
    ///
    /// Returns `Ok(None)` whenever a condition cannot be proven.  The caller
    /// then runs the regular reader path, so correctness never depends on the
    /// fast path being taken.
    fn try_index_only_batches(
        &self,
        context: &TaskContext,
        nprobe: usize,
        parallelism: usize,
    ) -> DFResult<Option<Vec<arrow::record_batch::RecordBatch>>> {
        if !self.index_only_eligible() {
            return Ok(None);
        }
        let store = context
            .runtime_env()
            .object_store(self.object_store_url.clone())
            .map_err(|error| {
                DataFusionError::External(
                    rootcause::report!("failed to get object store: {}", error)
                        .into_boxed_error(),
                )
            })?;
        let t_handoff = std::time::Instant::now();
        let mut handoff_ms = 0.0_f64;
        let batches =
            tokio::task::block_in_place(|| {
                handoff_ms = t_handoff.elapsed().as_secs_f64() * 1000.0;
                lakesoul_io::session::GLOBAL_RUNTIME
                    .block_on(self.index_only_batches_inner(&store, nprobe, parallelism))
            });
        if std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok() {
            debug!(
                handoff_ms,
                total_ms = t_handoff.elapsed().as_secs_f64() * 1000.0,
                "vector index-only profile"
            );
        }
        batches
    }

    /// Async body of [`Self::try_index_only_batches`]; the caller has already
    /// checked [`Self::index_only_eligible`].
    async fn index_only_batches_inner(
        &self,
        store: &Arc<dyn object_store::ObjectStore>,
        nprobe: usize,
        parallelism: usize,
    ) -> DFResult<Option<Vec<arrow::record_batch::RecordBatch>>> {
        let profile = std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok();
        let t_inner = std::time::Instant::now();
        let layout =
            KeyLayout::from_schema(self.file_schema.as_ref(), &self.primary_keys)
                .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
        let codec = KeyCodec::new(layout.clone())
            .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
        let expected_key_kind = layout.kind_hash();

        let groups = self
            .file_groups
            .iter()
            .map(|group| self.group_files(group))
            .collect::<DFResult<Vec<_>>>()?;
        let mut prefixes: Vec<String> = groups
            .iter()
            .flat_map(|group| {
                group
                    .index_prefixes
                    .iter()
                    .map(|(prefix, _)| prefix.clone())
            })
            .collect();
        prefixes.sort_unstable();
        prefixes.dedup();
        let column = &self.vector_search.columns[0];
        let query =
            lakesoul_io::vector::search::parse_query_vector(&column.query_csv, None)
                .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
        let top_k = self.vector_search.candidates_per_bucket();

        // A repeat query on the same file set reuses the shards the previous
        // one resolved: the plan is built from the snapshot's file groups and
        // data files are immutable, so the same file set reads the same rows.
        // The coverage proof re-runs below, so an unproven entry falls back.
        let t_search = std::time::Instant::now();
        let mut resolve_ms = 0.0_f64;
        let cache_key = self.snapshot_key(expected_key_kind);
        let expected_prefixes: Vec<String> = groups
            .iter()
            .flat_map(|group| {
                group
                    .index_prefixes
                    .iter()
                    .map(|(prefix, _)| prefix.clone())
            })
            .collect();
        let cached_snapshot = if snapshot_cache_enabled() {
            snapshot_cache_get(&cache_key)
                .filter(|entry| self.snapshot_covered(entry, &expected_prefixes))
        } else {
            None
        };
        let snapshot_cached = cached_snapshot.is_some();
        let snapshot = match cached_snapshot {
            Some(entry) => entry,
            None => {
                let t_resolve = std::time::Instant::now();
                let views = self.catalog.resolve_cached_many(&prefixes).await.map_err(
                    |error| {
                        DataFusionError::External(
                            rootcause::report!(
                                "failed to resolve vector index shards: {}",
                                error
                            )
                            .into_boxed_error(),
                        )
                    },
                )?;
                resolve_ms = t_resolve.elapsed().as_secs_f64() * 1000.0;
                // Prove that the resolved commits cover this snapshot before
                // touching any shard.
                let mut requested: Vec<(
                    String,
                    Vec<String>,
                    Arc<IndexCommitView<VectorSegmentEntry>>,
                )> = Vec::with_capacity(groups.len());
                for group in &groups {
                    for (prefix, _bucket) in &group.index_prefixes {
                        let Some(view) = views.get(prefix).and_then(Option::as_ref)
                        else {
                            return Ok(None);
                        };
                        let covered: Vec<&str> = view
                            .segments
                            .iter()
                            .flat_map(|segment| {
                                segment.data_files.iter().map(String::as_str)
                            })
                            .collect();
                        if !crate::vector_batch::can_skip_verification(
                            &self.cdc_column,
                            view.mutation_head.is_none(),
                            view.mutation_records,
                            &group.file_uris,
                            &covered,
                        ) {
                            return Ok(None);
                        }
                        requested.push((
                            prefix.clone(),
                            group.file_uris.clone(),
                            Arc::clone(view),
                        ));
                    }
                }
                // A hot shard comes straight from the index cache; four
                // lookups overlap instead of queuing.
                let lookups = requested.iter().map(|(prefix, _, view)| {
                    cached_typed_shard(prefix, view.commit_id, expected_key_kind)
                });
                let cached = futures::future::join_all(lookups).await;
                let mut shards: Vec<CachedSnapshotShard> =
                    Vec::with_capacity(requested.len());
                for ((prefix, files, view), cached_shard) in
                    requested.into_iter().zip(cached)
                {
                    let shard = match cached_shard {
                        Some(shard) => shard,
                        None => {
                            // A cold shard loads from its commit; the catalog
                            // view already holds the parsed segment list, so
                            // hand it over instead of rebuilding the serialized
                            // transport form.
                            let segments =
                                crate::vector_batch::segment_entries(view.as_ref());
                            let Some(shard) = load_typed_shard(
                                store,
                                &prefix,
                                view.commit_id,
                                view.version,
                                &view.header,
                                &segments,
                                view.mutation_head.as_deref(),
                                view.mutation_records,
                                expected_key_kind,
                            )
                            .await
                            .map_err(|error| {
                                DataFusionError::External(error.into_boxed_error())
                            })?
                            else {
                                // The shard was written for another key
                                // layout; the reader path reports that the
                                // same way.
                                return Ok(None);
                            };
                            shard
                        }
                    };
                    let covered = covered_file_names(view.as_ref());
                    shards.push(CachedSnapshotShard {
                        prefix,
                        files,
                        covered,
                        view,
                        shard,
                    });
                }
                let bytes = shards.iter().map(|slot| slot.shard.memory_bytes()).sum();
                let entry = Arc::new(CachedSnapshot {
                    key: cache_key.clone(),
                    shards,
                    bytes,
                });
                snapshot_cache_put(Arc::clone(&entry));
                entry
            }
        };
        let shards: Vec<LoadedShard> = snapshot
            .shards
            .iter()
            .map(|slot| slot.shard.clone())
            .collect();
        let load_ms = t_search.elapsed().as_secs_f64() * 1000.0;
        let t_probe = std::time::Instant::now();
        // Probe the shards on the shared blocking pool: a query does not
        // warrant four fresh OS threads, and the pool threads are already warm.
        let query = Arc::new(query);
        let mut tasks = Vec::with_capacity(shards.len());
        for shard in &shards {
            let shard = shard.clone();
            let query = Arc::clone(&query);
            tasks.push(
                lakesoul_io::session::GLOBAL_RUNTIME.spawn_blocking(move || {
                    let query_ref: [&[f32]; 1] = [query.as_slice()];
                    shard.search_batch(&query_ref, top_k, nprobe, parallelism)
                }),
            );
        }
        let mut per_shard: Vec<Vec<Vec<VectorHit>>> = Vec::with_capacity(tasks.len());
        for task in tasks {
            let probed = task.await.map_err(|error| {
                DataFusionError::Execution(format!("vector shard probe failed: {error}"))
            })?;
            per_shard.push(
                probed.map_err(|error| {
                    DataFusionError::External(error.into_boxed_error())
                })?,
            );
        }
        let probe_ms = t_probe.elapsed().as_secs_f64() * 1000.0;
        let candidates_total = per_shard.iter().map(|hits| hits.len()).sum::<usize>();
        let mut best: std::collections::HashMap<IndexKey, f32> =
            std::collections::HashMap::with_capacity(candidates_total);
        for hits in per_shard.into_iter().flatten().flatten() {
            best.entry(hits.key)
                .and_modify(|score| *score = score.min(hits.score))
                .or_insert(hits.score);
        }
        let mut ranked: Vec<(f32, IndexKey)> =
            best.into_iter().map(|(key, score)| (score, key)).collect();
        let descending = !column.metric.eq_ignore_ascii_case("L2");
        let compare = |left: &(f32, IndexKey), right: &(f32, IndexKey)| {
            let order = left
                .0
                .partial_cmp(&right.0)
                .unwrap_or(std::cmp::Ordering::Equal);
            let order = if descending { order.reverse() } else { order };
            order.then_with(|| left.1.cmp(&right.1))
        };
        if ranked.is_empty() {
            return Ok(Some(Vec::new()));
        }
        // Index-only ordering: the plan above keeps at most `top_k` rows, and
        // with no filter between the scan and that limit (`narrow`) nothing can
        // drop a candidate, so only the leading rows need to be ordered and
        // decoded.  Partitioning first keeps the cost of the candidate scan
        // linear in the candidate count instead of sorting all of it.
        let output_rows = if self.vector_search.narrow {
            self.vector_search.top_k.max(1).min(ranked.len())
        } else {
            ranked.len()
        };
        if output_rows < ranked.len() {
            ranked.select_nth_unstable_by(output_rows - 1, compare);
            ranked.truncate(output_rows);
        }
        ranked.sort_by(compare);
        let candidates = ranked.len();
        let keys: Vec<IndexKey> = ranked.into_iter().map(|(_, key)| key).collect();
        let decoded = codec
            .decode(&keys)
            .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        for field in self.schema.fields() {
            let position = self
                .primary_keys
                .iter()
                .position(|key| key == field.name())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "primary key column '{}' is missing from the index key layout",
                        field.name()
                    ))
                })?;
            columns.push(Arc::clone(&decoded[position]));
        }
        if profile {
            debug!(
                resolve_ms,
                load_ms,
                probe_ms,
                candidates,
                shards = shards.len(),
                snapshot_cached,
                total_ms = t_inner.elapsed().as_secs_f64() * 1000.0,
                "vector index-only inner profile"
            );
        }
        Ok(Some(vec![arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&self.schema),
            columns,
        )?]))
    }

    /// Read every bucket's candidates through the native reader.
    ///
    /// The reader's async API is not `Send`, so it is driven through the
    /// blocking wrapper on the global runtime; the candidate set is bounded by
    /// top_k × bucket_count and cheap to collect.  Buckets are independent —
    /// their indexes and data files do not overlap — so they are read in
    /// parallel, one scoped thread per bucket.
    fn read_configs(
        &self,
        configs: Vec<LakeSoulIOConfig>,
        span: &tracing::Span,
        profile: bool,
    ) -> DFResult<Vec<arrow::record_batch::RecordBatch>> {
        fn read_bucket(
            config: LakeSoulIOConfig,
            profile: bool,
        ) -> DFResult<Vec<arrow::record_batch::RecordBatch>> {
            let t_create = std::time::Instant::now();
            let reader = LakeSoulReader::new(config)
                .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
            let mut sync_reader =
                SyncSendableMutableLakeSoulReader::new_with_global_runtime(reader);
            let create = t_create.elapsed();
            let t_start = std::time::Instant::now();
            sync_reader
                .start_blocked()
                .map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
            let start = t_start.elapsed();
            let t_scan = std::time::Instant::now();
            let mut rows = 0usize;
            let mut batches = Vec::new();
            while let Some(batch) = sync_reader.next_rb_blocked() {
                let batch =
                    batch.map_err(|e| DataFusionError::External(e.into_boxed_error()))?;
                rows += batch.num_rows();
                batches.push(batch);
            }
            if profile {
                debug!(
                    create_ms = create.as_secs_f64() * 1000.0,
                    start_ms = start.as_secs_f64() * 1000.0,
                    scan_ms = t_scan.elapsed().as_secs_f64() * 1000.0,
                    rows,
                    "profiled vector search bucket"
                );
            }
            Ok(batches)
        }

        let t_handoff = std::time::Instant::now();
        let mut handoff_ms = 0.0_f64;
        let t_total = std::time::Instant::now();
        let batches = tokio::task::block_in_place(|| {
            handoff_ms = t_handoff.elapsed().as_secs_f64() * 1000.0;
            let mut configs = configs;
            if configs.len() == 1 {
                let config = configs.pop().expect("one config");
                return read_bucket(config, profile);
            }
            std::thread::scope(|scope| {
                let handles: Vec<_> = configs
                    .into_iter()
                    .map(|config| {
                        let span = span.clone();
                        scope.spawn(move || {
                            let _entered = span.enter();
                            read_bucket(config, profile)
                        })
                    })
                    .collect();
                let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();
                for handle in handles {
                    let mut bucket_batches = handle.join().map_err(|_| {
                        DataFusionError::Execution(
                            "vector search bucket reader panicked".to_string(),
                        )
                    })??;
                    batches.append(&mut bucket_batches);
                }
                Ok::<_, DataFusionError>(batches)
            })
        });
        if profile {
            debug!(
                handoff_ms,
                total_ms = t_total.elapsed().as_secs_f64() * 1000.0,
                "vector read_configs profile"
            );
        }
        batches
    }

    /// Rank the phase-1 candidates, keep the rows the plan's limit may return
    /// and read their payload columns in a second pass over the same files.
    #[allow(clippy::too_many_arguments)]
    fn deferred_payload_batches(
        &self,
        candidates: Vec<arrow::record_batch::RecordBatch>,
        nprobe: usize,
        parallelism: usize,
        metric: &str,
        span: &tracing::Span,
        profile: bool,
        partition: usize,
    ) -> DFResult<Vec<arrow::record_batch::RecordBatch>> {
        if candidates.is_empty() {
            return Ok(candidates);
        }
        let ranked = if self.vector_search.order_by {
            let ranked_schema = candidates[0].schema();
            order_by_index_distance(candidates, &ranked_schema, metric)?
        } else {
            rank_by_exact_distance(&self.vector_search, metric, candidates)?
        };
        if ranked.is_empty() {
            return Ok(Vec::new());
        }
        let kept = take_first_rows(&ranked, self.keep_rows())?;
        if kept.num_rows() == 0 {
            return Ok(Vec::new());
        }

        let counters = VectorSearchCounters::new(&self.metrics, partition);
        counters.ranked.add(kept.num_rows());
        let filters = self.pk_filters(&kept)?;
        let configs =
            self.build_configs(&self.file_schema, filters, nprobe, parallelism)?;
        let payload = self.read_configs(configs, span, profile)?;
        let payload_rows: usize = payload.iter().map(|batch| batch.num_rows()).sum();
        counters.payload_rows.add(payload_rows);
        let ordered = order_payload_by_keys(&kept, payload, &self.primary_keys)?;
        ordered
            .iter()
            .map(|batch| project_batch(batch, &self.schema))
            .collect()
    }

    /// `pk = v OR pk = w ...` for every primary-key column of `batch`.
    fn pk_filters(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> DFResult<Vec<datafusion::logical_expr::Expr>> {
        let mut filters = Vec::with_capacity(self.primary_keys.len());
        for name in &self.primary_keys {
            let index = batch.schema().index_of(name)?;
            let array = batch.column(index);
            let mut values = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                values.push(Expr::Literal(
                    ScalarValue::try_from_array(array, row)?,
                    None,
                ));
            }
            filters.push(
                Expr::Column(datafusion::common::Column::new_unqualified(name.clone()))
                    .in_list(values, false),
            );
        }
        Ok(filters)
    }
}

impl ExecutionPlanProperties for LakeSoulVectorSearchExec {
    fn output_partitioning(&self) -> &Partitioning {
        &Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        self.properties.equivalence_properties()
    }
}

impl ExecutionPlan for LakeSoulVectorSearchExec {
    fn name(&self) -> &str {
        "LakeSoulVectorSearchExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "LakeSoulVectorSearchExec has no children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "LakeSoulVectorSearchExec only supports 1 partition, got {partition}"
            )));
        }
        let (nprobe, parallelism) = context
            .session_config()
            .options()
            .extensions
            .get::<LakeSoulVectorSearchConfig>()
            .map(|options| (options.nprobe, options.parallelism))
            .unwrap_or((64, 1));
        let searched_columns = self
            .vector_search
            .columns
            .iter()
            .map(|column| column.column.as_str())
            .collect::<Vec<_>>()
            .join("+");
        let primary_metric = self
            .vector_search
            .columns
            .first()
            .map(|column| column.metric.as_str())
            .unwrap_or("L2");
        let span = info_span!(
            "vector_search_execute",
            partition,
            column = %searched_columns,
            metric = %primary_metric,
            top_k = self.vector_search.top_k,
            nprobe,
            bucket_group_count = self.file_groups.len(),
            outcome = tracing::field::Empty,
            output_batches = tracing::field::Empty,
            output_rows = tracing::field::Empty,
            error = tracing::field::Empty,
        );
        let entered = span.enter();

        // Phase 1 reads only the columns the ranking needs.  When the plan
        // above has no filter between the scan and the sort and a limit, the
        // scan keeps only the rows that limit may return and fetches the
        // payload columns for those rows in a second pass
        // (`lakesoul_vector_search.deferred_payload`).
        let schema = Arc::clone(&self.schema);
        let defer_payload = self.defers_payload(context.session_config());
        let ranking_schema = if defer_payload {
            self.ranking_schema()
        } else {
            Arc::clone(&self.file_schema)
        };
        let profile = std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok();
        let t_exec = std::time::Instant::now();
        // Fast path: for a primary-key-only projection of the index-only
        // ordering the shard keys themselves are the answer, so the four
        // bucket readers are unnecessary.
        if let Some(batches) =
            self.try_index_only_batches(&context, nprobe, parallelism)?
        {
            if profile {
                debug!(
                    batches = batches.len(),
                    rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                    "vector search served from the index keys"
                );
            }
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream,
            )) as SendableRecordBatchStream;
            drop(entered);
            return Ok(instrument_record_batch_stream(
                stream,
                span,
                "vector_search",
            ));
        }
        let t_phase = std::time::Instant::now();
        let configs =
            self.build_configs(&ranking_schema, Vec::new(), nprobe, parallelism)?;
        let build_ms = t_phase.elapsed().as_secs_f64() * 1000.0;
        let t_phase = std::time::Instant::now();
        let candidates = self.read_configs(configs, &span, profile)?;
        let read_ms = t_phase.elapsed().as_secs_f64() * 1000.0;
        if profile {
            debug!(
                build_ms,
                read_ms,
                total_ms = t_exec.elapsed().as_secs_f64() * 1000.0,
                defer_payload,
                batches = candidates.len(),
                "vector search exec profile"
            );
        }
        let batches = if defer_payload {
            self.deferred_payload_batches(
                candidates,
                nprobe,
                parallelism,
                primary_metric,
                &span,
                profile,
                partition,
            )?
        } else if self.vector_search.order_by {
            order_by_index_distance(candidates, &schema, primary_metric)?
        } else {
            candidates
        };
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream))
            as SendableRecordBatchStream;
        drop(entered);
        Ok(instrument_record_batch_stream(
            stream,
            span,
            "vector_search",
        ))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

/// Merge the per-bucket batches ordered by the reserved index-distance
/// column and project to `output_schema` (dropping the internal column).
///
/// The row count is left to the plan's `Limit` so filters between the scan
/// and the limit keep their semantics.
fn order_by_index_distance(
    batches: Vec<arrow::record_batch::RecordBatch>,
    output_schema: &SchemaRef,
    metric: &str,
) -> DFResult<Vec<arrow::record_batch::RecordBatch>> {
    use arrow::compute::{
        SortColumn, SortOptions, lexsort_to_indices, take_record_batch,
    };

    let batches: Vec<_> = batches
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect();
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let distance_index = batches[0]
        .schema()
        .index_of(lakesoul_io::vector::score::VECTOR_DISTANCE_FIELD)
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "vector index-distance column missing from the scan output: {error}"
            ))
        })?;
    let combined = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
    let indices = lexsort_to_indices(
        &[SortColumn {
            values: Arc::clone(combined.column(distance_index)),
            options: Some(SortOptions {
                descending: !metric.eq_ignore_ascii_case("L2"),
                nulls_first: false,
            }),
        }],
        None,
    )?;
    let sorted = take_record_batch(&combined, &indices)?;
    let mut columns = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        let index = sorted.schema().index_of(field.name())?;
        columns.push(Arc::clone(sorted.column(index)));
    }
    Ok(vec![arrow::record_batch::RecordBatch::try_new(
        Arc::clone(output_schema),
        columns,
    )?])
}

impl DisplayAs for LakeSoulVectorSearchExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let columns = self
            .vector_search
            .columns
            .iter()
            .map(|column| column.column.as_str())
            .collect::<Vec<_>>()
            .join("+");
        write!(
            f,
            "LakeSoulVectorSearchExec(vec={}, top_k={}, metric={}, order_by={})",
            columns,
            self.vector_search.top_k,
            self.vector_search
                .columns
                .first()
                .map(|column| column.metric.as_str())
                .unwrap_or("L2"),
            self.vector_search.order_by
        )
    }
}

/// Lease time-to-live for index readers (seconds).
pub(crate) fn lease_ttl() -> std::time::Duration {
    let seconds = std::env::var("LAKESOUL_VECTOR_INDEX_LEASE_TTL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(300);
    std::time::Duration::from_secs(seconds)
}

/// Owner tag recorded on reader leases.
pub(crate) fn lease_owner() -> String {
    format!(
        "{}:{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
        std::process::id()
    )
}

/// Reconstruct a full file URI for the native reader from the object store
/// URL and the store-relative location.
pub(crate) fn file_uri(
    object_store_url: &ObjectStoreUrl,
    location: &StorePath,
) -> String {
    let url = object_store_url.to_string();
    if url.starts_with("file:") {
        format!("file:///{}", location.as_ref())
    } else {
        format!("{}/{}", url.trim_end_matches('/'), location.as_ref())
    }
}

/// Derive the store prefix (parent directory) from a file URI, preserving
/// the scheme and authority.
pub(crate) fn derive_prefix(first_file: &str) -> String {
    let (scheme, rest) = match first_file.split_once("://") {
        Some((scheme, rest)) => (format!("{scheme}://"), rest),
        None => ("".to_string(), first_file),
    };
    let parent = std::path::Path::new(rest.trim_end_matches('/'))
        .parent()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();
    if scheme.is_empty() {
        parent
    } else {
        format!("{scheme}{parent}")
    }
}

/// Render a partition value as the string form used by the native reader.
pub(crate) fn scalar_to_string(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Utf8(Some(s)) => s.clone(),
        ScalarValue::Utf8View(Some(s)) => s.to_string(),
        ScalarValue::Int64(Some(v)) => v.to_string(),
        ScalarValue::Int32(Some(v)) => v.to_string(),
        ScalarValue::Date32(Some(v)) => v.to_string(),
        _ => value.to_string(),
    }
}

/// Counters the deferred payload read reports on the plan's metrics.
struct VectorSearchCounters {
    ranked: datafusion::physical_plan::metrics::Count,
    payload_rows: datafusion::physical_plan::metrics::Count,
}

impl VectorSearchCounters {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let builder = datafusion::physical_plan::metrics::MetricBuilder::new(metrics);
        Self {
            ranked: builder
                .clone()
                .counter("deferred_payload_ranked", partition),
            payload_rows: builder.counter("deferred_payload_rows", partition),
        }
    }
}

/// True for `FixedSizeList`/`List` columns of 32/64-bit floats, the vector
/// types the exact ranking can read.
fn is_float_list(data_type: &DataType) -> bool {
    match data_type {
        DataType::FixedSizeList(field, _) | DataType::List(field) => {
            matches!(field.data_type(), DataType::Float32 | DataType::Float64)
        }
        _ => false,
    }
}

/// Rank the phase-1 candidates by the exact distance the sort above computes.
///
/// The returned batch holds every candidate row in ranking order; the caller
/// keeps the rows the plan's limit may return.  Inner-product columns are
/// negated so that ascending order is the ranking for both metrics.
fn rank_by_exact_distance(
    request: &VectorSearchRequest,
    metric: &str,
    batches: Vec<arrow::record_batch::RecordBatch>,
) -> DFResult<Vec<arrow::record_batch::RecordBatch>> {
    let batches: Vec<_> = batches
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect();
    if batches.is_empty() {
        return Ok(Vec::new());
    }
    let combined = arrow::compute::concat_batches(&batches[0].schema(), &batches)?;
    let ascending = metric.eq_ignore_ascii_case("L2");

    let mut scores = vec![0.0f64; combined.num_rows()];
    for column in &request.columns {
        let index = combined
            .schema()
            .index_of(&column.column)
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "vector column '{}' missing from the vector search scan: {error}",
                    column.column
                ))
            })?;
        let query = column
            .query_csv
            .split(',')
            .map(|value| value.trim().parse::<f64>())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "invalid vector search query for '{}': {error}",
                    column.column
                ))
            })?;
        let weight = f64::from(column.weight);
        for (row, score) in scores.iter_mut().enumerate() {
            let distance = vector_values(combined.column(index), row)
                .map(|stored| {
                    if ascending {
                        euclidean_distance(&stored, &query)
                    } else {
                        -inner_product(&stored, &query)
                    }
                })
                .unwrap_or(f64::INFINITY);
            *score += weight * distance;
        }
    }

    let mut indices: Vec<u32> = (0..combined.num_rows() as u32).collect();
    indices.sort_by(|left, right| {
        scores[*left as usize].total_cmp(&scores[*right as usize])
    });
    let indices = arrow::array::UInt32Array::from(indices);
    Ok(vec![arrow::compute::take_record_batch(
        &combined, &indices,
    )?])
}

/// Read one row of a vector column as `f64`s.
fn vector_values(array: &arrow::array::ArrayRef, row: usize) -> Option<Vec<f64>> {
    use arrow::array::{Array, Float32Array, Float64Array, ListArray};

    let (values, start, len) = match array.data_type() {
        DataType::FixedSizeList(_, size) => {
            let list = array
                .as_any()
                .downcast_ref::<arrow::array::FixedSizeListArray>()?;
            if list.is_null(row) {
                return None;
            }
            let width = *size as usize;
            (list.values(), row * width, width)
        }
        DataType::List(_) => {
            let list = array.as_any().downcast_ref::<ListArray>()?;
            if list.is_null(row) {
                return None;
            }
            let offsets = list.value_offsets();
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            (list.values(), start, end - start)
        }
        _ => return None,
    };
    let mut vector = Vec::with_capacity(len);
    match values.data_type() {
        DataType::Float32 => {
            let values = values.as_any().downcast_ref::<Float32Array>()?;
            for index in start..start + len {
                vector.push(f64::from(values.value(index)));
            }
        }
        DataType::Float64 => {
            let values = values.as_any().downcast_ref::<Float64Array>()?;
            for index in start..start + len {
                vector.push(values.value(index));
            }
        }
        _ => return None,
    }
    Some(vector)
}

fn euclidean_distance(left: &[f64], right: &[f64]) -> f64 {
    if left.len() != right.len() {
        return f64::INFINITY;
    }
    left.iter()
        .zip(right)
        .map(|(left, right)| (left - right) * (left - right))
        .sum::<f64>()
        .sqrt()
}

fn inner_product(left: &[f64], right: &[f64]) -> f64 {
    left.iter().zip(right).map(|(l, r)| l * r).sum()
}

/// Keep the first `keep` rows of the ranked batches (always one batch).
fn take_first_rows(
    batches: &[arrow::record_batch::RecordBatch],
    keep: usize,
) -> DFResult<arrow::record_batch::RecordBatch> {
    let combined = arrow::compute::concat_batches(&batches[0].schema(), batches)?;
    if combined.num_rows() <= keep {
        Ok(combined)
    } else {
        Ok(combined.slice(0, keep))
    }
}

/// Reorder the phase-2 rows so they follow the ranking of phase 1.
fn order_payload_by_keys(
    ranked: &arrow::record_batch::RecordBatch,
    payload: Vec<arrow::record_batch::RecordBatch>,
    primary_keys: &[String],
) -> DFResult<Vec<arrow::record_batch::RecordBatch>> {
    use std::collections::HashMap;

    let payload: Vec<_> = payload
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect();
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    let layout = lakesoul_io::index::key::KeyLayout::from_schema(
        ranked.schema().as_ref(),
        primary_keys,
    )
    .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
    let codec = lakesoul_io::index::key::KeyCodec::new(layout)
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
    let ranked_keys = codec
        .encode_batch(ranked)
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;

    let combined = arrow::compute::concat_batches(&payload[0].schema(), &payload)?;
    let payload_keys = codec
        .encode_batch(&combined)
        .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
    let mut positions: HashMap<lakesoul_vector::IndexKey, usize> = HashMap::new();
    for (row, key) in payload_keys.into_iter().enumerate() {
        positions.insert(key, row);
    }

    let indices: Vec<u32> = ranked_keys
        .iter()
        .filter_map(|key| positions.get(key).copied())
        .map(|row| row as u32)
        .collect();
    if indices.len() != ranked_keys.len() {
        warn!(
            missing = ranked_keys.len() - indices.len(),
            "payload rows missing from the deferred vector search read"
        );
    }
    if indices.is_empty() {
        return Ok(Vec::new());
    }
    let indices = arrow::array::UInt32Array::from(indices);
    Ok(vec![arrow::compute::take_record_batch(
        &combined, &indices,
    )?])
}

/// Project a batch onto `schema`, matching columns by name.
fn project_batch(
    batch: &arrow::record_batch::RecordBatch,
    schema: &SchemaRef,
) -> DFResult<arrow::record_batch::RecordBatch> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let index = batch.schema().index_of(field.name())?;
        columns.push(Arc::clone(batch.column(index)));
    }
    Ok(arrow::record_batch::RecordBatch::try_new(
        Arc::clone(schema),
        columns,
    )?)
}
