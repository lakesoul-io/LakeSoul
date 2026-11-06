// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Vector similarity search via rabitq-rs IVF+RaBitQ index.
//!
//! The catalog is resolved by the caller (which has metadata access); this
//! module searches an already-resolved [`ResolvedIndex`] and derives index
//! prefixes from data file paths through the shared index framework.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use lakesoul_common::IndexKind;
use lakesoul_common::misc::SecretMap;
use lakesoul_vector::rabitq::math::{dot, l2_distance_sqr};
use lakesoul_vector::rabitq::mutation::load_state;
use lakesoul_vector::rabitq::segment::{IndexHeader, IndexStore, SegmentEntry};
use lakesoul_vector::{IndexKey, IvfRabitqIndex, Metric, RabitqError, SearchParams};
use object_store::ObjectStore;
use tracing::{debug, info, warn};

use crate::Result as IoResult;
use crate::config::LakeSoulIOConfig;
use crate::index::cache::{self, IndexCacheEntry};
use crate::index::commit::ResolvedIndex;
use crate::index::key::{KeyCodec, KeyLayout};
use crate::index::reader::read_shard_batches;
use crate::vector::reader::extract_vector_batch;

use crate::index::options::{SearchRequest, option_key};
use crate::index::prefix::derive_index_prefixes;

/// Why a shard could not be loaded from its commit.
#[derive(Debug)]
enum ShardLoadError {
    Mutation(RabitqError),
    Index(RabitqError),
}

/// A loaded vector index shard plus the commit it was loaded from.
pub struct CachedVectorIndex {
    pub commit_id: i64,
    pub index: IvfRabitqIndex,
    pub bytes: usize,
}

impl IndexCacheEntry for CachedVectorIndex {
    fn commit_id(&self) -> i64 {
        self.commit_id
    }

    fn memory_bytes(&self) -> usize {
        self.bytes
    }
}

/// Query-independent coverage of one immutable vector commit.  Match the
/// basename/unknown-coverage contract in `lakesoul_common::index::pending_files`.
#[derive(Default)]
struct CoveredFiles {
    names: HashSet<String>,
}

impl CoveredFiles {
    fn from_segments(segments: &[SegmentEntry]) -> Self {
        Self {
            names: segments
                .iter()
                .flat_map(|segment| &segment.data_files)
                .map(|file| file.rsplit('/').next().unwrap_or(file).to_owned())
                .collect(),
        }
    }

    fn pending(&self, active: &[String]) -> Vec<String> {
        if self.names.is_empty() {
            // Legacy segments have unknown coverage, not empty coverage.
            return Vec::new();
        }
        active
            .iter()
            .filter(|file| !self.names.contains(file.rsplit('/').next().unwrap_or(file)))
            .cloned()
            .collect()
    }

    fn memory_bytes(&self) -> usize {
        self.names.capacity() * (std::mem::size_of::<String>() + 1)
            + self.names.iter().map(String::capacity).sum::<usize>()
    }
}

// Preserve the public CachedVectorIndex shape; coverage belongs to this
// private, vector-only cache entry and shares its exact commit lifetime.
struct LoadedVectorShard {
    cached: CachedVectorIndex,
    covered: CoveredFiles,
    dim: usize,
    metric: Metric,
    /// Key layout the shard was built with; a cache hit must not serve a
    /// table whose layout changed.
    key_kind: u64,
}

impl IndexCacheEntry for LoadedVectorShard {
    fn commit_id(&self) -> i64 {
        self.cached.commit_id
    }

    fn memory_bytes(&self) -> usize {
        self.cached.bytes + self.covered.memory_bytes() + std::mem::size_of::<Self>()
    }
}

/// One search hit: the stored index key and its score.
#[derive(Debug, Clone)]
pub struct VectorHit {
    pub key: IndexKey,
    /// Estimated distance (lower is closer for L2; see the metric).
    pub score: f32,
    /// Segment version the entry came from; the newest entry of a key wins
    /// when several delta segments contain it.
    pub version: u32,
    /// Catalog revision the entry's segment was published at.  Unlike
    /// [`version`](Self::version) this is comparable across clusters, so it
    /// ranks duplicate keys from different clusters.
    pub birth_version: u64,
}

/// Search one resolved shard (base + deltas of its current commit).
///
/// Returns `Ok(None)` when the shard's stored key layout does not match
/// `expected_key_kind` (or its header cannot be read), i.e. the search
/// results could not be turned into row lookups; the caller then scans
/// normally instead of narrowing.
#[allow(clippy::too_many_arguments)]
pub async fn search_resolved_shard(
    store: &Arc<dyn ObjectStore>,
    resolved: &ResolvedIndex,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    parallelism: usize,
    expected_key_kind: u64,
) -> IoResult<Option<Vec<VectorHit>>> {
    let Some(entry) = load_resolved_shard(store, resolved, expected_key_kind).await?
    else {
        return Ok(None);
    };
    search_loaded_index(
        &entry.cached.index,
        &resolved.index_prefix,
        query,
        top_k,
        nprobe,
        parallelism,
    )
    .map(Some)
}

/// Search one resolved shard for several query vectors at once.
///
/// Same contract as [`search_resolved_shard`], except that the shard is
/// loaded (or taken from the index cache) once and every query is searched
/// against that one index.  A batch therefore pays the per-shard setup once
/// instead of once per query, which is where the DataFusion scan spends most
/// of its CPU for a single query.
#[allow(clippy::too_many_arguments)]
pub async fn search_resolved_shard_batch(
    store: &Arc<dyn ObjectStore>,
    resolved: &ResolvedIndex,
    queries: &[Vec<f32>],
    top_k: usize,
    nprobe: usize,
    parallelism: usize,
    expected_key_kind: u64,
) -> IoResult<Option<Vec<Vec<VectorHit>>>> {
    let Some(entry) = load_resolved_shard(store, resolved, expected_key_kind).await?
    else {
        return Ok(None);
    };
    if queries.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let params = SearchParams::new(top_k, nprobe).with_parallelism(parallelism);
    let refs: Vec<&[f32]> = queries.iter().map(Vec::as_slice).collect();
    let mut per_query = Vec::with_capacity(queries.len());
    for result in entry.cached.index.batch_search(&refs, params) {
        let results = result.map_err(|e| {
            rootcause::report!(
                "vector search failed at '{}': {:?}",
                resolved.index_prefix,
                e
            )
        })?;
        per_query.push(results.into_iter().map(vector_hit).collect::<Vec<_>>());
    }
    info!(
        "Vector batch search at '{}': {} queries (nprobe={})",
        resolved.index_prefix,
        queries.len(),
        nprobe
    );
    Ok(Some(per_query))
}

fn vector_hit(result: lakesoul_vector::SearchResult) -> VectorHit {
    VectorHit {
        key: result.id,
        score: result.score,
        version: result.version,
        birth_version: result.birth_version,
    }
}

async fn load_resolved_shard(
    store: &Arc<dyn ObjectStore>,
    resolved: &ResolvedIndex,
    expected_key_kind: u64,
) -> IoResult<Option<Arc<LoadedVectorShard>>> {
    if !resolved.is_kind(IndexKind::Vector) {
        return Err(rootcause::report!(
            "resolved index '{}' is not a vector index",
            resolved.index_prefix
        ));
    }
    let header = match IndexHeader::deserialize(&resolved.header) {
        Ok(header) => header,
        Err(e) => {
            warn!(
                "vector index at '{}' has an unreadable header ({}); scanning \
                 without the index",
                resolved.index_prefix, e
            );
            return Ok(None);
        }
    };
    let segments: Vec<SegmentEntry> = match resolved.segments_as() {
        Ok(segments) => segments,
        Err(error) => {
            return Err(rootcause::report!(
                "invalid vector index segment list at '{}': {error}",
                resolved.index_prefix
            ));
        }
    };
    load_shard(
        store,
        &resolved.index_prefix,
        resolved.commit_id,
        resolved.version,
        &header,
        &segments,
        resolved.mutation_head.as_deref(),
        resolved.mutation_records,
        expected_key_kind,
    )
    .await
}

/// Load (or take from the cache) one shard described by its typed commit
/// pieces.
///
/// [`load_resolved_shard`] needs the serialized transport form, which the
/// catalog layer can hand over as JSON.  A caller that already holds the
/// parsed segments (the DataFusion layer keeps the typed catalog view) calls
/// this instead and skips serializing a thousand segment entries per query.
#[allow(clippy::too_many_arguments)]
async fn load_shard(
    store: &Arc<dyn ObjectStore>,
    index_prefix: &str,
    commit_id: i64,
    revision: u64,
    header: &IndexHeader,
    segments: &[SegmentEntry],
    mutation_head: Option<&str>,
    mutation_records: u32,
    expected_key_kind: u64,
) -> IoResult<Option<Arc<LoadedVectorShard>>> {
    if header.key_kind != expected_key_kind {
        warn!(
            "vector index at '{}' was built with key kind {:#x} but the table \
             now uses {:#x}; scanning without the index",
            index_prefix, header.key_kind, expected_key_kind
        );
        return Ok(None);
    }
    let prefix = index_prefix.trim_end_matches('/').to_string();
    let error_prefix = index_prefix.to_string();
    let entry = cache::get_or_load(IndexKind::Vector, &prefix, commit_id, || {
        // Copied only on a cache miss: a hit must not rebuild coverage or
        // clone the segment list.
        let store = store.clone();
        let prefix = prefix.clone();
        let header = header.clone();
        let segments = segments.to_vec();
        let mutation_head = mutation_head.map(str::to_owned);
        async move {
            let covered = CoveredFiles::from_segments(&segments);
            let istore = IndexStore::new(store, prefix);
            let mutation = match mutation_head.as_deref() {
                Some(head) => Some(
                    load_state(&istore, Some(head), mutation_records, revision)
                        .await
                        .map_err(ShardLoadError::Mutation)?,
                ),
                None => None,
            };
            let index = IvfRabitqIndex::load_from_segments_with_mutations(
                &istore,
                &header,
                &segments,
                mutation.as_ref(),
            )
            .await
            .map_err(ShardLoadError::Index)?;
            let bytes = index.memory_bytes();
            Ok::<LoadedVectorShard, ShardLoadError>(LoadedVectorShard {
                cached: CachedVectorIndex {
                    commit_id,
                    index,
                    bytes,
                },
                covered,
                dim: header.dim,
                metric: header.metric,
                key_kind: header.key_kind,
            })
        }
    })
    .await;
    let entry = match entry {
        Ok(entry) => entry,
        Err(error) => {
            return match error.as_ref() {
                ShardLoadError::Mutation(error) => {
                    // Without the mutation chain the masks cannot hide
                    // logically deleted keys, so do not silently serve an
                    // index that still contains them.
                    warn!(
                        "vector index at '{}' has an unreadable mutation \
                         chain ({}); scanning without the index",
                        error_prefix, error
                    );
                    Ok(None)
                }
                ShardLoadError::Index(error) => Err(rootcause::report!(
                    "failed to load vector index at '{}': {:?}",
                    error_prefix,
                    error
                )),
            };
        }
    };
    Ok(Some(entry))
}

/// Search one shard from its typed catalog pieces.
///
/// Same contract as [`search_resolved_shard`], except that the caller passes
/// the parsed header and segment list instead of the serialized transport
/// form.  The DataFusion layer keeps the typed view in memory, so a
/// single-query scan no longer serializes the whole segment list (one entry
/// per cluster, i.e. thousands per shard) and parses it back.
#[allow(clippy::too_many_arguments)]
pub async fn search_typed_shard(
    store: &Arc<dyn ObjectStore>,
    index_prefix: &str,
    commit_id: i64,
    version: u64,
    header: &[u8],
    segments: &[SegmentEntry],
    mutation_head: Option<String>,
    mutation_records: u32,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    parallelism: usize,
    expected_key_kind: u64,
) -> IoResult<Option<Vec<VectorHit>>> {
    let header = match IndexHeader::deserialize(header) {
        Ok(header) => header,
        Err(e) => {
            warn!(
                "vector index at '{}' has an unreadable header ({}); scanning \
                 without the index",
                index_prefix, e
            );
            return Ok(None);
        }
    };
    let Some(entry) = load_shard(
        store,
        index_prefix,
        commit_id,
        version,
        &header,
        segments,
        mutation_head.as_deref(),
        mutation_records,
        expected_key_kind,
    )
    .await?
    else {
        return Ok(None);
    };
    let hits = search_loaded_index(
        &entry.cached.index,
        index_prefix,
        query,
        top_k,
        nprobe,
        parallelism,
    )?;
    Ok(Some(hits))
}

/// A loaded shard that any thread can search.
///
/// The handle wraps the cached index; [`Self::search_batch`] is synchronous so
/// a caller can probe several shards from its own threads (a batch entry that
/// wants every bucket probed concurrently) without moving async futures
/// between threads.
#[derive(Clone)]
pub struct LoadedShard {
    inner: Arc<LoadedVectorShard>,
}

impl LoadedShard {
    /// Vector dimension the shard was built for.
    pub fn dim(&self) -> usize {
        self.inner.dim
    }

    /// Search several queries against this shard (rayon parallel inside).
    pub fn search_batch(
        &self,
        queries: &[&[f32]],
        top_k: usize,
        nprobe: usize,
        parallelism: usize,
    ) -> IoResult<Vec<Vec<VectorHit>>> {
        let params = SearchParams::new(top_k, nprobe).with_parallelism(parallelism);
        let mut per_query = Vec::with_capacity(queries.len());
        for result in self.inner.cached.index.batch_search(queries, params) {
            let results = result
                .map_err(|e| rootcause::report!("vector search failed: {:?}", e))?;
            per_query.push(results.into_iter().map(vector_hit).collect::<Vec<_>>());
        }
        Ok(per_query)
    }
}

/// Take an already-loaded shard for `(index_prefix, commit_id)`.
///
/// Returns `None` when the shard is not cached (the caller then loads it with
/// [`load_typed_shard`]) or when the cached entry was built for another key
/// layout.  A hot query therefore skips the header parse and the segment list
/// entirely.
pub async fn cached_typed_shard(
    index_prefix: &str,
    commit_id: i64,
    expected_key_kind: u64,
) -> Option<LoadedShard> {
    let prefix = index_prefix.trim_end_matches('/');
    let entry =
        cache::get_loaded::<LoadedVectorShard>(IndexKind::Vector, prefix, commit_id)
            .await?;
    if entry.key_kind != expected_key_kind {
        return None;
    }
    Some(LoadedShard { inner: entry })
}

/// Load one shard from its typed catalog pieces.
///
/// The caller (the DataFusion layer) already holds the parsed header and
/// segment list from the catalog view; handing them over avoids serializing a
/// thousand segment entries per query and parsing them back.  A cache hit only
/// resolves the cache key, so the handle is cheap to obtain before a batch.
#[allow(clippy::too_many_arguments)]
pub async fn load_typed_shard(
    store: &Arc<dyn ObjectStore>,
    index_prefix: &str,
    commit_id: i64,
    version: u64,
    header: &[u8],
    segments: &[SegmentEntry],
    mutation_head: Option<&str>,
    mutation_records: u32,
    expected_key_kind: u64,
) -> IoResult<Option<LoadedShard>> {
    let header = match IndexHeader::deserialize(header) {
        Ok(header) => header,
        Err(e) => {
            warn!(
                "vector index at '{}' has an unreadable header ({}); scanning \
                 without the index",
                index_prefix, e
            );
            return Ok(None);
        }
    };
    let Some(inner) = load_shard(
        store,
        index_prefix,
        commit_id,
        version,
        &header,
        segments,
        mutation_head,
        mutation_records,
        expected_key_kind,
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(LoadedShard { inner }))
}

/// Search the vector index matching a single bucket's files.
///
/// One LakeSoulReader processes files from exactly one hash bucket, so the
/// first derived prefix selects the shard to search.  `resolved_shards`
/// carries the commits resolved by the caller; a file group whose index was
/// not resolved yields an error instead of silently scanning.
#[allow(clippy::too_many_arguments)]
pub async fn search_matching_shards(
    store: &Arc<dyn ObjectStore>,
    file_paths: &[String],
    vector_column: &str,
    prefix: &str,
    range_partitions: &[String],
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    parallelism: usize,
    metric: Metric,
    resolved_shards: &[ResolvedIndex],
    expected_key_kind: u64,
    primary_keys: &[String],
    object_store_options: &SecretMap,
) -> IoResult<Option<Vec<VectorHit>>> {
    // The index header is the authoritative metric (the caller's `metric` only
    // selects the shard).
    let _ = (range_partitions, metric);
    let prefixes =
        derive_index_prefixes(file_paths, prefix, IndexKind::Vector, vector_column);
    let Some((index_prefix, _bucket_id)) = prefixes.first() else {
        return Ok(None);
    };
    let normalized = index_prefix.trim_end_matches('/');
    let Some(resolved) = resolved_shards.iter().find(|shard| {
        shard.is_kind(IndexKind::Vector)
            && shard.index_prefix.trim_end_matches('/') == normalized
    }) else {
        return Err(rootcause::report!(
            "vector index at '{}' was not resolved by the caller; rebuild the index \
             if it was created by an unsupported (legacy) writer",
            normalized
        ));
    };
    let t_load = std::time::Instant::now();
    let load_profile = std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok();
    let Some(entry) = load_resolved_shard(store, resolved, expected_key_kind).await?
    else {
        return Ok(None);
    };
    if load_profile {
        debug!(
            load_ms = t_load.elapsed().as_secs_f64() * 1000.0,
            "vector shard load profile"
        );
    }
    let mut hits = search_loaded_index(
        &entry.cached.index,
        &resolved.index_prefix,
        query,
        top_k,
        nprobe,
        parallelism,
    )?;

    // The index is built after each commit, so a commit whose build is
    // deferred or failed leaves newer files outside it.  A row in those files
    // has no index entry, and narrowing to the index's keys alone would
    // silently drop it: score them exactly and merge them in.
    // Coverage is immutable with the pinned index commit, but the active
    // files are request-specific and must be checked even on a cache hit.
    let pending = entry.covered.pending(file_paths);
    if !pending.is_empty() {
        let Some(tail) = uncovered_file_candidates(
            &pending,
            primary_keys,
            vector_column,
            query,
            entry.dim,
            entry.metric,
            top_k,
            object_store_options,
            expected_key_kind,
        )
        .await?
        else {
            return Ok(None);
        };
        info!(
            "Vector search at '{}': {} file(s) are not covered by the index; \
             scored them exactly",
            resolved.index_prefix,
            pending.len()
        );
        hits.extend(tail);
    }
    Ok(Some(hits))
}

/// The subset of `files` that carries `column` in its own schema.
///
/// A commit may write a file without the indexed column (a sparse update);
/// such a file has no vectors to index or score, and the index build skips it
/// the same way.  Inference is metadata-only.
async fn files_with_column(
    files: &[String],
    column: &str,
    object_store_options: &HashMap<String, String>,
) -> Vec<String> {
    let mut with_column = Vec::new();
    for file in files {
        let config = crate::index::reader::shard_reader_config_builder(
            std::slice::from_ref(file),
            &[],
            object_store_options,
            None,
        )
        .set_inferring_schema(true)
        .build();
        let Ok(session) = crate::session::LakeSoulIOSession::try_new(config) else {
            debug!("skipping '{}': cannot open it to infer its schema", file);
            continue;
        };
        match session.get_table_schema().await {
            Ok(schema) if schema.file_schema().field_with_name(column).is_ok() => {
                with_column.push(file.clone());
            }
            Ok(_) => {
                debug!("skipping '{}': it does not carry '{}'", file, column);
            }
            Err(error) => {
                warn!("skipping '{}': schema inference failed: {}", file, error);
            }
        }
    }
    with_column
}

/// Exact candidates from the files an index commit does not cover.
///
/// The rows are scored with the index's own conventions (squared L2, or the
/// negated inner product: lower is closer), so the caller can merge them with
/// the index's hits and re-rank the union.
#[allow(clippy::too_many_arguments)]
async fn uncovered_file_candidates(
    files: &[String],
    primary_keys: &[String],
    vector_column: &str,
    query: &[f32],
    dim: usize,
    metric: Metric,
    top_k: usize,
    object_store_options: &SecretMap,
    expected_key_kind: u64,
) -> IoResult<Option<Vec<VectorHit>>> {
    let projection = vec![vector_column.to_string()];
    let options: HashMap<String, String> = object_store_options.clone().into();
    // A sparse write (a commit that does not carry the vector column) has no
    // vectors to score and must not be read: the projection would fail on it.
    let files = files_with_column(files, vector_column, &options).await;
    if files.is_empty() {
        return Ok(Some(Vec::new()));
    }
    // Boxed to break the async call graph cycle: `read_shard_batches` drives a
    // `LakeSoulReader`, which itself runs a vector search when its options
    // select one (never for the plain read configured here, but the compiler
    // sees the cycle).
    let batches = Box::pin(read_shard_batches(
        &files,
        primary_keys,
        &projection,
        &options,
        None,
    ))
    .await?;
    let Some(schema) = batches.first().map(|batch| batch.schema()) else {
        return Ok(Some(Vec::new()));
    };
    let codec = KeyCodec::new(KeyLayout::from_schema(schema.as_ref(), primary_keys)?)?;
    if codec.kind_hash() != expected_key_kind {
        warn!(
            "uncovered files encode keys as {:#x} but the table uses {:#x}; \
             scanning without index narrowing",
            codec.kind_hash(),
            expected_key_kind
        );
        return Ok(None);
    }

    let mut hits: Vec<VectorHit> = Vec::new();
    for batch in &batches {
        let extracted = extract_vector_batch(batch, &codec, vector_column, dim)?;
        for (index, key) in extracted.ids.iter().enumerate() {
            let stored = &extracted.vectors[index * dim..(index + 1) * dim];
            let score = match metric {
                Metric::L2 => l2_distance_sqr(query, stored),
                Metric::InnerProduct => -dot(query, stored),
            };
            hits.push(VectorHit {
                key: key.clone(),
                score,
                // The caller keeps the newest entry per key, and these rows
                // come from files newer than the whole index commit.
                version: u32::MAX,
                birth_version: u64::MAX,
            });
        }
    }
    // The same per-bucket budget the index search uses.
    hits.sort_by(|left, right| left.score.total_cmp(&right.score));
    hits.truncate(top_k);
    Ok(Some(hits))
}

/// Run the vector search selected by a parsed [`SearchRequest`].
pub async fn search_request(
    config: &LakeSoulIOConfig,
    store: &Arc<dyn ObjectStore>,
    request: &SearchRequest,
    table_prefix: &str,
    expected_key_kind: u64,
) -> IoResult<Option<Vec<VectorHit>>> {
    let nprobe: usize = config
        .option(&option_key(request.kind, "nprobe"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
    let parallelism: usize = config
        .option(&option_key(request.kind, "parallelism"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let metric = match config
        .option(&option_key(request.kind, "metric"))
        .map(|s| s.to_uppercase())
        .unwrap_or_else(|| "L2".to_string())
        .as_str()
    {
        "IP" | "INNERPRODUCT" => Metric::InnerProduct,
        _ => Metric::L2,
    };
    let query = parse_query_vector(&request.query, None)?;
    search_matching_shards(
        store,
        config.files_slice(),
        &request.column,
        table_prefix,
        config.range_partitions_slice(),
        &query,
        request.top_k,
        nprobe,
        parallelism,
        metric,
        config.resolved_index_shards_slice(),
        expected_key_kind,
        config.primary_keys_slice(),
        config.object_store_options(),
    )
    .await
}

fn search_loaded_index(
    index: &IvfRabitqIndex,
    prefix: &str,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    parallelism: usize,
) -> IoResult<Vec<VectorHit>> {
    let params = SearchParams::new(top_k, nprobe).with_parallelism(parallelism);
    let profile = std::env::var("LAKESOUL_VECTOR_SEARCH_PROFILE").is_ok();
    let (results, diagnostics) = if profile {
        index.search_with_diagnostics(query, params).map_err(|e| {
            rootcause::report!("vector search failed at '{}': {:?}", prefix, e)
        })?
    } else {
        (
            index.search(query, params).map_err(|e| {
                rootcause::report!("vector search failed at '{}': {:?}", prefix, e)
            })?,
            Default::default(),
        )
    };
    if profile {
        debug!(
            vectors = diagnostics.vectors_scanned,
            extended = diagnostics.extended_evaluations,
            fastscan_ms = diagnostics.fastscan_ns as f64 / 1e6,
            scalar_ms = diagnostics.scalar_ns as f64 / 1e6,
            hits = results.len(),
            "vector ann profile"
        );
    }
    let hits: Vec<VectorHit> = results.into_iter().map(vector_hit).collect();
    info!(
        "Vector search at '{}': {} results (nprobe={})",
        prefix,
        hits.len(),
        nprobe
    );
    Ok(hits)
}

pub fn parse_query_vector(s: &str, expected_dim: Option<usize>) -> IoResult<Vec<f32>> {
    let vec: Vec<f32> = s
        .split(',')
        .map(|p| p.trim().parse::<f32>())
        .collect::<Result<_, _>>()
        .map_err(|e| rootcause::report!("invalid vector search query: {}", e))?;
    if let Some(dim) = expected_dim
        && vec.len() != dim
    {
        return Err(rootcause::report!(
            "query vector dimension mismatch: expected {}, got {}",
            dim,
            vec.len()
        ));
    }
    Ok(vec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakesoul_vector::{IdAndVecBatch, IvfRabitqBuilder, RotatorType};
    use object_store::memory::InMemory;

    fn segment(files: &[&str]) -> SegmentEntry {
        SegmentEntry {
            cluster_id: 0,
            segment_version: 0,
            segment_filename: "cluster.seg".into(),
            num_vectors: 1,
            file_size: 1,
            data_files: files.iter().map(|file| (*file).to_owned()).collect(),
            birth_version: 1,
        }
    }

    #[test]
    fn cached_coverage_preserves_pending_file_contract() {
        let segments = vec![
            segment(&["s3://bucket/old/a.vortex", "file:///old/b.vortex"]),
            segment(&["a.vortex"]),
        ];
        let active = [
            "file:///new/b.vortex",
            "new/c.vortex",
            "a.vortex",
            "new/c.vortex",
        ]
        .map(str::to_owned);
        let covered = CoveredFiles::from_segments(&segments);
        let references: Vec<_> = segments
            .iter()
            .flat_map(|entry| entry.data_files.iter().map(String::as_str))
            .collect();
        assert_eq!(covered.names.len(), 2);
        assert_eq!(
            covered.pending(&active),
            lakesoul_common::index::pending_files(&active, &references)
        );
        assert_eq!(
            covered.pending(&active),
            vec!["new/c.vortex", "new/c.vortex"]
        );
        assert!(CoveredFiles::default().pending(&active).is_empty());
        assert!(covered.memory_bytes() > 0);
    }

    async fn fixture() -> (Arc<dyn ObjectStore>, ResolvedIndex) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = format!("vector-cache-test/{}", uuid::Uuid::new_v4());
        let batch = IdAndVecBatch {
            ids: (0u64..64)
                .map(|id| IndexKey::new(id.to_le_bytes()))
                .collect(),
            vectors: (0..64)
                .flat_map(|id| [id as f32, (id % 7) as f32])
                .collect(),
        };
        let mut builder = IvfRabitqBuilder::new(
            2,
            2,
            7,
            Metric::L2,
            RotatorType::FhtKacRotator,
            42,
            false,
            7,
        );
        builder.insert_batch(batch.clone()).unwrap();
        let index = builder
            .build(|| futures::stream::iter(vec![batch.clone()]))
            .await
            .unwrap();
        let istore = IndexStore::new(store.clone(), prefix.clone());
        let (header, mut segments) = index.write_base_segments(&istore).await.unwrap();
        for entry in &mut segments {
            entry.birth_version = 1;
            entry.data_files = vec!["base.vortex".into()];
        }
        let resolved = ResolvedIndex {
            kind: IndexKind::Vector,
            index_prefix: prefix,
            commit_id: 1,
            generation: 1,
            version: 1,
            header: header.serialize(),
            segments: serde_json::to_value(segments).unwrap(),
            mutation_head: None,
            mutation_records: 0,
        };
        (store, resolved)
    }

    #[tokio::test]
    async fn loaded_entry_reuses_coverage_but_checks_current_active_files() {
        let (store, resolved) = fixture().await;
        let first = load_resolved_shard(&store, &resolved, 7)
            .await
            .unwrap()
            .unwrap();
        let second = load_resolved_shard(&store, &resolved, 7)
            .await
            .unwrap()
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(first.covered.pending(&["base.vortex".into()]).is_empty());
        assert_eq!(
            second
                .covered
                .pending(&["base.vortex".into(), "new.vortex".into()]),
            vec!["new.vortex"]
        );
        assert!(
            load_resolved_shard(&store, &resolved, 8)
                .await
                .unwrap()
                .is_none()
        );

        let mut next = resolved.clone();
        next.commit_id += 1;
        next.version += 1;
        let mut segments: Vec<SegmentEntry> = next.segments_as().unwrap();
        for entry in &mut segments {
            entry.data_files.push("new.vortex".into());
        }
        next.segments = serde_json::to_value(segments).unwrap();
        let updated = load_resolved_shard(&store, &next, 7)
            .await
            .unwrap()
            .unwrap();
        assert!(!Arc::ptr_eq(&first, &updated));
        assert!(updated.covered.pending(&["new.vortex".into()]).is_empty());
        assert_eq!(
            first.covered.pending(&["new.vortex".into()]),
            vec!["new.vortex"]
        );
    }

    #[tokio::test]
    async fn corrupt_mutation_never_reuses_the_previous_commit() {
        let (store, mut resolved) = fixture().await;
        let loaded = load_resolved_shard(&store, &resolved, 7)
            .await
            .unwrap()
            .unwrap();
        resolved.commit_id += 1;
        resolved.version += 1;
        resolved.mutation_head = Some("missing.delta".into());
        resolved.mutation_records = 1;
        assert!(
            load_resolved_shard(&store, &resolved, 7)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !cache::is_loaded(
                IndexKind::Vector,
                &resolved.index_prefix,
                resolved.commit_id
            )
            .await
        );
        assert_eq!(loaded.cached.commit_id, 1);
    }

    #[tokio::test]
    async fn batch_search_matches_single_query_results() {
        let (store, resolved) = fixture().await;
        let queries: Vec<Vec<f32>> = vec![
            vec![0.0, 0.0],
            vec![63.0, 0.0],
            vec![12.5, 3.5],
            vec![-4.0, 6.0],
        ];
        let batch = search_resolved_shard_batch(&store, &resolved, &queries, 5, 2, 1, 7)
            .await
            .unwrap()
            .expect("batch search");
        assert_eq!(batch.len(), queries.len());
        let keyed =
            |hits: &[VectorHit]| -> Vec<(lakesoul_vector::IndexKey, u32, u32, u64)> {
                hits.iter()
                    .map(|hit| {
                        (
                            hit.key.clone(),
                            hit.score.to_bits(),
                            hit.version,
                            hit.birth_version,
                        )
                    })
                    .collect()
            };
        for (query, batched) in queries.iter().zip(&batch) {
            let single = search_resolved_shard(&store, &resolved, query, 5, 2, 1, 7)
                .await
                .unwrap()
                .expect("single search");
            assert_eq!(
                keyed(batched),
                keyed(&single),
                "batched and single query disagree for {query:?}"
            );
        }
        // A shard whose key layout does not match stays a caller-visible `None`.
        assert!(
            search_resolved_shard_batch(&store, &resolved, &queries, 5, 2, 1, 8)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn malformed_segment_transport_remains_an_error_on_load() {
        let (store, mut resolved) = fixture().await;
        resolved.segments = serde_json::json!({"invalid": "segment list"});
        assert!(load_resolved_shard(&store, &resolved, 7).await.is_err());
    }
}
