// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Vector similarity search via rabitq-rs IVF+RaBitQ index.
//!
//! The catalog is resolved by the caller (which has metadata access); this
//! module searches an already-resolved [`ResolvedIndex`] and derives index
//! prefixes from data file paths through the shared index framework.

use std::collections::HashMap;
use std::sync::Arc;

use lakesoul_common::IndexKind;
use lakesoul_common::misc::SecretMap;
use lakesoul_vector::rabitq::math::{dot, l2_distance_sqr};
use lakesoul_vector::rabitq::segment::{IndexHeader, IndexStore, SegmentEntry};
use lakesoul_vector::{IndexKey, IvfRabitqIndex, Metric, RabitqError, SearchParams};
use object_store::ObjectStore;
use tracing::{info, warn};

use crate::Result as IoResult;
use crate::config::LakeSoulIOConfig;
use crate::index::cache::{self, IndexCacheEntry};
use crate::index::commit::ResolvedIndex;
use crate::index::key::{KeyCodec, KeyLayout};
use crate::index::reader::read_shard_batches;
use crate::vector::reader::extract_vector_batch;

use crate::index::options::{SearchRequest, option_key};
use crate::index::prefix::derive_index_prefixes;

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

/// One search hit: the stored index key and its score.
#[derive(Debug, Clone)]
pub struct VectorHit {
    pub key: IndexKey,
    /// Estimated distance (lower is closer for L2; see the metric).
    pub score: f32,
    /// Segment version the entry came from; the newest entry of a key wins
    /// when several delta segments contain it.
    pub version: u32,
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
    if header.key_kind != expected_key_kind {
        warn!(
            "vector index at '{}' was built with key kind {:#x} but the table \
             now uses {:#x}; scanning without the index",
            resolved.index_prefix, header.key_kind, expected_key_kind
        );
        return Ok(None);
    }
    let prefix = resolved.index_prefix.trim_end_matches('/').to_string();
    let cache_prefix = prefix.clone();
    let commit_id = resolved.commit_id;
    let segments: Vec<SegmentEntry> = resolved.segments_as()?;
    let entry =
        cache::get_or_load(store, IndexKind::Vector, &prefix, commit_id, move || {
            let store = store.clone();
            let prefix = cache_prefix.clone();
            let header = header.clone();
            let segments = segments.clone();
            async move {
                let istore = IndexStore::new(store, prefix);
                let index =
                    IvfRabitqIndex::load_from_segments(&istore, &header, &segments)
                        .await?;
                let bytes = index.memory_bytes();
                Ok::<CachedVectorIndex, RabitqError>(CachedVectorIndex {
                    commit_id,
                    index,
                    bytes,
                })
            }
        })
        .await
        .map_err(|e| {
            rootcause::report!(
                "failed to load vector index at '{}': {:?}",
                resolved.index_prefix,
                e
            )
        })?;
    search_loaded_index(
        &entry.index,
        &resolved.index_prefix,
        query,
        top_k,
        nprobe,
        parallelism,
    )
    .map(Some)
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
    let hits = search_resolved_shard(
        store,
        resolved,
        query,
        top_k,
        nprobe,
        parallelism,
        expected_key_kind,
    )
    .await?;
    let Some(mut hits) = hits else {
        return Ok(None);
    };

    // The index is built after each commit, so a commit whose build is
    // deferred or failed leaves newer files outside it.  A row in those files
    // has no index entry, and narrowing to the index's keys alone would
    // silently drop it: score them exactly and merge them in.
    let segments: Vec<SegmentEntry> = resolved.segments_as()?;
    let covered: Vec<&str> = segments
        .iter()
        .flat_map(|segment| segment.data_files.iter())
        .map(String::as_str)
        .collect();
    let pending = lakesoul_common::index::pending_files(file_paths, &covered);
    if !pending.is_empty() {
        let header = IndexHeader::deserialize(&resolved.header).map_err(|error| {
            rootcause::report!(
                "unreadable vector index header at '{}': {error}",
                resolved.index_prefix
            )
        })?;
        let Some(tail) = uncovered_file_candidates(
            &pending,
            primary_keys,
            vector_column,
            query,
            header.dim,
            header.metric,
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
    let results = index.search(query, params).map_err(|e| {
        rootcause::report!("vector search failed at '{}': {:?}", prefix, e)
    })?;
    let hits: Vec<VectorHit> = results
        .into_iter()
        .map(|result| VectorHit {
            key: result.id,
            score: result.score,
            version: result.version,
        })
        .collect();
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
