// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Vector similarity search via rabitq-rs IVF+RaBitQ index.
//!
//! The catalog is resolved by the caller (which has metadata access); this
//! module searches an already-resolved [`ResolvedIndex`] and derives index
//! prefixes from data file paths through the shared index framework.

use std::sync::Arc;

use lakesoul_common::IndexKind;
use lakesoul_vector::rabitq::segment::{IndexHeader, IndexStore, SegmentEntry};
use lakesoul_vector::{IvfRabitqIndex, Metric, RabitqError, SearchParams};
use object_store::ObjectStore;
use tracing::info;

use crate::Result as IoResult;
use crate::config::LakeSoulIOConfig;
use crate::index::Candidate;
use crate::index::cache::{self, IndexCacheEntry};
use crate::index::commit::ResolvedIndex;
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

/// Search one resolved shard (base + deltas of its current commit).
pub async fn search_resolved_shard(
    store: &Arc<dyn ObjectStore>,
    resolved: &ResolvedIndex,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
) -> IoResult<Vec<Candidate>> {
    if !resolved.is_kind(IndexKind::Vector) {
        return Err(rootcause::report!(
            "resolved index '{}' is not a vector index",
            resolved.index_prefix
        ));
    }
    let prefix = resolved.index_prefix.trim_end_matches('/').to_string();
    let cache_prefix = prefix.clone();
    let commit_id = resolved.commit_id;
    let header = resolved.header.clone();
    let segments: Vec<SegmentEntry> = resolved.segments_as()?;
    let entry =
        cache::get_or_load(store, IndexKind::Vector, &prefix, commit_id, move || {
            let store = store.clone();
            let prefix = cache_prefix.clone();
            let header = header.clone();
            let segments = segments.clone();
            async move {
                let istore = IndexStore::new(store, prefix);
                let header = IndexHeader::deserialize(&header)?;
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
    search_loaded_index(&entry.index, &resolved.index_prefix, query, top_k, nprobe)
}

/// Search the vector index matching a single bucket's files.
///
/// One LakeSoulReader processes files from exactly one hash bucket, so the
/// first derived prefix selects the shard to search.  `resolved_shards`
/// carries the commits resolved by the caller; a file group whose index was
/// not resolved yields an error instead of silently scanning.
pub async fn search_matching_shards(
    store: &Arc<dyn ObjectStore>,
    file_paths: &[String],
    vector_column: &str,
    prefix: &str,
    range_partitions: &[String],
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    metric: Metric,
    resolved_shards: &[ResolvedIndex],
) -> IoResult<Vec<Candidate>> {
    let _ = (range_partitions, metric);
    let prefixes =
        derive_index_prefixes(file_paths, prefix, IndexKind::Vector, vector_column);
    let Some((index_prefix, _bucket_id)) = prefixes.first() else {
        return Ok(Vec::new());
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
    search_resolved_shard(store, resolved, query, top_k, nprobe).await
}

/// Run the vector search selected by a parsed [`SearchRequest`].
pub async fn search_request(
    config: &LakeSoulIOConfig,
    store: &Arc<dyn ObjectStore>,
    request: &SearchRequest,
    table_prefix: &str,
) -> IoResult<Vec<Candidate>> {
    let nprobe: usize = config
        .option(&option_key(request.kind, "nprobe"))
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);
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
        metric,
        config.resolved_index_shards_slice(),
    )
    .await
}

fn search_loaded_index(
    index: &IvfRabitqIndex,
    prefix: &str,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
) -> IoResult<Vec<Candidate>> {
    let params = SearchParams::new(top_k, nprobe);
    let results = index.search(query, params).map_err(|e| {
        rootcause::report!("vector search failed at '{}': {:?}", prefix, e)
    })?;
    let candidates: Vec<Candidate> = results
        .into_iter()
        .map(|result| Candidate::scored(result.id, result.score))
        .collect();
    info!(
        "Vector search at '{}': {} results (nprobe={})",
        prefix,
        candidates.len(),
        nprobe
    );
    Ok(candidates)
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
