// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Text index search: load a shard's splits and merge their BM25 top-k.

use std::sync::Arc;

use lakesoul_common::IndexKind;
use lakesoul_text::tantivy::Index;
use lakesoul_text::{
    SplitCache, TextError, TextHit, TextSplitEntry, merge_hits, search_index,
};
use object_store::ObjectStore;

use crate::Result as IoResult;
use crate::config::LakeSoulIOConfig;
use crate::index::Candidate;
use crate::index::cache::{self, IndexCacheEntry};
use crate::index::commit::ResolvedIndex;
use crate::index::options::SearchRequest;
use crate::index::prefix::derive_index_prefixes;

/// Opened splits of one index shard commit, kept by the shared index cache.
pub struct CachedTextShard {
    pub commit_id: i64,
    pub indexes: Vec<Index>,
    pub bytes: usize,
}

impl IndexCacheEntry for CachedTextShard {
    fn commit_id(&self) -> i64 {
        self.commit_id
    }

    fn memory_bytes(&self) -> usize {
        self.bytes
    }
}

/// Search one resolved text index shard (all splits of its current commit).
pub async fn search_resolved_shard(
    store: &Arc<dyn ObjectStore>,
    resolved: &ResolvedIndex,
    query: &str,
    top_k: usize,
) -> IoResult<Vec<Candidate>> {
    if !resolved.is_kind(IndexKind::Text) {
        return Err(rootcause::report!(
            "resolved index '{}' is not a text index",
            resolved.index_prefix
        ));
    }
    let entry = open_cached_shard(store, resolved).await?;

    let mut hits: Vec<TextHit> = Vec::new();
    for index in &entry.indexes {
        hits.extend(search_index(index, query, top_k).map_err(|error| {
            rootcause::report!(
                "text search failed at '{}': {}",
                resolved.index_prefix,
                error
            )
        })?);
    }
    let merged = merge_hits(hits, top_k);
    tracing::info!(
        prefix = %resolved.index_prefix,
        splits = entry.indexes.len(),
        results = merged.len(),
        "text index search"
    );
    Ok(merged
        .into_iter()
        .map(|hit| Candidate::scored(hit.id, hit.score))
        .collect())
}

/// Global BM25 statistics of one text index shard (all of its splits).
///
/// The statistics are merged across shards by the caller and used to score
/// candidates against one corpus-wide ranking (consistent cross-shard
/// relevance, like Elasticsearch's `dfs_query_then_fetch`).
pub async fn shard_stats(
    store: &Arc<dyn ObjectStore>,
    resolved: &ResolvedIndex,
    terms: &[String],
) -> IoResult<lakesoul_text::CorpusStats> {
    if !resolved.is_kind(IndexKind::Text) {
        return Err(rootcause::report!(
            "resolved index '{}' is not a text index",
            resolved.index_prefix
        ));
    }
    let entry = open_cached_shard(store, resolved).await?;
    let mut stats = lakesoul_text::CorpusStats::default();
    for index in &entry.indexes {
        stats.merge(lakesoul_text::collect_index_stats(index, terms).map_err(
            |error| {
                rootcause::report!(
                    "failed to collect text index statistics at '{}': {}",
                    resolved.index_prefix,
                    error
                )
            },
        )?);
    }
    Ok(stats)
}

/// Open (or reuse from the shared cache) every split of a text shard.
async fn open_cached_shard(
    store: &Arc<dyn ObjectStore>,
    resolved: &ResolvedIndex,
) -> IoResult<Arc<CachedTextShard>> {
    let splits: Vec<TextSplitEntry> = resolved.segments_as()?;
    let prefix = resolved.index_prefix.trim_end_matches('/').to_string();
    let cache_prefix = prefix.clone();
    let commit_id = resolved.commit_id;
    cache::get_or_load(store, IndexKind::Text, &prefix, commit_id, move || {
        let store = store.clone();
        let prefix = cache_prefix.clone();
        let splits = splits.clone();
        async move {
            let split_cache = SplitCache::from_env();
            let mut indexes = Vec::with_capacity(splits.len());
            let mut bytes = 0usize;
            for split in &splits {
                indexes.push(split_cache.open(&store, &prefix, split).await?);
                bytes += split.file_size as usize;
            }
            Ok::<CachedTextShard, TextError>(CachedTextShard {
                commit_id,
                indexes,
                bytes,
            })
        }
    })
    .await
    .map_err(|error| {
        rootcause::report!(
            "failed to load text index at '{}': {:?}",
            resolved.index_prefix,
            error
        )
    })
}

/// Search the text index matching a single bucket's files.
pub async fn search_matching_shards(
    store: &Arc<dyn ObjectStore>,
    file_paths: &[String],
    text_column: &str,
    table_prefix: &str,
    query: &str,
    top_k: usize,
    resolved_shards: &[ResolvedIndex],
) -> IoResult<Vec<Candidate>> {
    let prefixes =
        derive_index_prefixes(file_paths, table_prefix, IndexKind::Text, text_column);
    let Some((index_prefix, _bucket_id)) = prefixes.first() else {
        return Ok(Vec::new());
    };
    let normalized = index_prefix.trim_end_matches('/');
    let Some(resolved) = resolved_shards.iter().find(|shard| {
        shard.is_kind(IndexKind::Text)
            && shard.index_prefix.trim_end_matches('/') == normalized
    }) else {
        return Err(rootcause::report!(
            "text index at '{}' was not resolved by the caller; rebuild the index \
             if it was created by an unsupported (legacy) writer",
            normalized
        ));
    };
    search_resolved_shard(store, resolved, query, top_k).await
}

/// Run the text search selected by a parsed [`SearchRequest`].
pub async fn search_request(
    io_config: &LakeSoulIOConfig,
    store: &Arc<dyn ObjectStore>,
    request: &SearchRequest,
    table_prefix: &str,
) -> IoResult<Vec<Candidate>> {
    search_matching_shards(
        store,
        io_config.files_slice(),
        &request.column,
        table_prefix,
        &request.query,
        request.top_k,
        io_config.resolved_index_shards_slice(),
    )
    .await
}
