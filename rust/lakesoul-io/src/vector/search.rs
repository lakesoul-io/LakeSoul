// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Vector similarity search via rabitq-rs IVF+RaBitQ index.

use std::sync::Arc;

use lakesoul_vector::{IvfRabitqIndex, ManifestStore, Metric, SearchParams};
use object_store::ObjectStore;
use tracing::{debug, info, warn};

use crate::Result as IoResult;

pub async fn search_index_shard(
    store: &Arc<dyn ObjectStore>,
    index_prefix: &str,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    _metric: Metric,
) -> IoResult<Option<Vec<u64>>> {
    let prefix = index_prefix.trim_end_matches('/');
    let mstore = ManifestStore::new(store.clone(), prefix.to_string());
    let index = match IvfRabitqIndex::load_from_v4(&mstore).await {
        Ok(idx) => idx,
        Err(lakesoul_vector::RabitqError::InvalidPersistence(_)) => {
            debug!("No vector index found at '{}'", prefix);
            return Ok(None);
        }
        Err(e) => {
            warn!("Failed to load vector index at '{}': {:?}", prefix, e);
            return Ok(None);
        }
    };
    let params = SearchParams::new(top_k, nprobe);
    let results = index.search(query, params).map_err(|e| {
        rootcause::report!("vector search failed at '{}': {:?}", prefix, e)
    })?;
    let ids: Vec<u64> = results.into_iter().map(|r| r.id).collect();
    info!(
        "Vector search at '{}': {} results (nprobe={})",
        prefix,
        ids.len(),
        nprobe
    );
    Ok(Some(ids))
}

/// Search the vector index matching a single bucket's files.
///
/// One LakeSoulReader processes files from exactly one hash bucket.
/// We derive the index prefix from those files and search that one index.
/// Merging results across multiple buckets is the caller's responsibility.
pub async fn search_matching_shards(
    store: &Arc<dyn ObjectStore>,
    file_paths: &[String],
    vector_column: &str,
    prefix: &str,
    _range_partitions: &[String],
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    metric: Metric,
) -> IoResult<Vec<u64>> {
    let prefixes = derive_index_prefixes(file_paths, prefix, vector_column);
    // One reader = one bucket, use the first (only) matching index
    if let Some((index_prefix, _bucket_id)) = prefixes.first()
        && let Some(ids) =
            search_index_shard(store, index_prefix, query, top_k, nprobe, metric).await?
        {
            return Ok(ids);
        }
    Ok(Vec::new())
}

/// Derive the vector index prefix from file paths and table prefix.
///
/// Extracts partition_desc and bucket_id from each file path, then
/// constructs `{table_prefix}/_vector_index/{column}/{partition_desc}/{bucket_id}/`.
///
/// Returns a list of (index_prefix, bucket_id) pairs.
pub fn derive_index_prefixes(
    file_paths: &[String],
    table_prefix: &str,
    vector_column: &str,
) -> Vec<(String, u32)> {
    use std::collections::HashSet;

    let prefix = table_prefix
        .trim_start_matches("file://")
        .trim_start_matches("s3://")
        .trim_start_matches("s3a://");
    let mut seen: HashSet<(String, u32)> = HashSet::new();
    let mut result = Vec::new();
    for file_path in file_paths {
        let Some(bucket_id) = crate::helpers::extract_hash_bucket_id(file_path) else {
            continue;
        };
        let clean_path = file_path
            .trim_start_matches("file://")
            .trim_start_matches("s3://")
            .trim_start_matches("s3a://");
        let relative = clean_path
            .strip_prefix(prefix)
            .unwrap_or(clean_path)
            .trim_start_matches('/');
        let parent_dir = std::path::Path::new(relative)
            .parent()
            .and_then(|p| p.to_str())
            .unwrap_or("");
        let partition_desc = if parent_dir.is_empty() {
            "-5".to_string()
        } else {
            parent_dir.to_string()
        };
        let key = (partition_desc, bucket_id);
        if seen.insert(key.clone()) {
            result.push((
                format!(
                    "{}/_vector_index/{}/{}/{}",
                    prefix.trim_end_matches('/'),
                    vector_column,
                    key.0,
                    key.1
                ),
                bucket_id,
            ));
        }
    }
    result
}

pub fn parse_query_vector(s: &str, expected_dim: Option<usize>) -> IoResult<Vec<f32>> {
    let vec: Vec<f32> = s
        .split(',')
        .map(|p| p.trim().parse::<f32>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| rootcause::report!("invalid vector search query: {}", e))?;
    if let Some(dim) = expected_dim
        && vec.len() != dim {
            return Err(rootcause::report!(
                "query vector dimension mismatch: expected {}, got {}",
                dim,
                vec.len()
            ));
        }
    Ok(vec)
}
