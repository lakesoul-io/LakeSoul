// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Vector similarity search via rabitq-rs IVF+RaBitQ index.
//!
//! The catalog is resolved by the caller (which has metadata access); this
//! module searches an already-resolved [`ResolvedIndexShard`] and derives
//! index prefixes from data file paths.

use std::sync::Arc;

use lakesoul_vector::{IvfRabitqIndex, Metric, SearchParams};
use object_store::ObjectStore;
use tracing::info;

use crate::Result as IoResult;
use crate::vector::builder::ResolvedIndexShard;
use crate::vector::index_cache;

/// Search one resolved shard (base + deltas of its current commit).
pub async fn search_resolved_shard(
    store: &Arc<dyn ObjectStore>,
    resolved: &ResolvedIndexShard,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
) -> IoResult<Option<Vec<u64>>> {
    let prefix = resolved.index_prefix.trim_end_matches('/');
    let entry = index_cache::get_or_load(store, prefix, resolved)
        .await
        .map_err(|e| {
            rootcause::report!("failed to load vector index at '{}': {}", prefix, e)
        })?;
    search_loaded_index(&entry.index, prefix, query, top_k, nprobe)
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
    _range_partitions: &[String],
    query: &[f32],
    top_k: usize,
    nprobe: usize,
    _metric: Metric,
    resolved_shards: &[ResolvedIndexShard],
) -> IoResult<Vec<u64>> {
    let prefixes = derive_index_prefixes(file_paths, prefix, vector_column);
    let Some((index_prefix, _bucket_id)) = prefixes.first() else {
        return Ok(Vec::new());
    };
    let normalized = index_prefix.trim_end_matches('/');
    let Some(resolved) = resolved_shards
        .iter()
        .find(|shard| shard.index_prefix.trim_end_matches('/') == normalized)
    else {
        return Err(rootcause::report!(
            "vector index at '{}' was not resolved by the caller; rebuild the index \
             if it was created by an unsupported (legacy) writer",
            normalized
        ));
    };
    Ok(search_resolved_shard(store, resolved, query, top_k, nprobe)
        .await?
        .unwrap_or_default())
}

fn search_loaded_index(
    index: &IvfRabitqIndex,
    prefix: &str,
    query: &[f32],
    top_k: usize,
    nprobe: usize,
) -> IoResult<Option<Vec<u64>>> {
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

    let is_s3 = !file_paths.is_empty()
        && (file_paths[0].starts_with("s3://") || file_paths[0].starts_with("s3a://"));
    let prefix = table_prefix
        .trim_start_matches("file://")
        .trim_start_matches("s3://")
        .trim_start_matches("s3a://");
    // For S3 paths the first component is the bucket name.  Strip it
    // because the ObjectStore already knows the bucket.
    let store_prefix: &str = if is_s3 {
        prefix
            .split_once('/')
            .map(|(_, rest)| rest)
            .unwrap_or(prefix)
    } else {
        prefix
    };
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
        // Strip the bucket from S3 clean paths too, for consistent
        // relative-path computation.
        let store_clean_path: &str = if is_s3 {
            clean_path
                .split_once('/')
                .map(|(_, rest)| rest)
                .unwrap_or(clean_path)
        } else {
            clean_path
        };
        let relative = store_clean_path
            .strip_prefix(store_prefix)
            .unwrap_or(store_clean_path)
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
                    store_prefix.trim_end_matches('/'),
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
    use super::derive_index_prefixes;

    #[test]
    fn derive_prefixes_from_local_files() {
        let files = vec![
            "/tmp/table/part=1/part-x_0.parquet".to_string(),
            "/tmp/table/part=1/part-y_0.parquet".to_string(),
        ];
        let prefixes = derive_index_prefixes(&files, "/tmp/table", "vec");
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0].0, "/tmp/table/_vector_index/vec/part=1/0");
        assert_eq!(prefixes[0].1, 0);
    }

    #[test]
    fn derive_prefixes_strips_s3_bucket() {
        let files = vec!["s3://bucket/table/part-x_0.parquet".to_string()];
        let prefixes = derive_index_prefixes(&files, "s3://bucket/table", "vec");
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0].0, "table/_vector_index/vec/-5/0");
    }
}
