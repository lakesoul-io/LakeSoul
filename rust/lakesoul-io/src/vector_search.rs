// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Vector similarity search via rabitq-rs IVF+RaBitQ index.

use std::collections::HashSet;
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
    metric: Metric,
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
    let mut shard_keys: HashSet<(String, u32)> = HashSet::new();
    for file_path in file_paths {
        let Some(bucket_id) = crate::helpers::extract_hash_bucket_id(file_path) else {
            continue;
        };
        let relative = file_path
            .strip_prefix(prefix)
            .unwrap_or(file_path)
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
        shard_keys.insert((partition_desc, bucket_id));
    }
    let mut all_ids: Vec<u64> = Vec::new();
    for (partition_desc, bucket_id) in &shard_keys {
        let index_prefix = format!(
            "{}/_vector_index/{}/{}/{}",
            prefix.trim_end_matches('/'),
            vector_column,
            partition_desc,
            bucket_id
        );
        if let Some(ids) =
            search_index_shard(store, &index_prefix, query, top_k, nprobe, metric).await?
        {
            all_ids.extend(ids);
        }
    }
    all_ids.sort();
    all_ids.dedup();
    all_ids.truncate(top_k);
    Ok(all_ids)
}

pub fn parse_query_vector(s: &str, expected_dim: Option<usize>) -> IoResult<Vec<f32>> {
    let vec: Vec<f32> = s
        .split(',')
        .map(|p| p.trim().parse::<f32>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| rootcause::report!("invalid vector search query: {}", e))?;
    if let Some(dim) = expected_dim {
        if vec.len() != dim {
            return Err(rootcause::report!(
                "query vector dimension mismatch: expected {}, got {}",
                dim,
                vec.len()
            ));
        }
    }
    Ok(vec)
}
