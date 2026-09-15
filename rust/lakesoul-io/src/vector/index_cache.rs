// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Process-wide cache for loaded IVF+RaBitQ index shards.
//!
//! Every query used to re-read and re-merge the whole index shard from the
//! object store (tens to hundreds of milliseconds for large shards). The
//! cache keeps the merged in-memory index per `(object store, prefix)` and
//! reuses it while the caller resolves the same commit id; a new commit
//! (delta flush or rebuild) invalidates the entry automatically.
//!
//! Capacity is a byte budget ([`ENV_CACHE_BYTES`], default 512 MiB, `0`
//! disables the cache); entries are evicted by weighted LRU.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use lakesoul_vector::rabitq::segment::{IndexHeader, IndexStore};
use lakesoul_vector::{IvfRabitqIndex, RabitqError};
use moka::future::Cache;
use object_store::ObjectStore;
use tracing::{debug, info, warn};

use crate::vector::builder::ResolvedIndexShard;

/// Byte budget of the process-wide index cache; `0` disables caching.
pub const ENV_CACHE_BYTES: &str = "LAKESOUL_VECTOR_INDEX_CACHE_BYTES";

const DEFAULT_CACHE_BYTES: u64 = 512 * 1024 * 1024;

static CACHE: LazyLock<Option<Cache<String, Arc<CachedIndex>>>> = LazyLock::new(|| {
    let capacity = std::env::var(ENV_CACHE_BYTES)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_CACHE_BYTES);
    if capacity == 0 {
        info!("vector index cache disabled by {ENV_CACHE_BYTES}=0");
        return None;
    }
    info!(
        capacity_bytes = capacity,
        "vector index cache initialized ({ENV_CACHE_BYTES})"
    );
    Some(
        Cache::builder()
            .max_capacity(capacity)
            .weigher(|_key: &String, entry: &Arc<CachedIndex>| {
                entry.bytes.min(u32::MAX as usize) as u32
            })
            .build(),
    )
});

static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

/// A loaded index shard plus the commit it was loaded from.
pub struct CachedIndex {
    /// Catalog commit id the entry was loaded from.
    pub commit_id: i64,
    pub generation: u64,
    pub version: u64,
    pub index: IvfRabitqIndex,
    pub bytes: usize,
}

impl CachedIndex {
    /// Whether this entry was loaded from the given commit.
    pub fn matches(&self, commit_id: i64) -> bool {
        self.commit_id == commit_id
    }
}

/// `(hits, misses)` since process start; for diagnostics and tests.
pub fn stats() -> (u64, u64) {
    (HITS.load(Ordering::Relaxed), MISSES.load(Ordering::Relaxed))
}

fn cache_key(store: &Arc<dyn ObjectStore>, prefix: &str) -> String {
    format!("{store}\u{0}{prefix}")
}

async fn load_entry(
    store: Arc<dyn ObjectStore>,
    prefix: String,
    resolved: ResolvedIndexShard,
) -> Result<CachedIndex, RabitqError> {
    let istore = IndexStore::new(store, prefix);
    let header = IndexHeader::deserialize(&resolved.header)?;
    let index =
        IvfRabitqIndex::load_from_segments(&istore, &header, &resolved.segments).await?;
    let bytes = index.memory_bytes();
    Ok(CachedIndex {
        commit_id: resolved.commit_id,
        generation: resolved.generation,
        version: resolved.version,
        index,
        bytes,
    })
}

/// Whether the shard at `commit_id` is already loaded, so a query would not
/// touch any file (and therefore needs no reader lease).
pub async fn is_loaded(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    commit_id: i64,
) -> bool {
    let Some(cache) = CACHE.as_ref() else {
        return false;
    };
    cache
        .get(&cache_key(store, prefix))
        .await
        .is_some_and(|entry| entry.matches(commit_id))
}

/// Return the index shard for `(store, prefix)` at the resolved commit,
/// loading and caching it when needed.
///
/// The caller resolves the catalog commit before calling; a stale entry is
/// replaced when the commit id moved forward, so rebuilds and incremental
/// commits are picked up on the next query.
pub async fn get_or_load(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    resolved: &ResolvedIndexShard,
) -> Result<Arc<CachedIndex>, Arc<RabitqError>> {
    let Some(cache) = CACHE.as_ref() else {
        return load_entry(store.clone(), prefix.to_string(), resolved.clone())
            .await
            .map(Arc::new)
            .map_err(Arc::new);
    };
    let key = cache_key(store, prefix);
    if let Some(entry) = cache.get(&key).await
        && entry.matches(resolved.commit_id)
    {
        HITS.fetch_add(1, Ordering::Relaxed);
        debug!(prefix, "vector index cache hit");
        return Ok(entry);
    }
    MISSES.fetch_add(1, Ordering::Relaxed);
    // Drop the stale value first so `try_get_with` actually loads; the
    // loader future is shared by concurrent queries for this key.
    cache.invalidate(&key).await;
    let t0 = std::time::Instant::now();
    let entry = cache
        .try_get_with(key, {
            let store = store.clone();
            let prefix = prefix.to_string();
            let resolved = resolved.clone();
            async move { load_entry(store, prefix, resolved).await.map(Arc::new) }
        })
        .await?;
    if let Some(capacity) = cache.policy().max_capacity()
        && entry.bytes as u64 > capacity
    {
        warn!(
            prefix,
            bytes = entry.bytes,
            capacity,
            "vector index shard exceeds cache budget; reloading on every query"
        );
    }
    if !entry.matches(resolved.commit_id) {
        // Lost a race against a loader of another commit; serve this query
        // directly instead of returning a mismatched index.
        return load_entry(store.clone(), prefix.to_string(), resolved.clone())
            .await
            .map(Arc::new)
            .map_err(Arc::new);
    }
    debug!(
        prefix,
        commit_id = entry.commit_id,
        bytes = entry.bytes,
        elapsed = ?t0.elapsed(),
        "vector index cache miss, loaded"
    );
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakesoul_vector::{
        IdAndVecBatch, IndexStore, IvfRabitqBuilder, Metric, RotatorType,
    };
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    const DIM: usize = 4;
    const NLIST: usize = 1;

    fn base_vectors() -> Vec<f32> {
        vec![
            0.0, 0.0, 0.0, 0.0, //
            1.0, 0.0, 0.0, 0.0, //
            0.0, 1.0, 0.0, 0.0, //
            0.0, 0.0, 1.0, 0.0, //
            0.0, 0.0, 0.0, 1.0, //
        ]
    }

    async fn build_base(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        vectors: Vec<f32>,
        commit_id: i64,
    ) -> ResolvedIndexShard {
        let mut builder = IvfRabitqBuilder::new(
            DIM,
            NLIST,
            7,
            Metric::L2,
            RotatorType::FhtKacRotator,
            42,
            true,
        );
        builder
            .insert_batch(IdAndVecBatch {
                ids: (0..vectors.len() as u64 / DIM as u64).collect(),
                vectors: vectors.clone(),
            })
            .unwrap();
        let stream = vec![IdAndVecBatch {
            ids: (0..vectors.len() as u64 / DIM as u64).collect(),
            vectors,
        }];
        let index = builder
            .build(|| futures::stream::iter(stream.clone()))
            .await
            .unwrap();
        let istore = IndexStore::new(store, prefix.to_string());
        let (header, segments) = index.write_base_segments(&istore).await.unwrap();
        ResolvedIndexShard {
            index_prefix: prefix.to_string(),
            commit_id,
            generation: 1,
            version: 1,
            header: header.serialize(),
            segments,
        }
    }

    async fn append_delta(
        store: Arc<dyn ObjectStore>,
        base: &ResolvedIndexShard,
    ) -> ResolvedIndexShard {
        let istore = IndexStore::new(store, base.index_prefix.clone());
        let header = IndexHeader::deserialize(&base.header).unwrap();
        let mut builder = IvfRabitqBuilder::load(&istore, &header, &base.segments)
            .await
            .unwrap();
        builder
            .insert_batch(IdAndVecBatch {
                ids: vec![100],
                vectors: vec![5.0, 5.0, 5.0, 5.0],
            })
            .unwrap();
        let (_header, new_segments) = builder.flush(&istore).await.unwrap();
        let mut segments = base.segments.clone();
        segments.extend(new_segments);
        ResolvedIndexShard {
            commit_id: base.commit_id + 1,
            version: base.version + 1,
            segments,
            ..base.clone()
        }
    }

    async fn delete_prefix(store: &Arc<dyn ObjectStore>, prefix: &str) {
        use futures::StreamExt;
        let path = object_store::path::Path::from(prefix);
        let mut locations = Vec::new();
        let mut stream = store.list(Some(&path));
        while let Some(meta) = stream.next().await {
            locations.push(meta.unwrap().location);
        }
        for location in locations {
            store.delete(&location).await.unwrap();
        }
    }

    #[tokio::test]
    async fn cache_reuses_loaded_index_until_commit_changes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "cache_test";
        let base = build_base(store.clone(), prefix, base_vectors(), 1).await;

        let first = get_or_load(&store, prefix, &base).await.unwrap();
        let second = get_or_load(&store, prefix, &base).await.unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "same commit must reuse the cached index"
        );
        assert!(first.bytes > 0);
        assert!(first.matches(base.commit_id));

        let delta = append_delta(store.clone(), &base).await;
        let after = get_or_load(&store, prefix, &delta).await.unwrap();
        assert!(
            !Arc::ptr_eq(&first, &after),
            "new commit must reload the index"
        );
        assert!(after.matches(delta.commit_id));
        assert_eq!(after.index.len(), 6, "delta vector visible after reload");

        let cached_again = get_or_load(&store, prefix, &delta).await.unwrap();
        assert!(
            Arc::ptr_eq(&after, &cached_again),
            "reloaded entry must be cached for the new commit"
        );
    }

    #[tokio::test]
    async fn cache_drops_entry_when_index_is_recreated_from_scratch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "cache_reset_test";
        let base = build_base(store.clone(), prefix, base_vectors(), 1).await;
        let first = get_or_load(&store, prefix, &base).await.unwrap();
        assert_eq!(first.index.len(), 5);

        // Simulate table drop + recreate at the same location: wipe the
        // index and build a fresh one with a new commit id.
        delete_prefix(&store, prefix).await;
        let recreated = build_base(
            store.clone(),
            prefix,
            vec![
                0.0, 0.0, 0.0, 0.0, //
                1.0, 0.0, 0.0, 0.0, //
                0.0, 1.0, 0.0, 0.0, //
            ],
            2,
        )
        .await;

        let after = get_or_load(&store, prefix, &recreated).await.unwrap();
        assert!(
            !Arc::ptr_eq(&first, &after),
            "cache must not serve the dropped index"
        );
        assert_eq!(after.index.len(), 3, "recreated index visible");
    }
}
