// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Process-wide cache for loaded IVF+RaBitQ index shards.
//!
//! Every query used to re-read and re-merge the whole index shard from the
//! object store (tens to hundreds of milliseconds for large shards). The
//! cache keeps the merged in-memory index per `(object store, prefix)` and
//! reuses it while the manifest still reports the same
//! `(generation, version)`. Callers resolve the manifest view first, so a
//! commit (e.g. an index rebuild) invalidates the entry automatically.
//!
//! Capacity is a byte budget ([`ENV_CACHE_BYTES`], default 512 MiB, `0`
//! disables the cache); entries are evicted by weighted LRU.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use lakesoul_vector::rabitq::manifest::ResolvedView;
use lakesoul_vector::{IvfRabitqIndex, ManifestStore, RabitqError};
use moka::future::Cache;
use object_store::ObjectStore;
use tracing::{debug, info, warn};

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

/// A loaded index shard plus the manifest identity it was loaded from.
pub struct CachedIndex {
    pub generation: u64,
    pub version: u64,
    /// Object identity of the resolved manifests; see [`ResolvedView::view_token`].
    pub token: String,
    pub index: IvfRabitqIndex,
    pub bytes: usize,
}

impl CachedIndex {
    /// `(generation, version)` key of this entry.
    pub fn key(&self) -> (u64, u64) {
        (self.generation, self.version)
    }

    /// Whether this entry was loaded from the exact commit resolved in `view`.
    pub fn matches(&self, view: &ResolvedView) -> bool {
        self.generation == view.generation
            && self.version == view.version
            && self.token == view.view_token
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
    view: ResolvedView,
) -> Result<CachedIndex, RabitqError> {
    let mstore = ManifestStore::new(store, prefix);
    let index = IvfRabitqIndex::load_from_view(&mstore, &view).await?;
    let bytes = index.memory_bytes();
    Ok(CachedIndex {
        generation: view.generation,
        version: view.version,
        token: view.view_token.clone(),
        index,
        bytes,
    })
}

/// Return the index shard for `(store, prefix)` at the version of `view`,
/// loading and caching it when needed.
///
/// The caller resolves `view` from the manifest before calling; a stale
/// entry is replaced when the resolved version moved forward, so rebuilds
/// and incremental commits are picked up on the next query.
pub async fn get_or_load(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    view: &ResolvedView,
) -> Result<Arc<CachedIndex>, Arc<RabitqError>> {
    let Some(cache) = CACHE.as_ref() else {
        return load_entry(store.clone(), prefix.to_string(), view.clone())
            .await
            .map(Arc::new)
            .map_err(Arc::new);
    };
    let key = cache_key(store, prefix);
    if let Some(entry) = cache.get(&key).await
        && entry.matches(view)
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
            let view = view.clone();
            async move { load_entry(store, prefix, view).await.map(Arc::new) }
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
    if !entry.matches(view) {
        // Lost a race against a loader of another commit; serve this query
        // directly instead of returning a mismatched index.
        return load_entry(store.clone(), prefix.to_string(), view.clone())
            .await
            .map(Arc::new)
            .map_err(Arc::new);
    }
    debug!(
        prefix,
        generation = entry.generation,
        version = entry.version,
        bytes = entry.bytes,
        elapsed = ?t0.elapsed(),
        "vector index cache miss, loaded"
    );
    Ok(entry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lakesoul_vector::rabitq::manifest::resolve_view;
    use lakesoul_vector::{IdAndVecBatch, IvfRabitqBuilder, Metric, RotatorType};
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
    ) -> ManifestStore {
        let mstore = ManifestStore::new(store, prefix.to_string());
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
        builder
            .build(|| futures::stream::iter(stream.clone()))
            .await
            .unwrap()
            .save_to_v4(&mstore)
            .await
            .unwrap();
        mstore
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
    async fn cache_reuses_loaded_index_until_version_changes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "cache_test";
        let mstore = build_base(store.clone(), prefix, base_vectors()).await;
        let view = resolve_view(&mstore).await.unwrap().unwrap();

        let first = get_or_load(&store, prefix, &view).await.unwrap();
        let second = get_or_load(&store, prefix, &view).await.unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "same manifest version must reuse the cached index"
        );
        assert!(first.bytes > 0);

        // Append a delta: a new commit bumps the manifest version.
        let mut builder = IvfRabitqBuilder::load(
            &mstore,
            DIM,
            NLIST,
            7,
            Metric::L2,
            RotatorType::FhtKacRotator,
            42,
            true,
        )
        .await
        .unwrap();
        builder
            .insert_batch(IdAndVecBatch {
                ids: vec![100],
                vectors: vec![5.0, 5.0, 5.0, 5.0],
            })
            .unwrap();
        builder.flush(&mstore).await.unwrap();

        let view2 = resolve_view(&mstore).await.unwrap().unwrap();
        assert_ne!(view.key(), view2.key(), "flush must commit a new version");
        let after = get_or_load(&store, prefix, &view2).await.unwrap();
        assert!(
            !Arc::ptr_eq(&first, &after),
            "new manifest version must reload the index"
        );
        assert!(after.matches(&view2));
        assert_eq!(after.index.len(), 6, "delta vector visible after reload");

        let cached_again = get_or_load(&store, prefix, &view2).await.unwrap();
        assert!(
            Arc::ptr_eq(&after, &cached_again),
            "reloaded entry must be cached for the new version"
        );
    }

    #[tokio::test]
    async fn cache_drops_entry_when_index_is_recreated_from_scratch() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let prefix = "cache_reset_test";
        let mstore = build_base(store.clone(), prefix, base_vectors()).await;
        let view = resolve_view(&mstore).await.unwrap().unwrap();
        let first = get_or_load(&store, prefix, &view).await.unwrap();
        assert_eq!(first.index.len(), 5);

        // Simulate table drop + recreate at the same location: wipe the
        // index and build a fresh one, which restarts at generation 1.
        delete_prefix(&store, prefix).await;
        let mstore = build_base(
            store.clone(),
            prefix,
            vec![
                0.0, 0.0, 0.0, 0.0, //
                1.0, 0.0, 0.0, 0.0, //
                0.0, 1.0, 0.0, 0.0, //
            ],
        )
        .await;
        let view2 = resolve_view(&mstore).await.unwrap().unwrap();
        assert_eq!(
            view.key(),
            view2.key(),
            "recreated index restarts at the same (generation, version)"
        );
        assert_ne!(
            view.view_token, view2.view_token,
            "manifest object identity must differ after recreation"
        );

        let after = get_or_load(&store, prefix, &view2).await.unwrap();
        assert!(
            !Arc::ptr_eq(&first, &after),
            "cache must not serve the dropped index"
        );
        assert_eq!(after.index.len(), 3, "recreated index visible");
    }
}
