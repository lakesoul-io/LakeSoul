// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Process-wide cache for loaded index shards.
//!
//! Every query used to re-read and re-merge the whole index shard from the
//! object store (tens to hundreds of milliseconds for large shards). The
//! cache keeps the loaded, in-memory index per
//! `(kind, object store, prefix)` and reuses it while the caller resolves
//! the same commit id; a new commit (delta flush or rebuild) invalidates
//! the entry automatically.
//!
//! Capacity is a byte budget ([`ENV_CACHE_BYTES`], default 512 MiB, `0`
//! disables the cache); entries are evicted by weighted LRU.  Entries are
//! type-erased because one process may search several index kinds, which
//! keeps the cache and its byte accounting shared.

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use lakesoul_common::IndexKind;
use moka::future::Cache;
use object_store::ObjectStore;
use tracing::{debug, info, warn};

/// Byte budget of the process-wide index cache; `0` disables caching.
pub const ENV_CACHE_BYTES: &str = "LAKESOUL_INDEX_CACHE_BYTES";

/// Legacy name of the budget variable, kept as a fallback.
const LEGACY_ENV_CACHE_BYTES: &str = "LAKESOUL_VECTOR_INDEX_CACHE_BYTES";

const DEFAULT_CACHE_BYTES: u64 = 512 * 1024 * 1024;

/// A loaded index shard managed by [`get_or_load`].
pub trait IndexCacheEntry: Send + Sync + 'static {
    /// Catalog commit id the entry was loaded from.
    fn commit_id(&self) -> i64;

    /// In-memory footprint used for the LRU byte budget.
    fn memory_bytes(&self) -> usize;
}

/// A cached entry plus the bookkeeping the type-erased cache needs.
#[derive(Clone)]
struct ErasedEntry {
    data: Arc<dyn Any + Send + Sync>,
    commit_id: i64,
    bytes: usize,
}

static CACHE: LazyLock<Option<Cache<String, ErasedEntry>>> = LazyLock::new(|| {
    let capacity = std::env::var(ENV_CACHE_BYTES)
        .ok()
        .or_else(|| std::env::var(LEGACY_ENV_CACHE_BYTES).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_CACHE_BYTES);
    if capacity == 0 {
        info!("index cache disabled by {ENV_CACHE_BYTES}=0");
        return None;
    }
    info!(
        capacity_bytes = capacity,
        "index cache initialized ({ENV_CACHE_BYTES})"
    );
    Some(
        Cache::builder()
            .max_capacity(capacity)
            .weigher(|_key: &String, entry: &ErasedEntry| {
                entry.bytes.min(u32::MAX as usize) as u32
            })
            .build(),
    )
});

static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

/// `(hits, misses)` since process start; for diagnostics and tests.
pub fn stats() -> (u64, u64) {
    (HITS.load(Ordering::Relaxed), MISSES.load(Ordering::Relaxed))
}

fn cache_key(kind: IndexKind, store: &Arc<dyn ObjectStore>, prefix: &str) -> String {
    format!("{kind}\u{0}{store}\u{0}{prefix}")
}

/// Whether the shard at `commit_id` is already loaded, so a query would not
/// touch any file (and therefore needs no reader lease).
pub async fn is_loaded(
    store: &Arc<dyn ObjectStore>,
    kind: IndexKind,
    prefix: &str,
    commit_id: i64,
) -> bool {
    let Some(cache) = CACHE.as_ref() else {
        return false;
    };
    cache
        .get(&cache_key(kind, store, prefix))
        .await
        .is_some_and(|entry| entry.commit_id == commit_id)
}

/// Return the loaded index for `(kind, store, prefix)` at the resolved
/// commit, loading and caching it when needed.
///
/// The caller resolves the catalog commit before calling; a stale entry is
/// replaced when the commit id moved forward, so rebuilds and incremental
/// commits are picked up on the next query.  `loader` is only called on a
/// cache miss (or when the entry lost a race against a newer commit).
pub async fn get_or_load<T, E, F, Fut>(
    store: &Arc<dyn ObjectStore>,
    kind: IndexKind,
    prefix: &str,
    commit_id: i64,
    loader: F,
) -> Result<Arc<T>, Arc<E>>
where
    T: IndexCacheEntry,
    E: Send + Sync + 'static,
    F: Fn() -> Fut + Clone,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let Some(cache) = CACHE.as_ref() else {
        return loader().await.map(Arc::new).map_err(Arc::new);
    };
    let key = cache_key(kind, store, prefix);
    if let Some(entry) = cache.get(&key).await
        && entry.commit_id == commit_id
        && let Ok(value) = entry.data.clone().downcast::<T>()
    {
        HITS.fetch_add(1, Ordering::Relaxed);
        debug!(prefix, "index cache hit");
        return Ok(value);
    }
    MISSES.fetch_add(1, Ordering::Relaxed);
    // Drop the stale value first so `try_get_with` actually loads; the
    // loader future is shared by concurrent queries for this key.
    cache.invalidate(&key).await;
    let t0 = std::time::Instant::now();
    let cache_loader = loader.clone();
    let entry = cache
        .try_get_with(key, async move {
            let value = cache_loader().await?;
            Ok::<ErasedEntry, E>(ErasedEntry {
                commit_id,
                bytes: value.memory_bytes(),
                data: Arc::new(value) as Arc<dyn Any + Send + Sync>,
            })
        })
        .await?;
    if let Some(capacity) = cache.policy().max_capacity()
        && entry.bytes as u64 > capacity
    {
        warn!(
            prefix,
            bytes = entry.bytes,
            capacity,
            "index shard exceeds cache budget; reloading on every query"
        );
    }
    if entry.commit_id != commit_id {
        // Lost a race against a loader of another commit; serve this query
        // directly instead of returning a mismatched index.
        return loader().await.map(Arc::new).map_err(Arc::new);
    }
    let value = entry
        .data
        .clone()
        .downcast::<T>()
        .unwrap_or_else(|_| panic!("index cache entry type mismatch for '{prefix}'"));
    debug!(
        prefix,
        commit_id,
        bytes = entry.bytes,
        elapsed = ?t0.elapsed(),
        "index cache miss, loaded"
    );
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use object_store::memory::InMemory;

    use super::*;

    #[derive(Clone)]
    struct TestEntry {
        commit_id: i64,
        bytes: usize,
    }

    impl IndexCacheEntry for TestEntry {
        fn commit_id(&self) -> i64 {
            self.commit_id
        }

        fn memory_bytes(&self) -> usize {
            self.bytes
        }
    }

    fn store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    async fn load(
        commit_id: i64,
        bytes: usize,
    ) -> Result<TestEntry, std::convert::Infallible> {
        Ok(TestEntry { commit_id, bytes })
    }

    #[tokio::test]
    async fn cache_reuses_loaded_entry_until_commit_changes() {
        let store = store();
        let prefix = "index_cache_reuse_test";
        let loads = AtomicUsize::new(0);

        let first = get_or_load(&store, IndexKind::Vector, prefix, 1, || {
            loads.fetch_add(1, Ordering::Relaxed);
            load(1, 16)
        })
        .await
        .unwrap();
        let second = get_or_load(&store, IndexKind::Vector, prefix, 1, || {
            loads.fetch_add(1, Ordering::Relaxed);
            load(1, 16)
        })
        .await
        .unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "same commit must reuse the cached entry"
        );

        let after = get_or_load(&store, IndexKind::Vector, prefix, 2, || load(2, 32))
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&first, &after),
            "new commit must reload the entry"
        );
        assert_eq!(after.commit_id(), 2);
        assert_eq!(loads.load(Ordering::Relaxed), 1, "cache hit must not load");
    }

    #[tokio::test]
    async fn cache_is_scoped_by_kind() {
        let store = store();
        let prefix = "index_cache_kind_scope_test";
        let vector = get_or_load(&store, IndexKind::Vector, prefix, 1, || load(1, 8))
            .await
            .unwrap();
        let text = get_or_load(&store, IndexKind::Text, prefix, 1, || load(1, 8))
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&vector, &text),
            "different kinds must not share a cache entry"
        );
    }

    #[tokio::test]
    async fn is_loaded_matches_the_commit() {
        let store = store();
        let prefix = "index_cache_is_loaded_test";
        assert!(!is_loaded(&store, IndexKind::Vector, prefix, 1).await);
        let _ = get_or_load(&store, IndexKind::Vector, prefix, 1, || load(1, 8))
            .await
            .unwrap();
        assert!(is_loaded(&store, IndexKind::Vector, prefix, 1).await);
        assert!(!is_loaded(&store, IndexKind::Vector, prefix, 2).await);
    }
}
