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
use tracing::{debug, info, warn};

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
    _namespace: Option<Arc<crate::cache::store_namespace::StoreCacheNamespace>>,
}

static CACHE: LazyLock<Option<Cache<String, ErasedEntry>>> = LazyLock::new(|| {
    let capacity = lakesoul_vector::cache_config::config().cluster_bytes() as u64;
    if capacity == 0 {
        info!("index cache disabled by LAKESOUL_VECTOR_CACHE=0");
        return None;
    }
    info!(
        capacity_bytes = capacity,
        "index cache initialized (LAKESOUL_VECTOR_MEMORY_BYTES)"
    );
    metrics::describe_counter!(
        "lakesoul_index_cache_evictions_total",
        "Loaded index shards evicted from the in-memory index cache"
    );
    metrics::describe_counter!(
        "lakesoul_index_cache_hits_total",
        "Loaded index shards served from the in-memory index cache"
    );
    metrics::describe_counter!(
        "lakesoul_index_cache_misses_total",
        "Loaded index shards that missed the in-memory index cache"
    );
    Some(
        Cache::builder()
            .max_capacity(capacity)
            .weigher(|_key: &String, entry: &ErasedEntry| {
                entry.bytes.min(u32::MAX as usize) as u32
            })
            .eviction_listener(|_key: Arc<String>, _entry: ErasedEntry, cause| {
                if matches!(
                    cause,
                    moka::notification::RemovalCause::Size
                        | moka::notification::RemovalCause::Replaced
                ) {
                    metrics::counter!("lakesoul_index_cache_evictions_total")
                        .increment(1);
                }
            })
            .build(),
    )
});

static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

/// Configured in-memory budget, or `None` when caching is disabled.
pub fn capacity_bytes() -> Option<u64> {
    CACHE
        .as_ref()
        .and_then(|cache| cache.policy().max_capacity())
}

/// `(hits, misses)` since process start; for diagnostics and tests.
pub fn stats() -> (u64, u64) {
    (HITS.load(Ordering::Relaxed), MISSES.load(Ordering::Relaxed))
}

/// Cache key of a loaded index shard: `(kind, index_prefix)`.
///
/// That pair is the shard identity of the whole system — `index_shard` has
/// `unique (kind, index_prefix)` — and the prefix already embeds the table
/// path, so the cache keys on exactly the same identity.  The object store's
/// display form used to be part of the key, but the same store is reached
/// through different handles: the DataFusion side gets
/// `MonitoredObjectStore(kind, <store>)` while the native reader builds its
/// own runtime and gets the plain store, so one shard had two cache keys and
/// `is_loaded` could never report a hit.
fn cache_key(kind: IndexKind, prefix: &str) -> String {
    format!("{kind}\u{0}{prefix}")
}

fn scoped_cache_key(
    kind: IndexKind,
    prefix: &str,
    namespace: &crate::cache::store_namespace::StoreCacheNamespace,
) -> String {
    format!("{}\u{0}{}", cache_key(kind, prefix), namespace.token())
}

fn keys_for_prefix(cache: &Cache<String, ErasedEntry>, base: String) -> Vec<String> {
    cache
        .iter()
        .filter_map(|(key, _)| {
            (key.as_str() == base || key.starts_with(&format!("{base}\u{0}")))
                .then(|| key.as_str().to_string())
        })
        .collect()
}

/// Drop the cached entry of one shard.
///
/// Used by `drop_vector_index` after the catalog rows and object files are
/// removed so this process cannot keep serving the old parsed index.
pub async fn invalidate(kind: IndexKind, prefix: &str) {
    if let Some(cache) = CACHE.as_ref() {
        for key in keys_for_prefix(cache, cache_key(kind, prefix)) {
            cache.invalidate(&key).await;
        }
    }
}

/// Whether the shard at `commit_id` is already loaded, so a query would not
/// touch any file (and therefore needs no reader lease).
pub async fn is_loaded(kind: IndexKind, prefix: &str, commit_id: i64) -> bool {
    let Some(cache) = CACHE.as_ref() else {
        return false;
    };
    cache
        .get(&cache_key(kind, prefix))
        .await
        .is_some_and(|entry| entry.commit_id == commit_id)
}

pub async fn is_loaded_scoped(
    kind: IndexKind,
    prefix: &str,
    namespace: &crate::cache::store_namespace::StoreCacheNamespace,
    commit_id: i64,
) -> bool {
    let Some(cache) = CACHE.as_ref() else { return false; };
    cache.get(&scoped_cache_key(kind, prefix, namespace)).await
        .is_some_and(|entry| entry.commit_id == commit_id)
}

/// Return the cached entry for `(kind, prefix)` at `commit_id`, if present.
///
/// Used by callers that can rebuild the loader inputs only on a miss (the
/// typed shard segment list is not free to clone).
pub async fn get_loaded<T: IndexCacheEntry>(
    kind: IndexKind,
    prefix: &str,
    commit_id: i64,
) -> Option<Arc<T>> {
    let cache = CACHE.as_ref()?;
    let entry = cache.get(&cache_key(kind, prefix)).await?;
    if entry.commit_id != commit_id {
        return None;
    }
    entry.data.clone().downcast::<T>().ok()
}

/// Store-aware cache lookup for vector shards. Legacy callers without a store
/// identity must use `get_loaded`; native batch uses this scoped variant.
pub async fn get_loaded_scoped<T: IndexCacheEntry>(
    kind: IndexKind,
    prefix: &str,
    namespace: &crate::cache::store_namespace::StoreCacheNamespace,
    commit_id: i64,
) -> Option<Arc<T>> {
    let cache = CACHE.as_ref()?;
    let entry = cache.get(&scoped_cache_key(kind, prefix, namespace)).await?;
    if entry.commit_id != commit_id {
        return None;
    }
    entry.data.clone().downcast::<T>().ok()
}

/// Return the loaded index for `(kind, store, prefix)` at the resolved
/// commit, loading and caching it when needed.
///
/// The caller resolves the catalog commit before calling; a stale entry is
/// replaced when the commit id moved forward, so rebuilds and incremental
/// commits are picked up on the next query.  `loader` is only called on a
/// cache miss (or when the entry lost a race against a newer commit).
pub async fn get_or_load<T, E, F, Fut>(
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
    let key = cache_key(kind, prefix);
    if let Some(entry) = cache.get(&key).await
        && entry.commit_id == commit_id
        && let Ok(value) = entry.data.clone().downcast::<T>()
    {
        HITS.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("lakesoul_index_cache_hits_total").increment(1);
        debug!(prefix, "index cache hit");
        return Ok(value);
    }
    MISSES.fetch_add(1, Ordering::Relaxed);
    metrics::counter!("lakesoul_index_cache_misses_total").increment(1);
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
                _namespace: None,
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

/// Store-aware loader for vector shards. This keeps the namespace guard alive
/// for exactly as long as the cached parsed index.
pub async fn get_or_load_scoped<T, E, F, Fut>(
    kind: IndexKind,
    prefix: &str,
    namespace: Arc<crate::cache::store_namespace::StoreCacheNamespace>,
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
    let key = scoped_cache_key(kind, prefix, &namespace);
    if let Some(entry) = cache.get(&key).await
        && entry.commit_id == commit_id
        && let Ok(value) = entry.data.clone().downcast::<T>()
    {
        HITS.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("lakesoul_index_cache_hits_total").increment(1);
        return Ok(value);
    }
    MISSES.fetch_add(1, Ordering::Relaxed);
    cache.invalidate(&key).await;
    let cache_loader = loader.clone();
    let namespace_guard = Arc::clone(&namespace);
    let entry = cache
        .try_get_with(key, async move {
            let value = cache_loader().await?;
            Ok::<ErasedEntry, E>(ErasedEntry {
                commit_id,
                bytes: value.memory_bytes(),
                data: Arc::new(value) as Arc<dyn Any + Send + Sync>,
                _namespace: Some(namespace_guard),
            })
        })
        .await?;
    if entry.commit_id != commit_id {
        return loader().await.map(Arc::new).map_err(Arc::new);
    }
    Ok(entry
        .data
        .clone()
        .downcast::<T>()
        .unwrap_or_else(|_| panic!("index cache entry type mismatch for '{prefix}'")))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

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

    async fn load(
        commit_id: i64,
        bytes: usize,
    ) -> Result<TestEntry, std::convert::Infallible> {
        Ok(TestEntry { commit_id, bytes })
    }

    #[tokio::test]
    async fn cache_reuses_loaded_entry_until_commit_changes() {
        let prefix = "index_cache_reuse_test";
        let loads = AtomicUsize::new(0);

        let first = get_or_load(IndexKind::Vector, prefix, 1, || {
            loads.fetch_add(1, Ordering::Relaxed);
            load(1, 16)
        })
        .await
        .unwrap();
        let second = get_or_load(IndexKind::Vector, prefix, 1, || {
            loads.fetch_add(1, Ordering::Relaxed);
            load(1, 16)
        })
        .await
        .unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "same commit must reuse the cached entry"
        );

        let after = get_or_load(IndexKind::Vector, prefix, 2, || load(2, 32))
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
        let prefix = "index_cache_kind_scope_test";
        let vector = get_or_load(IndexKind::Vector, prefix, 1, || load(1, 8))
            .await
            .unwrap();
        let text = get_or_load(IndexKind::Text, prefix, 1, || load(1, 8))
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&vector, &text),
            "different kinds must not share a cache entry"
        );
    }

    #[tokio::test]
    async fn is_loaded_matches_the_commit() {
        let prefix = "index_cache_is_loaded_test";
        assert!(!is_loaded(IndexKind::Vector, prefix, 1).await);
        let _ = get_or_load(IndexKind::Vector, prefix, 1, || load(1, 8))
            .await
            .unwrap();
        assert!(is_loaded(IndexKind::Vector, prefix, 1).await);
        assert!(!is_loaded(IndexKind::Vector, prefix, 2).await);
    }
}
