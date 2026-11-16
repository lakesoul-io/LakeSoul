// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Process-wide cache for loaded index shards.
//!
//! Every query used to re-read and re-merge the whole index shard from the
//! object store (tens to hundreds of milliseconds for large shards). The
//! cache keeps the loaded, in-memory index per `(kind, store identity,
//! prefix)` and reuses it while the caller resolves the same commit id; a new
//! commit (delta flush or rebuild) invalidates the entry automatically.
//!
//! The store identity is derived from the store's configuration (display form
//! and storage options), not from the handle the caller happens to hold: the
//! DataFusion side reaches the store through a metrics wrapper while the
//! native reader holds the plain store, and both must resolve the same entry
//! (see [`store_identity`]).  Two different stores can serve the same relative
//! path, so their identities must not collide either.
//!
//! Capacity is a byte budget ([`ENV_CACHE_BYTES`], default 512 MiB, `0`
//! disables the cache); entries are evicted by weighted LRU.  Entries are
//! type-erased because one process may search several index kinds, which
//! keeps the cache and its byte accounting shared.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use lakesoul_common::IndexKind;
use lakesoul_common::misc::SecretMap;
use moka::future::Cache;
use object_store::ObjectStore;
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

/// Cache key of a loaded index shard: `(kind, store identity, index_prefix)`.
///
/// `(kind, index_prefix)` is the shard identity of the metadata catalog —
/// `index_shard` has `unique (kind, index_prefix)` — but the same prefix can
/// be served by different object stores in one process (different endpoints,
/// buckets or local runtime roots), so the key also carries a stable store
/// identity.  See [`store_identity`]: two handles of the same store resolve
/// the same identity, two different stores do not.
fn cache_key(kind: IndexKind, identity: &str, prefix: &str) -> String {
    format!("{kind}\u{0}{identity}\u{0}{prefix}")
}

fn scoped_cache_key(
    kind: IndexKind,
    prefix: &str,
    namespace: &crate::cache::store_namespace::StoreCacheNamespace,
) -> String {
    cache_key(kind, &namespace.token(), prefix)
}

/// Stable identity of the object store an index shard is loaded from.
///
/// The identity is derived from the store's configuration, not from the
/// handle the caller holds: the DataFusion side reaches the same store through
/// a `MonitoredObjectStore` metrics wrapper while the native reader holds the
/// plain store, so a handle-based identity would give one shard two cache keys
/// and `is_loaded` could never report a hit.  The wrapper layer is stripped
/// before hashing.
///
/// The storage options are hashed in raw form (only a digest is kept, and
/// cache keys never leave the process): the display form of most stores omits
/// the endpoint — `AmazonS3(bucket)` is the same for two endpoints — so two
/// sessions that share an index prefix but read different object stores would
/// otherwise share one entry.  Different credentials to the same store produce
/// different identities; that only costs a duplicate entry, never a wrong one.
pub fn store_identity(
    store: &Arc<dyn ObjectStore>,
    options: Option<&SecretMap>,
) -> String {
    let display = canonical_store_display(store);
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    display.hash(&mut hasher);
    if let Some(options) = options {
        let mut entries: Vec<(&String, &String)> = options.iter().collect();
        // HashMap iteration order is not stable across instances; sort so two
        // handles configured identically hash identically.
        entries.sort();
        for (key, value) in entries {
            key.hash(&mut hasher);
            value.hash(&mut hasher);
        }
    }
    format!("{:016x}", hasher.finish())
}

/// Display form of the store itself, with the metrics decorators that
/// LakeSoul adds removed.
fn canonical_store_display(store: &Arc<dyn ObjectStore>) -> String {
    let mut display = store.to_string();
    // `MonitoredObjectStore(<kind>, <inner>)`: strip one wrapper per loop so
    // nested wrappers of an older runtime still canonicalize to the inner
    // store.
    while let Some(rest) = display.strip_prefix("MonitoredObjectStore(") {
        let Some((_kind, inner)) = rest.rsplit_once(", ") else {
            break;
        };
        let Some(inner) = inner.strip_suffix(')') else {
            break;
        };
        display = inner.to_string();
    }
    display
}

/// Whether `key` is the cache entry of `(kind, prefix)` under any store
/// identity (or under no identity, for entries of an older build).
///
/// The trailing slash is ignored on both sides: loaders trim it before
/// building the key, while some callers of [`invalidate`] / [`is_loaded`]
/// still hold the catalog's spelling.
fn key_belongs_to_shard(key: &str, kind: IndexKind, prefix: &str) -> bool {
    let kind_prefix = format!("{kind}\u{0}");
    let Some(rest) = key.strip_prefix(&kind_prefix) else {
        return false;
    };
    let shard_prefix = match rest.rsplit_once('\u{0}') {
        Some((_identity, shard_prefix)) => shard_prefix,
        None => rest,
    };
    shard_prefix.trim_end_matches('/') == prefix.trim_end_matches('/')
}

fn keys_for_shard(
    cache: &Cache<String, ErasedEntry>,
    kind: IndexKind,
    prefix: &str,
) -> Vec<String> {
    cache
        .iter()
        .filter_map(|(key, _)| {
            key_belongs_to_shard(key.as_str(), kind, prefix)
                .then(|| key.as_str().to_string())
        })
        .collect()
}

/// Drop the cached entries of one shard, whichever store identity loaded
/// them.
///
/// Used by `drop_vector_index` after the catalog rows and object files are
/// removed so this process cannot keep serving the old parsed index.
pub async fn invalidate(kind: IndexKind, prefix: &str) {
    if let Some(cache) = CACHE.as_ref() {
        for key in keys_for_shard(cache, kind, prefix) {
            cache.invalidate(&key).await;
        }
    }
}

/// Whether the shard at `commit_id` is already loaded, so a query would not
/// touch any file (and therefore needs no reader lease).
///
/// A probe without a store handle cannot prove *which* store loaded an entry:
/// two stores can serve the same `(kind, prefix, commit_id)`, and accepting
/// the other store's entry as a hit would skip the lease that protects this
/// store's index files while they are read.  The probe therefore only matches
/// an identity-less key (the key layout of the older build); current loaders
/// always record a store identity, so an unscoped probe conservatively reports
/// a miss and the caller takes the lease.  Callers that hold the store
/// namespace use [`is_loaded_scoped`], which matches its own key.
pub async fn is_loaded(kind: IndexKind, prefix: &str, commit_id: i64) -> bool {
    let Some(cache) = CACHE.as_ref() else {
        return false;
    };
    cache
        .get(&format!("{kind}\u{0}{prefix}"))
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

/// Return the cached entry for `(kind, store identity, prefix)` at
/// `commit_id`, if present.
///
/// Used by callers that can rebuild the loader inputs only on a miss (the
/// typed shard segment list is not free to clone).
pub async fn get_loaded<T: IndexCacheEntry>(
    identity: &str,
    kind: IndexKind,
    prefix: &str,
    commit_id: i64,
) -> Option<Arc<T>> {
    let cache = CACHE.as_ref()?;
    let entry = cache.get(&cache_key(kind, identity, prefix)).await?;
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

/// Return the loaded index for `(kind, store identity, prefix)` at the
/// resolved commit, loading and caching it when needed.
///
/// The caller resolves the catalog commit before calling; a stale entry is
/// replaced when the commit id moved forward, so rebuilds and incremental
/// commits are picked up on the next query.  `loader` is only called on a
/// cache miss (or when the entry lost a race against a newer commit).
/// `identity` comes from [`store_identity`]; two stores serving the same
/// prefix load and cache separately.
pub async fn get_or_load<T, E, F, Fut>(
    identity: &str,
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
    let key = cache_key(kind, identity, prefix);
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
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;

    use crate::object_store_metrics::MonitoredObjectStore;

    use super::*;

    #[derive(Clone)]
    struct TestEntry {
        commit_id: i64,
        bytes: usize,
        payload: u64,
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
        Ok(TestEntry {
            commit_id,
            bytes,
            payload: 0,
        })
    }

    async fn load_payload(
        commit_id: i64,
        payload: u64,
    ) -> Result<TestEntry, std::convert::Infallible> {
        Ok(TestEntry {
            commit_id,
            bytes: 8,
            payload,
        })
    }

    fn local_store(root: &std::path::Path) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(root).unwrap())
    }

    #[tokio::test]
    async fn cache_reuses_loaded_entry_until_commit_changes() {
        let identity = "test-store";
        let prefix = "index_cache_reuse_test";
        let loads = AtomicUsize::new(0);

        let first = get_or_load(identity, IndexKind::Vector, prefix, 1, || {
            loads.fetch_add(1, Ordering::Relaxed);
            load(1, 16)
        })
        .await
        .unwrap();
        let second = get_or_load(identity, IndexKind::Vector, prefix, 1, || {
            loads.fetch_add(1, Ordering::Relaxed);
            load(1, 16)
        })
        .await
        .unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "same commit must reuse the cached entry"
        );

        let after = get_or_load(identity, IndexKind::Vector, prefix, 2, || load(2, 32))
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
        let identity = "test-store";
        let prefix = "index_cache_kind_scope_test";
        let vector = get_or_load(identity, IndexKind::Vector, prefix, 1, || load(1, 8))
            .await
            .unwrap();
        let text = get_or_load(identity, IndexKind::Text, prefix, 1, || load(1, 8))
            .await
            .unwrap();
        assert!(
            !Arc::ptr_eq(&vector, &text),
            "different kinds must not share a cache entry"
        );
    }

    #[tokio::test]
    async fn loaded_entries_match_the_commit() {
        let identity = "test-store";
        let prefix = "index_cache_is_loaded_test";
        assert!(
            get_loaded::<TestEntry>(identity, IndexKind::Vector, prefix, 1)
                .await
                .is_none()
        );
        let _ = get_or_load(identity, IndexKind::Vector, prefix, 1, || load(1, 8))
            .await
            .unwrap();
        let entry = get_loaded::<TestEntry>(identity, IndexKind::Vector, prefix, 1)
            .await
            .expect("loaded entry must be retrievable by its store identity");
        assert_eq!(entry.commit_id(), 1);
        assert!(
            get_loaded::<TestEntry>(identity, IndexKind::Vector, prefix, 2)
                .await
                .is_none(),
            "another commit must not match"
        );
        // An unscoped probe cannot prove which store loaded the entry; it must
        // miss a store-identified entry instead of skipping the reader lease.
        assert!(!is_loaded(IndexKind::Vector, prefix, 1).await);
    }

    #[tokio::test]
    async fn is_loaded_scoped_matches_its_own_namespace() {
        let prefix = "index_cache_is_loaded_scoped_test";
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let namespace = crate::cache::store_namespace::store_cache_namespace(&store);
        assert!(!is_loaded_scoped(IndexKind::Vector, prefix, &namespace, 3).await);
        let _ = get_or_load_scoped(
            IndexKind::Vector,
            prefix,
            Arc::clone(&namespace),
            3,
            || load(3, 8),
        )
        .await
        .unwrap();
        assert!(is_loaded_scoped(IndexKind::Vector, prefix, &namespace, 3).await);
        assert!(!is_loaded_scoped(IndexKind::Vector, prefix, &namespace, 4).await);

        // A different store namespace does not see this entry.
        let other: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let other_namespace =
            crate::cache::store_namespace::store_cache_namespace(&other);
        assert!(!is_loaded_scoped(IndexKind::Vector, prefix, &other_namespace, 3).await);
    }

    /// Two object stores can serve the same relative index prefix; the cache
    /// must not answer one store's query with the other store's parsed index.
    #[tokio::test]
    async fn entries_are_scoped_by_store_identity() {
        let prefix = "index_cache_store_scope_test";
        let first = get_or_load("store-a", IndexKind::Text, prefix, 1, || {
            load_payload(1, 0xAA)
        })
        .await
        .unwrap();
        let second = get_or_load("store-b", IndexKind::Text, prefix, 1, || {
            load_payload(1, 0xBB)
        })
        .await
        .unwrap();
        assert!(
            !Arc::ptr_eq(&first, &second),
            "different stores must not share a cache entry"
        );
        assert_eq!(first.payload, 0xAA);
        assert_eq!(second.payload, 0xBB);

        // Each identity keeps hitting its own entry.
        let again = get_or_load("store-a", IndexKind::Text, prefix, 1, || {
            load_payload(1, 0xCC)
        })
        .await
        .unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        assert_eq!(again.payload, 0xAA);
    }

    /// The DataFusion side reaches one store through the metrics wrapper while
    /// the native reader holds the plain store; both handles must resolve the
    /// same identity, or the shard gets two entries and `is_loaded` never hits
    /// (the double-key regression this cache key used to have).
    #[tokio::test]
    async fn store_identity_is_stable_across_handles_of_one_store() {
        let dir = tempfile::tempdir().unwrap();
        let plain = local_store(dir.path());
        let wrapped: Arc<dyn ObjectStore> =
            Arc::new(MonitoredObjectStore::new(Arc::clone(&plain), "local"));
        let plain_identity = store_identity(&plain, None);
        let wrapped_identity = store_identity(&wrapped, None);
        assert_eq!(
            plain_identity, wrapped_identity,
            "the metrics wrapper must not change the store identity"
        );

        let prefix = "index_cache_handle_identity_test";
        let loads = AtomicUsize::new(0);
        let first = get_or_load(&plain_identity, IndexKind::Text, prefix, 1, || {
            loads.fetch_add(1, Ordering::Relaxed);
            load(1, 8)
        })
        .await
        .unwrap();
        let second = get_or_load(&wrapped_identity, IndexKind::Text, prefix, 1, || {
            loads.fetch_add(1, Ordering::Relaxed);
            load(1, 8)
        })
        .await
        .unwrap();
        assert!(
            Arc::ptr_eq(&first, &second),
            "two handles of one store must share the entry"
        );
        assert_eq!(loads.load(Ordering::Relaxed), 1);
    }

    /// The display form of `AmazonS3` is the bucket only, so the storage
    /// options (endpoint, ...) have to be part of the identity as well.
    #[tokio::test]
    async fn store_identity_includes_storage_options() {
        let dir = tempfile::tempdir().unwrap();
        let store = local_store(dir.path());
        let first = SecretMap::from_map(HashMap::from([(
            "fs.s3a.endpoint".to_string(),
            "http://127.0.0.1:9000".to_string(),
        )]));
        let second = SecretMap::from_map(HashMap::from([(
            "fs.s3a.endpoint".to_string(),
            "http://127.0.0.2:9000".to_string(),
        )]));
        assert_ne!(
            store_identity(&store, None),
            store_identity(&store, Some(&first)),
            "configured stores must not share the unconfigured identity"
        );
        assert_ne!(
            store_identity(&store, Some(&first)),
            store_identity(&store, Some(&second)),
            "two endpoints must not share an identity"
        );
        // The same configuration built as a different map instance (HashMap
        // iteration order is not stable) must hash identically.
        let copy = SecretMap::from_map(first.clone().into_inner());
        assert_eq!(
            store_identity(&store, Some(&first)),
            store_identity(&store, Some(&copy))
        );
    }

    /// `invalidate` is used after a drop and has no store handle: it must drop
    /// the shard's entries of every store identity, and nothing else.
    #[tokio::test]
    async fn invalidate_drops_entries_of_all_store_identities() {
        let prefix = "index_cache_invalidate_all_test";
        let loads = AtomicUsize::new(0);
        for identity in ["store-a", "store-b"] {
            let _ = get_or_load(identity, IndexKind::Text, prefix, 1, || {
                loads.fetch_add(1, Ordering::Relaxed);
                load(1, 8)
            })
            .await
            .unwrap();
        }
        for identity in ["store-a", "store-b"] {
            assert!(
                get_loaded::<TestEntry>(identity, IndexKind::Text, prefix, 1)
                    .await
                    .is_some(),
                "both store identities must be cached before the drop"
            );
        }
        invalidate(IndexKind::Text, prefix).await;
        for identity in ["store-a", "store-b"] {
            assert!(
                get_loaded::<TestEntry>(identity, IndexKind::Text, prefix, 1)
                    .await
                    .is_none(),
                "invalidate must drop the entry of {identity}"
            );
        }

        // Another shard of the same kind is not touched.
        let other = "index_cache_invalidate_all_other_test";
        let _ = get_or_load("store-a", IndexKind::Text, other, 1, || load(1, 8))
            .await
            .unwrap();
        invalidate(IndexKind::Text, prefix).await;
        assert!(
            get_loaded::<TestEntry>("store-a", IndexKind::Text, other, 1)
                .await
                .is_some(),
            "invalidating one shard must not drop another"
        );

        let _ = get_or_load("store-a", IndexKind::Text, prefix, 1, || {
            loads.fetch_add(1, Ordering::Relaxed);
            load(1, 8)
        })
        .await
        .unwrap();
        assert_eq!(
            loads.load(Ordering::Relaxed),
            3,
            "both invalidated identities must load again"
        );
    }
}
