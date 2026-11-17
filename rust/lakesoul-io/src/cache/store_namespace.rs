// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Process-local, allocation-stable namespaces for caches keyed by object paths.
//!
//! An `ObjectStore`'s Display form is not a storage identity: wrappers can omit
//! endpoints, and two in-memory stores commonly display identically. Namespace
//! by the concrete Arc allocation instead. A namespace retains the store while
//! any cache entry retains the namespace, preventing pointer reuse. Persistent
//! cache keys include a process nonce so entries cannot cross a restart where
//! a different endpoint may have been configured; this intentionally trades
//! persistent hit rate for isolation.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::{ObjectStore, path::Path};

static PROCESS_NONCE: LazyLock<u128> = LazyLock::new(|| {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    time.rotate_left(37) ^ ((std::process::id() as u128) << 64)
});
static NEXT_STORE_ID: AtomicU64 = AtomicU64::new(1);

/// Allocation address -> (token id, weak store).
///
/// The store is only weakly held, so a session's stores are never pinned.  The
/// id is kept even after every `StoreCacheNamespace` for the allocation has
/// been dropped: recreating a cache (e.g. a new `IndexSsdCache` instance) must
/// land on the same disk keys, otherwise persisted entries become invisible
/// when the namespace is resolved again.  Pointer reuse is safe because the
/// entry is only honoured while the old store is still alive; a new store at
/// the same address gets a fresh id.
struct NamespaceEntry {
    store_id: u64,
    store: Weak<dyn ObjectStore>,
}

static NAMESPACES: LazyLock<Mutex<HashMap<usize, NamespaceEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub struct StoreCacheNamespace {
    store_id: u64,
    process_nonce: u128,
    _store: Arc<dyn ObjectStore>,
}

impl StoreCacheNamespace {
    /// A process-unique discriminator suitable for memory and disk cache keys.
    pub(crate) fn token(&self) -> String {
        format!("{:032x}-{:016x}", self.process_nonce, self.store_id)
    }

    pub(crate) fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self._store)
    }

    /// A path under a synthetic prefix, for APIs whose only namespace is Path.
    pub(crate) fn path(&self, location: &Path) -> Path {
        Path::from(format!(
            "_lakesoul_store_cache/{}/{}",
            self.token(),
            location.as_ref()
        ))
    }
}

pub(crate) fn store_cache_namespace(
    store: &Arc<dyn ObjectStore>,
) -> Arc<StoreCacheNamespace> {
    let address = Arc::as_ptr(store) as *const () as usize;
    let mut namespaces = NAMESPACES
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(entry) = namespaces.get(&address)
        && let Some(live_store) = entry.store.upgrade()
    {
        return Arc::new(StoreCacheNamespace {
            store_id: entry.store_id,
            process_nonce: *PROCESS_NONCE,
            _store: live_store,
        });
    }
    namespaces.retain(|_, entry| entry.store.strong_count() > 0);
    let store_id = NEXT_STORE_ID.fetch_add(1, Ordering::Relaxed);
    namespaces.insert(
        address,
        NamespaceEntry {
            store_id,
            store: Arc::downgrade(store),
        },
    );
    Arc::new(StoreCacheNamespace {
        store_id,
        process_nonce: *PROCESS_NONCE,
        _store: Arc::clone(store),
    })
}
