// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Persistent local (SSD) cache for index files, with an `ObjectStore`
//! adapter that the index loader can read through.
//!
//! The process-wide in-memory index cache ([`super::cache`]) only helps while
//! a parsed shard fits in memory.  On a restart, on a new node, or after the
//! memory budget evicted a shard, the loader had to fetch every segment file
//! from object storage again.  This module keeps the **immutable index files**
//! on local disk, so a cold process only pays for a local read plus the index
//! parse; the object store is touched only on a true cache miss.
//!
//! Design notes:
//!
//! * The cache is a byte-budgeted weighted LRU built with `moka`, mirroring
//!   [`crate::cache::DiskCache`]: every entry owns a local file, insertion
//!   goes through a temporary file and an atomic rename, and the eviction
//!   listener unlinks the file after in-flight readers released it.
//! * Unlike the generic page cache, this cache is **persistent**: the
//!   directory is not cleared on startup.  Existing entries are adopted by
//!   scanning the directory (mtime is used as the approximation of recency;
//!   the running process then keeps exact LRU order in `moka`).
//! * Index files are immutable and content-addressed by their object path.
//!   A commit only references files; a new commit gets new file names, so no
//!   explicit invalidation is needed.  The semantic source of truth remains
//!   object storage and callers verify the index commit before using a shard.
//!
//! Environment:
//!
//! * `LAKESOUL_INDEX_SSD_BYTES` — byte budget; `0` or unset disables.
//! * `LAKESOUL_INDEX_SSD_DIR` — directory; defaults to
//!   `${LAKESOUL_CACHE_PATH}/index-cache` or
//!   `${TMPDIR:-/tmp}/lakesoul-index-cache`.
//! * `LAKESOUL_INDEX_SSD_KIND` — cache namespace, default `index-v1`.
//!
//! The wrapper [`IndexCachedStore`] implements [`ObjectStore`], delegating
//! writes/listings to the inner store and serving `get`/`get_range`/`head`
//! from the local cache.  It is only used for loading index shards, never for
//! reading data files or writing artifacts.

use std::collections::hash_map::DefaultHasher;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::UNIX_EPOCH;

use futures::StreamExt;
use futures::stream::BoxStream;
use moka::future::Cache;
use object_store::path::Path;
use object_store::{
    Attributes, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, Result as OsResult,
};
use tokio::sync::OnceCell;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

const ENV_KIND: &str = "LAKESOUL_INDEX_SSD_KIND";
const DEFAULT_KIND: &str = "index-v1";
const TEMP_SUFFIX: &str = ".tmp";

fn entry_weight(size: u64) -> u32 {
    size.div_ceil(1024).min(u32::MAX as u64) as u32
}

/// A cached local index file.
#[derive(Clone, Debug)]
struct Entry {
    file: Arc<std::fs::File>,
    size: u64,
    path: PathBuf,
}

/// Persistent local cache for immutable index files.
#[derive(Debug)]
pub struct IndexSsdCache {
    cache: Cache<String, Entry>,
    root: Arc<PathBuf>,
    max_capacity_bytes: u64,
    kind: String,
    adopted: OnceCell<()>,
    hits: AtomicU64,
    misses: AtomicU64,
    fetched_bytes: AtomicU64,
}

impl IndexSsdCache {
    /// Create a cache rooted at `root` with a byte capacity.
    pub fn new(root: PathBuf, capacity_bytes: u64) -> Self {
        Self::with_kind(root, capacity_bytes, DEFAULT_KIND)
    }

    /// Create a cache with an explicit namespace suffix for the file names.
    pub fn with_kind(
        root: PathBuf,
        capacity_bytes: u64,
        kind: impl Into<String>,
    ) -> Self {
        let kind = kind.into();
        let root = Arc::new(root);
        if let Err(error) = std::fs::create_dir_all(root.as_ref()) {
            warn!(dir = %root.display(), %error, "failed to create index SSD cache dir");
        }
        let cache = Cache::builder()
            .weigher(|_key: &String, entry: &Entry| {
                // Keep the same KiB accounting as `DiskCache`; moka weights
                // are u32, and an index file larger than 4 TiB is not useful
                // for an in-process index anyway.
                (entry.size.div_ceil(1024)).min(u32::MAX as u64) as u32
            })
            .max_capacity((capacity_bytes as u64).div_ceil(1024))
            .eviction_listener(|_key: Arc<String>, entry: Entry, cause| {
                if matches!(
                    cause,
                    moka::notification::RemovalCause::Size
                        | moka::notification::RemovalCause::Replaced
                ) {
                    let path = entry.path.clone();
                    debug!(path = %path.display(), "evicting index SSD cache entry");
                    let _ = std::fs::remove_file(path);
                }
            })
            .build();

        // Existing files are adopted lazily on lookup.  This keeps the
        // constructor synchronous; the byte budget is enforced as entries are
        // touched after restart.
        info!(
            dir = %root.display(),
            capacity_bytes,
            "index SSD cache initialized (persistent)"
        );

        Self {
            cache,
            root,
            max_capacity_bytes: capacity_bytes,
            kind,
            adopted: OnceCell::new(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fetched_bytes: AtomicU64::new(0),
        }
    }

    /// The configured byte budget.
    pub fn capacity_bytes(&self) -> u64 {
        self.max_capacity_bytes
    }

    /// `(hits, misses, fetched_bytes)` since process start.
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.hits.load(Ordering::Relaxed),
            self.misses.load(Ordering::Relaxed),
            self.fetched_bytes.load(Ordering::Relaxed),
        )
    }

    fn file_key(&self, location: &Path) -> String {
        let mut hasher = DefaultHasher::new();
        location.as_ref().hash(&mut hasher);
        self.kind.hash(&mut hasher);
        format!("{:016x}.bin", hasher.finish())
    }

    async fn open_entry(path: &FsPath, size: u64) -> Option<Entry> {
        let file = tokio::fs::File::open(path).await.ok()?;
        let file = file.into_std().await;
        Some(Entry {
            file: Arc::new(file),
            size,
            path: path.to_path_buf(),
        })
    }

    async fn ensure_adopted(&self) {
        self.adopted
            .get_or_init(|| async { self.adopt_existing_entries().await })
            .await;
    }

    async fn adopt_existing_entries(&self) {
        const MAX_ADOPTED_ENTRIES: usize = 4096;
        let root = Arc::clone(&self.root);
        let capacity_kib = self.max_capacity_bytes.div_ceil(1024);
        let candidates = tokio::task::spawn_blocking(move || {
            let canonical_root = std::fs::canonicalize(root.as_ref())?;
            let mut files = Vec::new();
            for entry in std::fs::read_dir(root.as_ref())? {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(_) => continue,
                };
                let name = entry.file_name();
                let Some(name_text) = name.to_str() else {
                    continue;
                };
                let Some(hash) = name_text.strip_suffix(".bin") else {
                    continue;
                };
                if hash.len() != 16 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    continue;
                }
                let path = entry.path();
                let metadata = match std::fs::symlink_metadata(&path) {
                    Ok(metadata) if metadata.file_type().is_file() => metadata,
                    _ => continue,
                };
                let canonical = match std::fs::canonicalize(&path) {
                    Ok(path) if path.parent() == Some(canonical_root.as_path()) => path,
                    _ => continue,
                };
                let modified = metadata.modified().unwrap_or(UNIX_EPOCH);
                files.push((name_text.to_string(), canonical, metadata.len(), modified));
            }
            files.sort_by(|left, right| {
                left.3.cmp(&right.3).then_with(|| left.0.cmp(&right.0))
            });
            let mut keep = vec![false; files.len()];
            let mut used_kib = 0u64;
            let mut kept_count = 0usize;
            for index in (0..files.len()).rev() {
                if kept_count >= MAX_ADOPTED_ENTRIES {
                    break;
                }
                let weight = entry_weight(files[index].2) as u64;
                if weight == 0 || used_kib.saturating_add(weight) > capacity_kib {
                    continue;
                }
                keep[index] = true;
                used_kib += weight;
                kept_count += 1;
            }
            let mut retained = Vec::with_capacity(kept_count);
            for (index, (name, path, size, _)) in files.into_iter().enumerate() {
                if keep[index] {
                    retained.push((name, path, size));
                } else {
                    let owned_regular_file = std::fs::symlink_metadata(&path)
                        .is_ok_and(|metadata| metadata.file_type().is_file())
                        && std::fs::canonicalize(&path)
                            .is_ok_and(|canonical| canonical.parent() == Some(canonical_root.as_path()));
                    if owned_regular_file {
                        let _ = std::fs::remove_file(path);
                    }
                }
            }
            Ok::<_, std::io::Error>(retained)
        })
        .await;
        let retained = match candidates {
            Ok(Ok(files)) => files,
            Ok(Err(error)) => {
                warn!(%error, "failed to scan index SSD cache for prior entries");
                return;
            }
            Err(error) => {
                warn!(%error, "index SSD cache adoption task failed");
                return;
            }
        };
        for (key, path, size) in retained {
            if let Some(entry) = Self::open_entry(&path, size).await {
                self.cache.insert(key, entry).await;
            }
        }
        self.cache.run_pending_tasks().await;
    }

    /// Return the local file for `location`, downloading it on a miss.
    async fn get_or_fetch(
        &self,
        inner: &Arc<dyn ObjectStore>,
        location: &Path,
        cache_location: &Path,
    ) -> OsResult<Entry> {
        self.ensure_adopted().await;
        let key = self.file_key(cache_location);
        if let Some(entry) = self.cache.get(&key).await {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(entry);
        }
        let path = self.root.join(&key);
        // The file may exist on disk although it was not adopted into this
        // process' cache yet (e.g. another process created it after startup).
        if let Ok(metadata) = std::fs::metadata(&path)
            && metadata.is_file()
            && let Some(entry) = Self::open_entry(&path, metadata.len()).await
        {
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.cache.insert(key, entry.clone()).await;
            return Ok(entry);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::clone(inner);
        let location = location.clone();
        let root = Arc::clone(&self.root);
        let expected_path = path.clone();
        let result = self
            .cache
            .try_get_with(key, async move {
                // A concurrent process may have completed the file after the
                // check above.
                if let Ok(metadata) = std::fs::metadata(&expected_path)
                    && metadata.is_file()
                    && let Some(entry) =
                        Self::open_entry(&expected_path, metadata.len()).await
                {
                    return Ok::<Entry, String>(entry);
                }
                let entry = download_entry(inner, &location, &root, &expected_path)
                    .await
                    .map_err(|error| error.to_string())?;
                Ok::<Entry, String>(entry)
            })
            .await;
        match result {
            Ok(entry) => {
                self.fetched_bytes.fetch_add(entry.size, Ordering::Relaxed);
                Ok(entry)
            }
            Err(error) => Err(Error::Generic {
                store: "IndexSsdCache",
                source: Box::new(std::io::Error::other(error.to_string())),
            }),
        }
    }

    fn display_name(&self) -> String {
        format!(
            "IndexSsdCache(dir={}, capacity={})",
            self.root.display(),
            self.max_capacity_bytes
        )
    }
}

struct TemporaryFile(PathBuf);
impl Drop for TemporaryFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Download one object to `path`, atomically replacing any previous copy.
async fn download_entry(
    inner: Arc<dyn ObjectStore>,
    location: &Path,
    root: &FsPath,
    path: &FsPath,
) -> std::io::Result<Entry> {
    tokio::fs::create_dir_all(root).await?;
    let temp = root.join(format!(
        "{}-{}-{}{TEMP_SUFFIX}",
        std::process::id(),
        uuid::Uuid::new_v4(),
        path.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_else(|| "download".to_string())
    ));
    let _cleanup = TemporaryFile(temp.clone());
    let mut file = tokio::fs::File::create(&temp).await?;
    let mut stream = inner
        .get(location)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?
        .into_stream();
    let mut size = 0u64;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| std::io::Error::other(error.to_string()))?;
        size += chunk.len() as u64;
        file.write_all(&chunk).await?;
    }
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temp, path).await?;
    let entry = IndexSsdCache::open_entry(path, size).await.ok_or_else(|| {
        std::io::Error::other("downloaded index cache file cannot be opened")
    })?;
    Ok(entry)
}

/// `ObjectStore` adapter that serves index files from an [`IndexSsdCache`].
///
/// Reads are cached; writes/listings/deletes delegate to `inner`.  The loader
/// only reads index artifacts, so the wrapper is deliberately not installed on
/// the session object store (which also serves user data files).
pub struct IndexCachedStore {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<IndexSsdCache>,
    namespace: Arc<crate::cache::store_namespace::StoreCacheNamespace>,
}

impl IndexCachedStore {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<IndexSsdCache>) -> Self {
        let namespace = crate::cache::store_namespace::store_cache_namespace(&inner);
        Self { inner, cache, namespace }
    }
}

impl Debug for IndexCachedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexCachedStore")
            .field("inner", &self.inner)
            .field("cache", &self.cache.display_name())
            .finish()
    }
}

impl Display for IndexCachedStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IndexCachedStore(inner={}, cache={})",
            self.inner,
            self.cache.display_name()
        )
    }
}

#[async_trait::async_trait]
impl ObjectStore for IndexCachedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> OsResult<GetResult> {
        // HEAD and conditional GETs are metadata operations; do not download
        // an index file just to answer them.
        if options.head
            || options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
            || options.version.is_some()
        {
            return self.inner.get_opts(location, options).await;
        }
        let cache_location = self.namespace.path(location);
        let entry = self
            .cache
            .get_or_fetch(&self.inner, location, &cache_location)
            .await?;
        let size = entry.size;
        let range = match options.range {
            Some(range) => range.as_range(size).map_err(|error| Error::Generic {
                store: "IndexSsdCache",
                source: Box::new(std::io::Error::other(error.to_string())),
            })?,
            None => 0..size,
        };
        let file = entry.file.try_clone().map_err(|error| Error::Generic {
            store: "IndexSsdCache",
            source: Box::new(error),
        })?;
        Ok(GetResult {
            payload: GetResultPayload::File(file, entry.path.clone()),
            // Index loaders only consume the bytes and size; the cached file
            // is content-addressed by its path, so the object's ETag/version
            // are not needed to serve a cached range.
            meta: ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size,
                e_tag: None,
                version: None,
            },
            range,
            attributes: Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Process-wide cache configured from the environment.
pub fn global() -> Option<Arc<IndexSsdCache>> {
    static CACHE: LazyLock<Option<Arc<IndexSsdCache>>> = LazyLock::new(|| {
        let config = lakesoul_vector::cache_config::config();
        let capacity = config.index_ssd_bytes() as u64;
        if capacity == 0 {
            return None;
        }
        let root = PathBuf::from(config.index_ssd_path());
        let kind = std::env::var(ENV_KIND)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_KIND.to_string());
        Some(Arc::new(IndexSsdCache::with_kind(root, capacity, kind)))
    });
    CACHE.clone()
}

/// Wrap `store` with the process-wide SSD cache when enabled.
pub fn wrap(store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
    match global() {
        Some(cache) => Arc::new(IndexCachedStore::new(store, cache)),
        None => store,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    async fn put(store: &Arc<dyn ObjectStore>, path: &Path, bytes: &[u8]) {
        store
            .put(path, Bytes::copy_from_slice(bytes).into())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn serves_bytes_and_ranges_from_local_cache() {
        let dir = tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = Path::from("t/_vector_index/vec/0/0/seg-1.seg");
        put(&inner, &location, b"0123456789").await;
        let cache = Arc::new(IndexSsdCache::new(dir.path().to_path_buf(), 1024 * 1024));
        let store = IndexCachedStore::new(Arc::clone(&inner), cache.clone());

        let got = store.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.as_ref(), b"0123456789");
        // Remove the source: the second read must come from the local file.
        inner.delete(&location).await.unwrap();
        let got = store.get(&location).await.unwrap().bytes().await.unwrap();
        assert_eq!(got.as_ref(), b"0123456789");
        let range = store.get_range(&location, 2..5).await.unwrap();
        assert_eq!(range.as_ref(), b"234");
        assert_eq!(cache.stats().0, 2);
    }

    #[tokio::test]
    async fn adopts_only_owned_entries_and_enforces_budget_after_restart() {
        use std::io::Read;
        use object_store::ObjectStoreExt;

        let dir = tempdir().unwrap();
        let root = dir.path();
        let old_path = root.join("0000000000000001.bin");
        let new_path = root.join("0000000000000002.bin");
        let unknown_path = root.join("manual-entry.bin");
        let active_temp = root.join("active-writer.tmp");
        std::fs::write(&old_path, vec![b'o'; 2 * 1024]).unwrap();
        let mut old_reader = std::fs::File::open(&old_path).unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        std::fs::write(&new_path, vec![b'n'; 2 * 1024]).unwrap();
        std::fs::write(&unknown_path, b"not owned by this cache format").unwrap();
        std::fs::write(&active_temp, b"another process may still be writing").unwrap();

        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = Path::from("index/new.seg");
        put(&inner, &location, &vec![b'c'; 2 * 1024]).await;
        let cache = Arc::new(IndexSsdCache::new(root.to_path_buf(), 3 * 1024));
        let store = IndexCachedStore::new(inner, Arc::clone(&cache));
        assert_eq!(store.get(&location).await.unwrap().bytes().await.unwrap().len(), 2 * 1024);
        cache.cache.run_pending_tasks().await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut bytes = [0u8; 1];
        old_reader.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes[0], b'o', "unlink must not break an existing open handle");
        assert!(unknown_path.exists(), "unknown files are not owned by this cache");
        assert!(active_temp.exists(), "temporary files may belong to active writers");
        let owned_files = std::fs::read_dir(root)
            .unwrap()
            .flatten()
            .filter(|entry| {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { return false; };
                let Some(hash) = name.strip_suffix(".bin") else { return false; };
                hash.len() == 16 && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
            .count();
        assert!(owned_files <= 1, "recognized cache files must obey the 3 KiB LRU budget");
    }

    #[tokio::test]
    async fn persists_across_cache_instances() {
        let dir = tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let location = Path::from("t/index/seg-1.seg");
        put(&inner, &location, b"persisted").await;
        {
            let cache =
                Arc::new(IndexSsdCache::new(dir.path().to_path_buf(), 1024 * 1024));
            let store = IndexCachedStore::new(Arc::clone(&inner), cache);
            assert_eq!(
                store
                    .get(&location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap()
                    .as_ref(),
                b"persisted"
            );
        }
        inner.delete(&location).await.unwrap();
        let cache = Arc::new(IndexSsdCache::new(dir.path().to_path_buf(), 1024 * 1024));
        let store = IndexCachedStore::new(inner, cache);
        assert_eq!(
            store
                .get(&location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref(),
            b"persisted"
        );
    }

    #[tokio::test]
    async fn evicts_old_files_when_over_budget() {
        let dir = tempdir().unwrap();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // 8 KiB of entries in a 12 KiB cache: the first must be evicted when
        // the second is inserted.
        let a = Path::from("index/a.seg");
        let b = Path::from("index/b.seg");
        put(&inner, &a, &vec![b'a'; 8 * 1024]).await;
        put(&inner, &b, &vec![b'b'; 8 * 1024]).await;
        let cache = Arc::new(IndexSsdCache::new(dir.path().to_path_buf(), 12 * 1024));
        let store = IndexCachedStore::new(inner, cache.clone());
        let _ = store.get(&a).await.unwrap().bytes().await.unwrap();
        let _ = store.get(&b).await.unwrap().bytes().await.unwrap();
        cache.cache.run_pending_tasks().await;
        // Let moka's eviction listener unlink the file.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|entry| entry.path().is_file())
            .collect();
        assert_eq!(files.len(), 1, "cache must stay within the byte budget");
        assert!(cache.stats().2 >= 16 * 1024);
    }
}
