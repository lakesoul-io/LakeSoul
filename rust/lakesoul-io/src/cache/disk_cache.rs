use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use linked_hash_map::LinkedHashMap;
use moka::future::Cache;
use object_store::path::Path;
use object_store::{Error, ObjectMeta, Result};
use parking_lot::RwLock;
use uuid::Uuid;

use super::paging::PageCache;

/// Default memory page size is 16 KB
pub const DEFAULT_PAGE_SIZE: usize = 16 * 1024;
const DEFAULT_METADATA_CACHE_SIZE: usize = 32 * 1024 * 1024;

fn make_key(location_id: u64, page_id: u32) -> String {
    format!("{}_{}", location_id, page_id)
}

#[derive(Debug)]
struct CacheEntry {
    size: u64,
    filename: String,
}

#[derive(Debug)]
struct CacheState {
    entries: LinkedHashMap<String, CacheEntry>,
    total_size: u64,
    max_capacity: u64,
}

/// Local disk-based LRU page cache.
///
/// Uses a single `parking_lot::RwLock` for all cache state.
/// Files are stored with UUID-based filenames to prevent races
/// between concurrent reads and evictions.
/// Files are opened on demand and closed after each I/O operation.
#[derive(Debug)]
pub struct DiskCache {
    state: RwLock<CacheState>,
    root: PathBuf,
    page_size: usize,
    metadata_cache: Cache<u64, ObjectMeta>,
    location_lookup: RwLock<HashMap<Path, u64>>,
    next_location_id: AtomicU64,
}

impl DiskCache {
    pub fn new(disk_capacity: usize, page_size: usize) -> Self {
        let root_path =
            std::env::var("LAKESOUL_CACHE_PATH").unwrap_or_else(|_| "lakesoul_cache_dir".to_string());
        Self::with_path(disk_capacity, page_size, PathBuf::from(root_path))
    }

    pub fn with_path(disk_capacity: usize, page_size: usize, root: PathBuf) -> Self {

        if let Err(e) = std::fs::create_dir_all(&root) {
            warn!("Failed to create cache dir {}: {}", root.display(), e);
        }
        if let Ok(entries) = std::fs::read_dir(&root) {
            for entry in entries.flatten() {
                let _ = std::fs::remove_file(entry.path());
            }
        }

        debug!(
            "DiskCache created: root={}, capacity={}, page_size={}",
            root.display(),
            disk_capacity,
            page_size
        );

        let metadata_cache = Cache::builder()
            .max_capacity(DEFAULT_METADATA_CACHE_SIZE as u64)
            .build();

        Self {
            state: RwLock::new(CacheState {
                entries: LinkedHashMap::new(),
                total_size: 0,
                max_capacity: disk_capacity as u64,
            }),
            root,
            page_size,
            metadata_cache,
            location_lookup: RwLock::new(HashMap::new()),
            next_location_id: AtomicU64::new(0),
        }
    }

    fn get_location_id(&self, location: &Path) -> u64 {
        {
            let lookup = self.location_lookup.read();
            if let Some(&id) = lookup.get(location) {
                return id;
            }
        }
        let mut lookup = self.location_lookup.write();
        if let Some(&id) = lookup.get(location) {
            return id;
        }
        let id = self.next_location_id.fetch_add(1, Ordering::SeqCst);
        lookup.insert(location.clone(), id);
        id
    }

    /// Evict entries under write lock. Returns filenames to delete outside the lock.
    fn evict_locked(state: &mut CacheState, needed: u64) -> Vec<String> {
        let mut evicted = Vec::new();
        while state.total_size.saturating_add(needed) > state.max_capacity {
            match state.entries.pop_front() {
                Some((_, entry)) => {
                    state.total_size = state.total_size.saturating_sub(entry.size);
                    evicted.push(entry.filename);
                }
                None => break,
            }
        }
        evicted
    }
}

#[async_trait::async_trait]
impl PageCache for DiskCache {
    fn page_size(&self) -> usize {
        self.page_size
    }

    fn capacity(&self) -> usize {
        self.state.read().max_capacity as usize
    }

    fn size(&self) -> usize {
        self.state.read().total_size as usize
    }

    async fn get_with(
        &self,
        location: &Path,
        page_id: u32,
        loader: impl std::future::Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes> {
        if let Some(bytes) = self.get(location, page_id).await? {
            return Ok(bytes);
        }
        let bytes = loader.await?;
        self.put(location, page_id, bytes.clone()).await?;
        Ok(bytes)
    }

    async fn get(&self, location: &Path, page_id: u32) -> Result<Option<Bytes>> {
        let location_id = self.get_location_id(location);
        let key = make_key(location_id, page_id);

        let filename = {
            let state = self.state.read();
            state.entries.get(&key).map(|e| e.filename.clone())
        };

        let Some(filename) = filename else {
            return Ok(None);
        };

        let file_path = self.root.join(&filename);
        let data = match tokio::fs::read(&file_path).await {
            Ok(d) => d,
            Err(e) => {
                error!(
                    "Failed to read cache file {} (key={}): {}",
                    file_path.display(),
                    key,
                    e
                );
                return Ok(None);
            }
        };

        // Update MRU position (best-effort; skip if entry was evicted)
        {
            let mut state = self.state.write();
            if let Some(entry) = state.entries.remove(&key) {
                state.entries.insert(key, entry);
            }
        }

        Ok(Some(Bytes::from(data)))
    }

    async fn get_range_with(
        &self,
        location: &Path,
        page_id: u32,
        range: Range<usize>,
        loader: impl std::future::Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes> {
        assert!(range.start <= range.end && range.end <= self.page_size());

        // Try to read the range directly from cache (partial read)
        if let Some(bytes) = self.get_range(location, page_id, range.clone()).await? {
            return Ok(bytes);
        }

        // Cache miss: load the full page from remote, cache it, return the range
        let bytes = loader.await?;
        self.put(location, page_id, bytes.clone()).await?;
        Ok(bytes.slice(range))
    }

    async fn get_range(
        &self,
        location: &Path,
        page_id: u32,
        range: Range<usize>,
    ) -> Result<Option<Bytes>> {
        let location_id = self.get_location_id(location);
        let key = make_key(location_id, page_id);

        let filename = {
            let state = self.state.read();
            state.entries.get(&key).map(|e| e.filename.clone())
        };

        let Some(filename) = filename else {
            return Ok(None);
        };

        let file_path = self.root.join(&filename);

        use std::io::SeekFrom;
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let mut file = match tokio::fs::File::open(&file_path).await {
            Ok(f) => f,
            Err(e) => {
                error!(
                    "Failed to open cache file {} (key={}): {}",
                    file_path.display(),
                    key,
                    e
                );
                return Ok(None);
            }
        };

        let read_len = range.end - range.start;
        if let Err(e) = file.seek(SeekFrom::Start(range.start as u64)).await {
            error!("Failed to seek in cache file {}: {}", file_path.display(), e);
            return Ok(None);
        }

        let mut buf = vec![0u8; read_len];
        if let Err(e) = file.read_exact(&mut buf).await {
            error!(
                "Failed to read range [{}, {}) from cache file {}: {}",
                range.start,
                range.end,
                file_path.display(),
                e
            );
            return Ok(None);
        }

        // Update MRU position (best-effort; skip if entry was evicted)
        {
            let mut state = self.state.write();
            if let Some(entry) = state.entries.remove(&key) {
                state.entries.insert(key, entry);
            }
        }

        Ok(Some(Bytes::from(buf)))
    }

    async fn head(
        &self,
        location: &Path,
        loader: impl std::future::Future<Output = Result<ObjectMeta>> + Send,
    ) -> Result<ObjectMeta> {
        let location_id = self.get_location_id(location);
        debug!("[cache::head] location={}", location);
        match self.metadata_cache.try_get_with(location_id, loader).await {
            Ok(meta) => Ok(meta),
            Err(e) => match e.as_ref() {
                Error::NotFound { path, .. } => Err(Error::NotFound {
                    path: path.to_string(),
                    source: e.into(),
                }),
                _ => Err(Error::Generic {
                    store: "DiskCache",
                    source: Box::new(e),
                }),
            },
        }
    }

    async fn put(&self, location: &Path, page_id: u32, data: Bytes) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let location_id = self.get_location_id(location);
        let key = make_key(location_id, page_id);

        // Check if already in cache (fast path)
        {
            let state = self.state.read();
            if state.entries.contains_key(&key) {
                return Ok(());
            }
        }

        let filename = Uuid::new_v4().to_string();
        let file_path = self.root.join(&filename);

        // Write file to disk (outside lock)
        tokio::fs::write(&file_path, &data).await.map_err(|e| Error::Generic {
            store: "DiskCache",
            source: Box::new(e),
        })?;

        // Double-check under write lock; if key already exists, clean up and return
        let already_exists = {
            let state = self.state.write();
            state.entries.contains_key(&key)
        };
        if already_exists {
            let _ = tokio::fs::remove_file(&file_path).await;
            return Ok(());
        }

        // Update cache state: evict + insert under a fresh write lock
        let evicted = {
            let mut state = self.state.write();

            // Double-check again (race between our two checks)
            if state.entries.contains_key(&key) {
                drop(state);
                // Defer cleanup to after the write lock is released
                tokio::spawn(async move {
                    let _ = tokio::fs::remove_file(&file_path).await;
                });
                return Ok(());
            }

            // Evict to make room before inserting
            let needed = data.len() as u64;
            let evicted = Self::evict_locked(&mut state, needed);

            state.entries.insert(
                key,
                CacheEntry {
                    size: needed,
                    filename: filename.clone(),
                },
            );
            state.total_size += needed;

            evicted
        };

        // Delete evicted files outside the lock
        for evicted_file in evicted {
            let path = self.root.join(&evicted_file);
            tokio::spawn(async move {
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    warn!("Failed to remove evicted cache file {}: {}", path.display(), e);
                }
            });
        }

        Ok(())
    }

    async fn invalidate(&self, location: &Path) -> Result<()> {
        let location_id = {
            let lookup = self.location_lookup.read();
            lookup.get(location).copied()
        };

        let Some(location_id) = location_id else {
            return Ok(());
        };

        let prefix = format!("{}_", location_id);

        let to_remove = {
            let mut state = self.state.write();
            let keys: Vec<String> = state
                .entries
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            let mut filenames = Vec::new();
            for key in keys {
                if let Some(entry) = state.entries.remove(&key) {
                    state.total_size = state.total_size.saturating_sub(entry.size);
                    filenames.push(entry.filename);
                }
            }
            filenames
        };

        self.location_lookup.write().remove(location);
        self.metadata_cache.invalidate(&location_id).await;

        for filename in to_remove {
            let path = self.root.join(&filename);
            tokio::spawn(async move {
                let _ = tokio::fs::remove_file(&path).await;
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use chrono::TimeZone;
    use object_store::local::LocalFileSystem;
    use object_store::ObjectStoreExt;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use tempfile::tempdir;

    fn to_json(object_meta: &ObjectMeta) -> String {
        let mut json = String::from("{\"location\":\"");
        json.push_str(object_meta.location.as_ref());
        json.push_str("\",\"last_modified\":\"");
        json.push_str(&object_meta.last_modified.to_rfc3339());
        json.push_str("\",\"size\":");
        json.push_str(&object_meta.size.to_string());
        json.push(',');
        if let Some(e_tag) = &object_meta.e_tag {
            json.push_str("\"e_tag\":\"");
            json.push_str(e_tag);
            json.push_str("\",");
        } else {
            json.push_str("\"e_tag\":null,");
        }
        if let Some(version) = &object_meta.version {
            json.push_str("\"version\":\"");
            json.push_str(version);
            json.push('"');
        } else {
            json.push_str("\"version\":null");
        }
        json.push('}');
        json
    }

    fn from_json(json: &str) -> Result<ObjectMeta> {
        use serde_json::Value;
        let value: Value = serde_json::from_str(json).unwrap();
        let location = Path::from(value["location"].as_str().unwrap());
        let last_modified = chrono::DateTime::parse_from_rfc3339(
            value["last_modified"].as_str().unwrap(),
        )
        .unwrap()
        .with_timezone(&chrono::Utc);
        let size = value["size"].as_u64().unwrap();
        let e_tag = value["e_tag"].as_str().map(|s| s.to_string());
        let version = value["version"].as_str().map(|s| s.to_string());
        Ok(ObjectMeta {
            location,
            last_modified,
            size,
            e_tag,
            version,
        })
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let cache = DiskCache::new(1024 * 1024, 16 * 1024);
        let location = Path::from("test_key");
        let data = Bytes::from("hello world");

        cache.put(&location, 0, data.clone()).await.unwrap();
        let result = cache.get(&location, 0).await.unwrap();
        assert_eq!(result, Some(data));
    }

    #[tokio::test]
    async fn test_get_miss() {
        let cache = DiskCache::new(1024 * 1024, 16 * 1024);
        let location = Path::from("nonexistent");
        let result = cache.get(&location, 0).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_get_with() {
        let cache = DiskCache::new(1024 * 1024, 16 * 1024);
        let local_fs = Arc::new(LocalFileSystem::new());

        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("test.bin");
        std::fs::write(&file_path, "test data").unwrap();
        let location = Path::from(file_path.as_path().to_str().unwrap());

        let miss = Arc::new(AtomicUsize::new(0));

        let data = cache
            .get_with(&location, 0, {
                let miss = miss.clone();
                let local_fs = local_fs.clone();
                let location = location.clone();
                async move {
                    miss.fetch_add(1, Ordering::SeqCst);
                    local_fs.get(&location).await.unwrap().bytes().await
                }
            })
            .await
            .unwrap();
        assert_eq!(miss.load(Ordering::SeqCst), 1);
        assert_eq!(data, Bytes::from("test data"));

        // Second call should hit cache
        let data = cache
            .get_with(&location, 0, {
                let miss = miss.clone();
                let location = location.clone();
                async move {
                    miss.fetch_add(1, Ordering::SeqCst);
                    local_fs.get(&location).await.unwrap().bytes().await
                }
            })
            .await
            .unwrap();
        assert_eq!(miss.load(Ordering::SeqCst), 1);
        assert_eq!(data, Bytes::from("test data"));
    }

    #[tokio::test]
    async fn test_eviction() {
        const PAGE_SIZE: usize = 512;
        let cache = DiskCache::new(1024, PAGE_SIZE);
        let local_fs = Arc::new(LocalFileSystem::new());

        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("test.bin");
        {
            let mut file = std::fs::File::create(&file_path).unwrap();
            for i in 0_u64..1024 {
                file.write_all(&i.to_be_bytes()).unwrap();
            }
        }
        let location = Path::from(file_path.as_path().to_str().unwrap());

        let miss = Arc::new(AtomicUsize::new(0));

        for (page_id, expected_miss, expected_size) in
            &[(0, 1, 1), (0, 1, 1), (1, 2, 2), (4, 3, 2), (5, 4, 2)]
        {
            let data = cache
                .get_with(&location, *page_id, {
                    let miss = miss.clone();
                    let local_fs = local_fs.clone();
                    let location = location.clone();
                    async move {
                        miss.fetch_add(1, Ordering::SeqCst);
                        local_fs
                            .get_range(
                                &location,
                                PAGE_SIZE as u64 * (*page_id as u64)
                                    ..PAGE_SIZE as u64 * (page_id + 1) as u64,
                            )
                            .await
                    }
                })
                .await
                .unwrap();
            assert_eq!(miss.load(Ordering::SeqCst), *expected_miss);
            assert_eq!(data.len(), PAGE_SIZE);
            assert_eq!(cache.state.read().entries.len(), *expected_size);

            let mut expected_buf = BytesMut::with_capacity(PAGE_SIZE);
            for i in *page_id as u64 * PAGE_SIZE as u64 / 8
                ..(*page_id as u64 + 1) * PAGE_SIZE as u64 / 8
            {
                expected_buf.extend_from_slice(&i.to_be_bytes());
            }
            assert_eq!(data, expected_buf);
        }
    }

    #[tokio::test]
    async fn test_head() {
        let cache = DiskCache::new(1024 * 1024, 16 * 1024);
        let local_fs = Arc::new(LocalFileSystem::new());

        let tmp_dir = tempdir().unwrap();
        let file_path = tmp_dir.path().join("test.bin");
        let path = Path::from(file_path.as_path().to_str().unwrap());

        let r = cache
            .head(&path, {
                let local_fs = local_fs.clone();
                let path = path.clone();
                async move { local_fs.head(&path).await }
            })
            .await;
        assert!(matches!(r, Err(Error::NotFound { .. })));
        cache.metadata_cache.run_pending_tasks().await;
        assert_eq!(cache.metadata_cache.entry_count(), 0);

        std::fs::write(&file_path, "test data").unwrap();
        let meta = cache
            .head(&path, {
                let local_fs = local_fs.clone();
                let path = path.clone();
                async move { local_fs.head(&path).await }
            })
            .await
            .unwrap();
        assert_eq!(meta.size, 9);
    }

    #[tokio::test]
    async fn test_invalidate() {
        let cache = DiskCache::new(1024 * 1024, 16 * 1024);
        let location = Path::from("test_loc");
        let data = Bytes::from("some data");

        cache.put(&location, 0, data.clone()).await.unwrap();
        assert!(cache.get(&location, 0).await.unwrap().is_some());
        assert_eq!(cache.size(), data.len());

        cache.invalidate(&location).await.unwrap();

        // After invalidate, entries and files are cleaned up
        assert!(cache.get(&location, 0).await.unwrap().is_none());
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_to_json() {
        let object_meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: chrono::Utc.timestamp_nanos(100),
            size: 100,
            e_tag: Some("123".to_string()),
            version: None,
        };
        let json = to_json(&object_meta);
        assert_eq!(
            json,
            "{\"location\":\"test\",\"last_modified\":\"1970-01-01T00:00:00.000000100+00:00\",\"size\":100,\"e_tag\":\"123\",\"version\":null}"
        );
        let parsed = from_json(&json).unwrap();
        assert_eq!(parsed, object_meta);
    }

    #[test]
    fn test_cache_size_format() {
        let mut s = String::from("4GiB");
        let size = match s.split_off(s.len() - 3).as_str() {
            "KiB" => s.parse::<usize>().unwrap_or(1024) * 1024,
            "MiB" => s.parse::<usize>().unwrap_or(1024) * 1024 * 1024,
            "GiB" => s.parse::<usize>().unwrap_or(1024) * 1024 * 1024 * 1024,
            "TiB" => {
                s.parse::<usize>().unwrap_or(1024) * 1024 * 1024 * 1024 * 1024
            }
            _ => 1024 * 1024 * 1024,
        };
        assert_eq!(size, 4 * 1024 * 1024 * 1024);
    }
}
