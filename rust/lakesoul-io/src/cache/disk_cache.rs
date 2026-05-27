use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use moka::{future::Cache, notification::RemovalCause};
use object_store::path::Path;
use object_store::{Error, ObjectMeta, Result};
use parking_lot::RwLock;
use uuid::Uuid;

use super::paging::PageCache;

pub const DEFAULT_PAGE_SIZE: usize = 16 * 1024;
const DEFAULT_METADATA_CACHE_SIZE: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone)]
struct CacheEntry {
    size: u64,
    file: Arc<std::fs::File>,
    filename: String,
}

fn make_key(location_id: u64, page_id: u32) -> String {
    format!("{}_{}", location_id, page_id)
}

fn parse_location_id(key: &str) -> Option<u64> {
    key.split_once('_').and_then(|(id, _)| id.parse().ok())
}

fn entry_weight(size: u64) -> u32 {
    size.min(u32::MAX as u64) as u32
}

/// Read a range from a file descriptor via `pread` (single syscall, no seek).
fn read_range(file: &std::fs::File, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len];
    #[cfg(target_family = "unix")]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(&mut buf, offset)?;
    }
    #[cfg(target_family = "windows")]
    {
        use std::os::windows::fs::FileExt;
        file.seek_read(&mut buf, offset)?;
    }
    Ok(buf)
}

/// Read the entire file from offset 0.
fn read_all(file: &std::fs::File, size: usize) -> std::io::Result<Vec<u8>> {
    read_range(file, 0, size)
}

/// Create + write a file on disk, return the open File handle.
fn create_and_write(path: &std::path::Path, data: &[u8]) -> std::io::Result<std::fs::File> {
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(path)?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(file)
}

/// Local disk-based LRU page cache backed by moka.
///
/// Each cache entry holds an open `std::fs::File` descriptor. This ensures that
/// even if the file is unlinked by a concurrent eviction, reads through this
/// descriptor always succeed (Linux inode reference counting).
///
/// All blocking file I/O runs on `tokio::task::spawn_blocking`.
/// Range reads use `pread`/`read_at` — a single syscall without seek.
#[derive(Debug)]
pub struct DiskCache {
    cache: Cache<String, CacheEntry>,
    root: Arc<PathBuf>,
    page_size: usize,
    max_capacity_bytes: u64,
    metadata_cache: Cache<u64, ObjectMeta>,

    location_lookup: RwLock<HashMap<Path, u64>>,
    next_location_id: AtomicU64,

    location_keys: Arc<RwLock<HashMap<u64, HashSet<String>>>>,

    approximate_size: Arc<AtomicU64>,
}

impl DiskCache {
    pub fn new(disk_capacity: usize, page_size: usize) -> Self {
        let root_path = std::env::var("LAKESOUL_CACHE_PATH")
            .unwrap_or_else(|_| "lakesoul_cache_dir".to_string());
        Self::with_path(disk_capacity, page_size, PathBuf::from(root_path))
    }

    pub fn with_path(disk_capacity: usize, page_size: usize, root: PathBuf) -> Self {
        let root = Arc::new(root);

        if let Err(e) = std::fs::create_dir_all(root.as_ref()) {
            warn!("Failed to create cache dir {}: {}", root.display(), e);
        }
        if let Ok(entries) = std::fs::read_dir(root.as_ref()) {
            for entry in entries.flatten() {
                let _ = std::fs::remove_file(entry.path());
            }
        }

        let location_keys: Arc<RwLock<HashMap<u64, HashSet<String>>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let approximate_size: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

        let cache = Cache::builder()
            .weigher(|_k: &String, v: &CacheEntry| entry_weight(v.size))
            .max_capacity(disk_capacity as u64)
            .eviction_listener({
                let root = root.clone();
                let location_keys = location_keys.clone();
                let sz = approximate_size.clone();
                move |k: Arc<String>, v: CacheEntry, cause: RemovalCause| {
                    if matches!(cause, RemovalCause::Size | RemovalCause::Replaced) {
                        sz.fetch_sub(v.size, Ordering::Relaxed);
                        if let Some(loc_id) = parse_location_id(&k) {
                            let mut lk = location_keys.write();
                            if let Some(keys) = lk.get_mut(&loc_id) {
                                keys.remove(k.as_ref());
                                if keys.is_empty() {
                                    lk.remove(&loc_id);
                                }
                            }
                        }
                    }
                    let path = root.join(&v.filename);
                    // Unlink on a background thread; in-flight reads still succeed
                    // through the open file descriptor held by other CacheEntry clones.
                    tokio::task::spawn_blocking(move || {
                        let _ = std::fs::remove_file(&path);
                    });
                }
            })
            .build();

        let metadata_cache = Cache::builder()
            .max_capacity(DEFAULT_METADATA_CACHE_SIZE as u64)
            .build();

        debug!(
            "DiskCache created: root={}, capacity={}, page_size={}",
            root.display(),
            disk_capacity,
            page_size
        );

        Self {
            cache,
            root,
            page_size,
            max_capacity_bytes: disk_capacity as u64,
            metadata_cache,
            location_lookup: RwLock::new(HashMap::new()),
            next_location_id: AtomicU64::new(0),
            location_keys,
            approximate_size,
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

    fn register_key(&self, location_id: u64, key: &str) {
        self.location_keys
            .write()
            .entry(location_id)
            .or_default()
            .insert(key.to_string());
    }

    /// Spawn a blocking read on `file` and return the bytes.
    async fn blocking_read_all(&self, file: &Arc<std::fs::File>, size: usize) -> Option<Bytes> {
        let f = file.clone();
        match tokio::task::spawn_blocking(move || read_all(&f, size)).await {
            Ok(Ok(data)) => Some(Bytes::from(data)),
            _ => None,
        }
    }

    /// Spawn a blocking range read on `file` and return the bytes.
    async fn blocking_read_range(
        &self,
        file: &Arc<std::fs::File>,
        offset: u64,
        len: usize,
    ) -> Option<Bytes> {
        let f = file.clone();
        match tokio::task::spawn_blocking(move || read_range(&f, offset, len)).await {
            Ok(Ok(data)) => Some(Bytes::from(data)),
            _ => None,
        }
    }
}

#[async_trait::async_trait]
impl PageCache for DiskCache {
    fn page_size(&self) -> usize {
        self.page_size
    }

    fn capacity(&self) -> usize {
        self.max_capacity_bytes as usize
    }

    fn size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed) as usize
    }

    async fn get_with(
        &self,
        location: &Path,
        page_id: u32,
        loader: impl std::future::Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes> {
        let location_id = self.get_location_id(location);
        let key = make_key(location_id, page_id);

        // Try the cache — read via the open fd (survives concurrent unlink)
        if let Some(entry) = self.cache.get(&key).await {
            if let Some(data) = self.blocking_read_all(&entry.file, entry.size as usize).await {
                return Ok(data);
            }
            // Stale entry — invalidate and fall through
            self.cache.invalidate(&key).await;
        }

        // Cache miss — load from remote
        let bytes = loader.await?;
        if !bytes.is_empty() {
            let filename = Uuid::new_v4().to_string();
            let file_path = self.root.join(&filename);
            let size = bytes.len() as u64;

            let file = tokio::task::spawn_blocking({
                let data = bytes.to_vec();
                move || create_and_write(&file_path, &data)
            })
            .await
            .map_err(|e| Error::Generic {
                store: "DiskCache",
                source: Box::new(e),
            })?
            .map_err(|e| Error::Generic {
                store: "DiskCache",
                source: Box::new(e),
            })?;

            self.cache
                .insert(
                    key.clone(),
                    CacheEntry {
                        size,
                        file: Arc::new(file),
                        filename,
                    },
                )
                .await;

            self.approximate_size.fetch_add(size, Ordering::Relaxed);
            self.register_key(location_id, &key);
        }
        Ok(bytes)
    }

    async fn get(&self, location: &Path, page_id: u32) -> Result<Option<Bytes>> {
        let location_id = self.get_location_id(location);
        let key = make_key(location_id, page_id);

        let Some(entry) = self.cache.get(&key).await else {
            return Ok(None);
        };

        Ok(self.blocking_read_all(&entry.file, entry.size as usize).await)
    }

    async fn get_range_with(
        &self,
        location: &Path,
        page_id: u32,
        range: Range<usize>,
        loader: impl std::future::Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes> {
        assert!(range.start <= range.end && range.end <= self.page_size());

        if let Some(bytes) = self.get_range(location, page_id, range.clone()).await? {
            return Ok(bytes);
        }

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

        let Some(entry) = self.cache.get(&key).await else {
            return Ok(None);
        };

        Ok(self
            .blocking_read_range(&entry.file, range.start as u64, range.end - range.start)
            .await)
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

        if self.cache.contains_key(&key) {
            return Ok(());
        }

        let filename = Uuid::new_v4().to_string();
        let file_path = self.root.join(&filename);
        let size = data.len() as u64;

        let file = tokio::task::spawn_blocking({
            let data = data.to_vec();
            move || create_and_write(&file_path, &data)
        })
        .await
        .map_err(|e| Error::Generic {
            store: "DiskCache",
            source: Box::new(e),
        })?
        .map_err(|e| Error::Generic {
            store: "DiskCache",
            source: Box::new(e),
        })?;

        self.cache
            .insert(
                key.clone(),
                CacheEntry {
                    size,
                    file: Arc::new(file),
                    filename,
                },
            )
            .await;

        self.approximate_size.fetch_add(size, Ordering::Relaxed);
        self.register_key(location_id, &key);

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

        let keys: Vec<String> = self
            .location_keys
            .write()
            .remove(&location_id)
            .unwrap_or_default()
            .into_iter()
            .collect();

        for key in &keys {
            if let Some(entry) = self.cache.remove(key).await {
                self.approximate_size
                    .fetch_sub(entry.size, Ordering::Relaxed);
                let path = self.root.join(&entry.filename);
                tokio::task::spawn_blocking(move || {
                    let _ = std::fs::remove_file(&path);
                });
            }
        }

        self.location_lookup.write().remove(location);
        self.metadata_cache.invalidate(&location_id).await;

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

        for (page_id, expected_miss) in &[(0, 1), (0, 1), (1, 2), (4, 3), (5, 4)] {
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

        let path_for_err = path.clone();
        let r = cache
            .head(&path, {
                let local_fs = local_fs.clone();
                async move { local_fs.head(&path_for_err).await }
            })
            .await;
        assert!(matches!(r, Err(Error::NotFound { .. })));

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
