pub mod builder;
pub mod lru_cache;
pub mod lru_disk_cache;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use lru_disk_cache::LruDiskCache;
use moka::future::Cache;
use object_store::{path::Path, ObjectMeta};
use std::{
    collections::HashMap,
    future::Future,
    ops::Range,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tempfile::tempdir;
use tokio::sync::RwLock;

pub use self::builder::DiskCacheBuilder;
use crate::lakesoul_cache::paging::PageCache;
use object_store::Error;
use object_store::Result;

/// Default memory page size is 16 KB
pub const DEFAULT_PAGE_SIZE: usize = 16 * 1024;
const DEFAULT_TIME_TO_IDLE: Duration = Duration::from_secs(60 * 30); // 30 minutes
const DEFAULT_METADATA_CACHE_SIZE: usize = 32 * 1024 * 1024;

pub fn bytes_to_object_meta(bytes: &Bytes) -> Result<ObjectMeta> {
    let str = String::from_utf8(bytes.to_vec()).unwrap();
    from_json(&str)
}

pub fn object_meta_to_bytes(object_meta: &ObjectMeta) -> Bytes {
    let str = to_json(object_meta);
    Bytes::from(str)
}

/// 将 JSON 字符串反序列化为 ObjectMeta
pub fn from_json(json: &str) -> Result<ObjectMeta> {
    use serde_json::Value;
    let value: Value = serde_json::from_str(json).unwrap();

    let location = Path::from(value["location"].as_str().ok_or("Invalid location").unwrap());
    let last_modified =
        DateTime::parse_from_rfc3339(value["last_modified"].as_str().ok_or("Invalid last_modified").unwrap())
            .unwrap()
            .with_timezone(&Utc);
    let size = value["size"].as_u64().ok_or("Invalid size").unwrap() as usize;
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

/// 将 ObjectMeta 序列化为 JSON 字符串
pub fn to_json(object_meta: &ObjectMeta) -> String {
    let mut json = String::new();
    json.push('{');

    // 序列化 location
    json.push_str("\"location\":\"");
    json.push_str(&object_meta.location.to_string());
    json.push_str("\",");

    // 序列化 last_modified
    json.push_str("\"last_modified\":\"");
    json.push_str(&object_meta.last_modified.to_rfc3339());
    json.push_str("\",");

    // 序列化 size
    json.push_str("\"size\":");
    json.push_str(&object_meta.size.to_string());
    json.push_str(",");

    // 序列化 e_tag
    if let Some(e_tag) = &object_meta.e_tag {
        json.push_str("\"e_tag\":\"");
        json.push_str(e_tag);
        json.push_str("\",");
    } else {
        json.push_str("\"e_tag\":null,");
    }

    // 序列化 version
    if let Some(version) = &object_meta.version {
        json.push_str("\"version\":\"");
        json.push_str(version);
        json.push_str("\"");
    } else {
        json.push_str("\"version\":null");
    }

    json.push('}');
    json
}

pub fn concat_location_with_pagid(location: &Path, page_id: u32) -> String {
    format!("{}_{}", location.to_string(), page_id)
}

/// In-memory [`PageCache`] implementation.
///
/// This is a LRU mapping of page IDs to page data, with TTL eviction.
///
#[derive(Debug)]
pub struct DiskCache {
    /// Disk Cache Capacity in bytes
    disk_capacity: usize,

    /// Size of each page
    page_size: usize,

    /// In memory page cache: a mapping from `(path id, offset)` to data / bytes.
    // cache: Cache<(u64, u32), Bytes>,
    // HybridCache<(u64, u32),
    cache: LruDiskCache,

    /// Metadata cache
    // metadata_cache: Cache<u64, ObjectMeta>,
    metadata_cache: Cache<u64, ObjectMeta>,

    /// Provide fast lookup of path id
    location_lookup: RwLock<HashMap<Path, u64>>,

    /// Next location id to be assigned
    next_location_id: AtomicU64,
}

impl DiskCache {
    /// Create a [`Builder`](DiskCacheBuilder) to construct [`DiskCache`].
    ///
    /// # Parameters:
    /// - *capacity*: capacity in bytes
    ///
    /// ```
    /// # use std::time::Duration;
    /// use ocra::memory::DiskCache;
    ///
    /// let cache = DiskCache::builder(8*1024*1024)
    ///     .page_size(4096)
    ///     .time_to_idle(Duration::from_secs(60))
    ///     .build();
    /// ```
    #[must_use]
    pub fn builder(disk_capacity_bytes: usize) -> DiskCacheBuilder {
        DiskCacheBuilder::new(disk_capacity_bytes)
    }

    /// Explicitly create a new [DiskCache] with a fixed capacity and page size.
    ///
    /// # Parameters:
    /// - `capacity_bytes`: Max capacity in bytes.
    /// - `page_size`: The maximum size of each page.
    ///
    pub fn new(disk_capacity: usize, page_size: usize) -> Self {
        Self::with_params(disk_capacity, page_size, DEFAULT_TIME_TO_IDLE)
    }

    fn with_params(disk_capacity: usize, page_size: usize, _time_to_idle: Duration) -> Self {
        let dir = tempdir().unwrap();
        println!("tempdir: {}", dir.path().to_str().unwrap());
        let cache = LruDiskCache::new("path", disk_capacity.try_into().unwrap()).unwrap();

        let metadata_cache = Cache::builder()
            .max_capacity(DEFAULT_METADATA_CACHE_SIZE as u64)
            .build();
        Self {
            disk_capacity,
            page_size,
            cache,
            metadata_cache,
            location_lookup: RwLock::new(HashMap::new()),
            next_location_id: AtomicU64::new(0),
        }
    }

    /// Create a new [DiskCache] with a fixed capacity and page size.
    async fn location_id(&self, location: &Path) -> u64 {
        if let Some(&key) = self.location_lookup.read().await.get(location) {
            return key;
        }

        let mut id_map = self.location_lookup.write().await;
        // on lock-escalation, check if someone else has added it
        if let Some(&id) = id_map.get(location) {
            return id;
        }

        let id = self.next_location_id.fetch_add(1, Ordering::SeqCst);
        id_map.insert(location.clone(), id);

        id
    }
}

#[async_trait::async_trait]
impl PageCache for DiskCache {
    /// The size of each page.
    fn page_size(&self) -> usize {
        self.page_size
    }

    /// Cache capacity in bytes.
    fn capacity(&self) -> usize {
        self.disk_capacity
    }

    /// Memory cache size in bytes.
    fn size(&self) -> usize {
        self.cache.size() as usize
        // self.cache.memory().size() as usize
        // self.cache.weighted_size() as usize
    }

    /// Get the page with the given page ID and location, and load it if not found.
    async fn get_with(
        &self,
        location: &Path,
        page_id: u32,
        loader: impl Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes> {
        let location_id = self.location_id(location).await;
        let key = format!("{}_{}", location_id, page_id);
        match self.cache.get(&key) {
            Some(bytes) => Ok(Bytes::from(bytes)),
            // _ => Err(format!("PageCache get_with Error:  get location {:?} ,page id {:?} ",location.to_string(),page_id).to_string())
            _ => {
                // When the page is not found in the cache, load it from the loader.
                match loader.await {
                    Ok(bytes) => {
                        if bytes.len() == 0 {
                            return Ok(bytes);
                        }
                        self.put(location, page_id, bytes.clone()).await?;
                        return Ok(bytes);
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }

    /// Get a range of the page with the given page ID and location, and load it if not found.
    async fn get_range_with(
        &self,
        location: &Path,
        page_id: u32,
        range: Range<usize>,
        loader: impl Future<Output = Result<Bytes>> + Send,
    ) -> Result<Bytes> {
        // Check if the range is within the page size.
        assert!(range.start <= range.end && range.end <= self.page_size());
        let bytes = self.get_with(location, page_id, loader).await?;
        Ok(bytes.slice(range))
    }

    /// Get the page with the given page ID and location, and return `None` if not found.
    async fn get(&self, location: &Path, page_id: u32) -> Result<Option<Bytes>> {
        let location_id = self.location_id(location).await;
        // construct key
        let key = format!("{}_{}", location_id, page_id);
        match self.cache.get(&key) {
            Some(bytes) => {
                Ok(Some(Bytes::from(bytes)))
                // Ok(Some(bytes.value().clone())),
            }
            // When the page is not found in the cache, return None.
            None => Ok(None),
        }
        // Ok(self.cache.get(&(location_id, page_id)).await.map(|bytes| bytes.unwrap().value().clone()).unwrap_or(None))
    }

    /// Get a range of the page with the given page ID and location, and return `None` if not found.
    async fn get_range(&self, location: &Path, page_id: u32, range: Range<usize>) -> Result<Option<Bytes>> {
        Ok(self.get(location, page_id).await?.map(|bytes| bytes.slice(range)))
    }

    /// Put the page with the given page ID and location.
    async fn put(&self, location: &Path, page_id: u32, data: Bytes) -> Result<()> {
        let location_id = self.location_id(location).await;
        let key = format!("{}_{}", location_id, page_id);
        self.cache.insert_bytes(key, &data).unwrap();
        Ok(())
    }

    /// Get the metadata of the given location, and load it if not found.
    async fn head(
        &self,
        location: &Path,
        loader: impl Future<Output = Result<ObjectMeta>> + Send,
    ) -> Result<ObjectMeta> {
        let location_id = self.location_id(location).await;
        match self.metadata_cache.try_get_with(location_id, loader).await {
            Ok(meta) => Ok(meta),
            Err(e) =>
            //  Err(" self.metadata_cache.try_get_with err".to_string())
            {
                match e.as_ref() {
                    // TODO: this adds an extra layer of error wrapping
                    Error::NotFound { path, .. } => Err(Error::NotFound {
                        path: path.to_string(),
                        source: e.into(),
                    }),
                    _ => Err(Error::Generic {
                        store: "DiskCache",
                        source: Box::new(e),
                    }),
                }
            }
        }
    }

    async fn invalidate(&self, location: &Path) -> Result<()> {
        // Remove the location from lookup table.
        // This is cheaper (i.e., O(1)) instead of using O(n) to remove all entries from `self.cache`.
        // The cache will be eventually cleared by the time-to-idle or LRU eviction.
        let mut id_map = self.location_lookup.write().await;
        id_map.remove(location);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        io::Write,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use bytes::{BufMut, BytesMut};
    use chrono::TimeZone as _;
    use object_store::{local::LocalFileSystem, ObjectStore};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_get_range() {
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
        // cache.cache.run_pending_tasks().await;

        let miss = Arc::new(AtomicUsize::new(0));

        for (page_id, expected_miss, expected_size) in [(0, 1, 1), (0, 1, 1), (1, 2, 2), (4, 3, 2), (5, 4, 2)].iter() {
            println!("page_id: {}", page_id);
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
                                PAGE_SIZE * (*page_id as usize)..PAGE_SIZE * (page_id + 1) as usize,
                            )
                            .await
                    }
                })
                .await
                .unwrap();
            assert_eq!(miss.load(Ordering::SeqCst), *expected_miss);
            assert_eq!(data.len(), PAGE_SIZE);

            // cache.cache.run_pending_tasks().await;
            assert_eq!(cache.cache.len(), *expected_size);

            let mut buf = BytesMut::with_capacity(PAGE_SIZE);
            for i in page_id * PAGE_SIZE as u32 / 8..(page_id + 1) * PAGE_SIZE as u32 / 8 {
                buf.put_u64(i as u64);
            }
            assert_eq!(data, buf);
        }
    }

    #[tokio::test]
    async fn test_head() {
        const PAGE_SIZE: usize = 16 * 1024;
        let cache = DiskCache::new(1024 * 1024, PAGE_SIZE);
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

    #[test]
    fn test_to_json() {
        let object_meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: Utc.timestamp_nanos(100),
            size: 100,
            e_tag: Some("123".to_string()),
            version: None,
        };
        let object_meta_to_str = to_json(&object_meta);
        assert_eq!(
            object_meta_to_str,
            "{\"location\":\"test\",\"last_modified\":\"1970-01-01T00:00:00.000000100+00:00\",\"size\":100,\"e_tag\":\"123\",\"version\":null}"
        );
        let object_meta_from_str = from_json(&object_meta_to_str).unwrap();
        assert_eq!(object_meta_from_str, object_meta);
    }

    #[test]
    fn test_from_bytes() {
        let object_meta = ObjectMeta {
            location: Path::from("test"),
            last_modified: Utc.timestamp_nanos(100),
            size: 100,
            e_tag: Some("123".to_string()),
            version: None,
        };
        let bytes = object_meta_to_bytes(&object_meta);
        let object_meta_from_bytes = bytes_to_object_meta(&bytes).unwrap();
        assert_eq!(object_meta_from_bytes, object_meta);
    }

    #[test]
    fn test_concat_location_with_pagid() {
        let location = Path::from("test");
        let page_id = 100;
        let location_with_pagid = concat_location_with_pagid(&location, page_id);
        assert_eq!(location_with_pagid, "test_100");
    }
}
