use std::sync::Arc;
use std::thread;
use std::{ops::Range, time::Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream, stream::BoxStream};
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult,
    path::Path,
};

use crate::lakesoul_cache::{paging::PageCache, stats::CacheStats};
use object_store::Result;

/// Read-through Page Cache.
#[derive(Debug, Clone)]
pub struct ReadThroughCache<C: PageCache> {
    inner: Arc<dyn ObjectStore>,
    cache: Arc<C>,

    parallelism: usize,

    stats: Arc<dyn CacheStats>,
}

impl<C: PageCache> std::fmt::Display for ReadThroughCache<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReadThroughCache(inner={}, cache={:?})",
            self.inner, self.cache
        )
    }
}

impl<C: PageCache> ReadThroughCache<C> {
    pub fn new(inner: Arc<dyn ObjectStore>, cache: Arc<C>) -> Self {
        Self::new_with_stats(
            inner,
            cache,
            Arc::new(crate::lakesoul_cache::stats::AtomicIntCacheStats::new()),
        )
    }

    pub fn new_with_stats(
        inner: Arc<dyn ObjectStore>,
        cache: Arc<C>,
        stats: Arc<dyn CacheStats>,
    ) -> Self {
        Self {
            inner,
            cache,
            parallelism: num_cpus::get(),
            stats,
        }
    }

    async fn invalidate(&self, location: &Path) -> Result<()> {
        self.cache.invalidate(location).await
    }
}

/// Get a range of bytes from the DiskCache
async fn get_range<C: PageCache>(
    store: Arc<dyn ObjectStore>,
    cache: Arc<C>,
    stats: Arc<dyn CacheStats>,
    location: &Path,
    range: Range<usize>,
    parallelism: usize,
) -> Result<Bytes> {
    let current_time = Instant::now();
    let page_size = cache.page_size();
    let start = (range.start / page_size) * page_size;
    let meta = cache.head(location, store.head(location)).await?;

    let pages = stream::iter((start..range.end).step_by(page_size))
        .map(|offset| {
            let page_cache = cache.clone();
            let page_id = offset / page_size;
            let intersection = std::cmp::max(offset, range.start)
                ..std::cmp::min(offset + page_size, range.end);
            let range_in_page = intersection.start - offset..intersection.end - offset;
            let page_end = std::cmp::min(offset + page_size, meta.size as usize);
            let store = store.clone();
            let stats = stats.clone();

            stats.inc_total_reads();

            async move {
                // Actual range in the file.
                page_cache
                    .get_range_with(location, page_id as u32, range_in_page, async {
                        stats.inc_total_misses();
                        store
                            .get_range(location, offset as u64..page_end as u64)
                            .await
                    })
                    .await
            }
        })
        .buffered(parallelism)
        .try_collect::<Vec<_>>()
        .await?;

    if pages.len() == 1 {
        return Ok(pages.into_iter().next().unwrap());
    }

    // stick all bytes together.
    let mut buf = BytesMut::with_capacity(range.len());
    for page in pages {
        buf.extend_from_slice(&page);
    }
    let duration = Instant::now() - current_time;
    stats.inc_total_query_time(duration.as_millis() as u64);
    stats.inc_total_data_size(buf.len() as u64);
    let current_thread = thread::current();
    // info!("thread name: {:?}======thread id: {:?}========cache get data cost {} ms", current_thread.name(), current_thread.id(), stats.total_query_time());
    // println!("thread name: {:?}======thread id: {:?}========cache get data cost {} ms", current_thread.name(), current_thread.id(), stats.total_query_time());
    Ok(buf.into())
}

/// A ReadThroughCache is an ObjectStore that wraps another ObjectStore and
/// caches the results of get_range calls.
#[async_trait]
impl<C: PageCache> ObjectStore for ReadThroughCache<C> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.cache.invalidate(location).await?;

        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.invalidate(location).await?;

        self.inner.put_multipart_opts(location, _opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let meta = self.head(location).await?;
        let file_size = meta.size;
        let page_size = self.cache.page_size();
        let inner = self.inner.clone();
        let cache = self.cache.clone();
        let stats = self.stats.clone();
        let location = location.clone();
        let parallelism = self.parallelism;

        // TODO: This might yield too many small reads.
        let s = stream::iter((0..file_size).step_by(page_size))
            .map(move |offset| {
                let loc = location.clone();
                let store = inner.clone();
                let stats = stats.clone();
                let c = cache.clone();
                let page_size = cache.page_size();

                async move {
                    get_range(
                        store,
                        c,
                        stats,
                        &loc,
                        offset as usize..offset as usize + page_size,
                        parallelism,
                    )
                    .await
                }
            })
            .buffered(self.parallelism)
            .boxed();

        let payload = GetResultPayload::Stream(s);
        Ok(GetResult {
            payload,
            meta: meta.clone(),
            range: 0..meta.size,
            attributes: Attributes::default(),
        })
    }

    async fn get_opts(
        &self,
        _location: &Path,
        _options: GetOptions,
    ) -> Result<GetResult> {
        todo!()
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        get_range(
            self.inner.clone(),
            self.cache.clone(),
            self.stats.clone(),
            location,
            range.start as usize..range.end as usize,
            self.parallelism,
        )
        .await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        self.cache.head(location, self.inner.head(location)).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.invalidate(location).await?;
        self.inner.delete(location).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.invalidate(to).await?;
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.invalidate(to).await?;
        self.inner.copy_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use crate::lakesoul_cache::cache::DiskCache;

    use super::*;

    #[tokio::test]
    async fn test_get_end_of_file() {
        let cache = Arc::new(DiskCache::new(64 * 1024 * 1024, 16 * 1024));
        let store = Arc::new(object_store::local::LocalFileSystem::new());
        let cache = Arc::new(ReadThroughCache::new(store, cache));

        let temp_file = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        {
            std::fs::write(temp_file.to_str().unwrap(), "this is a long text").unwrap();
        }
        let path = Path::from(temp_file.to_str().unwrap());
        let meta = cache.head(&path).await.unwrap();

        let data = cache.get_range(&path, 10..meta.size).await.unwrap();
        assert_eq!(data.len(), 9);
        assert_eq!(data, "long text".as_bytes());
    }
}
