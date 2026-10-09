// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};
use std::thread;
use std::{ops::Range, time::Instant};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt, stream, stream::BoxStream};
use object_store::{
    Attributes, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, path::Path,
};

use super::{paging::PageCache, stats::CacheStats};
use object_store::Result;

const PAGE_READS_TOTAL: &str = "lakesoul_cache_page_reads_total";
const PAGE_MISSES_TOTAL: &str = "lakesoul_cache_page_misses_total";
const PAGE_HIT_BYTES_TOTAL: &str = "lakesoul_cache_page_hit_bytes_total";
const PAGE_MISS_BYTES_TOTAL: &str = "lakesoul_cache_page_miss_bytes_total";
const PAGE_INSERT_BYTES_TOTAL: &str = "lakesoul_cache_page_insert_bytes_total";
const CAPACITY_BYTES: &str = "lakesoul_cache_capacity_bytes";
const USAGE_BYTES: &str = "lakesoul_cache_usage_bytes";

static DESCRIBE_METRICS: Once = Once::new();

fn describe_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            PAGE_READS_TOTAL,
            "Page lookups served by the read-through cache"
        );
        metrics::describe_counter!(
            PAGE_MISSES_TOTAL,
            "Page lookups that missed and fetched from the object store"
        );
        metrics::describe_counter!(
            PAGE_HIT_BYTES_TOTAL,
            "Bytes served from the read-through cache"
        );
        metrics::describe_counter!(
            PAGE_MISS_BYTES_TOTAL,
            "Bytes served after a read-through cache miss"
        );
        metrics::describe_counter!(
            PAGE_INSERT_BYTES_TOTAL,
            "Bytes fetched from the object store to fill cache misses"
        );
        metrics::describe_gauge!(
            CAPACITY_BYTES,
            "Configured read-through cache capacity"
        );
        metrics::describe_gauge!(
            USAGE_BYTES,
            "Read-through cache bytes currently in use"
        );
    });
}

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
            Arc::new(super::stats::AtomicIntCacheStats::new()),
        )
    }

    pub fn new_with_stats(
        inner: Arc<dyn ObjectStore>,
        cache: Arc<C>,
        stats: Arc<dyn CacheStats>,
    ) -> Self {
        describe_metrics();
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
#[instrument(
    name = "object_store_read_range",
    level = "info",
    skip_all,
    fields(
        range_start = range.start,
        range_end = range.end,
        requested_bytes = range.len(),
        page_count = tracing::field::Empty,
        output_bytes = tracing::field::Empty,
    ),
    err
)]
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
    let page_count = range.end.saturating_sub(start).div_ceil(page_size);
    let span = tracing::Span::current();
    span.record("page_count", page_count as u64);
    // Cheap atomic loads: refreshing gauges here keeps them current without a
    // background task.
    metrics::gauge!(CAPACITY_BYTES).set(stats.max_capacity() as f64);
    metrics::gauge!(USAGE_BYTES).set(stats.usage() as f64);
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
            metrics::counter!(PAGE_READS_TOTAL).increment(1);

            async move {
                // Tracks whether this page had to be fetched from the inner store.
                let was_miss = Arc::new(AtomicBool::new(false));
                let miss_flag = was_miss.clone();
                let stats_for_fetch = stats.clone();
                // Actual range in the file.
                let page_bytes = page_cache
                    .get_range_with(location, page_id as u32, range_in_page, async move {
                        stats_for_fetch.inc_total_misses();
                        metrics::counter!(PAGE_MISSES_TOTAL).increment(1);
                        miss_flag.store(true, Ordering::Relaxed);
                        let fetched = store
                            .get_range(location, offset as u64..page_end as u64)
                            .await?;
                        stats_for_fetch.inc_insert_bytes(fetched.len() as u64);
                        metrics::counter!(PAGE_INSERT_BYTES_TOTAL)
                            .increment(fetched.len() as u64);
                        Ok::<Bytes, Error>(fetched)
                    })
                    .await?;
                if was_miss.load(Ordering::Relaxed) {
                    stats.inc_miss_bytes(page_bytes.len() as u64);
                    metrics::counter!(PAGE_MISS_BYTES_TOTAL)
                        .increment(page_bytes.len() as u64);
                } else {
                    stats.inc_hit_bytes(page_bytes.len() as u64);
                    metrics::counter!(PAGE_HIT_BYTES_TOTAL)
                        .increment(page_bytes.len() as u64);
                }
                Ok::<Bytes, Error>(page_bytes)
            }
        })
        .buffered(parallelism)
        .try_collect::<Vec<_>>()
        .await?;

    let total_read_bytes: usize = pages.iter().map(|p| p.len()).sum();
    let duration = Instant::now() - current_time;
    stats.inc_total_query_time(duration.as_millis() as u64);
    stats.inc_total_data_size(total_read_bytes as u64);

    if pages.len() == 1 {
        let bytes = pages.into_iter().next().unwrap();
        span.record("output_bytes", bytes.len() as u64);
        return Ok(bytes);
    }

    // stick all bytes together.
    let mut buf = BytesMut::with_capacity(total_read_bytes);
    for page in pages {
        buf.extend_from_slice(&page);
    }
    span.record("output_bytes", buf.len() as u64);
    let _current_thread = thread::current();
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
        _opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.invalidate(location).await?;

        self.inner.put_multipart_opts(location, _opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if options.version.is_some() {
            return self.inner.get_opts(location, options).await;
        }

        let meta = self.cache.head(location, self.inner.head(location)).await?;
        options.check_preconditions(&meta)?;

        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(stream::empty().boxed()),
                meta,
                range: 0..0,
                attributes: Attributes::default(),
            });
        }

        let range = match options.range {
            Some(range) => {
                range.as_range(meta.size).map_err(|source| Error::Generic {
                    store: "ReadThroughCache",
                    source: Box::new(source),
                })?
            }
            None => 0..meta.size,
        };
        let page_size = self.cache.page_size();
        let inner = self.inner.clone();
        let cache = self.cache.clone();
        let stats = self.stats.clone();
        let location = location.clone();
        let parallelism = self.parallelism;
        let range_start = range.start as usize;
        let range_end = range.end as usize;
        let first_page_start = (range_start / page_size) * page_size;

        // TODO: This might yield too many small reads.
        let s = stream::iter((first_page_start..range_end).step_by(page_size))
            .map(move |offset| {
                let loc = location.clone();
                let store = inner.clone();
                let stats = stats.clone();
                let c = cache.clone();
                let page_size = cache.page_size();
                let chunk_start = std::cmp::max(offset, range_start);
                let chunk_end = std::cmp::min(offset + page_size, range_end);

                async move {
                    get_range(store, c, stats, &loc, chunk_start..chunk_end, parallelism)
                        .await
                }
            })
            .buffered(self.parallelism)
            .boxed();

        let payload = GetResultPayload::Stream(s);
        Ok(GetResult {
            payload,
            meta: meta.clone(),
            range,
            attributes: Attributes::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let cache = self.cache.clone();
        let invalidated = locations
            .then(move |location| {
                let cache = cache.clone();
                async move {
                    let location = location?;
                    cache.invalidate(&location).await?;
                    Ok(location)
                }
            })
            .boxed();

        self.inner.delete_stream(invalidated)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<()> {
        self.invalidate(to).await?;
        self.inner.copy_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::disk_cache::DiskCache;

    use super::*;

    #[tokio::test]
    async fn test_get_end_of_file() {
        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(DiskCache::with_path(
            64 * 1024 * 1024,
            16 * 1024,
            cache_dir.path().join("cache"),
        ));
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

    #[tokio::test]
    async fn test_stats_hit_miss_bytes() {
        use crate::cache::stats::{AtomicIntCacheStats, CacheReadStats};

        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Arc::new(DiskCache::with_path(
            64 * 1024 * 1024,
            16 * 1024,
            cache_dir.path().join("cache"),
        ));
        let store = Arc::new(object_store::local::LocalFileSystem::new());
        let stats = Arc::new(AtomicIntCacheStats::new());
        let cache = Arc::new(ReadThroughCache::new_with_stats(
            store,
            cache,
            stats.clone(),
        ));

        let temp_file = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        {
            std::fs::write(temp_file.to_str().unwrap(), "this is a long text").unwrap();
        }
        let path = Path::from(temp_file.to_str().unwrap());
        let meta = cache.head(&path).await.unwrap();

        // First read is a miss: the whole page is fetched from the inner store.
        let first = cache.get_range(&path, 0..9).await.unwrap();
        assert_eq!(first, "this is a".as_bytes());
        assert_eq!(stats.total_reads(), 1);
        assert_eq!(stats.total_misses(), 1);
        assert_eq!(stats.total_hit_bytes(), 0);
        assert_eq!(stats.total_miss_bytes(), 9);
        assert_eq!(stats.total_insert_bytes(), meta.size);

        // Second read hits the cache: no fetch from the inner store.
        let second = cache.get_range(&path, 0..9).await.unwrap();
        assert_eq!(second, "this is a".as_bytes());
        assert_eq!(stats.total_reads(), 2);
        assert_eq!(stats.total_misses(), 1);
        assert_eq!(stats.total_hit_bytes(), 9);
        assert_eq!(stats.total_miss_bytes(), 9);
        assert_eq!(stats.total_insert_bytes(), meta.size);
    }
}
