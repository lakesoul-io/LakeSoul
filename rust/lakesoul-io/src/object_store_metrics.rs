// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Metrics instrumentation for object stores.
//!
//! Remote I/O enters LakeSoul through the stores registered in
//! [`crate::object_store`]; wrapping the innermost store there keeps accounting
//! in one place regardless of which DataFusion operator issued the request.
//!
//! Only low-cardinality labels are recorded: the operation, the store kind and
//! the outcome. Object paths are intentionally never labels.

use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, Once};
use std::task::{Context, Poll};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, Result, UploadPart, path::Path,
};

const REQUESTS_TOTAL: &str = "lakesoul_object_store_requests_total";
const REQUEST_DURATION_SECONDS: &str = "lakesoul_object_store_request_duration_seconds";
const BYTES_TOTAL: &str = "lakesoul_object_store_bytes_total";

static DESCRIBE_METRICS: Once = Once::new();

fn describe_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            REQUESTS_TOTAL,
            "Object store requests grouped by store, operation and outcome"
        );
        metrics::describe_histogram!(
            REQUEST_DURATION_SECONDS,
            metrics::Unit::Seconds,
            "Object store request latency grouped by store, operation and outcome"
        );
        metrics::describe_counter!(
            BYTES_TOTAL,
            "Bytes transferred to or from object stores grouped by store and operation"
        );
    });
}

/// Records one completed request as a counter increment.
fn count_request(store: &'static str, operation: &'static str, outcome: &'static str) {
    metrics::counter!(
        REQUESTS_TOTAL,
        "store" => store,
        "operation" => operation,
        "outcome" => outcome,
    )
    .increment(1);
}

/// Records the latency of one completed request.
fn record_latency(
    store: &'static str,
    operation: &'static str,
    outcome: &'static str,
    started: Instant,
) {
    metrics::histogram!(
        REQUEST_DURATION_SECONDS,
        "store" => store,
        "operation" => operation,
        "outcome" => outcome,
    )
    .record(started.elapsed().as_secs_f64());
}

/// Outcome label of a finished request.
fn request_outcome<T>(result: &Result<T>) -> &'static str {
    if result.is_ok() { "success" } else { "error" }
}

/// Records a request that returned `result`: counter plus latency.
fn record_request<T>(
    store: &'static str,
    operation: &'static str,
    started: Instant,
    result: &Result<T>,
) {
    let outcome = request_outcome(result);
    count_request(store, operation, outcome);
    record_latency(store, operation, outcome, started);
}

/// Records the bytes an operation transferred.
fn record_bytes(store: &'static str, operation: &'static str, bytes: u64) {
    metrics::counter!(
        BYTES_TOTAL,
        "store" => store,
        "operation" => operation,
    )
    .increment(bytes);
}

/// Wraps an [`ObjectStore`] to record request, latency and byte metrics.
///
/// The wrapper records operations when they are issued; `get` payload bytes
/// are counted as the returned stream is consumed, and the uploader returned
/// by `put_multipart_opts` is wrapped so the part uploads, completion and
/// aborts that follow are accounted too.
pub struct MonitoredObjectStore {
    inner: Arc<dyn ObjectStore>,
    kind: &'static str,
}

impl MonitoredObjectStore {
    /// Wraps `inner`, labelling metrics with `kind` (`s3`, `hdfs`, `local`).
    pub fn new(inner: Arc<dyn ObjectStore>, kind: &'static str) -> Self {
        describe_metrics();
        Self { inner, kind }
    }
}

impl Debug for MonitoredObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MonitoredObjectStore")
            .field("kind", &self.kind)
            .field("inner", &self.inner)
            .finish()
    }
}

impl std::fmt::Display for MonitoredObjectStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MonitoredObjectStore({}, {})", self.kind, self.inner)
    }
}

#[async_trait]
#[deny(clippy::missing_trait_methods)]
impl ObjectStore for MonitoredObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        let started = Instant::now();
        let bytes = payload.content_length() as u64;
        let result = self.inner.put_opts(location, payload, options).await;
        record_request(self.kind, "put", started, &result);
        if result.is_ok() {
            record_bytes(self.kind, "put", bytes);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let started = Instant::now();
        let result = self.inner.put_multipart_opts(location, options).await;
        record_request(self.kind, "put_multipart", started, &result);
        // The parts, the completion and the abort all happen after this call
        // returns, so hand back an instrumented uploader.
        result.map(|upload| {
            Box::new(InstrumentedMultipartUpload {
                inner: upload,
                store: self.kind,
            }) as Box<dyn MultipartUpload>
        })
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // `head` and ranged reads are expressed through `GetOptions`, so keep
        // them visible as their own operations instead of lumping them into
        // `get`.
        let head = options.head;
        let operation = if head { "head" } else { "get" };
        let started = Instant::now();
        let result = self.inner.get_opts(location, options).await;
        record_request(self.kind, operation, started, &result);
        result.map(|mut result| {
            if matches!(&result.payload, GetResultPayload::File(..)) {
                // A `File` payload hands the caller a file handle, so it cannot
                // be swapped for a counting stream without giving that handle
                // and the store's own read path away. `range` is exactly what
                // the payload yields, so account it up front. Note `head` also
                // returns a whole-object `File` payload while transferring
                // nothing.
                if !head {
                    record_bytes(self.kind, "get", result.range.end - result.range.start);
                }
            } else {
                result.payload = instrument_payload(result.payload, self.kind);
            }
            result
        })
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        // Parquet issues multi-range reads. Leaving this to the trait default
        // would re-coalesce the ranges and re-enter `get_opts` once per group,
        // losing the inner store's optimized path -- one file handle for local
        // and HDFS, one request for S3 -- and transferring the bytes between
        // coalesced ranges.
        let started = Instant::now();
        let result = self.inner.get_ranges(location, ranges).await;
        record_request(self.kind, "get_ranges", started, &result);
        if let Ok(chunks) = &result {
            let bytes: u64 = chunks.iter().map(|chunk| chunk.len() as u64).sum();
            record_bytes(self.kind, "get_ranges", bytes);
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.kind;
        self.inner
            .delete_stream(locations)
            .map(move |item| {
                count_request(store, "delete", request_outcome(&item));
                item
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        Box::pin(InstrumentedListStream {
            inner: self.inner.list(prefix),
            store: self.kind,
            started: Instant::now(),
            finished: false,
        })
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        // Same reason as `get_ranges`: the trait default would call `list` and
        // filter here, dropping the offset pushdown S3 and GCS implement.
        Box::pin(InstrumentedListStream {
            inner: self.inner.list_with_offset(prefix, offset),
            store: self.kind,
            started: Instant::now(),
            finished: false,
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let started = Instant::now();
        let result = self.inner.list_with_delimiter(prefix).await;
        record_request(self.kind, "list", started, &result);
        result
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.copy_opts(from, to, options).await;
        record_request(self.kind, "copy", started, &result);
        result
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.rename_opts(from, to, options).await;
        record_request(self.kind, "rename", started, &result);
        result
    }
}

/// Counts bytes of a `get` payload as its stream is consumed.
fn instrument_payload(
    payload: GetResultPayload,
    store: &'static str,
) -> GetResultPayload {
    match payload {
        GetResultPayload::Stream(stream) => {
            let stream = stream
                .map(move |chunk| {
                    if let Ok(bytes) = &chunk {
                        record_bytes(store, "get", bytes.len() as u64);
                    }
                    chunk
                })
                .boxed();
            GetResultPayload::Stream(stream)
        }
        other => other,
    }
}

/// Wraps the uploader returned by `put_multipart_opts`.
///
/// Initializing a multipart upload transfers nothing: the bytes move on
/// `put_part`, and `complete`/`abort` decide whether the object appears. The
/// wrapper accounts all three, so a Parquet write through
/// `object_store::buffered::BufWriter` (DataFusion's Parquet sink, which streams
/// via `WriteMultipart`) records its payload and its failures.
struct InstrumentedMultipartUpload {
    inner: Box<dyn MultipartUpload>,
    store: &'static str,
}

impl Debug for InstrumentedMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedMultipartUpload")
            .field("store", &self.store)
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for InstrumentedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let store = self.store;
        let bytes = data.content_length() as u64;
        // The underlying part must be submitted now: implementations expect
        // several `put_part` futures to be obtained before any is awaited.
        let part = self.inner.put_part(data);
        Box::pin(async move {
            let started = Instant::now();
            let result = part.await;
            record_request(store, "put_part", started, &result);
            if result.is_ok() {
                record_bytes(store, "put_part", bytes);
            }
            result
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let started = Instant::now();
        let result = self.inner.complete().await;
        record_request(self.store, "complete", started, &result);
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.abort().await;
        record_request(self.store, "abort", started, &result);
        result
    }
}

/// Measures the lifetime of an object `list` stream.
struct InstrumentedListStream<S> {
    inner: S,
    store: &'static str,
    started: Instant,
    finished: bool,
}

impl<S> InstrumentedListStream<S> {
    fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        count_request(self.store, "list", outcome);
        record_latency(self.store, "list", outcome, self.started);
    }
}

impl<S> Stream for InstrumentedListStream<S>
where
    S: Stream<Item = Result<ObjectMeta>> + Unpin,
{
    type Item = Result<ObjectMeta>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            Poll::Ready(Some(Err(error))) => {
                self.finish("error");
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.finish("success");
                Poll::Ready(None)
            }
            poll => poll,
        }
    }
}

impl<S> Drop for InstrumentedListStream<S> {
    fn drop(&mut self) {
        self.finish("dropped");
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::TryStreamExt;
    use metrics_exporter_prometheus::PrometheusBuilder;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn records_requests_and_bytes() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let monitored = MonitoredObjectStore::new(store, "memory");
        let path = Path::from("metrics-test/object");

        monitored
            .put(&path, Bytes::from_static(b"hello").into())
            .await
            .unwrap();
        let get = monitored.get(&path).await.unwrap();
        assert_eq!(get.bytes().await.unwrap(), Bytes::from_static(b"hello"));

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                monitored
                    .put(&path, Bytes::from_static(b"world").into())
                    .await
                    .unwrap();
                let get = monitored.get(&path).await.unwrap();
                let bytes = get.bytes().await.unwrap();
                assert_eq!(bytes, Bytes::from_static(b"world"));
                let _ = monitored.head(&path).await.unwrap();
            });
        });

        let rendered = handle.render();
        assert!(
            rendered.contains(
                "lakesoul_object_store_requests_total{store=\"memory\",operation=\"put\",outcome=\"success\"} 1"
            ),
            "put metric missing in:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "lakesoul_object_store_requests_total{store=\"memory\",operation=\"get\",outcome=\"success\"} 1"
            ),
            "get metric missing in:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "lakesoul_object_store_requests_total{store=\"memory\",operation=\"head\",outcome=\"success\"} 1"
            ),
            "head metric missing in:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "lakesoul_object_store_bytes_total{store=\"memory\",operation=\"put\"} 5"
            ),
            "put bytes metric missing in:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "lakesoul_object_store_bytes_total{store=\"memory\",operation=\"get\"} 5"
            ),
            "get bytes metric missing in:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn get_ranges_delegates_to_the_inner_store() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let monitored = MonitoredObjectStore::new(store.clone(), "memory");
        let path = Path::from("metrics-test/ranges");
        store
            .put(&path, Bytes::from_static(b"0123456789").into())
            .await
            .unwrap();

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let chunks = monitored.get_ranges(&path, &[0..3, 5..10]).await.unwrap();
                assert_eq!(
                    chunks,
                    vec![Bytes::from_static(b"012"), Bytes::from_static(b"56789")]
                );
            });
        });

        let rendered = handle.render();
        assert!(
            rendered.contains(
                "lakesoul_object_store_requests_total{store=\"memory\",operation=\"get_ranges\",outcome=\"success\"} 1"
            ),
            "get_ranges request metric missing in:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "lakesoul_object_store_bytes_total{store=\"memory\",operation=\"get_ranges\"} 8"
            ),
            "get_ranges bytes metric missing in:\n{rendered}"
        );
        // Without the delegation the trait default re-coalesces and re-enters
        // `get_opts` once per group, so multi-range reads would be accounted
        // as plain `get` requests.
        assert!(
            !rendered.contains("operation=\"get\""),
            "multi-range read fell back to the default `get` path:\n{rendered}"
        );
    }

    /// Uploader whose operations fail, to check the failure labels.
    #[derive(Debug)]
    struct FailingUpload;

    #[async_trait]
    impl MultipartUpload for FailingUpload {
        fn put_part(&mut self, _data: PutPayload) -> UploadPart {
            Box::pin(futures::future::ready(Err(object_store::Error::Generic {
                store: "mock",
                source: "put_part failed".into(),
            })))
        }

        async fn complete(&mut self) -> Result<PutResult> {
            Err(object_store::Error::Generic {
                store: "mock",
                source: "complete failed".into(),
            })
        }

        async fn abort(&mut self) -> Result<()> {
            Err(object_store::Error::Generic {
                store: "mock",
                source: "abort failed".into(),
            })
        }
    }

    #[tokio::test]
    async fn multipart_upload_records_parts_and_completion() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let monitored = MonitoredObjectStore::new(store, "memory");
        let path = Path::from("metrics-test/multipart");

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let mut upload = monitored.put_multipart(&path).await.unwrap();
                upload
                    .put_part(Bytes::from_static(b"hello").into())
                    .await
                    .unwrap();
                upload
                    .put_part(Bytes::from_static(b"world!").into())
                    .await
                    .unwrap();
                upload.complete().await.unwrap();

                // The wrapper must stay transparent: the parts really land.
                let read = monitored.get(&path).await.unwrap().bytes().await.unwrap();
                assert_eq!(read, Bytes::from_static(b"helloworld!"));
            });
        });

        let rendered = handle.render();
        for expected in [
            "lakesoul_object_store_requests_total{store=\"memory\",operation=\"put_multipart\",outcome=\"success\"} 1",
            "lakesoul_object_store_requests_total{store=\"memory\",operation=\"put_part\",outcome=\"success\"} 2",
            "lakesoul_object_store_requests_total{store=\"memory\",operation=\"complete\",outcome=\"success\"} 1",
            "lakesoul_object_store_bytes_total{store=\"memory\",operation=\"put_part\"} 11",
        ] {
            assert!(
                rendered.contains(expected),
                "missing `{expected}` in:\n{rendered}"
            );
        }
    }

    #[tokio::test]
    async fn multipart_upload_records_failures() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let mut upload = InstrumentedMultipartUpload {
                    inner: Box::new(FailingUpload),
                    store: "mock",
                };
                assert!(
                    upload
                        .put_part(Bytes::from_static(b"hello").into())
                        .await
                        .is_err()
                );
                assert!(upload.complete().await.is_err());
                assert!(upload.abort().await.is_err());
            });
        });

        let rendered = handle.render();
        for operation in ["put_part", "complete", "abort"] {
            let expected = format!(
                "lakesoul_object_store_requests_total{{store=\"mock\",operation=\"{operation}\",outcome=\"error\"}} 1"
            );
            assert!(
                rendered.contains(&expected),
                "missing `{expected}` in:\n{rendered}"
            );
        }
        // A part that failed transferred nothing.
        assert!(
            !rendered.contains("lakesoul_object_store_bytes_total{store=\"mock\""),
            "failed part counted bytes:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn local_file_payload_records_bytes() {
        use object_store::local::LocalFileSystem;

        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("five"), b"hello").unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let monitored = MonitoredObjectStore::new(store, "local");
        let path = Path::from("five");

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let get = monitored.get(&path).await.unwrap();
                // The file handle must stay with the caller.
                assert!(matches!(&get.payload, GetResultPayload::File(..)));
                assert_eq!(get.bytes().await.unwrap(), Bytes::from_static(b"hello"));
                // `head` returns the same whole-object payload but transfers
                // nothing, so it must not be counted as bytes.
                monitored.head(&path).await.unwrap();
            });
        });

        let rendered = handle.render();
        assert!(
            rendered.contains(
                "lakesoul_object_store_requests_total{store=\"local\",operation=\"get\",outcome=\"success\"} 1"
            ),
            "get request metric missing in:\n{rendered}"
        );
        assert!(
            rendered.contains(
                "lakesoul_object_store_bytes_total{store=\"local\",operation=\"get\"} 5"
            ),
            "local read bytes missing in:\n{rendered}"
        );
        assert!(
            !rendered.contains(
                "lakesoul_object_store_bytes_total{store=\"local\",operation=\"head\"}"
            ),
            "head counted bytes in:\n{rendered}"
        );
    }

    #[tokio::test]
    async fn list_stream_records_finished_request() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from("list-test/a"), Bytes::new().into())
            .await
            .unwrap();
        let monitored = MonitoredObjectStore::new(store, "memory");

        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();
        metrics::with_local_recorder(&recorder, || {
            futures::executor::block_on(async {
                let objects: Vec<ObjectMeta> = monitored
                    .list(Some(&Path::from("list-test")))
                    .try_collect()
                    .await
                    .unwrap();
                assert_eq!(objects.len(), 1);
            });
        });

        let rendered = handle.render();
        assert!(
            rendered.contains(
                "lakesoul_object_store_requests_total{store=\"memory\",operation=\"list\",outcome=\"success\"} 1"
            ),
            "list metric missing in:\n{rendered}"
        );
    }
}
