// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Metrics and tracing instrumentation for object stores.
//!
//! Remote I/O enters LakeSoul through the stores registered in
//! [`crate::object_store`]; wrapping the innermost store there keeps accounting
//! in one place regardless of which DataFusion operator issued the request.
//!
//! Only low-cardinality labels are recorded on metrics: the operation, the
//! store kind and the outcome. Object paths are intentionally never metric
//! labels. Spans do carry the path — one span covers one request, is sampled
//! away when it is not interesting, and is what makes an I/O trace actionable.
//! A request span is only opened while a span is current: a request is never
//! a root operation, so an untraced one stays out of the trace backend
//! entirely (see `request_span`).
//!
//! The scan operations (`head`, `get`, `get_ranges`) open `debug` spans, since
//! one scan emits one per input file and per read range; the operations that
//! fire once per statement stay at `info`. Both levels carry the same fields;
//! see `request_span` for which operation is which and how to enable the debug
//! ones.

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
use tracing::{Instrument, Level, Span};

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

/// Opens the span covering one object store request at `level`.
///
/// The name is the metric operation (`object_store_get`, ...), so Tempo's span
/// metrics split requests the same way `lakesoul_object_store_requests_total`
/// does; the trailing arguments are span fields, on top of the `bytes`,
/// `outcome` and `error` fields every request span carries. A span covers the
/// request itself: the body of a streamed `get` payload is consumed after the
/// span has ended.
///
/// The level separates the operations by how many spans one statement emits.
/// A scan emits one request per input file and per read range — a 1024-file
/// parquet scan emitted 6144 of them — so `get`, `get_ranges` and `head` open
/// `debug` spans: the default `info` filter drops them, and
/// `RUST_LOG=…,lakesoul_io::object_store_metrics=debug` asks for them, which
/// also keeps a wide scan from filling, or outgrowing, the trace. The rest
/// (`put`, `put_multipart`, `put_part`, `complete`, `abort`, `list`, `delete`,
/// `copy`, `rename`) fire once per statement or per commit, so they stay at
/// `info`: the default trace is where a failing upload should show up. The
/// request metrics cover every operation at every level.
///
/// The level is an argument rather than a choice of macro so that the gate and
/// the shared fields below stay in one body; the callsite's interest is still
/// resolved and cached by the subscriber.
///
/// A request is never a root operation, so a request without a current span
/// gets no span at all: it would be exported as a one-span trace of its own,
/// with nothing in it to say which operation issued the request. That case is
/// not hypothetical — vortex' IO runtime polls its reads on tasks spawned with
/// a `trace`-level span (`vortex_io::spawn_io`), so at the default `info` level
/// the reads below it have no current span; `RUST_LOG=…,vortex_io::spawn_io=trace`
/// reactivates them as traced spans. Metrics are unaffected either way.
macro_rules! request_span {
    ($level:expr, $name:literal, $($field:tt)*) => {{
        if tracing::Span::current().is_none() {
            tracing::Span::none()
        } else {
            tracing::span!(
                $level,
                $name,
                bytes = tracing::field::Empty,
                outcome = tracing::field::Empty,
                error = tracing::field::Empty,
                $($field)*
            )
        }
    }};
}

/// Records a finished request on its span: the outcome, and the error when the
/// request failed.
fn record_span_request<T>(span: &Span, result: &Result<T>) {
    span.record("outcome", request_outcome(result));
    if let Err(error) = result {
        span.record("error", tracing::field::display(error));
    }
}

/// Records the bytes a request returned on its span.
fn record_span_bytes(span: &Span, bytes: u64) {
    span.record("bytes", bytes);
}

/// Records transferred bytes on the byte counter and on the span.
///
/// Requests whose payload is counted as it is consumed (a streamed `get`) must
/// use [`record_span_bytes`] instead, or their bytes are counted twice.
fn record_counted_span_bytes(
    span: &Span,
    store: &'static str,
    operation: &'static str,
    bytes: u64,
) {
    record_bytes(store, operation, bytes);
    record_span_bytes(span, bytes);
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
        let span = request_span!(Level::INFO, "object_store_put", store = self.kind, path = %location);
        let result = self
            .inner
            .put_opts(location, payload, options)
            .instrument(span.clone())
            .await;
        record_request(self.kind, "put", started, &result);
        record_span_request(&span, &result);
        if result.is_ok() {
            record_counted_span_bytes(&span, self.kind, "put", bytes);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let started = Instant::now();
        let span = request_span!(Level::INFO, "object_store_put_multipart", store = self.kind, path = %location);
        let result = self
            .inner
            .put_multipart_opts(location, options)
            .instrument(span.clone())
            .await;
        record_request(self.kind, "put_multipart", started, &result);
        record_span_request(&span, &result);
        // The parts, the completion and the abort all happen after this call
        // returns, so hand back an instrumented uploader.
        result.map(|upload| {
            Box::new(InstrumentedMultipartUpload {
                inner: upload,
                store: self.kind,
                path: location.clone(),
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
        let span = if head {
            request_span!(Level::DEBUG, "object_store_head", store = self.kind, path = %location)
        } else {
            request_span!(Level::DEBUG, "object_store_get", store = self.kind, path = %location)
        };
        let result = self
            .inner
            .get_opts(location, options)
            .instrument(span.clone())
            .await;
        record_request(self.kind, operation, started, &result);
        record_span_request(&span, &result);
        result.map(|mut result| {
            let file_payload = matches!(&result.payload, GetResultPayload::File(..));
            if !head {
                // `range` is the object range the request returned, which is
                // the size of the read even when the payload is streamed past
                // the end of the span. Note `head` also returns a whole-object
                // `File` payload while transferring nothing.
                let bytes = result.range.end - result.range.start;
                if file_payload {
                    // A `File` payload hands the caller a file handle, so it
                    // cannot be swapped for a counting stream without giving
                    // that handle and the store's own read path away: account
                    // the range up front.
                    record_counted_span_bytes(&span, self.kind, "get", bytes);
                } else {
                    // The wrapper counts the bytes that actually arrive as the
                    // caller consumes them, so the span only reports what the
                    // request returned.
                    record_span_bytes(&span, bytes);
                }
            }
            if !file_payload {
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
        let span = request_span!(Level::DEBUG, "object_store_get_ranges", store = self.kind, path = %location);
        let result = self
            .inner
            .get_ranges(location, ranges)
            .instrument(span.clone())
            .await;
        record_request(self.kind, "get_ranges", started, &result);
        record_span_request(&span, &result);
        if let Ok(chunks) = &result {
            let bytes: u64 = chunks.iter().map(|chunk| chunk.len() as u64).sum();
            record_counted_span_bytes(&span, self.kind, "get_ranges", bytes);
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.kind;
        // The objects are named by the stream's items, not by the stream, so
        // the span carries no path; the close of the span times the deletion.
        let span = request_span!(Level::INFO, "object_store_delete", store = store);
        self.inner
            .delete_stream(locations)
            .map(move |item| {
                count_request(store, "delete", request_outcome(&item));
                record_span_request(&span, &item);
                item
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let span = request_span!(Level::INFO, "object_store_list",
            store = self.kind,
            path = ?prefix,
            objects = tracing::field::Empty,
        );
        Box::pin(InstrumentedListStream {
            inner: self.inner.list(prefix),
            store: self.kind,
            started: Instant::now(),
            span,
            objects: 0,
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
        let span = request_span!(Level::INFO, "object_store_list",
            store = self.kind,
            path = ?prefix,
            offset = %offset,
            objects = tracing::field::Empty,
        );
        Box::pin(InstrumentedListStream {
            inner: self.inner.list_with_offset(prefix, offset),
            store: self.kind,
            started: Instant::now(),
            span,
            objects: 0,
            finished: false,
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let started = Instant::now();
        let span = request_span!(Level::INFO, "object_store_list",
            store = self.kind,
            path = ?prefix,
            objects = tracing::field::Empty,
        );
        let result = self
            .inner
            .list_with_delimiter(prefix)
            .instrument(span.clone())
            .await;
        record_request(self.kind, "list", started, &result);
        record_span_request(&span, &result);
        if let Ok(result) = &result {
            span.record("objects", result.objects.len());
        }
        result
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<()> {
        let started = Instant::now();
        let span = request_span!(Level::INFO, "object_store_copy",
            store = self.kind,
            path = %from,
            to = %to,
        );
        let result = self
            .inner
            .copy_opts(from, to, options)
            .instrument(span.clone())
            .await;
        record_request(self.kind, "copy", started, &result);
        record_span_request(&span, &result);
        result
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<()> {
        let started = Instant::now();
        let span = request_span!(Level::INFO, "object_store_rename",
            store = self.kind,
            path = %from,
            to = %to,
        );
        let result = self
            .inner
            .rename_opts(from, to, options)
            .instrument(span.clone())
            .await;
        record_request(self.kind, "rename", started, &result);
        record_span_request(&span, &result);
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
    path: Path,
}

impl Debug for InstrumentedMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedMultipartUpload")
            .field("store", &self.store)
            .field("path", &self.path)
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for InstrumentedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let store = self.store;
        let bytes = data.content_length() as u64;
        let span = request_span!(Level::INFO, "object_store_put_part", store = store, path = %self.path);
        // The underlying part must be submitted now: implementations expect
        // several `put_part` futures to be obtained before any is awaited.
        let part = self.inner.put_part(data);
        Box::pin(async move {
            let started = Instant::now();
            let result = part.await;
            record_request(store, "put_part", started, &result);
            record_span_request(&span, &result);
            if result.is_ok() {
                record_counted_span_bytes(&span, store, "put_part", bytes);
            }
            result
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let started = Instant::now();
        let span = request_span!(Level::INFO, "object_store_complete", store = self.store, path = %self.path);
        let result = self.inner.complete().instrument(span.clone()).await;
        record_request(self.store, "complete", started, &result);
        record_span_request(&span, &result);
        result
    }

    async fn abort(&mut self) -> Result<()> {
        let started = Instant::now();
        let span = request_span!(Level::INFO, "object_store_abort", store = self.store, path = %self.path);
        let result = self.inner.abort().instrument(span.clone()).await;
        record_request(self.store, "abort", started, &result);
        record_span_request(&span, &result);
        result
    }
}

/// Measures the lifetime and the length of an object `list` stream.
struct InstrumentedListStream<S> {
    inner: S,
    store: &'static str,
    started: Instant,
    span: Span,
    objects: u64,
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
        self.span.record("objects", self.objects);
        self.span.record("outcome", outcome);
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
            Poll::Ready(Some(Ok(meta))) => {
                self.objects += 1;
                Poll::Ready(Some(Ok(meta)))
            }
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
    use parking_lot::Mutex;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;

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
                    path: Path::from("mock/object"),
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

    /// A layer that only remembers the name of every span it is told about.
    struct SpanNameLayer {
        names: Arc<Mutex<Vec<String>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for SpanNameLayer {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.names.lock().push(attrs.metadata().name().to_string());
        }
    }

    /// A request without a current span must not be traced: it would be
    /// exported as a one-span trace of its own, naming an operation with no
    /// parent to explain why it ran. Vortex' IO runtime polls its reads with no
    /// current span at the default level, so this is the common case, not an
    /// edge case.
    #[test]
    fn request_spans_need_a_current_span() {
        let names = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(SpanNameLayer {
            names: names.clone(),
        });
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let monitored = MonitoredObjectStore::new(store, "memory");
        let path = Path::from("span-test/object");

        tracing::subscriber::with_default(subscriber, || {
            futures::executor::block_on(async {
                monitored
                    .put(&path, Bytes::from_static(b"hello").into())
                    .await
                    .unwrap();
                let traced = names.lock().clone();
                assert!(
                    traced.is_empty(),
                    "traced without a current span: {traced:?}"
                );

                let outer = tracing::info_span!("outer");
                let entered = outer.enter();
                monitored.get(&path).await.unwrap();
                drop(entered);

                let traced = names.lock().clone();
                assert!(
                    traced.iter().any(|name| name == "object_store_get"),
                    "get not traced below a current span: {traced:?}"
                );
            });
        });
    }

    /// A scan emits one `head`, `get` and `get_ranges` per input file and read
    /// range, so those are `debug` spans and need the module's target enabled;
    /// the operations that fire once per statement are `info` and part of the
    /// default trace.
    #[test]
    fn scan_request_spans_are_debug() {
        let names = Arc::new(Mutex::new(Vec::new()));
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let monitored = MonitoredObjectStore::new(store, "memory");
        let path = Path::from("span-level-test/object");
        futures::executor::block_on(
            monitored.put(&path, Bytes::from_static(b"hello").into()),
        )
        .unwrap();

        for (filter, scan_traced) in [
            ("info", false),
            ("info,lakesoul_io::object_store_metrics=debug", true),
        ] {
            names.lock().clear();
            let subscriber = tracing_subscriber::registry()
                .with(EnvFilter::new(filter))
                .with(SpanNameLayer {
                    names: names.clone(),
                });
            tracing::subscriber::with_default(subscriber, || {
                futures::executor::block_on(async {
                    let outer = tracing::info_span!("outer");
                    let entered = outer.enter();
                    monitored
                        .put(&path, Bytes::from_static(b"world").into())
                        .await
                        .unwrap();
                    monitored.head(&path).await.unwrap();
                    let get = monitored.get(&path).await.unwrap();
                    assert_eq!(get.bytes().await.unwrap(), Bytes::from_static(b"world"));
                    monitored.get_ranges(&path, &[0..3, 3..5]).await.unwrap();
                    let listed: Vec<ObjectMeta> = monitored
                        .list(Some(&Path::from("span-level-test")))
                        .try_collect()
                        .await
                        .unwrap();
                    assert_eq!(listed.len(), 1);
                    drop(entered);
                });
            });

            let traced = names.lock().clone();
            for operation in ["object_store_put", "object_store_list"] {
                assert!(
                    traced.iter().any(|name| name == operation),
                    "{operation} is not an info span with filter {filter}: {traced:?}"
                );
            }
            let scan_exists = [
                "object_store_head",
                "object_store_get",
                "object_store_get_ranges",
            ]
            .iter()
            .all(|operation| traced.iter().any(|name| name == operation));
            assert_eq!(
                scan_exists, scan_traced,
                "scan spans with filter {filter}: {traced:?}"
            );
        }
    }
}
