// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! W3C trace-context propagation into distributed worker tasks.
//!
//! [`WorkerSessionBuilder::build_session_state`] cannot parent the task's
//! spans: it returns a `SessionState`, while the plan is decoded and executed
//! later, inside the `ExecuteTask` handler's future. This tower layer wraps
//! every worker request whose metadata carries a `traceparent` in a span
//! parented to that remote context, so the handler and the operators it starts
//! join the coordinator's trace.
//!
//! [`WorkerSessionBuilder::build_session_state`]: datafusion_distributed::WorkerSessionBuilder::build_session_state

use std::task::{Context, Poll};

use futures::future::BoxFuture;
use http::Request;
use opentelemetry::trace::TraceContextExt;
use tower::{Layer, Service};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::distributed::headers::{extract_trace_context, query_id_from_headers};

/// Wraps a worker service, parenting each traced request to its remote span.
#[derive(Debug, Clone, Copy, Default)]
pub struct TraceContextLayer;

impl<S> Layer<S> for TraceContextLayer {
    type Service = TraceContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TraceContextService { inner }
    }
}

#[derive(Clone, Copy)]
pub struct TraceContextService<S> {
    inner: S,
}

impl<S, B> Service<Request<B>> for TraceContextService<S>
where
    S: Service<Request<B>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let remote = extract_trace_context(request.headers());
        let query_id = query_id_from_headers(request.headers());
        let method = request.uri().path().to_string();

        // Replace the ready service with a clone: the returned future owns the
        // ready instance, as the tower contract requires.
        let clone = self.inner.clone();
        let inner = std::mem::replace(&mut self.inner, clone);
        let future = async move {
            let mut inner = inner;
            inner.call(request).await
        };

        if !remote.span().span_context().is_valid() {
            // No coordinator trace to join (single-node or tracing disabled):
            // do not add a root span per request.
            return Box::pin(future);
        }

        // One span name per RPC, so the trace tree separates "runs the stage"
        // from "control channel" without opening the `rpc.method` attribute.
        let span = match method.rsplit('/').next().unwrap_or(method.as_str()) {
            "ExecuteTask" => tracing::info_span!(
                "worker_execute_task",
                rpc.method = %method,
                query_id = tracing::field::Empty,
            ),
            "CoordinatorChannel" => tracing::info_span!(
                "worker_coordinator_channel",
                rpc.method = %method,
                query_id = tracing::field::Empty,
            ),
            _ => tracing::info_span!(
                "worker_task",
                rpc.method = %method,
                query_id = tracing::field::Empty,
            ),
        };
        if let Some(query_id) = query_id {
            span.record("query_id", query_id);
        }
        // The span has not been entered yet, so its OpenTelemetry span is
        // still a builder and `set_parent` can replace its parent. A missing
        // OTel layer (no `OTEL_EXPORTER_OTLP_ENDPOINT`) is not an error: the
        // span is then a plain `fmt` span.
        let _ = span.set_parent(remote);

        Box::pin(future.instrument(span))
    }
}
