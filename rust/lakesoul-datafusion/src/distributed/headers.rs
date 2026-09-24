// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Whitelisted identity and per-statement metadata forwarded to distributed workers.

use http::header::InvalidHeaderValue;
use http::{HeaderMap, HeaderName, HeaderValue};
use std::future::Future;
use std::sync::Arc;

use datafusion::prelude::SessionContext;
use datafusion_distributed::DistributedExt;
use opentelemetry::propagation::Injector;

pub const USER_HEADER: &str = "lakesoul-user";
pub const GROUP_HEADER: &str = "lakesoul-group";
pub const TENANT_HEADER: &str = "lakesoul-tenant";
pub const QUERY_ID_HEADER: &str = "lakesoul-query-id";
/// W3C trace context, so worker spans join the coordinator's trace.
pub const TRACEPARENT_HEADER: &str = "traceparent";
pub const TRACESTATE_HEADER: &str = "tracestate";
pub const PASSTHROUGH_HEADERS: [&str; 6] = [
    USER_HEADER,
    GROUP_HEADER,
    TENANT_HEADER,
    QUERY_ID_HEADER,
    TRACEPARENT_HEADER,
    TRACESTATE_HEADER,
];

/// Writes `headers` into an [`http::HeaderMap`] with the propagator's keys.
struct HeaderInjector<'a>(&'a mut HeaderMap);

impl Injector for HeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(key.as_bytes()),
            HeaderValue::from_str(&value),
        ) {
            self.0.insert(name, value);
        }
    }
}

/// Injects the current span context as `traceparent`/`tracestate`.
///
/// A no-op when no OTLP layer is installed or the current span is rejected by
/// the filter: there is no valid context to propagate then.
pub fn inject_trace_context(headers: &mut HeaderMap) {
    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.inject_context(
            &opentelemetry::Context::current(),
            &mut HeaderInjector(headers),
        );
    });
}

/// The span context extracted from incoming worker request headers.
pub fn extract_trace_context(headers: &HeaderMap) -> opentelemetry::Context {
    struct HeaderExtractor<'a>(&'a HeaderMap);

    impl opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|value| value.to_str().ok())
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|name| name.as_str()).collect()
        }
    }

    opentelemetry::global::get_text_map_propagator(|propagator| {
        propagator.extract(&HeaderExtractor(headers))
    })
}

pub fn set_identity_headers(
    context: &SessionContext,
    actor: &SessionActor,
) -> Result<(), InvalidHeaderValue> {
    let mut headers = actor.headers()?;
    inject_trace_context(&mut headers);
    let _ = context
        .state_ref()
        .write()
        .set_distributed_passthrough_headers(headers);
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionActor {
    pub user: String,
    pub group: String,
    pub tenant: String,
}

impl SessionActor {
    pub fn new(
        user: impl Into<String>,
        group: impl Into<String>,
        tenant: impl Into<String>,
    ) -> Self {
        Self {
            user: user.into(),
            group: group.into(),
            tenant: tenant.into(),
        }
    }

    pub fn headers(&self) -> Result<HeaderMap, InvalidHeaderValue> {
        let mut headers = HeaderMap::with_capacity(3);
        for (name, value) in [
            (USER_HEADER, &self.user),
            (GROUP_HEADER, &self.group),
            (TENANT_HEADER, &self.tenant),
        ] {
            headers.insert(HeaderName::from_static(name), HeaderValue::from_str(value)?);
        }
        Ok(headers)
    }
}

/// Temporarily labels one statement's outgoing worker requests.
pub struct StatementIdGuard {
    context: Arc<SessionContext>,
    actor: SessionActor,
}

impl StatementIdGuard {
    pub fn set(
        context: &Arc<SessionContext>,
        actor: &SessionActor,
        query_id: u64,
    ) -> Self {
        let mut headers = actor
            .headers()
            .expect("connection identity was validated during session startup");
        if let Ok(value) = HeaderValue::from_str(&query_id.to_string()) {
            headers.insert(HeaderName::from_static(QUERY_ID_HEADER), value);
        }
        inject_trace_context(&mut headers);
        let _ = context
            .state_ref()
            .write()
            .set_distributed_passthrough_headers(headers);
        Self {
            context: Arc::clone(context),
            actor: actor.clone(),
        }
    }
}

impl Drop for StatementIdGuard {
    fn drop(&mut self) {
        let _ = self
            .context
            .state_ref()
            .write()
            .set_distributed_passthrough_headers(
                self.actor
                    .headers()
                    .expect("connection identity was validated during session startup"),
            );
    }
}

pub async fn with_query_id<F: Future>(
    context: &Arc<SessionContext>,
    actor: &SessionActor,
    query_id: u64,
    future: F,
) -> F::Output {
    let _guard = StatementIdGuard::set(context, actor, query_id);
    future.await
}

pub fn whitelisted_headers(headers: &HeaderMap) -> HeaderMap {
    let mut result = HeaderMap::with_capacity(PASSTHROUGH_HEADERS.len());
    for name in PASSTHROUGH_HEADERS {
        let name = HeaderName::from_static(name);
        if let Some(value) = headers.get(&name) {
            result.insert(name, value.clone());
        }
    }
    result
}

pub fn query_id_from_headers(headers: &HeaderMap) -> Option<u64> {
    headers.get(QUERY_ID_HEADER)?.to_str().ok()?.parse().ok()
}

pub fn actor_from_headers(headers: &HeaderMap) -> Option<SessionActor> {
    fn value(headers: &HeaderMap, name: &'static str) -> Option<String> {
        Some(headers.get(name)?.to_str().ok()?.to_owned())
    }
    Some(SessionActor {
        user: value(headers, USER_HEADER)?,
        group: value(headers, GROUP_HEADER)?,
        tenant: value(headers, TENANT_HEADER)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwards_only_identity_and_statement_query_id() {
        let mut input = SessionActor::new("alice", "analytics", "shop")
            .headers()
            .unwrap();
        input.insert(QUERY_ID_HEADER, HeaderValue::from_static("19"));
        input.insert("x-untrusted", HeaderValue::from_static("secret"));
        input.insert("lakesoul-query-id-extra", HeaderValue::from_static("20"));
        let forwarded = whitelisted_headers(&input);
        assert_eq!(query_id_from_headers(&forwarded), Some(19));
        assert!(!forwarded.contains_key("x-untrusted"));
        assert!(!forwarded.contains_key("lakesoul-query-id-extra"));
        assert_eq!(
            actor_from_headers(&forwarded),
            Some(SessionActor::new("alice", "analytics", "shop"))
        );
    }

    #[test]
    fn connection_identity_does_not_include_statement_id() {
        assert_eq!(
            query_id_from_headers(&SessionActor::new("a", "g", "t").headers().unwrap()),
            None
        );
    }

    #[test]
    fn invalid_identity_rejects_the_whole_header_set() {
        let actor = SessionActor::new("alice\ninjected", "analytics", "shop");
        assert!(actor.headers().is_err());
    }

    #[test]
    fn trace_context_round_trips_through_the_headers() {
        use opentelemetry::trace::TraceContextExt;
        use opentelemetry::trace::{
            SpanContext, SpanId, TraceFlags, TraceId, TraceState,
        };
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        let span_context = SpanContext::new(
            TraceId::from_hex("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
            SpanId::from_hex("00f067aa0ba902b7").unwrap(),
            TraceFlags::SAMPLED,
            false,
            TraceState::default(),
        );
        let context = opentelemetry::Context::current()
            .with_remote_span_context(span_context.clone());

        // The propagator is normally installed by `init_tracing`; tests set it
        // here so injection does not depend on a global subscriber.
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let _guard = context.attach();

        let mut headers = HeaderMap::new();
        inject_trace_context(&mut headers);
        assert_eq!(
            headers.get(TRACEPARENT_HEADER).unwrap(),
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );

        // Extraction marks the context remote; the ids and flags must survive.
        let extracted = extract_trace_context(&headers);
        let extracted = extracted.span().span_context().clone();
        assert_eq!(extracted.trace_id(), span_context.trace_id());
        assert_eq!(extracted.span_id(), span_context.span_id());
        assert_eq!(extracted.trace_flags(), span_context.trace_flags());
        assert!(extracted.is_remote());
    }

    #[test]
    fn missing_trace_context_extracts_to_an_invalid_context() {
        use opentelemetry::trace::TraceContextExt;
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let extracted = extract_trace_context(&HeaderMap::new());
        assert!(!extracted.span().span_context().is_valid());
    }
}
