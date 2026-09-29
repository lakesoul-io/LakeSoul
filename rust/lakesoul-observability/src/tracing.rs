// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tracing setup: one subscriber per process that always logs through `fmt`
//! and additionally exports spans over OTLP when an endpoint is configured.
//!
//! The exporter follows the OpenTelemetry environment variables:
//!
//! | Variable | Meaning |
//! |---|---|
//! | `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint; when unset, spans are not exported |
//! | `OTEL_SERVICE_NAME` | overrides the service name passed by the binary |
//! | `OTEL_TRACES_SAMPLER_ARG` | head-sampling ratio in `[0, 1]`, default `1.0`; any other value fails initialization |
//! | `OTEL_TRACES_EXPORTER` | set to `none` to disable the exporter |
//! | `OTEL_SDK_DISABLED` | set to `true` to disable the exporter |
//! | `OTEL_RESOURCE_ATTRIBUTES` | extra resource attributes |
//!
//! The log filter stays caller-provided (`RUST_LOG`): spans that the filter
//! drops are also not exported.
//!
//! The filter can be replaced at runtime through [`TracingGuard::filter_handle`].
//! [`spawn_reload_on_sighup`] wires that to `SIGHUP`, re-reading
//! `LAKESOUL_LOG_FILTER_FILE` (when set) or `RUST_LOG`.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Registry, reload};

/// How long one OTLP export may take before it is abandoned.
const EXPORT_TIMEOUT: Duration = Duration::from_secs(5);
const TRACER_NAME: &str = "lakesoul";
/// Head-sampling ratio used when `OTEL_TRACES_SAMPLER_ARG` is unset.
const DEFAULT_SAMPLING_RATIO: f64 = 1.0;

/// The identity this process reports its spans under.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Used as `service.name` unless `OTEL_SERVICE_NAME` overrides it.
    pub service_name: String,
    /// Used as the `service.version` resource attribute.
    pub service_version: String,
}

impl TracingConfig {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            service_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

/// Which parts of each log line the `fmt` layer prints.
///
/// The defaults match `tracing_subscriber::fmt()`.
#[derive(Debug, Clone)]
pub struct LogFormat {
    pub show_level: bool,
    pub show_target: bool,
    pub show_file: bool,
    pub show_line_number: bool,
    pub ansi: bool,
}

impl Default for LogFormat {
    fn default() -> Self {
        Self {
            show_level: true,
            show_target: true,
            show_file: true,
            show_line_number: true,
            ansi: true,
        }
    }
}

impl LogFormat {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_target(mut self, show_target: bool) -> Self {
        self.show_target = show_target;
        self
    }

    pub fn with_file(mut self, show_file: bool) -> Self {
        self.show_file = show_file;
        self
    }

    pub fn with_line_number(mut self, show_line_number: bool) -> Self {
        self.show_line_number = show_line_number;
        self
    }

    pub fn with_ansi(mut self, ansi: bool) -> Self {
        self.ansi = ansi;
        self
    }
}

/// Owns the installed subscriber's OTLP pipeline.
///
/// Keep the guard alive for the lifetime of the process: dropping it flushes
/// buffered spans before shutting the exporter down.
pub struct TracingGuard {
    tracer_provider: Option<SdkTracerProvider>,
    otlp_endpoint: Option<String>,
    filter: FilterReloadHandle,
}

impl TracingGuard {
    /// The endpoint spans are exported to, or `None` when the exporter is off.
    pub fn otlp_endpoint(&self) -> Option<&str> {
        self.otlp_endpoint.as_deref()
    }

    /// A cloneable handle for replacing the log filter at runtime.
    pub fn filter_handle(&self) -> FilterReloadHandle {
        self.filter.clone()
    }
}

/// A cloneable handle that replaces the subscriber's filter at runtime.
///
/// Cloning the handle does not keep the [`TracingGuard`] alive, so a background
/// task (for example [`spawn_reload_on_sighup`]) can hold one without delaying
/// the OTLP flush on drop.
#[derive(Clone)]
pub struct FilterReloadHandle {
    handle: Arc<reload::Handle<EnvFilter, Registry>>,
}

impl std::fmt::Debug for FilterReloadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterReloadHandle").finish_non_exhaustive()
    }
}

impl FilterReloadHandle {
    fn new(handle: reload::Handle<EnvFilter, Registry>) -> Self {
        Self {
            handle: Arc::new(handle),
        }
    }

    /// Replaces the filter with `directives` (the `RUST_LOG` syntax).
    ///
    /// Takes effect immediately: the subscriber's interest cache is rebuilt,
    /// so callsites that were disabled become enabled and vice versa.
    pub fn reload(&self, directives: &str) -> Result<(), TracingInitError> {
        let filter = EnvFilter::try_new(directives)
            .map_err(|error| TracingInitError::Filter(error.to_string()))?;
        self.handle
            .reload(filter)
            .map_err(|error| TracingInitError::Filter(error.to_string()))
    }

    /// Re-reads `LAKESOUL_LOG_FILTER_FILE` (when set) or `RUST_LOG` and
    /// applies it, returning the directives that were applied.
    ///
    /// A missing, empty or unreadable source is an error and keeps the current
    /// filter: a stray signal must not silently change the log level.
    pub fn reload_from_env_or_file(&self) -> Result<String, TracingInitError> {
        let directives = resolve_directives(
            std::env::var("LAKESOUL_LOG_FILTER_FILE").ok(),
            std::env::var("RUST_LOG").ok(),
            |path| std::fs::read_to_string(path),
        )?;
        self.reload(&directives)?;
        Ok(directives)
    }
}

/// Chooses the directives for a runtime reload and trims them.
fn resolve_directives(
    file: Option<String>,
    env: Option<String>,
    read_file: impl FnOnce(&str) -> std::io::Result<String>,
) -> Result<String, TracingInitError> {
    if let Some(path) = file.filter(|path| !path.trim().is_empty()) {
        let content = read_file(&path).map_err(|error| {
            TracingInitError::Filter(format!("failed to read {path}: {error}"))
        })?;
        let directives = content.trim();
        if directives.is_empty() {
            return Err(TracingInitError::Filter(format!(
                "{path} is empty; keeping the current filter"
            )));
        }
        return Ok(directives.to_string());
    }
    match env {
        Some(directives) if !directives.trim().is_empty() => {
            Ok(directives.trim().to_string())
        }
        _ => Err(TracingInitError::Filter(
            "no filter source: set LAKESOUL_LOG_FILTER_FILE or RUST_LOG".to_string(),
        )),
    }
}

/// Reloads the log filter on every `SIGHUP`.
///
/// The source is `LAKESOUL_LOG_FILTER_FILE` when set (edit the file, then
/// `kill -HUP <pid>`), otherwise `RUST_LOG`. A reload that fails keeps the
/// current filter and logs a warning.
///
/// Must be called from within a Tokio runtime. The spawned task holds only a
/// clone of the handle, never the guard.
#[cfg(unix)]
pub fn spawn_reload_on_sighup(filter: FilterReloadHandle) {
    tokio::spawn(async move {
        use tokio::signal::unix::{SignalKind, signal};

        let mut hangup = match signal(SignalKind::hangup()) {
            Ok(hangup) => hangup,
            Err(error) => {
                eprintln!("failed to listen for SIGHUP: {error}");
                return;
            }
        };
        while hangup.recv().await.is_some() {
            match filter.reload_from_env_or_file() {
                Ok(directives) => tracing::info!(
                    target: "lakesoul_observability",
                    filter = %directives,
                    "reloaded log filter after SIGHUP"
                ),
                Err(error) => tracing::warn!(
                    target: "lakesoul_observability",
                    %error,
                    "SIGHUP log filter reload failed; keeping the current filter"
                ),
            }
        }
    });
}

/// Non-Unix platforms have no `SIGHUP`; this keeps call sites portable.
#[cfg(not(unix))]
pub fn spawn_reload_on_sighup(_filter: FilterReloadHandle) {}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take()
            && let Err(error) = provider.shutdown()
        {
            eprintln!("failed to flush OpenTelemetry traces: {error}");
        }
    }
}

/// Builds a [`TracingInitError`].
#[derive(Debug)]
pub enum TracingInitError {
    /// The OTLP exporter could not be built (usually a bad endpoint).
    Exporter(String),
    /// A global subscriber was already installed.
    Subscriber(String),
    /// The log filter could not be parsed, read or reloaded.
    Filter(String),
    /// The `OTEL_TRACES_SAMPLER_ARG` head-sampling configuration is invalid.
    Sampler(String),
}

impl std::fmt::Display for TracingInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exporter(message) => {
                write!(f, "failed to build the OTLP trace exporter: {message}")
            }
            Self::Subscriber(message) => {
                write!(f, "failed to install the tracing subscriber: {message}")
            }
            Self::Filter(message) => {
                write!(f, "failed to reload the log filter: {message}")
            }
            Self::Sampler(message) => {
                write!(f, "failed to configure trace sampling: {message}")
            }
        }
    }
}

impl std::error::Error for TracingInitError {}

/// Installs the process subscriber: `filter`, then a `fmt` layer formatted by
/// `log_format`/`timer`, then the OTLP layer when an endpoint is configured.
///
/// When OTLP is enabled this must be called from within a Tokio runtime:
/// tonic's channel setup needs an active reactor.
///
/// Callers must keep the returned [`TracingGuard`] alive.
pub fn init_tracing<T>(
    config: TracingConfig,
    filter: EnvFilter,
    log_format: LogFormat,
    timer: T,
) -> Result<TracingGuard, TracingInitError>
where
    T: tracing_subscriber::fmt::time::FormatTime + Send + Sync + 'static,
{
    // Distributes the current span context through `traceparent`/`tracestate`
    // (for example over the distributed coordinator -> worker gRPC calls).
    // Harmless when the OTLP layer is off: there is no valid context to inject.
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_level(log_format.show_level)
        .with_target(log_format.show_target)
        .with_file(log_format.show_file)
        .with_line_number(log_format.show_line_number)
        .with_ansi(log_format.ansi)
        .with_timer(timer);

    let (filter_layer, filter_handle): (reload::Layer<EnvFilter, Registry>, _) =
        reload::Layer::new(filter);
    let reload_handle = FilterReloadHandle::new(filter_handle);
    let subscriber = tracing_subscriber::registry()
        .with(filter_layer)
        .with(fmt_layer);

    let Some(endpoint) = otlp_endpoint() else {
        subscriber
            .try_init()
            .map_err(|error| TracingInitError::Subscriber(error.to_string()))?;
        return Ok(TracingGuard {
            tracer_provider: None,
            otlp_endpoint: None,
            filter: reload_handle,
        });
    };

    // Validated before the exporter exists, so a bad configuration fails
    // without opening an OTLP channel.
    let sampler = sampler_from_env()?;

    let exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.clone())
        .with_timeout(EXPORT_TIMEOUT)
        .build()
        .map_err(|error| TracingInitError::Exporter(error.to_string()))?;

    let resource = Resource::builder()
        .with_service_name(service_name(&config))
        .with_attribute(KeyValue::new(
            "service.version",
            config.service_version.clone(),
        ))
        // Which commit this replica was built from. One Tempo service spans
        // several restarts and rolled-out builds; this is what tells the
        // spans apart when a worker runs stale code.
        .with_attribute(KeyValue::new(
            "build.commit",
            lakesoul_build_info::GIT_COMMIT,
        ))
        .build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(sampler)
        .build();

    let tracer = tracer_provider.tracer(TRACER_NAME);
    subscriber
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()
        .map_err(|error| TracingInitError::Subscriber(error.to_string()))?;

    Ok(TracingGuard {
        tracer_provider: Some(tracer_provider),
        otlp_endpoint: Some(endpoint),
        filter: reload_handle,
    })
}

/// The configured OTLP endpoint, or `None` when export is disabled.
fn otlp_endpoint() -> Option<String> {
    if std::env::var("OTEL_SDK_DISABLED")
        .is_ok_and(|value| value.eq_ignore_ascii_case("true"))
    {
        return None;
    }
    if std::env::var("OTEL_TRACES_EXPORTER")
        .is_ok_and(|value| value.eq_ignore_ascii_case("none"))
    {
        return None;
    }
    pick_endpoint(
        std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").ok(),
        std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").ok(),
    )
}

/// The signal-specific endpoint wins; empty values count as unset.
fn pick_endpoint(traces: Option<String>, generic: Option<String>) -> Option<String> {
    traces
        .filter(|endpoint| !endpoint.is_empty())
        .or_else(|| generic.filter(|endpoint| !endpoint.is_empty()))
}

fn service_name(config: &TracingConfig) -> String {
    crate::resolve_service_name(config.service_name.clone())
}

/// `OTEL_TRACES_SAMPLER_ARG` as a head-sampling ratio, default `1.0`.
///
/// Sampling stays parent-based, so a `traceparent` propagated from another
/// LakeSoul process keeps its sampling decision.
fn sampler_from_env() -> Result<Sampler, TracingInitError> {
    let arg = std::env::var_os("OTEL_TRACES_SAMPLER_ARG")
        .map(|value| {
            value.to_str().map(str::to_string).ok_or_else(|| {
                TracingInitError::Sampler(
                    "OTEL_TRACES_SAMPLER_ARG is not valid UTF-8".to_string(),
                )
            })
        })
        .transpose()?;
    Ok(sampler_for_ratio(sampling_ratio(arg.as_deref())?))
}

/// The head-sampling ratio carried by `OTEL_TRACES_SAMPLER_ARG`.
///
/// An absent -- or empty, which counts as unset like the endpoint variables
/// above -- argument keeps [`DEFAULT_SAMPLING_RATIO`]. Anything else must be a
/// ratio in `[0, 1]`: a typo such as `0,01` used to fall back to full
/// sampling, which hid the mistake and multiplied the exported volume.
fn sampling_ratio(arg: Option<&str>) -> Result<f64, TracingInitError> {
    let Some(value) = arg.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(DEFAULT_SAMPLING_RATIO);
    };
    let ratio = value.parse::<f64>().map_err(|error| {
        TracingInitError::Sampler(format!(
            "OTEL_TRACES_SAMPLER_ARG must be a sampling ratio in [0, 1], got {value:?}: {error}"
        ))
    })?;
    // Also rejects `NaN` and the infinities, which parse but are no ratio.
    if !(0.0..=1.0).contains(&ratio) {
        return Err(TracingInitError::Sampler(format!(
            "OTEL_TRACES_SAMPLER_ARG must be a sampling ratio in [0, 1], got {value:?}"
        )));
    }
    Ok(ratio)
}

fn sampler_for_ratio(ratio: f64) -> Sampler {
    if ratio >= 1.0 {
        Sampler::ParentBased(Box::new(Sampler::AlwaysOn))
    } else if ratio <= 0.0 {
        Sampler::ParentBased(Box::new(Sampler::AlwaysOff))
    } else {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_specific_endpoint_wins() {
        assert_eq!(
            pick_endpoint(
                Some("http://traces:4317".to_string()),
                Some("http://host:4317".to_string())
            )
            .as_deref(),
            Some("http://traces:4317")
        );
    }

    #[test]
    fn empty_endpoint_falls_back_to_the_generic_variable() {
        assert_eq!(
            pick_endpoint(Some(String::new()), Some("http://host:4317".to_string()))
                .as_deref(),
            Some("http://host:4317")
        );
        assert_eq!(pick_endpoint(Some(String::new()), None), None);
        assert_eq!(pick_endpoint(None, None), None);
    }

    #[test]
    fn sampler_stays_parent_based_for_every_ratio() {
        for ratio in [-1.0, 0.0, 0.25, 1.0, 2.0] {
            assert!(
                matches!(sampler_for_ratio(ratio), Sampler::ParentBased(_)),
                "sampler for ratio {ratio} must stay parent-based"
            );
        }
    }

    #[test]
    fn an_absent_sampler_arg_keeps_the_default_ratio() {
        assert_eq!(sampling_ratio(None).unwrap(), 1.0);
        // Empty counts as unset, like the endpoint variables.
        assert_eq!(sampling_ratio(Some("")).unwrap(), 1.0);
        assert_eq!(sampling_ratio(Some("   ")).unwrap(), 1.0);
    }

    #[test]
    fn a_sampler_arg_in_range_is_the_ratio() {
        assert_eq!(sampling_ratio(Some("0")).unwrap(), 0.0);
        assert_eq!(sampling_ratio(Some(" 0.25 ")).unwrap(), 0.25);
        assert_eq!(sampling_ratio(Some("1")).unwrap(), 1.0);
    }

    #[test]
    fn a_malformed_sampler_arg_is_rejected_instead_of_defaulting() {
        // `0,01` (a decimal comma) used to become full sampling.
        for value in ["0,01", "abc", "0.5x", "1e-2x"] {
            assert!(
                sampling_ratio(Some(value)).is_err(),
                "{value:?} must be rejected rather than sampled at the default"
            );
        }
    }

    #[test]
    fn a_sampler_arg_outside_the_range_is_rejected() {
        // Also covers `NaN`/`inf`, which parse but are no sampling ratio.
        for value in ["-0.1", "-1", "1.5", "2", "inf", "-inf", "NaN"] {
            assert!(
                sampling_ratio(Some(value)).is_err(),
                "{value:?} must be rejected rather than clamped"
            );
        }
    }

    #[test]
    fn reload_switches_what_the_subscriber_records() {
        let captured = CaptureLayer::default();
        let events = Arc::clone(&captured.events);
        let (filter_layer, handle) = reload::Layer::new(EnvFilter::new("info"));
        let filter = FilterReloadHandle::new(handle);
        let subscriber = tracing_subscriber::registry()
            .with(filter_layer)
            .with(captured);

        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(target: "lakesoul_sql", "hidden before the reload");
            filter
                .reload("debug")
                .expect("the directives must parse and reload");
            tracing::debug!(target: "lakesoul_sql", "visible after the reload");
        });

        assert_eq!(
            events.lock().expect("capture lock").as_slice(),
            ["lakesoul_sql"],
            "only the event emitted after the reload must be recorded"
        );
    }

    #[test]
    fn a_reload_without_a_source_keeps_the_current_filter() {
        assert!(resolve_directives(None, None, |_| unreachable!()).is_err());
        assert!(
            resolve_directives(None, Some("  ".to_string()), |_| unreachable!()).is_err()
        );
        // An empty path counts as unset.
        assert_eq!(
            resolve_directives(Some(String::new()), Some("info".to_string()), |_| {
                unreachable!()
            })
            .unwrap(),
            "info"
        );
    }

    #[test]
    fn the_filter_file_wins_over_the_environment() {
        assert_eq!(
            resolve_directives(
                Some("/tmp/filter".to_string()),
                Some("info".to_string()),
                |path| {
                    assert_eq!(path, "/tmp/filter");
                    Ok("  info,lakesoul_sql=debug\n".to_string())
                },
            )
            .unwrap(),
            "info,lakesoul_sql=debug"
        );
        assert!(
            resolve_directives(
                Some("/tmp/filter".to_string()),
                Some("info".to_string()),
                |_| Ok(String::new()),
            )
            .is_err(),
            "an empty filter file must not silently disable logging"
        );
        assert!(
            resolve_directives(
                Some("/tmp/filter".to_string()),
                Some("info".to_string()),
                |_| Err(std::io::Error::other("missing")),
            )
            .is_err()
        );
    }

    /// Captures the target of every event that reaches the subscriber.
    #[derive(Clone, Default)]
    struct CaptureLayer {
        events: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureLayer {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.events
                .lock()
                .expect("capture lock")
                .push(event.metadata().target().to_string());
        }
    }
}
