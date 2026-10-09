// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shared observability setup for LakeSoul processes.
//!
//! Libraries emit telemetry but never install global recorders or subscribers.
//! Each binary calls the setup functions in this crate exactly once.

use std::net::SocketAddr;

use metrics::{describe_gauge, gauge};
use metrics_exporter_prometheus::{BuildError, Matcher, PrometheusBuilder};

pub mod tracing;

pub use tracing::{
    FilterReloadHandle, LogFormat, TracingConfig, TracingGuard, TracingInitError,
    init_tracing, spawn_reload_on_sighup,
};

/// Default latency buckets shared by LakeSoul duration histograms.
///
/// The range covers sub-millisecond cache operations through multi-minute
/// compaction, index-build, and query operations.
const DURATION_BUCKETS_SECONDS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
    60.0, 120.0, 300.0,
];

/// Prometheus endpoint configuration for one LakeSoul process.
#[derive(Debug, Clone)]
pub struct PrometheusMetricsConfig {
    /// Stable process role, such as `postgres-lakesoul` or `lakesoul-worker`.
    pub service_name: String,
    /// Address serving Prometheus's `/metrics` endpoint.
    pub listen_addr: SocketAddr,
}

impl PrometheusMetricsConfig {
    pub fn new(service_name: impl Into<String>, listen_addr: SocketAddr) -> Self {
        Self {
            service_name: service_name.into(),
            listen_addr,
        }
    }
}

/// Installs the process-global metrics recorder and starts its HTTP endpoint.
///
/// This must be called at most once per process. Metric-producing libraries
/// remain exporter-agnostic and use the `metrics` facade directly.
///
/// `OTEL_SERVICE_NAME` overrides the configured name, so the metrics' `service`
/// label and the traces' `service.name` always agree.
pub fn install_prometheus_metrics(
    config: PrometheusMetricsConfig,
) -> Result<(), BuildError> {
    PrometheusBuilder::new()
        .with_http_listener(config.listen_addr)
        .add_global_label("service", resolve_service_name(config.service_name))
        .add_global_label("service_version", env!("CARGO_PKG_VERSION"))
        .set_buckets_for_metric(
            Matcher::Suffix("duration_seconds".to_string()),
            DURATION_BUCKETS_SECONDS,
        )?
        .install()?;
    publish_build_info();
    Ok(())
}

/// Records the build identity this binary was compiled from.
///
/// This is the Prometheus `build_info{...} 1` convention: the labels carry the
/// identity while the value stays 1, so a panel joins on them (and
/// `build_info unless on(service, commit) …` finds replicas left on an old
/// commit). Both this metric and the traces' `build.commit` resource attribute
/// come from `lakesoul-build-info`.
fn publish_build_info() {
    describe_gauge!(
        "build_info",
        "Build identity of this binary, always 1; read the labels"
    );
    gauge!(
        "build_info",
        "version" => lakesoul_build_info::VERSION,
        "commit" => lakesoul_build_info::GIT_COMMIT,
        "target" => lakesoul_build_info::TARGET,
        "profile" => lakesoul_build_info::PROFILE,
    )
    .set(1.0);
}

/// The configured service name, unless `OTEL_SERVICE_NAME` overrides it.
pub(crate) fn resolve_service_name(configured: String) -> String {
    overridden_service_name(configured, std::env::var("OTEL_SERVICE_NAME").ok())
}

fn overridden_service_name(configured: String, from_env: Option<String>) -> String {
    from_env
        .filter(|name| !name.is_empty())
        .unwrap_or(configured)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_name_override() {
        assert_eq!(overridden_service_name("role".to_string(), None), "role");
        assert_eq!(
            overridden_service_name("role".to_string(), Some(String::new())),
            "role"
        );
        assert_eq!(
            overridden_service_name("role".to_string(), Some("demo".to_string())),
            "demo"
        );
    }

    #[test]
    fn build_info_carries_the_compiled_commit() {
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        let rendered = metrics::with_local_recorder(&recorder, || {
            publish_build_info();
            handle.render()
        });

        assert!(
            rendered.contains(&format!("commit=\"{}\"", lakesoul_build_info::GIT_COMMIT)),
            "build_info must name the compiled commit:\n{rendered}"
        );
        assert!(rendered.contains("# HELP build_info"), "{rendered}");
    }
}
