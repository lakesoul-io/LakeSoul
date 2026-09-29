// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Standalone LakeSoul distributed worker.
//!
//! Usage:
//!
//! ```sh
//! lakesoul-worker --bind 0.0.0.0 --port 50051 \
//!     --warehouse-prefix s3://bucket/warehouse --endpoint http://s3:9000 ...
//! ```
//!
//! Object-store flags match the coordinator CLI (`CoreArgs`): the worker and
//! the coordinator must observe identical S3/HDFS/warehouse configuration so
//! serialized stage plans resolve their object stores.

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use lakesoul_common::misc::JiffTime;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::distributed::{
    DISTRIBUTED_PROTOCOL_VERSION, LakeSoulWorkerOptions, spawn_lakesoul_worker,
};
use lakesoul_observability::{
    LogFormat, PrometheusMetricsConfig, TracingConfig, init_tracing,
    install_prometheus_metrics, spawn_reload_on_sighup,
};
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "lakesoul-worker",
    about = "LakeSoul distributed worker",
    version = lakesoul_build_info::VERSION_WITH_COMMIT,
)]
struct Args {
    /// Bind address.
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// gRPC port this worker serves on.
    #[arg(long, default_value_t = 50051)]
    port: u16,

    /// Address serving Prometheus metrics.
    #[arg(long, default_value = "127.0.0.1:19091")]
    metrics_addr: SocketAddr,

    #[command(flatten)]
    core: CoreArgs,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Args {
        bind,
        port,
        metrics_addr,
        core,
    } = Args::parse();
    let options = LakeSoulWorkerOptions { core_args: core };

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(options.core_args.worker_threads.max(2))
        .enable_all()
        .build()?
        .block_on(async move {
            // The OTLP/gRPC exporter builds its tonic channel here; that needs
            // an active Tokio reactor.
            let tracing_guard = Arc::new(init_tracing(
                TracingConfig::new("lakesoul-worker"),
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
                LogFormat::new()
                    .with_target(false)
                    .with_file(false)
                    // Tee'd into LAKESOUL_LOG_DIR for Loki: escape codes would
                    // end up in the log store, so color only a real terminal.
                    .with_ansi(std::io::stdout().is_terminal()),
                JiffTime::beijing("%m-%d %T%.3f %Z"),
            )?);
            // Dynamic log level: edit LAKESOUL_LOG_FILTER_FILE (or RUST_LOG)
            // and `kill -HUP <pid>`.
            spawn_reload_on_sighup(tracing_guard.filter_handle());
            install_prometheus_metrics(PrometheusMetricsConfig::new(
                "lakesoul-worker",
                metrics_addr,
            ))?;
            if let Some(endpoint) = tracing_guard.otlp_endpoint() {
                info!(endpoint, "OTLP trace exporter enabled");
            }
            info!(
                version = lakesoul_build_info::VERSION,
                commit = lakesoul_build_info::GIT_COMMIT,
                target = lakesoul_build_info::TARGET,
                profile = lakesoul_build_info::PROFILE,
                metrics_addr = %metrics_addr,
                "starting LakeSoul worker (protocol {DISTRIBUTED_PROTOCOL_VERSION}) on {bind}:{port}"
            );

            let addr: SocketAddr = format!("{bind}:{port}").parse()?;
            let listener = tokio::net::TcpListener::bind(addr).await?;
            spawn_lakesoul_worker(&options, listener).await?;
            Ok(())
        })
}
