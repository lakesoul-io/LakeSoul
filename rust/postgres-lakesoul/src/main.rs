// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
//

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use datafusion_postgres::{ServerOptions, auth::AuthManager, serve_with_handlers};
use lakesoul_common::misc::JiffTime;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::distributed::DistributedOptions;
use lakesoul_metadata::MetaDataClient;
use lakesoul_observability::{
    LogFormat, PrometheusMetricsConfig, TracingConfig, TracingGuard, init_tracing,
    install_prometheus_metrics, spawn_reload_on_sighup,
};
use tokio::runtime::{self};
use tracing::{info, warn};

use crate::limits::{Limits, ServerLimits};
use crate::server::LakeSoulHandlers;
use crate::session::PgSessionFactory;

mod cancel;
mod catalog;
mod limits;
mod pg_compat;
mod read_only;
mod server;
mod session;
use rootcause::Report;

pub(crate) type Result<T, E = Report> = std::result::Result<T, E>;

fn init_logger() -> Result<TracingGuard> {
    Ok(init_tracing(
        TracingConfig::new("postgres-lakesoul"),
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        LogFormat::new().with_target(false).with_file(false),
        JiffTime::beijing("%m-%d %T%.3f %Z"),
    )?)
}

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    pub core: CoreArgs,
    /// Port the server listens to, default to 5432
    #[clap(short, long, default_value_t = 5432)]
    port: u16,
    /// Address serving Prometheus metrics.
    #[clap(long, default_value = "127.0.0.1:19090")]
    metrics_addr: SocketAddr,

    /// Distributed workers to plan against, comma separated; empty keeps every
    /// session single-node
    #[clap(long, value_delimiter = ',')]
    distributed_workers: Vec<String>,

    /// Fall back to single-node execution when the distributed planner fails
    /// to plan a query, or plans one whose stages cannot be sent to a worker.
    /// If disabled, that failure fails the query instead of silently running
    /// it on the coordinator.
    #[clap(long)]
    distributed_fallback_local: bool,

    /// Overrides the distributed planner's bytes-per-partition scan estimate.
    /// Lower values fan a scan out over more tasks (and therefore workers);
    /// scans whose estimated size stays below the default (16 MiB) per
    /// partition are kept on the coordinator.
    #[clap(long)]
    distributed_bytes_per_partition: Option<usize>,

    /// Maximum number of concurrent connections, 0 for unlimited
    #[clap(long, default_value_t = 0)]
    max_connections: usize,

    /// Maximum number of concurrent statements per user, 0 for unlimited
    #[clap(long, default_value_t = 0)]
    max_queries_per_user: usize,

    /// Maximum number of rows a statement may return, 0 for unlimited
    #[clap(long, default_value_t = 0)]
    max_result_rows: usize,

    /// Maximum encoded size a statement's result may reach, 0 for unlimited
    #[clap(long, default_value_t = 0)]
    max_result_bytes: usize,
}

async fn main_inner(cli: Cli) -> Result<()> {
    let meta_client = Arc::new(MetaDataClient::from_env().await?);
    let auth_manager = Arc::new(AuthManager::new());
    let session_factory = PgSessionFactory::new(
        Arc::clone(&meta_client),
        &cli.core,
        Arc::clone(&auth_manager),
    )?;
    let tracing = Arc::new(init_logger()?);
    // Dynamic log level: edit LAKESOUL_LOG_FILTER_FILE (or RUST_LOG) and
    // `kill -HUP <pid>`.
    spawn_reload_on_sighup(tracing.filter_handle());
    install_prometheus_metrics(PrometheusMetricsConfig::new(
        "postgres-lakesoul",
        cli.metrics_addr,
    ))?;
    if let Some(endpoint) = tracing.otlp_endpoint() {
        info!(endpoint, "OTLP trace exporter enabled");
    }
    info!(metrics_addr = %cli.metrics_addr, "Prometheus metrics endpoint started");
    // Warm the shared catalog view once: every connection lists from it, so
    // the first one must not pay for the initial metadata load.
    if let Err(err) = session_factory.catalog_snapshot().load().await {
        warn!("initial catalog view load failed: {err}");
    }

    let distributed = if cli.distributed_workers.is_empty() {
        None
    } else {
        // TODO(jiax): add k8s
        let mut options =
            DistributedOptions::static_workers(cli.distributed_workers.clone());
        options.fallback_to_local = cli.distributed_fallback_local;
        options.bytes_per_partition = cli.distributed_bytes_per_partition;
        info!(
            "distributed planning enabled against {:?} (fallback_to_local={}, bytes_per_partition={:?})",
            cli.distributed_workers,
            options.fallback_to_local,
            cli.distributed_bytes_per_partition,
        );
        Some(options)
    };
    let session_factory = match distributed {
        Some(options) => session_factory.with_distributed(options),
        None => session_factory,
    };

    info!(
        "start serving on 127.0.0.1:{} (max_connections={}, max_queries_per_user={}, max_result_rows={}, max_result_bytes={})",
        cli.port,
        cli.max_connections,
        cli.max_queries_per_user,
        cli.max_result_rows,
        cli.max_result_bytes
    );

    let server_opts = ServerOptions::new()
        .with_host(String::from("127.0.0.1"))
        .with_port(cli.port);

    let limits = ServerLimits::new(Limits {
        max_connections: cli.max_connections,
        max_queries_per_user: cli.max_queries_per_user,
        max_result_rows: cli.max_result_rows,
        max_result_bytes: cli.max_result_bytes,
    });

    serve_with_handlers(
        Arc::new(LakeSoulHandlers::new(Arc::new(session_factory), limits)),
        &server_opts,
    )
    .await?;
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(cli.core.worker_threads.max(2))
        .thread_name("pg-lakesoul")
        .thread_stack_size(3 * 1024 * 1024) // 3MB
        .build()?;
    rt.block_on(main_inner(cli))?;
    Ok(())
}
