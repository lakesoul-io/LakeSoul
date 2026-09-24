// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
//

use std::sync::{Arc, Once};

use clap::Parser;
use datafusion_postgres::{ServerOptions, auth::AuthManager, serve_with_handlers};
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::distributed::DistributedOptions;
use lakesoul_metadata::MetaDataClient;
use rootcause::Report;
use tokio::runtime::{self};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::server::LakeSoulHandlers;
use crate::session::PgSessionFactory;

mod cancel;
mod catalog;
mod misc;
mod pg_compat;
mod read_only;
mod server;
mod session;

pub(crate) type Result<T, E = Report> = std::result::Result<T, E>;

fn init_logger() {
    static TRACING: Once = Once::new();
    TRACING.call_once(|| {
        let timer = misc::JiffTime::beijing("%m-%d %T%.3f %Z");
        tracing_subscriber::fmt()
            .with_level(true)
            .with_target(true)
            .with_timer(timer)
            .with_env_filter(EnvFilter::from_default_env())
            .with_file(false)
            .with_ansi(true)
            .init();
    });
}

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    pub core: CoreArgs,
    /// Port the server listens to, default to 5432
    #[clap(short, long, default_value_t = 5432)]
    port: u16,
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
}

async fn main_inner(cli: Cli) -> Result<()> {
    let meta_client = Arc::new(MetaDataClient::from_env().await?);
    let auth_manager = Arc::new(AuthManager::new());
    let session_factory = PgSessionFactory::new(
        Arc::clone(&meta_client),
        &cli.core,
        Arc::clone(&auth_manager),
    )?;
    init_logger();
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
        info!(
            "distributed planning enabled against {:?} (fallback_to_local={} )",
            cli.distributed_workers, options.fallback_to_local,
        );
        Some(options)
    };
    let session_factory = match distributed {
        Some(options) => session_factory.with_distributed(options),
        None => session_factory,
    };

    let server_opts = ServerOptions::new()
        .with_host(String::from("127.0.0.1"))
        .with_port(cli.port);
    info!("start serving on 127.0.0.1:{}", cli.port);
    serve_with_handlers(
        Arc::new(LakeSoulHandlers::new(Arc::new(session_factory))),
        &server_opts,
    )
    .await?;
    Ok(())
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(cli.core.worker_threads)
        .thread_name("pg-lakesoul")
        .thread_stack_size(3 * 1024 * 1024) // 3MB
        .build()?;
    rt.block_on(main_inner(cli))?;
    Ok(())
}
