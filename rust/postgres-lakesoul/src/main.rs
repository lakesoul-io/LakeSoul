// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
//

use std::sync::{Arc, Once};

use clap::Parser;
use datafusion_postgres::{ServerOptions, auth::AuthManager, serve_with_handlers};
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_metadata::MetaDataClient;
use rootcause::Report;
use tokio::runtime::{self};
use tracing::info;
use tracing_subscriber::EnvFilter;

use crate::server::LakeSoulHandlers;
use crate::session::PgSessionFactory;

mod catalog;
mod misc;
mod read_only;
mod server;
mod session;

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
    /// Port the server listens to, default to 5432
    #[clap(short, default_value_t = 5432)]
    port: u16,
}

async fn main_inner() -> Result<(), Report> {
    let cli = Cli::parse();
    let meta_client = Arc::new(MetaDataClient::from_env().await?);
    let auth_manager = Arc::new(AuthManager::new());
    let session_factory = PgSessionFactory::new(
        Arc::clone(&meta_client),
        &CoreArgs::from_env(),
        Arc::clone(&auth_manager),
    )?;
    init_logger();
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

fn main() -> Result<(), Report> {
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(8)
        .thread_name("pg-lakesoul")
        .thread_stack_size(3 * 1024 * 1024) // 3MB
        .build()?;
    rt.block_on(main_inner())?;
    Ok(())
}
