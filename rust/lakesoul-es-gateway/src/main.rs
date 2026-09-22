// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `lakesoul-es-gateway` binary entry point.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use lakesoul_es_gateway::config::IndexBuildMode;
use lakesoul_es_gateway::{GatewayConfig, build_router, build_state};

#[derive(Debug, Parser)]
#[command(
    name = "lakesoul-es-gateway",
    about = "Elasticsearch-compatible API gateway over LakeSoul"
)]
struct Args {
    /// TOML configuration file.
    #[arg(long, default_value = "lakesoul-es-gateway.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let config = GatewayConfig::load(&args.config)?;
    let listen = config.server.listen.clone();
    let state = build_state(config).await?;
    if state.config.defaults.index_build == IndexBuildMode::Deferred {
        tracing::info!("deferred secondary-index maintenance enabled");
        let namespace = state.config.lakesoul.namespace.clone();
        let tables: Vec<String> = state
            .config
            .indexes
            .iter()
            .map(|index| index.table_name().to_string())
            .collect();
        let interval = std::time::Duration::from_secs(
            state.config.defaults.index_build_interval_secs.max(1),
        );
        lakesoul_es_gateway::maintenance::spawn(namespace, tables, interval);
    }
    let router = build_router(state);

    let address: SocketAddr = listen.parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!("lakesoul-es-gateway listening on {address}");
    axum::serve(listener, router).await?;
    Ok(())
}
