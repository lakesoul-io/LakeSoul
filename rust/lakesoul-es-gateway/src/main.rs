// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `lakesoul-es-gateway` binary entry point.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
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
    let router = build_router(state);

    let address: SocketAddr = listen.parse()?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!("lakesoul-es-gateway listening on {address}");
    axum::serve(listener, router).await?;
    Ok(())
}
