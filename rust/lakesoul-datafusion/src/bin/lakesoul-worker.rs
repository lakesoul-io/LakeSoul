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

use std::net::SocketAddr;

use clap::Parser;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::distributed::{
    DISTRIBUTED_PROTOCOL_VERSION, LakeSoulWorkerOptions, spawn_lakesoul_worker,
};
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "lakesoul-worker", about = "LakeSoul distributed worker")]
struct Args {
    /// Bind address.
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// gRPC port this worker serves on.
    #[arg(long, default_value_t = 50051)]
    port: u16,

    #[command(flatten)]
    core: CoreArgs,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let options = LakeSoulWorkerOptions {
        core_args: args.core,
    };
    info!(
        "starting LakeSoul worker (protocol {DISTRIBUTED_PROTOCOL_VERSION}) on {}:{}",
        args.bind, args.port
    );

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(options.core_args.worker_threads.max(2))
        .enable_all()
        .build()?
        .block_on(async move {
            let addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse()?;
            let listener = tokio::net::TcpListener::bind(addr).await?;
            spawn_lakesoul_worker(&options, listener).await?;
            Ok(())
        })
}
