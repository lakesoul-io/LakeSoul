// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{net::SocketAddr, sync::Arc, time::Duration};

use clap::Parser;
use lakesoul_datafusion::cli::{CoreArgs, DistributedArgs};
use lakesoul_datafusion::distributed::compaction_coordinator::{
    CompactionCoordinator, ExecutionConfig,
};
use lakesoul_datafusion::distributed::compaction_discovery::{
    DEFAULT_MIRROR_NAMESPACE, DiscoveryConfig,
};
use lakesoul_datafusion::{
    MetaDataClient, create_lakesoul_session_config, session::LakeSoulSessionFactory,
};

#[derive(Parser)]
struct Args {
    /// Serve Prometheus metrics at this address when configured.
    #[arg(long, env = "LAKESOUL_COMPACTION_METRICS_ADDR")]
    metrics_addr: Option<SocketAddr>,
    #[command(flatten)]
    core: CoreArgs,
    #[command(flatten)]
    distributed: DistributedArgs,
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..))]
    period_seconds: u64,
    #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u64).range(1..))]
    execution_timeout_seconds: u64,
    #[arg(long, default_value = DEFAULT_MIRROR_NAMESPACE)]
    mirror_namespace: String,
    #[arg(long, value_delimiter = ',')]
    exclude_namespace: Vec<String>,
    #[arg(long, value_delimiter = ',', value_parser = qualified_table)]
    exclude_table: Vec<(String, String)>,
}

fn qualified_table(name: &str) -> Result<(String, String), String> {
    name.split_once('.')
        .filter(|(ns, t)| !ns.is_empty() && !t.is_empty() && !t.contains('.'))
        .map(|(ns, t)| (ns.to_owned(), t.to_owned()))
        .ok_or_else(|| format!("expected namespace.table, got {name}"))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();
    if let Some(addr) = args.metrics_addr {
        lakesoul_observability::install_prometheus_metrics(
            lakesoul_observability::PrometheusMetricsConfig::new(
                "lakesoul-compaction",
                addr,
            ),
        )?;
    }
    let catalog = Arc::new(MetaDataClient::from_env().await?);
    // Discovery reads the mirror namespace and compaction publishes back to
    // the same metadata database.
    let source = catalog.clone();
    let distributed = args
        .distributed
        .resolve_distributed_options()
        .await
        .map_err(|e| e.to_string())?
        .ok_or("worker discovery is required")?;
    let factory =
        LakeSoulSessionFactory::new(catalog, &args.core).map_err(|e| e.to_string())?;
    let mirror = factory.create_local_session().map_err(|e| e.to_string())?;
    let template = create_lakesoul_session_config().map_err(|e| e.to_string())?;
    let execution = factory
        .with_session_template(template)
        .with_distributed(distributed)
        .create_session(&Default::default())
        .map_err(|e| e.to_string())?;
    let mut coordinator = CompactionCoordinator::new(
        mirror,
        DiscoveryConfig {
            mirror_namespace: args.mirror_namespace,
            excluded_namespaces: args.exclude_namespace.into_iter().collect(),
            excluded_tables: args.exclude_table.into_iter().collect(),
        },
    )
    .with_execution(
        execution,
        ExecutionConfig {
            policy: Default::default(),
            reader_options: Default::default(),
            writer_options: Default::default(),
            execution_timeout: Duration::from_secs(args.execution_timeout_seconds),
        },
    );
    #[cfg(unix)]
    let mut terminate =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let shutdown = async {
        #[cfg(unix)]
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };
    coordinator
        .run(&source, Duration::from_secs(args.period_seconds), shutdown)
        .await?;
    Ok(())
}
