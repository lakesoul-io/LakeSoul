// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
//

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use datafusion_postgres::{ServerOptions, auth::AuthManager, serve_with_handlers};
use lakesoul_common::misc::JiffTime;
use lakesoul_datafusion::cli::{CoreArgs, DistributedArgs};
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
mod pg_vector;
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
        LogFormat::new()
            .with_target(false)
            .with_file(false)
            // Tee'd into LAKESOUL_LOG_DIR for Loki: escape codes would end up
            // in the log store, so color only a real terminal.
            .with_ansi(std::io::stdout().is_terminal()),
        JiffTime::beijing("%m-%d %T%.3f %Z"),
    )?)
}

#[derive(Parser)]
#[command(
    name = "postgres-lakesoul",
    about = "LakeSoul PostgreSQL wire server",
    version = lakesoul_build_info::VERSION_WITH_COMMIT
)]
struct Cli {
    #[command(flatten)]
    pub core: CoreArgs,

    #[command(flatten)]
    pub distributed: DistributedArgs,

    /// Bind address, defaults to the loopback interface; container/cluster
    /// deployments set `LAKESOUL_PG_BIND=0.0.0.0` explicitly.
    #[clap(long, env = "LAKESOUL_PG_BIND", default_value = "127.0.0.1")]
    bind: String,

    /// Port the server listens to, default to 5432
    #[clap(short, long, env = "LAKESOUL_PG_PORT", default_value_t = 5432)]
    port: u16,

    /// Address serving Prometheus metrics.
    #[clap(
        long,
        env = "LAKESOUL_PG_METRICS_ADDR",
        default_value = "127.0.0.1:19090"
    )]
    metrics_addr: SocketAddr,

    /// Maximum number of concurrent connections, 0 for unlimited
    #[clap(long, env = "LAKESOUL_PG_MAX_CONNECTIONS", default_value_t = 0)]
    max_connections: usize,

    /// Maximum number of concurrent statements per user, 0 for unlimited
    #[clap(long, env = "LAKESOUL_PG_MAX_QUERIES_PER_USER", default_value_t = 0)]
    max_queries_per_user: usize,

    /// Maximum number of rows a statement may return, 0 for unlimited
    #[clap(long, env = "LAKESOUL_PG_MAX_RESULT_ROWS", default_value_t = 0)]
    max_result_rows: usize,

    /// Maximum encoded size a statement's result may reach, 0 for unlimited
    #[clap(long, env = "LAKESOUL_PG_MAX_RESULT_BYTES", default_value_t = 0)]
    max_result_bytes: usize,
}

async fn main_inner(cli: Cli) -> Result<()> {
    // Installed first: constructing the session factory below starts the
    // catalog refresher, whose first refresh would otherwise run without a
    // subscriber and export its queries as parentless root traces.
    let tracing = Arc::new(init_logger()?);
    // Dynamic log level: edit LAKESOUL_LOG_FILTER_FILE (or RUST_LOG) and
    // `kill -HUP <pid>`.
    spawn_reload_on_sighup(tracing.filter_handle());
    let meta_client = Arc::new(MetaDataClient::from_env().await?);
    let auth_manager = Arc::new(AuthManager::new());
    let session_factory = PgSessionFactory::new(
        Arc::clone(&meta_client),
        &cli.core,
        Arc::clone(&auth_manager),
    )?;
    info!(
        version = lakesoul_build_info::VERSION,
        commit = lakesoul_build_info::GIT_COMMIT,
        target = lakesoul_build_info::TARGET,
        profile = lakesoul_build_info::PROFILE,
        "postgres-lakesoul build identity"
    );
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

    // One resolver (and one discovery loop) per process: resolution built it
    // once at start-up; `DistributedOptions::discovery` hands it to every
    // session via WorkerDiscovery::Shared.
    let distributed = cli.distributed.resolve_distributed_options().await?;
    if let Some(options) = &distributed {
        info!(
            "distributed planning enabled (fallback_to_local={}, bytes_per_partition={:?})",
            options.fallback_to_local, options.bytes_per_partition,
        );
    }
    let session_factory = match distributed {
        Some(options) => session_factory.with_distributed(options),
        None => session_factory,
    };

    info!(
        "start serving on {}:{} (max_connections={}, max_queries_per_user={}, max_result_rows={}, max_result_bytes={})",
        cli.bind,
        cli.port,
        cli.max_connections,
        cli.max_queries_per_user,
        cli.max_result_rows,
        cli.max_result_bytes
    );

    let server_opts = ServerOptions::new()
        .with_host(cli.bind.clone())
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
#[cfg(test)]
mod cli_probe_tests {
    use super::*;

    const PROBE_FILTER: &str = "pg_cli_probe";
    const PROBE_ARGV: &str = "LAKESOUL_PG_CLI_PROBE_ARGV";
    const PROBE_RESULT_PREFIX: &str = "LAKESOUL_PG_CLI_PROBE_RESULT ";

    /// Single-line redacted CLI summary the parent test asserts on.
    fn probe_result(cli: &Cli) -> serde_json::Value {
        serde_json::json!({
            "bind": cli.bind,
            "port": cli.port,
            "metrics_addr": cli.metrics_addr.to_string(),
            "max_connections": cli.max_connections,
        })
    }

    #[test]
    #[ignore = "subprocess probe target; re-invoked by the priority tests"]
    fn pg_cli_probe() {
        let argv: Vec<String> =
            serde_json::from_str(&std::env::var(PROBE_ARGV).expect("probe argv payload"))
                .expect("probe argv json");
        let cli =
            Cli::try_parse_from(std::iter::once("pg-cli-probe".to_string()).chain(argv))
                .expect("probe argv parse");
        println!("{}{}", PROBE_RESULT_PREFIX, probe_result(&cli));
    }

    #[test]
    fn pg_fields_follow_cli_beats_env_beats_default() {
        let defaults = probe(&[], &[]);
        assert_eq!(defaults["bind"], "127.0.0.1");
        assert_eq!(defaults["port"], 5432);
        assert_eq!(defaults["max_connections"], 0);

        let via_env = probe(
            &[
                ("LAKESOUL_PG_BIND", "0.0.0.0"),
                ("LAKESOUL_PG_PORT", "15432"),
                ("LAKESOUL_PG_MAX_CONNECTIONS", "64"),
            ],
            &[],
        );
        assert_eq!(via_env["bind"], "0.0.0.0");
        assert_eq!(via_env["port"], 15432);
        assert_eq!(via_env["max_connections"], 64);

        // Explicitly passed values that equal the defaults still beat env.
        let via_cli = probe(
            &[
                ("LAKESOUL_PG_BIND", "0.0.0.0"),
                ("LAKESOUL_PG_PORT", "15432"),
            ],
            &["--bind", "127.0.0.1", "--port", "5432"],
        );
        assert_eq!(via_cli["bind"], "127.0.0.1");
        assert_eq!(via_cli["port"], 5432);
    }

    /// Runs the probe subprocess with a cleared environment plus `envs`.
    fn probe(envs: &[(&str, &str)], args: &[&str]) -> serde_json::Value {
        let exe = std::env::current_exe().unwrap();
        let mut command = std::process::Command::new(exe);
        command.env_clear();
        command.env(PROBE_ARGV, serde_json::to_string(args).unwrap());
        for (key, value) in envs {
            command.env(key, value);
        }
        let output = command
            .args([PROBE_FILTER, "--ignored", "--test-threads=1", "--nocapture"])
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        // libtest interleaves captured output with progress lines; the
        // marker can end up mid-line.
        let line = stdout
            .lines()
            .find_map(|line| line.split_once(PROBE_RESULT_PREFIX).map(|(_, rest)| rest));
        let Some(line) = line else {
            panic!("probe printed no result\nstdout:\n{stdout}\nstderr:\n{stderr}");
        };
        serde_json::from_str(line).unwrap()
    }
}
