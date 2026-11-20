// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
//

use std::io::IsTerminal;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::Parser;
use datafusion_postgres::{
    ServerOptions, TcpKeepalive, auth::AuthManager, serve_with_handlers,
};
use lakesoul_common::misc::{JiffTime, SecretMap};
use lakesoul_datafusion::cli::{CoreArgs, DistributedArgs, IvmArgs};
use lakesoul_datafusion::session::{ExecutionMemoryArgs, IvmRefreshSessionFactory};
use lakesoul_ivm::IvmRuntimeConfig;
use lakesoul_metadata::MetaDataClient;
use lakesoul_observability::{
    LogFormat, MemorySamplingArgs, PrometheusMetricsConfig, TracingConfig, TracingGuard,
    init_tracing, install_prometheus_metrics, spawn_reload_on_sighup,
};
use tokio::runtime::{self};
use tracing::{info, warn};

use crate::ivm::IvmService;
use crate::limits::{Limits, ServerLimits};
use crate::server::LakeSoulHandlers;
use crate::session::PgSessionFactory;

mod acl;
mod allocator;
mod alter;
mod auth;
mod cancel;
mod catalog;
mod copy_in;
mod copy_out;
mod cursor;
mod ddl;
mod delete;
mod failed_transaction;
mod federation;
mod grant;
mod index_ddl;
mod index_runner;
mod insert;
mod ivm;
mod jobs;
mod limits;
mod pg_compat;
mod pg_numeric;
mod pg_settings;
mod pg_types;
mod pg_vector;
mod read_only;
mod server;
mod session;
mod session_settings;
mod subscribe;
mod transaction;
mod truncate;
mod update;
mod user;
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

    /// IVM refresh-engine options shared by the coordinator and its workers.
    #[command(flatten)]
    pub ivm_runtime: IvmArgs,

    #[command(flatten)]
    pub memory: ExecutionMemoryArgs,

    #[command(flatten)]
    pub memory_sampling: MemorySamplingArgs,

    #[command(flatten)]
    pub subscribe: subscribe::SubscribeArgs,

    /// Who owns subscriber processes: `local` starts them in this server,
    /// `external` leaves them to a `--subscription-runner` process.
    #[clap(
        long,
        env = "LAKESOUL_PG_SUBSCRIPTION_EXECUTOR",
        value_enum,
        default_value = "local"
    )]
    subscription_executor: subscribe::supervisor::SubscriptionMode,

    /// Run only the subscription supervisor (no PG wire server). It reconciles
    /// SQL-created subscriptions from the metadata database and starts their
    /// child processes.
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIPTION_RUNNER")]
    subscription_runner: bool,

    /// Stable identity of this subscription runner, used for the metadata
    /// lease. Defaults to `$HOSTNAME`, then a pid-based name.
    #[clap(long, env = "LAKESOUL_PG_RUNNER_ID")]
    runner_id: Option<String>,

    /// How often the subscription runner reconciles metadata, in milliseconds.
    #[clap(
        long,
        env = "LAKESOUL_PG_RUNNER_POLL_MS",
        default_value_t = 2_000,
        value_parser = clap::value_parser!(u64).range(100..)
    )]
    runner_poll_ms: u64,

    /// Who owns deferred-index builds: `local` starts one child process,
    /// `external` leaves it to a separate Deployment, `off` disables it.
    #[clap(
        long,
        env = "LAKESOUL_PG_INDEX_MAINTENANCE_EXECUTOR",
        value_enum,
        default_value = "local"
    )]
    index_maintenance_executor: index_runner::IndexMaintenanceExecutor,

    /// Run only the deferred-index builder (no PG wire server).
    #[clap(long, env = "LAKESOUL_PG_INDEX_MAINTENANCE_RUNNER")]
    index_maintenance_runner: bool,

    /// Stable identity of this index runner, used for the metadata lease.
    #[clap(long, env = "LAKESOUL_PG_INDEX_RUNNER_ID")]
    index_runner_id: Option<String>,

    /// How often the deferred-index runner scans metadata, in milliseconds.
    #[clap(
        long,
        env = "LAKESOUL_PG_INDEX_MAINTENANCE_POLL_MS",
        default_value_t = 5_000,
        value_parser = clap::value_parser!(u64).range(100..)
    )]
    index_maintenance_poll_ms: u64,

    /// Lifetime of the index-maintenance lease, in milliseconds.
    #[clap(
        long,
        env = "LAKESOUL_PG_INDEX_LEASE_TTL_MS",
        default_value_t = 60_000,
        value_parser = clap::value_parser!(u64).range(1_000..)
    )]
    index_lease_ttl_ms: u64,

    /// Start every new connection with `lakesoul.ivm.enabled = on`. IVM is
    /// always installed; without this flag only a session's own `SET` turns
    /// it on. A session can still disable it with `SET`.
    #[clap(long, env = "LAKESOUL_PG_IVM_DEFAULT")]
    ivm_default: bool,

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

    /// Do not install the Prometheus metrics endpoint. Subscriber children use
    /// this so they do not contend with the server or each other on one port.
    #[clap(long, env = "LAKESOUL_PG_NO_METRICS")]
    no_metrics: bool,

    /// Directory for supervised subscriber stdout/stderr logs. Defaults to
    /// `LAKESOUL_LOG_DIR` when set, otherwise a directory under the system
    /// temporary directory.
    #[clap(long, env = "LAKESOUL_PG_SUBSCRIPTION_LOG_DIR")]
    subscription_log_dir: Option<PathBuf>,

    /// Maximum number of concurrent connections, 0 for unlimited
    #[clap(long, env = "LAKESOUL_PG_MAX_CONNECTIONS", default_value_t = 0)]
    max_connections: usize,

    /// Maximum number of concurrent statements per user, 0 for unlimited
    #[clap(long, env = "LAKESOUL_PG_MAX_QUERIES_PER_USER", default_value_t = 0)]
    max_queries_per_user: usize,

    /// Maximum number of concurrent statements across all users, 0 for
    /// unlimited. A suspended portal holds its slot until it is closed.
    #[clap(long, env = "LAKESOUL_PG_MAX_CONCURRENT_QUERIES", default_value_t = 0)]
    max_concurrent_queries: usize,

    /// Maximum number of rows a statement may return, 0 for unlimited
    #[clap(long, env = "LAKESOUL_PG_MAX_RESULT_ROWS", default_value_t = 0)]
    max_result_rows: usize,

    /// Maximum encoded size a statement's result may reach, 0 for unlimited
    #[clap(long, env = "LAKESOUL_PG_MAX_RESULT_BYTES", default_value_t = 0)]
    max_result_bytes: usize,

    /// Files one transaction block may stage before COMMIT, 0 for unlimited
    #[clap(
        long,
        env = "LAKESOUL_PG_MAX_STAGED_FILES_PER_TRANSACTION",
        default_value_t = 0
    )]
    max_staged_files_per_transaction: usize,

    /// Bytes of staged files one transaction block may hold, 0 for unlimited
    #[clap(
        long,
        env = "LAKESOUL_PG_MAX_STAGED_BYTES_PER_TRANSACTION",
        default_value_t = 0
    )]
    max_staged_bytes_per_transaction: usize,

    /// Seconds a client connection may be idle before TCP keepalive probes
    /// start, 0 to leave keepalive off. Without it, a client that vanishes
    /// without closing its socket keeps its connection - and every slot it
    /// holds - open indefinitely.
    #[clap(
        long,
        env = "LAKESOUL_PG_TCP_KEEPALIVE_IDLE_SECS",
        default_value_t = 60
    )]
    tcp_keepalive_idle_secs: u64,

    /// Seconds between unanswered TCP keepalive probes, 0 for the OS default
    #[clap(
        long,
        env = "LAKESOUL_PG_TCP_KEEPALIVE_INTERVAL_SECS",
        default_value_t = 10
    )]
    tcp_keepalive_interval_secs: u64,

    /// Unanswered TCP keepalive probes before a connection is dropped, 0 for
    /// the OS default
    #[clap(long, env = "LAKESOUL_PG_TCP_KEEPALIVE_RETRIES", default_value_t = 6)]
    tcp_keepalive_retries: u32,

    /// Maximum number of detached queries running at once, 0 for unlimited
    #[clap(long, env = "LAKESOUL_PG_MAX_JOBS", default_value_t = 4)]
    max_jobs: usize,

    /// Retention of a detached SELECT result, in hours
    #[clap(long, env = "LAKESOUL_PG_JOB_RESULT_TTL_HOURS", default_value_t = 24)]
    job_result_ttl_hours: u64,

    /// Retention of detached query history, in hours
    #[clap(long, env = "LAKESOUL_PG_JOB_RECORD_TTL_HOURS", default_value_t = 72)]
    job_record_ttl_hours: u64,

    /// Maximum size of a detached SELECT result, in MiB
    #[clap(long, env = "LAKESOUL_PG_JOB_RESULT_MAX_MIB", default_value_t = 1024)]
    job_result_max_mib: u64,

    /// Stable identity of this pg-wire cluster for detached-job recovery.
    /// Defaults to <bind>:<port>; set it when the listen address can change
    /// across restarts.
    #[clap(long, env = "LAKESOUL_PG_JOB_CLUSTER_ID")]
    job_cluster_id: Option<String>,

    /// PEM certificate chain for TLS. Requires `--tls-key`; the key must be
    /// PKCS#8 (`openssl pkcs8 -topk8 -nocrypt`). The listener offers TLS only
    /// when both are configured.
    #[clap(long, env = "LAKESOUL_PG_TLS_CERT", requires = "tls_key")]
    tls_cert: Option<PathBuf>,

    /// PEM PKCS#8 private key for TLS. Requires `--tls-cert`.
    #[clap(long, env = "LAKESOUL_PG_TLS_KEY", requires = "tls_cert")]
    tls_key: Option<PathBuf>,

    /// Refuse clients that do not negotiate TLS. Requires `--tls-cert` and
    /// `--tls-key`; deployments that terminate TLS in front of the server
    /// leave this off.
    #[clap(long, env = "LAKESOUL_PG_REQUIRE_TLS", requires = "tls_cert")]
    require_tls: bool,

    /// Require a LakeSoul JWT as the password of every connection. The token
    /// is the one the Flight server and the S3 proxy accept: HS256 with the
    /// metadata client secret, and its `sub` must equal the connecting user.
    /// It travels as a cleartext password: configure `--tls-cert`/`--tls-key`
    /// with `--require-tls`, or terminate TLS in front of the server.
    #[clap(long, env = "LAKESOUL_PG_JWT_AUTH")]
    jwt_auth: bool,

    /// Confine every connection to the LakeSoul domain in its token's
    /// `group`: namespaces and tables of other domains do not exist for it,
    /// and its new tables are created in its domain. `public` and
    /// `lake-public` objects stay visible to everyone. Requires `--jwt-auth`.
    #[clap(long, env = "LAKESOUL_PG_RBAC", requires = "jwt_auth")]
    rbac: bool,

    /// The domain this cluster creates tables in and accepts writes for.
    /// Defaults to each token's `group`; set it when one shared cluster
    /// serves several domains read-only.
    #[clap(long, env = "LAKESOUL_PG_RBAC_DOMAIN", requires = "rbac")]
    rbac_domain: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Print a LakeSoul JWT for connecting to a server started with
    /// `--jwt-auth`. The signing secret comes from the metadata connection
    /// settings; an RBAC deployment also reads the user directory.
    Token(TokenArgs),
}

#[derive(clap::Args)]
struct TokenArgs {
    /// The PostgreSQL user the token is issued to (its `sub` claim).
    #[clap(long)]
    user: String,

    /// The LakeSoul domain the user acts in (its `group` claim).
    #[clap(long)]
    group: String,

    /// Lifetime of the token: a number with an optional unit s, m, h or d.
    #[clap(long, default_value = "1h", value_parser = auth::parse_ttl)]
    ttl: std::time::Duration,
}

/// Prints one token on stdout and nothing else, so it can be captured as
/// `PGPASSWORD=$(postgres-lakesoul token ...)`.
///
/// The signing secret comes from the metadata settings. An RBAC deployment
/// keeps its users in `domain_user`, so minting also connects once to validate
/// `(user, group)`; a legacy metadata database without that table keeps the
/// old offline signing behaviour.
async fn print_token(args: &TokenArgs) -> Result<()> {
    let secret = lakesoul_metadata::client_secret_from_env()?;
    let jwt = lakesoul_metadata::JwtServer::new(&secret);

    // An RBAC deployment keeps its users in metadata. Validate the pair before
    // issuing a token; a legacy JWT-only metadata database without the table
    // keeps the old offline behaviour.
    let metadata = lakesoul_metadata::MetaDataClient::from_env().await?;
    if metadata.has_user_directory().await?
        && !metadata.is_domain_user(&args.group, &args.user).await?
    {
        rootcause::bail!("user {} is not enabled in domain {}", args.user, args.group);
    }

    let token = auth::mint_token(&jwt, &args.user, &args.group, args.ttl)
        .map_err(|error| rootcause::report!("{error}"))?;
    println!("{token}");
    Ok(())
}

async fn main_inner(cli: Cli) -> Result<()> {
    // Installed first: constructing the session factory below starts the
    // catalog refresher, whose first refresh would otherwise run without a
    // subscriber and export its queries as parentless root traces.
    let tracing = Arc::new(init_logger()?);
    // Dynamic log level: edit LAKESOUL_LOG_FILTER_FILE (or RUST_LOG) and
    // `kill -HUP <pid>`.
    spawn_reload_on_sighup(tracing.filter_handle());
    allocator::log_active();
    // Validate TLS before any listener, metadata connection or worker starts.
    // datafusion-postgres' setup_tls warns and keeps serving in cleartext when
    // the material cannot be loaded; an operator who configured TLS must get a
    // hard failure instead.
    if cli.require_tls && (cli.tls_cert.is_none() || cli.tls_key.is_none()) {
        rootcause::bail!("--require-tls requires --tls-cert and --tls-key");
    }
    if let (Some(cert), Some(key)) = (&cli.tls_cert, &cli.tls_key) {
        validate_tls_material(cert, key)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(metadata) = std::fs::metadata(key)
                && metadata.permissions().mode() & 0o077 != 0
            {
                warn!(
                    key = %key.display(),
                    "TLS private key is readable by group or others"
                );
            }
        }
        info!(
            cert = %cert.display(),
            require_tls = cli.require_tls,
            "TLS configured"
        );
    } else {
        info!("TLS not configured; client traffic is unencrypted");
    }
    if cli.jwt_auth && cli.tls_cert.is_none() {
        warn!(
            "JWT authentication without TLS: the token travels as a cleartext \
             password; configure --tls-cert/--tls-key with --require-tls, or \
             terminate TLS in front of the server"
        );
    }
    if !cli.no_metrics {
        install_prometheus_metrics(PrometheusMetricsConfig::new(
            "postgres-lakesoul",
            cli.metrics_addr,
        ))?;
    }
    cancel::register_query_metrics();
    jobs::register_metrics();
    limits::register_limit_metrics();
    alter::register_metrics();
    auth::register_metrics();
    let meta_client = Arc::new(MetaDataClient::from_env().await?);
    let object_store_options = SecretMap::from_map(cli.core.s3_options());
    if cli.subscription_runner
        && (cli.subscribe.subscribe
            || cli.subscribe.subscription_id.is_some()
            || cli.index_maintenance_runner)
    {
        rootcause::bail!(
            "--subscription-runner cannot be combined with --subscribe or \
             --index-maintenance-runner"
        );
    }
    if cli.index_maintenance_runner
        && (cli.subscribe.subscribe || cli.subscribe.subscription_id.is_some())
    {
        rootcause::bail!(
            "--index-maintenance-runner cannot be combined with --subscribe"
        );
    }
    if cli.index_maintenance_runner {
        let owner = cli.index_runner_id.clone().unwrap_or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| format!("index-runner-{}", std::process::id()))
        });
        return index_runner::run(
            Arc::clone(&meta_client),
            object_store_options,
            owner,
            std::time::Duration::from_millis(cli.index_maintenance_poll_ms),
            std::time::Duration::from_millis(cli.index_lease_ttl_ms),
        )
        .await;
    }
    if cli.subscribe.subscribe || cli.subscribe.subscription_id.is_some() {
        return subscribe::run(
            cli.subscribe,
            Arc::clone(&meta_client),
            cli.core.warehouse_prefix.clone(),
        )
        .await;
    }

    let program = match std::env::current_exe() {
        Ok(program) => program,
        Err(error) => {
            warn!(%error, "cannot resolve the server binary for subscription children");
            PathBuf::new()
        }
    };
    let log_dir = cli
        .subscription_log_dir
        .clone()
        .or_else(|| std::env::var_os("LAKESOUL_LOG_DIR").map(PathBuf::from))
        .unwrap_or_else(|| std::env::temp_dir().join("lakesoul-subscriptions"));
    if cli.subscription_runner {
        if cli.subscription_executor != subscribe::supervisor::SubscriptionMode::Local {
            rootcause::bail!(
                "--subscription-runner owns local subscriber processes; it cannot \
                 also be configured as an external executor"
            );
        }
        let runner_id = cli.runner_id.clone().unwrap_or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| format!("runner-{}", std::process::id()))
        });
        return subscribe::subscription_runner::run(
            Arc::clone(&meta_client),
            program,
            log_dir,
            runner_id,
            std::time::Duration::from_millis(cli.runner_poll_ms),
        )
        .await;
    }
    let index_program = program.clone();
    let executor = cli.subscription_executor;
    let subscriptions =
        Arc::new(subscribe::supervisor::SubscriptionSupervisor::with_mode(
            Arc::clone(&meta_client),
            program,
            log_dir.clone(),
            executor,
        ));
    info!(
        log_dir = %log_dir.display(),
        executor = ?executor,
        "subscription supervisor ready"
    );
    {
        let metadata = subscriptions.metadata().await;
        if let Err(error) = metadata.init_schema().await {
            warn!(%error, "subscription metadata schema is unavailable");
        }
    }
    if executor == subscribe::supervisor::SubscriptionMode::Local {
        if let Err(error) = subscriptions.resume_all().await {
            warn!(%error, "failed to resume subscriptions");
        }
    } else {
        info!(
            "external subscription executor configured; start \
             `postgres-lakesoul --subscription-runner` to run subscribers"
        );
    }

    let auth_manager = Arc::new(AuthManager::new());
    cli.memory.install_vortex_decode_budget()?;
    let memory_pool = cli.memory.create_pool();
    {
        let pool = Arc::clone(&memory_pool);
        cli.memory_sampling
            .spawn(allocator::stats_reader(), move || pool.reserved())?;
    }
    let federation = Arc::new(federation::FederationRuntime::new(&meta_client).await?);
    if cli.rbac && !cli.jwt_auth {
        // clap enforces this for the flags; checked again so no other way
        // of setting them can turn domain enforcement on without tokens.
        rootcause::bail!("--rbac requires --jwt-auth: the domain comes from the token");
    }
    let auth = if cli.jwt_auth {
        info!(
            rbac = cli.rbac,
            "JWT authentication enabled: every connection must send a LakeSoul \
             token for its user as the password"
        );
        let settings =
            auth::AuthSettings::jwt(meta_client.get_client_secret()).with_rbac(cli.rbac);
        if cli.rbac {
            settings.with_user_directory(Arc::new(auth::UserDirectory::new(Arc::clone(
                &meta_client,
            ))))
        } else {
            settings
        }
    } else {
        auth::AuthSettings::default()
    };
    let auth = auth.with_require_tls(cli.require_tls);
    let session_factory = PgSessionFactory::new_with_memory_pool(
        Arc::clone(&meta_client),
        &cli.core,
        Arc::clone(&auth_manager),
        memory_pool,
    )?
    .with_subscriptions(Arc::clone(&subscriptions))
    .with_federation(federation)
    .with_auth(auth)
    .with_ivm_default(cli.ivm_default)
    .with_cluster_domain(cli.rbac_domain.clone());
    info!(
        version = lakesoul_build_info::VERSION,
        commit = lakesoul_build_info::GIT_COMMIT,
        target = lakesoul_build_info::TARGET,
        profile = lakesoul_build_info::PROFILE,
        "postgres-lakesoul build identity"
    );
    // Exact re-rank is the default candidate ordering of the pgvector path
    // (`ORDER BY vec <-> '[...]' LIMIT k`), and it needs the on-demand
    // decoded-block cache backed by the unified L2 cache: without it every
    // candidate row is read through the vectorized reader.
    let block_cache_enabled = lakesoul_vector::cache_config::config().disk_enabled();
    if !block_cache_enabled {
        warn!(
            "vector re-rank decoded-block cache is disabled; explicit exact \
             rerank queries (rerank='exact') fall back to the slow reader \
             path.  Set LAKESOUL_VECTOR_CACHE=1 and LAKESOUL_CACHE_SIZE > 0 \
             to enable the shared S3-SSD block cache"
        );
    }
    if let Some(endpoint) = tracing.otlp_endpoint() {
        info!(endpoint, "OTLP trace exporter enabled");
    }
    if !cli.no_metrics {
        info!(metrics_addr = %cli.metrics_addr, "Prometheus metrics endpoint started");
    }
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
    let session_factory = match distributed.clone() {
        Some(options) => session_factory.with_distributed(options),
        None => session_factory,
    };

    let distributed_enabled = distributed.is_some();
    if distributed_enabled {
        info!(
            "IVM distributed refresh enabled; workers must be built with the \
             IVM feature and the same protocol generation"
        );
    }
    let template = session_factory.create_template_session()?;
    let ivm_options = cli.ivm_runtime.options();
    info!(
        view_parallelism = ivm_options.view_parallelism,
        max_active_tasks = ?ivm_options.max_active_tasks,
        max_derived_steps = ivm_options.max_derived_steps,
        state_index = ?ivm_options.state_index,
        internal_file_format = ivm_options.internal_file_format.name(),
        "IVM refresh options"
    );
    let mut config = IvmRuntimeConfig::from_session(&template).with_options(ivm_options);
    if distributed_enabled {
        config = config.with_session_factory(Arc::new(IvmRefreshSessionFactory::new(
            session_factory.refresh_session_factory(),
        )));
    }
    let service = IvmService::new(Arc::clone(&meta_client), config).await?;
    let session_factory = session_factory.with_ivm(Arc::new(service));

    let job_config = jobs::JobConfig {
        max_jobs: cli.max_jobs,
        result_max_bytes: cli.job_result_max_mib.saturating_mul(1024 * 1024),
        result_ttl_ms: (cli.job_result_ttl_hours as i64).saturating_mul(60 * 60 * 1000),
        record_ttl_ms: (cli.job_record_ttl_hours as i64).saturating_mul(60 * 60 * 1000),
        ..jobs::JobConfig::default()
    };
    if job_config.record_ttl_ms < job_config.result_ttl_ms {
        warn!(
            result_ttl_hours = cli.job_result_ttl_hours,
            record_ttl_hours = cli.job_record_ttl_hours,
            "job record retention is shorter than result retention; \
             expired results may be left without a job row"
        );
    }
    let cluster_id = cli
        .job_cluster_id
        .clone()
        .unwrap_or_else(|| format!("{}:{}", cli.bind, cli.port));
    let jobs = jobs::JobService::new(
        Arc::clone(&meta_client),
        cli.core.warehouse_prefix.clone(),
        job_config,
        cluster_id,
    )
    .await?;
    let session_factory = Arc::new(session_factory.with_jobs(Arc::clone(&jobs)));
    Arc::new(jobs::JobRunner::new(
        Arc::clone(&jobs),
        Arc::clone(&session_factory),
    ))
    .spawn();

    info!(
        "start serving on {}:{} (max_connections={}, max_queries_per_user={}, max_concurrent_queries={}, max_result_rows={}, max_result_bytes={}, max_staged_files_per_transaction={}, max_staged_bytes_per_transaction={}, max_jobs={})",
        cli.bind,
        cli.port,
        cli.max_connections,
        cli.max_queries_per_user,
        cli.max_concurrent_queries,
        cli.max_result_rows,
        cli.max_result_bytes,
        cli.max_staged_files_per_transaction,
        cli.max_staged_bytes_per_transaction,
        cli.max_jobs
    );

    let mut server_opts = ServerOptions::new()
        .with_host(cli.bind.clone())
        .with_port(cli.port)
        .with_tcp_keepalive(tcp_keepalive(&cli));
    if let (Some(cert), Some(key)) = (&cli.tls_cert, &cli.tls_key) {
        server_opts = server_opts
            .with_tls_cert_path(Some(cert.display().to_string()))
            .with_tls_key_path(Some(key.display().to_string()));
    }
    info!(tcp_keepalive = ?server_opts.tcp_keepalive(), "client connection keepalive");

    let limits = Limits {
        max_connections: cli.max_connections,
        max_queries_per_user: cli.max_queries_per_user,
        max_concurrent_queries: cli.max_concurrent_queries,
        max_result_rows: cli.max_result_rows,
        max_result_bytes: cli.max_result_bytes,
        max_staged_files_per_transaction: cli.max_staged_files_per_transaction,
        max_staged_bytes_per_transaction: cli.max_staged_bytes_per_transaction,
    };
    if limits.one_user_can_fill_the_server() {
        warn!(
            max_queries_per_user = limits.max_queries_per_user,
            max_concurrent_queries = limits.max_concurrent_queries,
            "one user can take every server-wide statement slot; set \
             max_queries_per_user below max_concurrent_queries"
        );
    }
    let limits = ServerLimits::new(limits);

    let index_supervisor = match cli.index_maintenance_executor {
        index_runner::IndexMaintenanceExecutor::Local => {
            info!(
                poll_ms = cli.index_maintenance_poll_ms,
                lease_ttl_ms = cli.index_lease_ttl_ms,
                "starting the local index maintenance child"
            );
            Some(index_runner::spawn_local_supervisor(
                index_program,
                std::time::Duration::from_millis(cli.index_maintenance_poll_ms),
                std::time::Duration::from_millis(cli.index_lease_ttl_ms),
            ))
        }
        index_runner::IndexMaintenanceExecutor::External => {
            info!(
                "external index maintenance executor configured; start \
                 `postgres-lakesoul --index-maintenance-runner` to build deferred indexes"
            );
            None
        }
        index_runner::IndexMaintenanceExecutor::Off => {
            info!("deferred index maintenance is disabled");
            None
        }
    };

    let handlers = Arc::new(LakeSoulHandlers::new(Arc::clone(&session_factory), limits));
    let server_result = tokio::select! {
        result = serve_with_handlers(handlers, &server_opts) => Some(result),
        _ = shutdown_signal() => None,
    };
    // Children are separate processes. Stop them before this supervisor exits;
    // otherwise a graceful server shutdown would leave them holding
    // replication slots.
    subscriptions.stop_all().await;
    if let Some(supervisor) = index_supervisor {
        supervisor.stop().await;
    }
    jobs.shutdown().await;
    if let Some(result) = server_result {
        result?;
    }
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to install Ctrl-C handler");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                warn!(%error, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(Command::Token(args)) = &cli.command {
        let rt = runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        return rt.block_on(print_token(args));
    }
    let subscriber = cli.subscribe.subscribe || cli.subscribe.subscription_id.is_some();
    let worker_threads = if subscriber {
        // The initial snapshot decodes N COPY streams and builds Arrow batches
        // on the async runtime; the default server thread count would leave
        // most of the parallelism waiting on two cores.
        cli.core
            .worker_threads
            .max(cli.subscribe.snapshot_parallelism)
            .max(2)
    } else {
        cli.core.worker_threads.max(2)
    };
    let rt = runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(worker_threads)
        .thread_name("pg-lakesoul")
        .thread_stack_size(3 * 1024 * 1024) // 3MB
        .build()?;
    rt.block_on(main_inner(cli))?;
    Ok(())
}

/// The keepalive the CLI asks for on client connections: none when the idle
/// time is 0; an interval or probe count of 0 leaves that parameter to the
/// operating system.
fn tcp_keepalive(cli: &Cli) -> Option<TcpKeepalive> {
    let nonzero = |secs: u64| (secs > 0).then(|| std::time::Duration::from_secs(secs));
    Some(TcpKeepalive {
        idle: nonzero(cli.tcp_keepalive_idle_secs)?,
        interval: nonzero(cli.tcp_keepalive_interval_secs),
        retries: (cli.tcp_keepalive_retries > 0).then_some(cli.tcp_keepalive_retries),
    })
}

/// Load the configured certificate and key and build the rustls configuration
/// the listener will use.
///
/// `datafusion-postgres` treats a TLS setup failure as a warning and keeps
/// serving in cleartext. That is fine for an embedding that never claimed TLS,
/// but not for an operator who passed `--tls-cert`: validating here, with the
/// same PEM parser and provider, turns a bad path, a non-PKCS#8 key or a
/// mismatched pair into a start-up failure instead of a silent downgrade.
fn validate_tls_material(cert_path: &Path, key_path: &Path) -> Result<()> {
    use std::fs::File;
    use std::io::BufReader;

    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut certificate_reader =
        BufReader::new(File::open(cert_path).map_err(|error| {
            rootcause::report!("open TLS certificate {}: {error}", cert_path.display())
        })?);
    let certificates = rustls_pemfile::certs(&mut certificate_reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| {
            rootcause::report!("read TLS certificate {}: {error}", cert_path.display())
        })?;
    if certificates.is_empty() {
        rootcause::bail!(
            "TLS certificate {} contains no PEM certificate",
            cert_path.display()
        );
    }

    let mut key_reader = BufReader::new(File::open(key_path).map_err(|error| {
        rootcause::report!("open TLS private key {}: {error}", key_path.display())
    })?);
    let mut keys = rustls_pemfile::pkcs8_private_keys(&mut key_reader);
    let key = keys
        .next()
        .transpose()
        .map_err(|error| {
            rootcause::report!("read TLS private key {}: {error}", key_path.display())
        })?
        .ok_or_else(|| {
            rootcause::report!(
                "TLS private key {} is not PKCS#8; convert it with `openssl \
                 pkcs8 -topk8 -nocrypt -in <key> -out <key.pk8>`",
                key_path.display()
            )
        })?;
    if keys.next().is_some() {
        rootcause::bail!(
            "TLS private key {} contains more than one PKCS#8 key",
            key_path.display()
        );
    }

    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, rustls::pki_types::PrivateKeyDer::from(key))
        .map_err(|error| {
            rootcause::report!(
                "TLS certificate {} and key {} cannot be used together: {error}",
                cert_path.display(),
                key_path.display()
            )
        })?;
    Ok(())
}

#[cfg(test)]
mod cli_probe_tests {
    use super::*;

    fn keepalive_for(args: &[&str]) -> Option<TcpKeepalive> {
        let cli = Cli::try_parse_from(std::iter::once("pg").chain(args.iter().copied()))
            .expect("parse");
        tcp_keepalive(&cli)
    }

    #[test]
    fn tcp_keepalive_follows_the_cli() {
        let secs = std::time::Duration::from_secs;
        assert_eq!(
            keepalive_for(&[
                "--tcp-keepalive-idle-secs=60",
                "--tcp-keepalive-interval-secs=10",
                "--tcp-keepalive-retries=6",
            ]),
            Some(TcpKeepalive {
                idle: secs(60),
                interval: Some(secs(10)),
                retries: Some(6),
            })
        );
        assert_eq!(
            keepalive_for(&["--tcp-keepalive-idle-secs=0"]),
            None,
            "an idle time of 0 leaves keepalive off"
        );
        assert_eq!(
            keepalive_for(&[
                "--tcp-keepalive-idle-secs=30",
                "--tcp-keepalive-interval-secs=0",
                "--tcp-keepalive-retries=0",
            ]),
            Some(TcpKeepalive {
                idle: secs(30),
                interval: None,
                retries: None,
            }),
            "0 leaves the interval and the probe count to the OS"
        );
    }

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
            "ivm_view_parallelism": cli.ivm_runtime.view_parallelism,
            "distributed_workers": cli.distributed.distributed_workers,
            "max_concurrent_queries": cli.max_concurrent_queries,
            "tcp_keepalive_idle_secs": cli.tcp_keepalive_idle_secs,
            "tcp_keepalive_interval_secs": cli.tcp_keepalive_interval_secs,
            "tcp_keepalive_retries": cli.tcp_keepalive_retries,
            "jwt_auth": cli.jwt_auth,
            "rbac": cli.rbac,
            "rbac_domain": cli.rbac_domain,
            "tls_cert": cli.tls_cert.as_ref().map(|path| path.display().to_string()),
            "tls_key": cli.tls_key.as_ref().map(|path| path.display().to_string()),
            "require_tls": cli.require_tls,
            "ivm_default": cli.ivm_default,
            "subscription_executor": format!("{:?}", cli.subscription_executor),
            "subscription_runner": cli.subscription_runner,
        })
    }

    #[test]
    fn token_subcommand_parses_after_server_flags() {
        let cli = Cli::try_parse_from([
            "pg", "--port", "15432", "token", "--user", "alice", "--group", "team_a",
            "--ttl", "15m",
        ])
        .expect("parse");
        let Some(Command::Token(args)) = cli.command else {
            panic!("token subcommand not parsed");
        };
        assert_eq!(args.user, "alice");
        assert_eq!(args.group, "team_a");
        assert_eq!(args.ttl, std::time::Duration::from_secs(900));

        let cli = Cli::try_parse_from(["pg", "token", "--user", "a", "--group", "g"])
            .expect("parse");
        let Some(Command::Token(args)) = cli.command else {
            panic!("token subcommand not parsed");
        };
        assert_eq!(
            args.ttl,
            std::time::Duration::from_secs(3600),
            "default ttl"
        );

        assert!(
            Cli::try_parse_from([
                "pg", "token", "--user", "a", "--group", "g", "--ttl", "0"
            ])
            .is_err()
        );
        assert!(Cli::try_parse_from(["pg", "token", "--user", "a"]).is_err());
    }

    #[test]
    fn ivm_default_follows_cli_and_env() {
        assert_eq!(probe(&[], &[])["ivm_default"], false);
        assert_eq!(
            probe(&[("LAKESOUL_PG_IVM_DEFAULT", "true")], &[])["ivm_default"],
            true
        );
        assert_eq!(probe(&[], &["--ivm-default"])["ivm_default"], true);
    }

    #[test]
    fn subscription_runner_flags_parse() {
        assert_eq!(
            probe(&[], &[])["subscription_executor"],
            "Local",
            "subscriptions stay local by default"
        );
        assert_eq!(
            probe(&[("LAKESOUL_PG_SUBSCRIPTION_EXECUTOR", "external")], &[])["subscription_executor"],
            "External"
        );
        assert_eq!(
            probe(&[], &["--subscription-runner", "--runner-id", "r1"])["subscription_runner"],
            true
        );
        assert!(Cli::try_parse_from(["pg", "--subscription-runner"]).is_ok());
        assert!(
            Cli::try_parse_from(["pg", "--runner-poll-ms", "50"]).is_err(),
            "a runner poll interval below 100 ms must be refused"
        );
    }

    #[test]
    fn jwt_auth_follows_cli_and_env() {
        assert_eq!(probe(&[], &[])["jwt_auth"], false);
        assert_eq!(
            probe(&[("LAKESOUL_PG_JWT_AUTH", "true")], &[])["jwt_auth"],
            true
        );
        assert_eq!(probe(&[], &["--jwt-auth"])["jwt_auth"], true);
    }

    #[test]
    fn tls_flags_must_be_paired() {
        assert!(
            Cli::try_parse_from(["pg", "--tls-cert", "/etc/lakesoul/tls.crt"]).is_err(),
            "--tls-cert without --tls-key must be refused"
        );
        assert!(
            Cli::try_parse_from(["pg", "--tls-key", "/etc/lakesoul/tls.key"]).is_err(),
            "--tls-key without --tls-cert must be refused"
        );
        let cli = Cli::try_parse_from([
            "pg",
            "--tls-cert",
            "/etc/lakesoul/tls.crt",
            "--tls-key",
            "/etc/lakesoul/tls.key",
        ])
        .expect("parse");
        assert_eq!(
            cli.tls_cert.as_deref(),
            Some(std::path::Path::new("/etc/lakesoul/tls.crt"))
        );
        assert_eq!(
            cli.tls_key.as_deref(),
            Some(std::path::Path::new("/etc/lakesoul/tls.key"))
        );
        assert!(!cli.require_tls, "TLS is offered, not required, by default");
    }

    #[test]
    fn require_tls_needs_a_certificate() {
        assert!(
            Cli::try_parse_from(["pg", "--require-tls"]).is_err(),
            "--require-tls without a certificate must be refused"
        );
        assert!(
            Cli::try_parse_from([
                "pg",
                "--require-tls",
                "--tls-cert",
                "/etc/lakesoul/tls.crt",
            ])
            .is_err(),
            "--require-tls with only one half of the pair must be refused"
        );
        let cli = Cli::try_parse_from([
            "pg",
            "--require-tls",
            "--tls-cert",
            "/etc/lakesoul/tls.crt",
            "--tls-key",
            "/etc/lakesoul/tls.key",
        ])
        .expect("parse");
        assert!(cli.require_tls);
    }

    #[test]
    fn tls_flags_follow_env() {
        let result = probe(
            &[
                ("LAKESOUL_PG_TLS_CERT", "/etc/lakesoul/tls.crt"),
                ("LAKESOUL_PG_TLS_KEY", "/etc/lakesoul/tls.key"),
                ("LAKESOUL_PG_REQUIRE_TLS", "true"),
            ],
            &[],
        );
        assert_eq!(result["tls_cert"], "/etc/lakesoul/tls.crt");
        assert_eq!(result["tls_key"], "/etc/lakesoul/tls.key");
        assert_eq!(result["require_tls"], true);

        let result = probe(&[], &[]);
        assert!(result["tls_cert"].is_null());
        assert!(result["tls_key"].is_null());
        assert_eq!(result["require_tls"], false);
    }

    #[test]
    fn invalid_tls_material_is_rejected() {
        let directory = tempfile::tempdir().expect("tempdir");
        let cert = directory.path().join("server.crt");
        let key = directory.path().join("server.key");
        std::fs::write(&cert, "this is not a PEM certificate").expect("write cert");
        std::fs::write(&key, "this is not a PEM key").expect("write key");
        let error = validate_tls_material(&cert, &key)
            .expect_err("a malformed certificate must be refused");
        let message = format!("{error}");
        assert!(
            message.contains("certificate"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn rbac_requires_jwt_auth() {
        assert!(
            Cli::try_parse_from(["pg", "--rbac"]).is_err(),
            "--rbac without --jwt-auth must be refused"
        );
        let cli = Cli::try_parse_from(["pg", "--jwt-auth", "--rbac"]).expect("parse");
        assert!(cli.jwt_auth && cli.rbac);
        let cli = Cli::try_parse_from(["pg", "--jwt-auth"]).expect("parse");
        assert!(!cli.rbac, "domain enforcement is off by default");
    }

    #[test]
    fn rbac_domain_requires_rbac() {
        assert!(
            Cli::try_parse_from(["pg", "--jwt-auth", "--rbac-domain", "team_a",])
                .is_err(),
            "--rbac-domain without --rbac must be refused"
        );
        let cli = Cli::try_parse_from([
            "pg",
            "--jwt-auth",
            "--rbac",
            "--rbac-domain",
            "team_a",
        ])
        .expect("parse");
        assert_eq!(cli.rbac_domain.as_deref(), Some("team_a"));
    }

    #[test]
    fn rbac_follows_env() {
        assert_eq!(probe(&[], &[])["rbac"], false);
        assert_eq!(
            probe(
                &[
                    ("LAKESOUL_PG_JWT_AUTH", "true"),
                    ("LAKESOUL_PG_RBAC", "true")
                ],
                &[]
            )["rbac"],
            true
        );
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
        assert_eq!(defaults["max_concurrent_queries"], 0);
        assert_eq!(defaults["tcp_keepalive_idle_secs"], 60);
        assert_eq!(defaults["tcp_keepalive_interval_secs"], 10);
        assert_eq!(defaults["tcp_keepalive_retries"], 6);

        let via_env = probe(
            &[
                ("LAKESOUL_PG_BIND", "0.0.0.0"),
                ("LAKESOUL_PG_PORT", "15432"),
                ("LAKESOUL_PG_MAX_CONNECTIONS", "64"),
                ("LAKESOUL_PG_MAX_CONCURRENT_QUERIES", "32"),
                ("LAKESOUL_PG_TCP_KEEPALIVE_IDLE_SECS", "0"),
            ],
            &[],
        );
        assert_eq!(via_env["bind"], "0.0.0.0");
        assert_eq!(via_env["port"], 15432);
        assert_eq!(via_env["max_connections"], 64);
        assert_eq!(via_env["max_concurrent_queries"], 32);
        assert_eq!(via_env["tcp_keepalive_idle_secs"], 0);

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

    #[test]
    fn ivm_and_distributed_flags_can_be_combined() {
        let combined = probe(
            &[],
            &[
                "--ivm-view-parallelism",
                "2",
                "--distributed-workers",
                "http://127.0.0.1:50051",
            ],
        );
        assert_eq!(combined["ivm_view_parallelism"], 2);
        assert_eq!(combined["distributed_workers"][0], "http://127.0.0.1:50051");
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
