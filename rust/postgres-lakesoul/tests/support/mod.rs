// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Real-process SQL compatibility harness.
//!
//! One [`run_suite`] call owns a freshly created LakeSoul namespace, a
//! disposable warehouse directory, a matching reference schema in the
//! metadata PostgreSQL database, and one `postgres-lakesoul` process bound to
//! dynamically allocated PostgreSQL and metrics ports.
//!
//! The suite is handed clients whose unqualified SQL resolves to the same
//! fixture tables on both sides:
//!
//! * `items(id INTEGER, val INTEGER, label TEXT)`
//! * `empty_items(...)` with the same schema and no rows
//! * `pk_items(id BIGINT NOT NULL PRIMARY KEY, val BIGINT, label TEXT)`,
//!   written in two independent commits
//!
//! Nothing is skipped when the metadata database, the binary, or a socket is
//! unavailable: setup errors fail the test, and cleanup always runs, including
//! after suite panics.

mod catalog;
pub mod compare;
mod state;
mod storage;
pub mod tpch;
mod tpch_driver;
mod tpch_workers;
pub mod vector;

use std::any::Any;
use std::future::Future;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail, ensure};
use datafusion::prelude::SessionContext;
use futures::FutureExt;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use lakesoul_metadata::{
    MetaDataClient, PRIMARY_URL_ENV_KEY, PRIMARY_URL_PROP_KEY, pg_config_from_env,
};
use tempfile::TempDir;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, Config, NoTls};
use uuid::Uuid;

// Port-zero reservations must be released before exec. Serialize only this
// handoff through readiness, so another suite cannot reserve a released port
// while the first child has not bound it yet. Suite queries still run in parallel.
static STARTUP_GATE: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Upper bound for one connect or fixture-management operation.
const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound for the child process to answer protocol and fixture queries.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(90);
/// Upper bound for the entire suite body, including all client queries.
const SUITE_TIMEOUT: Duration = Duration::from_secs(180);
/// Upper bound for one cleanup step.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(20);
/// Test-only controlled-failure switch. `startup` makes the harness launch a
/// deliberately invalid command line; any other value belongs to the suite.
const FAULT_ENV: &str = "LAKESOUL_SQL_COMPAT_FAULT";

/// Session settings applied identically to both sides so textual and temporal
/// output has one deterministic environment.
const SESSION_SETTINGS: &[&str] = &[
    "SET TIME ZONE 'UTC'",
    "SET DateStyle = 'ISO, YMD'",
    "SET client_encoding = 'UTF8'",
    "SET statement_timeout = '60s'",
];

// Both backends consume these exact inputs; only their DDL/upsert syntax differs.
const ITEM_VALUES: &str =
    "(1, 10, 'a'), (1, NULL, ''), (2, 20, ' β '), (NULL, NULL, NULL)";
const PK_FIRST: &str = "(1, 10, 'old'), (2, 20, 'keep')";
const PK_SECOND: &str = "(1, 99, 'new'), (3, 30, 'added')";

/// Real clients and fixture coordinates for one isolated suite run.
pub struct Harness {
    /// LakeSoul PostgreSQL protocol client connected to the fresh namespace.
    pub lake: Client,
    /// PostgreSQL reference client connected to the isolated reference schema.
    pub reference: Client,
    /// LakeSoul namespace (the PG `dbname` used by [`Harness::lake`]).
    pub lake_namespace: String,
    /// PostgreSQL schema (the `search_path` used by [`Harness::reference`]).
    pub reference_schema: String,
    /// Dynamically assigned PostgreSQL listen address.
    pub lake_addr: SocketAddr,
    /// Dynamically assigned Prometheus metrics address.
    pub metrics_addr: SocketAddr,
    /// Disposable warehouse directory.
    pub warehouse_dir: PathBuf,
    state: StartupState,
}

/// Run one SQL compatibility suite against a real `postgres-lakesoul` child.
///
/// The future is polled until it completes or [`SUITE_TIMEOUT`] elapses. On
/// every error path (startup, assertion, panic, timeout) cleanup runs and the
/// returned error carries the child PID, reaping status, both port release
/// checks, the resource cleanup log and the server log tail.
pub async fn run_suite<F>(suite: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a Harness) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>,
{
    run_fixture_suite(Fixtures::Base, suite).await
}

/// Prepare physical storage fixtures before the server's catalog is loaded.
pub async fn run_storage_suite<F>(suite: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a Harness) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>,
{
    run_fixture_suite(Fixtures::Storage, suite).await
}

/// Prepare both namespace/database fixtures before starting the real server.
pub async fn run_catalog_suite<F>(suite: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a Harness) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>,
{
    run_fixture_suite(Fixtures::Catalog, suite).await
}

/// Build committed vector indexes before the server discovers the catalog.
pub async fn run_vector_suite<F>(suite: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a Harness) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>,
{
    run_fixture_suite(Fixtures::Vector, suite).await
}

/// Prepare the pinned TPC-H dataset before the real PG server loads its catalog.
pub async fn run_tpch_suite<F>(config: tpch::TpchConfig, suite: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a Harness) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>,
{
    run_fixture_suite(Fixtures::Tpch(config), suite).await
}

enum Fixtures {
    Base,
    Storage,
    Catalog,
    Vector,
    Tpch(tpch::TpchConfig),
}

async fn run_fixture_suite<F>(fixtures: Fixtures, suite: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a Harness) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>>,
{
    let fault = std::env::var(FAULT_ENV).ok();
    let suite_timeout = if matches!(&fixtures, Fixtures::Tpch(_)) {
        Duration::from_secs(3600)
    } else {
        SUITE_TIMEOUT
    };
    let mut state = StartupState::new()
        .await
        .context("create SQL compatibility harness")?;

    let prepared = AssertUnwindSafe(state.prepare(fault.as_deref(), fixtures))
        .catch_unwind()
        .await;
    let startup_error = match prepared {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(format!("startup failed: {error:#}")),
        Err(payload) => Some(format!("startup panicked: {}", panic_message(payload))),
    };
    if let Some(startup_error) = startup_error {
        let logs = state.log_tail();
        let cleanup = state.cleanup().await;
        return Err(failure(startup_error, &cleanup, &logs));
    }

    let harness = state.finish();
    eprintln!(
        "sql-compat ready: namespace={} reference_schema={} pg={} metrics={} warehouse={}",
        harness.lake_namespace,
        harness.reference_schema,
        harness.lake_addr,
        harness.metrics_addr,
        harness.warehouse_dir.display()
    );
    let suite_result = tokio::time::timeout(
        suite_timeout,
        AssertUnwindSafe(suite(&harness)).catch_unwind(),
    )
    .await;

    let primary = match suite_result {
        Err(_) => Some(format!("suite timed out after {suite_timeout:?}")),
        Ok(Ok(Ok(()))) => None,
        Ok(Ok(Err(error))) => Some(format!("suite failed: {error:#}")),
        Ok(Err(payload)) => Some(format!("suite panicked: {}", panic_message(payload))),
    };
    let logs = harness.state.log_tail();
    let cleanup = harness.shutdown().await;
    eprintln!("{}", cleanup.proof());

    match primary {
        Some(primary) => Err(failure(primary, &cleanup, &logs)),
        None if !cleanup.errors.is_empty() => Err(failure(
            "suite completed but cleanup reported errors".to_string(),
            &cleanup,
            &logs,
        )),
        None => Ok(()),
    }
}

impl Harness {
    async fn shutdown(self) -> CleanupOutcome {
        let Harness {
            lake,
            reference,
            mut state,
            ..
        } = self;
        drop(lake);
        drop(reference);
        state.cleanup().await
    }
}

/// Resources built by one harness startup.
///
/// Fields become `Some` as startup advances, so the same cleanup routine can
/// reclaim a partially initialized run after any startup error.
struct StartupState {
    namespace: String,
    reference_schema: String,
    warehouse_dir: PathBuf,
    warehouse_url: String,
    log_path: PathBuf,
    temp_dir: Option<TempDir>,
    meta: Arc<MetaDataClient>,
    management: Arc<SessionContext>,
    reference_config: Config,
    reference_client: Option<Client>,
    reference_task: Option<JoinHandle<()>>,
    lake_client: Option<Client>,
    lake_task: Option<JoinHandle<()>>,
    child: Option<ChildGuard>,
    workers: Option<tpch_workers::Workers>,
    lake_addr: Option<SocketAddr>,
    metrics_addr: Option<SocketAddr>,
    namespace_created: bool,
    created_tables: Vec<String>,
    extra_namespaces: Vec<(String, Vec<String>)>,
    connections: parking_lot::Mutex<Vec<JoinHandle<()>>>,
}

impl StartupState {
    async fn new() -> Result<Self> {
        let uuid = Uuid::new_v4().simple().to_string();
        let suffix = &uuid[..16];
        let namespace = format!("sqlcompat_{suffix}");
        let reference_schema = format!("sqlcompat_ref_{suffix}");

        let temp_dir = tempfile::Builder::new()
            .prefix("lakesoul-sql-compat-")
            .tempdir()
            .context("create disposable warehouse directory")?;
        let warehouse_dir = temp_dir.path().join("warehouse");
        std::fs::create_dir_all(&warehouse_dir)
            .with_context(|| format!("create {}", warehouse_dir.display()))?;
        let warehouse_url = format!("file://{}", warehouse_dir.to_string_lossy());
        let log_path = temp_dir.path().join("postgres-lakesoul.log");

        let reference_config = reference_config_from_env()?;

        let meta = bounded(OPERATION_TIMEOUT, "create metadata client", async {
            MetaDataClient::from_env()
                .await
                .map_err(|error| format!("{error:?}"))
        })
        .await
        .context("metadata PostgreSQL is required; no test is skipped without it")?;
        let meta = Arc::new(meta);

        let factory = LakeSoulSessionFactory::new(
            Arc::clone(&meta),
            &CoreArgs {
                warehouse_prefix: Some(warehouse_url.clone()),
                ..CoreArgs::default()
            },
        )
        .map_err(|error| anyhow!("create LakeSoul session factory: {error:?}"))?;
        let management = factory
            .create_session(&LakeSoulSessionOptions {
                default_schema: namespace.clone(),
                time_zone: Some("UTC".to_string()),
            })
            .map_err(|error| anyhow!("create management SessionContext: {error:?}"))?;

        let (reference_client, reference_task) =
            connect_pg(&reference_config, "sql_compat_reference").await?;
        configure_session(&reference_client).await?;

        Ok(Self {
            namespace,
            reference_schema,
            warehouse_dir,
            warehouse_url,
            log_path,
            temp_dir: Some(temp_dir),
            meta,
            management,
            reference_config,
            reference_client: Some(reference_client),
            reference_task: Some(reference_task),
            lake_client: None,
            lake_task: None,
            child: None,
            workers: None,
            lake_addr: None,
            metrics_addr: None,
            namespace_created: false,
            created_tables: Vec::new(),
            extra_namespaces: Vec::new(),
            connections: parking_lot::Mutex::new(Vec::new()),
        })
    }

    async fn prepare(&mut self, fault: Option<&str>, fixtures: Fixtures) -> Result<()> {
        self.create_reference_fixtures().await?;
        self.create_lake_fixtures().await?;
        match fixtures {
            Fixtures::Base => {}
            Fixtures::Storage => self.create_storage_fixtures().await?,
            Fixtures::Catalog => self.create_catalog_fixtures().await?,
            Fixtures::Vector => self.create_vector_fixtures().await?,
            Fixtures::Tpch(config) => {
                self.workers = tpch_workers::Workers::configured()?;
                if let Some(workers) = &mut self.workers {
                    ensure!(
                        config.layout != "pk",
                        "distributed TPC-H requires nopk or pkmonth; unpartitioned PK scans stay local"
                    );
                    workers.start(&self.warehouse_url).await?;
                }
                self.create_tpch_fixtures(&config).await?;
            }
        }
        let _startup = STARTUP_GATE.lock().await;
        self.launch_server(fault).await?;
        tokio::time::timeout(STARTUP_TIMEOUT, self.wait_until_ready())
            .await
            .context("server readiness deadline elapsed")??;
        Ok(())
    }

    async fn create_reference_fixtures(&self) -> Result<()> {
        let client = self
            .reference_client
            .as_ref()
            .ok_or_else(|| anyhow!("reference connection is unavailable"))?;
        let schema = &self.reference_schema;

        reference_execute(client, &format!("CREATE SCHEMA \"{schema}\"")).await?;
        reference_execute(client, &format!("SET search_path TO \"{schema}\"")).await?;
        reference_execute(
            client,
            &format!(
                "CREATE TABLE \"{schema}\".items (\
                   id INTEGER, val INTEGER, label TEXT COLLATE \"C\"\
                 )"
            ),
        )
        .await?;
        reference_execute(
            client,
            &format!(
                "CREATE TABLE \"{schema}\".empty_items (\
                   id INTEGER, val INTEGER, label TEXT COLLATE \"C\"\
                 )"
            ),
        )
        .await?;
        reference_execute(
            client,
            &format!(
                "CREATE TABLE \"{schema}\".pk_items (\
                   id BIGINT NOT NULL PRIMARY KEY, val BIGINT, \
                   label TEXT COLLATE \"C\"\
                 )"
            ),
        )
        .await?;

        reference_execute(
            client,
            &format!(
                "INSERT INTO \"{schema}\".items (id, val, label) VALUES {ITEM_VALUES}"
            ),
        )
        .await?;
        reference_execute(
            client,
            &format!(
                "INSERT INTO \"{schema}\".pk_items (id, val, label) VALUES {PK_FIRST}"
            ),
        )
        .await?;
        reference_execute(
            client,
            &format!(
                "INSERT INTO \"{schema}\".pk_items (id, val, label) VALUES {PK_SECOND} \
                 ON CONFLICT (id) DO UPDATE \
                   SET val = EXCLUDED.val, label = EXCLUDED.label"
            ),
        )
        .await?;

        ensure_eq(
            "reference items",
            reference_row_count(client, &format!("SELECT * FROM \"{schema}\".items"))
                .await?,
            4,
        )?;
        ensure_eq(
            "reference empty_items",
            reference_row_count(
                client,
                &format!("SELECT * FROM \"{schema}\".empty_items"),
            )
            .await?,
            0,
        )?;
        ensure_eq(
            "reference pk_items",
            reference_row_count(client, &format!("SELECT * FROM \"{schema}\".pk_items"))
                .await?,
            3,
        )?;
        Ok(())
    }

    async fn create_lake_fixtures(&mut self) -> Result<()> {
        let context = Arc::clone(&self.management);
        let namespace = self.namespace.clone();
        let location = |table: &str| {
            format!(
                "{}/{namespace}/{table}",
                self.warehouse_url.trim_end_matches('/')
            )
        };

        management_execute(&context, &format!("CREATE SCHEMA lakesoul.{namespace}"))
            .await?;
        self.namespace_created = true;

        management_execute(
            &context,
            &format!(
                "CREATE EXTERNAL TABLE lakesoul.{namespace}.items (\
                   id INTEGER, val INTEGER, label TEXT\
                 ) STORED AS LAKESOUL LOCATION '{}' \
                 OPTIONS ('file_format' 'parquet')",
                location("items")
            ),
        )
        .await?;
        self.created_tables.push("items".to_string());
        management_execute(
            &context,
            &format!(
                "CREATE EXTERNAL TABLE lakesoul.{namespace}.empty_items (\
                   id INTEGER, val INTEGER, label TEXT\
                 ) STORED AS LAKESOUL LOCATION '{}' \
                 OPTIONS ('file_format' 'parquet')",
                location("empty_items")
            ),
        )
        .await?;
        self.created_tables.push("empty_items".to_string());
        management_execute(
            &context,
            &format!(
                "CREATE EXTERNAL TABLE lakesoul.{namespace}.pk_items (\
                   id BIGINT NOT NULL PRIMARY KEY, val BIGINT, label TEXT\
                 ) STORED AS LAKESOUL LOCATION '{}' \
                 OPTIONS ('file_format' 'parquet', 'hashBucketNum' '1')",
                location("pk_items")
            ),
        )
        .await?;
        self.created_tables.push("pk_items".to_string());

        management_execute(
            &context,
            &format!(
                "INSERT INTO lakesoul.{namespace}.items (id, val, label) VALUES {ITEM_VALUES}"
            ),
        )
        .await?;
        // Two independent commits: the second replaces key 1 across a file
        // version boundary and adds key 3.
        management_execute(
            &context,
            &format!(
                "INSERT INTO lakesoul.{namespace}.pk_items (id, val, label) VALUES {PK_FIRST}"
            ),
        )
        .await?;
        management_execute(
            &context,
            &format!(
                "INSERT OR REPLACE INTO lakesoul.{namespace}.pk_items (id, val, label) VALUES {PK_SECOND}"
            ),
        )
        .await?;

        ensure_eq(
            "lake items",
            management_row_count(&context, &namespace, "items").await?,
            4,
        )?;
        ensure_eq(
            "lake empty_items",
            management_row_count(&context, &namespace, "empty_items").await?,
            0,
        )?;
        ensure_eq(
            "lake pk_items",
            management_row_count(&context, &namespace, "pk_items").await?,
            3,
        )?;

        prove_pk_two_commits(&self.meta, &namespace).await
    }

    async fn launch_server(&mut self, fault: Option<&str>) -> Result<()> {
        // Hold both listeners until the last moment so the two ports cannot
        // collide with each other, then release them for the child.
        let lake_listener =
            StdTcpListener::bind(("127.0.0.1", 0)).context("reserve PostgreSQL port")?;
        let metrics_listener =
            StdTcpListener::bind(("127.0.0.1", 0)).context("reserve metrics port")?;
        let lake_addr = lake_listener.local_addr().context("read PostgreSQL port")?;
        let metrics_addr = metrics_listener.local_addr().context("read metrics port")?;
        ensure!(
            lake_addr != metrics_addr,
            "PostgreSQL and metrics ports must differ"
        );
        drop(lake_listener);
        drop(metrics_listener);
        self.lake_addr = Some(lake_addr);
        self.metrics_addr = Some(metrics_addr);

        let log = std::fs::File::create(&self.log_path)
            .with_context(|| format!("create {}", self.log_path.display()))?;
        let log_err = log.try_clone().context("clone server log handle")?;

        let mut command = Command::new(env!("CARGO_BIN_EXE_postgres-lakesoul"));
        command
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(lake_addr.port().to_string())
            .arg("--metrics-addr")
            .arg(metrics_addr.to_string())
            .arg("--worker-threads")
            .arg("2")
            .arg("--max-connections")
            .arg("16")
            .arg("--max-queries-per-user")
            .arg("16")
            .args(["--max-result-rows", "0", "--max-result-bytes", "0"])
            .args(["--memory-limit", "0", "--vortex-decode-budget", "0"])
            .arg("--warehouse-prefix")
            .arg(&self.warehouse_url)
            .env("TZ", "UTC")
            .env_remove("LAKESOUL_DISTRIBUTED_WORKERS")
            .env_remove("LAKESOUL_K8S_WORKER_SERVICE")
            .env_remove("LAKESOUL_K8S_NAMESPACE")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log_err));
        if let Some(workers) = &self.workers {
            command
                .args(["--discovery", "static", "--distributed-workers"])
                .arg(workers.urls())
                .args([
                    "--distributed-fallback-local=false",
                    "--distributed-bytes-per-partition",
                    "1",
                ]);
        } else {
            command.args(["--discovery", "none"]);
        }
        if fault == Some("startup") {
            // Controlled startup failure: clap rejects the unknown option and
            // the child exits non-zero before readiness.
            command.arg("--sql-compat-injected-startup-failure");
        }
        let child = command.spawn().with_context(|| {
            format!("spawn {}", env!("CARGO_BIN_EXE_postgres-lakesoul"))
        })?;
        self.child = Some(ChildGuard::new(child));
        Ok(())
    }

    async fn wait_until_ready(&mut self) -> Result<()> {
        let lake_addr = self
            .lake_addr
            .ok_or_else(|| anyhow!("PostgreSQL listen address was not assigned"))?;
        let metrics_addr = self
            .metrics_addr
            .ok_or_else(|| anyhow!("metrics listen address was not assigned"))?;
        let deadline = tokio::time::Instant::now() + STARTUP_TIMEOUT;
        let mut last_error = "no readiness attempt completed".to_string();

        loop {
            let exited = match self.child.as_mut() {
                Some(child) => child
                    .poll_exit()
                    .map_err(|error| anyhow!("poll server process: {error}"))?
                    .map(|status| (child.pid, status)),
                None => None,
            };
            if let Some((pid, status)) = exited {
                bail!(
                    "server exited before readiness (pid {pid}: {status}); server log:\n{}",
                    self.log_tail()
                );
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "server was not ready within {STARTUP_TIMEOUT:?}: {last_error}; server log:\n{}",
                    self.log_tail()
                );
            }

            let config = lake_config(lake_addr, &self.namespace);
            match connect_pg(&config, "sql_compat_lake").await {
                Ok((client, task)) => match readiness(&client).await {
                    Ok(()) => match check_metrics(metrics_addr).await {
                        Ok(()) => {
                            configure_session(&client).await?;
                            self.lake_client = Some(client);
                            self.lake_task = Some(task);
                            return Ok(());
                        }
                        Err(error) => {
                            last_error = error;
                            stop_connection(Some(client), Some(task)).await;
                        }
                    },
                    Err(ReadinessFailure::Fatal(error)) => {
                        stop_connection(Some(client), Some(task)).await;
                        bail!(
                            "server became reachable but its fixture state is wrong: {error}; \
                             server log:\n{}",
                            self.log_tail()
                        );
                    }
                    Err(ReadinessFailure::Retry(error)) => {
                        last_error = error;
                        stop_connection(Some(client), Some(task)).await;
                    }
                },
                Err(error) => last_error = format!("{error:#}"),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn finish(mut self) -> Harness {
        Harness {
            lake: self
                .lake_client
                .take()
                .expect("readiness stored a lake client"),
            reference: self
                .reference_client
                .take()
                .expect("setup stored a reference client"),
            lake_namespace: self.namespace.clone(),
            reference_schema: self.reference_schema.clone(),
            lake_addr: self.lake_addr.expect("server listen address"),
            metrics_addr: self.metrics_addr.expect("metrics listen address"),
            warehouse_dir: self.warehouse_dir.clone(),
            state: self,
        }
    }

    fn log_tail(&self) -> String {
        let bytes = std::fs::read(&self.log_path).unwrap_or_default();
        let start = bytes.len().saturating_sub(8 * 1024);
        String::from_utf8_lossy(&bytes[start..]).into_owned()
    }

    /// Reclaim every resource this run created, collecting all failures rather
    /// than stopping at the first one. Child reaping and both port bindings are
    /// checked explicitly; dropping a `Child` handle is not treated as proof.
    async fn cleanup(&mut self) -> CleanupOutcome {
        let mut outcome = CleanupOutcome::default();

        stop_connection_recorded(
            self.lake_client.take(),
            self.lake_task.take(),
            "lake protocol",
            &mut outcome,
        )
        .await;
        stop_connection_recorded(
            self.reference_client.take(),
            self.reference_task.take(),
            "reference",
            &mut outcome,
        )
        .await;
        for task in self.connections.get_mut().drain(..) {
            stop_connection_recorded(None, Some(task), "additional client", &mut outcome)
                .await;
        }

        if let Some(mut child) = self.child.take() {
            outcome.pid = Some(child.pid);
            match child.terminate() {
                Ok(status) => {
                    outcome.reaped = true;
                    outcome.exit = Some(status);
                    outcome
                        .resources
                        .push(format!("child pid {} reaped ({status})", child.pid));
                }
                Err(error) => {
                    outcome.reaped = child.exit.is_some();
                    outcome
                        .errors
                        .push(format!("reap child pid {}: {error}", child.pid));
                }
            }
        } else {
            outcome.reaped = true;
            outcome
                .resources
                .push("no child process was spawned".to_string());
        }

        if let Some(workers) = &mut self.workers {
            workers.stop(&mut outcome).await;
        }

        for (label, address) in [
            ("PostgreSQL", self.lake_addr),
            ("metrics", self.metrics_addr),
        ] {
            let Some(address) = address else {
                continue;
            };
            let released = StdTcpListener::bind(address).is_ok();
            outcome
                .ports_free
                .push((label.to_string(), address.port(), released));
            if released {
                outcome
                    .resources
                    .push(format!("{label} port {} released", address.port()));
            } else {
                outcome
                    .errors
                    .push(format!("{label} port {} is still bound", address.port()));
            }
        }

        let mut namespaces = std::mem::take(&mut self.extra_namespaces);
        if self.namespace_created {
            namespaces.push((
                self.namespace.clone(),
                std::mem::take(&mut self.created_tables),
            ));
        }
        if namespaces.is_empty() {
            outcome
                .resources
                .push("metadata namespaces were not created".to_string());
        }
        for (namespace, tables) in namespaces {
            let meta = Arc::clone(&self.meta);
            for table in tables {
                let meta = Arc::clone(&meta);
                let namespace = namespace.clone();
                let table_for_log = table.clone();
                match bounded(CLEANUP_TIMEOUT, "drop metadata table", async {
                    meta.drop_table(&table, &namespace)
                        .await
                        .map_err(|error| format!("{error:?}"))
                })
                .await
                {
                    Ok(()) => outcome.resources.push(format!(
                        "metadata table {namespace}.{table_for_log} dropped"
                    )),
                    Err(error) => outcome.errors.push(format!(
                        "drop metadata table {namespace}.{table_for_log}: {error:#}"
                    )),
                }
            }
            let meta = Arc::clone(&self.meta);
            let namespace_for_delete = namespace;
            let namespace_for_log = namespace_for_delete.clone();
            match bounded(CLEANUP_TIMEOUT, "delete metadata namespace", async move {
                meta.delete_namespace_by_namespace(&namespace_for_delete)
                    .await
                    .map_err(|error| format!("{error:?}"))
            })
            .await
            {
                Ok(()) => outcome
                    .resources
                    .push(format!("metadata namespace {namespace_for_log} deleted")),
                Err(error) => outcome.errors.push(format!(
                    "delete metadata namespace {namespace_for_log}: {error:#}"
                )),
            }
        }

        // Index deletion is best-effort inside drop_table. A leftover shard
        // must still fail this harness; commits/leases reference it by FK.
        let catalog = self.meta.vector_index_catalog();
        match bounded(CLEANUP_TIMEOUT, "verify vector index cleanup", async {
            catalog
                .list_shards_under(&self.warehouse_url)
                .await
                .map_err(|error| format!("{error:?}"))
        })
        .await
        {
            Ok(shards) if shards.is_empty() => outcome
                .resources
                .push("owned vector index shards=0 (commit/lease FK cascade)".to_owned()),
            Ok(shards) => outcome
                .errors
                .push(format!("owned vector index shards remain: {shards:?}")),
            Err(error) => outcome
                .errors
                .push(format!("verify vector index cleanup: {error:#}")),
        }

        let reference_config = self.reference_config.clone();
        let reference_schema = self.reference_schema.clone();
        match connect_pg(&reference_config, "sql_compat_cleanup").await {
            Ok((client, task)) => {
                let sql = format!("DROP SCHEMA IF EXISTS \"{reference_schema}\" CASCADE");
                match bounded(CLEANUP_TIMEOUT, "drop reference schema", async {
                    client
                        .batch_execute(&sql)
                        .await
                        .map_err(|error| error.to_string())
                })
                .await
                {
                    Ok(()) => outcome
                        .resources
                        .push(format!("reference schema {reference_schema} dropped")),
                    Err(error) => outcome.errors.push(format!(
                        "drop reference schema {reference_schema}: {error:#}"
                    )),
                }
                stop_connection_recorded(
                    Some(client),
                    Some(task),
                    "cleanup reference",
                    &mut outcome,
                )
                .await;
            }
            Err(error) => outcome
                .errors
                .push(format!("connect reference cleanup client: {error:#}")),
        }

        if let Some(temp_dir) = self.temp_dir.take() {
            let path = temp_dir.path().to_path_buf();
            match temp_dir.close() {
                Ok(()) => outcome
                    .resources
                    .push(format!("temporary directory {} removed", path.display())),
                Err(error) => outcome.errors.push(format!(
                    "remove temporary directory {}: {error}",
                    path.display()
                )),
            }
        }

        outcome
    }
}

/// One child process with an explicit reaping state.
struct ChildGuard {
    child: Option<Child>,
    pid: u32,
    exit: Option<ExitStatus>,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        let pid = child.id();
        Self {
            child: Some(child),
            pid,
            exit: None,
        }
    }

    /// Non-blocking exit check that remembers the status for cleanup proof.
    fn poll_exit(&mut self) -> std::io::Result<Option<ExitStatus>> {
        if let Some(status) = self.exit {
            return Ok(Some(status));
        }
        let Some(child) = self.child.as_mut() else {
            return Ok(None);
        };
        match child.try_wait()? {
            Some(status) => {
                self.exit = Some(status);
                Ok(Some(status))
            }
            None => Ok(None),
        }
    }

    fn terminate(&mut self) -> std::result::Result<ExitStatus, String> {
        if let Some(status) = self.exit {
            self.child.take();
            return Ok(status);
        }
        let mut child = match self.child.take() {
            Some(child) => child,
            None => return Err("child handle was already taken".to_string()),
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                self.exit = Some(status);
                return Ok(status);
            }
            Ok(None) => {}
            Err(error) => {
                self.child = Some(child);
                return Err(format!("try_wait: {error}"));
            }
        }
        let _ = child.kill();
        match child.wait() {
            Ok(status) => {
                self.exit = Some(status);
                Ok(status)
            }
            Err(error) => {
                self.child = Some(child);
                Err(format!("wait after kill: {error}"))
            }
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.exit.is_some() {
            return;
        }
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[derive(Default)]
struct CleanupOutcome {
    pid: Option<u32>,
    reaped: bool,
    exit: Option<ExitStatus>,
    ports_free: Vec<(String, u16, bool)>,
    resources: Vec<String>,
    errors: Vec<String>,
}

impl CleanupOutcome {
    fn proof(&self) -> String {
        let mut proof = String::from("cleanup proof: ");
        match self.pid {
            Some(pid) => proof.push_str(&format!(
                "child pid={pid} reaped={} exit={}",
                self.reaped,
                self.exit
                    .as_ref()
                    .map(|status| status.to_string())
                    .unwrap_or_else(|| "unknown".to_string())
            )),
            None => proof.push_str("child=none reaped=true"),
        }
        proof.push_str("; ports=[");
        for (index, (label, port, released)) in self.ports_free.iter().enumerate() {
            if index > 0 {
                proof.push_str(", ");
            }
            proof.push_str(&format!("{label}:{port} released={released}"));
        }
        proof.push_str("]; resources=[");
        proof.push_str(&self.resources.join("; "));
        proof.push(']');
        if !self.errors.is_empty() {
            proof.push_str("; cleanup_errors=[");
            proof.push_str(&self.errors.join("; "));
            proof.push(']');
        }
        proof
    }
}

fn failure(primary: String, cleanup: &CleanupOutcome, logs: &str) -> anyhow::Error {
    let proof = cleanup.proof();
    eprintln!("sql-compat failure: {primary}");
    eprintln!("{proof}");
    if !logs.trim().is_empty() {
        eprintln!("postgres-lakesoul log tail:\n{logs}");
    }
    let mut message = format!("{primary}\n{proof}");
    if !logs.trim().is_empty() {
        message.push_str(&format!("\nserver log tail:\n{logs}"));
    }
    anyhow!(message)
}

enum ReadinessFailure {
    Retry(String),
    Fatal(String),
}

async fn readiness(client: &Client) -> std::result::Result<(), ReadinessFailure> {
    let sql = "SELECT 1";
    let messages = bounded(
        OPERATION_TIMEOUT,
        "readiness protocol query",
        client.simple_query(sql),
    )
    .await
    .map_err(|error| ReadinessFailure::Retry(format!("{error:#}")))?;
    let rows = simple_rows(&messages);
    if rows != 1 {
        return Err(ReadinessFailure::Fatal(format!(
            "{sql}: expected 1 row, got {rows}"
        )));
    }

    for (sql, expected) in [
        ("SELECT * FROM items", 4_usize),
        ("SELECT * FROM pk_items", 3),
        ("SELECT * FROM empty_items", 0),
    ] {
        let messages = bounded(
            OPERATION_TIMEOUT,
            "readiness fixture query",
            client.simple_query(sql),
        )
        .await
        .map_err(|error| {
            ReadinessFailure::Retry(format!("readiness query {sql} failed: {error:#}"))
        })?;
        let rows = simple_rows(&messages);
        if rows != expected {
            return Err(ReadinessFailure::Fatal(format!(
                "{sql}: expected {expected} rows, got {rows}"
            )));
        }
    }
    Ok(())
}

fn simple_rows(messages: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|message| matches!(message, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

async fn check_metrics(address: SocketAddr) -> std::result::Result<(), String> {
    match tokio::time::timeout(
        Duration::from_millis(750),
        tokio::net::TcpStream::connect(address),
    )
    .await
    {
        Ok(Ok(stream)) => {
            drop(stream);
            Ok(())
        }
        Ok(Err(error)) => Err(format!(
            "metrics listener {address} is unreachable: {error}"
        )),
        Err(_) => Err(format!("metrics listener {address} connect timed out")),
    }
}

fn reference_config_from_env() -> Result<Config> {
    let raw = pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY).map_err(
        |error| {
            anyhow!(
                "metadata PostgreSQL configuration is unavailable \
                 (LAKESOUL_PG_URL or lakesoul_home): {error:?}"
            )
        },
    )?;
    Config::from_str(&raw).with_context(|| "parse metadata PostgreSQL configuration")
}

fn lake_config(address: SocketAddr, namespace: &str) -> Config {
    let mut config = Config::new();
    config
        .host("127.0.0.1")
        .port(address.port())
        .user("sql_compat")
        .dbname(namespace)
        .connect_timeout(OPERATION_TIMEOUT);
    config
}

async fn connect_pg(config: &Config, label: &str) -> Result<(Client, JoinHandle<()>)> {
    let mut config = config.clone();
    config
        .application_name(label)
        .connect_timeout(OPERATION_TIMEOUT);
    // Keep the driver's typed ErrorResponse (not merely its Display text),
    // so startup rejection cases can distinguish SQLSTATE from transport errors.
    let (client, connection) =
        tokio::time::timeout(OPERATION_TIMEOUT, config.connect(NoTls))
            .await
            .with_context(|| format!("connect to {label} timed out"))?
            .with_context(|| format!("connect to {label}"))?;
    let label = label.to_string();
    let task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("sql-compat {label} connection ended: {error}");
        }
    });
    Ok((client, task))
}

async fn stop_connection(client: Option<Client>, task: Option<JoinHandle<()>>) {
    drop(client);
    if let Some(mut task) = task
        && tokio::time::timeout(Duration::from_secs(2), &mut task)
            .await
            .is_err()
    {
        task.abort();
        let _ = task.await;
    }
}

async fn stop_connection_recorded(
    client: Option<Client>,
    task: Option<JoinHandle<()>>,
    label: &str,
    outcome: &mut CleanupOutcome,
) {
    stop_connection(client, task).await;
    outcome.resources.push(format!("{label} connection closed"));
}

async fn configure_session(client: &Client) -> Result<()> {
    for sql in SESSION_SETTINGS {
        bounded(
            OPERATION_TIMEOUT,
            "configure session",
            client.batch_execute(sql),
        )
        .await
        .with_context(|| format!("configure session with {sql}"))?;
    }
    Ok(())
}

async fn reference_execute(client: &Client, sql: &str) -> Result<()> {
    bounded(
        OPERATION_TIMEOUT,
        "reference fixture SQL",
        client.batch_execute(sql),
    )
    .await
    .with_context(|| format!("reference fixture SQL failed: {sql}"))
}

async fn reference_row_count(client: &Client, sql: &str) -> Result<usize> {
    bounded(
        OPERATION_TIMEOUT,
        "reference fixture query",
        client.simple_query(sql),
    )
    .await
    .map(|messages| simple_rows(&messages))
    .with_context(|| format!("reference fixture query failed: {sql}"))
}

async fn management_execute(context: &SessionContext, sql: &str) -> Result<()> {
    bounded(OPERATION_TIMEOUT, "management SQL", async {
        let frame = context.sql(sql).await.map_err(|error| error.to_string())?;
        frame
            .collect()
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    })
    .await
    .with_context(|| format!("management SQL failed: {sql}"))
}

async fn management_row_count(
    context: &SessionContext,
    namespace: &str,
    table: &str,
) -> Result<usize> {
    let sql = format!("SELECT * FROM lakesoul.{namespace}.{table}");
    bounded(OPERATION_TIMEOUT, "management fixture query", async {
        let frame = context.sql(&sql).await.map_err(|error| error.to_string())?;
        frame.collect().await.map_err(|error| error.to_string())
    })
    .await
    .map(|batches| batches.iter().map(|batch| batch.num_rows()).sum())
    .with_context(|| format!("management fixture query failed: {sql}"))
}

/// Prove the primary-key fixture really spans two published versions and both
/// files exist on disk, using metadata rather than the read-only server.
async fn prove_pk_two_commits(meta: &MetaDataClient, namespace: &str) -> Result<()> {
    let table = bounded(OPERATION_TIMEOUT, "read pk_items table metadata", async {
        meta.get_table_info_by_table_name("pk_items", namespace)
            .await
            .map_err(|error| format!("{error:?}"))
    })
    .await?
    .ok_or_else(|| anyhow!("metadata has no pk_items table in namespace {namespace}"))?;
    ensure!(
        table.table_namespace == namespace,
        "pk_items metadata namespace is {}, expected {namespace}",
        table.table_namespace
    );

    let partitions = bounded(OPERATION_TIMEOUT, "read pk_items partitions", async {
        meta.get_all_partition_info(&table.table_id)
            .await
            .map_err(|error| format!("{error:?}"))
    })
    .await?;
    let commits: usize = partitions
        .iter()
        .map(|partition| partition.snapshot.len())
        .sum();
    ensure!(
        commits >= 2,
        "pk_items metadata holds {commits} commit version(s); the fixture must span at least two"
    );

    let files = bounded(OPERATION_TIMEOUT, "read pk_items data files", async {
        meta.get_data_files_by_table_name("pk_items", namespace)
            .await
            .map_err(|error| format!("{error:?}"))
    })
    .await?;
    ensure!(
        files.len() >= 2,
        "pk_items exposes {} active file(s); two independent commits must produce at least two",
        files.len()
    );
    for file in &files {
        let local: &Path = file
            .strip_prefix("file://")
            .map_or_else(|| Path::new(file), |path| Path::new(path));
        ensure!(
            local.is_absolute(),
            "pk_items metadata path is not a local absolute path: {file}"
        );
        ensure!(
            local.exists(),
            "pk_items metadata points at missing file: {file}"
        );
    }
    eprintln!(
        "fixture proof: pk_items commits={commits} active_files={} existing-local-parquet=true",
        files.len()
    );
    Ok(())
}

fn ensure_eq(what: &str, actual: usize, expected: usize) -> Result<()> {
    ensure!(
        actual == expected,
        "{what}: expected {expected} row(s), got {actual}"
    );
    Ok(())
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Bound one future and turn both timeouts and errors into `anyhow` errors
/// with the operation name attached.
async fn bounded<F, T, E>(limit: Duration, what: &str, future: F) -> Result<T>
where
    F: Future<Output = std::result::Result<T, E>>,
    E: std::fmt::Display,
{
    match tokio::time::timeout(limit, future).await {
        Err(_) => bail!("{what}: timed out after {limit:?}"),
        Ok(Err(error)) => bail!("{what}: {error}"),
        Ok(Ok(value)) => Ok(value),
    }
}
