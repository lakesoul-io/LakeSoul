// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Background builder for tables that use `index_maintenance = deferred`.
//!
//! Writes to a deferred table commit data files only.  This process (or the
//! child the coordinator starts in `local` mode) scans the metadata for such
//! tables and builds the shards whose active files are not covered by an
//! index commit yet.
//!
//! The runner holds a lease in `maintenance.lease`, so several coordinators
//! and an external Kubernetes Deployment can run at the same time and only
//! one of them does the work.  The lease is longer than the poll interval and
//! is renewed by a heartbeat while a large table is being built.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::ValueEnum;
use lakesoul_common::misc::SecretMap;
use lakesoul_metadata::{MetaDataClientRef, QueryType};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::Result;

/// How the PG wire server obtains the deferred-index builder.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum IndexMaintenanceExecutor {
    /// Start and supervise one child process (all-in-one and local runs).
    Local,
    /// Leave the process to an external Deployment (Helm).
    External,
    /// Do not build deferred indexes.
    Off,
}

/// The lease row name.  One row is enough: every namespace in one metadata
/// database is served by the same runner.
const LEASE_NAME: &str = "index-maintenance";

/// Run the standalone worker.  No PG wire server is started.
pub async fn run(
    meta: MetaDataClientRef,
    object_store_options: SecretMap,
    owner: String,
    poll_interval: Duration,
    lease_ttl: Duration,
) -> Result<()> {
    ensure_lease_table(&meta).await?;
    info!(
        runner = %owner,
        poll_ms = poll_interval.as_millis(),
        lease_ttl_ms = lease_ttl.as_millis(),
        "index maintenance runner started"
    );

    let mut ticker = tokio::time::interval(poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut heartbeat: Option<JoinHandle<()>> = None;
    let mut owns_lease = false;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if !owns_lease || heartbeat.as_ref().is_some_and(JoinHandle::is_finished) {
                    if let Some(task) = heartbeat.take() {
                        task.abort();
                    }
                    match acquire_lease(&meta, &owner, lease_ttl).await {
                        Ok(true) => {
                            owns_lease = true;
                            heartbeat = Some(spawn_heartbeat(
                                Arc::clone(&meta),
                                owner.clone(),
                                lease_ttl,
                            ));
                        }
                        Ok(false) => {
                            owns_lease = false;
                            tracing::debug!(runner = %owner, "another index runner owns the lease");
                            continue;
                        }
                        Err(error) => {
                            owns_lease = false;
                            warn!(%error, "acquiring the index maintenance lease failed");
                            continue;
                        }
                    }
                }

                let candidates = match deferred_tables(&meta).await {
                    Ok(candidates) => candidates,
                    Err(error) => {
                        warn!(%error, "listing deferred-index tables failed");
                        continue;
                    }
                };
                for (namespace, table) in candidates {
                    if heartbeat.as_ref().is_some_and(JoinHandle::is_finished) {
                        owns_lease = false;
                        break;
                    }
                    if let Err(error) = build_table(
                        Arc::clone(&meta),
                        table.clone(),
                        namespace.clone(),
                        object_store_options.clone(),
                    )
                    .await
                    {
                        warn!(
                            namespace = %namespace,
                            table = %table,
                            %error,
                            "deferred index build failed"
                        );
                    }
                }
            }
            _ = crate::shutdown_signal() => break,
        }
    }

    if let Some(task) = heartbeat.take() {
        task.abort();
    }
    if owns_lease && let Err(error) = release_lease(&meta, &owner).await {
        warn!(%error, "releasing the index maintenance lease failed");
    }
    info!(runner = %owner, "index maintenance runner stopped");
    Ok(())
}

/// A supervised local index-maintenance child.
pub struct LocalSupervisor {
    shutdown: watch::Sender<bool>,
    handle: JoinHandle<()>,
}

impl LocalSupervisor {
    /// Stop the child and wait for the supervisor task.  The child gets
    /// SIGTERM first so it can release its lease; a stuck build is killed
    /// after a short grace period.
    pub async fn stop(self) {
        let _ = self.shutdown.send(true);
        let _ = self.handle.await;
    }
}

/// Start the local child supervisor.  The returned task runs for the life of
/// the coordinator; the child inherits stdout/stderr and the server's
/// metadata/object-store environment.
pub fn spawn_local_supervisor(
    program: PathBuf,
    poll_interval: Duration,
    lease_ttl: Duration,
) -> LocalSupervisor {
    let (shutdown, mut shutdown_rx) = watch::channel(false);
    let handle = tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        loop {
            let mut command = Command::new(&program);
            command
                .arg("--index-maintenance-runner")
                .arg("--no-metrics")
                .arg("--index-maintenance-poll-ms")
                .arg(poll_interval.as_millis().to_string())
                .arg("--index-lease-ttl-ms")
                .arg(lease_ttl.as_millis().to_string())
                // The child is a worker, not another coordinator.
                .env_remove("LAKESOUL_PG_INDEX_MAINTENANCE_EXECUTOR")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::inherit())
                .stderr(std::process::Stdio::inherit());
            install_parent_death_signal(&mut command);

            let started = std::time::Instant::now();
            let mut child = match command.spawn() {
                Ok(child) => child,
                Err(error) => {
                    warn!(
                        %error,
                        program = %program.display(),
                        "spawning the index maintenance child failed"
                    );
                    backoff = (backoff * 2).min(Duration::from_secs(60));
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown_rx.changed() => break,
                    }
                    continue;
                }
            };

            tokio::select! {
                status = child.wait() => {
                    match status {
                        Ok(status) => warn!(%status, "index maintenance child exited"),
                        Err(error) => {
                            warn!(%error, "waiting for index maintenance child failed")
                        }
                    }
                    if started.elapsed() >= Duration::from_secs(60) {
                        backoff = Duration::from_secs(1);
                    } else {
                        backoff = (backoff * 2).min(Duration::from_secs(60));
                    }
                }
                _ = shutdown_rx.changed() => {
                    terminate_child(&mut child).await;
                    break;
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = shutdown_rx.changed() => break,
            }
        }
    });
    LocalSupervisor { shutdown, handle }
}

async fn terminate_child(child: &mut Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        // SAFETY: `kill` is async-signal-safe and only sends a signal.
        unsafe {
            libc::kill(pid as libc::pid_t, libc::SIGTERM);
        }
    }
    match tokio::time::timeout(Duration::from_secs(5), child.wait()).await {
        Ok(_) => {}
        Err(_) => {
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }
}

#[cfg(target_os = "linux")]
fn install_parent_death_signal(command: &mut Command) {
    // SAFETY: the closure only calls `prctl`, which is async-signal-safe and
    // does not touch the allocator. It runs after fork and before exec.
    unsafe {
        command.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(target_os = "linux"))]
fn install_parent_death_signal(_command: &mut Command) {}

// ---------------------------------------------------------------------------
// Lease
// ---------------------------------------------------------------------------

async fn ensure_lease_table(meta: &MetaDataClientRef) -> Result<()> {
    let pool = meta.pooled_client();
    let pool = pool.lock().await.clone();
    let installed: Option<bool> = pool
        .query_opt(
            "select to_regclass('maintenance.lease') is not null",
            QueryType::RW,
            &[],
        )
        .await
        .map_err(|error| rootcause::report!("check maintenance.lease: {error}"))?
        .map(|row| row.get(0));
    if installed != Some(true) {
        rootcause::bail!(
            "maintenance.lease is missing: apply script/meta_init.sql or run \
             script/metadata_migrate.py"
        );
    }
    Ok(())
}

/// Try to take or renew the single maintenance lease.  Timestamps come from
/// the metadata database clock, so pod clock skew cannot steal or extend a
/// lease.
async fn acquire_lease(
    meta: &MetaDataClientRef,
    owner: &str,
    ttl: Duration,
) -> Result<bool> {
    let ttl_ms = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
    let pool = meta.pooled_client();
    let pool = pool.lock().await.clone();
    pool.execute(
        "insert into maintenance.lease (name, owner, lease_expires_at, updated_at) \
         values ($1, $2, \
                 (extract(epoch from clock_timestamp()) * 1000)::bigint + $3, \
                 (extract(epoch from clock_timestamp()) * 1000)::bigint) \
         on conflict (name) do nothing",
        QueryType::RW,
        &[&LEASE_NAME, &owner, &ttl_ms],
    )
    .await
    .map_err(|error| rootcause::report!("insert index maintenance lease: {error}"))?;
    let updated = pool
        .execute(
            "update maintenance.lease \
             set owner = $2, \
                 lease_expires_at = \
                     (extract(epoch from clock_timestamp()) * 1000)::bigint + $3, \
                 updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint \
             where name = $1 \
               and (owner is null or owner = $2 or lease_expires_at is null \
                    or lease_expires_at < \
                       (extract(epoch from clock_timestamp()) * 1000)::bigint)",
            QueryType::RW,
            &[&LEASE_NAME, &owner, &ttl_ms],
        )
        .await
        .map_err(|error| rootcause::report!("renew index maintenance lease: {error}"))?;
    Ok(updated > 0)
}

async fn release_lease(meta: &MetaDataClientRef, owner: &str) -> Result<()> {
    let pool = meta.pooled_client();
    let pool = pool.lock().await.clone();
    pool.execute(
        "update maintenance.lease \
         set owner = null, lease_expires_at = null, \
             updated_at = (extract(epoch from clock_timestamp()) * 1000)::bigint \
         where name = $1 and owner = $2",
        QueryType::RW,
        &[&LEASE_NAME, &owner],
    )
    .await
    .map_err(|error| rootcause::report!("release index maintenance lease: {error}"))?;
    Ok(())
}

fn spawn_heartbeat(
    meta: MetaDataClientRef,
    owner: String,
    ttl: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let interval = (ttl / 3).max(Duration::from_secs(1));
        let mut ticker = tokio::time::interval(interval);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match acquire_lease(&meta, &owner, ttl).await {
                Ok(true) => {}
                Ok(false) => {
                    warn!("index maintenance lease was taken over");
                    return;
                }
                Err(error) => {
                    warn!(%error, "renewing the index maintenance lease failed");
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Sweep
// ---------------------------------------------------------------------------

async fn deferred_tables(meta: &MetaDataClientRef) -> Result<Vec<(String, String)>> {
    let pool = meta.pooled_client();
    let pool = pool.lock().await.clone();
    let rows = pool
        .query(
            "select table_namespace, table_name \
             from table_info \
             where properties ->> 'index_maintenance' = 'deferred' \
               and (properties ->> 'vector_index_columns' is not null \
                    or properties ->> 'text_index_columns' is not null) \
             order by table_namespace, table_name",
            QueryType::RO,
            &[],
        )
        .await
        .map_err(|error| rootcause::report!("list deferred-index tables: {error}"))?;
    Ok(rows.iter().map(|row| (row.get(0), row.get(1))).collect())
}

async fn build_table(
    meta: MetaDataClientRef,
    table: String,
    namespace: String,
    object_store_options: SecretMap,
) -> Result<usize> {
    let started = std::time::Instant::now();
    let task_table = table.clone();
    let task_namespace = namespace.clone();
    let built = tokio::task::spawn_blocking(move || -> Result<usize> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| rootcause::report!("create runtime: {error}"))?;
        let built = runtime
            .block_on(
                lakesoul_datafusion::index_maintenance::build_pending_indices_with_options(
                    meta,
                    &task_table,
                    &task_namespace,
                    object_store_options,
                ),
            )
            .map_err(|error| rootcause::report!("{error}"))?;
        Ok(built)
    })
    .await
    .map_err(|error| rootcause::report!("index build task failed: {error}"))??;
    if built > 0 {
        info!(
            namespace = %namespace,
            table = %table,
            built,
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            "built pending index shards"
        );
    }
    Ok(built)
}
