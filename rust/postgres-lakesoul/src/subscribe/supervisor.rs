// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Supervises subscriber processes.
//!
//! A subscription is one child process. The child reads its full definition
//! from the metadata PostgreSQL, so the parent only needs the subscription id
//! and a log file. The parent keeps the child's PID for `DROP`/`ALTER`, and a
//! reaper records an unexpected exit in the metadata.
//!
//! Two execution modes share this type:
//!
//! * `Local`: the PG wire server starts the child itself (all-in-one and
//!   single-server deployments).
//! * `External`: the server only writes the desired state; a separate
//!   `--subscription-runner` process reconciles metadata and owns the child.
//!   That process uses [`SubscriptionSupervisor::runner`] so it also holds the
//!   per-subscription lease while a child runs.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::ValueEnum;
use lakesoul_metadata::MetaDataClientRef;
use rootcause::{bail, report};
use tokio::sync::{Mutex, Notify};

use super::state::{SubscriptionMetadata, SubscriptionRecord};
use crate::Result;

const STOP_TIMEOUT: Duration = Duration::from_secs(10);
const KILL_TIMEOUT: Duration = Duration::from_secs(2);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_POLL: Duration = Duration::from_millis(200);

/// Who owns the subscriber process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SubscriptionMode {
    /// The PG wire server starts and stops the child itself.
    Local,
    /// A separate subscription runner reconciles metadata and owns the child.
    External,
}

#[derive(Clone)]
struct LeaseSettings {
    owner: String,
    ttl: Duration,
}

#[derive(Clone)]
pub struct SubscriptionSupervisor {
    meta: MetaDataClientRef,
    program: PathBuf,
    log_dir: PathBuf,
    children: Arc<Mutex<HashMap<String, ChildState>>>,
    mode: SubscriptionMode,
    control_timeout: Duration,
    lease: Option<LeaseSettings>,
}

struct ChildState {
    pid: u32,
    done: Arc<Notify>,
}

impl SubscriptionSupervisor {
    pub fn with_mode(
        meta: MetaDataClientRef,
        program: PathBuf,
        log_dir: PathBuf,
        mode: SubscriptionMode,
    ) -> Self {
        Self {
            meta,
            program,
            log_dir,
            children: Arc::new(Mutex::new(HashMap::new())),
            mode,
            control_timeout: CONTROL_TIMEOUT,
            lease: None,
        }
    }

    /// A local supervisor used by the standalone subscription runner. It
    /// owns children just like the server's local mode, but takes a lease in
    /// metadata so two runners do not manage the same subscription.
    pub fn runner(
        meta: MetaDataClientRef,
        program: PathBuf,
        log_dir: PathBuf,
        owner: String,
        ttl: Duration,
    ) -> Self {
        let mut supervisor =
            Self::with_mode(meta, program, log_dir, SubscriptionMode::Local);
        supervisor.lease = Some(LeaseSettings { owner, ttl });
        supervisor
    }

    pub async fn metadata(&self) -> SubscriptionMetadata {
        let pool = self.meta.pooled_client();
        let pool = pool.lock().await.clone();
        SubscriptionMetadata::from_pool(pool)
    }

    /// Ask for a subscription to run. Local mode starts the child; external
    /// mode only moves metadata to `starting` for the runner.
    pub async fn start(&self, subscription_id: &str) -> Result<()> {
        if self.mode == SubscriptionMode::External {
            let metadata = self.metadata().await;
            if metadata.state(subscription_id).await?.is_none() {
                bail!("subscription {subscription_id} does not exist");
            }
            metadata
                .set_state(subscription_id, "starting", None)
                .await?;
            return Ok(());
        }
        if !self.start_leased(subscription_id).await? {
            bail!(
                "subscription {subscription_id} is owned by another subscription runner"
            );
        }
        Ok(())
    }

    /// Start if this process owns (or can take) the lease. `Ok(false)` means
    /// another runner still owns it; that is not an error during resume.
    async fn start_leased(&self, subscription_id: &str) -> Result<bool> {
        if !self.acquire_lease(subscription_id).await? {
            return Ok(false);
        }
        if let Err(error) = self.start_local(subscription_id).await {
            self.release_lease(subscription_id).await;
            return Err(error);
        }
        Ok(true)
    }

    async fn start_local(&self, subscription_id: &str) -> Result<()> {
        let mut children = self.children.lock().await;
        if children.contains_key(subscription_id) {
            bail!("subscription {subscription_id} already has a running process");
        }

        tokio::fs::create_dir_all(&self.log_dir)
            .await
            .map_err(|error| {
                report!(
                    "create subscription log directory {}: {error}",
                    self.log_dir.display()
                )
            })?;
        let log_path = self
            .log_dir
            .join(format!("{}.log", safe_file_name(subscription_id)));
        let stdout = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|error| {
                report!("open subscription log {}: {error}", log_path.display())
            })?;
        let stderr = stdout
            .try_clone()
            .map_err(|error| report!("clone subscription log handle: {error}"))?;

        let mut command = tokio::process::Command::new(&self.program);
        command
            .arg("--subscribe")
            .arg("--subscription-id")
            .arg(subscription_id)
            .arg("--no-metrics")
            // A runner started through the environment must not pass its own
            // role switch to the subscriber child.
            .env_remove("LAKESOUL_PG_SUBSCRIPTION_RUNNER")
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        install_parent_death_signal(&mut command);

        let mut child = command.spawn().map_err(|error| {
            report!(
                "spawn subscriber process for {subscription_id} with {}: {error}",
                self.program.display()
            )
        })?;
        let Some(pid) = child.id() else {
            let _ = child.start_kill();
            let _ = child.wait().await;
            return Err(report!(
                "subscriber process for {subscription_id} has no pid"
            ));
        };
        let done = Arc::new(Notify::new());
        children.insert(
            subscription_id.to_string(),
            ChildState {
                pid,
                done: Arc::clone(&done),
            },
        );
        drop(children);

        let metadata = self.metadata().await;
        let log_path_text = log_path.to_string_lossy().to_string();
        if let Err(error) = metadata
            .set_process(subscription_id, pid, &log_path_text)
            .await
        {
            terminate_pid(pid, libc::SIGTERM);
            let _ = child.wait().await;
            self.children.lock().await.remove(subscription_id);
            return Err(error);
        }
        if !self.children.lock().await.contains_key(subscription_id) {
            // The child exited between `spawn` and the metadata update. Its
            // reaper already recorded the exit; do not leave a stale pid.
            metadata.clear_process(subscription_id).await?;
        }

        let meta = self.meta.clone();
        let children = Arc::clone(&self.children);
        let id = subscription_id.to_string();
        tokio::spawn(async move {
            let status = child.wait().await;
            let message = match &status {
                Ok(status) if status.success() => {
                    format!("subscriber process for {id} exited")
                }
                Ok(status) => {
                    format!("subscriber process for {id} exited with {status}")
                }
                Err(error) => {
                    format!(
                        "subscriber process for {id} could not be waited for: {error}"
                    )
                }
            };
            let pool = meta.pooled_client();
            let pool = pool.lock().await.clone();
            let metadata = SubscriptionMetadata::from_pool(pool);
            let succeeded = status.is_ok_and(|status| status.success());
            if !succeeded
                && let Err(error) = metadata.record_unexpected_exit(&id, &message).await
            {
                tracing::warn!(
                    %error,
                    subscription = %id,
                    "failed to record subscriber process exit"
                );
            }
            if let Err(error) = metadata.clear_process(&id).await {
                tracing::warn!(
                    %error,
                    subscription = %id,
                    "failed to clear subscriber process pid"
                );
            }
            children.lock().await.remove(&id);
            done.notify_waiters();
        });

        tracing::info!(
            subscription = %subscription_id,
            pid,
            log = %log_path.display(),
            "started subscription process"
        );
        Ok(())
    }

    /// Stop a locally owned child. External mode has no child here; the
    /// runner observes `disabled`/`dropping` metadata and stops its own.
    pub async fn stop(&self, subscription_id: &str) -> Result<()> {
        if self.mode == SubscriptionMode::External {
            return Ok(());
        }
        let (pid, done) = {
            let children = self.children.lock().await;
            match children.get(subscription_id) {
                Some(state) => (state.pid, Arc::clone(&state.done)),
                None => {
                    drop(children);
                    self.metadata().await.clear_process(subscription_id).await?;
                    return Ok(());
                }
            }
        };

        let notified = done.notified();
        terminate_pid(pid, libc::SIGTERM);
        if tokio::time::timeout(STOP_TIMEOUT, notified).await.is_err() {
            tracing::warn!(
                subscription = %subscription_id,
                pid,
                "subscriber did not stop after SIGTERM; killing it"
            );
            terminate_pid(pid, libc::SIGKILL);
            // The reaper removes the map entry; give it a moment to observe
            // the wait result before returning to the SQL statement.
            let notified = done.notified();
            let _ = tokio::time::timeout(KILL_TIMEOUT, notified).await;
        }
        self.metadata().await.clear_process(subscription_id).await?;
        Ok(())
    }

    pub async fn stop_all(&self) {
        if self.mode == SubscriptionMode::External {
            return;
        }
        let ids: Vec<String> = self.children.lock().await.keys().cloned().collect();
        for id in ids {
            if let Err(error) = self.stop(&id).await {
                tracing::warn!(%error, subscription = %id, "failed to stop subscriber");
            }
            self.release_lease(&id).await;
        }
    }

    pub async fn resume_all(&self) -> Result<()> {
        if self.mode == SubscriptionMode::External {
            return Ok(());
        }
        let metadata = self.metadata().await;
        let rows = metadata.list_supervised().await?;
        for (id, enabled, state) in rows {
            if !enabled || state == "dropping" {
                continue;
            }
            match self.start_leased(&id).await {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!(
                        subscription = %id,
                        "another subscription runner owns this lease; skipping resume"
                    );
                }
                Err(error) => {
                    tracing::error!(%error, subscription = %id, "failed to resume subscription");
                    let text = error.to_string();
                    if let Err(state_error) =
                        metadata.set_state(&id, "error", Some(&text)).await
                    {
                        tracing::warn!(
                            %state_error,
                            subscription = %id,
                            "failed to record resume failure"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn is_running(&self, subscription_id: &str) -> bool {
        self.children.lock().await.contains_key(subscription_id)
    }

    /// Turn a subscription on. Local mode starts it; external mode writes the
    /// desired state and returns (the runner starts it asynchronously).
    pub async fn enable(&self, record: &SubscriptionRecord) -> Result<()> {
        let id = &record.subscription_id;
        if record.state == "dropping" {
            bail!("subscription {id} is being dropped");
        }
        let metadata = self.metadata().await;
        if self.mode == SubscriptionMode::External {
            metadata.set_enabled(id, true).await?;
            if !matches!(
                record.state.as_str(),
                "starting" | "snapshotting" | "streaming"
            ) {
                metadata.set_state(id, "starting", None).await?;
            }
            return Ok(());
        }
        metadata.set_enabled(id, true).await?;
        if self.is_running(id).await {
            return Ok(());
        }
        metadata.set_state(id, "starting", None).await?;
        if let Err(error) = self.start(id).await {
            let text = error.to_string();
            let _ = metadata.set_state(id, "error", Some(&text)).await;
            return Err(error);
        }
        Ok(())
    }

    /// Turn a subscription off. External mode waits for the runner to confirm
    /// `disabled`, bounded by the control timeout.
    pub async fn disable(&self, record: &SubscriptionRecord) -> Result<()> {
        let id = &record.subscription_id;
        if record.state == "dropping" {
            bail!("subscription {id} is being dropped");
        }
        if self.mode == SubscriptionMode::External {
            let metadata = self.metadata().await;
            if record.state == "disabled" {
                metadata.set_enabled(id, false).await?;
                return Ok(());
            }
            metadata.set_enabled(id, false).await?;
            metadata.set_state(id, "disabling", None).await?;
            self.wait_for_state(id, "disabled").await?;
            return Ok(());
        }
        if self.is_running(id).await {
            self.stop(id).await?;
        }
        let metadata = self.metadata().await;
        metadata.set_enabled(id, false).await?;
        metadata.set_state(id, "disabled", None).await?;
        self.release_lease(id).await;
        Ok(())
    }

    /// Drop a subscription. Local mode stops the child, drops the replication
    /// slot and unregisters. External mode moves metadata to `dropping` and
    /// waits for the runner to finish the same work.
    pub async fn drop_subscription(&self, record: &SubscriptionRecord) -> Result<()> {
        let id = &record.subscription_id;
        if self.mode == SubscriptionMode::External {
            let metadata = self.metadata().await;
            metadata.set_enabled(id, false).await?;
            metadata.set_state(id, "dropping", None).await?;
            self.wait_for_gone(id).await?;
            return Ok(());
        }
        self.drop_owned(record).await
    }

    async fn drop_owned(&self, record: &SubscriptionRecord) -> Result<()> {
        let id = &record.subscription_id;
        self.stop(id).await?;
        if record.source_kind != "mysql" {
            drop_replication_slot(&super::ordinary_url(&record.source_url), &record.slot)
                .await?;
        }
        self.metadata().await.unregister(id).await?;
        self.release_lease(id).await;
        Ok(())
    }

    /// Reconcile metadata for the standalone runner. Only rows whose lease
    /// this runner owns are touched; a child is started for `starting`, and
    /// `disabled`/`dropping` rows are stopped or removed.
    pub async fn reconcile(&self) -> Result<()> {
        if self.mode == SubscriptionMode::External || self.lease.is_none() {
            return Ok(());
        }
        let metadata = self.metadata().await;
        for (id, enabled, state) in metadata.list_supervised().await? {
            if state == "dropping" {
                if !self.acquire_lease(&id).await? {
                    continue;
                }
                let Some(record) = metadata.load_by_id(&id).await? else {
                    self.release_lease(&id).await;
                    continue;
                };
                if let Err(error) = self.drop_owned(&record).await {
                    tracing::warn!(
                        %error,
                        subscription = %id,
                        "failed to complete subscription drop"
                    );
                }
                continue;
            }
            if !enabled {
                if self.is_running(&id).await
                    && let Err(error) = self.stop(&id).await
                {
                    tracing::warn!(
                        %error,
                        subscription = %id,
                        "failed to stop disabled subscription"
                    );
                }
                if state != "disabled"
                    && let Err(error) = metadata.set_state(&id, "disabled", None).await
                {
                    tracing::warn!(
                        %error,
                        subscription = %id,
                        "failed to mark subscription disabled"
                    );
                }
                self.release_lease(&id).await;
                continue;
            }
            if !self.acquire_lease(&id).await? {
                continue;
            }
            if self.is_running(&id).await {
                continue;
            }
            match state.as_str() {
                "starting" | "disabled" | "snapshotting" | "streaming" => {
                    if let Err(error) = self.start_local(&id).await {
                        tracing::warn!(
                            %error,
                            subscription = %id,
                            "failed to start subscription"
                        );
                        let text = error.to_string();
                        let _ = metadata.set_state(&id, "error", Some(&text)).await;
                        self.release_lease(&id).await;
                    }
                }
                "error" => {
                    // A failed subscription waits for ALTER ... ENABLE or a
                    // runner restart; it is not retried in a tight loop.
                    self.release_lease(&id).await;
                }
                _ => {
                    self.release_lease(&id).await;
                }
            }
        }
        Ok(())
    }

    async fn acquire_lease(&self, subscription_id: &str) -> Result<bool> {
        let Some(lease) = &self.lease else {
            return Ok(true);
        };
        self.metadata()
            .await
            .acquire_lease(subscription_id, &lease.owner, lease.ttl)
            .await
    }

    async fn release_lease(&self, subscription_id: &str) {
        let Some(lease) = &self.lease else {
            return;
        };
        if let Err(error) = self
            .metadata()
            .await
            .release_lease(subscription_id, &lease.owner)
            .await
        {
            tracing::warn!(
                %error,
                subscription = %subscription_id,
                "failed to release subscription lease"
            );
        }
    }

    async fn wait_for_state(&self, subscription_id: &str, wanted: &str) -> Result<()> {
        let deadline = Instant::now() + self.control_timeout;
        loop {
            let state = self.metadata().await.state(subscription_id).await?;
            match state.as_deref() {
                Some(state) if state == wanted => return Ok(()),
                None => bail!("subscription {subscription_id} disappeared"),
                _ => {}
            }
            if Instant::now() >= deadline {
                bail!(
                    "timed out after {}s waiting for subscription {} to become {}; \
                     the subscription runner may be unavailable",
                    self.control_timeout.as_secs(),
                    subscription_id,
                    wanted
                );
            }
            tokio::time::sleep(CONTROL_POLL).await;
        }
    }

    async fn wait_for_gone(&self, subscription_id: &str) -> Result<()> {
        let deadline = Instant::now() + self.control_timeout;
        loop {
            if !self.metadata().await.exists(subscription_id).await? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "timed out after {}s waiting for subscription {} to be dropped; \
                     the subscription runner may be unavailable",
                    self.control_timeout.as_secs(),
                    subscription_id
                );
            }
            tokio::time::sleep(CONTROL_POLL).await;
        }
    }
}

fn safe_file_name(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' || character == '-' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(unix)]
fn terminate_pid(pid: u32, signal: i32) {
    // `kill` only fails when the process is already gone, which is exactly the
    // state `stop` is waiting for.
    unsafe {
        libc::kill(pid as libc::pid_t, signal);
    }
}

#[cfg(not(unix))]
fn terminate_pid(_pid: u32, _signal: i32) {}

#[cfg(target_os = "linux")]
fn install_parent_death_signal(command: &mut tokio::process::Command) {
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
fn install_parent_death_signal(_command: &mut tokio::process::Command) {}

/// A convenience used by the SQL executor after a child has been stopped.
pub(crate) async fn drop_replication_slot(url: &str, slot: &str) -> Result<()> {
    let (client, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .map_err(|error| report!("connect to source PostgreSQL: {error}"))?;
    let handle = tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "source PostgreSQL connection closed");
        }
    });
    let result = async {
        let row = client
            .query_opt(
                "select active from pg_replication_slots \
                 where slot_name = $1 and database = current_database()",
                &[&slot],
            )
            .await
            .map_err(|error| report!("read replication slot {slot}: {error}"))?;
        let Some(row) = row else {
            return Ok(());
        };
        let active: bool = row.get(0);
        if active {
            bail!(
                "replication slot {slot} is still active; stop the subscriber \
                 before dropping the subscription"
            );
        }
        client
            .execute("select pg_drop_replication_slot($1)", &[&slot])
            .await
            .map_err(|error| report!("drop replication slot {slot}: {error}"))?;
        tracing::info!(slot, "dropped replication slot");
        Ok(())
    }
    .await;
    drop(client);
    handle.abort();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscribe::state::{SubscriptionDefinition, TableDefinition};
    use lakesoul_metadata::MetaDataClient;

    async fn metadata() -> SubscriptionMetadata {
        let client = MetaDataClient::from_env().await.unwrap();
        let pool = client.pooled_client();
        let pool = pool.lock().await.clone();
        SubscriptionMetadata::from_pool(pool)
    }

    fn test_id(name: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("runner_{name}_{}_{}", std::process::id(), nanos)
    }

    async fn register(metadata: &SubscriptionMetadata, id: &str) {
        let definition = SubscriptionDefinition {
            subscription_id: id.to_string(),
            source_kind: "postgres".to_string(),
            slot: id.to_string(),
            publication: "test_pub".to_string(),
            source_url: "postgresql://source/db".to_string(),
            snapshot_mode: "none".to_string(),
            auto_create: false,
            schema_evolution: "auto".to_string(),
            warehouse_prefix: None,
            cdc_column: "rowKinds".to_string(),
            supervised: true,
            commit_rows: 100,
            commit_interval_ms: 100,
            snapshot_batch_rows: 100,
            snapshot_batch_bytes: 1024,
            snapshot_parallelism: 1,
            snapshot_memory_bytes: 1024,
            tables: vec![TableDefinition {
                source_schema: "public".to_string(),
                source_table: "users".to_string(),
                source_relation_oid: 42,
                target_namespace: "default".to_string(),
                target_table: id.to_string(),
                target_table_id: format!("{id}_table"),
                primary_key: vec!["id".to_string()],
            }],
        };
        assert!(metadata.register(&definition).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lease_is_exclusive_and_expires() {
        let metadata = metadata().await;
        metadata.init_schema().await.unwrap();
        let id = test_id("lease");
        register(&metadata, &id).await;

        assert!(
            metadata
                .acquire_lease(&id, "runner-a", Duration::from_millis(50))
                .await
                .unwrap()
        );
        assert!(
            !metadata
                .acquire_lease(&id, "runner-b", Duration::from_millis(50))
                .await
                .unwrap(),
            "a live lease must exclude another runner"
        );
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            metadata
                .acquire_lease(&id, "runner-b", Duration::from_millis(50))
                .await
                .unwrap(),
            "an expired lease can be taken over"
        );
        metadata.release_lease(&id, "runner-b").await.unwrap();
        metadata.unregister(&id).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_disable_waits_for_the_runner() {
        let metadata = metadata().await;
        metadata.init_schema().await.unwrap();
        let id = test_id("disable");
        register(&metadata, &id).await;
        let record = metadata.load_by_id(&id).await.unwrap().unwrap();
        let supervisor = SubscriptionSupervisor::with_mode(
            Arc::new(MetaDataClient::from_env().await.unwrap()),
            PathBuf::new(),
            PathBuf::new(),
            SubscriptionMode::External,
        );

        let runner_metadata = metadata.clone();
        let runner_id = id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            runner_metadata
                .set_state(&runner_id, "disabled", None)
                .await
                .unwrap();
        });
        supervisor.disable(&record).await.unwrap();
        assert_eq!(
            metadata.state(&id).await.unwrap().as_deref(),
            Some("disabled")
        );
        metadata.unregister(&id).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn external_drop_waits_for_the_runner() {
        let metadata = metadata().await;
        metadata.init_schema().await.unwrap();
        let id = test_id("drop");
        register(&metadata, &id).await;
        let record = metadata.load_by_id(&id).await.unwrap().unwrap();
        let supervisor = SubscriptionSupervisor::with_mode(
            Arc::new(MetaDataClient::from_env().await.unwrap()),
            PathBuf::new(),
            PathBuf::new(),
            SubscriptionMode::External,
        );

        let runner_metadata = metadata.clone();
        let runner_id = id.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            runner_metadata.unregister(&runner_id).await.unwrap();
        });
        supervisor.drop_subscription(&record).await.unwrap();
        assert!(!metadata.exists(&id).await.unwrap());
    }
}
