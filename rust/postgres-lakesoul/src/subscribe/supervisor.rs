// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Supervises the local subscriber processes started by SQL.
//!
//! A subscription is one child process. The child reads its full definition
//! from the metadata PostgreSQL, so the parent only needs the subscription id
//! and a log file. The parent keeps the child's PID for `DROP`/`ALTER`, and a
//! reaper records an unexpected exit in the metadata.

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use lakesoul_metadata::MetaDataClientRef;
use rootcause::{bail, report};
use tokio::sync::{Mutex, Notify};

use super::state::SubscriptionMetadata;
use crate::Result;

const STOP_TIMEOUT: Duration = Duration::from_secs(10);
const KILL_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct SubscriptionSupervisor {
    meta: MetaDataClientRef,
    program: PathBuf,
    log_dir: PathBuf,
    children: Arc<Mutex<HashMap<String, ChildState>>>,
}

struct ChildState {
    pid: u32,
    done: Arc<Notify>,
}

impl SubscriptionSupervisor {
    pub fn new(meta: MetaDataClientRef, program: PathBuf, log_dir: PathBuf) -> Self {
        Self {
            meta,
            program,
            log_dir,
            children: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn metadata(&self) -> SubscriptionMetadata {
        let pool = self.meta.pooled_client();
        let pool = pool.lock().await.clone();
        SubscriptionMetadata::from_pool(pool)
    }

    pub async fn start(&self, subscription_id: &str) -> Result<()> {
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

    pub async fn stop(&self, subscription_id: &str) -> Result<()> {
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
        let ids: Vec<String> = self.children.lock().await.keys().cloned().collect();
        for id in ids {
            if let Err(error) = self.stop(&id).await {
                tracing::warn!(%error, subscription = %id, "failed to stop subscriber");
            }
        }
    }

    pub async fn resume_all(&self) -> Result<()> {
        let metadata = self.metadata().await;
        let ids = metadata.list_enabled_ids().await?;
        for id in ids {
            if let Err(error) = self.start(&id).await {
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
        Ok(())
    }

    pub async fn is_running(&self, subscription_id: &str) -> bool {
        self.children.lock().await.contains_key(subscription_id)
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
