// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Standalone subscription runner.
//!
//! The PG wire server in external-executor mode only writes desired state to
//! the metadata database. This process owns the subscriber children:
//!
//! * resume every enabled supervised subscription at startup;
//! * reconcile `starting` / `disabled` / `dropping` rows on a poll interval;
//! * hold `subscription.progress.owner` while a child runs;
//! * stop and release the lease on SIGTERM.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use lakesoul_metadata::MetaDataClientRef;
use tracing::{info, warn};

use super::runner::shutdown_signal;
use super::supervisor::SubscriptionSupervisor;
use crate::Result;

/// The lease is longer than the default poll interval and tolerates a small
/// scheduling pause. A live runner renews it on every reconcile pass.
const LEASE_TTL: Duration = Duration::from_secs(30);

pub async fn run(
    meta: MetaDataClientRef,
    program: PathBuf,
    log_dir: PathBuf,
    runner_id: String,
    poll_interval: Duration,
) -> Result<()> {
    let supervisor = Arc::new(SubscriptionSupervisor::runner(
        meta,
        program,
        log_dir.clone(),
        runner_id.clone(),
        LEASE_TTL,
    ));
    {
        let metadata = supervisor.metadata().await;
        metadata.init_schema().await?;
    }
    info!(
        runner = %runner_id,
        log_dir = %log_dir.display(),
        poll_ms = poll_interval.as_millis(),
        "subscription runner started"
    );
    supervisor.resume_all().await?;

    let mut ticker = tokio::time::interval(poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(error) = supervisor.reconcile().await {
                    warn!(%error, "subscription runner reconcile failed");
                }
            }
            _ = shutdown_signal() => break,
        }
    }

    supervisor.stop_all().await;
    info!(runner = %runner_id, "subscription runner stopped");
    Ok(())
}
