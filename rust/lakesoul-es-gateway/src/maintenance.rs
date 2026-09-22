// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Deferred secondary-index maintenance.
//!
//! With `index_build = "deferred"` writes only commit data; this task builds
//! the pending index shards on a fixed interval, so write latency no longer
//! includes index construction.  Searches keep read-your-writes visibility by
//! scanning the pending data files (see `search.rs`).

use std::sync::Arc;
use std::time::Duration;

use lakesoul_metadata::MetaDataClient;
use tokio::time::MissedTickBehavior;

/// Enable deferred maintenance and start the background build loop on its own
/// thread.
///
/// The loop owns a fresh metadata client instead of borrowing the gateway
/// state: the DataFusion write/read futures are not `Send`, and the
/// maintenance path does not need any per-request state.
pub fn spawn(namespace: String, tables: Vec<String>, interval: Duration) {
    if tables.is_empty() {
        return;
    }
    std::thread::Builder::new()
        .name("lakesoul-index-maintenance".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::error!("index maintenance runtime: {error}");
                    return;
                }
            };
            runtime.block_on(async move {
                let client = match MetaDataClient::from_env().await {
                    Ok(client) => Arc::new(client),
                    Err(error) => {
                        tracing::error!("index maintenance metadata client: {error}");
                        return;
                    }
                };
                let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
                ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    // The first tick fires immediately: this is also the
                    // catch-up run after a restart, when previous writes were
                    // never indexed.
                    ticker.tick().await;
                    for table in &tables {
                        match lakesoul_datafusion::index_maintenance::build_pending_indices(
                            Arc::clone(&client),
                            table,
                            &namespace,
                        )
                        .await
                        {
                            Ok(0) => {}
                            Ok(built) => tracing::info!(
                                table,
                                built,
                                "deferred index maintenance"
                            ),
                            Err(error) => tracing::warn!(
                                table,
                                "deferred index maintenance failed: {error}"
                            ),
                        }
                    }
                }
            });
        })
        .expect("spawn index maintenance thread");
}
