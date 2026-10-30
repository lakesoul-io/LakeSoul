// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Phase-two connection/session contracts against the real server.
//!
//! The shared harness starts a fresh `postgres-lakesoul` child and a matching
//! PostgreSQL reference schema. Cases are ordered deliberately:
//!
//! 1. session state (time zone, failed transactions, isolation),
//! 2. read-only refusals and parameter-error boundaries,
//! 3. `visibility::run` last, because it mutates the `pk_items` fixture
//!    through the management session to measure commit publication.
//!
//! Every case leaves both clients idle, out of any transaction, and restored to
//! the harness's UTC session settings.

#[path = "session_contract/common.rs"]
mod common;
#[path = "session_contract/readonly.rs"]
mod readonly;
#[path = "session_contract/state.rs"]
mod state;
pub mod support;
#[path = "session_contract/visibility.rs"]
mod visibility;

use anyhow::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_contract_phase_two() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            state::run(h).await?;
            readonly::run(h).await?;
            // Mutates pk_items through the management session; must run last.
            visibility::run(h).await?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Known SET TIME ZONE does not apply execution settings; strict PG reproduction (see README)"]
async fn time_zone_changes_query_and_is_connection_local() -> Result<()> {
    support::run_suite(|h| Box::pin(state::time_zone_is_connection_local(h))).await
}
