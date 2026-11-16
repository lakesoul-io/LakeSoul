// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#[path = "storage_contract/cdc.rs"]
mod cdc;
#[path = "storage_contract/common.rs"]
mod common;
#[path = "storage_contract/merge.rs"]
mod merge;
#[path = "storage_contract/partition.rs"]
mod partition;
pub mod support;

use anyhow::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_contract_phase_three_one() -> Result<()> {
    support::run_storage_suite(|h| {
        Box::pin(async move {
            merge::run(h).await?;
            partition::run(h).await?;
            cdc::run(h).await
        })
    })
    .await
}
