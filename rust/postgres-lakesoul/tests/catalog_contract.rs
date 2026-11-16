// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#[path = "catalog_contract/common.rs"]
mod common;
#[path = "catalog_contract/isolation.rs"]
mod isolation;
#[path = "catalog_contract/mapping.rs"]
mod mapping;
pub mod support;

use anyhow::Result;
use tokio_postgres::types::Type;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catalog_contract_phase_three_two() -> Result<()> {
    support::run_catalog_suite(|h| {
        Box::pin(async move {
            if std::env::var("LAKESOUL_SQL_COMPAT_FAULT").as_deref() == Ok("expectation")
            {
                // Controlled negative acceptance: the live primary connection must
                // never claim the peer database identity. Keep this strictly wrong.
                common::assert_query(
                    &h.lake,
                    "catalog-injected-wrong-database",
                    "intentional wrong expected database identity",
                    "SELECT current_database()::text AS database_name",
                    &[("database_name", Type::TEXT)],
                    &common::rows(&[&[Some(h.catalog_peer_namespace()?)]]),
                    true,
                )
                .await?;
            }
            mapping::run(h).await?;
            isolation::run(h).await
        })
    })
    .await
}
