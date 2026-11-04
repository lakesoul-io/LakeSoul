// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#[path = "vector_contract/codecs.rs"]
mod codecs;
#[path = "vector_contract/common.rs"]
mod common;
#[path = "vector_contract/search.rs"]
mod search;
pub mod support;

use anyhow::Result;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_contract_phase_three_three() -> Result<()> {
    support::run_vector_suite(|h| {
        Box::pin(async move {
            if std::env::var("LAKESOUL_SQL_COMPAT_FAULT").as_deref() == Ok("expectation")
            {
                // Deliberately wrong, after the index and server both exist:
                // failure must still remove committed vector metadata/files.
                common::assert_ids(
                    &h.lake,
                    "vector-injected-wrong-id",
                    "SELECT id FROM public.vector_values WHERE id = 1",
                    &[999],
                )
                .await?;
            }
            codecs::run(h).await?;
            search::run(h).await
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vector_probes_show_describe_matches_query() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            use anyhow::ensure;
            use tokio_postgres::types::Type;

            let sql = "SHOW ivfflat.probes";
            let simple = support::compare::simple(&h.lake, sql).await?;
            let statement = h.lake.prepare(sql).await?;
            let actual_names: Vec<_> = statement
                .columns()
                .iter()
                .map(|column| column.name())
                .collect();
            ensure!(
                actual_names == simple.names
                    && statement.columns().len() == 1
                    && *statement.columns()[0].type_() == Type::TEXT,
                "SHOW Describe must match execution header ivfflat.probes/TEXT; \
                 simple_names={:?}, describe={:?}",
                simple.names,
                statement.columns()
            );
            Ok(())
        })
    })
    .await
}
