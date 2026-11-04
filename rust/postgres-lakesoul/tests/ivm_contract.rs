// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! IVM entry contracts against the real PostgreSQL server.

mod support;

use anyhow::{Result, ensure};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivm_insert_select_bootstraps_and_refreshes() -> Result<()> {
    support::run_ivm_suite(|h| {
        Box::pin(async move {
            let namespace = h.lake_namespace.clone();
            h.lake
                .batch_execute("SET lakesoul.ivm.enabled = on")
                .await?;
            let first = h
                .lake
                .execute("INSERT INTO ivm_target SELECT id, val FROM pk_items", &[])
                .await?;
            ensure!(first == 0, "IVM INSERT must report no ordinary rows");

            let rows = h
                .lake
                .query("SELECT k, v FROM ivm_target ORDER BY k", &[])
                .await?;
            ensure_eq(
                "bootstrap rows",
                rows.iter()
                    .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
                    .collect::<Vec<_>>(),
                vec![(1, Some(99)), (2, Some(20)), (3, Some(30))],
            )?;

            h.lake
                .execute(
                    "INSERT INTO pk_items (id, val, label) VALUES (1, 999, 'new') \
                     ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, \
                     label = EXCLUDED.label",
                    &[],
                )
                .await?;
            let second = h
                .lake
                .execute("INSERT INTO ivm_target SELECT id, val FROM pk_items", &[])
                .await?;
            ensure_eq("second IVM INSERT tag", second, 0)?;

            let rows = h
                .lake
                .query("SELECT k, v FROM ivm_target ORDER BY k", &[])
                .await?;
            ensure_eq(
                "incremental rows",
                rows.iter()
                    .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
                    .collect::<Vec<_>>(),
                vec![(1, Some(999)), (2, Some(20)), (3, Some(30))],
            )?;

            // A changed definition rebuilds automatically, and the rebuild
            // publishes a single populated version: no empty truncate version
            // is visible to a reader.
            h.lake
                .execute(
                    "INSERT INTO ivm_target SELECT id, val + 1 AS val FROM pk_items",
                    &[],
                )
                .await?;
            let rows = h
                .lake
                .query("SELECT k, v FROM ivm_target ORDER BY k", &[])
                .await?;
            ensure_eq(
                "rebuilt rows",
                rows.iter()
                    .map(|row| (row.get::<_, i64>(0), row.get::<_, Option<i64>>(1)))
                    .collect::<Vec<_>>(),
                vec![(1, Some(1000)), (2, Some(21)), (3, Some(31))],
            )?;
            let info = h
                .meta()
                .get_table_info_by_table_name("ivm_target", &namespace)
                .await?
                .expect("ivm_target metadata");
            let versions = h.meta().get_all_partition_info(&info.table_id).await?;
            ensure!(
                versions.iter().all(|partition| !partition.snapshot.is_empty()),
                "an atomic rebuild must not publish an empty target version: {:?}",
                versions
                    .iter()
                    .map(|partition| (partition.version, partition.snapshot.len()))
                    .collect::<Vec<_>>()
            );

            // A data commit written by another engine is detected before IVM
            // touches the target; the user must resolve the overlap.
            support::management_execute(
                h.management(),
                &format!(
                    "INSERT INTO lakesoul.{namespace}.ivm_target                      SELECT 5, 50, 'insert', 0"
                ),
            )
            .await?;
            let error = h
                .lake
                .execute("INSERT INTO ivm_target SELECT id, val FROM pk_items", &[])
                .await
                .expect_err("IVM must reject an external target commit");
            let message = format!("{error:?}");
            ensure!(
                message.contains("not written by this IVM view"),
                "unexpected conflict error: {message}"
            );

            // With the switch off the same target still accepts ordinary
            // inserts; they are rejected only when IVM would overwrite them.
            h.lake
                .batch_execute("SET lakesoul.ivm.enabled = off")
                .await?;
            h.lake
                .execute(
                    "INSERT INTO empty_items (id, val, label) VALUES (7, 70, 'batch')",
                    &[],
                )
                .await?;
            let count = h
                .lake
                .query_one("SELECT count(*) FROM empty_items", &[])
                .await?
                .get::<_, i64>(0);
            ensure_eq("normal insert after switch off", count, 1)?;
            Ok(())
        })
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivm_adds_missing_epoch_column() -> Result<()> {
    support::run_ivm_suite(|h| {
        Box::pin(async move {
            h.lake
                .batch_execute("SET lakesoul.ivm.enabled = on")
                .await?;
            h.lake
                .execute(
                    "INSERT INTO ivm_target_no_epoch SELECT id, val FROM pk_items",
                    &[],
                )
                .await?;
            let rows = h
                .lake
                .query(
                    "SELECT k, v, __ivm_epoch FROM ivm_target_no_epoch ORDER BY k",
                    &[],
                )
                .await?;
            ensure_eq("epoch rows", rows.len(), 3)?;
            ensure!(
                rows.iter()
                    .all(|row| row.get::<_, Option<i64>>(2).is_some()),
                "the rebuild must materialize __ivm_epoch"
            );
            Ok(())
        })
    })
    .await
}

fn ensure_eq<T: std::fmt::Debug + PartialEq>(
    label: &str,
    actual: T,
    expected: T,
) -> Result<()> {
    ensure!(
        actual == expected,
        "{label}: actual {actual:?}, expected {expected:?}"
    );
    Ok(())
}
