// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! IVM entry contracts against the real PostgreSQL server.

mod support;

use anyhow::{Context, Result, ensure};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ivm_partitioned_target_coexists_with_batch_partitions() -> Result<()> {
    support::run_ivm_suite(|h| {
        Box::pin(async move {
            // The source only produces d1; d2 is written by a normal batch
            // insert and must survive IVM refreshes and rebuilds.
            h.lake
                .batch_execute(
                    "CREATE TABLE ivm_part_source (\
                       day TEXT NOT NULL, id BIGINT NOT NULL PRIMARY KEY, \
                       val BIGINT)",
                )
                .await?;
            h.lake
                .batch_execute(
                    "INSERT INTO ivm_part_source (day, id, val) \
                     VALUES ('d1', 1, 10), ('d1', 2, 20), ('d3', 3, 30)",
                )
                .await?;
            h.lake
                .batch_execute(
                    "CREATE TABLE ivm_part_target (\
                       day TEXT NOT NULL, k BIGINT NOT NULL, v BIGINT, \
                       \"rowKinds\" TEXT NOT NULL, PRIMARY KEY (k)) \
                     PARTITIONED BY (day) WITH (use_cdc = true, \
                       lakesoul_cdc_change_column = 'rowKinds', \
                       hash_bucket_num = 1)",
                )
                .await?;
            // A batch writer owns d2 before IVM ever touches the table.
            h.lake
                .batch_execute(
                    "INSERT INTO ivm_part_target (day, k, v, \"rowKinds\") \
                     VALUES ('d2', 20, 200, 'insert')",
                )
                .await?;
            let d2_before = partition_version(h, "ivm_part_target", "day=d2").await?;

            h.lake
                .batch_execute("SET lakesoul.ivm.enabled = on")
                .await?;
            h.lake
                .execute(
                    "INSERT INTO ivm_part_target \
                     SELECT day, id, val FROM ivm_part_source",
                    &[],
                )
                .await?;
            let rows = h
                .lake
                .query("SELECT day, k, v FROM ivm_part_target ORDER BY day, k", &[])
                .await?;
            ensure_eq(
                "partitioned bootstrap rows",
                rows.iter()
                    .map(|row| {
                        (
                            row.get::<_, String>(0),
                            row.get::<_, i64>(1),
                            row.get::<_, Option<i64>>(2),
                        )
                    })
                    .collect::<Vec<_>>(),
                vec![
                    ("d1".to_string(), 1, Some(10)),
                    ("d1".to_string(), 2, Some(20)),
                    ("d2".to_string(), 20, Some(200)),
                    ("d3".to_string(), 3, Some(30)),
                ],
            )?;
            ensure_eq(
                "d2 batch partition is untouched by the first refresh",
                partition_version(h, "ivm_part_target", "day=d2").await?,
                d2_before,
            )?;

            // A changed definition rebuilds only the IVM-owned d1 partition.
            h.lake
                .execute(
                    "INSERT INTO ivm_part_target \
                     SELECT day, id, val + 1 AS val FROM ivm_part_source",
                    &[],
                )
                .await?;
            let rows = h
                .lake
                .query("SELECT day, k, v FROM ivm_part_target ORDER BY day, k", &[])
                .await?;
            ensure_eq(
                "partitioned rebuild rows",
                rows.iter()
                    .map(|row| {
                        (
                            row.get::<_, String>(0),
                            row.get::<_, i64>(1),
                            row.get::<_, Option<i64>>(2),
                        )
                    })
                    .collect::<Vec<_>>(),
                vec![
                    ("d1".to_string(), 1, Some(11)),
                    ("d1".to_string(), 2, Some(21)),
                    ("d2".to_string(), 20, Some(200)),
                    ("d3".to_string(), 3, Some(31)),
                ],
            )?;
            ensure_eq(
                "d2 batch partition is untouched by the rebuild",
                partition_version(h, "ivm_part_target", "day=d2").await?,
                d2_before,
            )?;

            // A batch commit in the IVM-owned d1 partition is an overlap and
            // must be rejected before the next refresh.
            h.lake
                .batch_execute(
                    "INSERT INTO ivm_part_target (day, k, v, \"rowKinds\") \
                     VALUES ('d1', 4, 40, 'insert')",
                )
                .await?;
            let error = h
                .lake
                .execute(
                    "INSERT INTO ivm_part_target \
                     SELECT day, id, val + 1 AS val FROM ivm_part_source",
                    &[],
                )
                .await
                .expect_err("IVM must reject an external commit in its partition");
            let message = format!("{error:?}");
            ensure!(
                message.contains("not written by this IVM view"),
                "unexpected partition conflict error: {message}"
            );
            Ok(())
        })
    })
    .await
}

/// The latest metadata version of one target partition.
async fn partition_version(
    h: &support::Harness,
    table: &str,
    partition_desc: &str,
) -> Result<i64> {
    let info = h
        .meta()
        .get_table_info_by_table_name(table, &h.lake_namespace)
        .await?
        .with_context(|| format!("{table} metadata"))?;
    let mut latest = None;
    for partition in h.meta().get_all_partition_info(&info.table_id).await? {
        if partition.partition_desc == partition_desc {
            latest = Some(
                latest
                    .map(|version: i64| version.max(i64::from(partition.version)))
                    .unwrap_or(i64::from(partition.version)),
            );
        }
    }
    latest.with_context(|| format!("{table} has no partition {partition_desc}"))
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
