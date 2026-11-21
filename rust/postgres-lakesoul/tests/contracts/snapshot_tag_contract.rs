// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! SQL contracts for snapshot/tag management.
//!
//! Creation freezes a snapshot and tags it in one statement; dropping a tag
//! cascades to its snapshot only when nothing else references it. `SHOW TAGS`
//! returns all-text columns so a human can read it directly.

use crate::support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::support::Harness;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use lakesoul_metadata::MetaDataClient;

const TABLE: &str = "tt_snap_tag";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_tag_contract() -> Result<()> {
    support::run_suite(|h| Box::pin(async move { scenarios(h).await })).await
}

async fn scenarios(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(&format!(
            "CREATE TABLE {TABLE} (id BIGINT PRIMARY KEY, val BIGINT)"
        ))
        .await
        .context("CREATE TABLE")?;
    h.lake
        .batch_execute(&format!(
            "INSERT INTO {TABLE} SELECT i, i * 10 FROM generate_series(1, 2) AS t(i)"
        ))
        .await
        .context("first commit")?;

    let as_of_ms = now_ms() + 100;
    tokio::time::sleep(Duration::from_secs(1)).await;
    h.lake
        .batch_execute(&format!(
            "INSERT INTO {TABLE} SELECT i, i * 10 FROM generate_series(3, 4) AS t(i)"
        ))
        .await
        .context("second commit")?;
    ensure!(
        ids(h, &format!("SELECT id FROM {TABLE} ORDER BY id")).await? == vec![1, 2, 3, 4],
        "a plain scan must see both commits"
    );

    // Freeze the current state and name it in one statement.
    h.lake
        .batch_execute(&format!("ALTER TABLE {TABLE} CREATE TAG `v1`"))
        .await
        .context("CREATE TAG v1")?;

    let shown = show_tags(h).await?;
    ensure!(
        shown.len() == 1 && shown[0][0] == "v1",
        "SHOW TAGS must list v1, got {shown:?}"
    );
    let snapshot_id: i64 = shown[0][1]
        .parse()
        .context("the version column must be text holding an integer")?;
    // All five columns are text (SHOW TAGS is a human-readable view).
    ensure!(
        shown[0][2].contains('T') && shown[0][3].contains('T'),
        "timestamps must be RFC3339 text: {:?}",
        shown[0]
    );
    let pinned = ids(
        h,
        &format!(
            "SELECT id FROM lakesoul_table_scan('{TABLE}', 'tag', 'v1') ORDER BY id"
        ),
    )
    .await
    .context("read by tag")?;
    ensure!(pinned == vec![1, 2, 3, 4], "v1 must pin both commits");

    // A second tag on the same version. Dropping one keeps the shared version.
    h.lake
        .batch_execute(&format!(
            "ALTER TABLE {TABLE} CREATE TAG v2 AS OF VERSION {snapshot_id}"
        ))
        .await
        .context("CREATE TAG v2 by version")?;
    ensure!(
        show_tags(h).await?.len() == 2,
        "v1 and v2 must both be listed"
    );

    h.lake
        .batch_execute(&format!("ALTER TABLE {TABLE} DROP TAG v1"))
        .await
        .context("DROP TAG v1")?;
    ensure!(
        show_tags(h).await?.len() == 1,
        "only v2 must remain after dropping v1"
    );
    let still_readable = ids(
        h,
        &format!(
            "SELECT id FROM lakesoul_table_scan('{TABLE}', 'tag', 'v2') ORDER BY id"
        ),
    )
    .await
    .context("read by the surviving tag")?;
    ensure!(
        still_readable == vec![1, 2, 3, 4],
        "the shared snapshot must survive the first DROP TAG"
    );

    // Dropping the last tag cascades to its snapshot.
    h.lake
        .batch_execute(&format!("ALTER TABLE {TABLE} DROP TAG v2"))
        .await
        .context("DROP TAG v2")?;
    ensure!(show_tags(h).await?.is_empty(), "no tags must remain");
    let meta = MetaDataClient::from_env()
        .await
        .context("metadata client")?;
    let name_id = meta
        .get_table_name_id_by_table_name(TABLE, &h.lake_namespace)
        .await
        .context("resolve the table id")?
        .context("the table must exist in metadata")?;
    ensure!(
        meta.list_snapshots(&name_id.table_id)
            .await
            .context("list snapshots")?
            .is_empty(),
        "the snapshot must be removed with its last tag"
    );

    // AS OF timestamp freezes the state committed at or before that instant.
    let as_of = DateTime::<Utc>::from_timestamp_millis(as_of_ms)
        .context("build RFC3339 timestamp")?
        .to_rfc3339();
    h.lake
        .batch_execute(&format!(
            "ALTER TABLE {TABLE} CREATE TAG early AS OF timestamp '{as_of}'"
        ))
        .await
        .context("CREATE TAG as of timestamp")?;
    let early = ids(
        h,
        &format!(
            "SELECT id FROM lakesoul_table_scan('{TABLE}', 'tag', 'early') ORDER BY id"
        ),
    )
    .await
    .context("read the as-of tag")?;
    ensure!(
        early == vec![1, 2],
        "AS OF timestamp must pin only the first commit, got {early:?}"
    );
    h.lake
        .batch_execute(&format!("ALTER TABLE {TABLE} DROP TAG early"))
        .await
        .context("DROP TAG early")?;

    // Snapshot versions are immutable: there is no standalone DROP.
    let error = h
        .lake
        .batch_execute(&format!("ALTER TABLE {TABLE} DROP SNAPSHOT {snapshot_id}"))
        .await
        .expect_err("DROP SNAPSHOT must be refused");
    let db = error
        .as_db_error()
        .context("a server error must carry a SQLSTATE")?;
    ensure!(db.code().code() == "0A000", "unexpected SQLSTATE: {db:?}");
    ensure!(
        db.message().contains("immutable"),
        "unexpected message: {}",
        db.message()
    );

    // Metadata writes are refused inside an explicit transaction.
    h.lake.batch_execute("BEGIN").await.context("BEGIN")?;
    let error = h
        .lake
        .batch_execute(&format!("ALTER TABLE {TABLE} CREATE TAG in_tx"))
        .await
        .expect_err("snapshot DDL in a transaction must be refused");
    ensure!(
        error
            .as_db_error()
            .is_some_and(|db| db.code().code() == "0A000"),
        "unexpected transaction error: {error}"
    );
    h.lake.batch_execute("ROLLBACK").await.context("ROLLBACK")?;

    eprintln!("PASS case=snapshot-tag ddl=create/drop/as-of show=text cascade=ok");
    Ok(())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("the system clock is after the Unix epoch")
        .as_millis() as i64
}

async fn ids(h: &Harness, sql: &str) -> Result<Vec<i64>> {
    let rows = h
        .lake
        .query(sql, &[])
        .await
        .with_context(|| format!("SQL={sql}"))?;
    rows.into_iter()
        .map(|row| row.try_get::<_, i64>(0).context("read an id"))
        .collect()
}

async fn show_tags(h: &Harness) -> Result<Vec<Vec<String>>> {
    let sql = format!("SHOW TAGS {TABLE}");
    let rows = h
        .lake
        .query(&sql, &[])
        .await
        .with_context(|| format!("SQL={sql}"))?;
    rows.into_iter()
        .map(|row| {
            (0..5)
                .map(|index| row.try_get::<_, String>(index))
                .collect::<std::result::Result<Vec<_>, _>>()
                .context("read a text column")
        })
        .collect()
}
