// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `SET lakesoul.read.time_travel` contracts against the real server.
//!
//! The session-wide setting pins every local table scan to the latest
//! partition version committed at or before an instant. It needs no snapshot
//! or tag; a table with no matching version is an error, not an empty result.
//! Writes stay available: the setting is a read view, and an explicit
//! `lakesoul_table_scan` pin still wins.

pub mod support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, SecondsFormat, Utc};
use support::Harness;

const TABLE: &str = "tt_read_snapshot";
const OTHER: &str = "tt_read_snapshot_other";
const LATE: &str = "tt_read_snapshot_late";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_snapshot_contract() -> Result<()> {
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

    // The late table is filled before the setting is applied: it has no
    // version at or before the pinned instant.
    h.lake
        .batch_execute(&format!("CREATE TABLE {LATE} (id BIGINT PRIMARY KEY)"))
        .await
        .context("CREATE TABLE late")?;
    h.lake
        .batch_execute(&format!("INSERT INTO {LATE} VALUES (1)"))
        .await
        .context("INSERT into the late table")?;

    let as_of = DateTime::<Utc>::from_timestamp_millis(as_of_ms)
        .context("build an RFC3339 timestamp")?
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    let later_as_of = DateTime::<Utc>::from_timestamp_millis(now_ms())
        .context("build the later RFC3339 timestamp")?
        .to_rfc3339_opts(SecondsFormat::Millis, true);
    set_time_travel(h, &as_of).await?;

    ensure!(
        ids(h, &format!("SELECT id FROM {TABLE} ORDER BY id")).await? == vec![1, 2],
        "every scan must see only the first commit"
    );

    // SHOW / current_setting / pg_settings all report the canonical value.
    let shown: String = h
        .lake
        .query_one("SHOW lakesoul.read.time_travel", &[])
        .await
        .context("SHOW")?
        .get(0);
    ensure!(
        shown == as_of,
        "SHOW returned {shown:?}, expected {as_of:?}"
    );
    let current: String = h
        .lake
        .query_one("SELECT current_setting('lakesoul.read.time_travel')", &[])
        .await
        .context("current_setting")?
        .get(0);
    ensure!(current == as_of, "current_setting returned {current:?}");
    let from_pg_settings: String = h
        .lake
        .query_one(
            "SELECT setting FROM pg_settings \
             WHERE name = 'lakesoul.read.time_travel'",
            &[],
        )
        .await
        .context("pg_settings")?
        .get(0);
    ensure!(
        from_pg_settings == as_of,
        "pg_settings returned {from_pg_settings:?}"
    );

    // Writes are allowed while the setting is active. A plain INSERT …
    // SELECT reads the pinned rows into another table.
    h.lake
        .batch_execute(&format!("CREATE TABLE {OTHER} (id BIGINT, val BIGINT)"))
        .await
        .context("CREATE TABLE other")?;
    h.lake
        .batch_execute(&format!("INSERT INTO {OTHER} SELECT id, val FROM {TABLE}"))
        .await
        .context("INSERT the pinned rows elsewhere")?;
    // The new table is itself newer than the instant, so the pinned view
    // excludes it until the setting is cleared.
    h.lake
        .batch_execute("RESET lakesoul.read.time_travel")
        .await
        .context("RESET after the write")?;
    ensure!(
        ids(h, &format!("SELECT id FROM {OTHER} ORDER BY id")).await? == vec![1, 2],
        "the new table must hold only the pinned rows"
    );
    set_time_travel(h, &as_of).await?;

    // The snapshot applies uniformly: an UPDATE target sees the same rows a
    // SELECT sees. Row 3 was written after the instant, so it is invisible;
    // row 1 is visible and updatable.
    let skipped = h
        .lake
        .execute(&format!("UPDATE {TABLE} SET val = 99 WHERE id = 3"), &[])
        .await
        .context("UPDATE a post-as-of row")?;
    ensure!(
        skipped == 0,
        "the UPDATE must not see post-as-of rows, actual {skipped}"
    );
    let updated = h
        .lake
        .execute(&format!("UPDATE {TABLE} SET val = 99 WHERE id = 1"), &[])
        .await
        .context("UPDATE a visible row")?;
    ensure!(updated == 1, "expected UPDATE 1, actual {updated}");

    // An explicit per-query pin wins over the session setting: this one is
    // later than the session instant, so it must see both commits.
    let explicit = ids(
        h,
        &format!(
            "SELECT id FROM lakesoul_table_scan('{TABLE}', 'timestamp', '{later_as_of}') \
             ORDER BY id"
        ),
    )
    .await
    .context("explicit timestamp pin")?;
    ensure!(
        explicit == vec![1, 2, 3, 4],
        "the explicit pin must override the session view, got {explicit:?}"
    );
    let missing_tag = ids(
        h,
        &format!("SELECT id FROM lakesoul_table_scan('{TABLE}', 'tag', 'no-such-tag')"),
    )
    .await;
    ensure!(missing_tag.is_err(), "a missing tag must still be an error");

    // RESET returns the session to the latest state.
    h.lake
        .batch_execute("RESET lakesoul.read.time_travel")
        .await
        .context("RESET")?;
    ensure!(
        ids(h, &format!("SELECT id FROM {TABLE} ORDER BY id")).await? == vec![1, 2, 3, 4],
        "RESET must restore the latest view"
    );
    let val: i64 = h
        .lake
        .query_one(&format!("SELECT val FROM {TABLE} WHERE id = 1"), &[])
        .await
        .context("read the updated row")?
        .get(0);
    ensure!(val == 99, "the UPDATE must have changed id 1, got {val}");
    let later_val: i64 = h
        .lake
        .query_one(&format!("SELECT val FROM {TABLE} WHERE id = 3"), &[])
        .await
        .context("read the post-as-of row")?
        .get(0);
    ensure!(later_val == 30, "id 3 must stay unchanged, got {later_val}");

    // DISCARD ALL clears it too.
    set_time_travel(h, &as_of).await?;
    h.lake
        .batch_execute("DISCARD ALL")
        .await
        .context("DISCARD ALL")?;
    ensure!(
        ids(h, &format!("SELECT id FROM {TABLE} ORDER BY id")).await? == vec![1, 2, 3, 4],
        "DISCARD ALL must restore the latest view"
    );

    // A table whose every version is newer than the instant is an error.
    set_time_travel(h, &as_of).await?;
    let error = h
        .lake
        .query(&format!("SELECT id FROM {LATE}"), &[])
        .await
        .expect_err("a table with no matching version must be an error");
    let message = error
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| error.to_string());
    ensure!(
        message.contains("no version committed at or before"),
        "unexpected error: {message}"
    );

    // Invalid values are refused with 22023.
    let error = h
        .lake
        .batch_execute("SET lakesoul.read.time_travel = 'yesterday'")
        .await
        .expect_err("a malformed timestamp must be refused");
    ensure!(
        error
            .as_db_error()
            .is_some_and(|db| db.code().code() == "22023"),
        "unexpected error: {error}"
    );
    let error = h
        .lake
        .batch_execute("SET LOCAL lakesoul.read.time_travel = '2026-11-20T10:00:00Z'")
        .await
        .expect_err("SET LOCAL must be refused");
    ensure!(
        error
            .as_db_error()
            .is_some_and(|db| db.code().code() == "0A000"),
        "unexpected SET LOCAL error: {error}"
    );

    eprintln!(
        "PASS case=read-snapshot as_of={as_of} write=ok explicit-pin=ok strict-error=ok"
    );
    Ok(())
}

async fn set_time_travel(h: &Harness, value: &str) -> Result<()> {
    h.lake
        .batch_execute(&format!("SET lakesoul.read.time_travel = '{value}'"))
        .await
        .with_context(|| format!("SET lakesoul.read.time_travel = '{value}'"))
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
