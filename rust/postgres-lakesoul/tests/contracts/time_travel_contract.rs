// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! SQL time travel contracts against the real server.
//!
//! `lakesoul_table_scan(table, mode, value)` must resolve each of the three
//! modes to the partition versions that were current at that point, while a
//! plain scan still sees the latest commit. Snapshot and tag rows are created
//! directly in the metadata database because this server deliberately has no
//! SQL DDL for them yet.

use crate::support;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::support::Harness;
use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use lakesoul_metadata::{PRIMARY_URL_ENV_KEY, PRIMARY_URL_PROP_KEY, pg_config_from_env};
use tokio_postgres::{Client, NoTls, config::Config};

const TABLE: &str = "tt_time_travel";
const TAG: &str = "v1";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn time_travel_contract() -> Result<()> {
    support::run_suite(|h| Box::pin(async move { scenarios(h).await })).await
}

async fn scenarios(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(&format!(
            "CREATE TABLE {TABLE} (id BIGINT PRIMARY KEY, val BIGINT)"
        ))
        .await
        .context("CREATE TABLE")?;

    // First commit: ids 1 and 2.
    h.lake
        .batch_execute(&format!(
            "INSERT INTO {TABLE} SELECT i, i * 10 FROM generate_series(1, 2) AS t(i)"
        ))
        .await
        .context("INSERT first commit")?;
    ensure!(
        ids(h, &format!("SELECT id FROM {TABLE} ORDER BY id")).await? == vec![1, 2],
        "the first commit must hold ids 1 and 2"
    );

    // The as-of instant sits after the first commit and before the second.
    // One second of slack keeps the two commits in distinct milliseconds even
    // with clock jitter between the test process and PostgreSQL.
    let as_of_ms = now_ms() + 100;
    tokio::time::sleep(Duration::from_secs(1)).await;

    let meta = metadata_client().await?;
    let table_id = resolve_table_id(&meta, &h.lake_namespace, TABLE).await?;
    let snapshot_id = create_snapshot(&meta, &table_id, "after first commit").await?;
    create_tag(&meta, &table_id, TAG, snapshot_id).await?;

    // Second commit: ids 3 and 4.
    h.lake
        .batch_execute(&format!(
            "INSERT INTO {TABLE} SELECT i, i * 10 FROM generate_series(3, 4) AS t(i)"
        ))
        .await
        .context("INSERT second commit")?;
    ensure!(
        ids(h, &format!("SELECT id FROM {TABLE} ORDER BY id")).await? == vec![1, 2, 3, 4],
        "a plain scan must see both commits"
    );

    let as_of = DateTime::<Utc>::from_timestamp_millis(as_of_ms)
        .context("build an RFC3339 timestamp")?
        .to_rfc3339();
    let expected = vec![1, 2];

    let timestamp_rows = ids(
        h,
        &format!("SELECT id FROM lakesoul_table_scan('{TABLE}', 'timestamp', '{as_of}') ORDER BY id"),
    )
    .await
    .context("timestamp pinned scan")?;
    ensure!(
        timestamp_rows == expected,
        "timestamp must read only the first commit, got {timestamp_rows:?} (as_of={as_of})"
    );

    let tag_rows = ids(
        h,
        &format!(
            "SELECT id FROM lakesoul_table_scan('{TABLE}', 'tag', '{TAG}') ORDER BY id"
        ),
    )
    .await
    .context("tag pinned scan")?;
    ensure!(
        tag_rows == expected,
        "tag must read the snapshot taken after the first commit, got {tag_rows:?}"
    );

    let snapshot_rows = ids(
        h,
        &format!(
            "SELECT id FROM lakesoul_table_scan('{TABLE}', 'snapshot_id', '{snapshot_id}') \
             ORDER BY id"
        ),
    )
    .await
    .context("snapshot_id pinned scan")?;
    ensure!(
        snapshot_rows == expected,
        "snapshot_id must read the first commit, got {snapshot_rows:?}"
    );

    // A version alias and a filter on the pinned scan must both work.
    let version_rows = ids(
        h,
        &format!(
            "SELECT id FROM lakesoul_table_scan('{TABLE}', 'version', '{snapshot_id}') \
             WHERE val > 10 ORDER BY id"
        ),
    )
    .await
    .context("version pinned scan with a filter")?;
    ensure!(
        version_rows == vec![2],
        "a WHERE clause must apply to the pinned rows, got {version_rows:?}"
    );

    // Invalid arguments are reported at planning time, with the function name.
    let error = h
        .lake
        .query_opt(
            &format!("SELECT id FROM lakesoul_table_scan('{TABLE}', 'bogus', 'x')"),
            &[],
        )
        .await
        .expect_err("an unknown mode must be refused");
    let message = error
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| error.to_string());
    ensure!(
        message.contains("lakesoul_table_scan"),
        "the error must name the function: {message}"
    );

    eprintln!(
        "PASS case=time-travel modes=timestamp,tag,snapshot_id,version rows={:?}",
        expected
    );
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

async fn metadata_client() -> Result<Client> {
    let dsn = pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY)
        .context("read the metadata connection string")?;
    let config: Config = dsn.parse().context("parse the metadata DSN")?;
    let (client, connection) = config
        .connect(NoTls)
        .await
        .context("connect to the metadata database")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

async fn resolve_table_id(meta: &Client, namespace: &str, name: &str) -> Result<String> {
    let row = meta
        .query_opt(
            "select table_id from table_name_id where table_namespace = $1 and table_name = $2",
            &[&namespace, &name],
        )
        .await
        .context("resolve the table id")?
        .with_context(|| format!("no metadata id for {namespace}.{name}"))?;
    row.try_get::<_, String>(0).context("read the table id")
}

/// Freeze the current partition versions into a snapshot, mirroring the
/// `CreateSnapshot` statement this server's metadata layer uses.
async fn create_snapshot(
    meta: &Client,
    table_id: &str,
    description: &str,
) -> Result<i64> {
    let row = meta
        .query_one(
            "with new_snapshot as (
                 insert into table_snapshot(table_id, created_at, description)
                 values ($1::TEXT, (extract(epoch from now()) * 1000)::BIGINT, $2::TEXT)
                 returning table_id, snapshot_id, created_at, description
             ),
             latest as (
                 select distinct on (partition_desc) partition_desc, version, snapshot
                 from partition_info
                 where table_id = $1::TEXT
                 order by partition_desc, version desc
             ),
             inserted as (
                 insert into snapshot_commit(table_id, snapshot_id, partition_desc, version, commit_id)
                 select distinct ns.table_id, ns.snapshot_id, l.partition_desc, l.version, c.commit_id
                 from new_snapshot ns, latest l, unnest(l.snapshot) as c(commit_id)
                 returning commit_id, partition_desc, version
             )
             select snapshot_id from new_snapshot",
            &[&table_id, &description],
        )
        .await
        .context("create a snapshot")?;
    row.try_get::<_, i64>(0).context("read the snapshot id")
}

async fn create_tag(
    meta: &Client,
    table_id: &str,
    tag: &str,
    snapshot_id: i64,
) -> Result<()> {
    meta.execute(
        "insert into table_snapshot_tag(table_id, tag, snapshot_id, created_at)
         values ($1::TEXT, $2::TEXT, $3::BIGINT, (extract(epoch from now()) * 1000)::BIGINT)",
        &[&table_id, &tag, &snapshot_id],
    )
    .await
    .context("create a tag")?;
    Ok(())
}
