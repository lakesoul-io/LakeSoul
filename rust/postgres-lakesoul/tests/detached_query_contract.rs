// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Detached-query contracts against the real server.
//!
//! Every case runs through the PostgreSQL wire protocol against a fresh
//! `postgres-lakesoul` child. The point of a detached query is that the
//! submitting connection is not the execution's owner, so the first case
//! submits from one connection, drops it, and only then polls from another.

pub mod support;

use std::time::Duration;

use anyhow::{Result, ensure};
use tokio_postgres::Client;

use support::Harness;
use uuid::Uuid;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_query_contract() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            let suite = Uuid::new_v4().simple().to_string();
            select_survives_the_submitting_connection(h, &suite).await?;
            select_result_pages(h, &suite).await?;
            multi_statement_sets_and_result_sets(h, &suite).await?;
            insert_job_writes_the_target(h, &suite).await?;
            upsert_job_updates_the_target(h, &suite).await?;
            duplicate_job_key_returns_the_same_job(h, &suite).await?;
            cancel_stops_a_streaming_job(h, &suite).await?;
            Ok(())
        })
    })
    .await
}

/// A long aggregation keeps running after the client that submitted it closes.
async fn select_survives_the_submitting_connection(
    h: &Harness,
    suite: &str,
) -> Result<()> {
    let query_id = {
        let client = h.connect_lake().await?;
        submit(
            &client,
            "SELECT count(*) AS n FROM generate_series(1, 5000000) a \
             CROSS JOIN generate_series(1, 10) b",
            &format!("{suite}-disconnect"),
        )
        .await?
    };
    // The submit connection is gone here; the job is owned by the server.
    ensure_eq(&wait(h, &query_id, 120_000).await?, "succeeded")?;
    let row = h
        .lake
        .query_one(
            &format!("SELECT n FROM lakesoul_query_result('{query_id}')"),
            &[],
        )
        .await?;
    let n: i64 = row.get(0);
    ensure!(n == 50_000_000, "detached aggregate returned {n}");
    Ok(())
}

/// The spool is a table: projection, offset and limit work through SQL.
async fn select_result_pages(h: &Harness, suite: &str) -> Result<()> {
    let query_id = submit(
        &h.lake,
        "SELECT value FROM generate_series(1, 10) ORDER BY value",
        &format!("{suite}-pages"),
    )
    .await?;
    ensure_eq(&wait(h, &query_id, 30_000).await?, "succeeded")?;

    let all = h
        .lake
        .query(
            &format!("SELECT value FROM lakesoul_query_result('{query_id}')"),
            &[],
        )
        .await?;
    ensure_eq_usize(all.len(), 10)?;

    let page = h
        .lake
        .query(
            &format!(
                "SELECT value FROM lakesoul_query_result('{query_id}', 5, 3) \
                 ORDER BY value"
            ),
            &[],
        )
        .await?;
    ensure_eq_usize(page.len(), 3)?;
    let first: i64 = page[0].get(0);
    ensure_eq_i64(first, 6)?;
    Ok(())
}

/// A script shares one session: `SET` affects later statements, and each
/// SELECT gets its own result set.
async fn multi_statement_sets_and_result_sets(h: &Harness, suite: &str) -> Result<()> {
    let query_id = submit(
        &h.lake,
        "SET search_path = public;          SET statement_timeout = '10s';          SELECT value FROM generate_series(1, 3) ORDER BY value;          SET time zone 'UTC';          SELECT value FROM generate_series(10, 12) ORDER BY value",
        &format!("{suite}-multi"),
    )
    .await?;
    ensure_eq(&wait(h, &query_id, 30_000).await?, "succeeded")?;

    let sets: i64 = h
        .lake
        .query_one(
            &format!("SELECT count(*) FROM lakesoul_query_result_sets('{query_id}')"),
            &[],
        )
        .await?
        .get(0);
    ensure_eq_i64(sets, 2)?;

    let first = h
        .lake
        .query(
            &format!(
                "SELECT value FROM lakesoul_query_result_set('{query_id}', 0)                  ORDER BY value"
            ),
            &[],
        )
        .await?;
    ensure_eq_usize(first.len(), 3)?;
    let first_value: i64 = first[0].get(0);
    ensure_eq_i64(first_value, 1)?;

    let last = h
        .lake
        .query(
            &format!(
                "SELECT value FROM lakesoul_query_result('{query_id}') ORDER BY value"
            ),
            &[],
        )
        .await?;
    ensure_eq_usize(last.len(), 3)?;
    let last_value: i64 = last[0].get(0);
    ensure_eq_i64(last_value, 10)?;
    Ok(())
}

/// `INSERT ... SELECT` is the ETL shape: the target table is the result.
async fn insert_job_writes_the_target(h: &Harness, suite: &str) -> Result<()> {
    let before: i64 = h
        .lake
        .query_one("SELECT count(*) FROM empty_items", &[])
        .await?
        .get(0);
    ensure_eq_i64(before, 0)?;

    let query_id = submit(
        &h.lake,
        "INSERT INTO empty_items SELECT id, val, label FROM items",
        &format!("{suite}-insert"),
    )
    .await?;
    ensure_eq(&wait(h, &query_id, 30_000).await?, "succeeded")?;

    let after: i64 = h
        .lake
        .query_one("SELECT count(*) FROM empty_items", &[])
        .await?
        .get(0);
    ensure_eq_i64(after, 4)?;

    let row = h
        .lake
        .query_one(
            &format!(
                "SELECT command_tag, result_kind FROM lakesoul_query_status('{query_id}')"
            ),
            &[],
        )
        .await?;
    let tag: Option<String> = row.get(0);
    let kind: Option<String> = row.get(1);
    ensure!(
        tag.as_deref().is_some_and(|tag| tag.starts_with("INSERT")),
        "unexpected command tag {tag:?}"
    );
    ensure_eq(kind.as_deref(), Some("table"))?;
    Ok(())
}

/// A detached job runs the upsert path: the plain write hook only rejects
/// `DO UPDATE`, so the job entry point must route it through the planned
/// upsert instead.
async fn upsert_job_updates_the_target(h: &Harness, suite: &str) -> Result<()> {
    let query_id = submit(
        &h.lake,
        "INSERT INTO pk_items (id, val, label) VALUES (1, 100, 'job') \
         ON CONFLICT (id) DO UPDATE \
           SET val = EXCLUDED.val, label = EXCLUDED.label",
        &format!("{suite}-upsert"),
    )
    .await?;
    ensure_eq(&wait(h, &query_id, 30_000).await?, "succeeded")?;

    let row = h
        .lake
        .query_one("SELECT val, label FROM pk_items WHERE id = 1", &[])
        .await?;
    let val: i64 = row.get(0);
    let label: Option<String> = row.get(1);
    ensure_eq_i64(val, 100)?;
    ensure_eq(label.as_deref(), Some("job"))?;

    let row = h
        .lake
        .query_one(
            &format!("SELECT command_tag FROM lakesoul_query_status('{query_id}')"),
            &[],
        )
        .await?;
    let tag: Option<String> = row.get(0);
    ensure!(
        tag.as_deref().is_some_and(|tag| tag.starts_with("INSERT")),
        "unexpected command tag {tag:?}"
    );
    Ok(())
}

/// The same job key is one job, not two executions.
async fn duplicate_job_key_returns_the_same_job(h: &Harness, suite: &str) -> Result<()> {
    let key = format!("{suite}-idem");
    let first = submit(&h.lake, "SELECT count(*) AS n FROM items", &key).await?;
    let second = submit(&h.lake, "SELECT count(*) AS n FROM items", &key).await?;
    ensure_eq(&first, &second)?;
    ensure_eq(&wait(h, &first, 30_000).await?, "succeeded")?;
    let count: i64 = h
        .lake
        .query_one(
            &format!("SELECT count(*) FROM lakesoul_queries() WHERE job_key = '{key}'"),
            &[],
        )
        .await?
        .get(0);
    ensure_eq_i64(count, 1)?;
    Ok(())
}

/// Cancellation stops a streaming job and records PostgreSQL's `57014`.
async fn cancel_stops_a_streaming_job(h: &Harness, suite: &str) -> Result<()> {
    let query_id = submit(
        &h.lake,
        "SELECT * FROM generate_series(1, 100000000)",
        &format!("{suite}-cancel"),
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let cancelled: String = h
        .lake
        .query_one(&format!("SELECT lakesoul_cancel_query('{query_id}')"), &[])
        .await?
        .get(0);
    ensure_eq(&cancelled, "t")?;
    ensure_eq(&wait(h, &query_id, 60_000).await?, "canceled")?;
    let sqlstate: Option<String> = h
        .lake
        .query_one(
            &format!("SELECT error_sqlstate FROM lakesoul_query_status('{query_id}')"),
            &[],
        )
        .await?
        .get(0);
    ensure_eq(sqlstate.as_deref(), Some("57014"))?;
    Ok(())
}

async fn submit(client: &Client, sql: &str, key: &str) -> Result<String> {
    let row = client
        .query_one(
            &format!(
                "SELECT lakesoul_submit($lakesoul_job${sql}$lakesoul_job$, '{key}')"
            ),
            &[],
        )
        .await?;
    Ok(row.get(0))
}

async fn wait(h: &Harness, query_id: &str, timeout_ms: i64) -> Result<String> {
    let row = h
        .lake
        .query_one(
            &format!("SELECT lakesoul_wait_query('{query_id}', {timeout_ms})"),
            &[],
        )
        .await?;
    Ok(row.get(0))
}

fn ensure_eq<T, U>(actual: T, expected: U) -> Result<()>
where
    T: PartialEq<U> + std::fmt::Debug,
    U: std::fmt::Debug,
{
    ensure!(actual == expected, "expected {expected:?}, got {actual:?}");
    Ok(())
}

fn ensure_eq_usize(actual: usize, expected: usize) -> Result<()> {
    ensure!(actual == expected, "expected {expected} rows, got {actual}");
    Ok(())
}

fn ensure_eq_i64(actual: i64, expected: i64) -> Result<()> {
    ensure!(actual == expected, "expected {expected}, got {actual}");
    Ok(())
}

/// The result spool is read on the coordinator while the job's SELECT can be
/// planned onto real Flight workers. This pins that the local result scan does
/// not get shipped into a distributed stage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detached_query_result_with_distributed_workers() -> Result<()> {
    support::run_distributed_insert_suite(|h| {
        Box::pin(async move {
            let key = format!("{}-distributed", Uuid::new_v4().simple());
            let query_id =
                submit(&h.lake, "SELECT count(*) AS n FROM items", &key).await?;
            ensure_eq(&wait(h, &query_id, 60_000).await?, "succeeded")?;
            let n: i64 = h
                .lake
                .query_one(
                    &format!("SELECT n FROM lakesoul_query_result('{query_id}')"),
                    &[],
                )
                .await?
                .get(0);
            ensure_eq_i64(n, 4)?;
            Ok(())
        })
    })
    .await
}
