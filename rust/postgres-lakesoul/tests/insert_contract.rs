// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `INSERT` contracts against the real server.
//!
//! Every case runs through the PostgreSQL wire protocol against a fresh
//! `postgres-lakesoul` child, and the metadata rows a committed write must
//! produce are read from the metadata PostgreSQL database directly. Rejected
//! statements are proven to leave no row, no file and no metadata behind by
//! comparing the authoritative snapshot around the failure.
//!
//! The cases share one warehouse and run in order: a successful insert into a
//! plain table (metadata + files), a parameterized insert into a primary-key
//! table, then the conflict and refusal matrix, which must not change state.

pub mod support;

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::types::Type;

use support::Harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_contract() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            plain_insert_writes_rows_metadata_and_files(h).await?;
            parameterized_insert_executes_twice(h).await?;
            duplicate_key_leaves_no_state(h).await?;
            not_null_violation_is_23502(h).await?;
            on_conflict_do_nothing_skips_conflicts(h).await?;
            on_conflict_do_update_replaces_the_row(h).await?;
            multi_partition_insert_publishes_atomically(h).await?;
            refusal_matrix_changes_nothing(h).await?;
            Ok(())
        })
    })
    .await
}

/// `INSERT ... VALUES` commits rows, one `data_commit_info` row, one partition
/// version and parquet files that exist on disk.
async fn plain_insert_writes_rows_metadata_and_files(h: &Harness) -> Result<()> {
    let rows = simple_rows(
        h,
        "INSERT INTO empty_items (id, val, label) VALUES (1, 10, 'a'), (2, NULL, 'b')",
    )
    .await?;
    ensure!(
        rows == 2,
        "INSERT must report its written rows, actual tag rows {rows}"
    );
    ensure_eq(
        "empty_items rows",
        lake_i64(h, "SELECT count(*) FROM empty_items").await?,
        2,
    )?;
    ensure_eq(
        "empty_items first value",
        lake_text(h, "SELECT label FROM empty_items WHERE id = 1")
            .await?
            .as_deref(),
        Some("a"),
    )?;

    let table_id = table_id(h, "empty_items").await?;
    ensure_eq(
        "committed data_commit_info",
        count(h, "SELECT count(*) FROM public.data_commit_info WHERE table_id = $1 AND committed", &table_id).await?,
        1,
    )?;
    ensure_eq(
        "partition_info rows",
        count(
            h,
            "SELECT count(*) FROM public.partition_info WHERE table_id = $1",
            &table_id,
        )
        .await?,
        1,
    )?;
    // A partition that has never been committed starts at version 0.
    ensure_eq("partition version", version(h, &table_id).await?, 0)?;

    let paths = file_paths(h, &table_id).await?;
    ensure!(
        !paths.is_empty(),
        "INSERT must commit parquet file operations"
    );
    for path in paths {
        let local = path.strip_prefix("file://").unwrap_or(&path);
        ensure!(
            std::path::Path::new(local).exists(),
            "committed file {path} does not exist on disk"
        );
        ensure!(
            path.contains(&h.lake_namespace),
            "committed file {path} is outside the suite warehouse"
        );
    }

    // A second connection reads its own committed write.
    let other = h.connect_lake().await?;
    let count = other
        .query_one("SELECT count(*) FROM empty_items", &[])
        .await?
        .get::<_, i64>(0);
    ensure_eq("second connection row count", count, 2)?;
    Ok(())
}

/// A prepared statement binds, executes twice and reports the parameter types
/// the plan needs.
async fn parameterized_insert_executes_twice(h: &Harness) -> Result<()> {
    let statement = h
        .lake
        .prepare("INSERT INTO pk_items (id, val, label) VALUES ($1, $2, $3)")
        .await?;
    let parameter_types = statement.params();
    ensure_eq("parameter count", parameter_types.len(), 3)?;
    ensure!(
        parameter_types[0] == Type::INT8,
        "primary-key parameter must describe as int8, actual {:?}",
        parameter_types[0]
    );

    for (id, val, label) in [(9001i64, 1i64, "one"), (9002, 2, "two")] {
        let rows = h.lake.execute(&statement, &[&id, &val, &label]).await?;
        ensure_eq(&format!("parameterized insert {id}"), rows, 1)?;
    }
    ensure_eq(
        "pk_items parameterized rows",
        lake_i64(h, "SELECT count(*) FROM pk_items WHERE id >= 9001").await?,
        2,
    )?;
    Ok(())
}

/// A duplicate primary key is a unique violation and changes nothing.
async fn duplicate_key_leaves_no_state(h: &Harness) -> Result<()> {
    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .execute(
            "INSERT INTO pk_items (id, val, label) VALUES (1, 5, 'dup')",
            &[],
        )
        .await
        .expect_err("duplicate key must fail");
    ensure_sqlstate(&error, "23505", "duplicate primary key")?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "duplicate primary key")?;
    ensure_eq(
        "existing row unchanged",
        lake_text(h, "SELECT label FROM pk_items WHERE id = 1")
            .await?
            .as_deref(),
        Some("new"),
    )?;
    Ok(())
}

/// A NULL in a NOT NULL column is `23502` and commits nothing.
async fn not_null_violation_is_23502(h: &Harness) -> Result<()> {
    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .execute(
            "INSERT INTO pk_items (id, val, label) VALUES (NULL, 1, 'x')",
            &[],
        )
        .await
        .expect_err("NOT NULL column must reject NULL");
    ensure_sqlstate(&error, "23502", "NOT NULL violation")?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "NOT NULL violation")?;
    Ok(())
}

/// `ON CONFLICT DO NOTHING` drops conflicting rows and counts only inserts.
async fn on_conflict_do_nothing_skips_conflicts(h: &Harness) -> Result<()> {
    let rows = simple_rows(
        h,
        "INSERT INTO pk_items (id, val, label) VALUES (9003, 3, 'three'), (1, 100, 'ignored') \
         ON CONFLICT (id) DO NOTHING",
    )
    .await?;
    ensure_eq("DO NOTHING inserted rows", rows, 1)?;
    ensure_eq(
        "conflicting row unchanged",
        lake_text(h, "SELECT label FROM pk_items WHERE id = 1")
            .await?
            .as_deref(),
        Some("new"),
    )?;
    ensure_eq(
        "non-conflicting row inserted",
        lake_text(h, "SELECT label FROM pk_items WHERE id = 9003")
            .await?
            .as_deref(),
        Some("three"),
    )?;
    Ok(())
}

/// `ON CONFLICT ... DO UPDATE SET col = EXCLUDED.col` for every non-key column
/// rewrites the whole row.
async fn on_conflict_do_update_replaces_the_row(h: &Harness) -> Result<()> {
    let rows = simple_rows(
        h,
        "INSERT INTO pk_items (id, val, label) VALUES (1, 500, 'updated') \
         ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, label = EXCLUDED.label",
    )
    .await?;
    ensure_eq("DO UPDATE rows", rows, 1)?;
    let row = h
        .lake
        .query_one("SELECT val, label FROM pk_items WHERE id = 1", &[])
        .await?;
    ensure_eq("DO UPDATE val", row.get::<_, i64>(0), 500)?;
    ensure_eq(
        "DO UPDATE label",
        row.get::<_, Option<String>>(1).as_deref(),
        Some("updated"),
    )?;
    Ok(())
}

/// One statement spanning several range partitions publishes every partition
/// version in a single metadata transaction, and all rows become visible
/// together.
async fn multi_partition_insert_publishes_atomically(h: &Harness) -> Result<()> {
    let rows = simple_rows(
        h,
        "INSERT INTO range_items (part, id, val) \
         VALUES (0, 1, 10), (1, 2, 20), (2, 3, 30)",
    )
    .await?;
    ensure_eq("multi-partition inserted rows", rows, 3)?;
    ensure_eq(
        "multi-partition visible rows",
        lake_i64(h, "SELECT count(*) FROM range_items").await?,
        3,
    )?;
    let table_id = table_id(h, "range_items").await?;
    ensure_eq(
        "committed data_commit_info per partition",
        count(
            h,
            "SELECT count(*) FROM public.data_commit_info \
             WHERE table_id = $1 AND committed",
            &table_id,
        )
        .await?,
        3,
    )?;
    ensure_eq(
        "partition_info rows",
        count(
            h,
            "SELECT count(*) FROM public.partition_info WHERE table_id = $1",
            &table_id,
        )
        .await?,
        3,
    )?;
    // A first commit starts every partition at version 0.
    let versions: Vec<i32> = h
        .reference
        .query(
            "SELECT version FROM public.partition_info WHERE table_id = $1",
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, i32>(0))
        .collect();
    ensure!(
        versions == vec![0; 3],
        "every partition of one statement starts at version 0, actual {versions:?}"
    );
    for path in file_paths(h, &table_id).await? {
        let local = path.strip_prefix("file://").unwrap_or(&path);
        ensure!(
            std::path::Path::new(local).exists(),
            "committed file {path} does not exist on disk"
        );
    }
    // A new connection sees every partition of the same statement.
    let other = h.connect_lake().await?;
    let count = other
        .query_one("SELECT count(*) FROM range_items", &[])
        .await?
        .get::<_, i64>(0);
    ensure_eq("multi-partition second connection rows", count, 3)?;
    Ok(())
}

/// Unsupported clauses and unsafe contexts are refused before any write.
async fn refusal_matrix_changes_nothing(h: &Harness) -> Result<()> {
    let before = h.authoritative_snapshot().await?;

    for (sql, code, case) in [
        (
            "INSERT INTO items (id, val, label) VALUES (7, 7, 'x') RETURNING id",
            "0A000",
            "RETURNING",
        ),
        (
            "INSERT INTO pk_items (id, val, label) VALUES (7, 7, 'x') \
             ON CONFLICT (val) DO NOTHING",
            "0A000",
            "non-primary-key conflict target",
        ),
        (
            "INSERT INTO pk_items (id, val, label) VALUES (1, 1, 'x') \
             ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
            "0A000",
            "partial DO UPDATE assignment set",
        ),
        (
            "INSERT INTO pk_items (id, val, label) VALUES (1, 1, 'x') \
             ON CONFLICT (id) DO UPDATE SET label = 'constant'",
            "0A000",
            "DO UPDATE without EXCLUDED",
        ),
        (
            "INSERT INTO missing_table (id) VALUES (1)",
            "42P01",
            "missing table",
        ),
        (
            "INSERT INTO otherdb.public.items (id) VALUES (1)",
            "3D000",
            "other database",
        ),
        (
            "INSERT INTO other_schema.items (id) VALUES (1)",
            "3F000",
            "other schema",
        ),
    ] {
        let error = h
            .lake
            .execute(sql, &[])
            .await
            .expect_err(&format!("{case} must be refused"));
        ensure_sqlstate(&error, code, case)?;
    }

    h.lake.batch_execute("BEGIN").await?;
    let error = h
        .lake
        .execute("INSERT INTO items (id, val, label) VALUES (8, 8, 'x')", &[])
        .await
        .expect_err("INSERT inside a transaction block must be refused");
    ensure_sqlstate(&error, "0A000", "transaction block")?;
    h.lake.batch_execute("ROLLBACK").await?;

    let error = h
        .lake
        .simple_query("INSERT INTO items (id, val, label) VALUES (9, 9, 'x'); SELECT 1")
        .await
        .expect_err("INSERT in a simple-query batch must be refused");
    ensure_sqlstate(&error, "0A000", "simple-query batch")?;

    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "refusal matrix")?;
    Ok(())
}

/// Run one statement over the extended protocol and return the tag's rows.
async fn simple_rows(h: &Harness, sql: &str) -> Result<u64> {
    let messages = h.lake.simple_query(sql).await?;
    for message in messages {
        if let SimpleQueryMessage::CommandComplete(rows) = message {
            return Ok(rows);
        }
    }
    bail!("statement `{sql}` returned no CommandComplete tag")
}

async fn lake_i64(h: &Harness, sql: &str) -> Result<i64> {
    Ok(h.lake.query_one(sql, &[]).await?.get::<_, i64>(0))
}

async fn lake_text(h: &Harness, sql: &str) -> Result<Option<String>> {
    Ok(h.lake
        .query_one(sql, &[])
        .await?
        .get::<_, Option<String>>(0))
}

async fn table_id(h: &Harness, table: &str) -> Result<String> {
    Ok(h.reference
        .query_one(
            "SELECT table_id FROM public.table_info WHERE table_namespace = $1 AND table_name = $2",
            &[&h.lake_namespace, &table],
        )
        .await?
        .get::<_, String>(0))
}

async fn count(h: &Harness, sql: &str, table_id: &str) -> Result<i64> {
    Ok(h.reference
        .query_one(sql, &[&table_id])
        .await?
        .get::<_, i64>(0))
}

async fn version(h: &Harness, table_id: &str) -> Result<i32> {
    Ok(h.reference
        .query_one(
            "SELECT version FROM public.partition_info WHERE table_id = $1",
            &[&table_id],
        )
        .await?
        .get::<_, i32>(0))
}

async fn file_paths(h: &Harness, table_id: &str) -> Result<Vec<String>> {
    Ok(h.reference
        .query(
            "SELECT (operation).path FROM public.data_commit_info d \
             CROSS JOIN LATERAL unnest(d.file_ops) AS operation \
             WHERE d.table_id = $1 AND d.committed",
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>(0))
        .collect())
}

/// Assert that `error` is a server `ErrorResponse` with exactly `code`.
fn ensure_sqlstate(
    error: &tokio_postgres::Error,
    expected: &str,
    context: &str,
) -> Result<()> {
    let db = error.as_db_error().with_context(|| {
        format!(
            "{context}: expected server ErrorResponse SQLSTATE {expected}, \
             actual client-side rejection (no SQLSTATE): {error}"
        )
    })?;
    ensure!(
        db.code().code() == expected,
        "{context}: expected SQLSTATE {expected}, actual {} ({})",
        db.code().code(),
        db.message()
    );
    Ok(())
}

fn ensure_eq<T: std::fmt::Debug + PartialEq>(
    context: &str,
    actual: T,
    expected: T,
) -> Result<()> {
    ensure!(
        actual == expected,
        "{context}: expected {expected:?}, actual {actual:?}"
    );
    Ok(())
}

fn ensure_snapshot_unchanged(before: &str, after: &str, context: &str) -> Result<()> {
    if before == after {
        return Ok(());
    }
    let first_difference = before
        .bytes()
        .zip(after.bytes())
        .position(|(before, after)| before != after);
    bail!(
        "{context}: authoritative snapshot changed \
         (before {} bytes, after {} bytes, first difference at byte {:?})",
        before.len(),
        after.len(),
        first_difference
    )
}
