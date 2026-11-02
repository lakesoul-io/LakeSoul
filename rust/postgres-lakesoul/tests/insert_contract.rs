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
//! table, then the conflict and refusal matrix, which must not change state,
//! and finally the PostgreSQL `INSERT` analysis matrix (`42601`/`42804`/`22P02`
//! plus NULL completion of unlisted columns).

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
            prepared_insert_into_dropped_table_leaves_no_state(h).await?;
            insert_analysis_matches_postgres(h).await?;
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

/// A prepared `INSERT` whose target another connection dropped is rejected
/// at Execute with `42P01`, and leaves no metadata row or file behind.
///
/// The target is resolved from metadata when the statement executes, so this
/// is the insert path's own `42P01`; the prepared statement still exists on
/// this connection, but it cannot reach the deleted table.
async fn prepared_insert_into_dropped_table_leaves_no_state(h: &Harness) -> Result<()> {
    let other = h.connect_lake().await?;
    other
        .simple_query(
            "CREATE TABLE insert_drop_target (id BIGINT NOT NULL PRIMARY KEY, label TEXT)",
        )
        .await
        .context("cross-connection INSERT fixture CREATE")?;
    let insert = h
        .lake
        .prepare("INSERT INTO insert_drop_target (id, label) VALUES ($1, $2)")
        .await
        .context("prepare INSERT before the peer DROP")?;
    other
        .simple_query("DROP TABLE insert_drop_target")
        .await
        .context("peer DROP of the INSERT target")?;
    let remaining = h
        .reference
        .query_one(
            "SELECT count(*) FROM public.table_info \
             WHERE table_namespace = $1 AND table_name = $2",
            &[&h.lake_namespace, &"insert_drop_target"],
        )
        .await
        .context("table_info count after the peer DROP")?
        .get::<_, i64>(0);
    ensure_eq("table_info rows after peer DROP", remaining, 0)?;

    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .execute(&insert, &[&1i64, &"x"])
        .await
        .expect_err("INSERT into a dropped table must fail");
    ensure_sqlstate(
        &error,
        "42P01",
        "prepared INSERT into a table dropped by another connection",
    )?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "INSERT into a dropped table")?;
    eprintln!(
        "PASS case=prepared-insert-dropped-target SQLSTATE=42P01 \
         authoritative-snapshot=unchanged"
    );
    Ok(())
}

/// PostgreSQL analyses `INSERT` before planning it: the source must have one
/// expression per target column (`42601`), an untyped string literal is
/// converted to the target type (`22P02` when it does not convert) whether it
/// appears in a `VALUES` row or a `SELECT` projection, and every typed
/// expression must be assignment-castable (`42804`). Both servers must report
/// the same SQLSTATE, through the simple and the extended protocol, and a
/// rejection must leave no state.
///
/// The completion rules must still work: a no-list `VALUES` or `SELECT`
/// source with fewer expressions than the table has columns is padded with
/// NULLs and inserts, and a string literal that does convert (`'42'` into
/// `INTEGER`) inserts its value.
async fn insert_analysis_matches_postgres(h: &Harness) -> Result<()> {
    // BOOLEAN and DATE targets for the assignment matrix. DDL commits
    // metadata immediately, so create both before the snapshot below.
    let create = "CREATE TABLE insert_types (\
                  id BIGINT NOT NULL PRIMARY KEY, flag BOOLEAN, day DATE, note TEXT)";
    h.lake
        .simple_query(create)
        .await
        .context("LakeSoul CREATE TABLE insert_types")?;
    h.reference
        .simple_query(create)
        .await
        .context("reference CREATE TABLE insert_types")?;

    let before = h.authoritative_snapshot().await?;
    for (sql, expected, case) in [
        (
            "INSERT INTO empty_items (id, val, label) VALUES (1, 'abc', 'x')",
            "22P02",
            "string literal into INTEGER",
        ),
        (
            "INSERT INTO empty_items (id, val) VALUES (2, now())",
            "42804",
            "now() into INTEGER",
        ),
        (
            "INSERT INTO empty_items (id, val, label) VALUES (3, true, 'x')",
            "42804",
            "boolean literal into INTEGER",
        ),
        (
            "INSERT INTO insert_types (id, flag) VALUES (4, 1)",
            "42804",
            "integer literal into BOOLEAN",
        ),
        (
            "INSERT INTO insert_types (id, day) VALUES (5, 1)",
            "42804",
            "integer literal into DATE",
        ),
        (
            "INSERT INTO empty_items (id) SELECT 'abc'",
            "22P02",
            "string literal SELECT into INTEGER",
        ),
        (
            "INSERT INTO empty_items (id, val, label) \
             SELECT id, label, val FROM pk_items LIMIT 1",
            "42804",
            "TEXT SELECT column into INTEGER",
        ),
        (
            "INSERT INTO empty_items (id, val, label) VALUES (5, 6)",
            "42601",
            "explicit list with too few expressions",
        ),
        (
            "INSERT INTO empty_items (id, val, label) VALUES (6, 1, 2, 3)",
            "42601",
            "explicit list with too many expressions",
        ),
        (
            "INSERT INTO empty_items VALUES (1, 2, 'x', 4)",
            "42601",
            "no list with more expressions than columns",
        ),
    ] {
        for protocol in Protocol::BOTH {
            let lake = rejection(&h.lake, sql, protocol).await?;
            let reference = rejection(&h.reference, sql, protocol).await?;
            ensure_rejection(lake, reference, expected, case, protocol.label())?;
        }
    }
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "INSERT analysis rejection matrix")?;

    // A no-list `VALUES` source shorter than the table is completed with
    // NULLs, exactly as PostgreSQL does without a DEFAULT clause.
    let simple_null_fill = "INSERT INTO empty_items VALUES (30, 40)";
    ensure_eq(
        "NULL-fill simple tag rows",
        simple_rows(h, simple_null_fill).await?,
        1,
    )?;
    let extended_null_fill = "INSERT INTO empty_items VALUES (32, 40)";
    let statement = h
        .lake
        .prepare(extended_null_fill)
        .await
        .context("LakeSoul prepare NULL-fill insert")?;
    ensure_eq(
        "NULL-fill extended tag rows",
        h.lake.execute(&statement, &[]).await?,
        1,
    )?;
    for sql in [simple_null_fill, extended_null_fill] {
        let statement = h
            .reference
            .prepare(sql)
            .await
            .context("reference prepare NULL-fill insert")?;
        h.reference
            .execute(&statement, &[])
            .await
            .context("reference NULL-fill insert")?;
    }
    for id in [30, 32] {
        ensure_eq(
            &format!("NULL-fill row {id} count"),
            lake_i64(
                h,
                &format!("SELECT count(*) FROM empty_items WHERE id = {id}"),
            )
            .await?,
            1,
        )?;
        ensure_eq(
            &format!("NULL-fill row {id} unlisted label"),
            lake_text(h, &format!("SELECT label FROM empty_items WHERE id = {id}"))
                .await?
                .as_deref(),
            None,
        )?;
    }

    // A no-list `INSERT ... SELECT` shorter than the table is completed the
    // same way, also through both protocols.
    let simple_select_fill =
        "INSERT INTO empty_items SELECT id, val FROM items WHERE id = 2";
    ensure_eq(
        "SELECT-fill simple tag rows",
        simple_rows(h, simple_select_fill).await?,
        1,
    )?;
    let extended_select_fill =
        "INSERT INTO empty_items SELECT id, val FROM items WHERE id = 1 AND val IS NULL";
    let statement = h
        .lake
        .prepare(extended_select_fill)
        .await
        .context("LakeSoul prepare SELECT-fill insert")?;
    ensure_eq(
        "SELECT-fill extended tag rows",
        h.lake.execute(&statement, &[]).await?,
        1,
    )?;
    for sql in [simple_select_fill, extended_select_fill] {
        let statement = h
            .reference
            .prepare(sql)
            .await
            .context("reference prepare SELECT-fill insert")?;
        h.reference
            .execute(&statement, &[])
            .await
            .context("reference SELECT-fill insert")?;
    }
    ensure_eq(
        "SELECT-fill simple row",
        lake_i64(
            h,
            "SELECT count(*) FROM empty_items \
             WHERE id = 2 AND val = 20 AND label IS NULL",
        )
        .await?,
        1,
    )?;
    ensure_eq(
        "SELECT-fill extended row",
        lake_i64(
            h,
            "SELECT count(*) FROM empty_items \
             WHERE id = 1 AND val IS NULL AND label IS NULL",
        )
        .await?,
        1,
    )?;

    // A bare string literal in a `SELECT` projection is untyped too, so the
    // target column decides its type. A literal at one position must not mask
    // a typed expression at another.
    let simple_select_literal = "INSERT INTO empty_items (id) SELECT '42'";
    ensure_eq(
        "SELECT literal simple tag rows",
        simple_rows(h, simple_select_literal).await?,
        1,
    )?;
    let extended_select_literal = "INSERT INTO empty_items (id) SELECT '43'";
    let statement = h
        .lake
        .prepare(extended_select_literal)
        .await
        .context("LakeSoul prepare SELECT literal insert")?;
    ensure_eq(
        "SELECT literal extended tag rows",
        h.lake.execute(&statement, &[]).await?,
        1,
    )?;
    let mixed_simple =
        "INSERT INTO empty_items (id, val) SELECT id, '7' FROM pk_items LIMIT 1";
    ensure_eq(
        "mixed SELECT simple tag rows",
        simple_rows(h, mixed_simple).await?,
        1,
    )?;
    let mixed_extended =
        "INSERT INTO empty_items (id, val) SELECT id, '8' FROM pk_items LIMIT 1";
    let statement = h
        .lake
        .prepare(mixed_extended)
        .await
        .context("LakeSoul prepare mixed SELECT insert")?;
    ensure_eq(
        "mixed SELECT extended tag rows",
        h.lake.execute(&statement, &[]).await?,
        1,
    )?;
    for sql in [
        simple_select_literal,
        extended_select_literal,
        mixed_simple,
        mixed_extended,
    ] {
        let statement = h
            .reference
            .prepare(sql)
            .await
            .context("reference prepare SELECT literal insert")?;
        h.reference
            .execute(&statement, &[])
            .await
            .context("reference SELECT literal insert")?;
    }
    for id in [42, 43] {
        ensure_eq(
            &format!("SELECT literal row {id} count"),
            lake_i64(
                h,
                &format!(
                    "SELECT count(*) FROM empty_items \
                     WHERE id = {id} AND val IS NULL AND label IS NULL"
                ),
            )
            .await?,
            1,
        )?;
    }
    for value in [7, 8] {
        ensure_eq(
            &format!("mixed SELECT literal value {value}"),
            lake_i64(
                h,
                &format!("SELECT count(*) FROM empty_items WHERE val = {value}"),
            )
            .await?,
            1,
        )?;
    }

    // A string literal that converts still inserts its value.
    let simple_control =
        "INSERT INTO empty_items (id, val, label) VALUES (31, '42', 'ctrl')";
    ensure_eq(
        "control simple tag rows",
        simple_rows(h, simple_control).await?,
        1,
    )?;
    let extended_control =
        "INSERT INTO empty_items (id, val, label) VALUES (33, '42', 'ctrl')";
    let statement = h
        .lake
        .prepare(extended_control)
        .await
        .context("LakeSoul prepare control insert")?;
    ensure_eq(
        "control extended tag rows",
        h.lake.execute(&statement, &[]).await?,
        1,
    )?;
    let reference_control = h
        .reference
        .prepare(simple_control)
        .await
        .context("reference prepare control insert")?;
    h.reference
        .execute(&reference_control, &[])
        .await
        .context("reference control insert")?;
    for id in [31, 33] {
        ensure_eq(
            &format!("control row {id} converted val"),
            lake_i32(h, &format!("SELECT val FROM empty_items WHERE id = {id}")).await?,
            42,
        )?;
        ensure_eq(
            &format!("control row {id} label"),
            lake_text(h, &format!("SELECT label FROM empty_items WHERE id = {id}"))
                .await?
                .as_deref(),
            Some("ctrl"),
        )?;
    }
    eprintln!(
        "PASS case=insert-analysis lake-and-postgres=agree \
         checked=22P02,42804,42601,values-null-fill,select-null-fill,\
         select-literal,control"
    );
    Ok(())
}

#[derive(Clone, Copy)]
enum Protocol {
    Simple,
    Extended,
}

impl Protocol {
    const BOTH: [Protocol; 2] = [Protocol::Simple, Protocol::Extended];

    fn label(self) -> &'static str {
        match self {
            Protocol::Simple => "simple",
            Protocol::Extended => "extended",
        }
    }
}

/// Run one statement and return the server SQLSTATE when it is rejected, or
/// `None` when it succeeds. Extended-protocol analysis runs at Parse, but an
/// implementation may defer a rejection to Execute, so both are observed.
async fn rejection(
    client: &tokio_postgres::Client,
    sql: &str,
    protocol: Protocol,
) -> Result<Option<String>> {
    match protocol {
        Protocol::Simple => match client.simple_query(sql).await {
            Ok(_) => Ok(None),
            Err(error) => Ok(Some(server_sqlstate(&error)?)),
        },
        Protocol::Extended => match client.prepare(sql).await {
            Err(error) => Ok(Some(server_sqlstate(&error)?)),
            Ok(statement) => match client.execute(&statement, &[]).await {
                Ok(_) => Ok(None),
                Err(error) => Ok(Some(server_sqlstate(&error)?)),
            },
        },
    }
}

fn server_sqlstate(error: &tokio_postgres::Error) -> Result<String> {
    let database = error.as_db_error().with_context(|| {
        format!("expected a server ErrorResponse, actual client-side error: {error}")
    })?;
    Ok(database.code().code().to_string())
}

fn ensure_rejection(
    lake: Option<String>,
    reference: Option<String>,
    expected: &str,
    case: &str,
    protocol: &str,
) -> Result<()> {
    let lake = lake.with_context(|| {
        format!("case={case} protocol={protocol}: LakeSoul accepted the statement")
    })?;
    let reference = reference.with_context(|| {
        format!("case={case} protocol={protocol}: PostgreSQL accepted the statement")
    })?;
    ensure!(
        lake == reference,
        "case={case} protocol={protocol}: LakeSoul SQLSTATE={lake}, PostgreSQL SQLSTATE={reference}"
    );
    ensure!(
        lake == expected,
        "case={case} protocol={protocol}: expected SQLSTATE={expected}, actual {lake}"
    );
    eprintln!("PASS case={case} protocol={protocol} lake={lake} postgres={reference}");
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

async fn lake_i32(h: &Harness, sql: &str) -> Result<i32> {
    Ok(h.lake.query_one(sql, &[]).await?.get::<_, i32>(0))
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
