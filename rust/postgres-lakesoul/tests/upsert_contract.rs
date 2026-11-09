// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `INSERT ... ON CONFLICT DO UPDATE` contracts against the real server.
//!
//! Every successful case runs the same statement on LakeSoul and on the
//! PostgreSQL reference connection; the command tag's row count and the full
//! relation must agree. A rejected or failing statement is additionally
//! proven to change nothing by comparing the authoritative snapshot (metadata
//! of every table in the namespace plus the exact bytes of every warehouse
//! file) around it, and by reading the target relation afterwards.
//!
//! The matrix follows the PostgreSQL oracle recorded in
//! `.plans/update-upsert.md` §10.1 and `local://c1-upsert-evidence.md`:
//! partial `INSERT`/`SET`, unassigned columns, explicit NULL, old-value and
//! `EXCLUDED` expressions, simultaneous assignments, conflict `WHERE`
//! true/false/NULL, new keys outside the conflict filter, the repeated-source
//! key matrix (including both `ORDER BY` directions and the rule that `21000`
//! wins over a later `WHERE`), incoming vs output constraint order, parameter
//! rebinding on a new snapshot, DROP + same-name rebuild, and the refusal
//! boundaries.
//!
//! Two tests share the behavioral scenarios:
//!
//! * [`upsert_contract`] executes them on the coordinator;
//! * [`distributed_upsert_contract`] executes the same statements with two
//!   real Flight workers and `--distributed-fallback-local=false`, then pins a
//!   distributed publication so a statement either runs on the workers or
//!   fails instead of silently reverting to the coordinator.
//!
//! [`indexed_upsert_contract`] runs on the vector suite and proves that an
//! upsert's new vector/text is searchable through the committed physical
//! index, the superseded value no longer pulls the row back, and a
//! post-publication index failure does not delete committed data.

pub mod support;

use std::collections::BTreeMap;

use anyhow::{Context, Result, bail, ensure};
use tokio_postgres::types::Type;
use tokio_postgres::{Client, SimpleQueryMessage};

use lakesoul_common::IndexKind;
use support::vector::{ANN_TABLE, CORPUS_ROWS, DIM, NLIST, TEXT_TABLE, corpus_vector};
use support::{Harness, compare};

/// SQLSTATE for a statement the front end refuses to execute.
const FEATURE_NOT_SUPPORTED: &str = "0A000";
/// SQLSTATE for a NULL written into a NOT NULL column.
const NOT_NULL_VIOLATION: &str = "23502";
/// SQLSTATE for a text value that does not convert to the target type.
const INVALID_TEXT_REPRESENTATION: &str = "22P02";
/// SQLSTATE for an expression whose type cannot be assigned to the column.
const DATATYPE_MISMATCH: &str = "42804";
/// SQLSTATE for a malformed command (including a missing conflict target).
const SYNTAX_ERROR: &str = "42601";
/// SQLSTATE for a stale prepared target.
const UNDEFINED_TABLE: &str = "42P01";
/// SQLSTATE for a qualified `SET` target that names a relation.
const UNDEFINED_COLUMN: &str = "42703";
/// SQLSTATE for a bare column that both row versions could supply.
const AMBIGUOUS_COLUMN: &str = "42702";
/// SQLSTATE for a target table actually named `excluded` without an alias.
const AMBIGUOUS_ALIAS: &str = "42P09";
/// SQLSTATE for a conflict target without a primary key.
const NO_UNIQUE_CONSTRAINT: &str = "42P10";
/// SQLSTATE for a statement that would affect one row twice.
const CARDINALITY_VIOLATION: &str = "21000";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upsert_contract() -> Result<()> {
    support::run_suite(|h| Box::pin(async move { upsert_scenarios(h).await })).await
}

/// The same statements with two real Flight workers and local fallback
/// disabled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_upsert_contract() -> Result<()> {
    support::run_distributed_insert_suite(|h| {
        Box::pin(async move {
            upsert_scenarios(h).await?;
            distributed_upsert_publishes_on_workers(h).await
        })
    })
    .await
}

/// The vector/text index must follow an upsert.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_upsert_contract() -> Result<()> {
    support::run_vector_suite(|h| {
        Box::pin(async move { indexed_upsert_scenario(h).await })
    })
    .await
}

/// The behavior matrix shared by the coordinator-only and distributed runs.
async fn upsert_scenarios(h: &Harness) -> Result<()> {
    mixed_keys_partial_columns_and_expressions(h).await?;
    simultaneous_assignments_and_same_value_counts(h).await?;
    conflict_where_matrix(h).await?;
    duplicate_source_key_matrix(h).await?;
    select_source_order_preserves_non_projected_expression(h).await?;
    incoming_and_output_constraint_order(h).await?;
    internal_names_do_not_collide(h).await?;
    alias_and_ambiguous_names(h).await?;
    prepared_upsert_rebinds_and_reads_new_commits(h).await?;
    prepared_upsert_after_drop_and_recreate(h).await?;
    empty_and_all_skipped_change_nothing(h).await?;
    legacy_full_row_and_plain_insert_regressions(h).await?;
    refused_upserts_change_nothing(h).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Case harness
// ---------------------------------------------------------------------------

/// Protocol whose execution path a case exercises.
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

fn table_name(base: &str, protocol: Protocol) -> String {
    format!("upsert_{base}_{}", protocol.label())
}

fn render(template: &str, table: &str) -> String {
    template.replace("{t}", table)
}

async fn run_rows_case(
    h: &Harness,
    protocol: Protocol,
    base: &str,
    ddl: &str,
    seed: &str,
    sql: &str,
    projection: &str,
    expected: u64,
) -> Result<()> {
    let table = table_name(base, protocol);
    create_mirrored_table(
        h,
        &render(ddl, &table),
        &render(ddl, &table),
        &render(seed, &table),
    )
    .await?;
    let sql = render(sql, &table);
    let lake = match protocol {
        Protocol::Simple => simple_tag_rows(&h.lake, &sql).await?,
        Protocol::Extended => extended_rows(h, &sql).await?,
    };
    let reference = simple_tag_rows(&h.reference, &sql).await?;
    ensure_eq(
        &format!("{base}/{}: LakeSoul row count", protocol.label()),
        lake,
        reference,
    )?;
    ensure_eq(
        &format!("{base}/{}: expected row count", protocol.label()),
        lake,
        expected,
    )?;
    ensure_relation_parity(h, &render(projection, &table), base).await?;
    eprintln!(
        "PASS case={base} protocol={} rows={lake} pg-relation=equal",
        protocol.label()
    );
    Ok(())
}

async fn run_error_case(
    h: &Harness,
    protocol: Protocol,
    base: &str,
    ddl: &str,
    seed: &str,
    sql: &str,
    expected: &str,
) -> Result<()> {
    let table = table_name(base, protocol);
    create_mirrored_table(
        h,
        &render(ddl, &table),
        &render(ddl, &table),
        &render(seed, &table),
    )
    .await?;
    let sql = render(sql, &table);
    let before = h.authoritative_snapshot().await?;
    let lake = rejection(&h.lake, &sql, protocol).await?;
    let reference = rejection(&h.reference, &sql, Protocol::Simple).await?;
    ensure_rejection(lake, reference, expected, base, protocol.label())?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, base)?;
    ensure_connection_usable(h, base).await?;
    eprintln!(
        "PASS case={base} protocol={} SQLSTATE={expected} snapshot=unchanged",
        protocol.label()
    );
    Ok(())
}

/// A refusal that is deliberately LakeSoul-only (PostgreSQL accepts or reports
/// a different SQLSTATE); the snapshot must still be unchanged.
async fn reject_lake_only(
    h: &Harness,
    protocol: Protocol,
    sql: &str,
    expected: &str,
    context: &str,
) -> Result<()> {
    let before = h.authoritative_snapshot().await?;
    let actual = rejection(&h.lake, sql, protocol).await?;
    let actual = actual.with_context(|| {
        format!(
            "{context}/{}: LakeSoul accepted the statement",
            protocol.label()
        )
    })?;
    ensure!(
        actual == expected,
        "{context}/{}: expected SQLSTATE {expected}, actual {actual}",
        protocol.label()
    );
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, context)?;
    ensure_connection_usable(h, context).await?;
    Ok(())
}

/// Run one statement and return the server SQLSTATE when it is rejected, or
/// `None` when it succeeds. Extended analysis can reject at Parse; an
/// implementation may also defer to Execute, so both are observed.
async fn rejection(
    client: &Client,
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
        "case={case} protocol={protocol}: LakeSoul SQLSTATE={lake}, \
         PostgreSQL SQLSTATE={reference}"
    );
    ensure!(
        lake == expected,
        "case={case} protocol={protocol}: expected SQLSTATE={expected}, actual {lake}"
    );
    Ok(())
}

async fn extended_rows(h: &Harness, sql: &str) -> Result<u64> {
    h.lake
        .execute(sql, &[])
        .await
        .with_context(|| format!("extended Execute: {sql}"))
}

async fn simple_tag_rows(client: &Client, sql: &str) -> Result<u64> {
    for message in client
        .simple_query(sql)
        .await
        .with_context(|| format!("simple query: {sql}"))?
    {
        if let SimpleQueryMessage::CommandComplete(rows) = message {
            return Ok(rows);
        }
    }
    bail!("statement `{sql}` returned no CommandComplete tag")
}

async fn ensure_relation_parity(h: &Harness, sql: &str, context: &str) -> Result<()> {
    let lake = compare::simple(&h.lake, sql)
        .await
        .with_context(|| format!("{context}: LakeSoul {sql}"))?;
    let reference = compare::simple(&h.reference, sql)
        .await
        .with_context(|| format!("{context}: PostgreSQL {sql}"))?;
    compare::compare(&lake, &reference, true).with_context(|| format!("{context}: {sql}"))
}

async fn ensure_rows(
    h: &Harness,
    sql: &str,
    expected: compare::Rows,
    context: &str,
) -> Result<()> {
    let actual = compare::simple(&h.lake, sql)
        .await
        .with_context(|| format!("{context}: read {sql}"))?;
    ensure!(
        actual.rows == expected,
        "{context}: relation {sql}\n expected {expected:?}\n actual {:?}",
        actual.rows
    );
    Ok(())
}

fn text_rows(rows: &[&[Option<&str>]]) -> compare::Rows {
    rows.iter()
        .map(|row| row.iter().map(|value| value.map(str::to_owned)).collect())
        .collect()
}

async fn create_mirrored_table(
    h: &Harness,
    lake_ddl: &str,
    reference_ddl: &str,
    seed: &str,
) -> Result<()> {
    h.lake
        .simple_query(lake_ddl)
        .await
        .with_context(|| format!("LakeSoul DDL: {lake_ddl}"))?;
    h.reference
        .simple_query(reference_ddl)
        .await
        .with_context(|| format!("PostgreSQL DDL: {reference_ddl}"))?;
    h.lake
        .simple_query(seed)
        .await
        .with_context(|| format!("LakeSoul seed: {seed}"))?;
    h.reference
        .simple_query(seed)
        .await
        .with_context(|| format!("PostgreSQL seed: {seed}"))?;
    Ok(())
}

async fn ensure_connection_usable(h: &Harness, context: &str) -> Result<()> {
    let message = h
        .lake
        .simple_query("SELECT 1 AS healthy")
        .await
        .with_context(|| format!("{context}: connection unusable after the statement"))?;
    ensure!(
        !message.is_empty(),
        "{context}: `SELECT 1` returned no message"
    );
    Ok(())
}

async fn lake_i64(h: &Harness, sql: &str) -> Result<i64> {
    Ok(h.lake
        .query_one(sql, &[])
        .await
        .with_context(|| format!("read {sql}"))?
        .get::<_, i64>(0))
}

async fn table_id(h: &Harness, table: &str) -> Result<String> {
    Ok(h.reference
        .query_one(
            "SELECT table_id FROM public.table_info \
             WHERE table_namespace = $1 AND table_name = $2",
            &[&h.lake_namespace, &table],
        )
        .await
        .with_context(|| format!("metadata table_id of {table}"))?
        .get::<_, String>(0))
}

async fn committed_commits(h: &Harness, table_id: &str) -> Result<i64> {
    Ok(h.reference
        .query_one(
            "SELECT count(*) FROM public.data_commit_info \
             WHERE table_id = $1 AND committed",
            &[&table_id],
        )
        .await?
        .get::<_, i64>(0))
}

async fn commit_file_paths(h: &Harness, table_id: &str) -> Result<Vec<(String, String)>> {
    Ok(h.reference
        .query(
            "SELECT d.partition_desc, (operation).path \
             FROM public.data_commit_info d \
             CROSS JOIN LATERAL unnest(d.file_ops) AS operation \
             WHERE d.table_id = $1 AND d.committed \
             ORDER BY d.partition_desc, (operation).path",
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect())
}

/// Every metadata row that names `table_id` directly, without joining
/// `table_info`: a dropped table's orphan rows must still be visible.
async fn table_metadata_state(h: &Harness, table_id: &str) -> Result<String> {
    let mut state = Vec::new();
    for table in ["partition_info", "data_commit_info"] {
        let sql = format!(
            "SELECT row_to_json(t)::text FROM public.{table} t \
             WHERE table_id = $1 ORDER BY row_to_json(t)::text COLLATE \"C\""
        );
        let rows: Vec<String> = h
            .reference
            .query(&sql, &[&table_id])
            .await
            .with_context(|| format!("metadata table={table} table_id={table_id}"))?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        state.push((table, rows));
    }
    Ok(serde_json::to_string(&state)?)
}

fn ensure_committed_files_exist(
    h: &Harness,
    file_paths: &[(String, String)],
    context: &str,
) -> Result<()> {
    for (_, path) in file_paths {
        let local = std::path::Path::new(path.strip_prefix("file://").unwrap_or(path));
        ensure!(
            local.is_file(),
            "{context}: committed file {path} does not exist on disk"
        );
        ensure!(
            local.starts_with(&h.warehouse_dir),
            "{context}: committed file {path} is outside the suite warehouse"
        );
    }
    Ok(())
}

fn ensure_previous_files_unchanged(
    context: &str,
    before: &BTreeMap<String, Vec<u8>>,
    after: &BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    for (path, bytes) in before {
        match after.get(path) {
            None => bail!(
                "{context}: previously committed file {path} disappeared; an \
                 upsert must append new row versions instead of replacing the \
                 partition snapshot"
            ),
            Some(current) => ensure!(
                current == bytes,
                "{context}: previously committed file {path} changed \
                 ({} bytes before, {} after)",
                bytes.len(),
                current.len()
            ),
        }
    }
    Ok(())
}

fn ensure_sqlstate(
    error: &tokio_postgres::Error,
    expected: &str,
    context: &str,
) -> Result<()> {
    let database = error.as_db_error().with_context(|| {
        format!(
            "{context}: expected server ErrorResponse SQLSTATE {expected}, \
             actual client-side rejection (no SQLSTATE): {error}"
        )
    })?;
    ensure!(
        database.code().code() == expected,
        "{context}: expected SQLSTATE {expected}, actual {} ({})",
        database.code().code(),
        database.message()
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
    bail!(
        "{context}: authoritative snapshot changed (before {} bytes, after {} \
         bytes)",
        before.len(),
        after.len()
    )
}

// ---------------------------------------------------------------------------
// Mixed keys, partial columns, expressions
// ---------------------------------------------------------------------------

async fn mixed_keys_partial_columns_and_expressions(h: &Harness) -> Result<()> {
    const DDL: &str =
        "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT, label TEXT, PRIMARY KEY (id))";
    const SEED: &str = "INSERT INTO {t} (id, n, label) VALUES (1,10,'keep'),(2,20,'two')";
    const PROJECTION: &str = "SELECT id, n, label FROM {t} ORDER BY id";

    for protocol in Protocol::BOTH {
        // Partial INSERT list + partial SET: an unlisted/unassigned column
        // keeps the old row, while unlisted INSERT columns are NULL for a
        // genuinely new key.
        run_rows_case(
            h,
            protocol,
            "mixed_partial",
            DDL,
            SEED,
            "INSERT INTO {t} (id, label) VALUES (1,'old-key'),(3,'new-key') \
             ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label",
            PROJECTION,
            2,
        )
        .await?;
        // Old-value expression plus EXCLUDED in SET.
        run_rows_case(
            h,
            protocol,
            "mixed_expression",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n, label) VALUES (1,5,'x'),(2,7,'y') \
             ON CONFLICT (id) DO UPDATE SET n = {t}.n + EXCLUDED.n",
            PROJECTION,
            2,
        )
        .await?;
        // Explicit NULL overwrites the old non-NULL value; untouched n stays.
        run_rows_case(
            h,
            protocol,
            "mixed_explicit_null",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n, label) VALUES (1,5,NULL) \
             ON CONFLICT (id) DO UPDATE SET label = NULL",
            PROJECTION,
            1,
        )
        .await?;
        // Conflict WHERE comparing EXCLUDED with the old row.
        run_rows_case(
            h,
            protocol,
            "mixed_where",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n, label) VALUES (1,100,'big'),(2,1,'small') \
             ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label \
             WHERE EXCLUDED.n > {t}.n",
            PROJECTION,
            1,
        )
        .await?;
        // WHERE true updates every matched row.
        run_rows_case(
            h,
            protocol,
            "mixed_true",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n, label) VALUES (1,1,'a'),(2,2,'b') \
             ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label WHERE true",
            PROJECTION,
            2,
        )
        .await?;
        // The conflict WHERE does not filter the insertion branch.
        run_rows_case(
            h,
            protocol,
            "mixed_new_key",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n, label) VALUES (1,50,'skip'),(9,90,'new') \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE false",
            PROJECTION,
            1,
        )
        .await?;
    }
    Ok(())
}

async fn simultaneous_assignments_and_same_value_counts(h: &Harness) -> Result<()> {
    const DDL: &str =
        "CREATE TABLE {t} (id BIGINT NOT NULL, a INTEGER, b INTEGER, PRIMARY KEY (id))";
    const SEED: &str = "INSERT INTO {t} (id, a, b) VALUES (1,10,20),(2,1,2)";
    const PROJECTION: &str = "SELECT id, a, b FROM {t} ORDER BY id";

    for protocol in Protocol::BOTH {
        // Both assignments read the same old row: a = old b, b = old a.
        run_rows_case(
            h,
            protocol,
            "swap",
            DDL,
            SEED,
            "INSERT INTO {t} (id, a, b) VALUES (1,100,200),(2,3,4) \
             ON CONFLICT (id) DO UPDATE SET a = {t}.b, b = {t}.a",
            PROJECTION,
            2,
        )
        .await?;
        // Assigning the old value back still counts as an updated row.
        run_rows_case(
            h,
            protocol,
            "same_value",
            DDL,
            SEED,
            "INSERT INTO {t} (id, a, b) VALUES (2,30,40) \
             ON CONFLICT (id) DO UPDATE SET a = {t}.a",
            PROJECTION,
            1,
        )
        .await?;
    }
    Ok(())
}

async fn conflict_where_matrix(h: &Harness) -> Result<()> {
    const DDL: &str = "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (id))";
    const SEED: &str = "INSERT INTO {t} (id, n) VALUES (1,10),(2,20),(3,30)";
    const PROJECTION: &str = "SELECT id, n FROM {t} ORDER BY id";

    for protocol in Protocol::BOTH {
        for (base, predicate, expected) in [
            ("where_true", "true", 2u64),
            ("where_false", "false", 0),
            ("where_null", "NULL", 0),
            ("where_compare", "EXCLUDED.n > {t}.n", 1),
        ] {
            run_rows_case(
                h,
                protocol,
                base,
                DDL,
                SEED,
                &format!(
                    "INSERT INTO {{t}} (id, n) VALUES (1,15),(2,5) \
                     ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE {predicate}"
                ),
                PROJECTION,
                expected,
            )
            .await?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Repeated source keys
// ---------------------------------------------------------------------------

async fn duplicate_source_key_matrix(h: &Harness) -> Result<()> {
    const DDL: &str = "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (id))";
    const SEED: &str = "INSERT INTO {t} (id, n) VALUES (1,10)";
    const EMPTY_SEED: &str = "SELECT 1";
    const PROJECTION: &str = "SELECT id, n FROM {t} ORDER BY id";

    for protocol in Protocol::BOTH {
        // D02: first input updates; the second hits a modified row even
        // though its own WHERE is false.
        run_error_case(
            h,
            protocol,
            "dup_d02",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5),(1,6) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE EXCLUDED.n = 5",
            CARDINALITY_VIOLATION,
        )
        .await?;
        // D03: first input is skipped, so the second may update.
        run_rows_case(
            h,
            protocol,
            "dup_d03",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5),(1,6) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE EXCLUDED.n = 6",
            PROJECTION,
            1,
        )
        .await?;
        // D04: both inputs are skipped.
        run_rows_case(
            h,
            protocol,
            "dup_d04",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5),(1,6) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE EXCLUDED.n = 99",
            PROJECTION,
            0,
        )
        .await?;
        // D05: same as D02 with the inputs swapped.
        run_rows_case(
            h,
            protocol,
            "dup_d05",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,6),(1,5) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE EXCLUDED.n = 5",
            PROJECTION,
            1,
        )
        .await?;
        // D06: the row inserted by this command is also "modified"; a later
        // duplicate is 21000 even with the target initially empty.
        run_error_case(
            h,
            protocol,
            "dup_d06",
            DDL,
            EMPTY_SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5),(1,6) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE true",
            CARDINALITY_VIOLATION,
        )
        .await?;
        // A row inserted by this command is "modified" before the later
        // input's own WHERE; a false WHERE cannot exempt it (D07/D19).
        run_error_case(
            h,
            protocol,
            "dup_d06_false",
            DDL,
            EMPTY_SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5),(1,6) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE false",
            CARDINALITY_VIOLATION,
        )
        .await?;
        // D20a: ORDER BY ASC updates first, then the duplicate is 21000.
        run_error_case(
            h,
            protocol,
            "dup_d20_asc",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) SELECT v.id, v.n FROM (VALUES (1,5),(1,6)) \
             AS v(id,n) ORDER BY v.n ASC ON CONFLICT (id) DO UPDATE \
             SET n = EXCLUDED.n WHERE EXCLUDED.n = 5",
            CARDINALITY_VIOLATION,
        )
        .await?;
        // D20b: ORDER BY DESC skips first, then updates successfully.
        run_rows_case(
            h,
            protocol,
            "dup_d20_desc",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) SELECT v.id, v.n FROM (VALUES (1,5),(1,6)) \
             AS v(id,n) ORDER BY v.n DESC ON CONFLICT (id) DO UPDATE \
             SET n = EXCLUDED.n WHERE EXCLUDED.n = 5",
            PROJECTION,
            1,
        )
        .await?;
        // R10: the 21000 check runs before the later row's WHERE, so the
        // division by zero in that WHERE is never evaluated.
        run_error_case(
            h,
            protocol,
            "dup_r10",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5),(1,6) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n \
             WHERE CASE WHEN EXCLUDED.n = 5 THEN true ELSE 1/(EXCLUDED.n-6) = 0 END",
            CARDINALITY_VIOLATION,
        )
        .await?;
    }
    Ok(())
}

/// A `SELECT` source ordered by an expression over a column that is not in
/// the result keeps its actual order: ASC skips `20` then updates with `10`,
/// while DESC updates with `10` first and then hits its own row with `20`.
async fn select_source_order_preserves_non_projected_expression(
    h: &Harness,
) -> Result<()> {
    const TARGET_DDL: &str =
        "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (id))";
    const TARGET_SEED: &str = "INSERT INTO {t} (id, n) VALUES (1,5)";

    for protocol in Protocol::BOTH {
        // `ord` is only the source ORDER BY expression, never a projected
        // INSERT column.
        let ascending = "INSERT INTO {t} (id, n) SELECT v.id, v.n \
             FROM (VALUES (1,10,3),(1,20,1)) AS v(id,n,ord) ORDER BY v.ord ASC \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE EXCLUDED.n = 10";
        run_rows_case(
            h,
            protocol,
            "select_order_asc",
            TARGET_DDL,
            TARGET_SEED,
            ascending,
            "SELECT id, n FROM {t} ORDER BY id",
            1,
        )
        .await?;
        ensure_rows(
            h,
            &format!(
                "SELECT id, n FROM {} ORDER BY id",
                table_name("select_order_asc", protocol)
            ),
            text_rows(&[&[Some("1"), Some("10")]]),
            "non-projected ORDER BY ASC skipped the first input",
        )
        .await?;

        let descending = "INSERT INTO {t} (id, n) SELECT v.id, v.n \
             FROM (VALUES (1,10,3),(1,20,1)) AS v(id,n,ord) ORDER BY v.ord DESC \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE EXCLUDED.n = 10";
        run_error_case(
            h,
            protocol,
            "select_order_desc",
            TARGET_DDL,
            TARGET_SEED,
            descending,
            CARDINALITY_VIOLATION,
        )
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Incoming and output constraints
// ---------------------------------------------------------------------------

async fn incoming_and_output_constraint_order(h: &Harness) -> Result<()> {
    const DDL: &str = "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT NOT NULL, label TEXT, \
         PRIMARY KEY (id))";
    const SEED: &str =
        "INSERT INTO {t} (id, n, label) VALUES (1,10,'1'),(2,20,'bad'),(3,30,'3')";
    const PROJECTION: &str = "SELECT id, n, label FROM {t} ORDER BY id";

    for protocol in Protocol::BOTH {
        // R06: the skipped row's SET cast is not evaluated; the second row's
        // cast succeeds.
        run_rows_case(
            h,
            protocol,
            "types_r06",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n, label) VALUES (1,5,'bad'),(2,20,'7') \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.label::int \
             WHERE EXCLUDED.n > 10",
            PROJECTION,
            1,
        )
        .await?;
        // R07: the first row computes, the second fails; the whole statement
        // publishes nothing.
        run_error_case(
            h,
            protocol,
            "types_r07",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n, label) VALUES (1,20,'7'),(2,20,'bad') \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.label::int \
             WHERE EXCLUDED.n > 10",
            INVALID_TEXT_REPRESENTATION,
        )
        .await?;
        // Output row NOT NULL is checked only when the row is produced.
        run_rows_case(
            h,
            protocol,
            "types_output_skip",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5) \
             ON CONFLICT (id) DO UPDATE SET n = NULL WHERE false",
            PROJECTION,
            0,
        )
        .await?;
        run_error_case(
            h,
            protocol,
            "types_output_null",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5) \
             ON CONFLICT (id) DO UPDATE SET n = NULL",
            NOT_NULL_VIOLATION,
        )
        .await?;
        // A late output NOT NULL after an earlier row computed still rolls
        // the whole statement back.
        run_error_case(
            h,
            protocol,
            "types_late_null",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5),(2,20) \
             ON CONFLICT (id) DO UPDATE SET n = CASE WHEN EXCLUDED.n = 20 \
             THEN NULL ELSE EXCLUDED.n END",
            NOT_NULL_VIOLATION,
        )
        .await?;
        // Assignment type mismatch is a plan-time 42804.
        run_error_case(
            h,
            protocol,
            "types_mismatch",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5) \
             ON CONFLICT (id) DO UPDATE SET n = {t}.label",
            DATATYPE_MISMATCH,
        )
        .await?;
        // An invalid untyped literal is 22P02 before the conflict action.
        run_error_case(
            h,
            protocol,
            "types_literal",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,'bad') \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n",
            INVALID_TEXT_REPRESENTATION,
        )
        .await?;
        run_error_case(
            h,
            protocol,
            "types_division",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5) ON CONFLICT (id) \
             DO UPDATE SET n = 100 / ({t}.n - 10)",
            "22012",
        )
        .await?;
        run_error_case(
            h,
            protocol,
            "types_where",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5) ON CONFLICT (id) \
             DO UPDATE SET n = EXCLUDED.n WHERE 1",
            DATATYPE_MISMATCH,
        )
        .await?;
        // Incoming NOT NULL is checked before the conflict WHERE can skip.
        run_error_case(
            h,
            protocol,
            "types_incoming_null",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,NULL) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE false",
            NOT_NULL_VIOLATION,
        )
        .await?;

        // A new key with an incoming NOT NULL violation fails even though the
        // conflict action would only insert.
        run_error_case(
            h,
            protocol,
            "types_insert_null",
            DDL,
            SEED,
            "INSERT INTO {t} (id, label) VALUES (9, 'new') \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n",
            NOT_NULL_VIOLATION,
        )
        .await?;

        // An incoming CAST into a nullable column that the conflict action
        // never reads must still run before the skip: projection pruning may
        // not turn a PG 22P02 into a silent success.
        let target = table_name("types_prune", protocol);
        let source = table_name("types_prune_src", protocol);
        let source_ddl = format!("CREATE TABLE {source} (id BIGINT, txt TEXT)");
        let source_seed = format!("INSERT INTO {source} (id, txt) VALUES (1,'bad')");
        create_mirrored_table(h, &source_ddl, &source_ddl, &source_seed).await?;
        let target_ddl = format!(
            "CREATE TABLE {target} (id BIGINT NOT NULL, n BIGINT, extra BIGINT, \
             PRIMARY KEY (id))"
        );
        create_mirrored_table(
            h,
            &target_ddl,
            &target_ddl,
            &format!("INSERT INTO {target} (id, n, extra) VALUES (1,10,1)"),
        )
        .await?;
        let sql = format!(
            "INSERT INTO {target} (id, n, extra) \
             SELECT id, 100, txt::bigint FROM {source} \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE false"
        );
        let before = h.authoritative_snapshot().await?;
        let lake = rejection(&h.lake, &sql, protocol).await?;
        let reference = rejection(&h.reference, &sql, Protocol::Simple).await?;
        ensure_rejection(
            lake,
            reference,
            INVALID_TEXT_REPRESENTATION,
            "types_prune",
            protocol.label(),
        )?;
        let after = h.authoritative_snapshot().await?;
        ensure_snapshot_unchanged(&before, &after, "types_prune")?;

        // R09: a run-time CAST in the proposed row is still evaluated when the
        // conflict WHERE is false.
        let target = table_name("types_r09", protocol);
        let source = table_name("types_r09_src", protocol);
        let source_ddl = format!("CREATE TABLE {source} (id BIGINT, txt TEXT)");
        let source_seed = format!("INSERT INTO {source} (id, txt) VALUES (1,'bad')");
        create_mirrored_table(h, &source_ddl, &source_ddl, &source_seed).await?;
        create_mirrored_table(
            h,
            &render(DDL, &target),
            &render(DDL, &target),
            &render(SEED, &target),
        )
        .await?;
        let sql = format!(
            "INSERT INTO {target} (id, n) SELECT id, txt::int FROM {source} \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE false"
        );
        let before = h.authoritative_snapshot().await?;
        let lake = rejection(&h.lake, &sql, protocol).await?;
        let reference = rejection(&h.reference, &sql, Protocol::Simple).await?;
        ensure_rejection(
            lake,
            reference,
            INVALID_TEXT_REPRESENTATION,
            "types_r09",
            protocol.label(),
        )?;
        let after = h.authoritative_snapshot().await?;
        ensure_snapshot_unchanged(&before, &after, "types_r09")?;
        ensure_connection_usable(h, "types_r09").await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Internal marker names
// ---------------------------------------------------------------------------

/// User columns may be named like the executor's internal marker/order
/// fields. The generated fields carry their own qualifier, so neither the
/// projection nor the explicit source-order token may become ambiguous.
async fn internal_names_do_not_collide(h: &Harness) -> Result<()> {
    const DDL: &str = "CREATE TABLE {t} (id BIGINT NOT NULL, \
         \"__lakesoul_order\" BIGINT, \"__lakesoul_matched\" TEXT, n BIGINT, \
         PRIMARY KEY (id))";
    const SEED: &str = "INSERT INTO {t} (id, \"__lakesoul_order\", \
         \"__lakesoul_matched\", n) VALUES (1,100,'m',10)";
    const PROJECTION: &str = "SELECT id, \"__lakesoul_order\", \
         \"__lakesoul_matched\", n FROM {t} ORDER BY id";

    for protocol in Protocol::BOTH {
        // Unassigned user columns keep their old values.
        run_rows_case(
            h,
            protocol,
            "internal_names_keep",
            DDL,
            SEED,
            "INSERT INTO {t} (id, n) VALUES (1,5) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n",
            PROJECTION,
            1,
        )
        .await?;
        // The user columns themselves are assignable through EXCLUDED.
        run_rows_case(
            h,
            protocol,
            "internal_names_set",
            DDL,
            SEED,
            "INSERT INTO {t} (id, \"__lakesoul_order\", \"__lakesoul_matched\", n) \
             VALUES (1,7,'u',9) ON CONFLICT (id) DO UPDATE \
             SET \"__lakesoul_order\" = EXCLUDED.\"__lakesoul_order\", \
                 \"__lakesoul_matched\" = EXCLUDED.\"__lakesoul_matched\"",
            PROJECTION,
            1,
        )
        .await?;
        // A SELECT source that projects the same names also carries the
        // explicit order token without a collision.
        run_rows_case(
            h,
            protocol,
            "internal_names_select",
            DDL,
            SEED,
            "INSERT INTO {t} (id, \"__lakesoul_order\", \"__lakesoul_matched\", n) \
             SELECT id, \"__lakesoul_order\", \"__lakesoul_matched\", n \
             FROM {t} WHERE id = 1 \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n",
            PROJECTION,
            1,
        )
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Names and aliases
// ---------------------------------------------------------------------------

async fn alias_and_ambiguous_names(h: &Harness) -> Result<()> {
    const DDL: &str =
        "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT, label TEXT, PRIMARY KEY (id))";
    const SEED: &str = "INSERT INTO {t} (id, n, label) VALUES (1,10,'old')";
    const PROJECTION: &str = "SELECT id, n, label FROM {t} ORDER BY id";

    for protocol in Protocol::BOTH {
        run_rows_case(
            h,
            protocol,
            "alias_ok",
            DDL,
            SEED,
            "INSERT INTO {t} AS x VALUES (1,5,'new') \
             ON CONFLICT (id) DO UPDATE SET n = x.n + EXCLUDED.n",
            PROJECTION,
            1,
        )
        .await?;
        run_error_case(
            h,
            protocol,
            "alias_hides_name",
            DDL,
            SEED,
            "INSERT INTO {t} AS x VALUES (1,5,'new') \
             ON CONFLICT (id) DO UPDATE SET n = {t}.n + EXCLUDED.n",
            UNDEFINED_TABLE,
        )
        .await?;
        run_error_case(
            h,
            protocol,
            "bare_ambiguous",
            DDL,
            SEED,
            "INSERT INTO {t} VALUES (1,5,'new') \
             ON CONFLICT (id) DO UPDATE SET n = n + EXCLUDED.n",
            AMBIGUOUS_COLUMN,
        )
        .await?;
        run_error_case(
            h,
            protocol,
            "qualified_set_target",
            DDL,
            SEED,
            "INSERT INTO {t} AS x VALUES (1,5,'new') \
             ON CONFLICT (id) DO UPDATE SET x.n = EXCLUDED.n",
            UNDEFINED_COLUMN,
        )
        .await?;
    }

    // A target actually named `excluded` without an alias is 42P09; the same
    // table with an alias is usable.
    for protocol in Protocol::BOTH {
        let table = "excluded".to_string();
        create_mirrored_table(
            h,
            &format!(
                "CREATE TABLE {table} (id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (id))"
            ),
            &format!(
                "CREATE TABLE {table} (id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (id))"
            ),
            &format!("INSERT INTO {table} (id, n) VALUES (1,10)"),
        )
        .await?;
        reject_lake_only(
            h,
            protocol,
            &format!(
                "INSERT INTO {table} VALUES (1,5) \
                 ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n"
            ),
            AMBIGUOUS_ALIAS,
            "excluded-name",
        )
        .await?;
        let sql = format!(
            "INSERT INTO {table} AS x VALUES (1,5) \
             ON CONFLICT (id) DO UPDATE SET n = x.n + EXCLUDED.n"
        );
        let lake = match protocol {
            Protocol::Simple => simple_tag_rows(&h.lake, &sql).await?,
            Protocol::Extended => extended_rows(h, &sql).await?,
        };
        let reference = simple_tag_rows(&h.reference, &sql).await?;
        ensure_eq("excluded-name alias rows", lake, reference)?;
        ensure_eq("excluded-name alias expected", lake, 1)?;
        ensure_relation_parity(
            h,
            &format!("SELECT id, n FROM {table} ORDER BY id"),
            "excluded-name alias",
        )
        .await?;
        h.lake
            .simple_query(&format!("DROP TABLE {table}"))
            .await
            .context("drop excluded fixture")?;
        h.reference
            .simple_query(&format!("DROP TABLE {table}"))
            .await
            .context("drop reference excluded fixture")?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Prepared statements
// ---------------------------------------------------------------------------

async fn prepared_upsert_rebinds_and_reads_new_commits(h: &Harness) -> Result<()> {
    const DDL: &str =
        "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT, label TEXT, PRIMARY KEY (id))";
    const SEED: &str =
        "INSERT INTO {t} (id, n, label) VALUES (1,10,'old'),(3,30,'three')";
    let table = "upsert_prepared_extended".to_string();
    create_mirrored_table(
        h,
        &render(DDL, &table),
        &render(DDL, &table),
        &render(SEED, &table),
    )
    .await?;
    let projection = format!("SELECT id, n, label FROM {table} ORDER BY id");

    // The server must infer both parameters from the target columns.
    let statement = h
        .lake
        .prepare(&format!(
            "INSERT INTO {table} (id, n) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET n = {table}.n + EXCLUDED.n"
        ))
        .await
        .context("extended Parse of a parameterized upsert")?;
    ensure_eq(
        "inferred upsert parameter types",
        statement.params().to_vec(),
        vec![Type::INT8, Type::INT8],
    )?;

    let first = h
        .lake
        .execute(&statement, &[&1i64, &5i64])
        .await
        .context("first Execute of the prepared upsert")?;
    ensure_eq("first prepared upsert rows", first, 1)?;
    let reference = simple_tag_rows(
        &h.reference,
        &format!("INSERT INTO {table} (id, n) VALUES (1, 5) ON CONFLICT (id) DO UPDATE SET n = {table}.n + EXCLUDED.n"),
    )
    .await?;
    ensure_eq("first prepared upsert reference rows", reference, 1)?;
    ensure_relation_parity(h, &projection, "first prepared upsert").await?;
    ensure_eq(
        "first prepared upsert value",
        lake_i64(h, &format!("SELECT n FROM {table} WHERE id = 1")).await?,
        15,
    )?;

    // Another writer commits a new value while this connection still holds the
    // prepared statement. The next Execute must use that value and the new
    // snapshot.
    h.lake
        .simple_query(&format!("UPDATE {table} SET n = 100 WHERE id = 3"))
        .await
        .context("external LakeSoul UPDATE before the second Execute")?;
    h.reference
        .simple_query(&format!("UPDATE {table} SET n = 100 WHERE id = 3"))
        .await
        .context("external PostgreSQL UPDATE before the second Execute")?;
    let second = h
        .lake
        .execute(&statement, &[&3i64, &7i64])
        .await
        .context("second Execute of the prepared upsert")?;
    ensure_eq("second prepared upsert rows", second, 1)?;
    simple_tag_rows(
        &h.reference,
        &format!(
            "INSERT INTO {table} (id, n) VALUES (3, 7) ON CONFLICT (id) \
             DO UPDATE SET n = {table}.n + EXCLUDED.n"
        ),
    )
    .await?;
    ensure_eq(
        "second prepared upsert reads the external commit",
        lake_i64(h, &format!("SELECT n FROM {table} WHERE id = 3")).await?,
        107,
    )?;
    ensure_relation_parity(h, &projection, "second prepared upsert").await?;

    // A failing statement does not poison the prepared statement or the
    // connection.
    let error = h
        .lake
        .simple_query(&format!(
            "INSERT INTO {table} (id, n) VALUES (1, 1) \
             ON CONFLICT (id) DO UPDATE SET n = {table}.label"
        ))
        .await
        .expect_err("an assignment type mismatch must fail");
    ensure_sqlstate(&error, DATATYPE_MISMATCH, "prepared upsert error recovery")?;
    let third = h
        .lake
        .execute(&statement, &[&1i64, &2i64])
        .await
        .context("third Execute after a failed statement")?;
    ensure_eq("third prepared upsert rows", third, 1)?;
    simple_tag_rows(
        &h.reference,
        &format!(
            "INSERT INTO {table} (id, n) VALUES (1, 2) ON CONFLICT (id) \
             DO UPDATE SET n = {table}.n + EXCLUDED.n"
        ),
    )
    .await?;
    ensure_eq(
        "third prepared upsert value",
        lake_i64(h, &format!("SELECT n FROM {table} WHERE id = 1")).await?,
        17,
    )?;
    ensure_relation_parity(h, &projection, "third prepared upsert").await?;
    ensure_connection_usable(h, "prepared upsert").await?;
    Ok(())
}

async fn prepared_upsert_after_drop_and_recreate(h: &Harness) -> Result<()> {
    const REBUILD: &str = "upsert_rebuild_target";
    let peer = h.connect_lake().await?;
    peer.simple_query(&format!(
        "CREATE TABLE {REBUILD} (\
           id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (id))"
    ))
    .await
    .context("rebuild fixture CREATE")?;
    peer.simple_query(&format!("INSERT INTO {REBUILD} (id, n) VALUES (1, 10)"))
        .await
        .context("rebuild fixture seed")?;
    let original_id = table_id(h, REBUILD).await?;

    let statement = h
        .lake
        .prepare_typed(
            &format!(
                "INSERT INTO {REBUILD} (id, n) VALUES ($1, $2) \
                 ON CONFLICT (id) DO UPDATE SET n = {REBUILD}.n + EXCLUDED.n"
            ),
            &[Type::INT8, Type::INT8],
        )
        .await
        .context("prepare upsert before the peer DROP")?;

    peer.simple_query(&format!("DROP TABLE {REBUILD}"))
        .await
        .context("peer DROP of the upsert target")?;
    peer.simple_query(&format!(
        "CREATE TABLE {REBUILD} (\
           id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (id))"
    ))
    .await
    .context("peer CREATE of the same name")?;
    peer.simple_query(&format!("INSERT INTO {REBUILD} (id, n) VALUES (1, 99)"))
        .await
        .context("peer seed of the recreated table")?;
    let recreated_id = table_id(h, REBUILD).await?;
    ensure!(recreated_id != original_id, "the table_id must change");

    let dropped_state = table_metadata_state(h, &original_id).await?;
    let recreated_state = table_metadata_state(h, &recreated_id).await?;
    let files_before = h.table_file_bytes(REBUILD).await?;
    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .execute(&statement, &[&1i64, &5i64])
        .await
        .expect_err("a prepared upsert must not publish to a recreated table");
    ensure_sqlstate(
        &error,
        UNDEFINED_TABLE,
        "prepared upsert into a dropped and recreated table",
    )?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "stale prepared upsert")?;
    ensure_eq(
        "dropped table gained no publication",
        table_metadata_state(h, &original_id).await?,
        dropped_state,
    )?;
    ensure_eq(
        "recreated table gained no publication",
        table_metadata_state(h, &recreated_id).await?,
        recreated_state,
    )?;
    ensure_eq(
        "recreated table files",
        h.table_file_bytes(REBUILD).await?,
        files_before,
    )?;
    ensure_rows(
        h,
        &format!("SELECT id, n FROM {REBUILD} ORDER BY id"),
        text_rows(&[&[Some("1"), Some("99")]]),
        "the recreated table keeps the peer's row",
    )
    .await?;
    ensure_connection_usable(h, "stale prepared upsert").await?;

    // A plain upsert resolves the name and reaches the recreated table.
    ensure_eq(
        "plain upsert against the recreated table",
        simple_tag_rows(
            &h.lake,
            &format!(
                "INSERT INTO {REBUILD} (id, n) VALUES (1, 1) \
                 ON CONFLICT (id) DO UPDATE SET n = {REBUILD}.n + EXCLUDED.n"
            ),
        )
        .await?,
        1,
    )?;
    ensure_eq(
        "plain upsert result",
        lake_i64(h, &format!("SELECT n FROM {REBUILD} WHERE id = 1")).await?,
        100,
    )?;

    // Recreated as a CDC table: the stale prepared plan must not bypass the
    // re-read table properties.
    peer.simple_query(&format!("DROP TABLE {REBUILD}"))
        .await
        .context("peer DROP of the recreated table")?;
    peer.simple_query(&format!(
        "CREATE TABLE {REBUILD} (\
           id BIGINT NOT NULL, n BIGINT, op TEXT, PRIMARY KEY (id)) \
         WITH (use_cdc = true, lakesoul_cdc_change_column = 'op')"
    ))
    .await
    .context("peer CREATE of a CDC table under the same name")?;
    peer.simple_query(&format!(
        "INSERT INTO {REBUILD} (id, n, op) VALUES (1, 10, 'insert')"
    ))
    .await
    .context("peer seed of the recreated CDC table")?;
    let cdc_id = table_id(h, REBUILD).await?;
    let cdc_state = table_metadata_state(h, &cdc_id).await?;
    let cdc_files = h.table_file_bytes(REBUILD).await?;
    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .execute(&statement, &[&1i64, &5i64])
        .await
        .expect_err("a stale prepared upsert must not write a CDC table");
    ensure_sqlstate(
        &error,
        UNDEFINED_TABLE,
        "prepared upsert into a table recreated as CDC",
    )?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "stale prepared upsert into CDC")?;
    ensure_eq(
        "recreated CDC table gained no publication",
        table_metadata_state(h, &cdc_id).await?,
        cdc_state,
    )?;
    ensure_eq(
        "recreated CDC table files",
        h.table_file_bytes(REBUILD).await?,
        cdc_files,
    )?;
    ensure_eq(
        "the recreated CDC table keeps its seeded row",
        lake_i64(h, &format!("SELECT count(*) FROM {REBUILD}")).await?,
        1,
    )?;
    ensure_connection_usable(h, "stale prepared upsert into CDC").await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Empty and fully skipped statements
// ---------------------------------------------------------------------------

async fn empty_and_all_skipped_change_nothing(h: &Harness) -> Result<()> {
    const DDL: &str = "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (id))";
    const SEED: &str = "INSERT INTO {t} (id, n) VALUES (1,10)";
    const PROJECTION: &str = "SELECT id, n FROM {t} ORDER BY id";

    for protocol in Protocol::BOTH {
        let table = table_name("empty_state", protocol);
        create_mirrored_table(
            h,
            &render(DDL, &table),
            &render(DDL, &table),
            &render(SEED, &table),
        )
        .await?;
        let id = table_id(h, &table).await?;
        let commits = committed_commits(h, &id).await?;
        let files = h.table_file_bytes(&table).await?;
        for (sql, case) in [
            (
                format!(
                    "INSERT INTO {table} (id, n) SELECT id, n FROM {table} \
                     WHERE false ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n"
                ),
                "empty source",
            ),
            (
                format!(
                    "INSERT INTO {table} (id, n) VALUES (1, 5) \
                     ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE false"
                ),
                "conflict WHERE false",
            ),
            (
                format!(
                    "INSERT INTO {table} (id, n) VALUES (1, 5) \
                     ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n WHERE NULL"
                ),
                "conflict WHERE NULL",
            ),
        ] {
            let before = h.authoritative_snapshot().await?;
            let lake = match protocol {
                Protocol::Simple => simple_tag_rows(&h.lake, &sql).await?,
                Protocol::Extended => extended_rows(h, &sql).await?,
            };
            let reference = simple_tag_rows(&h.reference, &sql).await?;
            ensure_eq(&format!("{case} row count"), lake, reference)?;
            ensure_eq(&format!("{case} expected"), lake, 0)?;
            let after = h.authoritative_snapshot().await?;
            ensure_snapshot_unchanged(&before, &after, case)?;
            ensure_eq(
                &format!("{case} commits"),
                committed_commits(h, &id).await?,
                commits,
            )?;
            ensure_eq(
                &format!("{case} files"),
                h.table_file_bytes(&table).await?,
                files.clone(),
            )?;
        }
        ensure_relation_parity(h, &render(PROJECTION, &table), "empty/skipped").await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Compatibility regressions
// ---------------------------------------------------------------------------

async fn legacy_full_row_and_plain_insert_regressions(h: &Harness) -> Result<()> {
    const DDL: &str =
        "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT, label TEXT, PRIMARY KEY (id))";
    const SEED: &str = "INSERT INTO {t} (id, n, label) VALUES (1,10,'old')";
    const PROJECTION: &str = "SELECT id, n, label FROM {t} ORDER BY id";

    for protocol in Protocol::BOTH {
        // The pre-existing full-row `EXCLUDED` form keeps replacing the row.
        run_rows_case(
            h,
            protocol,
            "legacy_full_row",
            DDL,
            SEED,
            "INSERT INTO {t} VALUES (1,20,'new') \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n, label = EXCLUDED.label",
            PROJECTION,
            1,
        )
        .await?;
    }

    // Plain INSERT and DO NOTHING still have their own contracts after the
    // DO UPDATE path was replaced.
    let table = "upsert_plain_regression".to_string();
    create_mirrored_table(
        h,
        &render(DDL, &table),
        &render(DDL, &table),
        &render(SEED, &table),
    )
    .await?;
    let inserted = format!("INSERT INTO {table} (id, n, label) VALUES (2,20,'two')");
    ensure_eq(
        "plain INSERT rows",
        simple_tag_rows(&h.lake, &inserted).await?,
        simple_tag_rows(&h.reference, &inserted).await?,
    )?;
    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .simple_query(&format!(
            "INSERT INTO {table} (id, n, label) VALUES (1,99,'duplicate')"
        ))
        .await
        .expect_err("a plain INSERT on an existing key must be 23505");
    ensure_sqlstate(&error, "23505", "plain INSERT duplicate key")?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "plain INSERT duplicate key")?;
    let nothing = format!(
        "INSERT INTO {table} (id, n, label) VALUES (1,99,'skip'),(3,30,'new') \
         ON CONFLICT (id) DO NOTHING"
    );
    let lake_nothing = simple_tag_rows(&h.lake, &nothing).await?;
    let reference_nothing = simple_tag_rows(&h.reference, &nothing).await?;
    ensure_eq("DO NOTHING rows", lake_nothing, reference_nothing)?;
    ensure_eq("DO NOTHING expected", lake_nothing, 1)?;
    ensure_relation_parity(h, &render(PROJECTION, &table), "plain/DO NOTHING").await?;

    // The legacy full-row form stays available on a CDC table; the general
    // old-value form is refused there (`refused_upserts_change_nothing`).
    let cdc = "upsert_cdc_legacy".to_string();
    h.lake
        .simple_query(&format!(
            "CREATE TABLE {cdc} (id BIGINT NOT NULL, val BIGINT, op TEXT, \
             PRIMARY KEY (id)) WITH (use_cdc = true, \
             lakesoul_cdc_change_column = 'op')"
        ))
        .await
        .context("create CDC legacy fixture")?;
    h.lake
        .simple_query(&format!("INSERT INTO {cdc} VALUES (1,10,'insert')"))
        .await
        .context("seed CDC legacy fixture")?;
    ensure_eq(
        "CDC legacy full-row upsert rows",
        simple_tag_rows(
            &h.lake,
            &format!(
                "INSERT INTO {cdc} VALUES (1,20,'insert') \
                 ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val, op = EXCLUDED.op"
            ),
        )
        .await?,
        1,
    )?;
    ensure_eq(
        "CDC legacy upsert value",
        lake_i64(h, &format!("SELECT val FROM {cdc} WHERE id = 1")).await?,
        20,
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

async fn refused_upserts_change_nothing(h: &Harness) -> Result<()> {
    const DDL: &str =
        "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT, label TEXT, PRIMARY KEY (id))";
    const SEED: &str = "INSERT INTO {t} (id, n, label) VALUES (1,10,'old')";

    for protocol in Protocol::BOTH {
        // Missing conflict target is PostgreSQL's 42601.
        run_error_case(
            h,
            protocol,
            "refuse_missing_target",
            DDL,
            SEED,
            "INSERT INTO {t} VALUES (1,5,'new') \
             ON CONFLICT DO UPDATE SET n = EXCLUDED.n",
            SYNTAX_ERROR,
        )
        .await?;
        // A table without a primary key has no unique constraint for the
        // target: 42P10 on both sides.
        run_error_case(
            h,
            protocol,
            "refuse_no_pk",
            "CREATE TABLE {t} (id BIGINT NOT NULL, n BIGINT)",
            "INSERT INTO {t} (id, n) VALUES (1,10)",
            "INSERT INTO {t} (id, n) VALUES (1,5) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n",
            NO_UNIQUE_CONSTRAINT,
        )
        .await?;
    }

    for protocol in Protocol::BOTH {
        // Key assignment is an intentional difference from PostgreSQL: a key
        // change would leave the old row behind.
        let table = table_name("refuse_key_change", protocol);
        create_mirrored_table(
            h,
            &render(DDL, &table),
            &render(DDL, &table),
            &render(SEED, &table),
        )
        .await?;
        reject_lake_only(
            h,
            protocol,
            &format!(
                "INSERT INTO {table} (id, n) VALUES (2,5) \
                 ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id"
            ),
            FEATURE_NOT_SUPPORTED,
            "refuse-key-change",
        )
        .await?;
        // A conflict target that is not the primary key remains refused.
        reject_lake_only(
            h,
            protocol,
            &format!(
                "INSERT INTO {table} (id, n) VALUES (1,5) \
                 ON CONFLICT (n) DO UPDATE SET n = EXCLUDED.n"
            ),
            FEATURE_NOT_SUPPORTED,
            "refuse-non-pk-target",
        )
        .await?;
        // Named constraints are not opened.
        reject_lake_only(
            h,
            protocol,
            &format!(
                "INSERT INTO {table} (id, n) VALUES (1,5) \
                 ON CONFLICT ON CONSTRAINT {table}_pkey DO UPDATE SET n = EXCLUDED.n"
            ),
            FEATURE_NOT_SUPPORTED,
            "refuse-named-constraint",
        )
        .await?;
        // RETURNING has no row stream.
        reject_lake_only(
            h,
            protocol,
            &format!(
                "INSERT INTO {table} (id, n) VALUES (1,5) \
                 ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n RETURNING id"
            ),
            FEATURE_NOT_SUPPORTED,
            "refuse-returning",
        )
        .await?;
    }

    // appendOnly tables have no conflict-update semantics.
    let append = "upsert_refuse_append_only".to_string();
    h.lake
        .simple_query(&format!(
            "CREATE TABLE {append} (id BIGINT NOT NULL, n BIGINT, PRIMARY KEY (id)) \
             WITH (append_only = true)"
        ))
        .await
        .context("create appendOnly refusal fixture")?;
    h.lake
        .simple_query(&format!("INSERT INTO {append} (id, n) VALUES (1, 10)"))
        .await
        .context("seed appendOnly fixture")?;
    for protocol in Protocol::BOTH {
        reject_lake_only(
            h,
            protocol,
            &format!(
                "INSERT INTO {append} (id, n) VALUES (1,5) \
                 ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n"
            ),
            FEATURE_NOT_SUPPORTED,
            "refuse-append-only",
        )
        .await?;
    }

    // A CDC table keeps only the legacy full-row form; a partial general
    // upsert is refused.
    let cdc = "upsert_refuse_cdc".to_string();
    h.lake
        .simple_query(&format!(
            "CREATE TABLE {cdc} (id BIGINT NOT NULL, val BIGINT, op TEXT, \
             PRIMARY KEY (id)) WITH (use_cdc = true, \
             lakesoul_cdc_change_column = 'op')"
        ))
        .await
        .context("create CDC refusal fixture")?;
    h.lake
        .simple_query(&format!("INSERT INTO {cdc} VALUES (1,10,'insert')"))
        .await
        .context("seed CDC refusal fixture")?;
    for protocol in Protocol::BOTH {
        reject_lake_only(
            h,
            protocol,
            &format!(
                "INSERT INTO {cdc} (id, val) VALUES (1,5) \
                 ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val"
            ),
            FEATURE_NOT_SUPPORTED,
            "refuse-cdc-partial",
        )
        .await?;
    }

    // A range-partitioned target keeps its existing refusal.
    let ranges = "upsert_refuse_range".to_string();
    h.lake
        .simple_query(&format!(
            "CREATE TABLE {ranges} (part BIGINT NOT NULL, id BIGINT NOT NULL, \
             n BIGINT, PRIMARY KEY (id)) PARTITIONED BY (part)"
        ))
        .await
        .context("create range refusal fixture")?;
    h.lake
        .simple_query(&format!(
            "INSERT INTO {ranges} (part, id, n) VALUES (0, 1, 10)"
        ))
        .await
        .context("seed range refusal fixture")?;
    for protocol in Protocol::BOTH {
        reject_lake_only(
            h,
            protocol,
            &format!(
                "INSERT INTO {ranges} (part, id, n) VALUES (0, 1, 5) \
                 ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n"
            ),
            FEATURE_NOT_SUPPORTED,
            "refuse-range",
        )
        .await?;
    }

    // Explicit transactions and multi-statement simple-query batches.
    h.lake
        .batch_execute("BEGIN")
        .await
        .context("begin refusal transaction")?;
    let target = "upsert_refuse_txn";
    let error = h
        .lake
        .simple_query(&format!(
            "INSERT INTO {target} (id, n) VALUES (1,5) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n"
        ))
        .await
        .expect_err("an upsert inside a transaction block must be refused");
    ensure_sqlstate(&error, FEATURE_NOT_SUPPORTED, "upsert in a transaction")?;
    h.lake
        .batch_execute("ROLLBACK")
        .await
        .context("rollback refusal transaction")?;
    ensure_connection_usable(h, "upsert transaction refusal").await?;

    let table = "upsert_refuse_batch";
    create_mirrored_table(
        h,
        &render(DDL, table),
        &render(DDL, table),
        &render(SEED, table),
    )
    .await?;
    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .simple_query(&format!(
            "SELECT 1; INSERT INTO {table} (id, n) VALUES (1,5) \
             ON CONFLICT (id) DO UPDATE SET n = EXCLUDED.n"
        ))
        .await
        .expect_err("a simple-query batch containing an upsert must be refused");
    ensure_sqlstate(&error, FEATURE_NOT_SUPPORTED, "upsert in a batch")?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "upsert in a batch")?;
    ensure_connection_usable(h, "upsert batch refusal").await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Distributed publication
// ---------------------------------------------------------------------------

async fn distributed_upsert_publishes_on_workers(h: &Harness) -> Result<()> {
    let id = table_id(h, "pk_items").await?;
    let commits_before = committed_commits(h, &id).await?;
    let files_before = h.table_file_bytes("pk_items").await?;
    let rows = h
        .lake
        .execute(
            "INSERT INTO pk_items (id, val, label) \
             VALUES (1, 100, 'distributed'), (4, 40, 'new') \
             ON CONFLICT (id) DO UPDATE SET val = pk_items.val + EXCLUDED.val",
            &[],
        )
        .await
        .context("distributed upsert through the workers")?;
    ensure_eq("distributed upsert rows", rows, 2)?;
    ensure_eq(
        "distributed upsert commits",
        committed_commits(h, &id).await?,
        commits_before + 1,
    )?;
    ensure_previous_files_unchanged(
        "distributed upsert",
        &files_before,
        &h.table_file_bytes("pk_items").await?,
    )?;
    ensure_committed_files_exist(
        h,
        &commit_file_paths(h, &id).await?,
        "distributed upsert",
    )?;
    let other = h.connect_lake().await?;
    let value: i64 = other
        .query_one("SELECT val FROM pk_items WHERE id = 1", &[])
        .await?
        .get(0);
    ensure_eq("distributed upsert value", value, 199)?;
    ensure_eq(
        "distributed upsert keeps the row count",
        lake_i64(h, "SELECT count(*) FROM pk_items").await?,
        4,
    )?;
    eprintln!("PASS case=distributed-upsert rows=2 commits=+1 previous-files=unchanged");
    Ok(())
}

// ---------------------------------------------------------------------------
// Indexed upsert
// ---------------------------------------------------------------------------

async fn indexed_upsert_scenario(h: &Harness) -> Result<()> {
    indexed_vector_upsert(h).await?;
    indexed_text_upsert(h).await?;
    index_maintenance_failure_keeps_committed_upsert(h).await
}

const INDEX_UPDATED_ID: i64 = 0;
const NEW_VECTOR: [f32; DIM] = [350.0, 0.0, 0.5, -0.5, 1.0, -1.0, 0.25, -0.25];
const NEW_VECTOR_TEXT: &str = "[350,0,0.5,-0.5,1,-1,0.25,-0.25]";
const ZERO_VECTOR: [f32; DIM] = [0.0; DIM];
const ZERO_VECTOR_TEXT: &str = "[0,0,0,0,0,0,0,0]";
const NEW_VECTOR_SQL: &str = "ARRAY[350.0::float4, 0.0::float4, 0.5::float4, \
     -0.5::float4, 1.0::float4, -1.0::float4, 0.25::float4, -0.25::float4]";

async fn indexed_vector_upsert(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(&format!("SET ivfflat.probes = {NLIST}"))
        .await
        .context("set ivfflat.probes to the fixture's cluster count")?;

    let files_before = h.table_file_bytes(ANN_TABLE).await?;
    let data_before = select_files(&files_before, IndexKind::Vector.directory(), false);
    let index_before = select_files(&files_before, IndexKind::Vector.directory(), true);
    ensure!(!data_before.is_empty(), "indexed fixture data files");
    ensure!(!index_before.is_empty(), "indexed fixture index artifacts");

    let zero_before = ann_query(ZERO_VECTOR_TEXT, 1);
    ensure_vector_index_route(h, &zero_before, "indexed-upsert-before").await?;
    ensure_eq(
        "pre-upsert zero-vector top1",
        lake_ids(h, &zero_before).await?,
        vec![INDEX_UPDATED_ID],
    )?;

    let rows = h
        .lake
        .execute(
            &format!(
                "INSERT INTO {ANN_TABLE} (id, vec) VALUES ({INDEX_UPDATED_ID}, {NEW_VECTOR_SQL}) \
                 ON CONFLICT (id) DO UPDATE SET vec = EXCLUDED.vec"
            ),
            &[],
        )
        .await
        .context("indexed vector upsert")?;
    ensure_eq("indexed vector upsert rows", rows, 1)?;
    ensure_eq(
        "indexed fixture keeps every row",
        lake_i64(h, &format!("SELECT count(*) FROM {ANN_TABLE}")).await?,
        CORPUS_ROWS as i64,
    )?;

    let new_k1 = ann_query(NEW_VECTOR_TEXT, 1);
    ensure_vector_index_route(h, &new_k1, "indexed-upsert-new-exact").await?;
    ensure_eq(
        "upserted row is nearest to its new vector",
        lake_ids(h, &new_k1).await?,
        vec![INDEX_UPDATED_ID],
    )?;
    let zero_after = ann_query(ZERO_VECTOR_TEXT, 1);
    ensure_vector_index_route(h, &zero_after, "indexed-upsert-old-exact").await?;
    let old_ids = lake_ids(h, &zero_after).await?;
    ensure_eq(
        "old-vector top1 is the exact live nearest",
        old_ids.clone(),
        exact_top_k(true, &ZERO_VECTOR, 1)?,
    )?;
    ensure!(
        !old_ids.contains(&INDEX_UPDATED_ID),
        "the superseded vector must not return the upserted row, actual {old_ids:?}"
    );

    h.lake
        .batch_execute("SET lakesoul_vector_search.rerank = 'index'")
        .await
        .context("switch vector search to index-only ordering")?;
    ensure_vector_index_route(h, &new_k1, "indexed-upsert-new-index-only").await?;
    ensure_eq(
        "the index ranks the upserted row first for its new vector",
        lake_ids(h, &new_k1).await?,
        vec![INDEX_UPDATED_ID],
    )?;
    h.lake
        .batch_execute("SET lakesoul_vector_search.rerank = 'exact'")
        .await
        .context("restore exact re-ranking")?;

    let files_after = h.table_file_bytes(ANN_TABLE).await?;
    ensure_previous_files_unchanged(
        "indexed vector upsert data files",
        &data_before,
        &select_files(&files_after, IndexKind::Vector.directory(), false),
    )?;
    ensure!(
        select_files(&files_after, IndexKind::Vector.directory(), true) != index_before,
        "the upsert must publish changed vector index artifacts"
    );
    eprintln!(
        "PASS case=indexed-vector-upsert id={INDEX_UPDATED_ID} \
         exact-rerank=no-stale-result index-only=new-vector-first \
         index-artifacts=advanced"
    );
    Ok(())
}

async fn index_maintenance_failure_keeps_committed_upsert(h: &Harness) -> Result<()> {
    let id = table_id(h, ANN_TABLE).await?;
    let commits_before = committed_commits(h, &id).await?;
    let data_before = select_files(
        &h.table_file_bytes(ANN_TABLE).await?,
        IndexKind::Vector.directory(),
        false,
    );
    let directory = h
        .warehouse_dir
        .join(&h.lake_namespace)
        .join(ANN_TABLE)
        .join(IndexKind::Vector.directory());
    let saved = directory.with_file_name("saved-vector-index");
    std::fs::rename(&directory, &saved)?;
    if let Err(error) = std::fs::write(&directory, b"block index writes, not data writes")
    {
        std::fs::rename(&saved, &directory)?;
        return Err(error.into());
    }
    let upsert = h
        .lake
        .execute(
            &format!(
                "INSERT INTO {ANN_TABLE} (id, vec) VALUES ({INDEX_UPDATED_ID}, \
                 ARRAY[700.0::float4,0.0::float4,0.5::float4,-0.5::float4,\
                 1.0::float4,-1.0::float4,0.25::float4,-0.25::float4]) \
                 ON CONFLICT (id) DO UPDATE SET vec = EXCLUDED.vec"
            ),
            &[],
        )
        .await;
    std::fs::remove_file(&directory)?;
    std::fs::rename(&saved, &directory)?;
    let error = upsert.expect_err("index storage failure must reach the client");
    error
        .as_db_error()
        .context("index failure must be a server error, not a lost connection")?;

    ensure_eq(
        "index failure occurs after data publication",
        committed_commits(h, &id).await?,
        commits_before + 1,
    )?;
    let distance: f64 = h
        .lake
        .query_one(
            &format!(
                "SELECT CAST(vec <-> '[700,0,0.5,-0.5,1,-1,0.25,-0.25]'::vector({DIM}) \
                 AS DOUBLE PRECISION) FROM {ANN_TABLE} WHERE id={INDEX_UPDATED_ID}"
            ),
            &[],
        )
        .await?
        .get(0);
    ensure_eq("committed vector remains visible", distance, 0.0)?;
    ensure_eq(
        "index error retains every row",
        lake_i64(h, &format!("SELECT count(*) FROM {ANN_TABLE}")).await?,
        CORPUS_ROWS as i64,
    )?;
    ensure_previous_files_unchanged(
        "index error retains previous data files",
        &data_before,
        &select_files(
            &h.table_file_bytes(ANN_TABLE).await?,
            IndexKind::Vector.directory(),
            false,
        ),
    )?;
    ensure_committed_files_exist(
        h,
        &commit_file_paths(h, &id).await?,
        "post-publication index error",
    )?;
    eprintln!(
        "PASS case=upsert-index-maintenance-failure server-error=true \
         data-commits=+1 new-vector=visible committed-files=retained"
    );
    Ok(())
}

async fn indexed_text_upsert(h: &Harness) -> Result<()> {
    let files_before = h.table_file_bytes(TEXT_TABLE).await?;
    let data_before = select_files(&files_before, IndexKind::Text.directory(), false);
    let index_before = select_files(&files_before, IndexKind::Text.directory(), true);
    ensure!(!index_before.is_empty(), "text fixture index artifacts");

    let apple = text_query("apple");
    ensure_text_index_route(h, &apple, "indexed-text-upsert-before").await?;
    ensure_eq(
        "pre-upsert apple rows",
        lake_ids(h, &apple).await?,
        vec![1, 4],
    )?;

    let rows = h
        .lake
        .execute(
            &format!(
                "INSERT INTO {TEXT_TABLE} (id, body) VALUES (1, 'elderberry pie') \
                 ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body"
            ),
            &[],
        )
        .await
        .context("indexed text upsert")?;
    ensure_eq("indexed text upsert rows", rows, 1)?;

    let elderberry = text_query("elderberry");
    ensure_text_index_route(h, &elderberry, "indexed-text-upsert-new").await?;
    ensure_eq(
        "upserted row matches its new term",
        lake_ids(h, &elderberry).await?,
        vec![1],
    )?;
    ensure_text_index_route(h, &apple, "indexed-text-upsert-old").await?;
    ensure_eq(
        "apple keeps only the untouched row",
        lake_ids(h, &apple).await?,
        vec![4],
    )?;

    let files_after = h.table_file_bytes(TEXT_TABLE).await?;
    ensure_previous_files_unchanged(
        "indexed text upsert data files",
        &data_before,
        &select_files(&files_after, IndexKind::Text.directory(), false),
    )?;
    ensure!(
        select_files(&files_after, IndexKind::Text.directory(), true) != index_before,
        "the upsert must publish changed text index artifacts"
    );
    eprintln!("PASS case=indexed-text-upsert id=1 apple->elderberry index=advanced");
    Ok(())
}

/// `SELECT id ... ORDER BY vec <-> query LIMIT k` over the indexed fixture.
fn ann_query(query: &str, k: usize) -> String {
    format!(
        "SELECT id FROM public.{ANN_TABLE} \
         ORDER BY vec <-> '{query}'::vector({DIM}) LIMIT {k}"
    )
}

fn text_query(term: &str) -> String {
    format!(
        "SELECT id FROM public.{TEXT_TABLE} \
         WHERE text_match(body, '{term}') ORDER BY id LIMIT 10"
    )
}

fn select_files(
    files: &BTreeMap<String, Vec<u8>>,
    directory: &str,
    keep: bool,
) -> BTreeMap<String, Vec<u8>> {
    files
        .iter()
        .filter(|(path, _)| path.contains(directory) == keep)
        .map(|(path, bytes)| (path.clone(), bytes.clone()))
        .collect()
}

async fn lake_ids(h: &Harness, sql: &str) -> Result<Vec<i64>> {
    Ok(h.lake
        .query(sql, &[])
        .await
        .with_context(|| format!("read {sql}"))?
        .into_iter()
        .map(|row| row.get::<_, i64>(0))
        .collect())
}

fn vector_l2(a: &[f32; DIM], b: &[f32; DIM]) -> f64 {
    a.iter()
        .zip(b)
        .map(|(&a, &b)| (f64::from(a) - f64::from(b)).powi(2))
        .sum::<f64>()
        .sqrt()
}

fn exact_top_k(updated: bool, query: &[f32; DIM], k: usize) -> Result<Vec<i64>> {
    let mut ranked: Vec<(i64, f64)> = (0..CORPUS_ROWS)
        .map(|id| {
            let vector = if updated && id as i64 == INDEX_UPDATED_ID {
                NEW_VECTOR
            } else {
                corpus_vector(id)
            };
            (id as i64, vector_l2(&vector, query))
        })
        .collect();
    ranked.sort_by(|a, b| a.1.total_cmp(&b.1));
    ensure!(
        ranked
            .windows(2)
            .take(k)
            .all(|pair| pair[1].1 - pair[0].1 > 1e-9),
        "fixture oracle has an ambiguous top-{k} boundary: {ranked:?}"
    );
    Ok(ranked.into_iter().take(k).map(|(id, _)| id).collect())
}

async fn physical_plan(h: &Harness, sql: &str, context: &str) -> Result<String> {
    let explanation = compare::simple(&h.lake, &format!("EXPLAIN VERBOSE {sql}"))
        .await
        .with_context(|| format!("{context}: EXPLAIN {sql}"))?;
    let physical: Vec<&str> = explanation
        .rows
        .iter()
        .filter(|row| row.first().and_then(Option::as_deref) == Some("physical_plan"))
        .filter_map(|row| row.get(1).and_then(Option::as_deref))
        .collect();
    ensure!(
        physical.len() == 1,
        "{context}: expected one physical_plan row, actual={:?}",
        explanation.rows
    );
    Ok(physical[0].to_string())
}

async fn ensure_vector_index_route(h: &Harness, sql: &str, context: &str) -> Result<()> {
    let plan = physical_plan(h, sql, context).await?;
    let nodes: Vec<&str> = plan
        .lines()
        .filter(|line| line.contains("LakeSoulVectorSearchExec("))
        .collect();
    ensure!(
        nodes.len() == 1
            && nodes[0].contains("vec=vec")
            && nodes[0].contains("metric=L2"),
        "{context}: expected one L2 vector index scan for vec, actual physical \
         plan:\n{plan}"
    );
    Ok(())
}

async fn ensure_text_index_route(h: &Harness, sql: &str, context: &str) -> Result<()> {
    let plan = physical_plan(h, sql, context).await?;
    let nodes: Vec<&str> = plan
        .lines()
        .filter(|line| line.contains("LakeSoulTextSearchExec"))
        .collect();
    ensure!(
        nodes.len() == 1,
        "{context}: expected one text index scan, actual physical plan:\n{plan}"
    );
    Ok(())
}
