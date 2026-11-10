// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `UPDATE` contracts against the real server.
//!
//! Every case runs through the PostgreSQL wire protocol against a fresh
//! `postgres-lakesoul` child. A successful statement is mirrored on the
//! PostgreSQL reference connection and both relations must agree exactly; a
//! refused or failing statement is proven to change nothing by comparing the
//! authoritative snapshot (metadata of every table in the namespace plus the
//! exact bytes of every warehouse file) around it, and by reading the
//! fixtures' rows afterwards.
//!
//! An update must append a new version rather than rewrite the snapshot: the
//! files that existed before a statement are read back and their exact bytes
//! must still match, while new files must appear under the written partition.
//! Rows the statement did not touch must still be visible.
//!
//! Two tests share the scenario matrix:
//!
//! * [`update_contract`] executes it on the coordinator;
//! * [`distributed_update_contract`] executes the same statements with two
//!   real Flight workers and local fallback disabled, so a statement either
//!   runs on the workers or fails instead of silently reverting to the
//!   coordinator. It then pins one distributed publication by itself.
//!
//! [`indexed_update_contract`] runs on the vector suite, whose `vector_ann`
//! table already carries a committed IVF+RaBitQ index and whose `vector_text_ann`
//! table carries a committed `text_match` index: after an `UPDATE` moves one
//! row, the new vector/text must be searchable through the physical index
//! route and the superseded value must not pull the row back. The expected
//! top-k is independent float64 math over the expected live corpus, and the
//! index artifacts must advance while the committed data files keep their
//! bytes.
//!
//! Refusals cover the tables LakeSoul cannot update — CDC tables (identified
//! by their effective properties: `use_cdc = true` or a declared change
//! column), append-only tables and tables without a primary key — plus
//! primary-key/partition-column changes, `RETURNING`, `UPDATE ... FROM`,
//! data-modifying CTEs, `MERGE`, transaction blocks and multi-statement
//! simple-query batches.
//!
//! A table with a custom (non-default) merge operator has no wire-reachable
//! fixture in this tree: the verdict reads the provider's io-config merge
//! operators, which no table property or `CREATE TABLE ... WITH (...)` option
//! populates. The real entry point is the native io-config builder
//! (`with_merge_op` / the `lakesoul-io-c` C API) used to construct a provider,
//! so that refusal belongs to the provider-level contract and is not claimed
//! here.

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
/// SQLSTATE for duplicate assignment targets in a `SET` list.
const SYNTAX_ERROR: &str = "42601";
/// SQLSTATE for a prepared `UPDATE` whose pinned target `table_id` is no
/// longer the table the name resolves to (dropped or recreated).
const UNDEFINED_TABLE: &str = "42P01";

const COUNTERS: &str = "update_counters";
const BUCKETS: &str = "update_buckets";
const RANGES: &str = "update_ranges";
const TYPES: &str = "update_types";
const CDC_EMPTY: &str = "update_cdc_empty";
const REBUILD: &str = "update_rebuild_target";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn update_contract() -> Result<()> {
    support::run_suite(|h| Box::pin(async move { update_scenarios(h).await })).await
}

/// The same statements, planned and executed with two real Flight workers.
///
/// The harness starts the server with `--distributed-fallback-local=false`, so
/// a statement the distributed planner cannot ship fails instead of quietly
/// running in-process: a passing run proves the statements hold for the worker
/// path as well.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_update_contract() -> Result<()> {
    support::run_distributed_insert_suite(|h| {
        Box::pin(async move {
            update_scenarios(h).await?;
            distributed_update_publishes_on_workers(h).await
        })
    })
    .await
}

/// The vector index must follow an `UPDATE`.
///
/// Runs on the vector suite, where [`ANN_TABLE`] already carries a committed
/// IVF+RaBitQ index over its 64-row corpus. Moving one row to a far vector
/// must make the new vector searchable through the physical index route and
/// must stop the superseded vector from pulling the row back; the expected
/// top-k is independent float64 math over the expected live corpus.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_update_contract() -> Result<()> {
    support::run_vector_suite(|h| {
        Box::pin(async move { indexed_update_scenario(h).await })
    })
    .await
}

/// The scenario matrix shared by the coordinator-only and the distributed
/// contract.
///
/// Scenarios run in order and each owns its tables; only `pk_items`, whose
/// two-commit history is a base fixture, is shared, and each scenario that
/// touches it states the value it expects.
async fn update_scenarios(h: &Harness) -> Result<()> {
    constants_expressions_and_null_match_postgres(h).await?;
    multi_column_assignment_reads_one_old_row(h).await?;
    zero_match_publishes_nothing(h).await?;
    historical_versions_do_not_supply_the_predicate(h).await?;
    hash_buckets_and_range_partitions(h).await?;
    prepared_update_rebinds_and_reads_new_commits(h).await?;
    subquery_assignments_and_quoted_columns(h).await?;
    late_failures_publish_nothing(h).await?;
    refused_updates_change_nothing(h).await?;
    batch_with_leading_read_never_runs_the_update(h).await?;
    prepared_update_after_drop_and_recreate(h).await?;
    Ok(())
}

/// Real scalar subqueries must preserve assignment/cardinality semantics.
/// Ordinary columns named rowKinds or SQL keywords are not CDC configuration.
async fn subquery_assignments_and_quoted_columns(h: &Harness) -> Result<()> {
    for protocol in Protocol::BOTH {
        let table = format!("update_subqueries_{}", protocol.label());
        let ddl = format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY, n BIGINT, label TEXT, \
             \"rowKinds\" TEXT, \"select\" TEXT)"
        );
        create_mirrored_table(
            h,
            &ddl,
            &ddl,
            &format!(
                "INSERT INTO {table} VALUES \
                 (1,10,'one','ordinary','first'), \
                 (2,20,'two','ordinary','second'), \
                 (3,30,'three','ordinary','third')"
            ),
        )
        .await?;
        for sql in [
            format!(
                "UPDATE {table} AS dst \
                 SET n=(SELECT max(src.n)+1 FROM {table} AS src WHERE src.id=dst.id) \
                 WHERE dst.id=2"
            ),
            format!(
                "UPDATE {table} SET label=(SELECT label FROM {table} WHERE id=999) \
                 WHERE id=1"
            ),
            format!(
                "UPDATE {table} SET \"select\"='changed', \"rowKinds\"='still-ordinary' \
                 WHERE id=3"
            ),
        ] {
            ensure_update_parity(h, &sql, 1, protocol, "subquery / quoted column UPDATE")
                .await?;
        }
        let projection =
            format!("SELECT id,n,label,\"rowKinds\",\"select\" FROM {table} ORDER BY id");
        ensure_relation_parity(h, &projection, "subquery / quoted columns").await?;
        for (sql, expected) in [
            (
                format!("UPDATE {table} SET n=(SELECT n FROM {table}) WHERE id=3"),
                "21000",
            ),
            (format!("UPDATE {table} SET \"N\"=99 WHERE false"), "42703"),
        ] {
            let before = h.authoritative_snapshot().await?;
            ensure_rejection(
                rejection(&h.lake, &sql, protocol).await?,
                rejection(&h.reference, &sql, protocol).await?,
                expected,
                "subquery cardinality / quoted target rejection",
                protocol.label(),
            )?;
            ensure_snapshot_unchanged(
                &before,
                &h.authoritative_snapshot().await?,
                "subquery cardinality / quoted target rejection",
            )?;
            ensure_relation_parity(h, &projection, "failed subquery UPDATE").await?;
        }
        // A failed UPDATE must not leave its coordinator-only scalar-subquery
        // option on the connection: a subsequent SELECT still uses the
        // distributed session's normal, shippable query planning.
        ensure_relation_parity(
            h,
            &format!("SELECT (SELECT n FROM {table} WHERE id=1) AS n"),
            "scalar SELECT after failed UPDATE",
        )
        .await?;
    }
    Ok(())
}

/// Constants, old-value expressions, explicit NULLs, retained columns and a
/// whole-table update without `WHERE` agree with PostgreSQL, append one
/// version per statement, and leave the seed files byte-identical.
async fn constants_expressions_and_null_match_postgres(h: &Harness) -> Result<()> {
    let projection = "SELECT id, n, label FROM update_counters ORDER BY id";
    create_mirrored_table(
        h,
        "CREATE TABLE update_counters (\
           id BIGINT NOT NULL, n BIGINT, label TEXT, PRIMARY KEY (id))",
        "CREATE TABLE update_counters (\
           id BIGINT NOT NULL, n BIGINT, label TEXT, PRIMARY KEY (id))",
        "INSERT INTO update_counters (id, n, label) \
         VALUES (1, 10, 'a'), (2, 20, ''), (3, NULL, NULL)",
    )
    .await?;

    let table_id = table_id(h, COUNTERS).await?;
    let seed_commits = committed_commits(h, &table_id).await?;
    let seed_files = h.table_file_bytes(COUNTERS).await?;
    ensure!(
        !seed_files.is_empty(),
        "the seed commit must leave at least one data file"
    );

    // Extended protocol: a constant and an expression reading the old row.
    ensure_update_parity(
        h,
        "UPDATE update_counters SET n = n + 1, label = 'changed' WHERE id = 1",
        1,
        Protocol::Extended,
        "constant and old-value expression",
    )
    .await?;
    ensure_rows(
        h,
        projection,
        text_rows(&[
            &[Some("1"), Some("11"), Some("changed")],
            &[Some("2"), Some("20"), Some("")],
            &[Some("3"), None, None],
        ]),
        "assigned columns change, unassigned columns keep the old row",
    )
    .await?;
    ensure_relation_parity(h, projection, "constant and old-value expression").await?;

    // An explicit NULL overwrites a non-NULL value instead of being skipped.
    ensure_update_parity(
        h,
        "UPDATE update_counters SET label = NULL WHERE id = 2",
        1,
        Protocol::Simple,
        "explicit NULL",
    )
    .await?;
    ensure_relation_parity(h, projection, "explicit NULL").await?;

    // An expression over a NULL payload still updates the matched row, and the
    // unassigned column keeps its old NULL.
    ensure_update_parity(
        h,
        "UPDATE update_counters SET n = COALESCE(n, 0) + 5 WHERE id = 3",
        1,
        Protocol::Simple,
        "NULL payload expression",
    )
    .await?;
    ensure_relation_parity(h, projection, "NULL payload expression").await?;

    // No WHERE clause: every live row is a match, and every match counts even
    // when the computed value equals the old one.
    ensure_update_parity(
        h,
        "UPDATE update_counters SET n = n * 2",
        3,
        Protocol::Simple,
        "whole-table update",
    )
    .await?;
    ensure_rows(
        h,
        projection,
        text_rows(&[
            &[Some("1"), Some("22"), Some("changed")],
            &[Some("2"), Some("40"), None],
            &[Some("3"), Some("10"), None],
        ]),
        "whole-table update result",
    )
    .await?;
    ensure_relation_parity(h, projection, "whole-table update").await?;

    ensure_eq(
        "one commit per successful statement",
        committed_commits(h, &table_id).await?,
        seed_commits + 4,
    )?;
    // One unpartitioned partition at version 0 when seeded, +1 per statement.
    ensure_eq(
        "unpartitioned partition version after four statements",
        partition_versions(h, &table_id).await?,
        vec![("-5".to_string(), 4)],
    )?;

    let files = h.table_file_bytes(COUNTERS).await?;
    ensure_previous_files_unchanged("four updates", &seed_files, &files)?;
    ensure_committed_files_exist(
        h,
        &commit_file_paths(h, &table_id).await?,
        "four updates",
    )?;

    // A second connection sees the same committed relation.
    let other = h.connect_lake().await?;
    let visible: i64 = other
        .query_one("SELECT count(*) FROM update_counters WHERE n = 22", &[])
        .await?
        .get(0);
    ensure_eq("second-connection visibility", visible, 1)?;

    eprintln!(
        "PASS case=update-expressions protocol=simple+extended statements=4 \
         commits=+4 pg-relation=equal previous-files=unchanged"
    );
    Ok(())
}

/// `SET a = b, b = a` swaps in one statement: every assignment reads the same
/// old row. A same-value assignment still counts as an updated row.
async fn multi_column_assignment_reads_one_old_row(h: &Harness) -> Result<()> {
    let projection = "SELECT id, a, b FROM update_swap ORDER BY id";
    create_mirrored_table(
        h,
        "CREATE TABLE update_swap (\
           id BIGINT NOT NULL, a INTEGER, b INTEGER, PRIMARY KEY (id))",
        "CREATE TABLE update_swap (\
           id BIGINT NOT NULL, a INTEGER, b INTEGER, PRIMARY KEY (id))",
        "INSERT INTO update_swap (id, a, b) VALUES (1, 10, 20), (2, 1, 2)",
    )
    .await?;

    ensure_update_parity(
        h,
        "UPDATE update_swap SET a = b, b = a WHERE id = 1",
        1,
        Protocol::Extended,
        "column exchange",
    )
    .await?;
    ensure_rows(
        h,
        projection,
        text_rows(&[
            &[Some("1"), Some("20"), Some("10")],
            &[Some("2"), Some("1"), Some("2")],
        ]),
        "SET a = b, b = a completes the exchange",
    )
    .await?;
    ensure_relation_parity(h, projection, "column exchange").await?;

    // Both assignments read the pre-statement row: a = 20 + 10 = 30, b = 20.
    ensure_update_parity(
        h,
        "UPDATE update_swap SET a = a + b, b = a WHERE id = 1",
        1,
        Protocol::Simple,
        "two assignments over one old row",
    )
    .await?;
    ensure_rows(
        h,
        projection,
        text_rows(&[
            &[Some("1"), Some("30"), Some("20")],
            &[Some("2"), Some("1"), Some("2")],
        ]),
        "both expressions read the old row",
    )
    .await?;

    // Assigning the old value back still counts the match.
    ensure_update_parity(
        h,
        "UPDATE update_swap SET a = a WHERE id = 2",
        1,
        Protocol::Simple,
        "same-value assignment",
    )
    .await?;
    ensure_relation_parity(h, projection, "same-value assignment").await?;

    // Repeated source names/expressions must not make the full-row projection
    // ambiguous or accidentally read a value produced by an earlier SET.
    for (sql, protocol) in [
        (
            "UPDATE update_swap SET a = b WHERE id = 1",
            Protocol::Simple,
        ),
        (
            "UPDATE update_swap SET a = b + 1, b = b + 1 WHERE id = 1",
            Protocol::Extended,
        ),
    ] {
        ensure_update_parity(h, sql, 1, protocol, "repeated assignment expression")
            .await?;
        ensure_relation_parity(h, projection, "repeated assignment expression").await?;
    }

    eprintln!(
        "PASS case=update-multi-column exchange=1 same-value=counted \
         pg-relation=equal"
    );
    Ok(())
}

/// A statement that matches no live row reports `0` and publishes nothing.
async fn zero_match_publishes_nothing(h: &Harness) -> Result<()> {
    let table_id = table_id(h, COUNTERS).await?;
    let before = h.authoritative_snapshot().await?;
    let commits_before = committed_commits(h, &table_id).await?;
    let files_before = h.table_file_bytes(COUNTERS).await?;
    let projection = "SELECT id, n, label FROM update_counters ORDER BY id";
    let relation_before = compare::simple(&h.lake, projection).await?;

    ensure_update_parity(
        h,
        "UPDATE update_counters SET n = 0 WHERE id = 999999",
        0,
        Protocol::Simple,
        "missing key",
    )
    .await?;
    ensure_update_parity(
        h,
        "UPDATE update_counters SET n = 0 WHERE 1 = 0",
        0,
        Protocol::Extended,
        "false predicate",
    )
    .await?;
    ensure_update_parity(
        h,
        "UPDATE update_counters SET n = 0 WHERE id IS NULL",
        0,
        Protocol::Simple,
        "NULL key",
    )
    .await?;

    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "zero-match updates")?;
    ensure_eq(
        "zero-match commits",
        committed_commits(h, &table_id).await?,
        commits_before,
    )?;
    let files_after = h.table_file_bytes(COUNTERS).await?;
    ensure!(
        files_after == files_before,
        "zero-match updates must not add or rewrite files ({} before, {} after)",
        files_before.len(),
        files_after.len()
    );
    let relation_after = compare::simple(&h.lake, projection).await?;
    compare::compare(&relation_after, &relation_before, true)
        .context("zero-match updates must leave the relation unchanged")?;

    eprintln!(
        "PASS case=update-zero-match protocols=simple+extended rows=0 \
         metadata=unchanged files=unchanged"
    );
    Ok(())
}

/// A key updated in an earlier commit is gone from the merge-on-read relation:
/// a predicate that only the superseded version satisfies matches nothing and
/// must not revive the old row.
async fn historical_versions_do_not_supply_the_predicate(h: &Harness) -> Result<()> {
    // `pk_items` key 1 was committed twice by the fixtures: (1, 10, 'old'),
    // then (1, 99, 'new').
    let table_id = table_id(h, "pk_items").await?;
    ensure_eq(
        "current value of the historical key",
        lake_i64(h, "SELECT val FROM pk_items WHERE id = 1").await?,
        99,
    )?;

    let before = h.authoritative_snapshot().await?;
    let commits_before = committed_commits(h, &table_id).await?;
    ensure_update_parity(
        h,
        "UPDATE pk_items SET label = 'revived' WHERE val = 10",
        0,
        Protocol::Extended,
        "predicate satisfied only by the superseded version",
    )
    .await?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "superseded version predicate")?;
    ensure_eq(
        "superseded version predicate commits",
        committed_commits(h, &table_id).await?,
        commits_before,
    )?;
    ensure_eq(
        "superseded version did not become visible",
        lake_i64(h, "SELECT count(*) FROM pk_items WHERE val = 10").await?,
        0,
    )?;
    ensure_eq(
        "current row kept its label",
        lake_text(h, "SELECT label FROM pk_items WHERE id = 1")
            .await?
            .as_deref(),
        Some("new"),
    )?;

    // Positive control on the same key: the current version does match.
    ensure_update_parity(
        h,
        "UPDATE pk_items SET val = val + 1 WHERE val = 99",
        1,
        Protocol::Simple,
        "current version predicate",
    )
    .await?;
    ensure_eq(
        "current version was updated",
        lake_i64(h, "SELECT val FROM pk_items WHERE id = 1").await?,
        100,
    )?;
    ensure_relation_parity(
        h,
        "SELECT id, val, label FROM pk_items ORDER BY id",
        "current version predicate",
    )
    .await?;

    eprintln!(
        "PASS case=update-history superseded-predicate=0 current-predicate=1 \
         pg-relation=equal"
    );
    Ok(())
}

/// Multi-bucket and range-partitioned primary-key tables update only matching
/// rows, keep the untouched rows visible, publish one version per affected
/// partition in one statement, and append files under the right partition.
async fn hash_buckets_and_range_partitions(h: &Harness) -> Result<()> {
    // Four hash buckets over one unpartitioned table: rows live in different
    // physical files, and an update must not drop the rows it did not rewrite.
    create_mirrored_table(
        h,
        "CREATE TABLE update_buckets (\
           id BIGINT NOT NULL, val BIGINT, PRIMARY KEY (id)) \
         WITH (hash_bucket_num = 4, file_format = 'parquet')",
        "CREATE TABLE update_buckets (\
           id BIGINT NOT NULL, val BIGINT, PRIMARY KEY (id))",
        "INSERT INTO update_buckets (id, val) VALUES \
         (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80)",
    )
    .await?;
    ensure_eq(
        "hash bucket count reaches table metadata",
        hash_bucket_num(h, BUCKETS).await?.as_deref(),
        Some("4"),
    )?;
    let buckets_id = table_id(h, BUCKETS).await?;
    let bucket_commits = committed_commits(h, &buckets_id).await?;
    let bucket_seed_files = h.table_file_bytes(BUCKETS).await?;
    let projection = "SELECT id, val FROM update_buckets ORDER BY id";

    ensure_update_parity(
        h,
        "UPDATE update_buckets SET val = val + 1 WHERE id <= 4",
        4,
        Protocol::Simple,
        "multi-bucket first half",
    )
    .await?;
    ensure_rows(
        h,
        projection,
        text_rows(&[
            &[Some("1"), Some("11")],
            &[Some("2"), Some("21")],
            &[Some("3"), Some("31")],
            &[Some("4"), Some("41")],
            &[Some("5"), Some("50")],
            &[Some("6"), Some("60")],
            &[Some("7"), Some("70")],
            &[Some("8"), Some("80")],
        ]),
        "multi-bucket first half keeps the other rows",
    )
    .await?;
    ensure_eq(
        "multi-bucket first half commits",
        committed_commits(h, &buckets_id).await?,
        bucket_commits + 1,
    )?;

    ensure_update_parity(
        h,
        "UPDATE update_buckets SET val = val + 100 WHERE id > 4",
        4,
        Protocol::Simple,
        "multi-bucket second half",
    )
    .await?;
    ensure_rows(
        h,
        projection,
        text_rows(&[
            &[Some("1"), Some("11")],
            &[Some("2"), Some("21")],
            &[Some("3"), Some("31")],
            &[Some("4"), Some("41")],
            &[Some("5"), Some("150")],
            &[Some("6"), Some("160")],
            &[Some("7"), Some("170")],
            &[Some("8"), Some("180")],
        ]),
        "multi-bucket second half",
    )
    .await?;
    ensure_relation_parity(h, projection, "multi-bucket updates").await?;

    // One statement across every bucket: no bucket's rows may be lost or
    // duplicated by the merge.
    ensure_update_parity(
        h,
        "UPDATE update_buckets SET val = val + 1000",
        8,
        Protocol::Extended,
        "all buckets in one statement",
    )
    .await?;
    ensure_rows(
        h,
        projection,
        text_rows(&[
            &[Some("1"), Some("1011")],
            &[Some("2"), Some("1021")],
            &[Some("3"), Some("1031")],
            &[Some("4"), Some("1041")],
            &[Some("5"), Some("1150")],
            &[Some("6"), Some("1160")],
            &[Some("7"), Some("1170")],
            &[Some("8"), Some("1180")],
        ]),
        "all buckets in one statement",
    )
    .await?;
    ensure_relation_parity(h, projection, "all buckets in one statement").await?;
    ensure_eq(
        "one commit per multi-bucket statement",
        committed_commits(h, &buckets_id).await?,
        bucket_commits + 3,
    )?;
    let bucket_files = h.table_file_bytes(BUCKETS).await?;
    ensure_previous_files_unchanged(
        "multi-bucket updates",
        &bucket_seed_files,
        &bucket_files,
    )?;
    ensure_committed_files_exist(
        h,
        &commit_file_paths(h, &buckets_id).await?,
        "multi-bucket updates",
    )?;

    // Range partition with a partition-local primary key: the same id exists
    // in both partitions, and a predicate on `part` must update one of them.
    let range_projection = "SELECT part, id, val FROM update_ranges ORDER BY part, id";
    create_mirrored_table(
        h,
        "CREATE TABLE update_ranges (\
           part INTEGER NOT NULL, id BIGINT NOT NULL, val INTEGER, PRIMARY KEY (id)) \
         PARTITIONED BY (part) WITH (hash_bucket_num = 2, file_format = 'parquet')",
        "CREATE TABLE update_ranges (\
           part INTEGER NOT NULL, id BIGINT NOT NULL, val INTEGER)",
        "INSERT INTO update_ranges (part, id, val) VALUES \
         (0, 1, 10), (1, 1, 100), (0, 2, 20), (1, 2, 200)",
    )
    .await?;
    let ranges_id = table_id(h, RANGES).await?;
    let range_seed_files = h.table_file_bytes(RANGES).await?;
    let range_seed_commits = committed_per_partition(h, &ranges_id).await?;
    let range_seed_paths: std::collections::BTreeSet<(String, String)> =
        commit_file_paths(h, &ranges_id)
            .await?
            .into_iter()
            .collect();
    ensure_eq(
        "range partitions before updates",
        partition_versions(h, &ranges_id).await?,
        vec![("part=0".to_string(), 0), ("part=1".to_string(), 0)],
    )?;

    let statement = h
        .lake
        .prepare_typed(
            "UPDATE update_ranges SET val = val + $1 WHERE part = $2 AND id = $3",
            &[Type::INT4, Type::INT4, Type::INT8],
        )
        .await
        .context("extended Parse of a partition-local prepared UPDATE")?;
    ensure_eq(
        "partitioned UPDATE parameter types",
        statement.params().to_vec(),
        vec![Type::INT4, Type::INT4, Type::INT8],
    )?;
    let rows = h
        .lake
        .execute(&statement, &[&5i32, &0i32, &1i64])
        .await
        .context("extended Execute of a partition-local prepared UPDATE")?;
    ensure_eq("partition-local UPDATE rows", rows, 1)?;
    ensure_eq(
        "partition-local reference rows",
        simple_tag_rows(
            &h.reference,
            "UPDATE update_ranges SET val = val + 5 WHERE part = 0 AND id = 1",
        )
        .await?,
        1,
    )?;
    ensure_rows(
        h,
        range_projection,
        text_rows(&[
            &[Some("0"), Some("1"), Some("15")],
            &[Some("0"), Some("2"), Some("20")],
            &[Some("1"), Some("1"), Some("100")],
            &[Some("1"), Some("2"), Some("200")],
        ]),
        "only the matching partition's key changed",
    )
    .await?;
    ensure_relation_parity(h, range_projection, "partition-local UPDATE").await?;
    ensure_eq(
        "only the matching partition was published",
        partition_versions(h, &ranges_id).await?,
        vec![("part=0".to_string(), 1), ("part=1".to_string(), 0)],
    )?;
    ensure_eq(
        "only the matching partition gained a commit",
        committed_per_partition(h, &ranges_id).await?,
        vec![
            ("part=0".to_string(), range_seed_commits[0].1 + 1),
            ("part=1".to_string(), range_seed_commits[1].1),
        ],
    )?;
    ensure_previous_files_unchanged(
        "partition-local UPDATE",
        &range_seed_files,
        &h.table_file_bytes(RANGES).await?,
    )?;

    // One statement matching the same id in both partitions publishes every
    // affected partition version.
    ensure_update_parity(
        h,
        "UPDATE update_ranges SET val = val * 2 WHERE id = 2",
        2,
        Protocol::Simple,
        "two partitions in one statement",
    )
    .await?;
    ensure_rows(
        h,
        range_projection,
        text_rows(&[
            &[Some("0"), Some("1"), Some("15")],
            &[Some("0"), Some("2"), Some("40")],
            &[Some("1"), Some("1"), Some("100")],
            &[Some("1"), Some("2"), Some("400")],
        ]),
        "both partitions of the same key updated",
    )
    .await?;
    ensure_relation_parity(h, range_projection, "two partitions in one statement")
        .await?;
    ensure_eq(
        "both partitions published",
        partition_versions(h, &ranges_id).await?,
        vec![("part=0".to_string(), 2), ("part=1".to_string(), 1)],
    )?;
    let new_paths: Vec<(String, String)> = commit_file_paths(h, &ranges_id)
        .await?
        .into_iter()
        .filter(|path| !range_seed_paths.contains(path))
        .collect();
    ensure!(
        !new_paths.is_empty(),
        "range updates must commit new data files"
    );
    ensure_files_under_partitions("range updates", h, &new_paths, &["part=0", "part=1"])?;

    eprintln!(
        "PASS case=update-buckets buckets=4 statements=3 commits=+3 \
         previous-files=unchanged \
         case=update-ranges partition-local=1 both-partitions=2 pg-relation=equal"
    );
    Ok(())
}

/// A prepared `UPDATE` binds new parameters on every `Execute` and reads the
/// committed state of that execution, not a snapshot captured at Parse: an
/// external commit between two executions is visible to the second one.
async fn prepared_update_rebinds_and_reads_new_commits(h: &Harness) -> Result<()> {
    let statement = h
        .lake
        .prepare_typed(
            "UPDATE pk_items SET val = val + $1 WHERE id = $2",
            &[Type::INT8, Type::INT8],
        )
        .await
        .context("extended Parse of a parameterized UPDATE")?;
    ensure_eq(
        "rebinding parameter types",
        statement.params().to_vec(),
        vec![Type::INT8, Type::INT8],
    )?;

    let table_id = table_id(h, "pk_items").await?;
    let commits_before = committed_commits(h, &table_id).await?;
    let first = h
        .lake
        .execute(&statement, &[&1i64, &3i64])
        .await
        .context("first Execute of the prepared UPDATE")?;
    ensure_eq("first prepared execution rows", first, 1)?;
    ensure_eq(
        "first prepared execution value",
        lake_i64(h, "SELECT val FROM pk_items WHERE id = 3").await?,
        31,
    )?;
    ensure_eq(
        "first prepared execution reference rows",
        simple_tag_rows(
            &h.reference,
            "UPDATE pk_items SET val = val + 1 WHERE id = 3",
        )
        .await?,
        1,
    )?;
    ensure_relation_parity(
        h,
        "SELECT id, val, label FROM pk_items ORDER BY id",
        "first prepared execution",
    )
    .await?;
    ensure_eq(
        "one commit per prepared execution",
        committed_commits(h, &table_id).await?,
        commits_before + 1,
    )?;

    // Another writer commits key 3 while this connection's prepared statement
    // still exists. The next Execute must add to 500, not to the 31 it read
    // before, and must keep the externally written label.
    h.commit_pk_rows("(3, 500, 'external')").await?;
    let commits_external = committed_commits(h, &table_id).await?;
    let second = h
        .lake
        .execute(&statement, &[&5i64, &3i64])
        .await
        .context("second Execute of the prepared UPDATE")?;
    ensure_eq("second prepared execution rows", second, 1)?;
    ensure_eq(
        "prepared execution reads the externally committed value",
        lake_i64(h, "SELECT val FROM pk_items WHERE id = 3").await?,
        505,
    )?;
    ensure_eq(
        "unassigned column comes from the externally committed row",
        lake_text(h, "SELECT label FROM pk_items WHERE id = 3")
            .await?
            .as_deref(),
        Some("external"),
    )?;
    ensure_eq(
        "second prepared execution commit",
        committed_commits(h, &table_id).await?,
        commits_external + 1,
    )?;
    ensure_eq(
        "prepared UPDATE leaves the other rows in place",
        lake_i64(h, "SELECT count(*) FROM pk_items").await?,
        3,
    )?;

    eprintln!(
        "PASS case=update-prepared rebind=2 external-commit=visible \
         rows=1+1 commits=+1+1"
    );
    Ok(())
}

/// Type, NOT NULL and late expression failures abort the whole statement: no
/// row becomes visible and no file or metadata row is published, even when an
/// earlier row of the same statement would have succeeded.
async fn late_failures_publish_nothing(h: &Harness) -> Result<()> {
    let projection = "SELECT id, n, label FROM update_types ORDER BY id";
    create_mirrored_table(
        h,
        "CREATE TABLE update_types (\
           id BIGINT NOT NULL, n BIGINT NOT NULL, label TEXT, PRIMARY KEY (id))",
        "CREATE TABLE update_types (\
           id BIGINT NOT NULL, n BIGINT NOT NULL, label TEXT, PRIMARY KEY (id))",
        "INSERT INTO update_types (id, n, label) \
         VALUES (1, 10, '1'), (2, 20, 'not-a-number'), (3, 30, '3')",
    )
    .await?;
    let table_id = table_id(h, TYPES).await?;
    let seed_commits = committed_commits(h, &table_id).await?;
    let seed_files = h.table_file_bytes(TYPES).await?;
    let relation = compare::simple(&h.lake, projection).await?;

    for (sql, expected, case) in [
        (
            "UPDATE update_types SET n = NULL WHERE id = 1",
            NOT_NULL_VIOLATION,
            "NOT NULL violation",
        ),
        (
            // Row 1 converts, row 2 does not: the failure happens after part of
            // the projection was computed and must still publish nothing.
            "UPDATE update_types SET n = CAST(label AS BIGINT) WHERE id <= 2",
            INVALID_TEXT_REPRESENTATION,
            "late cast failure on the second row",
        ),
        (
            "UPDATE update_types SET n = label WHERE id = 1",
            DATATYPE_MISMATCH,
            "assignment type mismatch",
        ),
    ] {
        let before = h.authoritative_snapshot().await?;
        for protocol in Protocol::BOTH {
            let lake = rejection(&h.lake, sql, protocol).await?;
            let reference = rejection(&h.reference, sql, protocol).await?;
            ensure_rejection(lake, reference, expected, case, protocol.label())?;
        }
        let after = h.authoritative_snapshot().await?;
        ensure_snapshot_unchanged(&before, &after, case)?;
        ensure_eq(
            &format!("{case}: committed data records"),
            committed_commits(h, &table_id).await?,
            seed_commits,
        )?;
        let files = h.table_file_bytes(TYPES).await?;
        ensure!(
            files == seed_files,
            "{case}: a failed statement must not add or rewrite files \
             ({} before, {} after)",
            seed_files.len(),
            files.len()
        );
        let unchanged = compare::simple(&h.lake, projection).await?;
        compare::compare(&unchanged, &relation, true)
            .with_context(|| format!("{case}: rows must be unchanged"))?;
        ensure_connection_usable(h, case).await?;
    }

    ensure_relation_parity(h, projection, "failed statements").await?;

    eprintln!(
        "PASS case=update-late-failure sqlstates={NOT_NULL_VIOLATION},\
         {INVALID_TEXT_REPRESENTATION},{DATATYPE_MISMATCH} rows=unchanged \
         metadata=unchanged files=unchanged"
    );
    Ok(())
}

/// Every table LakeSoul cannot update is refused before reading or writing,
/// including CDC tables that match no row, and the refusals leave no metadata
/// row, file or row change behind.
async fn refused_updates_change_nothing(h: &Harness) -> Result<()> {
    // CDC configuration is two distinct effective shapes: an explicit change
    // column, and `use_cdc = true` resolving the declared default column.
    for ddl in [
        "CREATE TABLE update_cdc (\
           id BIGINT NOT NULL, op TEXT, val BIGINT, PRIMARY KEY (id)) \
         WITH (use_cdc = true, lakesoul_cdc_change_column = 'op')",
        "CREATE TABLE update_cdc_default (\
           id BIGINT NOT NULL, rowkinds TEXT, val BIGINT, PRIMARY KEY (id)) \
         WITH (use_cdc = true)",
        // The second effective shape: a declared change column without
        // `use_cdc` is still a CDC table for the update policy.
        "CREATE TABLE update_cdc_column (\
           id BIGINT NOT NULL, op TEXT, val BIGINT, PRIMARY KEY (id)) \
         WITH (lakesoul_cdc_change_column = 'op')",
        "CREATE TABLE update_cdc_empty (\
           id BIGINT NOT NULL, op TEXT, val BIGINT, PRIMARY KEY (id)) \
         WITH (use_cdc = true, lakesoul_cdc_change_column = 'op')",
        "CREATE TABLE update_append (\
           id BIGINT NOT NULL, val BIGINT, PRIMARY KEY (id)) \
         WITH (append_only = true)",
        "CREATE TABLE update_nopk (id BIGINT, val BIGINT)",
    ] {
        h.lake
            .simple_query(ddl)
            .await
            .with_context(|| format!("fixture DDL: {ddl}"))?;
    }
    for seed in [
        "INSERT INTO update_cdc (id, op, val) \
         VALUES (1, 'insert', 10), (2, 'insert', 20)",
        "INSERT INTO update_cdc_default (id, rowkinds, val) \
         VALUES (1, 'insert', 10)",
        "INSERT INTO update_cdc_column (id, op, val) VALUES (1, 'insert', 10)",
        "INSERT INTO update_append (id, val) VALUES (1, 10)",
        "INSERT INTO update_nopk (id, val) VALUES (1, 10)",
    ] {
        h.lake
            .simple_query(seed)
            .await
            .with_context(|| format!("fixture seed: {seed}"))?;
    }

    let before = h.authoritative_snapshot().await?;
    let counters_before = committed_commits(h, &table_id(h, COUNTERS).await?).await?;
    let ranges_before = committed_per_partition(h, &table_id(h, RANGES).await?).await?;

    // CDC is refused for the table's effective properties, so a zero-match
    // predicate, an empty table and a `SET` of the change column are refused
    // too — the statement must not even read the table.
    let refusals: &[(&str, &str, &str)] = &[
        (
            "UPDATE update_cdc SET val = 11 WHERE id = 1",
            FEATURE_NOT_SUPPORTED,
            "cdc explicit change column",
        ),
        (
            "UPDATE update_cdc SET val = val WHERE 1 = 0",
            FEATURE_NOT_SUPPORTED,
            "cdc zero-match predicate",
        ),
        (
            "UPDATE update_cdc SET op = 'delete' WHERE id = 1",
            FEATURE_NOT_SUPPORTED,
            "cdc change column assignment",
        ),
        (
            "UPDATE update_cdc_default SET val = 11 WHERE id = 1",
            FEATURE_NOT_SUPPORTED,
            "cdc default change column",
        ),
        (
            "UPDATE update_cdc_column SET val = 11 WHERE id = 1",
            FEATURE_NOT_SUPPORTED,
            "cdc change column without use_cdc",
        ),
        (
            "UPDATE update_cdc_empty SET val = 1 WHERE id = 1",
            FEATURE_NOT_SUPPORTED,
            "cdc empty table",
        ),
        (
            "UPDATE update_cdc_empty SET val = 1 WHERE false",
            FEATURE_NOT_SUPPORTED,
            "cdc empty table and false predicate",
        ),
        (
            "UPDATE update_append SET val = 11 WHERE id = 1",
            FEATURE_NOT_SUPPORTED,
            "append-only table",
        ),
        (
            "UPDATE update_nopk SET val = 11 WHERE id = 1",
            FEATURE_NOT_SUPPORTED,
            "table without a primary key",
        ),
        (
            "UPDATE update_nopk SET val = 11",
            FEATURE_NOT_SUPPORTED,
            "table without a primary key, no predicate",
        ),
        (
            "UPDATE update_counters SET id = id + 100 WHERE id = 1",
            FEATURE_NOT_SUPPORTED,
            "primary-key change",
        ),
        (
            "UPDATE update_ranges SET part = 2 WHERE part = 0 AND id = 1",
            FEATURE_NOT_SUPPORTED,
            "range partition column change",
        ),
        (
            "UPDATE update_counters SET n = 1 WHERE id = 1 RETURNING id",
            FEATURE_NOT_SUPPORTED,
            "RETURNING",
        ),
        (
            "UPDATE update_counters SET n = s.a FROM update_swap s \
             WHERE update_counters.id = s.id",
            FEATURE_NOT_SUPPORTED,
            "UPDATE FROM",
        ),
        (
            "UPDATE update_counters SET n = 1, n = 2 WHERE id = 1",
            SYNTAX_ERROR,
            "duplicate SET target column",
        ),
        (
            // PostgreSQL accepts a data-modifying CTE; LakeSoul claims writes
            // only as top-level statements, so the query is refused with its
            // own feature-not-supported code (not PG-equivalent).
            "WITH changed AS (UPDATE update_counters SET n = 1 WHERE id = 1 RETURNING id) \
             SELECT count(*) FROM changed",
            FEATURE_NOT_SUPPORTED,
            "data-modifying CTE",
        ),
        (
            "WITH changed AS (MERGE INTO update_counters AS dst \
             USING update_swap AS src ON dst.id=src.id \
             WHEN MATCHED THEN UPDATE SET n=src.a RETURNING dst.id) \
             SELECT count(*) FROM changed",
            FEATURE_NOT_SUPPORTED,
            "data-modifying MERGE CTE",
        ),
        (
            "MERGE INTO update_counters AS t USING update_swap AS s \
             ON t.id = s.id WHEN MATCHED THEN UPDATE SET n = s.a",
            FEATURE_NOT_SUPPORTED,
            "MERGE",
        ),
    ];

    for (sql, expected, case) in refusals {
        for protocol in Protocol::BOTH {
            let actual = rejection(&h.lake, sql, protocol).await?;
            let actual = actual.with_context(|| {
                format!(
                    "case={case} protocol={} SQL={sql}: accepted",
                    protocol.label()
                )
            })?;
            ensure!(
                actual == *expected,
                "case={case} protocol={} SQL={sql}: expected SQLSTATE {expected}, \
                 actual {actual}",
                protocol.label()
            );
        }
        ensure_connection_usable(h, case).await?;
    }

    // PostgreSQL updates inside a transaction; LakeSoul commits per statement
    // and cannot undo one, so the statement is refused and the block rolls back.
    h.lake
        .batch_execute("BEGIN")
        .await
        .context("open a transaction block")?;
    let error = h
        .lake
        .execute("UPDATE update_counters SET n = -1 WHERE id = 1", &[])
        .await
        .expect_err("UPDATE inside a transaction block must be refused");
    ensure_sqlstate(&error, FEATURE_NOT_SUPPORTED, "transaction block")?;
    h.lake
        .batch_execute("ROLLBACK")
        .await
        .context("roll back the transaction block")?;
    ensure_connection_usable(h, "transaction block").await?;

    let error = h
        .lake
        .simple_query("UPDATE update_counters SET n = -1 WHERE id = 1; SELECT 1")
        .await
        .expect_err("UPDATE in a simple-query batch must be refused");
    ensure_sqlstate(&error, FEATURE_NOT_SUPPORTED, "simple-query batch")?;
    ensure_connection_usable(h, "simple-query batch").await?;

    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "refusal matrix")?;

    // The empty CDC table has a catalog entry but must have no publication.
    let cdc_empty_id = table_id(h, CDC_EMPTY).await?;
    ensure_eq(
        "empty CDC table commits",
        committed_commits(h, &cdc_empty_id).await?,
        0,
    )?;
    ensure_eq(
        "empty CDC table partitions",
        partition_versions(h, &cdc_empty_id).await?,
        Vec::new(),
    )?;

    // Every fixture's relation is exactly what was seeded.
    ensure_rows(
        h,
        "SELECT id, op, val FROM update_cdc ORDER BY id",
        text_rows(&[
            &[Some("1"), Some("insert"), Some("10")],
            &[Some("2"), Some("insert"), Some("20")],
        ]),
        "CDC fixture rows after refusals",
    )
    .await?;
    ensure_rows(
        h,
        "SELECT id, rowkinds, val FROM update_cdc_default ORDER BY id",
        text_rows(&[&[Some("1"), Some("insert"), Some("10")]]),
        "default-column CDC fixture rows after refusals",
    )
    .await?;
    ensure_rows(
        h,
        "SELECT id, op, val FROM update_cdc_column ORDER BY id",
        text_rows(&[&[Some("1"), Some("insert"), Some("10")]]),
        "change-column CDC fixture rows after refusals",
    )
    .await?;
    ensure_rows(
        h,
        "SELECT id, val FROM update_append ORDER BY id",
        text_rows(&[&[Some("1"), Some("10")]]),
        "append-only fixture rows after refusals",
    )
    .await?;
    ensure_rows(
        h,
        "SELECT id, val FROM update_nopk ORDER BY id",
        text_rows(&[&[Some("1"), Some("10")]]),
        "no-primary-key fixture rows after refusals",
    )
    .await?;
    ensure_eq(
        "refusals leave the counters commits",
        committed_commits(h, &table_id(h, COUNTERS).await?).await?,
        counters_before,
    )?;
    ensure_eq(
        "refusals leave the range commits",
        committed_per_partition(h, &table_id(h, RANGES).await?).await?,
        ranges_before,
    )?;
    ensure_relation_parity(
        h,
        "SELECT id, n, label FROM update_counters ORDER BY id",
        "refusals leave the counters relation",
    )
    .await?;
    ensure_relation_parity(
        h,
        "SELECT part, id, val FROM update_ranges ORDER BY part, id",
        "refusals leave the range relation",
    )
    .await?;

    eprintln!(
        "PASS case=update-refusals cases={} protocols=simple+extended \
         cdc=explicit,default,change-column-only,empty,zero-match \
         no-pk=2 append-only key-change partition-change \
         clause=RETURNING,FROM,CTE,MERGE,duplicate-set transaction=0A000 \
         batch=0A000 snapshot=unchanged",
        refusals.len()
    );
    Ok(())
}

/// A simple-query batch runs its leading read before the refused `UPDATE`, and
/// the trailing `SET` sentinel must not run after the error.
async fn batch_with_leading_read_never_runs_the_update(h: &Harness) -> Result<()> {
    let table_id = table_id(h, COUNTERS).await?;
    let before = h.authoritative_snapshot().await?;
    let commits_before = committed_commits(h, &table_id).await?;
    let count = lake_i64(h, "SELECT count(*) FROM update_counters").await?;
    let budget_before = compare::simple(&h.lake, "SHOW statement_timeout").await?;
    let sql = "SELECT count(*) AS n FROM update_counters; \
               UPDATE update_counters SET n = -1 WHERE id = 1; \
               SET statement_timeout = '1ms'";
    let context = format!(
        "case=update-batch SQL={sql} protocol=simple-text expected SQLSTATE \
         {FEATURE_NOT_SUPPORTED} after the leading read; the trailing SET must not run"
    );

    let (error, names, rows) = {
        let mut stream = std::pin::pin!(h.lake.simple_query_raw(sql).await?);
        let mut names = Vec::new();
        let mut rows = Vec::new();
        let error = loop {
            match futures::StreamExt::next(&mut stream).await {
                Some(Ok(SimpleQueryMessage::RowDescription(columns))) => {
                    names = columns
                        .iter()
                        .map(|column| column.name().to_owned())
                        .collect::<Vec<_>>();
                }
                Some(Ok(SimpleQueryMessage::Row(row))) => {
                    rows.push(
                        (0..row.len())
                            .map(|i| row.get(i).map(str::to_owned))
                            .collect::<Vec<_>>(),
                    );
                }
                Some(Ok(_)) => {}
                Some(Err(error)) => break error,
                None => bail!("{context}: expected rejection, actual success"),
            }
        };
        (error, names, rows)
    };
    ensure!(
        names == ["n"] && rows == vec![vec![Some(count.to_string())]],
        "{context}: the leading read must execute before the refusal, \
         actual names={names:?} rows={rows:?}"
    );
    ensure_sqlstate(&error, FEATURE_NOT_SUPPORTED, &context)?;

    let budget_after = compare::simple(&h.lake, "SHOW statement_timeout").await?;
    compare::compare(&budget_after, &budget_before, true)
        .context("the trailing SET after a refused UPDATE must not run")?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "simple-query batch")?;
    ensure_eq(
        "batch leaves the counters commits",
        committed_commits(h, &table_id).await?,
        commits_before,
    )?;
    ensure_connection_usable(h, "simple-query batch").await?;

    eprintln!(
        "PASS case=update-batch leading-read=executed SQLSTATE={FEATURE_NOT_SUPPORTED} \
         trailing-set=not-executed snapshot=unchanged"
    );
    Ok(())
}

/// A prepared `UPDATE` whose target was dropped and recreated under the same
/// name must not publish to the new table (`42P01`, the target `table_id`
/// pinned at Parse differs from the resolved one), and recreating it as a CDC
/// table must not be reachable through the stale prepared plan either.
///
/// A plain statement has no Parse-time identity and updates the recreated
/// table normally, so the refusal is about the stale target, not the name.
/// The target tables are counted by `table_id` directly, never through the
/// surviving `table_info` join, so an orphan publication would still be
/// visible.
async fn prepared_update_after_drop_and_recreate(h: &Harness) -> Result<()> {
    let peer = h.connect_lake().await?;
    peer.simple_query(&format!(
        "CREATE TABLE {REBUILD} (\
           id BIGINT NOT NULL, label TEXT, PRIMARY KEY (id))"
    ))
    .await
    .context("rebuild fixture CREATE")?;
    peer.simple_query(&format!(
        "INSERT INTO {REBUILD} (id, label) VALUES (1, 'old')"
    ))
    .await
    .context("rebuild fixture seed")?;
    let original_id = table_id(h, REBUILD).await?;

    let statement = h
        .lake
        .prepare_typed(
            &format!("UPDATE {REBUILD} SET label = $1 WHERE id = $2"),
            &[Type::TEXT, Type::INT8],
        )
        .await
        .context("prepare UPDATE before the peer DROP")?;

    peer.simple_query(&format!("DROP TABLE {REBUILD}"))
        .await
        .context("peer DROP of the UPDATE target")?;
    peer.simple_query(&format!(
        "CREATE TABLE {REBUILD} (\
           id BIGINT NOT NULL, label TEXT, PRIMARY KEY (id))"
    ))
    .await
    .context("peer CREATE of the same name")?;
    peer.simple_query(&format!(
        "INSERT INTO {REBUILD} (id, label) VALUES (1, 'new')"
    ))
    .await
    .context("peer seed of the recreated table")?;
    let recreated_id = table_id(h, REBUILD).await?;
    ensure!(
        recreated_id != original_id,
        "the recreated table must have a new table_id"
    );

    // Both identities are measured by table_id: the dropped table's state and
    // the recreated table's publication.
    let dropped_state_before = table_metadata_state(h, &original_id).await?;
    let recreated_state_before = table_metadata_state(h, &recreated_id).await?;
    let recreated_files_before = h.table_file_bytes(REBUILD).await?;
    let before = h.authoritative_snapshot().await?;

    let error = h
        .lake
        .execute(&statement, &[&"stale", &1i64])
        .await
        .expect_err("a prepared UPDATE must not publish to a recreated table");
    ensure_sqlstate(
        &error,
        UNDEFINED_TABLE,
        "prepared UPDATE into a table dropped and recreated by another connection",
    )?;

    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "stale prepared UPDATE")?;
    ensure_eq(
        "the dropped table gained no publication",
        table_metadata_state(h, &original_id).await?,
        dropped_state_before,
    )?;
    ensure_eq(
        "the recreated table gained no publication",
        table_metadata_state(h, &recreated_id).await?,
        recreated_state_before,
    )?;
    let recreated_files_after = h.table_file_bytes(REBUILD).await?;
    ensure!(
        recreated_files_after == recreated_files_before,
        "the recreated table's files must not change"
    );
    ensure_rows(
        h,
        &format!("SELECT id, label FROM {REBUILD} ORDER BY id"),
        text_rows(&[&[Some("1"), Some("new")]]),
        "the recreated table's row must keep the peer's value",
    )
    .await?;
    ensure_connection_usable(h, "stale prepared UPDATE").await?;

    // A plain statement has no Parse-time identity: it resolves the name and
    // updates the recreated table normally, so the refusal above is about the
    // stale pinned identity, not about the name.
    ensure_eq(
        "plain UPDATE against the recreated table",
        simple_tag_rows(
            &h.lake,
            &format!("UPDATE {REBUILD} SET label = 'by-name' WHERE id = 1"),
        )
        .await?,
        1,
    )?;
    ensure_rows(
        h,
        &format!("SELECT id, label FROM {REBUILD} ORDER BY id"),
        text_rows(&[&[Some("1"), Some("by-name")]]),
        "plain UPDATE reaches the recreated table",
    )
    .await?;

    // Same name, now a CDC table: the stale prepared plan must not bypass the
    // table-property check either.
    peer.simple_query(&format!("DROP TABLE {REBUILD}"))
        .await
        .context("peer DROP of the recreated table")?;
    peer.simple_query(&format!(
        "CREATE TABLE {REBUILD} (\
           id BIGINT NOT NULL, label TEXT, PRIMARY KEY (id)) \
         WITH (use_cdc = true, lakesoul_cdc_change_column = 'label')"
    ))
    .await
    .context("peer CREATE of a CDC table under the same name")?;
    peer.simple_query(&format!(
        "INSERT INTO {REBUILD} (id, label) VALUES (1, 'insert')"
    ))
    .await
    .context("peer seed of the recreated CDC table")?;
    let cdc_id = table_id(h, REBUILD).await?;
    let cdc_state_before = table_metadata_state(h, &cdc_id).await?;
    let cdc_files_before = h.table_file_bytes(REBUILD).await?;
    let before = h.authoritative_snapshot().await?;

    let error = h
        .lake
        .execute(&statement, &[&"stale", &1i64])
        .await
        .expect_err("a stale prepared UPDATE must not write a CDC table");
    // The identity comparison runs before the table-property check, so even
    // the recreated CDC table is rejected as a stale target.
    ensure_sqlstate(
        &error,
        UNDEFINED_TABLE,
        "prepared UPDATE into a table recreated as a CDC table",
    )?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "stale prepared UPDATE into CDC")?;
    ensure_eq(
        "the recreated CDC table gained no publication",
        table_metadata_state(h, &cdc_id).await?,
        cdc_state_before,
    )?;
    let cdc_files_after = h.table_file_bytes(REBUILD).await?;
    ensure!(
        cdc_files_after == cdc_files_before,
        "the recreated CDC table's files must not change"
    );
    ensure_eq(
        "the recreated CDC table keeps exactly its seeded row",
        lake_i64(h, &format!("SELECT count(*) FROM {REBUILD}")).await?,
        1,
    )?;
    ensure_connection_usable(h, "stale prepared UPDATE into CDC").await?;

    eprintln!(
        "PASS case=update-rebuild SQLSTATE={UNDEFINED_TABLE} stale-plan=refused \
         recreated-table=unpublished plain-update=applied cdc-recreate=refused \
         snapshot=unchanged"
    );
    Ok(())
}

/// The distributed run's own publication contract.
///
/// Local fallback is disabled, so an `UPDATE` the distributed planner cannot
/// ship fails instead of quietly running on the coordinator. This case pins
/// what the wire protocol can observe: one commit, the untouched files keep
/// their bytes, and a second connection sees the new relation. Worker
/// placement itself is pinned by the datafusion integration suite.
async fn distributed_update_publishes_on_workers(h: &Harness) -> Result<()> {
    let table_id = table_id(h, "pk_items").await?;
    let commits_before = committed_commits(h, &table_id).await?;
    let files_before = h.table_file_bytes("pk_items").await?;

    let rows = h
        .lake
        .execute(
            "UPDATE pk_items SET label = 'distributed' WHERE id = 2",
            &[],
        )
        .await
        .context("distributed UPDATE through the workers")?;
    ensure_eq("distributed UPDATE rows", rows, 1)?;
    ensure_eq(
        "distributed UPDATE commits",
        committed_commits(h, &table_id).await?,
        commits_before + 1,
    )?;
    let files_after = h.table_file_bytes("pk_items").await?;
    ensure_previous_files_unchanged("distributed UPDATE", &files_before, &files_after)?;
    ensure_committed_files_exist(
        h,
        &commit_file_paths(h, &table_id).await?,
        "distributed UPDATE",
    )?;

    let other = h.connect_lake().await?;
    let label: Option<String> = other
        .query_one("SELECT label FROM pk_items WHERE id = 2", &[])
        .await?
        .get(0);
    ensure_eq(
        "distributed UPDATE visible on a second connection",
        label.as_deref(),
        Some("distributed"),
    )?;
    ensure_eq(
        "distributed UPDATE keeps the row count",
        lake_i64(h, "SELECT count(*) FROM pk_items").await?,
        3,
    )?;

    eprintln!(
        "PASS case=distributed-update rows=1 commits=+1 previous-files=unchanged \
         second-connection=visible"
    );
    Ok(())
}

/// The indexed-update acceptance shared by the vector and text fixtures.
async fn indexed_update_scenario(h: &Harness) -> Result<()> {
    indexed_vector_update(h).await?;
    indexed_text_update(h).await?;
    index_maintenance_failure_keeps_committed_update(h).await
}

/// Row moved by the indexed vector UPDATE: the fixture's nearest row to the
/// zero vector before the statement.
const INDEX_UPDATED_ID: i64 = 0;
/// New vector of the updated row: group-3 territory, far from its old group-0
/// vector and farther from every other fixture vector.
const NEW_VECTOR: [f32; DIM] = [350.0, 0.0, 0.5, -0.5, 1.0, -1.0, 0.25, -0.25];
const NEW_VECTOR_TEXT: &str = "[350,0,0.5,-0.5,1,-1,0.25,-0.25]";
const ZERO_VECTOR: [f32; DIM] = [0.0; DIM];
const ZERO_VECTOR_TEXT: &str = "[0,0,0,0,0,0,0,0]";
/// `List(Float32)` array matching the `vector_ann.vec` column type exactly.
/// A pgvector string (`'[...]'` or `'[...]'::vector(8)`) has no UPDATE-path
/// rewrite and is not assignable.
const NEW_VECTOR_SQL: &str = "ARRAY[350.0::float4, 0.0::float4, 0.5::float4, \
     -0.5::float4, 1.0::float4, -1.0::float4, 0.25::float4, -0.25::float4]";

/// A committed IVF+RaBitQ index must follow the updated row.
///
/// Before the statement id 0 is the nearest row to the zero vector and that
/// query already runs through the physical vector index. After moving the row
/// to [`NEW_VECTOR`]:
///
/// * exact re-ranking (the default mode) must never return the superseded
///   value for any top-k;
/// * index-only ordering must rank the updated row first for its new vector —
///   a stale index still ranks id 48, the nearest distinct old corpus vector,
///   first and never sees id 0;
/// * the index artifacts must change while the committed data files keep
///   their bytes.
///
/// The expected ids are independent float64 math over the expected live
/// corpus. Dropping the superseded index entry itself is the builder's
/// append/rebuild policy, not part of the `UPDATE` contract.
async fn indexed_vector_update(h: &Harness) -> Result<()> {
    h.lake
        .batch_execute(&format!("SET ivfflat.probes = {NLIST}"))
        .await
        .context("set ivfflat.probes to the fixture's cluster count")?;

    let files_before = h.table_file_bytes(ANN_TABLE).await?;
    let data_before = select_files(&files_before, IndexKind::Vector.directory(), false);
    let index_before = select_files(&files_before, IndexKind::Vector.directory(), true);
    ensure!(
        !data_before.is_empty(),
        "the indexed fixture must have committed data files"
    );
    ensure!(
        !index_before.is_empty(),
        "the indexed fixture must have committed vector index artifacts"
    );

    let zero_before = ann_query(ZERO_VECTOR_TEXT, 1);
    ensure_vector_index_route(h, &zero_before, "indexed-vector-before").await?;
    let before_ids = lake_ids(h, &zero_before).await?;
    ensure_eq(
        "pre-update zero-vector top1",
        before_ids.clone(),
        vec![INDEX_UPDATED_ID],
    )?;
    ensure_eq(
        "pre-update zero-vector oracle",
        before_ids,
        exact_top_k(false, &ZERO_VECTOR, 1)?,
    )?;

    let rows = h
        .lake
        .execute(
            &format!(
                "UPDATE {ANN_TABLE} SET vec = {NEW_VECTOR_SQL} \
                 WHERE id = {INDEX_UPDATED_ID}"
            ),
            &[],
        )
        .await
        .context("indexed vector UPDATE")?;
    ensure_eq("indexed vector UPDATE rows", rows, 1)?;
    ensure_eq(
        "indexed fixture keeps every row",
        lake_i64(h, &format!("SELECT count(*) FROM {ANN_TABLE}")).await?,
        CORPUS_ROWS as i64,
    )?;

    // Exact re-rank (the default): the candidate set is ranked on the live
    // vector column, so no stale candidate may be returned as a hit.
    let new_k1 = ann_query(NEW_VECTOR_TEXT, 1);
    ensure_vector_index_route(h, &new_k1, "indexed-vector-new-exact").await?;
    let new_ids = lake_ids(h, &new_k1).await?;
    ensure_eq(
        "updated row is nearest to its new vector",
        new_ids.clone(),
        vec![INDEX_UPDATED_ID],
    )?;
    ensure_eq(
        "new-vector oracle",
        new_ids,
        exact_top_k(true, &NEW_VECTOR, 1)?,
    )?;

    let zero_after = ann_query(ZERO_VECTOR_TEXT, 1);
    ensure_vector_index_route(h, &zero_after, "indexed-vector-old-exact").await?;
    let old_ids = lake_ids(h, &zero_after).await?;
    ensure_eq(
        "old-vector top1 is the exact live nearest",
        old_ids.clone(),
        exact_top_k(true, &ZERO_VECTOR, 1)?,
    )?;
    ensure!(
        !old_ids.contains(&INDEX_UPDATED_ID),
        "the superseded vector must not return the updated row, actual {old_ids:?}"
    );
    let zero_after_k5 = ann_query(ZERO_VECTOR_TEXT, 5);
    let old_top5 = lake_ids(h, &zero_after_k5).await?;
    ensure_eq(
        "old-vector top5 oracle",
        old_top5.clone(),
        exact_top_k(true, &ZERO_VECTOR, 5)?,
    )?;
    ensure!(
        !old_top5.contains(&INDEX_UPDATED_ID),
        "the updated row must not reappear in the old-vector top5, actual {old_top5:?}"
    );

    // Index-only ordering asks the index itself. The new vector can only rank
    // first if the UPDATE's commit indexed it: a stale index still ranks id 48
    // (the nearest old corpus vector) first and never sees id 0. The default
    // exact re-rank above is what prevents a superseded *result*; dropping the
    // old entry itself is the index builder's append/rebuild policy, not part
    // of the UPDATE contract.
    h.lake
        .batch_execute("SET lakesoul_vector_search.rerank = 'index'")
        .await
        .context("switch vector search to index-only ordering")?;
    ensure_vector_index_route(h, &new_k1, "indexed-vector-new-index-only").await?;
    ensure_eq(
        "the index itself ranks the updated row first for its new vector",
        lake_ids(h, &new_k1).await?,
        vec![INDEX_UPDATED_ID],
    )?;
    h.lake
        .batch_execute("SET lakesoul_vector_search.rerank = 'exact'")
        .await
        .context("restore exact re-ranking")?;

    let files_after = h.table_file_bytes(ANN_TABLE).await?;
    ensure_previous_files_unchanged(
        "indexed vector UPDATE data files",
        &data_before,
        &select_files(&files_after, IndexKind::Vector.directory(), false),
    )?;
    ensure!(
        select_files(&files_after, IndexKind::Vector.directory(), true) != index_before,
        "the UPDATE must publish changed vector index artifacts; the index \
         subtree is byte-identical"
    );

    eprintln!(
        "PASS case=indexed-vector-update id={INDEX_UPDATED_ID} exact-rerank=no-stale-result \
         index-only=new-vector-ranked-first index-artifacts=advanced \
         data-files=appended"
    );
    Ok(())
}

/// A post-publication index error must not turn into data-file rollback.
async fn index_maintenance_failure_keeps_committed_update(h: &Harness) -> Result<()> {
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
    // A regular file in place of the index directory also fails under root;
    // chmod alone would not deterministically inject the error in CI.
    let update = h
        .lake
        .execute(
            &format!(
                "UPDATE {ANN_TABLE} SET vec = ARRAY[700.0::float4,0.0::float4,\
                 0.5::float4,-0.5::float4,1.0::float4,-1.0::float4,\
                 0.25::float4,-0.25::float4] WHERE id={INDEX_UPDATED_ID}"
            ),
            &[],
        )
        .await;
    // Restore before inspecting the outcome; the harness owns both paths and
    // removes the entire warehouse if the suite is cancelled at the await.
    std::fs::remove_file(&directory)?;
    std::fs::rename(&saved, &directory)?;
    let error = update.expect_err("index storage failure must reach the client");
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
    ensure_eq(
        "committed vector remains visible after index error",
        distance,
        0.0,
    )?;
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
        "PASS case=update-index-maintenance-failure server-error=true \
         data-commits=+1 new-vector=visible rows=retained committed-files=retained"
    );
    Ok(())
}

/// A committed text index must follow the updated text.
///
/// The fixture holds `(1, 'apple pie'), (2, 'banana bread'), (3, 'cherry
/// tart'), (4, 'apple juice')` with one `text_match` index over `body`. After
/// moving row 1 to `elderberry pie`, `elderberry` must become findable through
/// the text index route (a stale index has no candidate at all) and `apple`
/// must keep only the untouched row.
async fn indexed_text_update(h: &Harness) -> Result<()> {
    let files_before = h.table_file_bytes(TEXT_TABLE).await?;
    let data_before = select_files(&files_before, IndexKind::Text.directory(), false);
    let index_before = select_files(&files_before, IndexKind::Text.directory(), true);
    ensure!(
        !index_before.is_empty(),
        "the text fixture must have committed index artifacts"
    );

    let apple = text_query("apple");
    ensure_text_index_route(h, &apple, "indexed-text-before").await?;
    ensure_eq(
        "pre-update apple rows",
        lake_ids(h, &apple).await?,
        vec![1, 4],
    )?;
    let elderberry = text_query("elderberry");
    ensure_eq(
        "new term matches nothing before the update",
        lake_ids(h, &elderberry).await?,
        Vec::new(),
    )?;

    let rows = h
        .lake
        .execute(
            &format!("UPDATE {TEXT_TABLE} SET body = 'elderberry pie' WHERE id = 1"),
            &[],
        )
        .await
        .context("indexed text UPDATE")?;
    ensure_eq("indexed text UPDATE rows", rows, 1)?;

    ensure_text_index_route(h, &elderberry, "indexed-text-new").await?;
    ensure_eq(
        "updated row matches its new term",
        lake_ids(h, &elderberry).await?,
        vec![1],
    )?;

    // A stale candidate for the old term must be removed by the exec's exact
    // predicate above the scan, not returned as a hit.
    ensure_text_index_route(h, &apple, "indexed-text-old").await?;
    ensure_eq(
        "apple keeps only the untouched row",
        lake_ids(h, &apple).await?,
        vec![4],
    )?;

    let files_after = h.table_file_bytes(TEXT_TABLE).await?;
    ensure_previous_files_unchanged(
        "indexed text UPDATE data files",
        &data_before,
        &select_files(&files_after, IndexKind::Text.directory(), false),
    )?;
    ensure!(
        select_files(&files_after, IndexKind::Text.directory(), true) != index_before,
        "the UPDATE must publish changed text index artifacts; the index \
         subtree is byte-identical"
    );

    eprintln!(
        "PASS case=indexed-text-update id=1 apple->elderberry new-term=indexed \
         superseded=not-returned index-artifacts=advanced"
    );
    Ok(())
}

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

/// Run one statement and return the server SQLSTATE when it is rejected, or
/// `None` when it succeeds. Extended-protocol analysis can reject at Parse;
/// an implementation may also defer the rejection to Execute, so both are
/// observed.
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

/// Assert that LakeSoul and PostgreSQL refuse a statement with the same
/// SQLSTATE, and that it is the expected one.
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

/// Run one statement over the extended protocol and return its row count.
async fn extended_rows(h: &Harness, sql: &str) -> Result<u64> {
    h.lake
        .execute(sql, &[])
        .await
        .with_context(|| format!("extended Execute: {sql}"))
}

/// Run one statement and return the `CommandComplete` row count.
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

/// Run one successful `UPDATE` on both servers and require the same row count.
async fn ensure_update_parity(
    h: &Harness,
    sql: &str,
    expected: u64,
    protocol: Protocol,
    context: &str,
) -> Result<()> {
    let lake = match protocol {
        Protocol::Simple => simple_tag_rows(&h.lake, sql).await?,
        Protocol::Extended => extended_rows(h, sql).await?,
    };
    let reference = simple_tag_rows(&h.reference, sql).await?;
    ensure!(
        lake == reference,
        "{context}: SQL={sql}: LakeSoul reported {lake} rows, PostgreSQL {reference}"
    );
    ensure!(
        lake == expected,
        "{context}: SQL={sql}: expected {expected} rows, actual {lake}"
    );
    Ok(())
}

/// Require the LakeSoul and PostgreSQL relations to agree exactly.
async fn ensure_relation_parity(h: &Harness, sql: &str, context: &str) -> Result<()> {
    let lake = compare::simple(&h.lake, sql)
        .await
        .with_context(|| format!("{context}: LakeSoul {sql}"))?;
    let reference = compare::simple(&h.reference, sql)
        .await
        .with_context(|| format!("{context}: PostgreSQL {sql}"))?;
    compare::compare(&lake, &reference, true).with_context(|| format!("{context}: {sql}"))
}

/// Require the LakeSoul relation to equal the expected text rows.
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

/// Build the text rows a simple query returns; `None` is SQL NULL and
/// `Some("")` is the empty string.
fn text_rows(rows: &[&[Option<&str>]]) -> compare::Rows {
    rows.iter()
        .map(|row| row.iter().map(|value| value.map(str::to_owned)).collect())
        .collect()
}

/// Create the same fixture relation on both servers and seed identical rows.
///
/// The LakeSoul DDL may carry LakeSoul-only extensions; the reference DDL is
/// the plain PostgreSQL equivalent holding the same columns and rows, so any
/// difference in the updated relation is a statement result difference.
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

/// The first query after a rejection: the connection and its statement cache
/// stay usable.
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

async fn lake_text(h: &Harness, sql: &str) -> Result<Option<String>> {
    Ok(h.lake
        .query_one(sql, &[])
        .await
        .with_context(|| format!("read {sql}"))?
        .get::<_, Option<String>>(0))
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

/// `(partition_desc, latest version)` of every published partition.
async fn partition_versions(h: &Harness, table_id: &str) -> Result<Vec<(String, i32)>> {
    Ok(h.reference
        .query(
            "SELECT DISTINCT ON (partition_desc) partition_desc, version \
             FROM public.partition_info WHERE table_id = $1 \
             ORDER BY partition_desc, version DESC",
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, i32>(1)))
        .collect())
}

/// `(partition_desc, committed records)` for every partition.
async fn committed_per_partition(
    h: &Harness,
    table_id: &str,
) -> Result<Vec<(String, i64)>> {
    Ok(h.reference
        .query(
            "SELECT partition_desc, count(*) FROM public.data_commit_info \
             WHERE table_id = $1 AND committed \
             GROUP BY partition_desc ORDER BY partition_desc",
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, i64>(1)))
        .collect())
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

/// `(partition_desc, path)` of every file operation of a committed commit.
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

async fn hash_bucket_num(h: &Harness, table: &str) -> Result<Option<String>> {
    Ok(h.reference
        .query_one(
            "SELECT properties->>'hashBucketNum' FROM public.table_info \
             WHERE table_namespace = $1 AND table_name = $2",
            &[&h.lake_namespace, &table],
        )
        .await?
        .get::<_, Option<String>>(0))
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

/// Every committed file exists on disk inside the suite warehouse.
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

/// Files the statement did not rewrite or replace must keep their exact bytes;
/// the statement must also add files.
fn ensure_previous_files_unchanged(
    context: &str,
    before: &BTreeMap<String, Vec<u8>>,
    after: &BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    for (path, bytes) in before {
        match after.get(path) {
            None => bail!(
                "{context}: previously committed file {path} disappeared; \
                 an update must append a new version instead of replacing the \
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
    ensure!(
        after.len() > before.len(),
        "{context}: the statement must add data files ({} before, {} after)",
        before.len(),
        after.len()
    );
    Ok(())
}

/// Every new committed file exists and sits under the partition it names.
fn ensure_files_under_partitions(
    context: &str,
    h: &Harness,
    files: &[(String, String)],
    expected: &[&str],
) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for (partition, path) in files {
        ensure!(
            path.contains(&format!("/{}/", partition.replace(',', "/"))),
            "{context}: committed file {path} is not under partition {partition}"
        );
        ensure!(
            path.ends_with(".parquet"),
            "{context}: committed file {path} is not a parquet file"
        );
        let local = std::path::Path::new(path.strip_prefix("file://").unwrap_or(path));
        ensure!(
            local.is_file(),
            "{context}: committed file {path} does not exist on disk"
        );
        ensure!(
            local.starts_with(&h.warehouse_dir),
            "{context}: committed file {path} is outside the suite warehouse"
        );
        seen.insert(partition.as_str());
    }
    for partition in expected {
        ensure!(
            seen.contains(partition),
            "{context}: no committed file for partition {partition}"
        );
    }
    Ok(())
}

/// `SELECT id ... ORDER BY vec <-> query LIMIT k` over the indexed fixture.
fn ann_query(query: &str, k: usize) -> String {
    format!(
        "SELECT id FROM public.{ANN_TABLE} \
         ORDER BY vec <-> '{query}'::vector({DIM}) LIMIT {k}"
    )
}

/// `text_match` query over the text-indexed fixture, ordered for determinism.
fn text_query(term: &str) -> String {
    format!(
        "SELECT id FROM public.{TEXT_TABLE} \
         WHERE text_match(body, '{term}') ORDER BY id LIMIT 10"
    )
}

/// Split one table's files into its `directory` artifacts (for example
/// `_vector_index`) and everything else, so data-file and index-file changes
/// can be asserted separately.
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

/// Exact top-k over the expected live corpus: the committed fixture vectors
/// with id [`INDEX_UPDATED_ID`] replaced by [`NEW_VECTOR`] when `updated`.
///
/// Independent float64 math over the input vectors; no query result, candidate
/// list or index supplies the oracle.
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

/// The final physical plan text of `EXPLAIN VERBOSE <sql>`.
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

/// The query must run through the physical vector index, not exact fallback.
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
    eprintln!(
        "PASS case={context} classification=lakesoul-vector-route indexed=true \
         SQL={sql}"
    );
    Ok(())
}

/// The query must run through the physical text index.
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
    eprintln!(
        "PASS case={context} classification=lakesoul-text-route indexed=true \
         SQL={sql}"
    );
    Ok(())
}

/// Assert that `error` is a server `ErrorResponse` with exactly `code`.
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
