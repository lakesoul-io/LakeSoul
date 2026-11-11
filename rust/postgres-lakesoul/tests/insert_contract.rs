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
//!
//! [`insert_scenarios`] is the matrix both contracts run:
//! [`insert_contract`] executes it on the coordinator, and
//! [`distributed_insert_contract`] executes the same statements with two real
//! Flight workers and local fallback disabled, then pins the distributed-write
//! contract itself — a remote `INSERT ... SELECT`, task-stamped file names, one
//! commit, and the SQLSTATE of a worker-side duplicate key or NOT NULL
//! violation.

pub mod support;

use std::sync::Arc;

use anyhow::{Context, Result, bail, ensure};
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::session::{LakeSoulSessionFactory, LakeSoulSessionOptions};
use lakesoul_metadata::MetaDataClient;
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::types::Type;

use support::Harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_contract() -> Result<()> {
    support::run_suite(|h| Box::pin(async move { insert_scenarios(h).await })).await
}

/// The same statements, planned and executed by two real Flight workers.
///
/// Local fallback is disabled for this harness, so a statement the distributed
/// planner cannot ship fails instead of silently reverting to the coordinator:
/// a passing run holds for writes that run on workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_insert_contract() -> Result<()> {
    support::run_distributed_insert_suite(|h| {
        Box::pin(async move {
            insert_scenarios(h).await?;
            distributed_insert_select_writes_on_workers(h).await
        })
    })
    .await
}

/// The scenario matrix shared by the coordinator-only and the distributed
/// contract.
async fn insert_scenarios(h: &Harness) -> Result<()> {
    plain_insert_writes_rows_metadata_and_files(h).await?;
    parameterized_insert_executes_twice(h).await?;
    cdc_delete_marker_hides_the_key(h).await?;
    duplicate_key_leaves_no_state(h).await?;
    not_null_violation_is_23502(h).await?;
    on_conflict_do_nothing_skips_conflicts(h).await?;
    on_conflict_do_update_replaces_the_row(h).await?;
    upsert_source_duplicate_keys_are_21000(h).await?;
    multi_partition_insert_publishes_atomically(h).await?;
    partitioned_table_created_by_pg_writes_and_merges(h).await?;
    extended_partitioned_table_writes_parquet(h).await?;
    multi_column_partition_order_is_preserved(h).await?;
    refusal_matrix_changes_nothing(h).await?;
    prepared_insert_into_dropped_table_leaves_no_state(h).await?;
    insert_analysis_matches_postgres(h).await?;
    Ok(())
}

/// The same append-only `INSERT ... SELECT` under a session with real workers
/// and the local fallback disabled.
///
/// This test owns the PostgreSQL-level contract: the statement reports its
/// rows, publishes one commit, and commits files that exist inside the suite
/// warehouse. Whether the distributed planner places the write on workers is
/// pinned by the datafusion integration suite (`distributed_write_tests`),
/// which inspects the plan and the task-stamped file names; the wire protocol
/// must not claim more than the SQL result shows here.
async fn distributed_insert_select_writes_on_workers(h: &Harness) -> Result<()> {
    let table_id = table_id(h, "empty_items").await?;
    let rows_before = lake_i64(h, "SELECT count(*) FROM empty_items").await?;
    let committed_before = count(
        h,
        "SELECT count(*) FROM public.data_commit_info WHERE table_id = $1 AND committed",
        &table_id,
    )
    .await?;
    let files_before: std::collections::BTreeSet<String> =
        file_paths(h, &table_id).await?.into_iter().collect();

    let sql = "INSERT INTO empty_items (id, val, label) SELECT id, val, label FROM items";
    let rows = simple_rows(h, sql).await?;
    ensure_eq("distributed INSERT rows", rows, 4)?;

    let rows_after = lake_i64(h, "SELECT count(*) FROM empty_items").await?;
    ensure!(
        rows_after == rows_before + 4,
        "empty_items must gain the 4 source rows: before {rows_before}, \
         after {rows_after}"
    );
    ensure_eq(
        "copied row",
        lake_i64(
            h,
            "SELECT count(*) FROM empty_items \
             WHERE id = 2 AND val = 20 AND label = ' β '",
        )
        .await?,
        1,
    )?;

    let files_after = file_paths(h, &table_id).await?;
    let new_files: Vec<&String> = files_after
        .iter()
        .filter(|path| !files_before.contains(*path))
        .collect();
    ensure!(
        !new_files.is_empty(),
        "the distributed INSERT ... SELECT committed no data file"
    );
    for path in new_files {
        let local = path.strip_prefix("file://").unwrap_or(path);
        ensure!(
            std::path::Path::new(local).is_file(),
            "committed file {path} does not exist on disk"
        );
        ensure!(
            path.contains(&h.lake_namespace),
            "committed file {path} is outside the suite warehouse"
        );
    }
    ensure_eq(
        "commits added by the statement",
        count(
            h,
            "SELECT count(*) FROM public.data_commit_info WHERE table_id = $1 AND committed",
            &table_id,
        )
        .await?,
        committed_before + 1,
    )?;

    // A key that is already committed is rejected on a worker; the SQLSTATE
    // must survive the hop and the statement must publish nothing.
    let duplicate =
        "INSERT INTO pk_items (id, val, label) SELECT id, val, label FROM pk_items";
    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .execute(duplicate, &[])
        .await
        .expect_err("a duplicate key on a worker must fail the statement");
    ensure_sqlstate(&error, "23505", "distributed duplicate primary key")?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "distributed duplicate primary key")?;

    // The same for a NOT NULL column.
    let null_key = "INSERT INTO pk_items (id, val, label) \
                    SELECT CAST(NULL AS BIGINT) AS id, val, label FROM pk_items";
    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .execute(null_key, &[])
        .await
        .expect_err("a NOT NULL violation on a worker must fail the statement");
    ensure_sqlstate(&error, "23502", "distributed NOT NULL violation")?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "distributed NOT NULL violation")?;
    Ok(())
}

/// `INSERT ... VALUES` commits rows, one `data_commit_info` row, one partition
/// version and data files that exist on disk.
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
    ensure!(!paths.is_empty(), "INSERT must commit data file operations");
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

/// A CDC table drops a key whose newest version is a `delete` marker, and a
/// later `insert` revives it.
async fn cdc_delete_marker_hides_the_key(h: &Harness) -> Result<()> {
    h.lake
        .simple_query(
            "CREATE TABLE cdc_items (id BIGINT PRIMARY KEY, op TEXT, val BIGINT) \
             WITH (use_cdc = true, lakesoul_cdc_change_column = 'op')",
        )
        .await
        .context("create the CDC table")?;

    simple_rows(
        h,
        "INSERT INTO cdc_items (id, op, val) VALUES (1, 'insert', 10)",
    )
    .await?;
    ensure_eq(
        "insert row is visible",
        lake_i64(h, "SELECT count(*) FROM cdc_items").await?,
        1,
    )?;

    // The delete marker is the newest version of key 1: it takes part in the
    // merge and is then filtered out, so the key disappears from SELECT. A
    // second write for an existing key is an upsert, as for any primary-key
    // table (a plain INSERT would be `23505`).
    simple_rows(
        h,
        "INSERT INTO cdc_items (id, op, val) VALUES (1, 'delete', 10) \
         ON CONFLICT (id) DO UPDATE SET op = EXCLUDED.op, val = EXCLUDED.val",
    )
    .await?;
    ensure_eq(
        "delete marker hides the key",
        lake_i64(h, "SELECT count(*) FROM cdc_items").await?,
        0,
    )?;

    simple_rows(
        h,
        "INSERT INTO cdc_items (id, op, val) VALUES (1, 'insert', 20) \
         ON CONFLICT (id) DO UPDATE SET op = EXCLUDED.op, val = EXCLUDED.val",
    )
    .await?;
    ensure_eq(
        "a later insert revives the key",
        lake_i64(h, "SELECT count(*) FROM cdc_items WHERE val = 20").await?,
        1,
    )?;

    h.lake
        .simple_query("DROP TABLE cdc_items")
        .await
        .context("drop the CDC table")?;
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

/// PostgreSQL rejects an `INSERT ... ON CONFLICT DO UPDATE` whose source
/// repeats a key (`21000`): the command cannot affect one row twice. The
/// check runs before any file is written, and `DO NOTHING` still drops the
/// duplicate instead of failing.
async fn upsert_source_duplicate_keys_are_21000(h: &Harness) -> Result<()> {
    let create = "CREATE TABLE upsert_dups (id BIGINT NOT NULL PRIMARY KEY, val BIGINT)";
    for client in [&h.lake, &h.reference] {
        client
            .simple_query(create)
            .await
            .context("CREATE TABLE upsert_dups")?;
        client
            .simple_query("INSERT INTO upsert_dups (id, val) VALUES (1, 10), (2, 20)")
            .await
            .context("seed upsert_dups")?;
    }
    let before = h.authoritative_snapshot().await?;
    let duplicated = "INSERT INTO upsert_dups (id, val) VALUES (3, 1), (3, 2) \
                      ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val";
    for protocol in Protocol::BOTH {
        let lake = rejection(&h.lake, duplicated, protocol).await?;
        let reference = rejection(&h.reference, duplicated, protocol).await?;
        ensure_rejection(
            lake,
            reference,
            "21000",
            "upsert source repeats a key",
            protocol.label(),
        )?;
    }
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "duplicate upsert source")?;
    ensure_eq(
        "duplicate upsert left the table unchanged",
        lake_i64(h, "SELECT count(*) FROM upsert_dups").await?,
        2,
    )?;

    let do_nothing =
        "INSERT INTO upsert_dups (id, val) VALUES (4, 1), (4, 2) ON CONFLICT DO NOTHING";
    for protocol in Protocol::BOTH {
        ensure!(
            rejection(&h.lake, do_nothing, protocol).await?.is_none(),
            "case=upsert DO NOTHING duplicate protocol={}: LakeSoul rejected it",
            protocol.label()
        );
        ensure!(
            rejection(&h.reference, do_nothing, protocol)
                .await?
                .is_none(),
            "case=upsert DO NOTHING duplicate protocol={}: PostgreSQL rejected it",
            protocol.label()
        );
    }
    ensure_eq(
        "DO NOTHING kept one row of the duplicate source",
        lake_i64(h, "SELECT count(*) FROM upsert_dups WHERE id = 4").await?,
        1,
    )?;

    let distinct = "INSERT INTO upsert_dups (id, val) VALUES (1, 100), (5, 50) \
                    ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val";
    for protocol in Protocol::BOTH {
        ensure!(
            rejection(&h.lake, distinct, protocol).await?.is_none(),
            "case=upsert distinct keys protocol={}: LakeSoul rejected it",
            protocol.label()
        );
        ensure!(
            rejection(&h.reference, distinct, protocol).await?.is_none(),
            "case=upsert distinct keys protocol={}: PostgreSQL rejected it",
            protocol.label()
        );
    }
    ensure_eq(
        "distinct upsert updated the existing row",
        lake_i64(h, "SELECT val FROM upsert_dups WHERE id = 1").await?,
        100,
    )?;
    ensure_eq(
        "distinct upsert inserted the new row",
        lake_i64(h, "SELECT val FROM upsert_dups WHERE id = 5").await?,
        50,
    )?;
    eprintln!(
        "PASS case=upsert-duplicate-source lake=21000 postgres=21000 do-nothing=accepted distinct=applied"
    );
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

/// A table created by the PostgreSQL front end with the LakeSoul
/// `PARTITIONED BY (part)` extension writes real per-partition files, and the
/// same key has one row per partition while a later commit for one key in one
/// partition replaces only that partition's row.
///
/// The DDL and both reads are PostgreSQL. The second commit is issued by the
/// backend merge path (`INSERT OR REPLACE`) because the PostgreSQL front end
/// still refuses `ON CONFLICT` on a range-partitioned table: a PostgreSQL
/// conflict target cannot name a partition-local key. The refusal itself is
/// part of this contract.
async fn partitioned_table_created_by_pg_writes_and_merges(h: &Harness) -> Result<()> {
    let table = "pg_partition_items";
    h.lake
        .simple_query(&format!(
            "CREATE TABLE {table} (\
               part INTEGER NOT NULL, id BIGINT NOT NULL, val TEXT, \
               PRIMARY KEY (id)\
             ) PARTITIONED BY (part) WITH (hash_bucket_num = 4)"
        ))
        .await
        .context("PG CREATE TABLE ... PARTITIONED BY (part)")?;

    let table_id = table_id(h, table).await?;
    let (partitions, file_format, buckets) = partition_table_info(h, table).await?;
    ensure_eq("partition declaration", partitions, "part;id".to_string())?;
    ensure_eq(
        "default file format",
        file_format.as_deref(),
        Some("vortex-compact"),
    )?;
    ensure_eq("hash bucket count", buckets.as_deref(), Some("4"))?;
    ensure_eq(
        "partition_info before the first write",
        partition_versions(h, &table_id).await?,
        Vec::new(),
    )?;
    ensure_eq(
        "data_commit_info before the first write",
        commit_rows(h, &table_id).await?,
        Vec::new(),
    )?;

    // One plain PostgreSQL INSERT spans partitions 0 and 1. Key id = 1
    // appears in both partitions: LakeSoul keys are partition-local, so both
    // rows stay visible instead of colliding.
    let rows = simple_rows(
        h,
        &format!(
            "INSERT INTO {table} (part, id, val) VALUES \
             (0, 1, 'zero-a'), (0, 2, 'zero-b'), (1, 1, 'one-a')"
        ),
    )
    .await?;
    ensure_eq("first partition commit rows", rows, 3)?;

    ensure_eq(
        "first partition_info",
        partition_versions(h, &table_id).await?,
        vec![("part=0".to_string(), 0), ("part=1".to_string(), 0)],
    )?;
    ensure_eq(
        "first commit data_commit_info",
        commit_rows(h, &table_id).await?,
        vec![("part=0".to_string(), true), ("part=1".to_string(), true)],
    )?;
    ensure_partition_files(
        "first partition commit",
        &commit_file_paths(h, &table_id).await?,
        &["part=0", "part=1"],
        ".vortex",
        h,
    )?;

    ensure_eq(
        "first commit rows via PG",
        partition_rows(h, table).await?,
        vec![
            (0, 1, Some("zero-a".to_string())),
            (0, 2, Some("zero-b".to_string())),
            (1, 1, Some("one-a".to_string())),
        ],
    )?;
    ensure_eq(
        "first commit partition filter",
        lake_i64(h, &format!("SELECT count(*) FROM {table} WHERE part = 1")).await?,
        1,
    )?;
    ensure_eq(
        "first commit partition aggregate",
        partition_groups(h, table).await?,
        vec![(0, 2), (1, 1)],
    )?;
    ensure_eq(
        "same key across partitions",
        partition_ids(h, table, 1).await?,
        vec![0, 1],
    )?;

    // A range-partitioned table has one unique key per partition, which a
    // PostgreSQL conflict target cannot express; the front end still refuses
    // ON CONFLICT before any file or metadata row is produced.
    let error = h
        .lake
        .execute(
            &format!(
                "INSERT INTO {table} (part, id, val) VALUES (0, 1, 'conflict') \
                 ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val"
            ),
            &[],
        )
        .await
        .expect_err("ON CONFLICT on a range-partitioned table must be refused");
    ensure_sqlstate(&error, "0A000", "ON CONFLICT on a range-partitioned table")?;
    ensure_eq(
        "ON CONFLICT refusal leaves partition_info",
        partition_versions(h, &table_id).await?,
        vec![("part=0".to_string(), 0), ("part=1".to_string(), 0)],
    )?;

    // The second commit updates id = 1 inside partition 0, inserts id = 2 into
    // partition 1, and inserts id = 1 into the new partition 2. The backend
    // merge publishes one version per affected partition.
    management_replace_rows(
        h,
        table,
        "(0, 1, 'zero-updated'), (1, 2, 'one-b'), (2, 1, 'two-a')",
    )
    .await?;

    ensure_eq(
        "merge commit partition_info",
        partition_versions(h, &table_id).await?,
        vec![
            ("part=0".to_string(), 1),
            ("part=1".to_string(), 1),
            ("part=2".to_string(), 0),
        ],
    )?;
    ensure_eq(
        "merge commit data_commit_info",
        commit_rows(h, &table_id).await?,
        vec![
            ("part=0".to_string(), true),
            ("part=0".to_string(), true),
            ("part=1".to_string(), true),
            ("part=1".to_string(), true),
            ("part=2".to_string(), true),
        ],
    )?;
    ensure_partition_files(
        "merged commits",
        &commit_file_paths(h, &table_id).await?,
        &["part=0", "part=1", "part=2"],
        ".vortex",
        h,
    )?;

    // PostgreSQL reads the exact merge result: partition 0's row was replaced,
    // partition 1 kept its old row and gained another key, and the same key in
    // partition 2 is a distinct row.
    ensure_eq(
        "merged rows via PG",
        partition_rows(h, table).await?,
        vec![
            (0, 1, Some("zero-updated".to_string())),
            (0, 2, Some("zero-b".to_string())),
            (1, 1, Some("one-a".to_string())),
            (1, 2, Some("one-b".to_string())),
            (2, 1, Some("two-a".to_string())),
        ],
    )?;
    ensure_eq(
        "merged partition aggregate",
        partition_groups(h, table).await?,
        vec![(0, 2), (1, 2), (2, 1)],
    )?;
    ensure_eq(
        "same key in three partitions",
        partition_ids(h, table, 1).await?,
        vec![0, 1, 2],
    )?;
    ensure_eq(
        "same key same partition merge",
        lake_i64(
            h,
            &format!(
                "SELECT count(*) FROM {table} \
                 WHERE part = 0 AND id = 1 AND val = 'zero-updated'"
            ),
        )
        .await?,
        1,
    )?;

    // A plain PostgreSQL INSERT is insert-only: the same key in the same
    // partition is a duplicate, and the rejected statement leaves no state.
    let before = h.authoritative_snapshot().await?;
    let error = h
        .lake
        .execute(
            &format!("INSERT INTO {table} (part, id, val) VALUES (0, 2, 'collision')"),
            &[],
        )
        .await
        .expect_err("a duplicate key inside one partition must fail");
    ensure_sqlstate(&error, "23505", "duplicate key inside one partition")?;
    let after = h.authoritative_snapshot().await?;
    ensure_snapshot_unchanged(&before, &after, "partition-local duplicate key")?;

    let other = h.connect_lake().await?;
    let count = other
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .await?
        .get::<_, i64>(0);
    ensure_eq("partitioned rows on a new connection", count, 5)?;

    eprintln!(
        "PASS case=pg-partitioned-create partitions=3 pg-insert-across-partitions=1 \
         backend-merge-commit=1 on-conflict-refused=0A000 duplicate=23505 \
         format=vortex-compact"
    );
    Ok(())
}

/// The extended protocol creates a partitioned table with an explicit
/// `file_format`, and a prepared parameterized `INSERT` spans two partitions.
/// Committed files carry the `.parquet` extension under their partition
/// directories.
async fn extended_partitioned_table_writes_parquet(h: &Harness) -> Result<()> {
    let table = "pg_partition_parquet_items";
    let create = h
        .lake
        .prepare(&format!(
            "CREATE TABLE {table} (\
               part INTEGER NOT NULL, id BIGINT NOT NULL, val TEXT, \
               PRIMARY KEY (id)\
             ) PARTITIONED BY (part) \
             WITH (hash_bucket_num = 4, file_format = 'parquet')"
        ))
        .await
        .context("extended Parse of partitioned CREATE TABLE")?;
    ensure_eq(
        "extended partitioned CREATE rows",
        h.lake.execute(&create, &[]).await?,
        0,
    )?;

    let table_id = table_id(h, table).await?;
    let (partitions, file_format, buckets) = partition_table_info(h, table).await?;
    ensure_eq(
        "extended partition declaration",
        partitions,
        "part;id".to_string(),
    )?;
    ensure_eq(
        "explicit parquet format",
        file_format.as_deref(),
        Some("parquet"),
    )?;
    ensure_eq("extended hash bucket count", buckets.as_deref(), Some("4"))?;

    let parameter_types = [
        Type::INT4,
        Type::INT8,
        Type::TEXT,
        Type::INT4,
        Type::INT8,
        Type::TEXT,
    ];
    let insert = h
        .lake
        .prepare_typed(
            &format!(
                "INSERT INTO {table} (part, id, val) VALUES \
                 ($1, $2, $3), ($4, $5, $6)"
            ),
            &parameter_types,
        )
        .await
        .context("extended Parse of partitioned INSERT")?;
    ensure_eq(
        "extended partition parameter types",
        insert.params().to_vec(),
        parameter_types.to_vec(),
    )?;
    let rows = h
        .lake
        .execute(&insert, &[&0i32, &10i64, &"p0", &1i32, &10i64, &"p1"])
        .await
        .context("extended Execute of partitioned INSERT")?;
    ensure_eq("extended partition insert rows", rows, 2)?;

    ensure_eq(
        "extended partition_info",
        partition_versions(h, &table_id).await?,
        vec![("part=0".to_string(), 0), ("part=1".to_string(), 0)],
    )?;
    ensure_eq(
        "extended data_commit_info",
        commit_rows(h, &table_id).await?,
        vec![("part=0".to_string(), true), ("part=1".to_string(), true)],
    )?;
    ensure_partition_files(
        "extended parquet commit",
        &commit_file_paths(h, &table_id).await?,
        &["part=0", "part=1"],
        ".parquet",
        h,
    )?;
    ensure_eq(
        "extended partition rows via PG",
        partition_rows(h, table).await?,
        vec![
            (0, 10, Some("p0".to_string())),
            (1, 10, Some("p1".to_string())),
        ],
    )?;
    ensure_eq(
        "extended partition aggregate",
        partition_groups(h, table).await?,
        vec![(0, 1), (1, 1)],
    )?;
    eprintln!(
        "PASS case=extended-partitioned-parquet PG-DDL=true prepared-insert=true \
         partitions=2 format=parquet"
    );
    Ok(())
}

/// Partition constants follow declaration order, not schema order. Exercise
/// both MOR and append scans with different-typed, reversed partition columns.
async fn multi_column_partition_order_is_preserved(h: &Harness) -> Result<()> {
    for (table, format, key, extension) in [
        (
            "pg_multi_vortex",
            "vortex-compact",
            ", PRIMARY KEY (id)",
            ".vortex",
        ),
        ("pg_multi_parquet", "parquet", "", ".parquet"),
    ] {
        h.lake
            .simple_query(&format!(
                "CREATE TABLE {table} (region TEXT NOT NULL, part INTEGER NOT NULL, \
                 id BIGINT NOT NULL, val TEXT{key}) PARTITIONED BY (part, region) \
                 WITH (file_format = '{format}')"
            ))
            .await?;
        h.lake
            .simple_query(&format!(
                "INSERT INTO {table} VALUES ('east', 1, 1, 'a'), \
                 ('west', 1, 1, 'b'), ('east', 2, 2, 'c')"
            ))
            .await?;
        let rows: Vec<(String, i32, i64, String)> = h
            .lake
            .query(
                &format!(
                    "SELECT region, part, id, val FROM {table} ORDER BY part, region"
                ),
                &[],
            )
            .await?
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect();
        ensure_eq(
            "multi-column partition projection",
            rows,
            vec![
                ("east".to_string(), 1, 1, "a".to_string()),
                ("west".to_string(), 1, 1, "b".to_string()),
                ("east".to_string(), 2, 2, "c".to_string()),
            ],
        )?;
        let groups: Vec<(i32, String, i64, i64)> = h
            .lake
            .query(
                &format!(
                    "SELECT part, region, count(*), sum(id) FROM {table} \
                     GROUP BY part, region ORDER BY part, region"
                ),
                &[],
            )
            .await?
            .into_iter()
            .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)))
            .collect();
        ensure_eq(
            "multi-column partition aggregate",
            groups,
            vec![
                (1, "east".to_string(), 1, 1),
                (1, "west".to_string(), 1, 1),
                (2, "east".to_string(), 1, 2),
            ],
        )?;
        let value: String = h
            .lake
            .query_one(
                &format!("SELECT val FROM {table} WHERE part = 1 AND region = 'west'"),
                &[],
            )
            .await?
            .get(0);
        ensure_eq("multi-column partition filter", value, "b".to_string())?;
        let id = table_id(h, table).await?;
        ensure_eq(
            "multi-column partition descriptions",
            partition_versions(h, &id).await?,
            vec![
                ("part=1,region=east".to_string(), 0),
                ("part=1,region=west".to_string(), 0),
                ("part=2,region=east".to_string(), 0),
            ],
        )?;
        ensure_partition_files(
            "multi-column partition files",
            &commit_file_paths(h, &id).await?,
            &[
                "part=1,region=east",
                "part=1,region=west",
                "part=2,region=east",
            ],
            extension,
            h,
        )?;
        eprintln!(
            "PASS case=multi-column-partition-order format={format} pk={}",
            !key.is_empty()
        );
    }
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

/// `(partitions, file_format, hash_bucket_num)` from the table's metadata.
async fn partition_table_info(
    h: &Harness,
    table: &str,
) -> Result<(String, Option<String>, Option<String>)> {
    let row = h
        .reference
        .query_one(
            "SELECT partitions, properties->>'file_format', \
             properties->>'hashBucketNum' FROM public.table_info \
             WHERE table_namespace = $1 AND table_name = $2",
            &[&h.lake_namespace, &table],
        )
        .await
        .with_context(|| format!("metadata read for {table}"))?;
    Ok((
        row.get::<_, String>(0),
        row.get::<_, Option<String>>(1),
        row.get::<_, Option<String>>(2),
    ))
}

/// `(partition_desc, latest version)` of every published partition.
///
/// `partition_info` keeps one row per published version, so the latest row per
/// partition is selected explicitly.
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

/// `(partition_desc, committed)` of every `data_commit_info` row.
async fn commit_rows(h: &Harness, table_id: &str) -> Result<Vec<(String, bool)>> {
    Ok(h.reference
        .query(
            "SELECT partition_desc, committed FROM public.data_commit_info \
             WHERE table_id = $1 ORDER BY partition_desc, commit_id",
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, bool>(1)))
        .collect())
}

/// `(partition_desc, path)` of every file operation of a committed commit.
async fn commit_file_paths(h: &Harness, table_id: &str) -> Result<Vec<(String, String)>> {
    Ok(h.reference
        .query(
            "SELECT d.partition_desc, (operation).path FROM public.data_commit_info d \
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

/// Rows of a partition fixture in partition/key order.
async fn partition_rows(
    h: &Harness,
    table: &str,
) -> Result<Vec<(i32, i64, Option<String>)>> {
    Ok(h.lake
        .query(
            &format!("SELECT part, id, val FROM {table} ORDER BY part, id"),
            &[],
        )
        .await?
        .into_iter()
        .map(|row| {
            (
                row.get::<_, i32>(0),
                row.get::<_, i64>(1),
                row.get::<_, Option<String>>(2),
            )
        })
        .collect())
}

/// `(part, count(*))` for a partition fixture, ordered by partition.
async fn partition_groups(h: &Harness, table: &str) -> Result<Vec<(i32, i64)>> {
    Ok(h.lake
        .query(
            &format!("SELECT part, count(*) FROM {table} GROUP BY part ORDER BY part"),
            &[],
        )
        .await?
        .into_iter()
        .map(|row| (row.get::<_, i32>(0), row.get::<_, i64>(1)))
        .collect())
}

/// Partitions holding `id`, ordered, to show one key can live in several.
async fn partition_ids(h: &Harness, table: &str, id: i64) -> Result<Vec<i32>> {
    Ok(h.lake
        .query(
            &format!("SELECT part FROM {table} WHERE id = $1 ORDER BY part"),
            &[&id],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, i32>(0))
        .collect())
}

/// Every committed file exists, sits under the directory of its partition,
/// and uses the expected physical format extension. Each expected partition
/// has at least one committed file.
fn ensure_partition_files(
    context: &str,
    files: &[(String, String)],
    expected_partitions: &[&str],
    extension: &str,
    h: &Harness,
) -> Result<()> {
    let mut seen = std::collections::BTreeSet::new();
    for (partition, path) in files {
        ensure!(
            path.contains(&format!("/{}/", partition.replace(',', "/"))),
            "{context}: committed file {path} is not under partition {partition}"
        );
        ensure!(
            path.ends_with(extension),
            "{context}: committed file {path} does not use {extension}"
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
    for partition in expected_partitions {
        ensure!(
            seen.contains(partition),
            "{context}: no committed file for partition {partition}"
        );
    }
    Ok(())
}

/// A backend merge commit into a PostgreSQL-created table.
///
/// The PostgreSQL front end still refuses `ON CONFLICT` on range-partitioned
/// tables because a PostgreSQL conflict target cannot name a partition-local
/// key. The existing `INSERT OR REPLACE` write path is the backend merge, so
/// the test uses it for the same-key update and the cross-partition key; the
/// table itself and every read stay on the PostgreSQL surface.
async fn management_replace_rows(h: &Harness, table: &str, values: &str) -> Result<()> {
    let meta = Arc::new(MetaDataClient::from_env().await.map_err(|error| {
        anyhow::anyhow!("partition merge metadata client: {error:?}")
    })?);
    let warehouse = format!("file://{}", h.warehouse_dir.to_string_lossy());
    let factory = LakeSoulSessionFactory::new(
        meta,
        &CoreArgs {
            warehouse_prefix: Some(warehouse),
            ..CoreArgs::default()
        },
    )
    .map_err(|error| anyhow::anyhow!("partition merge session factory: {error:?}"))?;
    let session = factory
        .create_session(&LakeSoulSessionOptions {
            default_schema: h.lake_namespace.clone(),
            time_zone: Some("UTC".to_string()),
            ..LakeSoulSessionOptions::default()
        })
        .map_err(|error| {
            anyhow::anyhow!("partition merge management session: {error:?}")
        })?;
    let sql = format!(
        "INSERT OR REPLACE INTO lakesoul.{}.{table} (part, id, val) VALUES {values}",
        h.lake_namespace
    );
    session
        .sql(&sql)
        .await
        .with_context(|| format!("management commit for {table}"))?
        .collect()
        .await
        .with_context(|| format!("collect management commit for {table}"))?;
    Ok(())
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
