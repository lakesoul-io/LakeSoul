// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Transaction-block contracts against the real server.
//!
//! The harness starts a fresh `postgres-lakesoul` child next to a PostgreSQL
//! reference schema. These cases cover what the block itself owes a client
//! before any write is staged: which statements it answers, which it refuses
//! with which SQLSTATE, and how a failed block ends.

#[path = "session_contract/common.rs"]
#[allow(dead_code)] // the block contract uses only part of the shared assertions
mod common;
#[allow(dead_code)] // this target uses only the fixtures and `run_suite`
use crate::support;

use anyhow::{Context, Result};

use crate::support::Harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_block_contract() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            nested_begin_keeps_one_block(h).await?;
            savepoints_are_transactional(h).await?;
            savepoint_recovers_a_failed_block(h).await?;
            create_table_is_staged(h).await?;
            create_table_rolls_back(h).await?;
            alter_table_is_staged(h).await?;
            drop_table_rolls_back(h).await?;
            drop_table_commits(h).await?;
            create_and_drop_is_a_noop(h).await?;
            drop_then_create_replaces_the_table(h).await?;
            insert_then_alter_commits(h).await?;
            concurrent_create_conflicts(h).await?;
            discard_all_cannot_run_inside_a_block(h).await?;
            commit_ends_a_failed_block(h).await?;
            extended_protocol_commit(h).await?;
            staged_insert_is_invisible_until_commit(h).await?;
            rollback_discards_a_staged_insert(h).await?;
            a_block_reads_its_own_writes(h).await?;
            a_duplicate_of_a_staged_key_is_refused(h).await?;
            an_update_inside_a_block_is_staged(h).await?;
            an_empty_write_does_not_block_the_commit(h).await?;
            a_lost_commit_race_is_a_serialization_failure(h).await?;
            a_peer_commit_between_statements_is_a_serialization_failure(h).await?;
            Ok(())
        })
    })
    .await
}

/// `BEGIN` inside a block is a warning, not a second transaction: the block
/// stays healthy and a read still runs.
async fn nested_begin_keeps_one_block(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.simple_query("BEGIN")
        .await
        .context("a nested BEGIN is answered, not refused")?;
    lake.simple_query("SELECT 1")
        .await
        .context("the block is still healthy after a nested BEGIN")?;
    lake.simple_query("ROLLBACK").await.context("ROLLBACK")?;
    eprintln!("sql-compat case=nested-begin contract=PG-equivalent outcome=ok");
    Ok(())
}

/// Savepoints stage and truncate data and table DDL independently.
async fn savepoints_are_transactional(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.execute(
        "INSERT INTO pk_items (id, val, label) VALUES (7401, 1, 'first')",
        &[],
    )
    .await
    .context("stage the first row")?;
    lake.simple_query("SAVEPOINT s")
        .await
        .context("SAVEPOINT")?;
    lake.execute(
        "INSERT INTO pk_items (id, val, label) VALUES (7402, 2, 'second')",
        &[],
    )
    .await
    .context("stage the second row")?;
    lake.simple_query("CREATE TABLE sp_ddl (id BIGINT PRIMARY KEY, val BIGINT)")
        .await
        .context("stage a table")?;
    lake.simple_query("ROLLBACK TO SAVEPOINT s")
        .await
        .context("ROLLBACK TO")?;
    lake.simple_query("COMMIT").await.context("COMMIT")?;

    let rows: i64 = h
        .lake
        .query_one(
            "SELECT count(*) FROM pk_items WHERE id IN (7401, 7402)",
            &[],
        )
        .await
        .context("count rows after savepoint commit")?
        .get(0);
    assert_eq!(rows, 1, "only the write before the savepoint committed");
    assert!(
        h.lake
            .query_one("SELECT count(*) FROM sp_ddl", &[])
            .await
            .is_err(),
        "the table created after the savepoint was rolled back"
    );
    // A savepoint also restores catalog visibility after a staged DROP.
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.simple_query("SAVEPOINT s")
        .await
        .context("SAVEPOINT")?;
    lake.simple_query("DROP TABLE empty_items")
        .await
        .context("stage DROP TABLE")?;
    assert!(
        lake.query_one("SELECT count(*) FROM empty_items", &[])
            .await
            .is_err(),
        "the staged drop hides the table"
    );
    lake.simple_query("ROLLBACK TO SAVEPOINT s")
        .await
        .context("ROLLBACK TO")?;
    lake.query_one("SELECT count(*) FROM empty_items", &[])
        .await
        .context("ROLLBACK TO restores the dropped table")?;
    lake.simple_query("COMMIT").await.context("COMMIT")?;
    eprintln!("sql-compat case=savepoint-truncation outcome=ok");
    Ok(())
}

/// `DROP TABLE` at `COMMIT` removes the table for every connection.
async fn drop_table_commits(h: &Harness) -> Result<()> {
    h.lake
        .simple_query("CREATE TABLE tx_drop_commit (id BIGINT PRIMARY KEY, val BIGINT)")
        .await
        .context("create the table to drop")?;
    h.lake
        .execute("INSERT INTO tx_drop_commit VALUES (1, 10)", &[])
        .await
        .context("insert the row to drop")?;
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.simple_query("DROP TABLE tx_drop_commit")
        .await
        .context("DROP TABLE inside a block")?;
    assert!(
        h.lake
            .query_one("SELECT count(*) FROM tx_drop_commit", &[])
            .await
            .is_ok(),
        "a peer still sees the table before COMMIT"
    );
    lake.simple_query("COMMIT").await.context("COMMIT")?;
    assert!(
        h.lake
            .query_one("SELECT count(*) FROM tx_drop_commit", &[])
            .await
            .is_err(),
        "COMMIT removes the table for peers"
    );
    eprintln!("sql-compat case=drop-table-commit outcome=removed");
    Ok(())
}

/// `CREATE` followed by `DROP` in one block leaves neither table nor files.
async fn create_and_drop_is_a_noop(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.simple_query("CREATE TABLE tx_noop (id BIGINT PRIMARY KEY, val BIGINT)")
        .await
        .context("CREATE TABLE")?;
    lake.execute("INSERT INTO tx_noop VALUES (1, 10)", &[])
        .await
        .context("INSERT")?;
    lake.simple_query("DROP TABLE tx_noop")
        .await
        .context("DROP TABLE")?;
    lake.simple_query("COMMIT").await.context("COMMIT")?;
    assert!(
        h.lake
            .query_one("SELECT count(*) FROM tx_noop", &[])
            .await
            .is_err(),
        "the table created and dropped in the block does not exist"
    );
    eprintln!("sql-compat case=create-drop-noop outcome=ok");
    Ok(())
}

/// `DROP` then `CREATE` with the same name publishes a fresh empty table.
async fn drop_then_create_replaces_the_table(h: &Harness) -> Result<()> {
    h.lake
        .simple_query("CREATE TABLE tx_swap (id BIGINT PRIMARY KEY, val BIGINT)")
        .await
        .context("create the original table")?;
    h.lake
        .execute("INSERT INTO tx_swap VALUES (1, 10)", &[])
        .await
        .context("insert the original row")?;
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.simple_query("DROP TABLE tx_swap")
        .await
        .context("DROP TABLE")?;
    lake.simple_query("CREATE TABLE tx_swap (id BIGINT PRIMARY KEY, val BIGINT)")
        .await
        .context("CREATE TABLE the replacement")?;
    lake.execute("INSERT INTO tx_swap VALUES (2, 20)", &[])
        .await
        .context("insert the replacement row")?;
    lake.simple_query("COMMIT").await.context("COMMIT")?;
    let rows: Vec<(i64, i64)> = h
        .lake
        .query("SELECT id, val FROM tx_swap ORDER BY id", &[])
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(rows, vec![(2, 20)], "only the replacement table remains");
    eprintln!("sql-compat case=drop-create-swap outcome=replaced");
    Ok(())
}

/// A write before `ALTER TABLE` uses the final definition at `COMMIT`.
async fn insert_then_alter_commits(h: &Harness) -> Result<()> {
    h.lake
        .simple_query("CREATE TABLE tx_alter (id BIGINT PRIMARY KEY, label TEXT)")
        .await
        .context("create the table to alter")?;
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.execute("INSERT INTO tx_alter VALUES (1, 'old')", &[])
        .await
        .context("stage the row before ALTER")?;
    lake.simple_query("ALTER TABLE tx_alter ADD COLUMN extra TEXT")
        .await
        .context("ALTER TABLE after the write")?;
    lake.execute("INSERT INTO tx_alter VALUES (2, 'new', 'x')", &[])
        .await
        .context("stage the row after ALTER")?;
    lake.simple_query("COMMIT").await.context("COMMIT")?;
    let rows: Vec<(i64, Option<String>)> = h
        .lake
        .query("SELECT id, extra FROM tx_alter ORDER BY id", &[])
        .await?
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(rows, vec![(1, None), (2, Some("x".to_string()))]);
    eprintln!("sql-compat case=insert-alter outcome=committed");
    Ok(())
}

/// A concurrent creator wins; the loser's `COMMIT` is `40001` and publishes
/// neither the table nor its rows.
async fn concurrent_create_conflicts(h: &Harness) -> Result<()> {
    let first = h.connect_lake().await?;
    let second = h.connect_lake().await?;
    first.simple_query("BEGIN").await.context("BEGIN")?;
    second.simple_query("BEGIN").await.context("BEGIN")?;
    first
        .simple_query("CREATE TABLE tx_race (id BIGINT PRIMARY KEY)")
        .await
        .context("first CREATE")?;
    second
        .simple_query("CREATE TABLE tx_race (id BIGINT PRIMARY KEY)")
        .await
        .context("second CREATE")?;
    first.simple_query("COMMIT").await.context("first COMMIT")?;
    let error = second
        .simple_query("COMMIT")
        .await
        .err()
        .context("the second COMMIT must lose the create race")?;
    common::ensure_sqlstate(&error, "40001", "concurrent CREATE")?;
    let count: i64 = h
        .lake
        .query_one("SELECT count(*) FROM tx_race", &[])
        .await?
        .get(0);
    assert_eq!(count, 0, "the winning table is empty");
    eprintln!("sql-compat case=concurrent-create sqlstate=40001 outcome=ok");
    Ok(())
}

/// `ROLLBACK TO SAVEPOINT` recovers a block whose statement failed.
async fn savepoint_recovers_a_failed_block(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.simple_query("SAVEPOINT s")
        .await
        .context("SAVEPOINT")?;
    lake.simple_query("SELECT 1/0")
        .await
        .err()
        .context("the statement fails")?;
    common::ensure_sqlstate(
        &lake.simple_query("SELECT 1").await.unwrap_err(),
        "25P02",
        "failed block",
    )?;
    lake.simple_query("ROLLBACK TO SAVEPOINT s")
        .await
        .context("ROLLBACK TO recovers")?;
    lake.simple_query("SELECT 1")
        .await
        .context("the block is usable again")?;
    lake.simple_query("ROLLBACK").await.context("ROLLBACK")?;
    eprintln!("sql-compat case=savepoint-recovery outcome=ok");
    Ok(())
}

/// `CREATE TABLE` and its rows become visible only at `COMMIT`.
async fn create_table_is_staged(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    // Extended protocol, like JDBC/Flyway.
    lake.execute(
        "CREATE TABLE tx_created (id BIGINT PRIMARY KEY, val BIGINT)",
        &[],
    )
    .await
    .context("CREATE TABLE inside a block")?;
    let catalog_rows: i64 = lake
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'tx_created'",
            &[],
        )
        .await
        .context("the pending table is visible in pg_class")?
        .get(0);
    assert_eq!(catalog_rows, 1);
    let empty: i64 = lake
        .query_one("SELECT count(*) FROM tx_created", &[])
        .await
        .context("the pending table is readable before any write")?
        .get(0);
    assert_eq!(empty, 0);
    lake.simple_query("ALTER TABLE tx_created ADD COLUMN extra TEXT")
        .await
        .context("ALTER the pending table")?;
    lake.execute("INSERT INTO tx_created VALUES (1, 10, 'x')", &[])
        .await
        .context("INSERT into the pending table")?;
    let staged: i64 = lake
        .query_one("SELECT count(*) FROM tx_created", &[])
        .await
        .context("the block reads its staged row in the pending table")?
        .get(0);
    assert_eq!(staged, 1);
    assert!(
        h.lake
            .query_one("SELECT count(*) FROM tx_created", &[])
            .await
            .is_err(),
        "a peer does not see the pending table"
    );
    lake.execute("COMMIT", &[]).await.context("COMMIT")?;
    let extra: String = h
        .lake
        .query_one("SELECT extra FROM tx_created", &[])
        .await
        .context("read the pending table after commit")?
        .get(0);
    assert_eq!(extra, "x", "COMMIT publishes the table and its rows");
    eprintln!("sql-compat case=create-table-staged outcome=committed");
    Ok(())
}

/// `ROLLBACK` removes a staged table and its files.
async fn create_table_rolls_back(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.simple_query("CREATE TABLE tx_rolled_back (id BIGINT PRIMARY KEY, val BIGINT)")
        .await
        .context("CREATE TABLE inside a block")?;
    lake.execute("INSERT INTO tx_rolled_back VALUES (1, 10)", &[])
        .await
        .context("INSERT into the pending table")?;
    lake.simple_query("ROLLBACK").await.context("ROLLBACK")?;
    assert!(
        h.lake
            .query_one("SELECT count(*) FROM tx_rolled_back", &[])
            .await
            .is_err(),
        "the rolled-back table does not exist"
    );
    eprintln!("sql-compat case=create-table-rollback outcome=gone");
    Ok(())
}

/// `ALTER TABLE` changes the pending definition later statements and the
/// committed metadata see.
async fn alter_table_is_staged(h: &Harness) -> Result<()> {
    h.lake
        .execute(
            "INSERT INTO pk_items (id, val, label) VALUES (7410, 10, 'old')",
            &[],
        )
        .await
        .context("publish the row to alter")?;
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.execute("ALTER TABLE pk_items ADD COLUMN extra TEXT", &[])
        .await
        .context("ALTER TABLE inside a block")?;
    lake.execute(
        "INSERT INTO pk_items (id, val, label, extra) VALUES (7411, 11, 'new', 'x')",
        &[],
    )
    .await
    .context("INSERT using the pending column")?;
    lake.simple_query("COMMIT").await.context("COMMIT")?;
    let extra: String = h
        .lake
        .query_one("SELECT extra FROM pk_items WHERE id = 7411", &[])
        .await
        .context("read the pending column after commit")?
        .get(0);
    assert_eq!(extra, "x");
    let old: Option<String> = h
        .lake
        .query_one("SELECT extra FROM pk_items WHERE id = 7410", &[])
        .await
        .context("read the old row after alter commit")?
        .get(0);
    assert_eq!(old, None, "old rows read the new column as NULL");
    eprintln!("sql-compat case=alter-table-staged outcome=committed");
    Ok(())
}

/// `DROP TABLE` is only applied at `COMMIT`.
async fn drop_table_rolls_back(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    lake.simple_query("DROP TABLE empty_items")
        .await
        .context("DROP TABLE inside a block")?;
    assert!(
        lake.query_one("SELECT count(*) FROM empty_items", &[])
            .await
            .is_err(),
        "the dropped table is hidden inside the block"
    );
    lake.simple_query("ROLLBACK").await.context("ROLLBACK")?;
    lake.query_one("SELECT count(*) FROM empty_items", &[])
        .await
        .context("ROLLBACK restores the table")?;
    eprintln!("sql-compat case=drop-table-rollback outcome=restored");
    Ok(())
}

/// `DISCARD ALL` runs outside a block and is refused - `25001` - inside one.
async fn discard_all_cannot_run_inside_a_block(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.simple_query("DISCARD ALL")
        .await
        .context("DISCARD ALL outside a block")?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    let error = lake
        .simple_query("DISCARD ALL")
        .await
        .err()
        .context("DISCARD ALL cannot run inside a block")?;
    common::ensure_sqlstate(&error, "25001", "DISCARD ALL inside a block")?;
    lake.simple_query("ROLLBACK").await.context("ROLLBACK")?;
    lake.simple_query("DISCARD ALL")
        .await
        .context("DISCARD ALL runs again after the block")?;
    eprintln!("sql-compat case=discard-all-in-block sqlstate=25001 outcome=ok");
    Ok(())
}

/// `COMMIT` in a failed block does not fail: it ends the transaction, and the
/// connection is usable afterwards.
async fn commit_ends_a_failed_block(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.simple_query("BEGIN").await.context("BEGIN")?;
    // The SQLSTATE of a DataFusion planning error is the front end's own
    // mapping (not this step's subject): what matters here is that the
    // statement fails and fails the block.
    lake.simple_query("SELECT * FROM missing_table_xyz")
        .await
        .err()
        .context("a missing table fails the statement")?;
    let aborted = lake
        .simple_query("SELECT 1")
        .await
        .err()
        .context("the block is failed")?;
    common::ensure_sqlstate(&aborted, "25P02", "failed block")?;
    lake.simple_query("COMMIT")
        .await
        .context("COMMIT in a failed block ends it")?;
    lake.simple_query("SELECT 1")
        .await
        .context("the connection is idle after the failed block")?;
    eprintln!("sql-compat case=commit-failed-block outcome=ended-and-usable");
    Ok(())
}

/// `BEGIN`/`COMMIT` through the extended protocol (what drivers use): the
/// `COMMIT` publishes at `Execute` and the connection is idle afterwards.
async fn extended_protocol_commit(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.execute("BEGIN", &[])
        .await
        .context("BEGIN via the extended protocol")?;
    lake.query("SELECT 1", &[])
        .await
        .context("a read runs inside the block")?;
    lake.execute("COMMIT", &[])
        .await
        .context("COMMIT via the extended protocol")?;
    lake.query("SELECT 1", &[])
        .await
        .context("the connection is idle after COMMIT")?;
    eprintln!("sql-compat case=extended-commit outcome=committed");
    Ok(())
}

/// A write inside a block is staged: nothing is published before `COMMIT`, and
/// the committed row is visible to a peer afterwards.
async fn staged_insert_is_invisible_until_commit(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(
        "INSERT INTO pk_items (id, val, label) VALUES (7101, 7, 'tx')",
        &[],
    )
    .await
    .context("INSERT inside a block is staged")?;
    let visible: i64 = h
        .lake
        .query_one("SELECT count(*) FROM pk_items WHERE id = 7101", &[])
        .await?
        .get(0);
    assert_eq!(visible, 0, "a peer does not see the staged row");
    lake.batch_execute("COMMIT").await.context("COMMIT")?;
    let visible: i64 = h
        .lake
        .query_one("SELECT count(*) FROM pk_items WHERE id = 7101", &[])
        .await?
        .get(0);
    assert_eq!(visible, 1, "COMMIT publishes the staged row");
    eprintln!("sql-compat case=staged-insert outcome=visible-after-commit");
    Ok(())
}

/// `ROLLBACK` leaves neither rows nor metadata behind.
async fn rollback_discards_a_staged_insert(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    let before = h.authoritative_snapshot().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(
        "INSERT INTO pk_items (id, val, label) VALUES (7102, 8, 'gone')",
        &[],
    )
    .await
    .context("INSERT inside a block is staged")?;
    lake.batch_execute("ROLLBACK").await.context("ROLLBACK")?;
    assert_eq!(
        h.authoritative_snapshot().await?,
        before,
        "ROLLBACK leaves no metadata behind"
    );
    let visible: i64 = h
        .lake
        .query_one("SELECT count(*) FROM pk_items WHERE id = 7102", &[])
        .await?
        .get(0);
    assert_eq!(visible, 0, "the rolled-back row was never published");
    eprintln!("sql-compat case=staged-insert-rollback outcome=discarded");
    Ok(())
}

/// A block reads its own writes, and a second statement may write the same
/// partition: its key scan sees what the earlier statement staged.
async fn a_block_reads_its_own_writes(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(
        "INSERT INTO pk_items (id, val, label) VALUES (7201, 1, 'first')",
        &[],
    )
    .await
    .context("the first statement stages")?;
    let inside: i64 = lake
        .query_one("SELECT count(*) FROM pk_items WHERE id = 7201", &[])
        .await?
        .get(0);
    assert_eq!(inside, 1, "the block reads the row it just staged");
    // Same partition, another statement: allowed now that the first write is
    // visible to its validation.
    lake.execute(
        "INSERT INTO pk_items (id, val, label) VALUES (7202, 2, 'second')",
        &[],
    )
    .await
    .context("a second statement may write the same partition")?;
    lake.batch_execute("COMMIT").await.context("COMMIT")?;
    let published: i64 = h
        .lake
        .query_one(
            "SELECT count(*) FROM pk_items WHERE id IN (7201, 7202)",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(published, 2, "both statements of the block published");
    eprintln!("sql-compat case=read-your-writes outcome=both-published");
    Ok(())
}

/// The key scan of a later statement sees the block's staged files, so a
/// duplicate of a staged key is refused with `23505` instead of publishing two
/// versions of the same key.
async fn a_duplicate_of_a_staged_key_is_refused(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(
        "INSERT INTO pk_items (id, val, label) VALUES (7203, 3, 'original')",
        &[],
    )
    .await
    .context("the first statement stages")?;
    let error = lake
        .execute(
            "INSERT INTO pk_items (id, val, label) VALUES (7203, 3, 'duplicate')",
            &[],
        )
        .await
        .err()
        .context("a duplicate of a staged key must be refused")?;
    common::ensure_sqlstate(&error, "23505", "duplicate of a staged key")?;
    // The failed statement aborts the block; COMMIT answers ROLLBACK.
    lake.batch_execute("COMMIT").await.context("COMMIT")?;
    let published: i64 = h
        .lake
        .query_one("SELECT count(*) FROM pk_items WHERE id = 7203", &[])
        .await?
        .get(0);
    assert_eq!(published, 0, "neither statement published");
    eprintln!("sql-compat case=staged-key-duplicate sqlstate=23505 outcome=refused");
    Ok(())
}

/// `UPDATE` inside a block is staged like an insert: the block reads the new
/// value, a peer does not, and `COMMIT` publishes it.
async fn an_update_inside_a_block_is_staged(h: &Harness) -> Result<()> {
    h.lake
        .execute(
            "INSERT INTO pk_items (id, val, label) VALUES (7204, 10, 'old')",
            &[],
        )
        .await
        .context("publish the row to update")?;
    let lake = h.connect_lake().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    let updated = lake
        .execute("UPDATE pk_items SET label = 'new' WHERE id = 7204", &[])
        .await
        .context("UPDATE inside a block is staged")?;
    assert_eq!(updated, 1, "the statement reports its row");
    let inside: String = lake
        .query_one("SELECT label FROM pk_items WHERE id = 7204", &[])
        .await?
        .get(0);
    assert_eq!(inside, "new", "the block reads its own update");
    let peer: String = h
        .lake
        .query_one("SELECT label FROM pk_items WHERE id = 7204", &[])
        .await?
        .get(0);
    assert_eq!(peer, "old", "a peer does not see the staged update");
    lake.batch_execute("COMMIT").await.context("COMMIT")?;
    let published: String = h
        .lake
        .query_one("SELECT label FROM pk_items WHERE id = 7204", &[])
        .await?
        .get(0);
    assert_eq!(published, "new", "COMMIT publishes the update");
    eprintln!("sql-compat case=staged-update outcome=visible-after-commit");
    Ok(())
}

/// A statement that writes no rows stages nothing: the block still commits, and
/// nothing is published.
async fn an_empty_write_does_not_block_the_commit(h: &Harness) -> Result<()> {
    let lake = h.connect_lake().await?;
    let before = h.authoritative_snapshot().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    let written = lake
        .execute(
            "INSERT INTO items (id, val, label) SELECT 1, 2, 'x' WHERE false",
            &[],
        )
        .await
        .context("an INSERT that writes no rows is accepted inside a block")?;
    assert_eq!(written, 0, "nothing was written");
    lake.batch_execute("COMMIT").await.context("COMMIT")?;
    assert_eq!(
        h.authoritative_snapshot().await?,
        before,
        "an empty write publishes nothing"
    );
    eprintln!("sql-compat case=empty-write-block outcome=commit-with-nothing-staged");
    Ok(())
}

/// Two blocks stage writes to the same partition; the first `COMMIT` wins and
/// the second loses the version race with `40001`, keeps nothing, and leaves
/// its connection usable.
async fn a_lost_commit_race_is_a_serialization_failure(h: &Harness) -> Result<()> {
    let first = h.connect_lake().await?;
    let second = h.connect_lake().await?;
    first
        .batch_execute("BEGIN")
        .await
        .context("BEGIN on the first block")?;
    second
        .batch_execute("BEGIN")
        .await
        .context("BEGIN on the second block")?;
    // Both blocks validate against the same partition version, so only the
    // first publication can succeed.
    first
        .execute(
            "INSERT INTO pk_items (id, val, label) VALUES (7105, 11, 'winner')",
            &[],
        )
        .await
        .context("the first block stages its row")?;
    second
        .execute(
            "INSERT INTO pk_items (id, val, label) VALUES (7106, 12, 'loser')",
            &[],
        )
        .await
        .context("the second block stages its row")?;
    // Both staged writes are already on disk; the loser's file carries its
    // label, so its absence afterwards proves the failed COMMIT deleted it.
    let staged_files = files_containing(&h.warehouse_dir, b"loser")?;
    assert_eq!(staged_files.len(), 1, "the loser staged exactly one file");

    first
        .batch_execute("COMMIT")
        .await
        .context("the first COMMIT publishes")?;

    let error = second
        .batch_execute("COMMIT")
        .await
        .err()
        .context("the second COMMIT must lose the partition version")?;
    common::ensure_sqlstate(&error, "40001", "the losing COMMIT")?;
    assert_eq!(
        files_containing(&h.warehouse_dir, b"loser")?,
        Vec::<std::path::PathBuf>::new(),
        "the losing COMMIT deletes the files it would have published"
    );
    let winner: i64 = h
        .lake
        .query_one("SELECT count(*) FROM pk_items WHERE id = 7105", &[])
        .await?
        .get(0);
    assert_eq!(winner, 1, "the winning block is published");
    let visible: i64 = h
        .lake
        .query_one("SELECT count(*) FROM pk_items WHERE id = 7106", &[])
        .await?
        .get(0);
    assert_eq!(visible, 0, "the losing block's row was never published");
    // The block is over: the connection is idle and runs the next statement.
    second
        .batch_execute("SELECT 1")
        .await
        .context("the losing connection is idle after the failed COMMIT")?;
    eprintln!("sql-compat case=commit-race sqlstate=40001 outcome=rolled-back-and-idle");
    Ok(())
}

/// Every warehouse file whose bytes contain `needle`, sorted.
fn files_containing(
    directory: &std::path::Path,
    needle: &[u8],
) -> Result<Vec<std::path::PathBuf>> {
    let mut found = Vec::new();
    let mut pending = vec![directory.to_path_buf()];
    while let Some(next) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if std::fs::read(&path)?
                .windows(needle.len())
                .any(|w| w == needle)
            {
                found.push(path);
            }
        }
    }
    found.sort();
    Ok(found)
}

/// A peer commit between two statements of a block is not silently
/// overwritten: the statements saw the same partition at different versions,
/// so `COMMIT` answers `40001` and publishes neither.
async fn a_peer_commit_between_statements_is_a_serialization_failure(
    h: &Harness,
) -> Result<()> {
    let lake = h.connect_lake().await?;
    lake.batch_execute("BEGIN").await.context("BEGIN")?;
    lake.execute(
        "INSERT INTO pk_items (id, val, label) VALUES (7301, 1, 'block')",
        &[],
    )
    .await
    .context("the block stages its first row")?;
    // The peer publishes into the same (single-partition) table while the
    // block is open: the block's second statement now sees a newer version.
    h.lake
        .execute(
            "INSERT INTO pk_items (id, val, label) VALUES (7302, 2, 'peer')",
            &[],
        )
        .await
        .context("the peer publishes")?;
    lake.execute(
        "INSERT INTO pk_items (id, val, label) VALUES (7303, 3, 'block')",
        &[],
    )
    .await
    .context("the block stages a second row")?;
    let error = lake
        .batch_execute("COMMIT")
        .await
        .err()
        .context("the block must not publish over the peer's commit")?;
    common::ensure_sqlstate(&error, "40001", "peer commit between statements")?;
    let peer_row: i64 = h
        .lake
        .query_one("SELECT count(*) FROM pk_items WHERE id = 7302", &[])
        .await?
        .get(0);
    assert_eq!(peer_row, 1, "the peer's row survives");
    let block_rows: i64 = h
        .lake
        .query_one(
            "SELECT count(*) FROM pk_items WHERE id IN (7301, 7303)",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(block_rows, 0, "the block published neither statement");
    eprintln!(
        "sql-compat case=peer-commit-between-statements sqlstate=40001 outcome=rolled-back"
    );
    Ok(())
}
