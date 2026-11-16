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
pub mod support;

use anyhow::{Context, Result};

use crate::support::Harness;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transaction_block_contract() -> Result<()> {
    support::run_suite(|h| {
        Box::pin(async move {
            nested_begin_keeps_one_block(h).await?;
            savepoints_and_chains_are_refused(h).await?;
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

/// Savepoints and the `AND CHAIN` forms do not exist here: they are refused
/// with `0A000`, which fails the block the way any statement error does.
async fn savepoints_and_chains_are_refused(h: &Harness) -> Result<()> {
    for sql in [
        "SAVEPOINT s",
        "RELEASE SAVEPOINT s",
        "ROLLBACK TO SAVEPOINT s",
        "COMMIT AND CHAIN",
        "ROLLBACK AND CHAIN",
    ] {
        let lake = h.connect_lake().await?;
        lake.simple_query("BEGIN").await.context("BEGIN")?;
        let error = lake
            .simple_query(sql)
            .await
            .err()
            .with_context(|| format!("{sql} must be refused inside a block"))?;
        common::ensure_sqlstate(&error, "0A000", sql)?;
        // PostgreSQL aborts the block on any statement error.
        let aborted = lake
            .simple_query("SELECT 1")
            .await
            .err()
            .with_context(|| format!("{sql} leaves the block failed"))?;
        common::ensure_sqlstate(&aborted, "25P02", sql)?;
        lake.simple_query("ROLLBACK").await.context("ROLLBACK")?;
        eprintln!(
            "sql-compat case=transaction-refusal sql={sql} sqlstate=0A000 outcome=ok"
        );
    }
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
