// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul CDC contract over real committed files.
//!
//! `cdc_items` holds three independent commits (three Parquet files) of a
//! primary-key table with the change column `op`: five inserts, then an
//! update of key 1, a delete tombstone for key 2 whose value is below the
//! query threshold, a delete tombstone for key 3, an insert of key 6, and
//! finally a reinsert of key 3 plus key 7. Key 2 is deleted, key 1 and key 3
//! have superseded values in older files.
//!
//! The PostgreSQL reference table is a plain table holding the hand-derived
//! final relation `(1,3),(3,7),(4,40),(5,50),(6,60),(7,70)` with
//! `op = 'insert'`. It is an oracle for the final rows only, not a claim that
//! PostgreSQL implements LakeSoul merge-on-read or delete tombstones.
//!
//! Every case is labeled with the LakeSoul-specific contract it covers and
//! the consumer-visible failure it must catch. No case reads back actual
//! output as its expectation, and none mutates the fixture. Identical SQL
//! text runs against both servers through the simple text and extended
//! binary protocols with exact column names and OIDs.

use anyhow::Result;
use tokio_postgres::types::Type;

use crate::common::{assert_query, rows};
use crate::support::Harness;

pub async fn run(h: &Harness) -> Result<()> {
    live_projection_drops_tombstones(h).await?;
    deleted_key_lookup(h).await?;
    non_key_filter_does_not_resurrect(h).await?;
    reinserted_key_exposes_new_value(h).await?;
    visible_version_accounting(h).await?;
    change_column_groups(h).await?;
    join_does_not_leak_tombstones(h).await?;
    Ok(())
}

/// LakeSoul-specific: the newest version per primary key wins, and rows whose
/// change column is `delete` are dropped after the merge. The projection does
/// not reference `op`, so tombstone removal cannot depend on it being read.
async fn live_projection_drops_tombstones(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "cdc-live-projection-drops-tombstones",
        "Projection ignoring `op` returns the delete tombstone of key 2, or a superseded version of key 1 (score 10) / key 3 (score 30), so the visible bag and cardinality change.",
        "SELECT id, score FROM cdc_items ORDER BY id",
        &[("id", Type::INT4), ("score", Type::INT4)],
        &rows(&[
            &[Some("1"), Some("3")],
            &[Some("3"), Some("7")],
            &[Some("4"), Some("40")],
            &[Some("5"), Some("50")],
            &[Some("6"), Some("60")],
            &[Some("7"), Some("70")],
        ]),
        true,
    )
    .await
}

/// LakeSoul-specific: a primary-key candidate/locator filter still goes
/// through the merge, so the tombstone wins over the pre-delete row and the
/// key disappears entirely.
async fn deleted_key_lookup(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "cdc-deleted-key-key-lookup",
        "The primary-key candidate path returns the tombstone (2,5) or resurrects the pre-delete row (2,20) although key 2's newest version is a delete.",
        "SELECT id, score FROM cdc_items WHERE id = 2",
        &[("id", Type::INT4), ("score", Type::INT4)],
        &rows(&[]),
        false,
    )
    .await
}

/// LakeSoul-specific: a non-key predicate is evaluated above the merge. Keys
/// 1, 2 and 3 have superseded inserts (10, 20, 30) that satisfy `score >= 10`
/// while their newest versions (3, tombstone 5, reinsert 7) do not; pushing
/// the predicate below the merge would resurrect those old rows.
async fn non_key_filter_does_not_resurrect(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "cdc-non-key-filter-no-resurrection",
        "Evaluating `score >= 10` per file before the merge keeps superseded inserts (1,10), (2,20), (3,30) whose latest version is an update, a delete, or a reinsert below the threshold.",
        "SELECT id, score FROM cdc_items WHERE score >= 10 ORDER BY id",
        &[("id", Type::INT4), ("score", Type::INT4)],
        &rows(&[
            &[Some("4"), Some("40")],
            &[Some("5"), Some("50")],
            &[Some("6"), Some("60")],
            &[Some("7"), Some("70")],
        ]),
        true,
    )
    .await
}

/// LakeSoul-specific: a later insert revives exactly the deleted key it
/// names. Key 2 must stay gone; key 3 must show its reinserted value, not its
/// delete tombstone (3,30) nor the pre-delete value 30.
async fn reinserted_key_exposes_new_value(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "cdc-reinserted-key-exposes-new-value",
        "Reinserting key 3 must expose only (3,7); returning key 2 again, key 3's tombstone (3,30), or its pre-delete value 30 means delete tombstones are ordered or filtered incorrectly.",
        "SELECT id, score FROM cdc_items WHERE id IN (2, 3) ORDER BY id",
        &[("id", Type::INT4), ("score", Type::INT4)],
        &rows(&[&[Some("3"), Some("7")]]),
        true,
    )
    .await
}

/// LakeSoul-specific: every physical row is a version contribution, so a
/// reader that counts files or keeps tombstones reports 11 physical rows
/// instead of the 6 rows visible after the merge.
async fn visible_version_accounting(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "cdc-visible-version-accounting",
        "COUNT(*)/SUM(score) over one row per file version or per tombstone reports 11 rows or a different total; the merged relation is 6 rows summing to 230.",
        "SELECT COUNT(*) AS rows_visible, SUM(score) AS total_score FROM cdc_items",
        &[("rows_visible", Type::INT8), ("total_score", Type::INT8)],
        &rows(&[&[Some("6"), Some("230")]]),
        true,
    )
    .await
}

/// LakeSoul-specific: live rows report their own latest change op, and no
/// `delete` group survives the post-merge tombstone filter.
async fn change_column_groups(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "cdc-change-column-groups",
        "Leaked tombstones add an `op = 'delete'` group; every current live record must report `insert` and there must be exactly 6 of them.",
        "SELECT op, COUNT(*) AS rows_visible FROM cdc_items GROUP BY op ORDER BY op",
        &[("op", Type::TEXT), ("rows_visible", Type::INT8)],
        &rows(&[&[Some("insert"), Some("6")]]),
        true,
    )
    .await
}

/// LakeSoul-specific: a join consumes the merged relation, so a leaked
/// tombstone joins key 2 against `items`, and a duplicated key 1 version
/// doubles the match count against `items`' two key-1 rows.
async fn join_does_not_leak_tombstones(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "cdc-join-no-tombstone-leak",
        "Key 2 appearing as a join group (matches=1) or key 1 matching 4 rows instead of 2 means tombstones or superseded versions reached the join.",
        "SELECT c.id AS cid, COUNT(*) AS matches FROM cdc_items c \
         JOIN items i ON i.id = c.id GROUP BY c.id",
        &[("cid", Type::INT4), ("matches", Type::INT8)],
        &rows(&[&[Some("1"), Some("2")]]),
        false,
    )
    .await
}
