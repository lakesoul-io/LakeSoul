// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul storage read contract over real committed files.
//!
//! Every case runs the same SQL text against `postgres-lakesoul` and the
//! PostgreSQL reference, through the simple text and extended binary
//! protocols, and asserts the independently derived rows plus exact column
//! names and OIDs. The reference side is a plain PostgreSQL table holding the
//! final relation; it is an oracle for the *final rows*, not a claim that
//! PostgreSQL has LakeSoul merge semantics.
//!
//! Case groups:
//!
//! * `append-*`: a PK-less append-only table where two independent commits
//!   repeat identical whole rows. Bag multiplicity, grouped counts/sums and
//!   the `''` versus NULL distinction are asserted, so a deduplicating reader
//!   (`UNION DISTINCT`, key-based merge) fails.
//! * `merge-*`: the shared `pk_items` fixture, updated across two commits
//!   (key 1 `val 10 -> 99`, key 3 added). Predicates on superseded/updated
//!   values, key filtering and per-key grouping distinguish latest from
//!   stale versions; the base suite already checks the full projection.
//! * `partial-*`: `partial_items`, whose stored declaration is NOT NULL but
//!   whose second commit omits `w` and writes an explicit NULL `v`, and whose
//!   third commit carries only keys. Expected rows are last-contribution
//!   semantics.
//!
//! Each string passed as `bug` names the consumer-visible failure the case
//! must catch. No case accepts actual output as its expectation and none
//! mutates the fixtures.

use anyhow::Result;
use tokio_postgres::types::Type;

use crate::common::{assert_query, rows};
use crate::support::Harness;

pub async fn run(h: &Harness) -> Result<()> {
    append_duplicates(h).await?;
    merge_across_commits(h).await?;
    partial_updates(h).await
}

/// Two commits, seven physical rows, five distinct rows.
async fn append_duplicates(h: &Harness) -> Result<()> {
    let columns = [
        ("id", Type::INT4),
        ("val", Type::INT4),
        ("label", Type::TEXT),
    ];
    let bag = rows(&[
        &[Some("1"), Some("10"), Some("a")],
        &[Some("1"), Some("10"), Some("a")],
        &[Some("2"), None, Some("")],
        &[Some("2"), None, Some("")],
        &[Some("3"), Some("30"), None],
        &[Some("4"), Some("40"), Some("b")],
        &[Some("5"), None, None],
    ]);

    assert_query(
        h,
        "append-duplicate-rows-preserved",
        "A deduplicating reader (UNION DISTINCT, or a key-based merge applied to a PK-less table) drops the whole rows repeated across the two committed files.",
        "SELECT id, val, label FROM append_items",
        &columns,
        &bag,
        false,
    )
    .await?;

    assert_query(
        h,
        "append-grouped-multiplicity",
        "Grouped counts over a deduplicated union collapse each duplicated key to one row and undercount the file multiplicity.",
        "SELECT id AS key_id, COUNT(*) AS row_count FROM append_items \
         GROUP BY id ORDER BY key_id NULLS LAST",
        &[("key_id", Type::INT4), ("row_count", Type::INT8)],
        &rows(&[
            &[Some("1"), Some("2")],
            &[Some("2"), Some("2")],
            &[Some("3"), Some("1")],
            &[Some("4"), Some("1")],
            &[Some("5"), Some("1")],
        ]),
        true,
    )
    .await?;

    assert_query(
        h,
        "append-aggregate-multiplicity",
        "COUNT(*)/COUNT(col)/SUM over a deduplicated or NULL-conflating union reports 5 rows, a wrong non-NULL label count, or an undercounted sum.",
        "SELECT COUNT(*) AS all_rows, COUNT(val) AS val_present, \
                COUNT(label) AS label_present, SUM(val) AS total, \
                MIN(val) AS lowest, MAX(val) AS highest \
         FROM append_items",
        &[
            ("all_rows", Type::INT8),
            ("val_present", Type::INT8),
            ("label_present", Type::INT8),
            ("total", Type::INT8),
            ("lowest", Type::INT4),
            ("highest", Type::INT4),
        ],
        &rows(&[&[
            Some("7"),
            Some("4"),
            Some("5"),
            Some("90"),
            Some("10"),
            Some("40"),
        ]]),
        true,
    )
    .await?;

    assert_query(
        h,
        "append-empty-label-is-not-null",
        "An empty string materialized as NULL (or NULL normalized to an empty string) selects the wrong keys.",
        "SELECT id AS key_id FROM append_items WHERE label = '' ORDER BY key_id",
        &[("key_id", Type::INT4)],
        &rows(&[&[Some("2")], &[Some("2")]]),
        true,
    )
    .await?;

    assert_query(
        h,
        "append-null-label-filter",
        "A NULL label dropped across the commit boundary or coalesced with an empty string misreports which rows have a NULL label.",
        "SELECT id AS key_id FROM append_items WHERE label IS NULL \
         ORDER BY key_id NULLS LAST",
        &[("key_id", Type::INT4)],
        &rows(&[&[Some("3")], &[Some("5")]]),
        true,
    )
    .await?;

    Ok(())
}

/// The shared `pk_items` fixture: key 1 `val 10 -> 99` and key 3 added in a
/// second commit, so stale and latest versions live in different files.
async fn merge_across_commits(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "merge-superseded-value-filter",
        "A predicate pushed before the merge matches the superseded (1, 10) version and resurrects a row that the latest version excludes.",
        "SELECT id, val FROM pk_items WHERE val = 10",
        &[("id", Type::INT8), ("val", Type::INT8)],
        &rows(&[]),
        false,
    )
    .await?;

    assert_query(
        h,
        "merge-updated-value-filter",
        "Filtering on the updated non-key value must return exactly the latest version of key 1, not an empty set and not a stale row.",
        "SELECT id, val FROM pk_items WHERE val = 99 ORDER BY id",
        &[("id", Type::INT8), ("val", Type::INT8)],
        &rows(&[&[Some("1"), Some("99")]]),
        true,
    )
    .await?;

    assert_query(
        h,
        "merge-key-filter-projection",
        "A key-restricted projection must read the latest version for updated key 1 while still including key 3, which exists only in the second commit.",
        "SELECT id, label FROM pk_items WHERE id IN (1, 3) ORDER BY id",
        &[("id", Type::INT8), ("label", Type::TEXT)],
        &rows(&[
            &[Some("1"), Some("new")],
            &[Some("3"), Some("added")],
        ]),
        true,
    )
    .await?;

    assert_query(
        h,
        "merge-one-latest-row-per-key",
        "Grouping over unmerged versions reports two rows for key 1 (old and new) instead of one latest row per key.",
        "SELECT id, COUNT(*) AS row_count FROM pk_items GROUP BY id ORDER BY id",
        &[("id", Type::INT8), ("row_count", Type::INT8)],
        &rows(&[
            &[Some("1"), Some("1")],
            &[Some("2"), Some("1")],
            &[Some("3"), Some("1")],
        ]),
        true,
    )
    .await?;

    Ok(())
}

/// `partial_items` final relation, derived from three commits:
/// `1=(100,100)`, `2=(NULL,200)`, `3=(30,300)`, `4=(400,NULL)`,
/// `5=(NULL,NULL)`. The stored declaration says NOT NULL on `v` and `w`.
async fn partial_updates(h: &Harness) -> Result<()> {
    assert_query(
        h,
        "partial-full-projection",
        "A reader that returns only the newest partial file, or pads absent columns with zero, loses key 1's v=100/w=100, key 2's w=200, key 3's values, or reports wrong values for the new keys.",
        "SELECT id, v, w FROM partial_items ORDER BY id",
        &[("id", Type::INT4), ("v", Type::INT4), ("w", Type::INT4)],
        &rows(&[
            &[Some("1"), Some("100"), Some("100")],
            &[Some("2"), None, Some("200")],
            &[Some("3"), Some("30"), Some("300")],
            &[Some("4"), Some("400"), None],
            &[Some("5"), None, None],
        ]),
        true,
    )
    .await?;

    assert_query(
        h,
        "partial-null-value-filter",
        "Optimizer trust in the stored NOT NULL declaration (or missing-column padding) removes the row whose value was overwritten with NULL and the new key that never contributed, so IS NULL returns nothing.",
        "SELECT id AS key_id FROM partial_items WHERE v IS NULL ORDER BY key_id",
        &[("key_id", Type::INT4)],
        &rows(&[&[Some("2")], &[Some("5")]]),
        true,
    )
    .await?;

    assert_query(
        h,
        "partial-not-null-value-filter",
        "A merge that drops keys whose latest commit had no value contribution, or that materializes NULL for key 1, hides present values.",
        "SELECT id AS key_id FROM partial_items WHERE v IS NOT NULL \
         ORDER BY key_id",
        &[("key_id", Type::INT4)],
        &rows(&[&[Some("1")], &[Some("3")], &[Some("4")]]),
        true,
    )
    .await?;

    assert_query(
        h,
        "partial-filter-after-merge-updated-value",
        "Filtering the pre-merge first commit on a non-key column resurrects key 1's superseded v=10.",
        "SELECT id AS key_id FROM partial_items WHERE v = 10",
        &[("key_id", Type::INT4)],
        &rows(&[]),
        false,
    )
    .await?;

    assert_query(
        h,
        "partial-filter-on-omitted-column",
        "Pushing a predicate on the omitted column w into only the newest file drops key 2, whose latest commit has no w; scanning only files that match w=200 instead returns the stale version with v=20.",
        "SELECT id, v FROM partial_items WHERE w = 200 ORDER BY id",
        &[("id", Type::INT4), ("v", Type::INT4)],
        &rows(&[&[Some("2"), None]]),
        true,
    )
    .await?;

    assert_query(
        h,
        "partial-aggregate-counts",
        "A plan that reads the stored NOT NULL declaration instead of the merged data reports COUNT(v)=COUNT(w)=5, and a SUM that drops NULL contributions or zero-fills absent columns changes the totals.",
        "SELECT COUNT(*) AS all_rows, COUNT(v) AS v_present, \
                COUNT(w) AS w_present, SUM(v) AS v_total, SUM(w) AS w_total, \
                MIN(v) AS v_lowest, MAX(v) AS v_highest, \
                MIN(w) AS w_lowest, MAX(w) AS w_highest \
         FROM partial_items",
        &[
            ("all_rows", Type::INT8),
            ("v_present", Type::INT8),
            ("w_present", Type::INT8),
            ("v_total", Type::INT8),
            ("w_total", Type::INT8),
            ("v_lowest", Type::INT4),
            ("v_highest", Type::INT4),
            ("w_lowest", Type::INT4),
            ("w_highest", Type::INT4),
        ],
        &rows(&[&[
            Some("5"),
            Some("3"),
            Some("3"),
            Some("530"),
            Some("600"),
            Some("30"),
            Some("400"),
            Some("100"),
            Some("300"),
        ]]),
        true,
    )
    .await?;

    Ok(())
}
