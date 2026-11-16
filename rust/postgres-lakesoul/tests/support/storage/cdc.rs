// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! CDC fixture: delete tombstones recorded as newer versions of a key.
//!
//! `cdc_items(id INTEGER NOT NULL, score INTEGER NOT NULL, op TEXT,
//! PRIMARY KEY (id))` is a primary-key table with no range partition, one
//! hash bucket, and the change column `op` (`use_cdc = true`). Three awaited
//! commits write three separate Parquet files, so every key below spans a
//! real file/version conflict:
//!
//! * commit 1 inserts keys 1..=5 with scores 10, 20, 30, 40, 50;
//! * commit 2 updates key 1 to score 3, deletes key 2 with a tombstone whose
//!   score 5 does not satisfy `score >= 10`, deletes key 3 with score 30,
//!   and inserts key 6 with score 60;
//! * commit 3 reinserts key 3 with score 7 and inserts key 7 with score 70.
//!
//! The newest version per key decides visibility, then the merge drops rows
//! whose change column is `delete`. The final relation is therefore derived
//! by hand from these inputs — never read back from LakeSoul — and the
//! PostgreSQL reference table holds exactly that relation with `op = 'insert'`
//! for every current live record:
//!
//! ```text
//! (1, 3, insert) (3, 7, insert) (4, 40, insert)
//! (5, 50, insert) (6, 60, insert) (7, 70, insert)
//! ```
//!
//! Key 2 is gone; key 1 keeps only its updated value; key 3 keeps only its
//! post-delete reinsert value.
//!
//! The tombstone values are chosen so a predicate applied *before* the merge
//! (`score >= 10`) keeps the superseded inserts of keys 1, 2 and 3 whose old
//! values satisfy it while dropping the newest versions whose values do not:
//! that shape resurrects deleted or updated rows, which the query module
//! asserts must never happen.

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use super::super::StartupState;

pub(super) const TABLE: &str = "cdc_items";

pub(super) async fn seed(state: &mut StartupState) -> Result<()> {
    state
        .create_storage_table(
            TABLE,
            "id INTEGER NOT NULL, score INTEGER NOT NULL, op TEXT, PRIMARY KEY (id)",
            &[],
            Some("op"),
        )
        .await?;

    // Commit 1: five live keys.
    state
        .write_storage_batch(
            TABLE,
            batch(&[
                (1, 10, "insert"),
                (2, 20, "insert"),
                (3, 30, "insert"),
                (4, 40, "insert"),
                (5, 50, "insert"),
            ])?,
        )
        .await?;

    // Commit 2: key 1 updated below the query threshold, key 2 deleted with a
    // tombstone below it, key 3 deleted, key 6 inserted.
    state
        .write_storage_batch(
            TABLE,
            batch(&[
                (1, 3, "insert"),
                (2, 5, "delete"),
                (3, 30, "delete"),
                (6, 60, "insert"),
            ])?,
        )
        .await?;

    // Commit 3: key 3 reinserted with a new value, key 7 inserted. The later
    // insert must revive only key 3.
    state
        .write_storage_batch(TABLE, batch(&[(3, 7, "insert"), (7, 70, "insert")])?)
        .await?;

    // Unpartitioned table: one partition must span all three commits, and each
    // commit must be visible as its own active Parquet file.
    state.prove_storage_table(TABLE, 1, 3).await?;

    state
        .reference_storage_table(
            TABLE,
            "id INTEGER NOT NULL, score INTEGER NOT NULL, \
             op TEXT COLLATE \"C\", PRIMARY KEY (id)",
            "(1, 3, 'insert'), (3, 7, 'insert'), (4, 40, 'insert'), \
             (5, 50, 'insert'), (6, 60, 'insert'), (7, 70, 'insert')",
        )
        .await?;
    Ok(())
}

fn batch(rows: &[(i32, i32, &str)]) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
        Field::new("op", DataType::Utf8, true),
    ]));
    let ids = rows.iter().map(|row| row.0).collect::<Vec<_>>();
    let scores = rows.iter().map(|row| row.1).collect::<Vec<_>>();
    let ops = rows.iter().map(|row| row.2).collect::<Vec<_>>();
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)) as ArrayRef,
            Arc::new(Int32Array::from(scores)) as ArrayRef,
            Arc::new(StringArray::from(ops)) as ArrayRef,
        ],
    )?)
}
