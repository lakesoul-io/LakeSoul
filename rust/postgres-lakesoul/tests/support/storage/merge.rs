// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Physical storage fixtures for append duplication, cross-commit merge
//! ordering and partial updates with absent columns.
//!
//! The inputs below are the only source of truth: the PostgreSQL reference
//! tables are filled with the final relation derived by hand from the LakeSoul
//! merge contract, never from a value read back through the read-only server.
//!
//! * `append_items(id INTEGER, val INTEGER, label TEXT)` is an append-only
//!   table (no primary key). Two independent commits write the *same whole
//!   rows* again across separate Parquet files, and the pair `label = ''`
//!   versus `label IS NULL` lives in the same fixture. A reader that
//!   deduplicates (`UNION DISTINCT`, key-based merge) or normalises an empty
//!   string to NULL changes the visible bag.
//! * `partial_items(id INTEGER NOT NULL, v INTEGER NOT NULL, w INTEGER NOT
//!   NULL, PRIMARY KEY (id))` is declared NOT NULL on purpose. The second
//!   commit only carries `id, v` (so `w` has no contribution), overwrites one
//!   value with an explicit NULL, and inserts a new key with no `w`; the third
//!   commit is key-only, so its rows contribute no values at all. The final
//!   relation is last-contribution semantics, and the stored NOT NULL
//!   declaration must not be mistaken for the actual data.

use std::sync::Arc;

use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use super::super::StartupState;

pub(super) async fn seed(state: &mut StartupState) -> Result<()> {
    append_items(state).await?;
    partial_items(state).await
}

/// Two commits, each with its own Parquet file, both containing the exact
/// rows `(1, 10, 'a')` and `(2, NULL, '')`.
async fn append_items(state: &mut StartupState) -> Result<()> {
    state
        .create_storage_table(
            "append_items",
            "id INTEGER, val INTEGER, label TEXT",
            &[],
            None,
        )
        .await?;

    // Commit 1.
    state
        .write_storage_batch(
            "append_items",
            batch(
                append_fields(),
                vec![
                    int32(&[Some(1), Some(2), Some(3)]),
                    int32(&[Some(10), None, Some(30)]),
                    text(&[Some("a"), Some(""), None]),
                ],
            )?,
        )
        .await?;

    // Commit 2: repeats (1, 10, 'a') and (2, NULL, '') verbatim, adds two rows.
    state
        .write_storage_batch(
            "append_items",
            batch(
                append_fields(),
                vec![
                    int32(&[Some(1), Some(2), Some(4), Some(5)]),
                    int32(&[Some(10), None, Some(40), None]),
                    text(&[Some("a"), Some(""), Some("b"), None]),
                ],
            )?,
        )
        .await?;

    // Every published file lives in the single default partition and both
    // commits must appear in the latest snapshot.
    state.prove_storage_table("append_items", 1, 2).await?;

    state
        .reference_storage_table(
            "append_items",
            "id INTEGER, val INTEGER, label TEXT COLLATE \"C\"",
            "(1, 10, 'a'), (1, 10, 'a'), \
             (2, NULL, ''), (2, NULL, ''), \
             (3, 30, NULL), (4, 40, 'b'), (5, NULL, NULL)",
        )
        .await?;
    Ok(())
}

/// Three commits with three different physical schemas. The final relation is
/// derived from the merge contract: a key that appears without a column
/// contributes nothing for that column, and an explicit NULL value wins.
async fn partial_items(state: &mut StartupState) -> Result<()> {
    state
        .create_storage_table(
            "partial_items",
            "id INTEGER NOT NULL, v INTEGER NOT NULL, w INTEGER NOT NULL, PRIMARY KEY (id)",
            &[],
            None,
        )
        .await?;

    // Commit 1: complete rows.
    state
        .write_storage_batch(
            "partial_items",
            batch(
                partial_fields(true),
                vec![
                    int32(&[Some(1), Some(2), Some(3)]),
                    int32(&[Some(10), Some(20), Some(30)]),
                    int32(&[Some(100), Some(200), Some(300)]),
                ],
            )?,
        )
        .await?;

    // Commit 2: `w` is absent from the physical schema. Key 1 gets a new v and
    // keeps w=100; key 2 gets an explicit NULL v and keeps w=200; new key 4
    // has v=400 and no w contribution at all.
    state
        .write_storage_batch(
            "partial_items",
            batch(
                partial_fields(false),
                vec![
                    int32(&[Some(1), Some(2), Some(4)]),
                    int32(&[Some(100), None, Some(400)]),
                ],
            )?,
        )
        .await?;

    // Commit 3: keys only. Key 1 must keep v=100/w=100, key 5 is new with no
    // value contributions, even though the declaration says NOT NULL.
    state
        .write_storage_batch(
            "partial_items",
            batch(
                vec![Field::new("id", DataType::Int32, false)],
                vec![int32(&[Some(1), Some(5)])],
            )?,
        )
        .await?;

    state.prove_storage_table("partial_items", 1, 3).await?;

    state
        .reference_storage_table(
            "partial_items",
            "id INTEGER NOT NULL, v INTEGER, w INTEGER, PRIMARY KEY (id)",
            "(1, 100, 100), (2, NULL, 200), (3, 30, 300), \
             (4, 400, NULL), (5, NULL, NULL)",
        )
        .await?;
    Ok(())
}

fn append_fields() -> Vec<Field> {
    vec![
        Field::new("id", DataType::Int32, true),
        Field::new("val", DataType::Int32, true),
        Field::new("label", DataType::Utf8, true),
    ]
}

/// `with_w` selects the commit-1 `id, v, w` schema; the partial commits carry
/// only the columns they actually contribute.
fn partial_fields(with_w: bool) -> Vec<Field> {
    let mut fields = vec![
        Field::new("id", DataType::Int32, false),
        Field::new("v", DataType::Int32, true),
    ];
    if with_w {
        fields.push(Field::new("w", DataType::Int32, true));
    }
    fields
}

fn batch(fields: Vec<Field>, columns: Vec<ArrayRef>) -> Result<RecordBatch> {
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn int32(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn text(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}
