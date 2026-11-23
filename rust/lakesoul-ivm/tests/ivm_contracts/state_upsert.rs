// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A state table is an ordinary keyed LakeSoul table.  A later insert for the
//! same primary key is a logical upsert through merge-on-read; a retraction is
//! an explicit delete marker.  These tests pin that contract before the join
//! refresh relies on it.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTableOptions};
use tempfile::tempdir;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

async fn current(
    runtime: &IvmRuntime,
    table: &lakesoul_ivm::IvmTable,
) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in table.read_current(runtime.client()).await.unwrap() {
        let ids = batch
            .column(batch.schema().index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(batch.schema().index_of("v").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(batch.schema().index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                rows.push((ids.value(row), values.value(row)));
            }
        }
    }
    rows.sort_unstable();
    rows
}

#[test_log::test(tokio::test)]
async fn same_key_appends_are_upserts_and_delete_is_explicit() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let table = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_state_upsert_{suffix}"),
                format!("file://{}", dir.path().join("state").display()),
                schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_bucket_columns(vec!["id".to_string()])
            .with_cdc_column(IVM_ROW_KINDS_COLUMN),
        )
        .await
        .unwrap();

    // A later insert on the same key supersedes the old row without a delete.
    table
        .append_batch(runtime.client(), batch(&[(1, 10, "insert")]))
        .await
        .unwrap();
    table
        .append_batch(runtime.client(), batch(&[(1, 20, "insert")]))
        .await
        .unwrap();
    assert_eq!(current(&runtime, &table).await, vec![(1, 20)]);

    // A delete retracts the key; an insert after the delete resurrects it.
    table
        .append_batch(runtime.client(), batch(&[(1, 20, "delete")]))
        .await
        .unwrap();
    assert!(current(&runtime, &table).await.is_empty());
    table
        .append_batch(runtime.client(), batch(&[(1, 30, "insert")]))
        .await
        .unwrap();
    assert_eq!(current(&runtime, &table).await, vec![(1, 30)]);

    // Delete before insert in one commit resolves to the new value.
    table
        .append_batch(
            runtime.client(),
            batch(&[(1, 30, "delete"), (1, 40, "insert")]),
        )
        .await
        .unwrap();
    assert_eq!(current(&runtime, &table).await, vec![(1, 40)]);
}

#[test_log::test(tokio::test)]
async fn state_delete_and_insert_can_share_one_append() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let table = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_state_replace_{suffix}"),
                format!("file://{}", dir.path().join("state").display()),
                schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_bucket_columns(vec!["id".to_string()])
            .with_cdc_column(IVM_ROW_KINDS_COLUMN),
        )
        .await
        .unwrap();

    table
        .append_batches(
            runtime.client(),
            vec![batch(&[(1, 1, "insert"), (2, 2, "insert")])],
        )
        .await
        .unwrap();
    // One state append for the replacement: the old key is retracted and the
    // new key is asserted in the same commit.
    table
        .append_batches(
            runtime.client(),
            vec![batch(&[(1, 1, "delete"), (1, 9, "insert")])],
        )
        .await
        .unwrap();
    assert_eq!(current(&runtime, &table).await, vec![(1, 9), (2, 2)]);
}
