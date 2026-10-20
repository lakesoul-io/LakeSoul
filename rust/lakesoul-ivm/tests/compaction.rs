// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Table compaction: the current logical state is rewritten into a fresh file
//! set and published as one compaction commit.

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{IvmRuntime, IvmTable, IvmTableOptions, PhysicalFormat};
use std::sync::Arc;
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
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

async fn logical_rows(runtime: &IvmRuntime, table: &IvmTable) -> Vec<(i64, i64)> {
    let context = SessionContext::new();
    let provider: Arc<dyn TableProvider> = Arc::new(runtime.table_provider(table));
    context.register_table("t", provider).unwrap();
    let batches = context
        .sql("select k, v from t order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), values.value(row)));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn compaction_preserves_the_logical_state_and_shrinks_files() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let mut options = IvmTableOptions::new(
        format!("ivm_compact_{suffix}"),
        format!("file://{}/table", dir.path().display()),
        schema(),
    )
    .with_primary_keys(vec!["k".to_string()])
    .with_cdc_column(CHANGE_COLUMN)
    .with_file_format(PhysicalFormat::Parquet);
    options.hash_bucket_num = "4".to_string();
    let table = runtime.create_table(options).await.unwrap();
    let client = runtime.client().clone();

    table
        .append_batch(
            &client,
            batch(&[(1, 10, "insert"), (2, 20, "insert"), (3, 30, "insert")]),
        )
        .await
        .unwrap();
    table
        .append_batch(&client, batch(&[(1, 11, "insert"), (2, 0, "delete")]))
        .await
        .unwrap();
    let before = logical_rows(&runtime, &table).await;
    assert_eq!(before, vec![(1, 11), (3, 30)]);
    let files_before = table.current_file_count(&client).await.unwrap();
    assert!(files_before >= 2, "files before: {files_before}");

    table.compact(&client).await.unwrap();

    assert_eq!(logical_rows(&runtime, &table).await, before);
    let files_after = table.current_file_count(&client).await.unwrap();
    assert!(
        files_after <= 4,
        "expected at most one file per bucket after compaction, got {files_after}"
    );

    // The compacted table keeps accepting appends and refreshes.
    table
        .append_batch(&client, batch(&[(3, 31, "insert")]))
        .await
        .unwrap();
    assert_eq!(logical_rows(&runtime, &table).await, vec![(1, 11), (3, 31)]);
}

#[test_log::test(tokio::test)]
async fn compaction_of_an_empty_state_publishes_an_empty_snapshot() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_compact_empty_{suffix}"),
                format!("file://{}/table", dir.path().display()),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let client = runtime.client().clone();
    // No files yet: compaction is a no-op.
    table.compact(&client).await.unwrap();
    assert_eq!(table.current_file_count(&client).await.unwrap(), 0);
}
