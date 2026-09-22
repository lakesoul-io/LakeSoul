// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Probe: internal LakeSoul tables with NULL merge keys (hash + stable sort +
//! merge-on-read), used to decide the NULL group key strategy.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{IvmRuntime, IvmTableOptions};
use tempfile::tempdir;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Int64, false),
        Field::new("rowKinds", DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(Option<&str>, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from_iter(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

async fn read_rows(
    runtime: &IvmRuntime,
    table: &lakesoul_ivm::IvmTable,
) -> Vec<(Option<String>, i64)> {
    let mut rows = Vec::new();
    for batch in table.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(schema.index_of("v").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                if keys.is_null(row) {
                    None
                } else {
                    Some(keys.value(row).to_string())
                },
                values.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

#[test_log::test(tokio::test)]
async fn internal_table_supports_null_merge_keys() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let table = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_null_probe_{}", uuid::Uuid::new_v4().simple()),
                format!("file://{}", dir.path().join("t").display()),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();

    // Two keys, one of them NULL.
    table
        .append_batch(
            runtime.client(),
            batch(&[(None, 1, "insert"), (Some("a"), 2, "insert")]),
        )
        .await
        .unwrap();
    assert_eq!(
        read_rows(&runtime, &table).await,
        vec![(None, 1), (Some("a".to_string()), 2)]
    );

    // Upsert on the NULL key in a later commit.
    table
        .append_batch(runtime.client(), batch(&[(None, 3, "insert")]))
        .await
        .unwrap();
    assert_eq!(
        read_rows(&runtime, &table).await,
        vec![(None, 3), (Some("a".to_string()), 2)]
    );

    // Delete + re-insert of the NULL key in one batch (stable sort order).
    table
        .append_batch(
            runtime.client(),
            batch(&[(None, 3, "delete"), (None, 4, "insert")]),
        )
        .await
        .unwrap();
    assert_eq!(
        read_rows(&runtime, &table).await,
        vec![(None, 4), (Some("a".to_string()), 2)]
    );

    runtime
        .client()
        .drop_table(&table.table_name, "default")
        .await
        .unwrap();
    drop(dir);
}
