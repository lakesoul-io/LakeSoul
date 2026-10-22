// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! A bucket-parallel refresh writes its buckets in parallel but publishes them
//! with one commit: workers return the files they wrote and the coordinator
//! commits them (the shape a distributed refresh uses as well).

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{IvmRuntime, IvmTableOptions};
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

fn batch(start: i64, rows: usize) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|offset| start + offset as i64),
            )),
            Arc::new(Int64Array::from_iter_values(
                (0..rows).map(|offset| (start + offset as i64) * 10),
            )),
            Arc::new(StringArray::from_iter_values((0..rows).map(|_| "insert"))),
        ],
    )
    .unwrap()
}

#[test_log::test(tokio::test)]
async fn bucket_writes_publish_with_one_commit() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let mut options = IvmTableOptions::new(
        format!("ivm_concurrent_{suffix}"),
        format!("file://{}/table", dir.path().display()),
        schema(),
    )
    .with_primary_keys(vec!["k".to_string()])
    .with_cdc_column(CHANGE_COLUMN);
    options.hash_bucket_num = "16".to_string();
    let table = runtime.create_table(options).await.unwrap();
    let client = runtime.client().clone();

    // The workers only write files; the coordinator publishes them once.
    let mut tasks = Vec::new();
    for task in 0..8 {
        let table = table.clone();
        tasks.push(tokio::spawn(async move {
            table.write_batch(batch(task * 10, 10)).await
        }));
    }
    let mut files = Vec::new();
    for task in tasks {
        files.extend(task.await.unwrap().unwrap());
    }
    assert!(!files.is_empty());
    let commit_ids = table.commit_files(&client, files).await.unwrap();
    assert_eq!(commit_ids.len(), 1);

    let rows = table
        .read_current(&client)
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(rows, 80);
}
