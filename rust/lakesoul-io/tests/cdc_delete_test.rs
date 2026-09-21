// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! CDC delete tombstones must be dropped after the merge-on-read merge.
//!
//! Flink CDC writes a deleted row as a newer version of the same primary key
//! with `rowKinds = 'delete'`.  The tombstone must participate in the merge
//! (it supersedes older versions) and be filtered out afterwards.  This is
//! also what keeps index candidates for deleted keys correct: the candidate
//! reaches the merge, the tombstone wins, and the post-merge filter drops it.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion_expr::{col, lit};
use lakesoul_io::config::{LakeSoulIOConfig, OPTION_KEY_CDC_COLUMN};
use lakesoul_io::reader::LakeSoulReader;
use lakesoul_io::writer::create_writer_with_io_config;
use tempfile::tempdir;

fn batch(id: [i64; 1], val: [i64; 1], kind: [&str; 1]) -> RecordBatch {
    RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from_iter_values(id)) as ArrayRef),
        (
            "val",
            Arc::new(Int64Array::from_iter_values(val)) as ArrayRef,
        ),
        (
            "rowKinds",
            Arc::new(StringArray::from_iter_values(kind)) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn write(path: String, b: RecordBatch) {
    let conf = LakeSoulIOConfig::builder()
        .with_file(path)
        .with_thread_num(1)
        .with_batch_size(2)
        .with_schema(b.schema())
        .with_primary_keys(vec!["id".to_string()])
        .build();
    let mut w = create_writer_with_io_config(conf).await.unwrap();
    w.write_record_batch(b).await.unwrap();
    w.flush_and_close().await.unwrap();
}

async fn read(
    paths: Vec<String>,
    schema: arrow_schema::SchemaRef,
    filters: Vec<datafusion_expr::Expr>,
) -> Vec<(i64, i64)> {
    let conf = LakeSoulIOConfig::builder()
        .with_files(paths)
        .with_thread_num(1)
        .with_batch_size(2)
        .with_schema(schema)
        .with_primary_keys(vec!["id".to_string()])
        .with_filters(filters)
        .with_option(OPTION_KEY_CDC_COLUMN, "rowKinds")
        .build();
    let mut reader = LakeSoulReader::new(conf).unwrap();
    reader.start().await.unwrap();
    let mut rows = vec![];
    while let Some(record) = reader.next_rb().await {
        let record = record.unwrap();
        let ids = record
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vals = record
            .column_by_name("val")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..record.num_rows() {
            rows.push((ids.value(i), vals.value(i)));
        }
    }
    rows
}

#[tokio::test]
async fn delete_tombstone_is_dropped_after_merge() {
    let dir = tempdir().unwrap();
    let p1 = dir.path().join("a.parquet").to_string_lossy().into_owned();
    let p2 = dir.path().join("b.parquet").to_string_lossy().into_owned();
    let insert = batch([1], [100], ["insert"]);
    write(p1.clone(), insert.clone()).await;
    write(p2.clone(), batch([1], [100], ["delete"])).await;

    let rows = read(vec![p1.clone(), p2.clone()], insert.schema(), vec![]).await;
    assert!(rows.is_empty(), "delete tombstone leaked: {rows:?}");

    // The same holds when the scan is narrowed by a primary-key candidate
    // filter (the shape an index search injects).
    let rows = read(
        vec![p1, p2],
        insert.schema(),
        vec![col("id").in_list(vec![lit(1_i64)], false)],
    )
    .await;
    assert!(
        rows.is_empty(),
        "stale candidate resurrected a delete: {rows:?}"
    );
}

#[tokio::test]
async fn update_row_kind_returns_the_new_version() {
    let dir = tempdir().unwrap();
    let p1 = dir.path().join("a.parquet").to_string_lossy().into_owned();
    let p2 = dir.path().join("b.parquet").to_string_lossy().into_owned();
    let insert = batch([1], [100], ["insert"]);
    write(p1.clone(), insert.clone()).await;
    write(p2.clone(), batch([1], [200], ["update"])).await;

    let rows = read(vec![p1, p2], insert.schema(), vec![]).await;
    assert_eq!(rows, vec![(1, 200)]);
}
