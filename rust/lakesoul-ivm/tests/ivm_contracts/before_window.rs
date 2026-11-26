// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The before image of a refresh window is pinned by explicit partition
//! versions, one per partition with a cursor. A partition the window did not
//! touch is still pinned to the version the view consumed, not to its current
//! version and not to a version resolved by a (non-reproducible) timestamp.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{IvmRuntime, IvmTableOptions};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("day", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(
                (0..rows.len()).map(|_| "insert"),
            )),
        ],
    )
    .unwrap()
}

#[test_log::test(tokio::test)]
async fn before_window_pins_every_partition_to_its_recorded_version() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let client = runtime.client().clone();
    let table = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_before_window_{suffix}"),
                format!("file://{}/table", dir.path().display()),
                schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();

    table
        .append_batch(&client, batch(&[(1, "d1", 10), (2, "d2", 20)]))
        .await
        .unwrap();
    let first = client
        .get_all_partition_info(&table.table_id)
        .await
        .unwrap();
    let d1 = first
        .iter()
        .find(|partition| partition.partition_desc == "day=d1")
        .expect("d1 partition")
        .clone();
    let d2 = first
        .iter()
        .find(|partition| partition.partition_desc == "day=d2")
        .expect("d2 partition")
        .clone();
    let boundary = d1.timestamp.max(d2.timestamp);

    // Land a commit on a partition the window does NOT touch after the
    // boundary. Its timestamp can even be stamped earlier than the boundary
    // (partition_info.timestamp is the transaction start, not commit time);
    // the recorded version must still win.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    table
        .append_batch(&client, batch(&[(3, "d2", 30)]))
        .await
        .unwrap();
    let current = client
        .get_all_partition_info(&table.table_id)
        .await
        .unwrap();
    let d2_now = current
        .iter()
        .find(|partition| partition.partition_desc == "day=d2")
        .expect("d2 partition");
    assert!(
        d2_now.timestamp > boundary,
        "the second commit must be after the boundary"
    );

    // The window touched d1 only, but d2 has a cursor, so both partitions are
    // pinned to the versions the view consumed. Row 3 (committed later) is not
    // part of the before image regardless of its timestamp.
    let before = HashMap::from([
        ("day=d1".to_string(), Some(i64::from(d1.version))),
        ("day=d2".to_string(), Some(i64::from(d2.version))),
    ]);
    let batches = table.read_before_window(&client, &before).await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(schema.index_of("v").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((ids.value(row), values.value(row)));
        }
    }
    rows.sort_unstable();
    assert_eq!(
        rows,
        vec![(1, 10), (2, 20)],
        "an untouched partition must be read at its recorded version, not at its current version"
    );
}
