// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the incremental join refresh.
//!
//! They require a running PostgreSQL configured through `LAKESOUL_PG_*`
//! (the same service `rust-ci` provides) and create their own internal tables
//! under a temporary directory.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::{JoinType, SessionContext, col};
use lakesoul_ivm::{IvmRuntime, IvmTable, IvmTableOptions, JoinView, join_view_schema};
use tempfile::tempdir;

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn source_batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
        ],
    )
    .unwrap()
}

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

/// Read all rows of the append-only join output as `(key, left, right)`.
async fn join_rows(runtime: &IvmRuntime, output: &IvmTable) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in output.read_current(runtime.client()).await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), left.value(row), right.value(row)));
        }
    }
    rows.sort_unstable();
    rows
}

/// Full join of the current left and right states.
async fn full_join(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
) -> Vec<(i64, i64, i64)> {
    let context = SessionContext::new();
    let left_frame = context
        .read_batches(left.read_current(runtime.client()).await.unwrap())
        .unwrap()
        .select(vec![
            col("k").alias("join_key"),
            col("v").alias("left_value"),
        ])
        .unwrap();
    let right_frame = context
        .read_batches(right.read_current(runtime.client()).await.unwrap())
        .unwrap()
        .select(vec![
            col("k").alias("right_key"),
            col("v").alias("right_value"),
        ])
        .unwrap();
    let joined = left_frame
        .join(
            right_frame,
            JoinType::Inner,
            &["join_key"],
            &["right_key"],
            None,
        )
        .unwrap()
        .select(vec![col("join_key"), col("left_value"), col("right_value")])
        .unwrap();

    let mut rows = Vec::new();
    for batch in joined.collect().await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), left.value(row), right.value(row)));
        }
    }
    rows.sort_unstable();
    rows
}

#[test_log::test(tokio::test)]
async fn join_refresh_matches_full_join_with_two_sided_windows() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let view_id = format!("join_{suffix}");
    let left_name = format!("ivm_left_{suffix}");
    let right_name = format!("ivm_right_{suffix}");
    let output_name = format!("ivm_join_{suffix}");

    let left = runtime
        .create_table(IvmTableOptions::new(
            left_name.clone(),
            table_path(&dir, "left"),
            source_schema(),
        ))
        .await
        .unwrap();
    let right = runtime
        .create_table(IvmTableOptions::new(
            right_name.clone(),
            table_path(&dir, "right"),
            source_schema(),
        ))
        .await
        .unwrap();
    let output = runtime
        .create_table(IvmTableOptions::new(
            output_name.clone(),
            table_path(&dir, "join"),
            join_view_schema(),
        ))
        .await
        .unwrap();
    let view = JoinView::new(
        view_id.clone(),
        left.clone(),
        right.clone(),
        output.clone(),
        "k",
        "v",
        "v",
    );

    // Empty sources: no-op.
    assert_eq!(runtime.refresh_join(&view).await.unwrap(), None);

    // Window 1: only k=2 matches.
    left.append_batch(runtime.client(), source_batch(&[(1, 10), (2, 20)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), source_batch(&[(2, 200), (3, 300)]))
        .await
        .unwrap();
    assert!(runtime.refresh_join(&view).await.unwrap().is_some());
    assert_eq!(join_rows(&runtime, &output).await, vec![(2, 20, 200)]);

    // Window 2: both sides get new rows, so all three inclusion-exclusion
    // terms contribute.
    left.append_batch(runtime.client(), source_batch(&[(2, 21)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), source_batch(&[(1, 100), (2, 201)]))
        .await
        .unwrap();
    assert!(runtime.refresh_join(&view).await.unwrap().is_some());

    let expected = vec![
        (1, 10, 100),
        (2, 20, 200),
        (2, 20, 201),
        (2, 21, 200),
        (2, 21, 201),
    ];
    assert_eq!(join_rows(&runtime, &output).await, expected);
    assert_eq!(full_join(&runtime, &left, &right).await, expected);

    // No new source commits: no-op and no new output rows.
    assert_eq!(runtime.refresh_join(&view).await.unwrap(), None);
    assert_eq!(join_rows(&runtime, &output).await, expected);

    // One cursor per source.
    let mut cursors = runtime.metadata().list_cursors(&view_id).await.unwrap();
    cursors.sort_by(|left, right| left.source_table_id.cmp(&right.source_table_id));
    assert_eq!(cursors.len(), 2);
    assert!(cursors.iter().all(|cursor| cursor.last_version == 1));

    runtime
        .client()
        .drop_table(&left_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&right_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&output_name, "default")
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn join_refresh_is_idempotent_when_cursors_are_replayed() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let view_id = format!("join_idem_{suffix}");
    let left_name = format!("ivm_left_idem_{suffix}");
    let right_name = format!("ivm_right_idem_{suffix}");
    let output_name = format!("ivm_join_idem_{suffix}");

    let left = runtime
        .create_table(IvmTableOptions::new(
            left_name.clone(),
            table_path(&dir, "left"),
            source_schema(),
        ))
        .await
        .unwrap();
    let right = runtime
        .create_table(IvmTableOptions::new(
            right_name.clone(),
            table_path(&dir, "right"),
            source_schema(),
        ))
        .await
        .unwrap();
    let output = runtime
        .create_table(IvmTableOptions::new(
            output_name.clone(),
            table_path(&dir, "join"),
            join_view_schema(),
        ))
        .await
        .unwrap();
    let view = JoinView::new(
        view_id.clone(),
        left.clone(),
        right.clone(),
        output.clone(),
        "k",
        "v",
        "v",
    );

    left.append_batch(runtime.client(), source_batch(&[(1, 10), (2, 20)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), source_batch(&[(2, 200)]))
        .await
        .unwrap();
    let first = runtime.refresh_join(&view).await.unwrap().unwrap();
    let rows = join_rows(&runtime, &output).await;
    assert_eq!(rows, vec![(2, 20, 200)]);

    // Simulate a crash after the output append but before the cursors moved.
    for table in [&left, &right] {
        runtime
            .metadata()
            .upsert_cursor(
                &view_id,
                &table.table_id,
                lakesoul_io::constant::DEFAULT_PARTITION_DESC,
                -1,
                0,
            )
            .await
            .unwrap();
    }

    let replayed = runtime.refresh_join(&view).await.unwrap().unwrap();
    assert_eq!(replayed, first);
    // the output carries the window epoch, so the replay appends nothing
    assert_eq!(join_rows(&runtime, &output).await, rows);

    let cursors = runtime.metadata().list_cursors(&view_id).await.unwrap();
    assert_eq!(cursors.len(), 2);
    assert!(cursors.iter().all(|cursor| cursor.last_version == 0));

    runtime
        .client()
        .drop_table(&left_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&right_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&output_name, "default")
        .await
        .unwrap();
}
