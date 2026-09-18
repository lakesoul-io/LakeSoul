// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for the IVM runtime.
//!
//! They require a running PostgreSQL configured through `LAKESOUL_PG_*`
//! (the same service `rust-ci` provides) and create their own internal tables
//! under a temporary directory.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::functions_aggregate::{count::count, sum::sum};
use datafusion::prelude::{SessionContext, col, lit};
use lakesoul_ivm::{
    IVM_COUNT_COLUMN, IVM_ROW_KINDS_COLUMN, IVM_SUM_COLUMN, IvmRuntime, IvmTable,
    IvmTableOptions, SumCountView, sum_count_mv_schema,
};
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

fn state_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("row_id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn state_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        state_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

/// Read the current state of a sum/count MV as `group -> (sum, count)`.
async fn mv_state(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<i64, (i64, i64)> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let key_index = schema.index_of("k").unwrap();
        let sum_index = schema.index_of(IVM_SUM_COLUMN).unwrap();
        let count_index = schema.index_of(IVM_COUNT_COLUMN).unwrap();
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap();

        let keys = batch
            .column(key_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(sum_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(count_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                state.insert(keys.value(row), (sums.value(row), counts.value(row)));
            }
        }
    }
    state
}

#[test_log::test(tokio::test)]
async fn sum_count_refresh_matches_full_aggregation() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let view_id = format!("sum_count_{suffix}");
    let source_name = format!("ivm_src_{suffix}");
    let mv_name = format!("ivm_mv_{suffix}");

    let source = runtime
        .create_table(IvmTableOptions::new(
            source_name.clone(),
            table_path(&dir, "src"),
            source_schema(),
        ))
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                mv_name.clone(),
                table_path(&dir, "mv"),
                sum_count_mv_schema("k"),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let view = SumCountView::new(
        view_id.clone(),
        source.clone(),
        mv.clone(),
        "k",
        Some("v".to_string()),
    );

    // Nothing committed yet: the refresh is a no-op and moves no cursor.
    assert_eq!(runtime.refresh_sum_count(&view).await.unwrap(), None);
    assert!(
        runtime
            .metadata()
            .list_cursors(&view_id)
            .await
            .unwrap()
            .is_empty()
    );

    // First window.
    source
        .append_batch(runtime.client(), source_batch(&[(1, 10), (2, 5)]))
        .await
        .unwrap();
    let epoch = runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert!(epoch > 0);
    assert_eq!(
        mv_state(&runtime, &mv).await,
        HashMap::from([(1, (10, 1)), (2, (5, 1))])
    );

    // Second window updates an existing group and adds a new one.
    source
        .append_batch(runtime.client(), source_batch(&[(1, 7), (3, 3)]))
        .await
        .unwrap();
    let second_epoch = runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert!(second_epoch >= epoch);
    assert_eq!(
        mv_state(&runtime, &mv).await,
        HashMap::from([(1, (17, 2)), (2, (5, 1)), (3, (3, 1))])
    );

    // The cursor reached the latest source version (0 then 1).
    let cursors = runtime.metadata().list_cursors(&view_id).await.unwrap();
    assert_eq!(cursors.len(), 1);
    assert_eq!(
        cursors[0].partition_desc,
        lakesoul_io::constant::DEFAULT_PARTITION_DESC
    );
    assert_eq!(cursors[0].last_version, 1);
    assert_eq!(cursors[0].last_timestamp, second_epoch);

    // A refresh without new source commits does not write the MV again.
    let mv_version = runtime
        .client()
        .get_all_partition_info(&mv.table_id)
        .await
        .unwrap()[0]
        .version;
    assert_eq!(runtime.refresh_sum_count(&view).await.unwrap(), None);
    assert_eq!(
        runtime
            .client()
            .get_all_partition_info(&mv.table_id)
            .await
            .unwrap()[0]
            .version,
        mv_version
    );

    // The incrementally maintained state equals a full aggregation over the
    // source table.
    let context = SessionContext::new();
    let frame = context
        .read_batches(source.read_current(runtime.client()).await.unwrap())
        .unwrap()
        .aggregate(
            vec![col("k")],
            vec![
                sum(col("v")).alias(IVM_SUM_COLUMN),
                count(lit(1_i64)).alias(IVM_COUNT_COLUMN),
            ],
        )
        .unwrap();
    let mut expected = HashMap::new();
    for batch in frame.collect().await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            expected.insert(keys.value(row), (sums.value(row), counts.value(row)));
        }
    }
    assert_eq!(mv_state(&runtime, &mv).await, expected);

    runtime
        .client()
        .drop_table(&source_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&mv_name, "default")
        .await
        .unwrap();
}

#[test_log::test(tokio::test)]
async fn state_table_reads_rows_bucketed_by_key_prefix() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let name = format!("ivm_state_{suffix}");

    let state = runtime
        .create_table(
            IvmTableOptions::new(name.clone(), table_path(&dir, "state"), state_schema())
                .with_primary_keys(vec!["k".to_string(), "row_id".to_string()])
                .with_bucket_columns(vec!["k".to_string()]),
        )
        .await
        .unwrap();

    state
        .append_batch(runtime.client(), state_batch(&[(1, 1, 10), (2, 1, 20)]))
        .await
        .unwrap();
    state
        .append_batch(runtime.client(), state_batch(&[(1, 2, 30)]))
        .await
        .unwrap();

    let mut rows = Vec::new();
    for batch in state.read_current(runtime.client()).await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let row_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), row_ids.value(row), values.value(row)));
        }
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 1, 10), (1, 2, 30), (2, 1, 20)]);

    runtime.client().drop_table(&name, "default").await.unwrap();
}
