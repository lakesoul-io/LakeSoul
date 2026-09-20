// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for `SUM`/`COUNT` views over sources with a primary key (upserts).
//!
//! A keyed source's changelog only contains the new version of a changed row,
//! so the refresh retracts the row's previous version (read as of the window
//! start) and applies the new one. Covered: value updates, group changes,
//! `rowKinds='delete'` rows, and several updates of the same key inside one
//! window.

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

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn two_column_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn three_column_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn cdc_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
    ]))
}

fn two_column_batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        two_column_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
        ],
    )
    .unwrap()
}

fn three_column_batch(rows: &[(i64, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        three_column_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

fn cdc_batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        cdc_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

async fn mv_state(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<i64, (i64, i64)> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let key_index = schema
            .index_of("k")
            .unwrap_or_else(|_| schema.index_of("g").unwrap());
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

/// `rowKinds` is case sensitive; `col()` would normalize it to lower case.
fn row_kinds_expr() -> datafusion::logical_expr::Expr {
    datafusion::logical_expr::Expr::Column(datafusion::common::Column::from_name(
        IVM_ROW_KINDS_COLUMN,
    ))
}

/// Full aggregation of the current source state, for cross-checking.
async fn full_aggregation(
    runtime: &IvmRuntime,
    source: &IvmTable,
    group_key: &str,
    value_column: &str,
) -> HashMap<i64, (i64, i64)> {
    let batches = source.read_current(runtime.client()).await.unwrap();
    if batches.is_empty() {
        return HashMap::new();
    }
    let context = SessionContext::new();
    let frame = context.read_batches(batches).unwrap();
    let frame = if source.schema.field_with_name(IVM_ROW_KINDS_COLUMN).is_ok() {
        frame
            .filter(row_kinds_expr().not_eq(lit("delete")))
            .unwrap()
    } else {
        frame
    };
    let aggregated = frame
        .aggregate(
            vec![col(group_key)],
            vec![
                sum(col(value_column)).alias("sum_v"),
                count(lit(1_i64)).alias("count_v"),
            ],
        )
        .unwrap();
    let mut expected = HashMap::new();
    for batch in aggregated.collect().await.unwrap() {
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
    expected
}

async fn keyed_fixture(
    group_key: &str,
    value_column: Option<&str>,
    schema: SchemaRef,
) -> (
    IvmRuntime,
    tempfile::TempDir,
    String,
    String,
    IvmTable,
    IvmTable,
    SumCountView,
) {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let view_id = format!("upsert_{suffix}");
    let source_name = format!("ivm_upsert_src_{suffix}");
    let mv_name = format!("ivm_upsert_mv_{suffix}");

    let source = runtime
        .create_table(
            IvmTableOptions::new(source_name.clone(), table_path(&dir, "src"), schema)
                .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                mv_name.clone(),
                table_path(&dir, "mv"),
                sum_count_mv_schema(group_key),
            )
            .with_primary_keys(vec![group_key.to_string()]),
        )
        .await
        .unwrap();
    let view = SumCountView::new(
        view_id,
        source.clone(),
        mv.clone(),
        group_key,
        value_column.map(str::to_string),
    );
    (runtime, dir, source_name, mv_name, source, mv, view)
}

#[test_log::test(tokio::test)]
async fn upsert_updates_retract_the_previous_value() {
    let schema = two_column_schema();
    let (runtime, dir, source_name, mv_name, source, mv, view) =
        keyed_fixture("k", Some("v"), schema).await;

    source
        .append_batch(runtime.client(), two_column_batch(&[(1, 10), (2, 5)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv).await,
        HashMap::from([(1, (10, 1)), (2, (5, 1))])
    );

    source
        .append_batch(runtime.client(), two_column_batch(&[(1, 7)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv).await,
        HashMap::from([(1, (7, 1)), (2, (5, 1))])
    );
    assert_eq!(
        mv_state(&runtime, &mv).await,
        full_aggregation(&runtime, &source, "k", "v").await
    );

    // Several updates of the same key inside one window: only the last one
    // counts, retracted once against the pre-window value.
    source
        .append_batch(runtime.client(), two_column_batch(&[(1, 4)]))
        .await
        .unwrap();
    source
        .append_batch(runtime.client(), two_column_batch(&[(1, 8), (3, 9)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv).await,
        HashMap::from([(1, (8, 1)), (2, (5, 1)), (3, (9, 1))])
    );
    assert_eq!(
        mv_state(&runtime, &mv).await,
        full_aggregation(&runtime, &source, "k", "v").await
    );

    // A rebuild of a keyed source recomputes from the merged source state.
    runtime.rebuild_sum_count(&view).await.unwrap();
    assert_eq!(
        mv_state(&runtime, &mv).await,
        full_aggregation(&runtime, &source, "k", "v").await
    );

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
    drop(dir);
}

#[test_log::test(tokio::test)]
async fn upsert_group_change_moves_the_contribution() {
    let schema = three_column_schema();
    let (runtime, dir, source_name, mv_name, source, mv, view) =
        keyed_fixture("g", Some("v"), schema).await;

    source
        .append_batch(
            runtime.client(),
            three_column_batch(&[(1, 1, 10), (2, 1, 5)]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(mv_state(&runtime, &mv).await, HashMap::from([(1, (15, 2))]));

    // k=1 moves from group 1 to group 2 and changes its value.
    source
        .append_batch(runtime.client(), three_column_batch(&[(1, 2, 7)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv).await,
        HashMap::from([(1, (5, 1)), (2, (7, 1))])
    );
    assert_eq!(
        mv_state(&runtime, &mv).await,
        full_aggregation(&runtime, &source, "g", "v").await
    );

    source
        .append_batch(runtime.client(), three_column_batch(&[(2, 1, 3)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv).await,
        HashMap::from([(1, (3, 1)), (2, (7, 1))])
    );
    assert_eq!(
        mv_state(&runtime, &mv).await,
        full_aggregation(&runtime, &source, "g", "v").await
    );

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
    drop(dir);
}

#[test_log::test(tokio::test)]
async fn upsert_delete_rows_retract_without_inserting() {
    let schema = cdc_schema();
    let (runtime, dir, source_name, mv_name, source, mv, view) =
        keyed_fixture("k", Some("v"), schema).await;

    source
        .append_batch(
            runtime.client(),
            cdc_batch(&[(1, 10, "insert"), (2, 5, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv).await,
        HashMap::from([(1, (10, 1)), (2, (5, 1))])
    );

    // Deleting k=1 removes its contribution and leaves no row behind.
    source
        .append_batch(runtime.client(), cdc_batch(&[(1, 10, "delete")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(mv_state(&runtime, &mv).await, HashMap::from([(2, (5, 1))]));
    assert_eq!(
        mv_state(&runtime, &mv).await,
        full_aggregation(&runtime, &source, "k", "v").await
    );

    // Deleting a key that was never seen is a no-op.
    source
        .append_batch(runtime.client(), cdc_batch(&[(9, 0, "delete")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(mv_state(&runtime, &mv).await, HashMap::from([(2, (5, 1))]));

    // A new key after the delete is inserted normally.
    source
        .append_batch(runtime.client(), cdc_batch(&[(3, 4, "insert")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv).await,
        HashMap::from([(2, (5, 1)), (3, (4, 1))])
    );
    assert_eq!(
        mv_state(&runtime, &mv).await,
        full_aggregation(&runtime, &source, "k", "v").await
    );

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
    drop(dir);
}
