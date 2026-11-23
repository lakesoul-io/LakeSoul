// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for the configurable CDC change column and the tombstone handling it
//! implies: deleted keys survive merge-on-read as `delete` rows and must be
//! excluded from rebuilds and from the retraction lookup.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, MinMaxKind, MinMaxView,
    SumCountView, min_max_mv_schema, sum_count_mv_schema, value_count_state_schema,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn cdc_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn cdc_batch(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        cdc_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

async fn mv_state(
    runtime: &IvmRuntime,
    mv: &IvmTable,
    group_column: &str,
) -> HashMap<i64, (i64, i64)> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let group_index = schema.index_of(group_column).unwrap();
        let sum_index = schema.index_of("sum_v").unwrap();
        let count_index = schema.index_of("count_v").unwrap();
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap();
        let groups = batch
            .column(group_index)
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
                state.insert(groups.value(row), (sums.value(row), counts.value(row)));
            }
        }
    }
    state
}

/// The current value of a MIN/MAX materialized view as `group -> value`.
async fn mv_value(
    runtime: &IvmRuntime,
    mv: &IvmTable,
    group_column: &str,
) -> HashMap<i64, i64> {
    let mut values = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let group_index = schema.index_of(group_column).unwrap();
        let value_index = schema.index_of("value").unwrap();
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap();
        let groups = batch
            .column(group_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vals = batch
            .column(value_index)
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
                values.insert(groups.value(row), vals.value(row));
            }
        }
    }
    values
}

/// Full `SUM(v)`/`COUNT(*)` of the current source state, filtering `delete`
/// rows through the configured change column.
async fn full_sum_count(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> HashMap<i64, (i64, i64)> {
    let batches = source.read_current(runtime.client()).await.unwrap();
    let context = SessionContext::new();
    let table =
        Arc::new(MemTable::try_new(source.schema.clone(), vec![batches]).unwrap());
    context.register_table("src", table).unwrap();
    let frame = context
        .sql(&format!(
            "select k, sum(v) as s, count(1) as c from src where \"{CHANGE_COLUMN}\" != 'delete' group by k"
        ))
        .await
        .unwrap();
    let mut state = HashMap::new();
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
            state.insert(keys.value(row), (sums.value(row), counts.value(row)));
        }
    }
    state
}

#[test_log::test(tokio::test)]
async fn cdc_column_drives_delete_detection_and_rebuilds() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_src_{suffix}"),
                table_path(&dir, "src"),
                cdc_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let source_name = source.table_name.clone();

    // The column is persisted as the LakeSoul property.
    let table_info = runtime
        .client()
        .get_table_info_by_table_id(&source.table_id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        table_info.properties.contains("lakesoul_cdc_change_column"),
        "properties: {}",
        table_info.properties
    );

    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema("k"),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let mv_name = mv.table_name.clone();
    let view = SumCountView::new(
        format!("cdc_{suffix}"),
        source.clone(),
        mv.clone(),
        "k",
        Some("v".to_string()),
    );

    source
        .append_batch(
            runtime.client(),
            cdc_batch(&[(1, 1, 10, "insert"), (2, 1, 5, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv, "k").await,
        HashMap::from([(1, (10, 1)), (2, (5, 1))])
    );

    source
        .append_batch(runtime.client(), cdc_batch(&[(1, 1, 7, "insert")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv, "k").await,
        HashMap::from([(1, (7, 1)), (2, (5, 1))])
    );

    // The delete goes through the configured column, not `rowKinds`.
    source
        .append_batch(runtime.client(), cdc_batch(&[(2, 1, 5, "delete")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv, "k").await,
        HashMap::from([(1, (7, 1))])
    );

    // A rebuild must not count the surviving delete tombstone.
    runtime.rebuild_sum_count(&view).await.unwrap();
    assert_eq!(
        mv_state(&runtime, &mv, "k").await,
        HashMap::from([(1, (7, 1))])
    );
    assert_eq!(
        mv_state(&runtime, &mv, "k").await,
        full_sum_count(&runtime, &source).await
    );

    // Re-inserting a deleted key must not retract the tombstone's value.
    source
        .append_batch(runtime.client(), cdc_batch(&[(2, 1, 9, "insert")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_state(&runtime, &mv, "k").await,
        HashMap::from([(1, (7, 1)), (2, (9, 1))])
    );
    assert_eq!(
        mv_state(&runtime, &mv, "k").await,
        full_sum_count(&runtime, &source).await
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
async fn min_max_uses_the_cdc_column_too() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_mm_src_{suffix}"),
                table_path(&dir, "src"),
                cdc_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let source_name = source.table_name.clone();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_mm_mv_{suffix}"),
                table_path(&dir, "mv"),
                min_max_mv_schema("k"),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_mm_state_{suffix}"),
                table_path(&dir, "state"),
                value_count_state_schema("k"),
            )
            .with_primary_keys(vec!["k".to_string(), "value".to_string()])
            .with_bucket_columns(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let mv_name = mv.table_name.clone();
    let view = MinMaxView::new(
        format!("cdc_mm_{suffix}"),
        source.clone(),
        mv.clone(),
        state,
        "k",
        "v",
        MinMaxKind::Min,
    );

    source
        .append_batch(
            runtime.client(),
            cdc_batch(&[(1, 1, 10, "insert"), (2, 1, 20, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_value(&runtime, &mv, "k").await,
        HashMap::from([(1, 10), (2, 20)])
    );

    // The minimum disappears through the CDC column.
    source
        .append_batch(runtime.client(), cdc_batch(&[(1, 1, 10, "delete")]))
        .await
        .unwrap();
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    assert_eq!(mv_value(&runtime, &mv, "k").await, HashMap::from([(2, 20)]));

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
async fn cdc_column_must_be_part_of_the_schema() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    let dir = tempdir().unwrap();
    let error = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_bad_{}", uuid::Uuid::new_v4().simple()),
                table_path(&dir, "bad"),
                cdc_schema(),
            )
            .with_cdc_column("missing"),
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("not part of the table schema"),
        "unexpected error: {error}"
    );
    drop(dir);
}
