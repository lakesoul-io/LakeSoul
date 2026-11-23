// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for `COUNT(DISTINCT)`/`SUM(DISTINCT)` views backed by the shared
//! value-count state table.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    DistinctAggKind, DistinctAggView, IVM_ROW_KINDS_COLUMN, IVM_VALUE_COLUMN, IvmRuntime,
    IvmTable, IvmTableOptions, distinct_agg_mv_schema, value_count_state_schema,
};
use tempfile::tempdir;

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn keyed_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
    ]))
}

fn keyed_batch(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        keyed_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// The current value of a distinct aggregate view as `group -> value`.
async fn mv_values(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<i64, i64> {
    let mut values = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let group_index = schema.index_of("g").unwrap();
        let value_index = schema.index_of(IVM_VALUE_COLUMN).unwrap();
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

/// Full `COUNT(DISTINCT)`/`SUM(DISTINCT)` of the current source state.
async fn full_distinct(
    runtime: &IvmRuntime,
    source: &IvmTable,
    kind: DistinctAggKind,
) -> HashMap<i64, i64> {
    let batches = source.read_current(runtime.client()).await.unwrap();
    let context = SessionContext::new();
    let table =
        Arc::new(MemTable::try_new(source.schema.clone(), vec![batches]).unwrap());
    context.register_table("src", table).unwrap();
    let aggregate = match kind {
        DistinctAggKind::Count => "count(distinct v)",
        DistinctAggKind::Sum => "sum(distinct v)",
    };
    let frame = context
        .sql(&format!(
            "select g, {aggregate} as x from src where \"rowKinds\" != 'delete' group by g"
        ))
        .await
        .unwrap();
    let mut values = HashMap::new();
    for batch in frame.collect().await.unwrap() {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vals = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.insert(groups.value(row), vals.value(row));
        }
    }
    values
}

async fn create_view(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    suffix: impl std::fmt::Display,
    source: &IvmTable,
    tag: &str,
    kind: DistinctAggKind,
) -> (IvmTable, DistinctAggView) {
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_{tag}_mv_{suffix}"),
                table_path(dir, &format!("{tag}_mv")),
                distinct_agg_mv_schema("g"),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_{tag}_state_{suffix}"),
                table_path(dir, &format!("{tag}_state")),
                value_count_state_schema("g"),
            )
            .with_primary_keys(vec!["g".to_string(), IVM_VALUE_COLUMN.to_string()])
            .with_bucket_columns(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let view = DistinctAggView::new(
        format!("{tag}_{suffix}"),
        source.clone(),
        mv.clone(),
        state,
        "g",
        "v",
        kind,
    );
    (mv, view)
}

#[test_log::test(tokio::test)]
async fn distinct_count_and_sum_over_upserts_and_deletes() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_distinct_src_{suffix}"),
                table_path(&dir, "src"),
                keyed_schema(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let source_name = source.table_name.clone();
    let (count_mv, count_view) = create_view(
        &runtime,
        &dir,
        &suffix,
        &source,
        "distinct_count",
        DistinctAggKind::Count,
    )
    .await;
    let (sum_mv, sum_view) = create_view(
        &runtime,
        &dir,
        &suffix,
        &source,
        "distinct_sum",
        DistinctAggKind::Sum,
    )
    .await;
    let count_mv_name = count_mv.table_name.clone();
    let sum_mv_name = sum_mv.table_name.clone();

    // Two values in group 1, one in group 2.
    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[
                (1, 1, 5, "insert"),
                (2, 1, 5, "insert"),
                (3, 1, 7, "insert"),
                (4, 2, 9, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime
        .refresh_distinct_agg(&count_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&sum_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mv_values(&runtime, &count_mv).await,
        HashMap::from([(1, 2), (2, 1)])
    );
    assert_eq!(
        mv_values(&runtime, &sum_mv).await,
        HashMap::from([(1, 12), (2, 9)])
    );

    // An update that removes a distinct value.
    source
        .append_batch(runtime.client(), keyed_batch(&[(3, 1, 5, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_distinct_agg(&count_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&sum_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mv_values(&runtime, &count_mv).await,
        HashMap::from([(1, 1), (2, 1)])
    );
    assert_eq!(
        mv_values(&runtime, &sum_mv).await,
        HashMap::from([(1, 5), (2, 9)])
    );
    assert_eq!(
        mv_values(&runtime, &count_mv).await,
        full_distinct(&runtime, &source, DistinctAggKind::Count).await
    );
    assert_eq!(
        mv_values(&runtime, &sum_mv).await,
        full_distinct(&runtime, &source, DistinctAggKind::Sum).await
    );

    // Deletes retract values; the last delete drops the group.
    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[(1, 1, 5, "delete"), (2, 1, 5, "delete")]),
        )
        .await
        .unwrap();
    runtime
        .refresh_distinct_agg(&count_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&sum_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mv_values(&runtime, &count_mv).await,
        HashMap::from([(1, 1), (2, 1)])
    );

    source
        .append_batch(runtime.client(), keyed_batch(&[(3, 1, 5, "delete")]))
        .await
        .unwrap();
    runtime
        .refresh_distinct_agg(&count_view)
        .await
        .unwrap()
        .unwrap();
    runtime
        .refresh_distinct_agg(&sum_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mv_values(&runtime, &count_mv).await,
        HashMap::from([(2, 1)])
    );
    assert_eq!(mv_values(&runtime, &sum_mv).await, HashMap::from([(2, 9)]));
    assert_eq!(
        mv_values(&runtime, &count_mv).await,
        full_distinct(&runtime, &source, DistinctAggKind::Count).await
    );

    runtime
        .client()
        .drop_table(&source_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&count_mv_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&sum_mv_name, "default")
        .await
        .unwrap();
    drop(dir);
}

#[test_log::test(tokio::test)]
async fn distinct_agg_replay_and_rebuild() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_distinct_rebuild_src_{suffix}"),
                table_path(&dir, "src"),
                keyed_schema(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let source_name = source.table_name.clone();
    let (mv, view) = create_view(
        &runtime,
        &dir,
        &suffix,
        &source,
        "distinct_rebuild",
        DistinctAggKind::Count,
    )
    .await;
    let mv_name = mv.table_name.clone();

    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[(1, 1, 5, "insert"), (2, 1, 7, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_distinct_agg(&view).await.unwrap().unwrap();
    source
        .append_batch(runtime.client(), keyed_batch(&[(2, 1, 9, "insert")]))
        .await
        .unwrap();
    runtime.refresh_distinct_agg(&view).await.unwrap().unwrap();
    assert_eq!(mv_values(&runtime, &mv).await, HashMap::from([(1, 2)]));

    // Replaying the last window must not double count the state changes.
    let previous_version = runtime
        .metadata()
        .list_cursors(&view.view_id)
        .await
        .unwrap()
        .first()
        .map(|cursor| cursor.last_version - 1)
        .unwrap();
    runtime
        .metadata()
        .upsert_cursor(
            &view.view_id,
            &source.table_id,
            lakesoul_io::constant::DEFAULT_PARTITION_DESC,
            previous_version,
            0,
        )
        .await
        .unwrap();
    runtime.refresh_distinct_agg(&view).await.unwrap().unwrap();
    assert_eq!(mv_values(&runtime, &mv).await, HashMap::from([(1, 2)]));

    // A rebuild recomputes the state and the MV from the full source state.
    runtime.rebuild_distinct_agg(&view).await.unwrap();
    assert_eq!(
        mv_values(&runtime, &mv).await,
        full_distinct(&runtime, &source, DistinctAggKind::Count).await
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
