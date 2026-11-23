// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for `MIN`/`MAX` views backed by the value-count state table.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::functions_aggregate::min_max::{max, min};
use datafusion::prelude::{SessionContext, col, lit};
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, MinMaxKind, MinMaxView,
    min_max_mv_schema, min_max_state_schema,
};
use tempfile::tempdir;

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn append_only_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn keyed_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(schema: SchemaRef, rows: &[Vec<i64>]) -> RecordBatch {
    let arrays = (0..2)
        .map(|index| {
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|row| row[index]),
            )) as arrow_array::ArrayRef
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(schema, arrays).unwrap()
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

/// `rowKinds` is case sensitive; `col()` would normalize it to lower case.
fn row_kinds_expr() -> datafusion::logical_expr::Expr {
    datafusion::logical_expr::Expr::Column(datafusion::common::Column::from_name(
        IVM_ROW_KINDS_COLUMN,
    ))
}

/// The current min/max of a MIN/MAX view as `group -> value`.
async fn mv_extremes(
    runtime: &IvmRuntime,
    mv: &IvmTable,
    group_key: &str,
) -> HashMap<i64, i64> {
    let mut values = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let group_index = schema.index_of(group_key).unwrap();
        let value_index = schema.index_of(lakesoul_ivm::IVM_VALUE_COLUMN).unwrap();
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

/// Full min/max of the current source state, for cross-checking.
async fn full_extremes(
    runtime: &IvmRuntime,
    source: &IvmTable,
    group_key: &str,
    value_column: &str,
    kind: MinMaxKind,
) -> HashMap<i64, i64> {
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
    let value_expr = match kind {
        MinMaxKind::Min => min(col(value_column)),
        MinMaxKind::Max => max(col(value_column)),
    };
    let aggregated = frame
        .aggregate(vec![col(group_key)], vec![value_expr.alias("extreme")])
        .unwrap();
    let mut values = HashMap::new();
    for batch in aggregated.collect().await.unwrap() {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let extremes = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.insert(groups.value(row), extremes.value(row));
        }
    }
    values
}

async fn create_min_max_view(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    suffix: impl std::fmt::Display,
    source: &IvmTable,
    tag: &str,
    kind: MinMaxKind,
) -> (IvmTable, MinMaxView) {
    let group_key = "g";
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_{tag}_mv_{suffix}"),
                table_path(dir, &format!("{tag}_mv")),
                min_max_mv_schema(group_key),
            )
            .with_primary_keys(vec![group_key.to_string()]),
        )
        .await
        .unwrap();
    let state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_{tag}_state_{suffix}"),
                table_path(dir, &format!("{tag}_state")),
                min_max_state_schema(group_key),
            )
            .with_primary_keys(vec![
                group_key.to_string(),
                lakesoul_ivm::IVM_VALUE_COLUMN.to_string(),
            ])
            .with_bucket_columns(vec![group_key.to_string()]),
        )
        .await
        .unwrap();
    let view = MinMaxView::new(
        format!("{tag}_{suffix}"),
        source.clone(),
        mv.clone(),
        state,
        group_key,
        "v",
        kind,
    );
    (mv, view)
}

#[test_log::test(tokio::test)]
async fn min_and_max_over_append_only_source() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_mm_src_{suffix}"),
            table_path(&dir, "src"),
            append_only_schema(),
        ))
        .await
        .unwrap();
    let source_name = source.table_name.clone();
    let (min_mv, min_view) =
        create_min_max_view(&runtime, &dir, &suffix, &source, "min", MinMaxKind::Min)
            .await;
    let (max_mv, max_view) =
        create_min_max_view(&runtime, &dir, &suffix, &source, "max", MinMaxKind::Max)
            .await;
    let min_mv_name = min_mv.table_name.clone();
    let max_mv_name = max_mv.table_name.clone();

    let rows = |values: &[(i64, i64)]| {
        batch(
            append_only_schema(),
            &values.iter().map(|(g, v)| vec![*g, *v]).collect::<Vec<_>>(),
        )
    };

    source
        .append_batch(runtime.client(), rows(&[(1, 10), (1, 5), (2, 3)]))
        .await
        .unwrap();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
    runtime.refresh_min_max(&max_view).await.unwrap().unwrap();
    assert_eq!(
        mv_extremes(&runtime, &min_mv, "g").await,
        HashMap::from([(1, 5), (2, 3)])
    );
    assert_eq!(
        mv_extremes(&runtime, &max_mv, "g").await,
        HashMap::from([(1, 10), (2, 3)])
    );

    source
        .append_batch(runtime.client(), rows(&[(1, 3), (2, 7)]))
        .await
        .unwrap();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
    runtime.refresh_min_max(&max_view).await.unwrap().unwrap();
    assert_eq!(
        mv_extremes(&runtime, &min_mv, "g").await,
        HashMap::from([(1, 3), (2, 3)])
    );
    assert_eq!(
        mv_extremes(&runtime, &max_mv, "g").await,
        HashMap::from([(1, 10), (2, 7)])
    );

    source
        .append_batch(runtime.client(), rows(&[(1, 20)]))
        .await
        .unwrap();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
    runtime.refresh_min_max(&max_view).await.unwrap().unwrap();
    assert_eq!(
        mv_extremes(&runtime, &min_mv, "g").await,
        full_extremes(&runtime, &source, "g", "v", MinMaxKind::Min).await
    );
    assert_eq!(
        mv_extremes(&runtime, &max_mv, "g").await,
        full_extremes(&runtime, &source, "g", "v", MinMaxKind::Max).await
    );

    runtime
        .client()
        .drop_table(&source_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&min_mv_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&max_mv_name, "default")
        .await
        .unwrap();
    drop(dir);
}

#[test_log::test(tokio::test)]
async fn min_max_over_upserts_and_deletes() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_mm_keyed_src_{suffix}"),
                table_path(&dir, "src"),
                keyed_schema(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let source_name = source.table_name.clone();
    let (mv, view) = create_min_max_view(
        &runtime,
        &dir,
        &suffix,
        &source,
        "keyed_min",
        MinMaxKind::Min,
    )
    .await;
    let mv_name = mv.table_name.clone();

    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[
                (1, 1, 10, "insert"),
                (2, 1, 20, "insert"),
                (3, 2, 5, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_extremes(&runtime, &mv, "g").await,
        HashMap::from([(1, 10), (2, 5)])
    );

    // Updating the current minimum retracts the old value.
    source
        .append_batch(runtime.client(), keyed_batch(&[(1, 1, 25, "insert")]))
        .await
        .unwrap();
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_extremes(&runtime, &mv, "g").await,
        HashMap::from([(1, 20), (2, 5)])
    );

    // Deleting the new minimum moves the group back to the next value.
    source
        .append_batch(runtime.client(), keyed_batch(&[(2, 1, 20, "delete")]))
        .await
        .unwrap();
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_extremes(&runtime, &mv, "g").await,
        HashMap::from([(1, 25), (2, 5)])
    );

    // Removing the last value of a group drops the group's row.
    source
        .append_batch(runtime.client(), keyed_batch(&[(1, 1, 25, "delete")]))
        .await
        .unwrap();
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_extremes(&runtime, &mv, "g").await,
        HashMap::from([(2, 5)])
    );
    assert_eq!(
        mv_extremes(&runtime, &mv, "g").await,
        full_extremes(&runtime, &source, "g", "v", MinMaxKind::Min).await
    );

    // A replayed last window must not double count the state changes: rewind
    // the cursor to the start of that window.
    let view_id = view.view_id.clone();
    let previous_version = runtime
        .metadata()
        .list_cursors(&view_id)
        .await
        .unwrap()
        .first()
        .map(|cursor| cursor.last_version - 1)
        .unwrap();
    runtime
        .metadata()
        .upsert_cursor(
            &view_id,
            &source.table_id,
            lakesoul_io::constant::DEFAULT_PARTITION_DESC,
            previous_version,
            0,
        )
        .await
        .unwrap();
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_extremes(&runtime, &mv, "g").await,
        HashMap::from([(2, 5)])
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
async fn min_max_rebuild_recomputes_state_and_mv() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_mm_rebuild_src_{suffix}"),
                table_path(&dir, "src"),
                keyed_schema(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let source_name = source.table_name.clone();
    let (mv, view) = create_min_max_view(
        &runtime,
        &dir,
        &suffix,
        &source,
        "rebuild_min",
        MinMaxKind::Min,
    )
    .await;
    let mv_name = mv.table_name.clone();

    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[(1, 1, 10, "insert"), (2, 1, 20, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    source
        .append_batch(runtime.client(), keyed_batch(&[(1, 1, 30, "insert")]))
        .await
        .unwrap();

    runtime.rebuild_min_max(&view).await.unwrap();
    assert_eq!(
        mv_extremes(&runtime, &mv, "g").await,
        HashMap::from([(1, 20)])
    );
    assert_eq!(
        mv_extremes(&runtime, &mv, "g").await,
        full_extremes(&runtime, &source, "g", "v", MinMaxKind::Min).await
    );

    // The next refresh only consumes the commits after the rebuild.
    source
        .append_batch(runtime.client(), keyed_batch(&[(3, 1, 5, "insert")]))
        .await
        .unwrap();
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_extremes(&runtime, &mv, "g").await,
        HashMap::from([(1, 5)])
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
