// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `update_before` / `update_after` CDC markers: an update is the retraction of
//! the old version plus the insertion of the new one. Append-only sources keep
//! every marker (no merge key), keyed sources fold the pair through merge-on-read.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IVM_VALUE_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    MinMaxKind, MinMaxView, SumCountView, ValueResultKind, sum_count_mv_schema_for,
    value_count_mv_schema_for, value_count_state_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn append_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn append_batch(rows: &[(&str, i64, Option<&str>, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        append_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn keyed_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn keyed_batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        keyed_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// The signed SUM/COUNT state of an append-only CDC source.
async fn signed_state(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> HashMap<String, (i64, i64)> {
    let context = SessionContext::new();
    let batches = source.read_current(runtime.client()).await.unwrap();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(source.schema.clone(), vec![batches]).unwrap());
    context.register_table("src", table).unwrap();
    let frame = context
        .sql(&format!(
            "select g, \
                    sum(case when \"{CHANGE_COLUMN}\" in ('delete','update_before') then -amount else amount end) as s, \
                    sum(case when \"{CHANGE_COLUMN}\" in ('delete','update_before') then -1 else 1 end) as c \
             from src group by g \
             having sum(case when \"{CHANGE_COLUMN}\" in ('delete','update_before') then -1 else 1 end) > 0"
        ))
        .await
        .unwrap();
    let mut state = HashMap::new();
    for batch in frame.collect().await.unwrap() {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
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
            state.insert(
                groups.value(row).to_string(),
                (sums.value(row), counts.value(row)),
            );
        }
    }
    state
}

async fn mv_sum_state(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> HashMap<String, (i64, i64)> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(schema.index_of("sum_v").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(schema.index_of("count_v").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                state.insert(
                    groups.value(row).to_string(),
                    (sums.value(row), counts.value(row)),
                );
            }
        }
    }
    state
}

async fn mv_value_state(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<String, String> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value = batch
            .column(schema.index_of(IVM_VALUE_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                state.insert(groups.value(row).to_string(), value.value(row).to_string());
            }
        }
    }
    state
}

#[test_log::test(tokio::test)]
async fn append_only_update_markers_are_signed() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_upd_src_{suffix}"),
                table_path(&dir, "src"),
                append_schema(),
            )
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let groups = vec!["g".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_upd_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &groups, Some("amount")).unwrap(),
            )
            .with_primary_keys(groups.clone()),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("cdc_upd_{suffix}"),
        source.clone(),
        mv.clone(),
        groups.clone(),
        Some("amount".to_string()),
    );
    let distinct_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_upd_distinct_mv_{suffix}"),
                table_path(&dir, "distinct_mv"),
                value_count_mv_schema_for(
                    &source.schema,
                    &groups,
                    "name",
                    ValueResultKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(groups.clone()),
        )
        .await
        .unwrap();
    let distinct_state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_upd_distinct_state_{suffix}"),
                table_path(&dir, "distinct_state"),
                value_count_state_schema_for(&source.schema, &groups, "name").unwrap(),
            )
            .with_primary_keys(vec!["g".to_string(), "value".to_string()])
            .with_bucket_columns(groups.clone()),
        )
        .await
        .unwrap();
    let distinct_view = MinMaxView::new(
        format!("cdc_upd_distinct_{suffix}"),
        source.clone(),
        distinct_mv.clone(),
        distinct_state,
        "g",
        "name",
        MinMaxKind::Min,
    );

    // Inserts.
    source
        .append_batch(
            runtime.client(),
            append_batch(&[
                ("a", 10, Some("x"), "insert"),
                ("b", 20, Some("y"), "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    runtime
        .refresh_min_max(&distinct_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        signed_state(&runtime, &source).await
    );
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (10, 1)), ("b".to_string(), (20, 1)),])
    );

    // An update pair in one window: -10 + 15 and x -> z.
    source
        .append_batch(
            runtime.client(),
            append_batch(&[
                ("a", 10, Some("x"), "update_before"),
                ("a", 15, Some("z"), "update_after"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    runtime
        .refresh_min_max(&distinct_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        signed_state(&runtime, &source).await
    );
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (15, 1)), ("b".to_string(), (20, 1)),])
    );
    assert_eq!(
        mv_value_state(&runtime, &distinct_mv).await,
        HashMap::from([
            ("a".to_string(), "z".to_string()),
            ("b".to_string(), "y".to_string()),
        ])
    );

    // A delete plus another update pair in one window.
    source
        .append_batch(
            runtime.client(),
            append_batch(&[
                ("b", 20, Some("y"), "delete"),
                ("a", 15, Some("z"), "update_before"),
                ("a", 18, Some("w"), "update_after"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    runtime
        .refresh_min_max(&distinct_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        signed_state(&runtime, &source).await
    );
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (18, 1))])
    );
    assert_eq!(
        mv_value_state(&runtime, &distinct_mv).await,
        HashMap::from([("a".to_string(), "w".to_string())])
    );

    // Rebuilds apply the same signed aggregation.
    runtime.rebuild_sum_count(&view).await.unwrap();
    runtime.rebuild_min_max(&distinct_view).await.unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (18, 1))])
    );
    assert_eq!(
        mv_value_state(&runtime, &distinct_mv).await,
        HashMap::from([("a".to_string(), "w".to_string())])
    );
}

#[test_log::test(tokio::test)]
async fn keyed_update_markers_fold_and_lone_before_retracts() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_keyed_src_{suffix}"),
                table_path(&dir, "src"),
                keyed_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let groups = vec!["g".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_keyed_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &groups, Some("amount")).unwrap(),
            )
            .with_primary_keys(groups.clone()),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("cdc_keyed_{suffix}"),
        source.clone(),
        mv.clone(),
        groups,
        Some("amount".to_string()),
    );

    source
        .append_batch(runtime.client(), keyed_batch(&[(1, "a", 10, "insert")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (10, 1))])
    );

    // Merge-on-read folds the pair to the update_after version.
    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[(1, "a", 10, "update_before"), (1, "a", 15, "update_after")]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (15, 1))])
    );

    // A lone update_before retracts the row until the new version arrives.
    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[(1, "a", 15, "update_before")]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert!(mv_sum_state(&runtime, &mv).await.is_empty());

    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[(1, "a", 20, "update_after")]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (20, 1))])
    );

    runtime.rebuild_sum_count(&view).await.unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (20, 1))])
    );
}
