// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The three-marker CDC contract: `insert` / `update` / `delete`.
//!
//! `update` carries the after image of a change and is simply a newer version
//! of its key (the latest version wins); `delete` retracts the key.  The
//! ingestion represents the before image of an update as a `delete` row, so an
//! append-only source can express "replace the old row" too (the signed
//! aggregation subtracts it).  Any other marker value is a live version.

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

/// The signed SUM/COUNT state of an append-only CDC source: `delete` rows
/// subtract their contribution, every other marker adds it.
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
                    sum(case when \"{CHANGE_COLUMN}\" = 'delete' then -amount else amount end) as s, \
                    sum(case when \"{CHANGE_COLUMN}\" = 'delete' then -1 else 1 end) as c \
             from src group by g \
             having sum(case when \"{CHANGE_COLUMN}\" = 'delete' then -1 else 1 end) > 0"
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
async fn append_only_cdc_markers_are_signed() {
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

    // An update: the ingestion writes the before image as `delete` and the
    // after image as `update`, so the signed state moves from 10 to 15 and the
    // distinct value from x to z.
    source
        .append_batch(
            runtime.client(),
            append_batch(&[
                ("a", 10, Some("x"), "delete"),
                ("a", 15, Some("z"), "update"),
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

    // A delete plus another update in one window.
    source
        .append_batch(
            runtime.client(),
            append_batch(&[
                ("b", 20, Some("y"), "delete"),
                ("a", 15, Some("z"), "delete"),
                ("a", 18, Some("w"), "update"),
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
async fn keyed_update_folds_and_delete_retracts() {
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

    // A same-key update is one `update` row: merge-on-read keeps the latest
    // version, so no before image is needed.
    source
        .append_batch(runtime.client(), keyed_batch(&[(1, "a", 15, "update")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (15, 1))])
    );

    // A delete retracts the key ...
    source
        .append_batch(runtime.client(), keyed_batch(&[(1, "a", 15, "delete")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert!(mv_sum_state(&runtime, &mv).await.is_empty());

    // ... and a later update resurrects it as a newer version.
    source
        .append_batch(runtime.client(), keyed_batch(&[(1, "a", 20, "update")]))
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

/// The keyed CDC contract corners: the latest version wins for every marker
/// mix, a key change is a delete of the old key plus an update/insert of the
/// new one, and a value outside the contract is a live version.
#[test_log::test(tokio::test)]
async fn keyed_cdc_contract_corners() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cdc_corner_src_{suffix}"),
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
                format!("ivm_cdc_corner_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &groups, Some("amount")).unwrap(),
            )
            .with_primary_keys(groups.clone()),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("cdc_corner_{suffix}"),
        source.clone(),
        mv.clone(),
        groups,
        Some("amount".to_string()),
    );

    // An insert followed by an update: the newer version wins.
    source
        .append_batch(runtime.client(), keyed_batch(&[(1, "a", 10, "insert")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    source
        .append_batch(runtime.client(), keyed_batch(&[(1, "a", 30, "update")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (30, 1))])
    );
    runtime.rebuild_sum_count(&view).await.unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("a".to_string(), (30, 1))])
    );

    // An update followed by a delete: the delete is the latest version, so
    // the row is retracted, and a rebuild agrees.
    source
        .append_batch(runtime.client(), keyed_batch(&[(1, "a", 30, "delete")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert!(mv_sum_state(&runtime, &mv).await.is_empty());
    runtime.rebuild_sum_count(&view).await.unwrap();
    assert!(mv_sum_state(&runtime, &mv).await.is_empty());

    // A primary key change is a delete of the old key plus an update of the
    // new one; both markers in one window fold to the new row.
    source
        .append_batch(runtime.client(), keyed_batch(&[(2, "b", 40, "insert")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("b".to_string(), (40, 1))])
    );
    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[(2, "b", 40, "delete"), (3, "b", 45, "update")]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("b".to_string(), (45, 1))])
    );

    // Duplicate live versions for one key: merge-on-read keeps the latest
    // (the writer keeps the input order for a key).
    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[(3, "b", 45, "insert"), (3, "b", 50, "update")]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("b".to_string(), (50, 1))])
    );
    runtime.rebuild_sum_count(&view).await.unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("b".to_string(), (50, 1))])
    );

    // Only `insert`, `update` and `delete` are contract values; any other
    // value is a live version (latest wins) — pin it.
    source
        .append_batch(runtime.client(), keyed_batch(&[(3, "b", 50, "upsert")]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("b".to_string(), (50, 1))])
    );
    runtime.rebuild_sum_count(&view).await.unwrap();
    assert_eq!(
        mv_sum_state(&runtime, &mv).await,
        HashMap::from([("b".to_string(), (50, 1))])
    );
}
