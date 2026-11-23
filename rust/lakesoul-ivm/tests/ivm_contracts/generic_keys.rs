// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for generic group keys and values: several key columns and non-Int64
//! (strings) are supported by the aggregate views, with the same
//! upsert/delete semantics.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    DistinctAggKind, DistinctAggView, IVM_ROW_KINDS_COLUMN, IVM_VALUE_COLUMN, IvmRuntime,
    IvmTable, IvmTableOptions, MinMaxKind, MinMaxView, SumCountView,
    distinct_agg_mv_schema_for, min_max_mv_schema_for, sum_count_mv_schema_for,
    value_count_state_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn sum_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Int32, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn value_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("tag", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn sum_batch(rows: &[(i64, &str, &str, i32, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        sum_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.3))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.4))),
        ],
    )
    .unwrap()
}

fn value_batch(rows: &[(i64, &str, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        value_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn register(
    context: &SessionContext,
    name: &str,
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) {
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| schema.clone());
    let batches = if batches.is_empty() {
        vec![RecordBatch::new_empty(schema.clone())]
    } else {
        batches
    };
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap());
    context.register_table(name, table).unwrap();
}

/// The MV state as `(region, category) -> (sum, count)`.
async fn sum_state(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> HashMap<(String, String), (i64, i64)> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let region = batch
            .column(schema.index_of("region").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let category = batch
            .column(schema.index_of("category").unwrap())
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
                    (
                        region.value(row).to_string(),
                        category.value(row).to_string(),
                    ),
                    (sums.value(row), counts.value(row)),
                );
            }
        }
    }
    state
}

/// Full `SUM`/`COUNT` of the source state, for cross-checking.
async fn full_sum(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> HashMap<(String, String), (i64, i64)> {
    let context = SessionContext::new();
    register(
        &context,
        "src",
        source.read_current(runtime.client()).await.unwrap(),
        &source.schema,
    );
    let frame = context
        .sql(&format!(
            "select region, category, sum(amount) as s, count(1) as c from src \
             where \"{CHANGE_COLUMN}\" <> 'delete' group by region, category"
        ))
        .await
        .unwrap();
    let mut state = HashMap::new();
    for batch in frame.collect().await.unwrap() {
        let region = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let category = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            state.insert(
                (
                    region.value(row).to_string(),
                    category.value(row).to_string(),
                ),
                (sums.value(row), counts.value(row)),
            );
        }
    }
    state
}

/// The MV state as `tag -> value` (string values).
async fn value_state(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<String, String> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let tags = batch
            .column(schema.index_of("tag").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
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
                state.insert(tags.value(row).to_string(), values.value(row).to_string());
            }
        }
    }
    state
}

#[test_log::test(tokio::test)]
async fn multi_column_string_keys_sum_count() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_generic_sum_src_{suffix}"),
                table_path(&dir, "src"),
                sum_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let source_name = source.table_name.clone();
    let group_keys = vec!["region".to_string(), "category".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_generic_sum_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &group_keys, Some("amount"))
                    .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let mv_name = mv.table_name.clone();
    let view = SumCountView::new_with_group_keys(
        format!("generic_sum_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        Some("amount".to_string()),
    );

    source
        .append_batch(
            runtime.client(),
            sum_batch(&[
                (1, "east", "a", 100, "insert"),
                (2, "east", "b", 50, "insert"),
                (3, "west", "a", 20, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        sum_state(&runtime, &mv).await,
        HashMap::from([
            (("east".to_string(), "a".to_string()), (100, 1)),
            (("east".to_string(), "b".to_string()), (50, 1)),
            (("west".to_string(), "a".to_string()), (20, 1)),
        ])
    );

    // An update retracts the previous amount.
    source
        .append_batch(
            runtime.client(),
            sum_batch(&[(1, "east", "a", 70, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        sum_state(&runtime, &mv).await,
        HashMap::from([
            (("east".to_string(), "a".to_string()), (70, 1)),
            (("east".to_string(), "b".to_string()), (50, 1)),
            (("west".to_string(), "a".to_string()), (20, 1)),
        ])
    );

    // Deleting and adding rows moves the groups.
    source
        .append_batch(
            runtime.client(),
            sum_batch(&[(2, "east", "b", 0, "delete"), (4, "east", "a", 5, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(
        sum_state(&runtime, &mv).await,
        full_sum(&runtime, &source).await
    );
    assert_eq!(
        sum_state(&runtime, &mv).await,
        HashMap::from([
            (("east".to_string(), "a".to_string()), (75, 2)),
            (("west".to_string(), "a".to_string()), (20, 1)),
        ])
    );

    // A rebuild over the full state gives the same result.
    runtime.rebuild_sum_count(&view).await.unwrap();
    assert_eq!(
        sum_state(&runtime, &mv).await,
        full_sum(&runtime, &source).await
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
async fn string_min_max_and_distinct_count() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_generic_value_src_{suffix}"),
                table_path(&dir, "src"),
                value_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let source_name = source.table_name.clone();
    let group_keys = vec!["tag".to_string()];

    let min_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_generic_min_mv_{suffix}"),
                table_path(&dir, "min_mv"),
                min_max_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    "name",
                    MinMaxKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let min_state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_generic_min_state_{suffix}"),
                table_path(&dir, "min_state"),
                value_count_state_schema_for(&source.schema, &group_keys, "name")
                    .unwrap(),
            )
            .with_primary_keys(vec!["tag".to_string(), IVM_VALUE_COLUMN.to_string()])
            .with_bucket_columns(group_keys.clone()),
        )
        .await
        .unwrap();
    let min_view = MinMaxView::new_with_group_keys(
        format!("generic_min_{suffix}"),
        source.clone(),
        min_mv.clone(),
        min_state,
        group_keys.clone(),
        "name",
        MinMaxKind::Min,
    );

    let distinct_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_generic_distinct_mv_{suffix}"),
                table_path(&dir, "distinct_mv"),
                distinct_agg_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    "name",
                    DistinctAggKind::Count,
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let distinct_state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_generic_distinct_state_{suffix}"),
                table_path(&dir, "distinct_state"),
                value_count_state_schema_for(&source.schema, &group_keys, "name")
                    .unwrap(),
            )
            .with_primary_keys(vec!["tag".to_string(), IVM_VALUE_COLUMN.to_string()])
            .with_bucket_columns(group_keys.clone()),
        )
        .await
        .unwrap();
    let distinct_view = DistinctAggView::new_with_group_keys(
        format!("generic_distinct_{suffix}"),
        source.clone(),
        distinct_mv.clone(),
        distinct_state,
        group_keys,
        "name",
        DistinctAggKind::Count,
    );

    source
        .append_batch(
            runtime.client(),
            value_batch(&[
                (1, "t1", "bob", "insert"),
                (2, "t1", "alice", "insert"),
                (3, "t2", "carol", "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
    runtime
        .refresh_distinct_agg(&distinct_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        value_state(&runtime, &min_mv).await,
        HashMap::from([
            ("t1".to_string(), "alice".to_string()),
            ("t2".to_string(), "carol".to_string())
        ])
    );

    // The current minimum is replaced; the distinct count stays at two.
    source
        .append_batch(runtime.client(), value_batch(&[(2, "t1", "zoe", "insert")]))
        .await
        .unwrap();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
    runtime
        .refresh_distinct_agg(&distinct_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        value_state(&runtime, &min_mv).await,
        HashMap::from([
            ("t1".to_string(), "bob".to_string()),
            ("t2".to_string(), "carol".to_string())
        ])
    );

    // Deleting the minimum moves it back to the next value.
    source
        .append_batch(runtime.client(), value_batch(&[(1, "t1", "bob", "delete")]))
        .await
        .unwrap();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
    runtime
        .refresh_distinct_agg(&distinct_view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        value_state(&runtime, &min_mv).await,
        HashMap::from([
            ("t1".to_string(), "zoe".to_string()),
            ("t2".to_string(), "carol".to_string()),
        ])
    );

    runtime
        .client()
        .drop_table(&source_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&min_mv.table_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&distinct_mv.table_name, "default")
        .await
        .unwrap();
    drop(dir);
}
