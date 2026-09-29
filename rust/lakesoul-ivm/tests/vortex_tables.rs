// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Vortex internal tables: IVM sources/MVs/state tables can be created with
//! `with_file_format(PhysicalFormat::Vortex)` and refresh, merge-on-read,
//! filtered reads and rebuilds all work (parquet sources feeding vortex MVs
//! included).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::{SessionContext, col, lit};
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IVM_VALUE_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    MinMaxKind, MinMaxView, PhysicalFormat, SumCountView, WindowFunction, WindowView,
    min_max_mv_schema_for, sum_count_mv_schema_for, value_count_state_schema_for,
    window_ranking_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn source_batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// Whether a table directory contains at least one file with `extension`.
fn has_extension(root: &Path, extension: &str) -> bool {
    std::fs::read_dir(root)
        .map(|entries| {
            entries.filter_map(|entry| entry.ok()).any(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|value| value == extension)
            })
        })
        .unwrap_or(false)
}

async fn register_source(runtime: &IvmRuntime, source: &IvmTable) -> SessionContext {
    let context = SessionContext::new();
    let batches = source.read_current(runtime.client()).await.unwrap();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(source.schema.clone(), vec![batches]).unwrap());
    context.register_table("src", table).unwrap();
    context
}

/// `g -> (sum, count)` of the current source state.
async fn sum_oracle(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> HashMap<String, (i64, i64)> {
    let context = register_source(runtime, source).await;
    let frame = context
        .sql(&format!(
            "select g, sum(amount) as s, count(1) as c from src \
             where \"{CHANGE_COLUMN}\" <> 'delete' group by g"
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

async fn min_oracle(runtime: &IvmRuntime, source: &IvmTable) -> HashMap<String, i64> {
    let context = register_source(runtime, source).await;
    let frame = context
        .sql(&format!(
            "select g, min(amount) as m from src \
             where \"{CHANGE_COLUMN}\" <> 'delete' group by g"
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
        let mins = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            state.insert(groups.value(row).to_string(), mins.value(row));
        }
    }
    state
}

async fn sum_state(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<String, (i64, i64)> {
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

async fn min_state(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<String, i64> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(schema.index_of(IVM_VALUE_COLUMN).unwrap())
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
                state.insert(groups.value(row).to_string(), values.value(row));
            }
        }
    }
    state
}

/// `(g, k, row_number)` of the window MV.
async fn window_state(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, i64, i64)> {
    let mut rows = Vec::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let keys = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let numbers = batch
            .column(schema.index_of("row_number").unwrap())
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
                rows.push((
                    groups.value(row).to_string(),
                    keys.value(row),
                    numbers.value(row),
                ));
            }
        }
    }
    rows.sort();
    rows
}

async fn window_oracle(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> Vec<(String, i64, i64)> {
    let context = register_source(runtime, source).await;
    let frame = context
        .sql(&format!(
            "select g, k, cast(row_number() over (partition by g order by amount, k) as bigint) as rn \
             from src where \"{CHANGE_COLUMN}\" <> 'delete'"
        ))
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let schema = batch.schema();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let keys = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let numbers = batch
            .column(schema.index_of("rn").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                groups.value(row).to_string(),
                keys.value(row),
                numbers.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

#[test_log::test(tokio::test)]
async fn vortex_internal_tables_refresh_and_merge() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_vortex_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let groups = vec!["g".to_string()];
    let sum_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_vortex_sum_{suffix}"),
                table_path(&dir, "sum_mv"),
                sum_count_mv_schema_for(&source.schema, &groups, Some("amount")).unwrap(),
            )
            .with_primary_keys(groups.clone())
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    let sum_view = SumCountView::new_with_group_keys(
        format!("vortex_sum_{suffix}"),
        source.clone(),
        sum_mv.clone(),
        groups.clone(),
        Some("amount".to_string()),
    );
    let min_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_vortex_min_{suffix}"),
                table_path(&dir, "min_mv"),
                min_max_mv_schema_for(&source.schema, &groups, "amount", MinMaxKind::Min)
                    .unwrap(),
            )
            .with_primary_keys(groups.clone())
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    let min_state_table = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_vortex_min_state_{suffix}"),
                table_path(&dir, "min_state"),
                value_count_state_schema_for(&source.schema, &groups, "amount").unwrap(),
            )
            .with_primary_keys(vec!["g".to_string(), IVM_VALUE_COLUMN.to_string()])
            .with_bucket_columns(groups.clone())
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    let min_view = MinMaxView::new(
        format!("vortex_min_{suffix}"),
        source.clone(),
        min_mv.clone(),
        min_state_table.clone(),
        "g",
        "amount",
        MinMaxKind::Min,
    );
    let window_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_vortex_window_{suffix}"),
                table_path(&dir, "window_mv"),
                window_ranking_mv_schema_for(
                    &source.schema,
                    &groups,
                    &["k".to_string()],
                    WindowFunction::RowNumber,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string(), "k".to_string()])
            .with_bucket_columns(groups.clone())
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    let window_view = WindowView::new_with_function(
        format!("vortex_window_{suffix}"),
        source.clone(),
        window_mv.clone(),
        groups.clone(),
        vec!["amount".to_string()],
        WindowFunction::RowNumber,
    );

    async fn refresh_all(
        runtime: &IvmRuntime,
        sum_view: &SumCountView,
        min_view: &MinMaxView,
        window_view: &WindowView,
    ) {
        runtime.refresh_sum_count(sum_view).await.unwrap().unwrap();
        runtime.refresh_min_max(min_view).await.unwrap().unwrap();
        runtime.refresh_window(window_view).await.unwrap().unwrap();
    }

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 20, "insert"),
                (3, "b", 30, "insert"),
            ]),
        )
        .await
        .unwrap();
    refresh_all(&runtime, &sum_view, &min_view, &window_view).await;
    assert_eq!(
        sum_state(&runtime, &sum_mv).await,
        sum_oracle(&runtime, &source).await
    );
    assert_eq!(
        min_state(&runtime, &min_mv).await,
        min_oracle(&runtime, &source).await
    );
    // The writer produced vortex files for the MVs and parquet for the source.
    assert!(has_extension(&dir.path().join("sum_mv"), "vortex"));
    assert!(has_extension(&dir.path().join("min_mv"), "vortex"));
    assert!(has_extension(&dir.path().join("min_state"), "vortex"));
    assert!(has_extension(&dir.path().join("window_mv"), "vortex"));
    assert!(has_extension(&dir.path().join("src"), "parquet"));

    assert_eq!(
        window_state(&runtime, &window_mv).await,
        window_oracle(&runtime, &source).await
    );

    // Upsert (retracting the old version through merge-on-read) and delete.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(2, "a", 5, "insert"), (3, "b", 30, "delete")]),
        )
        .await
        .unwrap();
    refresh_all(&runtime, &sum_view, &min_view, &window_view).await;
    assert_eq!(
        sum_state(&runtime, &sum_mv).await,
        sum_oracle(&runtime, &source).await
    );
    assert_eq!(
        min_state(&runtime, &min_mv).await,
        min_oracle(&runtime, &source).await
    );
    assert_eq!(
        window_state(&runtime, &window_mv).await,
        window_oracle(&runtime, &source).await
    );
    assert_eq!(
        sum_state(&runtime, &sum_mv).await,
        HashMap::from([("a".to_string(), (15, 2))])
    );
    assert_eq!(
        min_state(&runtime, &min_mv).await,
        HashMap::from([("a".to_string(), 5)])
    );

    // Filtered reads on the vortex MV and on the (group, value) state table.
    let sum_filtered = sum_mv
        .read_current_filtered(runtime.client(), vec![col("g").eq(lit("a"))])
        .await
        .unwrap();
    assert!(!sum_filtered.is_empty());
    assert!(sum_filtered.iter().all(|batch| batch.num_rows() > 0));
    let state_filtered = min_state_table
        .read_current_filtered(runtime.client(), vec![col("g").eq(lit("a"))])
        .await
        .unwrap();
    assert!(state_filtered.iter().all(|batch| {
        let schema = batch.schema();
        let index = schema.index_of("g").unwrap();
        let groups = batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..groups.len()).all(|row| groups.value(row) == "a")
    }));

    // Rebuilds truncate and refill the vortex tables.
    runtime.rebuild_sum_count(&sum_view).await.unwrap();
    runtime.rebuild_min_max(&min_view).await.unwrap();
    runtime.rebuild_window(&window_view).await.unwrap();
    assert_eq!(
        sum_state(&runtime, &sum_mv).await,
        sum_oracle(&runtime, &source).await
    );
    assert_eq!(
        min_state(&runtime, &min_mv).await,
        min_oracle(&runtime, &source).await
    );
    assert_eq!(
        window_state(&runtime, &window_mv).await,
        window_oracle(&runtime, &source).await
    );
}
