// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Aggregate state reads are pruned to the affected groups. This builds many
//! groups so the reads carry long `IN` filters, then updates/deletes a few of
//! them and cross-checks every group against SQL.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, MinMaxKind, MinMaxView,
    SumCountView, min_max_mv_schema_for, sum_count_mv_schema_for,
    value_count_state_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";
const GROUPS: i64 = 60;

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, String, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.1.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

async fn sql_state(
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
            "select g, sum(v) as s, min(v) as m from src \
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
        let mins = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            state.insert(
                groups.value(row).to_string(),
                (sums.value(row), mins.value(row)),
            );
        }
    }
    state
}

async fn read(
    runtime: &IvmRuntime,
    mv: &IvmTable,
    value_column: &str,
) -> HashMap<String, i64> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let s = batch.schema();
        let groups = batch
            .column(s.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(s.index_of(value_column).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(s.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
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

#[test_log::test(tokio::test)]
async fn many_groups_survive_pruned_state_reads() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_prune_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
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
                format!("ivm_prune_sum_{suffix}"),
                table_path(&dir, "sum_mv"),
                sum_count_mv_schema_for(&source.schema, &groups, Some("v")).unwrap(),
            )
            .with_primary_keys(groups.clone()),
        )
        .await
        .unwrap();
    let sum_view = SumCountView::new_with_group_keys(
        format!("prune_sum_{suffix}"),
        source.clone(),
        sum_mv.clone(),
        groups.clone(),
        Some("v".to_string()),
    );
    let min_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_prune_min_{suffix}"),
                table_path(&dir, "min_mv"),
                min_max_mv_schema_for(&source.schema, &groups, "v", MinMaxKind::Min)
                    .unwrap(),
            )
            .with_primary_keys(groups.clone()),
        )
        .await
        .unwrap();
    let min_state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_prune_min_state_{suffix}"),
                table_path(&dir, "min_state"),
                value_count_state_schema_for(&source.schema, &groups, "v").unwrap(),
            )
            .with_primary_keys(vec!["g".to_string(), "value".to_string()])
            .with_bucket_columns(groups.clone()),
        )
        .await
        .unwrap();
    let min_view = MinMaxView::new(
        format!("prune_min_{suffix}"),
        source.clone(),
        min_mv.clone(),
        min_state,
        "g",
        "v",
        MinMaxKind::Min,
    );

    let initial = (0..GROUPS)
        .map(|index| (index, format!("g{index:02}"), index * 10, "insert"))
        .collect::<Vec<_>>();
    source
        .append_batch(runtime.client(), batch(&initial))
        .await
        .unwrap();
    runtime.refresh_sum_count(&sum_view).await.unwrap().unwrap();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
    let expected = sql_state(&runtime, &source).await;
    assert_eq!(
        read(&runtime, &sum_mv, "sum_v").await.len(),
        GROUPS as usize
    );
    assert_eq!(
        read(&runtime, &sum_mv, "sum_v")
            .await
            .into_iter()
            .map(|(g, value)| (g.clone(), (value, expected[&g].1)))
            .collect::<HashMap<_, _>>(),
        expected
    );

    // Update five rows (one of them moves group) and delete two.
    let updates = vec![
        (3, "g03".to_string(), 3000, "insert"),
        (7, "g07".to_string(), 7000, "insert"),
        (11, "g11".to_string(), 11000, "insert"),
        (1, "g01".to_string(), 1, "insert"),
        (5, "g05".to_string(), 500, "insert"),
        (9, "g09".to_string(), 900, "delete"),
        (13, "g13".to_string(), 1300, "delete"),
    ];
    source
        .append_batch(runtime.client(), batch(&updates))
        .await
        .unwrap();
    runtime.refresh_sum_count(&sum_view).await.unwrap().unwrap();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();

    let expected = sql_state(&runtime, &source).await;
    assert_eq!(
        read(&runtime, &sum_mv, "sum_v").await,
        expected
            .iter()
            .map(|(g, (sum, _))| (g.clone(), *sum))
            .collect::<HashMap<_, _>>()
    );
    assert_eq!(
        read(&runtime, &min_mv, "value").await,
        expected
            .iter()
            .map(|(g, (_, min))| (g.clone(), *min))
            .collect::<HashMap<_, _>>()
    );
    assert!(!read(&runtime, &sum_mv, "sum_v").await.contains_key("g09"));

    // A rebuild is unaffected by the pruning.
    runtime.rebuild_sum_count(&sum_view).await.unwrap();
    runtime.rebuild_min_max(&min_view).await.unwrap();
    assert_eq!(
        read(&runtime, &sum_mv, "sum_v").await,
        expected
            .iter()
            .map(|(g, (sum, _))| (g.clone(), *sum))
            .collect::<HashMap<_, _>>()
    );
}
