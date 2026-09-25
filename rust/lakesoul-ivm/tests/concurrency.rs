// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Concurrent refreshes. The docker-compose test database runs PostgreSQL at
//! `serializable`, so parallel writers abort each other's statements with
//! `40001`; the metadata layer retries those with backoff, and every view here
//! must still converge to its SQL aggregation.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{IvmRuntime, SumCountView, sum_count_mv_schema_for};
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

fn source_batch(rows: &[(i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|_| "insert"))),
        ],
    )
    .unwrap()
}

async fn mv_state(
    runtime: &IvmRuntime,
    mv: &lakesoul_ivm::IvmTable,
) -> HashMap<String, (i64, i64)> {
    let context = SessionContext::new();
    let batches = mv.read_current(runtime.client()).await.unwrap();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(mv.schema.clone(), vec![batches]).unwrap());
    context.register_table("mv", table).unwrap();
    let frame = context
        .sql("select g, sum_v, count_v from mv where \"rowKinds\" = 'insert'")
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

async fn sql_state(
    runtime: &IvmRuntime,
    mv: &lakesoul_ivm::IvmTable,
    source: &lakesoul_ivm::IvmTable,
) -> HashMap<String, (i64, i64)> {
    let context = SessionContext::new();
    let batches = source.read_current(runtime.client()).await.unwrap();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(source.schema.clone(), vec![batches]).unwrap());
    context.register_table("src", table).unwrap();
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
    let _ = mv;
    state
}

async fn run_view(
    index: usize,
) -> (HashMap<String, (i64, i64)>, HashMap<String, (i64, i64)>) {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            lakesoul_ivm::IvmTableOptions::new(
                format!("ivm_concurrent_src_{index}_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let group_keys = vec!["g".to_string()];
    let mv = runtime
        .create_table(
            lakesoul_ivm::IvmTableOptions::new(
                format!("ivm_concurrent_mv_{index}_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &group_keys, Some("amount"))
                    .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("concurrent_{index}_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        Some("amount".to_string()),
    );

    for round in 0..3i64 {
        let base = index as i64 * 100 + round * 10;
        source
            .append_batch(
                runtime.client(),
                source_batch(&[
                    (base, "a", base + 1),
                    (base + 1, "a", base + 2),
                    (base + 2, "b", base + 3),
                ]),
            )
            .await
            .unwrap();
        runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    }
    (
        mv_state(&runtime, &mv).await,
        sql_state(&runtime, &mv, &source).await,
    )
}

#[test_log::test(tokio::test)]
async fn concurrent_refreshes_converge() {
    // The futures are not `Send` (the reader holds boxed streams), so drive
    // them on a local set: all eight database flows are in flight at once,
    // which is what triggers the serialization conflicts.
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut tasks = Vec::new();
            for index in 0..8 {
                tasks.push(tokio::task::spawn_local(run_view(index)));
            }
            for task in tasks {
                let (mv, sql) = task.await.unwrap();
                assert_eq!(mv, sql);
            }
        })
        .await;
}
