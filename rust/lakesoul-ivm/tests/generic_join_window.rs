// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Generic type coverage for the join and window views: string join keys and
//! payloads, multi-column join keys, and string window partition keys.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IVM_ROW_NUMBER_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    JoinView, WindowView, join_view_schema_for, window_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
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

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("sub", DataType::Utf8, false),
        Field::new("lv", DataType::Utf8, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, false),
        Field::new("sub", DataType::Utf8, false),
        Field::new("rv", DataType::Int32, false),
    ]))
}

fn left_batch(rows: &[(&str, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        left_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

fn right_batch(rows: &[(&str, &str, i32)]) -> RecordBatch {
    RecordBatch::try_new(
        right_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int32Array::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

async fn join_rows(
    runtime: &IvmRuntime,
    output: &IvmTable,
) -> Vec<(String, String, String, i32)> {
    let mut rows = Vec::new();
    for batch in output.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let subs = batch
            .column(schema.index_of("sub").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let left = batch
            .column(schema.index_of("left_value").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right = batch
            .column(schema.index_of("right_value").unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row).to_string(),
                subs.value(row).to_string(),
                left.value(row).to_string(),
                right.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

async fn full_join(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
) -> Vec<(String, String, String, i32)> {
    let context = SessionContext::new();
    register(
        &context,
        "l",
        left.read_current(runtime.client()).await.unwrap(),
        &left.schema,
    );
    register(
        &context,
        "r",
        right.read_current(runtime.client()).await.unwrap(),
        &right.schema,
    );
    let frame = context
        .sql("select l.k, l.sub, l.lv, r.rv from l join r on l.k = r.k and l.sub = r.sub")
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let subs = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let left = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row).to_string(),
                subs.value(row).to_string(),
                left.value(row).to_string(),
                right.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

#[test_log::test(tokio::test)]
async fn join_with_string_multi_keys_and_payloads() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_generic_join_left_{suffix}"),
            table_path(&dir, "left"),
            left_schema(),
        ))
        .await
        .unwrap();
    let right = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_generic_join_right_{suffix}"),
            table_path(&dir, "right"),
            right_schema(),
        ))
        .await
        .unwrap();
    let join_keys = vec!["k".to_string(), "sub".to_string()];
    let output = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_generic_join_out_{suffix}"),
            table_path(&dir, "out"),
            join_view_schema_for(&left.schema, &right.schema, &join_keys, "lv", "rv")
                .unwrap(),
        ))
        .await
        .unwrap();
    let view = JoinView::new_with_join_keys(
        format!("generic_join_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        join_keys,
        "lv",
        "rv",
    );

    left.append_batch(
        runtime.client(),
        left_batch(&[("a", "x", "L1"), ("a", "y", "L2"), ("b", "x", "L3")]),
    )
    .await
    .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();
    assert!(join_rows(&runtime, &output).await.is_empty());

    right
        .append_batch(
            runtime.client(),
            right_batch(&[("a", "x", 10), ("b", "x", 30)]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();
    assert_eq!(
        join_rows(&runtime, &output).await,
        vec![
            ("a".to_string(), "x".to_string(), "L1".to_string(), 10),
            ("b".to_string(), "x".to_string(), "L3".to_string(), 30),
        ]
    );
    assert_eq!(
        join_rows(&runtime, &output).await,
        full_join(&runtime, &left, &right).await
    );

    right
        .append_batch(runtime.client(), right_batch(&[("a", "y", 20)]))
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();
    assert_eq!(
        join_rows(&runtime, &output).await,
        full_join(&runtime, &left, &right).await
    );

    runtime.rebuild_join(&view).await.unwrap();
    assert_eq!(
        join_rows(&runtime, &output).await,
        full_join(&runtime, &left, &right).await
    );

    runtime
        .client()
        .drop_table(&left.table_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&right.table_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&output.table_name, "default")
        .await
        .unwrap();
    drop(dir);
}

fn window_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn window_batch(rows: &[(&str, &str, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        window_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

async fn window_state(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> HashMap<String, (String, i64)> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let regions = batch
            .column(schema.index_of("region").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let numbers = batch
            .column(schema.index_of(IVM_ROW_NUMBER_COLUMN).unwrap())
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
                    ids.value(row).to_string(),
                    (regions.value(row).to_string(), numbers.value(row)),
                );
            }
        }
    }
    state
}

async fn full_window(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> HashMap<String, (String, i64)> {
    let context = SessionContext::new();
    register(
        &context,
        "src",
        source.read_current(runtime.client()).await.unwrap(),
        &source.schema,
    );
    let frame = context
        .sql(&format!(
            "select id, region, cast(row_number() over (partition by region order by name, id) as bigint) as rn \
             from src where \"{CHANGE_COLUMN}\" <> 'delete'"
        ))
        .await
        .unwrap();
    let mut state = HashMap::new();
    for batch in frame.collect().await.unwrap() {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let regions = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let numbers = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            state.insert(
                ids.value(row).to_string(),
                (regions.value(row).to_string(), numbers.value(row)),
            );
        }
    }
    state
}

#[test_log::test(tokio::test)]
async fn window_with_string_partition_and_order_keys() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_generic_window_src_{suffix}"),
                table_path(&dir, "src"),
                window_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_generic_window_mv_{suffix}"),
                table_path(&dir, "mv"),
                window_mv_schema_for(
                    &source.schema,
                    &["region".to_string()],
                    &["id".to_string()],
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["region".to_string(), "id".to_string()])
            .with_bucket_columns(vec!["region".to_string()]),
        )
        .await
        .unwrap();
    let view = WindowView::new(
        format!("generic_window_{suffix}"),
        source.clone(),
        mv.clone(),
        vec!["region".to_string()],
        vec!["name".to_string()],
    );

    source
        .append_batch(
            runtime.client(),
            window_batch(&[
                ("a", "r1", "m", "insert"),
                ("b", "r1", "k", "insert"),
                ("c", "r2", "z", "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_window(&view).await.unwrap().unwrap();
    assert_eq!(
        window_state(&runtime, &mv).await,
        HashMap::from([
            ("a".to_string(), ("r1".to_string(), 2)),
            ("b".to_string(), ("r1".to_string(), 1)),
            ("c".to_string(), ("r2".to_string(), 1)),
        ])
    );

    // A name update moves the row to the front.
    source
        .append_batch(
            runtime.client(),
            window_batch(&[("a", "r1", "a", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_window(&view).await.unwrap().unwrap();
    assert_eq!(
        window_state(&runtime, &mv).await,
        full_window(&runtime, &source).await
    );

    // A delete compresses the ranks, then a partition move rewrites both.
    source
        .append_batch(
            runtime.client(),
            window_batch(&[("b", "r1", "k", "delete")]),
        )
        .await
        .unwrap();
    runtime.refresh_window(&view).await.unwrap().unwrap();
    assert_eq!(
        window_state(&runtime, &mv).await,
        full_window(&runtime, &source).await
    );
    source
        .append_batch(
            runtime.client(),
            window_batch(&[("c", "r1", "z", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_window(&view).await.unwrap().unwrap();
    assert_eq!(
        window_state(&runtime, &mv).await,
        full_window(&runtime, &source).await
    );

    runtime.rebuild_window(&view).await.unwrap();
    assert_eq!(
        window_state(&runtime, &mv).await,
        full_window(&runtime, &source).await
    );

    runtime
        .client()
        .drop_table(&source.table_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&mv.table_name, "default")
        .await
        .unwrap();
    drop(dir);
}
