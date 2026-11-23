// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The reusable PK lookup plan also serves a source whose primary key is the
//! join key.  No side-state table is needed in that case; the right source is
//! read through the same physical lookup node used by side state.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTableOptions, JoinView,
    keyed_join_output_primary_keys, keyed_join_view_schema_for,
};
use tempfile::tempdir;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
        ],
    )
    .unwrap()
}

fn path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

async fn rows(
    runtime: &IvmRuntime,
    table: &lakesoul_ivm::IvmTable,
) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in table.read_current(runtime.client()).await.unwrap() {
        let key = batch
            .column(batch.schema().index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left = batch
            .column(batch.schema().index_of("left_value").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
            .column(batch.schema().index_of("right_value").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(batch.schema().index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                rows.push((key.value(row), left.value(row), right.value(row)));
            }
        }
    }
    rows.sort_unstable();
    rows
}

#[test_log::test(tokio::test)]
async fn source_primary_key_as_join_key_uses_the_shared_lookup_plan() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pk_src_left_{suffix}"),
                path(&dir, "left"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_bucket_columns(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pk_src_right_{suffix}"),
                path(&dir, "right"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_bucket_columns(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pk_src_out_{suffix}"),
                path(&dir, "out"),
                keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &["k".to_string()],
                    &["k".to_string()],
                    &["k".to_string()],
                    "v",
                    "v",
                )
                .unwrap(),
            )
            .with_primary_keys(keyed_join_output_primary_keys(
                &["k".to_string()],
                &["k".to_string()],
            )),
        )
        .await
        .unwrap();
    let view = JoinView::new(
        format!("ivm_pk_src_view_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        "k",
        "v",
        "v",
    );

    left.append_batch(runtime.client(), batch(&[(1, 10), (2, 20)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), batch(&[(1, 100), (2, 200)]))
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();
    assert_eq!(
        rows(&runtime, &output).await,
        vec![(1, 10, 100), (2, 20, 200)]
    );

    // A left-only payload update must read the right source by the left delta
    // keys through the shared PK lookup plan, not scan the full right state.
    left.append_batch(runtime.client(), batch(&[(1, 11)]))
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();
    assert_eq!(
        rows(&runtime, &output).await,
        vec![(1, 11, 100), (2, 20, 200)]
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
}

fn composite_left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k1", DataType::Int64, false),
        Field::new("k2", DataType::Utf8, false),
        Field::new("lv", DataType::Int64, false),
    ]))
}

fn composite_right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int64, false),
        Field::new("k2", DataType::Utf8, false),
        Field::new("rv", DataType::Int64, false),
    ]))
}

fn composite_left_batch(rows: &[(i64, i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        composite_left_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn composite_right_batch(rows: &[(i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        composite_right_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

#[test_log::test(tokio::test)]
async fn composite_source_primary_key_as_join_key_uses_columns_lookup() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pk_comp_left_{suffix}"),
                path(&dir, "left"),
                composite_left_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_bucket_columns(vec!["id".to_string()]),
        )
        .await
        .unwrap();
    let mut right_options = IvmTableOptions::new(
        format!("ivm_pk_comp_right_{suffix}"),
        path(&dir, "right"),
        composite_right_schema(),
    )
    .with_primary_keys(vec!["k1".to_string(), "k2".to_string()])
    .with_bucket_columns(vec!["k1".to_string()]);
    right_options.hash_bucket_num = "4".to_string();
    let right = runtime.create_table(right_options).await.unwrap();
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pk_comp_out_{suffix}"),
                path(&dir, "out"),
                keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &["id".to_string()],
                    &["k1".to_string(), "k2".to_string()],
                    &["k1".to_string(), "k2".to_string()],
                    "lv",
                    "rv",
                )
                .unwrap(),
            )
            .with_primary_keys(keyed_join_output_primary_keys(
                &["id".to_string()],
                &["k1".to_string(), "k2".to_string()],
            )),
        )
        .await
        .unwrap();
    let view = JoinView::new_with_join_keys(
        format!("ivm_pk_comp_view_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        vec!["k1".to_string(), "k2".to_string()],
        "lv",
        "rv",
    );

    left.append_batch(
        runtime.client(),
        composite_left_batch(&[(1, 10, "a", 100), (2, 10, "b", 200)]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            composite_right_batch(&[(10, "a", 1000), (10, "b", 2000)]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();

    // Left-only payload update.  The right source is keyed by (k1,k2), so the
    // lookup plan must build a composite Arrow-Row key, route by k1, read the
    // matching right rows, and preserve both pairs.
    left.append_batch(runtime.client(), composite_left_batch(&[(1, 10, "a", 101)]))
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();

    let mut rows = Vec::new();
    for batch in output.read_current(runtime.client()).await.unwrap() {
        let kind = batch
            .column(batch.schema().index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let left_id = batch
            .column(batch.schema().index_of("__left_pk_id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left_value = batch
            .column(batch.schema().index_of("left_value").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_value = batch
            .column(batch.schema().index_of("right_value").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kind.value(row) == "insert" {
                rows.push((
                    left_id.value(row),
                    left_value.value(row),
                    right_value.value(row),
                ));
            }
        }
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(1, 101, 1000), (2, 200, 2000)]);

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
}
