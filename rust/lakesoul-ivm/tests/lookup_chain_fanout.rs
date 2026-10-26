// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! A dimension change in a star lookup chain whose join key is not the base
//! primary key.  The per-step base join-key index maps the changed dimension
//! keys to affected base rows without rebuilding the whole chain.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmTable, IvmTableOptions, JoinKeyExprs, LookupChainColumn,
    LookupChainStep, LookupChainView, lookup_chain_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn base_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn dim_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn base_batch(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        base_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn dim_batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        dim_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

async fn create_base(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(name.to_string(), table_path(dir, name), base_schema())
                .with_primary_keys(vec!["id".to_string()])
                .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

async fn create_dim(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(name.to_string(), table_path(dir, name), dim_schema())
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

async fn rows(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(i64, i64, Option<i64>, Option<i64>)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select id, v, bv, cv from mv where id in (1, 2) order by id")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let v = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let bv = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cv = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                id.value(index),
                v.value(index),
                (!bv.is_null(index)).then(|| bv.value(index)),
                (!cv.is_null(index)).then(|| cv.value(index)),
            ));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn dimension_changes_fan_out_through_the_base_join_key_index() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let base = create_base(&runtime, &dir, &format!("ivm_fan_base_{suffix}")).await;
    let dim1 = create_dim(&runtime, &dir, &format!("ivm_fan_dim1_{suffix}")).await;
    let dim2 = create_dim(&runtime, &dir, &format!("ivm_fan_dim2_{suffix}")).await;

    let steps = vec![
        LookupChainStep {
            source: 1,
            left: true,
            keys: vec!["k".to_string()],
            right_keys: vec!["k".to_string()],
            key_sources: Vec::new(),
            unique: true,
            key_exprs: JoinKeyExprs::default(),
        },
        LookupChainStep {
            source: 2,
            left: true,
            keys: vec!["k".to_string()],
            right_keys: vec!["k".to_string()],
            key_sources: Vec::new(),
            unique: true,
            key_exprs: JoinKeyExprs::default(),
        },
    ];
    let columns = vec![
        LookupChainColumn {
            source: 0,
            column: "id".to_string(),
            name: "id".to_string(),
        },
        LookupChainColumn {
            source: 0,
            column: "v".to_string(),
            name: "v".to_string(),
        },
        LookupChainColumn {
            source: 1,
            column: "v".to_string(),
            name: "bv".to_string(),
        },
        LookupChainColumn {
            source: 2,
            column: "v".to_string(),
            name: "cv".to_string(),
        },
    ];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fan_mv_{suffix}"),
                table_path(&dir, "mv"),
                lookup_chain_mv_schema_for(
                    &[
                        base.schema.clone(),
                        dim1.schema.clone(),
                        dim2.schema.clone(),
                    ],
                    &steps,
                    &columns,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["id".to_string()]),
        )
        .await
        .unwrap();
    let view = LookupChainView::new(
        format!("ivm_fan_view_{suffix}"),
        vec![
            (base.clone(), None),
            (dim1.clone(), None),
            (dim2.clone(), None),
        ],
        steps,
        columns,
        mv.clone(),
    );

    let mut base_rows = vec![(1, 10, 100, "insert"), (2, 20, 200, "insert")];
    for id in 3..=100 {
        base_rows.push((id, 1_000 + id, id, "insert"));
    }
    base.append_batch(runtime.client(), base_batch(&base_rows))
        .await
        .unwrap();
    dim1.append_batch(
        runtime.client(),
        dim_batch(&[(10, 5, "insert"), (20, 6, "insert")]),
    )
    .await
    .unwrap();
    dim2.append_batch(
        runtime.client(),
        dim_batch(&[(10, 7, "insert"), (20, 8, "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_lookup_chain(&view).await.unwrap();
    assert_eq!(
        rows(&runtime, &mv).await,
        vec![(1, 100, Some(5), Some(7)), (2, 200, Some(6), Some(8)),]
    );

    // Dimension-only change: the index maps k=10 to base id 1.
    dim1.append_batch(runtime.client(), dim_batch(&[(10, 50, "insert")]))
        .await
        .unwrap();
    runtime.refresh_lookup_chain(&view).await.unwrap();
    assert_eq!(
        rows(&runtime, &mv).await,
        vec![(1, 100, Some(50), Some(7)), (2, 200, Some(6), Some(8)),]
    );

    // Base key move: the index must move with the base PK.
    base.append_batch(runtime.client(), base_batch(&[(1, 20, 100, "insert")]))
        .await
        .unwrap();
    runtime.refresh_lookup_chain(&view).await.unwrap();
    assert_eq!(
        rows(&runtime, &mv).await,
        vec![(1, 100, Some(6), Some(8)), (2, 200, Some(6), Some(8)),]
    );

    // A second dimension change still uses the maintained index.
    dim2.append_batch(runtime.client(), dim_batch(&[(20, 80, "insert")]))
        .await
        .unwrap();
    runtime.refresh_lookup_chain(&view).await.unwrap();
    assert_eq!(
        rows(&runtime, &mv).await,
        vec![(1, 100, Some(6), Some(80)), (2, 200, Some(6), Some(80)),]
    );

    // Deleting the base row removes it from the index and the MV.
    base.append_batch(runtime.client(), base_batch(&[(2, 20, 200, "delete")]))
        .await
        .unwrap();
    runtime.refresh_lookup_chain(&view).await.unwrap();
    assert_eq!(rows(&runtime, &mv).await, vec![(1, 100, Some(6), Some(80))]);
}
