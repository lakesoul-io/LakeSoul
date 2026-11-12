// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! Multi-join probes on non-primary-key business keys.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    CompareOp, IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    MultiJoinColumn, MultiJoinCondition, MultiJoinKey, MultiJoinView, StateRole,
    multi_join_mv_schema_for, multi_join_primary_keys,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

async fn source(runtime: &IvmRuntime, dir: &tempfile::TempDir, name: &str) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(name.to_string(), table_path(dir, name), schema())
                .with_primary_keys(vec!["id".to_string()])
                .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

async fn rows(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, i64, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql(&format!(
            "select \"__pk0_id\" as id, av, bv, cv from mv \
             where \"{IVM_ROW_KINDS_COLUMN}\" = 'insert' order by id"
        ))
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
        let av = batch
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
                av.value(index),
                bv.value(index),
                cv.value(index),
            ));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn non_pk_edges_use_the_generic_column_probe() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let a = source(&runtime, &dir, &format!("ivm_npk_a_{suffix}")).await;
    let b = source(&runtime, &dir, &format!("ivm_npk_b_{suffix}")).await;
    let c = source(&runtime, &dir, &format!("ivm_npk_c_{suffix}")).await;

    let schemas = [a.schema.clone(), b.schema.clone(), c.schema.clone()];
    let primary_keys = vec![
        vec!["id".to_string()],
        vec!["id".to_string()],
        vec!["id".to_string()],
    ];
    let columns = vec![
        MultiJoinColumn {
            source: 0,
            column: "v".to_string(),
            name: "av".to_string(),
        },
        MultiJoinColumn {
            source: 1,
            column: "v".to_string(),
            name: "bv".to_string(),
        },
        MultiJoinColumn {
            source: 2,
            column: "v".to_string(),
            name: "cv".to_string(),
        },
    ];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_npk_mv_{suffix}"),
                table_path(&dir, "mv"),
                multi_join_mv_schema_for(&schemas, &primary_keys, &columns).unwrap(),
            )
            .with_primary_keys(multi_join_primary_keys(&primary_keys)),
        )
        .await
        .unwrap();
    let view = MultiJoinView::new(
        format!("ivm_npk_view_{suffix}"),
        vec![a.clone(), b.clone(), c.clone()],
        mv.clone(),
        vec![
            MultiJoinKey {
                left_source: 0,
                left_column: "k".to_string(),
                left_expr: None,
                right_source: 1,
                right_column: "k".to_string(),
                right_expr: None,
            },
            MultiJoinKey {
                left_source: 1,
                left_column: "k".to_string(),
                left_expr: None,
                right_source: 2,
                right_column: "k".to_string(),
                right_expr: None,
            },
        ],
        columns,
    );

    a.append_batch(
        runtime.client(),
        batch(&[(1, 10, 10, "insert"), (2, 20, 20, "insert")]),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        batch(&[(11, 10, 100, "insert"), (12, 20, 200, "insert")]),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        batch(&[(21, 10, 1000, "insert"), (22, 20, 2000, "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();
    assert_eq!(
        rows(&runtime, &mv).await,
        vec![(1, 10, 100, 1000), (2, 20, 200, 2000)]
    );

    // A payload change on a source whose edge key is not its primary key.
    b.append_batch(runtime.client(), batch(&[(12, 20, 201, "insert")]))
        .await
        .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();
    assert_eq!(
        rows(&runtime, &mv).await,
        vec![(1, 10, 100, 1000), (2, 20, 201, 2000)]
    );
}

/// A NULL-safe or computed edge keeps one narrow `(join key, source primary
/// key)` index per key set: the payload columns are not copied, and the
/// computed key follows its source expression across refreshes.
#[test_log::test(tokio::test)]
async fn computed_null_safe_edges_use_narrow_key_indexes() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let a = source(&runtime, &dir, &format!("ivm_nsk_a_{suffix}")).await;
    let b = source(&runtime, &dir, &format!("ivm_nsk_b_{suffix}")).await;
    let c = source(&runtime, &dir, &format!("ivm_nsk_c_{suffix}")).await;

    let schemas = [a.schema.clone(), b.schema.clone(), c.schema.clone()];
    let primary_keys = vec![
        vec!["id".to_string()],
        vec!["id".to_string()],
        vec!["id".to_string()],
    ];
    let columns = vec![
        MultiJoinColumn {
            source: 0,
            column: "v".to_string(),
            name: "av".to_string(),
        },
        MultiJoinColumn {
            source: 1,
            column: "v".to_string(),
            name: "bv".to_string(),
        },
        MultiJoinColumn {
            source: 2,
            column: "v".to_string(),
            name: "cv".to_string(),
        },
    ];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_nsk_mv_{suffix}"),
                table_path(&dir, "mv"),
                multi_join_mv_schema_for(&schemas, &primary_keys, &columns).unwrap(),
            )
            .with_primary_keys(multi_join_primary_keys(&primary_keys)),
        )
        .await
        .unwrap();
    let mut view = MultiJoinView::new(
        format!("ivm_nsk_view_{suffix}"),
        vec![a.clone(), b.clone(), c.clone()],
        mv.clone(),
        vec![
            MultiJoinKey {
                left_source: 0,
                left_column: "k".to_string(),
                left_expr: None,
                right_source: 1,
                right_column: "k".to_string(),
                right_expr: None,
            },
            MultiJoinKey {
                left_source: 1,
                left_column: "expr_key".to_string(),
                left_expr: Some("v + 1".to_string()),
                right_source: 2,
                right_column: "k".to_string(),
                right_expr: None,
            },
        ],
        columns,
    );
    view.null_safe = true;

    a.append_batch(runtime.client(), batch(&[(1, 10, 10, "insert")]))
        .await
        .unwrap();
    b.append_batch(runtime.client(), batch(&[(11, 10, 19, "insert")]))
        .await
        .unwrap();
    c.append_batch(runtime.client(), batch(&[(21, 20, 1000, "insert")]))
        .await
        .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();
    assert_eq!(rows(&runtime, &mv).await, vec![(1, 10, 19, 1000)]);

    // The narrow indexes exist for the measured and computed key sets; none
    // copies a payload column or the source CDC marker.
    let mut indexes = Vec::new();
    for state in runtime.metadata().list_states(&view.view_id).await.unwrap() {
        if matches!(state.role, StateRole::MultiJoinKeyIndex(_)) {
            indexes.push(state);
        }
    }
    assert_eq!(indexes.len(), 4, "one index per edge endpoint: {indexes:?}");
    for state in indexes {
        let index = runtime.open_table_by_id(&state.table_id).await.unwrap();
        assert!(index.schema.field_with_name("id").is_ok());
        assert!(
            index.schema.field_with_name("v").is_err(),
            "{:?}",
            index.schema
        );
        assert!(
            index.schema.field_with_name(CHANGE_COLUMN).is_err(),
            "the source CDC column must not be copied: {:?}",
            index.schema
        );
    }

    // Moving the computed key drops the tuple; the index follows the
    // expression, not the old value.
    b.append_batch(runtime.client(), batch(&[(11, 10, 29, "insert")]))
        .await
        .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();
    assert!(rows(&runtime, &mv).await.is_empty());
}

/// A multi-join with no equality key path cannot probe a source by key; the
/// refresh asks for the full recompute instead of walking every source.
#[test_log::test(tokio::test)]
async fn non_equi_multi_join_requires_a_rebuild() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let a = source(&runtime, &dir, &format!("ivm_neq_a_{suffix}")).await;
    let b = source(&runtime, &dir, &format!("ivm_neq_b_{suffix}")).await;
    let c = source(&runtime, &dir, &format!("ivm_neq_c_{suffix}")).await;

    let schemas = [a.schema.clone(), b.schema.clone(), c.schema.clone()];
    let primary_keys = vec![
        vec!["id".to_string()],
        vec!["id".to_string()],
        vec!["id".to_string()],
    ];
    let columns = vec![
        MultiJoinColumn {
            source: 0,
            column: "v".to_string(),
            name: "av".to_string(),
        },
        MultiJoinColumn {
            source: 1,
            column: "v".to_string(),
            name: "bv".to_string(),
        },
        MultiJoinColumn {
            source: 2,
            column: "v".to_string(),
            name: "cv".to_string(),
        },
    ];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_neq_mv_{suffix}"),
                table_path(&dir, "mv"),
                multi_join_mv_schema_for(&schemas, &primary_keys, &columns).unwrap(),
            )
            .with_primary_keys(multi_join_primary_keys(&primary_keys)),
        )
        .await
        .unwrap();
    let view = MultiJoinView::new(
        format!("ivm_neq_view_{suffix}"),
        vec![a.clone(), b.clone(), c.clone()],
        mv.clone(),
        vec![],
        columns,
    )
    .with_conditions(vec![
        MultiJoinCondition {
            left_source: 0,
            left_column: "v".to_string(),
            right_source: 1,
            right_column: "v".to_string(),
            op: CompareOp::Lt,
        },
        MultiJoinCondition {
            left_source: 1,
            left_column: "v".to_string(),
            right_source: 2,
            right_column: "v".to_string(),
            op: CompareOp::Lt,
        },
    ]);

    a.append_batch(runtime.client(), batch(&[(1, 10, 10, "insert")]))
        .await
        .unwrap();
    b.append_batch(runtime.client(), batch(&[(11, 10, 20, "insert")]))
        .await
        .unwrap();
    c.append_batch(runtime.client(), batch(&[(21, 20, 1000, "insert")]))
        .await
        .unwrap();
    runtime.rebuild_multi_join(&view).await.unwrap();
    assert_eq!(rows(&runtime, &mv).await, vec![(1, 10, 20, 1000)]);

    // The next window has no key to probe: it must rebuild, not silently read
    // every source as part of an "incremental" refresh.
    b.append_batch(runtime.client(), batch(&[(11, 10, 5, "insert")]))
        .await
        .unwrap();
    let error = runtime.refresh_multi_join(&view).await.unwrap_err();
    assert!(
        format!("{error}").contains("full recompute"),
        "unexpected error: {error}"
    );
    runtime.rebuild_multi_join(&view).await.unwrap();
    assert!(rows(&runtime, &mv).await.is_empty());
}
