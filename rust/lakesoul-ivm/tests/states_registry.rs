// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The `ivm.states` registry: every view registers the internal LakeSoul
//! tables it keeps its state in, the binding is stable across refreshes, a
//! conflicting table for the same `(view, role)` is refused, and deleting a
//! view removes its registrations.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    IvmRuntime, IvmTableOptions, JoinView, MinMaxKind, MinMaxView, StateRole,
    SumCountView, WindowView, keyed_join_output_primary_keys, keyed_join_view_schema_for,
    min_max_mv_schema_for, sum_count_mv_schema_for, value_count_state_schema_for,
    window_mv_schema_for,
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

fn source_batch() -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(StringArray::from(vec!["insert", "insert"])),
        ],
    )
    .unwrap()
}

async fn sum_count_fixture(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
) -> (lakesoul_ivm::IvmTable, SumCountView) {
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_states_sum_src_{suffix}"),
                table_path(dir, "sum_src"),
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
            IvmTableOptions::new(
                format!("ivm_states_sum_mv_{suffix}"),
                table_path(dir, "sum_mv"),
                sum_count_mv_schema_for(&source.schema, &group_keys, Some("amount"))
                    .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("states_sum_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        Some("amount".to_string()),
    );
    source
        .append_batch(runtime.client(), source_batch())
        .await
        .unwrap();
    (mv, view)
}

async fn min_max_fixture(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
) -> (lakesoul_ivm::IvmTable, lakesoul_ivm::IvmTable, MinMaxView) {
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_states_min_src_{suffix}"),
                table_path(dir, "min_src"),
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
            IvmTableOptions::new(
                format!("ivm_states_min_mv_{suffix}"),
                table_path(dir, "min_mv"),
                min_max_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    "amount",
                    MinMaxKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_states_min_state_{suffix}"),
                table_path(dir, "min_state"),
                value_count_state_schema_for(&source.schema, &group_keys, "amount")
                    .unwrap(),
            )
            .with_primary_keys(vec![
                "g".to_string(),
                lakesoul_ivm::IVM_VALUE_COLUMN.to_string(),
            ])
            .with_bucket_columns(group_keys.clone()),
        )
        .await
        .unwrap();
    let view = MinMaxView::new(
        format!("states_min_{suffix}"),
        source.clone(),
        mv.clone(),
        state.clone(),
        "g",
        "amount",
        MinMaxKind::Min,
    );
    source
        .append_batch(runtime.client(), source_batch())
        .await
        .unwrap();
    (mv, state, view)
}

async fn window_fixture(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
) -> (lakesoul_ivm::IvmTable, WindowView) {
    let suffix = uuid::Uuid::new_v4().simple();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("p", DataType::Utf8, false),
        Field::new("ord", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_states_win_src_{suffix}"),
                table_path(dir, "win_src"),
                schema,
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let partitions = vec!["p".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_states_win_mv_{suffix}"),
                table_path(dir, "win_mv"),
                window_mv_schema_for(&source.schema, &partitions, &["id".to_string()])
                    .unwrap(),
            )
            .with_primary_keys(vec!["p".to_string(), "id".to_string()])
            .with_bucket_columns(partitions.clone()),
        )
        .await
        .unwrap();
    let view = WindowView::new(
        format!("states_win_{suffix}"),
        source.clone(),
        mv.clone(),
        partitions,
        vec!["ord".to_string()],
    );
    source
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                source.schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec!["a"])),
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(StringArray::from(vec!["insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    (mv, view)
}

async fn keyed_join_fixture(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
) -> (lakesoul_ivm::IvmTable, JoinView) {
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_states_join_l_{suffix}"),
                table_path(dir, "join_l"),
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("jk", DataType::Utf8, false),
                    Field::new("lv", DataType::Utf8, false),
                    Field::new(CHANGE_COLUMN, DataType::Utf8, false),
                ])),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_states_join_r_{suffix}"),
                table_path(dir, "join_r"),
                Arc::new(Schema::new(vec![
                    Field::new("rid", DataType::Int64, false),
                    Field::new("jk", DataType::Utf8, false),
                    Field::new("rv", DataType::Int64, false),
                    Field::new(CHANGE_COLUMN, DataType::Utf8, false),
                ])),
            )
            .with_primary_keys(vec!["rid".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let join_keys = vec!["jk".to_string()];
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_states_join_out_{suffix}"),
                table_path(dir, "join_out"),
                keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &["id".to_string()],
                    &["rid".to_string()],
                    &join_keys,
                    "lv",
                    "rv",
                )
                .unwrap(),
            )
            .with_primary_keys(keyed_join_output_primary_keys(
                &["id".to_string()],
                &["rid".to_string()],
            )),
        )
        .await
        .unwrap();
    let view = JoinView::new_with_join_keys(
        format!("states_join_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        join_keys,
        "lv",
        "rv",
    );
    left.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            left.schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["L1"])),
                Arc::new(StringArray::from(vec!["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    (output, view)
}

async fn pair_list(runtime: &IvmRuntime, view_id: &str) -> Vec<(&'static str, String)> {
    runtime
        .list_states(view_id)
        .await
        .unwrap()
        .into_iter()
        .map(|state| (state.role.as_str(), state.table_id))
        .collect()
}

#[test_log::test(tokio::test)]
async fn states_are_registered_for_each_view_kind() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();

    let (sum_mv, sum_view) = sum_count_fixture(&runtime, &dir).await;
    let (min_mv, min_state, min_view) = min_max_fixture(&runtime, &dir).await;
    let (window_mv, window_view) = window_fixture(&runtime, &dir).await;
    let (join_mv, join_view) = keyed_join_fixture(&runtime, &dir).await;

    runtime.refresh_sum_count(&sum_view).await.unwrap().unwrap();
    runtime.refresh_min_max(&min_view).await.unwrap().unwrap();
    runtime.refresh_window(&window_view).await.unwrap().unwrap();
    runtime.refresh_join(&join_view).await.unwrap().unwrap();

    assert_eq!(
        pair_list(&runtime, &sum_view.view_id).await,
        vec![("mv", sum_mv.table_id.clone())]
    );
    assert_eq!(
        pair_list(&runtime, &min_view.view_id).await,
        vec![
            ("mv", min_mv.table_id.clone()),
            ("state", min_state.table_id.clone()),
        ]
    );
    assert_eq!(
        pair_list(&runtime, &window_view.view_id).await,
        vec![("mv", window_mv.table_id.clone())]
    );
    assert_eq!(
        pair_list(&runtime, &join_view.view_id).await,
        vec![("mv", join_mv.table_id.clone())]
    );

    // Re-registering the same tables (a fresh refresh or a rebuild) keeps one
    // row per role.
    runtime.refresh_sum_count(&sum_view).await.unwrap();
    runtime.rebuild_sum_count(&sum_view).await.unwrap();
    assert_eq!(
        pair_list(&runtime, &sum_view.view_id).await,
        vec![("mv", sum_mv.table_id.clone())]
    );

    // The registered table carries its name and path.
    let states = runtime.list_states(&sum_view.view_id).await.unwrap();
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].role.as_str(), "mv");
    assert_eq!(states[0].table_name, sum_mv.table_name);
    assert_eq!(states[0].table_path, sum_mv.table_path);

    // get_state finds the same binding by role.
    let state = runtime
        .metadata()
        .get_state(&sum_view.view_id, StateRole::Mv)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.table_id, sum_mv.table_id);
    assert!(
        runtime
            .metadata()
            .get_state(&sum_view.view_id, StateRole::State)
            .await
            .unwrap()
            .is_none()
    );
}

#[test_log::test(tokio::test)]
async fn conflicting_state_table_is_rejected() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();

    let (first_mv, first) = sum_count_fixture(&runtime, &dir).await;
    runtime.refresh_sum_count(&first).await.unwrap().unwrap();

    // A second view with the same id but a different MV table must not bind.
    let other_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_states_other_mv_{}", uuid::Uuid::new_v4().simple()),
                table_path(&dir, "other_mv"),
                sum_count_mv_schema_for(
                    &first.source.schema,
                    &first.group_keys,
                    first.value_column.as_deref(),
                )
                .unwrap(),
            )
            .with_primary_keys(first.group_keys.clone()),
        )
        .await
        .unwrap();
    let conflicting = SumCountView::new_with_group_keys(
        first.view_id.clone(),
        first.source.clone(),
        other_mv.clone(),
        first.group_keys.clone(),
        first.value_column.clone(),
    );
    let error = runtime.refresh_sum_count(&conflicting).await.unwrap_err();
    assert!(
        error.to_string().contains("already uses table")
            && error.to_string().contains(&first_mv.table_name),
        "unexpected error: {error}"
    );

    // The first binding is still in place.
    assert_eq!(
        runtime.list_states(&first.view_id).await.unwrap()[0].table_id,
        first_mv.table_id
    );
}

#[test_log::test(tokio::test)]
async fn delete_view_removes_states() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();

    let (_mv, _state, view) = min_max_fixture(&runtime, &dir).await;
    runtime.refresh_min_max(&view).await.unwrap().unwrap();
    assert_eq!(runtime.list_states(&view.view_id).await.unwrap().len(), 2);

    runtime.metadata().delete_view(&view.view_id).await.unwrap();
    assert!(runtime.list_states(&view.view_id).await.unwrap().is_empty());
    assert!(
        runtime
            .metadata()
            .get_view_spec(&view.view_id)
            .await
            .unwrap()
            .is_none()
    );
}
