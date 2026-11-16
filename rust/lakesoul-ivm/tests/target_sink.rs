// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Target projection sinks: a user target table whose schema/primary key is
//! independent of the maintenance state.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_EPOCH_COLUMN, IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmSqlExecutor, IvmTable,
    IvmTableOptions, StateRole, ViewSpec,
};
use tempfile::tempdir;

const CHANGE: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE, DataType::Utf8, false),
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

async fn source(runtime: &IvmRuntime, dir: &tempfile::TempDir, tag: &str) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_sink_{tag}"),
                table_path(dir, tag),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE),
        )
        .await
        .unwrap()
}

/// A target partition move must keep the old partition's delete.  The state
/// row is unpartitioned; the sink, not the state, knows the target partition.
#[test_log::test(tokio::test)]
async fn target_partition_move_removes_the_old_partition_row() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = source(&runtime, &dir, &format!("mv_src_{suffix}")).await;
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("biz_key", DataType::Utf8, false),
        Field::new("stat_date", DataType::Utf8, false),
        Field::new("v", DataType::Int64, true),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let target = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_move_target_{suffix}"),
                table_path(&dir, "target"),
                target_schema,
            )
            .with_primary_keys(vec!["biz_key".to_string()])
            .with_range_partition_columns(vec!["stat_date".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT CAST(k AS VARCHAR) AS biz_key, g AS stat_date, v FROM {}",
        target.table_name, source.table_name
    );
    let runtime = executor.runtime();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "2026-08-01", 10, "insert"),
                (2, "2026-08-01", 20, "insert"),
            ]),
        )
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();

    // The same business key moves to another target partition.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "2026-08-02", 11, "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");

    let context = SessionContext::new();
    context
        .register_table("target", Arc::new(runtime.table_provider(&target)))
        .unwrap();
    let batches = context
        .sql("select biz_key, stat_date, v from target order by biz_key, stat_date")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row).to_string(),
                dates.value(row).to_string(),
                values.value(row),
            ));
        }
    }
    assert_eq!(
        rows,
        vec![
            ("1".to_string(), "2026-08-02".to_string(), 11),
            ("2".to_string(), "2026-08-01".to_string(), 20),
        ]
    );
}

#[test_log::test(tokio::test)]
async fn target_key_and_schema_are_independent_of_the_state() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = source(&runtime, &dir, &format!("src_{suffix}")).await;
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("biz_key", DataType::Utf8, false),
        Field::new("sum_v", DataType::Int64, true),
        Field::new("count_v", DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let target = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_sink_mv_{suffix}"),
                table_path(&dir, "mv"),
                target_schema,
            )
            .with_primary_keys(vec!["biz_key".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT CAST(k AS VARCHAR) || ':' || g AS biz_key, \
         SUM(v) AS sum_v, COUNT(*) AS count_v FROM {} GROUP BY k, g",
        target.table_name, source.table_name
    );
    let runtime = executor.runtime();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "insert"), (2, "b", 20, "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_target(runtime, &target).await,
        vec![("1:a".to_string(), 10, 1), ("2:b".to_string(), 20, 1)]
    );

    source
        .append_batch(runtime.client(), source_batch(&[(1, "a", 12, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_target(runtime, &target).await,
        vec![("1:a".to_string(), 12, 1), ("2:b".to_string(), 20, 1)]
    );

    // Moving the group changes the target business key: the old target row
    // is retracted and the new one inserted.
    source
        .append_batch(runtime.client(), source_batch(&[(1, "c", 13, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_target(runtime, &target).await,
        vec![("1:c".to_string(), 13, 1), ("2:b".to_string(), 20, 1)]
    );

    source
        .append_batch(runtime.client(), source_batch(&[(2, "b", 20, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_target(runtime, &target).await,
        vec![("1:c".to_string(), 13, 1)]
    );

    // A changed maintenance definition rebuilds the state and the target.
    let changed = format!(
        "INSERT INTO {} SELECT CAST(k AS VARCHAR) || ':' || g AS biz_key, \
         SUM(v) AS sum_v, COUNT(*) AS count_v FROM {} WHERE v > 0 GROUP BY k, g",
        target.table_name, source.table_name
    );
    let execution = executor.execute(&changed).await.unwrap();
    assert_eq!(execution.action.label(), "rebuild");
    assert_eq!(
        read_target(executor.runtime(), &target).await,
        vec![("1:c".to_string(), 13, 1)]
    );

    // Rebuilding the maintenance state out of band resets its changelog; the
    // next statement must rebuild the target instead of consuming the
    // insert-only snapshot as a delta.
    let state_table = runtime
        .open_table(
            &format!("{}__ivm_state", target.table_name),
            &target.namespace,
        )
        .await
        .unwrap();
    let state_spec: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&state_table.table_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    runtime.rebuild_spec(&state_spec).await.unwrap();
    let execution = executor.execute(&changed).await.unwrap();
    assert_eq!(execution.action.label(), "rebuild");
    assert_eq!(
        read_target(executor.runtime(), &target).await,
        vec![("1:c".to_string(), 13, 1)]
    );
}

fn join_left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("lk", DataType::Utf8, false),
        Field::new("jk", DataType::Utf8, true),
        Field::new("lv", DataType::Utf8, true),
        Field::new(CHANGE, DataType::Utf8, false),
    ]))
}

fn join_right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("jk", DataType::Utf8, false),
        Field::new("rv", DataType::Int64, true),
        Field::new(CHANGE, DataType::Utf8, false),
    ]))
}

fn join_left_batch(rows: &[(&str, &str, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        join_left_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn join_right_batch(rows: &[(&str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        join_right_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

#[test_log::test(tokio::test)]
async fn a_computed_final_row_folds_into_the_sink() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fold_left_{suffix}"),
                table_path(&dir, "left"),
                join_left_schema(),
            )
            .with_primary_keys(vec!["lk".to_string()])
            .with_cdc_column(CHANGE),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fold_right_{suffix}"),
                table_path(&dir, "right"),
                join_right_schema(),
            )
            .with_primary_keys(vec!["jk".to_string()])
            .with_cdc_column(CHANGE),
        )
        .await
        .unwrap();
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("score", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let target = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_sink_fold_{suffix}"),
                table_path(&dir, "fold"),
                target_schema,
            )
            .with_primary_keys(vec!["score".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT \
         CASE WHEN r.rv IS NULL THEN 0 ELSE r.rv END + LENGTH(l.lv) AS score, \
         l.lv AS label FROM {} l LEFT JOIN {} r ON l.jk = r.jk",
        target.table_name, left.table_name, right.table_name
    );
    let runtime = executor.runtime();
    left.append_batch(
        runtime.client(),
        join_left_batch(&[("l1", "a", "la", "insert"), ("l2", "b", "lb", "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            join_right_batch(&[("a", 10, "insert"), ("b", 20, "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_score_label(runtime, &target).await,
        vec![(12, "la".to_string()), (22, "lb".to_string())]
    );

    // The final row only computed target columns; it is folded into the sink
    // instead of materializing a copy of the target.
    assert!(
        runtime
            .open_table(
                &format!("{}__ivm_state", target.table_name),
                &target.namespace
            )
            .await
            .is_err()
    );
    let states = runtime.list_states(&target.table_id).await.unwrap();
    assert!(
        states.iter().all(|state| state.role == StateRole::Mv),
        "{states:?}"
    );
    let sink: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&target.table_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let ViewSpec::Sink {
        source_table_id,
        columns,
        ..
    } = sink
    else {
        panic!("expected a sink");
    };
    assert!(columns.iter().any(|column| column.expression.is_some()));
    let source_spec: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&source_table_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(source_spec.kind(), "lookup_join");

    // A right-side update recomputes the expression and moves the target key.
    right
        .append_batch(runtime.client(), join_right_batch(&[("a", 30, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_score_label(runtime, &target).await,
        vec![(22, "lb".to_string()), (32, "la".to_string())]
    );

    // Deleting a left row removes its target row.
    left.append_batch(
        runtime.client(),
        join_left_batch(&[("l2", "b", "lb", "delete")]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_score_label(runtime, &target).await,
        vec![(32, "la".to_string())]
    );

    // A rebuild of the folded source resets its changelog; the sink rebuilds
    // from the source snapshot instead of consuming it as a delta.
    let source_spec: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&source_table_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    runtime.rebuild_spec(&source_spec).await.unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "rebuild");
    assert_eq!(
        read_score_label(executor.runtime(), &target).await,
        vec![(32, "la".to_string())]
    );
}

async fn read_score_label(runtime: &IvmRuntime, target: &IvmTable) -> Vec<(i64, String)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(target)))
        .unwrap();
    let batches = context
        .sql("select score, label from mv order by score")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let scores = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((scores.value(row), keys.value(row).to_string()));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn a_large_upsert_batch_keeps_every_target_row() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = source(&runtime, &dir, &format!("big_src_{suffix}")).await;
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("biz_key", DataType::Utf8, false),
        Field::new("sum_v", DataType::Int64, true),
        Field::new("count_v", DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let target = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_sink_big_mv_{suffix}"),
                table_path(&dir, "mv"),
                target_schema,
            )
            .with_primary_keys(vec!["biz_key".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT CAST(k AS VARCHAR) AS biz_key, \
         SUM(v) AS sum_v, COUNT(*) AS count_v FROM {} GROUP BY k",
        target.table_name, source.table_name
    );
    let runtime = executor.runtime();
    let rows = (0..9_000)
        .map(|id| (id, "a", 1, "insert"))
        .collect::<Vec<_>>();
    source
        .append_batch(runtime.client(), source_batch(&rows))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(read_target(runtime, &target).await.len(), 9_000);

    let updates = (0..9_000)
        .map(|id| (id, "a", 2, "insert"))
        .collect::<Vec<_>>();
    source
        .append_batch(runtime.client(), source_batch(&updates))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    let rows = read_target(runtime, &target).await;
    assert_eq!(rows.len(), 9_000);
    assert!(rows.iter().all(|(_, sum, count)| *sum == 2 && *count == 1));
}

async fn read_target(runtime: &IvmRuntime, target: &IvmTable) -> Vec<(String, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(target)))
        .unwrap();
    let batches = context
        .sql("select biz_key, sum_v, count_v from mv order by biz_key")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = batch
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
            rows.push((
                keys.value(row).to_string(),
                sums.value(row),
                counts.value(row),
            ));
        }
    }
    rows
}

async fn read_sum_label(
    runtime: &IvmRuntime,
    target: &IvmTable,
) -> Vec<(String, i64, String)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(target)))
        .unwrap();
    let batches = context
        .sql("select biz_key, sum_v, label from mv order by biz_key")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let labels = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row).to_string(),
                sums.value(row),
                labels.value(row).to_string(),
            ));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn sum_and_scalar_use_the_mv_as_state() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = source(&runtime, &dir, &format!("mvstate_src_{suffix}")).await;
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("biz_key", DataType::Utf8, false),
        Field::new("sum_v", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let target = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_mvstate_target_{suffix}"),
                table_path(&dir, "target"),
                target_schema,
            )
            .with_primary_keys(vec!["biz_key".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT CAST(k AS VARCHAR) || ':' || g AS biz_key, \
         SUM(v) AS sum_v, UPPER(g) AS label FROM {} GROUP BY k, g",
        target.table_name, source.table_name
    );
    let runtime = executor.runtime();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "insert"), (2, "b", 20, "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_sum_label(runtime, &target).await,
        vec![
            ("1:a".to_string(), 10, "A".to_string()),
            ("2:b".to_string(), 20, "B".to_string()),
        ]
    );

    // The derived aggregate step uses its MV as the state: it carries the
    // hidden row count and has no separate multi-agg state tables.
    let sink: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&target.table_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let ViewSpec::Sink {
        source_table_id, ..
    } = sink
    else {
        panic!("expected a sink");
    };
    let step_spec: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&source_table_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let ViewSpec::MultiAgg { mv_state, .. } = step_spec else {
        panic!("expected a multi aggregate step");
    };
    assert!(mv_state);
    let step = runtime.open_table_by_id(&source_table_id).await.unwrap();
    assert!(
        step.schema
            .field_with_name(lakesoul_ivm::IVM_ROW_COUNT_COLUMN)
            .is_ok(),
        "{:?}",
        step.schema
    );
    let states = runtime.list_states(&source_table_id).await.unwrap();
    assert!(
        states.iter().all(|state| state.role == StateRole::Mv),
        "{states:?}"
    );

    // An update accumulates into the MV state.
    source
        .append_batch(runtime.client(), source_batch(&[(1, "a", 12, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_sum_label(runtime, &target).await,
        vec![
            ("1:a".to_string(), 12, "A".to_string()),
            ("2:b".to_string(), 20, "B".to_string()),
        ]
    );

    // Moving the group key retracts the old group and inserts the new one.
    source
        .append_batch(runtime.client(), source_batch(&[(1, "b", 12, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_sum_label(runtime, &target).await,
        vec![
            ("1:b".to_string(), 12, "B".to_string()),
            ("2:b".to_string(), 20, "B".to_string()),
        ]
    );

    // Deleting the last source row of a group removes its state/MV row.
    source
        .append_batch(runtime.client(), source_batch(&[(2, "b", 20, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_sum_label(runtime, &target).await,
        vec![("1:b".to_string(), 12, "B".to_string())]
    );

    // A changed aggregate definition rebuilds the MV-state step from scratch.
    let changed = format!(
        "INSERT INTO {} SELECT CAST(k AS VARCHAR) || ':' || g AS biz_key, \
         SUM(v * 2) AS sum_v, UPPER(g) AS label FROM {} GROUP BY k, g",
        target.table_name, source.table_name
    );
    let execution = executor.execute(&changed).await.unwrap();
    assert_eq!(execution.action.label(), "rebuild");
    assert_eq!(
        read_sum_label(runtime, &target).await,
        vec![("1:b".to_string(), 24, "B".to_string())]
    );
}

async fn read_sum_flag(
    runtime: &IvmRuntime,
    target: &IvmTable,
) -> Vec<(String, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(target)))
        .unwrap();
    let batches = context
        .sql("select g, sum_v, has_big from mv order by g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let flags = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row).to_string(),
                sums.value(row),
                flags.value(row),
            ));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn indicator_max_uses_the_mv_as_state() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = source(&runtime, &dir, &format!("mvflag_src_{suffix}")).await;
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("sum_v", DataType::Int64, true),
        Field::new("has_big", DataType::Int64, true),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let target = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_mvflag_target_{suffix}"),
                table_path(&dir, "target"),
                target_schema,
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT g, SUM(v) AS sum_v, \
         MAX(CASE WHEN v > 10 THEN 1 ELSE 0 END) AS has_big FROM {} GROUP BY g",
        target.table_name, source.table_name
    );
    let runtime = executor.runtime();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 20, "insert"),
                (3, "b", 5, "insert"),
            ]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_sum_flag(runtime, &target).await,
        vec![("a".to_string(), 30, 1), ("b".to_string(), 5, 0),]
    );

    // The indicator MAX is carried by a hidden count inside the MV state.
    let sink: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&target.table_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let ViewSpec::Sink {
        source_table_id, ..
    } = sink
    else {
        panic!("expected a sink");
    };
    let step_spec: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&source_table_id)
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        step_spec,
        ViewSpec::MultiAgg { mv_state: true, .. }
    ));
    let step = runtime.open_table_by_id(&source_table_id).await.unwrap();
    assert!(
        step.schema.field_with_name("__ivm_cnt_has_big").is_ok(),
        "{:?}",
        step.schema
    );
    let states = runtime.list_states(&source_table_id).await.unwrap();
    assert!(
        states.iter().all(|state| state.role == StateRole::Mv),
        "{states:?}"
    );

    // A second matching row keeps the flag at 1 while the sum grows.
    source
        .append_batch(runtime.client(), source_batch(&[(4, "a", 30, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_sum_flag(runtime, &target).await,
        vec![("a".to_string(), 60, 1), ("b".to_string(), 5, 0),]
    );

    // Removing one of the two matching rows keeps the maximum at 1.
    source
        .append_batch(runtime.client(), source_batch(&[(2, "a", 20, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_sum_flag(runtime, &target).await,
        vec![("a".to_string(), 40, 1), ("b".to_string(), 5, 0),]
    );

    // Removing the last matching row drops the maximum back to 0.
    source
        .append_batch(runtime.client(), source_batch(&[(4, "a", 30, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_sum_flag(runtime, &target).await,
        vec![("a".to_string(), 10, 0), ("b".to_string(), 5, 0),]
    );

    // Deleting the last source row of the group removes its state/MV row.
    source
        .append_batch(runtime.client(), source_batch(&[(1, "a", 10, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_sum_flag(runtime, &target).await,
        vec![("b".to_string(), 5, 0)]
    );
}

#[test_log::test(tokio::test)]
async fn current_timestamp_does_not_change_the_definition_identity() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = source(&runtime, &dir, &format!("ts_src_{suffix}")).await;
    let target = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_sink_ts_mv_{suffix}"),
                table_path(&dir, "mv"),
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("sum_v", DataType::Int64, true),
                    Field::new(
                        "process_time",
                        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                        true,
                    ),
                    Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
                    Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
                ])),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT k, SUM(v) AS sum_v, CURRENT_TIMESTAMP AS process_time \
         FROM {} GROUP BY k",
        target.table_name, source.table_name
    );
    let runtime = executor.runtime();
    source
        .append_batch(runtime.client(), source_batch(&[(1, "a", 10, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");

    source
        .append_batch(runtime.client(), source_batch(&[(1, "a", 12, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(read_k_sum(executor.runtime(), &target).await, vec![(1, 12)]);
}

async fn read_k_sum(runtime: &IvmRuntime, target: &IvmTable) -> Vec<(i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(target)))
        .unwrap();
    let batches = context
        .sql("select k, sum_v from mv order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), sums.value(row)));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn target_partition_may_move_without_being_in_the_primary_key() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_sink_part_src_{suffix}"),
                table_path(&dir, "src"),
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("stat", DataType::Int64, false),
                    Field::new("v", DataType::Int64, false),
                    Field::new(CHANGE, DataType::Utf8, false),
                ])),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE),
        )
        .await
        .unwrap();
    let target = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_sink_part_mv_{suffix}"),
                table_path(&dir, "mv"),
                Arc::new(Schema::new(vec![
                    Field::new("k", DataType::Int64, false),
                    Field::new("stat", DataType::Int64, false),
                    Field::new("sum_v", DataType::Int64, true),
                    Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
                    Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
                ])),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_range_partition_columns(vec!["stat".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT k, stat, SUM(v) AS sum_v FROM {} GROUP BY k, stat",
        target.table_name, source.table_name
    );
    let source_batch = |rows: &[(i64, i64, i64, &str)]| {
        RecordBatch::try_new(
            source.schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
            ],
        )
        .unwrap()
    };
    let runtime = executor.runtime();
    source
        .append_batch(runtime.client(), source_batch(&[(1, 100, 10, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_partitioned_target(executor.runtime(), &target).await,
        vec![(1, 100, 10)]
    );

    // The same key moves to another partition: the sink deletes the old
    // partition row and inserts the new one.
    source
        .append_batch(runtime.client(), source_batch(&[(1, 200, 20, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_partitioned_target(executor.runtime(), &target).await,
        vec![(1, 200, 20)]
    );

    source
        .append_batch(runtime.client(), source_batch(&[(1, 200, 20, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert!(
        read_partitioned_target(executor.runtime(), &target)
            .await
            .is_empty()
    );
}

async fn read_partitioned_target(
    runtime: &IvmRuntime,
    target: &IvmTable,
) -> Vec<(i64, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(target)))
        .unwrap();
    let batches = context
        .sql("select k, stat, sum_v from mv order by k, stat")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let stats = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let amounts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), stats.value(row), amounts.value(row)));
        }
    }
    rows
}
