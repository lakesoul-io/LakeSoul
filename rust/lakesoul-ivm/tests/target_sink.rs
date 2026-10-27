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
    IvmTableOptions, ViewSpec,
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
