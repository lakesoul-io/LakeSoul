// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The dedicated incremental SQL entry end to end: bootstrap, incremental
//! maintenance, definition-change rebuilds (with state table replacement) and
//! `INSERT OVERWRITE`.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmExecutionAction, IvmRuntime, IvmSqlExecutor, IvmTable,
    IvmTableOptions, MinMaxKind, StateRole, lookup_join_view_schema_for,
    min_max_mv_schema_for, sum_count_mv_schema_for,
};
use lakesoul_metadata_proto::entity::{CommitOp, MetaInfo, PartitionInfo};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

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

fn batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// The value column of every group currently written in the MV.
async fn read_value(
    runtime: &IvmRuntime,
    mv: &IvmTable,
    column: &str,
) -> HashMap<String, i64> {
    let context = SessionContext::new();
    let batches = mv.read_current(runtime.client()).await.unwrap();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(mv.schema.clone(), vec![batches]).unwrap());
    context.register_table("mv", table).unwrap();
    let frame = context
        .sql(&format!(
            "select g, {column} from mv where \"{IVM_ROW_KINDS_COLUMN}\" = 'insert'"
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
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            state.insert(groups.value(row).to_string(), values.value(row));
        }
    }
    state
}

async fn state_table_id(runtime: &IvmRuntime, view_id: &str) -> Option<String> {
    runtime
        .metadata()
        .list_states(view_id)
        .await
        .unwrap()
        .into_iter()
        .find(|state| state.role == StateRole::State)
        .map(|state| state.table_id)
}

#[test_log::test(tokio::test)]
async fn insert_into_bootstraps_then_increments() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_sum_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_sum_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let sql = format!(
        "INSERT INTO m3_sum_mv_{suffix} \
         SELECT g, SUM(v), COUNT(*) FROM m3_sum_src_{suffix} GROUP BY g"
    );

    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    source
        .append_batch(
            runtime.client(),
            batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 20, "insert"),
                (3, "b", 5, "insert"),
            ]),
        )
        .await
        .unwrap();

    let execution = executor.execute(&sql).await.unwrap();
    assert_eq!(execution.action, IvmExecutionAction::Bootstrap);
    assert_eq!(
        read_value(runtime, &mv, "sum_v").await,
        HashMap::from([("a".to_string(), 30), ("b".to_string(), 5)])
    );

    // One update, one delete and one new group; only the delta is applied.
    source
        .append_batch(
            runtime.client(),
            batch(&[
                (1, "a", 11, "insert"),
                (3, "b", 5, "delete"),
                (4, "c", 7, "insert"),
            ]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&sql).await.unwrap();
    assert_eq!(execution.action, IvmExecutionAction::Incremental);
    assert_eq!(
        read_value(runtime, &mv, "sum_v").await,
        HashMap::from([("a".to_string(), 31), ("c".to_string(), 7)])
    );

    assert_eq!(
        runtime
            .metadata()
            .view_definition(&mv.table_id)
            .await
            .unwrap(),
        Some((execution.definition_hash, sql))
    );
}

#[test_log::test(tokio::test)]
async fn definition_change_rebuilds_and_replaces_state() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_mm_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_mm_mv_{suffix}"),
                table_path(&dir, "mv"),
                min_max_mv_schema_for(
                    &source.schema,
                    &["g".to_string()],
                    "v",
                    MinMaxKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();

    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    source
        .append_batch(
            runtime.client(),
            batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 3, "insert"),
                (3, "b", 7, "insert"),
            ]),
        )
        .await
        .unwrap();

    let min_sql = format!(
        "INSERT INTO m3_mm_mv_{suffix} \
         SELECT g, MIN(v) FROM m3_mm_src_{suffix} GROUP BY g"
    );
    let execution = executor.execute(&min_sql).await.unwrap();
    assert_eq!(execution.action, IvmExecutionAction::Bootstrap);
    assert_eq!(
        read_value(runtime, &mv, "value").await,
        HashMap::from([("a".to_string(), 3), ("b".to_string(), 7)])
    );
    let old_state = state_table_id(runtime, &mv.table_id).await.unwrap();

    // The same MV schema with a different aggregate: the view is rebuilt and
    // its generated state table is replaced.
    let max_sql = format!(
        "INSERT INTO m3_mm_mv_{suffix} \
         SELECT g, MAX(v) FROM m3_mm_src_{suffix} GROUP BY g"
    );
    let execution = executor.execute(&max_sql).await.unwrap();
    assert_eq!(execution.action, IvmExecutionAction::Rebuild);
    assert_eq!(
        read_value(runtime, &mv, "value").await,
        HashMap::from([("a".to_string(), 10), ("b".to_string(), 7)])
    );
    let new_state = state_table_id(runtime, &mv.table_id).await.unwrap();
    assert_ne!(old_state, new_state, "the state table was replaced");
    assert_eq!(
        runtime
            .metadata()
            .list_states(&mv.table_id)
            .await
            .unwrap()
            .iter()
            .filter(|state| state.role == StateRole::State)
            .count(),
        1
    );
    assert_eq!(
        runtime
            .metadata()
            .view_definition(&mv.table_id)
            .await
            .unwrap(),
        Some((execution.definition_hash, max_sql))
    );
}

#[test_log::test(tokio::test)]
async fn insert_overwrite_recomputes_and_resumes_incremental() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_ow_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_ow_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let insert_sql = format!(
        "INSERT INTO m3_ow_mv_{suffix} \
         SELECT g, SUM(v), COUNT(*) FROM m3_ow_src_{suffix} GROUP BY g"
    );
    let overwrite_sql = format!(
        "INSERT OVERWRITE m3_ow_mv_{suffix} \
         SELECT g, SUM(v), COUNT(*) FROM m3_ow_src_{suffix} GROUP BY g"
    );

    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    source
        .append_batch(runtime.client(), batch(&[(1, "a", 10, "insert")]))
        .await
        .unwrap();
    executor.execute(&insert_sql).await.unwrap();

    // The view is stale until the overwrite recomputes it.
    source
        .append_batch(
            runtime.client(),
            batch(&[(1, "a", 25, "insert"), (2, "b", 4, "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&overwrite_sql).await.unwrap();
    assert_eq!(execution.action, IvmExecutionAction::Overwrite);
    assert_eq!(
        read_value(runtime, &mv, "sum_v").await,
        HashMap::from([("a".to_string(), 25), ("b".to_string(), 4)])
    );

    // The cursors were reset: the next INSERT INTO applies only the new delta.
    // A new key: a changed key would be an upsert and retract its old value.
    source
        .append_batch(runtime.client(), batch(&[(5, "b", 6, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&insert_sql).await.unwrap();
    assert_eq!(execution.action, IvmExecutionAction::Incremental);
    assert_eq!(
        read_value(runtime, &mv, "sum_v").await,
        HashMap::from([("a".to_string(), 25), ("b".to_string(), 10)])
    );
}

#[test_log::test(tokio::test)]
async fn rejects_bad_statements_and_mismatched_targets() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_bad_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_bad_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    // A target whose schema belongs to a different grouping.
    let other = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_bad_other_{suffix}"),
                table_path(&dir, "other"),
                sum_count_mv_schema_for(&source.schema, &["k".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();

    let executor = IvmSqlExecutor::new(runtime);
    let statements =
        format!("SELECT g, SUM(v), COUNT(*) FROM m3_bad_src_{suffix} GROUP BY g",);
    assert!(executor.execute("SELECT 1").await.is_err());
    assert!(executor.execute("SELECT 1; SELECT 2").await.is_err());
    assert!(
        executor
            .execute(&format!("INSERT INTO m3_bad_mv_{suffix} VALUES (1)"))
            .await
            .is_err()
    );
    // AVG is not maintained incrementally.
    assert!(
        executor
            .execute(&format!(
                "INSERT INTO m3_bad_mv_{suffix} SELECT g, AVG(v) FROM m3_bad_src_{suffix} GROUP BY g"
            ))
            .await
            .is_err()
    );
    // Explicit column lists are not supported.
    assert!(
        executor
            .execute(&format!(
                "INSERT INTO m3_bad_mv_{suffix} (g, sum_v) {statements}"
            ))
            .await
            .is_err()
    );
    // The target schema must match the definition.
    assert!(
        executor
            .execute(&format!(
                "INSERT INTO m3_bad_other_{suffix} SELECT g, SUM(v), COUNT(*) FROM m3_bad_src_{suffix} GROUP BY g"
            ))
            .await
            .is_err()
    );
    let _ = mv;
    let _ = other;
}

#[tokio::test]
async fn rejects_keyless_target_for_keyed_views() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("m3_keyless_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let keyless = runtime
        .create_table(IvmTableOptions::new(
            format!("m3_keyless_mv_{suffix}"),
            table_path(&dir, "mv"),
            sum_count_mv_schema_for(&source.schema, &["g".to_string()], Some("v"))
                .unwrap(),
        ))
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    // A statement over keyed tables needs an MV primary key.
    let error = executor
        .execute(&format!(
            "INSERT INTO m3_keyless_mv_{suffix} \
             SELECT g, SUM(v), COUNT(*) FROM m3_keyless_src_{suffix} GROUP BY g"
        ))
        .await
        .unwrap_err();
    assert!(
        format!("{error}").contains("needs a primary key"),
        "{error}"
    );
    drop(keyless);

    // A global aggregate rewrites its single row and needs no key.
    executor
        .runtime()
        .create_table(IvmTableOptions::new(
            format!("m3_keyless_global_{suffix}"),
            table_path(&dir, "global"),
            sum_count_mv_schema_for(&source.schema, &Vec::<String>::new(), Some("v"))
                .unwrap(),
        ))
        .await
        .unwrap();
    executor
        .execute(&format!(
            "INSERT INTO m3_keyless_global_{suffix} \
             SELECT SUM(v), COUNT(*) FROM m3_keyless_src_{suffix}"
        ))
        .await
        .unwrap();

    // A statement over an append-only source is rejected: incremental views
    // need keyed sources.
    let append_source = executor
        .runtime()
        .create_table(
            IvmTableOptions::new(
                format!("m3_keyless_app_{suffix}"),
                table_path(&dir, "app"),
                schema(),
            )
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    executor
        .runtime()
        .create_table(IvmTableOptions::new(
            format!("m3_keyless_appmv_{suffix}"),
            table_path(&dir, "appmv"),
            sum_count_mv_schema_for(&append_source.schema, &["g".to_string()], Some("v"))
                .unwrap(),
        ))
        .await
        .unwrap();
    let error = executor
        .execute(&format!(
            "INSERT INTO m3_keyless_appmv_{suffix} \
             SELECT g, SUM(v), COUNT(*) FROM m3_keyless_app_{suffix} GROUP BY g"
        ))
        .await
        .unwrap_err();
    assert!(format!("{error}").contains("has no primary key"), "{error}");
}

/// An update commit on the source (a window that needs a snapshot) makes the
/// next statement rebuild the view instead of failing.
#[test_log::test(tokio::test)]
async fn an_update_commit_on_the_source_rebuilds_the_view() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_upd_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_upd_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_upd_mv_{suffix} \
         SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_upd_src_{suffix} GROUP BY g"
    );
    source
        .append_batch(
            executor.runtime().client(),
            batch(&[(1, "a", 10, "insert"), (2, "a", 5, "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(read_value(runtime, &mv, "sum_v").await.get("a"), Some(&15));

    // An update commit carries no new files but needs a snapshot: the window
    // is no longer incremental.
    let partitions = runtime
        .client()
        .get_all_partition_info(&source.table_id)
        .await
        .unwrap();
    let partition = partitions.first().expect("the source has a partition");
    let table_info = runtime
        .client()
        .get_table_info_by_table_id(&source.table_id)
        .await
        .unwrap()
        .expect("the source table is registered");
    runtime
        .client()
        .commit_data(
            MetaInfo {
                table_info: Some(table_info),
                list_partition: vec![PartitionInfo {
                    table_id: source.table_id.clone(),
                    partition_desc: partition.partition_desc.clone(),
                    commit_op: CommitOp::UpdateCommit.into(),
                    snapshot: partition.snapshot.clone(),
                    ..Default::default()
                }],
                read_partition_info: vec![PartitionInfo {
                    table_id: source.table_id.clone(),
                    partition_desc: partition.partition_desc.clone(),
                    version: partition.version,
                    ..Default::default()
                }],
            },
            CommitOp::UpdateCommit,
        )
        .await
        .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "rebuild");
    assert_eq!(read_value(runtime, &mv, "sum_v").await.get("a"), Some(&15));

    // The rebuilt view refreshes incrementally again.
    source
        .append_batch(runtime.client(), batch(&[(3, "a", 7, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(read_value(runtime, &mv, "sum_v").await.get("a"), Some(&22));
}

/// A lookup join whose left primary key is also the join key: the key column
/// is materialized once (not as key and primary key), and the view follows
/// both sides.
#[test_log::test(tokio::test)]
async fn a_lookup_join_materializes_a_key_that_is_also_the_primary_key() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lk_l_{suffix}"),
                table_path(&dir, "left"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lk_r_{suffix}"),
                table_path(&dir, "right"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lk_mv_{suffix}"),
                table_path(&dir, "mv"),
                lookup_join_view_schema_for(
                    &schema(),
                    &schema(),
                    &["k".to_string()],
                    &["k".to_string()],
                    &["k".to_string()],
                    "g",
                    "v",
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_allow_fallback(false),
        )
        .await
        .unwrap();
    left.append_batch(
        runtime.client(),
        batch(&[(1, "a", 0, "insert"), (2, "b", 0, "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(runtime.client(), batch(&[(1, "r", 10, "insert")]))
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_lk_mv_{suffix} SELECT a.g, b.v \
         FROM ivm_lk_l_{suffix} a LEFT JOIN ivm_lk_r_{suffix} b ON b.k = a.k"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_lookup_pairs(runtime, &mv).await,
        vec![(1, "a".to_string(), Some(10)), (2, "b".to_string(), None),]
    );

    // The right payload updates and then disappears.
    right
        .append_batch(runtime.client(), batch(&[(1, "r", 20, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_lookup_pairs(runtime, &mv).await,
        vec![(1, "a".to_string(), Some(20)), (2, "b".to_string(), None),]
    );
    right
        .append_batch(runtime.client(), batch(&[(1, "r", 20, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_lookup_pairs(runtime, &mv).await,
        vec![(1, "a".to_string(), None), (2, "b".to_string(), None),]
    );
}

/// The `(k, left_value, right_value)` rows of a lookup MV ordered by `k`.
async fn read_lookup_pairs(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(i64, String, Option<i64>)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select k, left_value, right_value from mv order by k")
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
        let lefts = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rights = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row),
                lefts.value(row).to_string(),
                (!rights.is_null(row)).then(|| rights.value(row)),
            ));
        }
    }
    rows
}

/// The `(k, g, sum_v, count_v)` rows of a count MV, ordered by `k`.
async fn read_group_rows(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(i64, String, i64, i64)> {
    let context = SessionContext::new();
    let batches = mv.read_current(runtime.client()).await.unwrap();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(mv.schema.clone(), vec![batches]).unwrap());
    context.register_table("mv", table).unwrap();
    let batches = context
        .sql(&format!(
            "select k, g, sum_v, count_v from mv where \"{IVM_ROW_KINDS_COLUMN}\" = 'insert' order by k"
        ))
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
        let groups = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row),
                groups.value(row).to_string(),
                sums.value(row),
                counts.value(row),
            ));
        }
    }
    rows
}

/// A group that migrates under a **coarser materialized-view key** keeps its
/// row: the refresh batch orders deletes before inserts by the MV's primary
/// keys, not by the group keys (whose payloads order the two rows the other
/// way around).
#[test_log::test(tokio::test)]
async fn a_group_migration_under_a_coarser_mv_key_keeps_its_row() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_gm_src_{suffix}"),
                table_path(&dir, "src"),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_gm_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(
                    &source.schema,
                    &["k".to_string(), "g".to_string()],
                    Some("v"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_gm_mv_{suffix} \
         SELECT k, g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_gm_src_{suffix} GROUP BY k, g"
    );
    source
        .append_batch(
            executor.runtime().client(),
            batch(&[(1, "a", 10, "insert"), (2, "q", 20, "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_group_rows(runtime, &mv).await,
        vec![(1, "a".to_string(), 10, 1), (2, "q".to_string(), 20, 1)]
    );

    // The same source row moves from `q` to `p`: the group (2, q) is deleted
    // and (2, p) inserted, both under the view key `k = 2`.
    source
        .append_batch(runtime.client(), batch(&[(2, "p", 20, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_group_rows(runtime, &mv).await,
        vec![(1, "a".to_string(), 10, 1), (2, "p".to_string(), 20, 1)]
    );
}
