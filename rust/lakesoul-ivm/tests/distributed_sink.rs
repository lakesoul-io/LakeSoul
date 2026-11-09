// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "distributed")]

//! A refresh whose output is written by the worker sink and committed by the
//! coordinator.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use lakesoul_ivm::config::IvmSessionFactory;
use lakesoul_ivm::{
    IvmRuntime, IvmRuntimeConfig, IvmSqlExecutor, IvmTableOptions, RowView,
    row_mv_schema_for,
};
use std::sync::OnceLock;
use tempfile::tempdir;

/// The crash test sets a process-wide abort switch; keep this binary's tests
/// from observing it while another refresh is running.
fn test_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[derive(Clone)]
struct SinkFactory {
    session: SessionConfig,
    runtime: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
}

impl IvmSessionFactory for SinkFactory {
    fn create(&self) -> lakesoul_ivm::Result<SessionContext> {
        let state = SessionStateBuilder::new()
            .with_config(self.session.clone())
            .with_runtime_env(Arc::clone(&self.runtime))
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(
                lakesoul_ivm::distributed::IvmSinkRule,
            ))
            .build();
        Ok(SessionContext::new_with_state(state))
    }
}

#[test_log::test(tokio::test)]
async fn a_worker_sink_writes_files_and_the_coordinator_commits_them() {
    let _guard = test_lock().lock().await;
    let base = IvmRuntimeConfig::default();
    let factory = SinkFactory {
        session: base.session.clone(),
        runtime: Arc::clone(&base.runtime),
    };
    let runtime =
        IvmRuntime::from_env_with_config(base.with_session_factory(Arc::new(factory)))
            .await
            .unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_sink_src_{suffix}"),
                format!("file://{}", dir.path().join("src").display()),
                schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_sink_mv_{suffix}"),
                format!("file://{}", dir.path().join("mv").display()),
                row_mv_schema_for(&source.schema, &[]).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(StringArray::from(vec!["insert", "insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let view = RowView::new(format!("row_sink_{suffix}"), source, mv.clone());
    runtime.refresh_row(&view).await.unwrap();
    let rows = mv
        .read_current(runtime.client())
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(rows, 3);
}

/// A coordinator crash after the MV commit but before the epoch mark is the
/// pending-window path: the retry must complete the existing epoch without
/// writing the rows again.
#[test_log::test(tokio::test)]
async fn a_pending_epoch_is_completed_without_duplicate_rows() {
    use lakesoul_io::constant::DEFAULT_PARTITION_DESC;

    let base = IvmRuntimeConfig::default();
    let factory = SinkFactory {
        session: base.session.clone(),
        runtime: Arc::clone(&base.runtime),
    };
    let runtime =
        IvmRuntime::from_env_with_config(base.with_session_factory(Arc::new(factory)))
            .await
            .unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_epoch_src_{suffix}"),
                format!("file://{}", dir.path().join("src").display()),
                schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_epoch_mv_{suffix}"),
                format!("file://{}", dir.path().join("mv").display()),
                row_mv_schema_for(&source.schema, &[]).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(StringArray::from(vec!["insert", "insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let view = RowView::new(format!("row_epoch_{suffix}"), source, mv.clone());
    let epoch = runtime.refresh_row(&view).await.unwrap().unwrap();
    let generation = runtime
        .metadata()
        .view_generation(&view.view_id)
        .await
        .unwrap();
    let record = runtime
        .metadata()
        .list_committed_epochs(&view.view_id, generation)
        .await
        .unwrap()
        .into_iter()
        .find(|record| record.epoch == epoch)
        .unwrap();
    // Rewind the source cursor and flip the epoch back to pending, as a crash
    // between the MV commit and the epoch mark would leave it.
    runtime
        .metadata()
        .upsert_cursor(
            &view.view_id,
            &view.source.table_id,
            DEFAULT_PARTITION_DESC,
            -1,
            0,
        )
        .await
        .unwrap();
    runtime
        .metadata()
        .set_epoch_pending(&view.view_id, record.generation, record.epoch)
        .await
        .unwrap();
    let replayed = runtime.refresh_row(&view).await.unwrap();
    assert_eq!(replayed, Some(epoch));
    assert_eq!(
        mv.read_current(runtime.client())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        3,
        "completing the pending epoch must not duplicate rows"
    );
    assert_eq!(
        runtime
            .metadata()
            .list_committed_epochs(&view.view_id, generation)
            .await
            .unwrap()
            .len(),
        1
    );
}

/// A coordinator crash after the staged-file plan is persisted but before the
/// target commit must resume that commit on the next attempt, without running
/// the refresh or duplicating rows.
#[test_log::test(tokio::test)]
async fn a_coordinator_crash_after_the_commit_plan_resumes_the_target_commit() {
    let _guard = test_lock().lock().await;
    let base = IvmRuntimeConfig::default();
    let factory = SinkFactory {
        session: base.session.clone(),
        runtime: Arc::clone(&base.runtime),
    };
    let runtime =
        IvmRuntime::from_env_with_config(base.with_session_factory(Arc::new(factory)))
            .await
            .unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_crash_src_{suffix}"),
                format!("file://{}", dir.path().join("src").display()),
                schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_crash_mv_{suffix}"),
                format!("file://{}", dir.path().join("mv").display()),
                row_mv_schema_for(&source.schema, &[]).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(StringArray::from(vec!["insert", "insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let context = SessionContext::new();
    context
        .register_table(
            source.table_name.as_str(),
            Arc::new(runtime.table_provider(&source)),
        )
        .unwrap();
    context
        .register_table(
            mv.table_name.as_str(),
            Arc::new(runtime.table_provider(&mv)),
        )
        .unwrap();
    let sql = format!(
        "INSERT INTO {} SELECT * FROM {}",
        mv.table_name, source.table_name
    );
    let executor =
        IvmSqlExecutor::with_shared_runtime(Arc::new(runtime)).with_session(context);
    // Bootstrap the view first; the abort switch applies to the incremental
    // window, which is the one that stages a target write.
    executor.execute(&sql).await.unwrap();
    assert_eq!(
        mv.read_current(executor.runtime().client())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        3
    );
    source
        .append_batch(
            executor.runtime().client(),
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![4])),
                    Arc::new(StringArray::from(vec!["d"])),
                    Arc::new(StringArray::from(vec!["insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    // SAFETY: the test lock serializes the process-wide switch.
    unsafe { std::env::set_var("LAKESOUL_IVM_TEST_ABORT_AFTER_COMMIT_PLAN", "1") };
    let error = executor.execute(&sql).await.unwrap_err();
    // SAFETY: see above; no other refresh is running while the lock is held.
    unsafe { std::env::remove_var("LAKESOUL_IVM_TEST_ABORT_AFTER_COMMIT_PLAN") };
    assert!(
        error.to_string().contains("after commit plan"),
        "unexpected error: {error}"
    );
    assert_eq!(
        mv.read_current(executor.runtime().client())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        3,
        "the aborted coordinator must not publish the incremental row"
    );
    let replayed = executor.execute(&sql).await.unwrap();
    assert!(replayed.epoch.is_some());
    assert_eq!(
        mv.read_current(executor.runtime().client())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        4,
        "the pending plan must commit exactly the staged target files"
    );
}

/// Kill the coordinator task (not just return an error) after it persisted the
/// commit plan.  The next attempt must resume the staged target commit.
#[test_log::test(tokio::test)]
async fn a_killed_coordinator_task_resumes_the_persisted_commit_plan() {
    let _guard = test_lock().lock().await;
    let base = IvmRuntimeConfig::default();
    let factory = SinkFactory {
        session: base.session.clone(),
        runtime: Arc::clone(&base.runtime),
    };
    let runtime =
        IvmRuntime::from_env_with_config(base.with_session_factory(Arc::new(factory)))
            .await
            .unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
        Field::new("op", DataType::Utf8, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_kill_src_{suffix}"),
                format!("file://{}", dir.path().join("src").display()),
                schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_kill_mv_{suffix}"),
                format!("file://{}", dir.path().join("mv").display()),
                row_mv_schema_for(&source.schema, &[]).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(StringArray::from(vec!["insert", "insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let context = SessionContext::new();
    context
        .register_table(
            source.table_name.as_str(),
            Arc::new(runtime.table_provider(&source)),
        )
        .unwrap();
    context
        .register_table(
            mv.table_name.as_str(),
            Arc::new(runtime.table_provider(&mv)),
        )
        .unwrap();
    let sql = format!(
        "INSERT INTO {} SELECT * FROM {}",
        mv.table_name, source.table_name
    );
    let executor =
        IvmSqlExecutor::with_shared_runtime(Arc::new(runtime)).with_session(context);
    executor.execute(&sql).await.unwrap();
    source
        .append_batch(
            executor.runtime().client(),
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![4])),
                    Arc::new(StringArray::from(vec!["d"])),
                    Arc::new(StringArray::from(vec!["insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    // SAFETY: the test lock serializes the process-wide switch.
    unsafe { std::env::set_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_COMMIT_PLAN", "1") };
    let executor = Arc::new(executor);
    let sql_for_task = sql.clone();
    let task_executor = Arc::clone(&executor);
    let handle = tokio::spawn(async move { task_executor.execute(&sql_for_task).await });
    let view_id = mv.table_id.clone();
    let generation = executor
        .runtime()
        .metadata()
        .view_generation(&view_id)
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let pending = executor
            .runtime()
            .metadata()
            .list_pending_epochs(&view_id, generation)
            .await
            .unwrap();
        if pending.iter().any(|record| !record.commit_plan.is_empty()) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the refresh never persisted a commit plan"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(
        mv.read_current(executor.runtime().client())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        3,
        "the paused task must not have committed the incremental row"
    );
    handle.abort();
    let _ = handle.await;
    // SAFETY: the paused task is gone and the lock keeps other tests out.
    unsafe { std::env::remove_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_COMMIT_PLAN") };
    executor.execute(&sql).await.unwrap();
    assert_eq!(
        mv.read_current(executor.runtime().client())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        4,
        "the persisted plan must commit the row after the task kill"
    );
}

/// A stateful view persists its auxiliary state before the target.  A crash
/// after the plan recorded the target commit (but before publishing it) must
/// resume the target commit, keeping the state the same attempt already
/// committed, instead of rebuilding the view.
#[test_log::test(tokio::test)]
async fn a_stateful_view_resumes_its_commit_plan_without_a_rebuild() {
    use lakesoul_ivm::{MinMaxKind, StateRole, min_max_mv_schema_for};

    let _guard = test_lock().lock().await;
    let base = IvmRuntimeConfig::default();
    let factory = SinkFactory {
        session: base.session.clone(),
        runtime: Arc::clone(&base.runtime),
    };
    let runtime =
        IvmRuntime::from_env_with_config(base.with_session_factory(Arc::new(factory)))
            .await
            .unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_mm_src_{suffix}"),
                format!("file://{}", dir.path().join("src").display()),
                schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_mm_mv_{suffix}"),
                format!("file://{}", dir.path().join("mv").display()),
                min_max_mv_schema_for(
                    &source.schema,
                    &["k".to_string()],
                    "v",
                    MinMaxKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(Int64Array::from(vec![10, 20])),
                    Arc::new(StringArray::from(vec!["insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let context = SessionContext::new();
    context
        .register_table(
            source.table_name.as_str(),
            Arc::new(runtime.table_provider(&source)),
        )
        .unwrap();
    context
        .register_table(
            mv.table_name.as_str(),
            Arc::new(runtime.table_provider(&mv)),
        )
        .unwrap();
    let sql = format!(
        "INSERT INTO {} SELECT k, MIN(v) FROM {} GROUP BY k",
        mv.table_name, source.table_name
    );
    let executor =
        IvmSqlExecutor::with_shared_runtime(Arc::new(runtime)).with_session(context);
    // Bootstrap first: the abort switch only applies to the incremental
    // window, which writes state and target through the commit plan.
    let bootstrap = executor.execute(&sql).await.unwrap();
    assert_eq!(bootstrap.epoch, Some(1));
    source
        .append_batch(
            executor.runtime().client(),
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![5])),
                    Arc::new(StringArray::from(vec!["insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    // SAFETY: the test lock serializes the process-wide switch.
    unsafe { std::env::set_var("LAKESOUL_IVM_TEST_ABORT_AFTER_COMMIT_PLAN", "1") };
    let error = executor.execute(&sql).await.unwrap_err();
    // SAFETY: no other refresh runs while the lock is held.
    unsafe { std::env::remove_var("LAKESOUL_IVM_TEST_ABORT_AFTER_COMMIT_PLAN") };
    assert!(
        error.to_string().contains("after commit plan"),
        "unexpected error: {error}"
    );
    let rows_after_abort = mv
        .read_current(executor.runtime().client())
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert_eq!(
        rows_after_abort, 2,
        "the aborted target commit must not publish the incremental min update"
    );
    let view_id = mv.table_id.clone();
    let generation = executor
        .runtime()
        .metadata()
        .view_generation(&view_id)
        .await
        .unwrap();
    let pending = executor
        .runtime()
        .metadata()
        .list_pending_epochs(&view_id, generation)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        pending[0].commit_plan.complete,
        "the target plan entry must mark the plan complete"
    );
    assert_eq!(
        pending[0].commit_plan.entries.len(),
        2,
        "the state write and the target write must both be journaled"
    );
    let replayed = executor.execute(&sql).await.unwrap();
    assert_eq!(replayed.epoch, Some(pending[0].epoch));
    let mut mins = std::collections::HashMap::new();
    for batch in mv.read_current(executor.runtime().client()).await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            mins.insert(keys.value(row), values.value(row));
        }
    }
    assert_eq!(
        mins,
        std::collections::HashMap::from([(1, 5), (2, 20)]),
        "the plan must publish exactly the staged target files"
    );
    let states = executor
        .runtime()
        .metadata()
        .list_states(&view_id)
        .await
        .unwrap();
    let state_info = states
        .iter()
        .find(|state| state.role == StateRole::State)
        .expect("the value-count state must be registered");
    let state = executor
        .runtime()
        .open_table_by_id(&state_info.table_id)
        .await
        .unwrap();
    let state_rows = state
        .read_current(executor.runtime().client())
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();
    assert!(
        state_rows >= 2,
        "the state written by the first attempt must stay, got {state_rows}"
    );
    let replay = executor.execute(&sql).await.unwrap();
    assert_eq!(
        replay.epoch, None,
        "a replay without new source files must not open a window"
    );
}

/// A crash after an auxiliary state write but before the refresh finished
/// leaves an incomplete plan.  Recovery must publish the staged files, replay
/// the window, and produce the affected join rows exactly once.
#[test_log::test(tokio::test)]
async fn an_incomplete_state_plan_replays_without_duplicate_state() {
    use lakesoul_ivm::{
        JoinView, ViewSpec, keyed_join_output_primary_keys, keyed_join_view_schema_for,
    };

    let _guard = test_lock().lock().await;
    let base = IvmRuntimeConfig::default();
    let factory = SinkFactory {
        session: base.session.clone(),
        runtime: Arc::clone(&base.runtime),
    };
    let runtime = Arc::new(
        IvmRuntime::from_env_with_config(base.with_session_factory(Arc::new(factory)))
            .await
            .unwrap(),
    );
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_partial_left_{suffix}"),
                format!("file://{}", dir.path().join("left").display()),
                schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_bucket_columns(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_partial_right_{suffix}"),
                format!("file://{}", dir.path().join("right").display()),
                schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_bucket_columns(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_partial_out_{suffix}"),
                format!("file://{}", dir.path().join("out").display()),
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
        format!("ivm_partial_join_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        "k",
        "v",
        "v",
    );
    runtime.register_join_view(&view).await.unwrap();
    let spec: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&view.view_id)
            .await
            .unwrap()
            .expect("the join view spec must be persisted"),
    )
    .unwrap();
    let batch = |rows: &[(i64, i64)]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    };
    left.append_batch(runtime.client(), batch(&[(1, 10), (2, 20)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), batch(&[(1, 100), (2, 200)]))
        .await
        .unwrap();
    runtime.refresh_spec(&spec).await.unwrap().unwrap();

    left.append_batch(runtime.client(), batch(&[(3, 30)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), batch(&[(3, 300)]))
        .await
        .unwrap();
    // SAFETY: the test lock serializes the process-wide switch.
    unsafe { std::env::set_var("LAKESOUL_IVM_TEST_ABORT_AFTER_STATE_COMMIT", "1") };
    let error = runtime.refresh_spec(&spec).await.unwrap_err();
    // SAFETY: no other refresh runs while the lock is held.
    unsafe { std::env::remove_var("LAKESOUL_IVM_TEST_ABORT_AFTER_STATE_COMMIT") };
    assert!(
        error.to_string().contains("after state commit"),
        "unexpected error: {error}"
    );
    let generation = runtime
        .metadata()
        .view_generation(&view.view_id)
        .await
        .unwrap();
    let pending = runtime
        .metadata()
        .list_pending_epochs(&view.view_id, generation)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        !pending[0].commit_plan.complete,
        "a crash before the target must leave the plan incomplete"
    );
    assert!(
        !pending[0].commit_plan.is_empty(),
        "the committed state write must be journaled"
    );
    assert!(
        pending[0]
            .commit_plan
            .entries
            .iter()
            .all(|entry| entry.table_id != output.table_id),
        "the target must not have been committed"
    );

    runtime.refresh_spec(&spec).await.unwrap().unwrap();
    let mut rows = std::collections::BTreeMap::new();
    for batch in output.read_current(runtime.client()).await.unwrap().iter() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left_values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                rows.insert(
                    keys.value(row),
                    (left_values.value(row), right_values.value(row))
                )
                .is_none(),
                "key {} was emitted more than once",
                keys.value(row)
            );
        }
    }
    assert_eq!(
        rows,
        std::collections::BTreeMap::from([
            (1, (10, 100)),
            (2, (20, 200)),
            (3, (30, 300))
        ])
    );

    // A crash between the state plan checkpoint and the state commit must
    // publish the staged files on recovery, then replay the window.
    left.append_batch(runtime.client(), batch(&[(4, 40)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), batch(&[(4, 400)]))
        .await
        .unwrap();
    // SAFETY: the test lock serializes the process-wide switch.
    unsafe { std::env::set_var("LAKESOUL_IVM_TEST_ABORT_BEFORE_STATE_COMMIT", "1") };
    let error = runtime.refresh_spec(&spec).await.unwrap_err();
    // SAFETY: no other refresh runs while the lock is held.
    unsafe { std::env::remove_var("LAKESOUL_IVM_TEST_ABORT_BEFORE_STATE_COMMIT") };
    assert!(
        error.to_string().contains("before state commit"),
        "unexpected error: {error}"
    );
    let pending = runtime
        .metadata()
        .list_pending_epochs(&view.view_id, generation)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        !pending[0].commit_plan.complete,
        "a crash before the target must leave the plan incomplete"
    );
    assert!(
        pending[0]
            .commit_plan
            .entries
            .iter()
            .any(|entry| !entry.committed),
        "the staged state files must be journaled as uncommitted"
    );
    runtime.refresh_spec(&spec).await.unwrap().unwrap();
    let mut rows = std::collections::BTreeMap::new();
    for batch in output.read_current(runtime.client()).await.unwrap().iter() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left_values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                rows.insert(
                    keys.value(row),
                    (left_values.value(row), right_values.value(row))
                )
                .is_none(),
                "key {} was emitted more than once",
                keys.value(row)
            );
        }
    }
    assert_eq!(
        rows,
        std::collections::BTreeMap::from([
            (1, (10, 100)),
            (2, (20, 200)),
            (3, (30, 300)),
            (4, (40, 400))
        ])
    );

    // A committed join MV with a missing state checkpoint must rebuild its
    // side/pair state by replaying the window with the target suppressed.
    left.append_batch(runtime.client(), batch(&[(5, 50)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), batch(&[(5, 500)]))
        .await
        .unwrap();
    let marker = dir.path().join("join-target-committed");
    // SAFETY: the test lock serializes the process-wide switches.
    unsafe {
        std::env::set_var("LAKESOUL_IVM_TEST_SKIP_STATE_CHECKPOINT", "1");
        std::env::set_var("LAKESOUL_IVM_TEST_SKIP_PLAN_COMPLETE", "1");
        std::env::set_var("LAKESOUL_IVM_TEST_TARGET_COMMIT_MARKER", &marker);
        std::env::set_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_TARGET_COMMIT", "1");
    }
    let handle = {
        let runtime = Arc::clone(&runtime);
        let spec = spec.clone();
        tokio::spawn(async move { runtime.refresh_spec(&spec).await })
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while !marker.exists() {
        assert!(
            !handle.is_finished(),
            "the join refresh finished before the target-commit hook"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the join refresh never reached the target-commit hook"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    handle.abort();
    let _ = handle.await;
    // SAFETY: the paused task is gone and the lock keeps other tests out.
    unsafe {
        std::env::remove_var("LAKESOUL_IVM_TEST_SKIP_STATE_CHECKPOINT");
        std::env::remove_var("LAKESOUL_IVM_TEST_SKIP_PLAN_COMPLETE");
        std::env::remove_var("LAKESOUL_IVM_TEST_TARGET_COMMIT_MARKER");
        std::env::remove_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_TARGET_COMMIT");
    }
    let pending = runtime
        .metadata()
        .list_pending_epochs(&view.view_id, generation)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        pending[0].state_versions.is_empty(),
        "the join attempt must not have checkpointed the state versions"
    );
    assert!(
        !pending[0].commit_plan.complete,
        "the join attempt must leave the plan incomplete"
    );
    runtime.refresh_spec(&spec).await.unwrap().unwrap();
    let mut rows = std::collections::BTreeMap::new();
    for batch in output.read_current(runtime.client()).await.unwrap().iter() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left_values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                rows.insert(
                    keys.value(row),
                    (left_values.value(row), right_values.value(row))
                )
                .is_none(),
                "join key {} was emitted more than once",
                keys.value(row)
            );
        }
    }
    assert_eq!(
        rows,
        std::collections::BTreeMap::from([
            (1, (10, 100)),
            (2, (20, 200)),
            (3, (30, 300)),
            (4, (40, 400)),
            (5, (50, 500))
        ])
    );
    assert!(
        runtime.refresh_spec(&spec).await.unwrap().is_none(),
        "a replay without new source files must not open a window"
    );
}

/// A pending epoch whose MV is committed but whose state checkpoint was lost
/// must rebuild the auxiliary state by replaying the window with the target
/// commit suppressed, not by rebuilding the whole view.
#[test_log::test(tokio::test)]
async fn a_committed_mv_with_a_missing_state_checkpoint_replays_state_only() {
    use lakesoul_ivm::{
        IVM_ROW_KINDS_COLUMN, IVM_VALUE_COLUMN, IVM_VALUE_COUNT_COLUMN, MinMaxKind,
        StateRole, ViewSpec, min_max_mv_schema_for,
    };

    let _guard = test_lock().lock().await;
    let base = IvmRuntimeConfig::default();
    let factory = SinkFactory {
        session: base.session.clone(),
        runtime: Arc::clone(&base.runtime),
    };
    let runtime = Arc::new(
        IvmRuntime::from_env_with_config(base.with_session_factory(Arc::new(factory)))
            .await
            .unwrap(),
    );
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_replay_src_{suffix}"),
                format!("file://{}", dir.path().join("src").display()),
                schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_replay_mv_{suffix}"),
                format!("file://{}", dir.path().join("mv").display()),
                min_max_mv_schema_for(
                    &source.schema,
                    &["k".to_string()],
                    "v",
                    MinMaxKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let batch = |rows: &[(i64, i64)]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(k, _)| *k).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(vec!["insert"; rows.len()])),
            ],
        )
        .unwrap()
    };
    source
        .append_batch(runtime.client(), batch(&[(1, 10), (2, 20)]))
        .await
        .unwrap();
    let context = SessionContext::new();
    context
        .register_table(
            source.table_name.as_str(),
            Arc::new(runtime.table_provider(&source)),
        )
        .unwrap();
    context
        .register_table(
            mv.table_name.as_str(),
            Arc::new(runtime.table_provider(&mv)),
        )
        .unwrap();
    let sql = format!(
        "INSERT INTO {} SELECT k, MIN(v) FROM {} GROUP BY k",
        mv.table_name, source.table_name
    );
    let executor =
        IvmSqlExecutor::with_shared_runtime(Arc::clone(&runtime)).with_session(context);
    executor.execute(&sql).await.unwrap();
    let view_id = mv.table_id.clone();
    let spec: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&view_id)
            .await
            .unwrap()
            .expect("the min/max view spec must be persisted"),
    )
    .unwrap();

    source
        .append_batch(runtime.client(), batch(&[(1, 5)]))
        .await
        .unwrap();
    let marker = dir.path().join("replay-target-committed");
    // SAFETY: the test lock serializes the process-wide switches.
    unsafe {
        std::env::set_var("LAKESOUL_IVM_TEST_SKIP_STATE_CHECKPOINT", "1");
        std::env::set_var("LAKESOUL_IVM_TEST_SKIP_PLAN_COMPLETE", "1");
        std::env::set_var("LAKESOUL_IVM_TEST_TARGET_COMMIT_MARKER", &marker);
        std::env::set_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_TARGET_COMMIT", "1");
    }
    let handle = {
        let runtime = Arc::clone(&runtime);
        let spec = spec.clone();
        tokio::spawn(async move { runtime.refresh_spec(&spec).await })
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while !marker.exists() {
        assert!(
            !handle.is_finished(),
            "the refresh finished before the target-commit hook"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the refresh never reached the target-commit hook"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    handle.abort();
    let _ = handle.await;
    // SAFETY: the paused task is gone and the lock keeps other tests out.
    unsafe {
        std::env::remove_var("LAKESOUL_IVM_TEST_SKIP_STATE_CHECKPOINT");
        std::env::remove_var("LAKESOUL_IVM_TEST_SKIP_PLAN_COMPLETE");
        std::env::remove_var("LAKESOUL_IVM_TEST_TARGET_COMMIT_MARKER");
        std::env::remove_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_TARGET_COMMIT");
    }
    let generation = runtime.metadata().view_generation(&view_id).await.unwrap();
    let pending = runtime
        .metadata()
        .list_pending_epochs(&view_id, generation)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        pending[0].state_versions.is_empty(),
        "the first attempt must not have checkpointed the state versions"
    );
    assert!(
        !pending[0].commit_plan.complete,
        "the first attempt must leave the plan incomplete"
    );

    let epoch = runtime.refresh_spec(&spec).await.unwrap().unwrap();
    assert_eq!(epoch, pending[0].epoch);
    let mut mins = std::collections::BTreeMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap().iter() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(
                mins.insert(keys.value(row), values.value(row)).is_none(),
                "the state-only replay must not duplicate MV rows"
            );
        }
    }
    assert_eq!(mins, std::collections::BTreeMap::from([(1, 5), (2, 20)]));
    let states = runtime.metadata().list_states(&view_id).await.unwrap();
    let state_info = states
        .iter()
        .find(|state| state.role == StateRole::State)
        .expect("the value-count state must be registered");
    let state = runtime
        .open_table_by_id(&state_info.table_id)
        .await
        .unwrap();
    let schema = state.schema.clone();
    let key_index = schema.index_of("k").unwrap();
    let value_index = schema.index_of(IVM_VALUE_COLUMN).unwrap();
    let count_index = schema.index_of(IVM_VALUE_COUNT_COLUMN).unwrap();
    let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap();
    let mut active = std::collections::BTreeMap::new();
    for batch in state.read_current(runtime.client()).await.unwrap().iter() {
        let keys = batch
            .column(key_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(value_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(count_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                active.insert((keys.value(row), values.value(row)), counts.value(row));
            }
        }
    }
    assert_eq!(
        active,
        std::collections::BTreeMap::from([((1, 5), 1), ((2, 20), 1)])
    );

    // The repaired state must still drive a later minimum update.
    source
        .append_batch(runtime.client(), batch(&[(2, 15)]))
        .await
        .unwrap();
    runtime.refresh_spec(&spec).await.unwrap().unwrap();
    let mut mins = std::collections::BTreeMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap().iter() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            mins.insert(keys.value(row), values.value(row));
        }
    }
    assert_eq!(mins, std::collections::BTreeMap::from([(1, 5), (2, 15)]));
}

/// A multi-aggregate SUM view keeps its measures in a value state that is
/// updated by `old_measure + delta`.  Replaying an already-applied window
/// would add the delta twice, so the recovery gate must fall back to a
/// rebuild until the measure merge is made epoch-aware.
#[test_log::test(tokio::test)]
async fn a_multi_agg_value_state_replay_does_not_double_its_measure() {
    use lakesoul_ivm::{
        IVM_GROUP_ROW_COLUMN, MultiAggView, ViewSpec, multi_agg_mv_schema_for,
        multi_agg_state_schema_for,
    };

    let _guard = test_lock().lock().await;
    let runtime = Arc::new(IvmRuntime::from_env().await.unwrap());
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("x", DataType::Int64, false),
    ]));
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ma_src_{suffix}"),
                format!("file://{}", dir.path().join("src").display()),
                schema.clone(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_bucket_columns(vec!["id".to_string()]),
        )
        .await
        .unwrap();
    let group_keys = vec!["k".to_string()];
    let aggregates = vec![
        ("SUM(v)".to_string(), "v_sum".to_string(), DataType::Int64),
        ("SUM(x)".to_string(), "w_sum".to_string(), DataType::Int64),
    ];
    let aggregate_columns = vec![
        ("v_sum".to_string(), DataType::Int64),
        ("w_sum".to_string(), DataType::Int64),
    ];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ma_mv_{suffix}"),
                format!("file://{}", dir.path().join("mv").display()),
                multi_agg_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    &[],
                    &aggregate_columns,
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone())
            .with_bucket_columns(group_keys.clone()),
        )
        .await
        .unwrap();
    let state = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ma_state_{suffix}"),
                format!("file://{}", dir.path().join("state").display()),
                multi_agg_state_schema_for(
                    &source.schema,
                    &group_keys,
                    &aggregate_columns,
                )
                .unwrap(),
            )
            .with_primary_keys(vec![IVM_GROUP_ROW_COLUMN.to_string()])
            .with_bucket_columns(vec![IVM_GROUP_ROW_COLUMN.to_string()]),
        )
        .await
        .unwrap();
    let view = MultiAggView::new(
        format!("ivm_ma_view_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        aggregates,
    )
    .with_state(state);
    runtime.register_multi_agg_view(&view).await.unwrap();
    let spec: ViewSpec = serde_json::from_value(
        runtime
            .metadata()
            .get_view_spec(&view.view_id)
            .await
            .unwrap()
            .expect("the multi-agg spec must be persisted"),
    )
    .unwrap();
    let batch = |rows: &[(i64, i64, i64, i64)]| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, k, _, _)| *k).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, v, _)| *v).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, _, x)| *x).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    };
    source
        .append_batch(
            runtime.client(),
            batch(&[(1, 1, 10, 100), (2, 1, 20, 200), (3, 2, 30, 300)]),
        )
        .await
        .unwrap();
    runtime.refresh_spec(&spec).await.unwrap().unwrap();
    assert_eq!(
        multi_agg_values(&runtime, &mv).await,
        std::collections::BTreeMap::from([(1, (30, 300)), (2, (30, 300))])
    );

    // Update id=1: k=1 changes from (10, 100) to (15, 150).
    source
        .append_batch(runtime.client(), batch(&[(1, 1, 15, 150)]))
        .await
        .unwrap();
    let marker = dir.path().join("multi-agg-target-committed");
    // SAFETY: the test lock serializes the process-wide switches.
    unsafe {
        std::env::set_var("LAKESOUL_IVM_TEST_SKIP_STATE_CHECKPOINT", "1");
        std::env::set_var("LAKESOUL_IVM_TEST_SKIP_PLAN_COMPLETE", "1");
        std::env::set_var("LAKESOUL_IVM_TEST_TARGET_COMMIT_MARKER", &marker);
        std::env::set_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_TARGET_COMMIT", "1");
    }
    let handle = {
        let runtime = Arc::clone(&runtime);
        let spec = spec.clone();
        tokio::spawn(async move { runtime.refresh_spec(&spec).await })
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while !marker.exists() {
        assert!(
            !handle.is_finished(),
            "the refresh finished before the target-commit hook"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the refresh never reached the target-commit hook"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    handle.abort();
    let _ = handle.await;
    // SAFETY: the paused task is gone and the lock keeps other tests out.
    unsafe {
        std::env::remove_var("LAKESOUL_IVM_TEST_SKIP_STATE_CHECKPOINT");
        std::env::remove_var("LAKESOUL_IVM_TEST_SKIP_PLAN_COMPLETE");
        std::env::remove_var("LAKESOUL_IVM_TEST_TARGET_COMMIT_MARKER");
        std::env::remove_var("LAKESOUL_IVM_TEST_PAUSE_AFTER_TARGET_COMMIT");
    }
    let generation = runtime
        .metadata()
        .view_generation(&view.view_id)
        .await
        .unwrap();
    let pending = runtime
        .metadata()
        .list_pending_epochs(&view.view_id, generation)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    assert!(
        pending[0].state_versions.is_empty(),
        "the first attempt must not have checkpointed the state versions"
    );

    // The value state is updated by old_measure + delta.  The epoch guard in
    // IvmStateMergeExec must keep the already-written state row, so the
    // replay resolves the epoch without adding the delta twice.
    let replayed = runtime
        .refresh_spec(&spec)
        .await
        .expect("the multi-agg value state must replay");
    assert!(
        replayed.is_some(),
        "the pending epoch must be resolved by the replay"
    );
    assert_eq!(
        multi_agg_values(&runtime, &mv).await,
        std::collections::BTreeMap::from([(1, (35, 350)), (2, (30, 300))]),
        "the measure delta must not be applied twice"
    );

    // The recovered state must still drive a later incremental window.
    source
        .append_batch(runtime.client(), batch(&[(2, 1, 25, 250)]))
        .await
        .unwrap();
    runtime.refresh_spec(&spec).await.unwrap().unwrap();
    assert_eq!(
        multi_agg_values(&runtime, &mv).await,
        std::collections::BTreeMap::from([(1, (40, 400)), (2, (30, 300))])
    );
}

async fn multi_agg_values(
    runtime: &IvmRuntime,
    mv: &lakesoul_ivm::IvmTable,
) -> std::collections::BTreeMap<i64, (i64, i64)> {
    use lakesoul_ivm::IVM_ROW_KINDS_COLUMN;
    let mut values = std::collections::BTreeMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap().iter() {
        let schema = batch.schema();
        let key_index = schema.index_of("k").unwrap();
        let v_index = schema.index_of("v_sum").unwrap();
        let w_index = schema.index_of("w_sum").unwrap();
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap();
        let keys = batch
            .column(key_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let vs = batch
            .column(v_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ws = batch
            .column(w_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                assert!(
                    values
                        .insert(keys.value(row), (vs.value(row), ws.value(row)))
                        .is_none(),
                    "key {} was emitted more than once",
                    keys.value(row)
                );
            }
        }
    }
    values
}
