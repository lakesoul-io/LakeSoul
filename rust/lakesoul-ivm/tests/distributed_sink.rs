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
