// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![cfg(feature = "distributed")]

//! A real coordinator process kill after the staged-file plan is persisted.
//!
//! The parent bootstraps a view and appends one source row.  A child test
//! process runs the incremental refresh, persists the target plan, writes a
//! marker and pauses.  The parent SIGKILLs the child, verifies the target was
//! not committed, and runs the refresh again: the new coordinator process
//! resumes the persisted plan.

use std::process::{Command, Stdio};
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use lakesoul_ivm::config::IvmSessionFactory;
use lakesoul_ivm::{
    IvmOptions, IvmRuntime, IvmRuntimeConfig, IvmSqlExecutor, IvmTableOptions,
    row_mv_schema_for,
};
use tempfile::tempdir;

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
        Ok(lakesoul_ivm::runtime::install_keyed_lookup_planner(
            SessionContext::new_with_state(state),
        ))
    }
}

fn runtime_config() -> IvmRuntimeConfig {
    let base = IvmRuntimeConfig::default().with_options(IvmOptions {
        refresh_lock_ttl_ms: 1_000,
        ..IvmOptions::default()
    });
    let factory = SinkFactory {
        session: base.session.clone(),
        runtime: Arc::clone(&base.runtime),
    };
    base.with_session_factory(Arc::new(factory))
}

async fn table(
    runtime: &IvmRuntime,
    name: &str,
    path: &std::path::Path,
    schema: arrow_schema::SchemaRef,
    primary_keys: Vec<String>,
    cdc_column: Option<String>,
) -> lakesoul_ivm::IvmTable {
    let mut options = IvmTableOptions::new(
        name.to_string(),
        format!("file://{}", path.display()),
        schema,
    )
    .with_primary_keys(primary_keys);
    if let Some(column) = cdc_column {
        options = options.with_cdc_column(column);
    }
    runtime.create_table(options).await.unwrap()
}

async fn executor_for(
    runtime: IvmRuntime,
    source: &lakesoul_ivm::IvmTable,
    mv: &lakesoul_ivm::IvmTable,
) -> IvmSqlExecutor {
    let context = SessionContext::new();
    context
        .register_table(
            source.table_name.as_str(),
            Arc::new(runtime.table_provider(source)),
        )
        .unwrap();
    context
        .register_table(mv.table_name.as_str(), Arc::new(runtime.table_provider(mv)))
        .unwrap();
    IvmSqlExecutor::with_shared_runtime(Arc::new(runtime)).with_session(context)
}

/// The child process: run the incremental refresh until the pause hook.
#[test]
#[ignore = "subprocess helper spawned by the coordinator-kill test"]
fn coordinator_kill_helper() {
    let source_name = std::env::var("LAKESOUL_IVM_KILL_SOURCE").unwrap();
    let mv_name = std::env::var("LAKESOUL_IVM_KILL_MV").unwrap();
    let sql = std::env::var("LAKESOUL_IVM_KILL_SQL").unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let runtime = IvmRuntime::from_env_with_config(runtime_config())
            .await
            .unwrap();
        let source = runtime.open_table(&source_name, "default").await.unwrap();
        let mv = runtime.open_table(&mv_name, "default").await.unwrap();
        let executor = executor_for(runtime, &source, &mv).await;
        let result = executor.execute(&sql).await;
        panic!("helper refresh returned before the pause hook: {result:?}");
    });
}

#[test_log::test(tokio::test)]
async fn a_real_coordinator_process_kill_resumes_the_commit_plan() {
    let runtime = IvmRuntime::from_env_with_config(runtime_config())
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
    let source = table(
        &runtime,
        &format!("ivm_kill_src_{suffix}"),
        &dir.path().join("src"),
        schema.clone(),
        vec!["k".to_string()],
        Some("op".to_string()),
    )
    .await;
    let mv = table(
        &runtime,
        &format!("ivm_kill_mv_{suffix}"),
        &dir.path().join("mv"),
        row_mv_schema_for(&source.schema, &[]).unwrap(),
        vec!["k".to_string()],
        None,
    )
    .await;
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
    let executor = executor_for(runtime, &source, &mv).await;
    let sql = format!(
        "INSERT INTO {} SELECT * FROM {}",
        mv.table_name, source.table_name
    );
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

    let marker = dir.path().join("plan-ready");
    let exe = std::env::current_exe().unwrap();
    let mut child = Command::new(exe)
        .args([
            "--ignored",
            "--exact",
            "coordinator_kill_helper",
            "--test-threads=1",
        ])
        .env("LAKESOUL_IVM_KILL_SOURCE", &source.table_name)
        .env("LAKESOUL_IVM_KILL_MV", &mv.table_name)
        .env("LAKESOUL_IVM_KILL_SQL", &sql)
        .env("LAKESOUL_IVM_TEST_PLAN_MARKER", &marker)
        .env("LAKESOUL_IVM_TEST_PAUSE_AFTER_COMMIT_PLAN", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while !marker.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("helper exited before persisting the plan: {status}");
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "helper did not persist the commit plan in time"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let generation = executor
        .runtime()
        .metadata()
        .view_generation(&mv.table_id)
        .await
        .unwrap();
    let pending = executor
        .runtime()
        .metadata()
        .list_pending_epochs(&mv.table_id, generation)
        .await
        .unwrap();
    assert!(
        pending.iter().any(|record| !record.commit_plan.is_empty()),
        "the child must have persisted a commit plan"
    );
    child.kill().unwrap();
    child.wait().unwrap();
    // The killed process stopped renewing its refresh lease.  Wait for the
    // short test lease to expire before the new coordinator takes over.
    tokio::time::sleep(std::time::Duration::from_millis(1_500)).await;
    assert_eq!(
        mv.read_current(executor.runtime().client())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        3,
        "the killed coordinator must not have committed the incremental row"
    );
    executor.execute(&sql).await.unwrap();
    assert_eq!(
        mv.read_current(executor.runtime().client())
            .await
            .unwrap()
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>(),
        4,
        "the new coordinator must resume the persisted plan"
    );
}
