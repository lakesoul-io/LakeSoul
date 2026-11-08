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
    IvmRuntime, IvmRuntimeConfig, IvmTableOptions, RowView, row_mv_schema_for,
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
        Ok(SessionContext::new_with_state(state))
    }
}

#[test_log::test(tokio::test)]
async fn a_worker_sink_writes_files_and_the_coordinator_commits_them() {
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
