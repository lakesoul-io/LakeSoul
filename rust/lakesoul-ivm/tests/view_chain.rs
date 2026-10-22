// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! View-chain orchestration: the dependency graph, the topological refresh
//! order, the retry/rebuild policy and the dirty self-heal.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, RefreshAction,
    keyed_join_output_primary_keys, keyed_join_view_schema_for, sum_count_mv_schema_for,
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
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
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

/// The logical `g -> sum_v` rows of a sum-count MV.
async fn read_sum_mv(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select g, sum_v from mv order by g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((groups.value(row).to_string(), sums.value(row)));
        }
    }
    rows
}

/// Create the keyed source table of a fixture.
async fn create_source(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(name, table_path(dir, name), source_schema())
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

/// A sum-count MV over `source`, summing `value`.
async fn create_sum_mv(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
    source: &IvmTable,
    value: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(
                name,
                table_path(dir, name),
                sum_count_mv_schema_for(&source.schema, &["g".to_string()], Some(value))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap()
}

/// A keyed join MV over the two sum-count MVs.
async fn create_join_mv(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
    left: &IvmTable,
    right: &IvmTable,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(
                name,
                table_path(dir, name),
                keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &["g".to_string()],
                    &["g".to_string()],
                    &["g".to_string()],
                    "sum_v",
                    "sum_v",
                )
                .unwrap(),
            )
            .with_primary_keys(keyed_join_output_primary_keys(
                &["g".to_string()],
                &["g".to_string()],
            )),
        )
        .await
        .unwrap()
}

/// The statement that registers a sum-count view over `value`.
fn sum_statement(target: &str, source: &str, value: &str) -> String {
    format!(
        "INSERT INTO {target} SELECT g, SUM({value}) AS sum_v, COUNT(*) AS count_v \
         FROM {source} GROUP BY g"
    )
}

#[test_log::test(tokio::test)]
async fn chain_rejects_cycles() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    // Two views that read each other's output; both carry the schema of a
    // sum over a sum-count MV (the shape reaches a fixed point after one
    // level).
    let level = sum_count_mv_schema_for(
        &sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v"))
            .unwrap(),
        &["g".to_string()],
        Some("sum_v"),
    )
    .unwrap();
    let mk = |name: String| {
        IvmTableOptions::new(name.clone(), table_path(&dir, &name), level.clone())
            .with_primary_keys(vec!["g".to_string()])
    };
    let left = runtime
        .create_table(mk(format!("ivm_cycle_l_{suffix}")))
        .await
        .unwrap();
    let right = runtime
        .create_table(mk(format!("ivm_cycle_r_{suffix}")))
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    executor
        .execute(&sum_statement(&left.table_name, &right.table_name, "sum_v"))
        .await
        .unwrap();
    executor
        .execute(&sum_statement(&right.table_name, &left.table_name, "sum_v"))
        .await
        .unwrap();

    let error = runtime
        .refresh_view_chain(&left.table_id)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("view chain cycle"), "{error}");
    assert!(error.contains(&left.table_id), "{error}");
    assert!(error.contains(&right.table_id), "{error}");

    // The multi-root entry point sees the same cycle.
    let error = runtime
        .refresh_view_chains(
            &[left.table_id.clone(), right.table_id.clone()],
            &Default::default(),
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("view chain cycle"), "{error}");
}

#[test_log::test(tokio::test)]
async fn chain_marks_a_stuck_view_dirty_and_self_heals() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &format!("ivm_stuck_src_{suffix}")).await;
    let mv1 = create_sum_mv(
        &runtime,
        &dir,
        &format!("ivm_stuck_mv1_{suffix}"),
        &source,
        "v",
    )
    .await;
    let mv2 = create_sum_mv(
        &runtime,
        &dir,
        &format!("ivm_stuck_mv2_{suffix}"),
        &mv1,
        "sum_v",
    )
    .await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    executor
        .execute(&sum_statement(&mv1.table_name, &source.table_name, "v"))
        .await
        .unwrap();
    executor
        .execute(&sum_statement(&mv2.table_name, &mv1.table_name, "sum_v"))
        .await
        .unwrap();

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
    runtime.refresh_view_chain(&mv2.table_id).await.unwrap();
    assert_eq!(
        read_sum_mv(runtime, &mv2).await,
        vec![("a".to_string(), 30), ("b".to_string(), 5)]
    );

    // Break the upstream spec, then change the source: the chain cannot
    // refresh or rebuild mv1, so it must stop before mv2.
    let original = runtime
        .metadata()
        .get_view_spec(&mv1.table_id)
        .await
        .unwrap()
        .unwrap();
    let mut broken = original.clone();
    broken["source_table_id"] = serde_json::json!("table_missing");
    runtime
        .metadata()
        .upsert_view(&mv1.table_id, &broken, 0)
        .await
        .unwrap();
    source
        .append_batch(runtime.client(), source_batch(&[(4, "b", 15, "insert")]))
        .await
        .unwrap();

    let error = runtime
        .refresh_view_chain(&mv2.table_id)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains(&mv1.table_id), "{error}");
    assert_eq!(
        runtime
            .metadata()
            .view_status(&mv1.table_id)
            .await
            .unwrap()
            .as_deref(),
        Some("dirty")
    );
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|view| view.view_id == mv1.table_id)
        .expect("mv1 is registered");
    assert_eq!(info.status, "dirty");
    assert!(info.last_error.is_some());
    // The downstream view did not read the stuck upstream.
    assert_eq!(
        read_sum_mv(runtime, &mv2).await,
        vec![("a".to_string(), 30), ("b".to_string(), 5)]
    );

    // Restoring the spec lets the dirty view self-heal (it is rebuilt before
    // anything else) and the downstream catches up.
    runtime
        .metadata()
        .upsert_view(&mv1.table_id, &original, 0)
        .await
        .unwrap();
    let refreshed = runtime.refresh_view_chain(&mv2.table_id).await.unwrap();
    let upstream = refreshed
        .iter()
        .find(|view| view.view_id == mv1.table_id)
        .expect("mv1 was refreshed");
    assert_eq!(upstream.action, RefreshAction::Rebuilt);
    let downstream = refreshed
        .iter()
        .find(|view| view.view_id == mv2.table_id)
        .expect("mv2 was refreshed");
    assert_eq!(downstream.action, RefreshAction::Incremental);
    assert_eq!(
        read_sum_mv(runtime, &mv2).await,
        vec![("a".to_string(), 30), ("b".to_string(), 20)]
    );
    assert_eq!(
        runtime
            .metadata()
            .view_status(&mv1.table_id)
            .await
            .unwrap()
            .as_deref(),
        Some("active")
    );
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|view| view.view_id == mv1.table_id)
        .unwrap();
    assert!(info.last_error.is_none());
}

#[test_log::test(tokio::test)]
async fn chain_refreshes_a_diamond_once_and_replays_idempotently() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source =
        create_source(&runtime, &dir, &format!("ivm_diamond_src_{suffix}")).await;
    let left = create_sum_mv(
        &runtime,
        &dir,
        &format!("ivm_diamond_l_{suffix}"),
        &source,
        "v",
    )
    .await;
    let right = create_sum_mv(
        &runtime,
        &dir,
        &format!("ivm_diamond_r_{suffix}"),
        &source,
        "v",
    )
    .await;
    let join = create_join_mv(
        &runtime,
        &dir,
        &format!("ivm_diamond_j_{suffix}"),
        &left,
        &right,
    )
    .await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    executor
        .execute(&sum_statement(&left.table_name, &source.table_name, "v"))
        .await
        .unwrap();
    executor
        .execute(&sum_statement(&right.table_name, &source.table_name, "v"))
        .await
        .unwrap();
    executor
        .execute(&format!(
            "INSERT INTO {} SELECT a.g, a.sum_v, b.sum_v FROM {} a JOIN {} b \
             ON a.g = b.g",
            join.table_name, left.table_name, right.table_name
        ))
        .await
        .unwrap();

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
    // One traversal from the join refreshes the shared upstream twice at
    // most once, upstream first.
    let refreshed = runtime
        .refresh_view_chains(std::slice::from_ref(&join.table_id), &Default::default())
        .await
        .unwrap();
    let ids = refreshed
        .iter()
        .map(|view| view.view_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        ids,
        [
            left.table_id.as_str(),
            right.table_id.as_str(),
            join.table_id.as_str()
        ],
        "the traversal follows the roots' dependency order"
    );
    let position = |id: &str| ids.iter().position(|entry| *entry == id).unwrap();
    assert!(position(&left.table_id) < position(&join.table_id));
    assert!(position(&right.table_id) < position(&join.table_id));

    // A replay visits the same order and finds every view unchanged.
    let replayed = runtime
        .refresh_view_chains(std::slice::from_ref(&join.table_id), &Default::default())
        .await
        .unwrap();
    assert_eq!(replayed.len(), 3);
    assert!(
        replayed
            .iter()
            .all(|view| view.action == RefreshAction::Unchanged),
        "{replayed:?}"
    );
}

#[test_log::test(tokio::test)]
async fn executor_refreshes_multiple_upstreams_in_one_traversal() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &format!("ivm_eager_src_{suffix}")).await;
    let left = create_sum_mv(
        &runtime,
        &dir,
        &format!("ivm_eager_l_{suffix}"),
        &source,
        "v",
    )
    .await;
    let right = create_sum_mv(
        &runtime,
        &dir,
        &format!("ivm_eager_r_{suffix}"),
        &source,
        "v",
    )
    .await;
    let join = create_join_mv(
        &runtime,
        &dir,
        &format!("ivm_eager_j_{suffix}"),
        &left,
        &right,
    )
    .await;
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    for (target, from) in [
        (&left.table_name, &source.table_name),
        (&right.table_name, &source.table_name),
    ] {
        executor
            .execute(&sum_statement(target, from, "v"))
            .await
            .unwrap();
    }
    let join_statement = format!(
        "INSERT INTO {} SELECT a.g, a.sum_v, b.sum_v FROM {} a JOIN {} b ON a.g = b.g",
        join.table_name, left.table_name, right.table_name
    );
    executor.execute(&join_statement).await.unwrap();

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
    // The opt-in statement brings both referenced views up to date first.
    drop(executor);
    let eager = IvmSqlExecutor::new(IvmRuntime::from_env().await.unwrap())
        .with_refresh_upstream(true);
    eager.execute(&join_statement).await.unwrap();
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(eager.runtime().table_provider(&join)))
        .unwrap();
    let batches = context
        .sql("select g, left_value, right_value from mv order by g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
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
            rows.push((
                groups.value(row).to_string(),
                left_values.value(row),
                right_values.value(row),
            ));
        }
    }
    assert_eq!(
        rows,
        vec![("a".to_string(), 30, 30), ("b".to_string(), 5, 5)]
    );

    // The default statement stays passive: a new source change leaves the
    // join at its previous state until the chain is asked to advance.
    source
        .append_batch(
            eager.runtime().client(),
            source_batch(&[(4, "b", 15, "insert")]),
        )
        .await
        .unwrap();
    let passive = IvmSqlExecutor::new(IvmRuntime::from_env().await.unwrap());
    passive.execute(&join_statement).await.unwrap();
    assert_eq!(
        read_sum_mv(passive.runtime(), &left).await,
        vec![("a".to_string(), 30), ("b".to_string(), 5)]
    );
}
