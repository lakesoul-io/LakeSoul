// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Derived-table rewriting: a statement whose operator reads a compound
//! subplan (an aggregate over a join) is maintained through an internal
//! derived view, materialized and refreshed automatically.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, sum_count_mv_schema_for,
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

/// The logical `g -> sum_v` rows of the MV.
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

/// The full recompute of the statement over the current source states.
async fn full_recompute(
    runtime: &IvmRuntime,
    fact: &IvmTable,
    dim: &IvmTable,
) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("f", Arc::new(runtime.table_provider(fact)))
        .unwrap();
    context
        .register_table("d", Arc::new(runtime.table_provider(dim)))
        .unwrap();
    let batches = context
        .sql(
            "select f.g as g, sum(d.v) as sum_v from f join d on f.k = d.k \
             where f.op <> 'delete' and d.op <> 'delete' group by f.g order by g",
        )
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

/// The derived view id the target reads, when its source is a derived step.
async fn derived_step_view(runtime: &IvmRuntime, mv: &IvmTable) -> Option<String> {
    let spec = runtime
        .metadata()
        .get_view_spec(&mv.table_id)
        .await
        .unwrap()?;
    let source = spec["source_table_id"].as_str()?.to_string();
    runtime
        .metadata()
        .get_view_spec(&source)
        .await
        .unwrap()
        .map(|_| source)
}

/// The sum-count MV schema of an aggregate over a (widened) join MV.
fn aggregate_over_join_mv_schema() -> SchemaRef {
    let join = lakesoul_ivm::wide_keyed_join_view_schema_for(
        &source_schema(),
        &source_schema(),
        &["k".to_string()],
        &["k".to_string()],
        &["k".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "g".to_string(),
                name: "g".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "v".to_string(),
                name: "v".to_string(),
            },
        ],
    )
    .unwrap();
    sum_count_mv_schema_for(&join, &["g".to_string()], Some("v")).unwrap()
}

const STATEMENT: &str = "INSERT INTO {mv} SELECT f.g, SUM(d.v) AS sum_v, \
                         COUNT(*) AS count_v FROM {f} f JOIN {d} d \
                         ON f.k = d.k GROUP BY f.g";

#[test_log::test(tokio::test)]
async fn aggregate_over_join_is_materialized_and_maintained() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der_f_{suffix}")).await;
    let dim = create_source(&runtime, &dir, &format!("ivm_der_d_{suffix}")).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der_mv_{suffix}"),
                table_path(&dir, "mv"),
                aggregate_over_join_mv_schema(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = STATEMENT
        .replace("{mv}", &mv.table_name)
        .replace("{f}", &fact.table_name)
        .replace("{d}", &dim.table_name);

    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 20, "insert"),
            (3, "b", 5, "insert"),
        ]),
    )
    .await
    .unwrap();
    dim.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "x", 7, "insert"),
            (1, "y", 100, "insert"),
            (2, "z", 8, "insert"),
        ]),
    )
    .await
    .unwrap();

    // Bootstrap: the join is materialized as the derived step 0.
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_recompute(runtime, &fact, &dim).await
    );
    // The derived step is a registered view (under its table id) whose
    // internal MV table carries the step name.
    let derived_view = derived_step_view(runtime, &mv)
        .await
        .expect("a derived step");
    let derived_table = runtime.open_table_by_id(&derived_view).await.unwrap();
    assert_eq!(
        derived_table.table_name,
        format!("{}__ivm_step0", mv.table_name)
    );
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|view| view.view_id == derived_view)
        .expect("the derived step is registered");
    assert_eq!(info.status, "active");

    // Incremental: updates and deletes on both sides flow through the step.
    fact.append_batch(
        runtime.client(),
        source_batch(&[(2, "b", 20, "insert"), (3, "b", 5, "delete")]),
    )
    .await
    .unwrap();
    dim.append_batch(
        runtime.client(),
        source_batch(&[(2, "z", 8, "delete"), (4, "w", 30, "insert")]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_recompute(runtime, &fact, &dim).await
    );
    // An unchanged definition reuses the same derived step.
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        Some(derived_view.as_str())
    );

    // The chain orchestration advances the whole chain (step 0 first).
    dim.append_batch(runtime.client(), source_batch(&[(4, "w", 100, "insert")]))
        .await
        .unwrap();
    let refreshed = runtime.refresh_view_chain(&mv.table_id).await.unwrap();
    let order = refreshed
        .iter()
        .map(|view| view.view_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        order,
        vec![derived_view.as_str(), mv.table_id.as_str()],
        "{refreshed:?}"
    );
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_recompute(runtime, &fact, &dim).await
    );
}

#[test_log::test(tokio::test)]
async fn a_shrunken_definition_drops_the_derived_step() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der2_f_{suffix}")).await;
    let dim = create_source(&runtime, &dir, &format!("ivm_der2_d_{suffix}")).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der2_mv_{suffix}"),
                table_path(&dir, "mv"),
                aggregate_over_join_mv_schema(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let join_statement = STATEMENT
        .replace("{mv}", &mv.table_name)
        .replace("{f}", &fact.table_name)
        .replace("{d}", &dim.table_name);
    fact.append_batch(
        runtime.client(),
        source_batch(&[(1, "a", 10, "insert"), (2, "b", 20, "insert")]),
    )
    .await
    .unwrap();
    dim.append_batch(runtime.client(), source_batch(&[(1, "x", 7, "insert")]))
        .await
        .unwrap();
    executor.execute(&join_statement).await.unwrap();
    let derived_view = derived_step_view(runtime, &mv)
        .await
        .expect("the join definition has a derived step");

    // The same target switches to a single-source aggregate: the derived
    // step is no longer part of the definition and is dropped.
    let direct = format!(
        "INSERT INTO {} SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM {} GROUP BY g",
        mv.table_name, fact.table_name
    );
    let execution = executor.execute(&direct).await.unwrap();
    assert_eq!(execution.action.label(), "rebuild");
    assert!(
        runtime
            .metadata()
            .get_view_spec(&derived_view)
            .await
            .unwrap()
            .is_none(),
        "the stale derived view is dropped"
    );
    assert!(
        runtime
            .open_table(&format!("{}__ivm_step0", mv.table_name), &mv.namespace)
            .await
            .is_err(),
        "the stale derived table is dropped"
    );
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        vec![("a".to_string(), 10), ("b".to_string(), 20)]
    );

    // Re-running the join definition reuses the step name (a fresh table).
    let execution = executor.execute(&join_statement).await.unwrap();
    assert_eq!(execution.action.label(), "rebuild");
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_recompute(runtime, &fact, &dim).await
    );
}

#[test_log::test(tokio::test)]
async fn an_unsupported_shape_reports_its_reason() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der3_f_{suffix}")).await;
    let mv = create_source(&runtime, &dir, &format!("ivm_der3_mv_{suffix}")).await;
    let executor = IvmSqlExecutor::new(runtime);
    // `LIMIT` without an `ORDER BY` is not maintained; the statement falls
    // into the Tier-2 fallback, whose schema check reports the shape reason.
    let statement = format!(
        "INSERT INTO {} SELECT k, g, v FROM {} LIMIT 2",
        mv.table_name, fact.table_name
    );
    let error = executor.execute(&statement).await.unwrap_err().to_string();
    assert!(error.contains("not maintained"), "{error}");
}

/// The full recompute of `SUM(v)` per `g` over the fact table.
async fn full_group_sum(runtime: &IvmRuntime, fact: &IvmTable) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("f", Arc::new(runtime.table_provider(fact)))
        .unwrap();
    let batches = context
        .sql(
            "select g, sum(v) as sum_v from f where op <> 'delete' \
             group by g order by g",
        )
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

#[test_log::test(tokio::test)]
async fn a_sum_count_step_makes_aggregate_over_aggregate_maintainable() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der4_f_{suffix}")).await;
    let inner =
        sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v")).unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der4_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&inner, &["g".to_string()], Some("sum_v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT g, SUM(sum_v) AS sum_v, COUNT(*) AS count_v \
         FROM (SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM {} GROUP BY g) t \
         GROUP BY g",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
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
    let step = derived_step_view(runtime, &mv)
        .await
        .expect("the inner aggregate is a derived step");
    let step_table = runtime.open_table_by_id(&step).await.unwrap();
    assert_eq!(
        step_table.table_name,
        format!("{}__ivm_step0", mv.table_name)
    );
    // The sum of the per-group sums is the total.
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_group_sum(runtime, &fact).await
    );

    // Incremental: a delete and an update on the fact table.
    fact.append_batch(
        runtime.client(),
        source_batch(&[(3, "b", 5, "delete"), (2, "a", 100, "insert")]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        Some(step.as_str()),
        "an unchanged definition reuses the step"
    );
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_group_sum(runtime, &fact).await
    );
}

#[test_log::test(tokio::test)]
async fn a_sum_count_step_makes_top_k_over_aggregate_maintainable() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der5_f_{suffix}")).await;
    let inner =
        sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v")).unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der5_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::top_k_mv_schema_for(
                    &inner,
                    &["g".to_string(), "sum_v".to_string()],
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT g, sum_v \
         FROM (SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM {} GROUP BY g) t \
         ORDER BY sum_v DESC LIMIT 2",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "b", 30, "insert"),
            (3, "c", 20, "insert"),
        ]),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let step = derived_step_view(runtime, &mv)
        .await
        .expect("the inner aggregate is a derived step");
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_top_groups(runtime, &fact).await
    );

    // A new group takes the top spot; the boundary group leaves.
    fact.append_batch(runtime.client(), source_batch(&[(4, "d", 100, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_top_groups(runtime, &fact).await
    );
}

/// The full recompute of the global top two groups.
async fn full_top_groups(runtime: &IvmRuntime, fact: &IvmTable) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("f", Arc::new(runtime.table_provider(fact)))
        .unwrap();
    let batches = context
        .sql(
            "select g, sum(v) as sum_v from f where op <> 'delete' \
             group by g order by sum_v desc, g limit 2",
        )
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
    rows.sort();
    rows
}

/// The logical `g -> value` rows of a value-count MV.
async fn read_value_mv(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select g, value from mv order by g")
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
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((groups.value(row).to_string(), values.value(row)));
        }
    }
    rows
}

/// The full recompute of `MIN(v)` per `g`.
async fn full_min(runtime: &IvmRuntime, fact: &IvmTable) -> Vec<(String, i64)> {
    full_single_value(runtime, fact, "min(v)").await
}

/// The full recompute of `COUNT(DISTINCT v)` per `g`.
async fn full_distinct(runtime: &IvmRuntime, fact: &IvmTable) -> Vec<(String, i64)> {
    full_single_value(runtime, fact, "count(distinct v)").await
}

async fn full_single_value(
    runtime: &IvmRuntime,
    fact: &IvmTable,
    expression: &str,
) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("f", Arc::new(runtime.table_provider(fact)))
        .unwrap();
    let batches = context
        .sql(&format!(
            "select g, {expression} as value from f where op <> 'delete' \
             group by g order by g"
        ))
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
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((groups.value(row).to_string(), values.value(row)));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn a_min_max_step_makes_min_over_min_maintainable() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der6_f_{suffix}")).await;
    let step_schema = lakesoul_ivm::min_max_mv_schema_for(
        &source_schema(),
        &["g".to_string()],
        "v",
        lakesoul_ivm::MinMaxKind::Min,
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der6_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::min_max_mv_schema_for(
                    &step_schema,
                    &["g".to_string()],
                    "value",
                    lakesoul_ivm::MinMaxKind::Min,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    // The step is a MIN aggregate with its own value-count state table.
    let statement = format!(
        "INSERT INTO {} SELECT g, MIN(value) AS value \
         FROM (SELECT g, MIN(v) AS value FROM {} GROUP BY g) t GROUP BY g",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 20, "insert"),
            (3, "b", 5, "insert"),
            (4, "a", 30, "insert"),
        ]),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let step = derived_step_view(runtime, &mv)
        .await
        .expect("the inner MIN is a derived step");
    assert_eq!(
        read_value_mv(runtime, &mv).await,
        full_min(runtime, &fact).await
    );

    // Deleting the current minimum moves the group's minimum through the
    // step's state table.
    fact.append_batch(runtime.client(), source_batch(&[(1, "a", 10, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_value_mv(runtime, &mv).await,
        full_min(runtime, &fact).await
    );

    // A new lower value wins again; the step is reused.
    fact.append_batch(runtime.client(), source_batch(&[(5, "a", 7, "insert")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
    assert_eq!(
        read_value_mv(runtime, &mv).await,
        full_min(runtime, &fact).await
    );
}

/// The full recompute of the `ROW_NUMBER()` sum per `g`.
async fn full_window_sum(runtime: &IvmRuntime, fact: &IvmTable) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("f", Arc::new(runtime.table_provider(fact)))
        .unwrap();
    let batches = context
        .sql(
            "select g, sum(cast(rn as bigint)) as sum_v from (\
             select k, g, row_number() over (partition by g order by v, k) as rn \
             from f where op <> 'delete') t group by g order by g",
        )
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

#[test_log::test(tokio::test)]
async fn a_window_step_makes_an_aggregate_over_it_maintainable() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der8_f_{suffix}")).await;
    let step_schema = lakesoul_ivm::window_ranking_mv_schema_for(
        &source_schema(),
        &["g".to_string()],
        &["k".to_string()],
        lakesoul_ivm::WindowFunction::RowNumber,
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der8_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(
                    &step_schema,
                    &["g".to_string()],
                    Some("row_number"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    // The step is a ranking window over the fact table.
    let statement = format!(
        "INSERT INTO {} SELECT g, SUM(rn) AS sum_v, COUNT(*) AS count_v \
         FROM (SELECT k, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v, k) AS rn \
         FROM {}) t GROUP BY g",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 20, "insert"),
            (3, "b", 5, "insert"),
            (4, "a", 30, "insert"),
        ]),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let step = derived_step_view(runtime, &mv)
        .await
        .expect("the inner window is a derived step");
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_window_sum(runtime, &fact).await
    );

    // A new row re-ranks its partition and the step maintains the ranks.
    fact.append_batch(runtime.client(), source_batch(&[(5, "a", 15, "insert")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_window_sum(runtime, &fact).await
    );

    // Deleting a row re-ranks again; the step is reused.
    fact.append_batch(runtime.client(), source_batch(&[(2, "a", 20, "delete")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_window_sum(runtime, &fact).await
    );
}

#[test_log::test(tokio::test)]
async fn a_distinct_step_makes_an_aggregate_over_it_maintainable() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der7_f_{suffix}")).await;
    let step_schema = lakesoul_ivm::distinct_agg_mv_schema_for(
        &source_schema(),
        &["g".to_string()],
        "v",
        lakesoul_ivm::DistinctAggKind::Count,
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der7_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&step_schema, &["g".to_string()], Some("value"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    // The step is a COUNT(DISTINCT) aggregate with its own state table.
    let statement = format!(
        "INSERT INTO {} SELECT g, SUM(dc) AS sum_v, COUNT(*) AS count_v \
         FROM (SELECT g, COUNT(DISTINCT v) AS dc FROM {} GROUP BY g) t GROUP BY g",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 10, "insert"),
            (3, "a", 20, "insert"),
            (4, "b", 5, "insert"),
        ]),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let step = derived_step_view(runtime, &mv)
        .await
        .expect("the inner COUNT(DISTINCT) is a derived step");
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_distinct(runtime, &fact).await
    );

    // A duplicate does not change the distinct count.
    fact.append_batch(runtime.client(), source_batch(&[(5, "a", 10, "insert")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_distinct(runtime, &fact).await
    );

    // Deleting the only `20` lowers the count; the step is reused.
    fact.append_batch(runtime.client(), source_batch(&[(3, "a", 20, "delete")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_distinct(runtime, &fact).await
    );
}

/// The full recompute of `SUM(v)` per `g` over the union of two sources.
async fn full_union_sum(
    runtime: &IvmRuntime,
    first: &IvmTable,
    second: &IvmTable,
) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("f0", Arc::new(runtime.table_provider(first)))
        .unwrap();
    context
        .register_table("f1", Arc::new(runtime.table_provider(second)))
        .unwrap();
    let batches = context
        .sql(
            "select g, sum(v) as sum_v from (\
             select k, g, v from f0 where op <> 'delete' \
             union all select k, g, v from f1 where op <> 'delete') t \
             group by g order by g",
        )
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

#[test_log::test(tokio::test)]
async fn a_union_step_makes_an_aggregate_over_it_maintainable() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let first = create_source(&runtime, &dir, &format!("ivm_der9_a_{suffix}")).await;
    let second = create_source(&runtime, &dir, &format!("ivm_der9_b_{suffix}")).await;
    let output = lakesoul_ivm::union_output_schema_for(
        &source_schema(),
        &["k".to_string(), "g".to_string(), "v".to_string()],
        &[],
    )
    .unwrap();
    let step_schema = lakesoul_ivm::union_all_mv_schema_for(&output).unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der9_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&step_schema, &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    // The step is the union of the two keyed sources.
    let statement = format!(
        "INSERT INTO {} SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM (SELECT k, g, v FROM {} UNION ALL SELECT k, g, v FROM {}) t GROUP BY g",
        mv.table_name, first.table_name, second.table_name
    );
    first
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "insert"), (2, "b", 5, "insert")]),
        )
        .await
        .unwrap();
    second
        .append_batch(
            runtime.client(),
            source_batch(&[(3, "a", 20, "insert"), (4, "c", 7, "insert")]),
        )
        .await
        .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let step = derived_step_view(runtime, &mv)
        .await
        .expect("the union is a derived step");
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_union_sum(runtime, &first, &second).await
    );

    // A delete in one branch flows through the union step.
    first
        .append_batch(runtime.client(), source_batch(&[(1, "a", 10, "delete")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_union_sum(runtime, &first, &second).await
    );

    // An insert in the other branch; the step is reused.
    second
        .append_batch(runtime.client(), source_batch(&[(5, "a", 15, "insert")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_union_sum(runtime, &first, &second).await
    );
}

#[test_log::test(tokio::test)]
async fn a_mixed_aggregate_step_makes_aggregate_over_aggregate_maintainable() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der11_f_{suffix}")).await;
    // The step is a mixed aggregate (`SUM(v)` and `MIN(v)`) over the fact.
    let step_schema = lakesoul_ivm::multi_agg_mv_schema_for(
        &source_schema(),
        &["g".to_string()],
        &[],
        &[
            ("sum_v".to_string(), DataType::Int64),
            ("min_v".to_string(), DataType::Int64),
        ],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der11_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&step_schema, &["g".to_string()], Some("sum_v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT g, SUM(s) AS sum_v, COUNT(*) AS count_v \
         FROM (SELECT g, SUM(v) AS s, MIN(v) AS mn FROM {} GROUP BY g) t GROUP BY g",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
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
    let step = derived_step_view(runtime, &mv)
        .await
        .expect("the inner mixed aggregate is a derived step");
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_group_sum(runtime, &fact).await
    );

    // Deleting the group minimum changes the step's MIN and the outer sum.
    fact.append_batch(runtime.client(), source_batch(&[(3, "b", 5, "delete")]))
        .await
        .unwrap();
    fact.append_batch(runtime.client(), source_batch(&[(4, "b", 7, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_group_sum(runtime, &fact).await
    );

    // The step is reused.
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        full_group_sum(runtime, &fact).await
    );
}

/// The full recompute of `SUM(rn)` and `SUM(cnt)` per `g` for the two-clause
/// window chain below.
async fn full_multi_window_sum(
    runtime: &IvmRuntime,
    fact: &IvmTable,
) -> Vec<(String, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("f", Arc::new(runtime.table_provider(fact)))
        .unwrap();
    let batches = context
        .sql(
            "select g, sum(cast(rn as bigint)) as sum_rn, sum(cnt) as sum_cnt from (\
             select k, g, row_number() over (partition by g order by v, k) as rn, \
                    sum(v) over (partition by v) as cnt \
             from f where op <> 'delete') t group by g order by g",
        )
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
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                groups.value(row).to_string(),
                sums.value(row),
                counts.value(row),
            ));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn a_multi_window_step_makes_an_aggregate_over_it_maintainable() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der12_f_{suffix}")).await;
    // The step is a two-clause window chain; its MV keeps the source key, the
    // partition-key values and the window columns, in the analyzer's order
    // (the top clause first).
    let groups = vec![
        lakesoul_ivm::WindowGroupSpec {
            partition_keys: vec!["v".to_string()],
            order_keys: Vec::new(),
            order_by: Vec::new(),
            columns: vec![lakesoul_ivm::WindowColumn {
                function: lakesoul_ivm::WindowFunction::Sum,
                value_column: Some("v".to_string()),
                window_args: None,
                window_filter: None,
                ignore_nulls: false,
                window_frame: None,
                column: "sum_v".to_string(),
            }],
        },
        lakesoul_ivm::WindowGroupSpec {
            partition_keys: vec!["g".to_string()],
            order_keys: vec!["v".to_string()],
            order_by: Vec::new(),
            columns: vec![lakesoul_ivm::WindowColumn {
                function: lakesoul_ivm::WindowFunction::RowNumber,
                value_column: None,
                window_args: None,
                window_filter: None,
                ignore_nulls: false,
                window_frame: None,
                column: "row_number".to_string(),
            }],
        },
    ];
    let step_schema = lakesoul_ivm::multi_window_mv_schema_for(
        &source_schema(),
        &["k".to_string()],
        &groups,
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der12_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::multi_agg_mv_schema_for(
                    &step_schema,
                    &["g".to_string()],
                    &[],
                    &[
                        // The arguments resolve to the step's materialized
                        // columns, so the result types come from them.
                        ("sum_row_number".to_string(), DataType::Int64),
                        ("sum_sum_v".to_string(), DataType::Int64),
                    ],
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT g, SUM(rn) AS sum_rn, SUM(cnt) AS sum_cnt \
         FROM (SELECT k, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v, k) AS rn, \
                      SUM(v) OVER (PARTITION BY v) AS cnt \
               FROM {}) t GROUP BY g",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 10, "insert"),
            (3, "b", 20, "insert"),
        ]),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let step = derived_step_view(runtime, &mv)
        .await
        .expect("the window chain is a derived step");
    assert_eq!(
        read_multi_window_mv(runtime, &mv).await,
        full_multi_window_sum(runtime, &fact).await
    );

    // A new row re-ranks its partition and joins the other partition's sum.
    fact.append_batch(runtime.client(), source_batch(&[(4, "b", 10, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_multi_window_mv(runtime, &mv).await,
        full_multi_window_sum(runtime, &fact).await
    );

    // Deleting a row re-ranks again; the step is reused.
    fact.append_batch(runtime.client(), source_batch(&[(3, "b", 20, "delete")]))
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
    assert_eq!(
        read_multi_window_mv(runtime, &mv).await,
        full_multi_window_sum(runtime, &fact).await
    );
}

/// The `g, sum_rn, sum_cnt` rows of the target MV.
async fn read_multi_window_mv(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(String, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select g, sum_row_number, sum_sum_v from mv order by g")
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
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                groups.value(row).to_string(),
                sums.value(row),
                counts.value(row),
            ));
        }
    }
    rows
}
