// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! Derived-table rewriting: a statement whose operator reads a compound
//! subplan (an aggregate over a join) is maintained through an internal
//! derived view, materialized and refreshed automatically.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_COUNT_COLUMN, IVM_EPOCH_COLUMN, IVM_NONNULL_COUNT_COLUMN, IVM_ROW_KINDS_COLUMN,
    IVM_SOURCE_COLUMN, IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions,
    WindowColumn, WindowFunction, WindowGroupSpec, multi_agg_mv_schema_for,
    multi_window_mv_schema_for_with_payload, row_expr_mv_schema_for,
    sum_count_mv_schema_for, top_k_mv_schema_for, window_columns_mv_schema_for,
    window_columns_mv_schema_for_with_payload,
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
    let mut mv_options = IvmTableOptions::new(
        format!("ivm_der_mv_{suffix}"),
        table_path(&dir, "mv"),
        aggregate_over_join_mv_schema(),
    )
    .with_primary_keys(vec!["g".to_string()]);
    mv_options.hash_bucket_num = "8".to_string();
    let mv = runtime.create_table(mv_options).await.unwrap();
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
    assert_eq!(derived_table.hash_bucket_num, "8");
    // A keyed lookup routes by the recorded bucket columns.  Leaving them
    // empty makes every before-image lookup scan the whole step table.
    assert_eq!(derived_table.bucket_columns, derived_table.primary_keys);
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

/// The sum-count MV schema of a DISTINCT over a derived join whose computed
/// key has the same name as the join's payload column.
fn case_over_join_mv_schema() -> SchemaRef {
    let join = lakesoul_ivm::wide_keyed_join_view_schema_for(
        &source_schema(),
        &source_schema(),
        &["k".to_string()],
        &["k".to_string()],
        &["k".to_string()],
        &[lakesoul_ivm::JoinOutputColumn {
            side: lakesoul_ivm::JoinSide::Right,
            column: "v".to_string(),
            name: "v".to_string(),
        }],
    )
    .unwrap();
    lakesoul_ivm::sum_count_groups_mv_schema_for(
        &join,
        &["k".to_string(), "v".to_string()],
        &[
            "k".to_string(),
            "CASE WHEN v = 7 THEN 70 ELSE v END".to_string(),
        ],
        None,
        None,
        false,
    )
    .unwrap()
}

const CASE_STATEMENT: &str = "INSERT INTO {mv} SELECT DISTINCT f.k AS k, \
    CASE WHEN d.v = 7 THEN 70 ELSE d.v END AS v \
    FROM {f} f JOIN {d} d ON f.k = d.k";

async fn read_kv_rows(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql(&format!(
            "select k, v from mv where \"{IVM_ROW_KINDS_COLUMN}\" = 'insert' \
             order by k, v"
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
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), values.value(row)));
        }
    }
    rows
}

async fn full_recompute_case(
    runtime: &IvmRuntime,
    fact: &IvmTable,
    dim: &IvmTable,
) -> Vec<(i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("f", Arc::new(runtime.table_provider(fact)))
        .unwrap();
    context
        .register_table("d", Arc::new(runtime.table_provider(dim)))
        .unwrap();
    let batches = context
        .sql(
            "select distinct f.k as k, case when d.v = 7 then 70 else d.v end as v \
             from f join d on f.k = d.k \
             where f.op <> 'delete' and d.op <> 'delete' order by k, v",
        )
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
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), values.value(row)));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn a_distinct_case_over_a_derived_join_keeps_the_computed_key() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_case_f_{suffix}")).await;
    let dim = create_source(&runtime, &dir, &format!("ivm_case_d_{suffix}")).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_case_mv_{suffix}"),
                table_path(&dir, "mv"),
                case_over_join_mv_schema(),
            )
            .with_primary_keys(vec!["k".to_string(), "v".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = CASE_STATEMENT
        .replace("{mv}", &mv.table_name)
        .replace("{f}", &fact.table_name)
        .replace("{d}", &dim.table_name);

    fact.append_batch(
        runtime.client(),
        source_batch(&[(1, "a", 10, "insert"), (2, "b", 20, "insert")]),
    )
    .await
    .unwrap();
    dim.append_batch(
        runtime.client(),
        source_batch(&[(1, "x", 7, "insert"), (2, "y", 8, "insert")]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_kv_rows(runtime, &mv).await,
        full_recompute_case(runtime, &fact, &dim).await
    );

    // The dimension value moves through the derived join and the CASE must
    // recompute instead of falling back to the join's raw payload column.
    dim.append_batch(runtime.client(), source_batch(&[(1, "x", 8, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_kv_rows(runtime, &mv).await,
        full_recompute_case(runtime, &fact, &dim).await
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

/// A bushy outer tree: the right side of an outer join is itself an outer
/// join, which becomes a derived step and leaves the outer join a plain
/// two-source shape.  Each side has its own schema, so every payload is a
/// source column.
#[test_log::test(tokio::test)]
async fn a_bushy_outer_tree_makes_the_join_side_a_derived_step() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let op = |values: &[&str]| StringArray::from_iter_values(values.iter().copied());
    let ints = |values: &[i64]| Int64Array::from_iter_values(values.iter().copied());

    let a_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("av", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let a = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_bu_a_{suffix}"),
                table_path(&dir, "a"),
                a_schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("b_k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("bv", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let b = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_bu_b_{suffix}"),
                table_path(&dir, "b"),
                b_schema.clone(),
            )
            .with_primary_keys(vec!["b_k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let c_schema = Arc::new(Schema::new(vec![
        Field::new("c_k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let c = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_bu_c_{suffix}"),
                table_path(&dir, "c"),
                c_schema.clone(),
            )
            .with_primary_keys(vec!["c_k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();

    // The step keeps one payload per side (`b.v AS rv`, `c.k`) and the
    // rewrite widens the compact payloads, so its MV carries the select
    // names the consumer references.
    let step_schema = lakesoul_ivm::wide_keyed_join_view_schema_with_keys(
        &b_schema,
        &c_schema,
        &["b_k".to_string()],
        &["c_k".to_string()],
        &["v".to_string()],
        &lakesoul_ivm::JoinKeyExprs::default(),
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "bv".to_string(),
                // The select list renamed the payload; the step exposes it
                // under the new name and the consuming plan's reference
                // resolves against it.
                name: "rv".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "k".to_string(),
                name: "k".to_string(),
            },
        ],
    )
    .unwrap();
    let step_keys = lakesoul_ivm::keyed_join_output_primary_keys(
        &["b_k".to_string()],
        &["c_k".to_string()],
    );
    let target_schema = lakesoul_ivm::left_join_view_schema_for(
        &a_schema,
        &step_schema,
        &["k".to_string()],
        &step_keys,
        &["k".to_string()],
        "av",
        "rv",
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_bu_mv_{suffix}"),
                table_path(&dir, "mv"),
                target_schema,
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &["k".to_string()],
                &step_keys,
            )),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT a.av, x.rv \
         FROM {} a LEFT JOIN (SELECT b.bv AS rv, c.k \
                              FROM {} b JOIN {} c ON c.v = b.v) x \
         ON x.k = a.k",
        mv.table_name, a.table_name, b.table_name, c.table_name
    );
    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![
                Arc::new(ints(&[1, 2])),
                Arc::new(ints(&[100, 200])),
                Arc::new(op(&["insert", "insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                Arc::new(ints(&[10])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[70])),
                Arc::new(op(&["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                Arc::new(ints(&[20])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[1])),
                Arc::new(op(&["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_pairs(runtime, &mv).await,
        full_bushy_pairs(runtime, &a, &b, &c).await
    );
    let step = derived_right_view(runtime, &mv)
        .await
        .expect("the inner join is a derived step");

    // Dropping the inner right row removes the inner match, so the matching
    // outer row loses its payload too.
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                Arc::new(ints(&[20])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[1])),
                Arc::new(op(&["delete"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_pairs(runtime, &mv).await,
        full_bushy_pairs(runtime, &a, &b, &c).await
    );
    assert_eq!(
        derived_right_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
}

/// The derived step behind an outer join's right side, when one exists.
async fn derived_right_view(runtime: &IvmRuntime, mv: &IvmTable) -> Option<String> {
    let spec = runtime
        .metadata()
        .get_view_spec(&mv.table_id)
        .await
        .unwrap()?;
    let right = spec["right_table_id"].as_str()?.to_string();
    runtime
        .metadata()
        .get_view_spec(&right)
        .await
        .unwrap()
        .map(|_| right)
}

/// The full recompute of the bushy statement: every left row with the inner
/// payload it matches (NULL without one).
async fn full_bushy_pairs(
    runtime: &IvmRuntime,
    a: &IvmTable,
    b: &IvmTable,
    c: &IvmTable,
) -> Vec<(i64, Option<i64>)> {
    let context = SessionContext::new();
    for (name, table) in [("a", a), ("b", b), ("c", c)] {
        context
            .register_table(name, Arc::new(runtime.table_provider(table)))
            .unwrap();
    }
    let batches = context
        .sql(
            "select a.av, x.rv from a left join (\
             select b.bv as rv, c.k from b join c on c.v = b.v) x \
             on x.k = a.k order by a.av",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    pair_rows(&batches)
}

/// The `av, bv` payload rows of the target MV, ordered by `av`.  The compact
/// join layout names them `left_value` / `right_value`.
async fn read_pairs(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, Option<i64>)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select left_value as av, right_value as bv from mv order by av")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    pair_rows(&batches)
}

fn pair_rows(batches: &[RecordBatch]) -> Vec<(i64, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in batches {
        let lefts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rights = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                lefts.value(row),
                if rights.is_null(row) {
                    None
                } else {
                    Some(rights.value(row))
                },
            ));
        }
    }
    rows
}

/// A join side that is itself a LEFT JOIN: the step materializes a non-null
/// row identity (the encoded right keys), so the outer join reads it as a
/// keyed source and the bushy tree works incrementally.
#[test_log::test(tokio::test)]
async fn a_left_join_side_materializes_a_non_null_row_identity() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let op = |values: &[&str]| StringArray::from_iter_values(values.iter().copied());
    let ints = |values: &[i64]| Int64Array::from_iter_values(values.iter().copied());

    let a_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("av", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let a = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_li_a_{suffix}"),
                table_path(&dir, "a"),
                a_schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("b_k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("bv", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let b = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_li_b_{suffix}"),
                table_path(&dir, "b"),
                b_schema.clone(),
            )
            .with_primary_keys(vec!["b_k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let c_schema = Arc::new(Schema::new(vec![
        Field::new("c_k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let c = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_li_c_{suffix}"),
                table_path(&dir, "c"),
                c_schema.clone(),
            )
            .with_primary_keys(vec!["c_k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();

    // The step is a LEFT JOIN whose MV also carries the non-null row
    // identity, so its key columns are the left keys plus that identity.
    let step_schema = lakesoul_ivm::wide_left_join_view_schema_with_identity(
        &b_schema,
        &c_schema,
        &["b_k".to_string()],
        &["c_k".to_string()],
        &["v".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "bv".to_string(),
                name: "bv".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "k".to_string(),
                name: "k".to_string(),
            },
        ],
    )
    .unwrap();
    let step_keys = {
        // The MV's key columns: the aliased left identity plus the encoded
        // row identity.
        let mut keys =
            lakesoul_ivm::keyed_join_output_primary_keys(&["b_k".to_string()], &[]);
        keys.push(lakesoul_ivm::IVM_PAIR_IDENTITY_COLUMN.to_string());
        keys
    };
    let target_schema = lakesoul_ivm::left_join_view_schema_for(
        &a_schema,
        &step_schema,
        &["k".to_string()],
        &step_keys,
        &["k".to_string()],
        "av",
        "bv",
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_li_mv_{suffix}"),
                table_path(&dir, "mv"),
                target_schema,
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &["k".to_string()],
                &step_keys,
            )),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT a.av, x.bv \
         FROM {} a LEFT JOIN (SELECT b.bv, c.k \
                              FROM {} b LEFT JOIN {} c ON c.v = b.v) x \
         ON x.k = a.k",
        mv.table_name, a.table_name, b.table_name, c.table_name
    );
    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![
                Arc::new(ints(&[1, 2])),
                Arc::new(ints(&[100, 200])),
                Arc::new(op(&["insert", "insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                Arc::new(ints(&[10])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[70])),
                Arc::new(op(&["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                Arc::new(ints(&[20])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[1])),
                Arc::new(op(&["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let step = derived_right_view(runtime, &mv)
        .await
        .expect("the left join is a derived step");
    assert_eq!(
        read_pairs(runtime, &mv).await,
        full_left_side_pairs(runtime, &a, &b, &c).await
    );

    // Deleting the inner right row keeps the inner left row with a NULL
    // payload, which no longer matches the outer key.
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                Arc::new(ints(&[20])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[1])),
                Arc::new(op(&["delete"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_pairs(runtime, &mv).await,
        full_left_side_pairs(runtime, &a, &b, &c).await
    );
    assert_eq!(
        derived_right_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
}

/// The full recompute of the left-join-side statement.
async fn full_left_side_pairs(
    runtime: &IvmRuntime,
    a: &IvmTable,
    b: &IvmTable,
    c: &IvmTable,
) -> Vec<(i64, Option<i64>)> {
    let context = SessionContext::new();
    for (name, table) in [("a", a), ("b", b), ("c", c)] {
        context
            .register_table(name, Arc::new(runtime.table_provider(table)))
            .unwrap();
    }
    let batches = context
        .sql(
            "select a.av, x.bv from a left join (\
             select b.bv, c.k from b left join c on c.v = b.v) x \
             on x.k = a.k order by a.av",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    pair_rows(&batches)
}

/// A join side that is itself a FULL JOIN: both sides may be unmatched, so
/// the step's row identity encodes both sides' keys and the outer join reads
/// the step as a keyed source.
#[test_log::test(tokio::test)]
async fn a_full_join_side_materializes_a_non_null_row_identity() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let op = |values: &[&str]| StringArray::from_iter_values(values.iter().copied());
    let ints = |values: &[i64]| Int64Array::from_iter_values(values.iter().copied());

    let a_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("av", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let a = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fj_a_{suffix}"),
                table_path(&dir, "a"),
                a_schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("b_k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("bv", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let b = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fj_b_{suffix}"),
                table_path(&dir, "b"),
                b_schema.clone(),
            )
            .with_primary_keys(vec!["b_k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();
    let c_schema = Arc::new(Schema::new(vec![
        Field::new("c_k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let c = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fj_c_{suffix}"),
                table_path(&dir, "c"),
                c_schema.clone(),
            )
            .with_primary_keys(vec!["c_k".to_string()])
            .with_cdc_column("op"),
        )
        .await
        .unwrap();

    // The step keeps one payload per side (`bv` from `b`, `k` from `c`), so
    // the rewrite widens the compact payloads to those names.
    let step_schema = lakesoul_ivm::wide_full_join_view_schema_with_identity(
        &b_schema,
        &c_schema,
        &["b_k".to_string()],
        &["c_k".to_string()],
        &["v".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "bv".to_string(),
                name: "bv".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "k".to_string(),
                name: "k".to_string(),
            },
        ],
    )
    .unwrap();
    let step_keys = vec![lakesoul_ivm::IVM_PAIR_IDENTITY_COLUMN.to_string()];
    let target_schema = lakesoul_ivm::left_join_view_schema_for(
        &a_schema,
        &step_schema,
        &["k".to_string()],
        &step_keys,
        &["k".to_string()],
        "av",
        "bv",
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_fj_mv_{suffix}"),
                table_path(&dir, "mv"),
                target_schema,
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &["k".to_string()],
                &step_keys,
            )),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT a.av, x.bv \
         FROM {} a LEFT JOIN (SELECT b.bv, c.k \
                              FROM {} b FULL JOIN {} c ON c.v = b.v) x \
         ON x.k = a.k",
        mv.table_name, a.table_name, b.table_name, c.table_name
    );
    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![
                Arc::new(ints(&[1, 2])),
                Arc::new(ints(&[100, 200])),
                Arc::new(op(&["insert", "insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                Arc::new(ints(&[10])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[70])),
                Arc::new(op(&["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                Arc::new(ints(&[20])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[1])),
                Arc::new(op(&["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let step = derived_right_view(runtime, &mv)
        .await
        .expect("the full join is a derived step");
    assert_eq!(
        read_pairs(runtime, &mv).await,
        full_full_side_pairs(runtime, &a, &b, &c).await
    );

    // Deleting the matched right row leaves the left row unmatched (its key
    // becomes NULL), so the outer row loses its payload.
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                Arc::new(ints(&[20])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[1])),
                Arc::new(op(&["delete"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_pairs(runtime, &mv).await,
        full_full_side_pairs(runtime, &a, &b, &c).await
    );

    // Re-inserting it restores the match; the step is reused.
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                Arc::new(ints(&[20])),
                Arc::new(ints(&[7])),
                Arc::new(ints(&[1])),
                Arc::new(op(&["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_pairs(runtime, &mv).await,
        full_full_side_pairs(runtime, &a, &b, &c).await
    );
    assert_eq!(
        derived_right_view(runtime, &mv).await.as_deref(),
        Some(step.as_str())
    );
}

/// The full recompute of the full-join-side statement.
async fn full_full_side_pairs(
    runtime: &IvmRuntime,
    a: &IvmTable,
    b: &IvmTable,
    c: &IvmTable,
) -> Vec<(i64, Option<i64>)> {
    let context = SessionContext::new();
    for (name, table) in [("a", a), ("b", b), ("c", c)] {
        context
            .register_table(name, Arc::new(runtime.table_provider(table)))
            .unwrap();
    }
    let batches = context
        .sql(
            "select a.av, x.bv from a left join (\
             select b.bv, c.k from b full join c on c.v = b.v) x \
             on x.k = a.k order by a.av",
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    pair_rows(&batches)
}

/// A computed projection over a join is materialized as a derived join
/// step; an operator above it can read the step as a normal keyed source.
#[test_log::test(tokio::test)]
async fn a_computed_join_side_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let a = create_source(&runtime, &dir, &format!("ivm_der13_a_{suffix}")).await;
    let b = create_source(&runtime, &dir, &format!("ivm_der13_b_{suffix}")).await;
    let c = create_source(&runtime, &dir, &format!("ivm_der13_c_{suffix}")).await;
    let mv_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("sum_v", DataType::Int64, true),
        Field::new(IVM_COUNT_COLUMN, DataType::Int64, false),
        Field::new(IVM_NONNULL_COUNT_COLUMN, DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der13_mv_{suffix}"),
                table_path(&dir, "mv"),
                mv_schema,
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT y.k AS k, SUM(y.w) AS sum_v FROM (\
           SELECT a.k AS k, x.w AS w FROM {} a JOIN (\
             SELECT b.v * 2 AS w, c.k AS k FROM {} b JOIN {} c ON c.g = b.g\
           ) x ON x.k = a.k\
         ) y GROUP BY y.k",
        mv.table_name, a.table_name, b.table_name, c.table_name
    );
    let runtime = executor.runtime();
    a.append_batch(
        runtime.client(),
        source_batch(&[(200, "ax", 10, "insert"), (201, "ay", 20, "insert")]),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        source_batch(&[(100, "x", 3, "insert"), (101, "y", 4, "insert")]),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        source_batch(&[(200, "x", 0, "insert"), (201, "y", 0, "insert")]),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_k_sum(executor.runtime(), &mv).await,
        vec![(200, 6), (201, 8)]
    );

    // The computed join side changes with its source.
    b.append_batch(runtime.client(), source_batch(&[(100, "x", 5, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_k_sum(executor.runtime(), &mv).await,
        vec![(200, 10), (201, 8)]
    );

    // A deleted outer row retracts its aggregate group.
    a.append_batch(runtime.client(), source_batch(&[(200, "ax", 10, "delete")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(read_k_sum(executor.runtime(), &mv).await, vec![(201, 8)]);
}

/// A computed aggregate projection is a normal join side: the aggregate
/// step materializes the computed business key and the join reads it.
#[test_log::test(tokio::test)]
async fn a_computed_aggregate_join_side_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &format!("ivm_der14_src_{suffix}")).await;
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("biz_key", DataType::Utf8, false),
        Field::new("w", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der14_dim_{suffix}"),
                table_path(&dir, "dim"),
                dim_schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("sum_v", DataType::Int64, true),
        Field::new(IVM_COUNT_COLUMN, DataType::Int64, false),
        Field::new(IVM_NONNULL_COUNT_COLUMN, DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der14_mv_{suffix}"),
                table_path(&dir, "mv"),
                mv_schema,
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT j.k AS k, SUM(j.amount) AS sum_v FROM (\
           SELECT a.k AS k, a.sum_v * d.w AS amount \
           FROM (SELECT k, g || '!' AS biz_key, SUM(v) AS sum_v \
                 FROM {} GROUP BY k, g) a \
           JOIN {} d ON d.k = a.k AND d.biz_key = a.biz_key\
         ) j GROUP BY j.k",
        mv.table_name, source.table_name, dim.table_name
    );
    let runtime = executor.runtime();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "insert"), (2, "b", 20, "insert")]),
        )
        .await
        .unwrap();
    dim.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            dim_schema,
            vec![
                Arc::new(Int64Array::from_iter_values([1_i64, 2])),
                Arc::new(StringArray::from_iter_values(["a!", "b!"])),
                Arc::new(Int64Array::from_iter_values([100_i64, 200])),
                Arc::new(StringArray::from_iter_values(["insert", "insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_k_sum(executor.runtime(), &mv).await,
        vec![(1, 1000), (2, 4000)]
    );

    source
        .append_batch(runtime.client(), source_batch(&[(1, "a", 12, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_k_sum(executor.runtime(), &mv).await,
        vec![(1, 1200), (2, 4000)]
    );

    dim.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("biz_key", DataType::Utf8, false),
                Field::new("w", DataType::Int64, false),
                Field::new(CHANGE_COLUMN, DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from_iter_values([2_i64])),
                Arc::new(StringArray::from_iter_values(["b!"])),
                Arc::new(Int64Array::from_iter_values([200_i64])),
                Arc::new(StringArray::from_iter_values(["delete"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(read_k_sum(executor.runtime(), &mv).await, vec![(1, 1200)]);
}

/// A `UNION ALL` branch that selects `*` from a derived step hides the
/// bookkeeping columns and joins the other branch through the visible schema.
#[test_log::test(tokio::test)]
async fn a_union_branch_over_a_derived_step_hides_bookkeeping_columns() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &format!("ivm_der15_src_{suffix}")).await;
    let mv_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(IVM_SOURCE_COLUMN, DataType::Int32, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der15_mv_{suffix}"),
                table_path(&dir, "mv"),
                mv_schema,
            )
            .with_primary_keys(vec![IVM_SOURCE_COLUMN.to_string(), "k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT u.k, u.g, u.v FROM (\
           SELECT k, g, v FROM {} \
           UNION ALL \
           SELECT * FROM (SELECT k, g, v * 2 AS v FROM {}) d\
         ) u",
        mv.table_name, source.table_name, source.table_name
    );
    let runtime = executor.runtime();
    source
        .append_batch(runtime.client(), source_batch(&[(1, "a", 10, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_union_rows(executor.runtime(), &mv).await,
        vec![(0, 1, 10), (1, 1, 20)]
    );

    source
        .append_batch(runtime.client(), source_batch(&[(1, "a", 12, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_union_rows(executor.runtime(), &mv).await,
        vec![(0, 1, 12), (1, 1, 24)]
    );
}

async fn read_union_rows(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i32, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql(&format!(
            "select {source}, k, v from mv order by {source}, k",
            source = IVM_SOURCE_COLUMN
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let sources = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((sources.value(row), keys.value(row), values.value(row)));
        }
    }
    rows
}

async fn read_k_sum(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
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

/// The three chain sources: `a LEFT JOIN b LEFT JOIN c`-style fixtures with
/// one distinct payload column each.
struct ChainSources {
    dir: tempfile::TempDir,
    a: IvmTable,
    b: IvmTable,
    c: IvmTable,
    a_schema: Arc<Schema>,
    b_schema: Arc<Schema>,
    c_schema: Arc<Schema>,
}

async fn chain_sources(runtime: &IvmRuntime, tag: &str) -> ChainSources {
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let a_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("av", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let b_schema = Arc::new(Schema::new(vec![
        Field::new("b_k", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("bv", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let c_schema = Arc::new(Schema::new(vec![
        Field::new("c_k", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("cv", DataType::Int64, false),
        Field::new("op", DataType::Utf8, false),
    ]));
    let mut sources = Vec::new();
    for (label, schema, keys) in [
        ("a", a_schema.clone(), "k"),
        ("b", b_schema.clone(), "b_k"),
        ("c", c_schema.clone(), "c_k"),
    ] {
        let table = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_{tag}_{label}_{suffix}"),
                    table_path(&dir, label),
                    schema,
                )
                .with_primary_keys(vec![keys.to_string()])
                .with_cdc_column("op"),
            )
            .await
            .unwrap();
        sources.push(table);
    }
    ChainSources {
        dir,
        a: sources[0].clone(),
        b: sources[1].clone(),
        c: sources[2].clone(),
        a_schema,
        b_schema,
        c_schema,
    }
}

fn chain_ints(values: &[i64]) -> Arc<dyn arrow::array::Array> {
    Arc::new(Int64Array::from_iter_values(values.iter().copied()))
}

fn chain_ops(values: &[&str]) -> Arc<dyn arrow::array::Array> {
    Arc::new(StringArray::from_iter_values(values.iter().copied()))
}

/// The `(av, bv, cv)` rows of a chain MV ordered for comparison.
async fn read_chain_rows(
    runtime: &IvmRuntime,
    mv: &IvmTable,
    columns: &[&str],
) -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let order = columns
        .iter()
        .map(|column| format!("{column} nulls last"))
        .collect::<Vec<_>>()
        .join(", ");
    let batches = context
        .sql(&format!(
            "select {} from mv order by {order}",
            columns.join(", ")
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    triple_rows(&batches)
}

/// The full recompute of a chain statement over the current source rows.
async fn full_chain_rows(
    runtime: &IvmRuntime,
    a: &IvmTable,
    b: &IvmTable,
    c: &IvmTable,
    sql: &str,
) -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
    let context = SessionContext::new();
    for (name, table) in [("a", a), ("b", b), ("c", c)] {
        context
            .register_table(name, Arc::new(runtime.table_provider(table)))
            .unwrap();
    }
    let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
    triple_rows(&batches)
}

fn triple_rows(batches: &[RecordBatch]) -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in batches {
        for index in 0..batch.num_rows() {
            let value = |column: usize| {
                let array = batch
                    .column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                (!array.is_null(index)).then(|| array.value(index))
            };
            rows.push((value(0), value(1), value(2)));
        }
    }
    rows
}

/// A computed select list over a unique lookup chain is materialized as a
/// derived join step, then the outer projection computes over that step.
#[test_log::test(tokio::test)]
async fn a_computed_unique_chain_output_uses_a_derived_join() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let sources = chain_sources(&runtime, "cu").await;
    let ChainSources {
        dir,
        a,
        b,
        c,
        a_schema,
        b_schema,
        c_schema,
    } = &sources;
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("total", DataType::Int64, true),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cu_mv_{}", uuid::Uuid::new_v4().simple()),
                table_path(dir, "mv"),
                target_schema,
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT a.k, a.av + b.bv + c.cv AS total \
         FROM {} a LEFT JOIN {} b ON b.b_k = a.k \
         LEFT JOIN {} c ON c.c_k = b.b_k",
        mv.table_name, a.table_name, b.table_name, c.table_name
    );
    let reference = "select a.k, a.av + b.bv + c.cv as total \
         from a left join b on b.b_k = a.k left join c on c.c_k = b.b_k \
         order by a.k";

    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![
                chain_ints(&[1, 2]),
                chain_ints(&[100, 200]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                chain_ints(&[1, 2]),
                chain_ints(&[1, 2]),
                chain_ints(&[5, 6]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                chain_ints(&[1, 2]),
                chain_ints(&[1, 2]),
                chain_ints(&[7, 8]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let context = SessionContext::new();
    for (name, table) in [("a", &a), ("b", &b), ("c", &c)] {
        context
            .register_table(name, Arc::new(runtime.table_provider(table)))
            .unwrap();
    }
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let expected = context
        .sql(reference)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let actual = context
        .sql(&format!(
            "select k, total from mv where \"{IVM_ROW_KINDS_COLUMN}\" = 'insert' order by k"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let expected = expected
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                )
            })
        })
        .collect::<Vec<_>>();
    let actual = actual
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                (
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                    batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                )
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

/// A chain whose outermost step is a RIGHT JOIN: the left side (the inner
/// LEFT JOIN) is materialized as a derived step and the outer join reads it
/// as a keyed source, so the whole chain stays incremental.
#[test_log::test(tokio::test)]
async fn a_chain_with_a_right_step_materializes_its_left_side() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let sources = chain_sources(&runtime, "cr").await;
    let ChainSources {
        dir: _dir,
        a,
        b,
        c,
        a_schema,
        b_schema,
        c_schema,
    } = &sources;

    let step_schema = lakesoul_ivm::wide_left_join_view_schema_with_identity(
        b_schema,
        c_schema,
        &["b_k".to_string()],
        &["c_k".to_string()],
        &["k".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "bv".to_string(),
                name: "bv".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "cv".to_string(),
                name: "cv".to_string(),
            },
        ],
    )
    .unwrap();
    let step_keys = {
        let mut keys =
            lakesoul_ivm::keyed_join_output_primary_keys(&["b_k".to_string()], &[]);
        keys.push(lakesoul_ivm::IVM_PAIR_IDENTITY_COLUMN.to_string());
        keys
    };
    let target_schema = lakesoul_ivm::wide_outer_join_view_schema_for(
        a_schema,
        &step_schema,
        &["k".to_string()],
        &step_keys,
        &["k".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "av".to_string(),
                name: "av".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "bv".to_string(),
                name: "bv".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "cv".to_string(),
                name: "cv".to_string(),
            },
        ],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cr_mv_{}", uuid::Uuid::new_v4().simple()),
                table_path(&sources.dir, "mv"),
                target_schema,
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &["k".to_string()],
                &step_keys,
            )),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT a.av, x.bv, x.cv \
         FROM (SELECT b.bv, c.cv, b.k FROM {} b LEFT JOIN {} c ON c.k = b.k) x \
         RIGHT JOIN {} a ON a.k = x.k",
        mv.table_name, b.table_name, c.table_name, a.table_name
    );
    let reference = "select a.av, x.bv, x.cv from (\
         select b.bv, c.cv, b.k from b left join c on c.k = b.k) x \
         right join a on a.k = x.k order by a.av nulls last, x.bv nulls last, x.cv nulls last";

    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![
                chain_ints(&[1, 2]),
                chain_ints(&[100, 200]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                chain_ints(&[10, 12]),
                chain_ints(&[1, 7]),
                chain_ints(&[70, 90]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                chain_ints(&[20]),
                chain_ints(&[1]),
                chain_ints(&[5]),
                chain_ops(&["insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert!(derived_right_view(runtime, &mv).await.is_some());
    assert_eq!(
        read_chain_rows(runtime, &mv, &["av", "bv", "cv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );

    // Deleting the matched right row keeps the inner left rows with a NULL
    // payload; the outer step sees the same rows with cv cleared.
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                chain_ints(&[20]),
                chain_ints(&[1]),
                chain_ints(&[5]),
                chain_ops(&["delete"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["av", "bv", "cv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );

    // Deleting an inner left row removes its pair from the outer step too.
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                chain_ints(&[12]),
                chain_ints(&[7]),
                chain_ints(&[90]),
                chain_ops(&["delete"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["av", "bv", "cv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );
}

/// The same outermost RIGHT JOIN without a subquery around the inner join:
/// the chain's left side is materialized directly from the join tree.
#[test_log::test(tokio::test)]
async fn a_plain_chain_with_a_right_step_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let sources = chain_sources(&runtime, "pr").await;
    let ChainSources {
        dir: _dir,
        a,
        b,
        c,
        a_schema,
        b_schema,
        c_schema,
    } = &sources;

    let step_schema = lakesoul_ivm::wide_left_join_view_schema_with_identity(
        a_schema,
        b_schema,
        &["k".to_string()],
        &["b_k".to_string()],
        &["k".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "av".to_string(),
                name: "av".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "bv".to_string(),
                name: "bv".to_string(),
            },
        ],
    )
    .unwrap();
    let step_keys = {
        let mut keys =
            lakesoul_ivm::keyed_join_output_primary_keys(&["k".to_string()], &[]);
        keys.push(lakesoul_ivm::IVM_PAIR_IDENTITY_COLUMN.to_string());
        keys
    };
    let target_schema = lakesoul_ivm::wide_outer_join_view_schema_for(
        c_schema,
        &step_schema,
        &["c_k".to_string()],
        &step_keys,
        &["k".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "cv".to_string(),
                name: "cv".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "av".to_string(),
                name: "av".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "bv".to_string(),
                name: "bv".to_string(),
            },
        ],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pr_mv_{}", uuid::Uuid::new_v4().simple()),
                table_path(&sources.dir, "mv"),
                target_schema,
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &["c_k".to_string()],
                &step_keys,
            )),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT c.cv, a.av, b.bv FROM {} a LEFT JOIN {} b ON b.k = a.k \
         RIGHT JOIN {} c ON c.k = b.k",
        mv.table_name, a.table_name, b.table_name, c.table_name
    );
    let reference = "select c.cv, a.av, b.bv from a left join b on b.k = a.k \
         right join c on c.k = b.k order by c.cv nulls last, a.av nulls last, b.bv nulls last";

    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![
                chain_ints(&[1, 2]),
                chain_ints(&[100, 200]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                chain_ints(&[10, 12]),
                chain_ints(&[1, 7]),
                chain_ints(&[70, 90]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                chain_ints(&[20, 21]),
                chain_ints(&[1, 3]),
                chain_ints(&[5, 6]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["cv", "av", "bv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );

    // `a`'s row for `k = 2` matches no `b` row, so the step keeps it with a
    // NULL payload; the RIGHT join still preserves every `c` row.
    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![chain_ints(&[2]), chain_ints(&[200]), chain_ops(&["delete"])],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["cv", "av", "bv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );
}

/// A chain whose outermost step is a FULL JOIN: the step's own rows may be
/// unmatched by the chain's other sources and still appear.
#[test_log::test(tokio::test)]
async fn a_chain_with_a_full_step_keeps_unmatched_step_rows() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let sources = chain_sources(&runtime, "cf").await;
    let ChainSources {
        dir: _dir,
        a,
        b,
        c,
        a_schema,
        b_schema,
        c_schema,
    } = &sources;

    let step_schema = lakesoul_ivm::wide_left_join_view_schema_with_identity(
        b_schema,
        c_schema,
        &["b_k".to_string()],
        &["c_k".to_string()],
        &["k".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "bv".to_string(),
                name: "bv".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "cv".to_string(),
                name: "cv".to_string(),
            },
        ],
    )
    .unwrap();
    let step_keys = {
        let mut keys =
            lakesoul_ivm::keyed_join_output_primary_keys(&["b_k".to_string()], &[]);
        keys.push(lakesoul_ivm::IVM_PAIR_IDENTITY_COLUMN.to_string());
        keys
    };
    let target_schema = lakesoul_ivm::wide_outer_join_view_schema_for(
        &step_schema,
        a_schema,
        &step_keys,
        &["k".to_string()],
        &["k".to_string()],
        &[
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "bv".to_string(),
                name: "bv".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Left,
                column: "cv".to_string(),
                name: "cv".to_string(),
            },
            lakesoul_ivm::JoinOutputColumn {
                side: lakesoul_ivm::JoinSide::Right,
                column: "av".to_string(),
                name: "av".to_string(),
            },
        ],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cf_mv_{}", uuid::Uuid::new_v4().simple()),
                table_path(&sources.dir, "mv"),
                target_schema,
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &step_keys,
                &["k".to_string()],
            )),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT x.bv, x.cv, a.av \
         FROM (SELECT b.bv, c.cv, b.k FROM {} b LEFT JOIN {} c ON c.k = b.k) x \
         FULL JOIN {} a ON a.k = x.k",
        mv.table_name, b.table_name, c.table_name, a.table_name
    );
    let reference = "select x.bv, x.cv, a.av from (\
         select b.bv, c.cv, b.k from b left join c on c.k = b.k) x \
         full join a on a.k = x.k order by x.bv nulls last, x.cv nulls last, a.av nulls last";

    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![
                chain_ints(&[1, 2]),
                chain_ints(&[100, 200]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                chain_ints(&[10, 12]),
                chain_ints(&[1, 7]),
                chain_ints(&[70, 90]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                chain_ints(&[20]),
                chain_ints(&[1]),
                chain_ints(&[5]),
                chain_ops(&["insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["bv", "cv", "av"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );

    // The step row for `b_k = 12` matches no `a` row and still appears.
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                chain_ints(&[10]),
                chain_ints(&[1]),
                chain_ints(&[70]),
                chain_ops(&["delete"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["bv", "cv", "av"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );
}

/// A plain left-deep chain whose first step is a FULL JOIN: the step's
/// unmatched rows of both sides flow through the rest of the chain.
#[test_log::test(tokio::test)]
async fn a_chain_with_an_inner_full_step_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let sources = chain_sources(&runtime, "if").await;
    let ChainSources {
        dir: _dir,
        a,
        b,
        c,
        a_schema,
        b_schema,
        c_schema,
    } = &sources;

    let target_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("av", DataType::Int64, true),
        Field::new("bv", DataType::Int64, true),
        Field::new("cv", DataType::Int64, true),
        Field::new("__left_pk___ivm_pair_id", DataType::Utf8, true),
        Field::new("__right_pk_c_k", DataType::Int64, true),
        Field::new(lakesoul_ivm::IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(lakesoul_ivm::IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_if_mv_{}", uuid::Uuid::new_v4().simple()),
                table_path(&sources.dir, "mv"),
                target_schema,
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &[lakesoul_ivm::IVM_PAIR_IDENTITY_COLUMN.to_string()],
                &["c_k".to_string()],
            )),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT a.av, b.bv, c.cv FROM {} a FULL JOIN {} b ON b.k = a.k \
         LEFT JOIN {} c ON c.k = b.k",
        mv.table_name, a.table_name, b.table_name, c.table_name
    );
    let reference = "select a.av, b.bv, c.cv from a full join b on b.k = a.k \
         left join c on c.k = b.k order by a.av nulls last, b.bv nulls last, c.cv nulls last";

    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![
                chain_ints(&[1, 2]),
                chain_ints(&[100, 200]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                chain_ints(&[10, 12]),
                chain_ints(&[1, 7]),
                chain_ints(&[70, 90]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                chain_ints(&[20]),
                chain_ints(&[1]),
                chain_ints(&[5]),
                chain_ops(&["insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["av", "bv", "cv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );

    // The unmatched `b` row leaves the step with a NULL `av` and still shows.
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                chain_ints(&[12]),
                chain_ints(&[7]),
                chain_ints(&[90]),
                chain_ops(&["delete"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["av", "bv", "cv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );
}

/// A plain left-deep chain whose first step is a RIGHT JOIN: the step itself
/// is materialized and the rest of the chain reads it as a keyed source.
#[test_log::test(tokio::test)]
async fn a_chain_with_an_inner_right_step_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let sources = chain_sources(&runtime, "ir").await;
    let ChainSources {
        dir: _dir,
        a,
        b,
        c,
        a_schema,
        b_schema,
        c_schema,
    } = &sources;

    // The outer join reads the materialized right-join step as a keyed
    // source: its payloads (`bv`, `av`) and the aliased primary keys make up
    // the MV, whose key is the pair of both sides' keys.
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("bv", DataType::Int64, true),
        Field::new("av", DataType::Int64, true),
        Field::new("cv", DataType::Int64, true),
        Field::new("__left_pk_b_k", DataType::Int64, true),
        Field::new("__right_pk_c_k", DataType::Int64, true),
        Field::new(lakesoul_ivm::IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(lakesoul_ivm::IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ir_mv_{}", uuid::Uuid::new_v4().simple()),
                table_path(&sources.dir, "mv"),
                target_schema,
            )
            .with_primary_keys(lakesoul_ivm::keyed_join_output_primary_keys(
                &["b_k".to_string()],
                &["c_k".to_string()],
            )),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT b.bv, a.av, c.cv FROM {} a RIGHT JOIN {} b ON b.k = a.k \
         LEFT JOIN {} c ON c.k = b.k",
        mv.table_name, a.table_name, b.table_name, c.table_name
    );
    let reference = "select b.bv, a.av, c.cv from a right join b on b.k = a.k \
         left join c on c.k = b.k order by b.bv nulls last, a.av nulls last, c.cv nulls last";

    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![
                chain_ints(&[1, 2]),
                chain_ints(&[100, 200]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    b.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            b_schema.clone(),
            vec![
                chain_ints(&[10, 12]),
                chain_ints(&[1, 7]),
                chain_ints(&[70, 90]),
                chain_ops(&["insert", "insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                chain_ints(&[20]),
                chain_ints(&[1]),
                chain_ints(&[5]),
                chain_ops(&["insert"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["bv", "av", "cv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );

    // Deleting the preserved side's row keeps the step row with a NULL `av`;
    // the outer step still matches it on the key.
    a.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            a_schema.clone(),
            vec![chain_ints(&[1]), chain_ints(&[100]), chain_ops(&["delete"])],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["bv", "av", "cv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );

    // Deleting the last right row removes the step row and the outer row.
    c.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            c_schema.clone(),
            vec![
                chain_ints(&[20]),
                chain_ints(&[1]),
                chain_ints(&[5]),
                chain_ops(&["delete"]),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_rows(runtime, &mv, &["bv", "av", "cv"]).await,
        full_chain_rows(runtime, a, b, c, reference).await
    );
}

/// A subquery that only orders its rows contributes the same multiset: the
/// operator below it reads through the sort.
#[test_log::test(tokio::test)]
async fn an_ordered_subquery_is_read_through() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &format!("ivm_ord_src_{suffix}")).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ord_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source_schema(), &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 4, "insert"),
                (3, "b", 7, "insert"),
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_ord_mv_{suffix} \
         SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM (SELECT g, v FROM ivm_ord_src_{suffix} ORDER BY v DESC) x GROUP BY g"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_sum_mv(runtime, &mv).await,
        vec![("a".to_string(), 14), ("b".to_string(), 7)]
    );

    // An update and a delete flow through the ordered subquery.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(2, "a", 11, "insert"), (3, "b", 7, "delete")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(read_sum_mv(runtime, &mv).await, vec![("a".to_string(), 21)]);
}

/// A `ORDER BY ... LIMIT` subquery materialized as a derived step: the outer
/// aggregate reads the step's current top-k rows.
#[test_log::test(tokio::test)]
async fn a_top_k_subquery_is_materialized_as_a_step() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &format!("ivm_topk_src_{suffix}")).await;
    let step_schema = top_k_mv_schema_for(
        &source_schema(),
        &["k".to_string()],
        &["k", "g", "v"].map(String::from),
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_topk_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(
                    &step_schema,
                    &["k".to_string(), "g".to_string()],
                    Some("v"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string(), "g".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 4, "insert"),
                (3, "b", 7, "insert"),
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_topk_mv_{suffix} \
         SELECT k, g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM (SELECT k, g, v FROM ivm_topk_src_{suffix} ORDER BY v DESC LIMIT 2) x \
         GROUP BY k, g"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_key_sums(runtime, &mv).await,
        vec![(1, "a".to_string(), 10), (3, "b".to_string(), 7)]
    );

    // A row enters the top-k and another leaves it.
    source
        .append_batch(runtime.client(), source_batch(&[(2, "a", 20, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_key_sums(runtime, &mv).await,
        vec![(1, "a".to_string(), 10), (2, "a".to_string(), 20)]
    );
}

/// The `(k, g, sum_v)` rows of a sum/count MV ordered by `k`.
async fn read_key_sums(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select k, g, sum_v from mv order by k")
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
        for row in 0..batch.num_rows() {
            rows.push((
                keys.value(row),
                groups.value(row).to_string(),
                sums.value(row),
            ));
        }
    }
    rows
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
                    &["g".to_string()],
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

/// The `(first, second)` rows of a query ordered by the first column.
async fn read_pair_rows(
    runtime: &IvmRuntime,
    table: &IvmTable,
    select: &str,
) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table(
            table.table_name.as_str(),
            Arc::new(runtime.table_provider(table)),
        )
        .unwrap();
    let batches = context.sql(select).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push((
                arrow::util::display::array_value_to_string(batch.column(0), row)
                    .unwrap(),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
            ));
        }
    }
    rows.sort();
    rows
}

/// A top-k over a **MIN** step with its own value-count state table.
#[test_log::test(tokio::test)]
async fn a_top_k_over_a_min_step_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der10_f_{suffix}")).await;
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
                format!("ivm_der10_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::top_k_mv_schema_for(
                    &step_schema,
                    &["g".to_string()],
                    &["g".to_string(), "value".to_string()],
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
        "INSERT INTO {} SELECT g, value FROM (SELECT g, MIN(v) AS value FROM {}          GROUP BY g) t ORDER BY value DESC LIMIT 2",
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
    let step = derived_step_view(runtime, &mv).await;
    assert!(step.is_some());
    let expected = format!(
        "select g, value from (select g, min(v) as value from {} where op <> 'delete'          group by g) t order by value desc, g limit 2",
        fact.table_name
    );
    assert_eq!(
        read_pair_rows(
            runtime,
            &mv,
            &format!("select g, value from {} order by g", mv.table_name)
        )
        .await,
        read_pair_rows(runtime, &fact, &expected).await
    );

    // A new group takes the top spot; the boundary group leaves it.
    fact.append_batch(runtime.client(), source_batch(&[(4, "d", 100, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        derived_step_view(runtime, &mv).await.as_deref(),
        step.as_deref()
    );
    assert_eq!(
        read_pair_rows(
            runtime,
            &mv,
            &format!("select g, value from {} order by g", mv.table_name)
        )
        .await,
        read_pair_rows(runtime, &fact, &expected).await
    );
}

/// A select-list (row) view over a derived window step: the subquery carries
/// a value window beside the rank, so the window cannot be inlined and the row
/// view reads the step.
#[test_log::test(tokio::test)]
async fn a_row_over_a_window_step_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der12_f_{suffix}")).await;
    let window_column =
        |function: WindowFunction, value: Option<&str>, name: &str| WindowColumn {
            function,
            value_column: value.map(str::to_string),
            window_args: None,
            window_filter: None,
            ignore_nulls: false,
            window_frame: None,
            column: name.to_string(),
        };
    // The step's layout: the partition key, the source primary key, the window
    // columns; the row view materializes its projection plus the source keys
    // it omits (here the partition key `g`).
    let step_schema = window_columns_mv_schema_for(
        &source_schema(),
        &["g".to_string()],
        &["k".to_string()],
        &[
            window_column(WindowFunction::Sum, Some("v"), "ws"),
            window_column(WindowFunction::RowNumber, None, "rn"),
        ],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der12_mv_{suffix}"),
                table_path(&dir, "mv"),
                row_expr_mv_schema_for(
                    &step_schema,
                    &["k".to_string(), "ws".to_string(), "g".to_string()],
                    &[],
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT k, ws FROM (SELECT k, sum(v) over \
         (partition by g order by v) AS ws, row_number() over \
         (partition by g order by v) AS rn FROM {}) t WHERE rn <= 2",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 30, "insert"),
            (3, "b", 20, "insert"),
        ]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let expected = format!(
        "select k, ws, g from (select k, g, v, sum(v) over \
         (partition by g order by v) as ws, row_number() over \
         (partition by g order by v) as rn from {} where op <> 'delete') t \
         where rn <= 2",
        fact.table_name
    );
    assert_eq!(
        read_key_sum_group(
            runtime,
            &mv,
            &format!("select k, ws, g from {} order by k", mv.table_name)
        )
        .await,
        read_key_sum_group(runtime, &fact, &format!("{expected} order by k")).await
    );

    // A new row changes the window boundary and the projection follows.
    fact.append_batch(runtime.client(), source_batch(&[(4, "b", 5, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_key_sum_group(
            runtime,
            &mv,
            &format!("select k, ws, g from {} order by k", mv.table_name)
        )
        .await,
        read_key_sum_group(runtime, &fact, &format!("{expected} order by k")).await
    );
}

/// The `(k, ws, g)` rows of a row-over-window MV, ordered by `k`.
async fn read_key_sum_group(
    runtime: &IvmRuntime,
    table: &IvmTable,
    select: &str,
) -> Vec<(i64, i64, String)> {
    let context = SessionContext::new();
    context
        .register_table(
            table.table_name.as_str(),
            Arc::new(runtime.table_provider(table)),
        )
        .unwrap();
    let batches = context.sql(select).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push((
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                arrow::util::display::array_value_to_string(batch.column(2), row)
                    .unwrap(),
            ));
        }
    }
    rows.sort();
    rows
}

/// A top-k over a derived window step whose input is a join step: the window
/// step carries the payload columns the consumer reads, and its own keys come
/// from the join step's row identities.
#[test_log::test(tokio::test)]
async fn a_top_k_over_a_window_over_a_join_step_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der13_f_{suffix}")).await;
    let dim = create_source(&runtime, &dir, &format!("ivm_der13_d_{suffix}")).await;
    let window_column =
        |function: WindowFunction, value: Option<&str>, name: &str| WindowColumn {
            function,
            value_column: value.map(str::to_string),
            window_args: None,
            window_filter: None,
            ignore_nulls: false,
            window_frame: None,
            column: name.to_string(),
        };
    // The join step's layout: the wide payload columns and both identities.
    let join_step_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("__left_pk_k", DataType::Int64, false),
        Field::new("__right_pk_k", DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
        Field::new(IVM_EPOCH_COLUMN, DataType::Int64, false),
    ]));
    let step_schema = window_columns_mv_schema_for_with_payload(
        &join_step_schema,
        &["g".to_string()],
        &["__left_pk_k".to_string(), "__right_pk_k".to_string()],
        &["k".to_string(), "v".to_string()],
        &[
            window_column(WindowFunction::Sum, Some("v"), "ws"),
            window_column(WindowFunction::RowNumber, None, "rn"),
        ],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der13_mv_{suffix}"),
                table_path(&dir, "mv"),
                top_k_mv_schema_for(
                    &step_schema,
                    &[
                        "g".to_string(),
                        "__left_pk_k".to_string(),
                        "__right_pk_k".to_string(),
                    ],
                    &["k".to_string(), "ws".to_string()],
                )
                .unwrap(),
            )
            .with_primary_keys(vec![
                "g".to_string(),
                "__left_pk_k".to_string(),
                "__right_pk_k".to_string(),
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT k, ws FROM (SELECT f.k AS k, sum(d.v) over \
         (partition by f.g order by d.v) AS ws, row_number() over \
         (partition by f.g order by d.v) AS rn \
         FROM {} f JOIN {} d ON d.k = f.k) t WHERE rn <= 2",
        mv.table_name, fact.table_name, dim.table_name
    );
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 30, "insert"),
            (3, "b", 20, "insert"),
        ]),
    )
    .await
    .unwrap();
    dim.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 100, "insert"),
            (2, "a", 200, "insert"),
            (3, "b", 300, "insert"),
        ]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let expected = format!(
        "select k, ws from (select f.k as k, sum(d.v) over \
         (partition by f.g order by d.v) as ws, row_number() over \
         (partition by f.g order by d.v) as rn \
         from (select * from {} where op <> 'delete') f \
         join (select * from {} where op <> 'delete') d on d.k = f.k) t \
         where rn <= 2",
        fact.table_name, dim.table_name
    );
    assert_eq!(
        read_pairs_i64(
            runtime,
            &[&mv],
            &format!("select k, ws from {} order by k", mv.table_name)
        )
        .await,
        read_pairs_i64(runtime, &[&fact, &dim], &format!("{expected} order by k")).await
    );
    // A new fact row shifts the ordering inside its partition.
    fact.append_batch(runtime.client(), source_batch(&[(4, "a", 5, "insert")]))
        .await
        .unwrap();
    dim.append_batch(runtime.client(), source_batch(&[(4, "a", 400, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_pairs_i64(
            runtime,
            &[&mv],
            &format!("select k, ws from {} order by k", mv.table_name)
        )
        .await,
        read_pairs_i64(runtime, &[&fact, &dim], &format!("{expected} order by k")).await
    );
}

/// A select-list view over a **multi-window** step (two clauses with
/// different order directions): the step carries the payload columns the
/// layout holds, and its MV schema and refresh agree on them.
#[test_log::test(tokio::test)]
async fn a_row_over_a_multi_window_step_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der15_f_{suffix}")).await;
    let window_column =
        |function: WindowFunction, value: Option<&str>, name: &str| WindowColumn {
            function,
            value_column: value.map(str::to_string),
            window_args: None,
            window_filter: None,
            ignore_nulls: false,
            window_frame: None,
            column: name.to_string(),
        };
    let step_schema = multi_window_mv_schema_for_with_payload(
        &source_schema(),
        &["k".to_string()],
        &["v".to_string()],
        &[
            WindowGroupSpec {
                partition_keys: vec!["g".to_string()],
                order_keys: vec!["v".to_string()],
                order_by: Vec::new(),
                columns: vec![window_column(WindowFunction::Sum, Some("v"), "ws")],
            },
            WindowGroupSpec {
                partition_keys: vec!["g".to_string()],
                order_keys: vec!["v".to_string()],
                order_by: vec!["v desc".to_string()],
                columns: vec![window_column(WindowFunction::RowNumber, None, "rn")],
            },
        ],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_der15_mv_{suffix}"),
                table_path(&dir, "mv"),
                row_expr_mv_schema_for(
                    &step_schema,
                    &[
                        "k".to_string(),
                        "ws".to_string(),
                        "v".to_string(),
                        "rn".to_string(),
                    ],
                    &[],
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT k, ws, v, rn FROM (SELECT k, g, v, sum(v) over \
         (partition by g order by v) AS ws, row_number() over \
         (partition by g order by v desc) AS rn FROM {}) t WHERE v >= 20",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 30, "insert"),
            (3, "b", 20, "insert"),
            (4, "a", 50, "insert"),
            (5, "b", 40, "insert"),
        ]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let expected = format!(
        "select k, ws, v from (select k, g, v, sum(v) over \
         (partition by g order by v) as ws, row_number() over \
         (partition by g order by v desc) as rn from {} where op <> 'delete') t \
         where v >= 20",
        fact.table_name
    );
    assert_eq!(
        read_triples_i64(
            runtime,
            &[&mv],
            &format!("select k, ws, v from {} order by k", mv.table_name)
        )
        .await,
        read_triples_i64(runtime, &[&fact], &format!("{expected} order by k")).await
    );
    // An updated ordering value moves the row through both clauses.
    fact.append_batch(runtime.client(), source_batch(&[(2, "a", 60, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_triples_i64(
            runtime,
            &[&mv],
            &format!("select k, ws, v from {} order by k", mv.table_name)
        )
        .await,
        read_triples_i64(runtime, &[&fact], &format!("{expected} order by k")).await
    );
}

/// The `(k, ws, v)` rows of a window-consumer MV ordered by `k`.
async fn read_triples_i64(
    runtime: &IvmRuntime,
    tables: &[&IvmTable],
    select: &str,
) -> Vec<(i64, i64, i64)> {
    let context = SessionContext::new();
    for table in tables {
        context
            .register_table(
                table.table_name.as_str(),
                Arc::new(runtime.table_provider(table)),
            )
            .unwrap();
    }
    let batches = context.sql(select).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push((
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
            ));
        }
    }
    rows.sort();
    rows
}

/// The `(k, ws)` rows of a top-k-over-window MV ordered by `k`.
async fn read_pairs_i64(
    runtime: &IvmRuntime,
    tables: &[&IvmTable],
    select: &str,
) -> Vec<(i64, i64)> {
    let context = SessionContext::new();
    for table in tables {
        context
            .register_table(
                table.table_name.as_str(),
                Arc::new(runtime.table_provider(table)),
            )
            .unwrap();
    }
    let batches = context.sql(select).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push((
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
            ));
        }
    }
    rows.sort();
    rows
}

/// A top-k over a **COUNT(DISTINCT)** step.
#[test_log::test(tokio::test)]
async fn a_top_k_over_a_distinct_step_is_materialized() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact = create_source(&runtime, &dir, &format!("ivm_der11_f_{suffix}")).await;
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
                format!("ivm_der11_mv_{suffix}"),
                table_path(&dir, "mv"),
                lakesoul_ivm::top_k_mv_schema_for(
                    &step_schema,
                    &["g".to_string()],
                    &["g".to_string(), "value".to_string()],
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
        "INSERT INTO {} SELECT g, value FROM (SELECT g, COUNT(DISTINCT v) AS value          FROM {} GROUP BY g) t ORDER BY value DESC LIMIT 2",
        mv.table_name, fact.table_name
    );
    fact.append_batch(
        runtime.client(),
        source_batch(&[
            (1, "a", 10, "insert"),
            (2, "a", 20, "insert"),
            (3, "b", 10, "insert"),
        ]),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|view| view.view_id == mv.table_id)
        .unwrap();
    println!("REASON {:?}", info.fallback_reason);
    assert_eq!(execution.action.label(), "bootstrap");
    assert!(derived_step_view(runtime, &mv).await.is_some());
    let expected = format!(
        "select g, value from (select g, count(distinct v) as value from {}          where op <> 'delete' group by g) t order by value desc, g limit 2",
        fact.table_name
    );
    assert_eq!(
        read_pair_rows(
            runtime,
            &mv,
            &format!("select g, value from {} order by g", mv.table_name)
        )
        .await,
        read_pair_rows(runtime, &fact, &expected).await
    );

    // A new distinct value for `b` ties it with `a`; the key breaks the tie.
    fact.append_batch(runtime.client(), source_batch(&[(4, "b", 20, "insert")]))
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    let info = runtime
        .metadata()
        .list_views()
        .await
        .unwrap()
        .into_iter()
        .find(|view| view.view_id == mv.table_id)
        .unwrap();
    println!("REASON {:?}", info.fallback_reason);
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_pair_rows(
            runtime,
            &mv,
            &format!("select g, value from {} order by g", mv.table_name)
        )
        .await,
        read_pair_rows(runtime, &fact, &expected).await
    );
}

/// The `(k, c1, c2)` rows of a two-count aggregate over a lookup chain.
async fn read_chain_counts(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select k, c1, c2 from mv order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    triple_rows(&batches)
}

/// The full recompute of the two-count aggregate over the current source rows.
async fn full_chain_counts(
    runtime: &IvmRuntime,
    base: &IvmTable,
    first: &IvmTable,
    second: &IvmTable,
    reference: &str,
) -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
    let context = SessionContext::new();
    for (name, table) in [("base", base), ("r1", first), ("r2", second)] {
        context
            .register_table(name, Arc::new(runtime.table_provider(table)))
            .unwrap();
    }
    let batches = context
        .sql(reference)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    triple_rows(&batches)
}

/// Build one chain of two 1:1 left lookups that share the same join keys.  The
/// compiler fuses the two steps into one multi-right lookup, so a right change
/// still probes the shared left key index instead of scanning the base.
#[test_log::test(tokio::test)]
async fn two_lookups_with_the_same_keys_fuse_and_stay_correct() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();

    let base_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("k", DataType::Int64, false),
        Field::new("av", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let base = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ml_base_{suffix}"),
                table_path(&dir, "base"),
                base_schema.clone(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let first = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ml_first_{suffix}"),
                table_path(&dir, "first"),
                right_schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let second = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ml_second_{suffix}"),
                table_path(&dir, "second"),
                right_schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();

    let derived_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v1", DataType::Int64, true),
        Field::new("v2", DataType::Int64, true),
    ]));
    let mv_schema = multi_agg_mv_schema_for(
        &derived_schema,
        &["k".to_string()],
        &[],
        &[
            ("c1".to_string(), DataType::Int64),
            ("c2".to_string(), DataType::Int64),
        ],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_ml_mv_{suffix}"),
                table_path(&dir, "mv"),
                mv_schema,
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let runtime = executor.runtime();
    let statement = format!(
        "INSERT INTO {} SELECT k, COUNT(v1) AS c1, COUNT(v2) AS c2 FROM (\
         SELECT x.k, x.v1, r2.v AS v2 FROM (\
           SELECT b.id, b.k, r1.v AS v1 FROM {} b \
           LEFT JOIN {} r1 ON b.k = r1.k) x \
         LEFT JOIN {} r2 ON x.k = r2.k) t GROUP BY k",
        mv.table_name, base.table_name, first.table_name, second.table_name
    );
    let reference = format!(
        "select b.k as k, count(r1.v) as c1, count(r2.v) as c2 \
         from (select * from base where \"{CHANGE_COLUMN}\" <> 'delete') b \
         left join (select * from r1 where \"{CHANGE_COLUMN}\" <> 'delete') r1 \
         on b.k = r1.k \
         left join (select * from r2 where \"{CHANGE_COLUMN}\" <> 'delete') r2 \
         on b.k = r2.k \
         group by b.k order by b.k"
    );
    base.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            base_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1, 2, 3])),
                Arc::new(Int64Array::from_iter_values([10, 20, 30])),
                Arc::new(Int64Array::from_iter_values([100, 200, 300])),
                Arc::new(StringArray::from_iter_values([
                    "insert", "insert", "insert",
                ])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    first
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                right_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values([10, 20])),
                    Arc::new(Int64Array::from_iter_values([1, 2])),
                    Arc::new(StringArray::from_iter_values(["insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    second
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                right_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values([10, 30])),
                    Arc::new(Int64Array::from_iter_values([10, 30])),
                    Arc::new(StringArray::from_iter_values(["insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_chain_counts(runtime, &mv).await,
        full_chain_counts(runtime, &base, &first, &second, &reference).await
    );

    // The target reads a fused multi-right lookup: the left source is the
    // base, and both rights are unique by the shared key.
    let mut fused = None;
    for view in runtime.metadata().list_views().await.unwrap() {
        let Some(spec) = runtime
            .metadata()
            .get_view_spec(&view.view_id)
            .await
            .unwrap()
        else {
            continue;
        };
        if spec["kind"] == "multi_lookup_join" && spec["left_table_id"] == base.table_id {
            fused = Some(view.view_id);
            break;
        }
    }
    let fused = fused.expect("a fused multi lookup view");
    let spec = runtime
        .metadata()
        .get_view_spec(&fused)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(spec["kind"], "multi_lookup_join");
    assert_eq!(spec["left_table_id"], base.table_id);
    let right_ids = spec["rights"]
        .as_array()
        .unwrap()
        .iter()
        .map(|right| right["table_id"].as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        right_ids,
        vec![first.table_id.clone(), second.table_id.clone()]
    );

    // A change in only the second right probes the shared left index.
    second
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                right_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values([10])),
                    Arc::new(Int64Array::from_iter_values([11])),
                    Arc::new(StringArray::from_iter_values(["insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_counts(runtime, &mv).await,
        full_chain_counts(runtime, &base, &first, &second, &reference).await
    );

    // A base join-key change moves a base row between groups.
    base.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            base_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([3])),
                Arc::new(Int64Array::from_iter_values([10])),
                Arc::new(Int64Array::from_iter_values([300])),
                Arc::new(StringArray::from_iter_values(["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_counts(runtime, &mv).await,
        full_chain_counts(runtime, &base, &first, &second, &reference).await
    );

    // A right delete and a base delete in one window.
    first
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                right_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values([20])),
                    Arc::new(Int64Array::from_iter_values([2])),
                    Arc::new(StringArray::from_iter_values(["delete"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    base.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            base_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1])),
                Arc::new(Int64Array::from_iter_values([10])),
                Arc::new(Int64Array::from_iter_values([100])),
                Arc::new(StringArray::from_iter_values(["delete"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_chain_counts(runtime, &mv).await,
        full_chain_counts(runtime, &base, &first, &second, &reference).await
    );

    for table in [&base, &first, &second, &mv] {
        runtime
            .client()
            .drop_table(&table.table_name, "default")
            .await
            .unwrap();
    }
    drop(dir);
}
