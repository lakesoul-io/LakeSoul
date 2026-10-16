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
    top_k_mv_schema_for,
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

/// A computed side projection has no column layout to materialize yet.
#[test_log::test(tokio::test)]
async fn a_computed_join_side_is_not_materialized_yet() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let a = create_source(&runtime, &dir, &format!("ivm_der13_a_{suffix}")).await;
    let b = create_source(&runtime, &dir, &format!("ivm_der13_b_{suffix}")).await;
    let c = create_source(&runtime, &dir, &format!("ivm_der13_c_{suffix}")).await;
    let mv = create_source(&runtime, &dir, &format!("ivm_der13_mv_{suffix}")).await;
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO {} SELECT a.k, x.w \
         FROM {} a LEFT JOIN (SELECT b.v * 2 AS w, c.k AS k \
                              FROM {} b JOIN {} c ON c.g = b.g) x \
         ON x.k = a.k",
        mv.table_name, a.table_name, b.table_name, c.table_name
    );
    let error = executor.execute(&statement).await.unwrap_err().to_string();
    assert!(
        error.contains("a table or a plain column projection"),
        "{error}"
    );
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
