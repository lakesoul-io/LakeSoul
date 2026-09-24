// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! SEMI/ANTI extensions: non-equi conditions (including pure non-equi joins)
//! and projected output columns.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    CompareOp, IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    SemiAntiCondition, SemiAntiView, semi_anti_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn register(
    context: &SessionContext,
    name: &str,
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) {
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| schema.clone());
    let batches = if batches.is_empty() {
        vec![RecordBatch::new_empty(schema.clone())]
    } else {
        batches
    };
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap());
    context.register_table(name, table).unwrap();
}

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("jk", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("rk", DataType::Int64, false),
        Field::new("jk", DataType::Int64, false),
        Field::new("w", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn left_batch(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        left_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn right_batch(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        right_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    left: IvmTable,
    right: IvmTable,
    view: SemiAntiView,
}

async fn make_fixture(
    join_keys: Vec<String>,
    conditions: Vec<SemiAntiCondition>,
    output_columns: Vec<String>,
    anti: bool,
) -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_saext_left_{suffix}"),
                table_path(&dir, "left"),
                left_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_saext_right_{suffix}"),
                table_path(&dir, "right"),
                right_schema(),
            )
            .with_primary_keys(vec!["rk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_saext_mv_{suffix}"),
                table_path(&dir, "mv"),
                semi_anti_mv_schema_for(&left.schema, &output_columns).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let view = SemiAntiView::new_with_conditions(
        format!("saext_{suffix}"),
        left.clone(),
        right.clone(),
        mv.clone(),
        join_keys,
        conditions,
        anti,
    )
    .with_output_columns(output_columns);
    Fixture {
        runtime,
        dir,
        left,
        right,
        view,
    }
}

/// The current MV left keys.
async fn view_keys(fixture: &Fixture) -> HashSet<i64> {
    let mut keys = HashSet::new();
    for batch in fixture
        .view
        .mv
        .read_current(fixture.runtime.client())
        .await
        .unwrap()
    {
        let schema = batch.schema();
        let values = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                keys.insert(values.value(row));
            }
        }
    }
    keys
}

/// The SQL `EXISTS` semantics of the same predicate.
async fn full_keys(fixture: &Fixture) -> HashSet<i64> {
    let context = SessionContext::new();
    register(
        &context,
        "l",
        fixture
            .left
            .read_current(fixture.runtime.client())
            .await
            .unwrap(),
        &fixture.left.schema,
    );
    register(
        &context,
        "r",
        fixture
            .right
            .read_current(fixture.runtime.client())
            .await
            .unwrap(),
        &fixture.right.schema,
    );
    let mut predicates = fixture
        .view
        .join_keys
        .iter()
        .map(|key| format!("r.{key} = l.{key}"))
        .collect::<Vec<_>>();
    for condition in &fixture.view.conditions {
        let op = match condition.op {
            CompareOp::Eq => "=",
            CompareOp::Ne => "<>",
            CompareOp::Lt => "<",
            CompareOp::Le => "<=",
            CompareOp::Gt => ">",
            CompareOp::Ge => ">=",
        };
        predicates.push(format!(
            "l.{} {} r.{}",
            condition.left_column, op, condition.right_column
        ));
    }
    let predicate = predicates.join(" and ");
    let quantifier = if fixture.view.anti {
        "not exists"
    } else {
        "exists"
    };
    let frame = context
        .sql(&format!(
            "select l.k from l where l.\"{CHANGE_COLUMN}\" <> 'delete' and {quantifier} \
             (select 1 from r where ({predicate}) and r.\"{CHANGE_COLUMN}\" <> 'delete')"
        ))
        .await
        .unwrap();
    let mut keys = HashSet::new();
    for batch in frame.collect().await.unwrap() {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            keys.insert(values.value(row));
        }
    }
    keys
}

async fn assert_matches_oracle(fixture: &Fixture) {
    assert_eq!(view_keys(fixture).await, full_keys(fixture).await);
}

fn lt(left: &str, right: &str) -> SemiAntiCondition {
    SemiAntiCondition {
        left_column: left.to_string(),
        right_column: right.to_string(),
        op: CompareOp::Lt,
    }
}

#[test_log::test(tokio::test)]
async fn non_equi_condition_tracks_both_directions() {
    let fixture =
        make_fixture(vec!["jk".to_string()], vec![lt("v", "w")], vec![], false).await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;

    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[
                (1, 1, 10, "insert"),
                (2, 1, 20, "insert"),
                (3, 2, 30, "insert"),
            ]),
        )
        .await
        .unwrap();
    fixture
        .right
        .append_batch(
            runtime.client(),
            right_batch(&[(100, 1, 15, "insert"), (101, 1, 25, "insert")]),
        )
        .await
        .unwrap();
    runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(view_keys(&fixture).await, HashSet::from([1, 2]));

    // An update flipping the comparison retracts the old match: right row 101
    // no longer covers left row 2.
    fixture
        .right
        .append_batch(runtime.client(), right_batch(&[(101, 1, 5, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(view_keys(&fixture).await, HashSet::from([1]));

    // A new right row matches the untouched partition.
    fixture
        .right
        .append_batch(runtime.client(), right_batch(&[(102, 2, 40, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(view_keys(&fixture).await, HashSet::from([1, 3]));

    // Changing the equality key drops the match (the previous key still
    // matched left row 3, so the affected set must include the old version).
    fixture
        .right
        .append_batch(runtime.client(), right_batch(&[(102, 3, 40, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(view_keys(&fixture).await, HashSet::from([1]));

    // Deleting the remaining matching right row empties the view.
    fixture
        .right
        .append_batch(runtime.client(), right_batch(&[(100, 1, 15, "delete")]))
        .await
        .unwrap();
    runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&fixture).await;
    assert!(view_keys(&fixture).await.is_empty());

    runtime.rebuild_semi_anti(&fixture.view).await.unwrap();
    assert_matches_oracle(&fixture).await;
}

#[test_log::test(tokio::test)]
async fn pure_non_equi_condition() {
    let fixture = make_fixture(Vec::new(), vec![lt("v", "w")], vec![], true).await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;

    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[
                (1, 1, 10, "insert"),
                (2, 1, 20, "insert"),
                (3, 1, 30, "insert"),
            ]),
        )
        .await
        .unwrap();
    fixture
        .right
        .append_batch(runtime.client(), right_batch(&[(100, 1, 25, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&fixture).await;
    // ANTI: only rows without a larger right value stay (30 > 25).
    assert_eq!(view_keys(&fixture).await, HashSet::from([3]));

    // Moving the right value re-opens the match for the smallest row.
    fixture
        .right
        .append_batch(runtime.client(), right_batch(&[(100, 1, 5, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(view_keys(&fixture).await, HashSet::from([1, 2, 3]));

    runtime.rebuild_semi_anti(&fixture.view).await.unwrap();
    assert_matches_oracle(&fixture).await;
}

#[test_log::test(tokio::test)]
async fn output_columns_are_projected() {
    let fixture = make_fixture(
        vec!["jk".to_string()],
        vec![lt("v", "w")],
        vec!["k".to_string(), "jk".to_string()],
        false,
    )
    .await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;

    // The MV only carries the projected columns plus the hidden ones.
    let names = fixture
        .view
        .mv
        .schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        vec![
            "k".to_string(),
            "jk".to_string(),
            IVM_ROW_KINDS_COLUMN.to_string(),
            lakesoul_ivm::IVM_EPOCH_COLUMN.to_string(),
        ]
    );

    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[(1, 1, 10, "insert"), (2, 2, 20, "insert")]),
        )
        .await
        .unwrap();
    fixture
        .right
        .append_batch(runtime.client(), right_batch(&[(100, 1, 15, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view_keys(&fixture).await, HashSet::from([1]));
    assert_matches_oracle(&fixture).await;

    // The payload column is not part of the MV; reading the source with a
    // projection returns only the requested columns.
    let projection = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("jk", DataType::Int64, false),
    ]));
    let batches = fixture
        .left
        .read_current_projected(runtime.client(), Some(&projection))
        .await
        .unwrap();
    assert!(batches.iter().all(|batch| {
        batch.schema().fields().len() == 2 && batch.schema().field_with_name("v").is_err()
    }));
    let mut projected = Vec::new();
    for batch in &batches {
        let values = batch
            .column(batch.schema().index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            projected.push(values.value(row));
        }
    }
    projected.sort();
    assert_eq!(projected, vec![1, 2]);

    runtime.rebuild_semi_anti(&fixture.view).await.unwrap();
    assert_matches_oracle(&fixture).await;
}

#[test_log::test(tokio::test)]
async fn conditions_and_projection_are_validated() {
    // A condition on a missing column is rejected.
    let fixture = make_fixture(
        vec!["jk".to_string()],
        vec![lt("missing", "w")],
        vec![],
        false,
    )
    .await;
    let error = fixture
        .runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("is not in the left source"),
        "unexpected error: {error}"
    );

    // Output columns must contain the left key.
    let fixture =
        make_fixture(vec!["jk".to_string()], vec![], vec!["v".to_string()], false).await;
    let error = fixture
        .runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("output columns must contain the left key"),
        "unexpected error: {error}"
    );

    // At least one join key or condition is required.
    let fixture = make_fixture(Vec::new(), Vec::new(), vec![], false).await;
    let error = fixture
        .runtime
        .refresh_semi_anti(&fixture.view)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("at least one join key or condition"),
        "unexpected error: {error}"
    );

    // An unknown projected column is rejected by the schema helper.
    assert!(semi_anti_mv_schema_for(&left_schema(), &["missing".to_string()]).is_err());
}
