// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! CROSS join views over two keyed sources: every pair of rows, maintained by
//! rewriting the pairs of the identities that changed.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    CrossJoinView, IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    cross_join_view_schema_for, keyed_join_output_primary_keys,
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
        Field::new("id", DataType::Int64, false),
        Field::new("lv", DataType::Utf8, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("rid", DataType::Int64, false),
        Field::new("rv", DataType::Int64, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn left_batch(rows: &[(i64, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        left_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

fn right_batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        right_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

type CrossJoinRow = (i64, i64, Option<String>, Option<i64>);

fn collect_rows(batches: &[RecordBatch], kinds: Option<&str>) -> Vec<CrossJoinRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of("__left_pk_id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_ids = batch
            .column(schema.index_of("__right_pk_rid").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lefts = batch
            .column(schema.index_of("left_value").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rights = batch
            .column(schema.index_of("right_value").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = kinds.map(|kinds| {
            batch
                .column(schema.index_of(kinds).unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
        });
        for row in 0..batch.num_rows() {
            if kinds.is_some_and(|kinds| kinds.value(row) != "insert") {
                continue;
            }
            rows.push((
                ids.value(row),
                right_ids.value(row),
                if lefts.is_null(row) {
                    None
                } else {
                    Some(lefts.value(row).to_string())
                },
                if rights.is_null(row) {
                    None
                } else {
                    Some(rights.value(row))
                },
            ));
        }
    }
    rows.sort();
    rows
}

async fn cross_join_rows(runtime: &IvmRuntime, output: &IvmTable) -> Vec<CrossJoinRow> {
    let batches = output.read_current(runtime.client()).await.unwrap();
    collect_rows(&batches, Some(IVM_ROW_KINDS_COLUMN))
}

/// The SQL semantics of the same cross join over the current source states.
async fn sql_cross_join_rows(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
) -> Vec<CrossJoinRow> {
    let context = SessionContext::new();
    register(
        &context,
        "l",
        left.read_current(runtime.client()).await.unwrap(),
        &left.schema,
    );
    register(
        &context,
        "r",
        right.read_current(runtime.client()).await.unwrap(),
        &right.schema,
    );
    let frame = context
        .sql(&format!(
            "select l.id as lid, r.rid as rid, l.lv as lv, r.rv as rv from \
             (select * from l where \"{CHANGE_COLUMN}\" <> 'delete') l \
             cross join (select * from r where \"{CHANGE_COLUMN}\" <> 'delete') r"
        ))
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lefts = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rights = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                right_ids.value(row),
                if lefts.is_null(row) {
                    None
                } else {
                    Some(lefts.value(row).to_string())
                },
                if rights.is_null(row) {
                    None
                } else {
                    Some(rights.value(row))
                },
            ));
        }
    }
    rows.sort();
    rows
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    left: IvmTable,
    right: IvmTable,
    output: IvmTable,
    view: CrossJoinView,
}

impl Fixture {
    async fn cleanup(self) {
        for table in [&self.left, &self.right, &self.output] {
            self.runtime
                .client()
                .drop_table(&table.table_name, "default")
                .await
                .unwrap();
        }
        drop(self.dir);
    }
}

async fn setup() -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_crossjoin_left_{suffix}"),
                table_path(&dir, "left"),
                left_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_crossjoin_right_{suffix}"),
                table_path(&dir, "right"),
                right_schema(),
            )
            .with_primary_keys(vec!["rid".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_crossjoin_out_{suffix}"),
                table_path(&dir, "out"),
                cross_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &left.primary_keys,
                    &right.primary_keys,
                    "lv",
                    "rv",
                )
                .unwrap(),
            )
            .with_primary_keys(keyed_join_output_primary_keys(
                &left.primary_keys,
                &right.primary_keys,
            )),
        )
        .await
        .unwrap();
    let view = CrossJoinView::new(
        format!("ivm_crossjoin_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        "lv",
        "rv",
    );
    Fixture {
        runtime,
        dir,
        left,
        right,
        output,
        view,
    }
}

async fn assert_matches_sql(fixture: &Fixture, step: &str) {
    assert_eq!(
        cross_join_rows(&fixture.runtime, &fixture.output).await,
        sql_cross_join_rows(&fixture.runtime, &fixture.left, &fixture.right).await,
        "{step}"
    );
}

#[test_log::test(tokio::test)]
async fn cross_join_tracks_both_sides() {
    let fixture = setup().await;
    let runtime = &fixture.runtime;
    let left = &fixture.left;
    let right = &fixture.right;
    let view = &fixture.view;

    // Bootstrap over an empty view.
    runtime.rebuild_cross_join(view).await.unwrap();
    assert_matches_sql(&fixture, "bootstrap").await;

    // A left row pairs with every right row.
    left.append_batch(
        runtime.client(),
        left_batch(&[(1, "a", "insert"), (2, "b", "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_cross_join(view).await.unwrap().unwrap();
    assert_matches_sql(&fixture, "left insert").await;

    // A right row pairs with every left row.
    right
        .append_batch(
            runtime.client(),
            right_batch(&[(10, 100, "insert"), (11, 101, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_cross_join(view).await.unwrap().unwrap();
    assert_matches_sql(&fixture, "right insert").await;

    // No new data: the refresh is a no-op.
    assert!(runtime.refresh_cross_join(view).await.unwrap().is_none());

    // An update rewrites the pairs of the updated row.
    left.append_batch(runtime.client(), left_batch(&[(1, "c", "insert")]))
        .await
        .unwrap();
    runtime.refresh_cross_join(view).await.unwrap().unwrap();
    assert_matches_sql(&fixture, "left update").await;

    // The same on the right side.
    right
        .append_batch(runtime.client(), right_batch(&[(10, 999, "insert")]))
        .await
        .unwrap();
    runtime.refresh_cross_join(view).await.unwrap().unwrap();
    assert_matches_sql(&fixture, "right update").await;

    // A left delete removes its pairs.
    left.append_batch(runtime.client(), left_batch(&[(2, "b", "delete")]))
        .await
        .unwrap();
    runtime.refresh_cross_join(view).await.unwrap().unwrap();
    assert_matches_sql(&fixture, "left delete").await;

    // A right delete removes the pairs referencing it.
    right
        .append_batch(runtime.client(), right_batch(&[(11, 101, "delete")]))
        .await
        .unwrap();
    runtime.refresh_cross_join(view).await.unwrap().unwrap();
    assert_matches_sql(&fixture, "right delete").await;

    // Both sides change in one window.
    left.append_batch(runtime.client(), left_batch(&[(3, "d", "insert")]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), right_batch(&[(12, 102, "insert")]))
        .await
        .unwrap();
    runtime.refresh_cross_join(view).await.unwrap().unwrap();
    assert_matches_sql(&fixture, "both sides").await;

    // A rebuild matches the incremental state.
    runtime.rebuild_cross_join(view).await.unwrap();
    assert_matches_sql(&fixture, "rebuild").await;

    fixture.cleanup().await;
}
