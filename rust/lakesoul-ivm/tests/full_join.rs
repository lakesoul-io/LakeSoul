// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! FULL join views over two keyed sources: all matching pairs plus the
//! unmatched rows of either side (NULL-padded).

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    FullJoinView, IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    full_join_view_schema_for, keyed_join_output_primary_keys,
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
        Field::new("jk", DataType::Utf8, true),
        Field::new("lv", DataType::Utf8, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("rid", DataType::Int64, false),
        Field::new("jk", DataType::Utf8, true),
        Field::new("rv", DataType::Int64, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn left_batch(rows: &[(i64, Option<&str>, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        left_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn right_batch(rows: &[(i64, Option<&str>, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        right_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

type FullJoinRow = (
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<i64>,
);

#[allow(clippy::too_many_arguments)]
fn collect_rows(
    batches: &[RecordBatch],
    left_id: &str,
    right_id: &str,
    jk: &str,
    left_value: &str,
    right_value: &str,
    kinds: Option<&str>,
) -> Vec<FullJoinRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of(left_id).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_ids = batch
            .column(schema.index_of(right_id).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = batch
            .column(schema.index_of(jk).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let lefts = batch
            .column(schema.index_of(left_value).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rights = batch
            .column(schema.index_of(right_value).unwrap())
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
                if ids.is_null(row) {
                    None
                } else {
                    Some(ids.value(row))
                },
                if right_ids.is_null(row) {
                    None
                } else {
                    Some(right_ids.value(row))
                },
                if keys.is_null(row) {
                    None
                } else {
                    Some(keys.value(row).to_string())
                },
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

async fn full_join_rows(runtime: &IvmRuntime, output: &IvmTable) -> Vec<FullJoinRow> {
    let batches = output.read_current(runtime.client()).await.unwrap();
    collect_rows(
        &batches,
        "__left_pk_id",
        "__right_pk_rid",
        "jk",
        "left_value",
        "right_value",
        Some(IVM_ROW_KINDS_COLUMN),
    )
}

/// The SQL semantics of the same full join over the current source states.
async fn sql_full_join_rows(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
) -> Vec<FullJoinRow> {
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
            "select l.id, r.rid, coalesce(l.jk, r.jk) as jk, l.lv, r.rv from \
             (select * from l where \"{CHANGE_COLUMN}\" <> 'delete') l \
             full join (select * from r where \"{CHANGE_COLUMN}\" <> 'delete') r \
             on l.jk = r.jk"
        ))
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    collect_rows(&batches, "id", "rid", "jk", "lv", "rv", None)
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    left: IvmTable,
    right: IvmTable,
    output: IvmTable,
    view: FullJoinView,
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
                format!("ivm_fulljoin_left_{suffix}"),
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
                format!("ivm_fulljoin_right_{suffix}"),
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
                format!("ivm_fulljoin_out_{suffix}"),
                table_path(&dir, "out"),
                full_join_view_schema_for(
                    &left_schema(),
                    &right_schema(),
                    &["id".to_string()],
                    &["rid".to_string()],
                    &["jk".to_string()],
                    "lv",
                    "rv",
                )
                .unwrap(),
            )
            .with_primary_keys(keyed_join_output_primary_keys(
                &["id".to_string()],
                &["rid".to_string()],
            )),
        )
        .await
        .unwrap();
    let view = FullJoinView::new(
        format!("fulljoin_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        "jk",
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

#[test_log::test(tokio::test)]
async fn full_join_keeps_unmatched_rows_of_both_sides() {
    let fixture = setup().await;
    let runtime = &fixture.runtime;
    let (left, right, output, view) = (
        &fixture.left,
        &fixture.right,
        &fixture.output,
        &fixture.view,
    );

    // A match, an unmatched left row, an unmatched right row and NULL keys.
    left.append_batch(
        runtime.client(),
        left_batch(&[
            (1, Some("a"), "L1", "insert"),
            (2, Some("b"), "L2", "insert"),
            (3, None, "L3", "insert"),
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            right_batch(&[
                (10, Some("a"), 100, "insert"),
                (11, Some("c"), 101, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_full_join(view).await.unwrap().unwrap();
    assert_eq!(
        full_join_rows(runtime, output).await,
        sql_full_join_rows(runtime, left, right).await
    );

    // A right update and a right delete that leaves a left row unmatched.
    right
        .append_batch(
            runtime.client(),
            right_batch(&[
                (10, Some("b"), 110, "insert"),
                (11, Some("c"), 101, "delete"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_full_join(view).await.unwrap().unwrap();
    assert_eq!(
        full_join_rows(runtime, output).await,
        sql_full_join_rows(runtime, left, right).await
    );

    // A left join-key change to an unmatched right row matches it.
    left.append_batch(
        runtime.client(),
        left_batch(&[(3, Some("b"), "L3", "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_full_join(view).await.unwrap().unwrap();
    assert_eq!(
        full_join_rows(runtime, output).await,
        sql_full_join_rows(runtime, left, right).await
    );

    // A left delete removes its pairs; its right partner becomes unmatched.
    left.append_batch(
        runtime.client(),
        left_batch(&[
            (1, Some("a"), "L1", "delete"),
            (2, Some("b"), "L2", "delete"),
        ]),
    )
    .await
    .unwrap();
    runtime.refresh_full_join(view).await.unwrap().unwrap();
    let expected = sql_full_join_rows(runtime, left, right).await;
    assert_eq!(full_join_rows(runtime, output).await, expected);

    // A rebuild agrees with the incremental state.
    runtime.rebuild_full_join(view).await.unwrap();
    assert_eq!(full_join_rows(runtime, output).await, expected);

    fixture.cleanup().await;
}
