// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! LEFT lookup join views: every left row keeps its referenced right payload
//! (or NULL), the right source is keyed by the join keys so a left row has at
//! most one match, and a change on either side rewrites exactly the affected
//! left rows.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, JoinKeyExprs,
    LookupJoinView, MultiLookupColumn, MultiLookupJoinView, StateRole,
    lookup_join_view_schema_for, multi_lookup_join_mv_schema_for,
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
        Field::new("jk", DataType::Utf8, false),
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

fn right_batch(rows: &[(&str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        right_schema(),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

type LookupRow = (i64, Option<String>, Option<String>, Option<i64>);

fn collect_lookup_rows(
    batches: &[RecordBatch],
    id: &str,
    jk: &str,
    value: &str,
    right: &str,
    kinds: Option<&str>,
) -> Vec<LookupRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of(id).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = batch
            .column(schema.index_of(jk).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(schema.index_of(value).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let rights = batch
            .column(schema.index_of(right).unwrap())
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
            if kinds.is_none_or(|kinds| kinds.value(row) == "insert") {
                rows.push((
                    ids.value(row),
                    if keys.is_null(row) {
                        None
                    } else {
                        Some(keys.value(row).to_string())
                    },
                    if values.is_null(row) {
                        None
                    } else {
                        Some(values.value(row).to_string())
                    },
                    if rights.is_null(row) {
                        None
                    } else {
                        Some(rights.value(row))
                    },
                ));
            }
        }
    }
    rows.sort();
    rows
}

async fn lookup_rows(runtime: &IvmRuntime, output: &IvmTable) -> Vec<LookupRow> {
    let batches = output.read_current(runtime.client()).await.unwrap();
    collect_lookup_rows(
        &batches,
        "id",
        "jk",
        "left_value",
        "right_value",
        Some(IVM_ROW_KINDS_COLUMN),
    )
}

/// The SQL semantics of the same left join over the current source states.
async fn full_lookup_rows(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
) -> Vec<LookupRow> {
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
            "select l.id, l.jk, l.lv, r.rv from \
             (select * from l where \"{CHANGE_COLUMN}\" <> 'delete') l \
             left join (select * from r where \"{CHANGE_COLUMN}\" <> 'delete') r \
             on l.jk = r.jk"
        ))
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    collect_lookup_rows(&batches, "id", "jk", "lv", "rv", None)
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    left: IvmTable,
    right: IvmTable,
    output: IvmTable,
    view: LookupJoinView,
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
                format!("ivm_lookup_left_{suffix}"),
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
                format!("ivm_lookup_right_{suffix}"),
                table_path(&dir, "right"),
                right_schema(),
            )
            .with_primary_keys(vec!["jk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lookup_out_{suffix}"),
                table_path(&dir, "out"),
                lookup_join_view_schema_for(
                    &left_schema(),
                    &right_schema(),
                    &["id".to_string()],
                    &["jk".to_string()],
                    &["jk".to_string()],
                    &JoinKeyExprs::default(),
                    "lv",
                    "rv",
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["id".to_string()]),
        )
        .await
        .unwrap();
    let view = LookupJoinView::new(
        format!("lookup_{suffix}"),
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
async fn lookup_join_refreshes_both_sides() {
    let fixture = setup().await;
    let runtime = &fixture.runtime;
    let (left, right, output, view) = (
        &fixture.left,
        &fixture.right,
        &fixture.output,
        &fixture.view,
    );

    // Unmatched rows (including a NULL join key) keep a NULL right payload.
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
        .append_batch(runtime.client(), right_batch(&[("a", 10, "insert")]))
        .await
        .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();
    assert_eq!(
        lookup_rows(runtime, output).await,
        full_lookup_rows(runtime, left, right).await
    );

    // A left-only payload update for a key with no right row must keep the
    // NULL-padded left row: the keyed right lookup emits an unmatched row and
    // the refresh must not filter it as a CDC tombstone.
    left.append_batch(
        runtime.client(),
        left_batch(&[(2, Some("b"), "L2x", "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();
    assert_eq!(
        lookup_rows(runtime, output).await,
        full_lookup_rows(runtime, left, right).await
    );

    // A new right row fills the previously NULL match.
    right
        .append_batch(runtime.client(), right_batch(&[("b", 20, "insert")]))
        .await
        .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();
    assert_eq!(
        lookup_rows(runtime, output).await,
        full_lookup_rows(runtime, left, right).await
    );

    // A right update is reflected.
    right
        .append_batch(runtime.client(), right_batch(&[("a", 11, "insert")]))
        .await
        .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();
    assert_eq!(
        lookup_rows(runtime, output).await,
        full_lookup_rows(runtime, left, right).await
    );

    // A right delete resets the left rows that referenced it to NULL.
    right
        .append_batch(runtime.client(), right_batch(&[("a", 11, "delete")]))
        .await
        .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();
    assert_eq!(
        lookup_rows(runtime, output).await,
        full_lookup_rows(runtime, left, right).await
    );

    // A left join-key change moves the row to another match.
    left.append_batch(
        runtime.client(),
        left_batch(&[(3, Some("b"), "L3", "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();
    assert_eq!(
        lookup_rows(runtime, output).await,
        full_lookup_rows(runtime, left, right).await
    );

    // A left delete removes the row.
    left.append_batch(
        runtime.client(),
        left_batch(&[(2, Some("b"), "L2", "delete")]),
    )
    .await
    .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();
    let expected = full_lookup_rows(runtime, left, right).await;
    assert_eq!(lookup_rows(runtime, output).await, expected);

    // A rebuild agrees with the incremental state.
    runtime.rebuild_lookup_join(view).await.unwrap();
    assert_eq!(lookup_rows(runtime, output).await, expected);

    fixture.cleanup().await;
}

/// Several versions of one right key in a single window: the lookup must see
/// the last one, not an earlier version still present in the source files.
#[test_log::test(tokio::test)]
async fn lookup_join_same_window_right_versions_use_the_last() {
    let fixture = setup().await;
    let runtime = &fixture.runtime;
    let (left, right, output, view) = (
        &fixture.left,
        &fixture.right,
        &fixture.output,
        &fixture.view,
    );
    left.append_batch(
        runtime.client(),
        left_batch(&[
            (1, Some("a"), "L1", "insert"),
            (2, Some("b"), "L2", "insert"),
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            right_batch(&[("a", 10, "insert"), ("b", 20, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();

    // The left row changes and the right row is rewritten twice in the same
    // window; the final right value is 12.
    left.append_batch(
        runtime.client(),
        left_batch(&[(1, Some("a"), "L1b", "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            right_batch(&[
                ("a", 10, "delete"),
                ("a", 11, "insert"),
                ("a", 12, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();
    assert_eq!(
        lookup_rows(runtime, output).await,
        full_lookup_rows(runtime, left, right).await
    );

    fixture.cleanup().await;
}

/// A window that changes both sides, including a left and a right delete,
/// retracts every affected row.
#[test_log::test(tokio::test)]
async fn lookup_join_retracts_both_sides_in_one_window() {
    let fixture = setup().await;
    let runtime = &fixture.runtime;
    let (left, right, output, view) = (
        &fixture.left,
        &fixture.right,
        &fixture.output,
        &fixture.view,
    );
    left.append_batch(
        runtime.client(),
        left_batch(&[
            (1, Some("a"), "L1", "insert"),
            (2, Some("b"), "L2", "insert"),
            (3, Some("c"), "L3", "insert"),
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            right_batch(&[
                ("a", 10, "insert"),
                ("b", 20, "insert"),
                ("c", 30, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();

    // One window: a left delete, a right delete and a right update.
    left.append_batch(
        runtime.client(),
        left_batch(&[(2, Some("b"), "L2", "delete")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            right_batch(&[("a", 10, "delete"), ("c", 31, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_lookup_join(view).await.unwrap().unwrap();
    assert_eq!(
        lookup_rows(runtime, output).await,
        full_lookup_rows(runtime, left, right).await
    );

    fixture.cleanup().await;
}

type MultiLookupRow = (
    i64,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
);

fn collect_multi_lookup_rows(batches: &[RecordBatch]) -> Vec<MultiLookupRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = batch
            .column(schema.index_of("jk").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(schema.index_of("lv").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let first = batch
            .column(schema.index_of("rv1").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let second = batch
            .column(schema.index_of("rv2").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = schema.index_of(IVM_ROW_KINDS_COLUMN).ok().map(|index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
        });
        for index in 0..batch.num_rows() {
            if kinds.is_some_and(|kinds| kinds.value(index) != "insert") {
                continue;
            }
            rows.push((
                ids.value(index),
                (!keys.is_null(index)).then(|| keys.value(index).to_string()),
                (!values.is_null(index)).then(|| values.value(index).to_string()),
                (!first.is_null(index)).then(|| first.value(index)),
                (!second.is_null(index)).then(|| second.value(index)),
            ));
        }
    }
    rows.sort();
    rows
}

async fn multi_lookup_rows(
    runtime: &IvmRuntime,
    output: &IvmTable,
) -> Vec<MultiLookupRow> {
    let batches = output.read_current(runtime.client()).await.unwrap();
    collect_multi_lookup_rows(&batches)
}

/// The SQL semantics of the same chain of two 1:1 left lookups.
async fn full_multi_lookup_rows(
    runtime: &IvmRuntime,
    left: &IvmTable,
    first: &IvmTable,
    second: &IvmTable,
) -> Vec<MultiLookupRow> {
    let context = SessionContext::new();
    for (name, table) in [("l", left), ("r1", first), ("r2", second)] {
        register(
            &context,
            name,
            table.read_current(runtime.client()).await.unwrap(),
            &table.schema,
        );
    }
    let batches = context
        .sql(&format!(
            "select l.id, l.jk, l.lv, r1.rv as rv1, r2.rv as rv2 from \
             (select * from l where \"{CHANGE_COLUMN}\" <> 'delete') l \
             left join (select * from r1 where \"{CHANGE_COLUMN}\" <> 'delete') r1 \
             on l.jk = r1.jk \
             left join (select * from r2 where \"{CHANGE_COLUMN}\" <> 'delete') r2 \
             on l.jk = r2.jk order by l.id"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    collect_multi_lookup_rows(&batches)
}

struct MultiLookupFixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    left: IvmTable,
    first: IvmTable,
    second: IvmTable,
    output: IvmTable,
    view: MultiLookupJoinView,
}

impl MultiLookupFixture {
    async fn cleanup(self) {
        for table in [&self.left, &self.first, &self.second, &self.output] {
            self.runtime
                .client()
                .drop_table(&table.table_name, "default")
                .await
                .unwrap();
        }
        drop(self.dir);
    }
}

async fn multi_lookup_setup() -> MultiLookupFixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_multi_left_{suffix}"),
                table_path(&dir, "left"),
                left_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let first = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_multi_first_{suffix}"),
                table_path(&dir, "first"),
                right_schema(),
            )
            .with_primary_keys(vec!["jk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let second = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_multi_second_{suffix}"),
                table_path(&dir, "second"),
                right_schema(),
            )
            .with_primary_keys(vec!["jk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let output_columns = vec![
        MultiLookupColumn {
            source: 0,
            column: "jk".to_string(),
            name: "jk".to_string(),
        },
        MultiLookupColumn {
            source: 0,
            column: "lv".to_string(),
            name: "lv".to_string(),
        },
        MultiLookupColumn {
            source: 1,
            column: "rv".to_string(),
            name: "rv1".to_string(),
        },
        MultiLookupColumn {
            source: 2,
            column: "rv".to_string(),
            name: "rv2".to_string(),
        },
        MultiLookupColumn {
            source: 0,
            column: "id".to_string(),
            name: "id".to_string(),
        },
    ];
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_multi_out_{suffix}"),
                table_path(&dir, "out"),
                multi_lookup_join_mv_schema_for(
                    &left_schema(),
                    &[right_schema(), right_schema()],
                    &["id".to_string()],
                    &["jk".to_string()],
                    &output_columns,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["id".to_string()]),
        )
        .await
        .unwrap();
    let view = MultiLookupJoinView {
        view_id: format!("multi_lookup_{suffix}"),
        left: left.clone(),
        rights: vec![(first.clone(), None), (second.clone(), None)],
        mv: output.clone(),
        join_keys: vec!["jk".to_string()],
        output_columns,
        refresh_interval_ms: 0,
    };
    MultiLookupFixture {
        runtime,
        dir,
        left,
        first,
        second,
        output,
        view,
    }
}

#[test_log::test(tokio::test)]
async fn multi_lookup_join_refreshes_every_right_source() {
    let fixture = multi_lookup_setup().await;
    let runtime = &fixture.runtime;
    let (left, first, second, output, view) = (
        &fixture.left,
        &fixture.first,
        &fixture.second,
        &fixture.output,
        &fixture.view,
    );

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
    first
        .append_batch(
            runtime.client(),
            right_batch(&[("a", 10, "insert"), ("b", 20, "insert")]),
        )
        .await
        .unwrap();
    second
        .append_batch(runtime.client(), right_batch(&[("a", 100, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_multi_lookup_join(view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        multi_lookup_rows(runtime, output).await,
        full_multi_lookup_rows(runtime, left, first, second).await
    );

    // The join-key index only stores the left identity and keys: the right
    // payload must not be copied into the side state.
    let state = runtime
        .metadata()
        .list_states(&view.view_id)
        .await
        .unwrap()
        .into_iter()
        .find(|state| state.role == StateRole::JoinLeft)
        .expect("multi lookup join state");
    let state_table = runtime.open_table_by_id(&state.table_id).await.unwrap();
    assert!(state_table.schema.field_with_name("id").is_ok());
    assert!(state_table.schema.field_with_name("lv").is_err());

    // A left-only update must keep both right payloads.
    left.append_batch(
        runtime.client(),
        left_batch(&[(2, Some("b"), "L2x", "insert")]),
    )
    .await
    .unwrap();
    runtime
        .refresh_multi_lookup_join(view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        multi_lookup_rows(runtime, output).await,
        full_multi_lookup_rows(runtime, left, first, second).await
    );

    // A change in only one right source rewrites the affected left rows.
    first
        .append_batch(runtime.client(), right_batch(&[("a", 11, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_multi_lookup_join(view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        multi_lookup_rows(runtime, output).await,
        full_multi_lookup_rows(runtime, left, first, second).await
    );

    // A right delete clears only that source's payload.
    second
        .append_batch(runtime.client(), right_batch(&[("a", 100, "delete")]))
        .await
        .unwrap();
    runtime
        .refresh_multi_lookup_join(view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        multi_lookup_rows(runtime, output).await,
        full_multi_lookup_rows(runtime, left, first, second).await
    );

    // Left, both rights and a left key change in one window.
    left.append_batch(
        runtime.client(),
        left_batch(&[(3, Some("b"), "L3", "insert")]),
    )
    .await
    .unwrap();
    first
        .append_batch(
            runtime.client(),
            right_batch(&[("b", 21, "insert"), ("c", 30, "insert")]),
        )
        .await
        .unwrap();
    second
        .append_batch(
            runtime.client(),
            right_batch(&[("a", 101, "insert"), ("b", 200, "insert")]),
        )
        .await
        .unwrap();
    runtime
        .refresh_multi_lookup_join(view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        multi_lookup_rows(runtime, output).await,
        full_multi_lookup_rows(runtime, left, first, second).await
    );

    // A left delete removes the row.
    left.append_batch(
        runtime.client(),
        left_batch(&[(2, Some("b"), "L2x", "delete")]),
    )
    .await
    .unwrap();
    runtime
        .refresh_multi_lookup_join(view)
        .await
        .unwrap()
        .unwrap();
    let expected = full_multi_lookup_rows(runtime, left, first, second).await;
    assert_eq!(multi_lookup_rows(runtime, output).await, expected);

    // A right delete clears only that source's payload.
    first
        .append_batch(runtime.client(), right_batch(&[("a", 11, "delete")]))
        .await
        .unwrap();
    runtime
        .refresh_multi_lookup_join(view)
        .await
        .unwrap()
        .unwrap();
    let expected = full_multi_lookup_rows(runtime, left, first, second).await;
    assert_eq!(multi_lookup_rows(runtime, output).await, expected);

    // The rebuild agrees with the incremental state.
    runtime.rebuild_multi_lookup_join(view).await.unwrap();
    assert_eq!(multi_lookup_rows(runtime, output).await, expected);

    fixture.cleanup().await;
}
