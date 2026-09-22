// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Keyed inner join views (both sources have primary keys). The output is
//! keyed by the pair of row identities, so an upsert, a delete or a join-key
//! change on either side retracts and rewrites exactly the affected pairs
//! (`delete(old) + insert(current)`), while NULL join keys keep SQL equality
//! semantics and match nothing.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, JoinView,
    keyed_join_output_primary_keys, keyed_join_view_schema_for,
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

fn left_batch(rows: &[(i64, Option<&str>, Option<&str>, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        left_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn right_batch(rows: &[(i64, Option<&str>, Option<i64>, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        right_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

type JoinRow = (i64, i64, Option<String>, Option<String>, Option<i64>);

/// The current output pairs, i.e. the rows whose latest version is an insert.
async fn read_join_rows(runtime: &IvmRuntime, output: &IvmTable) -> Vec<JoinRow> {
    let mut rows = Vec::new();
    for batch in output.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let left_ids = batch
            .column(schema.index_of("__left_pk_id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_ids = batch
            .column(schema.index_of("__right_pk_rid").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = batch
            .column(schema.index_of("jk").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let left = batch
            .column(schema.index_of("left_value").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right = batch
            .column(schema.index_of("right_value").unwrap())
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
                rows.push((
                    left_ids.value(row),
                    right_ids.value(row),
                    if keys.is_null(row) {
                        None
                    } else {
                        Some(keys.value(row).to_string())
                    },
                    if left.is_null(row) {
                        None
                    } else {
                        Some(left.value(row).to_string())
                    },
                    if right.is_null(row) {
                        None
                    } else {
                        Some(right.value(row))
                    },
                ));
            }
        }
    }
    rows.sort();
    rows
}

/// The SQL semantics of the same inner join over the current source states.
async fn full_join_rows(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
) -> Vec<JoinRow> {
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
            "select l.id, r.rid, l.jk, l.lv, r.rv from l join r on l.jk = r.jk \
             where l.\"{CHANGE_COLUMN}\" <> 'delete' and r.\"{CHANGE_COLUMN}\" <> 'delete'"
        ))
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let schema = batch.schema();
        let left_ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right_ids = batch
            .column(schema.index_of("rid").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = batch
            .column(schema.index_of("jk").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let left = batch
            .column(schema.index_of("lv").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let right = batch
            .column(schema.index_of("rv").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                left_ids.value(row),
                right_ids.value(row),
                if keys.is_null(row) {
                    None
                } else {
                    Some(keys.value(row).to_string())
                },
                if left.is_null(row) {
                    None
                } else {
                    Some(left.value(row).to_string())
                },
                if right.is_null(row) {
                    None
                } else {
                    Some(right.value(row))
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
    view: JoinView,
}

async fn fixture() -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_join_l_{suffix}"),
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
                format!("ivm_keyed_join_r_{suffix}"),
                table_path(&dir, "right"),
                right_schema(),
            )
            .with_primary_keys(vec!["rid".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let join_keys = vec!["jk".to_string()];
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_join_out_{suffix}"),
                table_path(&dir, "out"),
                keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &["id".to_string()],
                    &["rid".to_string()],
                    &join_keys,
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
    let view = JoinView::new_with_join_keys(
        format!("keyed_join_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        join_keys,
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

async fn assert_matches_oracle(fixture: &Fixture) {
    assert_eq!(
        read_join_rows(&fixture.runtime, &fixture.output).await,
        full_join_rows(&fixture.runtime, &fixture.left, &fixture.right).await
    );
}

#[test_log::test(tokio::test)]
async fn keyed_join_tracks_both_sides() {
    let fixture = fixture().await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;

    // NULL join keys match nothing; the other rows pair up.
    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[
                (1, Some("a"), Some("L1"), "insert"),
                (2, Some("b"), Some("L2"), "insert"),
                (5, None, Some("L5"), "insert"),
            ]),
        )
        .await
        .unwrap();
    fixture
        .right
        .append_batch(
            runtime.client(),
            right_batch(&[
                (10, Some("a"), Some(100), "insert"),
                (11, Some("b"), Some(110), "insert"),
                (13, None, Some(130), "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        read_join_rows(runtime, &fixture.output).await,
        vec![
            (
                1,
                10,
                Some("a".to_string()),
                Some("L1".to_string()),
                Some(100)
            ),
            (
                2,
                11,
                Some("b".to_string()),
                Some("L2".to_string()),
                Some(110)
            ),
        ]
    );

    // A payload update rewrites the pair in place.
    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[(2, Some("b"), Some("L2x"), "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        read_join_rows(runtime, &fixture.output).await,
        vec![
            (
                1,
                10,
                Some("a".to_string()),
                Some("L1".to_string()),
                Some(100)
            ),
            (
                2,
                11,
                Some("b".to_string()),
                Some("L2x".to_string()),
                Some(110)
            ),
        ]
    );

    // Moving a left join key retracts the old pair and adds the new one.
    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[(1, Some("b"), Some("L1"), "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        read_join_rows(runtime, &fixture.output).await,
        vec![
            (
                1,
                11,
                Some("b".to_string()),
                Some("L1".to_string()),
                Some(110)
            ),
            (
                2,
                11,
                Some("b".to_string()),
                Some("L2x".to_string()),
                Some(110)
            ),
        ]
    );

    // Moving a right join key plus a new right row replaces the affected
    // pairs only.
    fixture
        .right
        .append_batch(
            runtime.client(),
            right_batch(&[
                (11, Some("c"), Some(110), "insert"),
                (12, Some("b"), Some(120), "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        read_join_rows(runtime, &fixture.output).await,
        vec![
            (
                1,
                12,
                Some("b".to_string()),
                Some("L1".to_string()),
                Some(120)
            ),
            (
                2,
                12,
                Some("b".to_string()),
                Some("L2x".to_string()),
                Some(120)
            ),
        ]
    );

    // Deleting a left row removes its pairs.
    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[(1, Some("b"), Some("L1"), "delete")]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        read_join_rows(runtime, &fixture.output).await,
        vec![(
            2,
            12,
            Some("b".to_string()),
            Some("L2x".to_string()),
            Some(120)
        )]
    );

    // A left join key that becomes NULL drops the pair.
    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[(2, None, Some("L2x"), "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert!(read_join_rows(runtime, &fixture.output).await.is_empty());

    // Deleting a right row and re-adding a different one keeps the left row
    // matched only through the new pair.
    fixture
        .right
        .append_batch(
            runtime.client(),
            right_batch(&[
                (12, Some("b"), Some(120), "delete"),
                (14, Some("b"), Some(140), "insert"),
            ]),
        )
        .await
        .unwrap();
    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[(2, Some("b"), Some("L2x"), "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        read_join_rows(runtime, &fixture.output).await,
        vec![(
            2,
            14,
            Some("b".to_string()),
            Some("L2x".to_string()),
            Some(140)
        )]
    );

    // Both sides move to a new common key in the same window: the two
    // recomputation directions overlap and the pair must be produced once.
    fixture
        .left
        .append_batch(
            runtime.client(),
            left_batch(&[(2, Some("z"), Some("L2z"), "insert")]),
        )
        .await
        .unwrap();
    fixture
        .right
        .append_batch(
            runtime.client(),
            right_batch(&[(14, Some("z"), Some(141), "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        read_join_rows(runtime, &fixture.output).await,
        vec![(
            2,
            14,
            Some("z".to_string()),
            Some("L2z".to_string()),
            Some(141)
        )]
    );

    // No new commits: the refresh is a no-op.
    assert!(runtime.refresh_join(&fixture.view).await.unwrap().is_none());

    // A rebuild over the full states gives the same result.
    runtime.rebuild_join(&fixture.view).await.unwrap();
    assert_matches_oracle(&fixture).await;
}

// ---------------------------------------------------------------------------
// Multiple join keys over keyed sources
// ---------------------------------------------------------------------------

async fn multi_fixture() -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_multi_l_{suffix}"),
                table_path(&dir, "left"),
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, false),
                    Field::new("k1", DataType::Utf8, true),
                    Field::new("k2", DataType::Int64, false),
                    Field::new("lv", DataType::Int64, true),
                    Field::new(CHANGE_COLUMN, DataType::Utf8, false),
                ])),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_multi_r_{suffix}"),
                table_path(&dir, "right"),
                Arc::new(Schema::new(vec![
                    Field::new("rid", DataType::Utf8, false),
                    Field::new("k1", DataType::Utf8, true),
                    Field::new("k2", DataType::Int64, false),
                    Field::new("rv", DataType::Utf8, true),
                    Field::new(CHANGE_COLUMN, DataType::Utf8, false),
                ])),
            )
            .with_primary_keys(vec!["rid".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let join_keys = vec!["k1".to_string(), "k2".to_string()];
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_multi_out_{suffix}"),
                table_path(&dir, "out"),
                keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &["id".to_string()],
                    &["rid".to_string()],
                    &join_keys,
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
    let view = JoinView::new_with_join_keys(
        format!("keyed_multi_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        join_keys,
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

type MultiLeftRow<'a> = (i64, Option<&'a str>, i64, Option<i64>, &'a str);

fn multi_left_batch(rows: &[MultiLeftRow<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("k1", DataType::Utf8, true),
            Field::new("k2", DataType::Int64, false),
            Field::new("lv", DataType::Int64, true),
            Field::new(CHANGE_COLUMN, DataType::Utf8, false),
        ])),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.4))),
        ],
    )
    .unwrap()
}

type MultiRightRow<'a> = (&'a str, Option<&'a str>, i64, Option<&'a str>, &'a str);

fn multi_right_batch(rows: &[MultiRightRow<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("rid", DataType::Utf8, false),
            Field::new("k1", DataType::Utf8, true),
            Field::new("k2", DataType::Int64, false),
            Field::new("rv", DataType::Utf8, true),
            Field::new(CHANGE_COLUMN, DataType::Utf8, false),
        ])),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.4))),
        ],
    )
    .unwrap()
}

async fn read_multi_rows(
    runtime: &IvmRuntime,
    output: &IvmTable,
) -> Vec<(
    i64,
    String,
    Option<String>,
    i64,
    Option<i64>,
    Option<String>,
)> {
    let mut rows = Vec::new();
    for batch in output.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of("__left_pk_id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rids = batch
            .column(schema.index_of("__right_pk_rid").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let k1 = batch
            .column(schema.index_of("k1").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let k2 = batch
            .column(schema.index_of("k2").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let lv = batch
            .column(schema.index_of("left_value").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let rv = batch
            .column(schema.index_of("right_value").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                rows.push((
                    ids.value(row),
                    rids.value(row).to_string(),
                    if k1.is_null(row) {
                        None
                    } else {
                        Some(k1.value(row).to_string())
                    },
                    k2.value(row),
                    if lv.is_null(row) {
                        None
                    } else {
                        Some(lv.value(row))
                    },
                    if rv.is_null(row) {
                        None
                    } else {
                        Some(rv.value(row).to_string())
                    },
                ));
            }
        }
    }
    rows.sort();
    rows
}

#[test_log::test(tokio::test)]
async fn keyed_join_multiple_keys() {
    let fixture = multi_fixture().await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;

    fixture
        .left
        .append_batch(
            runtime.client(),
            multi_left_batch(&[
                (1, Some("a"), 1, Some(10), "insert"),
                (2, Some("a"), 2, Some(20), "insert"),
                (3, None, 1, Some(30), "insert"),
            ]),
        )
        .await
        .unwrap();
    fixture
        .right
        .append_batch(
            runtime.client(),
            multi_right_batch(&[
                ("r1", Some("a"), 1, Some("R1"), "insert"),
                ("r2", Some("a"), 2, Some("R2"), "insert"),
                ("r3", None, 1, Some("R3"), "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_eq!(
        read_multi_rows(runtime, &fixture.output).await,
        vec![
            (
                1,
                "r1".to_string(),
                Some("a".to_string()),
                1,
                Some(10),
                Some("R1".to_string())
            ),
            (
                2,
                "r2".to_string(),
                Some("a".to_string()),
                2,
                Some(20),
                Some("R2".to_string())
            ),
        ]
    );

    // Changing one key column rewrites the pair.
    fixture
        .left
        .append_batch(
            runtime.client(),
            multi_left_batch(&[(1, Some("a"), 2, Some(11), "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&fixture.view).await.unwrap().unwrap();
    assert_eq!(
        read_multi_rows(runtime, &fixture.output).await,
        vec![
            (
                1,
                "r2".to_string(),
                Some("a".to_string()),
                2,
                Some(11),
                Some("R2".to_string())
            ),
            (
                2,
                "r2".to_string(),
                Some("a".to_string()),
                2,
                Some(20),
                Some("R2".to_string())
            ),
        ]
    );

    runtime.rebuild_join(&fixture.view).await.unwrap();
    assert_eq!(
        read_multi_rows(runtime, &fixture.output).await,
        vec![
            (
                1,
                "r2".to_string(),
                Some("a".to_string()),
                2,
                Some(11),
                Some("R2".to_string())
            ),
            (
                2,
                "r2".to_string(),
                Some("a".to_string()),
                2,
                Some(20),
                Some("R2".to_string())
            ),
        ]
    );
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

#[test_log::test(tokio::test)]
async fn keyed_join_validation() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();

    // A keyed left source with an append-only right source is rejected.
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_valid_l_{suffix}"),
                table_path(&dir, "left"),
                left_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_keyed_valid_r_{suffix}"),
            table_path(&dir, "right"),
            Arc::new(Schema::new(vec![
                Field::new("rid", DataType::Int64, false),
                Field::new("jk", DataType::Utf8, true),
                Field::new("rv", DataType::Int64, true),
            ])),
        ))
        .await
        .unwrap();
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_valid_out_{suffix}"),
                table_path(&dir, "out"),
                keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
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
    let view = JoinView::new(
        format!("keyed_valid_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        "jk",
        "lv",
        "rv",
    );
    let error = runtime.refresh_join(&view).await.unwrap_err();
    assert!(
        error.to_string().contains("both sources must be keyed"),
        "unexpected error: {error}"
    );

    // A nullable row identity is rejected (with a keyed right source, so the
    // mixed-source error does not fire first).
    let keyed_right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_valid_r2_{suffix}"),
                table_path(&dir, "right2"),
                right_schema(),
            )
            .with_primary_keys(vec!["rid".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let nullable_left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_nullpk_l_{suffix}"),
                table_path(&dir, "nullable_left"),
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int64, true),
                    Field::new("jk", DataType::Utf8, true),
                    Field::new("lv", DataType::Utf8, true),
                    Field::new(CHANGE_COLUMN, DataType::Utf8, false),
                ])),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let nullable_output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_keyed_nullpk_out_{suffix}"),
                table_path(&dir, "nullable_out"),
                keyed_join_view_schema_for(
                    &nullable_left.schema,
                    &keyed_right.schema,
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
    let nullable_view = JoinView::new(
        format!("keyed_nullpk_{suffix}"),
        nullable_left.clone(),
        keyed_right.clone(),
        nullable_output.clone(),
        "jk",
        "lv",
        "rv",
    );
    let error = runtime.refresh_join(&nullable_view).await.unwrap_err();
    assert!(
        error.to_string().contains("must be non-nullable"),
        "unexpected error: {error}"
    );

    // The keyed schema constructor also rejects a missing identity.
    assert!(
        keyed_join_view_schema_for(
            &left.schema,
            &right.schema,
            &[],
            &["rid".to_string()],
            &["jk".to_string()],
            "lv",
            "rv",
        )
        .is_err()
    );
}
