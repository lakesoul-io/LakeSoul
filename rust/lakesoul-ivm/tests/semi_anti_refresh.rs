// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! Tests for `SEMI`/`ANTI` join views: the affected left rows are recomputed
//! against both current source states, so updates, deletes and join key
//! changes on either side are reflected.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, SemiAntiView,
    semi_anti_mv_schema,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn left_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("x", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn right_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("rk", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
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

/// The current state of the view as `left key -> (join key, payload)`.
async fn view_state(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<i64, (i64, i64)> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let key_index = schema.index_of("k").unwrap();
        let join_index = schema.index_of("g").unwrap();
        let payload_index = schema.index_of("x").unwrap();
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap();
        let keys = batch
            .column(key_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let joins = batch
            .column(join_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payloads = batch
            .column(payload_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                state.insert(keys.value(row), (joins.value(row), payloads.value(row)));
            }
        }
    }
    state
}

/// Full `SEMI`/`ANTI` result over the current source states.
async fn full_result(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
    anti: bool,
) -> HashMap<i64, (i64, i64)> {
    let context = SessionContext::new();
    let left_table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
        MemTable::try_new(
            left.schema.clone(),
            vec![left.read_current(runtime.client()).await.unwrap()],
        )
        .unwrap(),
    );
    let right_table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
        MemTable::try_new(
            right.schema.clone(),
            vec![right.read_current(runtime.client()).await.unwrap()],
        )
        .unwrap(),
    );
    context.register_table("l", left_table).unwrap();
    context.register_table("r", right_table).unwrap();
    let predicate = if anti { "not exists" } else { "exists" };
    let frame = context
        .sql(&format!(
            "select l.k, l.g, l.x from l where l.\"{CHANGE_COLUMN}\" != 'delete' and {predicate} \
             (select 1 from r where r.\"{CHANGE_COLUMN}\" != 'delete' and r.g = l.g)"
        ))
        .await
        .unwrap();
    let mut state = HashMap::new();
    for batch in frame.collect().await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let joins = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let payloads = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            state.insert(keys.value(row), (joins.value(row), payloads.value(row)));
        }
    }
    state
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    left: IvmTable,
    right: IvmTable,
    mv: IvmTable,
    view: SemiAntiView,
    names: (String, String, String),
}

async fn setup(anti: bool) -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_semi_left_{suffix}"),
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
                format!("ivm_semi_right_{suffix}"),
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
                format!("ivm_semi_mv_{suffix}"),
                table_path(&dir, "mv"),
                semi_anti_mv_schema(&left.schema),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let view = SemiAntiView::new(
        format!("semi_{suffix}"),
        left.clone(),
        right.clone(),
        mv.clone(),
        vec!["g".to_string()],
        anti,
    );
    let names = (
        left.table_name.clone(),
        right.table_name.clone(),
        mv.table_name.clone(),
    );
    Fixture {
        runtime,
        dir,
        left,
        right,
        mv,
        view,
        names,
    }
}

impl Fixture {
    async fn cleanup(self) {
        for name in [self.names.0, self.names.1, self.names.2] {
            self.runtime
                .client()
                .drop_table(&name, "default")
                .await
                .unwrap();
        }
        drop(self.dir);
    }
}

#[test_log::test(tokio::test)]
async fn semi_join_same_window_versions_use_the_last() {
    // Two versions of one key in a single window: the second is the current
    // row, so the left filter (`x > 10`) excludes it.
    let mut fixture = setup(false).await;
    fixture.view.left_filter = Some("x > 10".to_string());
    let runtime = &fixture.runtime;
    let (left, right, mv, view) =
        (&fixture.left, &fixture.right, &fixture.mv, &fixture.view);

    left.append_batch(
        runtime.client(),
        left_batch(&[
            (5, 2, 48, "insert"),
            (5, 2, 3, "insert"),
            (6, 2, 7, "insert"),
            (6, 2, 50, "insert"),
        ]),
    )
    .await
    .unwrap();
    right
        .append_batch(runtime.client(), right_batch(&[(100, 2, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(view_state(runtime, mv).await, HashMap::from([(6, (2, 50))]));

    // A later right-only window reads the left rows from their side state,
    // which must hold the same last version of each key.
    right
        .append_batch(runtime.client(), right_batch(&[(101, 2, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(view_state(runtime, mv).await, HashMap::from([(6, (2, 50))]));

    fixture.cleanup().await;
}

#[test_log::test(tokio::test)]
async fn semi_join_recomputes_affected_rows() {
    let fixture = setup(false).await;
    let runtime = &fixture.runtime;
    let (left, right, mv, view) =
        (&fixture.left, &fixture.right, &fixture.mv, &fixture.view);

    // No matches yet.
    left.append_batch(
        runtime.client(),
        left_batch(&[
            (1, 1, 10, "insert"),
            (2, 1, 20, "insert"),
            (3, 2, 30, "insert"),
        ]),
    )
    .await
    .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert!(view_state(runtime, mv).await.is_empty());

    // A right row matches the first two left rows.
    right
        .append_batch(runtime.client(), right_batch(&[(100, 1, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(
        view_state(runtime, mv).await,
        HashMap::from([(1, (1, 10)), (2, (1, 20))])
    );

    // A new left row with a match is added.
    left.append_batch(runtime.client(), left_batch(&[(4, 1, 40, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(
        view_state(runtime, mv).await,
        HashMap::from([(1, (1, 10)), (2, (1, 20)), (4, (1, 40))])
    );

    // A payload update rewrites the row.
    left.append_batch(runtime.client(), left_batch(&[(2, 1, 25, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(
        view_state(runtime, mv).await,
        HashMap::from([(1, (1, 10)), (2, (1, 25)), (4, (1, 40))])
    );

    // Removing the right row retracts every match.
    right
        .append_batch(runtime.client(), right_batch(&[(100, 1, "delete")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert!(view_state(runtime, mv).await.is_empty());
    assert_eq!(
        view_state(runtime, mv).await,
        full_result(runtime, left, right, false).await
    );

    // A right row again, then deleting a left row removes only that row.
    right
        .append_batch(runtime.client(), right_batch(&[(101, 1, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(
        view_state(runtime, mv).await,
        HashMap::from([(1, (1, 10)), (2, (1, 25)), (4, (1, 40))])
    );
    left.append_batch(runtime.client(), left_batch(&[(1, 1, 10, "delete")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(
        view_state(runtime, mv).await,
        HashMap::from([(2, (1, 25)), (4, (1, 40))])
    );
    assert_eq!(
        view_state(runtime, mv).await,
        full_result(runtime, left, right, false).await
    );

    // Replaying the last window (left side) changes nothing.
    let mut cursors = runtime
        .metadata()
        .list_cursors(&view.view_id)
        .await
        .unwrap();
    cursors.sort_by_key(|cursor| cursor.last_version);
    let last = cursors.last().unwrap().clone();
    runtime
        .metadata()
        .upsert_cursor(
            &view.view_id,
            &last.source_table_id,
            &last.partition_desc,
            last.last_version - 1,
            0,
        )
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(
        view_state(runtime, mv).await,
        full_result(runtime, left, right, false).await
    );

    // A rebuild recomputes the full result.
    runtime.rebuild_semi_anti(view).await.unwrap();
    assert_eq!(
        view_state(runtime, mv).await,
        full_result(runtime, left, right, false).await
    );

    fixture.cleanup().await;
}

#[test_log::test(tokio::test)]
async fn anti_join_tracks_matches_appearing_and_disappearing() {
    let fixture = setup(true).await;
    let runtime = &fixture.runtime;
    let (left, right, mv, view) =
        (&fixture.left, &fixture.right, &fixture.mv, &fixture.view);

    left.append_batch(
        runtime.client(),
        left_batch(&[
            (1, 1, 10, "insert"),
            (2, 1, 20, "insert"),
            (3, 2, 30, "insert"),
        ]),
    )
    .await
    .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(
        view_state(runtime, mv).await,
        HashMap::from([(1, (1, 10)), (2, (1, 20)), (3, (2, 30))])
    );

    // Matching rows disappear from the ANTI output.
    right
        .append_batch(runtime.client(), right_batch(&[(100, 1, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(view_state(runtime, mv).await, HashMap::from([(3, (2, 30))]));
    assert_eq!(
        view_state(runtime, mv).await,
        full_result(runtime, left, right, true).await
    );

    // Removing the match brings them back.
    right
        .append_batch(runtime.client(), right_batch(&[(100, 1, "delete")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    assert_eq!(
        view_state(runtime, mv).await,
        HashMap::from([(1, (1, 10)), (2, (1, 20)), (3, (2, 30))])
    );
    assert_eq!(
        view_state(runtime, mv).await,
        full_result(runtime, left, right, true).await
    );

    fixture.cleanup().await;
}

#[test_log::test(tokio::test)]
async fn semi_join_with_differently_named_state_key() {
    // The left join key (`x`) and the right join key (`g`) have different
    // names while the right source is keyed by `rk`, so both side states exist.
    let fixture = setup(false).await;
    let runtime = &fixture.runtime;
    let (left, right, mv) = (&fixture.left, &fixture.right, &fixture.mv);
    let mut view = fixture.view.clone();
    view.join_keys = vec!["x".to_string()];
    view.right_keys = vec!["g".to_string()];

    // Both sides change in the first window.
    left.append_batch(runtime.client(), left_batch(&[(1, 10, 5, "insert")]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), right_batch(&[(100, 5, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap().unwrap();
    assert_eq!(view_state(runtime, mv).await, HashMap::from([(1, (10, 5))]));

    // The right key moves, and the left row moves with it in the same window.
    left.append_batch(runtime.client(), left_batch(&[(1, 10, 6, "insert")]))
        .await
        .unwrap();
    right
        .append_batch(
            runtime.client(),
            right_batch(&[(100, 5, "delete"), (100, 6, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap().unwrap();
    assert_eq!(view_state(runtime, mv).await, HashMap::from([(1, (10, 6))]));

    // The left key moves away with the right side unchanged.
    left.append_batch(runtime.client(), left_batch(&[(1, 10, 7, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap().unwrap();
    assert!(view_state(runtime, mv).await.is_empty());

    fixture.cleanup().await;
}

#[test_log::test(tokio::test)]
async fn semi_join_with_differently_named_right_key() {
    let fixture = setup(false).await;
    let runtime = &fixture.runtime;
    let (left, right, mv) = (&fixture.left, &fixture.right, &fixture.mv);
    let mut view = fixture.view.clone();
    view.right_keys = vec!["rk".to_string()];

    left.append_batch(runtime.client(), left_batch(&[(1, 10, 5, "insert")]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), right_batch(&[(10, 99, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap().unwrap();
    assert_eq!(view_state(runtime, mv).await, HashMap::from([(1, (10, 5))]));

    right
        .append_batch(runtime.client(), right_batch(&[(10, 99, "delete")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap().unwrap();
    assert!(view_state(runtime, mv).await.is_empty());
    fixture.cleanup().await;
}

#[test_log::test(tokio::test)]
async fn plain_semi_views_share_the_right_key_set() {
    // Two views read the same right source, keyed by the join key, so the
    // refresh can answer both from one per-window existence set.
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();

    let mut lefts = Vec::new();
    let mut mvs = Vec::new();
    for index in 0..2 {
        let left = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_shared_left_{index}_{suffix}"),
                    table_path(&dir, &format!("left{index}")),
                    left_schema(),
                )
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN),
            )
            .await
            .unwrap();
        let mv = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_shared_mv_{index}_{suffix}"),
                    table_path(&dir, &format!("mv{index}")),
                    semi_anti_mv_schema(&left.schema),
                )
                .with_primary_keys(vec!["k".to_string()]),
            )
            .await
            .unwrap();
        lefts.push(left);
        mvs.push(mv);
    }
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_shared_right_{suffix}"),
                table_path(&dir, "right"),
                right_schema(),
            )
            .with_primary_keys(vec!["g".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let views = lefts
        .iter()
        .zip(&mvs)
        .enumerate()
        .map(|(index, (left, mv))| {
            SemiAntiView::new(
                format!("ivm_shared_semi_{index}_{suffix}"),
                left.clone(),
                right.clone(),
                mv.clone(),
                vec!["g".to_string()],
                false,
            )
        })
        .collect::<Vec<_>>();

    lefts[0]
        .append_batch(
            runtime.client(),
            left_batch(&[(1, 1, 10, "insert"), (2, 2, 20, "insert")]),
        )
        .await
        .unwrap();
    lefts[1]
        .append_batch(
            runtime.client(),
            left_batch(&[(3, 1, 30, "insert"), (4, 3, 40, "insert")]),
        )
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), right_batch(&[(100, 1, "insert")]))
        .await
        .unwrap();

    for view in &views {
        runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    }
    assert_eq!(
        view_state(&runtime, &mvs[0]).await,
        HashMap::from([(1, (1, 10))])
    );
    assert_eq!(
        view_state(&runtime, &mvs[1]).await,
        HashMap::from([(3, (1, 30))])
    );

    // A right-only window is seen by both views through the same key set.
    right
        .append_batch(runtime.client(), right_batch(&[(101, 2, "insert")]))
        .await
        .unwrap();
    for view in &views {
        runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    }
    assert_eq!(
        view_state(&runtime, &mvs[0]).await,
        HashMap::from([(1, (1, 10)), (2, (2, 20))])
    );
    assert_eq!(
        view_state(&runtime, &mvs[1]).await,
        HashMap::from([(3, (1, 30))])
    );

    // A left-only window of one view re-probes from the empty right window
    // and still sees the current right state.
    lefts[0]
        .append_batch(runtime.client(), left_batch(&[(5, 2, 50, "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(&views[0]).await.unwrap().unwrap();
    assert_eq!(
        view_state(&runtime, &mvs[0]).await,
        HashMap::from([(1, (1, 10)), (2, (2, 20)), (5, (2, 50))])
    );

    // Removing the shared key retracts the rows of both views.
    right
        .append_batch(runtime.client(), right_batch(&[(100, 1, "delete")]))
        .await
        .unwrap();
    for view in &views {
        runtime.refresh_semi_anti(view).await.unwrap().unwrap();
    }
    assert_eq!(
        view_state(&runtime, &mvs[0]).await,
        full_result(&runtime, &lefts[0], &right, false).await
    );
    assert_eq!(
        view_state(&runtime, &mvs[1]).await,
        full_result(&runtime, &lefts[1], &right, false).await
    );
    assert!(view_state(&runtime, &mvs[1]).await.is_empty());

    for table in lefts
        .iter()
        .chain(mvs.iter())
        .chain(std::iter::once(&right))
    {
        runtime
            .client()
            .drop_table(&table.table_name, &table.namespace)
            .await
            .unwrap();
    }
    drop(dir);
}
