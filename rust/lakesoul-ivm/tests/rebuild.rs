// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for rebuilds and consumer epoch pinning (`EPOCH.md` steps 4–5).

mod common;

use std::collections::HashMap;

use arrow_array::{Array, Int64Array};
use arrow_schema::SchemaRef;
use lakesoul_io::constant::DEFAULT_PARTITION_DESC;
use lakesoul_ivm::{
    IvmRuntime, IvmTable, IvmTableOptions, JoinView, SumCountView, join_view_schema_for,
    sum_count_mv_schema,
};
use tempfile::tempdir;

use common::{latest_version, mv_state, source_batch, source_schema, table_path};

struct SumCountFixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    view_id: String,
    source_name: String,
    mv_name: String,
    source: IvmTable,
    mv: IvmTable,
    view: SumCountView,
}

async fn setup_sum_count() -> SumCountFixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let view_id = format!("rebuild_sc_{suffix}");
    let source_name = format!("ivm_rebuild_src_{suffix}");
    let mv_name = format!("ivm_rebuild_mv_{suffix}");

    let source = runtime
        .create_table(IvmTableOptions::new(
            source_name.clone(),
            table_path(&dir, "src"),
            source_schema(),
        ))
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                mv_name.clone(),
                table_path(&dir, "mv"),
                sum_count_mv_schema("k"),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let view = SumCountView::new(
        view_id.clone(),
        source.clone(),
        mv.clone(),
        "k",
        Some("v".to_string()),
    );

    SumCountFixture {
        runtime,
        dir,
        view_id,
        source_name,
        mv_name,
        source,
        mv,
        view,
    }
}

fn join_source_schema() -> SchemaRef {
    source_schema()
}

fn join_source_batch(rows: &[(i64, i64)]) -> arrow_array::RecordBatch {
    source_batch(rows)
}

async fn join_rows(runtime: &IvmRuntime, output: &IvmTable) -> Vec<(i64, i64, i64)> {
    let mut rows = Vec::new();
    for batch in output.read_current(runtime.client()).await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), left.value(row), right.value(row)));
        }
    }
    rows.sort_unstable();
    rows
}

#[test_log::test(tokio::test)]
async fn rebuild_sum_count_recomputes_and_rebaselines() {
    let fixture = setup_sum_count().await;
    let (runtime, view_id, source_name, mv_name) = (
        &fixture.runtime,
        &fixture.view_id,
        &fixture.source_name,
        &fixture.mv_name,
    );
    let (source, mv, view) = (&fixture.source, &fixture.mv, &fixture.view);

    source
        .append_batch(runtime.client(), source_batch(&[(1, 10), (2, 5)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(view).await.unwrap().unwrap();

    // New commits are pending (as if the view was dirty), then rebuilt.
    source
        .append_batch(runtime.client(), source_batch(&[(1, 7), (3, 3)]))
        .await
        .unwrap();
    let epoch = runtime.rebuild_sum_count(view).await.unwrap();
    assert_ne!(epoch, 0);

    let expected = HashMap::from([(1, (17, 2)), (2, (5, 1)), (3, (3, 1))]);
    assert_eq!(mv_state(runtime, mv).await, expected);

    let record = runtime.latest_epoch(view_id).await.unwrap().unwrap();
    assert_eq!(record.epoch, epoch);
    assert_eq!(record.generation, 1);
    assert_eq!(record.window_key, "rebuild:1");
    assert_eq!(
        runtime
            .metadata()
            .view_status(view_id)
            .await
            .unwrap()
            .as_deref(),
        Some("active")
    );

    let cursors = runtime.metadata().list_cursors(view_id).await.unwrap();
    assert_eq!(cursors.len(), 1);
    assert_eq!(cursors[0].last_version, 1);
    assert_eq!(cursors[0].partition_desc, DEFAULT_PARTITION_DESC);

    // Nothing new: a refresh after the rebuild is a no-op.
    assert_eq!(runtime.refresh_sum_count(view).await.unwrap(), None);

    // And the next window only consumes the commits after the rebuild.
    source
        .append_batch(runtime.client(), source_batch(&[(1, 1)]))
        .await
        .unwrap();
    let next = runtime.refresh_sum_count(view).await.unwrap().unwrap();
    assert_ne!(next, epoch);
    assert_eq!(
        mv_state(runtime, mv).await,
        HashMap::from([(1, (18, 3)), (2, (5, 1)), (3, (3, 1))])
    );

    runtime
        .client()
        .drop_table(source_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(mv_name, "default")
        .await
        .unwrap();
    drop(fixture.dir);
}

#[test_log::test(tokio::test)]
async fn rebuild_join_recomputes_output() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let view_id = format!("rebuild_join_{suffix}");
    let left_name = format!("ivm_rebuild_left_{suffix}");
    let right_name = format!("ivm_rebuild_right_{suffix}");
    let output_name = format!("ivm_rebuild_join_{suffix}");

    let left = runtime
        .create_table(IvmTableOptions::new(
            left_name.clone(),
            table_path(&dir, "left"),
            join_source_schema(),
        ))
        .await
        .unwrap();
    let right = runtime
        .create_table(IvmTableOptions::new(
            right_name.clone(),
            table_path(&dir, "right"),
            join_source_schema(),
        ))
        .await
        .unwrap();
    let output = runtime
        .create_table(IvmTableOptions::new(
            output_name.clone(),
            table_path(&dir, "join"),
            join_view_schema_for(
                &left.schema,
                &right.schema,
                &["k".to_string()],
                "v",
                "v",
            )
            .unwrap(),
        ))
        .await
        .unwrap();
    let view = JoinView::new(
        view_id.clone(),
        left.clone(),
        right.clone(),
        output.clone(),
        "k",
        "v",
        "v",
    );

    left.append_batch(runtime.client(), join_source_batch(&[(1, 10)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), join_source_batch(&[(1, 100)]))
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap().unwrap();

    // New rows on both sides, then a rebuild.
    left.append_batch(runtime.client(), join_source_batch(&[(2, 20)]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), join_source_batch(&[(2, 200)]))
        .await
        .unwrap();
    let epoch = runtime.rebuild_join(&view).await.unwrap();
    assert_ne!(epoch, 0);
    assert_eq!(
        join_rows(&runtime, &output).await,
        vec![(1, 10, 100), (2, 20, 200)]
    );

    let record = runtime.latest_epoch(&view_id).await.unwrap().unwrap();
    assert_eq!(record.epoch, epoch);
    assert_eq!(record.window_key, "rebuild:1");
    let cursors = runtime.metadata().list_cursors(&view_id).await.unwrap();
    assert_eq!(cursors.len(), 2);
    assert!(cursors.iter().all(|cursor| cursor.last_version == 1));

    assert_eq!(runtime.refresh_join(&view).await.unwrap(), None);
    assert!(latest_version(&runtime, &output).await.is_some());

    runtime
        .client()
        .drop_table(&left_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&right_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&output_name, "default")
        .await
        .unwrap();
    drop(dir);
}

#[test_log::test(tokio::test)]
async fn view_state_at_epoch_pins_the_snapshot() {
    let fixture = setup_sum_count().await;
    let (runtime, view_id, source_name, mv_name) = (
        &fixture.runtime,
        &fixture.view_id,
        &fixture.source_name,
        &fixture.mv_name,
    );
    let (source, mv, view) = (&fixture.source, &fixture.mv, &fixture.view);

    source
        .append_batch(runtime.client(), source_batch(&[(1, 10)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(view).await.unwrap().unwrap();
    let first = runtime.latest_epoch(view_id).await.unwrap().unwrap();

    source
        .append_batch(runtime.client(), source_batch(&[(1, 7), (2, 5)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(view).await.unwrap().unwrap();
    let second = runtime.latest_epoch(view_id).await.unwrap().unwrap();
    assert!(second.epoch > first.epoch);

    let state_at_first =
        mv_state_batches(runtime.view_state_at_epoch(mv, &first).await.unwrap());
    assert_eq!(state_at_first, HashMap::from([(1, (10, 1))]));

    let state_at_second =
        mv_state_batches(runtime.view_state_at_epoch(mv, &second).await.unwrap());
    assert_eq!(state_at_second, HashMap::from([(1, (17, 2)), (2, (5, 1))]));

    runtime
        .client()
        .drop_table(source_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(mv_name, "default")
        .await
        .unwrap();
    drop(fixture.dir);
}

#[test_log::test(tokio::test)]
async fn rewind_then_rebuild_recovers() {
    let fixture = setup_sum_count().await;
    let (runtime, view_id, source_name, mv_name) = (
        &fixture.runtime,
        &fixture.view_id,
        &fixture.source_name,
        &fixture.mv_name,
    );
    let (source, mv, view) = (&fixture.source, &fixture.mv, &fixture.view);

    source
        .append_batch(runtime.client(), source_batch(&[(1, 10)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(view).await.unwrap().unwrap();
    source
        .append_batch(runtime.client(), source_batch(&[(1, 7)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(view).await.unwrap().unwrap();

    // A rewind that is not aligned with the last window is rejected...
    runtime
        .metadata()
        .upsert_cursor(view_id, &source.table_id, DEFAULT_PARTITION_DESC, -1, 0)
        .await
        .unwrap();
    assert!(runtime.refresh_sum_count(view).await.is_err());

    // ... and a rebuild recovers from the full source state.
    runtime.rebuild_sum_count(view).await.unwrap();
    assert_eq!(mv_state(runtime, mv).await, HashMap::from([(1, (17, 2))]));
    assert_eq!(runtime.refresh_sum_count(view).await.unwrap(), None);

    runtime
        .client()
        .drop_table(source_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(mv_name, "default")
        .await
        .unwrap();
    drop(fixture.dir);
}

fn mv_state_batches(batches: Vec<arrow_array::RecordBatch>) -> HashMap<i64, (i64, i64)> {
    let mut state = HashMap::new();
    for batch in batches {
        let schema = batch.schema();
        let key_index = schema.index_of("k").unwrap();
        let sum_index = schema.index_of(lakesoul_ivm::IVM_SUM_COLUMN).unwrap();
        let count_index = schema.index_of(lakesoul_ivm::IVM_COUNT_COLUMN).unwrap();
        let kind_index = schema.index_of(lakesoul_ivm::IVM_ROW_KINDS_COLUMN).unwrap();
        let keys = batch
            .column(key_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(sum_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(count_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                state.insert(keys.value(row), (sums.value(row), counts.value(row)));
            }
        }
    }
    state
}
