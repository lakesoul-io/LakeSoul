// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Consumer watermarks and epoch garbage collection (`EPOCH.md` §9).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    EpochStatus, IvmRuntime, IvmTable, IvmTableOptions, SumCountView,
    sum_count_mv_schema_for,
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
        Field::new("amount", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn source_batch(rows: &[(i64, &str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|_| "insert"))),
        ],
    )
    .unwrap()
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    source: IvmTable,
    mv: IvmTable,
    view: SumCountView,
    generation: i64,
}

async fn fixture() -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_gc_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let group_keys = vec!["g".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_gc_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &group_keys, Some("amount"))
                    .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("gc_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        Some("amount".to_string()),
    );
    let generation = runtime
        .metadata()
        .view_generation(&view.view_id)
        .await
        .unwrap_or(0);
    Fixture {
        runtime,
        dir,
        source,
        mv,
        view,
        generation,
    }
}

/// Append a row and refresh: one new epoch per call.
async fn advance(fixture: &Fixture, key: i64, group: &str, amount: i64) {
    fixture
        .source
        .append_batch(
            fixture.runtime.client(),
            source_batch(&[(key, group, amount)]),
        )
        .await
        .unwrap();
    fixture
        .runtime
        .refresh_sum_count(&fixture.view)
        .await
        .unwrap()
        .unwrap();
}

async fn committed_epochs(fixture: &Fixture) -> Vec<(i64, i64)> {
    // (epoch, generation)
    fixture
        .runtime
        .metadata()
        .list_committed_epochs(&fixture.view.view_id, fixture.generation)
        .await
        .unwrap()
        .into_iter()
        .map(|record| (record.epoch, record.generation))
        .collect()
}

/// The MV state as `g -> (sum, count)`.
async fn mv_state(fixture: &Fixture) -> HashMap<String, (i64, i64)> {
    let context = SessionContext::new();
    let batches = fixture
        .mv
        .read_current(fixture.runtime.client())
        .await
        .unwrap();
    let schema = fixture.mv.schema.clone();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap());
    context.register_table("mv", table).unwrap();
    let frame = context
        .sql("select g, sum_v, count_v from mv where \"rowKinds\" = 'insert'")
        .await
        .unwrap();
    let mut state = HashMap::new();
    for batch in frame.collect().await.unwrap() {
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
            state.insert(
                groups.value(row).to_string(),
                (sums.value(row), counts.value(row)),
            );
        }
    }
    state
}

#[test_log::test(tokio::test)]
async fn consumer_watermarks_are_tracked() {
    let fixture = fixture().await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;
    advance(&fixture, 1, "a", 10).await;
    advance(&fixture, 2, "a", 20).await;
    let epochs = committed_epochs(&fixture).await;
    let (first, _) = epochs[0];
    let (second, _) = epochs[1];
    assert!(first < second);

    runtime
        .register_consumer(&fixture.view.view_id, "c1", first)
        .await
        .unwrap();
    runtime
        .register_consumer(&fixture.view.view_id, "c2", second)
        .await
        .unwrap();
    assert_eq!(
        runtime
            .consumer_watermark(&fixture.view.view_id)
            .await
            .unwrap(),
        Some(first)
    );
    let consumers = runtime.list_consumers(&fixture.view.view_id).await.unwrap();
    assert_eq!(consumers.len(), 2);
    assert_eq!(consumers[0].consumer_id, "c1");
    assert_eq!(consumers[0].last_epoch, first);
    assert_eq!(consumers[1].consumer_id, "c2");

    // Moving a consumer forward moves the watermark; deleting the laggard
    // keeps the other.
    runtime
        .register_consumer(&fixture.view.view_id, "c1", second)
        .await
        .unwrap();
    assert_eq!(
        runtime
            .consumer_watermark(&fixture.view.view_id)
            .await
            .unwrap(),
        Some(second)
    );
    runtime
        .delete_consumer(&fixture.view.view_id, "c2")
        .await
        .unwrap();
    assert_eq!(
        runtime
            .consumer_watermark(&fixture.view.view_id)
            .await
            .unwrap(),
        Some(second)
    );
    assert_eq!(
        runtime
            .list_consumers(&fixture.view.view_id)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[test_log::test(tokio::test)]
async fn gc_deletes_committed_epochs_below_the_watermark() {
    let fixture = fixture().await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;
    advance(&fixture, 1, "a", 10).await;
    advance(&fixture, 2, "a", 20).await;
    advance(&fixture, 3, "b", 30).await;
    let epochs = committed_epochs(&fixture).await;
    assert_eq!(epochs.len(), 3);
    let latest = epochs[2].0;

    // Everything below the watermark goes.
    runtime
        .register_consumer(&fixture.view.view_id, "c", latest)
        .await
        .unwrap();
    assert_eq!(
        runtime.gc_epochs(&fixture.view.view_id, 0).await.unwrap(),
        2
    );
    assert_eq!(committed_epochs(&fixture).await, vec![epochs[2]]);

    // A new refresh still works and the MV stays correct: the cursors and
    // versions of the retained epoch are what the window bound needs.
    advance(&fixture, 4, "b", 40).await;
    let epochs = committed_epochs(&fixture).await;
    assert_eq!(epochs.len(), 2);
    let new_latest = epochs[1].0;
    assert_eq!(
        mv_state(&fixture).await,
        HashMap::from([("a".to_string(), (30, 2)), ("b".to_string(), (70, 2)),])
    );

    // The consumer still pinning the old latest keeps it.
    assert_eq!(
        runtime.gc_epochs(&fixture.view.view_id, 0).await.unwrap(),
        0
    );
    assert_eq!(committed_epochs(&fixture).await, vec![epochs[0], epochs[1]]);

    // Moving the consumer forward releases it, while its snapshot is still
    // readable from the recorded versions.
    let record = runtime
        .metadata()
        .latest_committed_epoch(&fixture.view.view_id, fixture.generation)
        .await
        .unwrap()
        .unwrap();
    assert!(
        !runtime
            .view_state_at_epoch(&fixture.mv, &record)
            .await
            .unwrap()
            .is_empty()
    );
    runtime
        .register_consumer(&fixture.view.view_id, "c", new_latest)
        .await
        .unwrap();
    assert_eq!(
        runtime.gc_epochs(&fixture.view.view_id, 0).await.unwrap(),
        1
    );
    assert_eq!(
        committed_epochs(&fixture).await,
        vec![(new_latest, fixture.generation)]
    );
}

#[test_log::test(tokio::test)]
async fn gc_respects_grace_and_keeps_pending_epochs() {
    let fixture = fixture().await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;
    advance(&fixture, 1, "a", 10).await;
    advance(&fixture, 2, "a", 20).await;
    advance(&fixture, 3, "a", 30).await;
    let records = runtime
        .metadata()
        .list_committed_epochs(&fixture.view.view_id, fixture.generation)
        .await
        .unwrap();
    let (first, middle, last) = (records[0].epoch, records[1].epoch, records[2].epoch);
    let middle_key = records[1].window_key.clone();

    // A pending epoch is never garbage collected, even below the watermark.
    runtime
        .metadata()
        .set_epoch_pending(&fixture.view.view_id, fixture.generation, middle)
        .await
        .unwrap();
    runtime
        .register_consumer(&fixture.view.view_id, "c", last)
        .await
        .unwrap();
    assert_eq!(
        runtime.gc_epochs(&fixture.view.view_id, 0).await.unwrap(),
        1
    );
    assert_eq!(
        committed_epochs(&fixture).await,
        vec![(last, fixture.generation)]
    );
    let pending = runtime
        .metadata()
        .get_epoch(&fixture.view.view_id, fixture.generation, &middle_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.status, EpochStatus::Pending);

    // Grace delays deletion by that many epochs.
    assert_eq!(
        runtime.gc_epochs(&fixture.view.view_id, 1).await.unwrap(),
        0
    );
    let _ = first;
    assert_eq!(
        runtime.gc_epochs(&fixture.view.view_id, 0).await.unwrap(),
        0
    );
}

#[test_log::test(tokio::test)]
async fn gc_without_consumers_keeps_history() {
    let fixture = fixture().await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;
    advance(&fixture, 1, "a", 10).await;
    advance(&fixture, 2, "a", 20).await;
    assert_eq!(committed_epochs(&fixture).await.len(), 2);
    assert_eq!(
        runtime.gc_epochs(&fixture.view.view_id, 0).await.unwrap(),
        0
    );
    assert_eq!(committed_epochs(&fixture).await.len(), 2);
}

#[test_log::test(tokio::test)]
async fn delete_view_removes_consumers() {
    let fixture = fixture().await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;
    advance(&fixture, 1, "a", 10).await;
    runtime
        .register_consumer(&fixture.view.view_id, "c", 1)
        .await
        .unwrap();
    assert!(
        runtime
            .consumer_watermark(&fixture.view.view_id)
            .await
            .unwrap()
            .is_some()
    );
    runtime
        .metadata()
        .delete_view(&fixture.view.view_id)
        .await
        .unwrap();
    assert!(
        runtime
            .list_consumers(&fixture.view.view_id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        runtime
            .consumer_watermark(&fixture.view.view_id)
            .await
            .unwrap()
            .is_none()
    );
}
