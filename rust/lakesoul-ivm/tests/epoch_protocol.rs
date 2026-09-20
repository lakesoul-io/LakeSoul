// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for the metadata-first epoch protocol (see `EPOCH.md`).
//!
//! They cover the crash windows around `ivm.epochs`: a pending window whose
//! data is already written is skipped based on the MV partition versions, a
//! pending window without data is applied with its pre-allocated epoch, and a
//! cursor rewind that does not align with a window boundary is rejected instead
//! of silently double counting.

mod common;

use lakesoul_io::constant::DEFAULT_PARTITION_DESC;
use lakesoul_ivm::{
    BeginEpoch, IvmRuntime, IvmTableOptions, SourceVersionRange, SumCountView,
    sum_count_mv_schema, window_key,
};
use tempfile::tempdir;

use common::{latest_version, mv_state, source_batch, source_schema, table_path};

/// Build a runtime, a source, an MV and the view; returns all of them.
async fn setup() -> (
    IvmRuntime,
    tempfile::TempDir,
    String,
    String,
    lakesoul_ivm::IvmTable,
    lakesoul_ivm::IvmTable,
    SumCountView,
) {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let view_id = format!("epoch_{suffix}");
    let source_name = format!("ivm_epoch_src_{suffix}");
    let mv_name = format!("ivm_epoch_mv_{suffix}");

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

    (runtime, dir, view_id, source_name, source, mv, view)
}

#[test_log::test(tokio::test)]
async fn pending_window_is_skipped_when_data_was_written() {
    let (runtime, dir, view_id, source_name, source, mv, view) = setup().await;

    source
        .append_batch(runtime.client(), source_batch(&[(1, 10)]))
        .await
        .unwrap();
    let epoch = runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    let state = mv_state(&runtime, &mv).await;
    let version = latest_version(&runtime, &mv).await;

    let records = runtime
        .metadata()
        .list_committed_epochs(&view_id, 0)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let record = records[0].clone();
    assert_eq!(record.epoch, epoch);

    // Simulate a crash between the MV commit and the epoch mark: rewind the
    // cursor and flip the epoch row back to pending.
    runtime
        .metadata()
        .upsert_cursor(&view_id, &source.table_id, DEFAULT_PARTITION_DESC, -1, 0)
        .await
        .unwrap();
    runtime
        .metadata()
        .set_epoch_pending(&view_id, record.generation, record.epoch)
        .await
        .unwrap();

    let replayed = runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(replayed, epoch);
    assert_eq!(mv_state(&runtime, &mv).await, state);
    assert_eq!(latest_version(&runtime, &mv).await, version);
    assert_eq!(
        runtime
            .metadata()
            .list_committed_epochs(&view_id, record.generation)
            .await
            .unwrap()
            .len(),
        1
    );

    runtime
        .client()
        .drop_table(&source_name, "default")
        .await
        .unwrap();
    drop(dir);
}

#[test_log::test(tokio::test)]
async fn pending_window_without_data_is_applied_with_its_epoch() {
    let (runtime, dir, view_id, source_name, source, mv, view) = setup().await;
    runtime.register_view(&view).await.unwrap();

    // Allocate the epoch of the first window before anything is written, as an
    // interrupted attempt between the pending insert and the data write would.
    let identity = vec![(
        source.table_id.clone(),
        DEFAULT_PARTITION_DESC.to_string(),
        -1_i64,
        0_i64,
    )];
    let key = window_key(&identity);
    let to_versions = vec![SourceVersionRange {
        source_table_id: source.table_id.clone(),
        partition_desc: DEFAULT_PARTITION_DESC.to_string(),
        from_version: -1,
        to_version: 0,
    }];
    let pending_epoch = match runtime
        .metadata()
        .begin_epoch(&view_id, &key, &to_versions, &[])
        .await
        .unwrap()
    {
        BeginEpoch::Created(record) => record.epoch,
        other => panic!("expected a fresh pending epoch, got {other:?}"),
    };

    source
        .append_batch(runtime.client(), source_batch(&[(1, 10), (2, 5)]))
        .await
        .unwrap();
    let applied = runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    assert_eq!(applied, pending_epoch);
    assert_eq!(
        mv_state(&runtime, &mv).await,
        std::collections::HashMap::from([(1, (10, 1)), (2, (5, 1))])
    );

    runtime
        .client()
        .drop_table(&source_name, "default")
        .await
        .unwrap();
    drop(dir);
}

#[test_log::test(tokio::test)]
async fn cursor_rewind_beyond_the_last_window_requires_rebuild() {
    let (runtime, dir, view_id, source_name, source, mv, view) = setup().await;

    source
        .append_batch(runtime.client(), source_batch(&[(1, 10)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    source
        .append_batch(runtime.client(), source_batch(&[(1, 7)]))
        .await
        .unwrap();
    runtime.refresh_sum_count(&view).await.unwrap().unwrap();
    let state = mv_state(&runtime, &mv).await;

    // Rewind two windows: the recomputed window (-1, 1] overlaps applied
    // history and must be rejected rather than double counted.
    runtime
        .metadata()
        .upsert_cursor(&view_id, &source.table_id, DEFAULT_PARTITION_DESC, -1, 0)
        .await
        .unwrap();
    let error = runtime.refresh_sum_count(&view).await.unwrap_err();
    assert!(
        error.to_string().contains("rebuild"),
        "unexpected error: {error}"
    );
    assert_eq!(mv_state(&runtime, &mv).await, state);

    runtime
        .client()
        .drop_table(&source_name, "default")
        .await
        .unwrap();
    drop(dir);
}
