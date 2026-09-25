// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Epochs publish the LakeSoul commit ids of the MV writes, so consumers can
//! pin an epoch snapshot through the standard commit semantics.

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{IvmRuntime, IvmTableOptions, SumCountView, sum_count_mv_schema_for};
use std::sync::Arc;
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

fn assert_uuid(value: &str) {
    assert_eq!(value.len(), 36, "not a UUID: {value}");
    assert_eq!(value.matches('-').count(), 4, "not a UUID: {value}");
}

#[test_log::test(tokio::test)]
async fn epochs_publish_mv_commit_ids() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_commit_ids_src_{suffix}"),
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
                format!("ivm_commit_ids_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &group_keys, Some("amount"))
                    .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("commit_ids_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        Some("amount".to_string()),
    );
    runtime.register_view(&view).await.unwrap();
    let generation = runtime
        .metadata()
        .view_generation(&view.view_id)
        .await
        .unwrap();

    let mut seen = Vec::new();
    for round in 0..2i64 {
        source
            .append_batch(
                runtime.client(),
                source_batch(&[
                    (round * 2, "a", 10 + round),
                    (round * 2 + 1, "b", 20 + round),
                ]),
            )
            .await
            .unwrap();
        runtime.refresh_sum_count(&view).await.unwrap().unwrap();
        let record = runtime
            .metadata()
            .latest_committed_epoch(&view.view_id, generation)
            .await
            .unwrap()
            .unwrap();
        assert!(!record.commit_ids.is_empty(), "epoch has no commit ids");
        for id in &record.commit_ids {
            assert_uuid(id);
            assert!(!seen.contains(id), "commit id reused across epochs");
        }
        seen.extend(record.commit_ids.clone());
    }

    // A rebuild publishes the commit ids of its baseline write too. A rebuild
    // bumps the generation, so read the new one.
    runtime.rebuild_sum_count(&view).await.unwrap();
    let rebuilt_generation = runtime
        .metadata()
        .view_generation(&view.view_id)
        .await
        .unwrap();
    assert_eq!(rebuilt_generation, generation + 1);
    let rebuilt = runtime
        .metadata()
        .latest_committed_epoch(&view.view_id, rebuilt_generation)
        .await
        .unwrap()
        .unwrap();
    assert!(!rebuilt.commit_ids.is_empty(), "rebuild has no commit ids");
    for id in &rebuilt.commit_ids {
        assert_uuid(id);
        assert!(!seen.contains(id), "commit id reused by the rebuild");
    }

    // Pending epochs start without commit ids and get them on commit.
    source
        .append_batch(runtime.client(), source_batch(&[(9, "a", 30)]))
        .await
        .unwrap();
    let window = format!("unit-{}", uuid::Uuid::new_v4().simple());
    let record = match runtime
        .metadata()
        .begin_epoch(&view.view_id, &window, &[], &[])
        .await
        .unwrap()
    {
        lakesoul_ivm::BeginEpoch::Created(record) => record,
        other => panic!("unexpected epoch state: {other:?}"),
    };
    assert!(record.commit_ids.is_empty());
}
