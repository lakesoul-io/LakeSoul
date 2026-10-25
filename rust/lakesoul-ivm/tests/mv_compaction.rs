// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Opt-in compaction of a materialized-view output.

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::util::display::array_value_to_string;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{IvmRuntime, IvmTableOptions, StateRole};
use std::sync::Arc;
use tempfile::tempdir;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn batch(rows: &[(i64, i64)]) -> RecordBatch {
    arrow::record_batch::RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
        ],
    )
    .unwrap()
}

#[test_log::test(tokio::test)]
async fn opt_in_mv_compaction_preserves_rows_and_bounds_files() {
    // SAFETY: this test binary contains only this test; the threshold is not
    // observed by any other process.
    unsafe {
        std::env::set_var("LAKESOUL_IVM_MV_COMPACT_FILES", "2");
    }
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let name = format!("ivm_mv_compact_{suffix}");
    let mut options = IvmTableOptions::new(
        name.clone(),
        format!("file://{}/mv", dir.path().display()),
        schema(),
    )
    .with_primary_keys(vec!["k".to_string()])
    .with_bucket_columns(vec!["k".to_string()]);
    options.hash_bucket_num = "4".to_string();
    let mv = runtime.create_table(options).await.unwrap();
    let view_id = format!("view_{suffix}");
    runtime
        .metadata()
        .register_state(&view_id, StateRole::Mv, &mv)
        .await
        .unwrap();

    for round in 0..3 {
        mv.append_batch(
            runtime.client(),
            batch(&[(1, round), (2, round + 1)]),
        )
        .await
        .unwrap();
    }
    assert!(mv.current_file_count(runtime.client()).await.unwrap() >= 2);
    runtime.compact_view_states(&view_id).await.unwrap();
    assert!(mv.current_file_count(runtime.client()).await.unwrap() <= 4);

    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let batches = context
        .sql("select k, v from mv order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let rows = batches
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(move |row| {
                (
                    array_value_to_string(batch.column(0), row).unwrap(),
                    array_value_to_string(batch.column(1), row).unwrap(),
                )
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![("1".into(), "2".into()), ("2".into(), "3".into())]);
}
