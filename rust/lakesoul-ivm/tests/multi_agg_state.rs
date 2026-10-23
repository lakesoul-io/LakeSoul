// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! Integration test for the multi-aggregate state table when both the state
//! table and the MV follow the target's range partition scheme.

use std::sync::Arc;

use arrow_array::{Array, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    IVM_GROUP_ROW_COLUMN, IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    MultiAggView, PhysicalFormat, multi_agg_mv_schema_for, multi_agg_state_schema_for,
};
use tempfile::tempdir;

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("stat_date", DataType::Date32, false),
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

fn source_batch(rows: &[(i64, i32, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Date32Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

async fn mv_values(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i32, i64, i64)> {
    let mut values = Vec::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let date = batch
            .column(schema.index_of("stat_date").unwrap())
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let key = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sum = batch
            .column(schema.index_of("v_sum").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kind = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kind.value(row) == "insert" {
                values.push((date.value(row), key.value(row), sum.value(row)));
            }
        }
    }
    values.sort_unstable();
    values
}

#[test_log::test(tokio::test)]
async fn partitioned_state_and_mv_only_refresh_the_changed_date() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source_name = format!("ivm_multi_state_src_{suffix}");
    let state_name = format!("ivm_multi_state_state_{suffix}");
    let mv_name = format!("ivm_multi_state_mv_{suffix}");

    let source = runtime
        .create_table(
            IvmTableOptions::new(
                source_name.clone(),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_bucket_columns(vec!["id".to_string()])
            .with_range_partition_columns(vec!["stat_date".to_string()])
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();

    let group_keys = vec!["stat_date".to_string(), "k".to_string()];
    let aggregates = vec![("SUM(v)".to_string(), "v_sum".to_string(), DataType::Int64)];
    let aggregate_columns = vec![("v_sum".to_string(), DataType::Int64)];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                mv_name.clone(),
                table_path(&dir, "mv"),
                multi_agg_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    &[],
                    &aggregate_columns,
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone())
            .with_bucket_columns(vec!["stat_date".to_string()])
            .with_range_partition_columns(vec!["stat_date".to_string()])
            .with_file_format(PhysicalFormat::Vortex),
        )
        .await
        .unwrap();
    let mut state_options = IvmTableOptions::new(
        state_name.clone(),
        table_path(&dir, "state"),
        multi_agg_state_schema_for(&source.schema, &group_keys, &aggregate_columns)
            .unwrap(),
    )
    .with_primary_keys(vec![IVM_GROUP_ROW_COLUMN.to_string()])
    .with_bucket_columns(vec![IVM_GROUP_ROW_COLUMN.to_string()])
    .with_range_partition_columns(vec!["stat_date".to_string()])
    .with_file_format(PhysicalFormat::Vortex);
    state_options.hash_bucket_num = "8".to_string();
    let state = runtime.create_table(state_options).await.unwrap();
    let view = MultiAggView::new(
        format!("ivm_multi_state_view_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        aggregates,
    );

    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, 0, 1, 10), (2, 0, 2, 20), (3, 1, 1, 30), (4, 1, 2, 40)]),
        )
        .await
        .unwrap();
    runtime
        .refresh_multi_agg_state(&view, &state)
        .await
        .unwrap();
    assert_eq!(
        mv_values(&runtime, &mv).await,
        vec![(0, 1, 10), (0, 2, 20), (1, 1, 30), (1, 2, 40)]
    );

    // A key that has no old state row exercises the null side of the keyed
    // lookup and the new-state insert path.
    source
        .append_batch(runtime.client(), source_batch(&[(5, 0, 5, 50)]))
        .await
        .unwrap();
    runtime
        .refresh_multi_agg_state(&view, &state)
        .await
        .unwrap();
    assert_eq!(
        mv_values(&runtime, &mv).await,
        vec![(0, 1, 10), (0, 2, 20), (0, 5, 50), (1, 1, 30), (1, 2, 40)]
    );

    // Update a row in the first date only.  The second state/MV partition must
    // stay untouched, while the first one is recomputed from the delta.
    source
        .append_batch(runtime.client(), source_batch(&[(1, 0, 1, 7)]))
        .await
        .unwrap();
    runtime
        .refresh_multi_agg_state(&view, &state)
        .await
        .unwrap();
    assert_eq!(
        mv_values(&runtime, &mv).await,
        vec![(0, 1, 7), (0, 2, 20), (0, 5, 50), (1, 1, 30), (1, 2, 40)]
    );

    // Compaction is an out-of-band maintenance step.  It must preserve both
    // state and MV, and the next keyed refresh must still find the state rows.
    state.compact(runtime.client()).await.unwrap();
    mv.compact(runtime.client()).await.unwrap();
    assert_eq!(
        mv_values(&runtime, &mv).await,
        vec![(0, 1, 7), (0, 2, 20), (0, 5, 50), (1, 1, 30), (1, 2, 40)]
    );
    source
        .append_batch(runtime.client(), source_batch(&[(2, 0, 2, 9)]))
        .await
        .unwrap();
    runtime
        .refresh_multi_agg_state(&view, &state)
        .await
        .unwrap();
    assert_eq!(
        mv_values(&runtime, &mv).await,
        vec![(0, 1, 7), (0, 2, 9), (0, 5, 50), (1, 1, 30), (1, 2, 40)]
    );

    let state_partitions = runtime
        .client()
        .get_all_partition_info(&state.table_id)
        .await
        .unwrap();
    assert_eq!(state_partitions.len(), 2);
    let mv_partitions = runtime
        .client()
        .get_all_partition_info(&mv.table_id)
        .await
        .unwrap();
    assert_eq!(mv_partitions.len(), 2);

    runtime
        .client()
        .drop_table(&source_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&state_name, "default")
        .await
        .unwrap();
    runtime
        .client()
        .drop_table(&mv_name, "default")
        .await
        .unwrap();
}
