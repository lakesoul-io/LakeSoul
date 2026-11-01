// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Range-partitioned source tables: the partition values are injected on every
//! read (changelog window, current state, before state and rebuild baseline),
//! so filters, group keys and joins see the partition columns exactly like the
//! DataFusion provider does.

use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions,
    JoinView, SemiAntiView, StateRole, keyed_join_output_primary_keys,
    keyed_join_view_schema_for, multi_agg_mv_schema_for, row_mv_schema_for,
    semi_anti_mv_schema_for, sum_count_mv_schema_for,
};
use lakesoul_metadata_proto::entity::{CommitOp, MetaInfo, PartitionInfo};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

/// `(k, g, v, day, op)`: `day` is the range partition column; the data files
/// written for the table do not contain it.
fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new("day", DataType::Utf8, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn source_schema_with_day() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("day", DataType::Utf8, false),
        Field::new("lk", DataType::Int64, false),
        Field::new("lv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn source_batch_with_day(rows: &[(i64, &str, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema_with_day(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.3))),
            Arc::new(StringArray::from_iter_values(
                (0..rows.len()).map(|_| "insert"),
            )),
        ],
    )
    .unwrap()
}

fn source_batch(rows: &[(i64, &str, i64, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.4))),
        ],
    )
    .unwrap()
}

async fn create_source(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    suffix: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_src_{suffix}"),
                table_path(dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap()
}

/// The logical `(day, g) -> sum_v` rows of a sum/count MV.
async fn read_day_groups(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(String, String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select day, g, sum_v from mv order by day, g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let days = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let groups = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                days.value(row).to_string(),
                groups.value(row).to_string(),
                values.value(row),
            ));
        }
    }
    rows
}

/// An aggregate grouped by the partition column crosses refreshes, including a
/// staggered cursor (one partition changes while another keeps an older one)
/// and a rebuild from the partitioned baseline.
#[test_log::test(tokio::test)]
async fn grouped_aggregate_over_partitioned_source() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(
                    &source.schema,
                    &["day".to_string(), "g".to_string()],
                    Some("v"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["day".to_string(), "g".to_string()]),
        )
        .await
        .unwrap();

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "d1", "insert"),
                (2, "a", 20, "d1", "insert"),
                (3, "b", 5, "d2", "insert"),
            ]),
        )
        .await
        .unwrap();

    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_mv_{suffix} \
         SELECT day, g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_part_src_{suffix} GROUP BY day, g"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 30),
            ("d2".to_string(), "b".to_string(), 5),
        ]
    );

    // A delete in one partition, an update in another and a new partition.
    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[
                (2, "a", 20, "d1", "delete"),
                (3, "b", 9, "d2", "update"),
                (4, "c", 7, "d3", "insert"),
            ]),
        )
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 10),
            ("d2".to_string(), "b".to_string(), 9),
            ("d3".to_string(), "c".to_string(), 7),
        ]
    );

    // Staggered cursors: only d1 changes, then only d2 changes; the before
    // state of the untouched partition is read from its own pinned version.
    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[(5, "a", 1, "d1", "insert")]),
        )
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 11),
            ("d2".to_string(), "b".to_string(), 9),
            ("d3".to_string(), "c".to_string(), 7),
        ]
    );
    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[(6, "b", 2, "d2", "insert")]),
        )
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 11),
            ("d2".to_string(), "b".to_string(), 11),
            ("d3".to_string(), "c".to_string(), 7),
        ]
    );

    // The overwrite recomputes from the full partitioned baseline.
    executor
        .execute(&format!(
            "INSERT OVERWRITE ivm_part_mv_{suffix} \
             SELECT day, g, SUM(v) AS sum_v, COUNT(*) AS count_v \
             FROM ivm_part_src_{suffix} GROUP BY day, g"
        ))
        .await
        .unwrap();
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 11),
            ("d2".to_string(), "b".to_string(), 11),
            ("d3".to_string(), "c".to_string(), 7),
        ]
    );
}

/// A row view filters on the partition column; the filter only sees the value
/// when the read injects it.
async fn read_row_tuples(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(i64, String, String)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select k, g, day from mv order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let groups = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let days = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                groups.value(row).to_string(),
                days.value(row).to_string(),
            ));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn row_view_filters_on_partition_column() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let output_columns = ["k", "g", "v", "day"].map(String::from);
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_row_{suffix}"),
                table_path(&dir, "row"),
                row_mv_schema_for(&source.schema, &output_columns).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "d1", "insert"),
                (2, "b", 20, "d2", "insert"),
                (3, "c", 30, "d2", "insert"),
            ]),
        )
        .await
        .unwrap();

    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_row_{suffix} \
         SELECT k, g, v, day FROM ivm_part_src_{suffix} WHERE day = 'd2'"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_row_tuples(executor.runtime(), &mv).await,
        vec![
            (2, "b".to_string(), "d2".to_string()),
            (3, "c".to_string(), "d2".to_string()),
        ]
    );

    // A delete in d2, a new d2 row and a new d1 row that the filter drops.
    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[
                (2, "b", 20, "d2", "delete"),
                (4, "d", 40, "d2", "insert"),
                (5, "e", 50, "d1", "insert"),
            ]),
        )
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_row_tuples(executor.runtime(), &mv).await,
        vec![
            (3, "c".to_string(), "d2".to_string()),
            (4, "d".to_string(), "d2".to_string()),
        ]
    );

    executor
        .execute(&format!(
            "INSERT OVERWRITE ivm_part_row_{suffix} \
             SELECT k, g, v, day FROM ivm_part_src_{suffix} WHERE day = 'd2'"
        ))
        .await
        .unwrap();
    assert_eq!(
        read_row_tuples(executor.runtime(), &mv).await,
        vec![
            (3, "c".to_string(), "d2".to_string()),
            (4, "d".to_string(), "d2".to_string()),
        ]
    );
}

fn dim_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("label", DataType::Utf8, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn dim_batch(rows: &[(i64, &str, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        dim_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

/// The join pairs `(k, left_value, right_value)` whose latest version is an
/// insert.
async fn read_join_pairs(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, i64, String)> {
    let mut rows = Vec::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of("__left_pk_k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let left = batch
            .column(schema.index_of("left_value").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
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
                    left.value(row),
                    right.value(row).to_string(),
                ));
            }
        }
    }
    rows.sort_unstable();
    rows
}

#[test_log::test(tokio::test)]
async fn keyed_join_reads_partitioned_left() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = create_source(&runtime, &dir, &suffix).await;
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_dim_{suffix}"),
                table_path(&dir, "dim"),
                dim_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let output = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_join_{suffix}"),
                table_path(&dir, "join"),
                keyed_join_view_schema_for(
                    &left.schema,
                    &right.schema,
                    &["k".to_string()],
                    &["k".to_string()],
                    &["k".to_string()],
                    "v",
                    "label",
                )
                .unwrap(),
            )
            .with_primary_keys(keyed_join_output_primary_keys(
                &["k".to_string()],
                &["k".to_string()],
            )),
        )
        .await
        .unwrap();
    let view = JoinView::new_with_join_keys(
        format!("ivm_part_join_view_{suffix}"),
        left.clone(),
        right.clone(),
        output.clone(),
        vec!["k".to_string()],
        "v",
        "label",
    );

    left.append_batch(
        runtime.client(),
        source_batch(&[(1, "a", 10, "d1", "insert"), (2, "b", 20, "d2", "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            dim_batch(&[(1, "A", "insert"), (2, "B", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_join(&view).await.unwrap();
    assert_eq!(
        read_join_pairs(&runtime, &output).await,
        vec![(1, 10, "A".to_string()), (2, 20, "B".to_string()),]
    );

    // Update the d2 row and delete the d1 row: the delta of the partitioned
    // left side carries its partition values through the join.
    left.append_batch(
        runtime.client(),
        source_batch(&[(2, "b", 25, "d2", "update"), (1, "a", 10, "d1", "delete")]),
    )
    .await
    .unwrap();
    runtime.refresh_join(&view).await.unwrap();
    assert_eq!(
        read_join_pairs(&runtime, &output).await,
        vec![(2, 25, "B".to_string())]
    );
}

/// Drop one source partition (a delete commit); `needle` selects it by name.
async fn drop_partition(runtime: &IvmRuntime, source: &IvmTable, needle: &str) {
    let partitions = runtime
        .client()
        .get_all_partition_info(&source.table_id)
        .await
        .unwrap();
    let dropped = partitions
        .iter()
        .find(|partition| partition.partition_desc.contains(needle))
        .expect("the dropped partition exists");
    let files = runtime
        .client()
        .get_data_files_of_single_partition(dropped)
        .await
        .unwrap();
    assert!(!files.is_empty(), "the dropped partition has files");
    let table_info = runtime
        .client()
        .get_table_info_by_table_id(&source.table_id)
        .await
        .unwrap()
        .expect("the source table is registered");
    runtime
        .client()
        .commit_data(
            MetaInfo {
                table_info: Some(table_info),
                list_partition: vec![PartitionInfo {
                    table_id: source.table_id.clone(),
                    partition_desc: dropped.partition_desc.clone(),
                    commit_op: CommitOp::DeleteCommit.into(),
                    ..Default::default()
                }],
                read_partition_info: vec![PartitionInfo {
                    table_id: source.table_id.clone(),
                    partition_desc: dropped.partition_desc.clone(),
                    ..Default::default()
                }],
            },
            CommitOp::DeleteCommit,
        )
        .await
        .unwrap();
}

/// `(day, v)` rows of an MV ordered by day.
async fn read_day_values(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select day, v from mv order by day")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let days = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((days.value(row).to_string(), values.value(row)));
        }
    }
    rows
}

/// A range-partitioned MV: the refresh writes every output partition into its
/// own directory and reads inject the partition values back.
#[test_log::test(tokio::test)]
async fn a_partitioned_mv_materializes_its_partitions() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pmv_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(
                    &source.schema,
                    &["day".to_string(), "g".to_string()],
                    Some("v"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["day".to_string(), "g".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "d1", "insert"), (2, "b", 5, "d2", "insert")]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_pmv_mv_{suffix} \
         SELECT day, g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_part_src_{suffix} GROUP BY day, g"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_day_groups(runtime, &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 10),
            ("d2".to_string(), "b".to_string(), 5),
        ]
    );

    // The MV is stored as one data partition per day.
    let mut descs = runtime
        .client()
        .get_all_partition_info(&mv.table_id)
        .await
        .unwrap()
        .into_iter()
        .map(|partition| partition.partition_desc)
        .collect::<Vec<_>>();
    descs.sort();
    assert_eq!(descs, vec!["day=d1".to_string(), "day=d2".to_string()]);

    // An update and a new row in d1 and a delete in d2 flow through the
    // partitioned write; the d2 group disappears.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 12, "d1", "insert"),
                (3, "c", 7, "d1", "insert"),
                (2, "b", 5, "d2", "delete"),
            ]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_day_groups(runtime, &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 12),
            ("d1".to_string(), "c".to_string(), 7),
        ]
    );
}

/// A multi-aggregate row state over a partitioned source: the state mirrors
/// the source partition column (it is one of the group keys), so a window
/// that only touches one day reads and writes that day's state.
#[test_log::test(tokio::test)]
async fn multi_agg_row_state_mirrors_source_partitions() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let groups = vec!["day".to_string(), "g".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_agg_mv_{suffix}"),
                table_path(&dir, "mv"),
                multi_agg_mv_schema_for(
                    &source.schema,
                    &groups,
                    &[],
                    &[
                        ("cnt".to_string(), DataType::Int64),
                        ("mx".to_string(), DataType::Int64),
                    ],
                )
                .unwrap(),
            )
            .with_primary_keys(groups.clone())
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "d1", "insert"),
                (2, "b", 5, "d1", "insert"),
                (3, "a", 7, "d2", "insert"),
                (4, "c", 1, "d3", "insert"),
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_agg_mv_{suffix}          SELECT day, g, COUNT(*) AS cnt, MAX(v) AS mx          FROM ivm_part_src_{suffix} GROUP BY day, g"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_multi_agg(runtime, &mv).await,
        full_multi_agg(runtime, &source).await
    );

    // The row state is range-partitioned by the source's day column.  The
    // statement creates a derived aggregate view behind a sink, so the state
    // belongs to the derived view rather than to the sink view id.
    let mut row_state = None;
    for view in runtime.metadata().list_views().await.unwrap() {
        for state in runtime.metadata().list_states(&view.view_id).await.unwrap() {
            if state.role == StateRole::MultiAggRows
                && state.table_name.starts_with(&mv.table_name)
            {
                row_state = Some(state);
            }
        }
    }
    let state = row_state.expect("the multi-agg row state exists");
    let state = runtime.open_table_by_id(&state.table_id).await.unwrap();
    assert_eq!(state.range_partition_columns, vec!["day".to_string()]);
    let mut descs = runtime
        .client()
        .get_all_partition_info(&state.table_id)
        .await
        .unwrap()
        .into_iter()
        .map(|partition| partition.partition_desc)
        .collect::<Vec<_>>();
    descs.sort();
    assert_eq!(
        descs,
        vec![
            "day=d1".to_string(),
            "day=d2".to_string(),
            "day=d3".to_string()
        ]
    );

    // Only d1 changes: the state lookup prunes to d1 and the untouched days
    // keep their state.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 12, "d1", "insert"), (2, "b", 5, "d1", "delete")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_multi_agg(executor.runtime(), &mv).await,
        full_multi_agg(executor.runtime(), &source).await
    );
}

async fn read_multi_agg(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(String, String, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select day, g, cnt, mx from mv order by day, g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let days = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let groups = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let maxima = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                days.value(row).to_string(),
                groups.value(row).to_string(),
                counts.value(row),
                maxima.value(row),
            ));
        }
    }
    rows
}

async fn full_multi_agg(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> Vec<(String, String, i64, i64)> {
    let context = SessionContext::new();
    let table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
        datafusion::datasource::memory::MemTable::try_new(
            source.schema.clone(),
            vec![source.read_current(runtime.client()).await.unwrap()],
        )
        .unwrap(),
    );
    context.register_table("src", table).unwrap();
    let batches = context
        .sql(&format!(
            "select day, g, count(*) as cnt, max(v) as mx from src              where \"{CHANGE_COLUMN}\" <> 'delete' group by day, g order by day, g"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let days = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let groups = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let maxima = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                days.value(row).to_string(),
                groups.value(row).to_string(),
                counts.value(row),
                maxima.value(row),
            ));
        }
    }
    rows
}

/// A multi-aggregate with a computed group key: the row state materializes
/// the expression column and keeps the source partition column as its prefix.
#[test_log::test(tokio::test)]
async fn multi_agg_computed_group_key_uses_row_state() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let group_keys = vec!["day".to_string(), "grp".to_string()];
    let group_exprs = vec!["day".to_string(), "g".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_gexpr_mv_{suffix}"),
                table_path(&dir, "mv"),
                multi_agg_mv_schema_for(
                    &source.schema,
                    &group_keys,
                    &group_exprs,
                    &[
                        ("cnt".to_string(), DataType::Int64),
                        ("mx".to_string(), DataType::Int64),
                    ],
                )
                .unwrap(),
            )
            .with_primary_keys(group_keys.clone())
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "d1", "insert"),
                (2, "b", 20, "d1", "insert"),
                (3, "a", 7, "d2", "insert"),
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_gexpr_mv_{suffix}          SELECT day, upper(g) AS grp, COUNT(*) AS cnt, MAX(v) AS mx          FROM ivm_part_src_{suffix} GROUP BY day, upper(g)"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_day_grp(runtime, &mv).await,
        full_day_grp(runtime, &source).await
    );

    // The state exists, carries the computed key column and is partitioned.
    let mut row_state = None;
    for view in runtime.metadata().list_views().await.unwrap() {
        for state in runtime.metadata().list_states(&view.view_id).await.unwrap() {
            if state.role == StateRole::MultiAggRows
                && state.table_name.starts_with(&mv.table_name)
            {
                row_state = Some(state);
            }
        }
    }
    let row_state = row_state.expect("the computed-key row state exists");
    let row_state = runtime.open_table_by_id(&row_state.table_id).await.unwrap();
    assert!(row_state.schema.field_with_name("grp").is_ok());
    assert_eq!(row_state.range_partition_columns, vec!["day".to_string()]);

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 12, "d1", "insert"),
                (2, "b", 20, "d1", "delete"),
                (4, "c", 4, "d2", "insert"),
            ]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_day_grp(executor.runtime(), &mv).await,
        full_day_grp(executor.runtime(), &source).await
    );
}

async fn read_day_grp(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(String, String, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select day, grp, cnt, mx from mv order by day, grp")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    collect_day_grp(&batches)
}

async fn full_day_grp(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> Vec<(String, String, i64, i64)> {
    let context = SessionContext::new();
    let table: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
        datafusion::datasource::memory::MemTable::try_new(
            source.schema.clone(),
            vec![source.read_current(runtime.client()).await.unwrap()],
        )
        .unwrap(),
    );
    context.register_table("src", table).unwrap();
    let batches = context
        .sql(&format!(
            "select day, upper(g) as grp, count(*) as cnt, max(v) as mx from src              where \"{CHANGE_COLUMN}\" <> 'delete' group by day, upper(g) order by day, grp"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    collect_day_grp(&batches)
}

fn collect_day_grp(batches: &[RecordBatch]) -> Vec<(String, String, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let days = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let groups = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let maxima = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                days.value(row).to_string(),
                groups.value(row).to_string(),
                counts.value(row),
                maxima.value(row),
            ));
        }
    }
    rows
}

/// A computed group key that reuses a source column name cannot be stored
/// next to the original column; the view must fall back to the full recompute
/// instead of failing or overwriting the source value.
#[test_log::test(tokio::test)]
async fn multi_agg_colliding_group_key_falls_back() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mut schema = multi_agg_mv_schema_for(
        &source.schema,
        &["g".to_string()],
        &["upper(g)".to_string()],
        &[
            ("cnt".to_string(), DataType::Int64),
            ("mx".to_string(), DataType::Int64),
        ],
    )
    .unwrap();
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            if field.name() == "g" {
                Arc::new(field.as_ref().clone().with_nullable(false))
            } else {
                field.clone()
            }
        })
        .collect::<Vec<_>>();
    schema = Arc::new(Schema::new(fields));
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_collision_mv_{suffix}"),
                table_path(&dir, "mv"),
                schema,
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "d1", "insert"),
                (2, "A", 20, "d2", "insert"),
                (3, "b", 7, "d1", "insert"),
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_collision_mv_{suffix}          SELECT upper(g) AS g, COUNT(*) AS cnt, MAX(v) AS mx          FROM ivm_part_src_{suffix} GROUP BY upper(g)"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    // The colliding key cannot use the row state; the view recomputes.
    let has_rows_state = runtime
        .metadata()
        .list_states(&execution.view_id)
        .await
        .unwrap()
        .iter()
        .any(|state| state.role == StateRole::MultiAggRows);
    assert!(!has_rows_state);
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 12, "d1", "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(&mv)))
        .unwrap();
    let rows = context
        .sql("select g, cnt, mx from mv order by g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut values = Vec::new();
    for batch in &rows {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let maxima = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.push((
                groups.value(row).to_string(),
                counts.value(row),
                maxima.value(row),
            ));
        }
    }
    assert_eq!(
        values,
        vec![("A".to_string(), 2, 20), ("B".to_string(), 1, 7),]
    );
}

/// Cross-side semi/anti lookups on partitioned sources: the join key carries
/// the day, so the left state probe from the right delta and the right state
/// probe from the left keys both prune to the changed days while key moves
/// still retract the old match.
#[test_log::test(tokio::test)]
async fn semi_anti_cross_side_partition_pruning_is_correct() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("day", DataType::Utf8, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("rid", DataType::Int64, false),
        Field::new("day", DataType::Utf8, false),
        Field::new("g", DataType::Utf8, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let left_batch = |rows: &[(i64, &str, &str, i64, &str)]| {
        RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.3))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.4))),
            ],
        )
        .unwrap()
    };
    let right_batch = |rows: &[(i64, &str, &str, &str)]| {
        RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.3))),
            ],
        )
        .unwrap()
    };
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cross_left_{suffix}"),
                table_path(&dir, "left"),
                left_schema.clone(),
            )
            .with_primary_keys(vec!["id".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cross_right_{suffix}"),
                table_path(&dir, "right"),
                right_schema.clone(),
            )
            .with_primary_keys(vec!["rid".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cross_mv_{suffix}"),
                table_path(&dir, "mv"),
                semi_anti_mv_schema_for(
                    &left_schema,
                    &["id".to_string(), "day".to_string(), "g".to_string()],
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["id".to_string(), "day".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let mut view = SemiAntiView::new(
        format!("cross_{suffix}"),
        left.clone(),
        right.clone(),
        mv.clone(),
        vec!["day".to_string(), "g".to_string()],
        false,
    );
    view.output_columns = vec!["id".to_string(), "day".to_string(), "g".to_string()];
    left.append_batch(
        runtime.client(),
        left_batch(&[(1, "d1", "g1", 10, "insert"), (2, "d2", "g2", 20, "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            right_batch(&[(10, "d1", "g1", "insert"), (20, "d2", "g2", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap().unwrap();
    assert_eq!(
        semi_anti_rows(&runtime, &left, &right).await,
        vec![
            (1, "d1".to_string(), "g1".to_string()),
            (2, "d2".to_string(), "g2".to_string())
        ]
    );

    // The right match moves from d1 to d2 while the left row is updated in
    // d1: the old d1 match must be retracted and the d2 match kept.
    left.append_batch(
        runtime.client(),
        left_batch(&[(1, "d1", "g1", 11, "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            right_batch(&[(10, "d1", "g1", "delete"), (11, "d2", "g3", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap().unwrap();
    assert_eq!(
        semi_anti_rows(&runtime, &left, &right).await,
        vec![(2, "d2".to_string(), "g2".to_string())]
    );

    // A d2-only right change still reads the d2 left state.
    right
        .append_batch(runtime.client(), right_batch(&[(12, "d2", "g2", "insert")]))
        .await
        .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap().unwrap();
    assert_eq!(
        semi_anti_rows(&runtime, &left, &right).await,
        vec![(2, "d2".to_string(), "g2".to_string())]
    );
}

async fn semi_anti_rows(
    runtime: &IvmRuntime,
    left: &IvmTable,
    right: &IvmTable,
) -> Vec<(i64, String, String)> {
    let context = SessionContext::new();
    context
        .register_table("l", Arc::new(runtime.table_provider(left)))
        .unwrap();
    context
        .register_table("r", Arc::new(runtime.table_provider(right)))
        .unwrap();
    let batches = context
        .sql(&format!(
            "select l.id, l.day, l.g from l where l.\"{CHANGE_COLUMN}\" <> 'delete' \
             and exists (select 1 from r where r.\"{CHANGE_COLUMN}\" <> 'delete' \
                         and r.day = l.day and r.g = l.g) order by l.id, l.day"
        ))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let days = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let groups = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                ids.value(row),
                days.value(row).to_string(),
                groups.value(row).to_string(),
            ));
        }
    }
    rows
}

/// A constant partition filter pins the view to one day: changes in another
/// day neither refresh the view nor leave its cursor behind.
#[test_log::test(tokio::test)]
async fn constant_partition_filter_skips_other_partitions() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_filter_mv_{suffix}"),
                table_path(&dir, "mv"),
                multi_agg_mv_schema_for(
                    &source.schema,
                    &["g".to_string()],
                    &[],
                    &[
                        ("cnt".to_string(), DataType::Int64),
                        ("mx".to_string(), DataType::Int64),
                    ],
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "d1", "insert"),
                (2, "b", 5, "d1", "insert"),
                (3, "a", 7, "d2", "insert"),
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_filter_mv_{suffix}          SELECT g, COUNT(*) AS cnt, MAX(v) AS mx FROM ivm_part_src_{suffix}          WHERE day = 'd1' GROUP BY g"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_group_values(runtime, &mv).await,
        vec![("a".to_string(), 1, 10), ("b".to_string(), 1, 5)]
    );

    // A d2 change is outside the filter: no epoch, unchanged MV, but the
    // source cursor for d2 must move past it.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(3, "a", 8, "d2", "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert!(execution.epoch.is_none());
    assert_eq!(
        read_group_values(runtime, &mv).await,
        vec![("a".to_string(), 1, 10), ("b".to_string(), 1, 5)]
    );
    let mut d2_version = 0;
    for view in runtime.metadata().list_views().await.unwrap() {
        for cursor in runtime
            .metadata()
            .list_cursors(&view.view_id)
            .await
            .unwrap()
        {
            if cursor.source_table_id == source.table_id
                && cursor.partition_desc.contains("d2")
            {
                d2_version = d2_version.max(cursor.last_version);
            }
        }
    }
    assert!(
        d2_version >= 1,
        "the skipped d2 cursor was not advanced: {d2_version}"
    );

    // A d1 change still refreshes the view.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 12, "d1", "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert!(execution.epoch.is_some());
    assert_eq!(
        read_group_values(runtime, &mv).await,
        vec![("a".to_string(), 1, 12), ("b".to_string(), 1, 5)]
    );
}

async fn read_group_values(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> Vec<(String, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select g, cnt, mx from mv order by g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let maxima = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                groups.value(row).to_string(),
                counts.value(row),
                maxima.value(row),
            ));
        }
    }
    rows
}

/// A range predicate on the partition column (`>=` / `<=` and `BETWEEN`)
/// prunes partitions outside the range for bootstrap and increments.
#[test_log::test(tokio::test)]
async fn range_partition_filter_prunes_outside_partitions() {
    for predicate in ["day >= 'd2' AND day <= 'd3'", "day BETWEEN 'd2' AND 'd3'"] {
        let runtime = IvmRuntime::from_env().await.unwrap();
        runtime.init_schema().await.unwrap();
        let dir = tempdir().unwrap();
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let source = create_source(&runtime, &dir, &suffix).await;
        let mv = runtime
            .create_table(
                IvmTableOptions::new(
                    format!("ivm_part_range_mv_{suffix}"),
                    table_path(&dir, "mv"),
                    multi_agg_mv_schema_for(
                        &source.schema,
                        &["g".to_string()],
                        &[],
                        &[
                            ("cnt".to_string(), DataType::Int64),
                            ("mx".to_string(), DataType::Int64),
                        ],
                    )
                    .unwrap(),
                )
                .with_primary_keys(vec!["g".to_string()]),
            )
            .await
            .unwrap();
        source
            .append_batch(
                runtime.client(),
                source_batch(&[
                    (1, "a", 10, "d1", "insert"),
                    (2, "b", 20, "d2", "insert"),
                    (3, "c", 30, "d3", "insert"),
                    (4, "d", 40, "d4", "insert"),
                ]),
            )
            .await
            .unwrap();
        let executor = IvmSqlExecutor::new(runtime);
        let statement = format!(
            "INSERT INTO ivm_part_range_mv_{suffix} \
             SELECT g, COUNT(*) AS cnt, MAX(v) AS mx FROM ivm_part_src_{suffix} \
             WHERE {predicate} GROUP BY g"
        );
        let execution = executor.execute(&statement).await.unwrap();
        assert_eq!(execution.action.label(), "bootstrap");
        let runtime = executor.runtime();
        assert_eq!(
            read_group_values(runtime, &mv).await,
            vec![("b".to_string(), 1, 20), ("c".to_string(), 1, 30)]
        );

        // Changes outside the range are skipped.
        source
            .append_batch(
                runtime.client(),
                source_batch(&[
                    (1, "a", 11, "d1", "insert"),
                    (4, "d", 41, "d4", "insert"),
                ]),
            )
            .await
            .unwrap();
        let execution = executor.execute(&statement).await.unwrap();
        assert_eq!(execution.action.label(), "incremental");
        assert!(execution.epoch.is_none());
        assert_eq!(
            read_group_values(runtime, &mv).await,
            vec![("b".to_string(), 1, 20), ("c".to_string(), 1, 30)]
        );

        // A change inside the range refreshes the view.
        source
            .append_batch(
                runtime.client(),
                source_batch(&[(2, "b", 22, "d2", "insert")]),
            )
            .await
            .unwrap();
        let execution = executor.execute(&statement).await.unwrap();
        assert_eq!(execution.action.label(), "incremental");
        assert!(execution.epoch.is_some());
        assert_eq!(
            read_group_values(runtime, &mv).await,
            vec![("b".to_string(), 1, 22), ("c".to_string(), 1, 30)]
        );
    }
}

/// A filtered sum/count view rebuilds from the matching partition only.
#[test_log::test(tokio::test)]
async fn filtered_view_rebuild_uses_partition_scope() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_rebuild_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "d1", "insert"),
                (2, "b", 5, "d1", "insert"),
                (3, "a", 7, "d2", "insert"),
            ]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_rebuild_mv_{suffix} \
         SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v FROM ivm_part_src_{suffix} \
         WHERE day = 'd1' GROUP BY g"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    let runtime = executor.runtime();
    assert_eq!(
        read_sum_count(runtime, &mv).await,
        vec![("a".to_string(), 10, 1), ("b".to_string(), 5, 1)]
    );

    source
        .append_batch(
            runtime.client(),
            source_batch(&[(3, "a", 8, "d2", "insert"), (1, "a", 12, "d1", "insert")]),
        )
        .await
        .unwrap();
    let overwrite = statement.replacen("INSERT INTO", "INSERT OVERWRITE", 1);
    let execution = executor.execute(&overwrite).await.unwrap();
    assert!(
        matches!(execution.action.label(), "overwrite" | "rebuild"),
        "unexpected action {}",
        execution.action.label()
    );
    assert_eq!(
        read_sum_count(executor.runtime(), &mv).await,
        vec![("a".to_string(), 12, 1), ("b".to_string(), 5, 1)]
    );
}

async fn read_sum_count(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, i64, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select g, sum_v, count_v from mv order by g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
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
            rows.push((
                groups.value(row).to_string(),
                sums.value(row),
                counts.value(row),
            ));
        }
    }
    rows
}

/// A source whose primary key contains the partition column: the injected
/// value is part of the merge key, so an upsert in a partition replaces the
/// stored row instead of duplicating it.
#[test_log::test(tokio::test)]
async fn a_partition_key_inside_the_source_primary_keys_merges_injected_values() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_pk_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let output_columns = ["k", "g", "v", "day"].map(String::from);
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_pk_mv_{suffix}"),
                table_path(&dir, "mv"),
                row_mv_schema_for(&source.schema, &output_columns).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "d1", "insert"), (1, "a", 20, "d2", "insert")]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_pk_mv_{suffix} \
         SELECT k, g, v, day FROM ivm_part_pk_src_{suffix}"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_day_values(executor.runtime(), &mv).await,
        vec![("d1".to_string(), 10), ("d2".to_string(), 20)]
    );

    // The same `(k, day)` is upserted: the injected value merges the previous
    // file's row away.
    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[(1, "a", 12, "d1", "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_day_values(executor.runtime(), &mv).await,
        vec![("d1".to_string(), 12), ("d2".to_string(), 20)]
    );

    // A delete removes the row of its own partition only.
    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[(1, "a", 12, "d1", "delete")]),
        )
        .await
        .unwrap();
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_day_values(executor.runtime(), &mv).await,
        vec![("d2".to_string(), 20)]
    );
}

/// A dropped source partition on a range-partitioned MV: the cleanup retracts
/// the rows through the MV's own partitions.
#[test_log::test(tokio::test)]
async fn a_dropped_source_partition_cleans_a_partitioned_mv() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pmv_drop_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(
                    &source.schema,
                    &["day".to_string(), "g".to_string()],
                    Some("v"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["day".to_string(), "g".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "d1", "insert"), (2, "b", 5, "d2", "insert")]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_pmv_drop_mv_{suffix} \
         SELECT day, g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_part_src_{suffix} GROUP BY day, g"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 10),
            ("d2".to_string(), "b".to_string(), 5),
        ]
    );

    drop_partition(executor.runtime(), &source, "d2").await;
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![("d1".to_string(), "a".to_string(), 10)]
    );
}

/// A refresh that only touches one partition does not read the MV's other
/// partitions: the group keys carry the partition column and no source key can
/// move between partitions (its primary keys carry the partition column too).
#[test_log::test(tokio::test)]
async fn a_partitioned_mv_refresh_prunes_unchanged_partitions() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pmv_prune_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pmv_prune_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(
                    &source.schema,
                    &["day".to_string(), "g".to_string()],
                    Some("v"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["day".to_string(), "g".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "d1", "insert"), (2, "b", 5, "d2", "insert")]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_pmv_prune_mv_{suffix} \
         SELECT day, g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_pmv_prune_src_{suffix} GROUP BY day, g"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 10),
            ("d2".to_string(), "b".to_string(), 5),
        ]
    );

    // Only d1 changes: the refresh may prune the d2 partition of the MV.
    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[(1, "a", 12, "d1", "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 12),
            ("d2".to_string(), "b".to_string(), 5),
        ]
    );

    // Only d2 changes: the pruned side flips.
    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[(2, "b", 7, "d2", "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 12),
            ("d2".to_string(), "b".to_string(), 7),
        ]
    );
}

/// A range-partitioned target may move a row to another partition when the
/// partition column is not in the target primary key: the sink writes the old
/// partition's delete and the new partition's insert.
#[test_log::test(tokio::test)]
async fn a_partitioned_mv_can_move_rows_through_the_sink() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pmv_bad_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(
                    &source.schema,
                    &["day".to_string(), "g".to_string()],
                    Some("v"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "d1", "insert")]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_pmv_bad_mv_{suffix} \
         SELECT day, g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_part_src_{suffix} GROUP BY day, g"
    );
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "bootstrap");
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![("d1".to_string(), "a".to_string(), 10)]
    );

    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[(1, "a", 20, "d2", "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![("d2".to_string(), "a".to_string(), 20)]
    );

    source
        .append_batch(
            executor.runtime().client(),
            source_batch(&[(1, "a", 20, "d2", "delete")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert!(read_day_groups(executor.runtime(), &mv).await.is_empty());
}

/// A dropped source partition removes its rows from the view: when the group
/// keys carry the partition column, the refresh retracts exactly those rows
/// instead of rebuilding.
#[test_log::test(tokio::test)]
async fn a_dropped_source_partition_removes_its_rows() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_drop_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(
                    &source.schema,
                    &["day".to_string(), "g".to_string()],
                    Some("v"),
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["day".to_string(), "g".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "d1", "insert"), (2, "b", 5, "d2", "insert")]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_drop_mv_{suffix} \
         SELECT day, g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_part_src_{suffix} GROUP BY day, g"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_day_groups(executor.runtime(), &mv).await,
        vec![
            ("d1".to_string(), "a".to_string(), 10),
            ("d2".to_string(), "b".to_string(), 5),
        ]
    );

    // Drop the `d2` partition of the source (a delete commit).
    let runtime = executor.runtime();
    let partitions = runtime
        .client()
        .get_all_partition_info(&source.table_id)
        .await
        .unwrap();
    let dropped = partitions
        .iter()
        .find(|partition| partition.partition_desc.contains("d2"))
        .expect("the d2 partition exists");
    let files = runtime
        .client()
        .get_data_files_of_single_partition(dropped)
        .await
        .unwrap();
    assert!(!files.is_empty(), "the dropped partition has files");
    // A delete commit clears the partition's snapshot; the commit needs the
    // partition in `read_partition_info` (the optimistic-concurrency read set).
    let table_info = runtime
        .client()
        .get_table_info_by_table_id(&source.table_id)
        .await
        .unwrap()
        .expect("the source table is registered");
    runtime
        .client()
        .commit_data(
            MetaInfo {
                table_info: Some(table_info),
                list_partition: vec![PartitionInfo {
                    table_id: source.table_id.clone(),
                    partition_desc: dropped.partition_desc.clone(),
                    commit_op: CommitOp::DeleteCommit.into(),
                    ..Default::default()
                }],
                read_partition_info: vec![PartitionInfo {
                    table_id: source.table_id.clone(),
                    partition_desc: dropped.partition_desc.clone(),
                    ..Default::default()
                }],
            },
            CommitOp::DeleteCommit,
        )
        .await
        .unwrap();

    // The dropped rows are retracted through the partition value without a
    // rebuild; the untouched partition keeps its state.
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_day_groups(runtime, &mv).await,
        vec![("d1".to_string(), "a".to_string(), 10)]
    );

    // The rebuilt view keeps refreshing incrementally.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(3, "a", 7, "d1", "insert")]),
        )
        .await
        .unwrap();
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "incremental");
    assert_eq!(
        read_day_groups(runtime, &mv).await,
        vec![("d1".to_string(), "a".to_string(), 17)]
    );
}

/// A source partition dropped outside the view's keys cannot be retracted by
/// value: the refresh rebuilds from the current state instead.
#[test_log::test(tokio::test)]
async fn a_dropped_source_partition_outside_the_group_keys_rebuilds() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = create_source(&runtime, &dir, &suffix).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_part_drop_all_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "d1", "insert"), (2, "b", 5, "d2", "insert")]),
        )
        .await
        .unwrap();
    let executor = IvmSqlExecutor::new(runtime);
    let statement = format!(
        "INSERT INTO ivm_part_drop_all_mv_{suffix} \
         SELECT g, SUM(v) AS sum_v, COUNT(*) AS count_v \
         FROM ivm_part_src_{suffix} GROUP BY g"
    );
    executor.execute(&statement).await.unwrap();
    assert_eq!(
        read_group_sums(executor.runtime(), &mv).await,
        vec![("a".to_string(), 10), ("b".to_string(), 5)]
    );

    // Drop the `d2` partition of the source (a delete commit).
    let runtime = executor.runtime();
    let partitions = runtime
        .client()
        .get_all_partition_info(&source.table_id)
        .await
        .unwrap();
    let dropped = partitions
        .iter()
        .find(|partition| partition.partition_desc.contains("d2"))
        .expect("the d2 partition exists");
    let files = runtime
        .client()
        .get_data_files_of_single_partition(dropped)
        .await
        .unwrap();
    assert!(!files.is_empty(), "the dropped partition has files");
    let table_info = runtime
        .client()
        .get_table_info_by_table_id(&source.table_id)
        .await
        .unwrap()
        .expect("the source table is registered");
    runtime
        .client()
        .commit_data(
            MetaInfo {
                table_info: Some(table_info),
                list_partition: vec![PartitionInfo {
                    table_id: source.table_id.clone(),
                    partition_desc: dropped.partition_desc.clone(),
                    commit_op: CommitOp::DeleteCommit.into(),
                    ..Default::default()
                }],
                read_partition_info: vec![PartitionInfo {
                    table_id: source.table_id.clone(),
                    partition_desc: dropped.partition_desc.clone(),
                    ..Default::default()
                }],
            },
            CommitOp::DeleteCommit,
        )
        .await
        .unwrap();

    // The group key does not carry `day`, so the refresh rebuilds.
    let execution = executor.execute(&statement).await.unwrap();
    assert_eq!(execution.action.label(), "rebuild");
    assert_eq!(
        read_group_sums(runtime, &mv).await,
        vec![("a".to_string(), 10)]
    );
}

/// The `(g, sum_v)` rows of a sum/count MV ordered by `g`.
async fn read_group_sums(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select g, sum_v from mv order by g")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let groups = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((groups.value(row).to_string(), values.value(row)));
        }
    }
    rows
}

/// A lookup chain over a partitioned fact table prunes the MV partitions the
/// delta did not touch.
#[test_log::test(tokio::test)]
async fn a_partitioned_lookup_chain_prunes_unchanged_partitions() {
    use lakesoul_ivm::{
        JoinKeyExprs, LookupChainColumn, LookupChainStep, LookupChainView,
        lookup_chain_mv_schema_for,
    };

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();

    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("day", DataType::Utf8, false),
        Field::new("fk", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("a", DataType::Utf8, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let fact_batch = |rows: &[(i64, &str, i64, i64, &str)]| {
        RecordBatch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.3))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.4))),
            ],
        )
        .unwrap()
    };
    let dim_batch = |rows: &[(i64, &str, &str)]| {
        RecordBatch::try_new(
            dim_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
            ],
        )
        .unwrap()
    };

    let fact = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pchain_fact_{suffix}"),
                table_path(&dir, "fact"),
                fact_schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let dim = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pchain_dim_{suffix}"),
                table_path(&dir, "dim"),
                dim_schema.clone(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();

    let steps = vec![LookupChainStep {
        source: 1,
        left: true,
        keys: vec!["fk".to_string()],
        right_keys: vec!["k".to_string()],
        key_sources: Vec::new(),
        key_exprs: JoinKeyExprs::default(),
        unique: true,
    }];
    let columns = vec![
        LookupChainColumn {
            source: 0,
            column: "k".to_string(),
            name: "k".to_string(),
        },
        LookupChainColumn {
            source: 0,
            column: "day".to_string(),
            name: "day".to_string(),
        },
        LookupChainColumn {
            source: 0,
            column: "v".to_string(),
            name: "v".to_string(),
        },
        LookupChainColumn {
            source: 1,
            column: "a".to_string(),
            name: "a".to_string(),
        },
    ];
    let schema = lookup_chain_mv_schema_for(
        &[fact.schema.clone(), dim.schema.clone()],
        &steps,
        &columns,
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pchain_mv_{suffix}"),
                table_path(&dir, "mv"),
                schema,
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let view = LookupChainView::new(
        format!("pchain_{suffix}"),
        vec![(fact.clone(), None), (dim.clone(), None)],
        steps,
        columns,
        mv.clone(),
    );

    fact.append_batch(
        runtime.client(),
        fact_batch(&[(1, "d1", 10, 100, "insert"), (2, "d2", 11, 200, "insert")]),
    )
    .await
    .unwrap();
    dim.append_batch(
        runtime.client(),
        dim_batch(&[(10, "a", "insert"), (11, "b", "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_lookup_chain(&view).await.unwrap();

    async fn read_rows(
        runtime: &IvmRuntime,
        mv: &IvmTable,
    ) -> Vec<(i64, String, i64, String)> {
        let context = SessionContext::new();
        let batches = mv.read_current(runtime.client()).await.unwrap();
        let provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(
            datafusion::datasource::memory::MemTable::try_new(
                mv.schema.clone(),
                vec![batches],
            )
            .unwrap(),
        );
        context.register_table("mv", provider).unwrap();
        let batches = context
            .sql("select k, day, v, a from mv order by k, day")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let k = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let day = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let v = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let a = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((
                    k.value(row),
                    day.value(row).to_string(),
                    v.value(row),
                    a.value(row).to_string(),
                ));
            }
        }
        rows
    }

    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![
            (1, "d1".to_string(), 100, "a".to_string()),
            (2, "d2".to_string(), 200, "b".to_string()),
        ]
    );

    // Only d1 changes: the d2 partition of the MV is pruned, its row stays.
    fact.append_batch(
        runtime.client(),
        fact_batch(&[(1, "d1", 10, 150, "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_lookup_chain(&view).await.unwrap();
    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![
            (1, "d1".to_string(), 150, "a".to_string()),
            (2, "d2".to_string(), 200, "b".to_string()),
        ]
    );
}

/// A row view over a partitioned source prunes the MV partitions the delta
/// did not touch.
#[test_log::test(tokio::test)]
async fn a_partitioned_row_view_prunes_unchanged_partitions() {
    use lakesoul_ivm::RowView;

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_prow_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let output_columns = ["k", "g", "v", "day"].map(String::from);
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_prow_mv_{suffix}"),
                table_path(&dir, "mv"),
                row_mv_schema_for(&source.schema, &output_columns).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let view = RowView::new(format!("prow_{suffix}"), source.clone(), mv.clone())
        .with_output_columns(output_columns.to_vec());

    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "d1", "insert"), (2, "b", 20, "d2", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_row(&view).await.unwrap();
    assert_eq!(
        read_row_tuples(&runtime, &mv).await,
        vec![
            (1, "a".to_string(), "d1".to_string()),
            (2, "b".to_string(), "d2".to_string()),
        ]
    );

    // Only d1 changes: the d2 partition of the MV is pruned, its row stays.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 11, "d1", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_row(&view).await.unwrap();
    assert_eq!(
        read_row_tuples(&runtime, &mv).await,
        vec![
            (1, "a".to_string(), "d1".to_string()),
            (2, "b".to_string(), "d2".to_string()),
        ]
    );
}

/// A window view over a partitioned source prunes the MV partitions the delta
/// did not touch.
#[test_log::test(tokio::test)]
async fn a_partitioned_window_view_prunes_unchanged_partitions() {
    use lakesoul_ivm::{
        WindowColumn, WindowFunction, WindowView, window_columns_mv_schema_for,
    };

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pwin_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let partition_keys = vec!["g".to_string()];
    let row_keys = vec!["k".to_string(), "day".to_string()];
    let schema = window_columns_mv_schema_for(
        &source.schema,
        &partition_keys,
        &row_keys,
        &[WindowColumn::new(WindowFunction::RowNumber)],
    )
    .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pwin_mv_{suffix}"),
                table_path(&dir, "mv"),
                schema,
            )
            .with_primary_keys(vec!["g".to_string(), "k".to_string(), "day".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let view = WindowView::new(
        format!("pwin_{suffix}"),
        source.clone(),
        mv.clone(),
        partition_keys,
        vec!["v".to_string()],
    );

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "d1", "insert"),
                (2, "a", 20, "d1", "insert"),
                (3, "b", 5, "d2", "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_window(&view).await.unwrap();

    async fn read_rows(
        runtime: &IvmRuntime,
        mv: &IvmTable,
    ) -> Vec<(String, i64, String, i64)> {
        let context = SessionContext::new();
        context
            .register_table("mv", Arc::new(runtime.table_provider(mv)))
            .unwrap();
        let batches = context
            .sql("select g, k, day, row_number from mv order by k, day")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let g = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let k = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let day = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let number = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((
                    g.value(row).to_string(),
                    k.value(row),
                    day.value(row).to_string(),
                    number.value(row),
                ));
            }
        }
        rows
    }

    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![
            ("a".to_string(), 1, "d1".to_string(), 1),
            ("a".to_string(), 2, "d1".to_string(), 2),
            ("b".to_string(), 3, "d2".to_string(), 1),
        ]
    );

    // Only d1 changes: the d2 partition of the MV is pruned, its row stays.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 30, "d1", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_window(&view).await.unwrap();
    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![
            ("a".to_string(), 1, "d1".to_string(), 2),
            ("a".to_string(), 2, "d1".to_string(), 1),
            ("b".to_string(), 3, "d2".to_string(), 1),
        ]
    );
}

/// A semi join over a partitioned left source reads only the delta's keys and
/// MV partitions when the right side did not change.
#[test_log::test(tokio::test)]
async fn a_partitioned_semi_view_reads_only_changed_partitions() {
    use lakesoul_ivm::{SemiAntiView, semi_anti_mv_schema};

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_psemi_left_{suffix}"),
                table_path(&dir, "left"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("rk", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_psemi_right_{suffix}"),
                table_path(&dir, "right"),
                right_schema.clone(),
            )
            .with_primary_keys(vec!["rk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_psemi_mv_{suffix}"),
                table_path(&dir, "mv"),
                semi_anti_mv_schema(&left.schema),
            )
            .with_primary_keys(vec!["k".to_string(), "day".to_string()])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let view = SemiAntiView::new(
        format!("psemi_{suffix}"),
        left.clone(),
        right.clone(),
        mv.clone(),
        vec!["g".to_string()],
        false,
    );

    left.append_batch(
        runtime.client(),
        source_batch(&[(1, "a", 10, "d1", "insert"), (2, "b", 20, "d2", "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                right_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values([1, 2])),
                    Arc::new(StringArray::from_iter_values(["a", "b"])),
                    Arc::new(StringArray::from_iter_values(["insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap();

    async fn read_rows(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, String, i64)> {
        let context = SessionContext::new();
        context
            .register_table("mv", Arc::new(runtime.table_provider(mv)))
            .unwrap();
        let batches = context
            .sql("select k, g, v from mv order by k")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let k = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let g = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let v = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((k.value(row), g.value(row).to_string(), v.value(row)));
            }
        }
        rows
    }

    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(1, "a".to_string(), 10), (2, "b".to_string(), 20),]
    );

    // Only d1 changes: key 1 loses its match and its MV row; key 2's row in
    // the untouched d2 partition stays.
    left.append_batch(
        runtime.client(),
        source_batch(&[(1, "z", 10, "d1", "insert")]),
    )
    .await
    .unwrap();
    runtime.refresh_semi_anti(&view).await.unwrap();
    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(2, "b".to_string(), 20)]
    );
}

/// A union over partitioned sources reads only the MV partitions the changed
/// branches touched.
#[test_log::test(tokio::test)]
async fn a_partitioned_union_view_prunes_unchanged_partitions() {
    use lakesoul_ivm::{
        IVM_SOURCE_COLUMN, UnionAllView, UnionSource, union_all_mv_schema_for,
    };

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let mut sources = Vec::new();
    for (name, id) in [("a", "a"), ("b", "b")] {
        sources.push(
            runtime
                .create_table(
                    IvmTableOptions::new(
                        format!("ivm_punion_{id}_{suffix}"),
                        table_path(&dir, name),
                        source_schema(),
                    )
                    .with_primary_keys(vec!["k".to_string(), "day".to_string()])
                    .with_cdc_column(CHANGE_COLUMN)
                    .with_range_partition_columns(vec!["day".to_string()]),
                )
                .await
                .unwrap(),
        );
    }
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_punion_mv_{suffix}"),
                table_path(&dir, "mv"),
                union_all_mv_schema_for(&sources[0].schema).unwrap(),
            )
            .with_primary_keys(vec![
                IVM_SOURCE_COLUMN.to_string(),
                "k".to_string(),
                "day".to_string(),
            ])
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let view = UnionAllView::new(
        format!("punion_{suffix}"),
        sources
            .iter()
            .map(|source| UnionSource::new(source.clone()))
            .collect(),
        mv.clone(),
    );

    sources[0]
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "x", 10, "d1", "insert")]),
        )
        .await
        .unwrap();
    sources[1]
        .append_batch(
            runtime.client(),
            source_batch(&[(2, "y", 20, "d2", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_union_all(&view).await.unwrap();

    async fn read_rows(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, i64, String)> {
        let context = SessionContext::new();
        context
            .register_table("mv", Arc::new(runtime.table_provider(mv)))
            .unwrap();
        let batches = context
            .sql("select k, v, day from mv order by k")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let k = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let v = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let day = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((k.value(row), v.value(row), day.value(row).to_string()));
            }
        }
        rows
    }

    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(1, 10, "d1".to_string()), (2, 20, "d2".to_string())]
    );

    // Only branch a's d1 changes: the d2 partition of the MV is pruned, the
    // branch b row stays.
    sources[0]
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "x", 11, "d1", "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_union_all(&view).await.unwrap();
    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(1, 11, "d1".to_string()), (2, 20, "d2".to_string())]
    );
}

/// A keyed left join over a partitioned left source prunes the MV partitions
/// the delta did not touch.
#[test_log::test(tokio::test)]
async fn a_partitioned_left_join_prunes_unchanged_partitions() {
    use lakesoul_ivm::{
        LeftJoinView, keyed_join_output_primary_keys, left_join_view_schema_for,
    };

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("day", DataType::Utf8, false),
        Field::new("jk", DataType::Int64, false),
        Field::new("lv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let right_schema = Arc::new(Schema::new(vec![
        Field::new("rid", DataType::Int64, false),
        Field::new("jk", DataType::Int64, false),
        Field::new("rv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let left = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_plj_left_{suffix}"),
                table_path(&dir, "left"),
                left_schema.clone(),
            )
            .with_primary_keys(vec!["id".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let right = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_plj_right_{suffix}"),
                table_path(&dir, "right"),
                right_schema.clone(),
            )
            .with_primary_keys(vec!["rid".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_plj_mv_{suffix}"),
                table_path(&dir, "mv"),
                left_join_view_schema_for(
                    &left_schema,
                    &right_schema,
                    &["id".to_string(), "day".to_string()],
                    &["rid".to_string()],
                    &["jk".to_string()],
                    "lv",
                    "rv",
                )
                .unwrap(),
            )
            .with_primary_keys(keyed_join_output_primary_keys(
                &["id".to_string(), "day".to_string()],
                &["rid".to_string()],
            ))
            .with_range_partition_columns(vec!["__left_pk_day".to_string()]),
        )
        .await
        .unwrap();
    let view = LeftJoinView::new(
        format!("plj_{suffix}"),
        left.clone(),
        right.clone(),
        mv.clone(),
        "jk",
        "lv",
        "rv",
    );

    left.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1, 2])),
                Arc::new(StringArray::from_iter_values(["d1", "d2"])),
                Arc::new(Int64Array::from_iter_values([10, 20])),
                Arc::new(Int64Array::from_iter_values([100, 200])),
                Arc::new(StringArray::from_iter_values(["insert", "insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                right_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values([1, 2])),
                    Arc::new(Int64Array::from_iter_values([10, 20])),
                    Arc::new(Int64Array::from_iter_values([5, 6])),
                    Arc::new(StringArray::from_iter_values(["insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    runtime.refresh_left_join(&view).await.unwrap();

    async fn read_rows(
        runtime: &IvmRuntime,
        mv: &IvmTable,
    ) -> Vec<(i64, String, i64, i64)> {
        let context = SessionContext::new();
        context
            .register_table("mv", Arc::new(runtime.table_provider(mv)))
            .unwrap();
        let batches = context
            .sql(
                "select \"__left_pk_id\", \"__left_pk_day\", left_value, right_value \
                 from mv order by \"__left_pk_id\"",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let id = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let day = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let lv = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let rv = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((
                    id.value(row),
                    day.value(row).to_string(),
                    lv.value(row),
                    rv.value(row),
                ));
            }
        }
        rows
    }

    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(1, "d1".to_string(), 100, 5), (2, "d2".to_string(), 200, 6),]
    );

    // Only d1 changes: the d2 partition of the MV is pruned, its row stays.
    left.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(Int64Array::from_iter_values([1])),
                Arc::new(StringArray::from_iter_values(["d1"])),
                Arc::new(Int64Array::from_iter_values([10])),
                Arc::new(Int64Array::from_iter_values([101])),
                Arc::new(StringArray::from_iter_values(["insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    runtime.refresh_left_join(&view).await.unwrap();
    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(1, "d1".to_string(), 101, 5), (2, "d2".to_string(), 200, 6),]
    );
}

/// A multi-way join over a partitioned base prunes the MV partitions the
/// delta did not touch when only the base source changed.
#[test_log::test(tokio::test)]
async fn a_partitioned_multi_join_prunes_unchanged_partitions() {
    use lakesoul_ivm::{
        MultiJoinColumn, MultiJoinKey, MultiJoinView, multi_join_mv_schema_for,
        multi_join_primary_keys,
    };

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact_schema = source_schema_with_day();
    let middle_schema = Arc::new(Schema::new(vec![
        Field::new("mid", DataType::Int64, false),
        Field::new("mk", DataType::Int64, false),
        Field::new("mv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("rid", DataType::Int64, false),
        Field::new("rk", DataType::Int64, false),
        Field::new("rv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let fact = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pmj_fact_{suffix}"),
                table_path(&dir, "fact"),
                fact_schema.clone(),
            )
            .with_primary_keys(vec!["id".to_string(), "day".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let middle = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pmj_middle_{suffix}"),
                table_path(&dir, "middle"),
                middle_schema.clone(),
            )
            .with_primary_keys(vec!["mid".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let dim = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pmj_dim_{suffix}"),
                table_path(&dir, "dim"),
                dim_schema.clone(),
            )
            .with_primary_keys(vec!["rid".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let columns = vec![
        MultiJoinColumn {
            source: 0,
            column: "day".to_string(),
            name: "day".to_string(),
        },
        MultiJoinColumn {
            source: 0,
            column: "lv".to_string(),
            name: "lv".to_string(),
        },
        MultiJoinColumn {
            source: 1,
            column: "mv".to_string(),
            name: "mv".to_string(),
        },
        MultiJoinColumn {
            source: 2,
            column: "rv".to_string(),
            name: "rv".to_string(),
        },
    ];
    let mut mv_keys = multi_join_primary_keys(&[
        vec!["id".to_string(), "day".to_string()],
        vec!["mid".to_string()],
        vec!["rid".to_string()],
    ]);
    mv_keys.push("day".to_string());
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pmj_mv_{suffix}"),
                table_path(&dir, "mv"),
                multi_join_mv_schema_for(
                    &[
                        fact_schema.clone(),
                        middle_schema.clone(),
                        dim_schema.clone(),
                    ],
                    &[
                        vec!["id".to_string(), "day".to_string()],
                        vec!["mid".to_string()],
                        vec!["rid".to_string()],
                    ],
                    &columns,
                )
                .unwrap(),
            )
            .with_primary_keys(mv_keys)
            .with_range_partition_columns(vec!["day".to_string()]),
        )
        .await
        .unwrap();
    let view = MultiJoinView::new(
        format!("pmj_{suffix}"),
        vec![fact.clone(), middle.clone(), dim.clone()],
        mv.clone(),
        vec![
            MultiJoinKey {
                left_source: 0,
                left_column: "lk".to_string(),
                left_expr: None,
                right_source: 1,
                right_column: "mk".to_string(),
                right_expr: None,
            },
            MultiJoinKey {
                left_source: 1,
                left_column: "mv".to_string(),
                left_expr: None,
                right_source: 2,
                right_column: "rk".to_string(),
                right_expr: None,
            },
        ],
        columns,
    );

    fact.append_batch(
        runtime.client(),
        source_batch_with_day(&[(1, "d1", 10, 100), (2, "d2", 20, 200)]),
    )
    .await
    .unwrap();
    middle
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                middle_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values([1, 2])),
                    Arc::new(Int64Array::from_iter_values([10, 20])),
                    Arc::new(Int64Array::from_iter_values([1000, 2000])),
                    Arc::new(StringArray::from_iter_values(["insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    dim.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            dim_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1, 2])),
                Arc::new(Int64Array::from_iter_values([1000, 2000])),
                Arc::new(Int64Array::from_iter_values([5, 6])),
                Arc::new(StringArray::from_iter_values(["insert", "insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();

    async fn read_rows(
        runtime: &IvmRuntime,
        mv: &IvmTable,
    ) -> Vec<(String, i64, i64, i64)> {
        let context = SessionContext::new();
        context
            .register_table("mv", Arc::new(runtime.table_provider(mv)))
            .unwrap();
        let batches = context
            .sql("select \"day\", \"lv\", \"mv\", \"rv\" from mv order by \"day\"")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let day = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let lv = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mv = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let rv = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((
                    day.value(row).to_string(),
                    lv.value(row),
                    mv.value(row),
                    rv.value(row),
                ));
            }
        }
        rows
    }

    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![
            ("d1".to_string(), 100, 1000, 5),
            ("d2".to_string(), 200, 2000, 6),
        ]
    );

    // Only the base's d1 changes: the d2 partition of the MV is pruned, its
    // row stays.
    fact.append_batch(
        runtime.client(),
        source_batch_with_day(&[(1, "d1", 10, 101)]),
    )
    .await
    .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();
    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![
            ("d1".to_string(), 101, 1000, 5),
            ("d2".to_string(), 200, 2000, 6),
        ]
    );
}

/// A multi-way join prunes the MV by the join-key values of a changed middle
/// source when the base partition column is that join key.
#[test_log::test(tokio::test)]
async fn a_multi_join_prunes_by_the_changed_join_key_values() {
    use lakesoul_ivm::{
        MultiJoinColumn, MultiJoinKey, MultiJoinView, multi_join_mv_schema_for,
        multi_join_primary_keys,
    };

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("lk", DataType::Int64, false),
        Field::new("lv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let middle_schema = Arc::new(Schema::new(vec![
        Field::new("mid", DataType::Int64, false),
        Field::new("mk", DataType::Int64, false),
        Field::new("mv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("rid", DataType::Int64, false),
        Field::new("rk", DataType::Int64, false),
        Field::new("rv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let fact = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pjk_fact_{suffix}"),
                table_path(&dir, "fact"),
                fact_schema.clone(),
            )
            .with_primary_keys(vec!["id".to_string(), "lk".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["lk".to_string()]),
        )
        .await
        .unwrap();
    let middle = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pjk_middle_{suffix}"),
                table_path(&dir, "middle"),
                middle_schema.clone(),
            )
            .with_primary_keys(vec!["mid".to_string(), "mk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let dim = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pjk_dim_{suffix}"),
                table_path(&dir, "dim"),
                dim_schema.clone(),
            )
            .with_primary_keys(vec!["rid".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let columns = vec![
        MultiJoinColumn {
            source: 0,
            column: "lk".to_string(),
            name: "lk".to_string(),
        },
        MultiJoinColumn {
            source: 0,
            column: "lv".to_string(),
            name: "lv".to_string(),
        },
        MultiJoinColumn {
            source: 1,
            column: "mv".to_string(),
            name: "mv".to_string(),
        },
        MultiJoinColumn {
            source: 2,
            column: "rv".to_string(),
            name: "rv".to_string(),
        },
    ];
    let mut mv_keys = multi_join_primary_keys(&[
        vec!["id".to_string(), "lk".to_string()],
        vec!["mid".to_string(), "mk".to_string()],
        vec!["rid".to_string()],
    ]);
    mv_keys.push("lk".to_string());
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pjk_mv_{suffix}"),
                table_path(&dir, "mv"),
                multi_join_mv_schema_for(
                    &[
                        fact_schema.clone(),
                        middle_schema.clone(),
                        dim_schema.clone(),
                    ],
                    &[
                        vec!["id".to_string(), "lk".to_string()],
                        vec!["mid".to_string(), "mk".to_string()],
                        vec!["rid".to_string()],
                    ],
                    &columns,
                )
                .unwrap(),
            )
            .with_primary_keys(mv_keys)
            .with_range_partition_columns(vec!["lk".to_string()]),
        )
        .await
        .unwrap();
    let view = MultiJoinView::new(
        format!("pjk_{suffix}"),
        vec![fact.clone(), middle.clone(), dim.clone()],
        mv.clone(),
        vec![
            MultiJoinKey {
                left_source: 0,
                left_column: "lk".to_string(),
                left_expr: None,
                right_source: 1,
                right_column: "mk".to_string(),
                right_expr: None,
            },
            MultiJoinKey {
                left_source: 1,
                left_column: "mv".to_string(),
                left_expr: None,
                right_source: 2,
                right_column: "rk".to_string(),
                right_expr: None,
            },
        ],
        columns,
    );

    fact.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            fact_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1, 2])),
                Arc::new(Int64Array::from_iter_values([10, 20])),
                Arc::new(Int64Array::from_iter_values([100, 200])),
                Arc::new(StringArray::from_iter_values(["insert", "insert"])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    middle
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                middle_schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values([1, 2])),
                    Arc::new(Int64Array::from_iter_values([10, 20])),
                    Arc::new(Int64Array::from_iter_values([1000, 2000])),
                    Arc::new(StringArray::from_iter_values(["insert", "insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    dim.append_batch(
        runtime.client(),
        RecordBatch::try_new(
            dim_schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1, 2, 3])),
                Arc::new(Int64Array::from_iter_values([1000, 2000, 1001])),
                Arc::new(Int64Array::from_iter_values([5, 6, 7])),
                Arc::new(StringArray::from_iter_values([
                    "insert", "insert", "insert",
                ])),
            ],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();

    async fn read_rows(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, i64, i64, i64)> {
        let context = SessionContext::new();
        context
            .register_table("mv", Arc::new(runtime.table_provider(mv)))
            .unwrap();
        let batches = context
            .sql("select \"lk\", \"lv\", \"mv\", \"rv\" from mv order by \"lk\"")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let lk = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let lv = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mv = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let rv = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((lk.value(row), lv.value(row), mv.value(row), rv.value(row)));
            }
        }
        rows
    }

    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(10, 100, 1000, 5), (20, 200, 2000, 6)]
    );

    // Only the middle changes: the join-key rule prunes the MV to lk=10, so
    // the lk=20 partition's row is untouched.
    middle
        .append_batch(
            runtime.client(),
            RecordBatch::try_new(
                middle_schema,
                vec![
                    Arc::new(Int64Array::from_iter_values([1])),
                    Arc::new(Int64Array::from_iter_values([10])),
                    Arc::new(Int64Array::from_iter_values([1001])),
                    Arc::new(StringArray::from_iter_values(["insert"])),
                ],
            )
            .unwrap(),
        )
        .await
        .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();
    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(10, 100, 1001, 7), (20, 200, 2000, 6)]
    );
}

/// A multi-way join propagates a deep source's changed key values down the
/// chain to the base partition.
#[test_log::test(tokio::test)]
async fn a_multi_join_prunes_a_deep_source_by_chain_keys() {
    use lakesoul_ivm::{
        MultiJoinColumn, MultiJoinKey, MultiJoinView, multi_join_mv_schema_for,
        multi_join_primary_keys,
    };

    fn batch_i64(schema: &SchemaRef, columns: &[&[i64]]) -> RecordBatch {
        let rows = columns.first().map_or(0, |column| column.len());
        let mut arrays: Vec<Arc<dyn arrow_array::Array>> = columns
            .iter()
            .map(|values| {
                Arc::new(Int64Array::from_iter_values(values.iter().copied()))
                    as Arc<dyn arrow_array::Array>
            })
            .collect();
        arrays.push(Arc::new(StringArray::from_iter_values(
            (0..rows).map(|_| "insert"),
        )));
        RecordBatch::try_new(schema.clone(), arrays).unwrap()
    }

    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("lk", DataType::Int64, false),
        Field::new("lv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let s1_schema = Arc::new(Schema::new(vec![
        Field::new("m1", DataType::Int64, false),
        Field::new("mk", DataType::Int64, false),
        Field::new("mv1", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let s2_schema = Arc::new(Schema::new(vec![
        Field::new("m2", DataType::Int64, false),
        Field::new("k2", DataType::Int64, false),
        Field::new("v2", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let s3_schema = Arc::new(Schema::new(vec![
        Field::new("rid", DataType::Int64, false),
        Field::new("k3", DataType::Int64, false),
        Field::new("rv", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]));
    let fact = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pchain_fact_{suffix}"),
                table_path(&dir, "fact"),
                fact_schema.clone(),
            )
            .with_primary_keys(vec!["id".to_string(), "lk".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_range_partition_columns(vec!["lk".to_string()]),
        )
        .await
        .unwrap();
    let s1 = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pchain_s1_{suffix}"),
                table_path(&dir, "s1"),
                s1_schema.clone(),
            )
            .with_primary_keys(vec!["m1".to_string(), "mk".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let s2 = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pchain_s2_{suffix}"),
                table_path(&dir, "s2"),
                s2_schema.clone(),
            )
            .with_primary_keys(vec!["m2".to_string(), "k2".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let s3 = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pchain_s3_{suffix}"),
                table_path(&dir, "s3"),
                s3_schema.clone(),
            )
            .with_primary_keys(vec!["rid".to_string(), "k3".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let columns = vec![
        MultiJoinColumn {
            source: 0,
            column: "lk".to_string(),
            name: "lk".to_string(),
        },
        MultiJoinColumn {
            source: 0,
            column: "lv".to_string(),
            name: "lv".to_string(),
        },
        MultiJoinColumn {
            source: 1,
            column: "mv1".to_string(),
            name: "mv1".to_string(),
        },
        MultiJoinColumn {
            source: 2,
            column: "v2".to_string(),
            name: "v2".to_string(),
        },
        MultiJoinColumn {
            source: 3,
            column: "rv".to_string(),
            name: "rv".to_string(),
        },
    ];
    let primary_keys = [
        vec!["id".to_string(), "lk".to_string()],
        vec!["m1".to_string(), "mk".to_string()],
        vec!["m2".to_string(), "k2".to_string()],
        vec!["rid".to_string(), "k3".to_string()],
    ];
    let mut mv_keys = multi_join_primary_keys(&primary_keys);
    mv_keys.push("lk".to_string());
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_pchain_mv_{suffix}"),
                table_path(&dir, "mv"),
                multi_join_mv_schema_for(
                    &[
                        fact_schema.clone(),
                        s1_schema.clone(),
                        s2_schema.clone(),
                        s3_schema.clone(),
                    ],
                    &primary_keys,
                    &columns,
                )
                .unwrap(),
            )
            .with_primary_keys(mv_keys)
            .with_range_partition_columns(vec!["lk".to_string()]),
        )
        .await
        .unwrap();
    let view = MultiJoinView::new(
        format!("pchain_{suffix}"),
        vec![fact.clone(), s1.clone(), s2.clone(), s3.clone()],
        mv.clone(),
        vec![
            MultiJoinKey {
                left_source: 0,
                left_column: "lk".to_string(),
                left_expr: None,
                right_source: 1,
                right_column: "mk".to_string(),
                right_expr: None,
            },
            MultiJoinKey {
                left_source: 1,
                left_column: "mv1".to_string(),
                left_expr: None,
                right_source: 2,
                right_column: "k2".to_string(),
                right_expr: None,
            },
            MultiJoinKey {
                left_source: 2,
                left_column: "v2".to_string(),
                left_expr: None,
                right_source: 3,
                right_column: "k3".to_string(),
                right_expr: None,
            },
        ],
        columns,
    );
    fact.append_batch(
        runtime.client(),
        batch_i64(&fact_schema, &[&[1, 2], &[10, 20], &[100, 200]]),
    )
    .await
    .unwrap();
    s1.append_batch(
        runtime.client(),
        batch_i64(&s1_schema, &[&[1, 2], &[10, 20], &[1000, 2000]]),
    )
    .await
    .unwrap();
    s2.append_batch(
        runtime.client(),
        batch_i64(&s2_schema, &[&[1, 2], &[1000, 2000], &[7, 8]]),
    )
    .await
    .unwrap();
    s3.append_batch(
        runtime.client(),
        batch_i64(&s3_schema, &[&[1, 2], &[7, 8], &[5, 6]]),
    )
    .await
    .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();

    async fn read_rows(
        runtime: &IvmRuntime,
        mv: &IvmTable,
    ) -> Vec<(i64, i64, i64, i64, i64)> {
        let context = SessionContext::new();
        context
            .register_table("mv", Arc::new(runtime.table_provider(mv)))
            .unwrap();
        let batches = context
            .sql(
                "select \"lk\", \"lv\", \"mv1\", \"v2\", \"rv\" from mv \
                 order by \"lk\"",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let lk = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let lv = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mv1 = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let v2 = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let rv = batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push((
                    lk.value(row),
                    lv.value(row),
                    mv1.value(row),
                    v2.value(row),
                    rv.value(row),
                ));
            }
        }
        rows
    }

    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(10, 100, 1000, 7, 5), (20, 200, 2000, 8, 6)]
    );

    // Only the deepest source changes: the chain rule propagates k3=7 down to
    // the base join key lk=10, so the lk=20 partition is untouched.
    s3.append_batch(
        runtime.client(),
        batch_i64(&s3_schema, &[&[1], &[7], &[55]]),
    )
    .await
    .unwrap();
    runtime.refresh_multi_join(&view).await.unwrap();
    assert_eq!(
        read_rows(&runtime, &mv).await,
        vec![(10, 100, 1000, 7, 55), (20, 200, 2000, 8, 6)]
    );
}
