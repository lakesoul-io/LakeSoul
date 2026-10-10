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
    JoinView, keyed_join_output_primary_keys, keyed_join_view_schema_for,
    row_mv_schema_for, sum_count_mv_schema_for,
};
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
                (3, "b", 5, "d2", "update_before"),
                (3, "b", 9, "d2", "update_after"),
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
        source_batch(&[
            (2, "b", 20, "d2", "update_before"),
            (2, "b", 25, "d2", "update_after"),
            (1, "a", 10, "d1", "delete"),
        ]),
    )
    .await
    .unwrap();
    runtime.refresh_join(&view).await.unwrap();
    assert_eq!(
        read_join_pairs(&runtime, &output).await,
        vec![(2, 25, "B".to_string())]
    );
}
