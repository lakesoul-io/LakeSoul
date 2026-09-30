// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cascading views: a materialized view feeding another view.
//!
//! The downstream view treats the upstream MV as an ordinary keyed source;
//! its `rowKinds` column acts as the change column, so the tombstone rows the
//! upstream refresh writes are filtered like any other CDC marker.  The
//! caller (or the future scheduler) has to refresh in topological order.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IvmRuntime, IvmSqlExecutor, IvmTable, IvmTableOptions, SumCountView, WindowFunction,
    WindowView, sum_count_mv_schema_for, window_ranking_mv_schema_for,
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
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn source_batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// The logical `g -> sum_v` rows of an MV (the provider drops tombstones).
async fn read_mv(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, i64)> {
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
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((groups.value(row).to_string(), sums.value(row)));
        }
    }
    rows
}

/// The logical `g -> row_number` rows of a window MV.
async fn read_window_mv(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(String, i64)> {
    let context = SessionContext::new();
    context
        .register_table("mv", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    let batches = context
        .sql("select g, row_number from mv order by g")
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
        let numbers = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((groups.value(row).to_string(), numbers.value(row)));
        }
    }
    rows
}

struct Cascade {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    source: IvmTable,
    mv1: IvmTable,
    mv2: IvmTable,
    view1: SumCountView,
    view2: SumCountView,
}

async fn cascade() -> Cascade {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cascade_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv1 = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cascade_mv1_{suffix}"),
                table_path(&dir, "mv1"),
                sum_count_mv_schema_for(&source.schema, &["g".to_string()], Some("v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let view1 = SumCountView::new_with_group_keys(
        format!("cascade1_{suffix}"),
        source.clone(),
        mv1.clone(),
        vec!["g".to_string()],
        Some("v".to_string()),
    );
    // The downstream view reads the upstream MV as a keyed source; its
    // `rowKinds` column doubles as the change column.
    let mv2 = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cascade_mv2_{suffix}"),
                table_path(&dir, "mv2"),
                sum_count_mv_schema_for(&mv1.schema, &["g".to_string()], Some("sum_v"))
                    .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let view2 = SumCountView::new_with_group_keys(
        format!("cascade2_{suffix}"),
        mv1.clone(),
        mv2.clone(),
        vec!["g".to_string()],
        Some("sum_v".to_string()),
    );
    Cascade {
        runtime,
        dir,
        source,
        mv1,
        mv2,
        view1,
        view2,
    }
}

impl Cascade {
    /// Refresh upstream then downstream, as a scheduler would.
    async fn refresh(&self) {
        self.runtime.refresh_sum_count(&self.view1).await.unwrap();
        self.runtime.refresh_sum_count(&self.view2).await.unwrap();
    }
}

#[test_log::test(tokio::test)]
async fn cascading_views_follow_the_upstream_refresh() {
    let fixture = cascade().await;
    let runtime = &fixture.runtime;
    let source = &fixture.source;
    let _keep_dir = &fixture.dir;

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 5, "insert"),
                (3, "b", 7, "insert"),
            ]),
        )
        .await
        .unwrap();
    fixture.refresh().await;
    assert_eq!(
        read_mv(runtime, &fixture.mv2).await,
        vec![("a".to_string(), 15), ("b".to_string(), 7)]
    );

    // An update and a delete in the upstream MV must retract downstream too.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (2, "a", 9, "insert"),
                (3, "b", 7, "delete"),
                (4, "c", 2, "insert"),
            ]),
        )
        .await
        .unwrap();
    fixture.refresh().await;
    assert_eq!(
        read_mv(runtime, &fixture.mv2).await,
        vec![("a".to_string(), 19), ("c".to_string(), 2)]
    );

    // Both views replay as no-ops when nothing changed.
    assert!(
        runtime
            .refresh_sum_count(&fixture.view1)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        runtime
            .refresh_sum_count(&fixture.view2)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        read_mv(runtime, &fixture.mv2).await,
        vec![("a".to_string(), 19), ("c".to_string(), 2)]
    );

    // The upstream MV itself still hides its tombstones.
    assert_eq!(
        read_mv(runtime, &fixture.mv1).await,
        vec![("a".to_string(), 19), ("c".to_string(), 2)]
    );
}

#[test_log::test(tokio::test)]
async fn cascading_views_through_the_sql_executor() {
    let fixture = cascade().await;
    let runtime = &fixture.runtime;
    let source = &fixture.source;
    let _keep_dir = &fixture.dir;

    // No caller session: the executor registers each relation it finds in an
    // internal session.
    let executor = IvmSqlExecutor::new(IvmRuntime::from_env().await.unwrap());
    let mv1_name = fixture.mv1.table_name.clone();
    let mv2_name = fixture.mv2.table_name.clone();
    let source_name = fixture.source.table_name.clone();

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 5, "insert"),
                (3, "b", 7, "insert"),
            ]),
        )
        .await
        .unwrap();
    executor
        .execute(&format!(
            "INSERT INTO {mv1_name} SELECT g, SUM(v) FROM {source_name} GROUP BY g"
        ))
        .await
        .unwrap();
    executor
        .execute(&format!(
            "INSERT INTO {mv2_name} SELECT g, SUM(sum_v) FROM {mv1_name} GROUP BY g"
        ))
        .await
        .unwrap();
    assert_eq!(
        read_mv(runtime, &fixture.mv2).await,
        vec![("a".to_string(), 15), ("b".to_string(), 7)]
    );

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (2, "a", 9, "insert"),
                (3, "b", 7, "delete"),
                (4, "c", 2, "insert"),
            ]),
        )
        .await
        .unwrap();
    executor
        .execute(&format!(
            "INSERT INTO {mv1_name} SELECT g, SUM(v) FROM {source_name} GROUP BY g"
        ))
        .await
        .unwrap();
    executor
        .execute(&format!(
            "INSERT INTO {mv2_name} SELECT g, SUM(sum_v) FROM {mv1_name} GROUP BY g"
        ))
        .await
        .unwrap();
    assert_eq!(
        read_mv(runtime, &fixture.mv2).await,
        vec![("a".to_string(), 19), ("c".to_string(), 2)]
    );
}

#[test_log::test(tokio::test)]
async fn cascading_window_over_an_aggregate_mv() {
    let fixture = cascade().await;
    let runtime = &fixture.runtime;
    let source = &fixture.source;
    let _keep_dir = &fixture.dir;
    let suffix = uuid::Uuid::new_v4().simple();

    // A global ROW_NUMBER ordered by the upstream MV's aggregate value.
    let mv3 = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_cascade_mv3_{suffix}"),
                table_path(&fixture.dir, "mv3"),
                window_ranking_mv_schema_for(
                    &fixture.mv1.schema,
                    &[],
                    &["g".to_string()],
                    WindowFunction::RowNumber,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let view3 = WindowView::new_with_function(
        format!("cascade3_{suffix}"),
        fixture.mv1.clone(),
        mv3.clone(),
        Vec::new(),
        vec!["sum_v".to_string()],
        WindowFunction::RowNumber,
    );

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 5, "insert"),
                (3, "b", 7, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&fixture.view1).await.unwrap();
    runtime.refresh_window(&view3).await.unwrap();
    assert_eq!(
        read_window_mv(runtime, &mv3).await,
        vec![("a".to_string(), 2), ("b".to_string(), 1)]
    );

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (2, "a", 9, "insert"),
                (3, "b", 7, "delete"),
                (4, "c", 2, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_sum_count(&fixture.view1).await.unwrap();
    runtime.refresh_window(&view3).await.unwrap();
    assert_eq!(
        read_window_mv(runtime, &mv3).await,
        vec![("a".to_string(), 2), ("c".to_string(), 1)]
    );
}
