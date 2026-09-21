// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for `ROW_NUMBER()` views: a refresh recomputes the affected
//! partitions and rewrites the rows whose rank changed (including rows that
//! disappeared or moved to another partition).

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IVM_ROW_NUMBER_COLUMN, IvmRuntime, IvmTable, IvmTableOptions,
    WindowView, window_mv_schema,
};
use tempfile::tempdir;

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(IVM_ROW_KINDS_COLUMN, DataType::Utf8, false),
    ]))
}

fn source_batch(rows: &[(i64, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

/// The current `(partition, row key) -> row_number` state of the view.
async fn window_map(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<(i64, i64), i64> {
    let mut values = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let partition_index = schema.index_of("g").unwrap();
        let key_index = schema.index_of("k").unwrap();
        let number_index = schema.index_of(IVM_ROW_NUMBER_COLUMN).unwrap();
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap();
        let partitions = batch
            .column(partition_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = batch
            .column(key_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let numbers = batch
            .column(number_index)
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
                values
                    .insert((partitions.value(row), keys.value(row)), numbers.value(row));
            }
        }
    }
    values
}

/// Full `ROW_NUMBER()` of the current source state.
async fn full_window(
    runtime: &IvmRuntime,
    source: &IvmTable,
) -> HashMap<(i64, i64), i64> {
    let batches = source.read_current(runtime.client()).await.unwrap();
    let context = SessionContext::new();
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(source.schema.clone(), vec![batches]).unwrap());
    context.register_table("src", table).unwrap();
    let frame = context
        .sql(&format!(
            "select g, k, cast(row_number() over (partition by g order by v, k) as bigint) as rn \
             from src where \"{IVM_ROW_KINDS_COLUMN}\" != 'delete'"
        ))
        .await
        .unwrap();
    let mut values = HashMap::new();
    for batch in frame.collect().await.unwrap() {
        let partitions = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let numbers = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            values.insert((partitions.value(row), keys.value(row)), numbers.value(row));
        }
    }
    values
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    source: IvmTable,
    mv: IvmTable,
    view: WindowView,
    source_name: String,
    mv_name: String,
}

async fn setup() -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_window_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_window_mv_{suffix}"),
                table_path(&dir, "mv"),
                window_mv_schema(&["g".to_string()], &["k".to_string()]),
            )
            .with_primary_keys(vec!["g".to_string(), "k".to_string()])
            .with_bucket_columns(vec!["g".to_string()]),
        )
        .await
        .unwrap();
    let view = WindowView::new(
        format!("window_{suffix}"),
        source.clone(),
        mv.clone(),
        vec!["g".to_string()],
        vec!["v".to_string()],
    );
    let source_name = source.table_name.clone();
    let mv_name = mv.table_name.clone();
    Fixture {
        runtime,
        dir,
        source,
        mv,
        view,
        source_name,
        mv_name,
    }
}

impl Fixture {
    async fn cleanup(self) {
        self.runtime
            .client()
            .drop_table(&self.source_name, "default")
            .await
            .unwrap();
        self.runtime
            .client()
            .drop_table(&self.mv_name, "default")
            .await
            .unwrap();
        drop(self.dir);
    }
}

#[test_log::test(tokio::test)]
async fn window_recomputes_affected_partitions() {
    let fixture = setup().await;
    let runtime = &fixture.runtime;
    let (source, mv, view) = (&fixture.source, &fixture.mv, &fixture.view);

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, 1, 10, "insert"),
                (2, 1, 20, "insert"),
                (3, 1, 30, "insert"),
                (4, 2, 5, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_window(view).await.unwrap().unwrap();
    assert_eq!(
        window_map(runtime, mv).await,
        HashMap::from([((1, 1), 1), ((1, 2), 2), ((1, 3), 3), ((2, 4), 1)])
    );

    // A new row in the middle shifts the ranks below it.
    source
        .append_batch(runtime.client(), source_batch(&[(5, 1, 15, "insert")]))
        .await
        .unwrap();
    runtime.refresh_window(view).await.unwrap().unwrap();
    assert_eq!(
        window_map(runtime, mv).await,
        HashMap::from([
            ((1, 1), 1),
            ((1, 5), 2),
            ((1, 2), 3),
            ((1, 3), 4),
            ((2, 4), 1),
        ])
    );

    // An order-value update moves the row to the front.
    source
        .append_batch(runtime.client(), source_batch(&[(3, 1, 5, "insert")]))
        .await
        .unwrap();
    runtime.refresh_window(view).await.unwrap().unwrap();
    assert_eq!(
        window_map(runtime, mv).await,
        HashMap::from([
            ((1, 3), 1),
            ((1, 1), 2),
            ((1, 5), 3),
            ((1, 2), 4),
            ((2, 4), 1),
        ])
    );

    // A delete removes the row and compresses the ranks.
    source
        .append_batch(runtime.client(), source_batch(&[(5, 1, 15, "delete")]))
        .await
        .unwrap();
    runtime.refresh_window(view).await.unwrap().unwrap();
    assert_eq!(
        window_map(runtime, mv).await,
        HashMap::from([((1, 3), 1), ((1, 1), 2), ((1, 2), 3), ((2, 4), 1)])
    );
    assert_eq!(
        window_map(runtime, mv).await,
        full_window(runtime, source).await
    );

    // A partition move recomputes both the old and the new partition.
    source
        .append_batch(runtime.client(), source_batch(&[(2, 2, 20, "insert")]))
        .await
        .unwrap();
    runtime.refresh_window(view).await.unwrap().unwrap();
    assert_eq!(
        window_map(runtime, mv).await,
        HashMap::from([((1, 3), 1), ((1, 1), 2), ((2, 4), 1), ((2, 2), 2)])
    );
    assert_eq!(
        window_map(runtime, mv).await,
        full_window(runtime, source).await
    );

    // Replaying the last window does not change anything.
    let previous_version = runtime
        .metadata()
        .list_cursors(&view.view_id)
        .await
        .unwrap()
        .first()
        .map(|cursor| cursor.last_version - 1)
        .unwrap();
    runtime
        .metadata()
        .upsert_cursor(
            &view.view_id,
            &source.table_id,
            lakesoul_io::constant::DEFAULT_PARTITION_DESC,
            previous_version,
            0,
        )
        .await
        .unwrap();
    runtime.refresh_window(view).await.unwrap().unwrap();
    assert_eq!(
        window_map(runtime, mv).await,
        full_window(runtime, source).await
    );

    // A rebuild recomputes every partition.
    runtime.rebuild_window(view).await.unwrap();
    assert_eq!(
        window_map(runtime, mv).await,
        full_window(runtime, source).await
    );

    fixture.cleanup().await;
}

#[test_log::test(tokio::test)]
async fn window_view_requires_a_keyed_source_and_valid_columns() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();

    // A source without a primary key cannot identify rows.
    let append_only = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_window_ao_{suffix}"),
            table_path(&dir, "ao"),
            source_schema(),
        ))
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_window_ao_mv_{suffix}"),
                table_path(&dir, "ao_mv"),
                window_mv_schema(&["g".to_string()], &["k".to_string()]),
            )
            .with_primary_keys(vec!["g".to_string(), "k".to_string()]),
        )
        .await
        .unwrap();
    let view = WindowView::new(
        format!("window_ao_{suffix}"),
        append_only,
        mv,
        vec!["g".to_string()],
        vec!["v".to_string()],
    );
    let error = runtime.refresh_window(&view).await.unwrap_err();
    assert!(
        error.to_string().contains("primary key"),
        "unexpected error: {error}"
    );

    // A missing order column is rejected.
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_window_bad_{suffix}"),
                table_path(&dir, "bad"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_window_bad_mv_{suffix}"),
                table_path(&dir, "bad_mv"),
                window_mv_schema(&["g".to_string()], &["k".to_string()]),
            )
            .with_primary_keys(vec!["g".to_string(), "k".to_string()]),
        )
        .await
        .unwrap();
    let view = WindowView::new(
        format!("window_bad_{suffix}"),
        source,
        mv,
        vec!["g".to_string()],
        vec!["missing".to_string()],
    );
    let error = runtime.refresh_window(&view).await.unwrap_err();
    assert!(
        error.to_string().contains("order column"),
        "unexpected error: {error}"
    );

    drop(dir);
}
