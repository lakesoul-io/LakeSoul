// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Aggregate window views: `SUM(value)` and `COUNT(*)`/`COUNT(value)` over a
//! partition, either whole-partition (no order keys) or running with the SQL
//! default frame. The refresh reads only the affected partitions of the
//! source, so the pruned read is exercised by every update.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::{SessionContext, col, lit};
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, WindowFunction,
    WindowView, window_aggregate_mv_schema_for,
};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn table_path(dir: &tempfile::TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

fn register(
    context: &SessionContext,
    name: &str,
    batches: Vec<RecordBatch>,
    schema: &SchemaRef,
) {
    let schema = batches
        .first()
        .map(|batch| batch.schema())
        .unwrap_or_else(|| schema.clone());
    let batches = if batches.is_empty() {
        vec![RecordBatch::new_empty(schema.clone())]
    } else {
        batches
    };
    let table: Arc<dyn datafusion::catalog::TableProvider> =
        Arc::new(MemTable::try_new(schema, vec![batches]).unwrap());
    context.register_table(name, table).unwrap();
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("p", DataType::Utf8, false),
        Field::new("ord", DataType::Int64, false),
        Field::new("amount", DataType::Int64, true),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

type SourceRow<'a> = (i64, &'a str, i64, Option<i64>, &'a str);

fn source_batch(rows: &[SourceRow<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.4))),
        ],
    )
    .unwrap()
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    source: IvmTable,
    view: WindowView,
    function: WindowFunction,
    ordered: bool,
    value_column: Option<String>,
}

async fn fixture(
    function: WindowFunction,
    ordered: bool,
    value_column: Option<&str>,
) -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_aggwin_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let partitions = vec!["p".to_string()];
    let order_keys = if ordered {
        vec!["ord".to_string()]
    } else {
        Vec::new()
    };
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_aggwin_mv_{suffix}"),
                table_path(&dir, "mv"),
                window_aggregate_mv_schema_for(
                    &source.schema,
                    &partitions,
                    &["id".to_string()],
                    function,
                    value_column,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["p".to_string(), "id".to_string()])
            .with_bucket_columns(partitions.clone()),
        )
        .await
        .unwrap();
    let view = WindowView::new_aggregate(
        format!("aggwin_{suffix}"),
        source.clone(),
        mv,
        partitions,
        order_keys,
        function,
        value_column.map(str::to_string),
    );
    Fixture {
        runtime,
        dir,
        source,
        view,
        function,
        ordered,
        value_column: value_column.map(str::to_string),
    }
}

/// The current MV rows as `(partition, id, value)`.
async fn read_values(fixture: &Fixture) -> Vec<(String, i64, Option<i64>)> {
    let value_column = fixture.function.column_name();
    let mut rows = Vec::new();
    for batch in fixture
        .view
        .mv
        .read_current(fixture.runtime.client())
        .await
        .unwrap()
    {
        let schema = batch.schema();
        let partitions = batch
            .column(schema.index_of("p").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(schema.index_of(value_column).unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                rows.push((
                    partitions.value(row).to_string(),
                    ids.value(row),
                    if values.is_null(row) {
                        None
                    } else {
                        Some(values.value(row))
                    },
                ));
            }
        }
    }
    rows.sort();
    rows
}

/// The SQL semantics of the same window function.
async fn full_values(fixture: &Fixture) -> Vec<(String, i64, Option<i64>)> {
    let over = if fixture.ordered {
        "partition by p order by ord"
    } else {
        "partition by p"
    };
    let aggregate = match fixture.function {
        WindowFunction::Sum => "sum(amount)".to_string(),
        WindowFunction::Count => match fixture.value_column.as_deref() {
            Some(column) => format!("count({column})"),
            None => "count(1)".to_string(),
        },
        other => other.sql_name().to_string(),
    };
    let context = SessionContext::new();
    register(
        &context,
        "src",
        fixture
            .source
            .read_current(fixture.runtime.client())
            .await
            .unwrap(),
        &fixture.source.schema,
    );
    let frame = context
        .sql(&format!(
            "select p, id, {aggregate} over ({over}) as v \
             from src where \"{CHANGE_COLUMN}\" <> 'delete'"
        ))
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let schema = batch.schema();
        let partitions = batch
            .column(schema.index_of("p").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(schema.index_of("v").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                partitions.value(row).to_string(),
                ids.value(row),
                if values.is_null(row) {
                    None
                } else {
                    Some(values.value(row))
                },
            ));
        }
    }
    rows.sort();
    rows
}

async fn assert_matches_oracle(fixture: &Fixture) {
    assert_eq!(read_values(fixture).await, full_values(fixture).await);
}

#[test_log::test(tokio::test)]
async fn sum_and_count_windows_match_sql() {
    let sum_running = fixture(WindowFunction::Sum, true, Some("amount")).await;
    let sum_whole = fixture(WindowFunction::Sum, false, Some("amount")).await;
    let count_running = fixture(WindowFunction::Count, true, None).await;
    let count_whole = fixture(WindowFunction::Count, false, None).await;
    let _keep = (
        &sum_running.dir,
        &sum_whole.dir,
        &count_running.dir,
        &count_whole.dir,
    );
    let fixtures = [&sum_running, &sum_whole, &count_running, &count_whole];

    // Running sums use the SQL default frame (peers included), whole-partition
    // sums are constant per partition, NULL amounts are ignored and COUNT(*)
    // counts rows.
    let rows = source_batch(&[
        (1, "a", 1, Some(10), "insert"),
        (2, "a", 2, Some(20), "insert"),
        (3, "a", 2, Some(5), "insert"),
        (4, "a", 3, None, "insert"),
        (5, "b", 1, Some(100), "insert"),
        (6, "b", 2, None, "insert"),
    ]);
    for fixture in fixtures {
        fixture
            .source
            .append_batch(fixture.runtime.client(), rows.clone())
            .await
            .unwrap();
        fixture
            .runtime
            .refresh_window(&fixture.view)
            .await
            .unwrap()
            .unwrap();
        assert_matches_oracle(fixture).await;
    }
    assert_eq!(
        read_values(&sum_running).await,
        vec![
            ("a".to_string(), 1, Some(10)),
            ("a".to_string(), 2, Some(35)),
            ("a".to_string(), 3, Some(35)),
            ("a".to_string(), 4, Some(35)),
            ("b".to_string(), 5, Some(100)),
            ("b".to_string(), 6, Some(100)),
        ]
    );
    assert_eq!(
        read_values(&sum_whole).await,
        vec![
            ("a".to_string(), 1, Some(35)),
            ("a".to_string(), 2, Some(35)),
            ("a".to_string(), 3, Some(35)),
            ("a".to_string(), 4, Some(35)),
            ("b".to_string(), 5, Some(100)),
            ("b".to_string(), 6, Some(100)),
        ]
    );
    assert_eq!(
        read_values(&count_running).await,
        vec![
            ("a".to_string(), 1, Some(1)),
            ("a".to_string(), 2, Some(3)),
            ("a".to_string(), 3, Some(3)),
            ("a".to_string(), 4, Some(4)),
            ("b".to_string(), 5, Some(1)),
            ("b".to_string(), 6, Some(2)),
        ]
    );
    assert_eq!(
        read_values(&count_whole).await,
        vec![
            ("a".to_string(), 1, Some(4)),
            ("a".to_string(), 2, Some(4)),
            ("a".to_string(), 3, Some(4)),
            ("a".to_string(), 4, Some(4)),
            ("b".to_string(), 5, Some(2)),
            ("b".to_string(), 6, Some(2)),
        ]
    );

    // An update moving to another ordering point renumbers only its partition.
    for fixture in fixtures {
        fixture
            .source
            .append_batch(
                fixture.runtime.client(),
                source_batch(&[(2, "a", 3, Some(30), "insert")]),
            )
            .await
            .unwrap();
        fixture
            .runtime
            .refresh_window(&fixture.view)
            .await
            .unwrap()
            .unwrap();
        assert_matches_oracle(fixture).await;
    }

    // Moving a row to another partition and deleting a row.
    for fixture in fixtures {
        fixture
            .source
            .append_batch(
                fixture.runtime.client(),
                source_batch(&[
                    (3, "b", 5, Some(5), "insert"),
                    (5, "b", 1, Some(100), "delete"),
                    (7, "c", 1, None, "insert"),
                ]),
            )
            .await
            .unwrap();
        fixture
            .runtime
            .refresh_window(&fixture.view)
            .await
            .unwrap()
            .unwrap();
        assert_matches_oracle(fixture).await;
    }

    // An all-NULL partition sums to NULL and counts its rows.
    assert_eq!(
        read_values(&sum_whole).await,
        vec![
            ("a".to_string(), 1, Some(40)),
            ("a".to_string(), 2, Some(40)),
            ("a".to_string(), 4, Some(40)),
            ("b".to_string(), 3, Some(5)),
            ("b".to_string(), 6, Some(5)),
            ("c".to_string(), 7, None),
        ]
    );
    assert_eq!(
        read_values(&count_whole).await,
        vec![
            ("a".to_string(), 1, Some(3)),
            ("a".to_string(), 2, Some(3)),
            ("a".to_string(), 4, Some(3)),
            ("b".to_string(), 3, Some(2)),
            ("b".to_string(), 6, Some(2)),
            ("c".to_string(), 7, Some(1)),
        ]
    );

    // Rebuilds agree with the incremental result.
    for fixture in fixtures {
        fixture.runtime.rebuild_window(&fixture.view).await.unwrap();
        assert_matches_oracle(fixture).await;
    }
}

#[test_log::test(tokio::test)]
async fn source_reads_are_pruned_to_partitions() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_prune_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 1, Some(10), "insert"),
                (2, "b", 1, Some(20), "insert"),
                (3, "c", 1, Some(30), "insert"),
            ]),
        )
        .await
        .unwrap();

    let partitions = |batches: Vec<RecordBatch>| {
        let mut values = batches
            .iter()
            .flat_map(|batch| {
                let index = batch.schema().index_of("p").unwrap();
                let array = batch
                    .column(index)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .clone();
                (0..array.len())
                    .map(|row| array.value(row).to_string())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        values.sort();
        values
    };

    // An equality filter reads only the matching partition.
    let filtered = source
        .read_current_filtered(runtime.client(), vec![col("p").eq(lit("a"))])
        .await
        .unwrap();
    assert_eq!(partitions(filtered), vec!["a".to_string()]);

    // An IN filter reads the selected partitions.
    let filtered = source
        .read_current_filtered(
            runtime.client(),
            vec![col("p").in_list(vec![lit("a"), lit("c")], false)],
        )
        .await
        .unwrap();
    assert_eq!(partitions(filtered), vec!["a".to_string(), "c".to_string()]);

    // A false filter reads nothing.
    let filtered = source
        .read_current_filtered(runtime.client(), vec![lit(false)])
        .await
        .unwrap();
    assert!(filtered.iter().all(|batch| batch.num_rows() == 0));
}
