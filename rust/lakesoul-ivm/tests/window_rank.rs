// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! `RANK()` and `DENSE_RANK()` window views. They reuse the ROW_NUMBER
//! maintenance (recompute the affected partitions), but the declared ordering
//! is used as-is so ties share a rank, and the MV column is named after the
//! function.

use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, WindowFunction,
    WindowView, window_ranking_mv_schema_for,
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
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

type SourceRow<'a> = (i64, &'a str, i64, &'a str);

fn source_batch(rows: &[SourceRow<'_>]) -> RecordBatch {
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

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    source: IvmTable,
    view: WindowView,
    value_column: &'static str,
}

async fn fixture(function: WindowFunction) -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_rank_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["id".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let partitions = vec!["p".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_rank_mv_{suffix}"),
                table_path(&dir, "mv"),
                window_ranking_mv_schema_for(
                    &source.schema,
                    &partitions,
                    &["id".to_string()],
                    function,
                )
                .unwrap(),
            )
            .with_primary_keys(vec!["p".to_string(), "id".to_string()])
            .with_bucket_columns(partitions.clone()),
        )
        .await
        .unwrap();
    let view = WindowView::new_with_function(
        format!("rank_{suffix}"),
        source.clone(),
        mv,
        partitions,
        vec!["ord".to_string()],
        function,
    );
    Fixture {
        runtime,
        dir,
        source,
        view,
        value_column: function.column_name(),
    }
}

/// The current MV rows as `(partition, id, rank)`.
async fn read_ranks(fixture: &Fixture) -> Vec<(String, i64, i64)> {
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
        let ranks = batch
            .column(schema.index_of(fixture.value_column).unwrap())
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
                    ranks.value(row),
                ));
            }
        }
    }
    rows.sort();
    rows
}

/// The SQL semantics of the same ranking function.
async fn full_ranks(fixture: &Fixture) -> Vec<(String, i64, i64)> {
    let function = fixture.view.function.sql_name();
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
            "select p, id, cast({function}() over (partition by p order by ord) as bigint) as r \
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
        let ranks = batch
            .column(schema.index_of("r").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                partitions.value(row).to_string(),
                ids.value(row),
                ranks.value(row),
            ));
        }
    }
    rows.sort();
    rows
}

async fn assert_matches_oracle(fixture: &Fixture) {
    assert_eq!(read_ranks(fixture).await, full_ranks(fixture).await);
}

#[test_log::test(tokio::test)]
async fn rank_and_dense_rank_match_sql() {
    let rank = fixture(WindowFunction::Rank).await;
    let dense = fixture(WindowFunction::DenseRank).await;
    let _keep_rank = &rank.dir;
    let _keep_dense = &dense.dir;

    let rows = source_batch(&[
        (1, "a", 10, "insert"),
        (2, "a", 10, "insert"),
        (3, "a", 20, "insert"),
        (4, "b", 5, "insert"),
    ]);
    rank.source
        .append_batch(rank.runtime.client(), rows.clone())
        .await
        .unwrap();
    dense
        .source
        .append_batch(dense.runtime.client(), rows)
        .await
        .unwrap();
    rank.runtime
        .refresh_window(&rank.view)
        .await
        .unwrap()
        .unwrap();
    dense
        .runtime
        .refresh_window(&dense.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&rank).await;
    assert_matches_oracle(&dense).await;
    assert_eq!(
        read_ranks(&rank).await,
        vec![
            ("a".to_string(), 1, 1),
            ("a".to_string(), 2, 1),
            ("a".to_string(), 3, 3),
            ("b".to_string(), 4, 1),
        ]
    );
    assert_eq!(
        read_ranks(&dense).await,
        vec![
            ("a".to_string(), 1, 1),
            ("a".to_string(), 2, 1),
            ("a".to_string(), 3, 2),
            ("b".to_string(), 4, 1),
        ]
    );

    // A tie joins the first rank, skipping the next rank for RANK.
    let rows = source_batch(&[(5, "a", 10, "insert")]);
    rank.source
        .append_batch(rank.runtime.client(), rows.clone())
        .await
        .unwrap();
    dense
        .source
        .append_batch(dense.runtime.client(), rows)
        .await
        .unwrap();
    rank.runtime
        .refresh_window(&rank.view)
        .await
        .unwrap()
        .unwrap();
    dense
        .runtime
        .refresh_window(&dense.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&rank).await;
    assert_matches_oracle(&dense).await;
    assert_eq!(
        read_ranks(&rank).await,
        vec![
            ("a".to_string(), 1, 1),
            ("a".to_string(), 2, 1),
            ("a".to_string(), 3, 4),
            ("a".to_string(), 5, 1),
            ("b".to_string(), 4, 1),
        ]
    );

    // An ordering update renumbers only its partition.
    let rows = source_batch(&[(3, "a", 25, "insert")]);
    rank.source
        .append_batch(rank.runtime.client(), rows.clone())
        .await
        .unwrap();
    dense
        .source
        .append_batch(dense.runtime.client(), rows)
        .await
        .unwrap();
    rank.runtime
        .refresh_window(&rank.view)
        .await
        .unwrap()
        .unwrap();
    dense
        .runtime
        .refresh_window(&dense.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&rank).await;
    assert_matches_oracle(&dense).await;
    assert_eq!(
        read_ranks(&rank).await,
        vec![
            ("a".to_string(), 1, 1),
            ("a".to_string(), 2, 1),
            ("a".to_string(), 3, 4),
            ("a".to_string(), 5, 1),
            ("b".to_string(), 4, 1),
        ]
    );

    // Deleting the only row of a partition empties it; new ties are ranked.
    let rows = source_batch(&[
        (4, "b", 5, "delete"),
        (6, "b", 7, "insert"),
        (7, "b", 7, "insert"),
    ]);
    rank.source
        .append_batch(rank.runtime.client(), rows.clone())
        .await
        .unwrap();
    dense
        .source
        .append_batch(dense.runtime.client(), rows)
        .await
        .unwrap();
    rank.runtime
        .refresh_window(&rank.view)
        .await
        .unwrap()
        .unwrap();
    dense
        .runtime
        .refresh_window(&dense.view)
        .await
        .unwrap()
        .unwrap();
    assert_matches_oracle(&rank).await;
    assert_matches_oracle(&dense).await;
    assert_eq!(
        read_ranks(&rank).await,
        vec![
            ("a".to_string(), 1, 1),
            ("a".to_string(), 2, 1),
            ("a".to_string(), 3, 4),
            ("a".to_string(), 5, 1),
            ("b".to_string(), 6, 1),
            ("b".to_string(), 7, 1),
        ]
    );
    assert_eq!(
        read_ranks(&dense).await,
        vec![
            ("a".to_string(), 1, 1),
            ("a".to_string(), 2, 1),
            ("a".to_string(), 3, 2),
            ("a".to_string(), 5, 1),
            ("b".to_string(), 6, 1),
            ("b".to_string(), 7, 1),
        ]
    );

    // Rebuilds agree with the incremental result.
    rank.runtime.rebuild_window(&rank.view).await.unwrap();
    dense.runtime.rebuild_window(&dense.view).await.unwrap();
    assert_matches_oracle(&rank).await;
    assert_matches_oracle(&dense).await;

    // The function is persisted in the view spec.
    let spec = rank
        .runtime
        .metadata()
        .get_view_spec(&rank.view.view_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(spec["function"], "rank");
    let spec = dense
        .runtime
        .metadata()
        .get_view_spec(&dense.view.view_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(spec["function"], "dense_rank");
}

#[test_log::test(tokio::test)]
async fn ranks_are_not_broken_by_primary_keys() {
    // Two tied rows rank equally regardless of their primary keys or insert
    // order; ROW_NUMBER would have numbered them 1 and 2.
    let rank = fixture(WindowFunction::Rank).await;
    let _keep = &rank.dir;
    rank.source
        .append_batch(
            rank.runtime.client(),
            source_batch(&[(10, "a", 5, "insert"), (2, "a", 5, "insert")]),
        )
        .await
        .unwrap();
    rank.runtime
        .refresh_window(&rank.view)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        read_ranks(&rank).await,
        vec![("a".to_string(), 2, 1), ("a".to_string(), 10, 1)]
    );
    assert_matches_oracle(&rank).await;
}
