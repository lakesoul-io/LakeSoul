// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! TOP-K views: the top `limit` rows of every group, recomputed per affected
//! group, with ties broken deterministically by the source primary keys.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, RowView, TopKView,
    top_k_mv_schema_for,
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
        Field::new("k", DataType::Int64, false),
        Field::new("g", DataType::Utf8, false),
        Field::new("ord", DataType::Int64, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn source_batch(rows: &[(i64, &str, i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.3))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.4))),
        ],
    )
    .unwrap()
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    source: IvmTable,
    mv: IvmTable,
    view: TopKView,
}

async fn fixture(output_columns: Vec<String>, limit: i64) -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_topk_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_topk_mv_{suffix}"),
                table_path(&dir, "mv"),
                top_k_mv_schema_for(&source.schema, &output_columns).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let view = TopKView::new(
        format!("topk_{suffix}"),
        source.clone(),
        mv.clone(),
        vec!["g".to_string()],
        vec!["ord".to_string()],
        limit,
    )
    .with_output_columns(output_columns);
    Fixture {
        runtime,
        dir,
        source,
        mv,
        view,
    }
}

/// The current MV rows as `(k, g, ord, amount)`.
async fn top_k_rows(fixture: &Fixture) -> HashSet<(i64, String, i64, i64)> {
    let mut rows = HashSet::new();
    for batch in fixture
        .mv
        .read_current(fixture.runtime.client())
        .await
        .unwrap()
    {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let orders = batch
            .column(schema.index_of("ord").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let amounts = batch
            .column(schema.index_of("amount").unwrap())
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
                rows.insert((
                    keys.value(row),
                    groups.value(row).to_string(),
                    orders.value(row),
                    amounts.value(row),
                ));
            }
        }
    }
    rows
}

/// The SQL semantics: `row_number() <= limit` per group, ties broken by `k`.
async fn full_top_k_rows(fixture: &Fixture) -> HashSet<(i64, String, i64, i64)> {
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
            "select k, g, ord, amount from ( \
               select k, g, ord, amount, \
                      row_number() over (partition by g order by ord, k) as rn \
               from src where \"{CHANGE_COLUMN}\" <> 'delete' \
             ) where rn <= {}",
            fixture.view.limit
        ))
        .await
        .unwrap();
    let mut rows = HashSet::new();
    for batch in frame.collect().await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let groups = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let orders = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let amounts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.insert((
                keys.value(row),
                groups.value(row).to_string(),
                orders.value(row),
                amounts.value(row),
            ));
        }
    }
    rows
}

async fn assert_matches_oracle(fixture: &Fixture) {
    assert_eq!(top_k_rows(fixture).await, full_top_k_rows(fixture).await);
}

#[test_log::test(tokio::test)]
async fn top_k_keeps_and_replaces_rows() {
    let fixture = fixture(
        vec![
            "k".to_string(),
            "g".to_string(),
            "ord".to_string(),
            "amount".to_string(),
        ],
        2,
    )
    .await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;

    fixture
        .source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 1, 10, "insert"),
                (2, "a", 2, 20, "insert"),
                (3, "a", 3, 30, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_top_k(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        top_k_rows(&fixture).await,
        HashSet::from([(1, "a".to_string(), 1, 10), (2, "a".to_string(), 2, 20),])
    );

    // A row entering the top-k displaces the worst row of the group.
    fixture
        .source
        .append_batch(runtime.client(), source_batch(&[(4, "a", 0, 40, "insert")]))
        .await
        .unwrap();
    runtime.refresh_top_k(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        top_k_rows(&fixture).await,
        HashSet::from([(1, "a".to_string(), 1, 10), (4, "a".to_string(), 0, 40),])
    );

    // An ordering update lets a row fall out of the top-k.
    fixture
        .source
        .append_batch(runtime.client(), source_batch(&[(1, "a", 5, 15, "insert")]))
        .await
        .unwrap();
    runtime.refresh_top_k(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        top_k_rows(&fixture).await,
        HashSet::from([(2, "a".to_string(), 2, 20), (4, "a".to_string(), 0, 40),])
    );

    // Deleting the best row promotes the next one.
    fixture
        .source
        .append_batch(runtime.client(), source_batch(&[(4, "a", 0, 40, "delete")]))
        .await
        .unwrap();
    runtime.refresh_top_k(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        top_k_rows(&fixture).await,
        HashSet::from([(2, "a".to_string(), 2, 20), (3, "a".to_string(), 3, 30),])
    );

    // Ties on the ordering are broken by the primary key.
    fixture
        .source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (5, "b", 1, 50, "insert"),
                (6, "b", 1, 60, "insert"),
                (7, "b", 2, 70, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_top_k(&fixture.view).await.unwrap().unwrap();
    assert_matches_oracle(&fixture).await;
    assert_eq!(
        top_k_rows(&fixture).await,
        HashSet::from([
            (2, "a".to_string(), 2, 20),
            (3, "a".to_string(), 3, 30),
            (5, "b".to_string(), 1, 50),
            (6, "b".to_string(), 1, 60),
        ])
    );

    runtime.rebuild_top_k(&fixture.view).await.unwrap();
    assert_matches_oracle(&fixture).await;
}

/// The MV rows of the projected view as `(k, g, ord)`.
async fn projected_rows(fixture: &Fixture) -> HashSet<(i64, String, i64)> {
    let mut rows = HashSet::new();
    for batch in fixture
        .mv
        .read_current(fixture.runtime.client())
        .await
        .unwrap()
    {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let orders = batch
            .column(schema.index_of("ord").unwrap())
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
                rows.insert((
                    keys.value(row),
                    groups.value(row).to_string(),
                    orders.value(row),
                ));
            }
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn top_k_projection_and_group_moves() {
    let fixture =
        fixture(vec!["k".to_string(), "g".to_string(), "ord".to_string()], 2).await;
    let runtime = &fixture.runtime;
    let _keep_dir = &fixture.dir;

    // The projected schema excludes the payload column.
    assert!(fixture.mv.schema.field_with_name("amount").is_err());

    fixture
        .source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 1, 10, "insert"),
                (2, "a", 2, 20, "insert"),
                (3, "a", 3, 30, "insert"),
                (4, "b", 2, 40, "insert"),
                (5, "b", 3, 50, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_top_k(&fixture.view).await.unwrap().unwrap();
    assert_eq!(
        projected_rows(&fixture).await,
        HashSet::from([
            (1, "a".to_string(), 1),
            (2, "a".to_string(), 2),
            (4, "b".to_string(), 2),
            (5, "b".to_string(), 3),
        ])
    );

    // Moving a row to another group retracts it from the old group and may
    // displace a row of the new one.
    fixture
        .source
        .append_batch(runtime.client(), source_batch(&[(1, "b", 0, 10, "insert")]))
        .await
        .unwrap();
    runtime.refresh_top_k(&fixture.view).await.unwrap().unwrap();
    assert_eq!(
        projected_rows(&fixture).await,
        HashSet::from([
            (2, "a".to_string(), 2),
            (3, "a".to_string(), 3),
            (1, "b".to_string(), 0),
            (4, "b".to_string(), 2),
        ])
    );

    runtime.rebuild_top_k(&fixture.view).await.unwrap();
    assert_eq!(
        projected_rows(&fixture).await,
        HashSet::from([
            (2, "a".to_string(), 2),
            (3, "a".to_string(), 3),
            (1, "b".to_string(), 0),
            (4, "b".to_string(), 2),
        ])
    );
}

#[test_log::test(tokio::test)]
async fn top_k_validation() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_topk_valid_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_topk_valid_mv_{suffix}"),
                table_path(&dir, "mv"),
                top_k_mv_schema_for(&source.schema, &[]).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();

    // A non-positive limit.
    let view = TopKView::new(
        format!("valid_a_{suffix}"),
        source.clone(),
        mv.clone(),
        vec!["g".to_string()],
        vec!["ord".to_string()],
        0,
    );
    assert!(
        runtime
            .refresh_top_k(&view)
            .await
            .unwrap_err()
            .to_string()
            .contains("positive limit")
    );

    // Empty group or order keys.
    let view = TopKView::new(
        format!("valid_b_{suffix}"),
        source.clone(),
        mv.clone(),
        Vec::new(),
        vec!["ord".to_string()],
        1,
    );
    assert!(
        runtime
            .refresh_top_k(&view)
            .await
            .unwrap_err()
            .to_string()
            .contains("group and order keys")
    );

    // A keyed source is required.
    let append_only = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_topk_append_{suffix}"),
            table_path(&dir, "append"),
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("g", DataType::Utf8, false),
                Field::new("ord", DataType::Int64, false),
            ])),
        ))
        .await
        .unwrap();
    let append_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_topk_append_mv_{suffix}"),
                table_path(&dir, "append_mv"),
                top_k_mv_schema_for(&append_only.schema, &[]).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let view = TopKView::new(
        format!("valid_c_{suffix}"),
        append_only.clone(),
        append_mv,
        vec!["g".to_string()],
        vec!["ord".to_string()],
        1,
    );
    assert!(
        runtime
            .refresh_top_k(&view)
            .await
            .unwrap_err()
            .to_string()
            .contains("primary key")
    );

    // The output must contain the group keys and the primary keys.
    let view = TopKView::new(
        format!("valid_d_{suffix}"),
        source.clone(),
        mv.clone(),
        vec!["g".to_string()],
        vec!["ord".to_string()],
        1,
    )
    .with_output_columns(vec!["amount".to_string()]);
    assert!(
        runtime
            .refresh_top_k(&view)
            .await
            .unwrap_err()
            .to_string()
            .contains("output columns must contain")
    );

    // An unknown filter/projection column is rejected by the schema helper.
    assert!(top_k_mv_schema_for(&source_schema(), &["missing".to_string()]).is_err());
    let _ = RowView::new(format!("unused_{suffix}"), source, mv);
}
