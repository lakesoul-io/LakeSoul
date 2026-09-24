// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Projection/filter (`RowView`) and `UNION ALL` (`UnionAllView`) views.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    CompareOp, FilterCondition, IVM_ROW_KINDS_COLUMN, IVM_SOURCE_COLUMN, IvmRuntime,
    IvmTable, IvmTableOptions, LiteralValue, RowView, UnionAllView, row_mv_schema_for,
    union_all_mv_schema_for,
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

fn keyed_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn append_only_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]))
}

fn keyed_batch(rows: &[(i64, &str, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        keyed_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.3))),
        ],
    )
    .unwrap()
}

fn append_only_batch(rows: &[(i64, Option<&str>)]) -> RecordBatch {
    RecordBatch::try_new(
        append_only_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn int_condition(column: &str, op: CompareOp, value: i64) -> FilterCondition {
    FilterCondition {
        column: column.to_string(),
        op,
        value: LiteralValue::Int(value),
    }
}

fn string_condition(column: &str, op: CompareOp, value: &str) -> FilterCondition {
    FilterCondition {
        column: column.to_string(),
        op,
        value: LiteralValue::String(value.to_string()),
    }
}

async fn keyed_source(
    runtime: &IvmRuntime,
    dir: &tempfile::TempDir,
    name: &str,
) -> IvmTable {
    runtime
        .create_table(
            IvmTableOptions::new(name, table_path(dir, name), keyed_schema())
                .with_primary_keys(vec!["k".to_string()])
                .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap()
}

/// The rows of the row view MV as `(k, amount)`.
async fn row_values(runtime: &IvmRuntime, mv: &IvmTable) -> Vec<(i64, i64)> {
    let mut rows = Vec::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("k").unwrap())
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
                rows.push((keys.value(row), amounts.value(row)));
            }
        }
    }
    rows.sort();
    rows
}

/// The SQL semantics of the projected, filtered source.
async fn full_row_values(runtime: &IvmRuntime, source: &IvmTable) -> Vec<(i64, i64)> {
    let context = SessionContext::new();
    register(
        &context,
        "src",
        source.read_current(runtime.client()).await.unwrap(),
        &source.schema,
    );
    let frame = context
        .sql(&format!(
            "select k, amount from src \
             where \"{CHANGE_COLUMN}\" <> 'delete' and amount > 15 order by k"
        ))
        .await
        .unwrap();
    let mut rows = Vec::new();
    for batch in frame.collect().await.unwrap() {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let amounts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((keys.value(row), amounts.value(row)));
        }
    }
    rows.sort();
    rows
}

#[test_log::test(tokio::test)]
async fn row_view_projection_and_filter() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = keyed_source(&runtime, &dir, &format!("ivm_row_src_{suffix}")).await;
    let output_columns = vec!["k".to_string(), "amount".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_row_mv_{suffix}"),
                table_path(&dir, "mv"),
                row_mv_schema_for(&source.schema, &output_columns).unwrap(),
            )
            .with_primary_keys(vec!["k".to_string()]),
        )
        .await
        .unwrap();
    let view = RowView::new(format!("row_{suffix}"), source.clone(), mv.clone())
        .with_output_columns(output_columns)
        .with_filters(vec![int_condition("amount", CompareOp::Gt, 15)]);

    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[
                (1, "a", 10, "insert"),
                (2, "b", 20, "insert"),
                (3, "c", 30, "insert"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_row(&view).await.unwrap().unwrap();
    assert_eq!(row_values(&runtime, &mv).await, vec![(2, 20), (3, 30)]);
    assert_eq!(
        row_values(&runtime, &mv).await,
        full_row_values(&runtime, &source).await
    );

    // Updates crossing the filter in both directions and a delete.
    source
        .append_batch(
            runtime.client(),
            keyed_batch(&[
                (1, "a", 25, "insert"),
                (2, "b", 5, "insert"),
                (3, "c", 30, "delete"),
            ]),
        )
        .await
        .unwrap();
    runtime.refresh_row(&view).await.unwrap().unwrap();
    assert_eq!(
        row_values(&runtime, &mv).await,
        full_row_values(&runtime, &source).await
    );
    assert_eq!(row_values(&runtime, &mv).await, vec![(1, 25)]);

    // An insert passing the filter.
    source
        .append_batch(runtime.client(), keyed_batch(&[(4, "d", 100, "insert")]))
        .await
        .unwrap();
    runtime.refresh_row(&view).await.unwrap().unwrap();
    assert_eq!(
        row_values(&runtime, &mv).await,
        full_row_values(&runtime, &source).await
    );

    runtime.rebuild_row(&view).await.unwrap();
    assert_eq!(
        row_values(&runtime, &mv).await,
        full_row_values(&runtime, &source).await
    );
}

async fn append_only_rows(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> HashSet<(i64, Option<String>)> {
    let mut rows = HashSet::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column(schema.index_of("name").unwrap())
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
                rows.insert((
                    ids.value(row),
                    if names.is_null(row) {
                        None
                    } else {
                        Some(names.value(row).to_string())
                    },
                ));
            }
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn row_view_append_only_source_and_nullable_filters() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_row_append_{suffix}"),
            table_path(&dir, "src"),
            append_only_schema(),
        ))
        .await
        .unwrap();
    let output_columns = vec!["id".to_string(), "name".to_string()];
    let matching = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_row_append_mv_{suffix}"),
            table_path(&dir, "matching"),
            row_mv_schema_for(&source.schema, &output_columns).unwrap(),
        ))
        .await
        .unwrap();
    let matching_view = RowView::new(
        format!("row_append_{suffix}"),
        source.clone(),
        matching.clone(),
    )
    .with_output_columns(output_columns.clone())
    .with_filters(vec![string_condition("name", CompareOp::Eq, "x")]);
    let null_mv = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_row_append_null_{suffix}"),
            table_path(&dir, "nulls"),
            row_mv_schema_for(&source.schema, &output_columns).unwrap(),
        ))
        .await
        .unwrap();
    let null_view = RowView::new(
        format!("row_null_{suffix}"),
        source.clone(),
        null_mv.clone(),
    )
    .with_output_columns(output_columns)
    .with_filters(vec![FilterCondition {
        column: "name".to_string(),
        op: CompareOp::Eq,
        value: LiteralValue::Null,
    }]);

    source
        .append_batch(
            runtime.client(),
            append_only_batch(&[(1, Some("x")), (2, Some("y")), (3, None)]),
        )
        .await
        .unwrap();
    source
        .append_batch(
            runtime.client(),
            append_only_batch(&[(4, Some("x")), (5, None)]),
        )
        .await
        .unwrap();
    runtime.refresh_row(&matching_view).await.unwrap().unwrap();
    runtime.refresh_row(&null_view).await.unwrap().unwrap();
    assert_eq!(
        append_only_rows(&runtime, &matching).await,
        HashSet::from([(1, Some("x".to_string())), (4, Some("x".to_string()))])
    );
    assert_eq!(
        append_only_rows(&runtime, &null_mv).await,
        HashSet::from([(3, None), (5, None)])
    );

    // A rebuild keeps the same rows.
    runtime.rebuild_row(&matching_view).await.unwrap();
    runtime.rebuild_row(&null_view).await.unwrap();
    assert_eq!(
        append_only_rows(&runtime, &matching).await,
        HashSet::from([(1, Some("x".to_string())), (4, Some("x".to_string()))])
    );
    assert_eq!(
        append_only_rows(&runtime, &null_mv).await,
        HashSet::from([(3, None), (5, None)])
    );
}

async fn union_rows(
    runtime: &IvmRuntime,
    mv: &IvmTable,
) -> HashSet<(i32, i64, String, i64)> {
    let mut rows = HashSet::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let sources = batch
            .column(schema.index_of(IVM_SOURCE_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let keys = batch
            .column(schema.index_of("k").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column(schema.index_of("name").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
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
                    sources.value(row),
                    keys.value(row),
                    names.value(row).to_string(),
                    amounts.value(row),
                ));
            }
        }
    }
    rows
}

/// The SQL semantics of `UNION ALL` over the current source states.
async fn full_union_rows(
    runtime: &IvmRuntime,
    sources: &[&IvmTable],
) -> HashSet<(i32, i64, String, i64)> {
    let context = SessionContext::new();
    let mut selects = Vec::new();
    for (index, source) in sources.iter().enumerate() {
        let name = format!("s{index}");
        register(
            &context,
            &name,
            source.read_current(runtime.client()).await.unwrap(),
            &source.schema,
        );
        selects.push(format!(
            "select {index} as src, k, name, amount from {name} \
             where \"{CHANGE_COLUMN}\" <> 'delete'"
        ));
    }
    let frame = context.sql(&selects.join(" union all ")).await.unwrap();
    let mut rows = HashSet::new();
    for batch in frame.collect().await.unwrap() {
        // Integer literals in SQL are Int64.
        let src = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let keys = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let amounts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.insert((
                src.value(row) as i32,
                keys.value(row),
                names.value(row).to_string(),
                amounts.value(row),
            ));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn union_all_tracks_keyed_sources() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = keyed_source(&runtime, &dir, &format!("ivm_union_l_{suffix}")).await;
    let right = keyed_source(&runtime, &dir, &format!("ivm_union_r_{suffix}")).await;
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_union_mv_{suffix}"),
                table_path(&dir, "mv"),
                union_all_mv_schema_for(&left.schema).unwrap(),
            )
            .with_primary_keys(vec![IVM_SOURCE_COLUMN.to_string(), "k".to_string()]),
        )
        .await
        .unwrap();
    let view = UnionAllView::new(
        format!("union_{suffix}"),
        vec![left.clone(), right.clone()],
        mv.clone(),
    );

    left.append_batch(
        runtime.client(),
        keyed_batch(&[(1, "a", 10, "insert"), (2, "b", 20, "insert")]),
    )
    .await
    .unwrap();
    right
        .append_batch(
            runtime.client(),
            keyed_batch(&[(1, "c", 30, "insert"), (3, "d", 40, "insert")]),
        )
        .await
        .unwrap();
    runtime.refresh_union_all(&view).await.unwrap().unwrap();
    assert_eq!(
        union_rows(&runtime, &mv).await,
        full_union_rows(&runtime, &[&left, &right]).await
    );

    // Updates and deletes in either source.
    left.append_batch(runtime.client(), keyed_batch(&[(1, "a", 11, "insert")]))
        .await
        .unwrap();
    left.append_batch(runtime.client(), keyed_batch(&[(2, "b", 20, "delete")]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), keyed_batch(&[(3, "d", 40, "delete")]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), keyed_batch(&[(3, "e", 41, "insert")]))
        .await
        .unwrap();
    runtime.refresh_union_all(&view).await.unwrap().unwrap();
    assert_eq!(
        union_rows(&runtime, &mv).await,
        full_union_rows(&runtime, &[&left, &right]).await
    );

    runtime.rebuild_union_all(&view).await.unwrap();
    assert_eq!(
        union_rows(&runtime, &mv).await,
        full_union_rows(&runtime, &[&left, &right]).await
    );
}

#[test_log::test(tokio::test)]
async fn union_all_append_only_sources() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let left = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_union_append_l_{suffix}"),
            table_path(&dir, "l"),
            append_only_schema(),
        ))
        .await
        .unwrap();
    let right = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_union_append_r_{suffix}"),
            table_path(&dir, "r"),
            append_only_schema(),
        ))
        .await
        .unwrap();
    let mv = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_union_append_mv_{suffix}"),
            table_path(&dir, "mv"),
            union_all_mv_schema_for(&left.schema).unwrap(),
        ))
        .await
        .unwrap();
    let view = UnionAllView::new(
        format!("union_append_{suffix}"),
        vec![left.clone(), right.clone()],
        mv.clone(),
    );

    left.append_batch(runtime.client(), append_only_batch(&[(1, Some("a"))]))
        .await
        .unwrap();
    right
        .append_batch(runtime.client(), append_only_batch(&[(2, Some("b"))]))
        .await
        .unwrap();
    left.append_batch(runtime.client(), append_only_batch(&[(3, Some("c"))]))
        .await
        .unwrap();
    runtime.refresh_union_all(&view).await.unwrap().unwrap();
    assert_eq!(
        source_ids(&runtime, &mv).await,
        HashSet::from([(0, 1), (0, 3), (1, 2)])
    );

    // Nothing new: a refresh is a no-op.
    assert!(runtime.refresh_union_all(&view).await.unwrap().is_none());
    runtime.rebuild_union_all(&view).await.unwrap();
    assert_eq!(
        source_ids(&runtime, &mv).await,
        HashSet::from([(0, 1), (0, 3), (1, 2)])
    );
}

async fn source_ids(runtime: &IvmRuntime, mv: &IvmTable) -> HashSet<(i32, i64)> {
    let mut rows = HashSet::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let sources = batch
            .column(schema.index_of(IVM_SOURCE_COLUMN).unwrap())
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let ids = batch
            .column(schema.index_of("id").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.insert((sources.value(row), ids.value(row)));
        }
    }
    rows
}

#[test_log::test(tokio::test)]
async fn row_and_union_validation() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = keyed_source(&runtime, &dir, &format!("ivm_valid_{suffix}")).await;
    let mv = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_valid_mv_{suffix}"),
            table_path(&dir, "mv"),
            row_mv_schema_for(&source.schema, &[]).unwrap(),
        ))
        .await
        .unwrap();

    // A projected column that does not exist.
    let view = RowView::new(format!("valid_a_{suffix}"), source.clone(), mv.clone())
        .with_output_columns(vec!["missing".to_string()]);
    assert!(
        runtime
            .refresh_row(&view)
            .await
            .unwrap_err()
            .to_string()
            .contains("is not part of the schema")
    );

    // A keyed view must materialize its key.
    let view = RowView::new(format!("valid_b_{suffix}"), source.clone(), mv.clone())
        .with_output_columns(vec!["name".to_string()]);
    assert!(
        runtime
            .refresh_row(&view)
            .await
            .unwrap_err()
            .to_string()
            .contains("output columns must contain the source key")
    );

    // A filter column that does not exist / NULL with an ordering operator.
    let view = RowView::new(format!("valid_c_{suffix}"), source.clone(), mv.clone())
        .with_filters(vec![int_condition("missing", CompareOp::Gt, 1)]);
    assert!(
        runtime
            .refresh_row(&view)
            .await
            .unwrap_err()
            .to_string()
            .contains("filter column missing is not in the source")
    );
    let view = RowView::new(format!("valid_d_{suffix}"), source.clone(), mv.clone())
        .with_filters(vec![FilterCondition {
            column: "name".to_string(),
            op: CompareOp::Lt,
            value: LiteralValue::Null,
        }]);
    assert!(
        runtime
            .refresh_row(&view)
            .await
            .unwrap_err()
            .to_string()
            .contains("NULL can only be compared")
    );

    // Union-all: schemas must match and all sources must be keyed or all
    // append-only.
    let append_only = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_valid_append_{suffix}"),
            table_path(&dir, "append"),
            append_only_schema(),
        ))
        .await
        .unwrap();
    let union_mv = runtime
        .create_table(IvmTableOptions::new(
            format!("ivm_valid_union_mv_{suffix}"),
            table_path(&dir, "union_mv"),
            union_all_mv_schema_for(&source.schema).unwrap(),
        ))
        .await
        .unwrap();
    let view = UnionAllView::new(
        format!("valid_union_a_{suffix}"),
        vec![source.clone(), append_only.clone()],
        union_mv,
    );
    assert!(
        runtime
            .refresh_union_all(&view)
            .await
            .unwrap_err()
            .to_string()
            .contains("does not have the same schema")
    );

    // A mismatching append-only schema is rejected too.
    let append_union_mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_valid_union_append_mv_{suffix}"),
                table_path(&dir, "union_append_mv"),
                union_all_mv_schema_for(&append_only.schema).unwrap(),
            )
            .with_primary_keys(vec![IVM_SOURCE_COLUMN.to_string(), "id".to_string()]),
        )
        .await
        .unwrap();
    let view = UnionAllView::new(
        format!("valid_union_c_{suffix}"),
        vec![append_only.clone(), append_only],
        append_union_mv,
    );
    assert!(runtime.refresh_union_all(&view).await.unwrap().is_none());
}
