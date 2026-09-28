// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The DataFusion table provider: register an internal table in a session and
//! query it with SQL, in the current, as-of or pinned-epoch modes.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{
    IVM_ROW_KINDS_COLUMN, IvmRuntime, IvmTable, IvmTableOptions, SumCountView,
    sum_count_mv_schema_for,
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
        Field::new("amount", DataType::Int64, false),
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

/// `g -> (sum_v, count_v)` of any batches with those columns, dropping CDC
/// tombstones when the row kind column is present.
fn mv_map(batches: &[RecordBatch]) -> HashMap<String, (i64, i64)> {
    let mut map = HashMap::new();
    for batch in batches {
        let schema = batch.schema();
        let groups = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(schema.index_of("sum_v").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(schema.index_of("count_v").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = schema.index_of(IVM_ROW_KINDS_COLUMN).ok().map(|index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
        });
        for row in 0..batch.num_rows() {
            if kinds.is_some_and(|kinds| kinds.value(row) != "insert") {
                continue;
            }
            map.insert(
                groups.value(row).to_string(),
                (sums.value(row), counts.value(row)),
            );
        }
    }
    map
}

async fn query_map(context: &SessionContext, sql: &str) -> HashMap<String, (i64, i64)> {
    let frame = context.sql(sql).await.unwrap();
    let batches = frame.collect().await.unwrap();
    mv_map(&batches)
}

struct Fixture {
    runtime: IvmRuntime,
    dir: tempfile::TempDir,
    source: IvmTable,
    mv: IvmTable,
    view: SumCountView,
}

async fn fixture() -> Fixture {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_provider_src_{suffix}"),
                table_path(&dir, "src"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    let group_keys = vec!["g".to_string()];
    let mv = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_provider_mv_{suffix}"),
                table_path(&dir, "mv"),
                sum_count_mv_schema_for(&source.schema, &group_keys, Some("amount"))
                    .unwrap(),
            )
            .with_primary_keys(group_keys.clone()),
        )
        .await
        .unwrap();
    let view = SumCountView::new_with_group_keys(
        format!("provider_{suffix}"),
        source.clone(),
        mv.clone(),
        group_keys,
        Some("amount".to_string()),
    );
    Fixture {
        runtime,
        dir,
        source,
        mv,
        view,
    }
}

#[test_log::test(tokio::test)]
async fn provider_reads_current_as_of_and_pinned_epochs() {
    let fixture = fixture().await;
    let runtime = &fixture.runtime;
    let source = &fixture.source;
    let mv = &fixture.mv;
    let _keep_dir = &fixture.dir;

    // Window 1, then window 2 with a pause so as-of can separate them.
    source
        .append_batch(
            runtime.client(),
            source_batch(&[(1, "a", 10, "insert"), (2, "b", 20, "insert")]),
        )
        .await
        .unwrap();
    runtime
        .refresh_sum_count(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    let generation = runtime
        .metadata()
        .view_generation(&fixture.view.view_id)
        .await
        .unwrap();
    let first = runtime
        .metadata()
        .list_committed_epochs(&fixture.view.view_id, generation)
        .await
        .unwrap()
        .pop()
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    source
        .append_batch(runtime.client(), source_batch(&[(3, "a", 5, "insert")]))
        .await
        .unwrap();
    runtime
        .refresh_sum_count(&fixture.view)
        .await
        .unwrap()
        .unwrap();
    let latest = runtime
        .metadata()
        .list_committed_epochs(&fixture.view.view_id, generation)
        .await
        .unwrap()
        .pop()
        .unwrap();

    let context = SessionContext::new();
    context
        .register_table("mv_current", Arc::new(runtime.table_provider(mv)))
        .unwrap();
    context
        .register_table(
            "mv_pinned",
            Arc::new(runtime.table_provider_at_epoch(mv, &first)),
        )
        .unwrap();
    context
        .register_table(
            "mv_as_of",
            Arc::new(runtime.table_provider_as_of(mv, first.committed_at.unwrap())),
        )
        .unwrap();

    assert_eq!(
        query_map(&context, "select g, sum_v, count_v from mv_current").await,
        mv_map(&runtime.view_state_at_epoch(mv, &latest).await.unwrap())
    );
    let pinned = query_map(&context, "select g, sum_v, count_v from mv_pinned").await;
    assert_eq!(
        pinned,
        mv_map(&runtime.view_state_at_epoch(mv, &first).await.unwrap())
    );
    assert_eq!(
        pinned,
        HashMap::from([("a".to_string(), (10, 1)), ("b".to_string(), (20, 1))])
    );
    assert_eq!(
        query_map(&context, "select g, sum_v, count_v from mv_as_of").await,
        pinned
    );

    // Projection and filter pushdown.
    let projected = context
        .sql("select sum_v from mv_current where sum_v > 10")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    // a = 10 + 5 = 15 and b = 20 both pass the filter.
    assert_eq!(projected[0].num_columns(), 1);
    assert_eq!(projected[0].column(0).len(), 2);
}

#[test_log::test(tokio::test)]
async fn provider_hides_cdc_tombstones() {
    let fixture = fixture().await;
    let runtime = &fixture.runtime;
    let source = &fixture.source;
    let _keep_dir = &fixture.dir;

    source
        .append_batch(
            runtime.client(),
            source_batch(&[
                (1, "a", 10, "insert"),
                (2, "a", 20, "insert"),
                (3, "b", 30, "insert"),
            ]),
        )
        .await
        .unwrap();
    source
        .append_batch(runtime.client(), source_batch(&[(2, "a", 20, "delete")]))
        .await
        .unwrap();

    let context = SessionContext::new();
    context
        .register_table("src_now", Arc::new(runtime.table_provider(source)))
        .unwrap();
    let frame = context
        .sql("select k from src_now order by k")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut keys = Vec::new();
    for batch in &frame {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            keys.push(values.value(row));
        }
    }
    // The deleted row is a tombstone in the merge-on-read state and must not
    // surface through the provider.
    assert_eq!(keys, vec![1, 3]);
}
