// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "512"]

//! The IVM table provider scans as a streaming DataFusion plan.
//!
//! Logical reads hide the CDC tombstones, projections are pushed into the
//! reader (including the empty projection of `COUNT(*)`), and only
//! primary-key predicates reach the reader: a payload predicate must run
//! after merge-on-read, otherwise an older version of a key could resurface.

use std::sync::Arc;

use arrow::util::display::array_value_to_string;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{IvmRuntime, IvmTableOptions};
use tempfile::tempdir;

const CHANGE_COLUMN: &str = "op";

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new(CHANGE_COLUMN, DataType::Utf8, false),
    ]))
}

fn batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.2))),
        ],
    )
    .unwrap()
}

async fn query(context: &SessionContext, sql: &str) -> Vec<Vec<String>> {
    let batches = context.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            rows.push(
                (0..batch.num_columns())
                    .map(|column| {
                        let array = batch.column(column);
                        if array.is_null(row) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(array.as_ref(), row).unwrap()
                        }
                    })
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

#[test_log::test(tokio::test)]
async fn provider_scans_logical_rows_with_pushdown() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let table = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_provider_{suffix}"),
                format!("file://{}/src", dir.path().display()),
                schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN),
        )
        .await
        .unwrap();
    table
        .append_batch(
            runtime.client(),
            batch(&[(1, 10, "insert"), (2, 20, "insert")]),
        )
        .await
        .unwrap();
    // Key 1 is updated, key 2 is deleted and key 3 is new: the merged state
    // is {1: 30, 3: 30}.
    table
        .append_batch(
            runtime.client(),
            batch(&[(1, 30, "insert"), (2, 0, "delete"), (3, 30, "insert")]),
        )
        .await
        .unwrap();

    let context = SessionContext::new();
    let provider: Arc<dyn TableProvider> = Arc::new(runtime.table_provider(&table));
    context.register_table("t", provider).unwrap();

    // The logical state hides the tombstone and keeps the latest version.
    assert_eq!(
        query(&context, "select k, v from t order by k").await,
        vec![vec!["1", "30"], vec!["3", "30"]]
    );
    // The empty projection of COUNT(*) keeps the row count.
    assert_eq!(
        query(&context, "select count(*) from t").await,
        vec![vec!["2"]]
    );
    // A key predicate is pushed into the reader.
    assert_eq!(
        query(&context, "select k, v from t where k = 1").await,
        vec![vec!["1", "30"]]
    );
    assert_eq!(
        query(&context, "select k from t where k = 2").await,
        Vec::<Vec<String>>::new()
    );
    // A payload predicate must be evaluated after merge-on-read: neither the
    // old value of key 1 (10) nor the deleted key 2's value (20) may surface.
    assert_eq!(
        query(&context, "select k from t where v = 10").await,
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        query(&context, "select k from t where v = 20").await,
        Vec::<Vec<String>>::new()
    );
    assert_eq!(
        query(&context, "select k from t where v = 30").await,
        vec![vec!["1"], vec!["3"]]
    );
    // A projected scan through the as-of mode streams too.
    assert_eq!(
        query(&context, "select count(*) from t where v > 0").await,
        vec![vec!["2"]]
    );
}
