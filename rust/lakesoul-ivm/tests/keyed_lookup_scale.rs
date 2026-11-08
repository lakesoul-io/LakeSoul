// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A keyed source lookup by an arbitrary (non-PK) join key must return every
//! matching row, including at high cardinality and with unsorted contribution
//! keys.  This guards the join side-state and FULL-join fanout paths.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray, StringViewArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::SessionContext;
use lakesoul_ivm::{IvmRuntime, IvmTableOptions};
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

fn source_batch(rows: &[(i64, String, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|row| row.1.as_str()),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.2))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|_| "insert"))),
        ],
    )
    .unwrap()
}

/// A deterministic shuffle so the contribution is not already in key order.
fn shuffled_keys(count: usize) -> Vec<String> {
    let mut keys = (1..=count).map(|key| format!("g{key}")).collect::<Vec<_>>();
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    for index in (1..keys.len()).rev() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let swap = (state as usize) % (index + 1);
        keys.swap(index, swap);
    }
    keys
}

#[test_log::test(tokio::test)]
async fn keyed_source_lookup_returns_every_high_cardinality_key() {
    let runtime = IvmRuntime::from_env().await.unwrap();
    runtime.init_schema().await.unwrap();
    let dir = tempdir().unwrap();
    let suffix = uuid::Uuid::new_v4().simple();
    let source = runtime
        .create_table(
            IvmTableOptions::new(
                format!("ivm_lookup_scale_{suffix}"),
                table_path(&dir, "source"),
                source_schema(),
            )
            .with_primary_keys(vec!["k".to_string()])
            .with_cdc_column(CHANGE_COLUMN)
            .with_bucket_columns(vec!["k".to_string()]),
        )
        .await
        .unwrap();

    let rows = 100_000usize;
    let mut batch_rows = Vec::with_capacity(8192);
    for key in 1..=rows {
        batch_rows.push((key as i64, format!("g{key}"), key as i64));
        if batch_rows.len() == 8192 || key == rows {
            source
                .append_batch(runtime.client(), source_batch(&batch_rows))
                .await
                .unwrap();
            batch_rows.clear();
        }
    }

    let context = SessionContext::new();
    let keys = shuffled_keys(10_000);
    let contribution = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("g", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from_iter_values(
            keys.iter().map(String::as_str),
        ))],
    )
    .unwrap();
    let contribution = context.read_batches(vec![contribution]).unwrap();
    let frame = runtime
        .lookup_source_by_join_keys(
            &context,
            &source,
            contribution,
            &["g".to_string()],
            &["g".to_string()],
            true,
        )
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    let mut seen = HashSet::new();
    for batch in &batches {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            seen.insert(keys.value(row).to_string());
        }
    }
    let missing = keys
        .iter()
        .filter(|key| !seen.contains(key.as_str()))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "the keyed source lookup dropped {} of {} keys (first: {:?})",
        missing.len(),
        keys.len(),
        &missing[..missing.len().min(10)]
    );

    // A DataFusion plan can hand over the join key as Utf8View even though
    // the table stores Utf8; the lookup must cast it and match every key.
    let contribution = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "g",
            DataType::Utf8View,
            false,
        )])),
        vec![Arc::new(StringViewArray::from_iter_values(
            keys.iter().map(String::as_str),
        ))],
    )
    .unwrap();
    let contribution = context.read_batches(vec![contribution]).unwrap();
    let frame = runtime
        .lookup_source_by_join_keys(
            &context,
            &source,
            contribution,
            &["g".to_string()],
            &["g".to_string()],
            true,
        )
        .await
        .unwrap();
    let batches = frame.collect().await.unwrap();
    let mut seen = HashSet::new();
    for batch in &batches {
        let schema = batch.schema();
        let keys = batch
            .column(schema.index_of("g").unwrap())
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            seen.insert(keys.value(row).to_string());
        }
    }
    let missing = keys
        .iter()
        .filter(|key| !seen.contains(key.as_str()))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "the Utf8View keyed source lookup dropped {} of {} keys (first: {:?})",
        missing.len(),
        keys.len(),
        &missing[..missing.len().min(10)]
    );
}
