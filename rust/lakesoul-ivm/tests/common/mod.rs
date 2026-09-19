// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Shared helpers for the IVM integration tests.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_ivm::{
    IVM_COUNT_COLUMN, IVM_ROW_KINDS_COLUMN, IVM_SUM_COLUMN, IvmRuntime, IvmTable,
};
use tempfile::TempDir;

pub fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]))
}

pub fn source_batch(rows: &[(i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        source_schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
        ],
    )
    .unwrap()
}

pub fn table_path(dir: &TempDir, name: &str) -> String {
    format!("file://{}", dir.path().join(name).display())
}

/// Read the current state of a sum/count MV as `group -> (sum, count)`.
pub async fn mv_state(runtime: &IvmRuntime, mv: &IvmTable) -> HashMap<i64, (i64, i64)> {
    let mut state = HashMap::new();
    for batch in mv.read_current(runtime.client()).await.unwrap() {
        let schema = batch.schema();
        let key_index = schema.index_of("k").unwrap();
        let sum_index = schema.index_of(IVM_SUM_COLUMN).unwrap();
        let count_index = schema.index_of(IVM_COUNT_COLUMN).unwrap();
        let kind_index = schema.index_of(IVM_ROW_KINDS_COLUMN).unwrap();

        let keys = batch
            .column(key_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(sum_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let counts = batch
            .column(count_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let kinds = batch
            .column(kind_index)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            if kinds.value(row) == "insert" {
                state.insert(keys.value(row), (sums.value(row), counts.value(row)));
            }
        }
    }
    state
}

/// The latest partition version of a table, or `None` when it has no data yet.
pub async fn latest_version(runtime: &IvmRuntime, table: &IvmTable) -> Option<i64> {
    runtime
        .client()
        .get_all_partition_info(&table.table_id)
        .await
        .unwrap()
        .first()
        .map(|partition| i64::from(partition.version))
}
