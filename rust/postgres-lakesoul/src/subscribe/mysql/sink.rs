// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-table MySQL CDC sink.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::execution::memory_pool::MemoryPool;

use super::plan::MySqlTablePlan;
use crate::Result;
use crate::subscribe::decode::KeyValue;
use crate::subscribe::plan::{Op, RowState, commit_id_for_mysql};
use crate::subscribe::sink::{
    STREAM_BATCH_MAX_BYTES, build_batch, commit_outputs_with_id, write_files,
};

/// One generation of a row. `values` is a full target-schema row; the CDC
/// column is filled by `build_batch`.
pub struct MySqlTableSink {
    pub plan: MySqlTablePlan,
    rows: HashMap<Vec<KeyValue>, RowState>,
    memory_pool: Arc<dyn MemoryPool>,
    buffered_bytes: usize,
}

impl MySqlTableSink {
    pub fn new(plan: MySqlTablePlan, memory_pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            plan,
            rows: HashMap::new(),
            memory_pool,
            buffered_bytes: 0,
        }
    }

    pub fn apply(&mut self, key: Vec<KeyValue>, state: RowState) {
        self.buffered_bytes += state.values.iter().map(scalar_size).sum::<usize>();
        self.rows.insert(key, state);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn pending_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn should_flush(&self, max_rows: usize) -> bool {
        !self.rows.is_empty()
            && (self.pending_rows() >= max_rows
                || self.buffered_bytes >= STREAM_BATCH_MAX_BYTES)
    }

    pub fn apply_insert(&mut self, source_values: Vec<ScalarValue>) -> Result<()> {
        let key = self.plan.key_value(&source_values)?;
        let values = self.plan.row_values(&source_values)?;
        self.apply(
            key,
            RowState {
                op: Op::Insert,
                values,
            },
        );
        Ok(())
    }

    pub fn apply_update(
        &mut self,
        before: Vec<ScalarValue>,
        after: Vec<ScalarValue>,
    ) -> Result<()> {
        let old_key = self.plan.key_value(&before)?;
        let new_key = self.plan.key_value(&after)?;
        if old_key != new_key {
            let old_values = self.plan.row_values(&before)?;
            self.apply(
                old_key,
                RowState {
                    op: Op::Delete,
                    values: old_values,
                },
            );
            let new_values = self.plan.row_values(&after)?;
            self.apply(
                new_key,
                RowState {
                    op: Op::Insert,
                    values: new_values,
                },
            );
        } else {
            let values = self.plan.row_values(&after)?;
            self.apply(
                old_key,
                RowState {
                    op: Op::Update,
                    values,
                },
            );
        }
        Ok(())
    }

    pub fn apply_delete(&mut self, before: Vec<ScalarValue>) -> Result<()> {
        let key = self.plan.key_value(&before)?;
        let values = self.plan.row_values(&before)?;
        self.apply(
            key,
            RowState {
                op: Op::Delete,
                values,
            },
        );
        Ok(())
    }

    /// Write the buffered rows to data files without publishing a commit.
    /// Snapshot readers use this to publish one table in a single atomic
    /// commit after all of its files exist.
    pub async fn flush_files(
        &mut self,
    ) -> Result<Vec<lakesoul_io::writer::async_writer::FlushOutput>> {
        if self.rows.is_empty() {
            return Ok(Vec::new());
        }
        let states: Vec<&RowState> = self.rows.values().collect();
        let batch = build_batch(&self.plan.schema, self.plan.cdc_index, &states)?;
        let outputs = write_files(
            &self.plan.table,
            &self.plan.target_table,
            batch,
            &self.memory_pool,
        )
        .await?;
        self.rows.clear();
        self.buffered_bytes = 0;
        Ok(outputs)
    }

    /// Write and publish the batch at `sequence`. Returns the number of rows
    /// committed, or 0 for an empty batch. The buffered rows are cleared only
    /// after the commit succeeds, so a caller that retries keeps the batch.
    pub async fn flush(&mut self, sequence: u64) -> Result<usize> {
        let row_count = self.rows.len();
        if row_count == 0 {
            return Ok(0);
        }
        let states: Vec<&RowState> = self.rows.values().collect();
        let batch = build_batch(&self.plan.schema, self.plan.cdc_index, &states)?;
        let outputs = write_files(
            &self.plan.table,
            &self.plan.target_table,
            batch,
            &self.memory_pool,
        )
        .await?;
        commit_outputs_with_id(
            &self.plan.table,
            &self.plan.target_table,
            outputs,
            commit_id_for_mysql(sequence),
            row_count,
            Some(sequence),
        )
        .await?;
        self.rows.clear();
        self.buffered_bytes = 0;
        tracing::info!(
            target = %self.plan.target_name(),
            rows = row_count,
            sequence,
            "published MySQL subscription batch"
        );
        Ok(row_count)
    }
}

fn scalar_size(value: &ScalarValue) -> usize {
    match value {
        ScalarValue::Utf8(Some(value)) | ScalarValue::LargeUtf8(Some(value)) => {
            value.len()
        }
        ScalarValue::Binary(Some(value)) | ScalarValue::LargeBinary(Some(value)) => {
            value.len()
        }
        ScalarValue::Utf8View(Some(value)) => value.len(),
        _ => 16,
    }
}

/// Convert a row of `mysql_async` values into Arrow values in source order.
pub fn values_from_snapshot_row(
    table: &super::catalog::MySqlTable,
    row: &mysql_async::Row,
) -> Result<Vec<ScalarValue>> {
    let mut values = Vec::with_capacity(table.columns.len());
    for (index, column) in table.columns.iter().enumerate() {
        let value = row
            .as_ref(index)
            .cloned()
            .unwrap_or(mysql_common::value::Value::NULL);
        let target_type = column.data_type.arrow_type()?;
        values.push(super::types::value_to_scalar(
            &value,
            &column.data_type,
            column.charset.as_deref(),
            &target_type,
        )?);
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_size_counts_text() {
        assert_eq!(scalar_size(&ScalarValue::Utf8(Some("abc".into()))), 3);
    }
}
