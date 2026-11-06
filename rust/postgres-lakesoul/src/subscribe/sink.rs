// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Per-batch LakeSoul sink and the shared writer primitives used by the
//! initial snapshot.

use std::collections::{BTreeMap, HashMap};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::ScalarValue;
use lakesoul_common::misc::SecretMap;
use lakesoul_datafusion::lakesoul_table::LakeSoulTable;
use lakesoul_io::writer::async_writer::FlushOutput;
use lakesoul_metadata_proto::entity::{CommitOp, DataCommitInfo, DataFileOp, FileOp};
use rootcause::{bail, report};

use super::decode::KeyValue;
use super::plan::{RowState, TablePlan, commit_id_for};
use crate::Result;

pub struct TableSink {
    pub plan: TablePlan,
    rows: HashMap<Vec<KeyValue>, RowState>,
}

impl TableSink {
    pub fn new(plan: TablePlan) -> Self {
        Self {
            plan,
            rows: HashMap::new(),
        }
    }

    pub fn apply(&mut self, key: Vec<KeyValue>, state: RowState) {
        self.rows.insert(key, state);
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn pending_rows(&self) -> usize {
        self.rows.len()
    }

    /// Write and publish the batch. Returns the LSN that became durable, or
    /// `None` when the batch was empty.
    pub async fn flush(&mut self, batch_lsn: u64) -> Result<Option<u64>> {
        if self.rows.is_empty() {
            return Ok(None);
        }
        let row_count = self.rows.len();
        let states: Vec<&RowState> = self.rows.values().collect();
        let batch = build_batch(&self.plan.schema, self.plan.cdc_index, &states)?;
        let outputs =
            write_files(&self.plan.table, &self.plan.target_table, batch).await?;
        commit_outputs(
            &self.plan.table,
            &self.plan.target_table,
            outputs,
            batch_lsn,
            row_count,
        )
        .await?;
        self.rows.clear();
        Ok(Some(batch_lsn))
    }
}

/// Build a full-schema batch. `states` are in arbitrary order (the writer
/// sorts by primary key); the CDC column is filled from each row's operation.
pub(crate) fn build_batch(
    schema: &SchemaRef,
    cdc_index: usize,
    states: &[&RowState],
) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (index, field) in schema.fields().iter().enumerate() {
        let scalars: Vec<ScalarValue> = states
            .iter()
            .map(|state| {
                if index == cdc_index {
                    ScalarValue::Utf8(Some(state.op.as_str().to_string()))
                } else {
                    state.values[index].clone()
                }
            })
            .collect();
        let array = ScalarValue::iter_to_array(scalars)
            .map_err(|error| report!("build target column {}: {error}", field.name()))?;
        columns.push(array);
    }
    RecordBatch::try_new(schema.clone(), columns)
        .map_err(|error| report!("build target batch: {error}"))
}

pub(crate) async fn write_files(
    table: &LakeSoulTable,
    target_table: &str,
    batch: RecordBatch,
) -> Result<Vec<FlushOutput>> {
    let mut writer = table
        .get_writer(SecretMap::new())
        .await
        .map_err(|error| report!("create LakeSoul writer for {target_table}: {error}"))?;
    const CHUNK_ROWS: usize = 8192;
    let mut offset = 0;
    while offset < batch.num_rows() {
        let length = CHUNK_ROWS.min(batch.num_rows() - offset);
        let chunk = batch.slice(offset, length);
        if let Err(error) = writer.write_record_batch(chunk).await {
            let _ = writer.abort_and_close().await;
            return Err(report!("write batch into {target_table}: {error}"));
        }
        offset += length;
    }
    writer
        .flush_and_close()
        .await
        .map_err(|error| report!("flush batch into {target_table}: {error}"))
}

pub(crate) async fn commit_outputs(
    table: &LakeSoulTable,
    target_table: &str,
    outputs: Vec<FlushOutput>,
    lsn: u64,
    row_count: usize,
) -> Result<()> {
    let started = std::time::Instant::now();
    let commit_id = commit_id_for(lsn);
    let table_info = table.table_info();
    let mut files_by_partition: BTreeMap<String, Vec<DataFileOp>> = BTreeMap::new();
    for output in outputs {
        files_by_partition
            .entry(output.partition_desc.clone())
            .or_default()
            .push(DataFileOp {
                path: output.file_path.clone(),
                file_op: FileOp::Add.into(),
                size: output.object_meta.size as i64,
                file_exist_cols: output.file_exist_cols.join(","),
            });
    }
    if files_by_partition.is_empty() {
        bail!("target {target_table} produced no files for {row_count} rows");
    }
    let commits: Vec<DataCommitInfo> = files_by_partition
        .into_iter()
        .map(|(partition_desc, file_ops)| DataCommitInfo {
            table_id: table_info.table_id.clone(),
            partition_desc,
            pinned: false,
            commit_id: Some(commit_id),
            file_ops,
            commit_op: CommitOp::AppendCommit.into(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            committed: false,
            domain: table_info.domain.clone(),
        })
        .collect();

    table
        .client()
        .commit_data_commit_infos(commits)
        .await
        .map_err(|error| {
            report!("commit {row_count} rows into {target_table} at LSN {lsn}: {error}")
        })?;

    tracing::info!(
        table = %target_table,
        rows = row_count,
        lsn,
        elapsed_ms = started.elapsed().as_millis(),
        "published subscription batch"
    );
    Ok(())
}
