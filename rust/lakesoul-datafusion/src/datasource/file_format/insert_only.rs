// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! SQL INSERT validation, separate from sparse/full-row Upsert execution.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::memory_pool::MemoryConsumer;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream, execute_stream,
};
use futures::StreamExt;
use lakesoul_io::writer::async_writer::WriteInvariants;
use object_store::ObjectStoreExt;

use crate::write_mode::{ConflictMode, WriteViolation};

#[derive(Clone, Debug)]
pub(crate) struct InsertOnly {
    pub existing_keys: Arc<dyn ExecutionPlan>,
    /// Range columns followed by PK columns: LakeSoul keys are partition-local.
    pub key_schema: SchemaRef,
    /// Whether a conflicting row fails the statement or is dropped from it.
    pub conflict_mode: ConflictMode,
    /// Qualified table name used in the error message and the SQLSTATE mapping.
    pub table_name: String,
}

impl InsertOnly {
    /// Validate the entire statement before opening any output file. Retain the
    /// input arrays, not copies, and stream historical keys without retaining them.
    ///
    /// With [`ConflictMode::Skip`] the conflicting input rows are removed from
    /// the statement instead of failing it: the first row of a duplicate key
    /// inside the statement wins, and rows whose key is already committed are
    /// dropped. The returned streams then contain only the rows to write, so the
    /// statement's row count is the number of rows actually inserted.
    pub async fn validate(
        &self,
        input: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        invariants: &WriteInvariants,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let schema = input.schema();
        let converter = RowConverter::new(
            self.key_schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?;
        let reservation = MemoryConsumer::new("LakeSoulInsertOnly")
            .register(&context.runtime_env().memory_pool);
        let mut keys = HashSet::<Vec<u8>>::new();
        let mut key_bytes = 0;
        let partitions = input.output_partitioning().partition_count();
        let mut batches = vec![Vec::new(); partitions];
        // Drain all buckets concurrently; the repartitioner's bounded channels
        // can block each other if only one output is consumed at a time.
        let mut incoming = futures::stream::SelectAll::new();
        for partition in 0..partitions {
            incoming.push(
                input
                    .execute(partition, context.clone())?
                    .map(move |batch| (partition, batch))
                    .boxed(),
            );
        }
        while let Some((partition, batch)) = incoming.next().await {
            let batch = batch?;
            invariants.validate(&batch)?;
            if batch.num_rows() == 0 {
                continue;
            }
            reservation.try_grow(
                batch.get_array_memory_size() + 2 * std::mem::size_of::<RecordBatch>(),
            )?;
            batches[partition].push(batch);
        }
        // One mask per buffered batch: `true` keeps the row. A rejected conflict
        // is only ever observed as an error, so building masks first costs one
        // pass over the buffered rows in either mode.
        let mut keep: Vec<Vec<Vec<bool>>> = batches
            .iter()
            .map(|batches| {
                batches
                    .iter()
                    .map(|batch| vec![true; batch.num_rows()])
                    .collect()
            })
            .collect();
        for (partition, batches) in batches.iter().enumerate() {
            for (batch_index, batch) in batches.iter().enumerate() {
                let rows = converter.convert_columns(&self.key_columns(batch)?)?;
                reservation.try_grow(rows.size())?;
                for (row_index, row) in rows.iter().enumerate() {
                    let bytes = row.as_ref();
                    // Include hash-table slots as well as the separately owned key.
                    let size = bytes.len() + 2 * std::mem::size_of::<Vec<u8>>() + 16;
                    reservation.try_grow(size)?;
                    key_bytes += size;
                    if !keys.insert(bytes.to_vec()) {
                        match self.conflict_mode {
                            ConflictMode::Reject => {
                                return Err(DataFusionError::External(Box::new(
                                    WriteViolation::DuplicateKey {
                                        table: self.table_name.clone(),
                                    },
                                )));
                            }
                            ConflictMode::Skip => {
                                keep[partition][batch_index][row_index] = false;
                            }
                        }
                    }
                }
                reservation.shrink(rows.size());
            }
        }
        if !keys.is_empty() {
            // Only the intersection matters: rows of the statement whose key is
            // already committed are the ones skipped or rejected.
            let mut conflicting = HashSet::<Vec<u8>>::new();
            let mut existing = execute_stream(self.existing_keys.clone(), context)?;
            while let Some(batch) = existing.next().await.transpose()? {
                let rows = converter.convert_columns(&self.key_columns(&batch)?)?;
                reservation.try_grow(rows.size())?;
                for row in rows.iter() {
                    if !keys.contains::<[u8]>(row.as_ref()) {
                        continue;
                    }
                    match self.conflict_mode {
                        ConflictMode::Reject => {
                            return Err(DataFusionError::External(Box::new(
                                WriteViolation::ExistingKey {
                                    table: self.table_name.clone(),
                                },
                            )));
                        }
                        ConflictMode::Skip => {
                            conflicting.insert(row.as_ref().to_vec());
                        }
                    }
                }
                reservation.shrink(rows.size());
            }
            if !conflicting.is_empty() {
                for (partition, batches) in batches.iter().enumerate() {
                    for (batch_index, batch) in batches.iter().enumerate() {
                        let rows =
                            converter.convert_columns(&self.key_columns(batch)?)?;
                        for (row_index, row) in rows.iter().enumerate() {
                            if conflicting.contains::<[u8]>(row.as_ref()) {
                                keep[partition][batch_index][row_index] = false;
                            }
                        }
                    }
                }
            }
            drop(conflicting);
        }
        drop(keys);
        reservation.shrink(key_bytes);
        // Retain each physical hash bucket and share the reservation until all
        // buffered streams are dropped. No spilling: limits fail before IO.
        let reservation = Arc::new(reservation);
        let mut streams = Vec::with_capacity(partitions);
        for (batches, keep) in batches.into_iter().zip(keep) {
            let mut kept = Vec::with_capacity(batches.len());
            for (batch, keep) in batches.into_iter().zip(keep) {
                if keep.iter().all(|keep| *keep) {
                    kept.push(batch);
                } else if keep.iter().any(|keep| *keep) {
                    let mask = arrow::array::BooleanArray::from(keep);
                    kept.push(arrow::compute::filter_record_batch(&batch, &mask)?);
                }
            }
            let stream = futures::stream::unfold(
                (kept.into_iter(), reservation.clone()),
                |(mut batches, reservation)| async move {
                    batches
                        .next()
                        .map(|batch| (Ok(batch), (batches, reservation)))
                },
            );
            streams.push(
                Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream))
                    as SendableRecordBatchStream,
            );
        }
        Ok(streams)
    }
    fn key_columns(&self, batch: &RecordBatch) -> Result<Vec<arrow::array::ArrayRef>> {
        self.key_schema
            .fields()
            .iter()
            .map(|field| {
                let column = batch.column_by_name(field.name()).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Missing INSERT key column '{}'",
                        field.name()
                    ))
                })?;
                // Scans can return string views/dictionaries. Compare both sides
                // in stored key types rather than their physical representation.
                Ok(cast(column, field.data_type())?)
            })
            .collect()
    }
}

/// A rejected conditional commit must not leave the statement's data files.
pub(crate) async fn remove_files(context: &TaskContext, files: &[String]) -> Result<()> {
    let mut errors = Vec::new();
    for file in files {
        let result = async {
            let url = ListingTableUrl::parse(file)?;
            let store = context.runtime_env().object_store(url.object_store())?;
            match store.delete(url.prefix()).await {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                Err(error) => Err(DataFusionError::ObjectStore(Box::new(error))),
            }
        }
        .await;
        if let Err(error) = result {
            errors.push(format!("{file}: {error}"));
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(DataFusionError::Execution(format!(
            "Failed to remove rejected INSERT files: {}",
            errors.join("; ")
        )))
    }
}
