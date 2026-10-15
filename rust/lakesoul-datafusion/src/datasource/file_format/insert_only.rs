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
use lakesoul_metadata_proto::entity::PartitionInfo;
use object_store::ObjectStoreExt;

#[derive(Clone, Debug)]
pub(super) struct InsertOnly {
    pub existing_keys: Arc<dyn ExecutionPlan>,
    /// Captured before planning the existing-key scan. Publication must not rebase.
    pub snapshot: Vec<PartitionInfo>,
    /// Range columns followed by PK columns: LakeSoul keys are partition-local.
    pub key_schema: SchemaRef,
}

impl InsertOnly {
    /// Validate the entire statement before opening any output file. Retain the
    /// input arrays, not copies, and stream historical keys without retaining them.
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
            let rows = converter.convert_columns(&self.key_columns(&batch)?)?;
            reservation.try_grow(rows.size())?;
            for row in rows.iter() {
                let bytes = row.as_ref();
                // Include hash-table slots as well as the separately owned key.
                let size = bytes.len() + 2 * std::mem::size_of::<Vec<u8>>() + 16;
                reservation.try_grow(size)?;
                key_bytes += size;
                if !keys.insert(bytes.to_vec()) {
                    return Err(DataFusionError::Execution(
                        "INSERT contains a duplicate primary key; use INSERT OR REPLACE for Upsert"
                            .into(),
                    ));
                }
            }
            reservation.shrink(rows.size());
            batches[partition].push(batch);
        }
        if !keys.is_empty() {
            let mut existing = execute_stream(self.existing_keys.clone(), context)?;
            while let Some(batch) = existing.next().await.transpose()? {
                let rows = converter.convert_columns(&self.key_columns(&batch)?)?;
                reservation.try_grow(rows.size())?;
                if rows.iter().any(|row| keys.contains::<[u8]>(row.as_ref())) {
                    return Err(DataFusionError::Execution(
                        "INSERT conflicts with an existing primary key; use INSERT OR REPLACE for Upsert"
                            .into(),
                    ));
                }
                reservation.shrink(rows.size());
            }
        }
        drop(keys);
        reservation.shrink(key_bytes);
        // Retain each physical hash bucket and share the reservation until all
        // buffered streams are dropped. No spilling: limits fail before IO.
        let reservation = Arc::new(reservation);
        Ok(batches
            .into_iter()
            .map(|batches| {
                let stream = futures::stream::unfold(
                    (batches.into_iter(), reservation.clone()),
                    |(mut batches, reservation)| async move {
                        batches
                            .next()
                            .map(|batch| (Ok(batch), (batches, reservation)))
                    },
                );
                Box::pin(RecordBatchStreamAdapter::new(schema.clone(), stream))
                    as SendableRecordBatchStream
            })
            .collect())
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
pub(super) async fn remove_files(context: &TaskContext, files: &[String]) -> Result<()> {
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
