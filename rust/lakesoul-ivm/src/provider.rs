// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A DataFusion [`TableProvider`] over the internal LakeSoul tables.
//!
//! Consumers can register an IVM table (or a pinned epoch, or an as-of
//! snapshot) in their own `SessionContext` and query it with SQL, without
//! materializing the batches themselves. The provider pushes the query
//! projection into the LakeSoul reader (keeping the merge key and the change
//! column) and hides CDC tombstones, so a scan returns the logical state.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_array::{Array, BooleanArray};
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use lakesoul_metadata::MetaDataClient;

use crate::error::Result;
use crate::metadata::PartitionVersion;
use crate::table::IvmTable;

/// How an [`IvmTableProvider`] reads its table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IvmReadMode {
    /// The current merge-on-read state.
    Current,
    /// The state as of a timestamp in milliseconds (inclusive).
    AsOf(i64),
    /// The state pinned to the partition versions of an epoch.
    AtVersions(Vec<PartitionVersion>),
}

/// A DataFusion table provider over one internal LakeSoul table.
#[derive(Debug, Clone)]
pub struct IvmTableProvider {
    table: IvmTable,
    client: MetaDataClient,
    mode: IvmReadMode,
}

impl IvmTableProvider {
    /// A provider for `table` in the given read mode.
    pub fn new(table: IvmTable, client: MetaDataClient, mode: IvmReadMode) -> Self {
        Self {
            table,
            client,
            mode,
        }
    }

    /// The current state of the table.
    pub fn current(table: IvmTable, client: MetaDataClient) -> Self {
        Self::new(table, client, IvmReadMode::Current)
    }

    /// The state as of `as_of_ms` (inclusive).
    pub fn as_of(table: IvmTable, client: MetaDataClient, as_of_ms: i64) -> Self {
        Self::new(table, client, IvmReadMode::AsOf(as_of_ms))
    }

    /// The state pinned to the given partition versions.
    pub fn at_versions(
        table: IvmTable,
        client: MetaDataClient,
        versions: Vec<PartitionVersion>,
    ) -> Self {
        Self::new(table, client, IvmReadMode::AtVersions(versions))
    }

    /// The read mode.
    pub fn mode(&self) -> &IvmReadMode {
        &self.mode
    }

    fn change_column(&self) -> Option<&str> {
        if let Some(column) = self.table.cdc_column.as_deref()
            && self.table.schema.field_with_name(column).is_ok()
        {
            return Some(column);
        }
        self.table
            .schema
            .field_with_name(crate::table::IVM_ROW_KINDS_COLUMN)
            .ok()
            .map(|_| crate::table::IVM_ROW_KINDS_COLUMN)
    }

    /// The reader projection and the schema the scan returns.
    ///
    /// The reader projection is the requested one plus the merge key and the
    /// change column, which merge-on-read and tombstone filtering need; the
    /// scan result is then projected back to the requested columns.
    fn reader_projection(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Result<(Option<SchemaRef>, SchemaRef)> {
        let schema = &self.table.schema;
        let Some(indices) = projection else {
            return Ok((None, schema.clone()));
        };
        let mut needed = indices.clone();
        let mut extras = Vec::new();
        for key in &self.table.primary_keys {
            if let Ok(index) = schema.index_of(key) {
                extras.push(index);
            }
        }
        if let Some(column) = self.change_column()
            && let Ok(index) = schema.index_of(column)
        {
            extras.push(index);
        }
        for index in extras {
            if !needed.contains(&index) {
                needed.push(index);
            }
        }
        let needed_fields = needed
            .iter()
            .map(|index| Arc::new(schema.field(*index).clone()))
            .collect::<Vec<Arc<Field>>>();
        Ok((
            Some(Arc::new(Schema::new(needed_fields))),
            project_schema(schema, indices),
        ))
    }
}

#[async_trait::async_trait]
impl TableProvider for IvmTableProvider {
    fn schema(&self) -> SchemaRef {
        self.table.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (reader_projection, out_schema) = self
            .reader_projection(projection)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let table = self.table.clone();
        let client = self.client.clone();
        let mode = self.mode.clone();
        // The LakeSoul reader is not `Sync`, so its futures are not `Send` and
        // cannot run inside the (Send) scan future. Run the read on the
        // blocking pool with a private current-thread runtime; the reader is
        // constructed there, so nothing non-Send crosses threads.
        let batches = tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| {
                    rootcause::report!("building the IVM provider runtime: {error}")
                })?;
            runtime.block_on(async move {
                match &mode {
                    IvmReadMode::Current => {
                        table
                            .read_current_projected(&client, reader_projection.as_ref())
                            .await
                    }
                    IvmReadMode::AsOf(as_of_ms) => {
                        table
                            .read_as_of_projected(
                                &client,
                                *as_of_ms,
                                reader_projection.as_ref(),
                            )
                            .await
                    }
                    IvmReadMode::AtVersions(versions) => {
                        table
                            .read_at_versions_projected(
                                &client,
                                versions,
                                reader_projection.as_ref(),
                            )
                            .await
                    }
                }
            })
        })
        .await
        .map_err(|error| DataFusionError::Execution(error.to_string()))?
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let batches = self
            .drop_tombstones(batches)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let batches = match projection {
            Some(_) => project_batches(&batches, &out_schema),
            None => batches,
        };
        Ok(MemorySourceConfig::try_new_exec(
            &[batches],
            out_schema,
            None,
        )?)
    }
}

impl IvmTableProvider {
    /// Drop the rows whose latest version is a CDC tombstone.
    fn drop_tombstones(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let Some(column) = self.change_column() else {
            return Ok(batches);
        };
        let mut kept = Vec::with_capacity(batches.len());
        for batch in batches {
            let index = batch.schema().index_of(column).map_err(|_| {
                rootcause::report!("change column {column} is not part of the batch")
            })?;
            let values = batch
                .column(index)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .ok_or_else(|| {
                    rootcause::report!("change column {column} is not a string column")
                })?;
            let mask = BooleanArray::from_iter(
                (0..values.len()).map(|row| Some(values.value(row) != "delete")),
            );
            kept.push(arrow::compute::filter_record_batch(&batch, &mask)?);
        }
        Ok(kept)
    }
}

/// The schema of `indices`, in that order.
fn project_schema(schema: &SchemaRef, indices: &[usize]) -> SchemaRef {
    Arc::new(Schema::new(
        indices
            .iter()
            .map(|index| schema.field(*index).clone())
            .collect::<Vec<_>>(),
    ))
}

/// The first `out_schema.fields().len()` columns of every batch are the
/// requested ones (the reader projection puts them first).
fn project_batches(batches: &[RecordBatch], out_schema: &SchemaRef) -> Vec<RecordBatch> {
    let count = out_schema.fields().len();
    batches
        .iter()
        .map(|batch| {
            let columns = (0..count)
                .map(|position| batch.column(position).clone())
                .collect::<Vec<_>>();
            RecordBatch::try_new(out_schema.clone(), columns)
                .expect("the projected columns match the output schema")
        })
        .collect()
}
