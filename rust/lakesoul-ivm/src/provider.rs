// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A DataFusion [`TableProvider`] over the internal LakeSoul tables.
//!
//! Consumers can register an IVM table (or a pinned epoch, or an as-of
//! snapshot) in their own `SessionContext` and query it with SQL.  A scan is a
//! streaming [`IvmStreamExec`]: the reader runs on its own blocking task and
//! hands batches to the plan as they arrive, so the table is never materialized
//! in the driver.
//!
//! The query projection is pushed into the LakeSoul reader (which also keeps
//! the merge key and the change column) and CDC tombstones (`delete`) are
//! dropped after merge-on-read, so a scan returns the logical state by default;
//! [`IvmReadMode::Raw`] exposes the physical merge-on-read rows instead.
//! Filters are pushed into the reader only when every referenced column is a
//! primary key: a key predicate selects whole keys (all their versions), while
//! a payload predicate applied before the merge could drop the latest version
//! of a key and resurrect an older one, so those stay with DataFusion's own
//! `FilterExec`.

use std::sync::Arc;

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use lakesoul_metadata::MetaDataClient;

use crate::error::Result;
use crate::metadata::PartitionVersion;
use crate::table::IvmTable;

/// How an [`IvmTableProvider`] reads its table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IvmReadMode {
    /// The current logical state: merge-on-read with the CDC tombstones
    /// (`delete`) dropped.
    Current,
    /// The state as of a timestamp in milliseconds (inclusive).
    AsOf(i64),
    /// The state pinned to the partition versions of an epoch.
    AtVersions(Vec<PartitionVersion>),
    /// The raw merge-on-read state, including the CDC tombstone rows.  Used
    /// by tools that inspect the changelog protocol; logical queries should
    /// keep the default [`IvmReadMode::Current`].
    Raw,
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

    /// The raw current state, including the CDC tombstone rows.
    pub fn raw(table: IvmTable, client: MetaDataClient) -> Self {
        Self::new(table, client, IvmReadMode::Raw)
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

    /// Only primary-key conjuncts are handed to the reader.
    ///
    /// The reader applies a filter before merge-on-read.  A key predicate
    /// selects whole keys (all of their versions), so the merge result is
    /// unchanged; a payload predicate pushed down could drop the latest
    /// version of a key and resurrect an older one, so those stay with
    /// DataFusion's own `FilterExec`.  The response is `Inexact` for the key
    /// conjuncts: the reader narrows the scan and DataFusion keeps the exact
    /// filter on top.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| {
                if self.is_key_filter(filter) {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let (reader_projection, out_schema) = self
            .reader_projection(projection)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let pushed = self.key_filters(filters);
        // The raw mode keeps the CDC tombstones; every logical mode hides
        // them after merge-on-read.
        let change_filter = match self.mode {
            IvmReadMode::Raw => None,
            _ => self.change_column().map(str::to_string),
        };
        self.stream_plan(reader_projection, out_schema, pushed, change_filter)
    }
}

impl IvmTableProvider {
    /// Whether every column a conjunct references is a primary key.
    fn is_key_filter(&self, filter: &Expr) -> bool {
        let columns = filter.column_refs();
        !columns.is_empty()
            && columns.iter().all(|column| {
                self.table
                    .primary_keys
                    .iter()
                    .any(|key| key == &column.name)
            })
    }

    /// The conjuncts that only constrain primary-key columns and can be
    /// pushed into the reader as a scan narrowing.
    fn key_filters(&self, filters: &[Expr]) -> Vec<Expr> {
        let mut pushed = Vec::new();
        for filter in filters {
            for conjunct in split_conjunction(filter) {
                if self.is_key_filter(conjunct) {
                    pushed.push(conjunct.clone());
                }
            }
        }
        pushed
    }

    /// A streaming, re-executable plan for the provider's read mode.
    ///
    /// The reader runs on a private current-thread runtime inside the blocking
    /// pool (it is not `Send`), and hands batches to the plan as they arrive,
    /// so a scan never materializes the table in the driver.
    fn stream_plan(
        &self,
        reader_projection: Option<SchemaRef>,
        out_schema: SchemaRef,
        filters: Vec<Expr>,
        change_filter: Option<String>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let table = self.table.clone();
        let client = self.client.clone();
        let mode = self.mode.clone();
        let stream_schema = out_schema.clone();
        let plan_schema = out_schema.clone();
        let factory = Arc::new(move || -> DataFusionResult<SendableRecordBatchStream> {
            let mut builder =
                RecordBatchReceiverStreamBuilder::new(stream_schema.clone(), 2);
            let tx = builder.tx();
            let table = table.clone();
            let client = client.clone();
            let mode = mode.clone();
            let reader_projection = reader_projection.clone();
            let out_schema = out_schema.clone();
            let filters = filters.clone();
            let change_filter = change_filter.clone();
            builder.spawn_blocking(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                runtime.block_on(async move {
                    let result = match &mode {
                        IvmReadMode::Current | IvmReadMode::Raw => {
                            table
                                .stream_current(
                                    &client,
                                    filters,
                                    reader_projection.as_ref(),
                                    Some(&out_schema),
                                    change_filter.as_deref(),
                                    tx,
                                )
                                .await
                        }
                        IvmReadMode::AsOf(as_of_ms) => {
                            table
                                .stream_as_of(
                                    &client,
                                    *as_of_ms,
                                    filters,
                                    reader_projection.as_ref(),
                                    Some(&out_schema),
                                    change_filter.as_deref(),
                                    tx,
                                )
                                .await
                        }
                        IvmReadMode::AtVersions(versions) => {
                            table
                                .stream_at_versions(
                                    &client,
                                    versions,
                                    filters,
                                    reader_projection.as_ref(),
                                    Some(&out_schema),
                                    change_filter.as_deref(),
                                    tx,
                                )
                                .await
                        }
                    };
                    result.map_err(|error| DataFusionError::Execution(error.to_string()))
                })
            });
            Ok(builder.build())
        });
        Ok(Arc::new(IvmStreamExec::new(plan_schema, factory)))
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

/// A one-partition, re-executable streaming scan.
struct IvmStreamExec {
    schema: SchemaRef,
    factory: Arc<dyn Fn() -> DataFusionResult<SendableRecordBatchStream> + Send + Sync>,
    properties: Arc<PlanProperties>,
}

impl IvmStreamExec {
    fn new(
        schema: SchemaRef,
        factory: Arc<
            dyn Fn() -> DataFusionResult<SendableRecordBatchStream> + Send + Sync,
        >,
    ) -> Self {
        Self {
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            schema,
            factory,
        }
    }
}

impl std::fmt::Debug for IvmStreamExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IvmStreamExec")
    }
}

impl DisplayAs for IvmStreamExec {
    fn fmt_as(
        &self,
        _format: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "IvmStreamExec")
    }
}

impl ExecutionPlanProperties for IvmStreamExec {
    fn output_partitioning(&self) -> &Partitioning {
        &self.properties.partitioning
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        Boundedness::Bounded
    }

    fn pipeline_behavior(&self) -> EmissionType {
        EmissionType::Incremental
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.properties.eq_properties
    }
}

impl ExecutionPlan for IvmStreamExec {
    fn name(&self) -> &str {
        "IvmStreamExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        (self.factory)()
    }
}
