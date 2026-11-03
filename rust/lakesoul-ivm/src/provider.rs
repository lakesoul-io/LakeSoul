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

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType, lit};
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalExpr};
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchReceiverStreamBuilder;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::Stream;
use lakesoul_io::constant::DEFAULT_PARTITION_DESC;
use lakesoul_metadata::MetaDataClient;

use crate::error::Result;
use crate::metadata::PartitionVersion;
use crate::observability::{self, RefreshIo};
use crate::table::{IvmTable, PartitionFiles, file_bucket};

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
    /// Explicit file groups, used by refresh paths that already resolved the
    /// window/baseline metadata.  When set, `mode` only contributes the
    /// tombstone policy.
    groups: Option<Vec<PartitionFiles>>,
    /// Whether the reader merges duplicate primary keys.  Unmerged scans
    /// expose the physical delete+insert changelog of a delta.
    merge: bool,
    /// Optional per-hash-bucket key filters.  The refresh uses this for
    /// state lookups: the full key list is much larger than one bucket's
    /// slice, and handing every bucket partition the complete list makes the
    /// locator carry redundant candidates.
    bucket_filters: Option<HashMap<u32, Vec<Expr>>>,
}

impl IvmTableProvider {
    /// A provider for `table` in the given read mode.
    pub fn new(table: IvmTable, client: MetaDataClient, mode: IvmReadMode) -> Self {
        Self {
            table,
            client,
            mode,
            groups: None,
            merge: true,
            bucket_filters: None,
        }
    }

    /// A provider over explicit file groups, without another metadata read.
    pub(crate) fn groups(
        table: IvmTable,
        client: MetaDataClient,
        groups: Vec<PartitionFiles>,
    ) -> Self {
        Self {
            table,
            client,
            // Explicit groups are used for delta/baseline reads: keep the
            // CDC tombstones, exactly like `IvmTable::read_partition_files`.
            mode: IvmReadMode::Raw,
            groups: Some(groups),
            merge: true,
            bucket_filters: None,
        }
    }

    /// A provider over explicit delta groups **without** merge-on-read.
    pub(crate) fn unmerged_groups(
        table: IvmTable,
        client: MetaDataClient,
        groups: Vec<PartitionFiles>,
    ) -> Self {
        Self {
            table,
            client,
            mode: IvmReadMode::Raw,
            groups: Some(groups),
            merge: false,
            bucket_filters: None,
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

    /// Attach per-hash-bucket reader filters.
    ///
    /// The provider still applies the DataFusion filter for correctness; these
    /// filters only narrow each bucket's reader and PK locator candidate set.
    pub(crate) fn with_bucket_filters(
        mut self,
        filters: HashMap<u32, Vec<Expr>>,
    ) -> Self {
        self.bucket_filters = Some(filters);
        self
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

impl IvmTableProvider {
    pub(crate) async fn groups_for_mode(&self) -> DataFusionResult<Vec<PartitionFiles>> {
        if let Some(groups) = &self.groups {
            return Ok(groups.clone());
        }
        let groups = match &self.mode {
            IvmReadMode::Current | IvmReadMode::Raw => {
                self.table.current_groups(&self.client).await
            }
            IvmReadMode::AsOf(as_of_ms) => {
                self.table.as_of_groups(&self.client, *as_of_ms).await
            }
            IvmReadMode::AtVersions(versions) => {
                self.table.version_groups(&self.client, versions).await
            }
        };
        groups.map_err(|error| DataFusionError::Execution(error.to_string()))
    }

    /// Split one range partition's files by their hash bucket.
    ///
    /// A LakeSoul metadata partition holds every bucket of that range
    /// partition.  Reading it as one group makes the provider one DataFusion
    /// partition and one reader task, even when the table has 16 buckets.
    /// Bucket files contain disjoint merge keys, so splitting them preserves
    /// merge-on-read semantics while exposing one stream per bucket.
    fn split_bucket_groups(&self, groups: Vec<PartitionFiles>) -> Vec<PartitionFiles> {
        if self.table.effective_bucket_columns().is_empty() {
            return groups;
        }
        let bucket_count = self
            .table
            .hash_bucket_num
            .parse::<u32>()
            .ok()
            .filter(|count| *count > 1);
        let mut split = Vec::with_capacity(groups.len());
        for group in groups {
            if group.files.len() <= 1 {
                split.push(group);
                continue;
            }
            let mut buckets: BTreeMap<Option<u32>, Vec<String>> = BTreeMap::new();
            for file in group.files {
                let bucket = file_bucket(&file)
                    .filter(|bucket| bucket_count.is_none_or(|count| *bucket < count));
                buckets.entry(bucket).or_default().push(file);
            }
            if buckets.len() <= 1 {
                split.push(PartitionFiles {
                    partition_desc: group.partition_desc,
                    files: buckets.into_values().flatten().collect(),
                });
            } else {
                for files in buckets.into_values() {
                    split.push(PartitionFiles {
                        partition_desc: group.partition_desc.clone(),
                        files,
                    });
                }
            }
        }
        split
    }

    pub(crate) fn stream_plan_groups(
        &self,
        groups: Vec<PartitionFiles>,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let groups = self.split_bucket_groups(groups);
        let (reader_projection, out_schema) = self
            .reader_projection(projection)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let pushed = self.key_filters(filters);
        let filters_per_group = if let Some(routing) = &self.bucket_filters {
            groups
                .iter()
                .map(|group| {
                    let bucket = group.files.first().and_then(|file| file_bucket(file));
                    match bucket.and_then(|bucket| routing.get(&bucket)) {
                        Some(filters) if !filters.is_empty() => filters.clone(),
                        // Keep the partition but read no rows: its bucket has
                        // no keys in this refresh.
                        Some(_) => vec![lit(false)],
                        None => pushed.clone(),
                    }
                })
                .collect()
        } else {
            vec![pushed.clone(); groups.len()]
        };
        // The raw mode keeps the CDC tombstones; every logical mode hides
        // them after merge-on-read.  An unmerged delta scan exposes the
        // physical changelog.
        let change_filter = if self.merge {
            match self.mode {
                IvmReadMode::Raw => None,
                _ => self.change_column().map(str::to_string),
            }
        } else {
            None
        };
        self.stream_plan(
            groups,
            reader_projection,
            out_schema,
            filters_per_group,
            change_filter,
            self.merge,
        )
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
        let groups = self.groups_for_mode().await?;
        self.stream_plan_groups(groups, projection, filters)
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
        groups: Vec<PartitionFiles>,
        reader_projection: Option<SchemaRef>,
        out_schema: SchemaRef,
        filters_per_group: Vec<Vec<Expr>>,
        change_filter: Option<String>,
        merge: bool,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let partitions = groups.len().max(1);
        let file_counts = Arc::new(
            groups
                .iter()
                .map(|group| group.files.len())
                .collect::<Vec<_>>(),
        );
        let refresh_io = observability::current_refresh_io();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(IvmStreamExec::new(
            self.table.clone(),
            Arc::new(groups),
            Arc::new(filters_per_group),
            reader_projection,
            out_schema,
            change_filter,
            merge,
            partitions,
            refresh_io,
            file_counts,
        ));
        // State/MV files can produce tiny batches after merge.  Coalescing at
        // the scan boundary is cheap and keeps every downstream operator from
        // looping over one-row batches.
        Ok(Arc::new(CoalesceBatchesExec::new(plan, 8192)))
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
///
/// The plan owns only plain values and table/file references.  It deliberately
/// does not capture a closure: the execution configuration must remain
/// serializable for a future distributed scheduler.
struct IvmStreamExec {
    table: IvmTable,
    groups: Arc<Vec<PartitionFiles>>,
    filters_per_group: Arc<Vec<Vec<Expr>>>,
    reader_projection: Option<SchemaRef>,
    out_schema: SchemaRef,
    change_filter: Option<String>,
    merge: bool,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    refresh_io: Option<Arc<Mutex<RefreshIo>>>,
    file_counts: Arc<Vec<usize>>,
    gate: Arc<tokio::sync::Semaphore>,
    metrics: ExecutionPlanMetricsSet,
}

impl IvmStreamExec {
    #[allow(clippy::too_many_arguments)]
    fn new(
        table: IvmTable,
        groups: Arc<Vec<PartitionFiles>>,
        filters_per_group: Arc<Vec<Vec<Expr>>>,
        reader_projection: Option<SchemaRef>,
        out_schema: SchemaRef,
        change_filter: Option<String>,
        merge: bool,
        partitions: usize,
        refresh_io: Option<Arc<Mutex<RefreshIo>>>,
        file_counts: Arc<Vec<usize>>,
    ) -> Self {
        Self {
            table,
            groups,
            filters_per_group,
            reader_projection,
            schema: out_schema.clone(),
            out_schema: out_schema.clone(),
            change_filter,
            merge,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(out_schema),
                Partitioning::UnknownPartitioning(partitions.max(1)),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
            refresh_io,
            file_counts,
            gate: Arc::new(tokio::sync::Semaphore::new(
                crate::runtime::keyed_plan::max_active_tasks(),
            )),
            metrics: ExecutionPlanMetricsSet::new(),
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
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
        partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let mut builder = RecordBatchReceiverStreamBuilder::new(self.schema.clone(), 2);
        let tx = builder.tx();
        let group =
            self.groups
                .get(partition)
                .cloned()
                .unwrap_or_else(|| PartitionFiles {
                    partition_desc: DEFAULT_PARTITION_DESC.to_string(),
                    files: Vec::new(),
                });
        if group.files.is_empty() {
            drop(tx);
            return Ok(builder.build());
        }

        let table = self.table.clone();
        let filters = self
            .filters_per_group
            .get(partition)
            .cloned()
            .unwrap_or_default();
        let reader_projection = self.reader_projection.clone();
        let out_schema = self.out_schema.clone();
        let change_filter = self.change_filter.clone();
        let merge = self.merge;
        let gate = self.gate.clone();
        builder.spawn_blocking(move || {
            // LakeSoulIO owns the process-wide reader/writer runtime.  Drive
            // every scan stream on that runtime instead of creating a new
            // current-thread runtime per DataFusion partition.  This plan's
            // gate keeps at most four reader/state tasks active even when the
            // table has more buckets than the task budget.
            let _permit = lakesoul_io::session::GLOBAL_RUNTIME.block_on(async {
                gate.acquire_owned().await.map_err(|error| {
                    DataFusionError::Execution(format!("scan task gate closed: {error}"))
                })
            })?;
            lakesoul_io::session::GLOBAL_RUNTIME.block_on(async move {
                table
                    .stream_files_with_group(
                        group,
                        filters,
                        reader_projection.as_ref(),
                        Some(&out_schema),
                        change_filter.as_deref(),
                        merge,
                        None,
                        tx,
                    )
                    .await
                    .map_err(|error| DataFusionError::Execution(error.to_string()))
            })
        });
        let input = builder.build();
        let input: SendableRecordBatchStream = match &self.refresh_io {
            None => input,
            Some(refresh_io) => {
                let files = self.file_counts.get(partition).copied().unwrap_or(0) as u64;
                observability::add_files_read_shared(refresh_io, files);
                let schema = input.schema();
                Box::pin(RefreshCountingStream {
                    input,
                    schema,
                    refresh_io: refresh_io.clone(),
                })
            }
        };
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        Ok(Box::pin(MetricStream { input, baseline }))
    }
}

/// Counts rows consumed from a provider scan into the refresh's IO sink.
struct RefreshCountingStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    refresh_io: Arc<Mutex<RefreshIo>>,
}

impl Stream for RefreshCountingStream {
    type Item = DataFusionResult<arrow_array::RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.input.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                observability::add_rows_scanned_shared(
                    &self.refresh_io,
                    batch.num_rows() as u64,
                );
                Poll::Ready(Some(Ok(batch)))
            }
            other => other,
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for RefreshCountingStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Records poll time and output rows for one provider scan.
struct MetricStream {
    input: SendableRecordBatchStream,
    baseline: BaselineMetrics,
}

impl Stream for MetricStream {
    type Item = DataFusionResult<arrow_array::RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let timer = this.baseline.elapsed_compute().timer();
        let poll = this.input.as_mut().poll_next(cx);
        timer.done();
        this.baseline.record_poll(poll)
    }
}

impl datafusion::physical_plan::RecordBatchStream for MetricStream {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
}
