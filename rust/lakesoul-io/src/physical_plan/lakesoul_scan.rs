// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! A metadata-only scan descriptor and the execution node that consumes it.
//!
//! Building a scan plan is metadata work: which files a work unit holds, which
//! partition values they sit under, and which schema the result has. Reading
//! file footers is not: it belongs to consumption, where the query's batch
//! size, memory pool and object stores are the ones that actually read.
//!
//! [`LakeSoulScanConfig`] describes a scan without touching storage: schema,
//! partition schema, optional pre-merge predicate and the IO configuration
//! merge-on-read needs. [`ScanWorkUnit`] names the files of one unit and the
//! partition values every row of it carries. [`LakeSoulScanExec`] turns those
//! descriptors into plans only when a consumer polls it, one work unit at a
//! time, and releases each unit before initializing the next.
//!
//! Resolution costs are paid once per unit, never once per plan: the node
//! keeps no resolved file metadata, so the same descriptor can be serialized
//! to another process, split over more tasks, or re-executed without reading
//! anything at plan time.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow_schema::{FieldRef, Schema, SchemaRef};
use async_stream::try_stream;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, PhysicalExpr, split_conjunction,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{
    DataFusionError, Result as DFResult, ScalarValue, exec_datafusion_err, internal_err,
};
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStoreExt};
use rootcause::compat::boxed_error::IntoBoxedError;

use crate::config::LakeSoulIOConfig;
use crate::file_format::{
    LakeSoulFormatRegistry, PhysicalFormat, acquire_metadata_fetch_permit,
    append_only_scan_exec, flatten_one_file_scan_config, strip_partition_columns,
};
use crate::physical_plan::merge::{MergeParquetExec, MergeWorkUnit, merge_input_scan};
use crate::pk_locator::{KeyConstraint, try_build_key_inputs};
use crate::session::LakeSoulIOSession;

/// The smallest byte range a parquet file is split into when a scan has to be
/// spread over more tasks than it has files.
///
/// A parquet row group is read when its first page falls inside the range, so
/// splitting a file into contiguous ranges partitions its row groups without
/// duplicates or omissions, and each task only pays the footer of its own
/// ranges. Splitting far below a realistic row group size would cost more
/// footer reads than the parallelism buys.
pub const MIN_PARQUET_RANGE_BYTES: u64 = 4 * 1024 * 1024;

/// The most pieces a single file contributes when a scan is split, so a
/// metadata-only split stays cheap for very large files.
const MAX_PIECES_PER_FILE: usize = 1024;

/// One file of a scan work unit, named by its object store URL.
///
/// `size` is `None` when the planner did not know it: execution then resolves
/// the real size before the file is read, and never invents zero.
#[derive(Debug, Clone)]
pub struct ScanFile {
    /// The file's full URL, e.g. `s3://bucket/table/part-0.parquet`.
    pub path: String,
    /// The physical format the file is stored in.
    pub physical_format: PhysicalFormat,
    /// The file's length in bytes, when the planner knew it.
    pub size: Option<u64>,
    /// The byte range of the file this unit reads, for parquet files that a
    /// split cut apart. `None` reads the whole file.
    pub range: Option<Range<i64>>,
}

/// One unit of work: the files that share a merge lane and the partition values
/// every row they contain carries.
#[derive(Debug, Clone)]
pub struct ScanWorkUnit {
    /// The unit's files, in merge version order.
    pub files: Vec<ScanFile>,
    /// One value per table partition column, as written in the table path.
    pub default_column_value: HashMap<String, String>,
    /// The output partition this unit executes on.
    pub lane: usize,
}

/// Everything a metadata-only scan needs to become a plan, without file I/O.
#[derive(Debug, Clone)]
pub struct LakeSoulScanConfig {
    /// The table's logical schema, including partition columns.
    pub table_schema: SchemaRef,
    /// The table's partition columns.
    pub partition_schema: SchemaRef,
    /// The scan's output schema: the scan projection plus the columns merge
    /// needs. This is exactly the schema the node emits, on every unit.
    pub output_schema: SchemaRef,
    /// The pre-merge predicate, bound to `table_schema` and already adapted to
    /// the file schemas it will be pushed into.
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// The IO configuration merge-on-read and key lookup consume.
    pub io_config: LakeSoulIOConfig,
    /// The primary-key constraint the scan fetches candidate rows for.
    pub key_constraint: Option<KeyConstraint>,
    /// The parquet read options resolved by the planner session, restricted to
    /// `datafusion.execution.parquet.*`.
    pub read_options: HashMap<String, String>,
    /// Whether parquet columns are forced to view types.
    pub parquet_force_view_types: bool,
    /// The row cap a leaf may stop reading at, forwarded from the consumer's
    /// `LIMIT`.
    ///
    /// Only set when the scan itself drops no rows: a pre-merge predicate, the
    /// CDC tombstone filter or a pk-locator candidate set all decide which
    /// rows survive, and a leaf that stopped early could not be topped up
    /// above the scan. Merge-on-read never sets it either — a merge has to see
    /// every version of a key.
    pub limit: Option<usize>,
    /// The row count one work unit must hold before the key lookup builds
    /// local indexes for it.
    ///
    /// [`crate::pk_locator::MIN_INDEX_ROWS`] is a decision about the whole
    /// query, but the lookup runs per work unit and only sees that unit's
    /// files. The planner divides the constant by the number of units the
    /// query has (see [`crate::pk_locator::min_index_rows_per_unit`]), so a
    /// bucketed table whose buckets are each smaller than the constant still
    /// takes the indexed path when the query's files together cross it.
    pub min_index_rows: u64,
}

/// Reads the scan's work units until its input is exhausted.
///
/// The node holds descriptors only: `execute` returns a stream that resolves
/// the first unit's file metadata on its first poll, reads it, drops it, and
/// only then resolves the next unit of that lane.
#[derive(Debug, Clone)]
pub struct LakeSoulScanExec {
    config: Arc<LakeSoulScanConfig>,
    units: Arc<Vec<ScanWorkUnit>>,
    /// The unit indexes of each output partition, in execution order.
    lanes: Arc<Vec<Vec<usize>>>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl LakeSoulScanExec {
    /// Describes a scan, rejecting descriptors that cannot execute.
    ///
    /// Validation is metadata only: it never touches an object store. It
    /// catches the mistakes that would otherwise surface as a batch with an
    /// unexpected schema deep inside a query.
    pub fn try_new(
        config: LakeSoulScanConfig,
        units: Vec<ScanWorkUnit>,
        partition_count: usize,
    ) -> DFResult<Self> {
        if partition_count == 0 {
            return internal_err!("LakeSoul scan needs at least one output partition");
        }
        validate_config(&config)?;
        validate_units(&config, &units, partition_count)?;
        let output_schema = freeze_output_schema(&config);
        let mut lanes = vec![Vec::new(); partition_count];
        for (index, unit) in units.iter().enumerate() {
            lanes[unit.lane].push(index);
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&output_schema)),
            Partitioning::UnknownPartitioning(partition_count),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            config: Arc::new(config),
            units: Arc::new(units),
            lanes: Arc::new(lanes),
            output_schema,
            properties,
        })
    }

    /// The scan this node executes.
    pub fn scan_config(&self) -> &LakeSoulScanConfig {
        &self.config
    }

    /// The scan's work units.
    pub fn work_units(&self) -> &[ScanWorkUnit] {
        &self.units
    }

    /// Whether the scan merges versions on read, because the table has primary
    /// keys. Merge work cannot be split: every version of a key must reach one
    /// merge.
    pub fn is_merge_on_read(&self) -> bool {
        !self.config.io_config.primary_keys_slice().is_empty()
    }

    /// How many disjoint append-only pieces this scan has.
    ///
    /// `None` when the scan merges on read and therefore cannot be split at
    /// all. Otherwise the number of files plus the parquet ranges their sizes
    /// allow, with one file's share — however many descriptors it already has,
    /// a repartitioned scan may hold several ranges of it — capped at
    /// [`MAX_PIECES_PER_FILE`] like the splitter does.
    pub fn splittable_task_limit(&self) -> Option<usize> {
        if self.is_merge_on_read() {
            return None;
        }
        let mut per_file: HashMap<&str, usize> = HashMap::new();
        for file in self.units.iter().flat_map(|unit| unit.files.iter()) {
            let capacity = per_file.entry(file.path.as_str()).or_insert(0);
            *capacity = capacity
                .saturating_add(file_piece_capacity(file))
                .min(MAX_PIECES_PER_FILE);
        }
        Some(per_file.values().sum::<usize>().max(1))
    }

    /// Splits the scan into exactly `task_count` tasks of disjoint work.
    ///
    /// Every file (and every legal parquet byte range) is covered exactly
    /// once, work keeps its order, and every task emits the same schema and
    /// reports the same partition count as this node. Tasks beyond the scan's
    /// legal pieces read nothing.
    ///
    /// A merge-on-read scan is not splittable and comes back as itself, a
    /// single task: splitting it would split one key's versions across merges.
    pub fn split_into_tasks(
        &self,
        task_count: usize,
    ) -> DFResult<Vec<Arc<LakeSoulScanExec>>> {
        if task_count == 0 {
            return internal_err!("LakeSoul scan cannot be split into no tasks");
        }
        if self.is_merge_on_read() {
            return Ok(vec![Arc::new(self.clone())]);
        }
        let mut variants = Vec::with_capacity(task_count);
        for bucket in self.append_only_buckets(task_count)? {
            variants.push(self.with_units(bucket)?);
        }
        Ok(variants)
    }

    /// A variant of this scan executing exactly `units` in their own lanes.
    ///
    /// A task executes every partition of its variant, so the units are spread
    /// round-robin over the node's lanes exactly as the builtin file-scan split
    /// fills every partition of every variant. Leaving them all on lane 0
    /// would read the task's files one after another while the other lanes
    /// idled: the row count would still be right, but a stage split over N
    /// tasks would use one lane per task instead of N.
    fn with_units(&self, units: Vec<ScanWorkUnit>) -> DFResult<Arc<Self>> {
        let lanes = self.lane_count();
        let units = units
            .into_iter()
            .enumerate()
            .map(|(index, mut unit)| {
                unit.lane = index % lanes;
                unit
            })
            .collect();
        Ok(Arc::new(Self::try_new(
            (*self.config).clone(),
            units,
            lanes,
        )?))
    }

    /// The number of output partitions this node reports.
    fn lane_count(&self) -> usize {
        self.properties.partitioning.partition_count()
    }

    /// Distributes the scan's append-only work over exactly `buckets` lanes'
    /// worth of units.
    ///
    /// Each bucket holds units that execute in order; together the buckets
    /// cover every file, and every legal byte range of it, exactly once.
    fn append_only_buckets(&self, buckets: usize) -> DFResult<Vec<Vec<ScanWorkUnit>>> {
        let active: Vec<&ScanWorkUnit> = self
            .units
            .iter()
            .filter(|unit| !unit.files.is_empty())
            .collect();
        if active.is_empty() {
            return Ok(vec![Vec::new(); buckets]);
        }
        let mut out: Vec<Vec<ScanWorkUnit>> = vec![Vec::new(); buckets];
        if active.len() >= buckets {
            // More units than lanes: hand whole units to the lanes in turn.
            for (index, unit) in active.iter().enumerate() {
                out[index % buckets].push(ScanWorkUnit {
                    files: unit.files.clone(),
                    default_column_value: unit.default_column_value.clone(),
                    lane: 0,
                });
            }
            return Ok(out);
        }
        // Fewer units than lanes: cut the units into more pieces, at legal
        // parquet byte ranges, until the lanes can be filled.
        let mut pieces: Vec<Vec<ScanFile>> =
            active.iter().map(|unit| unit.files.clone()).collect();
        let mut total: usize = pieces.iter().map(Vec::len).sum();
        // A file already spread over several descriptors keeps counting them:
        // the cap is about how many footer reads one file costs the scan, not
        // how many this call adds.
        let mut pieces_per_file: HashMap<String, usize> = HashMap::new();
        for piece in pieces.iter().flatten() {
            *pieces_per_file.entry(piece.path.clone()).or_insert(0) += 1;
        }
        while total < buckets {
            let Some((unit, index)) = largest_splittable_piece(&pieces, &pieces_per_file)
            else {
                break;
            };
            let path = pieces[unit][index].path.clone();
            let (left, right) = split_parquet_piece(&pieces[unit][index])
                .expect("a splittable piece splits");
            pieces[unit][index] = left;
            pieces[unit].insert(index + 1, right);
            *pieces_per_file.entry(path).or_insert(0) += 1;
            total += 1;
        }
        let mut groups: Vec<usize> = vec![1; pieces.len()];
        let mut assigned = pieces.len();
        while assigned < buckets {
            let candidate = (0..pieces.len())
                .filter(|unit| groups[*unit] < pieces[*unit].len())
                .max_by_key(|unit| piece_weight(&pieces[*unit]) / groups[*unit] as u64);
            let Some(unit) = candidate else {
                break;
            };
            groups[unit] += 1;
            assigned += 1;
        }
        let mut bucket = 0;
        for (index, (unit, unit_pieces)) in active.iter().zip(pieces.iter()).enumerate() {
            for files in chunk_contiguous(unit_pieces, groups[index]) {
                out[bucket].push(ScanWorkUnit {
                    files,
                    default_column_value: unit.default_column_value.clone(),
                    lane: 0,
                });
                bucket += 1;
            }
        }
        Ok(out)
    }
}

impl DisplayAs for LakeSoulScanExec {
    fn fmt_as(
        &self,
        format: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match format {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LakeSoulScanExec: lanes={}, units={}, files={}, merge_on_read={}",
                    self.lane_count(),
                    self.units.len(),
                    self.units
                        .iter()
                        .map(|unit| unit.files.len())
                        .sum::<usize>(),
                    self.is_merge_on_read(),
                )?;
                // Surfacing the cap keeps `EXPLAIN` honest about whether the
                // consumer's `LIMIT` reached the leaves.
                match self.config.limit {
                    Some(limit) => write!(f, ", limit={limit}"),
                    None => Ok(()),
                }
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "format=lakesoul")?;
                writeln!(f, "lanes={}", self.lane_count())?;
                writeln!(f, "units={}", self.units.len())?;
                if let Some(limit) = self.config.limit {
                    writeln!(f, "limit={limit}")?;
                }
                write!(f, "merge_on_read={}", self.is_merge_on_read())
            }
        }
    }
}

impl ExecutionPlanProperties for LakeSoulScanExec {
    fn output_partitioning(&self) -> &Partitioning {
        &self.properties.partitioning
    }

    fn output_ordering(&self) -> Option<&LexOrdering> {
        None
    }

    fn boundedness(&self) -> Boundedness {
        self.properties.boundedness
    }

    fn pipeline_behavior(&self) -> EmissionType {
        self.properties.emission_type
    }

    fn equivalence_properties(&self) -> &EquivalenceProperties {
        &self.properties.eq_properties
    }
}

impl ExecutionPlan for LakeSoulScanExec {
    fn name(&self) -> &str {
        "LakeSoulScanExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        // The descriptor's pre-merge predicate is the node's only expression;
        // the node has no children to recurse into.
        match &self.config.predicate {
            Some(predicate) => f(predicate),
            None => Ok(TreeNodeRecursion::Continue),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("LakeSoulScanExec has no children");
        }
        Ok(self)
    }

    /// Spreads an append-only scan over more lanes when the optimizer asks for
    /// more parallelism than the planner chose.
    ///
    /// A merge-on-read scan stays as it is: its lanes follow the table's
    /// partitioning, and no key's versions may be separated.
    fn repartitioned(
        &self,
        target_partitions: usize,
        _config: &ConfigOptions,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if self.is_merge_on_read() || target_partitions <= self.lane_count() {
            return Ok(None);
        }
        let Some(limit) = self.splittable_task_limit() else {
            return Ok(None);
        };
        let lanes = limit.min(target_partitions);
        if lanes <= self.lane_count() {
            return Ok(None);
        }
        let mut units = Vec::new();
        for (lane, bucket) in self.append_only_buckets(lanes)?.into_iter().enumerate() {
            for mut unit in bucket {
                unit.lane = lane;
                units.push(unit);
            }
        }
        Ok(Some(Arc::new(Self::try_new(
            (*self.config).clone(),
            units,
            lanes,
        )?)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let partition_count = self.lane_count();
        if partition >= partition_count {
            return internal_err!(
                "LakeSoul scan lane {partition} does not exist: the scan has {partition_count} lanes"
            );
        }
        let config = Arc::clone(&self.config);
        let units = Arc::clone(&self.units);
        let lanes = Arc::clone(&self.lanes);
        let output_schema = Arc::clone(&self.output_schema);
        let stream = try_stream! {
            // Nothing above this line reads storage, and neither does the
            // code below: it runs on the first poll, inside the consumer's
            // task, so it uses the consumer's runtime, memory pool and
            // object stores.
            let context = restored_task_context(&context, &config.read_options)?;
            let session = LakeSoulIOSession::from_plain_config_and_context(
                config.io_config.clone(),
                Arc::clone(&context),
            );
            let registry = Arc::new(
                LakeSoulFormatRegistry::new(
                    config.io_config.clone(),
                    config.parquet_force_view_types,
                )
                .map_err(|error| {
                    DataFusionError::External(error.into_boxed_error())
                })?,
            );
            for unit_index in lanes[partition].iter().copied() {
                let inner = initialize_unit(
                    &config,
                    &output_schema,
                    &registry,
                    &session,
                    &units[unit_index],
                )
                .await?;
                if inner.schema() != output_schema {
                    internal_err!(
                        "LakeSoul scan unit {unit_index} initialized a plan with schema {}, expected the scan's frozen output schema {}",
                        inner.schema(),
                        output_schema
                    )?;
                }
                let mut stream = inner.execute(0, Arc::clone(&context))?;
                while let Some(batch) = stream.next().await {
                    yield batch?;
                }
                // Release this unit's plan, readers, batches and memory
                // reservation before the next unit of this lane is
                // initialized.
                drop(stream);
                drop(inner);
            }
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }
}

/// The session option prefix the scan carries from planning to execution.
///
/// Only reader semantics travel: how parquet files are decoded and which
/// filters they apply. Worker resource policy (batch size, target partitions,
/// memory) and credentials stay with the consumer task.
pub const PARQUET_READ_OPTION_PREFIX: &str = "datafusion.execution.parquet.";

/// Captures the planner session's parquet reader options.
///
/// These are the resolved values the planning session would read files with;
/// copying them into [`LakeSoulScanConfig::read_options`] lets a descriptor
/// execute the same way in another process.
pub fn capture_read_options(options: &ConfigOptions) -> HashMap<String, String> {
    options
        .entries()
        .into_iter()
        .filter(|entry| entry.key.starts_with(PARQUET_READ_OPTION_PREFIX))
        .filter_map(|entry| entry.value.map(|value| (entry.key, value)))
        .collect()
}

/// Applies captured reader options to a task's session configuration.
///
/// The task's own configuration stays the base, so the consumer keeps its
/// batch size, target partitions and memory policy; only the captured parquet
/// reader semantics are overlaid. An unknown key is an error: silently
/// ignoring a reader option would read files differently than planned.
pub fn restore_read_options(
    session_config: datafusion::prelude::SessionConfig,
    read_options: &HashMap<String, String>,
) -> DFResult<datafusion::prelude::SessionConfig> {
    let mut session_config = session_config;
    if read_options.is_empty() {
        return Ok(session_config);
    }
    {
        let options = session_config.options_mut();
        for (key, value) in read_options {
            if !key.starts_with(PARQUET_READ_OPTION_PREFIX) {
                return Err(exec_datafusion_err!(
                    "scan read option '{key}' is not a parquet reader option, so it cannot be restored at execution"
                ));
            }
            options.set(key, value)?;
        }
    }
    Ok(session_config)
}

/// Restores the planner's parquet read options onto this task's session
/// configuration.
///
/// The consumer task owns the batch size, memory pool and registries, so its
/// session configuration is the base; only the reader semantics that decided
/// how the plan's files are read are carried over.
fn restored_task_context(
    context: &Arc<TaskContext>,
    read_options: &HashMap<String, String>,
) -> DFResult<Arc<TaskContext>> {
    let session_config =
        restore_read_options(context.session_config().clone(), read_options)?;
    Ok(Arc::new(TaskContext::new(
        context.task_id(),
        context.session_id(),
        session_config,
        context.scalar_functions().clone(),
        context.higher_order_functions().clone(),
        context.aggregate_functions().clone(),
        context.window_functions().clone(),
        context.runtime_env(),
    )))
}

/// Builds the plan of one work unit, on first use.
///
/// Every file metadata read this function performs draws on the process
/// metadata budget the eager planner also uses. The files of the unit are
/// resolved concurrently, bounded like the eager planner's own metadata
/// fetches, and their configurations are restored to the unit's file order
/// afterwards: completion order must never become merge version order.
async fn initialize_unit(
    config: &LakeSoulScanConfig,
    output_schema: &SchemaRef,
    registry: &Arc<LakeSoulFormatRegistry>,
    session: &LakeSoulIOSession,
    unit: &ScanWorkUnit,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    if unit.files.is_empty() {
        return Ok(Arc::new(EmptyExec::new(Arc::clone(output_schema))));
    }
    let partition_values = unit_partition_values(config, unit)?;
    let primary_keys = config.io_config.primary_keys_slice();
    let cdc_column = config.io_config.cdc_column();
    // Locations are pure metadata. One template per format and store: adapting
    // a source and pushing the predicate down is unit-independent.
    let mut locations = Vec::with_capacity(unit.files.len());
    let mut templates: HashMap<(PhysicalFormat, ObjectStoreUrl), FileScanConfig> =
        HashMap::new();
    for file in &unit.files {
        let (object_store_url, location) = scan_file_location(file)?;
        if let std::collections::hash_map::Entry::Vacant(slot) =
            templates.entry((file.physical_format, object_store_url.clone()))
        {
            slot.insert(unit_scan_template(
                config,
                registry,
                file.physical_format,
                object_store_url.clone(),
                session,
            )?);
        }
        locations.push((object_store_url, location));
    }
    let meta_fetch_concurrency: usize = session
        .config_options()
        .execution
        .meta_fetch_concurrency
        .into();
    let mut per_file_configs =
        futures::stream::iter(unit.files.iter().zip(locations).enumerate())
            .map(|(index, (file, (object_store_url, location)))| {
                let partition_values = &partition_values;
                let templates = &templates;
                let cdc_column = &cdc_column;
                async move {
                    let object_meta = match file.size {
                        Some(size) => ObjectMeta {
                            location,
                            // Unknown to the planner; no request learns it.
                            last_modified: chrono::DateTime::<chrono::Utc>::from(
                                std::time::SystemTime::UNIX_EPOCH,
                            ),
                            size,
                            e_tag: None,
                            version: None,
                        },
                        None => {
                            let store =
                                session.runtime_env().object_store(&object_store_url)?;
                            let _permit = acquire_metadata_fetch_permit().await?;
                            store.head(&location).await.map_err(|error| {
                                exec_datafusion_err!(
                                    "cannot resolve the size of scan file '{}': {error}",
                                    file.path
                                )
                            })?
                        }
                    };
                    let mut partitioned_file =
                        PartitionedFile::new_from_meta(object_meta)
                            .with_partition_values(partition_values.clone());
                    if let Some(range) = &file.range {
                        partitioned_file =
                            partitioned_file.with_range(range.start, range.end);
                    }
                    let key = (file.physical_format, object_store_url);
                    let template = templates.get(&key).ok_or_else(|| {
                        exec_datafusion_err!(
                            "scan file '{}' has no initializer template",
                            file.path
                        )
                    })?;
                    let flattened = flatten_one_file_scan_config(
                        session,
                        registry.file_format(file.physical_format),
                        template,
                        partitioned_file,
                        primary_keys,
                        cdc_column,
                        Arc::clone(&config.partition_schema),
                        Arc::clone(output_schema),
                        // Reader initialization runs one unit at a time: a
                        // per-unit schema summary would only repeat itself.
                        None,
                    )
                    .await?;
                    Ok::<(usize, FileScanConfig), DataFusionError>((
                        index,
                        flattened.config,
                    ))
                }
            })
            .boxed() // Workaround https://github.com/rust-lang/rust/issues/64552
            .buffer_unordered(meta_fetch_concurrency)
            .try_collect::<Vec<_>>()
            .await?;
    // Initialization ran concurrently; the unit's order did not change.
    per_file_configs.sort_unstable_by_key(|(index, _)| *index);
    let per_file_configs: Vec<FileScanConfig> = per_file_configs
        .into_iter()
        .map(|(_, file_config)| file_config)
        .collect();
    let file_paths: Vec<String> = per_file_configs
        .iter()
        .filter_map(|file_config| {
            let file = file_config.file_groups.first()?.files().first()?;
            Some(file.path().to_string())
        })
        .collect();
    let mut unit_io_config = config.io_config.clone();
    unit_io_config.set_files(file_paths.clone());
    if primary_keys.is_empty() {
        let format = unit.files[0].physical_format;
        if unit.files.iter().all(|file| file.physical_format == format) {
            let format = registry.file_format(format);
            if let Some(leaf) = append_only_scan_exec(
                session,
                format.as_ref(),
                &per_file_configs,
                output_schema,
            )
            .map_err(|error| DataFusionError::External(error.into_boxed_error()))?
            {
                return Ok(leaf);
            }
        }
    }
    let inputs = match config.key_constraint.as_ref() {
        Some(constraint) => match try_build_key_inputs(
            session,
            &unit_io_config,
            &per_file_configs,
            constraint,
            config.min_index_rows,
        )
        .await?
        {
            Some(inputs) => inputs,
            None => merge_inputs(&per_file_configs, primary_keys),
        },
        None => merge_inputs(&per_file_configs, primary_keys),
    };
    let work_unit = MergeWorkUnit {
        input_range: 0..inputs.len(),
        files: file_paths,
        default_column_value: Arc::new(unit.default_column_value.clone()),
        lane: 0,
    };
    Ok(Arc::new(MergeParquetExec::from_parts(
        Arc::clone(output_schema),
        Arc::new(primary_keys.to_vec()),
        Arc::new(config.io_config.merge_operators.clone()),
        inputs,
        unit_io_config,
        vec![work_unit],
        1,
    )?))
}

/// One merge input per file, in the unit's version order.
fn merge_inputs(
    configs: &[FileScanConfig],
    primary_keys: &[String],
) -> Vec<Arc<dyn ExecutionPlan>> {
    configs
        .iter()
        .cloned()
        .map(|config| merge_input_scan(config, primary_keys))
        .collect()
}

/// The file-free scan template of one physical format for one object store.
fn unit_scan_template(
    config: &LakeSoulScanConfig,
    registry: &LakeSoulFormatRegistry,
    physical_format: PhysicalFormat,
    object_store_url: ObjectStoreUrl,
    session: &dyn Session,
) -> DFResult<FileScanConfig> {
    let format: Arc<dyn FileFormat> = registry.file_format(physical_format);
    let file_schema = strip_partition_columns(
        Arc::clone(&config.table_schema),
        Arc::clone(&config.partition_schema),
    );
    let table_schema = TableSchema::new(
        file_schema,
        config
            .partition_schema
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<FieldRef>>(),
    );
    let mut source = format.file_source(table_schema);
    if let Some(predicate) = &config.predicate {
        // Push each conjunct into the source so the reader can apply it while
        // decoding; `flatten_one_file_scan_config` rebinds it per file schema.
        let conjuncts: Vec<Arc<dyn PhysicalExpr>> = split_conjunction(predicate)
            .into_iter()
            .map(Arc::clone)
            .collect();
        if !conjuncts.is_empty() {
            let pushed =
                source.try_pushdown_filters(conjuncts, session.config_options())?;
            if let Some(updated) = pushed.updated_node {
                source = updated;
            }
        }
    }
    Ok(FileScanConfigBuilder::new(object_store_url, source)
        .with_file_compression_type(registry.file_compression_type(physical_format))
        .with_limit(config.limit)
        .build())
}

/// Parses one scan file's object store and location.
///
/// Pure metadata: no object store request is made here, and an unknown size is
/// not resolved until the file's unit executes.
fn scan_file_location(
    file: &ScanFile,
) -> DFResult<(ObjectStoreUrl, object_store::path::Path)> {
    let url = url::Url::parse(&file.path).map_err(|error| {
        exec_datafusion_err!("invalid scan file url '{}': {error}", file.path)
    })?;
    let location =
        object_store::path::Path::from_url_path(url.path()).map_err(|error| {
            exec_datafusion_err!("invalid scan file path '{}': {error}", file.path)
        })?;
    let object_store_url = ListingTableUrl::parse(&file.path)
        .map_err(|error| {
            exec_datafusion_err!("invalid scan file url '{}': {error}", file.path)
        })?
        .object_store();
    Ok((object_store_url, location))
}

/// The partition values every row of `unit` carries, in partition schema
/// order.
fn unit_partition_values(
    config: &LakeSoulScanConfig,
    unit: &ScanWorkUnit,
) -> DFResult<Vec<ScalarValue>> {
    config
        .partition_schema
        .fields()
        .iter()
        .map(|field| {
            let value = unit.default_column_value.get(field.name()).ok_or_else(|| {
                exec_datafusion_err!(
                    "work unit of lane {} has no value for partition column '{}'",
                    unit.lane,
                    field.name()
                )
            })?;
            ScalarValue::try_from_string(value.clone(), field.data_type())
        })
        .collect()
}

/// Rejects a descriptor whose parts disagree with each other.
fn validate_config(config: &LakeSoulScanConfig) -> DFResult<()> {
    // A zero row threshold would build an index for every key lookup, which
    // is a decision the planner has to make explicitly rather than by
    // accident: it always hands over at least one row.
    if config.min_index_rows == 0 {
        return Err(exec_datafusion_err!(
            "scan declares a zero-row key-lookup threshold"
        ));
    }
    for field in config.output_schema.fields() {
        match config.table_schema.field_with_name(field.name()) {
            Ok(table_field) if table_field.data_type() == field.data_type() => {}
            Ok(table_field) => {
                return Err(exec_datafusion_err!(
                    "scan output column '{}' has type {}, but the table declares {}",
                    field.name(),
                    field.data_type(),
                    table_field.data_type()
                ));
            }
            Err(_) => {
                return Err(exec_datafusion_err!(
                    "scan output column '{}' is not a column of the table",
                    field.name()
                ));
            }
        }
    }
    for field in config.partition_schema.fields() {
        match config.table_schema.field_with_name(field.name()) {
            Ok(table_field) if table_field.data_type() == field.data_type() => {}
            Ok(table_field) => {
                return Err(exec_datafusion_err!(
                    "partition column '{}' has type {}, but the table declares {}",
                    field.name(),
                    field.data_type(),
                    table_field.data_type()
                ));
            }
            Err(_) => {
                return Err(exec_datafusion_err!(
                    "partition column '{}' is not a column of the table",
                    field.name()
                ));
            }
        }
    }
    // Merge groups rows by primary key, so a scan that does not carry the
    // keys cannot merge: the hidden key columns must survive the projection.
    for primary_key in config.io_config.primary_keys_slice() {
        match config.table_schema.field_with_name(primary_key) {
            Ok(table_field) => match config.output_schema.field_with_name(primary_key) {
                Ok(output_field)
                    if output_field.data_type() == table_field.data_type() => {}
                Ok(output_field) => {
                    return Err(exec_datafusion_err!(
                        "primary key '{}' has type {} in the scan output, but the table declares {}",
                        primary_key,
                        output_field.data_type(),
                        table_field.data_type()
                    ));
                }
                Err(_) => {
                    return Err(exec_datafusion_err!(
                        "scan output omits primary key '{}', so merge-on-read cannot group rows by key",
                        primary_key
                    ));
                }
            },
            Err(_) => {
                return Err(exec_datafusion_err!(
                    "primary key '{}' is not a column of the table",
                    primary_key
                ));
            }
        }
    }
    if let Some(predicate) = &config.predicate {
        // The predicate is consumed by readers of `table_schema` files, so it
        // must be bound to that schema: a stale binding would read the wrong
        // column from a reader whose schema has a different column order.
        for column in collect_columns(predicate) {
            match config.table_schema.fields().get(column.index()) {
                Some(field) if field.name() == column.name() => {}
                _ => {
                    return Err(exec_datafusion_err!(
                        "pre-merge predicate is not bound to the scan's table schema: column '{}' at index {}",
                        column.name(),
                        column.index()
                    ));
                }
            }
        }
    }
    if let Some(constraint) = config.key_constraint.as_ref() {
        let keys = config.io_config.primary_keys_slice();
        if constraint.columns.is_empty()
            || constraint.columns.len() > keys.len()
            || constraint
                .columns
                .iter()
                .zip(keys)
                .any(|(column, key)| column != key)
        {
            return Err(exec_datafusion_err!(
                "primary-key constraint on {:?} is not a prefix of the scan's primary keys {:?}",
                constraint.columns,
                keys
            ));
        }
        if constraint
            .values
            .iter()
            .any(|value| value.len() != constraint.columns.len())
        {
            return Err(exec_datafusion_err!(
                "primary-key constraint tuples must have {} values",
                constraint.columns.len()
            ));
        }
    }
    Ok(())
}

/// Rejects work units that cannot execute.
///
/// The byte ranges of all units are validated together: a plan that reads any
/// byte of a file twice would emit the same rows twice, and no execution
/// engine below this node removes duplicates. Which pieces together cover a
/// file is the splitter's decision; that they never overlap is checked here,
/// once, for every descriptor that reaches execution.
fn validate_units(
    config: &LakeSoulScanConfig,
    units: &[ScanWorkUnit],
    partition_count: usize,
) -> DFResult<()> {
    let merge_on_read = !config.io_config.primary_keys_slice().is_empty();
    let mut claimed: HashMap<&str, Vec<(i64, i64)>> = HashMap::new();
    for unit in units {
        if unit.lane >= partition_count {
            return Err(exec_datafusion_err!(
                "work unit of lane {} exceeds the scan's {partition_count} lanes",
                unit.lane
            ));
        }
        if merge_on_read && unit.files.iter().any(|file| file.range.is_some()) {
            return Err(exec_datafusion_err!(
                "merge-on-read work unit of lane {} reads a byte range of a version file; a merge needs whole files",
                unit.lane
            ));
        }
        for file in &unit.files {
            let extension_format = PhysicalFormat::from_extension(&file.path)
                .map_err(|error| DataFusionError::External(error.into_boxed_error()))?;
            if extension_format.extension() != file.physical_format.extension() {
                return Err(exec_datafusion_err!(
                    "scan file '{}' is described as {} but its extension says {}",
                    file.path,
                    file.physical_format,
                    extension_format
                ));
            }
            // Byte offsets are signed; a size that does not fit would
            // wrap when a piece of it becomes a range.
            if file.size.is_some_and(|size| size > i64::MAX as u64) {
                return Err(exec_datafusion_err!(
                    "scan file '{}' is larger than the largest addressable byte offset",
                    file.path
                ));
            }
            if let Some(range) = &file.range {
                if file.physical_format != PhysicalFormat::Parquet {
                    return Err(exec_datafusion_err!(
                        "scan file '{}' declares a byte range, which only parquet files support",
                        file.path
                    ));
                }
                if range.start < 0 || range.end <= range.start {
                    return Err(exec_datafusion_err!(
                        "scan file '{}' declares the empty byte range {}..{}",
                        file.path,
                        range.start,
                        range.end
                    ));
                }
                if let Some(size) = file.size
                    && range.end > size as i64
                {
                    return Err(exec_datafusion_err!(
                        "scan file '{}' declares a byte range ending at {}, past its size {}",
                        file.path,
                        range.end,
                        size
                    ));
                }
            }
            let span = match &file.range {
                Some(range) => (range.start, range.end),
                // A whole file claims everything up to its size; when the size
                // is unknown, it claims to the end of the address space so any
                // other piece of the same file overlaps it.
                None => (0, file.size.map_or(i64::MAX, |size| size as i64)),
            };
            let spans = claimed.entry(file.path.as_str()).or_default();
            if spans
                .iter()
                .any(|&(start, end)| span.0 < end && start < span.1)
            {
                return Err(exec_datafusion_err!(
                    "scan file '{}' is read twice: {}..{} overlaps a piece already assigned to this scan",
                    file.path,
                    span.0,
                    span.1
                ));
            }
            spans.push(span);
        }
        if !unit.files.is_empty() {
            // Also parses the values, so a malformed partition value fails
            // here instead of in the middle of a query.
            unit_partition_values(config, unit)?;
        }
    }
    Ok(())
}

/// The schema every unit and every task of the scan emits.
///
/// Nullability is frozen conservatively: a value column may be absent from an
/// individual file, and a sparse file read pads it with NULL, so its output
/// must be nullable even when the table declares it non-nullable. Primary-key
/// and partition columns are known for every row the scan emits and keep their
/// declared nullability.
fn freeze_output_schema(config: &LakeSoulScanConfig) -> SchemaRef {
    let primary_keys = config.io_config.primary_keys_slice();
    let mut changed = false;
    let fields: Vec<FieldRef> = config
        .output_schema
        .fields()
        .iter()
        .map(|field| {
            let constant = primary_keys.iter().any(|key| key == field.name())
                || config
                    .partition_schema
                    .field_with_name(field.name())
                    .is_ok();
            if field.is_nullable() || constant {
                Arc::clone(field)
            } else {
                changed = true;
                Arc::new(field.as_ref().clone().with_nullable(true))
            }
        })
        .collect();
    if !changed {
        return Arc::clone(&config.output_schema);
    }
    Arc::new(Schema::new_with_metadata(
        fields,
        config.output_schema.metadata().clone(),
    ))
}

/// The byte range a piece reads, or `None` when its size is unknown.
fn piece_range(piece: &ScanFile) -> Option<(i64, i64)> {
    match &piece.range {
        Some(range) => Some((range.start, range.end)),
        None => piece.size.map(|size| (0, size as i64)),
    }
}

/// The bytes a piece reads, or 0 when they are unknown.
fn piece_bytes(piece: &ScanFile) -> u64 {
    piece_range(piece).map_or(0, |(start, end)| (end - start).max(0) as u64)
}

/// The bytes a single unit's pieces read.
fn piece_weight(pieces: &[ScanFile]) -> u64 {
    pieces.iter().map(piece_bytes).sum()
}

/// The most pieces one file can contribute to a split.
///
/// A piece is split only while it holds at least two minimum ranges, and a
/// split halves it, so a `length`-byte file ends up with `2^k` leaves of at
/// most `2M - 1` bytes, where `2^k` is the largest power of two that fits in
/// `length / M` — plus one more piece for every spare byte that lets a leaf
/// reach exactly `2M` and split once more. `length / M` alone would overstate
/// it: a `12M` file halves into `6M`, then `3M`, then `1.5M` leaves, which is
/// eight pieces, not twelve, and the four extra tasks the planner would ask
/// for could never read anything.
///
/// A `4M - 1` file is the other side of the same rule: its halves are `2M - 1`
/// and `2M`, and only the second one splits again, so it yields three pieces
/// although `(4M - 1) / M` rounds to three only by accident.
fn file_piece_capacity(file: &ScanFile) -> usize {
    if file.physical_format != PhysicalFormat::Parquet {
        // A vortex file is read as a whole: ranges only apply to parquet.
        return 1;
    }
    let bytes = piece_bytes(file);
    let ranges = bytes / MIN_PARQUET_RANGE_BYTES;
    if ranges == 0 {
        return 1;
    }
    // Largest power of two that fits in the available ranges.
    let leaves = 1u64 << (63 - ranges.leading_zeros());
    // Leaves are at least `2M - 1` bytes here; each byte above that lets one
    // of them split into two.
    let spare =
        bytes.saturating_sub(leaves.saturating_mul(2 * MIN_PARQUET_RANGE_BYTES - 1));
    usize::try_from(leaves.saturating_add(spare))
        .unwrap_or(MAX_PIECES_PER_FILE)
        .clamp(1, MAX_PIECES_PER_FILE)
}

/// The length of a piece that may still be split, without building anything.
fn splittable_length(piece: &ScanFile) -> Option<i64> {
    if piece.physical_format != PhysicalFormat::Parquet {
        return None;
    }
    let (start, end) = piece_range(piece)?;
    let length = end - start;
    if length < 2 * MIN_PARQUET_RANGE_BYTES as i64 {
        return None;
    }
    Some(length)
}

/// Cuts a parquet piece into two legal halves, if it is big enough to be worth
/// splitting.
fn split_parquet_piece(piece: &ScanFile) -> Option<(ScanFile, ScanFile)> {
    let length = splittable_length(piece)?;
    let (start, end) = piece_range(piece)?;
    let middle = start + length / 2;
    let mut left = piece.clone();
    let mut right = piece.clone();
    left.range = Some(start..middle);
    right.range = Some(middle..end);
    Some((left, right))
}

/// The largest piece that can still be split, as (unit index, piece index).
///
/// Eligibility is decided without cloning: this runs once per split step over
/// every candidate of every unit. A file that already contributes
/// [`MAX_PIECES_PER_FILE`] pieces is skipped, so one huge file cannot absorb
/// the whole budget while other files wait to be split.
fn largest_splittable_piece(
    pieces: &[Vec<ScanFile>],
    pieces_per_file: &HashMap<String, usize>,
) -> Option<(usize, usize)> {
    let mut best: Option<(usize, usize, u64)> = None;
    for (unit, unit_pieces) in pieces.iter().enumerate() {
        for (index, piece) in unit_pieces.iter().enumerate() {
            if splittable_length(piece).is_none() {
                continue;
            }
            if pieces_per_file.get(&piece.path).copied().unwrap_or(0)
                >= MAX_PIECES_PER_FILE
            {
                continue;
            }
            let bytes = piece_bytes(piece);
            if best.is_none_or(|(_, _, best_bytes)| bytes > best_bytes) {
                best = Some((unit, index, bytes));
            }
        }
    }
    best.map(|(unit, index, _)| (unit, index))
}

/// Cuts ordered pieces into `groups` contiguous runs of comparable weight.
///
/// Contiguity keeps a unit's pieces in their planned order; the byte share
/// keeps the runs comparable, so one split task does not read the whole file
/// while another reads a few bytes.
fn chunk_contiguous(pieces: &[ScanFile], groups: usize) -> Vec<Vec<ScanFile>> {
    if pieces.is_empty() {
        return Vec::new();
    }
    let groups = groups.clamp(1, pieces.len());
    if groups == 1 {
        return vec![pieces.to_vec()];
    }
    let total = piece_weight(pieces);
    let target = total.div_ceil(groups as u64).max(1);
    let mut out: Vec<Vec<ScanFile>> = Vec::with_capacity(groups);
    let mut current: Vec<ScanFile> = Vec::new();
    let mut current_bytes = 0;
    for (index, piece) in pieces.iter().enumerate() {
        current.push(piece.clone());
        current_bytes += piece_bytes(piece);
        let groups_left = groups - out.len();
        let pieces_left = pieces.len() - index;
        // Close a run at its byte share, and always leave one piece for each
        // run still to be filled.
        if groups_left > 1 && (current_bytes >= target || pieces_left == groups_left) {
            out.push(std::mem::take(&mut current));
            current_bytes = 0;
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}
