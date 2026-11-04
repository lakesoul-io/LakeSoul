// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! The LakeSoul write path, split into a write node and a commit node.
//!
//! [`LakeSoulWriteExec`] writes the input's record batches to object storage
//! and returns one *report* per written file. [`LakeSoulCommitExec`] drains
//! those reports and publishes the statement with exactly one metadata
//! commit, then runs the post-commit index maintenance.
//!
//! The split is what makes a distributed write possible: the write node only
//! needs the table metadata that is serialized with the plan, so it can run on
//! a worker, while the commit node keeps the metadata client, the snapshot
//! taken at plan time, and the object-store credentials on the coordinator.
//! In a single-node plan the two nodes are adjacent and behave exactly like
//! the historical single sink.
//!
//! # Report format
//!
//! A report row is `(partition_desc, bucket, path, row_count, size,
//! file_exist_cols)`. It is an Arrow [`RecordBatch`] rather than a bespoke RPC
//! message so the distributed planner can carry it across a network boundary
//! with the machinery it already has. `bucket` is the input partition index the
//! file was written for; for primary-key tables that is the hash bucket
//! LakeSoul's file names encode. `size` and `file_exist_cols` are what the
//! writer's own object metadata reported when it closed the file, so the
//! metadata commit records them without asking the store again.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Once};

use arrow::array::{Array, ArrayRef, Int64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DFSchema, Result as DFResult};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{
    EquivalenceProperties, LexOrdering, LexRequirement, OrderingRequirements,
    PhysicalExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datafusion_common::TableReference;
use futures::StreamExt;
use lakesoul_common::misc::SecretMap;
use lakesoul_common::ser::arrow_java::schema_from_table_info_metadata;
use lakesoul_io::execution_trace::instrument_record_batch_stream;
use lakesoul_io::helpers::{
    columnar_values_to_partition_desc, columnar_values_to_sub_path, get_columnar_values,
};
use lakesoul_io::session::LakeSoulIOSession;
use lakesoul_io::writer::async_writer::{AsyncBatchWriter, WriteInvariants};
use lakesoul_io::writer::create_writer;
use lakesoul_metadata::transfusion::DataFileInfo;
use lakesoul_metadata::{LakeSoulMetaDataError, MetaDataClientRef};
use lakesoul_metadata_proto::entity::{
    CommitOp, DataCommitInfo, DataFileOp, FileOp, PartitionInfo, TableInfo, Uuid,
};
use rand::distr::SampleString;
use rootcause::compat::boxed_error::IntoBoxedError;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::Instrument;
use tracing::instrument::WithSubscriber;

use super::insert_only::{InsertOnly, remove_files};
use crate::Result;
use crate::catalog::parse_table_info_partitions;
use crate::lakesoul_table::helpers::create_io_config_builder_from_table_info;

/// One file of a statement, as the commit node records it in `file_ops`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CommittedFile {
    /// Absolute path of the written file.
    path: String,
    /// Bytes written, from the writer's own object metadata.
    size: i64,
    /// Stored columns, comma-separated: `DataFileOp::file_exist_cols`.
    file_exist_cols: String,
}

type PartitionedFile = HashMap<String, (Vec<CommittedFile>, u64)>;

/// One file written by a [`LakeSoulWriteExec`] task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct WriteFileReport {
    /// The range-partition description the file belongs to.
    pub partition_desc: String,
    /// Input partition index the file was written for. For primary-key tables
    /// this is the hash bucket encoded in the file name.
    pub bucket: u32,
    /// Absolute path of the written file.
    pub path: String,
    /// Rows in this file (after `ON CONFLICT DO NOTHING` filtering).
    pub row_count: u64,
    /// Bytes the writer reported for the file. Committed as the file op's
    /// `size`, which is what lets a reader budget and range-split the scan
    /// without touching the object store.
    pub size: i64,
    /// Columns the file stores, comma-separated as `file_ops` expects.
    pub file_exist_cols: String,
}

const REPORT_PARTITION_DESC: &str = "partition_desc";
const REPORT_BUCKET: &str = "bucket";
const REPORT_PATH: &str = "path";
const REPORT_ROW_COUNT: &str = "row_count";
const REPORT_SIZE: &str = "size";
const REPORT_FILE_EXIST_COLS: &str = "file_exist_cols";
const REPORT_ERROR_KIND: &str = "error_kind";
const REPORT_ERROR_DETAIL: &str = "error_detail";

/// Schema of the write report stream that crosses the write→commit edge.
pub(super) fn report_schema() -> SchemaRef {
    static SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
        Arc::new(Schema::new(vec![
            Field::new(REPORT_PARTITION_DESC, DataType::Utf8, false),
            Field::new(REPORT_BUCKET, DataType::UInt32, false),
            Field::new(REPORT_PATH, DataType::Utf8, false),
            Field::new(REPORT_ROW_COUNT, DataType::UInt64, false),
            Field::new(REPORT_SIZE, DataType::Int64, false),
            Field::new(REPORT_FILE_EXIST_COLS, DataType::Utf8, false),
            Field::new(REPORT_ERROR_KIND, DataType::Utf8, true),
            Field::new(REPORT_ERROR_DETAIL, DataType::Utf8, true),
        ]))
    });
    Arc::clone(&SCHEMA)
}

/// Builds the single batch a write task returns.
pub(super) fn report_batch(reports: &[WriteFileReport]) -> DFResult<RecordBatch> {
    let partitions: Vec<&str> = reports
        .iter()
        .map(|report| report.partition_desc.as_str())
        .collect();
    let buckets: Vec<u32> = reports.iter().map(|report| report.bucket).collect();
    let paths: Vec<&str> = reports.iter().map(|report| report.path.as_str()).collect();
    let counts: Vec<u64> = reports.iter().map(|report| report.row_count).collect();
    let sizes: Vec<i64> = reports.iter().map(|report| report.size).collect();
    let exist_cols: Vec<&str> = reports
        .iter()
        .map(|report| report.file_exist_cols.as_str())
        .collect();
    Ok(RecordBatch::try_new(
        report_schema(),
        vec![
            Arc::new(StringArray::from(partitions)) as ArrayRef,
            Arc::new(UInt32Array::from(buckets)) as ArrayRef,
            Arc::new(StringArray::from(paths)) as ArrayRef,
            Arc::new(UInt64Array::from(counts)) as ArrayRef,
            Arc::new(Int64Array::from(sizes)) as ArrayRef,
            Arc::new(StringArray::from(exist_cols)) as ArrayRef,
            Arc::new(StringArray::new_null(reports.len())) as ArrayRef,
            Arc::new(StringArray::new_null(reports.len())) as ArrayRef,
        ],
    )?)
}

/// Flight errors lose Rust error types. Constraint failures travel as report
/// rows instead, so the coordinator can retain the PostgreSQL SQLSTATE.
fn violation_report(error: &DataFusionError) -> Option<DFResult<RecordBatch>> {
    use crate::write_mode::{NotNullViolation, WriteViolation};
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        let payload = if let Some(violation) = current.downcast_ref::<WriteViolation>() {
            Some(match violation {
                WriteViolation::DuplicateKey { table } => {
                    ("duplicate_key", table.as_str())
                }
                WriteViolation::ExistingKey { table } => ("existing_key", table.as_str()),
                WriteViolation::NotNull { column } => ("not_null", column.as_str()),
            })
        } else {
            current
                .downcast_ref::<NotNullViolation>()
                .map(|violation| ("not_null", violation.column.as_str()))
        };
        if let Some((kind, detail)) = payload {
            return Some(
                RecordBatch::try_new(
                    report_schema(),
                    vec![
                        Arc::new(StringArray::from(vec![""])) as ArrayRef,
                        Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                        Arc::new(StringArray::from(vec![""])) as ArrayRef,
                        Arc::new(UInt64Array::from(vec![0])) as ArrayRef,
                        Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                        Arc::new(StringArray::from(vec![""])) as ArrayRef,
                        Arc::new(StringArray::from(vec![kind])) as ArrayRef,
                        Arc::new(StringArray::from(vec![detail])) as ArrayRef,
                    ],
                )
                .map_err(Into::into),
            );
        }
        source = current.source();
    }
    None
}

/// Accumulates one report batch into the commit node's partition map and
/// returns the number of rows it reported.
fn collect_reports(
    batch: &RecordBatch,
    partitioned: &mut PartitionedFile,
) -> DFResult<u64> {
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let report_error = |column: &str| {
        DataFusionError::Internal(format!("write report is missing {column}"))
    };
    let partitions = batch
        .column_by_name(REPORT_PARTITION_DESC)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| report_error(REPORT_PARTITION_DESC))?;
    let paths = batch
        .column_by_name(REPORT_PATH)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| report_error(REPORT_PATH))?;
    let counts = batch
        .column_by_name(REPORT_ROW_COUNT)
        .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| report_error(REPORT_ROW_COUNT))?;
    let sizes = batch
        .column_by_name(REPORT_SIZE)
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| report_error(REPORT_SIZE))?;
    let exist_cols = batch
        .column_by_name(REPORT_FILE_EXIST_COLS)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| report_error(REPORT_FILE_EXIST_COLS))?;
    let kinds = batch
        .column_by_name(REPORT_ERROR_KIND)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| report_error(REPORT_ERROR_KIND))?;
    let details = batch
        .column_by_name(REPORT_ERROR_DETAIL)
        .and_then(|column| column.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| report_error(REPORT_ERROR_DETAIL))?;
    let mut total = 0u64;
    for row in 0..batch.num_rows() {
        if !kinds.is_null(row) {
            use crate::write_mode::WriteViolation;
            if details.is_null(row) {
                return Err(report_error(REPORT_ERROR_DETAIL));
            }
            let detail = details.value(row).to_owned();
            let violation = match kinds.value(row) {
                "duplicate_key" => WriteViolation::DuplicateKey { table: detail },
                "existing_key" => WriteViolation::ExistingKey { table: detail },
                "not_null" => WriteViolation::NotNull { column: detail },
                kind => {
                    return Err(DataFusionError::Internal(format!(
                        "unknown write report violation {kind}"
                    )));
                }
            };
            return Err(DataFusionError::External(Box::new(violation)));
        }
        let partition_desc = partitions.value(row).to_string();
        let path = paths.value(row).to_string();
        let count = counts.value(row);
        let entry = partitioned
            .entry(partition_desc)
            .or_insert_with(|| (Vec::new(), 0));
        entry.0.push(CommittedFile {
            path,
            size: sizes.value(row),
            file_exist_cols: exist_cols.value(row).to_string(),
        });
        entry.1 += count;
        total += count;
    }
    Ok(total)
}

/// Convert a report into the `DataFusionError` the execution layer expects.
///
/// The writer and the metadata client return `rootcause::Report`; converting
/// it back into a `DataFusionError` normally boxes the report and loses the
/// concrete error type. When the report was built from a `DataFusionError`,
/// recover that error instead, so a typed write violation still reaches the
/// SQL front end's SQLSTATE mapping.
fn into_datafusion_error(report: rootcause::Report) -> DataFusionError {
    match report.downcast_report::<DataFusionError>() {
        Ok(report) => report.into_current_context(),
        Err(report) => match report.downcast_report::<LakeSoulMetaDataError>() {
            Ok(report) => metadata_error(report.into_current_context()),
            Err(report) => DataFusionError::External(report.into_boxed_error()),
        },
    }
}

/// Keep a lost write race a typed, retryable error instead of an internal one.
fn metadata_error(error: LakeSoulMetaDataError) -> DataFusionError {
    match error {
        error @ (LakeSoulMetaDataError::InsertConflict { .. }
        | LakeSoulMetaDataError::CommitConflict(_)) => {
            DataFusionError::External(Box::new(crate::write_mode::WriteConflict {
                detail: error.to_string(),
            }))
        }
        error => DataFusionError::External(Box::new(error)),
    }
}

fn write_checks(
    table_schema: &Schema,
    input_schema: &Schema,
    range_partitions: &[String],
    primary_keys: &[String],
) -> DFResult<WriteInvariants> {
    for name in range_partitions.iter().chain(primary_keys) {
        input_schema.index_of(name).map_err(|_| {
            DataFusionError::Plan(format!("Missing partition/key column '{name}'"))
        })?;
    }
    for field in input_schema.fields() {
        let stored = table_schema.field_with_name(field.name())?;
        if !DFSchema::datatype_is_logically_equal(stored.data_type(), field.data_type())
            || !write_type_parameters_match(stored.data_type(), field.data_type())
        {
            return Err(DataFusionError::Plan(format!(
                "Column '{}' has type {}, expected {}",
                field.name(),
                field.data_type(),
                stored.data_type()
            )));
        }
    }
    if primary_keys.is_empty()
        && table_schema.fields().len() != input_schema.fields().len()
    {
        return Err(DataFusionError::Plan(
            "Inserting query must have the same schema with the table.".to_string(),
        ));
    }
    WriteInvariants::try_new(table_schema, input_schema)
}

// Logical equality permits different vector sizes and decimal precision/scale.
// The sink does not cast contributions, so those parameters must also match.
fn write_type_parameters_match(stored: &DataType, input: &DataType) -> bool {
    use DataType::*;
    match (stored, input) {
        (Dictionary(_, value), _) => write_type_parameters_match(value, input),
        (_, Dictionary(_, value)) => write_type_parameters_match(stored, value),
        (RunEndEncoded(_, value), _) => {
            write_type_parameters_match(value.data_type(), input)
        }
        (_, RunEndEncoded(_, value)) => {
            write_type_parameters_match(stored, value.data_type())
        }
        (Decimal32(..) | Decimal64(..) | Decimal128(..) | Decimal256(..), _) => {
            stored == input
        }
        (FixedSizeList(left, left_size), FixedSizeList(right, right_size)) => {
            left_size == right_size
                && write_type_parameters_match(left.data_type(), right.data_type())
        }
        (List(left), List(right))
        | (LargeList(left), LargeList(right))
        | (ListView(left), ListView(right))
        | (LargeListView(left), LargeListView(right))
        | (Map(left, _), Map(right, _)) => {
            write_type_parameters_match(left.data_type(), right.data_type())
        }
        (Struct(left), Struct(right)) => left.iter().zip(right).all(|(left, right)| {
            write_type_parameters_match(left.data_type(), right.data_type())
        }),
        (Union(left, _), Union(right, _)) => {
            left.iter()
                .zip(right.iter())
                .all(|((_, left), (_, right))| {
                    write_type_parameters_match(left.data_type(), right.data_type())
                })
        }
        _ => true,
    }
}

const WRITE_FILES_TOTAL: &str = "lakesoul_write_files_total";
const WRITE_BYTES_TOTAL: &str = "lakesoul_write_bytes_total";
const WRITE_ROWS_TOTAL: &str = "lakesoul_write_rows_total";
const COMMIT_DURATION_SECONDS: &str = "lakesoul_commit_duration_seconds";
const COMMIT_PARTITIONS_TOTAL: &str = "lakesoul_commit_partitions_total";
const INDEX_BUILD_DURATION_SECONDS: &str = "lakesoul_index_build_duration_seconds";
const INDEX_BUILT_SHARDS_TOTAL: &str = "lakesoul_index_built_shards_total";

static DESCRIBE_METRICS: Once = Once::new();

fn describe_metrics() {
    DESCRIBE_METRICS.call_once(|| {
        metrics::describe_counter!(
            WRITE_FILES_TOTAL,
            "Data files written by LakeSoul sinks"
        );
        metrics::describe_counter!(WRITE_BYTES_TOTAL, "Bytes written by LakeSoul sinks");
        metrics::describe_counter!(WRITE_ROWS_TOTAL, "Rows written by LakeSoul sinks");
        metrics::describe_histogram!(
            COMMIT_DURATION_SECONDS,
            metrics::Unit::Seconds,
            "Duration of LakeSoul metadata commits grouped by outcome"
        );
        metrics::describe_counter!(
            COMMIT_PARTITIONS_TOTAL,
            "Partitions committed by LakeSoul metadata commits"
        );
        metrics::describe_histogram!(
            INDEX_BUILD_DURATION_SECONDS,
            metrics::Unit::Seconds,
            "Index maintenance duration grouped by index type and outcome"
        );
        metrics::describe_counter!(
            INDEX_BUILT_SHARDS_TOTAL,
            "Index shards built by index maintenance grouped by index type"
        );
    });
}

fn record_index_build(
    index_type: &'static str,
    started: std::time::Instant,
    result: &DFResult<usize>,
) {
    let outcome = if result.is_ok() { "success" } else { "error" };
    metrics::histogram!(
        INDEX_BUILD_DURATION_SECONDS,
        "index_type" => index_type,
        "outcome" => outcome,
    )
    .record(started.elapsed().as_secs_f64());
    if let Ok(built) = result {
        metrics::counter!(INDEX_BUILT_SHARDS_TOTAL, "index_type" => index_type)
            .increment(*built as u64);
    }
}

/// Records *metadata* commit latency and partitions, marking the commit
/// failed when it is dropped without [`WriteCommitMetrics::success`].
///
/// Only the publication calls belong inside: writing files before and
/// building indexes after are separate phases with their own metrics, and
/// their failures must not be reported as metadata-commit failures.
struct WriteCommitMetrics {
    started: std::time::Instant,
    partitions: u64,
    finished: bool,
}

impl WriteCommitMetrics {
    fn start() -> Self {
        describe_metrics();
        Self {
            started: std::time::Instant::now(),
            partitions: 0,
            finished: false,
        }
    }

    fn record_partitions(&mut self, partitions: u64) {
        self.partitions = partitions;
    }

    fn success(mut self) {
        self.finish("success");
    }

    fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        metrics::histogram!(COMMIT_DURATION_SECONDS, "outcome" => outcome)
            .record(self.started.elapsed().as_secs_f64());
        if outcome == "success" {
            metrics::counter!(COMMIT_PARTITIONS_TOTAL).increment(self.partitions);
        }
    }
}

impl Drop for WriteCommitMetrics {
    fn drop(&mut self) {
        self.finish("error");
    }
}

/// A network hash assigns equal keys to one worker, but it is not LakeSoul's
/// persisted hash. Sort each worker's share, then use the existing LakeSoul
/// repartitioner to produce range-uniform batches and correctly named buckets.
fn prepare_task_input(
    input: Arc<dyn ExecutionPlan>,
    range: &[String],
    primary_keys: &[String],
    buckets: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    use arrow::compute::SortOptions;
    use datafusion::physical_expr::PhysicalSortExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;

    if range.is_empty() && primary_keys.is_empty() {
        return Ok(input);
    }
    let schema = input.schema();
    let columns = |names: &[String]| -> DFResult<Vec<Arc<dyn PhysicalExpr>>> {
        names
            .iter()
            .map(|name| {
                Ok(Arc::new(Column::new(name, schema.index_of(name)?))
                    as Arc<dyn PhysicalExpr>)
            })
            .collect()
    };
    let range_exprs = columns(range)?;
    let key_exprs = columns(primary_keys)?;
    let ordering = LexOrdering::new(range_exprs.iter().chain(&key_exprs).map(|expr| {
        PhysicalSortExpr {
            expr: Arc::clone(expr),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }
    }))
    .expect("a range or primary key supplies at least one sort expression");
    let sorted: Arc<dyn ExecutionPlan> =
        if input.output_partitioning().partition_count() <= 1 {
            Arc::new(SortExec::new(ordering, input))
        } else {
            let sorted = Arc::new(
                SortExec::new(ordering.clone(), input).with_preserve_partitioning(true),
            );
            Arc::new(SortPreservingMergeExec::new(ordering, sorted))
        };
    Ok(Arc::new(
        lakesoul_io::physical_plan::RepartitionByRangeAndHashExec::try_new(
            sorted,
            range_exprs,
            Partitioning::Hash(key_exprs, buckets),
            ExecutionPlanMetricsSet::new(),
        )?,
    ))
}

/// Writes record batches to a LakeSoul table and reports the files it wrote.
///
/// This is the half of the historical sink that can be serialized to a
/// worker: it never touches the metadata client and never publishes anything.
/// A statement's rows become visible only when the sibling
/// [`LakeSoulCommitExec`] commits the collected reports.
pub struct LakeSoulWriteExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,

    /// Optional required sort order for output data.
    sort_order: Option<LexRequirement>,

    /// The table info of LakeSoul table.
    table_info: Arc<TableInfo>,

    /// The range partitions.
    range_partitions: Arc<Vec<String>>,

    /// The primary keys of the table.
    primary_keys: Vec<String>,

    /// Persisted LakeSoul bucket count (independent of the network hash).
    bucket_count: usize,

    /// Worker inputs need local range splitting and persisted-bucket routing.
    task_partitioning: bool,

    /// Stored-schema NOT NULL constraints resolved against the sparse input.
    not_null_checks: Arc<WriteInvariants>,

    /// Present only for ordinary SQL INSERT into primary-key tables.
    insert_only: Option<InsertOnly>,

    /// Files this statement has opened so far, shared with the commit node so
    /// a failed statement can remove them.
    attempted_files: Arc<Mutex<Vec<String>>>,

    /// Unique per statement, embedded in every data file name.
    write_id: String,

    /// The properties of the plan.
    properties: Arc<PlanProperties>,
}

impl Debug for LakeSoulWriteExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LakeSoulWriteExec")
            .field("write_id", &self.write_id)
            .finish()
    }
}

impl LakeSoulWriteExec {
    /// Create a plan that writes into `table_info`.
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        sort_order: Option<LexRequirement>,
        table_info: Arc<TableInfo>,
    ) -> Result<Self> {
        Self::try_new(
            input,
            sort_order,
            table_info,
            rand::distr::Alphanumeric.sample_string(&mut rand::rng(), 16),
        )
    }

    /// Reconstruct a write task with its coordinator-assigned statement id.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        sort_order: Option<LexRequirement>,
        table_info: Arc<TableInfo>,
        write_id: String,
    ) -> Result<Self> {
        if write_id.is_empty()
            || !write_id.bytes().all(|byte| byte.is_ascii_alphanumeric())
        {
            return Err(DataFusionError::Plan(
                "LakeSoul write id must be nonempty ASCII alphanumeric".into(),
            )
            .into());
        }
        let (range_partitions, primary_keys) =
            parse_table_info_partitions(&table_info.partitions)?;
        let table_schema = schema_from_table_info_metadata(
            &table_info.table_schema,
            &table_info.table_schema_arrow_ipc,
            &table_info.table_schema_arrow_ipc_json_hash,
        )?;
        let not_null_checks = Arc::new(write_checks(
            &table_schema,
            input.schema().as_ref(),
            &range_partitions,
            &primary_keys,
        )?);
        let bucket_count = if primary_keys.is_empty() {
            1
        } else {
            serde_json::from_str::<crate::catalog::LakeSoulTableProperty>(
                &table_info.properties,
            )?
            .hash_bucket_num
            .as_deref()
            .unwrap_or("1")
            .parse::<isize>()?
            .max(1) as usize
        };
        let range_partitions = Arc::new(range_partitions);
        Ok(Self {
            input,
            sort_order,
            table_info,
            range_partitions,
            primary_keys,
            bucket_count,
            task_partitioning: false,
            not_null_checks,
            insert_only: None,
            attempted_files: Arc::new(Mutex::new(Vec::new())),
            write_id,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(report_schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        })
    }

    /// The table this plan writes into.
    pub fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    pub fn sort_order(&self) -> &Option<LexRequirement> {
        &self.sort_order
    }

    pub fn primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    pub fn range_partitions(&self) -> &[String] {
        &self.range_partitions
    }

    pub fn task_partitioning(&self) -> bool {
        self.task_partitioning
    }

    pub fn with_task_partitioning(mut self) -> Self {
        self.task_partitioning = true;
        self
    }

    /// The source of the existing-key scan, when this is an insert-only plan.
    pub(crate) fn insert_only(&self) -> Option<&InsertOnly> {
        self.insert_only.as_ref()
    }

    /// Attach the insert-only validation state built by the planner.
    pub(crate) fn with_insert_only(mut self, insert_only: InsertOnly) -> Self {
        self.insert_only = Some(insert_only);
        self
    }

    /// Files this plan opened. The commit node uses the same registry.
    pub fn attempted_files(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.attempted_files)
    }

    /// Unique id stamped into this statement's file names.
    pub fn write_id(&self) -> &str {
        &self.write_id
    }

    /// Creates this node's root span through `dispatch`, resolving its parent
    /// from that same dispatcher.
    ///
    /// `Span::current()` and `info_span!` each consult the thread-local
    /// dispatcher, so resolving the parent under one dispatcher and creating the
    /// child under another hands `Registry::clone_span` an id owned by a foreign
    /// registry; it panics with "no span exists with that ID". Running both
    /// through the captured dispatcher keeps parent and child in the same
    /// registry no matter which dispatcher is ambient by then (a caller under a
    /// scoped subscriber, or a future polled on a different thread).
    ///
    /// The span is created eagerly for the same reason the parent is cloned
    /// explicitly: the implicit parent in `info_span!` clones the thread-local
    /// id after the macro has already observed it, which can race with a
    /// test/task closing that span, and holding our own clone keeps the id
    /// valid for the lifetime of the span it parents.
    fn create_exec_span(
        dispatch: &tracing::Dispatch,
        table_info: &TableInfo,
        write_id: &str,
    ) -> tracing::Span {
        tracing::dispatcher::with_default(dispatch, || {
            let parent = tracing::Span::current();
            tracing::info_span!(
                parent: &parent,
                "lake_soul_write_execute",
                table_id = %table_info.table_id,
                table_name = %table_info.table_name,
                write_id = %write_id,
                outcome = tracing::field::Empty,
                error = tracing::field::Empty,
            )
        })
    }

    /// Write every input partition, then return one report batch.
    fn write_all(
        &self,
        context: Arc<TaskContext>,
        span: tracing::Span,
        dispatch: tracing::Dispatch,
        abandoned: Arc<AtomicBool>,
    ) -> impl Future<Output = DFResult<RecordBatch>> + Send + 'static {
        let table_info = self.table_info();
        let range_partitions = Arc::clone(&self.range_partitions);
        let invariants = Arc::clone(&self.not_null_checks);
        let attempted_files = Arc::clone(&self.attempted_files);
        let task = datafusion_distributed::DistributedTaskContext::from_ctx(&context);
        let write_id = if task.task_count > 1 {
            format!("{}T{}", self.write_id, task.task_index)
        } else {
            self.write_id.clone()
        };
        let input = Arc::clone(&self.input);
        let insert_only = self.insert_only.clone();
        let task_partitioning = self.task_partitioning;
        let primary_keys = self.primary_keys.clone();
        let bucket_count = self.bucket_count;
        async move {
            let result: Result<Vec<WriteFileReport>> = async {
                let input = if task_partitioning {
                    prepare_task_input(
                        input,
                        &range_partitions,
                        &primary_keys,
                        bucket_count,
                    )?
                } else {
                    input
                };
                let streams = match insert_only {
                    Some(insert) => {
                        insert
                            .validate(input.clone(), context.clone(), &invariants)
                            .await?
                    }
                    None => {
                        let partitions = input.output_partitioning().partition_count();
                        let mut streams = Vec::with_capacity(partitions);
                        for partition in 0..partitions {
                            streams.push(input.execute(partition, context.clone())?);
                        }
                        streams
                    }
                };
                let writes: Vec<JoinHandle<Result<Vec<WriteFileReport>>>> = streams
                    .into_iter()
                    .enumerate()
                    .map(|(partition, stream)| {
                        let context = Arc::clone(&context);
                        let table_info = Arc::clone(&table_info);
                        let range_partitions = Arc::clone(&range_partitions);
                        let invariants = Arc::clone(&invariants);
                        let write_id = write_id.clone();
                        let attempted_files = Arc::clone(&attempted_files);
                        tokio::spawn(
                            Self::pull_and_sink(
                                Arc::clone(&input),
                                partition,
                                context,
                                table_info,
                                range_partitions,
                                invariants,
                                write_id,
                                Some(stream),
                                Some(attempted_files),
                            )
                            .instrument(span.clone())
                            .with_subscriber(dispatch.clone()),
                        )
                    })
                    .collect();
                let mut reports = Vec::new();
                for result in futures::future::join_all(writes).await {
                    match result {
                        Ok(Ok(mut written)) => reports.append(&mut written),
                        Ok(Err(report)) => return Err(report),
                        Err(error) => {
                            return Err(rootcause::report!(DataFusionError::Execution(
                                error.to_string()
                            ))
                            .into());
                        }
                    }
                }
                Ok(reports)
            }
            .await;
            // The report RPC may disappear while uploads are still in flight.
            // Finish/abort every writer first, then remove unreported files.
            // If this check wins a race with stream drop, all uploads already
            // finished before that drop, so the coordinator's failure sweep sees
            // them. If stream drop wins, this task performs the late cleanup.
            if abandoned.load(Ordering::Acquire) {
                let files = attempted_files.lock().await;
                remove_files(&context, &files).await?;
                return Err(DataFusionError::Execution(
                    "LakeSoul write report was abandoned before delivery".into(),
                ));
            }

            match result {
                Ok(reports) => report_batch(&reports),
                Err(error) => {
                    // The statement failed before it could be committed; its data
                    // files are not referenced by any metadata version.
                    let files = attempted_files.lock().await;
                    if let Err(cleanup) = remove_files(&context, &files).await {
                        return Err(DataFusionError::Execution(format!(
                            "{error}; {cleanup}"
                        )));
                    }
                    let error = into_datafusion_error(error);
                    violation_report(&error).unwrap_or(Err(error))
                }
            }
        }
    }

    #[instrument(
        name = "table_write_partition",
        level = "info",
        skip_all,
        fields(
            table_id = %table_info.table_id,
            table_name = %table_info.table_name,
            partition,
            range_partition_count = range_partitions.len(),
        ),
        err
    )]
    #[allow(clippy::too_many_arguments)]
    async fn pull_and_sink(
        input: Arc<dyn ExecutionPlan>,
        partition: usize,
        context: Arc<TaskContext>,
        table_info: Arc<TableInfo>,
        range_partitions: Arc<Vec<String>>,
        not_null_checks: Arc<WriteInvariants>,
        write_id: String,
        validated: Option<SendableRecordBatchStream>,
        attempted_files: Option<Arc<Mutex<Vec<String>>>>,
    ) -> Result<Vec<WriteFileReport>> {
        debug!("{}", input.name());
        let mut data = match validated {
            Some(stream) => stream,
            None => input.execute(partition, context.clone())?,
        };
        // O(nm), n = number of data fields, m = number of range partitions
        let schema_projection_excluding_range = data
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter_map(
                |(idx, field)| match range_partitions.contains(field.name()) {
                    true => None,
                    false => Some(idx),
                },
            )
            .collect::<Vec<_>>();

        let physical_format = crate::catalog::table_file_format(&table_info.properties)?;
        // One writer (and one data file) per input partition; the writer is
        // chosen by the table's physical format (parquet / vortex).
        let mut partitioned_writer =
            HashMap::<String, Box<dyn AsyncBatchWriter + Send>>::new();
        let read_result: Result<()> = async {
            while let Some(batch) = data.next().await.transpose()? {
                not_null_checks.validate(&batch)?;
                debug!("write record_batch with {} rows", batch.num_rows());
                let columnar_values =
                    get_columnar_values(&batch, range_partitions.clone())?;
                let partition_desc = columnar_values_to_partition_desc(&columnar_values);
                debug!("{partition_desc}");
                let batch_excluding_range =
                    batch.project(&schema_projection_excluding_range)?;
                let file_absolute_path = format!(
                    "{}{}part-{}_{:0>4}.{}",
                    table_info.table_path,
                    columnar_values_to_sub_path(&columnar_values),
                    write_id,
                    partition,
                    physical_format.extension()
                );

                if !partitioned_writer.contains_key(&partition_desc) {
                    debug!("create writer for partition {partition_desc}");
                    if let Some(files) = &attempted_files {
                        files.lock().await.push(file_absolute_path.clone());
                    }
                    let io_config = create_io_config_builder_from_table_info(
                        table_info.clone(),
                        SecretMap::new(),
                        SecretMap::new(),
                    )?
                    // The sink already assigns one file per input partition, so
                    // disable the writer's own dynamic partitioning.
                    .set_dynamic_partition(false)
                    .with_files(vec![file_absolute_path])
                    .with_schema(batch_excluding_range.schema())
                    .build();
                    let new_session =
                        Arc::new(LakeSoulIOSession::from_plain_config_and_context(
                            io_config,
                            context.clone(),
                        ));
                    let writer = create_writer(new_session).await?;
                    partitioned_writer.insert(partition_desc.clone(), writer);
                }

                if let Some(async_writer) = partitioned_writer.get_mut(&partition_desc) {
                    async_writer
                        .write_record_batch(batch_excluding_range)
                        .await?;
                }
            }
            Ok(())
        }
        .await;
        let mut writers = partitioned_writer.into_iter();
        let mut reports = Vec::new();
        let result: Result<()> = async {
            read_result?;

            // TODO: apply rolling strategy
            for (partition_desc, writer) in writers.by_ref() {
                let outputs = writer.flush_and_close().await?;
                for output in outputs {
                    metrics::counter!(WRITE_FILES_TOTAL, "format" => physical_format.extension())
                        .increment(1);
                    metrics::counter!(WRITE_BYTES_TOTAL, "format" => physical_format.extension())
                        .increment(output.object_meta.size);
                    metrics::counter!(WRITE_ROWS_TOTAL, "format" => physical_format.extension())
                        .increment(output.row_count as u64);
                    // `file_ops.size` is an i64; a writer that reported more
                    // bytes than that cannot be recorded truthfully.
                    let size = i64::try_from(output.object_meta.size).map_err(|_| {
                        DataFusionError::Internal(format!(
                            "written file '{}' reports {} bytes, more than a metadata \
                             file op can hold",
                            output.file_path, output.object_meta.size
                        ))
                    })?;
                    reports.push(WriteFileReport {
                        partition_desc: partition_desc.clone(),
                        bucket: partition as u32,
                        path: output.file_path,
                        row_count: output.row_count as u64,
                        size,
                        file_exist_cols: output.file_exist_cols.join(","),
                    });
                }
            }
            Ok(())
        }
        .await;
        if let Err(mut error) = result {
            // Await aborts before the caller removes files; dropping a sender
            // alone can race a background writer's end-of-input publication.
            for (_, writer) in writers {
                if let Err(abort) = writer.abort_and_close().await {
                    error = rootcause::report!(DataFusionError::Execution(format!(
                        "{error}; writer abort failed: {abort}"
                    )))
                    .into();
                }
            }
            return Err(error);
        }

        Ok(reports)
    }
}

impl DisplayAs for LakeSoulWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LakeSoulWriteExec")
    }
}

impl ExecutionPlanProperties for LakeSoulWriteExec {
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

impl ExecutionPlan for LakeSoulWriteExec {
    fn name(&self) -> &str {
        "LakeSoulWriteExec"
    }

    fn schema(&self) -> SchemaRef {
        report_schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // Each input partition gets its own writer. Coalescing PK partitions
        // would mislabel their hash buckets; append-only inputs can likewise
        // stay parallel without changing row semantics.
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // The input order is either explicitly set (such as by a ListingTable),
        // or require that the sink gets the data in the order the input
        // produced it (otherwise the optimizer may choose to reorder the input
        // which could result in unintended / poor UX).
        let mut requirements = vec![None; self.children().len()];
        requirements[0] = self
            .sort_order
            .as_ref()
            .map(|order| OrderingRequirements::Soft(vec![order.clone()]));
        requirements
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // The sink is responsible for dynamically partitioning its own input
        // at execution time.
        vec![false; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // Historical keys are an immutable captured plan, serialized inside
        // this node. Exposing it here would let stage planning split the file
        // set and make a worker miss existing keys.
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: if children.is_empty() {
                self.input.clone()
            } else {
                children[0].clone()
            },
            sort_order: self.sort_order.clone(),
            table_info: self.table_info.clone(),
            range_partitions: self.range_partitions.clone(),
            primary_keys: self.primary_keys.clone(),
            bucket_count: self.bucket_count,
            task_partitioning: self.task_partitioning,
            not_null_checks: self.not_null_checks.clone(),
            insert_only: self.insert_only.clone(),
            attempted_files: Arc::clone(&self.attempted_files),
            write_id: self.write_id.clone(),
            properties: self.properties.clone(),
        }))
    }

    /// Execute the plan and return this task's write report.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::NotImplemented(
                "LakeSoulWriteExec can only be called on partition 0!".to_string(),
            ));
        }
        // Preserve scoped subscribers as well as the current span across the
        // spawned write tasks. A Tokio task may be polled after moving away
        // from the thread where `execute` was called.
        let dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
        // Resolve the parent and create the span through that same subscriber;
        // see `create_exec_span`.
        let span = Self::create_exec_span(
            &dispatch,
            self.table_info().as_ref(),
            self.write_id(),
        );
        let entered = span.enter();

        let abandoned = Arc::new(AtomicBool::new(false));
        let write = self.write_all(
            context,
            span.clone(),
            dispatch.clone(),
            Arc::clone(&abandoned),
        );
        let join = tokio::spawn(write.instrument(span.clone()).with_subscriber(dispatch));
        let schema = self.schema();
        let stream = futures::stream::once(async move {
            match join.await {
                Ok(Ok(batch)) => Ok(batch),
                Ok(Err(error)) => Err(error),
                Err(error) => Err(DataFusionError::Execution(error.to_string())),
            }
        })
        .boxed();
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream))
            as SendableRecordBatchStream;
        drop(entered);
        Ok(Box::pin(CancelOnDropStream::new(
            instrument_record_batch_stream(stream, span, "write_exec"),
            abandoned,
        )))
    }
}

/// Publishes the files reported by a [`LakeSoulWriteExec`].
///
/// This node stays on the coordinator: it owns the metadata client, the
/// snapshot captured at plan time, and the index maintenance. It drains every
/// report partition, then commits the whole statement exactly once.
pub struct LakeSoulCommitExec {
    /// Report stream produced by the write node (or a network boundary in
    /// front of a distributed write stage).
    input: Arc<dyn ExecutionPlan>,

    /// Schema describing the command tag payload.
    sink_schema: SchemaRef,

    /// The table info of LakeSoul table.
    table_info: Arc<TableInfo>,

    /// The metadata client.
    metadata_client: MetaDataClientRef,

    /// The primary keys of the table (used for the vector index build).
    primary_keys: Vec<String>,

    /// Object store configuration options forwarded to the vector index
    /// builder after the write commits.
    object_store_options: SecretMap,

    /// Snapshot captured at plan time; present for insert-only statements.
    insert_only_snapshot: Option<Vec<PartitionInfo>>,

    /// Files the write node opened, so a failed statement can remove them.
    attempted_files: Arc<Mutex<Vec<String>>>,

    /// Statement prefix used to find unreported files after a remote failure.
    remote_write_id: Option<String>,

    /// Set when the statement's result stream is dropped before the commit.
    cancelled: Arc<AtomicBool>,

    /// The properties of the plan.
    properties: Arc<PlanProperties>,
}

impl Debug for LakeSoulCommitExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LakeSoulCommitExec schema: {:?}", self.sink_schema)
    }
}

impl LakeSoulCommitExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        table_info: Arc<TableInfo>,
        metadata_client: MetaDataClientRef,
        primary_keys: Vec<String>,
        object_store_options: SecretMap,
        insert_only_snapshot: Option<Vec<PartitionInfo>>,
        attempted_files: Arc<Mutex<Vec<String>>>,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            input,
            sink_schema: make_sink_schema(),
            table_info,
            metadata_client,
            primary_keys,
            object_store_options,
            insert_only_snapshot,
            attempted_files,
            remote_write_id: None,
            cancelled,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(make_sink_schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }

    pub fn with_remote_write(mut self, write_id: String) -> Self {
        self.remote_write_id = Some(write_id);
        self
    }

    /// Creates this node's root span through `dispatch`, resolving its parent
    /// from that same dispatcher. The commit node keeps its span on the
    /// coordinator, so it has to be created before the commit task is spawned:
    /// that assigns the parent deterministically instead of letting whichever
    /// executor thread first polls the task decide it.
    ///
    /// See [`LakeSoulWriteExec::create_exec_span`] for why both the parent and
    /// the child must go through the captured dispatcher.
    fn create_commit_span(
        dispatch: &tracing::Dispatch,
        table_info: &TableInfo,
    ) -> tracing::Span {
        tracing::dispatcher::with_default(dispatch, || {
            let parent = tracing::Span::current();
            tracing::info_span!(
                parent: &parent,
                "lake_soul_commit_execute",
                table_id = %table_info.table_id,
                table_name = %table_info.table_name,
                outcome = tracing::field::Empty,
                error = tracing::field::Empty,
            )
        })
    }
}

impl DisplayAs for LakeSoulCommitExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LakeSoulCommitExec")
    }
}

impl ExecutionPlanProperties for LakeSoulCommitExec {
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

impl ExecutionPlan for LakeSoulCommitExec {
    fn name(&self) -> &str {
        "LakeSoulCommitExec"
    }

    fn schema(&self) -> SchemaRef {
        self.sink_schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: if children.is_empty() {
                self.input.clone()
            } else {
                children[0].clone()
            },
            sink_schema: self.sink_schema.clone(),
            table_info: self.table_info.clone(),
            metadata_client: self.metadata_client.clone(),
            primary_keys: self.primary_keys.clone(),
            object_store_options: self.object_store_options.clone(),
            insert_only_snapshot: self.insert_only_snapshot.clone(),
            attempted_files: Arc::clone(&self.attempted_files),
            remote_write_id: self.remote_write_id.clone(),
            cancelled: Arc::clone(&self.cancelled),
            properties: self.properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::NotImplemented(
                "LakeSoulCommitExec can only be called on partition 0!".to_string(),
            ));
        }
        let dispatch = tracing::dispatcher::get_default(|dispatch| dispatch.clone());
        // Resolve the parent and create the span through that same subscriber;
        // see `create_commit_span`.
        let span = Self::create_commit_span(&dispatch, self.table_info.as_ref());
        let entered = span.enter();

        let input = Arc::clone(&self.input);
        let table_info = Arc::clone(&self.table_info);
        let table_name =
            format!("{}.{}", table_info.table_namespace, table_info.table_name);
        let metadata_client = self.metadata_client.clone();
        let primary_keys = self.primary_keys.clone();
        let object_store_options = self.object_store_options.clone();
        let insert_only_snapshot = self.insert_only_snapshot.clone();
        let attempted_files = Arc::clone(&self.attempted_files);
        let remote_write_id = self.remote_write_id.clone();
        let cancelled = Arc::clone(&self.cancelled);
        let commit_span = span.clone();
        let commit = tokio::spawn(
            async move {
                commit_reports(
                    input,
                    table_info,
                    table_name,
                    metadata_client,
                    primary_keys,
                    object_store_options,
                    insert_only_snapshot,
                    attempted_files,
                    remote_write_id,
                    cancelled,
                    context,
                )
                .await
            }
            .instrument(commit_span)
            .with_subscriber(dispatch),
        );

        let sink_schema = self.sink_schema.clone();
        let stream = futures::stream::once(async move {
            match commit.await {
                Ok(Ok(count)) => Ok(make_sink_batch(count, String::new())),
                Ok(Err(error)) => Err(into_datafusion_error(error)),
                Err(error) => Err(DataFusionError::Execution(error.to_string())),
            }
        })
        .boxed();
        let stream = Box::pin(RecordBatchStreamAdapter::new(sink_schema, stream))
            as SendableRecordBatchStream;
        drop(entered);
        Ok(Box::pin(CancelOnDropStream::new(
            instrument_record_batch_stream(stream, span, "commit_exec"),
            Arc::clone(&self.cancelled),
        )) as SendableRecordBatchStream)
    }
}

#[allow(clippy::too_many_arguments)]
async fn commit_reports(
    input: Arc<dyn ExecutionPlan>,
    table_info: Arc<TableInfo>,
    table_name: String,
    metadata_client: MetaDataClientRef,
    primary_keys: Vec<String>,
    object_store_options: SecretMap,
    insert_only_snapshot: Option<Vec<PartitionInfo>>,
    attempted_files: Arc<Mutex<Vec<String>>>,
    remote_write_id: Option<String>,
    cancelled: Arc<AtomicBool>,
    context: Arc<TaskContext>,
) -> Result<u64> {
    let partitions = input.output_partitioning().partition_count();
    let mut partitioned = PartitionedFile::new();
    // Start and drain every report partition, even after one fails. A
    // repartition or network shuffle has bounded channels; draining only one
    // partition can deadlock, and cleanup must wait for the other writers.
    let mut incoming = futures::stream::SelectAll::new();
    let mut failure = None;
    for partition in 0..partitions {
        match input.execute(partition, Arc::clone(&context)) {
            Ok(stream) => incoming.push(stream),
            Err(error) => {
                failure.get_or_insert(error);
            }
        }
    }
    while let Some(batch) = incoming.next().await {
        if let Err(error) =
            batch.and_then(|batch| collect_reports(&batch, &mut partitioned).map(|_| ()))
        {
            failure.get_or_insert(error);
        }
    }
    if remote_write_id.is_some() {
        attempted_files.lock().await.extend(
            partitioned
                .values()
                .flat_map(|(files, _)| files.iter().map(|file| file.path.clone())),
        );
    }
    if let Some(error) = failure {
        // Nothing was published; the files are unreferenced.
        if let Some(write_id) = &remote_write_id {
            let url = datafusion::datasource::listing::ListingTableUrl::parse(
                &table_info.table_path,
            )?;
            let store = context.runtime_env().object_store(url.object_store())?;
            let prefix = format!("part-{write_id}");
            let mut objects = store.list(Some(url.prefix()));
            let mut files = attempted_files.lock().await;
            while let Some(object) = objects.next().await.transpose()? {
                if object
                    .location
                    .filename()
                    .is_some_and(|name| name.starts_with(&prefix))
                {
                    files.push(format!("{}{}", url.object_store(), object.location));
                }
            }
        }
        let files = attempted_files.lock().await;
        if let Err(cleanup) = remove_files(&context, &files).await {
            return Err(rootcause::report!(DataFusionError::Execution(format!(
                "{error}; {cleanup}"
            )))
            .into());
        }
        return Err(error.into());
    }

    let keep_files = insert_only_snapshot
        .as_ref()
        .map(|_| Arc::new(AtomicBool::new(false)));
    let commit_guard = match &insert_only_snapshot {
        Some(snapshot) => Some(CommitGuard::InsertOnly {
            table_info: Arc::clone(&table_info),
            snapshot: snapshot.clone(),
            keep_files: Arc::clone(keep_files.as_ref().expect("keep_files")),
        }),
        None => Some(CommitGuard::Streamed {
            context: Arc::clone(&context),
            attempted_files: Arc::clone(&attempted_files),
        }),
    };
    let result = wait_for_commit(
        metadata_client,
        table_name,
        primary_keys,
        object_store_options,
        partitioned,
        Some(Arc::clone(&cancelled)),
        commit_guard,
    )
    .await;
    if let (Some(keep_files), Err(_)) = (keep_files, &result)
        && !keep_files.load(Ordering::Acquire)
    {
        // The insert-only guard only removes files inside the publication
        // path; a cancellation before publication leaves them to this outer
        // guard, like the historical sink.
        let files = attempted_files.lock().await;
        remove_files(&context, &files).await?;
    }
    result
}

/// How the commit node cleans up a failed statement.
enum CommitGuard {
    InsertOnly {
        table_info: Arc<TableInfo>,
        snapshot: Vec<PartitionInfo>,
        keep_files: Arc<AtomicBool>,
    },
    Streamed {
        context: Arc<TaskContext>,
        attempted_files: Arc<Mutex<Vec<String>>>,
    },
}

#[allow(clippy::too_many_arguments)]
async fn wait_for_commit(
    client: MetaDataClientRef,
    table_name: String,
    primary_keys: Vec<String>,
    object_store_options: SecretMap,
    partitioned_file_path_and_row_count: PartitionedFile,
    cancelled: Option<Arc<AtomicBool>>,
    commit_guard: Option<CommitGuard>,
) -> Result<u64> {
    // A cancelled statement must not publish: the client already received
    // `57014` and a commit here would make the cancellation a lie. Once the
    // metadata commit below has started, it runs to completion instead.
    if cancelled
        .as_ref()
        .is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
    {
        if let Some(CommitGuard::Streamed {
            context,
            attempted_files,
        }) = &commit_guard
        {
            let files = attempted_files.lock().await;
            remove_files(context, &files).await?;
        }
        return Err(DataFusionError::Execution(format!(
            "INSERT into {table_name} was cancelled before its metadata commit"
        ))
        .into());
    }
    let count = partitioned_file_path_and_row_count
        .values()
        .map(|(_, rows)| rows)
        .sum();

    // Timed and finalized around the publication alone: the file writers
    // above and the index maintenance below are not part of the metadata
    // commit, and index outcomes have their own metrics.
    let mut commit_metrics = WriteCommitMetrics::start();
    commit_metrics.record_partitions(partitioned_file_path_and_row_count.len() as u64);

    let commit_started = std::time::Instant::now();
    if let Some(CommitGuard::InsertOnly {
        table_info,
        snapshot,
        keep_files,
    }) = &commit_guard
    {
        let mut data_files = Vec::new();
        for (partition_desc, (files, _)) in partitioned_file_path_and_row_count.iter() {
            for file in files {
                data_files.push(DataFileInfo {
                    partition_desc: partition_desc.clone(),
                    path: file.path.clone(),
                    file_op: "add".into(),
                    size: file.size,
                    file_exist_cols: file.file_exist_cols.clone(),
                    ..Default::default()
                });
            }
        }
        match client
            .commit_insert_only(table_info, snapshot, data_files)
            .await
        {
            Ok(_) => keep_files.store(true, Ordering::Release),
            Err(error) => {
                if matches!(error, LakeSoulMetaDataError::InsertCommitOutcomeUnknown(_)) {
                    keep_files.store(true, Ordering::Release);
                }
                return Err(error.into());
            }
        }
    } else if partitioned_file_path_and_row_count
        .values()
        .any(|(files, _)| !files.is_empty())
    {
        // A statement that wrote no files publishes nothing, so it must not
        // require the table in the metadata store either: the resolved
        // table id below only exists to publish those files.
        // One publication transaction for every range partition: a failure
        // leaves the statement invisible instead of publishing a prefix.
        // The attempted files are removed when publication fails: they are
        // not referenced by any committed version.
        // Publication may fail ambiguously (a dropped connection during
        // COMMIT). These ids let the failure path ask the metadata store
        // whether this statement did become visible before deleting files.
        let mut publication_table: Option<String> = None;
        let mut publication_ids: Vec<(String, String)> = Vec::new();
        let publication: DFResult<()> = async {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?
                .as_millis() as i64;
            let table_ref = TableReference::from(table_name.as_str());
            let table_id = client
                .get_table_info_by_table_name(
                    table_ref.table(),
                    table_ref.schema().unwrap_or("default"),
                )
                .await
                .map_err(metadata_error)?
                .ok_or_else(|| {
                    DataFusionError::Execution(format!("table {table_name} not found"))
                })?
                .table_id;
            publication_table = Some(table_id.clone());
            let mut commits =
                Vec::with_capacity(partitioned_file_path_and_row_count.len());
            for (partition_desc, (files, _)) in partitioned_file_path_and_row_count.iter()
            {
                if files.is_empty() {
                    continue;
                }
                let (high, low) = uuid::Uuid::new_v4().as_u64_pair();
                publication_ids.push((
                    partition_desc.clone(),
                    uuid::Uuid::from_u64_pair(high, low).to_string(),
                ));
                commits.push(DataCommitInfo {
                    table_id: table_id.clone(),
                    partition_desc: partition_desc.clone(),
                    file_ops: files
                        .iter()
                        .map(|file| DataFileOp {
                            file_op: FileOp::Add as i32,
                            path: file.path.clone(),
                            size: file.size,
                            file_exist_cols: file.file_exist_cols.clone(),
                        })
                        .collect(),
                    commit_op: CommitOp::AppendCommit as i32,
                    timestamp,
                    commit_id: Some(Uuid { high, low }),
                    committed: false,
                    ..Default::default()
                });
            }
            if !commits.is_empty() {
                client
                    .commit_data_commit_infos(commits)
                    .await
                    .map_err(metadata_error)?;
                debug!(
                    "table: {} insert success at {:?}",
                    &table_name,
                    std::time::SystemTime::now()
                )
            }
            Ok(())
        }
        .await;
        if let Err(error) = publication {
            if let Some(CommitGuard::Streamed {
                context,
                attempted_files,
            }) = &commit_guard
            {
                // Delete only when the metadata store confirms this
                // statement is not visible. A failed lookup is treated as
                // "may be visible": orphan files are recoverable, a deleted
                // committed file is not.
                let mut may_be_visible = false;
                if let Some(table_id) = &publication_table {
                    let primary = client.primary_only().await;
                    for (partition_desc, commit_id) in &publication_ids {
                        match primary
                            .get_single_data_commit_info(
                                table_id,
                                partition_desc,
                                commit_id,
                            )
                            .await
                        {
                            Ok(Some(info)) if info.committed => {
                                may_be_visible = true;
                                break;
                            }
                            Ok(_) => {}
                            Err(_) => {
                                may_be_visible = true;
                                break;
                            }
                        }
                    }
                }
                if !may_be_visible {
                    let files = attempted_files.lock().await;
                    if let Err(cleanup) = remove_files(context, &files).await {
                        return Err(rootcause::report!(DataFusionError::Execution(
                            format!("{error}; {cleanup}")
                        ))
                        .into());
                    }
                }
            }
            return Err(error.into());
        }
    }
    debug!(
        elapsed_ms = commit_started.elapsed().as_secs_f64() * 1000.0,
        "committed metadata for {}", &table_name
    );
    commit_metrics.success();

    // Auto-build / incrementally update the vector index from the newly
    // committed files, driven by the table's `vector_index_columns`
    // property (same semantics as the Python write path). Re-fetch the table
    // info so properties changed after plan construction apply.
    // The native builder's future is not Send, so it is driven on the
    // blocking pool via the global runtime.
    let committed_files = partitioned_file_path_and_row_count;
    // The index builders take the historical `(paths, rows)` view: an index
    // entry only needs to know which files belong to a partition, not how the
    // commit records each of them.
    let index_files: HashMap<String, (Vec<String>, u64)> = committed_files
        .iter()
        .map(|(partition_desc, (files, rows))| {
            (
                partition_desc.clone(),
                (files.iter().map(|file| file.path.clone()).collect(), *rows),
            )
        })
        .collect();

    let table_ref = TableReference::from(table_name.as_str());
    let namespace = table_ref.schema().unwrap_or("default").to_string();

    let (configs, text_configs, deferred) = if let Some(fresh_info) = client
        .get_table_info_by_table_name(table_ref.table(), &namespace)
        .await?
    {
        let deferred = serde_json::from_str::<crate::catalog::LakeSoulTableProperty>(
            &fresh_info.properties,
        )
        .ok()
        .and_then(|properties| properties.index_maintenance)
        .is_some_and(|mode| mode.eq_ignore_ascii_case("deferred"));
        (
            crate::vector_index::parse_vector_index_from_table_properties(
                &fresh_info.properties,
            )
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?,
            crate::text_index::parse_text_index_from_table_properties(
                &fresh_info.properties,
            )
            .map_err(|report| DataFusionError::External(report.into_boxed_error()))?,
            deferred,
        )
    } else {
        (Vec::new(), Vec::new(), false)
    };

    // A deferred table commits data only; the embedder builds the pending
    // shards out of band from the recorded data-file coverage (see
    // lakesoul_common::index::pending_shard_files).
    if deferred {
        debug!("index maintenance deferred for {}", &table_name);
        return Ok(count);
    }

    // One active-files listing serves both auto-rebuild policies.
    let wants_vector_rebuild = !configs.is_empty()
        && !primary_keys.is_empty()
        && configs
            .iter()
            .any(|c| c.management.rebuild_mode.eq_ignore_ascii_case("auto"));
    let wants_text_rebuild = !text_configs.is_empty()
        && !primary_keys.is_empty()
        && text_configs
            .iter()
            .any(|c| c.management.rebuild_mode.eq_ignore_ascii_case("auto"));
    let active_files: Option<Vec<String>> = if wants_vector_rebuild || wants_text_rebuild
    {
        client
            .get_data_files_by_table_name(table_ref.table(), &namespace)
            .await
            .ok()
    } else {
        None
    };

    // Text and vector index maintenance are independent; build them
    // concurrently instead of serializing two blocking builds.
    let text_index_files = index_files.clone();
    let text_primary_keys = primary_keys.clone();
    let text_object_store_options = object_store_options.clone();
    let vector_active_files = active_files.clone();
    let text_client = Arc::clone(&client);
    let vector_config_count = configs.len();
    let vector_build = async {
        if configs.is_empty() || primary_keys.is_empty() {
            return Ok(0usize);
        }
        let active = if wants_vector_rebuild {
            vector_active_files
        } else {
            None
        };
        let started = std::time::Instant::now();
        let span = tracing::Span::current();
        let built = tokio::task::spawn_blocking(move || {
            let _entered = span.enter();
            lakesoul_io::session::GLOBAL_RUNTIME.block_on(
                crate::vector_index::auto_build_vector_index(
                    &configs,
                    &primary_keys,
                    &object_store_options,
                    &index_files,
                    active.as_deref(),
                    &client.vector_index_catalog(),
                ),
            )
        })
        .await
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "vector index auto build task failed: {error}"
            ))
        })
        .and_then(|built| {
            built.map_err(|report| DataFusionError::External(report.into_boxed_error()))
        });
        record_index_build("vector", started, &built);
        let built = built?;
        debug!(
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            "auto-built {built} vector index shard(s) for {}", &table_name
        );
        tracing::Span::current().record("built_shard_count", built as u64);
        Ok::<usize, DataFusionError>(built)
    }
    .instrument(tracing::info_span!(
        "vector_index_maintenance",
        config_count = vector_config_count,
        rebuild = wants_vector_rebuild,
        built_shard_count = tracing::field::Empty,
    ));
    let text_config_count = text_configs.len();
    let text_build = async {
        if text_configs.is_empty() || text_primary_keys.is_empty() {
            return Ok(0usize);
        }
        let active = if wants_text_rebuild {
            active_files
        } else {
            None
        };
        let started = std::time::Instant::now();
        let span = tracing::Span::current();
        let built = tokio::task::spawn_blocking(move || {
            let _entered = span.enter();
            lakesoul_io::session::GLOBAL_RUNTIME.block_on(
                crate::text_index::auto_build_text_index(
                    &text_configs,
                    &text_primary_keys,
                    &text_object_store_options,
                    &text_index_files,
                    active.as_deref(),
                    &text_client.index_catalog::<lakesoul_text::TextSplitEntry>(
                        lakesoul_common::IndexKind::Text,
                    ),
                ),
            )
        })
        .await
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "text index auto build task failed: {error}"
            ))
        })
        .and_then(|built| {
            built.map_err(|report| DataFusionError::External(report.into_boxed_error()))
        });
        record_index_build("text", started, &built);
        let built = built?;
        debug!(
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            "auto-built {built} text index shard(s) for {}", &table_name
        );
        tracing::Span::current().record("built_shard_count", built as u64);
        Ok::<usize, DataFusionError>(built)
    }
    .instrument(tracing::info_span!(
        "text_index_maintenance",
        config_count = text_config_count,
        rebuild = wants_text_rebuild,
        built_shard_count = tracing::field::Empty,
    ));
    let (vector_built, text_built) = tokio::join!(vector_build, text_build);
    vector_built?;
    text_built?;
    Ok(count)
}

/// Marks a statement's write as cancelled when its result stream is dropped.
///
/// DataFusion stops a query by dropping its stream; the commit runs in a
/// detached task (dropping a `JoinHandle` does not stop it). The detached task
/// holds the same flag and checks it before publishing, so a cancellation that
/// arrives while the files are written leaves no commit.
struct CancelOnDropStream {
    inner: SendableRecordBatchStream,
    cancelled: Arc<AtomicBool>,
}

impl CancelOnDropStream {
    fn new(inner: SendableRecordBatchStream, cancelled: Arc<AtomicBool>) -> Self {
        Self { inner, cancelled }
    }
}

impl datafusion::execution::RecordBatchStream for CancelOnDropStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl futures::Stream for CancelOnDropStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

impl Drop for CancelOnDropStream {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

fn make_sink_batch(count: u64, msg: String) -> RecordBatch {
    let count_array = Arc::new(UInt64Array::from(vec![count])) as ArrayRef;
    let msg_array = Arc::new(StringArray::from(vec![msg])) as ArrayRef;
    RecordBatch::try_from_iter_with_nullable(vec![
        ("count", count_array, false),
        ("msg", msg_array, false),
    ])
    .unwrap()
}

fn make_sink_schema() -> SchemaRef {
    // define a schema.
    Arc::new(Schema::new(vec![
        Field::new("count", DataType::UInt64, false),
        Field::new("msg", DataType::Utf8, false),
    ]))
}

#[cfg(test)]
#[path = "write_exec_tests.rs"]
mod tests;
