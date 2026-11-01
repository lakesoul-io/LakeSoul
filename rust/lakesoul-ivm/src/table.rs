// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Internal LakeSoul tables used by the IVM runtime.
//!
//! IVM tables are regular LakeSoul tables marked with
//! `lakesoul.ivm.internal=true`. Materialized view outputs additionally carry
//! the [`IVM_ROW_KINDS_COLUMN`] (`insert` / `delete`) and
//! [`IVM_EPOCH_COLUMN`] bookkeeping columns; a refresh writes the retraction
//! of a key followed by its new value in one commit, and the stable sort keeps
//! the two adjacent so merge-on-read resolves the key to the new value.
//!
//! Bucket columns may be a prefix of the merge key
//! (`lakesoul.ivm.bucket_columns`), which join state tables use to bucket by
//! the join key while merging on the full row identity.

use std::sync::Arc;
use std::time::Instant;

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use arrow::row::{RowConverter, SortField};
use arrow_array::Array;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::error::DataFusionError;
use datafusion::prelude::Expr;
use lakesoul_common::ser::arrow_java::{
    schema_from_table_info_metadata, schema_to_metadata_str,
};
use lakesoul_io::{
    config::{
        LakeSoulIOConfig, LakeSoulIOConfigBuilder, OPTION_KEY_POOL_DIR,
        OPTION_KEY_POOL_SIZE, OPTION_KEY_STABLE_SORT,
        OPTION_KEY_VORTEX_DATA_BLOCK_TARGET_BYTES, OPTION_KEY_VORTEX_ROW_BLOCK_SIZE,
    },
    file_format::PhysicalFormat,
    reader::LakeSoulReader,
    writer::{SendableWriter, async_writer::FlushOutput, create_writer_with_io_config},
};
use lakesoul_metadata::{MetaDataClient, transfusion::DataFileInfo};
use lakesoul_metadata_proto::entity::{CommitOp, MetaInfo, PartitionInfo, TableInfo};

use crate::error::Result;
use crate::metadata::PartitionVersion;
use crate::provider::IvmTableProvider;
/// The channel a streaming scan sends its batches through.
type BatchSender =
    tokio::sync::mpsc::Sender<std::result::Result<RecordBatch, DataFusionError>>;

/// The largest state write (in rows) a refresh coalesces before appending.
/// State batches above this size are split so one append stays bounded in
/// memory; the writer still emits one file per hash bucket per append.
pub(crate) const MAX_STATE_APPEND_ROWS: usize = 262_144;

/// The writer's target file size.  A bound here also makes the LakeSoul
/// writer split a large batch and spill the remainder instead of buffering the
/// whole append in memory.
pub(crate) const MAX_WRITE_FILE_SIZE: u64 = 64 * 1024 * 1024;

/// Rows one append coalesces before handing a batch to the writer.
///
/// A refresh can produce one-row batches: a union over per-file delta
/// partitions, or a sorted delete/insert stream.  Writing each batch as its
/// own Vortex block leaves a healthy MV/state with hundreds of thousands of
/// tiny batches after a single window, and every later key lookup then pays
/// the block overhead.  Coalescing at the sink keeps the write path's batch
/// granularity independent of the upstream plan's partitioning.
pub(crate) const APPEND_COALESCE_ROWS: usize = 8192;

/// Coalesce small refresh batches into writer-sized batches.
struct BatchCoalescer {
    schema: SchemaRef,
    target_rows: usize,
    pending: Vec<RecordBatch>,
    rows: usize,
}

impl BatchCoalescer {
    fn new(schema: SchemaRef, target_rows: usize) -> Self {
        Self {
            schema,
            target_rows: target_rows.max(1),
            pending: Vec::new(),
            rows: 0,
        }
    }

    /// Add one batch, returning a coalesced batch once the target is reached.
    fn push(&mut self, batch: RecordBatch) -> crate::error::Result<Option<RecordBatch>> {
        if batch.num_rows() == 0 {
            return Ok(None);
        }
        self.rows += batch.num_rows();
        self.pending.push(batch);
        if self.rows < self.target_rows {
            return Ok(None);
        }
        self.take().map(Some)
    }

    /// Flush the remaining rows at end of stream.
    fn finish(&mut self) -> crate::error::Result<Option<RecordBatch>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        self.take().map(Some)
    }

    fn take(&mut self) -> crate::error::Result<RecordBatch> {
        let pending = std::mem::take(&mut self.pending);
        self.rows = 0;
        match pending.len() {
            0 => Err(rootcause::report!(
                "coalesce refresh batches: empty pending"
            )),
            1 => Ok(pending.into_iter().next().expect("one pending batch")),
            _ => concat_batches(&self.schema, &pending)
                .map_err(|error| rootcause::report!("coalesce refresh batches: {error}")),
        }
    }
}

/// Wrap a refresh output stream with [`BatchCoalescer`].
///
/// The stream keeps the input order; only batch boundaries change.
fn coalesce_batch_stream<S>(
    stream: S,
    table: IvmTable,
    target_rows: usize,
) -> impl futures::Stream<Item = datafusion::error::Result<RecordBatch>> + Send
where
    S: futures::Stream<Item = datafusion::error::Result<RecordBatch>> + Send + Unpin,
{
    use futures::StreamExt;
    let coalescer = BatchCoalescer::new(table.schema.clone(), target_rows);
    futures::stream::try_unfold(
        (stream, coalescer, table),
        |(mut stream, mut coalescer, table)| async move {
            loop {
                match stream.next().await {
                    Some(Ok(batch)) => {
                        // Cast before concatenating: upstream batches may use
                        // Utf8View while the table schema stores Utf8.
                        let batch = table.cast_batch(batch).map_err(|error| {
                            DataFusionError::Execution(format!(
                                "coalesce refresh batches: {error:?}"
                            ))
                        })?;
                        let next = coalescer.push(batch).map_err(|error| {
                            DataFusionError::Execution(format!(
                                "coalesce refresh batches: {error:?}"
                            ))
                        })?;
                        if let Some(batch) = next {
                            return Ok(Some((batch, (stream, coalescer, table))));
                        }
                    }
                    Some(Err(error)) => return Err(error),
                    None => {
                        let next = coalescer.finish().map_err(|error| {
                            DataFusionError::Execution(format!(
                                "coalesce refresh batches: {error:?}"
                            ))
                        })?;
                        return Ok(next.map(|batch| (batch, (stream, coalescer, table))));
                    }
                }
            }
        },
    )
}

/// The in-memory multipart buffer used by the LakeSoul writer.
pub(crate) const WRITE_MEMORY_BUFFER: usize = 8 * 1024 * 1024;

/// Apply IO threads to a reader.  Readers stream batches and never spill, so
/// they do not get a private memory pool: a pool per reader multiplied the
/// refresh's memory by the number of open buckets.  An explicit
/// `LAKESOUL_IVM_READ_POOL_SIZE` still opts into the old bounded-reader
/// behavior for deployments that rely on it.
fn with_reader_io(builder: LakeSoulIOConfigBuilder) -> LakeSoulIOConfigBuilder {
    let threads = std::env::var("LAKESOUL_IVM_IO_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4);
    let builder = builder.with_thread_num(threads);
    let Ok(size) = std::env::var("LAKESOUL_IVM_READ_POOL_SIZE") else {
        return builder;
    };
    let directory = std::env::var("LAKESOUL_IVM_SPILL_DIR")
        .unwrap_or_else(|_| "/tmp/lakesoul/ivm-spill".to_string());
    let _ = std::fs::create_dir_all(&directory);
    tracing::debug!(size, threads, %directory, "IVM reader spill pool configured");
    builder
        .with_option(OPTION_KEY_POOL_SIZE, size)
        .with_option(OPTION_KEY_POOL_DIR, directory)
}

/// Give a LakeSoul writer its own bounded memory pool and spill directory.
/// DataFusion's session limit does not automatically apply to the IO layer:
/// its sort and merge operators create a separate runtime.
fn with_spill_pool(
    builder: LakeSoulIOConfigBuilder,
    size_env: &str,
    default_size: &str,
) -> LakeSoulIOConfigBuilder {
    let size = std::env::var(size_env).unwrap_or_else(|_| default_size.to_string());
    let threads = std::env::var("LAKESOUL_IVM_IO_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4);
    let directory = std::env::var("LAKESOUL_IVM_SPILL_DIR")
        .unwrap_or_else(|_| "/tmp/lakesoul/ivm-spill".to_string());
    let _ = std::fs::create_dir_all(&directory);
    tracing::debug!(
        size_env,
        size,
        threads,
        %directory,
        "IVM IO spill pool configured"
    );
    builder
        .with_thread_num(threads)
        .with_option(OPTION_KEY_POOL_SIZE, size)
        .with_option(OPTION_KEY_POOL_DIR, directory)
}

/// The largest affected-key set a key-filtered state read resolves through the
/// row-level pk index.  A refresh's candidate set is the change ratio times
/// the view size (hundreds of thousands of keys at 1%–5% of a 10M view), far
/// above the point-lookup default of the locator; above this the read falls
/// back to a full scan.
pub(crate) const MAX_STATE_KEY_CANDIDATES: usize = 1_000_000;

/// Let a key-filtered state read resolve candidate rows through the row-level
/// pk index.
///
/// Enabled for Vortex tables: the writer gives every column a 1024-row block,
/// so a scattered candidate read touches a small block per matched row instead
/// of a whole Parquet page.  Measured on the 10M star (join step p50):
/// Vortex + index 402–421 ms at 1% change vs 756–1258 ms for the Parquet
/// key-filtered scan, and 182–204 ms vs 367–422 ms at 0.01%.
///
/// Parquet stays off by default: a page holds tens of thousands of rows, so at
/// 0.1%–1% almost every page contains a key and the index cannot skip data —
/// measured slower than the plain scan (639–674 ms vs 367–422 ms at 10M/0.01%).
/// `LAKESOUL_IVM_STATE_INDEX=1` forces it on for any format (clustered key sets
/// and point lookups can still benefit on Parquet).  The index needs the local
/// disk cache (`LAKESOUL_CACHE`); without it the read falls back to a scan.
fn with_state_key_lookup(
    builder: lakesoul_io::config::LakeSoulIOConfigBuilder,
    file_format: PhysicalFormat,
) -> lakesoul_io::config::LakeSoulIOConfigBuilder {
    let configured = std::env::var("LAKESOUL_IVM_STATE_INDEX").ok();
    if matches!(
        configured.as_deref(),
        Some("0") | Some("false") | Some("off")
    ) {
        return builder;
    }
    let vortex = !matches!(file_format, PhysicalFormat::Parquet);
    if !vortex && configured.is_none() {
        return builder;
    }
    builder
        .with_max_pk_candidates(MAX_STATE_KEY_CANDIDATES)
        .with_bulk_pk_lookup(true)
}

/// The physical format of every IVM-internal table.
///
/// Internal state and derived steps are read by point lookups far more often
/// than by full scans, so they use Vortex even when the user target/source
/// tables are Parquet.  Vortex's small row blocks let the row-level primary
/// key locator skip most of a file; `LAKESOUL_IVM_INTERNAL_FILE_FORMAT` can
/// still select parquet or vortex-compact for experiments.
pub fn internal_file_format() -> PhysicalFormat {
    match std::env::var("LAKESOUL_IVM_INTERNAL_FILE_FORMAT")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "parquet" => PhysicalFormat::Parquet,
        "vortex-compact" | "vortex_compact" => PhysicalFormat::VortexCompact,
        _ => PhysicalFormat::Vortex,
    }
}

/// The format of keyed auxiliary state tables.  IVM keeps these on Vortex as
/// well: the row-level key index is the point-lookup path for join, aggregate
/// and top-k state.
pub fn keyed_state_file_format() -> PhysicalFormat {
    internal_file_format()
}

/// The row kind column of materialized view outputs (`insert` / `delete`).
pub const IVM_ROW_KINDS_COLUMN: &str = "rowKinds";
/// The epoch a materialized view row was written in.
pub const IVM_EPOCH_COLUMN: &str = "__ivm_epoch";

/// A handle to an internal LakeSoul table.
#[derive(Debug, Clone)]
pub struct IvmTable {
    /// The LakeSoul table id.
    pub table_id: String,
    /// The LakeSoul table name.
    pub table_name: String,
    /// The namespace the table lives in.
    pub namespace: String,
    /// The table root path.
    pub table_path: String,
    /// The Arrow schema of the table.
    pub schema: SchemaRef,
    /// The merge key of the table.
    pub primary_keys: Vec<String>,
    /// The range partition columns of the table (empty when unpartitioned).
    /// Partition columns are part of [`IvmTable::schema`]; the data files do
    /// not contain them and reads inject their values from the
    /// `partition_desc`.
    pub range_partition_columns: Vec<String>,
    /// The columns the writer buckets on; empty means the merge key.
    pub bucket_columns: Vec<String>,
    /// The number of hash buckets.
    pub hash_bucket_num: String,
    /// The CDC change column of the table (`insert` / `delete` values), when
    /// the table carries one.
    pub cdc_column: Option<String>,
    /// The physical file format of the table.
    pub file_format: PhysicalFormat,
    /// Whether a statement shape the incremental analyzer cannot maintain
    /// falls back to a full recompute (Tier-2) instead of being rejected.
    /// Persisted as the `lakesoul.ivm.allow_fallback` table property and true
    /// by default.
    pub allow_fallback: bool,
}

/// The data files of one storage partition of a table.
#[derive(Debug, Clone)]
pub struct PartitionFiles {
    /// The `partition_desc` of the group (the LakeSoul default `-5` for an
    /// unpartitioned group).
    pub partition_desc: String,
    /// The data file paths.
    pub files: Vec<String>,
}

/// The hash bucket encoded in a LakeSoul data file name.
pub(crate) fn file_bucket(path: &str) -> Option<u32> {
    lakesoul_io::helpers::extract_hash_bucket_id(path)
}

/// Split a comma separated partition key list.
fn split_partition_keys(keys: &str) -> Vec<String> {
    keys.split(',')
        .filter(|key| !key.is_empty())
        .map(str::to_string)
        .collect()
}

/// Parse the range partition values of a `partition_desc` against `columns`.
///
/// Mirrors the DataFusion provider: the desc must list `column=value` pairs in
/// the table column order, otherwise `None` is returned and the read leaves the
/// partition columns null.
pub(crate) fn parse_partition_values(
    partition_desc: &str,
    columns: &[String],
) -> Option<Vec<String>> {
    let mut values = Vec::new();
    for (part, column) in partition_desc.split(',').zip(columns) {
        match part.split_once('=') {
            Some((name, value)) if name == column => values.push(value.to_string()),
            _ => return None,
        }
    }
    (values.len() == columns.len()).then_some(values)
}

/// Whether a partition (by its descriptor) satisfies a value restriction
/// `(column, allowed values)`.  A column outside the table's range columns and
/// a descriptor that cannot be parsed both match: the read keeps such
/// partitions rather than dropping rows.
pub(crate) fn partition_matches(
    partition_desc: &str,
    range_columns: &[String],
    restriction: &[(String, Vec<String>)],
) -> bool {
    let Some(values) = parse_partition_values(partition_desc, range_columns) else {
        return true;
    };
    restriction.iter().all(|(column, allowed)| {
        match range_columns
            .iter()
            .position(|candidate| candidate == column)
            .and_then(|index| values.get(index))
        {
            Some(value) => allowed.iter().any(|allowed| allowed == value),
            None => true,
        }
    })
}

/// The source range partitions a state table may mirror.
///
/// A state key must carry the partition columns, otherwise a source row that
/// moves between partitions would leave its old state behind under the same
/// key.  Returning an empty list keeps the state unpartitioned (the safe
/// fallback).
pub(crate) fn state_range_partition_columns(
    source: &IvmTable,
    keys: &[String],
) -> Vec<String> {
    if !source.range_partition_columns.is_empty()
        && source
            .range_partition_columns
            .iter()
            .all(|column| keys.contains(column))
    {
        source.range_partition_columns.clone()
    } else {
        Vec::new()
    }
}

/// Options for [`create_ivm_table`].
#[derive(Debug, Clone)]
pub struct IvmTableOptions {
    /// The table name.
    pub name: String,
    /// The namespace the table lives in.
    pub namespace: String,
    /// The table root path.
    pub table_path: String,
    /// The Arrow schema of the table.
    pub schema: SchemaRef,
    /// The merge key of the table; empty for append-only tables.
    pub primary_keys: Vec<String>,
    /// The range partition columns of the table; they must be part of the
    /// schema and are stripped from the data files.
    pub range_partition_columns: Vec<String>,
    /// The bucket columns; must be a prefix of `primary_keys`.
    pub bucket_columns: Vec<String>,
    /// The number of hash buckets.
    pub hash_bucket_num: String,
    /// The CDC change column of the table, persisted as the
    /// `lakesoul_cdc_change_column` table property.
    pub cdc_column: Option<String>,
    /// The physical file format of the table (parquet by default; vortex
    /// enables the row-level primary-key locator).
    pub file_format: PhysicalFormat,
    /// Whether an unsupported statement shape falls back to a full recompute
    /// (Tier-2) instead of being rejected.
    pub allow_fallback: bool,
}

impl IvmTableOptions {
    /// New options with a single hash bucket and no keys.
    pub fn new(
        name: impl Into<String>,
        table_path: impl Into<String>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            name: name.into(),
            namespace: "default".to_string(),
            table_path: table_path.into(),
            schema,
            primary_keys: Vec::new(),
            range_partition_columns: Vec::new(),
            bucket_columns: Vec::new(),
            hash_bucket_num: "4".to_string(),
            cdc_column: None,
            file_format: PhysicalFormat::Parquet,
            allow_fallback: true,
        }
    }

    /// Set the merge key.
    pub fn with_primary_keys(mut self, primary_keys: Vec<String>) -> Self {
        self.primary_keys = primary_keys;
        self
    }

    /// Set the range partition columns.
    pub fn with_range_partition_columns(
        mut self,
        range_partition_columns: Vec<String>,
    ) -> Self {
        self.range_partition_columns = range_partition_columns;
        self
    }

    /// Set the bucket columns.
    pub fn with_bucket_columns(mut self, bucket_columns: Vec<String>) -> Self {
        self.bucket_columns = bucket_columns;
        self
    }

    /// Set the namespace.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Set the CDC change column.
    pub fn with_cdc_column(mut self, cdc_column: impl Into<String>) -> Self {
        self.cdc_column = Some(cdc_column.into());
        self
    }

    /// Set the physical file format (parquet by default).
    pub fn with_file_format(mut self, file_format: PhysicalFormat) -> Self {
        self.file_format = file_format;
        self
    }

    /// Disable the Tier-2 full-recompute fallback for the table: an
    /// unsupported statement shape is then rejected as before.
    pub fn with_allow_fallback(mut self, allow_fallback: bool) -> Self {
        self.allow_fallback = allow_fallback;
        self
    }
}

/// Normalize Arrow view types to the physical column types LakeSoul stores.
///
/// DataFusion plans string functions as `Utf8View`; LakeSoul metadata
/// serializes that as `Utf8`, so a table created from the plan's schema would
/// advertise `Utf8View` in memory and `Utf8` after reopening.  Normalize when
/// a table is created so both views agree.
pub(crate) fn normalize_data_type(
    data_type: &arrow_schema::DataType,
) -> arrow_schema::DataType {
    match data_type {
        arrow_schema::DataType::Utf8View => arrow_schema::DataType::Utf8,
        arrow_schema::DataType::BinaryView => arrow_schema::DataType::Binary,
        arrow_schema::DataType::List(field) => arrow_schema::DataType::List(Arc::new(
            field
                .as_ref()
                .clone()
                .with_data_type(normalize_data_type(field.data_type())),
        )),
        arrow_schema::DataType::LargeList(field) => {
            arrow_schema::DataType::LargeList(Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(normalize_data_type(field.data_type())),
            ))
        }
        arrow_schema::DataType::Struct(fields) => arrow_schema::DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_data_type(normalize_data_type(field.data_type())),
                    )
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

/// The physical schema LakeSoul stores for an IVM table.
pub(crate) fn normalize_schema(schema: &Schema) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            Arc::new(
                Field::new(
                    field.name(),
                    normalize_data_type(field.data_type()),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone()),
            ) as arrow_schema::FieldRef
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Create an internal LakeSoul table.
pub async fn create_ivm_table(
    client: &MetaDataClient,
    options: IvmTableOptions,
) -> Result<IvmTable> {
    let options = IvmTableOptions {
        schema: normalize_schema(&options.schema),
        ..options
    };
    let mut properties = serde_json::json!({
        "hashBucketNum": options.hash_bucket_num,
        "file_format": options.file_format.name(),
        "lakesoul.ivm.internal": "true",
    });
    if !options.bucket_columns.is_empty() {
        properties["lakesoul.ivm.bucket_columns"] =
            serde_json::Value::String(options.bucket_columns.join(","));
    }
    properties["lakesoul.ivm.allow_fallback"] =
        serde_json::Value::Bool(options.allow_fallback);
    if let Some(cdc_column) = &options.cdc_column {
        if options.schema.field_with_name(cdc_column).is_err() {
            return Err(rootcause::report!(
                "cdc column {cdc_column:?} is not part of the table schema"
            ));
        }
        properties["lakesoul_cdc_change_column"] =
            serde_json::Value::String(cdc_column.clone());
    }

    for (index, column) in options.range_partition_columns.iter().enumerate() {
        if options.range_partition_columns[index + 1..].contains(column) {
            return Err(rootcause::report!(
                "range partition column {column:?} is listed twice"
            ));
        }
        if options.schema.field_with_name(column).is_err() {
            return Err(rootcause::report!(
                "range partition column {column:?} is not part of the table schema"
            ));
        }
    }

    let table_id = format!("table_{}", uuid::Uuid::new_v4().simple());
    client
        .create_table(TableInfo {
            table_id: table_id.clone(),
            table_name: options.name.clone(),
            table_namespace: options.namespace.clone(),
            table_path: options.table_path.clone(),
            table_schema: schema_to_metadata_str(&options.schema),
            table_schema_arrow_ipc: Vec::new(),
            table_schema_arrow_ipc_json_hash: String::new(),
            properties: properties.to_string(),
            partitions: format!(
                "{};{}",
                options.range_partition_columns.join(","),
                options.primary_keys.join(",")
            ),
            domain: "public".to_string(),
        })
        .await?;

    Ok(IvmTable {
        table_id,
        table_name: options.name,
        namespace: options.namespace,
        table_path: options.table_path,
        schema: options.schema,
        primary_keys: options.primary_keys,
        range_partition_columns: options.range_partition_columns,
        bucket_columns: options.bucket_columns,
        hash_bucket_num: options.hash_bucket_num,
        cdc_column: options.cdc_column,
        file_format: options.file_format,
        allow_fallback: options.allow_fallback,
    })
}

impl IvmTable {
    /// The columns whose hash determines the file bucket.
    ///
    /// The writer falls back to the primary keys when no dedicated bucket
    /// columns are set.  Tables created before the bucket columns were
    /// persisted therefore still have their files laid out by primary key;
    /// routing must use the same fallback or every lookup scans every bucket.
    pub fn effective_bucket_columns(&self) -> &[String] {
        if self.bucket_columns.is_empty() {
            &self.primary_keys
        } else {
            &self.bucket_columns
        }
    }

    /// Open an existing LakeSoul table (internal or user-created) from its
    /// metadata.  Missing IVM properties fall back to their defaults, so a
    /// table created by plain DDL can be opened as well.
    pub fn from_table_info(info: &TableInfo) -> Result<Self> {
        let schema = schema_from_table_info_metadata(
            &info.table_schema,
            &info.table_schema_arrow_ipc,
            &info.table_schema_arrow_ipc_json_hash,
        )
        .map_err(|error| {
            rootcause::report!(
                "parse schema of table {}.{}: {error}",
                info.table_namespace,
                info.table_name
            )
        })?;
        let properties: serde_json::Value =
            serde_json::from_str(&info.properties).unwrap_or(serde_json::Value::Null);
        let (range_partition_columns, primary_keys) = info
            .partitions
            .split_once(';')
            .map(|(range, hash)| {
                (split_partition_keys(range), split_partition_keys(hash))
            })
            .unwrap_or_default();
        for column in &range_partition_columns {
            if schema.field_with_name(column).is_err() {
                return Err(rootcause::report!(
                    "range partition column {column:?} of table {}.{} is not part of the table schema",
                    info.table_namespace,
                    info.table_name
                ));
            }
        }
        let bucket_columns = properties
            .get("lakesoul.ivm.bucket_columns")
            .and_then(|value| value.as_str())
            .map(|columns| {
                columns
                    .split(',')
                    .filter(|column| !column.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let hash_bucket_num = properties
            .get("hashBucketNum")
            .and_then(|value| {
                value
                    .as_str()
                    .map(str::to_string)
                    .or_else(|| value.as_i64().map(|number| number.to_string()))
            })
            .unwrap_or_else(|| "1".to_string());
        let cdc_column = properties
            .get("lakesoul_cdc_change_column")
            .and_then(|value| value.as_str())
            .map(str::to_string);
        let file_format = properties
            .get("file_format")
            .and_then(|value| value.as_str())
            .and_then(|name| name.parse::<PhysicalFormat>().ok())
            .unwrap_or_default();
        let allow_fallback = properties
            .get("lakesoul.ivm.allow_fallback")
            .and_then(|value| value.as_bool())
            .unwrap_or(true);
        Ok(Self {
            table_id: info.table_id.clone(),
            table_name: info.table_name.clone(),
            namespace: info.table_namespace.clone(),
            table_path: info.table_path.clone(),
            schema: Arc::new(schema),
            primary_keys,
            range_partition_columns,
            bucket_columns,
            hash_bucket_num,
            cdc_column,
            file_format,
            allow_fallback,
        })
    }

    /// Write one batch and commit the produced files.
    ///
    /// Keyed tables write through the partitioning writer with stable sort, so
    /// rows with the same key keep their input order (a `delete` written before
    /// an `insert` still wins in merge-on-read).
    /// Returns the LakeSoul commit ids created by the write (empty when the
    /// batch has no rows or the writer produced no files).
    pub async fn append_batch(
        &self,
        client: &MetaDataClient,
        batch: RecordBatch,
    ) -> Result<Vec<String>> {
        let files = self.write_batch(batch).await?;
        self.commit_files(client, files).await
    }

    fn writer_builder(&self) -> LakeSoulIOConfigBuilder {
        let mut builder = with_spill_pool(
            LakeSoulIOConfig::builder()
                .with_prefix(self.table_path.clone())
                .with_schema(self.schema.clone())
                .with_primary_keys(self.primary_keys.clone())
                .with_hash_partitioning_columns(self.bucket_columns.clone())
                .with_hash_bucket_num(self.hash_bucket_num.clone())
                .with_physical_format(self.file_format)
                .with_max_file_size(MAX_WRITE_FILE_SIZE)
                .with_memory_buffer_capacity(WRITE_MEMORY_BUFFER),
            "LAKESOUL_IVM_WRITE_POOL_SIZE",
            "500mb",
        );
        if !self.range_partition_columns.is_empty() {
            builder = builder.with_range_partitions(self.range_partition_columns.clone());
        }
        if !self.primary_keys.is_empty() {
            builder = builder
                .set_dynamic_partition(true)
                .with_option(OPTION_KEY_STABLE_SORT, "true");
        }
        if matches!(
            self.file_format,
            PhysicalFormat::Vortex | PhysicalFormat::VortexCompact
        ) {
            if let Ok(value) = std::env::var("LAKESOUL_IVM_VORTEX_ROW_BLOCK_SIZE") {
                builder = builder.with_option(OPTION_KEY_VORTEX_ROW_BLOCK_SIZE, value);
            }
            if let Ok(value) = std::env::var("LAKESOUL_IVM_VORTEX_DATA_BLOCK_BYTES") {
                builder =
                    builder.with_option(OPTION_KEY_VORTEX_DATA_BLOCK_TARGET_BYTES, value);
            }
        }
        builder
    }

    /// Create one rolling writer for a refresh/compaction output.
    pub(crate) async fn writer(&self) -> Result<SendableWriter> {
        create_writer_with_io_config(self.writer_builder().build()).await
    }

    /// A clone of this table with a projected schema, for an auxiliary state
    /// that only needs the primary key, join keys, change marker and range
    /// partition columns.  Reads through the clone push the projection into the
    /// native reader instead of materializing every source column.
    pub(crate) fn projected_state_source(&self, extra: &[String]) -> Result<Self> {
        let mut names = std::collections::HashSet::new();
        for name in self
            .primary_keys
            .iter()
            .chain(extra)
            .chain(self.range_partition_columns.iter())
            .chain(self.bucket_columns.iter())
        {
            names.insert(name.clone());
        }
        if let Some(column) = &self.cdc_column {
            names.insert(column.clone());
        }
        for column in [IVM_ROW_KINDS_COLUMN, IVM_EPOCH_COLUMN] {
            if self.schema.field_with_name(column).is_ok() {
                names.insert(column.to_string());
            }
        }
        let fields = self
            .schema
            .fields()
            .iter()
            .filter(|field| names.contains(field.name().as_str()))
            .map(|field| Arc::new(field.as_ref().clone()))
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(fields));
        let mut projected = self.clone();
        projected.schema = schema;
        Ok(projected)
    }

    /// Convert writer outputs to metadata file entries and record metrics.
    pub(crate) fn output_files(
        &self,
        outputs: Vec<FlushOutput>,
        rows: u64,
    ) -> Vec<DataFileInfo> {
        let written_bytes = outputs
            .iter()
            .map(|output| output.object_meta.size)
            .sum::<u64>();
        let modification_time = crate::now_ms();
        let files = outputs
            .into_iter()
            .map(|output| DataFileInfo {
                partition_desc: output.partition_desc,
                path: output.file_path,
                file_op: "add".to_string(),
                size: output.object_meta.size as i64,
                bucket_id: None,
                modification_time,
                file_exist_cols: output.file_exist_cols.join(","),
            })
            .collect::<Vec<_>>();
        crate::observability::add_rows_written(rows);
        crate::observability::add_bytes_written(written_bytes);
        files
    }

    /// Write one batch to new data files without publishing them.
    ///
    /// Keyed tables write through the partitioning writer with stable sort, so
    /// rows with the same key keep their input order (a `delete` written before
    /// an `insert` still wins in merge-on-read).  The returned files can be
    /// committed by a coordinator with [`Self::commit_files`], which is what a
    /// distributed refresh does: workers write, the driver publishes once.
    pub async fn write_batch(&self, batch: RecordBatch) -> Result<Vec<DataFileInfo>> {
        if batch.num_rows() == 0 {
            return Ok(Vec::new());
        }
        let batch = self.cast_batch(batch)?;
        let rows = batch.num_rows() as u64;
        let mut writer = self.writer().await?;
        writer.write_record_batch(batch).await?;
        let outputs = writer.flush_and_close().await?;
        Ok(self.output_files(outputs, rows))
    }

    /// Cast a refresh batch to the table's physical schema.  DataFusion plans
    /// string expressions as `Utf8View`; LakeSoul stores `Utf8`, so the
    /// writer must see the table's canonical types.
    pub(crate) fn cast_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if batch.schema().fields().len() != self.schema.fields().len() {
            return Err(rootcause::report!(
                "refresh batch has {} columns, table {} expects {}",
                batch.schema().fields().len(),
                self.table_name,
                self.schema.fields().len()
            ));
        }
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (array, field) in batch.columns().iter().zip(self.schema.fields()) {
            if array.data_type() == field.data_type() {
                columns.push(array.clone());
            } else {
                columns.push(arrow::compute::cast(array, field.data_type()).map_err(
                    |error| {
                        rootcause::report!(
                            "cast refresh column {} to {}: {error}",
                            field.name(),
                            field.data_type()
                        )
                    },
                )?);
            }
        }
        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|error| rootcause::report!("refresh batch schema: {error}"))
    }

    /// Publish already written files in one metadata commit.
    pub async fn commit_files(
        &self,
        client: &MetaDataClient,
        files: Vec<DataFileInfo>,
    ) -> Result<Vec<String>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let commit_ids = client
            .commit_data_files(&self.table_name, &self.namespace, files)
            .await?;
        Ok(commit_ids)
    }

    /// Append refresh output as few state files as possible.
    ///
    /// A SQL refresh can emit many small record batches (the executor's
    /// batch size).  One rolling writer consumes the batches and flushes a
    /// bounded sort/write window into files, so a large bootstrap does not
    /// need to hold or sort the whole result in one writer.
    pub async fn append_batches(
        &self,
        client: &MetaDataClient,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<String>> {
        let files = self.write_batches(batches).await?;
        self.commit_files(client, files).await
    }

    /// Publish a refresh output stream without collecting the whole result.
    ///
    /// The stream runs the DataFusion plan; batches are written through one
    /// rolling writer so refresh memory stays independent of the MV/state
    /// size.
    pub async fn append_stream<S>(
        &self,
        client: &MetaDataClient,
        stream: S,
    ) -> Result<Vec<String>>
    where
        S: futures::Stream<Item = datafusion::error::Result<RecordBatch>> + Send + Unpin,
    {
        use futures::StreamExt;
        let mut stream = Box::pin(coalesce_batch_stream(
            stream,
            self.clone(),
            APPEND_COALESCE_ROWS,
        ));
        let mut writer = self.writer().await?;
        let mut rows = 0u64;
        while let Some(batch) = stream.next().await {
            let batch = batch
                .map_err(|error| rootcause::report!("refresh output batch: {error}"))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = self.cast_batch(batch)?;
            rows += batch.num_rows() as u64;
            let started = Instant::now();
            writer.write_record_batch(batch).await?;
            crate::observability::add_write_time(started.elapsed());
        }
        let started = Instant::now();
        let outputs = writer.flush_and_close().await?;
        crate::observability::add_write_time(started.elapsed());
        let files = self.output_files(outputs, rows);
        self.commit_files(client, files).await
    }

    /// Publish one refresh output stream to this table and a mirror table.
    ///
    /// A top-k refresh writes the same delete/insert frame to its MV and to
    /// the mirror state.  Pull the source stream once, coalesce it once, and
    /// hand each batch to both writers; cloning a `RecordBatch` only clones
    /// its `Arc` buffers.  Memory stays at one writer batch instead of the
    /// whole result set.
    pub(crate) async fn append_stream_pair<S>(
        &self,
        client: &MetaDataClient,
        stream: S,
        other: &IvmTable,
    ) -> Result<(Vec<String>, Vec<String>)>
    where
        S: futures::Stream<Item = datafusion::error::Result<RecordBatch>> + Send + Unpin,
    {
        use futures::StreamExt;
        let mut stream = Box::pin(coalesce_batch_stream(
            stream,
            self.clone(),
            APPEND_COALESCE_ROWS,
        ));
        let mut writer = self.writer().await?;
        let mut other_writer = other.writer().await?;
        let mut rows = 0u64;
        while let Some(batch) = stream.next().await {
            let batch = batch
                .map_err(|error| rootcause::report!("refresh output batch: {error}"))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let mine = self.cast_batch(batch.clone())?;
            let theirs = other.cast_batch(batch)?;
            rows += mine.num_rows() as u64;
            let started = Instant::now();
            writer.write_record_batch(mine).await?;
            crate::observability::add_write_time(started.elapsed());
            let started = Instant::now();
            other_writer.write_record_batch(theirs).await?;
            crate::observability::add_write_time(started.elapsed());
        }
        let started = Instant::now();
        let outputs = writer.flush_and_close().await?;
        let other_outputs = other_writer.flush_and_close().await?;
        crate::observability::add_write_time(started.elapsed());
        let files = self.output_files(outputs, rows);
        let other_files = other.output_files(other_outputs, rows);
        let ids = self.commit_files(client, files).await?;
        let other_ids = other.commit_files(client, other_files).await?;
        Ok((ids, other_ids))
    }

    /// Publish a stream that is sorted by the table's primary keys, keeping
    /// all rows of one key in the same writer batch.
    ///
    /// A target sink writes a delete and an insert for the same update.  If a
    /// DataFusion batch boundary splits those two rows, a rolling writer can
    /// put them in different files, and merge-on-read then resolves the same
    /// key ambiguously.  Buffering only the trailing key group of each batch
    /// keeps the pair together without collecting the whole delta.
    pub(crate) async fn append_stream_by_key<S>(
        &self,
        client: &MetaDataClient,
        stream: S,
    ) -> Result<Vec<String>>
    where
        S: futures::Stream<Item = datafusion::error::Result<RecordBatch>> + Send + Unpin,
    {
        use futures::StreamExt;
        if self.primary_keys.is_empty() {
            return self.append_stream(client, stream).await;
        }
        let mut stream = Box::pin(coalesce_batch_stream(
            stream,
            self.clone(),
            APPEND_COALESCE_ROWS,
        ));
        let key_indices = self
            .primary_keys
            .iter()
            .map(|key| {
                self.schema.index_of(key).map_err(|error| {
                    rootcause::report!("primary key {key} is missing: {error}")
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let sort_fields = self
            .primary_keys
            .iter()
            .map(|key| {
                let field = self.schema.field_with_name(key)?;
                Ok(SortField::new(field.data_type().clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        let converter = RowConverter::new(sort_fields)
            .map_err(|error| rootcause::report!("key row converter: {error}"))?;

        let mut writer = self.writer().await?;
        let mut rows = 0u64;
        let mut carry: Option<RecordBatch> = None;
        while let Some(batch) = stream.next().await {
            let batch = batch
                .map_err(|error| rootcause::report!("refresh output batch: {error}"))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = self.cast_batch(batch)?;
            rows += batch.num_rows() as u64;
            let batch = match carry.take() {
                Some(previous) => {
                    let previous_key =
                        boundary_key(&converter, &previous, &key_indices, false)?;
                    let next_key = boundary_key(&converter, &batch, &key_indices, true)?;
                    if previous_key == next_key {
                        concat_batches(&self.schema, &[previous, batch]).map_err(
                            |error| rootcause::report!("merge key batch: {error}"),
                        )?
                    } else {
                        let started = Instant::now();
                        writer.write_record_batch(previous).await?;
                        crate::observability::add_write_time(started.elapsed());
                        batch
                    }
                }
                None => batch,
            };
            let last_start = last_key_group_start(&converter, &batch, &key_indices)?;
            if last_start > 0 {
                let started = Instant::now();
                writer
                    .write_record_batch(batch.slice(0, last_start))
                    .await?;
                crate::observability::add_write_time(started.elapsed());
            }
            carry = Some(batch.slice(last_start, batch.num_rows() - last_start));
        }
        if let Some(carry) = carry {
            let started = Instant::now();
            writer.write_record_batch(carry).await?;
            crate::observability::add_write_time(started.elapsed());
        }
        let started = Instant::now();
        let outputs = writer.flush_and_close().await?;
        crate::observability::add_write_time(started.elapsed());
        let files = self.output_files(outputs, rows);
        self.commit_files(client, files).await
    }

    /// Write refresh output through one rolling writer, without publishing.
    pub async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<DataFileInfo>> {
        let mut rows = 0u64;
        let mut writer = self.writer().await?;
        let mut coalescer =
            BatchCoalescer::new(self.schema.clone(), APPEND_COALESCE_ROWS);
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = self.cast_batch(batch)?;
            rows += batch.num_rows() as u64;
            if let Some(batch) = coalescer.push(batch)? {
                writer.write_record_batch(batch).await?;
            }
        }
        if let Some(batch) = coalescer.finish()? {
            writer.write_record_batch(batch).await?;
        }
        let outputs = writer.flush_and_close().await?;
        Ok(self.output_files(outputs, rows))
    }

    /// The current data file count across all partitions.
    pub async fn current_file_count(&self, client: &MetaDataClient) -> Result<usize> {
        Ok(self.current_files(client).await?.len())
    }

    /// Rewrite the current logical state into a fresh file set and publish it
    /// as one compaction commit.
    ///
    /// CDC tombstones are dropped and the rows written by this compaction are
    /// marked as old state (`rowKinds = insert`, epoch `0`), so a later refresh
    /// treats them exactly like rows written by an earlier window.  The commit
    /// replaces the partition snapshot, so the old files stop being read and
    /// are reclaimed by the table's retention cleanup.
    pub async fn compact(&self, client: &MetaDataClient) -> Result<Vec<String>> {
        use futures::StreamExt;

        // Read current state through the streaming provider: a single large
        // `read_current()` would materialize the entire table before the
        // writer even starts.  Execute all hash/range partitions so this is
        // not accidentally limited to the first bucket.
        let context = crate::runtime::refresh_session();
        let provider = IvmTableProvider::current(self.clone(), client.clone());
        let frame = context.read_table(Arc::new(provider))?;
        let streams = frame.execute_stream_partitioned().await?;
        if streams.is_empty() {
            self.truncate(client).await?;
            return Ok(Vec::new());
        }
        let kinds = self.schema.index_of(IVM_ROW_KINDS_COLUMN).ok();
        let epoch = self.schema.index_of(IVM_EPOCH_COLUMN).ok();
        let mut writer = self.writer().await?;
        let mut written_rows = 0u64;
        for mut stream in streams {
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let mut columns = batch.columns().to_vec();
                if let Some(index) = kinds {
                    columns[index] =
                        Arc::new(arrow::array::StringArray::from_iter_values(
                            (0..batch.num_rows()).map(|_| "insert"),
                        ));
                }
                if let Some(index) = epoch {
                    columns[index] =
                        Arc::new(arrow::array::Int64Array::from_iter_values(
                            (0..batch.num_rows()).map(|_| 0i64),
                        ));
                }
                written_rows += batch.num_rows() as u64;
                writer
                    .write_record_batch(RecordBatch::try_new(
                        self.schema.clone(),
                        columns,
                    )?)
                    .await?;
            }
        }
        let outputs = writer.flush_and_close().await?;
        let files = self.output_files(outputs, written_rows);
        if files.is_empty() {
            self.truncate(client).await?;
            return Ok(Vec::new());
        }
        let read_partitions = client.get_all_partition_info(&self.table_id).await?;
        let commit_ids = client
            .commit_compaction(&self.table_name, &self.namespace, files, read_partitions)
            .await?;
        Ok(commit_ids)
    }

    /// Read the given data files with this table's schema and merge key.
    pub async fn read_files(&self, files: Vec<String>) -> Result<Vec<RecordBatch>> {
        self.read_files_with_options(files, Vec::new(), None).await
    }

    /// Read explicit data files with pushed filters (no projection).
    pub async fn read_files_filtered(
        &self,
        files: Vec<String>,
        filters: Vec<Expr>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_files_with_options(files, filters, None).await
    }

    /// The current data files of an unpartitioned table.
    pub async fn current_files(&self, client: &MetaDataClient) -> Result<Vec<String>> {
        let mut files = Vec::new();
        for partition in client.get_all_partition_info(&self.table_id).await? {
            files.extend(
                client
                    .get_data_files_of_single_partition(&partition)
                    .await?,
            );
        }
        Ok(files)
    }

    /// Read the given data files projected to `projection`.
    pub async fn read_files_projected(
        &self,
        files: Vec<String>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_files_with_options(files, Vec::new(), projection)
            .await
    }

    /// Read the given partition groups with this table's schema and merge key.
    ///
    /// Each group is read under its own range partition values (injected as
    /// constants, exactly like the DataFusion provider); merge-on-read runs
    /// within a group, so a key must not span partitions (LakeSoul semantics).
    pub async fn read_partition_files(
        &self,
        groups: Vec<PartitionFiles>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_groups_with_options(groups, Vec::new(), None)
            .await
    }

    /// Read the given partition groups projected to `projection`.
    pub async fn read_partition_files_projected(
        &self,
        groups: Vec<PartitionFiles>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_groups_with_options(groups, Vec::new(), projection)
            .await
    }

    /// Read explicit partition groups with pushed primary-key filters.
    ///
    /// The keyed-state physical plans use this to resolve one bucket's state
    /// rows for the keys of one contribution batch without materializing the
    /// whole state table.
    /// Stream one bucketed state read through the LakeSoul reader.  The
    /// receiver yields reader batches; callers can match contribution keys
    /// incrementally instead of retaining the whole filtered bucket.
    pub(crate) async fn stream_partition_files_filtered(
        &self,
        groups: Vec<PartitionFiles>,
        filters: Vec<Expr>,
        explicit_pk: Option<(Vec<String>, Vec<lakesoul_vector::IndexKey>)>,
        gate: Arc<tokio::sync::Semaphore>,
    ) -> Result<
        tokio::sync::mpsc::Receiver<std::result::Result<RecordBatch, DataFusionError>>,
    > {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let table = self.clone();
        tokio::task::spawn_blocking(move || {
            lakesoul_io::session::GLOBAL_RUNTIME.block_on(async move {
                let _permit = match gate.acquire_owned().await {
                    Ok(permit) => permit,
                    Err(error) => {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(format!(
                                "state lookup gate closed: {error}"
                            ))))
                            .await;
                        return;
                    }
                };
                for group in groups {
                    if group.files.is_empty() {
                        continue;
                    }
                    if let Err(error) = table
                        .stream_files_with_group(
                            group,
                            filters.clone(),
                            None,
                            None,
                            None,
                            true,
                            explicit_pk.clone(),
                            tx.clone(),
                        )
                        .await
                    {
                        let _ = tx
                            .send(Err(DataFusionError::Execution(error.to_string())))
                            .await;
                        return;
                    }
                }
            })
        });
        Ok(rx)
    }

    /// Read delta files **without** merge-on-read.
    ///
    /// A table written by an internal view stores a delete row with the old
    /// payload followed by the insert row for the same primary key.  The
    /// aggregate over such a view needs both versions to subtract the old
    /// contribution and add the new one; the merged delta keeps only the
    /// insert.  Reading the added files raw exposes the changelog.
    pub async fn read_partition_files_unmerged_projected(
        &self,
        groups: Vec<PartitionFiles>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        let groups = groups
            .into_iter()
            .filter(|group| !group.files.is_empty())
            .collect::<Vec<_>>();
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        let mut batches = Vec::new();
        for group in groups {
            let schema = projection.cloned().unwrap_or_else(|| self.schema.clone());
            let range_columns = self
                .range_partition_columns
                .iter()
                .filter(|column| schema.field_with_name(column).is_ok())
                .cloned()
                .collect::<Vec<_>>();
            let file_count = group.files.len() as u64;
            let mut builder = with_reader_io(
                LakeSoulIOConfig::builder()
                    .with_files(group.files)
                    .with_schema(schema)
                    .with_physical_format(self.file_format),
            );
            if !range_columns.is_empty() {
                builder = builder.with_range_partitions(range_columns.clone());
                if let Some(values) =
                    parse_partition_values(&group.partition_desc, &range_columns)
                {
                    for (column, value) in range_columns.iter().zip(values) {
                        builder =
                            builder.with_default_column_value(column.clone(), value);
                    }
                }
            }
            let mut reader = LakeSoulReader::new(builder.build())?;
            reader.start().await?;
            let before = batches.len();
            while let Some(batch) = reader.next_rb().await {
                batches.push(batch?);
            }
            crate::observability::add_files_read(file_count);
            crate::observability::add_rows_scanned(
                batches[before..]
                    .iter()
                    .map(|batch| batch.num_rows() as u64)
                    .sum(),
            );
        }
        Ok(batches)
    }

    /// Read the state of the table before a refresh window.
    ///
    /// Every partition the window touched is pinned to the version it was
    /// consumed at (`before`); a partition the window touched for the first
    /// time contributes nothing and a partition the window did not touch is
    /// read at its current state, so the result is the exact before state of
    /// the window.
    pub async fn read_before_window(
        &self,
        client: &MetaDataClient,
        before: &std::collections::HashMap<String, Option<i64>>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_before_window_with_options(client, before, Vec::new(), None)
            .await
    }

    /// Read the before state of a refresh window restricted to `filters`.
    pub async fn read_before_window_filtered(
        &self,
        client: &MetaDataClient,
        before: &std::collections::HashMap<String, Option<i64>>,
        filters: Vec<Expr>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_before_window_with_options(client, before, filters, None)
            .await
    }

    /// Read the before state of a refresh window projected to `projection`.
    pub async fn read_before_window_projected(
        &self,
        client: &MetaDataClient,
        before: &std::collections::HashMap<String, Option<i64>>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_before_window_with_options(client, before, Vec::new(), projection)
            .await
    }

    /// Read the before state of a refresh window with pushed filters and a
    /// pushed projection.
    ///
    /// Both are handed to the LakeSoul reader: the filter is the affected-key
    /// constraint (and drives the row-level pk index), the projection keeps
    /// the scan to the columns the refresh actually consumes.  The projection
    /// must contain the merge key and the change column.
    pub async fn read_before_window_filtered_projected(
        &self,
        client: &MetaDataClient,
        before: &std::collections::HashMap<String, Option<i64>>,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_before_window_with_options(client, before, filters, projection)
            .await
    }

    pub(crate) async fn before_window_groups(
        &self,
        client: &MetaDataClient,
        before: &std::collections::HashMap<String, Option<i64>>,
    ) -> Result<Vec<PartitionFiles>> {
        let mut versions = Vec::new();
        for partition in client.get_all_partition_info(&self.table_id).await? {
            match before.get(&partition.partition_desc) {
                // The partition is first consumed by this window: it has no
                // before state.
                Some(None) => continue,
                Some(Some(version)) => versions.push(PartitionVersion {
                    partition_desc: partition.partition_desc.clone(),
                    version: *version,
                }),
                // The window did not touch the partition: before == current.
                None => versions.push(PartitionVersion {
                    partition_desc: partition.partition_desc.clone(),
                    version: i64::from(partition.version),
                }),
            }
        }
        self.version_groups(client, &versions).await
    }

    async fn read_before_window_with_options(
        &self,
        client: &MetaDataClient,
        before: &std::collections::HashMap<String, Option<i64>>,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        let groups = self.before_window_groups(client, before).await?;
        self.read_groups_with_options(groups, filters, projection)
            .await
    }

    async fn read_groups_with_options(
        &self,
        groups: Vec<PartitionFiles>,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        let groups = groups
            .into_iter()
            .filter(|group| !group.files.is_empty())
            .collect::<Vec<_>>();
        if groups.is_empty() {
            return Ok(Vec::new());
        }
        if self.range_partition_columns.is_empty() {
            // Unpartitioned source: a single read keeps the previous
            // merge-on-read scope.
            let files = groups.into_iter().flat_map(|group| group.files).collect();
            return self
                .read_files_with_options(files, filters, projection)
                .await;
        }

        let mut batches = Vec::new();
        for group in groups {
            let schema = projection.cloned().unwrap_or_else(|| self.schema.clone());
            let range_columns = self
                .range_partition_columns
                .iter()
                .filter(|column| schema.field_with_name(column).is_ok())
                .cloned()
                .collect::<Vec<_>>();
            let group_files = group.files.len() as u64;
            let mut builder = with_reader_io(
                LakeSoulIOConfig::builder()
                    .with_files(group.files)
                    .with_schema(schema)
                    .with_primary_keys(self.primary_keys.clone())
                    .with_physical_format(self.file_format),
            );
            if !range_columns.is_empty() {
                builder = builder.with_range_partitions(range_columns.clone());
                if let Some(values) =
                    parse_partition_values(&group.partition_desc, &range_columns)
                {
                    for (column, value) in range_columns.iter().zip(values) {
                        builder =
                            builder.with_default_column_value(column.clone(), value);
                    }
                }
            }
            if !filters.is_empty() {
                builder = with_state_key_lookup(builder, self.file_format);
                #[allow(deprecated)]
                {
                    builder = builder.with_filters(filters.clone());
                }
            }
            let mut reader = LakeSoulReader::new(builder.build())?;
            reader.start().await?;
            let before = batches.len();
            while let Some(batch) = reader.next_rb().await {
                batches.push(batch?);
            }
            crate::observability::add_files_read(group_files);
            crate::observability::add_rows_scanned(
                batches[before..]
                    .iter()
                    .map(|batch| batch.num_rows() as u64)
                    .sum(),
            );
        }
        Ok(batches)
    }

    /// The current partition groups whose descriptor matches `partitions`
    /// (`(column, allowed values)`).  Used to skip partitions a refresh
    /// cannot touch without materializing the table.
    pub(crate) async fn current_groups_for_partitions(
        &self,
        client: &MetaDataClient,
        partitions: &[(String, Vec<String>)],
    ) -> Result<Vec<PartitionFiles>> {
        let mut groups = Vec::new();
        for partition in client.get_all_partition_info(&self.table_id).await? {
            if !partition_matches(
                &partition.partition_desc,
                &self.range_partition_columns,
                partitions,
            ) {
                continue;
            }
            let files = client
                .get_data_files_of_single_partition(&partition)
                .await?;
            if !files.is_empty() {
                groups.push(PartitionFiles {
                    partition_desc: partition.partition_desc.clone(),
                    files,
                });
            }
        }
        Ok(groups)
    }

    pub(crate) async fn current_groups(
        &self,
        client: &MetaDataClient,
    ) -> Result<Vec<PartitionFiles>> {
        let mut groups = Vec::new();
        for partition in client.get_all_partition_info(&self.table_id).await? {
            let files = client
                .get_data_files_of_single_partition(&partition)
                .await?;
            if !files.is_empty() {
                groups.push(PartitionFiles {
                    partition_desc: partition.partition_desc.clone(),
                    files,
                });
            }
        }
        Ok(groups)
    }

    /// The partition groups as of `as_of_ms`.
    pub(crate) async fn as_of_groups(
        &self,
        client: &MetaDataClient,
        as_of_ms: i64,
    ) -> Result<Vec<PartitionFiles>> {
        let mut groups = Vec::new();
        for partition in client
            .get_all_partition_info_as_of(&self.table_id, as_of_ms)
            .await?
        {
            let files = client
                .get_data_files_of_single_partition(&partition)
                .await?;
            groups.push(PartitionFiles {
                partition_desc: partition.partition_desc.clone(),
                files,
            });
        }
        Ok(groups)
    }

    /// The partition groups pinned to `versions`.
    pub(crate) async fn version_groups(
        &self,
        client: &MetaDataClient,
        versions: &[PartitionVersion],
    ) -> Result<Vec<PartitionFiles>> {
        let mut groups = Vec::new();
        for version in versions {
            let version_i32 = version
                .version
                .clamp(i64::from(i32::MIN), i64::from(i32::MAX))
                as i32;
            if let Some(partition) = client
                .get_partition_info_by_version(
                    &self.table_id,
                    &version.partition_desc,
                    version_i32,
                )
                .await?
            {
                let files = client
                    .get_data_files_of_single_partition(&partition)
                    .await?;
                groups.push(PartitionFiles {
                    partition_desc: version.partition_desc.clone(),
                    files,
                });
            }
        }
        Ok(groups)
    }

    /// Keep the rows whose change column is not a CDC tombstone.
    pub(crate) fn filter_change_batch(
        batch: RecordBatch,
        column: &str,
    ) -> std::result::Result<RecordBatch, DataFusionError> {
        let index = batch.schema().index_of(column).map_err(|_| {
            DataFusionError::Execution(format!(
                "change column {column} is not part of the batch"
            ))
        })?;
        let values = batch
            .column(index)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "change column {column} is not a string column"
                ))
            })?;
        let mask = arrow_array::BooleanArray::from_iter(
            (0..values.len()).map(|row| Some(values.value(row) != "delete")),
        );
        arrow::compute::filter_record_batch(&batch, &mask)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }

    /// Project one streamed batch to the subscriber's output schema.
    fn project_output(
        batch: RecordBatch,
        output: Option<&SchemaRef>,
    ) -> std::result::Result<RecordBatch, DataFusionError> {
        let Some(output) = output else {
            return Ok(batch);
        };
        if output.fields().is_empty() {
            // A scan that needs no columns (`COUNT(*)`): keep the row count.
            return RecordBatch::try_new_with_options(
                output.clone(),
                Vec::new(),
                &arrow::array::RecordBatchOptions::new()
                    .with_row_count(Some(batch.num_rows())),
            )
            .map_err(|error| DataFusionError::Execution(error.to_string()));
        }
        let batch_schema = batch.schema();
        let indices = output
            .fields()
            .iter()
            .map(|field| {
                batch_schema.index_of(field.name()).map_err(|_| {
                    DataFusionError::Execution(format!(
                        "stream projection column {} is missing",
                        field.name()
                    ))
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        batch
            .project(&indices)
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }

    /// Stream one range partition group with its default partition values.
    pub(crate) async fn stream_files_with_group(
        &self,
        group: PartitionFiles,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
        output: Option<&SchemaRef>,
        change_filter: Option<&str>,
        merge: bool,
        explicit_pk: Option<(Vec<String>, Vec<lakesoul_vector::IndexKey>)>,
        tx: BatchSender,
    ) -> Result<()> {
        let schema = projection.cloned().unwrap_or_else(|| self.schema.clone());
        let range_columns = self
            .range_partition_columns
            .iter()
            .filter(|column| schema.field_with_name(column).is_ok())
            .cloned()
            .collect::<Vec<_>>();
        let file_count = group.files.len() as u64;
        let mut builder = with_reader_io(
            LakeSoulIOConfig::builder()
                .with_files(group.files)
                .with_schema(schema)
                .with_primary_keys(if merge {
                    self.primary_keys.clone()
                } else {
                    Vec::new()
                })
                .with_physical_format(self.file_format),
        );
        if !range_columns.is_empty() {
            builder = builder.with_range_partitions(range_columns.clone());
            if let Some(values) =
                parse_partition_values(&group.partition_desc, &range_columns)
            {
                for (column, value) in range_columns.iter().zip(values) {
                    builder = builder.with_default_column_value(column.clone(), value);
                }
            }
        }
        if !filters.is_empty() {
            builder = with_state_key_lookup(builder, self.file_format);
            #[allow(deprecated)]
            {
                builder = builder.with_filters(filters);
            }
        }
        if let Some((columns, keys)) = explicit_pk {
            // The row-level index is only a win on Vortex's small blocks; a
            // Parquet page holds too many rows, and building the index on the
            // fly is much slower than the regular scan.
            let vortex = !matches!(self.file_format, PhysicalFormat::Parquet);
            let forced = std::env::var("LAKESOUL_IVM_STATE_INDEX")
                .ok()
                .is_some_and(|value| !matches!(value.as_str(), "0" | "false" | "off"));
            if vortex || forced {
                builder = with_state_key_lookup(builder, self.file_format);
                builder = builder.with_explicit_pk_keys(columns, keys);
            }
        }
        let mut reader = LakeSoulReader::new(builder.build())?;
        reader.start().await?;
        let mut rows = 0u64;
        while let Some(batch) = reader.next_rb().await {
            match batch {
                Ok(batch) => {
                    rows += batch.num_rows() as u64;
                    let batch = match change_filter {
                        Some(column) => match Self::filter_change_batch(batch, column) {
                            Ok(batch) => batch,
                            Err(error) => {
                                let _ = tx.send(Err(error)).await;
                                return Ok(());
                            }
                        },
                        None => batch,
                    };
                    let batch = match Self::project_output(batch, output) {
                        Ok(batch) => batch,
                        Err(error) => {
                            let _ = tx.send(Err(error)).await;
                            return Ok(());
                        }
                    };
                    if tx.send(Ok(batch)).await.is_err() {
                        return Ok(());
                    }
                }
                Err(error) => {
                    let _ = tx
                        .send(Err(DataFusionError::Execution(error.to_string())))
                        .await;
                    return Ok(());
                }
            }
        }
        crate::observability::add_files_read(file_count);
        crate::observability::add_rows_scanned(rows);
        Ok(())
    }

    async fn read_files_with_options(
        &self,
        files: Vec<String>,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let file_count = files.len() as u64;
        let mut builder = with_reader_io(
            LakeSoulIOConfig::builder()
                .with_files(files)
                .with_schema(projection.cloned().unwrap_or_else(|| self.schema.clone()))
                .with_primary_keys(self.primary_keys.clone())
                .with_physical_format(self.file_format),
        );
        if !filters.is_empty() {
            builder = with_state_key_lookup(builder, self.file_format);
            // `with_filters` is deprecated in favour of the string/proto
            // variants, but it is the only API that keeps the filters as typed
            // expressions, which is what the runtime builds.
            #[allow(deprecated)]
            {
                builder = builder.with_filters(filters);
            }
        }
        let config = builder.build();
        let mut reader = LakeSoulReader::new(config)?;
        reader.start().await?;

        let mut batches = Vec::new();
        while let Some(batch) = reader.next_rb().await {
            batches.push(batch?);
        }
        crate::observability::add_files_read(file_count);
        crate::observability::add_rows_scanned(
            batches.iter().map(|batch| batch.num_rows() as u64).sum(),
        );
        Ok(batches)
    }

    /// Read the current merge-on-read state of the table.
    pub async fn read_current(
        &self,
        client: &MetaDataClient,
    ) -> Result<Vec<RecordBatch>> {
        self.read_current_with_options(client, Vec::new(), None)
            .await
    }

    /// Read the current merge-on-read state restricted to `filters`.
    ///
    /// The filters are pushed into the LakeSoul reader, so row groups whose
    /// statistics do not overlap the predicate can be skipped. Used by the
    /// window refresh to read only the affected partitions of a source.
    pub async fn read_current_filtered(
        &self,
        client: &MetaDataClient,
        filters: Vec<Expr>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_current_with_options(client, filters, None).await
    }

    /// Read the current merge-on-read state filtered and projected.
    pub async fn read_current_filtered_projected(
        &self,
        client: &MetaDataClient,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_current_with_options(client, filters, projection)
            .await
    }

    /// Read the current merge-on-read state projected to `projection`.
    ///
    /// The projection is pushed into the LakeSoul reader; it must contain the
    /// merge key columns and the change column, otherwise the read cannot be
    /// merged or filtered.
    pub async fn read_current_projected(
        &self,
        client: &MetaDataClient,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_current_with_options(client, Vec::new(), projection)
            .await
    }

    async fn read_current_with_options(
        &self,
        client: &MetaDataClient,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_current_with_partitions(client, filters, projection, None)
            .await
    }

    /// Read the current state restricted to the range partitions whose values
    /// satisfy `partitions` (`(column, allowed values)`), projected to
    /// `filters`-pruned state.
    ///
    /// Used to skip partitions the refresh cannot touch (changed-partition
    /// pruning); a partition whose descriptor cannot be parsed is kept.
    pub async fn read_current_filtered_partitions(
        &self,
        client: &MetaDataClient,
        filters: Vec<Expr>,
        partitions: &[(String, Vec<String>)],
    ) -> Result<Vec<RecordBatch>> {
        self.read_current_with_partitions(client, filters, None, Some(partitions))
            .await
    }

    async fn read_current_with_partitions(
        &self,
        client: &MetaDataClient,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
        partitions: Option<&[(String, Vec<String>)]>,
    ) -> Result<Vec<RecordBatch>> {
        let mut groups = Vec::new();
        for partition in client.get_all_partition_info(&self.table_id).await? {
            if let Some(restriction) = partitions
                && !partition_matches(
                    &partition.partition_desc,
                    &self.range_partition_columns,
                    restriction,
                )
            {
                tracing::debug!(
                    table = %self.table_name,
                    partition = %partition.partition_desc,
                    "pruned by the partition restriction"
                );
                continue;
            }
            let files = client
                .get_data_files_of_single_partition(&partition)
                .await?;
            groups.push(PartitionFiles {
                partition_desc: partition.partition_desc.clone(),
                files,
            });
        }
        self.read_groups_with_options(groups, filters, projection)
            .await
    }

    /// Read the state of the table as of `as_of_ms` (inclusive).
    ///
    /// Used by the join refresh to reconstruct the state each delta was
    /// joined against.
    pub async fn read_as_of(
        &self,
        client: &MetaDataClient,
        as_of_ms: i64,
    ) -> Result<Vec<RecordBatch>> {
        let mut files = Vec::new();
        for partition in client
            .get_all_partition_info_as_of(&self.table_id, as_of_ms)
            .await?
        {
            files.extend(
                client
                    .get_data_files_of_single_partition(&partition)
                    .await?,
            );
        }
        self.read_files(files).await
    }

    /// Read the state as of `as_of_ms`, projected to `projection`.
    pub async fn read_as_of_projected(
        &self,
        client: &MetaDataClient,
        as_of_ms: i64,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_as_of_with_options(client, as_of_ms, Vec::new(), projection)
            .await
    }

    /// Read the state as of `as_of_ms` restricted to `filters`.
    pub async fn read_as_of_filtered(
        &self,
        client: &MetaDataClient,
        as_of_ms: i64,
        filters: Vec<Expr>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_as_of_with_options(client, as_of_ms, filters, None)
            .await
    }

    async fn read_as_of_with_options(
        &self,
        client: &MetaDataClient,
        as_of_ms: i64,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        let mut groups = Vec::new();
        for partition in client
            .get_all_partition_info_as_of(&self.table_id, as_of_ms)
            .await?
        {
            let files = client
                .get_data_files_of_single_partition(&partition)
                .await?;
            groups.push(PartitionFiles {
                partition_desc: partition.partition_desc.clone(),
                files,
            });
        }
        self.read_groups_with_options(groups, filters, projection)
            .await
    }

    /// Read the given partition versions with this table's schema and merge key.
    ///
    /// This is how a consumer pins the state of an epoch: the epoch row records
    /// the MV partition versions it produced.
    pub async fn read_at_versions(
        &self,
        client: &MetaDataClient,
        versions: &[PartitionVersion],
    ) -> Result<Vec<RecordBatch>> {
        self.read_at_versions_projected(client, versions, None)
            .await
    }

    /// Read the given partition versions projected to `projection`.
    pub async fn read_at_versions_projected(
        &self,
        client: &MetaDataClient,
        versions: &[PartitionVersion],
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_at_versions_with_options(client, versions, Vec::new(), projection)
            .await
    }

    async fn read_at_versions_with_options(
        &self,
        client: &MetaDataClient,
        versions: &[PartitionVersion],
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
        let mut groups = Vec::new();
        for version in versions {
            let version_i32 = version
                .version
                .clamp(i64::from(i32::MIN), i64::from(i32::MAX))
                as i32;
            if let Some(partition) = client
                .get_partition_info_by_version(
                    &self.table_id,
                    &version.partition_desc,
                    version_i32,
                )
                .await?
            {
                let files = client
                    .get_data_files_of_single_partition(&partition)
                    .await?;
                groups.push(PartitionFiles {
                    partition_desc: version.partition_desc.clone(),
                    files,
                });
            }
        }
        self.read_groups_with_options(groups, filters, projection)
            .await
    }

    /// Clear every partition snapshot of the table, starting a rebuild from an
    /// empty state. The data files stay on disk for the retention cleanup.
    ///
    /// An empty compaction snapshot is used instead of a delete commit: a
    /// delete commit would reject the following merge/append via the commit
    /// conflict rules, while a compaction leaves the partition writable again.
    pub async fn truncate(&self, client: &MetaDataClient) -> Result<()> {
        let partitions = client.get_all_partition_info(&self.table_id).await?;
        if partitions.is_empty() {
            return Ok(());
        }
        let table_info = client
            .get_table_info_by_table_id(&self.table_id)
            .await?
            .ok_or_else(|| rootcause::report!("table {} not found", self.table_id))?;
        let list_partition = partitions
            .iter()
            .map(|partition| PartitionInfo {
                table_id: self.table_id.clone(),
                partition_desc: partition.partition_desc.clone(),
                ..Default::default()
            })
            .collect();
        client
            .commit_data(
                MetaInfo {
                    table_info: Some(table_info),
                    list_partition,
                    read_partition_info: partitions,
                },
                CommitOp::CompactionCommit,
            )
            .await?;
        Ok(())
    }
}

/// The first or last key bytes of a batch.
fn boundary_key(
    converter: &RowConverter,
    batch: &RecordBatch,
    key_indices: &[usize],
    first: bool,
) -> Result<Vec<u8>> {
    let columns = key_indices
        .iter()
        .map(|index| batch.column(*index).clone())
        .collect::<Vec<_>>();
    let rows = converter
        .convert_columns(&columns)
        .map_err(|error| rootcause::report!("key rows: {error}"))?;
    let index = if first { 0 } else { batch.num_rows() - 1 };
    Ok(rows.row(index).as_ref().to_vec())
}

/// The start of the final key group in a key-sorted batch.
fn last_key_group_start(
    converter: &RowConverter,
    batch: &RecordBatch,
    key_indices: &[usize],
) -> Result<usize> {
    let columns = key_indices
        .iter()
        .map(|index| batch.column(*index).clone())
        .collect::<Vec<_>>();
    let rows = converter
        .convert_columns(&columns)
        .map_err(|error| rootcause::report!("key rows: {error}"))?;
    let mut start = batch.num_rows() - 1;
    while start > 0
        && rows.row(start - 1).as_ref() == rows.row(batch.num_rows() - 1).as_ref()
    {
        start -= 1;
    }
    Ok(start)
}

#[cfg(test)]
mod coalesce_tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]))
    }

    fn batch(start: i32, rows: usize) -> RecordBatch {
        let values = (start..start + rows as i32).collect::<Vec<_>>();
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(values))])
            .expect("test batch")
    }

    #[test]
    fn coalesces_small_batches_to_target() {
        let mut coalescer = BatchCoalescer::new(schema(), 4);
        assert!(coalescer.push(batch(0, 1)).expect("push").is_none());
        assert!(coalescer.push(batch(1, 1)).expect("push").is_none());
        let output = coalescer
            .push(batch(2, 2))
            .expect("push")
            .expect("coalesced batch");
        assert_eq!(output.num_rows(), 4);
        assert!(coalescer.finish().expect("finish").is_none());
    }

    #[test]
    fn flushes_remainder_at_end_of_stream() {
        let mut coalescer = BatchCoalescer::new(schema(), 8);
        assert!(coalescer.push(batch(0, 3)).expect("push").is_none());
        let output = coalescer
            .finish()
            .expect("finish")
            .expect("remaining batch");
        assert_eq!(output.num_rows(), 3);
        assert!(coalescer.finish().expect("finish").is_none());
    }

    #[test]
    fn preserves_row_order_across_batches() {
        let mut coalescer = BatchCoalescer::new(schema(), 5);
        assert!(coalescer.push(batch(0, 2)).expect("push").is_none());
        assert!(coalescer.push(batch(2, 2)).expect("push").is_none());
        let output = coalescer
            .push(batch(4, 2))
            .expect("push")
            .expect("coalesced batch");
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 column");
        assert_eq!(values.values(), &[0, 1, 2, 3, 4, 5]);
    }
}
