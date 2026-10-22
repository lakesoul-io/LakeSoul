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

use arrow::record_batch::RecordBatch;
use arrow_array::Array;
use arrow_schema::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::prelude::Expr;
use lakesoul_common::ser::arrow_java::{
    schema_from_table_info_metadata, schema_to_metadata_str,
};
use lakesoul_io::{
    config::{LakeSoulIOConfig, OPTION_KEY_STABLE_SORT},
    file_format::PhysicalFormat,
    reader::LakeSoulReader,
    writer::create_writer_with_io_config,
};
use lakesoul_metadata::{MetaDataClient, transfusion::DataFileInfo};
use lakesoul_metadata_proto::entity::{CommitOp, MetaInfo, PartitionInfo, TableInfo};

use crate::error::Result;
use crate::metadata::PartitionVersion;
/// The channel a streaming scan sends its batches through.
type BatchSender =
    tokio::sync::mpsc::Sender<std::result::Result<RecordBatch, DataFusionError>>;

/// The largest state write (in rows) a refresh coalesces before appending.
/// State batches above this size are split so one append stays bounded in
/// memory; the writer still emits one file per hash bucket per append.
pub(crate) const MAX_STATE_APPEND_ROWS: usize = 1_048_576;

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
    let vortex = !matches!(file_format, PhysicalFormat::Parquet);
    if !vortex && std::env::var("LAKESOUL_IVM_STATE_INDEX").is_err() {
        return builder;
    }
    builder
        .with_max_pk_candidates(MAX_STATE_KEY_CANDIDATES)
        .with_bulk_pk_lookup(true)
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
fn partition_matches(
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

/// Create an internal LakeSoul table.
pub async fn create_ivm_table(
    client: &MetaDataClient,
    options: IvmTableOptions,
) -> Result<IvmTable> {
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

        let mut builder = LakeSoulIOConfig::builder()
            .with_prefix(self.table_path.clone())
            .with_schema(self.schema.clone())
            .with_primary_keys(self.primary_keys.clone())
            .with_hash_partitioning_columns(self.bucket_columns.clone())
            .with_hash_bucket_num(self.hash_bucket_num.clone())
            .with_physical_format(self.file_format);
        if !self.range_partition_columns.is_empty() {
            builder = builder.with_range_partitions(self.range_partition_columns.clone());
        }
        if !self.primary_keys.is_empty() {
            builder = builder
                .set_dynamic_partition(true)
                .with_option(OPTION_KEY_STABLE_SORT, "true");
        }

        let written_rows = batch.num_rows() as u64;
        let mut writer = create_writer_with_io_config(builder.build()).await?;
        writer.write_record_batch(batch).await?;
        let outputs = writer.flush_and_close().await?;
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
        crate::observability::add_rows_written(written_rows);
        crate::observability::add_bytes_written(written_bytes);
        Ok(files)
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
    /// batch size), and every [`Self::append_batch`] call becomes its own
    /// file per hash bucket.  Concatenating up to [`MAX_STATE_APPEND_ROWS`]
    /// rows per append keeps the row order (the refresh already sorted the
    /// output) while cutting the state file count by an order of magnitude
    /// for initial and rebuild refreshes.
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
    /// The stream runs the DataFusion plan; batches are written and committed
    /// in bounded chunks so refresh memory stays independent of the MV/state
    /// size.
    pub async fn append_stream<S>(
        &self,
        client: &MetaDataClient,
        mut stream: S,
    ) -> Result<Vec<String>>
    where
        S: futures::Stream<Item = datafusion::error::Result<RecordBatch>> + Send + Unpin,
    {
        use futures::StreamExt;
        let mut files = Vec::new();
        let mut pending: Vec<RecordBatch> = Vec::new();
        let mut pending_rows = 0usize;
        while let Some(batch) = stream.next().await {
            let batch = batch
                .map_err(|error| rootcause::report!("refresh output batch: {error}"))?;
            if batch.num_rows() == 0 {
                continue;
            }
            pending_rows += batch.num_rows();
            pending.push(batch);
            if pending_rows >= MAX_STATE_APPEND_ROWS {
                files.extend(self.write_batches(std::mem::take(&mut pending)).await?);
                pending_rows = 0;
            }
        }
        if !pending.is_empty() {
            files.extend(self.write_batches(pending).await?);
        }
        self.commit_files(client, files).await
    }

    /// Write refresh output as few state files as possible, without publishing.
    pub async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<DataFileInfo>> {
        let mut files = Vec::new();
        let mut pending: Vec<RecordBatch> = Vec::new();
        let mut pending_rows = 0usize;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            if pending_rows + batch.num_rows() > MAX_STATE_APPEND_ROWS && pending_rows > 0
            {
                let schema = pending[0].schema();
                let coalesced = arrow::compute::concat_batches(&schema, &pending)?;
                files.extend(self.write_batch(coalesced).await?);
                pending.clear();
                pending_rows = 0;
            }
            pending_rows += batch.num_rows();
            pending.push(batch);
        }
        if pending_rows > 0 {
            let schema = pending[0].schema();
            let coalesced = arrow::compute::concat_batches(&schema, &pending)?;
            files.extend(self.write_batch(coalesced).await?);
        }
        Ok(files)
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
        let batches = self.read_current(client).await?;
        let rows = self.compacted_batches(batches)?;
        if rows.iter().all(|batch| batch.num_rows() == 0) {
            self.truncate(client).await?;
            return Ok(Vec::new());
        }
        let files = self.write_batches(rows).await?;
        let read_partitions = client.get_all_partition_info(&self.table_id).await?;
        let commit_ids = client
            .commit_compaction(&self.table_name, &self.namespace, files, read_partitions)
            .await?;
        Ok(commit_ids)
    }

    /// The logical rows to rewrite: tombstones dropped, bookkeeping reset.
    fn compacted_batches(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let schema = self.schema.clone();
        let kinds = schema.index_of(IVM_ROW_KINDS_COLUMN).ok();
        let epoch = schema.index_of(IVM_EPOCH_COLUMN).ok();
        let mut out = Vec::new();
        for batch in batches {
            let filtered = match kinds {
                Some(index) => {
                    let values = batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .ok_or_else(|| {
                            rootcause::report!("row kind column is not a string column")
                        })?;
                    let mask = arrow_array::BooleanArray::from_iter(
                        (0..values.len()).map(|row| Some(values.value(row) != "delete")),
                    );
                    arrow::compute::filter_record_batch(&batch, &mask)?
                }
                None => batch,
            };
            if filtered.num_rows() == 0 {
                continue;
            }
            let mut columns = filtered.columns().to_vec();
            if let Some(index) = kinds {
                columns[index] = Arc::new(arrow_array::StringArray::from_iter_values(
                    (0..filtered.num_rows()).map(|_| "insert"),
                ));
            }
            if let Some(index) = epoch {
                columns[index] = Arc::new(arrow_array::Int64Array::from_iter_values(
                    (0..filtered.num_rows()).map(|_| 0i64),
                ));
            }
            out.push(RecordBatch::try_new(schema.clone(), columns)?);
        }
        Ok(out)
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
            let mut builder = LakeSoulIOConfig::builder()
                .with_files(group.files)
                .with_schema(schema)
                .with_physical_format(self.file_format);
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
            let mut builder = LakeSoulIOConfig::builder()
                .with_files(group.files)
                .with_schema(schema)
                .with_primary_keys(self.primary_keys.clone())
                .with_physical_format(self.file_format);
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
        let mut builder = LakeSoulIOConfig::builder()
            .with_files(group.files)
            .with_schema(schema)
            .with_primary_keys(if merge {
                self.primary_keys.clone()
            } else {
                Vec::new()
            })
            .with_physical_format(self.file_format);
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
        let mut builder = LakeSoulIOConfig::builder()
            .with_files(files)
            .with_schema(projection.cloned().unwrap_or_else(|| self.schema.clone()))
            .with_primary_keys(self.primary_keys.clone())
            .with_physical_format(self.file_format);
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
