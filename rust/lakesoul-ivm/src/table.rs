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
use arrow_schema::SchemaRef;
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
fn parse_partition_values(
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

        let mut writer = create_writer_with_io_config(builder.build()).await?;
        writer.write_record_batch(batch).await?;
        let outputs = writer.flush_and_close().await?;

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
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let commit_ids = client
            .commit_data_files(&self.table_name, &self.namespace, files)
            .await?;
        Ok(commit_ids)
    }

    /// Read the given data files with this table's schema and merge key.
    pub async fn read_files(&self, files: Vec<String>) -> Result<Vec<RecordBatch>> {
        self.read_files_with_options(files, Vec::new(), None).await
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

    async fn read_before_window_with_options(
        &self,
        client: &MetaDataClient,
        before: &std::collections::HashMap<String, Option<i64>>,
        filters: Vec<Expr>,
        projection: Option<&SchemaRef>,
    ) -> Result<Vec<RecordBatch>> {
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
        self.read_at_versions_with_options(client, &versions, filters, projection)
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
                #[allow(deprecated)]
                {
                    builder = builder.with_filters(filters.clone());
                }
            }
            let mut reader = LakeSoulReader::new(builder.build())?;
            reader.start().await?;
            while let Some(batch) = reader.next_rb().await {
                batches.push(batch?);
            }
        }
        Ok(batches)
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
        let mut builder = LakeSoulIOConfig::builder()
            .with_files(files)
            .with_schema(projection.cloned().unwrap_or_else(|| self.schema.clone()))
            .with_primary_keys(self.primary_keys.clone())
            .with_physical_format(self.file_format);
        if !filters.is_empty() {
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
        let mut groups = Vec::new();
        for partition in client.get_all_partition_info(&self.table_id).await? {
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
