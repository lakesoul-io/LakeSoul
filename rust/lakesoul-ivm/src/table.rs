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

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use lakesoul_common::ser::arrow_java::schema_to_metadata_str;
use lakesoul_io::{
    config::{LakeSoulIOConfig, OPTION_KEY_STABLE_SORT},
    file_format::PhysicalFormat,
    reader::LakeSoulReader,
    writer::create_writer_with_io_config,
};
use lakesoul_metadata::{MetaDataClient, transfusion::DataFileInfo};
use lakesoul_metadata_proto::entity::TableInfo;

use crate::error::Result;

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
    /// The columns the writer buckets on; empty means the merge key.
    pub bucket_columns: Vec<String>,
    /// The number of hash buckets.
    pub hash_bucket_num: String,
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
    /// The bucket columns; must be a prefix of `primary_keys`.
    pub bucket_columns: Vec<String>,
    /// The number of hash buckets.
    pub hash_bucket_num: String,
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
            bucket_columns: Vec::new(),
            hash_bucket_num: "4".to_string(),
        }
    }

    /// Set the merge key.
    pub fn with_primary_keys(mut self, primary_keys: Vec<String>) -> Self {
        self.primary_keys = primary_keys;
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
}

/// Create an internal LakeSoul table.
pub async fn create_ivm_table(
    client: &MetaDataClient,
    options: IvmTableOptions,
) -> Result<IvmTable> {
    let mut properties = serde_json::json!({
        "hashBucketNum": options.hash_bucket_num,
        "file_format": "parquet",
        "lakesoul.ivm.internal": "true",
    });
    if !options.bucket_columns.is_empty() {
        properties["lakesoul.ivm.bucket_columns"] =
            serde_json::Value::String(options.bucket_columns.join(","));
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
            partitions: format!(";{}", options.primary_keys.join(",")),
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
        bucket_columns: options.bucket_columns,
        hash_bucket_num: options.hash_bucket_num,
    })
}

impl IvmTable {
    /// Write one batch and commit the produced files.
    ///
    /// Keyed tables write through the partitioning writer with stable sort, so
    /// rows with the same key keep their input order (a `delete` written before
    /// an `insert` still wins in merge-on-read).
    pub async fn append_batch(
        &self,
        client: &MetaDataClient,
        batch: RecordBatch,
    ) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let mut builder = LakeSoulIOConfig::builder()
            .with_prefix(self.table_path.clone())
            .with_schema(self.schema.clone())
            .with_primary_keys(self.primary_keys.clone())
            .with_hash_partitioning_columns(self.bucket_columns.clone())
            .with_hash_bucket_num(self.hash_bucket_num.clone())
            .with_physical_format(PhysicalFormat::Parquet);
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
            return Ok(());
        }
        client
            .commit_data_files(&self.table_name, &self.namespace, files)
            .await?;
        Ok(())
    }

    /// Read the given data files with this table's schema and merge key.
    pub async fn read_files(&self, files: Vec<String>) -> Result<Vec<RecordBatch>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let config = LakeSoulIOConfig::builder()
            .with_files(files)
            .with_schema(self.schema.clone())
            .with_primary_keys(self.primary_keys.clone())
            .with_physical_format(PhysicalFormat::Parquet)
            .build();
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
        let mut files = Vec::new();
        for partition in client.get_all_partition_info(&self.table_id).await? {
            files.extend(
                client
                    .get_data_files_of_single_partition(&partition)
                    .await?,
            );
        }
        self.read_files(files).await
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
}
