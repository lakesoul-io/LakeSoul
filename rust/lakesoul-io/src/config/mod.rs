// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc};

use arrow_schema::{Schema, SchemaRef};
use datafusion_common::DFSchema;
use datafusion_expr::Expr;
use datafusion_substrait::substrait::proto::Plan;
use educe::Educe;
use itertools::Itertools;

use crate::{
    Result,
    filter::parser::{FilterContainer, Parser},
    helpers::coerce_filter_type,
};

mod options;
pub use options::*;

#[derive(Debug, Clone)]
pub struct IOSchema(pub(crate) SchemaRef);

impl Default for IOSchema {
    fn default() -> Self {
        Self(Arc::new(Schema::empty()))
    }
}

/// Configuration for LakeSoul IO operations.
///
/// This struct contains all the necessary parameters for configuring LakeSoul IO operations,
/// including file paths, schema information, partitioning settings, and performance tuning options.
#[derive(Educe, Debug, Clone)]
#[educe(Default)]
pub struct LakeSoulIOConfig {
    /// Root directory path for files, unescaped
    pub(crate) prefix: String,
    /// List of file paths to read or write, unescaped
    pub(crate) files: Vec<String>,
    /// Names of primary key columns
    pub(crate) primary_keys: Vec<String>,
    /// Names of range partition columns
    pub(crate) range_partitions: Vec<String>,
    /// Number of hash buckets for hash partitioning
    #[educe(Default = "1")]
    hash_bucket_num: String,
    /// Names of columns to select
    #[deprecated(since = "2.5.0", note = "deprecated")]
    pub(crate) columns: Vec<String>,
    /// Names of auxiliary columns for sorting
    pub(crate) aux_sort_cols: Vec<String>,
    /// Encoded java filter predicates as strings
    pub(crate) filter_strs: Vec<String>,
    /// Filter predicates as expressions
    #[deprecated(since = "2.5.0", note = "deprecated")]
    pub(crate) filters: Vec<Expr>,
    /// Filter predicates as substrait plans
    pub(crate) filter_protos: Vec<Plan>,
    /// Filter predicates as substrait raw buf
    pub(crate) filter_buf: Vec<Vec<u8>>,
    /// Number of rows per batch for reading/writing
    #[educe(Default = 8192)]
    pub(crate) batch_size: usize,
    /// Maximum number of rows per row group when writing
    #[educe(Default = 250000)]
    pub(crate) max_row_group_size: usize,
    /// Maximum number of values per row group when writing
    #[educe(Default = 2147483647)]
    pub(crate) max_row_group_num_values: usize,
    /// Number of batches to prefetch
    #[educe(Default = 1)]
    pub(crate) prefetch_size: usize,
    /// Whether to enable Parquet filter pushdown
    #[educe(Default = false)]
    pub(crate) parquet_filter_pushdown: bool,
    /// Target Arrow schema for the reader and writer
    pub(crate) target_schema: IOSchema,
    /// Arrow schema for partition columns
    pub(crate) partition_schema: IOSchema,
    /// Object store configuration options (e.g., S3 credentials)
    pub(crate) object_store_options: HashMap<String, String>,
    /// Merge operators for each column
    pub(crate) merge_operators: HashMap<String, String>,
    /// Default values for columns
    pub(crate) default_column_value: HashMap<String, String>,
    /// Number of threads for parallel processing
    #[educe(Default = 2)]
    pub(crate) thread_num: usize,
    /// Default filesystem URI (compatible with Hadoop's fs.defaultFS)
    pub(crate) default_fs: String,
    /// Additional configuration options
    pub(super) options: HashMap<String, String>,
    /// Whether to use dynamic partitioning
    #[educe(Default = false)]
    pub(crate) use_dynamic_partition: bool,
    /// Whether to infer schema from data
    #[educe(Default = false)]
    pub(crate) inferring_schema: bool,
    /// Maximum file size in bytes
    #[educe(Default  = None)]
    pub(crate) max_file_size: Option<u64>,
    /// Random number generator seed
    #[educe(Default = 1234)]
    pub(crate) seed: u64,
    /// Memory buffer capacity
    #[educe(Default = 16 * 1024)]
    pub(crate) memory_buffer_capacity: usize,
    /// Chunk size
    #[educe(Default=128 * 1024 * 1024)]
    pub(crate) multipart_chunk_size: usize,
    /// Receiver capacity
    #[educe(Default = 8)]
    pub(crate) receiver_capacity: usize,
}

impl LakeSoulIOConfig {
    /// Returns a builder for LakeSoulIOConfig
    pub fn builder() -> LakeSoulIOConfigBuilder {
        LakeSoulIOConfigBuilder::new()
    }

    /// Returns the target Arrow schema for the reader and writer
    pub fn target_schema(&self) -> SchemaRef {
        self.target_schema.0.clone()
    }

    /// Returns the Arrow schema for partition columns
    pub fn partition_schema(&self) -> SchemaRef {
        self.partition_schema.0.clone()
    }

    /// Returns a slice of primary key column names
    pub fn primary_keys_slice(&self) -> &[String] {
        &self.primary_keys
    }

    /// Returns a slice of range partition column names
    pub fn range_partitions_slice(&self) -> &[String] {
        &self.range_partitions
    }

    /// Returns a slice of file paths to read or write
    pub fn files_slice(&self) -> &[String] {
        &self.files
    }

    /// Returns a slice of auxiliary sort column names
    pub fn aux_sort_cols_slice(&self) -> &[String] {
        &self.aux_sort_cols
    }

    /// Returns the value of a configuration option by key
    pub fn option(&self, key: &str) -> Option<&String> {
        self.options.get(key)
    }

    /// Returns the root directory path for files
    pub fn prefix(&self) -> &String {
        &self.prefix
    }

    /// Returns whether to support parquet pushdown filters
    pub fn parquet_pushdown_filters(&self) -> bool {
        self.parquet_filter_pushdown
    }

    /// Returns whether to keep row order in output
    pub fn keep_ordering(&self) -> bool {
        self.option(OPTION_KEY_KEEP_ORDERS)
            .is_some_and(|x| x.eq("true"))
    }

    /// Returns the memory limit in bytes if set
    pub fn mem_limit(&self) -> Option<usize> {
        self.option(OPTION_KEY_MEM_LIMIT)
            .map(|x| x.parse().unwrap())
    }

    /// Returns the maximum file size in bytes if set
    pub fn max_file_size_option(&self) -> Option<u64> {
        self.option(OPTION_KEY_MAX_FILE_SIZE)
            .map(|x| x.parse().unwrap())
    }

    /// Returns the memory pool size in bytes if set
    pub fn pool_size(&self) -> Option<usize> {
        self.option(OPTION_KEY_POOL_SIZE)
            .map(|x| x.parse().unwrap())
    }

    /// Returns the hash bucket ID for partitioning (defaults to 0)
    pub fn hash_bucket_id(&self) -> usize {
        self.option(OPTION_KEY_HASH_BUCKET_ID)
            .map_or(0, |x| x.parse().unwrap())
    }

    /// Returns the number of hash buckets for partitioning (defaults to 1, equvalent to not partitioning)
    pub fn hash_bucket_num(&self) -> usize {
        self.option(OPTION_KEY_HASH_BUCKET_NUM)
            .map_or(1, |x| x.parse().unwrap())
    }

    // Get hash_bucket_num field directly, not from option
    pub fn get_hash_bucket_num(&self) -> Result<usize> {
        let mut tmp = self.hash_bucket_num.parse::<isize>()?;
        tmp = tmp.max(1);
        Ok(tmp as usize)
    }

    /// Returns the CDC (Change Data Capture) column name if set
    pub fn cdc_column(&self) -> String {
        self.option(OPTION_KEY_CDC_COLUMN)
            .map_or_else(String::new, |x| x.to_string())
    }

    /// Returns whether the data is compacted, default is false
    pub fn is_compacted(&self) -> bool {
        self.option(OPTION_KEY_IS_COMPACTED)
            .is_some_and(|x| x.eq("true"))
    }

    /// Returns whether to skip merge operation during read, default is false
    pub fn skip_merge_on_read(&self) -> bool {
        self.option(OPTION_KEY_SKIP_MERGE_ON_READ)
            .is_some_and(|x| x.eq("true"))
    }

    /// Returns whether to compute Local Sensitive Hash (defaults to true)
    pub fn compute_lsh(&self) -> bool {
        self.option(OPTION_KEY_COMPUTE_LSH)
            .is_none_or(|x| x.eq("true"))
    }

    /// Returns whether to use stable sort algorithm
    pub fn stable_sort(&self) -> bool {
        self.option(OPTION_KEY_STABLE_SORT)
            .is_some_and(|x| x.eq("true"))
    }
}

#[derive(Debug, Default, Clone)]
/// Builder for LakeSoulIOConfig
///
/// This struct provides a fluent builder interface for creating LakeSoulIOConfig instances.
/// It allows for setting various configuration options using method chaining.
pub struct LakeSoulIOConfigBuilder {
    config: LakeSoulIOConfig,
}

impl LakeSoulIOConfigBuilder {
    /// Creates a new LakeSoulIOConfigBuilder instance with default configuration
    ///
    /// # Returns
    ///
    /// A new LakeSoulIOConfigBuilder instance with default configuration
    pub fn new() -> Self {
        LakeSoulIOConfigBuilder {
            config: LakeSoulIOConfig::default(),
        }
    }

    /// Creates a new LakeSoulIOConfigBuilder instance with object store options
    ///
    /// # Arguments
    ///
    /// * `options` - A HashMap of string key-value pairs representing object store options
    ///
    pub fn new_with_object_store_options(options: HashMap<String, String>) -> Self {
        let mut builder = LakeSoulIOConfigBuilder::new();
        builder.config.object_store_options = options;
        builder
    }

    pub fn schema(&self) -> SchemaRef {
        self.config.target_schema()
    }

    pub fn primary_keys_slice(&self) -> &[String] {
        self.config.primary_keys_slice()
    }

    pub fn aux_sort_cols_slice(&self) -> &[String] {
        self.config.aux_sort_cols_slice()
    }

    pub fn prefix(&self) -> &String {
        &self.config.prefix
    }

    pub fn object_store_options(&self) -> &HashMap<String, String> {
        &self.config.object_store_options
    }

    /// Sets the prefix for the file path
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix for the file path
    pub fn with_prefix(mut self, prefix: String) -> Self {
        self.config.prefix = prefix;
        self
    }

    /// Adds a file path to the list of files to read or write
    ///
    /// # Arguments
    ///
    /// * `file` - The file path to add
    pub fn with_file(mut self, file: impl Into<String>) -> Self {
        self.config.files.push(file.into());
        self
    }

    /// Adds a list of file paths to the list of files to read or write
    ///
    /// # Arguments
    ///
    /// * `files` - The list of file paths to add
    pub fn with_files(mut self, files: Vec<impl ToString>) -> Self {
        let files = files.into_iter().map(|x| x.to_string()).collect();
        self.config.files = files;
        self
    }

    /// Adds a primary key to the list of primary keys
    ///
    /// # Arguments
    ///
    /// * `pks` - The primary key to add
    pub fn with_primary_key(mut self, pks: impl Into<String>) -> Self {
        self.config.primary_keys.push(pks.into());
        self
    }

    /// Adds a list of primary keys to the list of primary keys
    ///
    /// # Arguments
    ///
    /// * `pks` - The list of primary keys to add
    pub fn with_primary_keys(mut self, pks: Vec<String>) -> Self {
        self.config.primary_keys = pks;
        self
    }

    /// Adds a range partition to the list of range partitions
    ///
    /// # Arguments
    ///
    /// * `range_partition` - The range partition to add
    pub fn with_range_partition(mut self, range_partition: String) -> Self {
        self.config.range_partitions.push(range_partition);
        self
    }

    /// Adds a list of range partitions to the list of range partitions
    ///
    /// # Arguments
    ///
    /// * `range_partitions` - The list of range partitions to add
    pub fn with_range_partitions(mut self, range_partitions: Vec<String>) -> Self {
        self.config.range_partitions = range_partitions;
        self
    }

    /// Sets the number of hash buckets for partitioning
    ///
    /// # Arguments
    ///
    /// * `hash_bucket_num` - The number of hash buckets for partitioning
    pub fn with_hash_bucket_num(mut self, hash_bucket_num: impl Into<String>) -> Self {
        self.config.hash_bucket_num = hash_bucket_num.into();
        self
    }

    /// Adds a column to the list of columns to select
    ///
    /// # Arguments
    ///
    /// * `col` - The column to add
    #[deprecated(
        since = "2.5.0",
        note = "This method is deprecated. Use target_schema instead."
    )]
    #[allow(deprecated)]
    pub fn with_column(mut self, col: String) -> Self {
        self.config.columns.push(String::from(&col));
        self
    }

    /// Adds an auxiliary sort column to the list of auxiliary sort columns
    ///
    /// # Arguments
    ///
    /// * `col` - The auxiliary sort column to add
    pub fn with_aux_sort_column(mut self, col: String) -> Self {
        self.config.aux_sort_cols.push(String::from(&col));
        self
    }

    /// Sets the number of rows per batch for reading/writing
    ///
    /// # Arguments
    ///
    /// * `batch_size` - The number of rows per batch for reading/writing
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }

    /// Sets the maximum number of rows per row group when writing
    ///
    /// # Arguments
    ///
    /// * `max_row_group_size` - The maximum number of rows per row group when writing
    pub fn with_max_row_group_size(mut self, max_row_group_size: usize) -> Self {
        self.config.max_row_group_size = max_row_group_size;
        self
    }

    /// Sets the maximum number of values per row group when writing
    ///
    /// # Arguments
    ///
    /// * `max_row_group_num_values` - The maximum number of values per row group when writing
    pub fn with_max_row_group_num_values(
        mut self,
        max_row_group_num_values: usize,
    ) -> Self {
        self.config.max_row_group_num_values = max_row_group_num_values;
        self
    }

    /// Sets the number of batches to prefetch
    ///
    /// # Arguments
    ///
    /// * `prefetch_size` - The number of batches to prefetch
    pub fn with_prefetch_size(mut self, prefetch_size: usize) -> Self {
        self.config.prefetch_size = prefetch_size;
        self
    }

    /// Sets whether to enable Parquet filter pushdown
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to enable Parquet filter pushdown
    pub fn with_parquet_filter_pushdown(mut self, enable: bool) -> Self {
        self.config.parquet_filter_pushdown = enable;
        self
    }

    #[deprecated(
        since = "2.5.0",
        note = "This method is deprecated. Use target_schema instead."
    )]
    #[allow(deprecated)]
    pub fn with_columns(mut self, cols: Vec<String>) -> Self {
        self.config.columns = cols;
        self
    }

    /// Sets the target Arrow schema for the reader and writer
    ///
    /// # Arguments
    ///
    /// * `schema` - The target Arrow schema to set
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.config.target_schema = IOSchema(schema);
        self
    }

    /// Sets the Arrow schema for partition columns
    ///
    /// # Arguments
    ///
    /// * `schema` - The Arrow schema for partition columns to set
    pub fn with_partition_schema(mut self, schema: SchemaRef) -> Self {
        self.config.partition_schema = IOSchema(schema);
        self
    }

    /// Adds a filter string to the list of filter strings
    ///
    /// # Arguments
    ///
    /// * `filter_str` - The filter string to add
    pub fn with_filter_str(mut self, filter_str: String) -> Self {
        self.config.filter_strs.push(filter_str);
        self
    }

    /// Adds a filter proto to the list of filter protos
    ///
    /// # Arguments
    ///
    /// * `filter_proto` - The filter proto to add
    pub fn with_filter_proto(mut self, filter_proto: Plan) -> Self {
        self.config.filter_protos.push(filter_proto);
        self
    }

    /// Adds a filter protobuf to the list of filter bufs
    ///
    /// # Arguments
    ///
    /// * `filter_buf` - The filter proto buf to add
    pub fn with_filter_buf(mut self, filter_buf: Vec<u8>) -> Self {
        self.config.filter_buf.push(filter_buf);
        self
    }

    #[deprecated(
        since = "2.5.0",
        note = "This method is deprecated. Use with_filter_str and with_filter_proto instead."
    )]
    #[allow(deprecated)]
    pub fn with_filters(mut self, filters: Vec<Expr>) -> Self {
        self.config.filters = filters;
        self
    }

    /// Adds a merge operator to the list of merge operators
    ///
    /// # Arguments
    ///
    /// * `field_name` - The field name to add the merge operator to
    /// * `merge_op` - The merge operator to add
    pub fn with_merge_op(mut self, field_name: String, merge_op: String) -> Self {
        self.config.merge_operators.insert(field_name, merge_op);
        self
    }

    /// Sets the default value for a column
    ///
    /// # Arguments
    ///
    /// * `field_name` - The field name to set the default value for
    /// * `value` - The default value to set
    pub fn with_default_column_value(
        mut self,
        field_name: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.config
            .default_column_value
            .insert(field_name.into(), value.into());
        self
    }

    /// Adds an object store option
    ///
    /// # Arguments
    ///
    /// * `key` - The key to add the object store option for
    /// * `value` - The value to add the object store option for
    pub fn with_object_store_option(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.config
            .object_store_options
            .insert(key.into(), value.into());
        self
    }

    /// Adds an option
    ///
    /// # Arguments
    ///
    /// * `key` - The key to add the option for
    /// * `value` - The value to add the option for
    pub fn with_option(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.config.options.insert(key.into(), value.into());
        self
    }

    /// Sets the number of threads for parallel processing
    ///
    /// # Arguments
    ///
    /// * `thread_num` - The number of threads for parallel processing
    pub fn with_thread_num(mut self, thread_num: usize) -> Self {
        self.config.thread_num = thread_num;
        self
    }

    /// Sets whether to use dynamic partitioning
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to use dynamic partitioning
    pub fn set_dynamic_partition(mut self, enable: bool) -> Self {
        self.config.use_dynamic_partition = enable;
        self
    }

    /// Sets whether to infer schema from data
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to infer schema from data
    pub fn set_inferring_schema(mut self, enable: bool) -> Self {
        self.config.inferring_schema = enable;
        self
    }

    /// Sets the maximum file size
    ///
    /// # Arguments
    ///
    /// * `size` - The maximum file size to set
    pub fn with_max_file_size(mut self, size: u64) -> Self {
        self.config.max_file_size = Some(size);
        self
    }

    /// Sets the random number generator seed for Local Sensitive Hash
    ///
    /// # Arguments
    ///
    /// * `seed` - The random number generator seed to set
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.config.seed = seed;
        self
    }

    /// Sets the capacity for In Memory Buffer
    ///
    /// # Arguments
    ///
    /// * `capacity` - The capacity for memory buffer
    pub fn with_memory_buffer_capacity(mut self, capacity: usize) -> Self {
        self.config.memory_buffer_capacity = capacity;
        self
    }

    /// Sets the chunk size for multipart upload
    ///
    /// # Arguments
    ///
    /// * `chunk_size` - size used by multipart writer
    pub fn with_multipart_chunk_size(mut self, chunk_size: usize) -> Self {
        self.config.multipart_chunk_size = chunk_size;
        self
    }

    /// Sets the capacity for receiver
    ///
    /// # Arguments
    ///
    /// * `capacity` - receiver's buffer capacity
    pub fn with_receiver_capacity(mut self, capacity: usize) -> Self {
        self.config.receiver_capacity = capacity;
        self
    }

    /// Builds the LakeSoulIOConfig instance
    ///
    /// # Returns
    ///
    /// The built LakeSoulIOConfig instance
    pub fn build(self) -> LakeSoulIOConfig {
        self.config
    }
}

impl LakeSoulIOConfig {
    /// Get filter in datafusion logical expr
    ///
    /// This will consume all filters
    pub async fn get_filter_exprs(&mut self, table_schema: &Schema) -> Result<Vec<Expr>> {
        let filter_strs = std::mem::take(&mut self.filter_strs);
        let filter_protos = std::mem::take(&mut self.filter_protos);
        let filter_bufs = std::mem::take(&mut self.filter_buf);
        let iter = filter_strs
            .into_iter()
            .map(FilterContainer::String)
            .chain(filter_protos.into_iter().map(FilterContainer::Plan))
            .chain(filter_bufs.into_iter().map(FilterContainer::RawBuf));

        let df_schema = DFSchema::try_from(table_schema.clone())?;

        let mut exprs: Vec<Expr> = Vec::new();

        let dummy_ctx = datafusion::prelude::SessionContext::new();

        for container in iter {
            exprs.extend(
                Parser::parse_filter_container(&dummy_ctx, &df_schema, container).await?,
            );
        }
        let exprs = exprs
            .into_iter()
            .map(|expr| coerce_filter_type(expr, &df_schema))
            .collect::<Result<Vec<_>>>()?;

        debug!("parser filter exprs: {}", exprs.iter().format(","));

        Ok(exprs)
    }
}

impl From<LakeSoulIOConfig> for LakeSoulIOConfigBuilder {
    fn from(val: LakeSoulIOConfig) -> Self {
        LakeSoulIOConfigBuilder { config: val }
    }
}
