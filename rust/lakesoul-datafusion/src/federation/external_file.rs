// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! DataFusion `ListingTable` backed foreign files.
//!
//! The FDW catalog stores only the table path and format. A scan carries the
//! object-store configuration with it so a distributed worker can register the
//! store immediately before reading; workers do not need to load federation
//! metadata at startup.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion::common::config::{CsvOptions, JsonOptions, TableParquetOptions};
use datafusion::common::file_options::csv_writer::CsvWriterOptions;
use datafusion::common::file_options::json_writer::JsonWriterOptions;
use datafusion::common::{DataFusionError, Result as DFResult, SchemaExt};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::csv::{CsvFormat, CsvSink};
use datafusion::datasource::file_format::json::{JsonFormat, JsonSink};
use datafusion::datasource::file_format::parquet::{ParquetFormat, ParquetSink};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::TaskContext;
use datafusion::execution::cache::cache_manager::TableScopedPath;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_expr_common::sort_expr::OrderingRequirements;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions, SendableRecordBatchStream,
};
use datafusion_datasource::file_sink_config::{FileOutputMode, FileSinkConfig};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, PhysicalExtensionCodec, PhysicalPlanNodeExt,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::PhysicalPlanNode;
use futures::stream;
use lakesoul_io::object_store_metrics::MonitoredObjectStore;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use url::Url;

const KNOWN_SERVER_OPTIONS: &[&str] = &[
    "endpoint",
    "access_key_id",
    "ak",
    "secret_access_key",
    "sk",
    "session_token",
    "region",
    "virtual_host_style",
    "allow_http",
];

const KNOWN_TABLE_OPTIONS: &[&str] = &[
    "path",
    "format",
    "file_extension",
    "has_header",
    "delimiter",
    "quote",
    "escape",
    "comment",
    "null_regex",
    "newlines_in_values",
    "json_array",
    "newline_delimited",
];

/// S3 connection settings stored on a foreign server.
///
/// These settings are serialized into the distributed scan plan. They never
/// include credentials in `Debug` output.
#[derive(Clone, Serialize, Deserialize)]
pub struct ExternalFileStoreConfig {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
    #[serde(default)]
    pub session_token: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default = "default_virtual_host_style")]
    pub virtual_host_style: bool,
    #[serde(default)]
    pub allow_http: bool,
}

fn default_virtual_host_style() -> bool {
    true
}

impl fmt::Debug for ExternalFileStoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalFileStoreConfig")
            .field("endpoint", &self.endpoint)
            .field(
                "access_key_id",
                &self.access_key_id.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("region", &self.region)
            .field("virtual_host_style", &self.virtual_host_style)
            .field("allow_http", &self.allow_http)
            .finish()
    }
}

impl ExternalFileStoreConfig {
    pub fn from_options(options: &BTreeMap<String, String>) -> DFResult<Self> {
        reject_unknown(options, KNOWN_SERVER_OPTIONS, "server")?;
        let endpoint = options.get("endpoint").cloned();
        if let Some(endpoint) = &endpoint {
            let url = Url::parse(endpoint).map_err(|error| {
                DataFusionError::Plan(format!(
                    "external file endpoint {endpoint:?} is not a valid URL: {error}"
                ))
            })?;
            if !matches!(url.scheme(), "http" | "https") {
                return Err(DataFusionError::Plan(format!(
                    "external file endpoint {endpoint:?} must use http:// or https://"
                )));
            }
            if !url.has_host() {
                return Err(DataFusionError::Plan(format!(
                    "external file endpoint {endpoint:?} has no host"
                )));
            }
        }

        let access_key_id = options
            .get("access_key_id")
            .or_else(|| options.get("ak"))
            .cloned();
        let secret_access_key = options
            .get("secret_access_key")
            .or_else(|| options.get("sk"))
            .cloned();
        if access_key_id.is_some() != secret_access_key.is_some() {
            return Err(DataFusionError::Plan(
                "external file server options must set both access_key_id and \
                 secret_access_key, or neither"
                    .to_string(),
            ));
        }

        let virtual_host_style = parse_bool_option(options, "virtual_host_style", true)?;
        let allow_http = parse_bool_option(
            options,
            "allow_http",
            endpoint
                .as_deref()
                .is_some_and(|endpoint| endpoint.starts_with("http://")),
        )?;
        Ok(Self {
            endpoint,
            access_key_id,
            secret_access_key,
            session_token: options.get("session_token").cloned(),
            region: options.get("region").cloned(),
            virtual_host_style,
            allow_http,
        })
    }

    /// Build the object store for the bucket in `object_store_url`.
    pub fn build_store(
        &self,
        object_store_url: &ObjectStoreUrl,
    ) -> DFResult<Arc<dyn ObjectStore>> {
        let url: &Url = object_store_url.as_ref();
        let bucket = url.host_str().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "external file object store URL {object_store_url} has no bucket"
            ))
        })?;
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(self.region.as_deref().unwrap_or("us-east-1"))
            .with_virtual_hosted_style_request(self.virtual_host_style)
            .with_allow_http(self.allow_http);
        if let Some(endpoint) = &self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(access_key_id) = &self.access_key_id {
            builder = builder.with_access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = &self.secret_access_key {
            builder = builder.with_secret_access_key(secret_access_key);
        }
        if let Some(session_token) = &self.session_token {
            builder = builder.with_token(session_token);
        }
        let store = builder.build().map_err(|error| {
            DataFusionError::Execution(format!(
                "build external file S3 store for {object_store_url}: {error}"
            ))
        })?;
        Ok(Arc::new(MonitoredObjectStore::new(Arc::new(store), "s3")))
    }
}

/// Concrete external-file format and its writer options.
#[derive(Clone)]
pub enum ExternalFileFormat {
    Parquet(TableParquetOptions),
    Csv(CsvOptions),
    Json(JsonOptions),
}

impl ExternalFileFormat {
    fn file_format(&self) -> Arc<dyn FileFormat> {
        match self {
            Self::Parquet(options) => {
                Arc::new(ParquetFormat::default().with_options(options.clone()))
            }
            Self::Csv(options) => {
                Arc::new(CsvFormat::default().with_options(options.clone()))
            }
            Self::Json(options) => {
                Arc::new(JsonFormat::default().with_options(options.clone()))
            }
        }
    }

    fn sink(&self, config: FileSinkConfig) -> DFResult<Arc<dyn DataSink>> {
        Ok(match self {
            Self::Parquet(options) => Arc::new(ParquetSink::new(config, options.clone())),
            Self::Csv(options) => {
                Arc::new(CsvSink::new(config, CsvWriterOptions::try_from(options)?))
            }
            Self::Json(options) => {
                Arc::new(JsonSink::new(config, JsonWriterOptions::try_from(options)?))
            }
        })
    }
}

/// Parsed table location and format.
#[derive(Clone)]
pub struct ExternalFileSpec {
    pub url: ListingTableUrl,
    pub listing_options: ListingOptions,
    pub format: ExternalFileFormat,
    /// Original FDW options, carried by the write plan so a worker can
    /// rebuild the format sink.
    pub options: BTreeMap<String, String>,
}

impl ExternalFileSpec {
    pub fn parse(options: &BTreeMap<String, String>) -> DFResult<Self> {
        reject_unknown(options, KNOWN_TABLE_OPTIONS, "foreign table")?;
        let path = options.get("path").ok_or_else(|| {
            DataFusionError::Plan(
                "external file foreign table requires OPTIONS (path 's3://...')"
                    .to_string(),
            )
        })?;
        let path = normalize_s3_path(path)?;
        let url = ListingTableUrl::parse(&path).map_err(|error| {
            DataFusionError::Plan(format!(
                "external file path {path:?} is not a valid listing URL: {error}"
            ))
        })?;
        let parsed = url.get_url();
        if parsed.scheme() != "s3" {
            return Err(DataFusionError::Plan(format!(
                "external file path {path:?} must use s3:// (s3a:// is normalized)"
            )));
        }
        if !parsed.has_host() {
            return Err(DataFusionError::Plan(format!(
                "external file path {path:?} has no bucket"
            )));
        }
        if parsed.query().is_some() || parsed.fragment().is_some() {
            return Err(DataFusionError::Plan(format!(
                "external file path {path:?} must not carry a query or fragment"
            )));
        }

        let format = match options
            .get("format")
            .map(|value| value.trim().to_ascii_lowercase())
        {
            Some(format) => format,
            None => infer_format(parsed.path()).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "external file path {path:?} has no recognizable extension; \
                     set OPTIONS (format 'parquet|csv|json')"
                ))
            })?,
        };
        let default_extension = match format.as_str() {
            "parquet" => ".parquet",
            "csv" => ".csv",
            "json" | "ndjson" => {
                if format == "ndjson" {
                    ".ndjson"
                } else {
                    ".json"
                }
            }
            other => {
                return Err(DataFusionError::Plan(format!(
                    "unsupported external file format {other:?}; use parquet, csv or json"
                )));
            }
        };
        let file_extension = options
            .get("file_extension")
            .cloned()
            .unwrap_or_else(|| default_extension.to_string());
        if !file_extension.starts_with('.') {
            return Err(DataFusionError::Plan(format!(
                "file_extension {file_extension:?} must start with '.'"
            )));
        }

        let format = match format.as_str() {
            "parquet" => ExternalFileFormat::Parquet(TableParquetOptions::default()),
            "csv" => ExternalFileFormat::Csv(csv_options(options)?),
            "json" => ExternalFileFormat::Json(json_options(options, false)?),
            "ndjson" => ExternalFileFormat::Json(json_options(options, true)?),
            _ => unreachable!(),
        };
        let listing_options =
            ListingOptions::new(format.file_format()).with_file_extension(file_extension);
        Ok(Self {
            url,
            listing_options,
            format,
            options: options.clone(),
        })
    }
}

fn csv_options(options: &BTreeMap<String, String>) -> DFResult<CsvOptions> {
    let mut format = CsvOptions::default().with_has_header(parse_bool_option(
        options,
        "has_header",
        false,
    )?);
    if let Some(value) = options.get("delimiter") {
        format = format.with_delimiter(parse_byte(value, "delimiter")?);
    }
    if let Some(value) = options.get("quote") {
        format = format.with_quote(parse_byte(value, "quote")?);
    }
    if let Some(value) = options.get("escape") {
        format = format.with_escape(Some(parse_byte(value, "escape")?));
    }
    if let Some(value) = options.get("comment") {
        format.comment = Some(parse_byte(value, "comment")?);
    }
    if let Some(value) = options.get("null_regex") {
        format.null_regex = Some(value.clone());
    }
    if options.contains_key("newlines_in_values") {
        format = format.with_newlines_in_values(parse_bool_option(
            options,
            "newlines_in_values",
            false,
        )?);
    }
    Ok(format)
}

fn json_options(
    options: &BTreeMap<String, String>,
    ndjson: bool,
) -> DFResult<JsonOptions> {
    let newline_delimited = if options.contains_key("newline_delimited") {
        parse_bool_option(options, "newline_delimited", !ndjson)?
    } else if options.contains_key("json_array") {
        !parse_bool_option(options, "json_array", false)?
    } else {
        true
    };
    let mut options = JsonOptions::default();
    options.newline_delimited = newline_delimited;
    Ok(options)
}

fn infer_format(path: &str) -> Option<String> {
    let path = path.trim_end_matches('/');
    let extension = path.rsplit('/').next()?.rsplit_once('.')?.1;
    match extension.to_ascii_lowercase().as_str() {
        "parquet" => Some("parquet".to_string()),
        "csv" => Some("csv".to_string()),
        "json" => Some("json".to_string()),
        "ndjson" => Some("ndjson".to_string()),
        _ => None,
    }
}

fn normalize_s3_path(path: &str) -> DFResult<String> {
    let path = path.trim();
    if let Some(rest) = path.strip_prefix("s3a://") {
        return Ok(format!("s3://{rest}"));
    }
    if path.starts_with("s3://") {
        return Ok(path.to_string());
    }
    Err(DataFusionError::Plan(format!(
        "external file path {path:?} must be an absolute s3:// URL"
    )))
}

fn reject_unknown(
    options: &BTreeMap<String, String>,
    known: &[&str],
    object: &str,
) -> DFResult<()> {
    for key in options.keys() {
        if !known.contains(&key.as_str()) {
            return Err(DataFusionError::Plan(format!(
                "unknown external file {object} option {key:?}; supported options: {}",
                known.join(", ")
            )));
        }
    }
    Ok(())
}

fn parse_bool_option(
    options: &BTreeMap<String, String>,
    key: &str,
    default: bool,
) -> DFResult<bool> {
    match options.get(key) {
        None => Ok(default),
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "on" | "yes" => Ok(true),
            "false" | "0" | "off" | "no" => Ok(false),
            other => Err(DataFusionError::Plan(format!(
                "option {key} must be a boolean, got {other:?}"
            ))),
        },
    }
}

fn parse_byte(value: &str, option: &str) -> DFResult<u8> {
    let value = match value {
        "\\t" => "\t",
        "\\n" => "\n",
        "\\r" => "\r",
        "\\\\" => "\\",
        other => other,
    };
    let mut bytes = value.bytes();
    let byte = bytes.next().ok_or_else(|| {
        DataFusionError::Plan(format!("option {option} must not be empty"))
    })?;
    if bytes.next().is_some() {
        return Err(DataFusionError::Plan(format!(
            "option {option} must be a single byte"
        )));
    }
    Ok(byte)
}

/// A `ListingTable` provider that registers its object store before scanning.
#[derive(Clone)]
pub struct ExternalFileTableProvider {
    inner: Arc<ListingTable>,
    spec: ExternalFileSpec,
    object_store_url: ObjectStoreUrl,
    store: Arc<dyn ObjectStore>,
    store_config: ExternalFileStoreConfig,
    table_name: String,
    path: String,
}

impl fmt::Debug for ExternalFileTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalFileTableProvider")
            .field("table_name", &self.table_name)
            .field("path", &self.path)
            .finish()
    }
}

impl ExternalFileTableProvider {
    pub fn try_new(
        table_name: String,
        options: &BTreeMap<String, String>,
        schema: SchemaRef,
        store_config: ExternalFileStoreConfig,
    ) -> DFResult<Self> {
        let spec = ExternalFileSpec::parse(options)?;
        let object_store_url = spec.url.object_store();
        let store = store_config.build_store(&object_store_url)?;
        Self::try_new_with_store(table_name, spec, schema, store_config, store)
    }

    /// Construct a provider with an already-created object store. Used by
    /// tests and by callers that share one store across several tables.
    pub fn try_new_with_store(
        table_name: String,
        spec: ExternalFileSpec,
        schema: SchemaRef,
        store_config: ExternalFileStoreConfig,
        store: Arc<dyn ObjectStore>,
    ) -> DFResult<Self> {
        let object_store_url = spec.url.object_store();
        let path = spec.url.to_string();
        let config = ListingTableConfig::new(spec.url.clone())
            .with_listing_options(spec.listing_options.clone())
            .with_schema(schema);
        let table = ListingTable::try_new(config)?;
        Ok(Self {
            inner: Arc::new(table),
            spec,
            object_store_url,
            store,
            store_config,
            table_name,
            path,
        })
    }

    fn register_store(&self, state: &dyn Session) {
        state.runtime_env().register_object_store(
            self.object_store_url.as_ref(),
            Arc::clone(&self.store),
        );
    }

    fn invalidate_list_cache(&self, state: &dyn Session) {
        if let Some(cache) = state.runtime_env().cache_manager.get_list_files_cache() {
            let key = TableScopedPath {
                table: self.spec.url.get_table_ref().clone(),
                path: self.spec.url.prefix().clone(),
            };
            let _ = cache.remove(&key);
        }
    }

    fn wrap_plan(&self, inner: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        // The wrapper is a single-partition distributed leaf. Coalescing here
        // keeps the credential-carrying node as the stage's leaf; file-level
        // parallelism is a later optimization that needs a leaf splitter.
        let inner = if inner.properties().output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(inner)) as Arc<dyn ExecutionPlan>
        } else {
            inner
        };
        Arc::new(ExternalFileScanExec {
            inner,
            object_store_url: self.object_store_url.clone(),
            store: Arc::clone(&self.store),
            store_config: self.store_config.clone(),
            table_name: self.table_name.clone(),
            path: self.path.clone(),
        })
    }
}

#[async_trait]
impl TableProvider for ExternalFileTableProvider {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<datafusion::common::Statistics> {
        self.inner.statistics()
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.register_store(state);
        let inner = self.inner.scan(state, projection, filters, limit).await?;
        Ok(self.wrap_plan(inner))
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> DFResult<ScanResult> {
        self.register_store(state);
        let result = self.inner.scan_with_args(state, args).await?;
        Ok(ScanResult::new(self.wrap_plan(result.into_inner())))
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if insert_op != InsertOp::Append {
            return Err(DataFusionError::NotImplemented(
                "INSERT OVERWRITE is not supported on external file tables; only append is supported"
                    .to_string(),
            ));
        }
        self.schema()
            .logically_equivalent_names_and_types(&input.schema())?;
        self.register_store(state);
        self.invalidate_list_cache(state);

        // One write task per input partition. A VALUES source has one partition
        // and therefore writes one file; a distributed SELECT keeps its scan
        // partitions and writes one file per partition.
        let parallelism = input
            .properties()
            .output_partitioning()
            .partition_count()
            .max(1);

        let write = Arc::new(ExternalFileWriteExec::try_new_with_store(
            input,
            self.spec.clone(),
            self.store_config.clone(),
            Arc::clone(&self.store),
            parallelism,
            self.table_name.clone(),
        )?);
        Ok(Arc::new(CoalescePartitionsExec::new(write)))
    }
}

/// Wrapper that carries S3 credentials to a worker and registers the object
/// store at execution time. Its child is the native DataFusion `DataSourceExec`.
#[derive(Clone)]
pub struct ExternalFileScanExec {
    inner: Arc<dyn ExecutionPlan>,
    object_store_url: ObjectStoreUrl,
    store: Arc<dyn ObjectStore>,
    store_config: ExternalFileStoreConfig,
    table_name: String,
    path: String,
}

impl fmt::Debug for ExternalFileScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalFileScanExec")
            .field("table_name", &self.table_name)
            .field("path", &self.path)
            .finish()
    }
}

impl DisplayAs for ExternalFileScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExternalFileScanExec table={} path={}",
            self.table_name, self.path
        )
    }
}

impl ExternalFileScanExec {
    pub fn try_new(
        inner: Arc<dyn ExecutionPlan>,
        object_store_url: ObjectStoreUrl,
        store_config: ExternalFileStoreConfig,
        table_name: String,
        path: String,
    ) -> DFResult<Self> {
        let store = store_config.build_store(&object_store_url)?;
        Ok(Self {
            inner,
            object_store_url,
            store,
            store_config,
            table_name,
            path,
        })
    }
}

impl ExecutionPlan for ExternalFileScanExec {
    fn name(&self) -> &str {
        "ExternalFileScanExec"
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This is a distributed leaf. Keeping the inner plan out of the
        // planner's child tree prevents DataFusion from splitting the scan
        // stage below the credential-carrying node.
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )
            -> DFResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "ExternalFileScanExec has no children".to_string(),
            ))
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "ExternalFileScanExec has no children".to_string(),
            ))
        }
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![]
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        self.inner.metrics()
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> DFResult<Arc<datafusion::common::Statistics>> {
        self.inner.partition_statistics(partition)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "ExternalFileScanExec has one partition, got {partition}"
            )));
        }
        context.runtime_env().register_object_store(
            self.object_store_url.as_ref(),
            Arc::clone(&self.store),
        );
        self.inner.execute(0, context)
    }
}

/// Writes appended rows through the native DataFusion file sink.
///
/// The input is repartitioned at planning time; each task writes its own part
/// files. There is no cross-task transaction or rollback.
#[derive(Clone)]
pub struct ExternalFileWriteExec {
    input: Arc<dyn ExecutionPlan>,
    sink: Arc<dyn DataSink>,
    spec: ExternalFileSpec,
    store: Arc<dyn ObjectStore>,
    store_config: ExternalFileStoreConfig,
    parallelism: usize,
    table_name: String,
    count_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for ExternalFileWriteExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalFileWriteExec")
            .field("table_name", &self.table_name)
            .field("path", &self.spec.url.to_string())
            .field("parallelism", &self.parallelism)
            .finish()
    }
}

impl DisplayAs for ExternalFileWriteExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ExternalFileWriteExec table={} path={} tasks={}",
            self.table_name, self.spec.url, self.parallelism
        )
    }
}

impl ExternalFileWriteExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        spec: ExternalFileSpec,
        store_config: ExternalFileStoreConfig,
        parallelism: usize,
        table_name: String,
    ) -> DFResult<Self> {
        let store = store_config.build_store(&spec.url.object_store())?;
        Self::try_new_with_store(
            input,
            spec,
            store_config,
            store,
            parallelism,
            table_name,
        )
    }

    pub fn try_new_with_store(
        input: Arc<dyn ExecutionPlan>,
        spec: ExternalFileSpec,
        store_config: ExternalFileStoreConfig,
        store: Arc<dyn ObjectStore>,
        parallelism: usize,
        table_name: String,
    ) -> DFResult<Self> {
        if parallelism == 0 {
            return Err(DataFusionError::Plan(
                "external file write parallelism must be greater than zero".to_string(),
            ));
        }
        if !spec.url.is_collection() {
            return Err(DataFusionError::Plan(format!(
                "INSERT INTO external file table requires a directory path; \
                 {} is a single object",
                spec.url
            )));
        }
        let object_store_url = spec.url.object_store();
        let file_extension = spec
            .listing_options
            .file_extension
            .trim_start_matches('.')
            .to_string();
        if file_extension.is_empty() {
            return Err(DataFusionError::Plan(
                "external file table has an empty file extension".to_string(),
            ));
        }
        let output_schema = input.schema();
        let config = FileSinkConfig {
            original_url: spec.url.to_string(),
            object_store_url,
            table_paths: vec![spec.url.clone()],
            file_group: FileGroup::new(Vec::new()),
            output_schema,
            table_partition_cols: Vec::new(),
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension,
            file_output_mode: FileOutputMode::Automatic,
        };
        let sink = spec.format.sink(config)?;
        let count_schema = make_count_schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&count_schema)),
            Partitioning::UnknownPartitioning(parallelism),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            sink,
            spec,
            store,
            store_config,
            parallelism,
            table_name,
            count_schema,
            properties,
        })
    }

    fn with_input(self: &Arc<Self>, input: Arc<dyn ExecutionPlan>) -> Self {
        let mut new = (**self).clone();
        new.input = input;
        new
    }
}

impl ExecutionPlan for ExternalFileWriteExec {
    fn name(&self) -> &str {
        "ExternalFileWriteExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.count_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        )
            -> DFResult<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> DFResult<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ExternalFileWriteExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(self.with_input(Arc::clone(&children[0]))))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "ExternalFileWriteExec expects exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(self.with_input(Arc::clone(&children[0]))))
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

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        self.sink.metrics()
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> DFResult<Arc<datafusion::common::Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition >= self.parallelism {
            return Err(DataFusionError::Internal(format!(
                "ExternalFileWriteExec partition {partition} is outside {} tasks",
                self.parallelism
            )));
        }
        context.runtime_env().register_object_store(
            self.spec.url.object_store().as_ref(),
            Arc::clone(&self.store),
        );
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let sink = Arc::clone(&self.sink);
        let count_schema = Arc::clone(&self.count_schema);
        let stream = stream::once(async move {
            let count = sink.write_all(input, &context).await?;
            make_count_batch(count)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema,
            stream,
        )))
    }
}

fn make_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn make_count_batch(count: u64) -> DFResult<RecordBatch> {
    RecordBatch::try_new(
        make_count_schema(),
        vec![Arc::new(UInt64Array::from(vec![count]))],
    )
    .map_err(|error| {
        DataFusionError::Execution(format!("build external file write count: {error}"))
    })
}

#[derive(Debug, Clone, Default)]
pub struct ExternalFileWriteCodec;

#[derive(Debug, Serialize, Deserialize)]
struct ExternalFileWriteWirePlan {
    table_name: String,
    parallelism: usize,
    store: ExternalFileStoreConfig,
    options: BTreeMap<String, String>,
}

impl PhysicalExtensionCodec for ExternalFileWriteCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan: ExternalFileWriteWirePlan =
            serde_json::from_slice(buf).map_err(|error| {
                DataFusionError::Internal(format!(
                    "decode external file write plan: {error}"
                ))
            })?;
        let input = inputs.first().ok_or_else(|| {
            DataFusionError::Internal(
                "external file write plan has no input plan".to_string(),
            )
        })?;
        let spec = ExternalFileSpec::parse(&plan.options)?;
        Ok(Arc::new(ExternalFileWriteExec::try_new(
            Arc::clone(input),
            spec,
            plan.store,
            plan.parallelism,
            plan.table_name,
        )?))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        let Some(exec) = node.downcast_ref::<ExternalFileWriteExec>() else {
            return Err(DataFusionError::NotImplemented(
                "ExternalFileWriteCodec only encodes ExternalFileWriteExec".to_string(),
            ));
        };
        let plan = ExternalFileWriteWirePlan {
            table_name: exec.table_name.clone(),
            parallelism: exec.parallelism,
            store: exec.store_config.clone(),
            options: exec.spec.options.clone(),
        };
        serde_json::to_writer(buf, &plan).map_err(|error| {
            DataFusionError::Internal(format!("encode external file write plan: {error}"))
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ExternalFileScanCodec;

#[derive(Debug, Serialize, Deserialize)]
struct ExternalFileWirePlan {
    object_store_url: String,
    table_name: String,
    path: String,
    store: ExternalFileStoreConfig,
    inner: Vec<u8>,
}

impl PhysicalExtensionCodec for ExternalFileScanCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        ctx: &TaskContext,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan: ExternalFileWirePlan =
            serde_json::from_slice(buf).map_err(|error| {
                DataFusionError::Internal(format!(
                    "decode external file scan plan: {error}"
                ))
            })?;
        let inner = PhysicalPlanNode::try_decode(&plan.inner)?
            .try_into_physical_plan_with_converter(ctx, self, proto_converter)?;
        let object_store_url = ObjectStoreUrl::parse(&plan.object_store_url)?;
        Ok(Arc::new(ExternalFileScanExec::try_new(
            inner,
            object_store_url,
            plan.store,
            plan.table_name,
            plan.path,
        )?))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> DFResult<()> {
        let Some(exec) = node.downcast_ref::<ExternalFileScanExec>() else {
            return Err(DataFusionError::NotImplemented(
                "ExternalFileScanCodec only encodes ExternalFileScanExec".to_string(),
            ));
        };
        let inner = PhysicalPlanNode::try_from_physical_plan_with_converter(
            Arc::clone(&exec.inner),
            self,
            proto_converter,
        )?
        .encode_to_vec();
        let plan = ExternalFileWirePlan {
            object_store_url: exec.object_store_url.to_string(),
            table_name: exec.table_name.clone(),
            path: exec.path.clone(),
            store: exec.store_config.clone(),
            inner,
        };
        serde_json::to_writer(buf, &plan).map_err(|error| {
            DataFusionError::Internal(format!("encode external file scan plan: {error}"))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::catalog::{CatalogProvider, SchemaProvider};
    use datafusion::datasource::physical_plan::{CsvSource, JsonSource};
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;
    use datafusion_proto::physical_plan::{
        AsExecutionPlan, DefaultPhysicalProtoConverter,
    };
    use datafusion_proto::protobuf::PhysicalPlanNode;
    use object_store::local::LocalFileSystem;
    use parquet::arrow::ArrowWriter;
    use std::fs::File;

    fn store_config() -> ExternalFileStoreConfig {
        ExternalFileStoreConfig {
            endpoint: Some("http://127.0.0.1:9000".to_string()),
            access_key_id: Some("access".to_string()),
            secret_access_key: Some("secret".to_string()),
            session_token: None,
            region: Some("us-east-1".to_string()),
            virtual_host_style: false,
            allow_http: true,
        }
    }

    #[test]
    fn parses_s3_server_options_and_redacts_secrets() {
        let mut options = BTreeMap::new();
        options.insert("endpoint".to_string(), "http://127.0.0.1:9000".to_string());
        options.insert("access_key_id".to_string(), "a".to_string());
        options.insert("secret_access_key".to_string(), "b".to_string());
        options.insert("virtual_host_style".to_string(), "false".to_string());
        let config = ExternalFileStoreConfig::from_options(&options).unwrap();
        assert_eq!(config.endpoint.as_deref(), Some("http://127.0.0.1:9000"));
        assert!(!config.virtual_host_style);
        assert!(config.allow_http);
        let debug = format!("{config:?}");
        assert!(!debug.contains("\"a\""));
        assert!(!debug.contains("\"b\""));
        assert!(debug.contains("<redacted>"));
    }

    #[test]
    fn parses_full_s3_path_and_infers_format() {
        let mut options = BTreeMap::new();
        options.insert(
            "path".to_string(),
            "s3a://bucket/events/part.parquet".to_string(),
        );
        let spec = ExternalFileSpec::parse(&options).unwrap();
        assert_eq!(spec.url.to_string(), "s3://bucket/events/part.parquet");
        assert_eq!(spec.listing_options.file_extension, ".parquet");
    }

    #[test]
    fn parses_csv_options() {
        let mut options = BTreeMap::new();
        options.insert("path".to_string(), "s3://bucket/events/".to_string());
        options.insert("format".to_string(), "csv".to_string());
        options.insert("has_header".to_string(), "true".to_string());
        options.insert("delimiter".to_string(), "\\t".to_string());
        let spec = ExternalFileSpec::parse(&options).unwrap();
        assert_eq!(spec.listing_options.file_extension, ".csv");
    }

    #[test]
    fn rejects_relative_path_and_unknown_option() {
        let mut options = BTreeMap::new();
        options.insert("path".to_string(), "events/".to_string());
        assert!(ExternalFileSpec::parse(&options).is_err());

        let mut options = BTreeMap::new();
        options.insert("path".to_string(), "s3://bucket/events/".to_string());
        options.insert("format".to_string(), "parquet".to_string());
        options.insert("unknown".to_string(), "true".to_string());
        assert!(ExternalFileSpec::parse(&options).is_err());
    }

    #[test]
    fn external_scan_codec_round_trips() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let url = ObjectStoreUrl::parse("s3://bucket").unwrap();
        let exec = ExternalFileScanExec::try_new(
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            url,
            store_config(),
            "events".to_string(),
            "s3://bucket/events/".to_string(),
        )
        .unwrap();
        let codec = ExternalFileScanCodec;
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(exec), &mut buf, &DefaultPhysicalProtoConverter {})
            .unwrap();
        let decoded = codec
            .try_decode(
                &buf,
                &[Arc::new(EmptyExec::new(schema))],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap();
        assert!(decoded.is::<ExternalFileScanExec>());
        assert!(!format!("{decoded:?}").contains("secret"));
    }

    #[test]
    fn external_scan_round_trips_through_the_proto_codec() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            ExternalFileScanExec::try_new(
                Arc::new(EmptyExec::new(Arc::clone(&schema))),
                ObjectStoreUrl::parse("s3://bucket").unwrap(),
                store_config(),
                "events".to_string(),
                "s3://bucket/events/".to_string(),
            )
            .unwrap(),
        );
        let codec = ExternalFileScanCodec;
        let proto = PhysicalPlanNode::try_from_physical_plan(Arc::clone(&exec), &codec)
            .expect("encode external scan");
        let decoded = proto
            .try_into_physical_plan(&TaskContext::default(), &codec)
            .expect("decode external scan");
        assert!(decoded.is::<ExternalFileScanExec>());
        assert_eq!(decoded.children().len(), 0);
        assert!(!format!("{decoded:?}").contains("secret"));
    }

    #[tokio::test]
    async fn provider_appends_csv_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("events")).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut options = BTreeMap::new();
        options.insert("path".to_string(), "s3://bucket/events/".to_string());
        options.insert("format".to_string(), "csv".to_string());
        options.insert("has_header".to_string(), "true".to_string());

        let spec = ExternalFileSpec::parse(&options).unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let provider = ExternalFileTableProvider::try_new_with_store(
            "written_events".to_string(),
            spec,
            Arc::clone(&schema),
            store_config(),
            store,
        )
        .unwrap();

        let context = SessionContext::new();
        let catalog = MemoryCatalogProvider::new();
        let schema_provider = MemorySchemaProvider::new();
        schema_provider
            .register_table("written_events".to_string(), Arc::new(provider))
            .unwrap();
        catalog
            .register_schema("public", Arc::new(schema_provider))
            .unwrap();
        context.register_catalog("external", Arc::new(catalog));

        context
            .sql(
                "INSERT INTO external.public.written_events VALUES (1, 'alice'), (2, 'bob')",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        context
            .sql("INSERT INTO external.public.written_events VALUES (3, 'carol')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let batches = context
            .sql("SELECT id, name FROM external.public.written_events ORDER BY id")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rows: Vec<(i64, Option<String>)> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let names = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| (ids.value(row), Some(names.value(row).to_string())))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            rows,
            vec![
                (1, Some("alice".to_string())),
                (2, Some("bob".to_string())),
                (3, Some("carol".to_string()))
            ]
        );
    }

    #[tokio::test]
    async fn provider_appends_json_and_parquet_files() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let context = SessionContext::new();
        let catalog = MemoryCatalogProvider::new();
        let schema_provider = MemorySchemaProvider::new();
        for format in ["json", "parquet"] {
            std::fs::create_dir_all(dir.path().join(format)).unwrap();
            let mut options = BTreeMap::new();
            options.insert("path".to_string(), format!("s3://bucket/{format}/"));
            options.insert("format".to_string(), format.to_string());
            let spec = ExternalFileSpec::parse(&options).unwrap();
            let table = format!("{format}_events");
            let provider = ExternalFileTableProvider::try_new_with_store(
                table.clone(),
                spec,
                Arc::clone(&schema),
                store_config(),
                Arc::clone(&store),
            )
            .unwrap();
            schema_provider
                .register_table(table.clone(), Arc::new(provider))
                .unwrap();
        }
        catalog
            .register_schema("public", Arc::new(schema_provider))
            .unwrap();
        context.register_catalog("external", Arc::new(catalog));

        for table in ["json_events", "parquet_events"] {
            context
                .sql(&format!(
                    "INSERT INTO external.public.{table} VALUES (1, 'alice'), (2, 'bob')"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let batches = context
                .sql(&format!(
                    "SELECT id, name FROM external.public.{table} ORDER BY id"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let rows: Vec<(i64, Option<String>)> = batches
                .iter()
                .flat_map(|batch| {
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let names = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    (0..batch.num_rows())
                        .map(|row| (ids.value(row), Some(names.value(row).to_string())))
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(
                rows,
                vec![(1, Some("alice".to_string())), (2, Some("bob".to_string()))],
                "table {table}"
            );
        }
    }

    #[test]
    fn write_codec_round_trips() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut options = BTreeMap::new();
        options.insert("path".to_string(), "s3://bucket/events/".to_string());
        options.insert("format".to_string(), "csv".to_string());
        let spec = ExternalFileSpec::parse(&options).unwrap();
        let exec = ExternalFileWriteExec::try_new(
            Arc::new(EmptyExec::new(Arc::clone(&schema))),
            spec,
            store_config(),
            1,
            "events".to_string(),
        )
        .unwrap();
        let codec = ExternalFileWriteCodec;
        let mut buf = Vec::new();
        codec
            .try_encode(Arc::new(exec), &mut buf, &DefaultPhysicalProtoConverter {})
            .unwrap();
        let decoded = codec
            .try_decode(
                &buf,
                &[Arc::new(EmptyExec::new(schema))],
                &TaskContext::default(),
                &DefaultPhysicalProtoConverter {},
            )
            .unwrap();
        assert!(decoded.is::<ExternalFileWriteExec>());
        assert_eq!(decoded.children().len(), 1);
    }

    #[tokio::test]
    async fn provider_reads_parquet_csv_and_json() {
        let dir = tempfile::tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        std::fs::create_dir_all(dir.path().join("parquet")).unwrap();
        std::fs::create_dir_all(dir.path().join("csv")).unwrap();
        std::fs::create_dir_all(dir.path().join("json")).unwrap();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();
        let file = File::create(dir.path().join("parquet/events.parquet")).unwrap();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        std::fs::write(
            dir.path().join("csv/events.csv"),
            "id,name\n1,alice\n2,bob\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("json/events.json"),
            "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n",
        )
        .unwrap();

        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let context = SessionContext::new();
        for (name, subdir, format, extra) in [
            ("parquet_events", "parquet", "parquet", None),
            ("csv_events", "csv", "csv", Some(("has_header", "true"))),
            ("json_events", "json", "json", None),
        ] {
            let mut options = BTreeMap::new();
            options.insert("path".to_string(), format!("s3://bucket/{subdir}/"));
            options.insert("format".to_string(), format.to_string());
            if let Some((key, value)) = extra {
                options.insert(key.to_string(), value.to_string());
            }
            let spec = ExternalFileSpec::parse(&options).unwrap();
            let provider = ExternalFileTableProvider::try_new_with_store(
                name.to_string(),
                spec,
                Arc::clone(&schema),
                store_config(),
                Arc::clone(&store),
            )
            .unwrap();
            context.register_table(name, Arc::new(provider)).unwrap();
        }

        for table in ["parquet_events", "csv_events", "json_events"] {
            let batches = context
                .sql(&format!("SELECT id, name FROM {table} ORDER BY id"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let rows: Vec<(i64, Option<String>)> = batches
                .iter()
                .flat_map(|batch| {
                    let ids = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    let names = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    (0..batch.num_rows())
                        .map(|row| (ids.value(row), Some(names.value(row).to_string())))
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(
                rows,
                vec![(1, Some("alice".to_string())), (2, Some("bob".to_string()))]
            );
        }
    }

    #[test]
    fn csv_and_json_sources_have_a_distributed_wire_form() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let table_schema = TableSchema::new(Arc::clone(&schema), Vec::new());
        assert!(crate::distributed::codec::source_has_wire_form(
            &CsvSource::new(table_schema.clone())
        ));
        assert!(crate::distributed::codec::source_has_wire_form(
            &JsonSource::new(table_schema)
        ));
    }

    #[test]
    fn external_scan_round_trips_through_the_composed_codec() {
        let schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            ExternalFileScanExec::try_new(
                Arc::new(EmptyExec::new(schema)),
                ObjectStoreUrl::parse("s3://bucket").unwrap(),
                store_config(),
                "events".to_string(),
                "s3://bucket/events/".to_string(),
            )
            .unwrap(),
        );
        let decoded = crate::distributed::codec::roundtrip::exec(exec)
            .expect("composed codec roundtrip");
        assert!(decoded.is::<ExternalFileScanExec>());
        assert_eq!(decoded.children().len(), 0);
        assert!(!format!("{decoded:?}").contains("secret"));
    }
}
