// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::anyhow;
use arrow::error::ArrowError;
use arrow_schema::{Schema, SchemaRef};
pub use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::Expr;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::push_down_projection::PushDownProjection;
use datafusion::optimizer::rewrite_disjunctive_predicate::RewriteDisjunctivePredicate;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::unwrap_cast_in_comparison::UnwrapCastInComparison;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::DataFusionError::{External, ObjectStore};
use datafusion_substrait::substrait::proto::Plan;
use derivative::Derivative;
use log::info;
use object_store::aws::AmazonS3Builder;
use object_store::{ClientOptions, RetryConfig};
use tracing::debug;
use url::{ParseError, Url};

#[cfg(feature = "hdfs")]
use crate::hdfs::Hdfs;

#[derive(Debug, Derivative)]
#[derivative(Clone)]
pub struct IOSchema(pub(crate) SchemaRef);

impl Default for IOSchema {
    fn default() -> Self {
        IOSchema(Arc::new(Schema::empty()))
    }
}

pub static OPTION_KEY_KEEP_ORDERS: &str = "keep_orders";
pub static OPTION_DEFAULT_VALUE_KEEP_ORDERS: &str = "false";

pub static OPTION_KEY_MEM_LIMIT: &str = "mem_limit";
pub static OPTION_KEY_POOL_SIZE: &str = "pool_size";
pub static OPTION_KEY_HASH_BUCKET_ID : &str = "hash_bucket_id";
pub static OPTION_KEY_MAX_FILE_SIZE: &str = "max_file_size";



#[derive(Debug, Derivative)]
#[derivative(Default, Clone)]
pub struct LakeSoulIOConfig {
    // unescaped root dir path of files
    pub(crate) prefix: String,
    // unescaped file paths to read or write
    pub(crate) files: Vec<String>,
    // primary key column names
    pub(crate) primary_keys: Vec<String>,
    // range partitions column names
    pub(crate) range_partitions: Vec<String>,
    // number of hash bucket
    #[derivative(Default(value = "1"))]
    pub(crate) hash_bucket_num: usize,
    // selecting columns
    pub(crate) columns: Vec<String>,
    // auxiliary sorting columns
    pub(crate) aux_sort_cols: Vec<String>,

    // filtering predicates
    pub(crate) filter_strs: Vec<String>,
    pub(crate) filters: Vec<Expr>,
    pub(crate) filter_protos: Vec<Plan>,
    // read or write batch size
    #[derivative(Default(value = "8192"))]
    pub(crate) batch_size: usize,
    // write row group max row num
    #[derivative(Default(value = "250000"))]
    pub(crate) max_row_group_size: usize,
    // write row group max num of values
    #[derivative(Default(value = "2147483647"))]
    pub(crate) max_row_group_num_values: usize,
    #[derivative(Default(value = "1"))]
    pub(crate) prefetch_size: usize,
    #[derivative(Default(value = "false"))]
    pub(crate) parquet_filter_pushdown: bool,

    // arrow schema
    pub(crate) target_schema: IOSchema,

    // arrow schema of partition columns
    pub(crate) partition_schema: IOSchema,

    // object store related configs
    pub(crate) object_store_options: HashMap<String, String>,

    // merge operators
    pub(crate) merge_operators: HashMap<String, String>,

    // default column value
    pub(crate) default_column_value: HashMap<String, String>,

    // tokio runtime related configs
    #[derivative(Default(value = "2"))]
    pub(crate) thread_num: usize,

    // to be compatible with hadoop's fs.defaultFS
    pub(crate) default_fs: String,

    pub(super) options: HashMap<String, String>,

    // if dynamic partition
    #[derivative(Default(value = "false"))]
    pub(crate) use_dynamic_partition: bool,

    // if inferring schema
    #[derivative(Default(value = "false"))]
    pub(crate) inferring_schema: bool,

    #[derivative(Default(value = "None"))]
    pub(crate) memory_limit: Option<usize>,

    #[derivative(Default(value = "None"))]
    pub(crate) memory_pool_size: Option<usize>,

    // max file size of bytes
    #[derivative(Default(value = "None"))]
    pub(crate) max_file_size: Option<u64>,
}

impl LakeSoulIOConfig {
    pub fn target_schema(&self) -> SchemaRef {
        self.target_schema.0.clone()
    }

    pub fn partition_schema(&self) -> SchemaRef {
        self.partition_schema.0.clone()
    }

    pub fn primary_keys_slice(&self) -> &[String] {
        &self.primary_keys
    }

    pub fn range_partitions_slice(&self) -> &[String] {
        &self.range_partitions
    }

    pub fn files_slice(&self) -> &[String] {
        &self.files
    }

    pub fn aux_sort_cols_slice(&self) -> &[String] {
        &self.aux_sort_cols
    }

    pub fn option(&self, key: &str) -> Option<&String> {
        self.options.get(key)
    }

    pub fn prefix(&self) -> &String {
        &self.prefix
    }

    pub fn keep_ordering(&self) -> bool {
        self.option(OPTION_KEY_KEEP_ORDERS).map_or(false, |x| x.eq("true"))
    }

    pub fn mem_limit(&self) -> Option<usize> {
        self.option(OPTION_KEY_MEM_LIMIT).map(|x| x.parse().unwrap())
    }

    pub fn pool_size(&self) -> Option<usize> {
        self.option(OPTION_KEY_POOL_SIZE).map(|x| x.parse().unwrap())
    }

    pub fn hash_bucket_id(&self) -> usize {
        self.option(OPTION_KEY_HASH_BUCKET_ID).map_or(0, |x| x.parse().unwrap())
    }
}

#[derive(Derivative, Debug)]
#[derivative(Clone, Default)]
pub struct LakeSoulIOConfigBuilder {
    config: LakeSoulIOConfig,
}

impl LakeSoulIOConfigBuilder {
    pub fn new() -> Self {
        LakeSoulIOConfigBuilder {
            config: LakeSoulIOConfig::default(),
        }
    }

    pub fn with_prefix(mut self, prefix: String) -> Self {
        self.config.prefix = prefix;
        self
    }

    pub fn with_file(mut self, file: String) -> Self {
        self.config.files.push(file);
        self
    }

    pub fn with_files(mut self, files: Vec<String>) -> Self {
        self.config.files = files;
        self
    }

    pub fn with_primary_key(mut self, pks: String) -> Self {
        self.config.primary_keys.push(pks);
        self
    }

    pub fn with_primary_keys(mut self, pks: Vec<String>) -> Self {
        self.config.primary_keys = pks;
        self
    }

    pub fn with_range_partition(mut self, range_partition: String) -> Self {
        self.config.range_partitions.push(range_partition);
        self
    }

    pub fn with_range_partitions(mut self, range_partitions: Vec<String>) -> Self {
        self.config.range_partitions = range_partitions;
        self
    }

    pub fn with_hash_bucket_num(mut self, hash_bucket_num: usize) -> Self {
        self.config.hash_bucket_num = hash_bucket_num;
        self
    }

    pub fn with_column(mut self, col: String) -> Self {
        self.config.columns.push(String::from(&col));
        self
    }

    pub fn with_aux_sort_column(mut self, col: String) -> Self {
        self.config.aux_sort_cols.push(String::from(&col));
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.config.batch_size = batch_size;
        self
    }

    pub fn with_max_row_group_size(mut self, max_row_group_size: usize) -> Self {
        self.config.max_row_group_size = max_row_group_size;
        self
    }

    pub fn with_max_row_group_num_values(mut self, max_row_group_num_values: usize) -> Self {
        self.config.max_row_group_num_values = max_row_group_num_values;
        self
    }

    pub fn with_prefetch_size(mut self, prefetch_size: usize) -> Self {
        self.config.prefetch_size = prefetch_size;
        self
    }

    pub fn with_parquet_filter_pushdown(mut self, enable: bool) -> Self {
        self.config.parquet_filter_pushdown = enable;
        self
    }

    pub fn with_columns(mut self, cols: Vec<String>) -> Self {
        self.config.columns = cols;
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.config.target_schema = IOSchema(schema);
        self
    }

    pub fn with_partition_schema(mut self, schema: SchemaRef) -> Self {
        self.config.partition_schema = IOSchema(schema);
        self
    }

    pub fn with_filter_str(mut self, filter_str: String) -> Self {
        self.config.filter_strs.push(filter_str);
        self
    }

    pub fn with_filter_proto(mut self, filter_proto: Plan) -> Self {
        self.config.filter_protos.push(filter_proto);
        self
    }

    pub fn with_filters(mut self, filters: Vec<Expr>) -> Self {
        self.config.filters = filters;
        self
    }

    pub fn with_merge_op(mut self, field_name: String, merge_op: String) -> Self {
        self.config.merge_operators.insert(field_name, merge_op);
        self
    }

    pub fn with_default_column_value(mut self, field_name: String, value: String) -> Self {
        self.config.default_column_value.insert(field_name, value);
        self
    }

    pub fn with_object_store_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.object_store_options.insert(key.into(), value.into());
        self
    }

    pub fn with_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.options.insert(key.into(), value.into());
        self
    }

    pub fn with_thread_num(mut self, thread_num: usize) -> Self {
        self.config.thread_num = thread_num;
        self
    }

    pub fn set_dynamic_partition(mut self, enable: bool) -> Self {
        self.config.use_dynamic_partition = enable;
        self
    }

    pub fn set_inferring_schema(mut self, enable: bool) -> Self {
        self.config.inferring_schema = enable;
        self
    }

    pub fn with_max_file_size(mut self, size: u64) -> Self {
        self.config.max_file_size = Some(size);
        self
    }

    pub fn build(self) -> LakeSoulIOConfig {
        self.config
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
}

impl From<LakeSoulIOConfig> for LakeSoulIOConfigBuilder {
    fn from(val: LakeSoulIOConfig) -> Self {
        LakeSoulIOConfigBuilder { config: val }
    }
}

/// First check envs for credentials, region and endpoint.
/// Second check fs.s3a.xxx, to keep compatible with hadoop s3a.
/// If no region is provided, default to us-east-1.
/// Bucket name would be retrieved from file names.
/// Currently only one s3 object store with one bucket is supported.
pub fn register_s3_object_store(url: &Url, config: &LakeSoulIOConfig, runtime: &RuntimeEnv) -> Result<()> {
    let key = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .or_else(|| config.object_store_options.get("fs.s3a.access.key").cloned());
    let secret = std::env::var("AWS_SECRET_ACCESS_KEY")
        .ok()
        .or_else(|| config.object_store_options.get("fs.s3a.secret.key").cloned());
    let region = std::env::var("AWS_REGION").ok().or_else(|| {
        std::env::var("AWS_DEFAULT_REGION")
            .ok()
            .or_else(|| config.object_store_options.get("fs.s3a.endpoint.region").cloned())
    });
    let mut endpoint = std::env::var("AWS_ENDPOINT")
        .ok()
        .or_else(|| config.object_store_options.get("fs.s3a.endpoint").cloned());
    let bucket = config.object_store_options.get("fs.s3a.bucket").cloned();
    let virtual_path_style = config.object_store_options.get("fs.s3a.path.style.access").cloned();
    let virtual_path_style = virtual_path_style.is_some_and(|s| s == "true");
    if !virtual_path_style {
        if let (Some(endpoint_str), Some(bucket)) = (&endpoint, &bucket) {
            // for host style access with endpoint defined, we need to check endpoint contains bucket name
            if !endpoint_str.contains(bucket) {
                let mut endpoint_url = Url::parse(endpoint_str.as_str()).map_err(|e| External(Box::new(e)))?;
                endpoint_url
                    .set_host(Some(&*format!(
                        "{}.{}",
                        bucket,
                        endpoint_url
                            .host_str()
                            .ok_or(External(anyhow!("endpoint host missing").into()))?
                    )))
                    .map_err(|e| External(Box::new(e)))?;
                let endpoint_s = endpoint_url.to_string();
                endpoint = endpoint_s.strip_suffix('/').map(|s| s.to_string()).or(Some(endpoint_s));
            }
        }
    }

    if bucket.is_none() {
        return Err(DataFusionError::ArrowError(ArrowError::InvalidArgumentError(
            "missing fs.s3a.bucket".to_string(),
        )));
    }

    let retry_config = RetryConfig::default();
    let mut s3_store_builder = AmazonS3Builder::new()
        .with_region(region.unwrap_or_else(|| "us-east-1".to_owned()))
        .with_bucket_name(bucket.unwrap())
        .with_retry(retry_config)
        .with_virtual_hosted_style_request(!virtual_path_style)
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(true)
                .with_connect_timeout(Duration::from_secs(10))
                .with_pool_idle_timeout(Duration::from_secs(300))
                .with_timeout(Duration::from_secs(10)),
        )
        .with_allow_http(true);
    if let (Some(k), Some(s)) = (key, secret) {
        s3_store_builder = s3_store_builder.with_access_key_id(k).with_secret_access_key(s);
    }
    if let Some(ep) = endpoint {
        s3_store_builder = s3_store_builder.with_endpoint(ep);
    }
    let s3_store = Arc::new(s3_store_builder.build()?);
    runtime.register_object_store(url, s3_store);
    Ok(())
}

fn register_hdfs_object_store(
    _url: &Url,
    _host: &str,
    _config: &LakeSoulIOConfig,
    _runtime: &RuntimeEnv,
) -> Result<()> {
    #[cfg(not(feature = "hdfs"))]
    {
        Err(DataFusionError::ObjectStore(object_store::Error::NotSupported {
            source: "hdfs support is not enabled".into(),
        }))
    }
    #[cfg(feature = "hdfs")]
    {
        let hdfs = Hdfs::try_new(_host, _config.clone())?;
        _runtime.register_object_store(_url, Arc::new(hdfs));
        Ok(())
    }
}

// try to register object store of this path string, and return normalized path string if
// this path is local path style but fs.defaultFS config exists
fn register_object_store(path: &str, config: &mut LakeSoulIOConfig, runtime: &RuntimeEnv) -> Result<String> {
    let url = Url::parse(path);
    match url {
        Ok(url) => match url.scheme() {
            "s3" | "s3a" => {
                if runtime
                    .object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?)
                    .is_ok()
                {
                    return Ok(path.to_owned());
                }
                if !config.object_store_options.contains_key("fs.s3a.bucket") {
                    config.object_store_options.insert(
                        "fs.s3a.bucket".to_string(),
                        url.host_str()
                            .ok_or(DataFusionError::Internal("host str missing".to_string()))?
                            .to_string(),
                    );
                }
                register_s3_object_store(&url, config, runtime)?;
                Ok(path.to_owned())
            }
            "hdfs" => {
                if url.has_host() {
                    if runtime
                        .object_store(ObjectStoreUrl::parse(&url[..url::Position::BeforePath])?)
                        .is_ok()
                    {
                        return Ok(path.to_owned());
                    }
                    register_hdfs_object_store(
                        &url,
                        &url[url::Position::BeforeHost..url::Position::BeforePath],
                        config,
                        runtime,
                    )?;
                    Ok(path.to_owned())
                } else {
                    // defaultFS should have been registered with hdfs,
                    // and we convert hdfs://user/hadoop/file to
                    // hdfs://defaultFS/user/hadoop/file
                    let path = url.path().trim_start_matches('/');
                    let joined_path = [config.default_fs.as_str(), path].join("/");
                    Ok(joined_path)
                }
            }
            "file" => Ok(path.to_owned()),
            _ => Err(ObjectStore(object_store::Error::NotSupported {
                source: "FileSystem not supported".into(),
            })),
        },
        Err(ParseError::RelativeUrlWithoutBase) => {
            let path = path.trim_start_matches('/');
            if config.default_fs.is_empty() {
                // local filesystem
                Ok(["file://", path].join("/"))
            } else {
                // concat default fs and path
                let joined_path = [config.default_fs.as_str(), path].join("/");
                Ok(joined_path)
            }
        }
        Err(e) => Err(DataFusionError::External(Box::new(e))),
    }
}

pub fn create_session_context(config: &mut LakeSoulIOConfig) -> Result<SessionContext> {
    create_session_context_with_planner(config, None)
}

pub fn create_session_context_with_planner(
    config: &mut LakeSoulIOConfig,
    planner: Option<Arc<dyn QueryPlanner + Send + Sync>>,
) -> Result<SessionContext> {
    let mut sess_conf = SessionConfig::default()
        .with_batch_size(config.batch_size)
        .with_parquet_pruning(true)
        .with_prefetch(config.prefetch_size)
        .with_information_schema(true)
        .with_create_default_catalog_and_schema(true);

    sess_conf.options_mut().optimizer.enable_round_robin_repartition = false; // if true, the record_batches poll from stream become unordered
    sess_conf.options_mut().optimizer.prefer_hash_join = false; //if true, panicked at 'range end out of bounds'
    sess_conf.options_mut().execution.parquet.pushdown_filters = config.parquet_filter_pushdown;
    sess_conf.options_mut().execution.target_partitions = 1;
    // sess_conf.options_mut().execution.sort_in_place_threshold_bytes = 16 * 1024;
    // sess_conf.options_mut().execution.sort_spill_reservation_bytes = 2 * 1024 * 1024;
    // sess_conf.options_mut().catalog.default_catalog = "lakesoul".into();

    let mut runtime_conf = RuntimeConfig::new();
    if let Some(pool_size) = config.pool_size() {
        let memory_pool = FairSpillPool::new(pool_size);
        dbg!(&memory_pool);
        runtime_conf = runtime_conf.with_memory_pool(Arc::new(memory_pool));
    }
    let runtime = RuntimeEnv::new(runtime_conf)?;

    // firstly parse default fs if exist
    let default_fs = config
        .object_store_options
        .get("fs.defaultFS")
        .or_else(|| config.object_store_options.get("fs.default.name"))
        .cloned();
    if let Some(fs) = default_fs {
        config.default_fs = fs.clone();
        info!("NativeIO register default fs {}", fs);
        register_object_store(&fs, config, &runtime)?;
    };

    if !config.prefix.is_empty() {
        let prefix = config.prefix.clone();
        info!("NativeIO register prefix fs {}", prefix);
        let normalized_prefix = register_object_store(&prefix, config, &runtime)?;
        config.prefix = normalized_prefix;
    }
    debug!("{}", &config.prefix);

    // register object store(s) for input/output files' path
    // and replace file names with default fs concatenated if exist
    let files = config.files.clone();
    let normalized_filenames = files
        .into_iter()
        .map(|file_name| register_object_store(&file_name, config, &runtime))
        .collect::<Result<Vec<String>>>()?;
    config.files = normalized_filenames;
    info!("NativeIO normalized file names: {:?}", config.files);
    info!("NativeIO final config: {:?}", config);

    // create session context
    let mut state = if let Some(planner) = planner {
        SessionState::new_with_config_rt(sess_conf, Arc::new(runtime)).with_query_planner(planner)
    } else {
        SessionState::new_with_config_rt(sess_conf, Arc::new(runtime))
    };
    // only keep projection/filter rules as others are unnecessary
    let physical_opt_rules = state
        .physical_optimizers()
        .iter()
        .filter_map(|r| {
            // this rule is private mod in datafusion, so we use name to filter out it
            if r.name() == "ProjectionPushdown" {
                Some(r.clone())
            } else {
                None
            }
        })
        .collect();
    state = state
        .with_analyzer_rules(vec![Arc::new(TypeCoercion {})])
        .with_optimizer_rules(vec![
            Arc::new(PushDownFilter {}),
            Arc::new(PushDownProjection {}),
            Arc::new(SimplifyExpressions {}),
            Arc::new(UnwrapCastInComparison {}),
            Arc::new(RewriteDisjunctivePredicate {}),
        ])
        .with_physical_optimizer_rules(physical_opt_rules);

    Ok(SessionContext::new_with_state(state))
}

#[cfg(test)]
mod tests {
    use crate::lakesoul_io_config::{create_session_context, LakeSoulIOConfigBuilder};

    #[test]
    fn test_path_normalize() {
        let mut conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![
                "file:///some/absolute/local/file1".into(),
                "/some/absolute/local/file2".into(),
            ])
            .build();
        let _sess_ctx = create_session_context(&mut conf).unwrap();
        assert_eq!(
            conf.files,
            vec![
                "file:///some/absolute/local/file1".to_string(),
                "file:///some/absolute/local/file2".to_string(),
            ]
        );
    }
}
