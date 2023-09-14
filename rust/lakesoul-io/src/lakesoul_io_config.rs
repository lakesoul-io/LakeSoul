// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arrow::error::ArrowError;
use arrow_schema::{Schema, SchemaRef};
use datafusion::datasource::object_store::ObjectStoreUrl;
pub use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::Expr;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::DataFusionError::ObjectStore;
use derivative::Derivative;
use object_store::aws::AmazonS3Builder;
use object_store::RetryConfig;
use std::collections::HashMap;
use std::sync::Arc;
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

#[derive(Debug, Derivative)]
#[derivative(Default, Clone)]
pub struct LakeSoulIOConfig {
    // files to read or write
    pub(crate) files: Vec<String>,
    // primary key column names
    pub(crate) primary_keys: Vec<String>,
    // selecting columns
    pub(crate) columns: Vec<String>,
    // auxiliary sorting columns
    pub(crate) aux_sort_cols: Vec<String>,

    // filtering predicates
    pub(crate) filter_strs: Vec<String>,
    pub(crate) filters: Vec<Expr>,
    // read or write batch size
    #[derivative(Default(value = "8192"))]
    pub(crate) batch_size: usize,
    // write row group max row num
    #[derivative(Default(value = "250000"))]
    pub(crate) max_row_group_size: usize,
    #[derivative(Default(value = "2"))]
    pub(crate) prefetch_size: usize,

    // arrow schema
    pub(crate) schema: IOSchema,

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
}

#[derive(Derivative)]
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

    pub fn with_prefetch_size(mut self, prefetch_size: usize) -> Self {
        self.config.prefetch_size = prefetch_size;
        self
    }

    pub fn with_columns(mut self, cols: Vec<String>) -> Self {
        self.config.columns = cols;
        self
    }

    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.config.schema = IOSchema(schema);
        self
    }

    pub fn with_filter_str(mut self, filter_str: String) -> Self {
        self.config.filter_strs.push(filter_str);
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

    pub fn with_object_store_option(mut self, key: String, value: String) -> Self {
        self.config.object_store_options.insert(key, value);
        self
    }

    pub fn with_thread_num(mut self, thread_num: usize) -> Self {
        self.config.thread_num = thread_num;
        self
    }

    pub fn build(self) -> LakeSoulIOConfig {
        self.config
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
    let endpoint = std::env::var("AWS_ENDPOINT")
        .ok()
        .or_else(|| config.object_store_options.get("fs.s3a.endpoint").cloned());
    let bucket = config.object_store_options.get("fs.s3a.bucket").cloned();

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
                    config
                        .object_store_options
                        .insert("fs.s3a.bucket".to_string(), url.host_str().unwrap().to_string());
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
    let mut sess_conf = SessionConfig::default()
        .with_batch_size(config.batch_size)
        .with_parquet_pruning(false)
        .with_prefetch(config.prefetch_size);

    sess_conf.options_mut().optimizer.enable_round_robin_repartition = false; // if true, the record_batches poll from stream become unordered
    sess_conf.options_mut().optimizer.prefer_hash_join = false; //if true, panicked at 'range end out of bounds'

    // limit memory for sort writer
    let runtime = RuntimeEnv::new(RuntimeConfig::new().with_memory_limit(128 * 1024 * 1024, 1.0))?;

    // firstly parse default fs if exist
    let default_fs = config
        .object_store_options
        .get("fs.defaultFS")
        .or_else(|| config.object_store_options.get("fs.default.name"))
        .cloned();
    if let Some(fs) = default_fs {
        config.default_fs = fs.clone();
        register_object_store(&fs, config, &runtime)?;
    };

    // register object store(s) for input/output files' path
    // and replace file names with default fs concatenated if exist
    let files = config.files.clone();
    let normalized_filenames = files
        .into_iter()
        .map(|file_name| register_object_store(&file_name, config, &runtime))
        .collect::<Result<Vec<String>>>()?;
    config.files = normalized_filenames;

    // create session context
    Ok(SessionContext::with_config_rt(sess_conf, Arc::new(runtime)))
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
