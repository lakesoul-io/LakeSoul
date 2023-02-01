/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::filter::Parser as FilterParser;
use arrow::error::ArrowError;
pub use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::Expr;
use datafusion::prelude::{SessionConfig, SessionContext};
use derivative::Derivative;
use object_store::aws::AmazonS3Builder;
use object_store::RetryConfig;
use std::collections::HashMap;
use std::sync::Arc;
use url::Url;

#[derive(Derivative)]
#[derivative(Default)]
pub struct LakeSoulIOConfig {
    // files to read or write
    pub(crate) files: Vec<String>,
    // primary key column names
    pub(crate) primary_keys: Vec<String>,
    // selecting columns
    pub(crate) columns: Vec<String>,

    // filtering predicates
    pub(crate) filters: Vec<Expr>,
    // read or write batch size
    pub(crate) batch_size: usize,
    // write row group max row num
    #[derivative(Default(value = "250000"))]
    pub(crate) max_row_group_size: usize,
    #[derivative(Default(value = "2"))]
    pub(crate) prefetch_size: usize,

    // arrow schema in json for read and write
    pub(crate) schema_json: String,

    // object store related configs
    pub(crate) object_store_options: HashMap<String, String>,

    // tokio runtime related configs
    #[derivative(Default(value = "2"))]
    pub(crate) thread_num: usize,
}

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

    pub fn with_primary_keys(mut self, pks: Vec<String>) -> Self {
        self.config.primary_keys = pks;
        self
    }

    pub fn with_column(mut self, col: String) -> Self {
        self.config.columns.push(String::from(&col));
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

    pub fn with_schema_json(mut self, json_str: String) -> Self {
        self.config.schema_json = json_str;
        self
    }

    pub fn with_filter_str(mut self, filter_str: String) -> Self {
        let expr = FilterParser::parse(filter_str, &self.config.schema_json);
        self.config.filters.push(expr);
        self
    }

    pub fn with_filters(mut self, filters: Vec<Expr>) -> Self {
        self.config.filters = filters;
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
pub fn register_s3_object_store(config: &LakeSoulIOConfig, runtime: &RuntimeEnv) -> Result<()> {
    let key = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .or_else(|| config.object_store_options.get("fs.s3a.access.key").cloned());
    let secret = std::env::var("AWS_ACCESS_KEY_ID")
        .ok()
        .or_else(|| config.object_store_options.get("fs.s3a.access.secret").cloned());
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
        .with_region(region.unwrap_or("us-east-1".to_string()))
        .with_bucket_name(bucket.clone().unwrap())
        .with_retry(retry_config)
        .with_allow_http(true);
    match (key, secret) {
        (Some(k), Some(s)) => {
            s3_store_builder = s3_store_builder.with_access_key_id(k).with_secret_access_key(s);
        }
        _ => {}
    }
    if let Some(ep) = endpoint {
        s3_store_builder = s3_store_builder.with_endpoint(ep);
    }
    let s3_store = s3_store_builder.build()?;
    runtime.register_object_store("s3", bucket.unwrap(), Arc::new(s3_store));
    Ok(())
}

pub fn create_session_context(config: &mut LakeSoulIOConfig) -> Result<SessionContext> {
    let sess_conf = SessionConfig::default()
        .with_batch_size(config.batch_size)
        .with_prefetch(config.prefetch_size);
    let runtime = RuntimeEnv::new(RuntimeConfig::new())?;

    // register object store(s)
    for file_name in &config.files {
        let url = Url::parse(file_name.as_str());
        let s3_registered = match url {
            Ok(url) => {
                if url.scheme() == "s3" || url.scheme() == "s3a" {
                    if !config.object_store_options.contains_key("fs.s3a.bucket") {
                        config
                            .object_store_options
                            .insert("fs.s3a.bucket".to_string(), url.host_str().unwrap().to_string());
                    }
                    register_s3_object_store(config, &runtime)?;
                    true
                } else {
                    false
                }
            }
            Err(_) => false,
        };
        if s3_registered {
            break;
        }
    }

    // create session context
    Ok(SessionContext::with_config_rt(sess_conf, Arc::new(runtime)))
}
