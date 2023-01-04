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

use datafusion::logical_expr::Expr;
use std::collections::HashMap;
use std::sync::Arc;
use arrow::error::ArrowError;
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::{SessionConfig, SessionContext};
pub use datafusion::error::{DataFusionError, Result};
use object_store::aws::AmazonS3Builder;
use object_store::RetryConfig;
use crate::filter::Parser as FilterParser;
use derivative::Derivative;

#[derive(Derivative)]
#[derivative(Default)]
pub struct LakeSoulIOConfig {
    // files to read or write
    pub(crate) files: Vec<String>,
    // primary key column names
    primary_keys: Vec<String>,
    // selecting columns
    pub(crate) columns: Vec<String>,
    schema: HashMap<String, String>,

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

    pub fn with_column(mut self, col: String, datatype: String) -> Self {
        self.config.columns.push(String::from(&col));
        self.config.schema.insert(String::from(&col), datatype);
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
        let expr = FilterParser::parse(filter_str, &self.config.schema);
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

pub fn register_s3_object_store(config: &LakeSoulIOConfig, runtime: &RuntimeEnv) -> Result<()> {
    let key = config.object_store_options.get("fs.s3.access.key");
    let secret = config.object_store_options.get("fs.s3.access.secret");
    let region = config.object_store_options.get("fs.s3.region");
    let bucket = config.object_store_options.get("fs.s3.bucket");

    if region == None {
        return Err(DataFusionError::ArrowError(ArrowError::InvalidArgumentError(
            "missing fs.s3.region".to_string(),
        )));
    }

    if bucket == None {
        return Err(DataFusionError::ArrowError(ArrowError::InvalidArgumentError(
            "missing fs.s3.bucket".to_string(),
        )));
    }

    let endpoint = config.object_store_options.get("fs.s3.endpoint");
    let retry_config = RetryConfig {
        backoff: Default::default(),
        max_retries: 4,
        retry_timeout: Default::default(),
    };
    let s3_store = AmazonS3Builder::new()
        .with_access_key_id(key.unwrap())
        .with_secret_access_key(secret.unwrap())
        .with_region(region.unwrap())
        .with_bucket_name(bucket.unwrap())
        .with_endpoint(endpoint.unwrap())
        .with_retry(retry_config)
        .with_allow_http(true)
        .build();
    runtime.register_object_store("s3", bucket.unwrap(), Arc::new(s3_store.unwrap()));
    Ok(())
}

pub fn create_session_context(config: &LakeSoulIOConfig) -> Result<SessionContext> {
    let sess_conf = SessionConfig::default()
        .with_batch_size(config.batch_size)
        .with_prefetch(config.prefetch_size);
    let runtime = RuntimeEnv::new(RuntimeConfig::new())?;

    // register object store(s)
    // register_s3_object_store(config, &runtime)?;

    // create session context
    Ok(SessionContext::with_config_rt(sess_conf, Arc::new(runtime)))
}
