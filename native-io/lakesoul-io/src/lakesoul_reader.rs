use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use datafusion::arrow::error::ArrowError;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::aws;

#[derive(Default)]
pub struct LakeSoulReaderConfig {
    files: Vec<String>,
    primary_keys: Vec<String>,
    // primary key column names
    columns: Vec<String>,
    // selecting columns
    filters: Vec<Expr>,
    // filtering predicates
    object_store_options: HashMap<String, String>,
}

pub struct LakeSoulReaderConfigBuilder {
    config: LakeSoulReaderConfig,
}

impl LakeSoulReaderConfigBuilder {
    pub fn new() -> Self {
        LakeSoulReaderConfigBuilder {
            config: LakeSoulReaderConfig::default(),
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

    pub fn with_columns(mut self, cols: Vec<String>) -> Self {
        self.config.columns = cols;
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

    pub fn build(self) -> LakeSoulReaderConfig {
        self.config
    }
}

pub struct LakeSoulReader {
    sess_ctx: SessionContext,
    config: LakeSoulReaderConfig,
}

impl LakeSoulReader {
    pub fn new(conf: LakeSoulReaderConfig) -> Result<Self> {
        let sess_ctx = LakeSoulReader::create_session_context(&conf)?;
        Ok(LakeSoulReader { sess_ctx, config: conf })
    }

    fn check_fs_type_enabled(config: &LakeSoulReaderConfig, fs_name: &str) -> bool {
        if let Some(fs_enabled) = config
            .object_store_options
            .get(format!("fs.{fs_name}.enabled").as_str())
        {
            return match fs_enabled.parse::<bool>() {
                Ok(enabled) => enabled,
                _ => false,
            };
        }
        false
    }

    fn register_s3_object_store(config: &LakeSoulReaderConfig, runtime: &RuntimeEnv) -> Result<()> {
        if !LakeSoulReader::check_fs_type_enabled(config, "s3") {
            return Ok(());
        }
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
        let s3_store = aws::new_s3(
            key,
            secret,
            region.unwrap(),
            bucket.unwrap(),
            endpoint,
            None::<String>,
            NonZeroUsize::new(4).unwrap(),
            true,
        )?;
        runtime.register_object_store("s3", bucket.unwrap(), Arc::new(s3_store));
        Ok(())
    }

    fn create_session_context(config: &LakeSoulReaderConfig) -> Result<SessionContext> {
        let sess_conf = SessionConfig::default();
        let runtime = RuntimeEnv::new(RuntimeConfig::new())?;

        // register object store(s)
        LakeSoulReader::register_s3_object_store(config, &runtime)?;

        // create session context
        Ok(SessionContext::with_config_rt(sess_conf, Arc::new(runtime)))
    }

    pub async fn read(&self) -> Result<SendableRecordBatchStream> {
        let mut df = self
            .sess_ctx
            .read_parquet(self.config.files[0].as_str(), Default::default())
            .await?;
        if !self.config.columns.is_empty() {
            let cols: Vec<_> = self.config.columns.iter().map(String::as_str).collect();
            df = df.select_columns(&cols)?;
        }
        df = self.config.filters.iter().try_fold(df, |df, f| df.filter(f.clone()))?;
        df.execute_stream().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;
    use datafusion::arrow::util::pretty::print_batches;

    #[tokio::test]
    async fn test_reader_local() -> Result<()> {
        let reader_conf = LakeSoulReaderConfigBuilder::new()
            .with_files(vec!["test/test.snappy.parquet".to_string()])
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut stream = reader.read().await?;
        while let Some(record_batch) = stream.next().await {
            print_batches(std::slice::from_ref(&record_batch?))?;
        }
        Ok(())
    }
}
