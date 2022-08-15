use atomic_refcell::AtomicRefCell;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::num::NonZeroUsize;
use std::sync::Arc;

use derivative::Derivative;

pub use datafusion::arrow::array::{export_array_into_raw, StructArray};
pub use datafusion::arrow::error::ArrowError;
pub use datafusion::arrow::error::Result as ArrowResult;
pub use datafusion::arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
pub use datafusion::arrow::record_batch::RecordBatch;
pub use datafusion::error::{DataFusionError, Result};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::aws;

use tokio::runtime::{Builder, Runtime};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;

#[derive(Derivative, Default)]
pub struct LakeSoulReaderConfig {
    // files to read
    files: Vec<String>,
    // primary key column names
    primary_keys: Vec<String>,
    // selecting columns
    columns: Vec<String>,
    // filtering predicates
    filters: Vec<Expr>,

    // object store related configs
    object_store_options: HashMap<String, String>,

    // tokio runtime related configs
    #[derivative(Default(value = "2"))]
    thread_num: usize,
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

    pub fn with_column(mut self, col: String) -> Self {
        self.config.columns.push(col);
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

    pub fn with_thread_num(mut self, thread_num: usize) -> Self {
        self.config.thread_num = thread_num;
        self
    }

    pub fn build(self) -> LakeSoulReaderConfig {
        self.config
    }
}

pub struct LakeSoulReader {
    sess_ctx: SessionContext,
    runtime: Arc<Runtime>,
    config: LakeSoulReaderConfig,
    stream: Box<MaybeUninit<SendableRecordBatchStream>>,
}

impl LakeSoulReader {
    pub fn new(config: LakeSoulReaderConfig) -> Result<Self> {
        let sess_ctx = LakeSoulReader::create_session_context(&config)?;
        let runtime = Builder::new_multi_thread().worker_threads(config.thread_num).build()?;
        Ok(LakeSoulReader {
            sess_ctx,
            runtime: Arc::new(runtime),
            config,
            stream: Box::new_uninit(),
        })
    }

    fn check_fs_type_enabled(config: &LakeSoulReaderConfig, fs_name: &str) -> bool {
        if let Some(fs_enabled) = config
            .object_store_options
            .get(format!("fs.{}.enabled", fs_name).as_str())
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

    pub async fn start(&mut self) -> Result<()> {
        let mut df = self
            .sess_ctx
            .read_parquet(self.config.files[0].as_str(), Default::default())
            .await?;
        if !self.config.columns.is_empty() {
            let cols: Vec<_> = self.config.columns.iter().map(String::as_str).collect();
            df = df.select_columns(&cols)?;
        }
        df = self.config.filters.iter().try_fold(df, |df, f| df.filter(f.clone()))?;
        self.stream = Box::new(MaybeUninit::new(df.execute_stream().await?));
        Ok(())
    }

    pub async fn next_rb(&mut self) -> Option<ArrowResult<RecordBatch>> {
        unsafe { self.stream.assume_init_mut().next().await }
    }

    pub fn get_runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }
}

// Reader will be used in async closure sent to tokio
// while accessing its mutable methods.
pub struct SyncSendableMutableLakeSoulReader {
    inner: Arc<AtomicRefCell<Mutex<LakeSoulReader>>>,
}

impl SyncSendableMutableLakeSoulReader {
    pub fn new(reader: LakeSoulReader) -> Self {
        SyncSendableMutableLakeSoulReader {
            inner: Arc::new(AtomicRefCell::new(Mutex::new(reader))),
        }
    }

    pub fn start_blocked(&self) -> Result<()> {
        let inner_reader = self.inner.clone();
        let runtime = (inner_reader).borrow_mut().get_mut().get_runtime();
        runtime.block_on(async { inner_reader.borrow().lock().await.start().await })
    }

    pub fn next_rb_callback(
        &self,
        f: Box<dyn FnOnce(Option<ArrowResult<RecordBatch>>) + Send + Sync>,
    ) -> JoinHandle<()> {
        let inner_reader = self.inner.clone();
        let runtime = (inner_reader).borrow_mut().get_mut().get_runtime();
        runtime.spawn(async move {
            let reader = inner_reader.borrow();
            let mut reader = reader.lock().await;
            f(reader.next_rb().await);
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::pretty::print_batches;
    use std::sync::mpsc::sync_channel;

    #[tokio::test]
    async fn test_reader_local() -> Result<()> {
        let reader_conf = LakeSoulReaderConfigBuilder::new()
            .with_files(vec!["test/test.snappy.parquet".to_string()])
            .build();
        let mut reader = LakeSoulReader::new(reader_conf)?;
        reader.start().await?;
        while let Some(record_batch) = reader.next_rb().await {
            print_batches(std::slice::from_ref(&record_batch?))?;
        }
        Ok(())
    }

    #[test]
    fn test_reader_local_bloked() -> Result<()> {
        let reader_conf = LakeSoulReaderConfigBuilder::new()
            .with_files(vec![
                "parquet-testing/data/alltypes_plain.snappy.parquet"
                    .to_string(),
            ])
            .with_thread_num(2)
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let reader = SyncSendableMutableLakeSoulReader::new(reader);
        reader.start_blocked()?;
        loop {
            let (tx, rx) = sync_channel(1);
            let f = move |rb: Option<ArrowResult<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    print_batches(std::slice::from_ref(&rb.unwrap())).unwrap();
                    tx.send(false).unwrap();
                }
            };
            reader.next_rb_callback(Box::new(f));
            let done = rx.recv().unwrap();
            if done {
                break;
            }
        }
        Ok(())
    }
}
