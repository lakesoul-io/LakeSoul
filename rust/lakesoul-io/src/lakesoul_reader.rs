// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul Reader Module
//!
//! This module provides functionality for reading data from LakeSoul tables.
//! It supports reading from various file formats (currently Parquet) and includes
//! features like filtering, partitioning, and optimized reading with primary keys.
//!
//! # Examples
//! ```rust
//! use lakesoul_io::lakesoul_reader::LakeSoulReader;
//! use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
//!
//! let config = LakeSoulIOConfigBuilder::new()
//!     .with_files(vec!["path/to/file.parquet"])
//!     .with_thread_num(1)
//!     .with_batch_size(256)
//!     .build();
//!
//! let mut reader = LakeSoulReader::new(config)?;
//! reader.start().await?;
//!
//! while let Some(batch) = reader.next_rb().await {
//!     let record_batch = batch?;
//!     // Process the record batch
//! }
//! ```

use atomic_refcell::AtomicRefCell;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use std::sync::Arc;

use arrow_schema::SchemaRef;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};

use datafusion::prelude::SessionContext;

use futures::StreamExt;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::datasource::file_format::LakeSoulParquetFormat;
use crate::datasource::listing::LakeSoulTableProvider;
use crate::datasource::physical_plan::merge::convert_filter;
use crate::datasource::physical_plan::merge::prune_filter_and_execute;
use crate::helpers::{
    collect_or_conjunctive_filter_expressions, compute_scalar_hash,
    extract_hash_bucket_id, extract_scalar_value_from_expr,
};
use crate::lakesoul_io_config::{LakeSoulIOConfig, create_session_context};

/// A reader for LakeSoul tables that supports efficient reading of data with various optimizations.
///
/// This reader provides functionality to:
/// - Read data from Parquet files
/// - Apply filters during reading
/// - Support partitioned tables
/// - Optimize reads using primary keys
/// - Handle both local and S3 storage
///
/// # Thread Safety
///
/// The reader is designed to be used in both synchronous and asynchronous contexts.
/// For synchronous usage, consider using `SyncSendableMutableLakeSoulReader`.
///
/// # Examples
///
/// ```rust
/// # tokio_test::block_on(async {
/// use lakesoul_io::lakesoul_reader::LakeSoulReader;
/// use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
///
/// let config = LakeSoulIOConfigBuilder::new()
///     .with_files(vec!["path/to/file.parquet"])
///     .with_thread_num(1)
///     .with_batch_size(256)
///     .build();
///
/// let mut reader = LakeSoulReader::new(config)?;
/// reader.start().await?;
///
/// while let Some(batch) = reader.next_rb().await {
///     let record_batch = batch?;
///     // Process the record batch
/// }
/// })
/// ```
pub struct LakeSoulReader {
    sess_ctx: SessionContext,
    config: LakeSoulIOConfig,
    stream: Option<SendableRecordBatchStream>,
    pub(crate) schema: Option<SchemaRef>,
}

impl LakeSoulReader {
    /// Creates a new LakeSoulReader with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the reader, including file paths, thread count, and batch size
    ///
    /// # Returns
    ///
    /// A Result containing the new LakeSoulReader instance
    pub fn new(mut config: LakeSoulIOConfig) -> Result<Self> {
        let sess_ctx = create_session_context(&mut config)?;
        Ok(LakeSoulReader {
            sess_ctx,
            config,
            stream: None,
            schema: None,
        })
    }

    /// Initializes the reader and prepares it for reading data.
    ///
    /// This method:
    /// - Sets up the file format and table provider
    /// - Applies any configured filters
    /// - Optimizes the read operation if primary keys are configured
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of the initialization
    #[instrument(level = "debug", skip(self))]
    pub async fn start(&mut self) -> Result<()> {
        let target_schema: SchemaRef = self.config.target_schema.0.clone();
        if self.config.files.is_empty() {
            Err(DataFusionError::Internal(
                "LakeSoulReader has the wrong number of files".to_string(),
            ))
        } else {
            let file_format = Arc::new(LakeSoulParquetFormat::new(
                Arc::new(
                    ParquetFormat::new().with_force_view_types(
                        self.sess_ctx
                            .state()
                            .config_options()
                            .execution
                            .parquet
                            .schema_force_view_types,
                    ),
                ),
                self.config.clone(),
            ));
            let source = LakeSoulTableProvider::new_with_config_and_format(
                &self.sess_ctx.state(),
                self.config.clone(),
                file_format,
                false,
            )
            .await?;

            let dataframe = self.sess_ctx.read_table(Arc::new(source))?;
            let filters = convert_filter(
                &dataframe,
                self.config.filter_strs.clone(),
                self.config.filter_protos.clone(),
            )?;

            // Check if filters are or-conjunction of primary column
            let skip_reader = if self.config.skip_merge_on_read()
                && !self.config.primary_keys.is_empty()
                && !filters.is_empty()
            {
                // Refactored approach for OR-conjunction optimization
                let or_conjunctive_filter = collect_or_conjunctive_filter_expressions(
                    &filters,
                    &self.config.primary_keys,
                );

                if !or_conjunctive_filter.is_empty() {
                    debug!(
                        "Found {} optimizable expressions with primary keys",
                        or_conjunctive_filter.len()
                    );

                    let hash_bucket_num = self.config.hash_bucket_num() as u32;
                    // Collect all scalar values from optimizable expressions that match the hash bucket
                    let mut matching_scalar_values = std::collections::HashSet::new();

                    for expr in &or_conjunctive_filter {
                        if let Some(scalar_value) = extract_scalar_value_from_expr(expr) {
                            // Calculate the hash bucket for this scalar value
                            let hash_value =
                                compute_scalar_hash(scalar_value) % hash_bucket_num;

                            // Add the scalar value to our set
                            matching_scalar_values.insert(hash_value);

                            debug!(
                                "Found scalar value with hash {} (mod {}) = {}",
                                scalar_value, hash_bucket_num, hash_value
                            );
                        }
                    }

                    if !matching_scalar_values.is_empty() {
                        debug!(
                            "Collected {} scalar values for hash bucket optimization",
                            matching_scalar_values.len()
                        );
                    }
                    if let Some(hash_bucket_id) =
                        extract_hash_bucket_id(&self.config.files[0])
                    {
                        debug!(
                            "matching_scalar_values: {:?}, hash_bucket_id: {}",
                            &matching_scalar_values, hash_bucket_id
                        );
                        !matching_scalar_values.contains(&hash_bucket_id)
                    } else {
                        debug!(
                            "Found optimizable expressions with primary keys, but couldn't extract hash_bucket_id"
                        );
                        false
                    }
                } else {
                    debug!("No optimizable expressions found with primary keys");
                    false
                }
            } else {
                false
            };

            let stream = if skip_reader {
                // Create an empty stream with the target schema
                debug!("Skipping reader due to hash bucket optimization");
                let empty_batch = RecordBatch::new_empty(target_schema.clone());
                // Use the same stream type as prune_filter_and_execute returns
                Box::pin(RecordBatchStreamAdapter::new(
                    target_schema.clone(),
                    futures::stream::once(async move { Ok(empty_batch) }).boxed(),
                )) as SendableRecordBatchStream
            } else {
                prune_filter_and_execute(
                    dataframe,
                    target_schema.clone(),
                    filters,
                    self.config.batch_size,
                )
                .await?
            };
            self.schema = Some(stream.schema());
            self.stream = Some(stream);

            Ok(())
        }
    }

    /// Retrieves the next record batch from the reader.
    ///
    /// # Returns
    ///
    /// An Option containing a Result with the next RecordBatch, or None if there are no more batches
    pub async fn next_rb(&mut self) -> Option<Result<RecordBatch>> {
        if let Some(stream) = &mut self.stream {
            stream.next().await
        } else {
            None
        }
    }
}

/// A thread-safe wrapper for LakeSoulReader that can be used in synchronous contexts.
///
/// This wrapper provides methods to:
/// - Start the reader in a blocking fashion
/// - Read record batches using callbacks
/// - Access the reader's schema
///
/// # Thread Safety
///
/// This wrapper ensures thread-safe access to the underlying reader using Arc and Mutex.
///
/// # Examples
///
/// ```rust
/// use lakesoul_io::lakesoul_reader::SyncSendableMutableLakeSoulReader;
/// use tokio::runtime::Runtime;
///
/// let runtime = Runtime::new()?;
/// let mut reader = SyncSendableMutableLakeSoulReader::new(lake_soul_reader, runtime);
/// reader.start_blocked()?;
///
/// let (tx, rx) = std::sync::mpsc::channel(1);
/// reader.next_rb_callback(Box::new(move |batch| {
///     tx.send(batch).unwrap();
/// }));
/// ```
pub struct SyncSendableMutableLakeSoulReader {
    inner: Arc<AtomicRefCell<Mutex<LakeSoulReader>>>,
    runtime: Arc<Runtime>,
    schema: Option<SchemaRef>,
}

impl SyncSendableMutableLakeSoulReader {
    /// Creates a new SyncSendableMutableLakeSoulReader with the given reader and runtime.
    ///
    /// # Arguments
    ///
    /// * `reader` - The LakeSoulReader instance to wrap
    /// * `runtime` - The Tokio runtime to use for async operations
    pub fn new(reader: LakeSoulReader, runtime: Runtime) -> Self {
        SyncSendableMutableLakeSoulReader {
            inner: Arc::new(AtomicRefCell::new(Mutex::new(reader))),
            runtime: Arc::new(runtime),
            schema: None,
        }
    }

    /// Starts the reader in a blocking fashion.
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure of the initialization
    pub fn start_blocked(&mut self) -> Result<()> {
        let inner_reader = self.inner.clone();
        let runtime = self.get_runtime();
        runtime.block_on(async {
            let reader = inner_reader.borrow();
            let mut reader = reader.lock().await;
            reader.start().await?;
            self.schema = reader.schema.clone();
            Ok(())
        })
    }

    /// Registers a callback to be called with the next record batch.
    ///
    /// # Arguments
    ///
    /// * `f` - The callback function to be called with the next batch
    ///
    /// # Returns
    ///
    /// A JoinHandle that can be used to wait for the callback to complete
    pub fn next_rb_callback(
        &self,
        f: Box<dyn FnOnce(Option<Result<RecordBatch>>) + Send + Sync>,
    ) -> JoinHandle<()> {
        let inner_reader = self.get_inner_reader();
        let runtime = self.get_runtime();
        runtime.spawn(async move {
            let reader = inner_reader.borrow();
            let mut reader = reader.lock().await;
            let rb = reader.next_rb().await;
            f(rb);
        })
    }

    /// Retrieves the next record batch in a blocking fashion.
    ///
    /// # Returns
    ///
    /// An Option containing a Result with the next RecordBatch, or None if there are no more batches
    pub fn next_rb_blocked(
        &self,
    ) -> Option<std::result::Result<RecordBatch, DataFusionError>> {
        let inner_reader = self.get_inner_reader();
        let runtime = self.get_runtime();
        runtime.block_on(async move {
            let reader = inner_reader.borrow();
            let mut reader = reader.lock().await;
            reader.next_rb().await
        })
    }

    /// Gets the schema of the reader.
    ///
    /// # Returns
    ///
    /// An Option containing the SchemaRef, or None if the schema is not yet available
    pub fn get_schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }

    fn get_runtime(&self) -> Arc<Runtime> {
        self.runtime.clone()
    }

    fn get_inner_reader(&self) -> Arc<AtomicRefCell<Mutex<LakeSoulReader>>> {
        self.inner.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::as_primitive_array;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use rand::distr::SampleString;
    use std::mem::ManuallyDrop;
    use std::ops::Not;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::mpsc::sync_channel;
    use std::time::Instant;
    use tokio::runtime::Builder;

    use arrow::datatypes::{DataType, Field, Schema, TimestampSecondType};
    use arrow::util::pretty::print_batches;

    use rand::Rng;

    #[tokio::test]
    async fn test_reader_local() -> Result<()> {
        let project_dir = std::env::current_dir()?;
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![
                project_dir.join("../lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet").into_os_string().into_string().unwrap()
            ])
            .with_thread_num(1)
            .with_batch_size(256)
            .build();
        let mut reader = LakeSoulReader::new(reader_conf)?;
        reader.start().await?;
        let mut row_cnt: usize = 0;

        while let Some(rb) = reader.next_rb().await {
            let num_rows = &rb.unwrap().num_rows();
            row_cnt += num_rows;
        }
        assert_eq!(row_cnt, 1000);
        Ok(())
    }

    #[test]
    fn test_reader_local_blocked() -> Result<()> {
        let project_dir = std::env::current_dir()?;
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![
                project_dir.join("../lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet").into_os_string().into_string().unwrap()
            ])
            .with_thread_num(2)
            .with_batch_size(11)
            .with_primary_keys(vec!["id".to_string()])
            .with_schema(Arc::new(Schema::new(vec![
                // Field::new("name", DataType::Utf8, true),
                Field::new("id", DataType::Int64, false),
                // Field::new("x", DataType::Float64, true),
                // Field::new("y", DataType::Float64, true),
            ])))
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(reader.config.thread_num)
            .build()
            .unwrap();
        let mut reader = SyncSendableMutableLakeSoulReader::new(reader, runtime);
        reader.start_blocked()?;
        let mut rng = rand::rng();
        loop {
            let (tx, rx) = sync_channel(1);
            let start = Instant::now();
            let f = move |rb: Option<Result<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    thread::sleep(Duration::from_millis(200));
                    let _ = print_batches(&[rb.as_ref().unwrap().clone()]);

                    println!("time cost: {:?} ms", start.elapsed().as_millis()); // ms
                    tx.send(false).unwrap();
                }
            };
            thread::sleep(Duration::from_millis(rng.random_range(600..1200)));

            reader.next_rb_callback(Box::new(f));
            let done = rx.recv().unwrap();
            if done {
                break;
            }
        }
        Ok(())
    }

    #[test]
    fn test_reader_partition() -> Result<()> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["/path/to/file.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(reader.config.thread_num)
            .build()
            .unwrap();
        let mut reader = SyncSendableMutableLakeSoulReader::new(reader, runtime);
        reader.start_blocked()?;
        let row_cnt = Arc::new(AtomicUsize::new(0));
        loop {
            let (tx, rx) = sync_channel(1);
            let cnt = Arc::clone(&row_cnt);
            let f = move |rb: Option<Result<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    let rb = rb.unwrap();
                    let num_rows = rb.num_rows();
                    cnt.fetch_add(num_rows, Relaxed);
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

    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_reader_s3() -> Result<()> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["s3://path/to/file.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .with_object_store_option(
                String::from("fs.s3a.access.key"),
                String::from("fs.s3.access.key"),
            )
            .with_object_store_option(
                String::from("fs.s3a.secret.key"),
                String::from("fs.s3.secret.key"),
            )
            .with_object_store_option(
                String::from("fs.s3a.region"),
                String::from("us-east-1"),
            )
            .with_object_store_option(
                String::from("fs.s3a.bucket"),
                String::from("fs.s3.bucket"),
            )
            .with_object_store_option(
                String::from("fs.s3a.endpoint"),
                String::from("fs.s3.endpoint"),
            )
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;
        let row_cnt = Arc::new(AtomicUsize::new(0));

        let start = Instant::now();
        while let Some(rb) = reader.next_rb().await {
            let num_rows = rb.unwrap().num_rows();
            row_cnt.fetch_add(num_rows, Relaxed);
            println!("{:?}", row_cnt);
            sleep(Duration::from_millis(20)).await;
        }
        println!("time cost: {:?}ms", start.elapsed().as_millis()); // ms

        Ok(())
    }

    use std::thread;
    #[test]
    fn test_reader_s3_blocked() -> Result<()> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["s3://path/to/file.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .with_object_store_option(
                String::from("fs.s3a.access.key"),
                String::from("fs.s3.access.key"),
            )
            .with_object_store_option(
                String::from("fs.s3a.secret.key"),
                String::from("fs.s3.secret.key"),
            )
            .with_object_store_option(
                String::from("fs.s3a.region"),
                String::from("us-east-1"),
            )
            .with_object_store_option(
                String::from("fs.s3a.bucket"),
                String::from("fs.s3.bucket"),
            )
            .with_object_store_option(
                String::from("fs.s3a.endpoint"),
                String::from("fs.s3.endpoint"),
            )
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(reader.config.thread_num)
            .enable_all()
            .build()
            .unwrap();
        let mut reader = SyncSendableMutableLakeSoulReader::new(reader, runtime);
        reader.start_blocked()?;
        let start = Instant::now();
        let row_cnt = Arc::new(AtomicUsize::new(0));
        loop {
            let row_cnt = Arc::clone(&row_cnt);
            let (tx, rx) = sync_channel(1);

            let f = move |rb: Option<Result<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    let num_rows = rb.unwrap().num_rows();
                    row_cnt.fetch_add(num_rows, Relaxed);
                    println!("{}", row_cnt.load(Relaxed));

                    thread::sleep(Duration::from_millis(20));
                    tx.send(false).unwrap();
                }
            };
            reader.next_rb_callback(Box::new(f));
            let done = rx.recv().unwrap();

            if done {
                println!("time cost: {:?} ms", start.elapsed().as_millis()); // ms
                break;
            }
        }
        Ok(())
    }

    use crate::lakesoul_io_config::LakeSoulIOConfigBuilder;
    use crate::lakesoul_writer::SyncSendableMutableLakeSoulWriter;
    use datafusion::logical_expr::{Expr, col};
    use datafusion_common::ScalarValue;

    // todo use filter_proto
    async fn get_num_rows_of_file_with_filters(
        file_path: String,
        _filters: Vec<Expr>,
    ) -> Result<usize> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![file_path])
            .with_thread_num(1)
            .with_batch_size(32)
            // .with_filters(filters)
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;
        let mut row_cnt: usize = 0;

        while let Some(rb) = reader.next_rb().await {
            row_cnt += &rb?.num_rows();
        }

        Ok(row_cnt)
    }

    #[tokio::test]
    async fn test_expr_eq_neq() -> Result<()> {
        let mut filters1: Vec<Expr> = vec![];
        let v = ScalarValue::Utf8(Some("Amanda".to_string()));
        let filter = col("first_name").eq(Expr::Literal(v));
        filters1.push(filter);
        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters1,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters2: Vec<Expr> = vec![];
        let v = ScalarValue::Utf8(Some("Amanda".to_string()));
        let filter = col("first_name").not_eq(Expr::Literal(v));
        filters2.push(filter);

        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters2,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2, 1000);

        Ok(())
    }

    #[tokio::test]
    async fn test_expr_lteq_gt() -> Result<()> {
        let mut filters1: Vec<Expr> = vec![];
        let v = ScalarValue::Float64(Some(139177.2));
        let filter = col("salary").lt_eq(Expr::Literal(v));
        filters1.push(filter);

        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters1,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters2: Vec<Expr> = vec![];
        let v = ScalarValue::Float64(Some(139177.2));
        let filter = col("salary").gt(Expr::Literal(v));
        filters2.push(filter);

        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters2,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters3: Vec<Expr> = vec![];
        let filter = col("salary").is_null();
        filters3.push(filter);

        let mut row_cnt3 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters3,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt3 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2, 1000 - row_cnt3);

        Ok(())
    }

    #[tokio::test]
    async fn test_expr_null_notnull() -> Result<()> {
        let mut filters1: Vec<Expr> = vec![];
        let filter = col("cc").is_null();
        filters1.push(filter);

        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters1,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters2: Vec<Expr> = vec![];
        let filter = col("cc").is_not_null();
        filters2.push(filter);

        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters2,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2, 1000);

        Ok(())
    }

    #[tokio::test]
    async fn test_expr_or_and() -> Result<()> {
        let mut filters1: Vec<Expr> = vec![];
        let first_name = ScalarValue::Utf8(Some("Amanda".to_string()));
        // let filter = col("first_name").eq(Expr::Literal(first_name)).and(col("last_name").eq(Expr::Literal(last_name)));
        let filter = col("first_name").eq(Expr::Literal(first_name));

        filters1.push(filter);
        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters1,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }
        // println!("{}", row_cnt1);

        let mut filters2: Vec<Expr> = vec![];
        let last_name = ScalarValue::Utf8(Some("Jordan".to_string()));
        // let filter = col("first_name").eq(Expr::Literal(first_name)).and(col("last_name").eq(Expr::Literal(last_name)));
        let filter = col("last_name").eq(Expr::Literal(last_name));

        filters2.push(filter);
        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters2,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters: Vec<Expr> = vec![];
        let first_name = ScalarValue::Utf8(Some("Amanda".to_string()));
        let last_name = ScalarValue::Utf8(Some("Jordan".to_string()));
        let filter = col("first_name")
            .eq(Expr::Literal(first_name))
            .and(col("last_name").eq(Expr::Literal(last_name)));

        filters.push(filter);
        let mut row_cnt3 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt3 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters: Vec<Expr> = vec![];
        let first_name = ScalarValue::Utf8(Some("Amanda".to_string()));
        let last_name = ScalarValue::Utf8(Some("Jordan".to_string()));
        let filter = col("first_name")
            .eq(Expr::Literal(first_name))
            .or(col("last_name").eq(Expr::Literal(last_name)));
        filters.push(filter);
        let mut row_cnt4 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt4 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2 - row_cnt3, row_cnt4);

        Ok(())
    }

    #[tokio::test]
    async fn test_expr_not() -> Result<()> {
        let mut filters: Vec<Expr> = vec![];
        let filter = col("salary").is_null();
        filters.push(filter);
        let mut row_cnt1 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters: Vec<Expr> = vec![];
        let filter = Expr::not(col("salary").is_null());
        filters.push(filter);
        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters(
            "/path/to/file.parquet".to_string(),
            filters,
        )
        .await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2, 1000);

        Ok(())
    }

    #[tokio::test]
    async fn test_read_file_with_partition_column() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Decimal128(10, 0), true),
        ]));
        let partition_schema = Arc::new(Schema::new(vec![Field::new(
            "order_id",
            DataType::Int32,
            true,
        )]));
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["file:/var/folders/4c/34n9w2cd65n0pyjkc3n4q7pc0000gn/T/lakeSource/user_nonpk_partitioned/order_id=4/part-00011-989b7a5d-6ed7-4e51-a3bd-a9fa7853155d_00011.c000.parquet".to_string()])
            // .with_files(vec!["file:/var/folders/4c/34n9w2cd65n0pyjkc3n4q7pc0000gn/T/lakeSource/user1/order_id=4/part-59guLCg5R6v4oLUT_0000.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .with_schema(schema)
            .with_partition_schema(partition_schema)
            .with_default_column_value("order_id".to_string(), "4".to_string())
            .set_inferring_schema(true)
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;

        let row_cnt = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();
        while let Some(rb) = reader.next_rb().await {
            let num_rows = rb.unwrap().num_rows();
            row_cnt.fetch_add(num_rows, Relaxed);
            println!("{}", row_cnt.load(Relaxed));
            sleep(Duration::from_millis(20)).await;
        }
        println!("time cost: {:?}ms", start.elapsed().as_millis()); // ms

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_read_file() -> Result<()> {
        println!("hello world");
        let schema = Arc::new(Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, true),
            Field::new("l_partkey", DataType::Int64, true),
            Field::new("l_suppkey", DataType::Int64, true),
            Field::new("l_linenumber", DataType::Int32, true),
            Field::new("l_quantity", DataType::Decimal128(15, 2), true),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), true),
            Field::new("l_discount", DataType::Decimal128(15, 2), true),
            // Field::new("l_tax", DataType::Decimal128(15, 2), true),
            // Field::new("l_returnflag", DataType::Utf8, true),
            // Field::new("l_linestatus", DataType::Utf8, true),
            // Field::new("l_shipdate", DataType::Date32, true),
            // Field::new("l_commitdate", DataType::Date32, true),
            // Field::new("l_receiptdate", DataType::Date32, true),
            // Field::new("l_shipinstruct", DataType::Utf8, true),
            // Field::new("l_shipmode", DataType::Utf8, true),
            // Field::new("l_comment", DataType::Utf8, true),
        ]));
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![
                "file:///data/tpch/sf10/lineitem/part-00000-f66b4403-0102-4168-b38b-dcffebd0fb4d-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00001-06880f87-1f57-455a-afc8-470c4f12b108-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00002-5ad96efe-eaeb-460e-83b8-036d6c766165-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00003-7fce9a7e-3eb5-4104-88b2-c93f1b73313b-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00004-3d68999c-ad19-425a-aa18-d00ee593cefd-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00005-a4bbc0a8-3dd8-42fc-895f-2d7ec0180918-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00006-989b4ecd-dda8-4ab9-8583-6924ad1ba2a0-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00007-28062768-45db-48c7-ba08-40908be7d34e-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00008-72cc45a1-a726-4f91-9c14-948eb983913a-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00009-183072c1-dc43-4db6-9570-c3d975244157-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00010-3cad5c49-7eff-4afa-8ebd-3962e6e986c0-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00011-bdc6bea3-d525-4ed4-87c1-3f17bc4a2618-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00012-99f0d434-0932-4da2-9f5d-e3a45fb126e0-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00013-60202a1d-20ec-406b-bc57-3b3490a6fd28-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00014-53cb4270-b2af-4531-a0ea-bba50221b8c9-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00015-97063302-d1bd-4359-875f-00dc1893b6ff-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00016-c8feea44-8577-4b55-8a16-f6b659c0eb05-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00017-9f627fdd-d641-493d-b4da-c2171e5163ea-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00018-fda7113d-a56a-43b4-a4cc-5e9c2411315d-c000.parquet"
                    .to_string(),
                "file:///data/tpch/sf10/lineitem/part-00007-28062768-45db-48c7-ba08-40908be7d34e-c000.parquet"
                    .to_string(),
            ])
            .with_thread_num(1)
            .with_batch_size(8192)
            .with_schema(schema)
            .build();
        let mut reader = LakeSoulReader::new(reader_conf)?;
        // let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;
        let row_cnt = Arc::new(AtomicUsize::new(0));
        let start = Instant::now();
        while let Some(rb) = reader.next_rb().await {
            let rb = rb.unwrap();
            let num_rows = rb.num_rows();
            let _num_cols = rb.num_columns();
            row_cnt.fetch_add(num_rows, Relaxed);
            // println!("rows {}, cols {}", ROW_CNT, num_cols);
            // sleep(Duration::from_millis(20)).await;
        }
        println!("time cost: {:?}ms", start.elapsed().as_millis()); // ms

        Ok(())
    }

    #[tokio::test]
    async fn test_as_primitive_array_timestamp_second_type() -> Result<()> {
        use arrow_array::{Array, TimestampSecondArray};
        use std::sync::Arc;

        // 创建一个 TimestampSecondArray
        let data = vec![Some(1627846260), Some(1627846261), None, Some(1627846263)];
        let array = Arc::new(TimestampSecondArray::from(data)) as ArrayRef;

        // 调用 as_primitive_array::<TimestampSecondType>(&array)
        let primitive_array = as_primitive_array::<TimestampSecondType>(&array);

        // 验证结果
        assert_eq!(primitive_array.value(0), 1627846260);
        assert_eq!(primitive_array.value(1), 1627846261);
        assert!(primitive_array.is_null(2));
        assert_eq!(primitive_array.value(3), 1627846263);
        Ok(())
    }

    #[test]
    fn test_primary_key_generator() -> Result<()> {
        let mut generator = LinearPKGenerator::new(2, 3);
        assert_eq!(generator.next_pk(), 3); // x=0: 2*0 + 3
        assert_eq!(generator.next_pk(), 5); // x=1: 2*1 + 3
        assert_eq!(generator.next_pk(), 7); // x=2: 2*2 + 3
        Ok(())
    }

    struct LinearPKGenerator {
        a: i64,
        b: i64,
        current: i64,
    }

    impl LinearPKGenerator {
        fn new(a: i64, b: i64) -> Self {
            LinearPKGenerator { a, b, current: 0 }
        }

        fn next_pk(&mut self) -> i64 {
            let pk = self.a * self.current + self.b;
            self.current += 1;
            pk
        }
    }

    fn create_batch(
        num_columns: usize,
        num_rows: usize,
        str_len: usize,
        pk_generator: &mut Option<LinearPKGenerator>,
    ) -> RecordBatch {
        let mut rng = rand::rng();
        let mut len_rng = rand::rng();
        let mut iter = vec![];
        if let Some(generator) = pk_generator {
            let pk_iter = (0..num_rows).map(|_| generator.next_pk());
            iter.push((
                "pk".to_string(),
                Arc::new(Int64Array::from_iter_values(pk_iter)) as ArrayRef,
                true,
            ));
        }
        for i in 0..num_columns {
            iter.push((
                format!("col_{}", i),
                Arc::new(StringArray::from(
                    (0..num_rows)
                        .into_iter()
                        .map(|_| {
                            rand::distr::Alphanumeric.sample_string(
                                &mut rng,
                                len_rng.random_range(str_len..str_len * 3),
                            )
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
                true,
            ));
        }
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    fn create_schema(num_columns: usize, with_pk: bool) -> Schema {
        let mut fields = vec![];
        if with_pk {
            fields.push(Field::new("pk", DataType::Int64, true));
        }
        for i in 0..num_columns {
            fields.push(Field::new(format!("col_{}", i), DataType::Utf8, true));
        }
        Schema::new(fields)
    }

    #[test]
    fn profiling_2ways_merge_on_read() -> Result<()> {
        let num_batch = 10;
        let num_rows = 1000;
        let num_columns = 100;
        let str_len = 4;
        let _temp_dir = tempfile::tempdir()?.into_path();
        let temp_dir = std::env::current_dir()?.join("temp_dir");
        let with_pk = true;
        let to_write_schema = create_schema(num_columns, with_pk);

        for i in 0..2 {
            let mut generator = if with_pk {
                Some(LinearPKGenerator::new(i + 2, 0))
            } else {
                None
            };
            let to_write = create_batch(num_columns, num_rows, str_len, &mut generator);
            let path = temp_dir
                .clone()
                .join(format!("test{}.parquet", i))
                .into_os_string()
                .into_string()
                .unwrap();
            let writer_conf = LakeSoulIOConfigBuilder::new()
                .with_files(vec![path.clone()])
                // .with_prefix(tempfile::tempdir()?.into_path().into_os_string().into_string().unwrap())
                .with_thread_num(2)
                .with_batch_size(num_rows)
                // .with_max_row_group_size(2000)
                // .with_max_row_group_num_values(4_00_000)
                .with_schema(to_write.schema())
                .with_primary_keys(vec!["pk".to_string()])
                // .with_aux_sort_column("col2".to_string())
                // .with_option(OPTION_KEY_MEM_LIMIT, format!("{}", 1024 * 1024 * 48))
                // .set_dynamic_partition(true)
                .with_hash_bucket_num(4)
                // .with_max_file_size(1024 * 1024 * 32)
                .build();

            let mut writer = SyncSendableMutableLakeSoulWriter::try_new(
                writer_conf,
                Builder::new_multi_thread().enable_all().build().unwrap(),
            )?;

            let _start = Instant::now();
            for _ in 0..num_batch {
                let once_start = Instant::now();
                writer.write_batch(create_batch(
                    num_columns,
                    num_rows,
                    str_len,
                    &mut generator,
                ))?;
                println!(
                    "write batch once cost: {}",
                    once_start.elapsed().as_millis()
                );
            }
            let _flush_start = Instant::now();
            writer.flush_and_close()?;
        }

        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(
                (0..2)
                    .map(|i| {
                        temp_dir
                            .join(format!("test{}.parquet", i))
                            .into_os_string()
                            .into_string()
                            .unwrap()
                    })
                    .collect::<Vec<_>>(),
            )
            .with_thread_num(2)
            .with_batch_size(num_rows)
            .with_schema(Arc::new(to_write_schema))
            .with_primary_keys(vec!["pk".to_string()])
            .build();

        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let lakesoul_reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = SyncSendableMutableLakeSoulReader::new(lakesoul_reader, runtime);
        let _ = reader.start_blocked();
        let start = Instant::now();
        while let Some(rb) = reader.next_rb_blocked() {
            dbg!(&rb.unwrap().num_rows());
        }
        println!("time cost: {:?}ms", start.elapsed().as_millis()); // ms
        Ok(())
    }
}
