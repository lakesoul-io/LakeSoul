// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use atomic_refcell::AtomicRefCell;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow_schema::SchemaRef;

pub use datafusion::arrow::error::ArrowError;
pub use datafusion::arrow::error::Result as ArrowResult;
pub use datafusion::arrow::record_batch::RecordBatch;
pub use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::SendableRecordBatchStream;

use datafusion::prelude::{DataFrame, SessionContext};

use core::pin::Pin;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::prelude::Expr::Column;
use futures::future::try_join_all;
use futures::StreamExt;

use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::default_column_stream::empty_schema_stream::EmptySchemaStream;
use crate::default_column_stream::DefaultColumnStream;
use crate::filter::Parser as FilterParser;
use crate::lakesoul_io_config::{create_session_context, LakeSoulIOConfig};
use crate::sorted_merge::merge_operator::MergeOperator;
use crate::sorted_merge::sorted_stream_merger::{SortedStream, SortedStreamMerger};

pub struct LakeSoulReader {
    sess_ctx: SessionContext,
    config: LakeSoulIOConfig,
    stream: Option<Pin<Box<dyn RecordBatchStream + Send>>>,
    pub(crate) schema: Option<SchemaRef>,
}

impl LakeSoulReader {
    pub fn new(mut config: LakeSoulIOConfig) -> Result<Self> {
        let sess_ctx = create_session_context(&mut config)?;
        Ok(LakeSoulReader {
            sess_ctx,
            config,
            stream: None,
            schema: None,
        })
    }

    pub async fn prune_filter_and_execute(
        df: DataFrame,
        request_schema: SchemaRef,
        filter_str: Vec<String>,
        batch_size: usize,
    ) -> Result<SendableRecordBatchStream> {
        let file_schema = df.schema().clone();

        // find columns requested and prune others
        let cols = request_schema
            .fields()
            .iter()
            .filter_map(|field| match file_schema.field_with_name(None, field.name()) {
                // datafusion's select is case sensitive, but col will transform field name to lower case
                // so we use Column::new_unqualified instead
                Ok(file_field) => Some(Column(datafusion::common::Column::new_unqualified(file_field.name()))),
                _ => None,
            })
            .collect::<Vec<_>>();
        if cols.is_empty() {
            Ok(Box::pin(EmptySchemaStream::new(batch_size, df.count().await?)))
        } else {
            // row filtering should go first since filter column may not in the selected cols
            let arrow_schema = Arc::new(Schema::from(file_schema));
            let df = filter_str.iter().try_fold(df, |df, f| {
                let filter = FilterParser::parse(f.clone(), arrow_schema.clone());
                df.filter(filter)
            })?;
            // column pruning
            let df = df.select(cols)?;
            df.execute_stream().await
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        let schema: SchemaRef = self.config.schema.0.clone();
        let batch_size = self.config.batch_size;
        if self.config.primary_keys.is_empty() {
            if !self.config.files.is_empty() {
                let mut stream_init_futs = Vec::with_capacity(self.config.files.len());
                for i in 0..self.config.files.len() {
                    let file = self.config.files[i].clone();
                    let sess_ctx = self.sess_ctx.clone();
                    let filter_str = self.config.filter_strs.clone();
                    let schema = schema.clone();
                    let future = async move {
                        let df = sess_ctx.read_parquet(file.clone(), Default::default()).await?;
                        LakeSoulReader::prune_filter_and_execute(df, schema, filter_str, batch_size).await
                    };
                    stream_init_futs.push(future);
                }
                let stream_vec = try_join_all(stream_init_futs).await?;
                let stream = DefaultColumnStream::new_from_streams_with_default(
                    stream_vec,
                    schema.clone(),
                    Arc::new(self.config.default_column_value.clone()),
                );
                self.schema = Some(stream.schema());
                self.stream = Some(Box::pin(stream));

                Ok(())
            } else {
                Err(DataFusionError::Internal(
                    "LakeSoulReader has wrong number of file".to_string(),
                ))
            }
        } else if self.config.files.is_empty() {
            Err(DataFusionError::Internal(
                "LakeSoulReader has wrong number of file".to_string(),
            ))
        } else {
            let finalize_schema: SchemaRef = self.config.schema.0.clone();
            let schema: SchemaRef = Arc::new(Schema::new(
                finalize_schema
                    .fields
                    .iter()
                    .filter_map(|field| {
                        if self.config.default_column_value.get(field.name()).is_none() {
                            Some(field.clone())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>(),
            )); //merge_schema

            let mut stream_init_futs = Vec::with_capacity(self.config.files.len());
            for i in 0..self.config.files.len() {
                let file = self.config.files[i].clone();
                let sess_ctx = self.sess_ctx.clone();
                let filter_str = self.config.filter_strs.clone();
                let schema = schema.clone();
                let future = async move {
                    let df = sess_ctx.read_parquet(file.as_str(), Default::default()).await?;
                    LakeSoulReader::prune_filter_and_execute(df, schema, filter_str, batch_size).await
                };
                stream_init_futs.push(future);
            }

            let stream_res = try_join_all(stream_init_futs).await?;
            let streams = stream_res
                .into_iter()
                .map(|s| SortedStream::new(Box::pin(DefaultColumnStream::new_from_stream(s, schema.clone()))))
                .collect();

            let mut sort_exprs = Vec::with_capacity(self.config.primary_keys.len());
            for i in 0..self.config.primary_keys.len() {
                sort_exprs.push(PhysicalSortExpr {
                    expr: datafusion::physical_expr::expressions::col(self.config.primary_keys[i].as_str(), &schema)?,
                    options: Default::default(),
                });
            }

            let merge_ops = self
                .config
                .schema
                .0
                .fields()
                .iter()
                .map(|field| {
                    MergeOperator::from_name(
                        self.config
                            .merge_operators
                            .get(field.name())
                            .unwrap_or(&String::from("UseLast")),
                    )
                })
                .collect::<Vec<_>>();

            let merge_stream = SortedStreamMerger::new_from_streams(
                streams,
                schema.clone(),
                self.config.primary_keys.clone(),
                self.config.batch_size,
                merge_ops,
            )
            .unwrap();
            let finalized_stream = DefaultColumnStream::new_from_streams_with_default(
                vec![Box::pin(merge_stream)],
                finalize_schema.clone(),
                Arc::new(self.config.default_column_value.clone()),
            );
            self.schema = Some(finalized_stream.schema());
            self.stream = Some(Box::pin(finalized_stream));
            Ok(())
        }
    }

    pub async fn next_rb(&mut self) -> Option<Result<RecordBatch>> {
        if let Some(stream) = &mut self.stream {
            stream.next().await
        } else {
            None
        }
    }
}

// Reader will be used in async closure sent to tokio
// while accessing its mutable methods.
pub struct SyncSendableMutableLakeSoulReader {
    inner: Arc<AtomicRefCell<Mutex<LakeSoulReader>>>,
    runtime: Arc<Runtime>,
    schema: Option<SchemaRef>,
}

impl SyncSendableMutableLakeSoulReader {
    pub fn new(reader: LakeSoulReader, runtime: Runtime) -> Self {
        SyncSendableMutableLakeSoulReader {
            inner: Arc::new(AtomicRefCell::new(Mutex::new(reader))),
            runtime: Arc::new(runtime),
            schema: None,
        }
    }

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

    pub fn next_rb_callback(&self, f: Box<dyn FnOnce(Option<Result<RecordBatch>>) + Send + Sync>) -> JoinHandle<()> {
        let inner_reader = self.get_inner_reader();
        let runtime = self.get_runtime();
        runtime.spawn(async move {
            let reader = inner_reader.borrow();
            let mut reader = reader.lock().await;
            let rb = reader.next_rb().await;
            f(rb);
        })
    }

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
    use rand::prelude::*;
    use std::mem::ManuallyDrop;
    use std::ops::Not;
    use std::sync::mpsc::sync_channel;
    use std::time::Instant;
    use tokio::runtime::Builder;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::util::pretty::print_batches;

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
            row_cnt = row_cnt + num_rows;
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
        let mut rng = thread_rng();
        loop {
            let (tx, rx) = sync_channel(1);
            let start = Instant::now();
            let f = move |rb: Option<Result<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    thread::sleep(Duration::from_millis(200));
                    let num_rows = &rb.as_ref().unwrap().num_rows();
                    print_batches(&[rb.as_ref().unwrap().clone()]);

                    println!("time cost: {:?} ms", start.elapsed().as_millis()); // ms
                    tx.send(false).unwrap();
                }
            };
            thread::sleep(Duration::from_millis(rng.gen_range(600..1200)));

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
        static mut ROW_CNT: usize = 0;
        loop {
            let (tx, rx) = sync_channel(1);
            let f = move |rb: Option<Result<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    let rb = rb.unwrap();
                    let num_rows = &rb.num_rows();
                    unsafe {
                        ROW_CNT = ROW_CNT + num_rows;
                        println!("{}", ROW_CNT);
                    }

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

    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_reader_s3() -> Result<()> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec!["s3://path/to/file.parquet".to_string()])
            .with_thread_num(1)
            .with_batch_size(8192)
            .with_object_store_option(String::from("fs.s3a.access.key"), String::from("fs.s3.access.key"))
            .with_object_store_option(String::from("fs.s3a.secret.key"), String::from("fs.s3.secret.key"))
            .with_object_store_option(String::from("fs.s3a.region"), String::from("us-east-1"))
            .with_object_store_option(String::from("fs.s3a.bucket"), String::from("fs.s3.bucket"))
            .with_object_store_option(String::from("fs.s3a.endpoint"), String::from("fs.s3.endpoint"))
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;
        static mut ROW_CNT: usize = 0;

        let start = Instant::now();
        while let Some(rb) = reader.next_rb().await {
            let num_rows = &rb.unwrap().num_rows();
            unsafe {
                ROW_CNT = ROW_CNT + num_rows;
                println!("{}", ROW_CNT);
            }
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
            .with_object_store_option(String::from("fs.s3a.access.key"), String::from("fs.s3.access.key"))
            .with_object_store_option(String::from("fs.s3a.secret.key"), String::from("fs.s3.secret.key"))
            .with_object_store_option(String::from("fs.s3a.region"), String::from("us-east-1"))
            .with_object_store_option(String::from("fs.s3a.bucket"), String::from("fs.s3.bucket"))
            .with_object_store_option(String::from("fs.s3a.endpoint"), String::from("fs.s3.endpoint"))
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let runtime = Builder::new_multi_thread()
            .worker_threads(reader.config.thread_num)
            .enable_all()
            .build()
            .unwrap();
        let mut reader = SyncSendableMutableLakeSoulReader::new(reader, runtime);
        reader.start_blocked()?;
        static mut ROW_CNT: usize = 0;
        let start = Instant::now();
        loop {
            let (tx, rx) = sync_channel(1);

            let f = move |rb: Option<Result<RecordBatch>>| match rb {
                None => tx.send(true).unwrap(),
                Some(rb) => {
                    let num_rows = &rb.unwrap().num_rows();
                    unsafe {
                        ROW_CNT = ROW_CNT + num_rows;
                        println!("{}", ROW_CNT);
                    }

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
    use datafusion::logical_expr::{col, Expr};
    use datafusion_common::ScalarValue;

    async fn get_num_rows_of_file_with_filters(file_path: String, filters: Vec<Expr>) -> Result<usize> {
        let reader_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![file_path])
            .with_thread_num(1)
            .with_batch_size(32)
            .with_filters(filters)
            .build();
        let reader = LakeSoulReader::new(reader_conf)?;
        let mut reader = ManuallyDrop::new(reader);
        reader.start().await?;
        let mut row_cnt: usize = 0;

        while let Some(rb) = reader.next_rb().await {
            row_cnt += &rb.unwrap().num_rows();
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters1).await;
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters2).await;
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters1).await;
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters2).await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters3: Vec<Expr> = vec![];
        let filter = col("salary").is_null();
        filters3.push(filter);

        let mut row_cnt3 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters3).await;
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters1).await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters2: Vec<Expr> = vec![];
        let filter = col("cc").is_not_null();
        filters2.push(filter);

        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters2).await;
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters1).await;
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters2).await;
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters).await;
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters).await;
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
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters).await;
        if let Ok(row_cnt) = result {
            row_cnt1 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        let mut filters: Vec<Expr> = vec![];
        let filter = Expr::not(col("salary").is_null());
        filters.push(filter);
        let mut row_cnt2 = 0;
        let result = get_num_rows_of_file_with_filters("/path/to/file.parquet".to_string(), filters).await;
        if let Ok(row_cnt) = result {
            row_cnt2 = row_cnt;
        } else {
            assert_eq!(0, 1);
        }

        assert_eq!(row_cnt1 + row_cnt2, 1000);

        Ok(())
    }
}
