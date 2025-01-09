// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Borrow;
use std::collections::HashMap;
use std:: ptr;

use arrow::datatypes::{DataType, Field};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use rand::distributions::DistString;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tracing::debug;
use rand::{Rng,SeedableRng,rngs::StdRng};
use ndarray::{concatenate, s, Array2, Axis,ArrayView2};
use arrow::array::{Array as OtherArray, Float64Array, ListArray,Float32Array,Int64Array,GenericListArray};
use arrow::buffer::OffsetBuffer;
use std::sync::Arc;


use crate::async_writer::{AsyncBatchWriter, MultiPartAsyncWriter, PartitioningAsyncWriter, SortAsyncWriter, WriterFlushResult};
use crate::helpers::{get_batch_memory_size, get_file_exist_col};
use crate::lakesoul_io_config::{IOSchema, LakeSoulIOConfig};
use crate::transform::uniform_schema;

pub type SendableWriter = Box<dyn AsyncBatchWriter + Send>;

// inner is sort writer
// multipart writer
pub struct SyncSendableMutableLakeSoulWriter {
    runtime: Arc<Runtime>,
    schema: SchemaRef,
    config: LakeSoulIOConfig,
    /// The in-progress file writer if any
    in_progress: Option<Arc<Mutex<SendableWriter>>>,
    flush_results: WriterFlushResult,
}

impl SyncSendableMutableLakeSoulWriter {
    pub fn try_new(config: LakeSoulIOConfig, runtime: Runtime) -> Result<Self> {
        let runtime = Arc::new(runtime);
        runtime.clone().block_on(async move {
            let mut config = config.clone();
            let writer_config = config.clone();
            let writer = Self::create_writer(writer_config).await?;
            let schema = writer.schema();
            if let Some(mem_limit) = config.mem_limit() {
                if config.use_dynamic_partition {
                    config.max_file_size = Some((mem_limit as f64 * 0.15) as u64);
                } else if !config.primary_keys.is_empty() && !config.keep_ordering() {
                    config.max_file_size = Some((mem_limit as f64 * 0.2) as u64);
                }
            }

            Ok(SyncSendableMutableLakeSoulWriter {
                in_progress: Some(Arc::new(Mutex::new(writer))),
                runtime,
                schema, // this should be the final written schema
                config,
                flush_results: vec![],
            })
        })
    }

    async fn create_writer(config: LakeSoulIOConfig) -> Result<Box<dyn AsyncBatchWriter + Send>> {
        // if aux sort cols exist, we need to adjust the schema of final writer
        // to exclude all aux sort cols
        let writer_schema: SchemaRef = if !config.aux_sort_cols.is_empty() {
            let schema = config.target_schema.0.clone();
            // O(nm), n = number of target schema fields, m = number of aux sort cols
            let proj_indices = schema
                .fields
                .iter()
                .filter(|f| !config.aux_sort_cols.contains(f.name()))
                .map(|f| schema.index_of(f.name().as_str()).map_err(DataFusionError::ArrowError))
                .collect::<Result<Vec<usize>>>()?;
            Arc::new(schema.project(proj_indices.borrow())?)
        } else {
            config.target_schema.0.clone()
        };

        let mut writer_config = config.clone();
        let writer: Box<dyn AsyncBatchWriter + Send> = if config.use_dynamic_partition {
            Box::new(PartitioningAsyncWriter::try_new(writer_config)?)
        } else if !writer_config.primary_keys.is_empty() && !writer_config.keep_ordering() {
            // sort primary key table
            writer_config.target_schema = IOSchema(uniform_schema(writer_schema));
            if writer_config.files.is_empty() && !writer_config.prefix().is_empty() {
                writer_config.files = vec![format!(
                    "{}/part-{}_{:0>4}.parquet",
                    writer_config.prefix(),
                    rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16),
                    writer_config.hash_bucket_id()
                )];
            }
            let writer = MultiPartAsyncWriter::try_new(writer_config).await?;
            Box::new(SortAsyncWriter::try_new(writer, config)?)
        } else {
            // else multipart
            writer_config.target_schema = IOSchema(uniform_schema(writer_schema));
            if writer_config.files.is_empty() && !writer_config.prefix().is_empty() {
                writer_config.files = vec![format!(
                    "{}/part-{}_{:0>4}.parquet",
                    writer_config.prefix(),
                    rand::distributions::Alphanumeric.sample_string(&mut rand::thread_rng(), 16),
                    writer_config.hash_bucket_id()
                )];
            }
            let writer = MultiPartAsyncWriter::try_new(writer_config).await?;
            Box::new(writer)
        };
        Ok(writer)
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn config(&self) -> &LakeSoulIOConfig {
        &self.config
    }

    // blocking method for writer record batch.
    // since the underlying multipart upload would accumulate buffers
    // and upload concurrently in background, we only need blocking method here
    // for ffi callers
    pub fn write_batch(&mut self, record_batch: RecordBatch) -> Result<()> {
        let runtime = self.runtime.clone();
        if record_batch.num_rows() == 0{
            runtime.block_on(async move { self.write_batch_async(record_batch, false).await })
        }
        else{
            if self.config.is_lsh() {
                let projection: ListArray= if let Some(array) = record_batch.column_by_name("Embedding") {
                    let embedding = array.as_any().downcast_ref::<ListArray>().unwrap();
                    let projection_result:Result<ListArray,String> = self.lsh(&Some(embedding.clone()));
                    projection_result.unwrap().into()

                } else {
                    eprintln!("there is no column named Embedding");
                    return Ok(()) ;
                };

                let mut new_columns = record_batch.columns().to_vec();
                new_columns[record_batch.schema().index_of("LSH").unwrap()] = Arc::new(projection.clone());
                let new_record_batch = RecordBatch::try_new(self.config.target_schema(),new_columns).unwrap();

                runtime.block_on(async move { self.write_batch_async(new_record_batch, false).await })
            }
            else{
                runtime.block_on(async move { self.write_batch_async(record_batch, false).await })
            }
        }
    }

    #[async_recursion::async_recursion(?Send)]
    async fn write_batch_async(&mut self, record_batch: RecordBatch, do_spill: bool) -> Result<()> {
        debug!(record_batch_row=?record_batch.num_rows(), do_spill=?do_spill, "write_batch_async");
        let config = self.config().clone();
        if let Some(max_file_size) = self.config().max_file_size {
            // if max_file_size is set, we need to split batch into multiple files
            let in_progress_writer = match &mut self.in_progress {
                Some(writer) => writer,
                x => x.insert(Arc::new(Mutex::new(Self::create_writer(config).await?))),
            };
            let mut guard = in_progress_writer.lock().await;

            let batch_memory_size = get_batch_memory_size(&record_batch)? as u64;
            let batch_rows = record_batch.num_rows() as u64;
            // If would exceed max_file_size, split batch
            if !do_spill && guard.buffered_size() + batch_memory_size > max_file_size {
                let to_write = (batch_rows * (max_file_size - guard.buffered_size())) / batch_memory_size;
                if to_write + 1 < batch_rows {
                    let to_write = to_write as usize + 1;
                    let a = record_batch.slice(0, to_write);
                    let b = record_batch.slice(to_write, record_batch.num_rows() - to_write);
                    drop(guard);
                    self.write_batch_async(a, true).await?;
                    return self.write_batch_async(b, false).await;
                }
            }
            let rb_schema = record_batch.schema();
            guard.write_record_batch(record_batch).await.map_err(|e| {
                DataFusionError::Internal(format!(
                    "err={}, config={:?}, batch_schema={:?}",
                    e,
                    self.config.clone(),
                    rb_schema
                ))
            })?;

            if do_spill {
                dbg!(format!("spilling writer with size: {}", guard.buffered_size()));
                drop(guard);
                if let Some(writer) = self.in_progress.take() {
                    let inner_writer = match Arc::try_unwrap(writer) {
                        Ok(inner) => inner,
                        Err(_) => {
                            return Err(DataFusionError::Internal(
                                "Cannot get ownership of inner writer".to_string(),
                            ))
                        }
                    };
                    let writer = inner_writer.into_inner();
                    let results = writer.flush_and_close().await.map_err(|e| {
                        DataFusionError::Internal(format!(
                            "err={}, config={:?}, batch_schema={:?}",
                            e,
                            self.config.clone(),
                            rb_schema
                        ))
                    })?;
                    self.flush_results.extend(results);
                }
            }
            Ok(())
        } else if let Some(inner_writer) = &self.in_progress {
            let inner_writer = inner_writer.clone();
            let mut writer = inner_writer.lock().await;
            writer.write_record_batch(record_batch).await
        } else {
            Err(DataFusionError::Internal("Invalid state of inner writer".to_string()))
        }
    }

    pub fn flush_and_close(self) -> Result<Vec<u8>> {
        if let Some(inner_writer) = self.in_progress {
            let inner_writer = match Arc::try_unwrap(inner_writer) {
                Ok(inner) => inner,
                Err(_) => {
                    return Err(DataFusionError::Internal(
                        "Cannot get ownership of inner writer".to_string(),
                    ))
                }
            };
            let runtime = self.runtime;
            runtime.block_on(async move {
                let writer = inner_writer.into_inner();

                let mut grouped_results: HashMap<String, Vec<String>> = HashMap::new();
                let results = writer
                    .flush_and_close()
                    .await
                    .map_err(|e| DataFusionError::Internal(format!("err={}, config={:?}", e, self.config.clone())))?;
                for (partition_desc, file, object_meta, metadata) in self.flush_results.into_iter().chain(results) {
                    let encoded = format!("{}\x03{}\x03{}", file, object_meta.size, get_file_exist_col(&metadata));
                    match grouped_results.get_mut(&partition_desc) {
                        Some(files) => {
                            files.push(encoded);
                        }
                        None => {
                            grouped_results.insert(partition_desc, vec![encoded]);
                        }
                    }
                }
                let mut summary = format!("{}", grouped_results.len());
                for (partition_desc, files) in grouped_results.iter() {
                    summary += "\x01";
                    summary += partition_desc.as_str();
                    summary += "\x02";
                    summary += files.join("\x02").as_str();
                }
                Ok(summary.into_bytes())
            })
        } else {
            Ok(vec![])
        }
    }

    pub fn abort_and_close(self) -> Result<()> {
        if let Some(inner_writer) = self.in_progress {
            let inner_writer = match Arc::try_unwrap(inner_writer) {
                Ok(inner) => inner,
                Err(_) => {
                    return Err(DataFusionError::Internal(
                        "Cannot get ownership of inner writer".to_string(),
                    ))
                }
            };
            let runtime = self.runtime;
            runtime.block_on(async move {
                let writer = inner_writer.into_inner();
                writer.abort_and_close().await
            })
        } else {
            Ok(())
        }
    }

    pub fn get_schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    // generate random digit with fixed seed
    fn create_rng_with_seed(&self) -> StdRng {
        StdRng::seed_from_u64(self.config.seed)
    }

    // generate random planes
    fn generate_random_array(&self) -> Result<Array2<f64>,String>{
        match self.config.nbits() {
            Some(nbits) if nbits > 0 => {
                match self.config.d() {
                    Some(d) if d > 0 => {
                        let mut rng = self.create_rng_with_seed();
//                         assert!(d >= nbits,"the dimension of the embedding must be greater than nbits");
                        let random_array = Array2::from_shape_fn((nbits as usize, d as usize), |_| rng.gen_range(-1.0..1.0));
                        Ok(random_array)
                    }
                    Some(_) => Err("the dimension you input in the config must be greater than 0".to_string()),
                    None => Err("the dimension you input in the config is None".to_string()),
                }
            }
            Some(_) => Err("the number of bits used for binary encoding must be greater than 0".to_string()),
            None => Err("the number of bits used for binary encoding must be greater than 0".to_string()),
        }
    }

    // project the input data
    fn project(&self,input_data:&ListArray,random_plans:&Result<Array2<f64>,String>) -> Result<Array2<f64>,String>{
        let list_len = input_data.len();
        assert!(list_len > 0,"the length of input data must be large than 0");
        let dimension_len = input_data.value(0).len();

        let input_values = if let Some(values) = input_data.values().as_any().downcast_ref::<Float32Array>(){
            let float64_values: Vec<f64> = values.iter().map(|x| x.unwrap() as f64).collect();
            Float64Array::from(float64_values)
        } else if let Some(values) = input_data.values().as_any().downcast_ref::<Float64Array>(){
            values.clone()
            }
        else {
                return Err("Unsupported data type in ListArray.".to_string());
            };

        let mut re_array2 = Array2::<f64>::zeros((list_len,dimension_len));

        unsafe {
            let data_ptr = input_values.values().as_ptr();
            let data_size = list_len * dimension_len;
            ptr::copy_nonoverlapping(data_ptr,re_array2.as_mut_ptr(),data_size);
        }
        match random_plans {
            Ok(random_array) => {
                assert!(re_array2.shape()[1] == random_array.shape()[1],"the dimension corresponding to the matrix must be the same");
//                    let final_result = re_array2.dot(&random_array.t());
                let batch_size = 1000;
                let num_batches = re_array2.shape()[0] / batch_size;
                let remaining_rows = re_array2.shape()[0] % batch_size;
                let mut result = vec![];

                for batch_idx in 0..num_batches{
                    let batch_start = batch_idx * batch_size;
                    let batch_end = batch_start + batch_size;

                    let current_batch = re_array2.slice(s![batch_start..batch_end,..]);
                    let random_projection = current_batch.dot(&random_array.t());

                    result.push(random_projection);
                }

                if remaining_rows > 0{
                    let batch_start = num_batches * batch_size;
                    let batch_end = batch_start + remaining_rows;

                    let remaining_batch = re_array2.slice(s![batch_start..batch_end,..]);

                    let random_projection = remaining_batch.dot(&random_array.t());

                    result.push(random_projection);
                }

                let result_views: Vec<ArrayView2<f64>> = result.iter().map(|arr| ArrayView2::from(arr)).collect();


                let final_result = concatenate(Axis(0),&result_views).expect("Failed to concatenate results");

                // println!("{:}",end);

                Ok(final_result)
            }
            Err(e) => {
                eprintln!("Error:{}",e);
                Err(e.to_string())
            }
        }
    }
    // add the input data with their projection
    pub fn lsh(&self,input_embedding:&Option<ListArray>) -> Result<ListArray,String>
    where
    {
        match input_embedding {
            Some(data) => {
                let random_plans = self.generate_random_array();
                let data_projection = self.project(data,&random_plans).unwrap();
                match Ok(data_projection) {
                    Ok(mut projection) => {
                        projection.mapv_inplace(|x| if x >= 0.0 {1.0} else {0.0});
                        let convert:Vec<Vec<u64>> = Self::convert_array_to_u64_vec(&projection);
                        Ok(Self::convert_vec_to_byte_u64(convert))
                    }
                    Err(e) => {
                        eprintln!("Error:{}",e);
                        Err(e)
                    }
                }
            }
            None => {
                Err("the input data is None".to_string())
            }
        }
    }

    fn convert_vec_to_byte_u64(array:Vec<Vec<u64>>) -> ListArray {
            let field = Arc::new(Field::new("element", DataType::Int64,true));
             let values = Int64Array::from(array.iter().flatten().map(|&x| x as i64).collect::<Vec<i64>>());
             let mut offsets = vec![];
             for subarray in array{
                 let current_offset = subarray.len() as usize;
                 offsets.push(current_offset);
               }
             let offsets_buffer = OffsetBuffer::from_lengths(offsets);
             let list_array = GenericListArray::try_new(field,offsets_buffer,Arc::new(values),None).expect("can not list_array");
             list_array

            }

    fn convert_array_to_u64_vec<T>(array:&Array2<f64>) -> Vec<Vec<T>>
    where
        T: TryFrom<u64> + Copy,
        <T as TryFrom<u64>>::Error: std::fmt::Debug,
    {
        let bianry_encode:Vec<Vec<u64>> = array
        .axis_iter(ndarray::Axis(0))
        .map(|row|{
            let mut results = Vec::new();
            let mut acc = 0u64;

            for(i,&bit) in row.iter().enumerate(){
                acc = (acc << 1) | bit as u64;
                if(i + 1) % 64 == 0{
                    results.push(acc);
                    acc = 0;
                }
            }
            if row.len() % 64 != 0{
                results.push(acc);
            }
            results
        })
        .collect();

        bianry_encode
        .into_iter()
        .map(|inner_vec|{
            inner_vec
            .into_iter()
            .map(|x| T::try_from(x).unwrap())
            .collect()
        }).collect()
    }

}

#[cfg(test)]
mod tests {
    use arrow_array::builder;
    use datafusion::catalog::schema;
    use hdf5::File as OtherFile;
    use hdf5::Group;
    use parquet::arrow::ArrowWriter;
    use parquet::column;
    use parquet::file::properties::WriterProperties;
    use crate::{
        lakesoul_io_config::{LakeSoulIOConfigBuilder, OPTION_KEY_MEM_LIMIT},
        lakesoul_reader::LakeSoulReader,
        lakesoul_writer::{AsyncBatchWriter, MultiPartAsyncWriter, SyncSendableMutableLakeSoulWriter},
    };

    use arrow::{
        array::{ArrayRef, Int64Array,FixedSizeListArray,ArrayData},
        record_batch::RecordBatch,
    };
    use arrow_array::{Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{error::Result, physical_expr::math_expressions};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
    use rand::{distributions::DistString, Rng};
    use std::{fs::File, sync::Arc};
    use tokio::{runtime::Builder, time::Instant};
    use tracing_subscriber::layer::SubscriberExt;
    use arrow::buffer::{Buffer, NullBuffer};

    use super::SortAsyncWriter;
    use std::env;
    use std::path::Path;
    use ndarray::{Array2,s,Dim};
    use crate::helpers::get_batch_memory_size;
    use parquet::file::reader::{FileReader,SerializedFileReader};
    use arrow::compute::sort;
    use arrow::compute::sort_to_indices;
    use arrow::compute::SortOptions;

    #[test]
    fn test_parquet_async_write() -> Result<()> {
        let runtime = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        runtime.clone().block_on(async move {
            let col = Arc::new(Int64Array::from_iter_values([3, 2, 1])) as ArrayRef;
            let to_write = RecordBatch::try_from_iter([("col", col)])?;
            let temp_dir = tempfile::tempdir()?;
            let path = temp_dir
                .into_path()
                .join("test.parquet")
                .into_os_string()
                .into_string()
                .unwrap();
            let writer_conf = LakeSoulIOConfigBuilder::new()
                .with_files(vec![path.clone()])
                .with_thread_num(2)
                .with_batch_size(256)
                .with_max_row_group_size(2)
                .with_schema(to_write.schema())
                .build();
            let mut async_writer = MultiPartAsyncWriter::try_new(writer_conf).await?;
            async_writer.write_record_batch(to_write.clone()).await?;
            Box::new(async_writer).flush_and_close().await?;

            let file = File::open(path.clone())?;
            let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 1024).unwrap();

            let actual_batch = record_batch_reader
                .next()
                .expect("No batch found")
                .expect("Unable to get batch");

            assert_eq!(to_write.schema(), actual_batch.schema());
            assert_eq!(to_write.num_columns(), actual_batch.num_columns());
            assert_eq!(to_write.num_rows(), actual_batch.num_rows());
            for i in 0..to_write.num_columns() {
                let expected_data = to_write.column(i).to_data();
                let actual_data = actual_batch.column(i).to_data();

                assert_eq!(expected_data, actual_data);
            }

            let writer_conf = LakeSoulIOConfigBuilder::new()
                .with_files(vec![path.clone()])
                .with_thread_num(2)
                .with_batch_size(256)
                .with_max_row_group_size(2)
                .with_schema(to_write.schema())
                .with_primary_keys(vec!["col".to_string()])
                .build();

            let async_writer = MultiPartAsyncWriter::try_new(writer_conf.clone()).await?;
            let mut async_writer = SortAsyncWriter::try_new(async_writer, writer_conf)?;
            async_writer.write_record_batch(to_write.clone()).await?;
            Box::new(async_writer).flush_and_close().await?;

            let file = File::open(path)?;
            let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 1024).unwrap();

            let actual_batch = record_batch_reader
                .next()
                .expect("No batch found")
                .expect("Unable to get batch");

            let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
            let to_read = RecordBatch::try_from_iter([("col", col)])?;
            assert_eq!(to_read.schema(), actual_batch.schema());
            assert_eq!(to_read.num_columns(), actual_batch.num_columns());
            assert_eq!(to_read.num_rows(), actual_batch.num_rows());
            for i in 0..to_read.num_columns() {
                let expected_data = to_read.column(i).to_data();
                let actual_data = actual_batch.column(i).to_data();

                assert_eq!(expected_data, actual_data);
            }
            Ok(())
        })
    }

    #[test]
    fn test_parquet_async_write_with_aux_sort() -> Result<()> {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let col = Arc::new(Int64Array::from_iter_values([3, 2, 3])) as ArrayRef;
        let col1 = Arc::new(Int64Array::from_iter_values([5, 3, 2])) as ArrayRef;
        let col2 = Arc::new(Int64Array::from_iter_values([3, 2, 1])) as ArrayRef;
        let to_write = RecordBatch::try_from_iter([("col", col), ("col1", col1), ("col2", col2)])?;
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir
            .into_path()
            .join("test.parquet")
            .into_os_string()
            .into_string()
            .unwrap();
        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![path.clone()])
            .with_thread_num(2)
            .with_batch_size(256)
            .with_max_row_group_size(2)
            .with_schema(to_write.schema())
            .with_primary_keys(vec!["col".to_string()])
            .with_aux_sort_column("col2".to_string())
            .build();

        let mut writer = SyncSendableMutableLakeSoulWriter::try_new(writer_conf, runtime)?;
        writer.write_batch(to_write.clone())?;
        writer.flush_and_close()?;

        let file = File::open(path.clone())?;
        let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 1024).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");
        let col = Arc::new(Int64Array::from_iter_values([2, 3, 3])) as ArrayRef;
        let col1 = Arc::new(Int64Array::from_iter_values([3, 2, 5])) as ArrayRef;
        let to_read = RecordBatch::try_from_iter([("col", col), ("col1", col1)])?;

        assert_eq!(to_read.schema(), actual_batch.schema());
        assert_eq!(to_read.num_columns(), actual_batch.num_columns());
        assert_eq!(to_read.num_rows(), actual_batch.num_rows());
        for i in 0..to_read.num_columns() {
            let expected_data = to_read.column(i).to_data();
            let actual_data = actual_batch.column(i).to_data();

            assert_eq!(expected_data, actual_data);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_s3_read_write() -> Result<()> {
        let common_conf_builder = LakeSoulIOConfigBuilder::new()
            .with_thread_num(2)
            .with_batch_size(8192)
            .with_max_row_group_size(250000)
            .with_object_store_option("fs.s3a.access.key".to_string(), "minioadmin1".to_string())
            .with_object_store_option("fs.s3a.secret.key".to_string(), "minioadmin1".to_string())
            .with_object_store_option("fs.s3a.endpoint".to_string(), "http://localhost:9000".to_string());

        let read_conf = common_conf_builder
            .clone()
            .with_files(vec![
                "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet".to_string()
            ])
            .build();
        let mut reader = LakeSoulReader::new(read_conf)?;
        reader.start().await?;

        let schema = reader.schema.clone().unwrap();

        let write_conf = common_conf_builder
            .clone()
            .with_files(vec![
                "s3://lakesoul-test-bucket/data/native-io-test/large_file_written.parquet".to_string(),
            ])
            .with_schema(schema)
            .build();
        let mut async_writer = MultiPartAsyncWriter::try_new(write_conf).await?;

        while let Some(rb) = reader.next_rb().await {
            let rb = rb?;
            async_writer.write_record_batch(rb).await?;
        }

        Box::new(async_writer).flush_and_close().await?;
        drop(reader);

        Ok(())
    }

    #[test]
    fn test_sort_spill_write() -> Result<()> {
        let runtime = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        runtime.clone().block_on(async move {
            let common_conf_builder = LakeSoulIOConfigBuilder::new()
                .with_thread_num(2)
                .with_batch_size(8192)
                .with_max_row_group_size(250000);
            let read_conf = common_conf_builder
                .clone()
                .with_files(vec!["large_file.snappy.parquet".to_string()])
                .with_schema(Arc::new(Schema::new(vec![
                    Arc::new(Field::new("uuid", DataType::Utf8, false)),
                    Arc::new(Field::new("ip", DataType::Utf8, false)),
                    Arc::new(Field::new("hostname", DataType::Utf8, false)),
                    Arc::new(Field::new("requests", DataType::Int64, false)),
                    Arc::new(Field::new("name", DataType::Utf8, false)),
                    Arc::new(Field::new("city", DataType::Utf8, false)),
                    Arc::new(Field::new("job", DataType::Utf8, false)),
                    Arc::new(Field::new("phonenum", DataType::Utf8, false)),
                ])))
                .build();
            let mut reader = LakeSoulReader::new(read_conf)?;
            reader.start().await?;

            let schema = reader.schema.clone().unwrap();

            let write_conf = common_conf_builder
                .clone()
                .with_files(vec!["/home/chenxu/program/data/large_file_written.parquet".to_string()])
                .with_primary_key("uuid".to_string())
                .with_schema(schema)
                .build();
            let async_writer = MultiPartAsyncWriter::try_new(write_conf.clone()).await?;
            let mut async_writer = SortAsyncWriter::try_new(async_writer, write_conf)?;

            while let Some(rb) = reader.next_rb().await {
                let rb = rb?;
                async_writer.write_record_batch(rb).await?;
            }

            Box::new(async_writer).flush_and_close().await?;
            drop(reader);

            Ok(())
        })
    }

    #[test]
    fn test_s3_read_sort_write() -> Result<()> {
        let runtime = Arc::new(Builder::new_multi_thread().enable_all().build().unwrap());
        runtime.clone().block_on(async move {
            let common_conf_builder = LakeSoulIOConfigBuilder::new()
                .with_thread_num(2)
                .with_batch_size(8192)
                .with_max_row_group_size(250000)
                .with_object_store_option("fs.s3a.access.key".to_string(), "minioadmin1".to_string())
                .with_object_store_option("fs.s3a.secret.key".to_string(), "minioadmin1".to_string())
                .with_object_store_option("fs.s3a.endpoint".to_string(), "http://localhost:9000".to_string());

            let read_conf = common_conf_builder
                .clone()
                .with_files(vec![
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file.parquet".to_string()
                ])
                .build();
            let mut reader = LakeSoulReader::new(read_conf)?;
            reader.start().await?;

            let schema = reader.schema.clone().unwrap();

            let write_conf = common_conf_builder
                .clone()
                .with_files(vec![
                    "s3://lakesoul-test-bucket/data/native-io-test/large_file_written_sorted.parquet".to_string(),
                ])
                .with_schema(schema)
                .with_primary_keys(vec!["str0".to_string(), "str1".to_string(), "int1".to_string()])
                .build();
            let async_writer = MultiPartAsyncWriter::try_new(write_conf.clone()).await?;
            let mut async_writer = SortAsyncWriter::try_new(async_writer, write_conf)?;

            while let Some(rb) = reader.next_rb().await {
                let rb = rb?;
                async_writer.write_record_batch(rb).await?;
            }

            Box::new(async_writer).flush_and_close().await?;
            drop(reader);

            Ok(())
        })
    }

    fn create_batch(num_columns: usize, num_rows: usize, str_len: usize) -> RecordBatch {
        let mut rng = rand::thread_rng();
        let mut len_rng = rand::thread_rng();
        let iter = (0..num_columns)
            .into_iter()
            .map(|i| {
                (
                    format!("col_{}", i),
                    Arc::new(StringArray::from(
                        (0..num_rows)
                            .into_iter()
                            .map(|_| {
                                rand::distributions::Alphanumeric
                                    .sample_string(&mut rng, len_rng.gen_range(str_len..str_len * 3))
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                    true,
                )
            })
            .collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    #[test]
    fn test_writer_of_large_columns() -> Result<()> {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let num_batch = 39;
        let num_rows = 100;
        let num_columns = 2000;
        let str_len = 4;

        let to_write = create_batch(num_columns, num_rows, str_len);
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir
            .into_path()
            .join("test.parquet")
            .into_os_string()
            .into_string()
            .unwrap();
        dbg!(&path);
        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![path.clone()])
            .with_thread_num(2)
            .with_batch_size(num_rows)
            .with_max_row_group_size(2000)
            // .with_max_row_group_num_values(4_00_000)
            .with_schema(to_write.schema())
            // .with_primary_keys(vec!["col".to_string()])
            // .with_aux_sort_column("col2".to_string())
            .build();

        let mut writer = SyncSendableMutableLakeSoulWriter::try_new(writer_conf, runtime)?;

        let start = Instant::now();
        for _ in 0..num_batch {
            let once_start = Instant::now();
            writer.write_batch(create_batch(num_columns, num_rows, str_len))?;
            println!("write batch once cost: {}", once_start.elapsed().as_millis());
        }
        let flush_start = Instant::now();
        writer.flush_and_close()?;
        println!("flush cost: {}", flush_start.elapsed().as_millis());
        println!(
            "num_batch={}, num_columns={}, num_rows={}, str_len={}, cost_mills={}",
            num_batch,
            num_columns,
            num_rows,
            str_len,
            start.elapsed().as_millis()
        );

        let file = File::open(path.clone())?;
        let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 100_000).unwrap();

        let actual_batch = record_batch_reader
            .next()
            .expect("No batch found")
            .expect("Unable to get batch");

        assert_eq!(to_write.schema(), actual_batch.schema());
        assert_eq!(num_columns, actual_batch.num_columns());
        assert_eq!(num_rows * num_batch, actual_batch.num_rows());
        Ok(())
    }

    #[cfg(feature = "dhat-heap")]
    #[global_allocator]
    static ALLOC: dhat::Alloc = dhat::Alloc;

    #[tracing::instrument]
    #[test]
    fn writer_profiling() -> Result<()> {
        use tracing_subscriber::fmt;

        tracing_subscriber::fmt::init();

        let subscriber = fmt::layer().event_format(
            fmt::format::Format::default()
                .with_level(true)
                .with_source_location(true)
                .with_file(true),
        );
        // .with_max_level(Level::TRACE);
        tracing_subscriber::registry().with(subscriber);

        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let num_batch = 100;
        let num_rows = 1000;
        let num_columns = 100;
        let str_len = 4;

        let to_write = create_batch(num_columns, num_rows, str_len);
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir
            .into_path()
            .join("test.parquet")
            .into_os_string()
            .into_string()
            .unwrap();
        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![path.clone()])
            .with_prefix(tempfile::tempdir()?.into_path().into_os_string().into_string().unwrap())
            .with_thread_num(2)
            .with_batch_size(num_rows)
            // .with_max_row_group_size(2000)
            // .with_max_row_group_num_values(4_00_000)
            .with_schema(to_write.schema())
            .with_primary_keys(
                // (0..num_columns - 1)
                (0..3)
                    .into_iter()
                    .map(|i| format!("col_{}", i))
                    .collect::<Vec<String>>(),
            )
            // .with_aux_sort_column("col2".to_string())
            .with_option(OPTION_KEY_MEM_LIMIT, format!("{}", 1024 * 1024 * 48))
            .set_dynamic_partition(true)
            .with_hash_bucket_num(4)
            // .with_max_file_size(1024 * 1024 * 32)
            .build();

        let mut writer = SyncSendableMutableLakeSoulWriter::try_new(writer_conf, runtime)?;

        let start = Instant::now();
        for _ in 0..num_batch {
            // let once_start = Instant::now();
            writer.write_batch(create_batch(num_columns, num_rows, str_len))?;
            // println!("write batch once cost: {}", once_start.elapsed().as_millis());
        }
        let flush_start = Instant::now();
        writer.flush_and_close()?;
        println!("flush cost: {}", flush_start.elapsed().as_millis());
        println!(
            "num_batch={}, num_columns={}, num_rows={}, str_len={}, cost_mills={}",
            num_batch,
            num_columns,
            num_rows,
            str_len,
            start.elapsed().as_millis()
        );

        // let file = File::open(path.clone())?;
        // let mut record_batch_reader = ParquetRecordBatchReader::try_new(file, 100_000).unwrap();

        // let actual_batch = record_batch_reader
        //     .next()
        //     .expect("No batch found")
        //     .expect("Unable to get batch");

        // assert_eq!(to_write.schema(), actual_batch.schema());
        // assert_eq!(num_columns, actual_batch.num_columns());
        // assert_eq!(num_rows * num_batch, actual_batch.num_rows());
        Ok(())
    }

}
