#[cfg(test)]
mod tests{
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use futures::{TryStreamExt, StreamExt};
    use tokio::fs::File;


    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[test]
    fn test_batch_reader(){
        let file = std::fs::File::open(
            "/Users/ceng/part-00003-68b546de-5cc6-4abb-a8a9-f6af2e372791-c000.snappy.parquet"
            // "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
        ).unwrap();
        
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        println!("Converted arrow schema is: {}", builder.schema());
        
        let mut reader = builder.build().unwrap();
        
        let record_batch = reader.next().unwrap().unwrap();
        
        println!("Read {} records.", record_batch.num_rows());
    }
    
    use parquet::arrow::{async_reader::ParquetRecordBatchStreamBuilder, ProjectionMask};
    use arrow::util::pretty::print_batches;
    

    #[tokio::test]
    async fn test_stream(){
        // let testdata = arrow::util::test_util::parquet_test_data();
        // let path = format!("{}/alltypes_plain.parquet", testdata);
        let file = File::open(
            // "/Users/ceng/part-00003-68b546de-5cc6-4abb-a8a9-f6af2e372791-c000.snappy.parquet"
            "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
        ).await.unwrap();

        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap()
            .with_batch_size(3);

        let file_metadata = builder.metadata().file_metadata();
        let mask = ProjectionMask::roots(file_metadata.schema_descr(), [0, 1, 2, 6]);

        let mut stream = builder.with_projection(mask).build().unwrap();
        // let results =  stream.try_next().await.unwrap().unwrap();
        let results = stream.try_collect::<Vec<_>>().await.unwrap();
        // assert_eq!(results.len(), 3);

        
        print_batches(&results);
    }

    use bytes::{Buf, Bytes};

    use futures::future::BoxFuture;
    use futures::FutureExt;

    use parquet::file::footer::parse_metadata;
    use parquet::file::metadata::ParquetMetaData;
    use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
    use parquet::arrow::async_reader::AsyncFileReader;
    use parquet::errors::{ParquetError, Result};


    use arrow::error::Result as ArrowResult;

    use std::string::ParseError;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::ops::Range;



    struct TestReader {
        data: Bytes,
        metadata: Arc<ParquetMetaData>,
        requests: Arc<Mutex<Vec<Range<usize>>>>,
    }

    impl AsyncFileReader for TestReader {
        fn get_bytes(&mut self, range: Range<usize>) -> BoxFuture<'_, Result<Bytes>> {
            self.requests.lock().unwrap().push(range.clone());
            futures::future::ready(Ok(self.data.slice(range))).boxed()
        }

        fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>> {
            futures::future::ready(Ok(self.metadata.clone())).boxed()
        }
    }


    #[tokio::test]
    async fn test_async_reader_skip_pages() {
        // let testdata = arrow::util::test_util::parquet_test_data();
        // let path = format!("{}/alltypes_tiny_pages_plain.parquet", testdata);
        let data = Bytes::from(std::fs::read(
            // "/Users/ceng/part-00003-68b546de-5cc6-4abb-a8a9-f6af2e372791-c000.snappy.parquet"
            "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
        ).unwrap());

        let metadata = parse_metadata(&data).unwrap();
        let metadata = Arc::new(metadata);
        // println!("{:?}",metadata);
        // println!("{:?}",metadata.row_group(0));
        // return;

        assert_eq!(metadata.num_row_groups(), 1);

        let async_reader = TestReader {
            data: data.clone(),
            metadata: metadata.clone(),
            requests: Default::default(),
        };

        let options = ArrowReaderOptions::new().with_page_index(true);
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_options(async_reader, options)
                .await
                .unwrap();

        let selection = RowSelection::from(vec![
            RowSelector::skip(21),   
            RowSelector::select(100),
        ]);

        let mask = ProjectionMask::leaves(builder.parquet_schema(), vec![0, 1, 2]);

        let stream = builder
            .with_projection(mask.clone())
            .with_row_groups(vec![0])
            .with_row_selection(selection.clone())
            .build()
            .expect("building stream");

        let async_batches: Vec<_> = stream.try_collect().await.unwrap();

        let sync_batches = ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_projection(mask)
            .with_batch_size(1024)
            .with_row_selection(selection)
            .build()
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();

        assert_eq!(async_batches, sync_batches);
        print_batches(async_batches.as_slice());

    }


    use parquet::file::reader::SerializedFileReader;
    use std::convert::TryFrom;


    #[test]
    fn test_read() {
        let paths = vec![
            "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet",
        ];
        // Create a reader for each file and flat map rows
        let rows = paths.iter()
            .map(|p| SerializedFileReader::try_from(*p).unwrap())
            .flat_map(|r| r.into_iter());

        for row in rows {
            println!("{}", row);
        }

    }

    use object_store::{RetryConfig, path::Path, ObjectStore, aws::AmazonS3Builder};

    fn get_s3_object_store() -> Arc<dyn ObjectStore> {
        let key = "minioadmin1";
        let secret = "minioadmin1";
        let region = "us-east-1";
        let bucket = "lakesoul-test-s3";
        let endpoint = "http://localhost:9000";
        let retry_config = RetryConfig {
            backoff: Default::default(),
            max_retries: 4,
            retry_timeout: Default::default()
        };
        let s3_store = AmazonS3Builder::new()
            .with_access_key_id(key)
            .with_secret_access_key(secret)
            .with_region(region)
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .with_retry(retry_config)
            .with_allow_http(true)
            .build();
        let object_store: Arc<dyn ObjectStore> = Arc::new(s3_store.unwrap());
        object_store
    }

    #[tokio::test]
    async fn test_s3_read() {
        let object_store = get_s3_object_store();

        // list all files under bucket_name/sub_folder
        let prefix: Path = "/sub_folder".try_into().unwrap();
        let list_stream = object_store
            .list(Some(&prefix))
            .await
            .expect("Error listing files");
        list_stream
            .for_each(move |meta| {
                async {
                    let meta = meta.expect("Error listing");
                    println!("Name: {}, size: {}", meta.location, meta.size);
                }
            })
            .await;
        
        // fetch objects
        let path: Path = "part-00004-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet".try_into().unwrap();
        let s3_data = object_store.get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let s3_metadata = parse_metadata(&s3_data).unwrap();
        let s3_metadata = Arc::new(s3_metadata);

        assert_eq!(s3_metadata.num_row_groups(), 1);

        let s3_sync_batches = ParquetRecordBatchReaderBuilder::try_new(s3_data)
            .unwrap()
            .with_batch_size(1024)
            .build()
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();
        // print_batches(s3_sync_batches.as_slice());

        let local_data = Bytes::from(std::fs::read(
            "/home/yuchanghui/syl_code/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00004-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
        ).unwrap());

        let local_metadata = parse_metadata(&local_data).unwrap();
        let local_metadata = Arc::new(local_metadata);
        let local_sync_batches = ParquetRecordBatchReaderBuilder::try_new(local_data)
            .unwrap()
            .with_batch_size(1024)
            .build()
            .unwrap()
            .collect::<ArrowResult<Vec<_>>>()
            .unwrap();
        
        assert_eq!(s3_metadata.num_row_groups(), 1);
        assert_eq!(local_sync_batches, s3_sync_batches);
    }

    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::datasource::object_store::ObjectStoreUrl;
    #[tokio::test]
    async fn test_datafusion_runtimeenv() {
        let object_store = get_s3_object_store();
        let bucket = "lakesoul-test-s3";

        let runtime = RuntimeEnv::new(RuntimeConfig::new()).unwrap();
        runtime.register_object_store("s3", bucket, object_store);

        let object_store = runtime.object_store(ObjectStoreUrl::parse("s3://lakesoul-test-s3/").unwrap()).unwrap();
        let path: Path = "part-00004-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet".try_into().unwrap();
        let s3_data = object_store.get(&path)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let s3_metadata = parse_metadata(&s3_data).unwrap();
        assert_eq!(s3_metadata.num_row_groups(), 1);
    }

}

