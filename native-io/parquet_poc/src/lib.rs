#[cfg(test)]
mod tests{
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use futures::TryStreamExt;
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
}

