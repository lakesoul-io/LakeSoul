
use bytes::{Buf, Bytes};
use parquet::arrow::{async_reader::{ParquetRecordBatchStreamBuilder,ParquetRecordBatchStream}, ProjectionMask};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector};
use parquet::errors::{ParquetError, Result};
use parquet::file::footer::parse_metadata;
use tokio::fs::File;



pub struct OffsetParquetStreamBuilder {
    file_path: String,
    offset_rows: usize,
    batch_size: usize,
}


impl OffsetParquetStreamBuilder{
    // Build a new [`ParquetRecordBatchStream`]
     pub async fn build(self) -> Result<ParquetRecordBatchStream<File>> {
        let file = File::open(
            self.file_path
        ).await.unwrap();
        let builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap()
            .with_batch_size(self.batch_size);

        let file_metadata = builder.metadata().file_metadata();
        // let mask = ProjectionMask::roots(file_metadata.schema_descr(), vec![0, 1, 2]);
        let mask = ProjectionMask::all();
        println!("{:?}",file_metadata);
        
        let selection = RowSelection::from(vec![
            RowSelector::skip(self.offset_rows),   
            RowSelector::select(file_metadata.num_rows() as usize - self.offset_rows),
        ]);
        builder
            .with_projection(mask)
            .with_row_selection(selection.clone())
            .build()
    }
}



#[cfg(test)]
mod tests{
    use tokio::fs::File;
    use parquet::arrow::{async_reader::ParquetRecordBatchStreamBuilder, ProjectionMask};
    use arrow::util::pretty::print_batches;
    use futures::TryStreamExt;

    use crate::reader::parquet_stream::OffsetParquetStreamBuilder;

    #[tokio::test]
    async fn test_stream(){

        let builder = OffsetParquetStreamBuilder{
            file_path: 
                "/Users/ceng/part-00003-68b546de-5cc6-4abb-a8a9-f6af2e372791-c000.snappy.parquet"
                // "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
                .to_string(),
            offset_rows: 0,
            batch_size: 8192,
        };

        let mut stream = builder.build().await.unwrap();
        
        let results = stream.try_collect::<Vec<_>>().await.unwrap();
        // assert_eq!(results.len(), 3);
        assert_eq!(results.iter().map(|x| x.num_rows()).sum::<usize>(), 10000000);

        
        // print_batches(&results);
    }

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow::error::Result as ArrowResult;

    #[test]
    fn test_batch_reader(){
        let file = std::fs::File::open(
            "/Users/ceng/part-00003-68b546de-5cc6-4abb-a8a9-f6af2e372791-c000.snappy.parquet"
            // "/Users/ceng/Documents/GitHub/LakeSoul/native-io/lakesoul-io-java/src/test/resources/sample-parquet-files/part-00000-a9e77425-5fb4-456f-ba52-f821123bd193-c000.snappy.parquet"
        ).unwrap();
        
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        println!("Converted arrow schema is: {}", builder.schema());
        
        let mut reader = builder.with_batch_size(8192).build().unwrap();
        
        let batches = reader.collect::<ArrowResult<Vec<_>>>().unwrap();
        
        println!("Read {} records.", batches.len());
        assert_eq!(batches.iter().map(|x| x.num_rows()).sum::<usize>(), 10000000);
    }
}