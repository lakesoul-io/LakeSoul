// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod streaming_tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use arrow::record_batch::RecordBatch;
    use arrow::array::{ArrayRef, Int32Array};

    use arrow::array::StringArray;
    use arrow::datatypes::SchemaRef;
    use datafusion::assert_batches_eq;
    use datafusion::execution::context::SessionContext;
    use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfigBuilder, create_session_context};
    use lakesoul_metadata::MetaDataClient;

    use crate::catalog::{create_table, create_table_with_config_map};
    use crate::error::Result;
    use crate::lakesoul_table::LakeSoulTable;

    async fn test_snapshot_read_with_single_partition() -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = "test_snapshot_read_with_single_partition";

        let record_batch_1 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(StringArray::from(vec!["range1", "range2"])) as ArrayRef, false),
                ("hash", Arc::new(StringArray::from(vec!["hash1-1", "hash2-1"])) as ArrayRef, false),
                ("op", Arc::new(StringArray::from(vec!["insert", "insert"])) as ArrayRef, true),
            ]
        )?;

        let record_batch_2 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(StringArray::from(vec!["range1", "range1", "range2", "range2"])) as ArrayRef, false),
                ("hash", Arc::new(StringArray::from(vec!["hash1-1", "hash1-5", "hash2-1", "hash2-5"])) as ArrayRef, false),
                ("op", Arc::new(StringArray::from(vec!["delete", "insert", "delete", "insert"])) as ArrayRef, true),
            ]
        )?;

        let record_batch_3 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(StringArray::from(vec!["range1", "range2"])) as ArrayRef, false),
                ("hash", Arc::new(StringArray::from(vec!["hash1-2", "hash2-2"])) as ArrayRef, false),
                ("op", Arc::new(StringArray::from(vec!["update", "update"])) as ArrayRef, true),
            ]
        )?;

        let schema = record_batch_1.schema();
        create_table_with_config_map(client.clone(), table_name, 
            [
                    ("schema".to_string(), serde_json::to_string(&schema).unwrap()),
                    ("rangePartitions".to_string(), "range".to_string()),
                    ("hashPartitions".to_string(), "hash".to_string()),
                    ("hashBucketNum".to_string(), "2".to_string()),
                ].into_iter().collect()).await?;

        let context = create_session_context(&mut LakeSoulIOConfigBuilder::default().build())?;

        // let table = LakeSoulTable::for_path_snapshot(table_name.to_string());
        let table = LakeSoulTable::for_name(table_name).await?;

        table.execute_upsert(record_batch_1).await?;

        table.execute_upsert(record_batch_2).await?;

        let current = SystemTime::now();
        
        table.execute_upsert(record_batch_3).await?;


        let result = table.to_dataframe(&context).await?.collect().await?;

        assert_batches_eq!(
            &[
                "+--------+---------+--------+",
                "| range  | hash    | op     |",
                "+--------+---------+--------+",
                "| range1 | hash1-1 | delete |",
                "| range1 | hash1-2 | update |",
                "| range1 | hash1-5 | insert |",
                "+--------+---------+--------+",
            ], 
            &result
        );


        Ok(())
    }

    async fn test_snapshot_read_with_multi_partition() -> Result<()> {
        Ok(())
    }

    async fn test_snapshot_read_without_partition() -> Result<()> {
        Ok(())
    }

    async fn test_incremental_read_with_single_partition() -> Result<()> {
        Ok(())
    }

    async fn test_incremental_read_with_multi_partition() -> Result<()> {
        Ok(())
    }

    async fn test_incremental_read_without_partition() -> Result<()> {
        Ok(())
    }

    async fn test_streaming_read_with_single_partition() -> Result<()> {
        Ok(())
    }

    async fn test_streaming_read_with_multi_partition() -> Result<()> {
        Ok(())
    }

    async fn test_streaming_read_without_partition() -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = "test_streaming_read_without_partition";

        let record_batch_1 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 1, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef, false),
                ("value", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef, true),
            ]
        )?;

        let record_batch_2 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, false),
                ("value", Arc::new(Int32Array::from(vec![11, 22, 33])) as ArrayRef, true),
            ]
        )?;

        let record_batch_3 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 1, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![2, 3, 4, 5])) as ArrayRef, false),
                ("value", Arc::new(Int32Array::from(vec![222, 333, 444, 555])) as ArrayRef, true),
            ]
        )?;

        create_table_with_config_map(client.clone(), table_name, [("a".to_string(), "value".to_string())].into_iter().collect()).await?;

        let context = create_session_context(&mut LakeSoulIOConfigBuilder::default().build())?;

        let table = LakeSoulTable::for_path_snapshot(table_name.to_string()).await?;

        table.execute_upsert(record_batch_1).await?;

        table.execute_upsert(record_batch_2).await?;

        let current = SystemTime::now();

        let join_handle = tokio::spawn(async {
            
        });
        
        // table.execute_upsert(record_batch_3).await?;

        

        assert_batches_eq!(&[""], &[record_batch_3]);

        Ok(())
    }



    #[tokio::test]
    async fn test_all_cases()  -> Result<()> {
        test_snapshot_read_with_single_partition().await?;
        test_snapshot_read_with_multi_partition().await?;
        test_snapshot_read_without_partition().await?;
        test_incremental_read_with_single_partition().await?;
        test_incremental_read_with_multi_partition().await?;
        test_incremental_read_without_partition().await?;
        test_streaming_read_with_single_partition().await?;
        test_streaming_read_with_multi_partition().await?;
        test_streaming_read_without_partition().await?;
        Ok(())
    }
}