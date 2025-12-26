// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod compaction_tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
    
    use arrow::record_batch::RecordBatch;
    use arrow::array::{ArrayRef, Int32Array};
    use lakesoul_metadata::MetaDataClient;

    use crate::catalog::compaction_with_merge_op;
    use crate::catalog::{create_table, compaction};
    use crate::error::Result;

    async fn check_record_batch() -> Result<()> {
        Ok(())
    }

    async fn check_data_info() -> Result<()> {
        Ok(())
    }

    async fn test_simple_compaction() -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = "test_simple_compaction";

        let _record_batch_1 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 2, 3, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef, false),
                ("value", Arc::new(Int32Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef, true),
            ]
        )?;

        create_table(client.clone(), table_name, LakeSoulIOConfigBuilder::default().build()).await?;


        check_data_info().await?;

        let _record_batch_2 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 2, 3, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef, false),
                ("value", Arc::new(Int32Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef, true),
            ]
        )?;


        check_data_info().await?;

        compaction(client.clone(), table_name, "").await?;

        check_data_info().await?;

        Ok(())
    }

    async fn test_simple_compaction_with_condition() -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = "test_simple_compaction_with_condition";

        let _record_batch_1 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 1, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef, false),
                ("value", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef, true),
            ]
        )?;

        let _record_batch_2 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, false),
                ("name", Arc::new(Int32Array::from(vec![11, 22, 33])) as ArrayRef, true),
            ]
        )?;

        create_table(client.clone(), table_name, LakeSoulIOConfigBuilder::default().build()).await?;


        check_data_info().await?;

        compaction(client.clone(), table_name, "range=1").await?;

        check_data_info().await?;

        check_data_info().await?;
        
        Ok(())
    }

    async fn test_upsert_after_compaction() -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = "test_upsert_after_compaction";

        let _record_batch_1 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 1, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef, false),
                ("value", Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef, true),
            ]
        )?;

        let _record_batch_2 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef, false),
                ("value", Arc::new(Int32Array::from(vec![11, 22, 33])) as ArrayRef, true),
            ]
        )?;

        let _record_batch_3 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 1, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![2, 3, 4, 5])) as ArrayRef, false),
                ("value", Arc::new(Int32Array::from(vec![222, 333, 444, 555])) as ArrayRef, true),
            ]
        )?;

        create_table(client.clone(), table_name, LakeSoulIOConfigBuilder::default().build()).await?;
        
        compaction(client.clone(), table_name, "range=1").await?;

        check_record_batch().await?;


        check_record_batch().await?;

        compaction(client, table_name, "range=1").await?;

        check_record_batch().await?;

        Ok(())
    }

    async fn test_simple_compaction_with_merge_operator() -> Result<()> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let table_name = "test_simple_compaction_with_merge_operator";

        let _record_batch_1 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 2, 3, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef, false),
                ("v1", Arc::new(Int32Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef, true),
                ("v2", Arc::new(StringArray::from(vec!["1", "1", "1", "2", "3"])) as ArrayRef, true),
            ]
        )?;

        let _record_batch_2 = RecordBatch::try_from_iter_with_nullable(
            vec![
                ("range", Arc::new(Int32Array::from(vec![1, 2, 3, 1, 1])) as ArrayRef, false),
                ("hash", Arc::new(Int32Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef, false),
                ("v1", Arc::new(Int32Array::from(vec![1, 1, 1, 2, 3])) as ArrayRef, true),
                ("v2", Arc::new(StringArray::from(vec!["1", "1", "1", "2", "3"])) as ArrayRef, true),
            ]
        )?;

        create_table(client.clone(), table_name, LakeSoulIOConfigBuilder::default().build()).await?;

        compaction_with_merge_op(client.clone(), table_name, "").await?;

        check_record_batch().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_all_cases()  -> Result<()> {
        test_upsert_after_compaction().await?;
        test_simple_compaction_with_merge_operator().await?;
        test_simple_compaction_with_condition().await?;
        test_simple_compaction().await?;
        Ok(())
    }
    
}