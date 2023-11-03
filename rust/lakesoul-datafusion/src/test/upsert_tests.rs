// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod upsert_with_io_config_tests {
    use std::sync::Arc;
    use std::env;
    use std::path::PathBuf;
    use std::time::SystemTime;
    
    use lakesoul_io::arrow::record_batch::RecordBatch;
    use lakesoul_io::arrow::util::pretty::print_batches;
    use lakesoul_io::datafusion::assert_batches_eq;
    use lakesoul_io::datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use lakesoul_io::datafusion::prelude::SessionContext;
    use lakesoul_io::lakesoul_reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
    use lakesoul_io::tokio::runtime::Builder;
    use lakesoul_io::arrow;
    use lakesoul_io::arrow::array::{ArrayRef, Int32Array};
    use lakesoul_io::arrow::datatypes::{Schema, SchemaRef, Field};
    use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
    use lakesoul_io::lakesoul_writer::SyncSendableMutableLakeSoulWriter;

    
    fn init_table(batch: RecordBatch, table_name: &str, pks:Vec<String>) -> LakeSoulIOConfigBuilder {

        let builder = LakeSoulIOConfigBuilder::new()
                .with_schema(batch.schema())
                .with_primary_keys(pks);
        execute_upsert(batch, table_name, builder.clone())
    }

    fn check_upsert(batch: RecordBatch, table_name: &str, selected_cols: Vec<&str>, filters: Option<String>, builder: LakeSoulIOConfigBuilder, expected: &[&str]) -> LakeSoulIOConfigBuilder {
        let builder = execute_upsert(batch, table_name, builder.clone());
        let builder = builder
            .with_schema(SchemaRef::new(Schema::new(
                selected_cols.iter().map(|col| Field::new(*col, arrow::datatypes::DataType::Int32, true)).collect::<Vec<_>>()
            )));
        let builder = if let Some(filters) = filters {
            builder.with_filter_str(filters)
        } else {
            builder
        };
        let config = builder.clone().build();

        let mut reader = SyncSendableMutableLakeSoulReader::new(LakeSoulReader::new(config).unwrap(), Builder::new_current_thread().build().unwrap());
        let _ = reader.start_blocked();
        let result = reader.next_rb_blocked();
        assert_batches_eq!(expected, &[result.unwrap().unwrap()]);
        builder
    }

    fn execute_upsert(batch: RecordBatch, table_name: &str, builder: LakeSoulIOConfigBuilder) -> LakeSoulIOConfigBuilder {
        let file = [env::temp_dir().to_str().unwrap(), table_name, format!("{}.parquet", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string()).as_str()].iter().collect::<PathBuf>().to_str().unwrap().to_string();
        let builder = builder.with_file(file.clone()).with_schema(batch.schema());
        let config = builder.clone().build();

        let writer = SyncSendableMutableLakeSoulWriter::try_new(config, Builder::new_current_thread().build().unwrap()).unwrap();
        let _ = writer.write_batch(batch);
        let _ = writer.flush_and_close();
        builder
    }

    fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
        let values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names.into_iter().zip(values).map(|(name, array)| (name, array, true)).collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    fn create_batch_optional_i32(names: Vec<&str>, values: Vec<&[Option<i32>]>) -> RecordBatch {
        let values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names.into_iter().zip(values).map(|(name, array)| (name, array, true)).collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    
    #[test]
    fn test_merge_same_column_i32() {
        let table_name = "merge-same_column";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             vec!["range".to_string(), "hash".to_string()]);
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value"], 
            None,
            builder.clone(), 
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 11    |",
                "| 20201101 | 2    | 2     |",
                "| 20201101 | 3    | 33    |",
                "| 20201101 | 4    | 44    |",
                "| 20201102 | 4    | 4     |",
                "+----------+------+-------+",
            ]
        );
    }

    #[test]
    fn test_merge_different_column_i32() {
        let table_name = "merge-different_column";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            vec!["range".to_string(), "hash".to_string()]);
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "name"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value", "name"], 
            None, 
            builder.clone(), 
            &[
                "+----------+------+-------+------+",
                "| range    | hash | value | name |",
                "+----------+------+-------+------+",
                "| 20201101 | 1    | 1     | 11   |",
                "| 20201101 | 2    | 2     |      |",
                "| 20201101 | 3    | 3     | 33   |",
                "| 20201101 | 4    |       | 44   |",
                "| 20201102 | 4    | 4     |      |",
                "+----------+------+-------+------+",
            ]
        );
    }

    #[test]
    fn test_merge_different_columns_and_filter_by_non_selected_columns_i32() {
        let table_name = "merge-different_columns_and_filter_by_non_selected_columns_i32";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            vec!["range".to_string(), "hash".to_string()]);
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "name"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value"], 
            Some("and(noteq(name, null), gt(name, 0))".to_string()),
            builder.clone(), 
    &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 1     |",
                "| 20201101 | 3    | 3     |",
                "| 20201101 | 4    |       |",
                "+----------+------+-------+",
            ]
        );
    }

    #[test]
    fn test_merge_different_columns_and_filter_partial_rows_i32() {
        let table_name = "merge-different_columns_and_filter_partial_rows_i32";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value", "name"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4], &[11, 22, 33, 44]]),
            table_name, 
            vec!["range".to_string(), "hash".to_string()]);
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 3, 4, 4], &[2, 4, 5, 5]]), 
            table_name, 
            vec!["range", "hash", "value", "name"], 
            Some("and(and(noteq(value, null), lt(value, 5)),and(noteq(name, null), gt(name, 0)))".to_string()),
            builder.clone(), 
    &[
                "+----------+------+-------+------+",
                "| range    | hash | value | name |",
                "+----------+------+-------+------+",
                "| 20201101 | 1    | 2     | 11   |",
                "| 20201101 | 2    | 2     | 22   |",
                "| 20201101 | 3    | 4     | 33   |",
                "+----------+------+-------+------+",
            ]
        );
    }

    #[test]
    fn test_merge_one_file_with_empty_batch_i32() {
        let table_name = "merge_one_file_with_empty_batch";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             vec!["range".to_string(), "hash".to_string()]);
        
        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                vec!["range", "hash", "value"].iter().map(|col| Field::new(*col, arrow::datatypes::DataType::Int32, true)).collect::<Vec<_>>()
            ))), 
            table_name, 
            vec!["range", "hash", "value"], 
            Some("and(noteq(value, null), lt(value, 3))".to_string()),
            builder.clone(), 
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 1     |",
                "| 20201101 | 2    | 2     |",
                "+----------+------+-------+",
            ]
        );
    }

    #[test]
    fn test_merge_multi_files_with_empty_batch_i32() {
        let table_name = "merge_multi_files_with_empty_batch";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102, 20201102], &[1, 2, 3, 4, 1], &[1, 2, 3, 4, 1]]),
             table_name, 
             vec!["range".to_string(), "hash".to_string()]);
        
        let builder = execute_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102], &[4], &[5]]),
            table_name, 
            builder);
        
        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                vec!["range", "hash", "value"].iter().map(|col| Field::new(*col, arrow::datatypes::DataType::Int32, true)).collect::<Vec<_>>()
            ))), 
            table_name, 
            vec!["range", "hash", "value"], 
            Some("and(noteq(value, null), lt(value, 3))".to_string()),
            builder.clone(), 
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 1     |",
                "| 20201101 | 2    | 2     |",
                "| 20201102 | 1    | 1     |",
                "+----------+------+-------+",
            ]
        );
    }

    #[test]
    fn test_basic_upsert_same_columns() {
        // require metadata checker
    }

    #[test]
    fn test_basic_upsert_different_columns() {
        // require metadata checker
    }

    #[test]
    fn test_should_failed_to_upsert_external_columns_when_schema_auto_migrate_is_false() {
        // require metadata checker
    }

    #[test]
    fn test_upsert_in_new_table_should_failed() {
        // require metadata checker
    }

    #[test]
    fn test_upsert_cant_use_delta_file() {
        // require metadata checker
    }

    #[test]
    fn test_upsert_without_range_parqitions_i32() {
        let table_name = "upsert_without_range_parqitions";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             vec!["hash".to_string()]);
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value"], 
            None,
            builder.clone(), 
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 11    |",
                "| 20201101 | 2    | 2     |",
                "| 20201101 | 3    | 33    |",
                "| 20201101 | 4    | 44    |",
                "+----------+------+-------+",
            ]
        );

    }

    #[test]
    fn test_upsert_without_hash_partitions_should_fail() {
        // require metadata checker
    }

    #[test]
    fn test_upsert_with_multiple_range_and_hash_parqitions_i32() {
        let table_name = "upsert_with_multiple_range_and_hash_parqitions";
        let builder = init_table(
            create_batch_i32(vec!["range1", "range2", "hash1", "hash2", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             vec!["range1".to_string(), "range2".to_string(), "hash1".to_string(), "hash2".to_string()]);
        
        check_upsert(
            create_batch_i32(vec!["range1", "range2", "hash1", "hash2", "value"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[1, 3, 4],&[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range1", "range2", "hash1", "hash2", "value"], 
            None,
            builder.clone(), 
            &[
                "+----------+--------+-------+-------+-------+",
                "| range1   | range2 | hash1 | hash2 | value |",
                "+----------+--------+-------+-------+-------+",
                "| 20201101 | 1      | 1     | 1     | 11    |",
                "| 20201101 | 2      | 2     | 2     | 2     |",
                "| 20201101 | 3      | 3     | 3     | 33    |",
                "| 20201101 | 4      | 4     | 4     | 44    |",
                "| 20201102 | 4      | 4     | 4     | 4     |",
                "+----------+--------+-------+-------+-------+",
            ]
        );

    }

    #[test]
    fn test_upsert_with_condition() {
        // require metadata checker
    }

    #[test]
    fn test_filter_requested_columns_upsert_1_times_i32() {
        let table_name = "filter_requested_columns_upsert_1_times";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101, 20201101, 20201101], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             vec!["range".to_string(), "hash".to_string()]);
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value", "name", "age"], 
            Some("and(noteq(range, null), eq(range, 20201102))".to_string()),
            builder.clone(), 
            &[
                "+----------+------+-------+------+-----+",
                "| range    | hash | value | name | age |",
                "+----------+------+-------+------+-----+",
                "| 20201102 | 1    | 11    |      |     |",
                "| 20201102 | 3    | 33    |      |     |",
                "| 20201102 | 4    | 44    |      |     |",
                "+----------+------+-------+------+-----+",
            ]
        );

    }

    #[test]
    fn test_filter_requested_columns_upsert_2_times_i32() {
        let table_name = "filter_requested_columns_upsert_2_times";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101, 20201101, 20201101], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             vec!["range".to_string(), "hash".to_string()]);
        
        let builder = execute_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]),  
            table_name, 
            builder);
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value", "name"], vec![&[20201102, 20201102, 20201102], &[1, 2, 3], &[111, 222, 333], &[11, 22, 33]]), 
            table_name, 
            vec!["range", "hash", "value", "name", "age"], 
            Some("and(noteq(range, null), eq(range, 20201102))".to_string()),
            builder.clone(), 
            &[
                "+----------+------+-------+------+-----+",
                "| range    | hash | value | name | age |",
                "+----------+------+-------+------+-----+",
                "| 20201102 | 1    | 111   | 11   |     |",
                "| 20201102 | 2    | 222   | 22   |     |",
                "| 20201102 | 3    | 333   | 33   |     |",
                "| 20201102 | 4    | 44    |      |     |",
                "+----------+------+-------+------+-----+",
            ]
        );
    }

    #[test]
    fn test_filter_requested_columns_upsert_3_times_i32() {
        let table_name = "filter_requested_columns_upsert_3_times";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101, 20201101, 20201101], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             vec!["range".to_string(), "hash".to_string()]);
            
        let builder = execute_upsert(
            create_batch_i32(vec!["range", "hash", "value", "name"], vec![&[20201102, 20201102, 20201102], &[1, 2, 3], &[111, 222, 333], &[11, 22, 33]]),
            table_name, 
            builder);
        
        let builder = execute_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]),  
            table_name, 
            builder);
        
        /***
             !!! Error all below conditions are satisfied
             1. entire 'value' column is null 
             2. filter exists 
             3. filter pushed down into TableProvider with TableProviderFilterPushDown::Inexact
             4. SessionConfig.execution.parquet.pruning = true （equivalent SessionConfig.with_parquet_pruning(true)）
             5. SessionConfig.execution.parquet.enable_page_index = true
             6. 
         */
        check_upsert(
            create_batch_optional_i32(
                vec!["range", "hash", "age", "name", "value"], 
                vec![
                    &[Some(20201102), Some(20201102)], 
                    &[Some(1), Some(3)], 
                    &[Some(111), Some(333)], 
                    &[Some(11), Some(33)], 
                    &[None, Some(3333)]]),
                    // &[None, None]]),
            table_name, 
            vec!["range", "hash", "value", "name", "age"], 
            Some("and(noteq(range, null), eq(range, 20201102))".to_string()),
            // None,
            builder.clone(), 
            &[
                "+----------+------+-------+------+-----+",
                "| range    | hash | value | name | age |",
                "+----------+------+-------+------+-----+",
                "| 20201102 | 1    |       | 11   | 111 |",
                "| 20201102 | 2    | 222   | 22   |     |",
                "| 20201102 | 3    | 3333  | 33   | 333 |",
                // "| 20201102 | 3    |       | 33   | 333 |",
                "| 20201102 | 4    | 44    |      |     |",
                "+----------+------+-------+------+-----+",
            ]
        );

    }

    #[test]
    fn test_select_requested_columns_without_hash_columns_upsert_1_times_i32() {
        let table_name = "select_requested_columns_without_hash_columns_upsert_1_times";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2]]),
            table_name, 
            vec!["range".to_string(), "hash".to_string()]);
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["age"], 
            None,
            builder.clone(), 
            &[
                "+-----+",
                "| age |",
                "+-----+",
                "| 1   |",
                "| 2   |",
                "|     |",
                "|     |",
                "|     |",
                "+-----+",
            ]
        );
    }

    #[test]
    fn test_select_requested_columns_without_hash_columns_upsert_2_times_i32() {
        let table_name = "select_requested_columns_without_hash_columns_upsert_2_times";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2]]),
            table_name, 
            vec!["range".to_string(), "hash".to_string()]);

        let builder = execute_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]),  
            table_name, 
            builder);
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value", "name"], vec![&[20201102, 20201102, 20201102], &[1, 2, 3], &[111, 222, 333], &[11, 22, 33]]), 
            table_name, 
            vec!["age"], 
            None,
            builder.clone(), 
            &[
                "+-----+",
                "| age |",
                "+-----+",
                "| 1   |",
                "| 2   |",
                "|     |",
                "|     |",
                "|     |",
                "|     |",
                "+-----+",
            ]
        );
    }

    
}

mod upsert_with_metadata_tests {
    use std::sync::Arc;
    use std::env;
    use std::path::PathBuf;
    use std::time::SystemTime;
    
    use lakesoul_io::{arrow, datafusion, tokio, serde_json};

    use lakesoul_io::arrow::array::*;
    use lakesoul_io::arrow::datatypes::{DataType, i256, GenericBinaryType, Int32Type};
    use lakesoul_io::datasource::parquet_source::EmptySchemaProvider;
    use lakesoul_io::serde_json::json;

    use arrow::util::pretty::print_batches;
    use arrow::datatypes::{Schema, SchemaRef, Field};
    use arrow::record_batch::RecordBatch;
    use arrow::array::{ArrayRef, Int32Array};

    use datafusion::assert_batches_eq;
    use datafusion::prelude::{DataFrame, SessionContext};
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::error::Result;

    use proto::proto::entity::{TableInfo, DataCommitInfo, FileOp, DataFileOp, CommitOp, Uuid};
    use tokio::runtime::{Builder, Runtime};

    use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfigBuilder, LakeSoulIOConfig};
    use lakesoul_io::lakesoul_reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
    use lakesoul_io::lakesoul_writer::SyncSendableMutableLakeSoulWriter;

    use lakesoul_metadata::{Client, PreparedStatementMap, MetaDataClient};


    fn commit_data(client: &mut MetaDataClient, table_name: &str, config: LakeSoulIOConfig) -> Result<()>{
        let table_name_id = client.get_table_name_id_by_table_name(table_name, "default")?;
        match client.commit_data_commit_info(DataCommitInfo {
            table_id: table_name_id.table_id,
            partition_desc: "-5".to_string(),
            file_ops: config.files_slice()
                .iter()
                .map(|file| DataFileOp {
                    file_op: FileOp::Add as i32,
                    path: file.clone(),
                    ..Default::default()
                })
                .collect(),
            commit_op: CommitOp::AppendCommit as i32,
            timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as i64,
            commit_id: {
                let (high, low) = uuid::Uuid::new_v4().as_u64_pair(); 
                Some(Uuid{high, low})
            },
            ..Default::default()
        }) 
        {
            Ok(()) => Ok(()),
            Err(e) => Err(lakesoul_io::lakesoul_reader::DataFusionError::IoError(e))
        }
    }

    fn create_table(client: &mut MetaDataClient, table_name: &str, config: LakeSoulIOConfig) -> Result<()> {
        match client.create_table(
            TableInfo {
                table_id: format!("table_{}", uuid::Uuid::new_v4().to_string()),
                table_name: table_name.to_string(), 
                table_path: [env::temp_dir().to_str().unwrap(), table_name].iter().collect::<PathBuf>().to_str().unwrap().to_string(),
                table_schema: serde_json::to_string(&config.schema()).unwrap(),
                table_namespace: "default".to_string(),
                properties: "{}".to_string(),
                partitions: ";".to_owned() + config.primary_keys_slice().iter().map(String::as_str).collect::<Vec<_>>().join(",").as_str(),
                domain: "public".to_string(),
            }) 
        {
            Ok(()) => Ok(()),
            Err(e) => Err(lakesoul_io::lakesoul_reader::DataFusionError::IoError(e))
        }
    }

    fn create_io_config_builder(client: &mut MetaDataClient, table_name: &str) -> LakeSoulIOConfigBuilder {
        let table_info = client.get_table_info_by_table_name(table_name, "default").unwrap();
        let data_files = client.get_data_files_by_table_name(table_name, vec![], "default").unwrap();
        let schema_str = client.get_schema_by_table_name(table_name, "default").unwrap();
        let schema = serde_json::from_str::<Schema>(schema_str.as_str()).unwrap();

        LakeSoulIOConfigBuilder::new()
            .with_files(data_files)
            .with_schema(Arc::new(schema))
            .with_primary_keys(
                parse_table_info_partitions(table_info.partitions).1
            )
    }

    fn parse_table_info_partitions(partitions: String) -> (Vec<String>, Vec<String>) {
        let (range_keys, hash_keys) = partitions.split_at(partitions.find(';').unwrap());
        let hash_keys = &hash_keys[1..];
        (
            range_keys.split(',')
                .collect::<Vec<&str>>()
                .iter()
                .filter_map(|str| if str.is_empty() { 
                        None 
                    } else {
                        Some(str.to_string())
                })
                .collect::<Vec<String>>(), 
            hash_keys.split(',')
                .collect::<Vec<&str>>()
                .iter()
                .filter_map(|str| if str.is_empty() { 
                        None 
                    } else {
                        Some(str.to_string())
                })
                .collect::<Vec<String>>()
        )
    }

    fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
        let values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names.into_iter().zip(values).map(|(name, array)| (name, array, true)).collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }


    fn execute_upsert(batch: RecordBatch, table_name: &str, client: &mut MetaDataClient) -> Result<()> {
        let file = [env::temp_dir().to_str().unwrap(), table_name, format!("{}.parquet", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis().to_string()).as_str()].iter().collect::<PathBuf>().to_str().unwrap().to_string();
        let builder = create_io_config_builder(client, table_name).with_file(file.clone()).with_schema(batch.schema());
        let config = builder.clone().build();

        let writer = SyncSendableMutableLakeSoulWriter::try_new(config, Builder::new_current_thread().build().unwrap()).unwrap();
        writer.write_batch(batch)?;
        writer.flush_and_close()?;
        commit_data(client, table_name, builder.clone().build()) 
    }

    


    fn init_table(batch: RecordBatch, table_name: &str, pks:Vec<String>, client: &mut MetaDataClient) -> Result<()> {
        let schema = batch.schema();
        let builder = LakeSoulIOConfigBuilder::new()
                .with_schema(schema.clone())
                .with_primary_keys(pks);
        create_table(client, table_name, builder.build())?;
        execute_upsert(batch, table_name, client)
    }



    fn check_upsert(batch: RecordBatch, table_name: &str, selected_cols: Vec<&str>, filters: Option<String>, client: &mut MetaDataClient, expected: &[&str]) -> Result<()> {
        execute_upsert(batch, table_name, client)?;
        let builder = create_io_config_builder(client, table_name);
        let builder = builder
            .with_schema(SchemaRef::new(Schema::new(
                selected_cols.iter().map(|col| Field::new(*col, arrow::datatypes::DataType::Int32, true)).collect::<Vec<_>>()
            )));
        let builder = if let Some(filters) = filters {
            builder.with_filter_str(filters)
        } else {
            builder
        };
        let mut reader = SyncSendableMutableLakeSoulReader::new(LakeSoulReader::new(builder.build()).unwrap(), Builder::new_current_thread().build().unwrap());
        reader.start_blocked()?;
        let result = reader.next_rb_blocked();
        match result {
            Some(result) => {
                assert_batches_eq!(expected, &[result?]);
                Ok(())
            },
            None => Ok(())
        }
    }

    #[test]
    fn test_merge_same_column_i32() -> Result<()>{
        let table_name = "merge-same_column";
        let mut client = MetaDataClient::from_env();
        // let mut client = MetaDataClient::from_config("host=127.0.0.1 port=5433 dbname=test_lakesoul_meta user=yugabyte password=yugabyte".to_string());
        client.meta_cleanup()?;
        init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             vec!["range".to_string(), "hash".to_string()],
             &mut client,
        )?;
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value"], 
            None,
            &mut client,
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 11    |",
                "| 20201101 | 2    | 2     |",
                "| 20201101 | 3    | 33    |",
                "| 20201101 | 4    | 44    |",
                "| 20201102 | 4    | 4     |",
                "+----------+------+-------+",
            ]
        )
    }

    // #[test]
    fn test_datatypes() -> Result<()>{
        let table_name = "test_datatypes";
        let mut client = MetaDataClient::from_env();
        // let mut client = MetaDataClient::from_config("host=127.0.0.1 port=5433 dbname=test_lakesoul_meta user=yugabyte password=yugabyte".to_string());
        client.meta_cleanup()?;

        let iter = vec![
            ("Boolean", Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef, true),
            ("Binary", Arc::new(BinaryArray::from_vec(vec![&[1u8], &[2u8, 3u8]])) as ArrayRef, true),

            ("Date32", Arc::new(Date32Array::from(vec![1, -2])) as ArrayRef, true),
            ("Date64", Arc::new(Date64Array::from(vec![1, -2])) as ArrayRef, true),
            ("Decimal128", Arc::new(Decimal128Array::from(vec![1, -2])) as ArrayRef, true),
            ("Decimal256", Arc::new(Decimal256Array::from(vec![Some(i256::default()), None])) as ArrayRef, true),

            // ParquetError(ArrowError("Converting Duration to parquet not supported"))
            // ("DurationMicrosecond", Arc::new(DurationMicrosecondArray::from(vec![1])) as ArrayRef, true),
            // ("DurationMillisecond", Arc::new(DurationMillisecondArray::from(vec![1])) as ArrayRef, true),

            // ("Float16", Arc::new(Float16Array::from(vec![1.0])) as ArrayRef, true),

            ("FixedSizeBinary", Arc::new(FixedSizeBinaryArray::from(vec![&[1u8][..], &[2u8][..]])) as ArrayRef, true),
            ("FixedSizeList", Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    None,
                    // Some(vec![Some(3), None, Some(5)]),
                    // Some(vec![Some(6), Some(7)]),
                ], 3)) as ArrayRef, true),

            ("Float32", Arc::new(Float32Array::from(vec![1.0, -1.0])) as ArrayRef, true),
            ("Float64", Arc::new(Float64Array::from(vec![1.0, -1.0])) as ArrayRef, true),


            ("Int8", Arc::new(Int8Array::from(vec![1i8, -2i8])) as ArrayRef, true),
            ("Int8Dictionary", Arc::new(Int8DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            ("Int16", Arc::new(Int16Array::from(vec![1i16, -2i16])) as ArrayRef, true),
            ("Int16Dictionary", Arc::new(Int16DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            ("Int32", Arc::new(Int32Array::from(vec![1i32, -2i32])) as ArrayRef, true),
            ("Int32Dictionary", Arc::new(Int32DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            ("Int64", Arc::new(Int64Array::from(vec![1i64, -2i64])) as ArrayRef, true),
            ("Int64Dictionary", Arc::new(Int64DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),

            ("IntervalDayTime", Arc::new(IntervalDayTimeArray::from(vec![1, 2])) as ArrayRef, true),
            // ParquetError(NYI("Attempting to write an Arrow interval type MonthDayNano to parquet that is not yet implemented"))
            //("IntervalMonthDayNano", Arc::new(IntervalMonthDayNanoArray::from(vec![1])) as ArrayRef, true),
            ("IntervalYearMonth", Arc::new(IntervalYearMonthArray::from(vec![1, 2])) as ArrayRef, true),

            ("Map", Arc::new({
                let string_builder = StringBuilder::new();
                let int_builder = Int32Builder::with_capacity(4);

                // Construct `[{"joe": 1}, {"blogs": 2, "foo": 4}]`
                let mut builder = MapBuilder::new(None, string_builder, int_builder);

                builder.keys().append_value("joe");
                builder.values().append_value(1);
                builder.append(true).unwrap();

                builder.keys().append_value("blogs");
                builder.values().append_value(2);
                builder.keys().append_value("foo");
                builder.values().append_value(4);
                builder.append(true).unwrap();

                builder.finish()
                }) as ArrayRef, true),

            ("Null", Arc::new(NullArray::new(2)) as ArrayRef, true),

            ("LargeBinary", Arc::new(LargeBinaryArray::from_vec(vec![&[1u8], &[2u8, 3u8]])) as ArrayRef, true),
            ("LargeString", Arc::new(LargeStringArray::from(vec!["1", ""])) as ArrayRef, true),

            ("List", Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    None,
                    // Some(vec![Some(3), None, Some(5)]),
                    // Some(vec![Some(6), Some(7)]),
                ])) as ArrayRef, true),
            
            // ParquetError(ArrowError("Converting RunEndEncodedType to parquet not supported"))
            // ("Run", Arc::new(RunArray::<Int32Type>::from_iter([Some("a"), None])) as ArrayRef, true),

            ("String", Arc::new(StringArray::from(vec!["1", ""])) as ArrayRef, true),
            ("Struct", Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("b", DataType::Boolean, false)),
                        Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("c", DataType::Int32, false)),
                        Arc::new(Int32Array::from(vec![42, 31])) as ArrayRef,
                    ),
                ])) as ArrayRef, true),
            
            ("Time32Millisecond", Arc::new(Time32MillisecondArray::from(vec![1i32, -2i32])) as ArrayRef, true),
            ("Time32Second", Arc::new(Time32SecondArray::from(vec![1i32, -2i32])) as ArrayRef, true),
            ("Time64Microsecond", Arc::new(Time64MicrosecondArray::from(vec![1i64, -2i64])) as ArrayRef, true),
            ("Time64Nanosecond", Arc::new(Time64NanosecondArray::from(vec![1i64, -2i64])) as ArrayRef, true),
            ("TimestampMicrosecond", Arc::new(TimestampMicrosecondArray::from(vec![1i64, -2i64])) as ArrayRef, true),
            ("TimestampMillisecond", Arc::new(TimestampMillisecondArray::from(vec![1i64, -2i64])) as ArrayRef, true),
            ("TimestampNanosecond", Arc::new(TimestampNanosecondArray::from(vec![1i64, -2i64])) as ArrayRef, true),
            ("TimestampSecond", Arc::new(TimestampSecondArray::from(vec![1i64, -2i64])) as ArrayRef, true),

            ("UInt8", Arc::new(UInt8Array::from(vec![1u8, 2u8])) as ArrayRef, true),
            ("UInt8Dictionary", Arc::new(UInt8DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            ("UInt16", Arc::new(UInt16Array::from(vec![1u16, 2u16])) as ArrayRef, true),
            ("UInt16Dictionary", Arc::new(UInt16DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            ("UInt32", Arc::new(UInt32Array::from(vec![1u32, 2u32])) as ArrayRef, true),
            ("UInt32Dictionary", Arc::new(UInt32DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            ("UInt64", Arc::new(UInt64Array::from(vec![1u64, 2u64])) as ArrayRef, true),
            ("UInt64Dictionary", Arc::new(UInt64DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
        ];
        let batch = RecordBatch::try_from_iter_with_nullable(iter).unwrap();
        init_table(
            batch,
            // create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
            //  vec!["range".to_string(), "hash".to_string()],
            vec![],
             &mut client,
        )
    }
}
