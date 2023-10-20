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
    use lakesoul_io::tokio::runtime::{Builder};
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