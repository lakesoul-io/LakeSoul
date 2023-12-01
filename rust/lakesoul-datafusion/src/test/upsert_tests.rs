// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
mod upsert_with_io_config_tests {
    use std::sync::Arc;
    use std::env;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use chrono::naive::NaiveDate;

    use lakesoul_io::arrow::record_batch::RecordBatch;
    use lakesoul_io::arrow::util::pretty::print_batches;
    use lakesoul_io::datafusion::assert_batches_eq;
    use lakesoul_io::datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use lakesoul_io::datafusion::prelude::SessionContext;
    use lakesoul_io::lakesoul_reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
    use lakesoul_io::tokio::runtime::Builder;
    use lakesoul_io::arrow;
    use lakesoul_io::arrow::array::{ArrayRef, Int32Array,TimestampMicrosecondArray,StringArray};
    use lakesoul_io::arrow::datatypes::{Schema, SchemaRef, Field,TimestampMicrosecondType, TimeUnit};
    use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
    use lakesoul_io::lakesoul_writer::SyncSendableMutableLakeSoulWriter;
    use lakesoul_io::arrow::array::Int64Array;

    
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

    fn create_batch_string(names: Vec<&str>, values: Vec<&[&str]>) -> RecordBatch {
        let mut values=values
            .into_iter()
            .map(|vec| Arc::new(StringArray::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names.into_iter().zip(values).map(|(name, array)| (name, array, true)).collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    fn create_batch_i32_and_timestamp(names: Vec<&str>, values: Vec<&[i32]>, timestamp:Vec<i64>) -> RecordBatch {
        let mut values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let timestamp = Arc::new(TimestampMicrosecondArray::from(timestamp)) as ArrayRef;
        values.push(timestamp);
        let iter = names.into_iter().zip(values).map(|(name, array)| (name, array, true)).collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    fn check_upsert_i32_and_timestamp(batch: RecordBatch, table_name: &str, selected_cols: Vec<&str>, filters: Option<String>, builder: LakeSoulIOConfigBuilder, expected: &[&str]) -> LakeSoulIOConfigBuilder {
        let builder = execute_upsert(batch, table_name, builder.clone());
        let builder = builder
            .with_schema(SchemaRef::new(Schema::new(
                selected_cols.iter().map(|col| 
                if *col=="timestamp"{
                    Field::new(*col, arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None), true)
                }else{
                    Field::new(*col, arrow::datatypes::DataType::Int32, true)
                }
            ).collect::<Vec<_>>()
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

    fn check_upsert_string(batch: RecordBatch, table_name: &str, selected_cols: Vec<&str>, filters: Option<String>, builder: LakeSoulIOConfigBuilder, expected: &[&str]) -> LakeSoulIOConfigBuilder {
        let builder = execute_upsert(batch, table_name, builder.clone());
        let builder = builder
            .with_schema(SchemaRef::new(Schema::new(
                selected_cols.iter().map(|col| Field::new(*col, arrow::datatypes::DataType::Utf8, true)).collect::<Vec<_>>()
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
        let table_name = "test_basic_upsert_same_columns";
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
            None,
            builder.clone(), 
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 1     |",
                "| 20201101 | 2    | 2     |",
                "| 20201101 | 3    | 3     |",
                "| 20201102 | 4    | 4     |",
                "+----------+------+-------+",
            ]
        );
        
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
    fn test_basic_upsert_different_columns() {
        // require metadata checker
        let table_name = "basic_upsert_different_columns";
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
    fn test_source_dataFrame_without_partition_columns(){
        // require metadata checker

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

    #[test]
    fn test_derange_hash_key_and_data_schema_order_int_type_upsert_1_times_i32(){
        let table_name = "derange_hash_key_and_data_schema_order_int_type_upsert_1_times_i32";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2],&[1, 2]]),
            table_name, 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()]);
        check_upsert(
            create_batch_i32(vec!["range", "hash1", "hash2", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[12, 32, 42], &[1, 3, 4]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(noteq(range, null), eq(range, 20201102))".to_string()),
            builder.clone(), 
            &[
                "+----------+-------+-------+-------+------+-----+",
                "| range    | hash1 | hash2 | value | name | age |",
                "+----------+-------+-------+-------+------+-----+",
                "| 20201102 | 1     | 12    | 1     |      |     |",
                "| 20201102 | 3     | 32    | 3     |      |     |",
                "| 20201102 | 4     | 42    | 4     |      |     |",
                "+----------+-------+-------+-------+------+-----+",
            ]
        );
    }

    #[test]
    fn test_derange_hash_key_and_data_schema_order_int_type_upsert_2_times_i32(){
        let table_name = "derange_hash_key_and_data_schema_order_int_type_upsert_2_times_i32";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2],&[1, 2]]),
            table_name, 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()]);
        let builder = execute_upsert(
            create_batch_i32(vec!["range", "hash1", "hash2", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[12, 32, 42], &[1, 3, 4]]),  
            table_name, 
            builder);
        check_upsert(
            create_batch_i32(vec!["range", "hash2", "name", "hash1"], vec![&[20201102, 20201102, 20201102], &[12, 22, 32], &[11, 22, 33], &[1, 2, 3]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(noteq(range, null), eq(range, 20201102))".to_string()),
            builder.clone(), 
            &[
                "+----------+-------+-------+-------+------+-----+",
                "| range    | hash1 | hash2 | value | name | age |",
                "+----------+-------+-------+-------+------+-----+",
                "| 20201102 | 1     | 12    | 1     | 11   |     |",
                "| 20201102 | 2     | 22    |       | 22   |     |",
                "| 20201102 | 3     | 32    | 3     | 33   |     |",
                "| 20201102 | 4     | 42    | 4     |      |     |",
                "+----------+-------+-------+-------+------+-----+",
            ]
        );
    }

    #[test]
    fn test_derange_hash_key_and_data_schema_order_int_type_upsert_3_times_i32(){
        let table_name = "derange_hash_key_and_data_schema_order_int_type_upsert_3_times_i32";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2],&[1, 2]]),
            table_name, 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()]);
        let builder = execute_upsert(
            create_batch_i32(vec!["range", "hash1", "hash2", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[12, 32, 42], &[1, 3, 4]]),  
            table_name, 
            builder);
        let builder = execute_upsert(
            create_batch_i32(vec!["range", "hash2", "name", "hash1"], vec![&[20201102, 20201102, 20201102], &[12, 22, 32], &[11, 22, 33], &[1, 2, 3]]),  
            table_name, 
            builder);
        check_upsert(
            create_batch_i32(vec!["range", "age", "hash2", "name", "hash1"], vec![&[20201102, 20201102, 20201102], &[4567, 2345, 3456], &[42, 22, 32], &[456, 234, 345], &[4, 2, 3]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(and(noteq(range, null), eq(range, 20201102)), noteq(value, null))".to_string()),
            builder.clone(), 
            &[
                "+----------+-------+-------+-------+------+------+",
                "| range    | hash1 | hash2 | value | name | age  |",
                "+----------+-------+-------+-------+------+------+",
                "| 20201102 | 1     | 12    | 1     | 11   |      |",
                "| 20201102 | 3     | 32    | 3     | 345  | 3456 |",
                "| 20201102 | 4     | 42    | 4     | 456  | 4567 |",
                "+----------+-------+-------+-------+------+------+",
            ]
        );
    }

    #[test]
    fn test_derange_hash_key_and_data_schema_order_string_type_upsert_1_times_i32(){
        let table_name = "derange_hash_key_and_data_schema_order_string_type_upsert_1_times_i32";
        let builder = init_table(
            create_batch_string(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&["20201101", "20201101"], &["1", "2"], &["1", "2"], &["1", "2"], &["1", "2"],&["1", "2"]]),
            table_name, 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()]);
        check_upsert_string(
            create_batch_string(vec!["range", "hash1", "hash2", "value"], vec![&["20201102", "20201102", "20201102"], &["1", "3", "4"], &["12", "32", "42"], &["1", "3", "4"]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(noteq(range, null), eq(range, ________20201102__))".to_string()),
            builder.clone(), 
            &[
                "+----------+-------+-------+-------+------+-----+",
                "| range    | hash1 | hash2 | value | name | age |",
                "+----------+-------+-------+-------+------+-----+",
                "| 20201102 | 1     | 12    | 1     |      |     |",
                "| 20201102 | 3     | 32    | 3     |      |     |",
                "| 20201102 | 4     | 42    | 4     |      |     |",
                "+----------+-------+-------+-------+------+-----+",
            ]
        );
    }

    #[test]
    fn test_derange_hash_key_and_data_schema_order_string_type_upsert_2_times_i32(){
        let table_name = "derange_hash_key_and_data_schema_order_string_type_upsert_2_times_i32";
        let builder = init_table(
            create_batch_string(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&["20201101", "20201101"], &["1", "2"], &["1", "2"], &["1", "2"], &["1", "2"],&["1", "2"]]),
            table_name, 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()]);
        let builder = execute_upsert(
            create_batch_string(vec!["range", "hash1", "hash2", "value"], vec![&["20201102", "20201102", "20201102"], &["1", "3", "4"], &["12", "32", "42"], &["1", "3", "4"]]),  
            table_name, 
            builder);
        check_upsert_string(
            create_batch_string(vec!["range", "hash2", "name", "hash1"], vec![&["20201102", "20201102", "20201102"], &["12", "22", "32"], &["11", "22", "33"], &["1", "2", "3"]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(noteq(range, null), eq(range, ________20201102__))".to_string()),
            builder.clone(), 
            &[
                "+----------+-------+-------+-------+------+-----+",
                "| range    | hash1 | hash2 | value | name | age |",
                "+----------+-------+-------+-------+------+-----+",
                "| 20201102 | 1     | 12    | 1     | 11   |     |",
                "| 20201102 | 2     | 22    |       | 22   |     |",
                "| 20201102 | 3     | 32    | 3     | 33   |     |",
                "| 20201102 | 4     | 42    | 4     |      |     |",
                "+----------+-------+-------+-------+------+-----+",
            ]
        );
    }

    #[test]
    fn test_derange_hash_key_and_data_schema_order_string_type_upsert_3_times_i32(){
        let table_name = "derange_hash_key_and_data_schema_order_string_type_upsert_3_times_i32";
        let builder = init_table(
            create_batch_string(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&["20201101", "20201101"], &["1", "2"], &["1", "2"], &["1", "2"], &["1", "2"],&["1", "2"]]),
            table_name, 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()]);
        let builder = execute_upsert(
            create_batch_string(vec!["range", "hash1", "hash2", "value"], vec![&["20201102", "20201102", "20201102"], &["1", "3", "4"], &["12", "32", "42"], &["1", "3", "4"]]),  
            table_name, 
            builder);
        let builder = execute_upsert(
            create_batch_string(vec!["range", "hash2", "name", "hash1"], vec![&["20201102", "20201102", "20201102"], &["12", "22", "32"], &["11", "22", "33"], &["1", "2", "3"]]),  
            table_name, 
            builder);
        check_upsert_string(
            create_batch_string(vec!["range", "age", "hash2", "name", "hash1"], vec![&["20201102", "20201102", "20201102"], &["4567", "2345", "3456"], &["42", "22", "32"], &["456", "234", "345"], &["4", "2", "3"]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(and(noteq(range, null), eq(range, ________20201102__)), noteq(value, null))".to_string()),
            builder.clone(), 
            &[
                "+----------+-------+-------+-------+------+------+",
                "| range    | hash1 | hash2 | value | name | age  |",
                "+----------+-------+-------+-------+------+------+",
                "| 20201102 | 1     | 12    | 1     | 11   |      |",
                "| 20201102 | 3     | 32    | 3     | 345  | 3456 |",
                "| 20201102 | 4     | 42    | 4     | 456  | 4567 |",
                "+----------+-------+-------+-------+------+------+",
            ]
        );
    }


    #[test]
    fn test_create_table_with_hash_key_disordered(){
        // mark
    }


    #[test]
    fn test_merge_same_column_with_timestamp_type_i32_time(){
        let dt1=NaiveDate::from_ymd_opt(1000, 6, 14).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt2=NaiveDate::from_ymd_opt(1582, 6, 15).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt3=NaiveDate::from_ymd_opt(1900, 6, 16).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt4=NaiveDate::from_ymd_opt(2018, 6, 17).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        
        let val1=dt1.timestamp_micros();
        let val2=dt2.timestamp_micros();
        let val3=dt3.timestamp_micros();
        let val4=dt4.timestamp_micros();

        let table_name = "test_merge_same_column_with_timestamp_type_i64_time";
        let builder = init_table(
            create_batch_i32_and_timestamp(vec!["range", "hash", "value", "timestamp"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]], vec![val1, val2, val3, val4]),
            table_name, 
            vec!["range".to_string(), "hash".to_string()]);
        check_upsert_i32_and_timestamp(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value", "timestamp"],
            None,
            builder.clone(), 
            &[
                "+----------+------+-------+----------------------------+",
                "| range    | hash | value | timestamp                  |",
                "+----------+------+-------+----------------------------+",
                "| 20201101 | 1    | 11    | 1000-06-14T08:28:53.123456 |",
                "| 20201101 | 2    | 2     | 1582-06-15T08:28:53.123456 |",
                "| 20201101 | 3    | 33    | 1900-06-16T08:28:53.123456 |",
                "| 20201101 | 4    | 44    |                            |",
                "| 20201102 | 4    | 4     | 2018-06-17T08:28:53.123456 |",
                "+----------+------+-------+----------------------------+",
            ]
        );
    }

    #[test]
    fn test_merge_different_columns_with_timestamp_type_i32_time(){
        let dt1=NaiveDate::from_ymd_opt(1000, 6, 14).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let _dt2=NaiveDate::from_ymd_opt(1582, 6, 15).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt3=NaiveDate::from_ymd_opt(1900, 6, 16).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt4=NaiveDate::from_ymd_opt(2018, 6, 17).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        
        let val1=dt1.timestamp_micros();
        let _val2=_dt2.timestamp_micros();
        let val3=dt3.timestamp_micros();
        let val4=dt4.timestamp_micros();

        let table_name = "merge_different_columns_with_timestamp_type_i32_time";
        let builder = init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            vec!["range".to_string(), "hash".to_string()]);
        check_upsert_i32_and_timestamp(
            create_batch_i32_and_timestamp(vec!["range", "hash", "name", "timestamp"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]],vec![val1, val3, val4]), 
            table_name, 
            vec!["range", "hash", "value", "name", "timestamp"],
            None,
            builder.clone(), 
            &[
                "+----------+------+-------+------+----------------------------+",
                "| range    | hash | value | name | timestamp                  |",
                "+----------+------+-------+------+----------------------------+",
                "| 20201101 | 1    | 1     | 11   | 1000-06-14T08:28:53.123456 |",
                "| 20201101 | 2    | 2     |      |                            |",
                "| 20201101 | 3    | 3     | 33   | 1900-06-16T08:28:53.123456 |",
                "| 20201101 | 4    |       | 44   | 2018-06-17T08:28:53.123456 |",
                "| 20201102 | 4    | 4     |      |                            |",
                "+----------+------+-------+------+----------------------------+",
            ]
        );
    }

    
}


mod upsert_with_metadata_tests {
    use std::ops::Deref;
    use std::sync::Arc;
    use std::env;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use std::thread;
    use std::time::Duration;
    use chrono::naive::NaiveDate;
    
    use lakesoul_io::filter::parser::Parser;
    use lakesoul_io::{arrow, datafusion, tokio, serde_json};

    use lakesoul_io::arrow::array::*;
    use lakesoul_io::arrow::datatypes::{DataType, i256, GenericBinaryType, Int32Type};
    use lakesoul_io::datasource::parquet_source::EmptySchemaProvider;
    use lakesoul_io::serde_json::json;

    use arrow::util::pretty::print_batches;
    use arrow::datatypes::{Schema, SchemaRef, Field, TimestampMicrosecondType, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use arrow::array::{ArrayRef, Int32Array};

    use datafusion::assert_batches_eq;
    use datafusion::prelude::{DataFrame, SessionContext};
    use datafusion::logical_expr::LogicalPlanBuilder;
    use crate::catalog::lakesoul_sink::LakeSoulSinkProvider;
    use crate::catalog::lakesoul_source::LakeSoulSourceProvider;
    use crate::error::Result;

    use proto::proto::entity::{TableInfo, DataCommitInfo, FileOp, DataFileOp, CommitOp, Uuid};
    use tokio::runtime::{Builder, Runtime};

    use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfigBuilder, LakeSoulIOConfig, create_session_context};
    use lakesoul_io::lakesoul_reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
    use lakesoul_io::lakesoul_writer::SyncSendableMutableLakeSoulWriter;

    use lakesoul_metadata::{Client, PreparedStatementMap, MetaDataClient, MetaDataClientRef};

    use crate::catalog::{create_io_config_builder, create_table, commit_data, parse_table_info_partitions};


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

    fn create_batch_i32_and_timestamp(names: Vec<&str>, values: Vec<&[i32]>, timestamp:Vec<i64>) -> RecordBatch {
        let mut values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let timestamp = Arc::new(TimestampMicrosecondArray::from(timestamp)) as ArrayRef;
        values.push(timestamp);
        let iter = names.into_iter().zip(values).map(|(name, array)| (name, array, true)).collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    fn create_batch_string(names: Vec<&str>, values: Vec<&[&str]>) -> RecordBatch {
        let mut values=values
            .into_iter()
            .map(|vec| Arc::new(StringArray::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names.into_iter().zip(values).map(|(name, array)| (name, array, true)).collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    async fn execute_upsert(record_batch: RecordBatch, table_name: &str, client: MetaDataClientRef) -> Result<()> {
        let builder = create_io_config_builder(client, None).await?;
        let sess_ctx = create_session_context(&mut builder.clone().build())?;
        
        let provider = LakeSoulSinkProvider::new(table_name).await?;
        sess_ctx.register_table(table_name, Arc::new(provider)).unwrap();

        let num_rows = record_batch.num_rows();
        let schema = record_batch.schema();

        let logical_plan = LogicalPlanBuilder::insert_into(
            sess_ctx.read_batch(record_batch)?.into_unoptimized_plan(), 
            table_name.to_string(), 
            &schema.deref())?
            .build()?;
        let dataframe = DataFrame::new(sess_ctx.state(), logical_plan);
        
        let results = dataframe
            // .explain(true, false)?
            .collect()
            .await?;

        // print_batches(&results);

        assert_batches_eq!(&[
            "+-----------+",
            "| row_count |",
            "+-----------+",
            format!("| {:<10}|", num_rows).as_str(),
            "+-----------+",
            ], &results);
        Ok(())
    }

    async fn check_upsert_print(table_name: &str, selected_cols: Vec<&str>, filters: Option<String>, client: MetaDataClientRef, expected: &[&str]) -> Result<()> {
        let builder = create_io_config_builder(client, None).await?;
        let sess_ctx = create_session_context(&mut builder.clone().build())?;
        
        let provider = LakeSoulSourceProvider::new(table_name).await?;

        let dataframe = sess_ctx.read_table(Arc::new(provider))?;
        let schema = SchemaRef::new(dataframe.schema().into());

        let dataframe = if let Some(f) = filters {
            dataframe.filter(Parser::parse(f.clone(), schema))?
        } else {
            dataframe
        };

        let dataframe = if selected_cols.is_empty() {
            dataframe
        } else {
            dataframe.select_columns(&selected_cols)?
        };

        let result = dataframe    
            // .explain(true, false)?        
            .collect()
            .await?;
        
        // print_batches(&result);
        assert_batches_eq!(expected, &result);
        Ok(())
    }

    async fn check_upsert_debug(batch: RecordBatch, table_name: &str, selected_cols: Vec<&str>, filters: Option<String>, client: MetaDataClientRef, expected: &[&str]) -> Result<()> {
        execute_upsert(batch, table_name, client.clone()).await?;
        
        let builder = create_io_config_builder(client.clone(), None).await?;
        let sess_ctx = create_session_context(&mut builder.clone().build())?;
        
        let provider = LakeSoulSourceProvider::new(table_name).await?;

        let dataframe = sess_ctx.read_table(Arc::new(provider))?;
        let schema = SchemaRef::new(dataframe.schema().into());

        let dataframe = if let Some(f) = filters {
            dataframe.filter(Parser::parse(f.clone(), schema))?
        } else {
            dataframe
        };

        let dataframe = if selected_cols.is_empty() {
            dataframe
        } else {
            dataframe.select_columns(&selected_cols)?
        };

        let result = dataframe    
            // .explain(true, false)?        
            .collect()
            .await?;
        
        // print_batches(&result);
        assert_batches_eq!(expected, &result);
        Ok(())
    }

    async fn init_table(batch: RecordBatch, table_name: &str, schema: SchemaRef,  pks:Vec<String>, client: MetaDataClientRef) -> Result<()> {
        let builder = LakeSoulIOConfigBuilder::new()
                .with_schema(schema)
                .with_primary_keys(pks);
        create_table(client.clone(), table_name, builder.build()).await?;
        execute_upsert(batch, table_name, client).await
    }

    async fn init_table_without_schema(batch: RecordBatch, table_name: &str, pks:Vec<String>, client: MetaDataClientRef) -> Result<()> {
        let schema = batch.schema();
        let builder = LakeSoulIOConfigBuilder::new()
                .with_schema(schema)
                .with_primary_keys(pks);
        create_table(client.clone(), table_name, builder.build()).await?;
        execute_upsert(batch, table_name, client).await
    }

    async fn check_upsert(batch: RecordBatch, table_name: &str, selected_cols: Vec<&str>, filters: Option<String>, client: MetaDataClientRef, expected: &[&str]) -> Result<()> {
        execute_upsert(batch, table_name, client.clone()).await?;

        let builder = create_io_config_builder(client, None).await?;
        let sess_ctx = create_session_context(&mut builder.clone().build())?;
        
        let provider = LakeSoulSourceProvider::new(table_name).await?;

        let dataframe = sess_ctx.read_table(Arc::new(provider))?;
        let schema = SchemaRef::new(dataframe.schema().into());

        let dataframe = if let Some(f) = filters {
            dataframe.filter(Parser::parse(f.clone(), schema))?
        } else {
            dataframe
        };

        let dataframe = if selected_cols.is_empty() {
            dataframe
        } else {
            dataframe.select_columns(&selected_cols)?
        };

        let result = dataframe    
            // .explain(true, false)?        
            .collect()
            .await?;
        
        // print_batches(&result);
        assert_batches_eq!(expected, &result);
        Ok(())
    }
    
    async fn test_merge_same_column_i32() -> Result<()> {
        let table_name = "test_merge_same_column_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);
        init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             SchemaRef::new(Schema::new(["range", "hash", "value"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())),
             vec!["range".to_string(), "hash".to_string()],
             client.clone(),
        ).await?;
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value"], 
            None,
            client.clone(),
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
        ).await
    }
    
    async fn test_merge_different_column_i32() -> Result<()> {
        let table_name = "test_merge_different_column_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "name"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())),
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
        ).await?;
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "name"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value", "name"], 
            None, 
            client.clone(),
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
        ).await
    }

    async fn test_merge_different_columns_and_filter_by_non_selected_columns_i32() -> Result<()> {
        let table_name = "test_merge_different_columns_and_filter_by_non_selected_columns_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "name"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())),
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
        ).await?;

        check_upsert(
            create_batch_i32(vec!["range", "hash", "name"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value"], 
            Some("and(noteq(name, null), gt(name, 0))".to_string()),
            client.clone(),
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 1     |",
                "| 20201101 | 3    | 3     |",
                "| 20201101 | 4    |       |",
                "+----------+------+-------+",
            ]
        ).await

    }

    async fn test_merge_different_columns_and_filter_partial_rows_i32() -> Result<()>{
        let table_name = "merge-different_columns_and_filter_partial_rows_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value", "name"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4], &[11, 22, 33, 44]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "name"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())),
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
        ).await?;
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 3, 4, 4], &[2, 4, 5, 5]]), 
            table_name, 
            vec!["range", "hash", "value", "name"], 
            Some("and(and(noteq(value, null), lt(value, 5)),and(noteq(name, null), gt(name, 0)))".to_string()),
            client.clone(), 
            &[
                "+----------+------+-------+------+",
                "| range    | hash | value | name |",
                "+----------+------+-------+------+",
                "| 20201101 | 1    | 2     | 11   |",
                "| 20201101 | 2    | 2     | 22   |",
                "| 20201101 | 3    | 4     | 33   |",
                "+----------+------+-------+------+",
            ]
        ).await
    }

    async fn test_merge_one_file_with_empty_batch_i32() -> Result<()>{
        let table_name = "merge_one_file_with_empty_batch";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())),
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
            ).await?;
        
        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                vec!["range", "hash", "value"].iter().map(|col| Field::new(*col, arrow::datatypes::DataType::Int32, true)).collect::<Vec<_>>()
            ))), 
            table_name, 
            vec!["range", "hash", "value"], 
            Some("and(noteq(value, null), lt(value, 3))".to_string()),
            client.clone(), 
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 1     |",
                "| 20201101 | 2    | 2     |",
                "+----------+------+-------+",
            ]
        ).await
    }

    async fn test_merge_multi_files_with_empty_batch_i32() -> Result<()>{
        let table_name = "merge_multi_files_with_empty_batch";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102, 20201102], &[1, 2, 3, 4, 1], &[1, 2, 3, 4, 1]]),
            table_name,
            SchemaRef::new(Schema::new(["range", "hash", "value"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
            ).await?;

        execute_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102], &[4], &[5]]),
            table_name, 
            client.clone(),
        ).await?;
        
        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                vec!["range", "hash", "value"].iter().map(|col| Field::new(*col, arrow::datatypes::DataType::Int32, true)).collect::<Vec<_>>()
            ))), 
            table_name, 
            vec!["range", "hash", "value"], 
            Some("and(noteq(value, null), lt(value, 3))".to_string()),
            client.clone(), 
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 1     |",
                "| 20201101 | 2    | 2     |",
                "| 20201102 | 1    | 1     |",
                "+----------+------+-------+",
            ]
        ).await
    }

    
    async fn test_basic_upsert_same_columns() -> Result<()>{
        // require metadata checker
        Ok(())
    }

    async fn test_basic_upsert_different_columns() -> Result<()>{
        // require metadata checker
        Ok(())
    }

    async fn test_should_failed_to_upsert_external_columns_when_schema_auto_migrate_is_false() -> Result<()>{
        // require metadata checker
        Ok(())
    }

    async fn test_upsert_in_new_table_should_failed() -> Result<()>{
        // require metadata checker
        Ok(())
    }

    async fn test_upsert_cant_use_delta_file() -> Result<()>{
        // require metadata checker
        Ok(())
    }
    
    async fn test_upsert_without_range_parqitions_i32() -> Result<()>{
        let table_name = "upsert_without_range_parqitions";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["hash".to_string()],
            client.clone(),
            ).await?;
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value"], 
            None,
            client.clone(), 
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
        ).await
    }

    async fn test_upsert_without_hash_partitions_should_fail() -> Result<()>{
        // require metadata checker
        Ok(())
    }

    async fn test_upsert_with_multiple_range_and_hash_parqitions_i32() -> Result<()>{
        let table_name = "upsert_with_multiple_range_and_hash_parqitions";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range1", "range2", "hash1", "hash2", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            SchemaRef::new(Schema::new(["range1", "range2", "hash1", "hash2", "value"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range1".to_string(), "range2".to_string(), "hash1".to_string(), "hash2".to_string()],
            client.clone(),
        ).await?;
        
        check_upsert(
            create_batch_i32(vec!["range1", "range2", "hash1", "hash2", "value"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[1, 3, 4],&[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range1", "range2", "hash1", "hash2", "value"], 
            None,
            client.clone(), 
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
        ).await
    }

    async fn test_source_dataframe_without_partition_columns() -> Result<()>{
        // require metadata checker
        Ok(())
    }
    
    async fn test_upsert_with_condition() -> Result<()>{
        // require metadata checker
        Ok(())
    }

    async fn test_filter_requested_columns_upsert_1_times_i32() -> Result<()>{
        let table_name = "filter_requested_columns_upsert_1_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101, 20201101, 20201101], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
        ).await?;
            
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value", "name", "age"], 
            Some("and(noteq(range, null), eq(range, 20201102))".to_string()),
            client.clone(), 
            &[
                "+----------+------+-------+------+-----+",
                "| range    | hash | value | name | age |",
                "+----------+------+-------+------+-----+",
                "| 20201102 | 1    | 11    |      |     |",
                "| 20201102 | 3    | 33    |      |     |",
                "| 20201102 | 4    | 44    |      |     |",
                "+----------+------+-------+------+-----+",
            ]
        ).await    
    }

    async fn test_filter_requested_columns_upsert_2_times_i32() -> Result<()>{
        let table_name = "filter_requested_columns_upsert_2_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101, 20201101, 20201101], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
            ).await?;
        
        execute_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]),  
            table_name, 
            client.clone(),
        ).await?;
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value", "name"], vec![&[20201102, 20201102, 20201102], &[1, 2, 3], &[111, 222, 333], &[11, 22, 33]]), 
            table_name, 
            vec!["range", "hash", "value", "name", "age"], 
            Some("and(noteq(range, null), eq(range, 20201102))".to_string()),
            client.clone(),  
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
        ).await
    }
    
    async fn test_filter_requested_columns_upsert_3_times_i32() -> Result<()>{
        let table_name = "filter_requested_columns_upsert_3_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101, 20201101, 20201101], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
        ).await?;
            
        execute_upsert(
            create_batch_i32(vec!["range", "hash", "value", "name"], vec![&[20201102, 20201102, 20201102], &[1, 2, 3], &[111, 222, 333], &[11, 22, 33]]),
            table_name, 
            client.clone(),
        ).await?;
        
        execute_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]),  
            table_name, 
            client.clone(),
        ).await?;
        
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
            client.clone(), 
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
        ).await

    }

    async fn test_select_requested_columns_without_hash_columns_upsert_1_times_i32() -> Result<()>{
        let table_name = "select_requested_columns_without_hash_columns_upsert_1_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
        ).await?;
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["age"], 
            None,
            client.clone(), 
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
        ).await
    }

    async fn test_select_requested_columns_without_hash_columns_upsert_2_times_i32() -> Result<()>{
        let table_name = "select_requested_columns_without_hash_columns_upsert_2_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
        ).await?;

        execute_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]]),  
            table_name, 
            client.clone(),
        ).await?;
        
        check_upsert(
            create_batch_i32(vec!["range", "hash", "value", "name"], vec![&[20201102, 20201102, 20201102], &[1, 2, 3], &[111, 222, 333], &[11, 22, 33]]), 
            table_name, 
            vec!["age"], 
            None,
            client.clone(), 
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
        ).await
    }

    async fn test_derange_hash_key_and_data_schema_order_int_type_upsert_1_times_i32() -> Result<()>{
        let table_name = "derange_hash_key_and_data_schema_order_int_type_upsert_1_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);
        
        init_table(
            create_batch_i32(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2],&[1, 2]]),
            table_name,
            SchemaRef::new(Schema::new(["range", "hash1", "hash2", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()],
            client.clone(),
        ).await?;

        check_upsert(
            create_batch_i32(vec!["range", "hash1", "hash2", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[12, 32, 42], &[1, 3, 4]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(noteq(range, null), eq(range, 20201102))".to_string()),
            client.clone(), 
            &[
                "+----------+-------+-------+-------+------+-----+",
                "| range    | hash1 | hash2 | value | name | age |",
                "+----------+-------+-------+-------+------+-----+",
                "| 20201102 | 1     | 12    | 1     |      |     |",
                "| 20201102 | 3     | 32    | 3     |      |     |",
                "| 20201102 | 4     | 42    | 4     |      |     |",
                "+----------+-------+-------+-------+------+-----+",
            ]
        ).await
    }

    async fn test_derange_hash_key_and_data_schema_order_int_type_upsert_2_times_i32() -> Result<()>{
        let table_name = "derange_hash_key_and_data_schema_order_int_type_upsert_2_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2],&[1, 2]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash1", "hash2", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()],
            client.clone(),
        ).await?;

        execute_upsert(
            create_batch_i32(vec!["range", "hash1", "hash2", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[12, 32, 42], &[1, 3, 4]]),  
            table_name, 
            client.clone(),
        ).await?;
        
        check_upsert(
            create_batch_i32(vec!["range", "hash2", "name", "hash1"], vec![&[20201102, 20201102, 20201102], &[12, 22, 32], &[11, 22, 33], &[1, 2, 3]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(noteq(range, null), eq(range, 20201102))".to_string()),
            client.clone(), 
            &[
                "+----------+-------+-------+-------+------+-----+",
                "| range    | hash1 | hash2 | value | name | age |",
                "+----------+-------+-------+-------+------+-----+",
                "| 20201102 | 1     | 12    | 1     | 11   |     |",
                "| 20201102 | 2     | 22    |       | 22   |     |",
                "| 20201102 | 3     | 32    | 3     | 33   |     |",
                "| 20201102 | 4     | 42    | 4     |      |     |",
                "+----------+-------+-------+-------+------+-----+",
            ]
        ).await
    }

    async fn test_derange_hash_key_and_data_schema_order_int_type_upsert_3_times_i32() -> Result<()>{
        let table_name = "derange_hash_key_and_data_schema_order_int_type_upsert_3_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2],&[1, 2]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash1", "hash2", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()],
            client.clone(),
        ).await?;

        execute_upsert(
            create_batch_i32(vec!["range", "hash1", "hash2", "value"], vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[12, 32, 42], &[1, 3, 4]]),  
            table_name, 
            client.clone(),
        ).await?;

        execute_upsert(
            create_batch_i32(vec!["range", "hash2", "name", "hash1"], vec![&[20201102, 20201102, 20201102], &[12, 22, 32], &[11, 22, 33], &[1, 2, 3]]),  
            table_name, 
            client.clone(),
        ).await?;

        check_upsert_debug(
            create_batch_i32(vec!["range", "age", "hash2", "name", "hash1"], vec![&[20201102, 20201102, 20201102], &[2345, 3456, 4567], &[22, 32, 42], &[234, 345, 456], &[2, 3, 4]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(and(noteq(range, null), eq(range, 20201102)), noteq(value, null))".to_string()),
            client.clone(), 
            &[
                "+----------+-------+-------+-------+------+------+",
                "| range    | hash1 | hash2 | value | name | age  |",
                "+----------+-------+-------+-------+------+------+",
                "| 20201102 | 1     | 12    | 1     | 11   |      |",
                "| 20201102 | 3     | 32    | 3     | 345  | 3456 |",
                "| 20201102 | 4     | 42    | 4     | 456  | 4567 |",
                "+----------+-------+-------+-------+------+------+",
            ]
        ).await
    }
    
    async fn test_derange_hash_key_and_data_schema_order_string_type_upsert_1_times_i32() -> Result<()>{
        let table_name = "derange_hash_key_and_data_schema_order_string_type_upsert_1_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_string(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&["20201101", "20201101"], &["1", "2"], &["1", "2"], &["1", "2"], &["1", "2"],&["1", "2"]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash1", "hash2", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Utf8, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()],
            client.clone(),
        ).await?;

        check_upsert(
            create_batch_string(vec!["range", "hash1", "hash2", "value"], vec![&["20201102", "20201102", "20201102"], &["1", "3", "4"], &["12", "32", "42"], &["1", "3", "4"]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(noteq(range, null), eq(range, ________20201102__))".to_string()),
            client.clone(), 
            &[
                "+----------+-------+-------+-------+------+-----+",
                "| range    | hash1 | hash2 | value | name | age |",
                "+----------+-------+-------+-------+------+-----+",
                "| 20201102 | 1     | 12    | 1     |      |     |",
                "| 20201102 | 3     | 32    | 3     |      |     |",
                "| 20201102 | 4     | 42    | 4     |      |     |",
                "+----------+-------+-------+-------+------+-----+",
            ]
        ).await
    }
    
    async fn test_derange_hash_key_and_data_schema_order_string_type_upsert_2_times_i32() -> Result<()>{
        let table_name = "derange_hash_key_and_data_schema_order_string_type_upsert_2_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_string(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&["20201101", "20201101"], &["1", "2"], &["1", "2"], &["1", "2"], &["1", "2"],&["1", "2"]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash1", "hash2", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Utf8, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()],
            client.clone(),
        ).await?;

        execute_upsert(
            create_batch_string(vec!["range", "hash1", "hash2", "value"], vec![&["20201102", "20201102", "20201102"], &["1", "3", "4"], &["12", "32", "42"], &["1", "3", "4"]]),  
            table_name, 
            client.clone(),
        ).await?;

        check_upsert(
            create_batch_string(vec!["range", "hash2", "name", "hash1"], vec![&["20201102", "20201102", "20201102"], &["12", "22", "32"], &["11", "22", "33"], &["1", "2", "3"]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(noteq(range, null), eq(range, ________20201102__))".to_string()),
            client.clone(), 
            &[
                "+----------+-------+-------+-------+------+-----+",
                "| range    | hash1 | hash2 | value | name | age |",
                "+----------+-------+-------+-------+------+-----+",
                "| 20201102 | 1     | 12    | 1     | 11   |     |",
                "| 20201102 | 2     | 22    |       | 22   |     |",
                "| 20201102 | 3     | 32    | 3     | 33   |     |",
                "| 20201102 | 4     | 42    | 4     |      |     |",
                "+----------+-------+-------+-------+------+-----+",
            ]
        ).await
    }

    async fn test_derange_hash_key_and_data_schema_order_string_type_upsert_3_times_i32() -> Result<()>{
        let table_name = "derange_hash_key_and_data_schema_order_string_type_upsert_3_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_string(vec!["range", "hash1", "hash2", "value", "name", "age"], vec![&["20201101", "20201101"], &["1", "2"], &["1", "2"], &["1", "2"], &["1", "2"],&["1", "2"]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash1", "hash2", "value", "name", "age"].into_iter().map(|name| Field::new(name, DataType::Utf8, true)).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash1".to_string(),"hash2".to_string()],
            client.clone(),
        ).await?;

        execute_upsert(
            create_batch_string(vec!["range", "hash1", "hash2", "value"], vec![&["20201102", "20201102", "20201102"], &["1", "3", "4"], &["12", "32", "42"], &["1", "3", "4"]]),  
            table_name, 
            client.clone(),
        ).await?;

        execute_upsert(
            create_batch_string(vec!["range", "hash2", "name", "hash1"], vec![&["20201102", "20201102", "20201102"], &["12", "22", "32"], &["11", "22", "33"], &["1", "2", "3"]]),  
            table_name, 
            client.clone(),
        ).await?;
        
        check_upsert(
            create_batch_string(vec!["range", "age", "hash2", "name", "hash1"], vec![&["20201102", "20201102", "20201102"], &["2345", "3456", "4567"], &["22", "32", "42"], &["234", "345", "456"], &["2", "3", "4"]]), 
            table_name, 
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some("and(and(noteq(range, null), eq(range, ________20201102__)), noteq(value, null))".to_string()),
            client.clone(), 
            &[
                "+----------+-------+-------+-------+------+------+",
                "| range    | hash1 | hash2 | value | name | age  |",
                "+----------+-------+-------+-------+------+------+",
                "| 20201102 | 1     | 12    | 1     | 11   |      |",
                "| 20201102 | 3     | 32    | 3     | 345  | 3456 |",
                "| 20201102 | 4     | 42    | 4     | 456  | 4567 |",
                "+----------+-------+-------+-------+------+------+",
            ]
        ).await
    }
    
    async fn test_create_table_with_hash_key_disordered() -> Result<()>{
        // require metadata checker
        Ok(())
    }

    async fn test_merge_same_column_with_timestamp_type_i32_time() -> Result<()>{
        let dt1=NaiveDate::from_ymd_opt(1000, 6, 14).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt2=NaiveDate::from_ymd_opt(1582, 6, 15).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt3=NaiveDate::from_ymd_opt(1900, 6, 16).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt4=NaiveDate::from_ymd_opt(2018, 6, 17).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        
        let val1=dt1.timestamp_micros();
        let val2=dt2.timestamp_micros();
        let val3=dt3.timestamp_micros();
        let val4=dt4.timestamp_micros();

        let table_name = "test_merge_same_column_with_timestamp_type_i64_time";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32_and_timestamp(vec!["range", "hash", "value", "timestamp"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]], vec![val1, val2, val3, val4]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "timestamp"].into_iter().map(|name| 
                if name=="timestamp" {
                    Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true)
                }else {
                    Field::new(name, DataType::Int32, true)
                }
            ).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
        ).await?;

        check_upsert(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]]), 
            table_name, 
            vec!["range", "hash", "value", "timestamp"],
            None,
            client.clone(), 
            &[
                "+----------+------+-------+----------------------------+",
                "| range    | hash | value | timestamp                  |",
                "+----------+------+-------+----------------------------+",
                "| 20201101 | 1    | 11    | 1000-06-14T08:28:53.123456 |",
                "| 20201101 | 2    | 2     | 1582-06-15T08:28:53.123456 |",
                "| 20201101 | 3    | 33    | 1900-06-16T08:28:53.123456 |",
                "| 20201101 | 4    | 44    |                            |",
                "| 20201102 | 4    | 4     | 2018-06-17T08:28:53.123456 |",
                "+----------+------+-------+----------------------------+",
            ]
        ).await
    }

    async fn test_merge_different_columns_with_timestamp_type_i32_time() -> Result<()>{
        let dt1=NaiveDate::from_ymd_opt(1000, 6, 14).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let _dt2=NaiveDate::from_ymd_opt(1582, 6, 15).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt3=NaiveDate::from_ymd_opt(1900, 6, 16).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        let dt4=NaiveDate::from_ymd_opt(2018, 6, 17).unwrap().and_hms_micro_opt(8, 28, 53, 123456).unwrap();
        
        let val1=dt1.timestamp_micros();
        let _val2=_dt2.timestamp_micros();
        let val3=dt3.timestamp_micros();
        let val4=dt4.timestamp_micros();

        let table_name = "merge_different_columns_with_timestamp_type_i32_time";
        let client = Arc::new(MetaDataClient::from_env().await?);


        init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
            table_name, 
            SchemaRef::new(Schema::new(["range", "hash", "value", "name", "timestamp"].into_iter().map(|name| 
                if name=="timestamp" {
                    Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true)
                }else {
                    Field::new(name, DataType::Int32, true)
                }
            ).collect::<Vec<Field>>())), 
            vec!["range".to_string(), "hash".to_string()],
            client.clone(),
        ).await?;

        check_upsert(
            create_batch_i32_and_timestamp(vec!["range", "hash", "name", "timestamp"], vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]],vec![val1, val3, val4]), 
            table_name, 
            vec!["range", "hash", "value", "name", "timestamp"],
            None,
            client.clone(), 
            &[
                "+----------+------+-------+------+----------------------------+",
                "| range    | hash | value | name | timestamp                  |",
                "+----------+------+-------+------+----------------------------+",
                "| 20201101 | 1    | 1     | 11   | 1000-06-14T08:28:53.123456 |",
                "| 20201101 | 2    | 2     |      |                            |",
                "| 20201101 | 3    | 3     | 33   | 1900-06-16T08:28:53.123456 |",
                "| 20201101 | 4    |       | 44   | 2018-06-17T08:28:53.123456 |",
                "| 20201102 | 4    | 4     |      |                            |",
                "+----------+------+-------+------+----------------------------+",
            ]
        ).await
    }
    
    #[tokio::test]
    async fn test_all_cases()  -> Result<()> {
        test_merge_same_column_i32().await?;
        test_merge_different_column_i32().await?;
        test_merge_different_columns_and_filter_by_non_selected_columns_i32().await?;
        test_merge_different_columns_and_filter_partial_rows_i32().await?;
        test_merge_one_file_with_empty_batch_i32().await?;
        test_merge_multi_files_with_empty_batch_i32().await?;
        test_upsert_without_range_parqitions_i32().await?;
        test_upsert_with_multiple_range_and_hash_parqitions_i32().await?;
        test_filter_requested_columns_upsert_1_times_i32().await?;
        test_filter_requested_columns_upsert_2_times_i32().await?;
        test_filter_requested_columns_upsert_3_times_i32().await?;
        test_select_requested_columns_without_hash_columns_upsert_1_times_i32().await?;
        test_select_requested_columns_without_hash_columns_upsert_2_times_i32().await?;
        test_derange_hash_key_and_data_schema_order_int_type_upsert_1_times_i32().await?;
        test_derange_hash_key_and_data_schema_order_int_type_upsert_2_times_i32().await?;
        test_derange_hash_key_and_data_schema_order_int_type_upsert_3_times_i32().await?;//
        test_derange_hash_key_and_data_schema_order_string_type_upsert_1_times_i32().await?;
        test_derange_hash_key_and_data_schema_order_string_type_upsert_2_times_i32().await?;
        test_derange_hash_key_and_data_schema_order_string_type_upsert_3_times_i32().await?;//
        test_merge_same_column_with_timestamp_type_i32_time().await?;
        test_merge_different_columns_with_timestamp_type_i32_time().await?;

        Ok(())
    }

}

