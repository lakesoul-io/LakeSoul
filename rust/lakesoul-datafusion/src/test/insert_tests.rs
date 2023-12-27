// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

mod insert_tests {
    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::{i256, Int32Type};
    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        record_batch::RecordBatch,
    };
    use datafusion::assert_batches_eq;
    use lakesoul_io::filter::parser::Parser;
    use lakesoul_io::lakesoul_io_config::{create_session_context, LakeSoulIOConfigBuilder};
    use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

    use crate::lakesoul_table::LakeSoulTable;
    use crate::{
        catalog::{create_io_config_builder, create_table},
        error::Result,
    };

    async fn init_table(client: MetaDataClientRef, schema: SchemaRef, table_name: &str) -> Result<()> {
        let builder = LakeSoulIOConfigBuilder::new().with_schema(schema.clone());
        // .with_primary_keys(pks);
        create_table(client, table_name, builder.build()).await
    }

    async fn init_partitioned_table(
        client: MetaDataClientRef,
        schema: SchemaRef,
        table_name: &str,
        partition_key: Vec<&str>,
    ) -> Result<()> {
        // todo: partitioned table is replaced by primary key table currently
        let builder = LakeSoulIOConfigBuilder::new()
            .with_schema(schema.clone())
            .with_primary_keys(partition_key.into_iter().map(String::from).collect());
        create_table(client, table_name, builder.build()).await
    }

    async fn do_insert(record_batch: RecordBatch, table_name: &str) -> Result<()> {
        let lakesoul_table = LakeSoulTable::for_name(table_name).await?;
        lakesoul_table.execute_upsert(record_batch).await
    }

    async fn check_insert(
        client: MetaDataClientRef,
        table_name: &str,
        selected_cols: Vec<&str>,
        filters: Option<String>,
        expected: &[&str],
    ) -> Result<()> {
        let lakesoul_table = LakeSoulTable::for_name(table_name).await?;

        let builder = create_io_config_builder(client, None, false).await?;
        let sess_ctx = create_session_context(&mut builder.clone().build())?;

        let dataframe = lakesoul_table.to_dataframe(&sess_ctx).await?;
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

        let result = dataframe.collect().await?;

        assert_batches_eq!(expected, &result);
        Ok(())
    }

    fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
        let values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names
            .into_iter()
            .zip(values)
            .map(|(name, array)| (name, array, true))
            .collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    async fn test_insert_into_append() -> Result<()> {
        let table_name = "test_insert_into_append";
        let client = Arc::new(MetaDataClient::from_env().await?);
        let record_batch = create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
        init_table(client.clone(), record_batch.schema(), table_name).await?;
        do_insert(record_batch, table_name).await?;
        check_insert(
            client.clone(),
            table_name,
            vec!["id", "data"],
            None,
            &[
                "+----+------+",
                "| id | data |",
                "+----+------+",
                "| 1  | 1    |",
                "| 2  | 2    |",
                "| 3  | 3    |",
                "+----+------+",
            ],
        )
        .await
    }

    async fn test_insert_into_append_by_position() -> Result<()> {
        let table_name = "test_insert_into_append_by_position";
        let client = Arc::new(MetaDataClient::from_env().await?);
        let record_batch = create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
        init_table(client.clone(), record_batch.schema(), table_name).await?;
        do_insert(record_batch, table_name).await?;
        check_insert(
            client.clone(),
            table_name,
            vec!["data", "id"],
            None,
            &[
                "+------+----+",
                "| data | id |",
                "+------+----+",
                "| 1    | 1  |",
                "| 2    | 2  |",
                "| 3    | 3  |",
                "+------+----+",
            ],
        )
        .await
    }

    async fn test_insert_into_append_partitioned_table() -> Result<()> {
        let table_name = "test_insert_into_append_partitioned_table";
        let client = Arc::new(MetaDataClient::from_env().await?);
        let record_batch = create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
        init_partitioned_table(client.clone(), record_batch.schema(), table_name, vec!["id"]).await?;
        do_insert(record_batch, table_name).await?;
        check_insert(
            client.clone(),
            table_name,
            vec!["data", "id"],
            None,
            &[
                "+------+----+",
                "| data | id |",
                "+------+----+",
                "| 1    | 1  |",
                "| 2    | 2  |",
                "| 3    | 3  |",
                "+------+----+",
            ],
        )
        .await
    }

    async fn test_insert_into_append_non_partitioned_table_and_read_with_filter() -> Result<()> {
        let table_name = "test_insert_into_append_non_partitioned_table_and_read_with_filter";
        let client = Arc::new(MetaDataClient::from_env().await?);
        let record_batch = create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
        init_table(client.clone(), record_batch.schema(), table_name).await?;
        do_insert(record_batch, table_name).await?;
        check_insert(
            client.clone(),
            table_name,
            vec!["data", "id"],
            Some("and(noteq(id, null), lteq(id, 2))".to_string()),
            &[
                "+------+----+",
                "| data | id |",
                "+------+----+",
                "| 1    | 1  |",
                "| 2    | 2  |",
                "+------+----+",
            ],
        )
        .await
    }

    async fn test_insert_into_append_partitioned_table_and_read_with_partition_filter() -> Result<()> {
        let table_name = "test_insert_into_append_partitioned_table_and_read_with_partition_filter";
        let client = Arc::new(MetaDataClient::from_env().await?);
        let record_batch = create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
        init_partitioned_table(client.clone(), record_batch.schema(), table_name, vec!["id"]).await?;
        do_insert(record_batch, table_name).await?;
        check_insert(
            client.clone(),
            table_name,
            vec!["data", "id"],
            Some("and(noteq(id, null), lteq(id, 2))".to_string()),
            &[
                "+------+----+",
                "| data | id |",
                "+------+----+",
                "| 1    | 1  |",
                "| 2    | 2  |",
                "+------+----+",
            ],
        )
        .await
    }

    // todo: insert_overwrite is not supported by datafusion 27.0
    // #[tokio::test]
    async fn test_insert_into_overwrite_non_partitioned_table() -> Result<()> {
        let table_name = "test_insert_into_overwrite_non_partitioned_table";
        let client = Arc::new(MetaDataClient::from_env().await?);
        let record_batch = create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
        init_table(client.clone(), record_batch.schema(), table_name).await?;
        do_insert(record_batch, table_name).await?;
        // todo: should do_insert_overwrite
        do_insert(
            create_batch_i32(vec!["id", "data"], vec![&[4, 5, 6], &[4, 5, 6]]),
            table_name,
        )
        .await?;
        check_insert(
            client.clone(),
            table_name,
            vec!["id", "data"],
            None,
            &[
                "+----+------+",
                "| id | data |",
                "+----+------+",
                "| 4  | 4    |",
                "| 5  | 5    |",
                "| 6  | 6    |",
                "+----+------+",
            ],
        )
        .await
    }

    async fn test_insert_into_fails_when_missing_a_column() -> Result<()> {
        let table_name = "test_insert_into_fails_when_missing_a_column";
        let client = Arc::new(MetaDataClient::from_env().await?);
        let record_batch = create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
        init_table(
            client.clone(),
            SchemaRef::new(Schema::new(
                ["id", "data", "missing"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            table_name,
        )
        .await?;
        match do_insert(record_batch, table_name).await {
            Err(e) => {
                dbg!(&e);
                Ok(())
            }
            Ok(()) => Err(crate::error::LakeSoulError::Internal(
                "InsertInto should fail when missing columns".to_string(),
            )),
        }
    }

    async fn test_insert_into_fails_when_an_extra_column_is_present_but_can_evolve_schema() -> Result<()> {
        let table_name = "test_insert_into_fails_when_an_extra_column_is_present_but_can_evolve_schema";
        let client = Arc::new(MetaDataClient::from_env().await?);
        let record_batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef, true),
            ("data", Arc::new(StringArray::from(vec!["a"])) as ArrayRef, true),
            ("fruit", Arc::new(StringArray::from(vec!["mango"])) as ArrayRef, true),
        ])
        .unwrap();
        init_table(
            client.clone(),
            SchemaRef::new(Schema::new(
                ["id", "data"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            table_name,
        )
        .await?;
        match do_insert(record_batch, table_name).await {
            Err(e) => {
                dbg!(&e);
                Ok(())
            }
            Ok(()) => Err(crate::error::LakeSoulError::Internal(
                "InsertInto should fails when an extra column is present but can evolve schema".to_string(),
            )),
        }
        // todo: pass this case when SCHEMA_AUTO_MIGRATE is true
    }

    async fn test_datatypes() -> Result<()> {
        let table_name = "test_datatypes";
        let client = Arc::new(MetaDataClient::from_env().await?);
        // let mut client = MetaDataClient::from_config("host=127.0.0.1 port=5433 dbname=test_lakesoul_meta user=yugabyte password=yugabyte".to_string());

        let iter = vec![
            (
                "Boolean",
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
                true,
            ),
            (
                "Binary",
                Arc::new(BinaryArray::from_vec(vec![&[1u8], &[2u8, 3u8]])) as ArrayRef,
                true,
            ),
            ("Date32", Arc::new(Date32Array::from(vec![1, -2])) as ArrayRef, true),
            ("Date64", Arc::new(Date64Array::from(vec![1, -2])) as ArrayRef, true),
            (
                "Decimal128",
                Arc::new(Decimal128Array::from(vec![1, -2])) as ArrayRef,
                true,
            ),
            (
                "Decimal256",
                Arc::new(Decimal256Array::from(vec![Some(i256::default()), None])) as ArrayRef,
                true,
            ),
            // ParquetError(ArrowError("Converting Duration to parquet not supported"))
            // ("DurationMicrosecond", Arc::new(DurationMicrosecondArray::from(vec![1])) as ArrayRef, true),
            // ("DurationMillisecond", Arc::new(DurationMillisecondArray::from(vec![1])) as ArrayRef, true),

            // ("Float16", Arc::new(Float16Array::from(vec![1.0])) as ArrayRef, true),
            (
                "FixedSizeBinary",
                Arc::new(FixedSizeBinaryArray::from(vec![&[1u8][..], &[2u8][..]])) as ArrayRef,
                true,
            ),
            (
                "FixedSizeList",
                Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                    vec![
                        Some(vec![Some(0), Some(1), Some(2)]),
                        None,
                        // Some(vec![Some(3), None, Some(5)]),
                        // Some(vec![Some(6), Some(7)]),
                    ],
                    3,
                )) as ArrayRef,
                true,
            ),
            (
                "Float32",
                Arc::new(Float32Array::from(vec![1.0, -1.0])) as ArrayRef,
                true,
            ),
            (
                "Float64",
                Arc::new(Float64Array::from(vec![1.0, -1.0])) as ArrayRef,
                true,
            ),
            ("Int8", Arc::new(Int8Array::from(vec![1i8, -2i8])) as ArrayRef, true),
            // ("Int8Dictionary", Arc::new(Int8DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            ("Int16", Arc::new(Int16Array::from(vec![1i16, -2i16])) as ArrayRef, true),
            // ("Int16Dictionary", Arc::new(Int16DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            ("Int32", Arc::new(Int32Array::from(vec![1i32, -2i32])) as ArrayRef, true),
            // ("Int32Dictionary", Arc::new(Int32DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            ("Int64", Arc::new(Int64Array::from(vec![1i64, -2i64])) as ArrayRef, true),
            // ("Int64Dictionary", Arc::new(Int64DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),

            // ("IntervalDayTime", Arc::new(IntervalDayTimeArray::from(vec![1, 2])) as ArrayRef, true),
            // ParquetError(NYI("Attempting to write an Arrow interval type MonthDayNano to parquet that is not yet implemented"))
            //("IntervalMonthDayNano", Arc::new(IntervalMonthDayNanoArray::from(vec![1])) as ArrayRef, true),
            // ("IntervalYearMonth", Arc::new(IntervalYearMonthArray::from(vec![1, 2])) as ArrayRef, true),
            (
                "Map",
                Arc::new({
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
                }) as ArrayRef,
                true,
            ),
            ("Null", Arc::new(NullArray::new(2)) as ArrayRef, true),
            (
                "LargeBinary",
                Arc::new(LargeBinaryArray::from_vec(vec![&[1u8], &[2u8, 3u8]])) as ArrayRef,
                true,
            ),
            (
                "LargeString",
                Arc::new(LargeStringArray::from(vec!["1", ""])) as ArrayRef,
                true,
            ),
            (
                "List",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(0), Some(1), Some(2)]),
                    None,
                    // Some(vec![Some(3), None, Some(5)]),
                    // Some(vec![Some(6), Some(7)]),
                ])) as ArrayRef,
                true,
            ),
            // ParquetError(ArrowError("Converting RunEndEncodedType to parquet not supported"))
            // ("Run", Arc::new(RunArray::<Int32Type>::from_iter([Some("a"), None])) as ArrayRef, true),
            ("String", Arc::new(StringArray::from(vec!["1", ""])) as ArrayRef, true),
            (
                "Struct",
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(Field::new("b", DataType::Boolean, false)),
                        Arc::new(BooleanArray::from(vec![false, true])) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("c", DataType::Int32, false)),
                        Arc::new(Int32Array::from(vec![42, 31])) as ArrayRef,
                    ),
                ])) as ArrayRef,
                true,
            ),
            (
                "Time32Millisecond",
                Arc::new(Time32MillisecondArray::from(vec![1i32, -2i32])) as ArrayRef,
                true,
            ),
            (
                "Time32Second",
                Arc::new(Time32SecondArray::from(vec![1i32, -2i32])) as ArrayRef,
                true,
            ),
            (
                "Time64Microsecond",
                Arc::new(Time64MicrosecondArray::from(vec![1i64, -2i64])) as ArrayRef,
                true,
            ),
            (
                "Time64Nanosecond",
                Arc::new(Time64NanosecondArray::from(vec![1i64, -2i64])) as ArrayRef,
                true,
            ),
            (
                "TimestampMicrosecond",
                Arc::new(TimestampMicrosecondArray::from(vec![1i64, -2i64])) as ArrayRef,
                true,
            ),
            (
                "TimestampMillisecond",
                Arc::new(TimestampMillisecondArray::from(vec![1i64, -2i64])) as ArrayRef,
                true,
            ),
            (
                "TimestampNanosecond",
                Arc::new(TimestampNanosecondArray::from(vec![1i64, -2i64])) as ArrayRef,
                true,
            ),
            (
                "TimestampSecond",
                Arc::new(TimestampSecondArray::from(vec![1i64, -2i64])) as ArrayRef,
                true,
            ),
            ("UInt8", Arc::new(UInt8Array::from(vec![1u8, 2u8])) as ArrayRef, true),
            // ("UInt8Dictionary", Arc::new(UInt8DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            (
                "UInt16",
                Arc::new(UInt16Array::from(vec![1u16, 2u16])) as ArrayRef,
                true,
            ),
            // ("UInt16Dictionary", Arc::new(UInt16DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            (
                "UInt32",
                Arc::new(UInt32Array::from(vec![1u32, 2u32])) as ArrayRef,
                true,
            ),
            // ("UInt32Dictionary", Arc::new(UInt32DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
            (
                "UInt64",
                Arc::new(UInt64Array::from(vec![1u64, 2u64])) as ArrayRef,
                true,
            ),
            // ("UInt64Dictionary", Arc::new(UInt64DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
        ];
        let record_batch = RecordBatch::try_from_iter_with_nullable(iter).unwrap();
        init_table(client.clone(), record_batch.schema(), table_name).await?;
        do_insert(record_batch, table_name).await?;
        check_insert(client.clone(), table_name, vec![], None, &[
            "+---------+--------+------------+---------------------+---------------+--------------+-----------------+---------------+---------+---------+------+-------+-------+-------+--------------------+------+-------------+-------------+-----------+--------+-------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------+-----------------------------------------------------------------------------+----------------------------------------------------------------------------+----------------------------+-------------------------+-------------------------------+---------------------+-------+--------+--------+--------+",
            "| Boolean | Binary | Date32     | Date64              | Decimal128    | Decimal256   | FixedSizeBinary | FixedSizeList | Float32 | Float64 | Int8 | Int16 | Int32 | Int64 | Map                | Null | LargeBinary | LargeString | List      | String | Struct            | Time32Millisecond                                                           | Time32Second                                                           | Time64Microsecond                                                           | Time64Nanosecond                                                           | TimestampMicrosecond       | TimestampMillisecond    | TimestampNanosecond           | TimestampSecond     | UInt8 | UInt16 | UInt32 | UInt64 |",
            "+---------+--------+------------+---------------------+---------------+--------------+-----------------+---------------+---------+---------+------+-------+-------+-------+--------------------+------+-------------+-------------+-----------+--------+-------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------+-----------------------------------------------------------------------------+----------------------------------------------------------------------------+----------------------------+-------------------------+-------------------------------+---------------------+-------+--------+--------+--------+",
            "| true    | 01     | 1970-01-02 | 1970-01-01T00:00:00 | 0.0000000001  | 0.0000000000 | 01              | [0, 1, 2]     | 1.0     | 1.0     | 1    | 1     | 1     | 1     | {joe: 1}           |      | 01          | 1           | [0, 1, 2] | 1      | {b: false, c: 42} | 00:00:00.001                                                                | 00:00:01                                                               | 00:00:00.000001                                                             | 00:00:00.000000001                                                         | 1970-01-01T00:00:00.000001 | 1970-01-01T00:00:00.001 | 1970-01-01T00:00:00.000000001 | 1970-01-01T00:00:01 | 1     | 1      | 1      | 1      |",
            "| false   | 0203   | 1969-12-30 | 1970-01-01T00:00:00 | -0.0000000002 |              | 02              |               | -1.0    | -1.0    | -2   | -2    | -2    | -2    | {blogs: 2, foo: 4} |      | 0203        |             |           |        | {b: true, c: 31}  | ERROR: Cast error: Failed to convert -2 to temporal for Time32(Millisecond) | ERROR: Cast error: Failed to convert -2 to temporal for Time32(Second) | ERROR: Cast error: Failed to convert -2 to temporal for Time64(Microsecond) | ERROR: Cast error: Failed to convert -2 to temporal for Time64(Nanosecond) | 1969-12-31T23:59:59.999998 | 1969-12-31T23:59:59.998 | 1969-12-31T23:59:59.999999998 | 1969-12-31T23:59:58 | 2     | 2      | 2      | 2      |",
            "+---------+--------+------------+---------------------+---------------+--------------+-----------------+---------------+---------+---------+------+-------+-------+-------+--------------------+------+-------------+-------------+-----------+--------+-------------------+-----------------------------------------------------------------------------+------------------------------------------------------------------------+-----------------------------------------------------------------------------+----------------------------------------------------------------------------+----------------------------+-------------------------+-------------------------------+---------------------+-------+--------+--------+--------+"
        ]).await
    }

    #[tokio::test]
    async fn test_all_cases() -> Result<()> {
        test_insert_into_append().await?;
        test_insert_into_append_by_position().await?;
        test_insert_into_append_partitioned_table().await?;
        test_insert_into_append_non_partitioned_table_and_read_with_filter().await?;
        test_insert_into_append_partitioned_table_and_read_with_partition_filter().await?;

        test_insert_into_fails_when_missing_a_column().await?;
        test_insert_into_fails_when_an_extra_column_is_present_but_can_evolve_schema().await?;

        test_datatypes().await?;

        // overwrite case
        // todo: insert_overwrite is not supported by datafusion 27.0

        // test_insert_into_overwrite_non_partitioned_table().await?;
        // test_insert_into_overwrite_by_position().await?;

        // todo:
        // test_insert_info_schema_enforcement().await?;
        // test_insert_info_struct_types_and_schema_enforcement().await?;
        Ok(())
    }
}
