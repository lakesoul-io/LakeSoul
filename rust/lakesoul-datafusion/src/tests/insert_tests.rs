// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::{path::PathBuf, sync::Arc};

use arrow::array::*;
use arrow::datatypes::{Int32Type, i256};
use arrow::{
    array::{ArrayRef, Int32Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use arrow_cast::pretty::print_batches;
use datafusion::datasource::memory::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::collect;
use datafusion::prelude::{SessionContext, col};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::session::create_session_context;
use lakesoul_metadata::{
    MetaDataClient, MetaDataClientRef, PRIMARY_URL_ENV_KEY, PRIMARY_URL_PROP_KEY,
    PooledClient, QueryType, pg_config_from_env,
};
use rootcause::report;

use crate::lakesoul_table::LakeSoulTable;
use crate::tests::{assert_batches_eq, create_table, create_table_with_file_format};
use crate::{
    Result, catalog::create_io_config_builder, cli::CoreArgs, create_lakesoul_session_ctx,
};

async fn init_table(
    client: MetaDataClientRef,
    schema: SchemaRef,
    table_name: &str,
) -> Result<()> {
    let builder = LakeSoulIOConfigBuilder::new().with_schema(schema.clone());
    create_table(client, table_name, builder.build()).await
}

/// `init_table` pinned to one physical format, for cases whose arrow types a
/// single format cannot encode.
async fn init_table_with_file_format(
    client: MetaDataClientRef,
    schema: SchemaRef,
    table_name: &str,
    file_format: PhysicalFormat,
) -> Result<()> {
    let builder = LakeSoulIOConfigBuilder::new().with_schema(schema.clone());
    create_table_with_file_format(client, table_name, builder.build(), file_format).await
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
        .with_range_partitions(partition_key.into_iter().map(String::from).collect());
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
    filters: Option<Expr>,
    expected: &[&str],
) -> Result<()> {
    let lakesoul_table = LakeSoulTable::for_name(table_name).await?;

    let builder = create_io_config_builder(
        client,
        None,
        false,
        "default",
        Default::default(),
        Default::default(),
    )
    .await?;
    let sess_ctx = create_session_context(&mut builder.clone().build())?;

    let dataframe = lakesoul_table.to_dataframe(&sess_ctx).await?;

    let dataframe = if let Some(f) = filters {
        dataframe.filter(f)?
    } else {
        dataframe
    };

    let dataframe = if selected_cols.is_empty() {
        dataframe
    } else {
        dataframe.select_columns(&selected_cols)?
    };

    let batches = &dataframe.clone().collect().await?;
    print_batches(batches)?;

    let results = dataframe.collect().await?;

    assert_batches_eq(table_name, expected, &results);
    Ok(())
}

fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
    let values: Vec<Arc<dyn Array>> = values
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
    let record_batch =
        create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[10, 20, 30]]);
    init_partitioned_table(
        client.clone(),
        record_batch.schema(),
        table_name,
        vec!["id"],
    )
    .await?;
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
            "| 10   | 1  |",
            "| 20   | 2  |",
            "| 30   | 3  |",
            "+------+----+",
        ],
    )
    .await
}

async fn test_insert_into_append_partitioned_table_with_reordered_subsets() -> Result<()>
{
    let table_name = "test_insert_into_append_partitioned_table_with_reordered_subsets";
    let client = Arc::new(MetaDataClient::from_env().await?);
    let record_batch = create_batch_i32(
        vec!["id", "part", "data"],
        vec![&[1, 2, 3], &[10, 20, 30], &[100, 200, 300]],
    );
    init_partitioned_table(
        client.clone(),
        record_batch.schema(),
        table_name,
        vec!["part"],
    )
    .await?;
    do_insert(record_batch, table_name).await?;

    check_insert(
        client.clone(),
        table_name,
        vec!["data", "part"],
        None,
        &[
            "+------+------+",
            "| data | part |",
            "+------+------+",
            "| 100  | 10   |",
            "| 200  | 20   |",
            "| 300  | 30   |",
            "+------+------+",
        ],
    )
    .await?;

    check_insert(
        client.clone(),
        table_name,
        vec!["part"],
        None,
        &[
            "+------+", "| part |", "+------+", "| 10   |", "| 20   |", "| 30   |",
            "+------+",
        ],
    )
    .await?;

    check_insert(
        client.clone(),
        table_name,
        vec!["data"],
        None,
        &[
            "+------+", "| data |", "+------+", "| 100  |", "| 200  |", "| 300  |",
            "+------+",
        ],
    )
    .await
}

async fn test_insert_into_append_non_partitioned_table_and_read_with_filter() -> Result<()>
{
    let table_name = "test_insert_into_append_non_partitioned_table_and_read_with_filter";
    let client = Arc::new(MetaDataClient::from_env().await?);
    let record_batch = create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[1, 2, 3]]);
    init_table(client.clone(), record_batch.schema(), table_name).await?;
    do_insert(record_batch, table_name).await?;
    check_insert(
        client.clone(),
        table_name,
        vec!["data", "id"],
        Some(col("id").lt_eq(Expr::Literal(
            datafusion::scalar::ScalarValue::Int32(Some(2)),
            None,
        ))),
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

async fn test_insert_into_append_partitioned_table_and_read_with_partition_filter()
-> Result<()> {
    let table_name =
        "test_insert_into_append_partitioned_table_and_read_with_partition_filter";
    let client = Arc::new(MetaDataClient::from_env().await?);
    let record_batch =
        create_batch_i32(vec!["id", "data"], vec![&[1, 2, 3], &[10, 20, 30]]);
    init_partitioned_table(
        client.clone(),
        record_batch.schema(),
        table_name,
        vec!["id"],
    )
    .await?;
    do_insert(record_batch, table_name).await?;
    check_insert(
        client.clone(),
        table_name,
        vec!["data", "id"],
        Some(col("id").lt_eq(Expr::Literal(
            datafusion::scalar::ScalarValue::Int32(Some(2)),
            None,
        ))),
        &[
            "+------+----+",
            "| data | id |",
            "+------+----+",
            "| 10   | 1  |",
            "| 20   | 2  |",
            "+------+----+",
        ],
    )
    .await
}

// todo: insert_overwrite is not supported by datafusion 27.0
// #[tokio::test]
async fn _test_insert_into_overwrite_non_partitioned_table() -> Result<()> {
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
            dbg!(&e,);
            Ok(())
        }
        Ok(()) => Err(report!("Insert Into should fail when missing columns")),
    }
}

async fn test_insert_into_fails_when_an_extra_column_is_present_but_can_evolve_schema()
-> Result<()> {
    let table_name =
        "test_insert_into_fails_when_an_extra_column_is_present_but_can_evolve_schema";
    let client = Arc::new(MetaDataClient::from_env().await?);
    let record_batch = RecordBatch::try_from_iter_with_nullable(vec![
        ("id", Arc::new(Int32Array::from(vec![1])) as ArrayRef, true),
        (
            "data",
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            true,
        ),
        (
            "fruit",
            Arc::new(StringArray::from(vec!["mango"])) as ArrayRef,
            true,
        ),
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
        Ok(()) => Err(report!(
            "Insert Into should fails when an extra column is present but can evolve schema"
        )),
    }
    // todo: pass this case when SCHEMA_AUTO_MIGRATE is true
}

async fn test_datatypes() -> Result<()> {
    let table_name = "test_datatypes";
    let client = Arc::new(MetaDataClient::from_env().await?);

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
        (
            "Date32",
            Arc::new(Date32Array::from(vec![1, -2])) as ArrayRef,
            true,
        ),
        (
            "Date64",
            Arc::new(Date64Array::from(vec![1, -2])) as ArrayRef,
            true,
        ),
        (
            "Decimal128",
            Arc::new(Decimal128Array::from(vec![1, -2])) as ArrayRef,
            true,
        ),
        (
            "Decimal256",
            Arc::new(Decimal256Array::from(vec![Some(i256::default()), None]))
                as ArrayRef,
            true,
        ),
        // ParquetError(ArrowError("Converting Duration to parquet not supported"))
        // ("DurationMicrosecond", Arc::new(DurationMicrosecondArray::from(vec![1])) as ArrayRef, true),
        // ("DurationMillisecond", Arc::new(DurationMillisecondArray::from(vec![1])) as ArrayRef, true),

        // ("Float16", Arc::new(Float16Array::from(vec![1.0])) as ArrayRef, true),
        (
            "FixedSizeBinary",
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    vec![&[1u8][..], &[2u8][..]].into_iter(),
                )
                .unwrap(),
            ) as ArrayRef,
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
            Arc::new(Float64Array::from(vec![3000.6, 300.6])) as ArrayRef,
            true,
        ),
        (
            "Int8",
            Arc::new(Int8Array::from(vec![1i8, -2i8])) as ArrayRef,
            true,
        ),
        // ("Int8Dictionary", Arc::new(Int8DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
        (
            "Int16",
            Arc::new(Int16Array::from(vec![1i16, -2i16])) as ArrayRef,
            true,
        ),
        // ("Int16Dictionary", Arc::new(Int16DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
        (
            "Int32",
            Arc::new(Int32Array::from(vec![1i32, -2i32])) as ArrayRef,
            true,
        ),
        // ("Int32Dictionary", Arc::new(Int32DictionaryArray::from_iter([Some("a"), None])) as ArrayRef, true),
        (
            "Int64",
            Arc::new(Int64Array::from(vec![1i64, -2i64])) as ArrayRef,
            true,
        ),
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
        (
            "String",
            Arc::new(StringArray::from(vec!["1", ""])) as ArrayRef,
            true,
        ),
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
            Arc::new(Time32MillisecondArray::from(vec![1i32, 2i32])) as ArrayRef,
            true,
        ),
        (
            "Time32Second",
            Arc::new(Time32SecondArray::from(vec![1i32, 2i32])) as ArrayRef,
            true,
        ),
        (
            "Time64Microsecond",
            Arc::new(Time64MicrosecondArray::from(vec![1i64, 2i64])) as ArrayRef,
            true,
        ),
        (
            "Time64Nanosecond",
            Arc::new(Time64NanosecondArray::from(vec![1i64, 2i64])) as ArrayRef,
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
        (
            "UInt8",
            Arc::new(UInt8Array::from(vec![1u8, 2u8])) as ArrayRef,
            true,
        ),
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
    // The matrix spans every arrow type the connectors accept; the vortex
    // writer cannot encode `FixedSizeBinary`, so this table stays on Parquet
    // and vortex type coverage lives in the vortex-specific suites.
    init_table_with_file_format(
        client.clone(),
        record_batch.schema(),
        table_name,
        PhysicalFormat::Parquet,
    )
    .await?;
    do_insert(record_batch, table_name).await?;
    check_insert(client.clone(), table_name, vec![], None, &[
    "+---------+--------+------------+-------------------------+---------------+--------------+-----------------+---------------+---------+---------+------+-------+-------+-------+--------------------+------+-------------+-------------+-----------+--------+-------------------+-------------------+--------------+-------------------+--------------------+----------------------------+-------------------------+-------------------------------+---------------------+-------+--------+--------+--------+",
    "| Boolean | Binary | Date32     | Date64                  | Decimal128    | Decimal256   | FixedSizeBinary | FixedSizeList | Float32 | Float64 | Int8 | Int16 | Int32 | Int64 | Map                | Null | LargeBinary | LargeString | List      | String | Struct            | Time32Millisecond | Time32Second | Time64Microsecond | Time64Nanosecond   | TimestampMicrosecond       | TimestampMillisecond    | TimestampNanosecond           | TimestampSecond     | UInt8 | UInt16 | UInt32 | UInt64 |",
    "+---------+--------+------------+-------------------------+---------------+--------------+-----------------+---------------+---------+---------+------+-------+-------+-------+--------------------+------+-------------+-------------+-----------+--------+-------------------+-------------------+--------------+-------------------+--------------------+----------------------------+-------------------------+-------------------------------+---------------------+-------+--------+--------+--------+",
    "| false   | 0203   | 1969-12-30 | 1969-12-31T23:59:59.998 | -0.0000000002 |              | 02              |               | -1.0    | 300.6   | -2   | -2    | -2    | -2    | {blogs: 2, foo: 4} |      | 0203        |             |           |        | {b: true, c: 31}  | 00:00:00.002      | 00:00:02     | 00:00:00.000002   | 00:00:00.000000002 | 1969-12-31T23:59:59.999998 | 1969-12-31T23:59:59.998 | 1969-12-31T23:59:59.999999998 | 1969-12-31T23:59:58 | 2     | 2      | 2      | 2      |",
    "| true    | 01     | 1970-01-02 | 1970-01-01T00:00:00.001 | 0.0000000001  | 0.0000000000 | 01              | [0, 1, 2]     | 1.0     | 3000.6  | 1    | 1     | 1     | 1     | {joe: 1}           |      | 01          | 1           | [0, 1, 2] | 1      | {b: false, c: 42} | 00:00:00.001      | 00:00:01     | 00:00:00.000001   | 00:00:00.000000001 | 1970-01-01T00:00:00.000001 | 1970-01-01T00:00:00.001 | 1970-01-01T00:00:00.000000001 | 1970-01-01T00:00:01 | 1     | 1      | 1      | 1      |",
    "+---------+--------+------------+-------------------------+---------------+--------------+-----------------+---------------+---------+---------+------+-------+-------+-------+--------------------+------+-------------+-------------+-----------+--------+-------------------+-------------------+--------------+-------------------+--------------------+----------------------------+-------------------------+-------------------------------+---------------------+-------+--------+--------+--------+",
        ]).await
}

#[test_log::test(tokio::test)]
async fn test_all_cases() -> Result<()> {
    test_insert_into_append().await?;
    test_insert_into_append_by_position().await?;
    test_insert_into_append_partitioned_table().await?;
    test_insert_into_append_partitioned_table_with_reordered_subsets().await?;
    test_insert_into_append_non_partitioned_table_and_read_with_filter().await?;
    test_insert_into_append_partitioned_table_and_read_with_partition_filter().await?;

    test_insert_into_fails_when_missing_a_column().await?;
    test_insert_into_fails_when_an_extra_column_is_present_but_can_evolve_schema()
        .await?;

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

#[derive(Debug, PartialEq, Eq)]
struct SqlInsertSnapshot {
    physical_files: Vec<(PathBuf, u64)>,
    data_commits: Vec<String>,
    partition_versions: Vec<String>,
    merged_rows: Vec<Vec<Option<i32>>>,
}

struct SqlInsertFixture {
    name: String,
    table_id: String,
    root: PathBuf,
    ctx: Arc<SessionContext>,
    metadata: PooledClient,
    range_partitioned: bool,
}

impl SqlInsertFixture {
    async fn new(
        format: PhysicalFormat,
        primary_key: bool,
        range_partitioned: bool,
        via_ddl: bool,
    ) -> Result<Self> {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let name = format!("sql_insert_{}", uuid::Uuid::new_v4().simple());
        let ctx = create_lakesoul_session_ctx(client.clone(), &CoreArgs::default())?;
        if via_ddl {
            let root = std::env::temp_dir().join(&name);
            let range_field = if range_partitioned {
                ", part INT NOT NULL"
            } else {
                ""
            };
            let primary_key_clause = if primary_key {
                ", PRIMARY KEY (id)"
            } else {
                ""
            };
            let range_clause = if range_partitioned {
                "PARTITIONED BY (part)"
            } else {
                ""
            };
            ctx.sql(&format!(
                "CREATE EXTERNAL TABLE {name} (
                    id INT NOT NULL, required INT NOT NULL, optional INT
                    {range_field}{primary_key_clause}
                 ) STORED AS LAKESOUL
                 {range_clause}
                 OPTIONS ('hashBucketNum' '4', 'file_format' '{}')
                 LOCATION 'file://{}'",
                format.name(),
                root.display(),
            ))
            .await?
            .collect()
            .await?;
        } else {
            let mut fields = vec![
                Field::new("id", DataType::Int32, false),
                Field::new("required", DataType::Int32, false),
                Field::new("optional", DataType::Int32, true),
            ];
            if range_partitioned {
                fields.push(Field::new("part", DataType::Int32, false));
            }
            create_table_with_file_format(
                client.clone(),
                &name,
                LakeSoulIOConfigBuilder::new()
                    .with_schema(Arc::new(Schema::new(fields)))
                    .with_primary_keys(if primary_key {
                        vec!["id".into()]
                    } else {
                        vec![]
                    })
                    .with_range_partitions(if range_partitioned {
                        vec!["part".into()]
                    } else {
                        vec![]
                    })
                    .build(),
                format,
            )
            .await?;
        }
        let table = client
            .get_table_info_by_table_name(&name, "default")
            .await?
            .expect("newly created INSERT test table");
        let root = url::Url::parse(&table.table_path)?
            .to_file_path()
            .expect("INSERT test table uses an isolated local directory");
        let metadata = PooledClient::try_new(
            pg_config_from_env(PRIMARY_URL_PROP_KEY, PRIMARY_URL_ENV_KEY)?,
            None,
        )
        .await?;
        Ok(Self {
            name,
            table_id: table.table_id,
            root,
            ctx,
            metadata,
            range_partitioned,
        })
    }

    async fn execute(&self, query: &str) -> Result<()> {
        self.ctx.sql(query).await?.collect().await?;
        Ok(())
    }

    async fn rows(&self) -> Result<Vec<Vec<Option<i32>>>> {
        let order = if self.range_partitioned {
            "id, required, optional, part"
        } else {
            "id, required, optional"
        };
        let batches = self
            .ctx
            .sql(&format!("SELECT * FROM {} ORDER BY {order}", self.name))
            .await?
            .collect()
            .await?;
        let mut rows = Vec::new();
        for batch in batches {
            let columns = batch
                .columns()
                .iter()
                .map(|array| {
                    array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("INSERT fixture columns are INT")
                })
                .collect::<Vec<_>>();
            for row in 0..batch.num_rows() {
                rows.push(
                    columns
                        .iter()
                        .map(|column| (!column.is_null(row)).then(|| column.value(row)))
                        .collect(),
                );
            }
        }
        Ok(rows)
    }

    async fn assert_rows(&self, expected: &[&[Option<i32>]]) -> Result<()> {
        let expected = expected.iter().map(|row| row.to_vec()).collect::<Vec<_>>();
        assert_eq!(self.rows().await?, expected, "table {}", self.name);
        Ok(())
    }

    async fn snapshot(&self) -> Result<SqlInsertSnapshot> {
        let mut physical_files = Vec::new();
        let mut directories = vec![self.root.clone()];
        while let Some(directory) = directories.pop() {
            if !directory.exists() {
                continue;
            }
            for entry in std::fs::read_dir(directory)? {
                let entry = entry?;
                let metadata = entry.metadata()?;
                if metadata.is_dir() {
                    directories.push(entry.path());
                } else {
                    physical_files.push((
                        entry.path().strip_prefix(&self.root)?.to_path_buf(),
                        metadata.len(),
                    ));
                }
            }
        }
        physical_files.sort();

        // Snapshot-based file APIs cannot expose orphan/uncommitted commit rows.
        // Read every row for this unique table, including historical versions.
        let data_commits = self
            .metadata
            .query(
                "SELECT row_to_json(d)::text FROM data_commit_info AS d
                 WHERE table_id = $1 ORDER BY partition_desc, commit_id",
                QueryType::RW,
                &[&self.table_id],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        let partition_versions = self
            .metadata
            .query(
                "SELECT row_to_json(p)::text FROM partition_info AS p
                 WHERE table_id = $1 ORDER BY partition_desc, version",
                QueryType::RW,
                &[&self.table_id],
            )
            .await?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        Ok(SqlInsertSnapshot {
            physical_files,
            data_commits,
            partition_versions,
            merged_rows: self.rows().await?,
        })
    }

    async fn assert_rejected(&self, query: &str) -> Result<()> {
        let before = self.snapshot().await?;
        assert!(
            self.execute(query).await.is_err(),
            "accepted invalid statement: {query}"
        );
        assert_eq!(
            self.snapshot().await?,
            before,
            "rejected statement changed files, metadata, or rows: {query}"
        );
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_insert_checks_stored_not_null_for_append_and_primary_key_tables()
-> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        for primary_key in [false, true] {
            let table = SqlInsertFixture::new(format, primary_key, false, false).await?;
            let name = &table.name;
            table
                .execute(&format!("INSERT INTO {name} VALUES (1,100,10)"))
                .await?;
            table
                .execute(&format!(
                    "INSERT INTO {name}
                     SELECT id+1, NULLIF(required,-1), optional+1 FROM {name}"
                ))
                .await?;
            table
                .execute(&format!("INSERT INTO {name}(id,required) VALUES (3,300)"))
                .await?;
            table
                .execute(&format!("INSERT INTO {name} VALUES (4,400,NULL)"))
                .await?;

            // Computed NULLs reach the sink through a nullable source field,
            // rather than relying only on literal/VALUES validation.
            for insert in ["INSERT", "INSERT OR REPLACE"] {
                for query in [
                    format!(
                        "{insert} INTO {name}
                         SELECT id+10, NULLIF(required,required), optional FROM {name}"
                    ),
                    format!("{insert} INTO {name}(id,optional) VALUES (5,50)"),
                    format!("{insert} INTO {name} VALUES (5,NULL,50)"),
                    format!("{insert} INTO {name}(required,optional) VALUES (500,50)"),
                    format!("{insert} INTO {name} VALUES (NULL,500,50)"),
                ] {
                    table.assert_rejected(&query).await?;
                }
            }
            table
                .assert_rows(&[
                    &[Some(1), Some(100), Some(10)],
                    &[Some(2), Some(100), Some(11)],
                    &[Some(3), Some(300), None],
                    &[Some(4), Some(400), None],
                ])
                .await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_primary_key_insert_accepts_new_keys_and_rejects_duplicates() -> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        for via_ddl in [false, true] {
            let table = SqlInsertFixture::new(format, true, false, via_ddl).await?;
            let name = &table.name;
            let empty = table.snapshot().await?;
            assert!(empty.physical_files.is_empty());
            assert!(empty.data_commits.is_empty());
            assert!(empty.partition_versions.is_empty());
            assert!(empty.merged_rows.is_empty());
            table
                .assert_rejected(&format!(
                    "INSERT INTO {name} VALUES (1,100,10),(1,101,11)"
                ))
                .await?;
            table
                .execute(&format!("INSERT INTO {name} VALUES (1,100,10)"))
                .await?;
            table
                .execute(&format!("INSERT INTO {name} VALUES (2,200,20)"))
                .await?;
            table
                .execute(&format!("INSERT INTO {name} VALUES (4,400,40),(3,300,30)"))
                .await?;
            for query in [
                format!("INSERT INTO {name} VALUES (1,999,99)"),
                format!("INSERT INTO {name} VALUES (5,500,50),(2,999,99)"),
                format!("INSERT INTO {name} VALUES (6,600,60),(6,601,61)"),
            ] {
                table.assert_rejected(&query).await?;
            }

            // SQL replacement is full-row: an omitted nullable value becomes
            // NULL instead of preserving the previous value as a sparse Upsert.
            table
                .execute(&format!(
                    "INSERT OR REPLACE INTO {name}(id,required) VALUES (1,900)"
                ))
                .await?;
            table
                .assert_rows(&[
                    &[Some(1), Some(900), None],
                    &[Some(2), Some(200), Some(20)],
                    &[Some(3), Some(300), Some(30)],
                    &[Some(4), Some(400), Some(40)],
                ])
                .await?;
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_append_insert_keeps_duplicate_rows() -> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        let table = SqlInsertFixture::new(format, false, false, false).await?;
        let name = &table.name;
        table
            .execute(&format!("INSERT INTO {name} VALUES (1,100,10),(1,100,10)"))
            .await?;
        table
            .execute(&format!("INSERT INTO {name} VALUES (1,100,10)"))
            .await?;
        table
            .assert_rows(&[
                &[Some(1), Some(100), Some(10)],
                &[Some(1), Some(100), Some(10)],
                &[Some(1), Some(100), Some(10)],
            ])
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_range_primary_key_insert_is_partition_scoped_and_atomic() -> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        let table = SqlInsertFixture::new(format, true, true, false).await?;
        let name = &table.name;
        table
            .execute(&format!("INSERT INTO {name} VALUES (1,100,10,10)"))
            .await?;
        table
            .execute(&format!("INSERT INTO {name} VALUES (1,200,20,20)"))
            .await?;
        table
            .execute(&format!("INSERT INTO {name} VALUES (2,110,11,10)"))
            .await?;
        for query in [
            format!("INSERT INTO {name} VALUES (7,700,70,30),(1,999,99,10)"),
            format!(
                "INSERT INTO {name} VALUES
                 (8,800,80,30),(9,900,90,40),(8,801,81,30)"
            ),
            format!("INSERT INTO {name} VALUES (7,700,70,30),(8,NULL,80,40)"),
        ] {
            table.assert_rejected(&query).await?;
        }
        for insert in ["INSERT", "INSERT OR REPLACE"] {
            for query in [
                format!("{insert} INTO {name}(id,required,optional) VALUES (7,700,70)"),
                format!("{insert} INTO {name} VALUES (7,700,70,NULL)"),
                format!(
                    "{insert} INTO {name}(required,optional,part) VALUES (700,70,30)"
                ),
                format!("{insert} INTO {name} VALUES (NULL,700,70,30)"),
            ] {
                table.assert_rejected(&query).await?;
            }
        }
        table
            .assert_rows(&[
                &[Some(1), Some(100), Some(10), Some(10)],
                &[Some(1), Some(200), Some(20), Some(20)],
                &[Some(2), Some(110), Some(11), Some(10)],
            ])
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_primary_key_insert_rejects_stale_physical_plan_without_side_effects()
-> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        for populated in [false, true] {
            let table = SqlInsertFixture::new(format, true, false, false).await?;
            let name = &table.name;
            if populated {
                table
                    .execute(&format!("INSERT OR REPLACE INTO {name} VALUES (0,50,5)"))
                    .await?;
            }
            let held_insert = table
                .ctx
                .sql(&format!("INSERT INTO {name} VALUES (1,100,10)"))
                .await?
                .create_physical_plan()
                .await?;
            let competing_upsert = table
                .ctx
                .sql(&format!("INSERT OR REPLACE INTO {name} VALUES (2,200,20)"))
                .await?
                .create_physical_plan()
                .await?;
            collect(competing_upsert, table.ctx.task_ctx()).await?;
            let before = table.snapshot().await?;
            // Different incoming keys ensure this exercises a stale snapshot,
            // not an ordinary existing-key collision.
            assert!(
                collect(held_insert, table.ctx.task_ctx()).await.is_err(),
                "stale INSERT plan committed over a competing Upsert"
            );
            assert_eq!(table.snapshot().await?, before);
            if populated {
                table
                    .assert_rows(&[
                        &[Some(0), Some(50), Some(5)],
                        &[Some(2), Some(200), Some(20)],
                    ])
                    .await?;
            } else {
                table
                    .assert_rows(&[&[Some(2), Some(200), Some(20)]])
                    .await?;
            }
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_primary_key_insert_rejects_cross_batch_duplicates_and_late_null()
-> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        let table = SqlInsertFixture::new(format, true, false, false).await?;
        table
            .execute("SET datafusion.execution.batch_size = 1")
            .await?;
        let source_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("required", DataType::Int32, true),
            Field::new("optional", DataType::Int32, true),
        ]));
        table.ctx.register_catalog(
            "input",
            SessionContext::new().catalog("datafusion").unwrap(),
        );
        for (suffix, rows) in [
            ("duplicates", [(1, Some(100)), (1, Some(200))]),
            ("late_null", [(2, Some(200)), (3, None)]),
        ] {
            let batches = rows
                .into_iter()
                .map(|(id, required)| {
                    RecordBatch::try_new(
                        source_schema.clone(),
                        vec![
                            Arc::new(Int32Array::from(vec![id])) as ArrayRef,
                            Arc::new(Int32Array::from(vec![required])) as ArrayRef,
                            Arc::new(Int32Array::from(vec![Some(10)])) as ArrayRef,
                        ],
                    )
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let source = format!("{}_source_{suffix}", table.name);
            table.ctx.register_table(
                datafusion_common::TableReference::full(
                    "input",
                    "public",
                    source.clone(),
                ),
                Arc::new(MemTable::try_new(source_schema.clone(), vec![batches])?),
            )?;
            table
                .assert_rejected(&format!(
                    "INSERT INTO {} SELECT id, required, optional FROM input.public.{source}",
                    table.name
                ))
                .await?;
        }
        table.assert_rows(&[]).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_insert_compares_composite_string_keys_across_storage_views() -> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        let client = Arc::new(MetaDataClient::from_env().await?);
        let ctx = create_lakesoul_session_ctx(client.clone(), &CoreArgs::default())?;
        let name = format!("sql_insert_strings_{}", uuid::Uuid::new_v4().simple());
        let root = std::env::temp_dir().join(&name);
        ctx.sql(&format!(
            "CREATE EXTERNAL TABLE {name} (
                id VARCHAR NOT NULL, sub INT NOT NULL, value INT,
                PRIMARY KEY (id, sub)
             ) STORED AS LAKESOUL
             OPTIONS ('hashBucketNum' '4', 'file_format' '{}')
             LOCATION 'file://{}'",
            format.name(),
            root.display(),
        ))
        .await?
        .collect()
        .await?;
        for sql in [
            format!("INSERT INTO {name} VALUES ('x',1,10),('x',2,20)"),
            format!("INSERT INTO {name} SELECT 'y', 1, 30"),
        ] {
            ctx.sql(&sql).await?.collect().await?;
        }
        for sql in [
            format!("INSERT INTO {name} VALUES ('x',1,99)"),
            format!("INSERT INTO {name} VALUES ('z',1,40),('z',1,41)"),
        ] {
            assert!(ctx.sql(&sql).await?.collect().await.is_err());
        }
        let rows = ctx
            .sql(&format!("SELECT * FROM {name} ORDER BY id, sub"))
            .await?
            .collect()
            .await?;
        assert_batches_eq(
            &name,
            &[
                "+----+-----+-------+",
                "| id | sub | value |",
                "+----+-----+-------+",
                "| x  | 1   | 10    |",
                "| x  | 2   | 20    |",
                "| y  | 1   | 30    |",
                "+----+-----+-------+",
            ],
            &rows,
        );
        client.drop_table(&name, "default").await?;
        std::fs::remove_dir_all(root)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_primary_key_insert_empty_query_does_not_publish_files() -> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        let table = SqlInsertFixture::new(format, true, false, false).await?;
        for populated in [false, true] {
            if populated {
                table
                    .execute(&format!("INSERT INTO {} VALUES (1,100,10)", table.name))
                    .await?;
            }
            let before = table.snapshot().await?;
            table
                .execute(&format!(
                    "INSERT INTO {} SELECT id, required, optional FROM {} WHERE false",
                    table.name, table.name,
                ))
                .await?;
            assert_eq!(table.snapshot().await?, before);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_primary_key_writes_allow_subsequent_native_sparse_delta() -> Result<()> {
    use lakesoul_metadata::transfusion::DataFileInfo;
    use parquet::arrow::ArrowWriter;

    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        let table = SqlInsertFixture::new(format, true, false, true).await?;
        let client = MetaDataClient::from_env().await?;
        table
            .execute(&format!("INSERT INTO {} VALUES (1,100,10)", table.name))
            .await?;
        for index in 0..3 {
            if index == 2 {
                table
                    .execute(&format!(
                        "INSERT OR REPLACE INTO {} VALUES (1,200,20)",
                        table.name
                    ))
                    .await?;
            }
            let sparse_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("required", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                sparse_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1])),
                    Arc::new(Int32Array::from(vec![300])),
                ],
            )?;
            let mut hash = vec![0];
            lakesoul_io::utils::hash::create_hashes(&batch.columns()[..1], &mut hash)?;
            let path = table.root.join(format!(
                "part-native-sparse-{index}_{:04}.parquet",
                hash[0] % 4
            ));
            let mut writer =
                ArrowWriter::try_new(std::fs::File::create(&path)?, sparse_schema, None)?;
            writer.write(&batch)?;
            writer.close()?;
            let partition = client
                .get_all_partition_info(&table.table_id)
                .await?
                .pop()
                .unwrap()
                .partition_desc;
            let files = vec![DataFileInfo {
                partition_desc: partition,
                path: url::Url::from_file_path(&path).unwrap().to_string(),
                file_op: "add".into(),
                file_exist_cols: "id,required".into(),
                size: std::fs::metadata(&path)?.len() as i64,
                ..Default::default()
            }];
            if index == 1 {
                // Older Python/SQL writers tagged PK deltas as AppendCommit.
                client
                    .commit_data_files_with_commit_op(
                        &table.name,
                        "default",
                        files,
                        lakesoul_metadata_proto::entity::CommitOp::AppendCommit,
                    )
                    .await?;
            } else {
                client
                    .commit_data_files(&table.name, "default", files)
                    .await?;
            }
            assert_eq!(
                table.rows().await?,
                vec![vec![
                    Some(1),
                    Some(300),
                    Some(if index == 2 { 20 } else { 10 }),
                ]]
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_primary_key_insert_preserves_native_hash_bucket_routing() -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let table = SqlInsertFixture::new(PhysicalFormat::Parquet, true, false, true).await?;
    for (operation, base) in [("INSERT INTO", 100), ("INSERT OR REPLACE INTO", 200)] {
        let values = (0..32)
            .map(|id| format!("({id},{},{})", base + id, 1000 + id))
            .collect::<Vec<_>>()
            .join(",");
        table
            .execute(&format!("{operation} {} VALUES {values}", table.name))
            .await?;
    }
    assert_eq!(
        table.rows().await?,
        (0..32)
            .map(|id| { vec![Some(id), Some(200 + id), Some(1000 + id)] })
            .collect::<Vec<_>>()
    );
    // Bucket ids are a persisted routing contract: native/Spark readers merge
    // and prune each bucket separately, unlike DataFusion's range-wide scan.
    for (relative, _) in table.snapshot().await?.physical_files {
        let bucket: u32 = relative
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .rsplit_once('_')
            .unwrap()
            .1
            .parse()?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(
            table.root.join(relative),
        )?)?
        .build()?;
        for batch in reader {
            let batch = batch?;
            let mut hashes = vec![0; batch.num_rows()];
            lakesoul_io::utils::hash::create_hashes(
                &[batch.column_by_name("id").unwrap().clone()],
                &mut hashes,
            )?;
            assert!(hashes.into_iter().all(|hash| hash % 4 == bucket));
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_replace_rejects_cross_bucket_null_without_leaving_files() -> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        let table = SqlInsertFixture::new(format, true, false, true).await?;
        let values = (0..32)
            .map(|id| format!("({id},{},{})", 100 + id, 1000 + id))
            .collect::<Vec<_>>()
            .join(",");
        table.assert_rejected(&format!(
            "INSERT OR REPLACE INTO {} \
             SELECT id, CASE WHEN id=31 THEN CAST(NULL AS INT) ELSE required END, optional \
             FROM (VALUES {values}) AS src(id,required,optional)", table.name
        )).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_append_rejects_late_null_across_input_partitions_without_leaving_files()
-> Result<()> {
    for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
        let table = SqlInsertFixture::new(format, false, false, false).await?;
        table.ctx.register_catalog(
            "input",
            SessionContext::new().catalog("datafusion").unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("required", DataType::Int32, true),
            Field::new("optional", DataType::Int32, true),
        ]));
        let batch = |id, required| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![id])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![required])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![Some(10)])) as ArrayRef,
                ],
            )
        };
        table.ctx.register_table(
            datafusion_common::TableReference::full("input", "public", "late_null"),
            Arc::new(MemTable::try_new(
                schema.clone(),
                vec![
                    vec![batch(1, Some(100))?, batch(2, Some(200))?],
                    vec![batch(3, Some(300))?, batch(4, None)?],
                ],
            )?),
        )?;
        table
            .assert_rejected(&format!(
                "INSERT INTO {} SELECT * FROM input.public.late_null",
                table.name
            ))
            .await?;
    }
    Ok(())
}
