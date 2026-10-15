// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
mod upsert_with_metadata_tests {

    use chrono::naive::NaiveDate;
    use lakesoul_io::session::create_session_context;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::DataType;
    use arrow::datatypes::{Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use lakesoul_io::config::LakeSoulIOConfigBuilder;
    use lakesoul_io::filter::parser::Parser;
    use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

    use crate::Result;
    use crate::catalog::create_io_config_builder;
    use crate::lakesoul_table::LakeSoulTable;
    use crate::tests::{assert_batches_eq, create_table, create_table_with_file_format};

    enum StrOrI32 {
        V1(&'static str),
        V2(i32),
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

    fn create_batch_optional_i32(
        names: Vec<&str>,
        values: Vec<&[Option<i32>]>,
    ) -> RecordBatch {
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

    fn create_batch_i32_and_timestamp(
        names: Vec<&str>,
        values: Vec<&[i32]>,
        timestamp: Vec<i64>,
    ) -> RecordBatch {
        let mut values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let timestamp = Arc::new(TimestampMicrosecondArray::from(timestamp)) as ArrayRef;
        values.push(timestamp);
        let iter = names
            .into_iter()
            .zip(values)
            .map(|(name, array)| (name, array, true))
            .collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    fn create_batch_string(names: Vec<&str>, values: Vec<&[&str]>) -> RecordBatch {
        let values = values
            .into_iter()
            .map(|vec| Arc::new(StringArray::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names
            .into_iter()
            .zip(values)
            .map(|(name, array)| (name, array, true))
            .collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    fn create_batch_str_or_i32(
        names: Vec<&str>,
        values: Vec<&[StrOrI32]>,
    ) -> RecordBatch {
        let values = values
            .into_iter()
            .map(|vec| match vec[0] {
                StrOrI32::V1(_) => {
                    let vec = vec
                        .iter()
                        .map(|val| match val {
                            StrOrI32::V1(v1) => Some(*v1),
                            StrOrI32::V2(_v2) => None,
                        })
                        .map(|val| val.unwrap())
                        .collect::<Vec<&str>>();
                    Arc::new(StringArray::from(vec)) as ArrayRef
                }
                StrOrI32::V2(_) => {
                    let vec = vec
                        .iter()
                        .map(|val| match val {
                            StrOrI32::V1(_v1) => None,
                            StrOrI32::V2(v2) => Some(v2),
                        })
                        .map(|val| *val.unwrap())
                        .collect::<Vec<i32>>();
                    Arc::new(Int32Array::from(vec)) as ArrayRef
                }
            })
            .collect::<Vec<ArrayRef>>();
        let iter = names
            .into_iter()
            .zip(values)
            .map(|(name, array)| (name, array, true))
            .collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    async fn execute_upsert(
        record_batch: RecordBatch,
        table_name: &str,
        _client: MetaDataClientRef,
    ) -> Result<()> {
        let lakesoul_table = LakeSoulTable::for_name(table_name).await?;
        lakesoul_table.execute_upsert(record_batch).await
    }

    async fn check_upsert_debug(
        batch: RecordBatch,
        table_name: &str,
        selected_cols: Vec<&str>,
        filters: Option<String>,
        client: MetaDataClientRef,
        expected: &[&str],
    ) -> Result<()> {
        let lakesoul_table = LakeSoulTable::for_name(table_name).await?;
        lakesoul_table.execute_upsert(batch).await?;
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
        let schema = SchemaRef::new(dataframe.schema().as_arrow().clone());

        let dataframe = if let Some(f) = filters {
            dataframe.filter(Parser::parse(f.clone(), schema)?)?
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
        assert_batches_eq(table_name, expected, &result);
        Ok(())
    }

    async fn init_table(
        batch: RecordBatch,
        table_name: &str,
        schema: SchemaRef,
        pks: Vec<String>,
        range_partitions: Vec<String>,
        client: MetaDataClientRef,
    ) -> Result<()> {
        let builder = LakeSoulIOConfigBuilder::new()
            .with_schema(schema)
            .with_primary_keys(pks)
            .with_range_partitions(range_partitions);
        create_table(client.clone(), table_name, builder.build()).await?;
        let lakesoul_table = LakeSoulTable::for_name(table_name).await?;
        lakesoul_table.execute_upsert(batch).await
    }

    async fn check_upsert(
        batch: RecordBatch,
        table_name: &str,
        selected_cols: Vec<&str>,
        filters: Option<String>,
        client: MetaDataClientRef,
        expected: &[&str],
    ) -> Result<()> {
        let lakesoul_table = LakeSoulTable::for_name(table_name).await?;
        lakesoul_table.execute_upsert(batch).await?;
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
        let schema = SchemaRef::new(dataframe.schema().as_arrow().clone());

        let dataframe = if let Some(f) = filters {
            dataframe.filter(Parser::parse(f.clone(), schema)?)?
        } else {
            dataframe
        };

        let dataframe = if selected_cols.is_empty() {
            dataframe
        } else {
            dataframe.select_columns(&selected_cols)?
        };

        let result = dataframe.collect().await?;
        assert_batches_eq(table_name, expected, &result);
        Ok(())
    }

    async fn test_merge_same_column_i32() -> Result<()> {
        let table_name = "test_merge_same_column_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);
        init_table(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201101, 20201101, 20201101], &[4, 3, 1], &[44, 33, 11]],
            ),
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
            ],
        )
        .await
    }

    async fn test_merge_different_column_i32() -> Result<()> {
        let table_name = "test_merge_different_column_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "name"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "name"],
                vec![&[20201101, 20201101, 20201101], &[1, 4, 3], &[11, 44, 33]],
            ),
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
            ],
        )
        .await
    }

    async fn test_merge_different_columns_and_filter_by_non_selected_columns_i32()
    -> Result<()> {
        let table_name =
            "test_merge_different_columns_and_filter_by_non_selected_columns_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "name"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "name"],
                vec![&[20201101, 20201101, 20201101], &[3, 1, 4], &[33, 11, 44]],
            ),
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
            ],
        )
        .await
    }

    async fn test_merge_different_columns_and_filter_partial_rows_i32() -> Result<()> {
        let table_name = "merge_different_columns_and_filter_partial_rows_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value", "name"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                    &[11, 22, 33, 44],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "name"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201101, 20201101, 20201101, 20201102], &[1, 3, 4, 4], &[2, 4, 5, 5]],
            ),
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
            ],
        )
        .await
    }

    async fn test_merge_one_file_with_empty_batch_i32() -> Result<()> {
        let table_name = "merge_one_file_with_empty_batch";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .iter()
                    .map(|col| Field::new(*col, DataType::Int32, true))
                    .collect::<Vec<_>>(),
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
            ],
        )
        .await
    }

    async fn test_merge_multi_files_with_empty_batch_i32() -> Result<()> {
        let table_name = "merge_multi_files_with_empty_batch";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102, 20201102],
                    &[1, 2, 3, 4, 1],
                    &[1, 2, 3, 4, 1],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201102], &[4], &[5]],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .iter()
                    .map(|col| Field::new(*col, DataType::Int32, true))
                    .collect::<Vec<_>>(),
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
            ],
        )
        .await
    }

    async fn _test_basic_upsert_same_columns() -> Result<()> {
        // require metadata checker
        let table_name = "test_basic_upsert_same_columns";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .iter()
                    .map(|col| Field::new(*col, DataType::Int32, true))
                    .collect::<Vec<_>>(),
            ))),
            table_name,
            vec!["range", "hash", "value"],
            None,
            client.clone(),
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 1     |",
                "| 20201101 | 2    | 2     |",
                "| 20201101 | 3    | 3     |",
                "| 20201102 | 4    | 4     |",
                "+----------+------+-------+",
            ],
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]],
            ),
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
            ],
        )
        .await
    }

    async fn _test_basic_upsert_different_columns() -> Result<()> {
        // require metadata checker
        let table_name = "test_basic_upsert_same_columns";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .iter()
                    .map(|col| Field::new(*col, DataType::Int32, true))
                    .collect::<Vec<_>>(),
            ))),
            table_name,
            vec!["range", "hash", "value"],
            None,
            client.clone(),
            &[
                "+----------+------+-------+",
                "| range    | hash | value |",
                "+----------+------+-------+",
                "| 20201101 | 1    | 1     |",
                "| 20201101 | 2    | 2     |",
                "| 20201101 | 3    | 3     |",
                "| 20201102 | 4    | 4     |",
                "+----------+------+-------+",
            ],
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]],
            ),
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
            ],
        )
        .await
    }

    async fn test_upsert_without_range_partitions_i32() -> Result<()> {
        let table_name = "upsert_without_range_partitions";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec![],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]],
            ),
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
            ],
        )
        .await
    }

    async fn test_upsert_with_multiple_range_and_hash_partitions_i32() -> Result<()> {
        let table_name = "upsert_with_multiple_range_and_hash_partitions";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range1", "range2", "hash1", "hash2", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range1", "range2", "hash1", "hash2", "value"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash1".to_string(), "hash2".to_string()],
            vec!["range1".to_string(), "range2".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range1", "range2", "hash1", "hash2", "value"],
                vec![
                    &[20201101, 20201101, 20201101],
                    &[1, 3, 4],
                    &[1, 3, 4],
                    &[1, 3, 4],
                    &[11, 33, 44],
                ],
            ),
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
            ],
        )
        .await
    }

    async fn test_filter_requested_columns_upsert_1_times_i32() -> Result<()> {
        let table_name = "filter_requested_columns_upsert_1_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value", "name", "age"],
                vec![
                    &[20201101, 20201101, 20201101, 20201101],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]],
            ),
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
            ],
        )
        .await
    }

    async fn test_filter_requested_columns_upsert_2_times_i32() -> Result<()> {
        let table_name = "filter_requested_columns_upsert_2_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value", "name", "age"],
                vec![
                    &[20201101, 20201101, 20201101, 20201101],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value", "name"],
                vec![
                    &[20201102, 20201102, 20201102],
                    &[1, 2, 3],
                    &[111, 222, 333],
                    &[11, 22, 33],
                ],
            ),
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
            ],
        )
        .await
    }

    async fn test_filter_requested_columns_upsert_3_times_i32() -> Result<()> {
        let table_name = "filter_requested_columns_upsert_3_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value", "name", "age"],
                vec![
                    &[20201101, 20201101, 20201101, 20201101],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_i32(
                vec!["range", "hash", "value", "name"],
                vec![
                    &[20201102, 20201102, 20201102],
                    &[1, 2, 3],
                    &[111, 222, 333],
                    &[11, 22, 33],
                ],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]],
            ),
            table_name,
            client.clone(),
        )
        .await?;

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
                    &[None, Some(3333)],
                ],
            ),
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
            ],
        )
        .await
    }

    async fn test_select_requested_columns_without_hash_columns_upsert_1_times_i32()
    -> Result<()> {
        let table_name = "select_requested_columns_without_hash_columns_upsert_1_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value", "name", "age"],
                vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2]],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]],
            ),
            table_name,
            vec!["age"],
            None,
            client.clone(),
            &[
                "+-----+", "| age |", "+-----+", "| 1   |", "| 2   |", "|     |",
                "|     |", "|     |", "+-----+",
            ],
        )
        .await
    }

    async fn test_select_requested_columns_without_hash_columns_upsert_2_times_i32()
    -> Result<()> {
        let table_name = "select_requested_columns_without_hash_columns_upsert_2_times";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value", "name", "age"],
                vec![&[20201101, 20201101], &[1, 2], &[1, 2], &[1, 2], &[1, 2]],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201102, 20201102, 20201102], &[1, 3, 4], &[11, 33, 44]],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value", "name"],
                vec![
                    &[20201102, 20201102, 20201102],
                    &[1, 2, 3],
                    &[111, 222, 333],
                    &[11, 22, 33],
                ],
            ),
            table_name,
            vec!["age"],
            None,
            client.clone(),
            &[
                "+-----+", "| age |", "+-----+", "| 1   |", "| 2   |", "|     |",
                "|     |", "|     |", "|     |", "+-----+",
            ],
        )
        .await
    }

    async fn test_derange_hash_key_and_data_schema_order_int_type_upsert_1_times_i32()
    -> Result<()> {
        let table_name =
            "derange_hash_key_and_data_schema_order_int_type_upsert_1_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash1", "hash2", "value", "name", "age"],
                vec![
                    &[20201101, 20201101],
                    &[1, 2],
                    &[1, 2],
                    &[1, 2],
                    &[1, 2],
                    &[1, 2],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash1", "hash2", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash1".to_string(), "hash2".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash1", "hash2", "value"],
                vec![
                    &[20201102, 20201102, 20201102],
                    &[1, 3, 4],
                    &[12, 32, 42],
                    &[1, 3, 4],
                ],
            ),
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
            ],
        )
        .await
    }

    async fn test_derange_hash_key_and_data_schema_order_int_type_upsert_2_times_i32()
    -> Result<()> {
        let table_name =
            "derange_hash_key_and_data_schema_order_int_type_upsert_2_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash1", "hash2", "value", "name", "age"],
                vec![
                    &[20201101, 20201101],
                    &[1, 2],
                    &[1, 2],
                    &[1, 2],
                    &[1, 2],
                    &[1, 2],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash1", "hash2", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash1".to_string(), "hash2".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_i32(
                vec!["range", "hash1", "hash2", "value"],
                vec![
                    &[20201102, 20201102, 20201102],
                    &[1, 3, 4],
                    &[12, 32, 42],
                    &[1, 3, 4],
                ],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash2", "name", "hash1"],
                vec![
                    &[20201102, 20201102, 20201102],
                    &[12, 22, 32],
                    &[11, 22, 33],
                    &[1, 2, 3],
                ],
            ),
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
            ],
        )
        .await
    }

    async fn test_derange_hash_key_and_data_schema_order_int_type_upsert_3_times_i32()
    -> Result<()> {
        let table_name =
            "derange_hash_key_and_data_schema_order_int_type_upsert_3_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash1", "hash2", "value", "name", "age"],
                vec![
                    &[20201101, 20201101],
                    &[1, 2],
                    &[1, 2],
                    &[1, 2],
                    &[1, 2],
                    &[1, 2],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash1", "hash2", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash1".to_string(), "hash2".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_i32(
                vec!["range", "hash1", "hash2", "value"],
                vec![
                    &[20201102, 20201102, 20201102],
                    &[1, 3, 4],
                    &[12, 32, 42],
                    &[1, 3, 4],
                ],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_i32(
                vec!["range", "hash2", "name", "hash1"],
                vec![
                    &[20201102, 20201102, 20201102],
                    &[12, 22, 32],
                    &[11, 22, 33],
                    &[1, 2, 3],
                ],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        check_upsert_debug(
            create_batch_i32(
                vec!["range", "age", "hash2", "name", "hash1"],
                vec![
                    &[20201102, 20201102, 20201102],
                    &[4567, 2345, 3456],
                    &[42, 22, 32],
                    &[456, 234, 345],
                    &[4, 2, 3],
                ],
            ),
            table_name,
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some(
                "and(and(noteq(range, null), eq(range, 20201102)), noteq(value, null))"
                    .to_string(),
            ),
            client.clone(),
            &[
                "+----------+-------+-------+-------+------+------+",
                "| range    | hash1 | hash2 | value | name | age  |",
                "+----------+-------+-------+-------+------+------+",
                "| 20201102 | 1     | 12    | 1     | 11   |      |",
                "| 20201102 | 3     | 32    | 3     | 345  | 3456 |",
                "| 20201102 | 4     | 42    | 4     | 456  | 4567 |",
                "+----------+-------+-------+-------+------+------+",
            ],
        )
        .await
    }

    async fn test_derange_hash_key_and_data_schema_order_string_type_upsert_1_times_i32()
    -> Result<()> {
        let table_name =
            "derange_hash_key_and_data_schema_order_string_type_upsert_1_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_string(
                vec!["range", "hash1", "hash2", "value", "name", "age"],
                vec![
                    &["20201101", "20201101"],
                    &["1", "2"],
                    &["1", "2"],
                    &["1", "2"],
                    &["1", "2"],
                    &["1", "2"],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash1", "hash2", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash1".to_string(), "hash2".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_string(
                vec!["range", "hash1", "hash2", "value"],
                vec![
                    &["20201102", "20201102", "20201102"],
                    &["1", "3", "4"],
                    &["12", "32", "42"],
                    &["1", "3", "4"],
                ],
            ),
            table_name,
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some(r#"and(noteq(range, null), eq(range, Binary{"20201102"}))"#.to_string()),
            client.clone(),
            &[
                "+----------+-------+-------+-------+------+-----+",
                "| range    | hash1 | hash2 | value | name | age |",
                "+----------+-------+-------+-------+------+-----+",
                "| 20201102 | 1     | 12    | 1     |      |     |",
                "| 20201102 | 3     | 32    | 3     |      |     |",
                "| 20201102 | 4     | 42    | 4     |      |     |",
                "+----------+-------+-------+-------+------+-----+",
            ],
        )
        .await
    }

    async fn test_derange_hash_key_and_data_schema_order_string_type_upsert_2_times_i32()
    -> Result<()> {
        let table_name =
            "derange_hash_key_and_data_schema_order_string_type_upsert_2_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_string(
                vec!["range", "hash1", "hash2", "value", "name", "age"],
                vec![
                    &["20201101", "20201101"],
                    &["1", "2"],
                    &["1", "2"],
                    &["1", "2"],
                    &["1", "2"],
                    &["1", "2"],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash1", "hash2", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash1".to_string(), "hash2".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_string(
                vec!["range", "hash1", "hash2", "value"],
                vec![
                    &["20201102", "20201102", "20201102"],
                    &["1", "3", "4"],
                    &["12", "32", "42"],
                    &["1", "3", "4"],
                ],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_string(
                vec!["range", "hash2", "name", "hash1"],
                vec![
                    &["20201102", "20201102", "20201102"],
                    &["12", "22", "32"],
                    &["11", "22", "33"],
                    &["1", "2", "3"],
                ],
            ),
            table_name,
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some(r#"and(noteq(range, null), eq(range, Binary{"20201102"}))"#.to_string()),
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
            ],
        )
        .await
    }

    async fn test_derange_hash_key_and_data_schema_order_string_type_upsert_3_times_i32()
    -> Result<()> {
        let table_name =
            "derange_hash_key_and_data_schema_order_string_type_upsert_3_times_i32";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_string(
                vec!["range", "hash1", "hash2", "value", "name", "age"],
                vec![
                    &["20201101", "20201101"],
                    &["1", "2"],
                    &["1", "2"],
                    &["1", "2"],
                    &["1", "2"],
                    &["1", "2"],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash1", "hash2", "value", "name", "age"]
                    .into_iter()
                    .map(|name| Field::new(name, DataType::Utf8, true))
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash1".to_string(), "hash2".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_string(
                vec!["range", "hash1", "hash2", "value"],
                vec![
                    &["20201102", "20201102", "20201102"],
                    &["1", "3", "4"],
                    &["12", "32", "42"],
                    &["1", "3", "4"],
                ],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        execute_upsert(
            create_batch_string(
                vec!["range", "hash2", "name", "hash1"],
                vec![
                    &["20201102", "20201102", "20201102"],
                    &["12", "22", "32"],
                    &["11", "22", "33"],
                    &["1", "2", "3"],
                ],
            ),
            table_name,
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_string(
                vec!["range", "age", "hash2", "name", "hash1"],
                vec![
                    &["20201102", "20201102", "20201102"],
                    &["4567", "2345", "3456"],
                    &["42", "22", "32"],
                    &["456", "234", "345"],
                    &["4", "2", "3"],
                ],
            ),
            table_name,
            vec!["range", "hash1", "hash2", "value", "name", "age"],
            Some(
                r#"and(and(noteq(range, null), eq(range, Binary{"20201102"})), noteq(value, null))"#
                    .to_string(),
            ),
            client.clone(),
            &[
                "+----------+-------+-------+-------+------+------+",
                "| range    | hash1 | hash2 | value | name | age  |",
                "+----------+-------+-------+-------+------+------+",
                "| 20201102 | 1     | 12    | 1     | 11   |      |",
                "| 20201102 | 3     | 32    | 3     | 345  | 3456 |",
                "| 20201102 | 4     | 42    | 4     | 456  | 4567 |",
                "+----------+-------+-------+-------+------+------+",
            ],
        )
        .await
    }

    async fn test_create_table_with_hash_key_disordered() -> Result<()> {
        let table_name = "test_create_table_with_hash_key_disordered";
        let client = Arc::new(MetaDataClient::from_env().await?);

        let batch1 = create_batch_str_or_i32(
            vec!["range", "v1", "hash1", "v2", "hash2"],
            vec![
                &[
                    StrOrI32::V1("range"),
                    StrOrI32::V1("range"),
                    StrOrI32::V1("range"),
                ],
                &[StrOrI32::V1("a1"), StrOrI32::V1("b1"), StrOrI32::V1("c1")],
                &[StrOrI32::V2(1), StrOrI32::V2(2), StrOrI32::V2(3)],
                &[StrOrI32::V1("a2"), StrOrI32::V1("b2"), StrOrI32::V1("c2")],
                &[StrOrI32::V1("a"), StrOrI32::V1("b"), StrOrI32::V1("c")],
            ],
        );

        let batch2 = create_batch_str_or_i32(
            vec!["range", "hash1", "v1", "v2", "hash2"],
            vec![
                &[
                    StrOrI32::V1("range"),
                    StrOrI32::V1("range"),
                    StrOrI32::V1("range"),
                ],
                &[StrOrI32::V2(1), StrOrI32::V2(2), StrOrI32::V2(3)],
                &[
                    StrOrI32::V1("a11"),
                    StrOrI32::V1("b11"),
                    StrOrI32::V1("c11"),
                ],
                &[
                    StrOrI32::V1("a22"),
                    StrOrI32::V1("b22"),
                    StrOrI32::V1("c22"),
                ],
                &[StrOrI32::V1("a"), StrOrI32::V1("b"), StrOrI32::V1("c")],
            ],
        );

        let batch3 = create_batch_str_or_i32(
            vec!["range", "v1", "hash1", "v2", "hash2"],
            vec![
                &[
                    StrOrI32::V1("range"),
                    StrOrI32::V1("range"),
                    StrOrI32::V1("range"),
                ],
                &[
                    StrOrI32::V1("d1"),
                    StrOrI32::V1("b111"),
                    StrOrI32::V1("c111"),
                ],
                &[StrOrI32::V2(4), StrOrI32::V2(2), StrOrI32::V2(3)],
                &[
                    StrOrI32::V1("d2"),
                    StrOrI32::V1("b222"),
                    StrOrI32::V1("c222"),
                ],
                &[StrOrI32::V1("d"), StrOrI32::V1("b"), StrOrI32::V1("c")],
            ],
        );

        init_table(
            batch1,
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "v1", "hash1", "v2", "hash2"]
                    .into_iter()
                    .map(|name| {
                        if name == "hash1" {
                            Field::new(name, DataType::Int32, true)
                        } else {
                            Field::new(name, DataType::Utf8, true)
                        }
                    })
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash1".to_string(), "hash2".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        execute_upsert(batch2, table_name, client.clone()).await?;

        execute_upsert(batch3, table_name, client.clone()).await?;

        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                ["range", "v1", "hash1", "v2", "hash2"]
                    .iter()
                    .map(|col| {
                        if *col == "hash1" {
                            Field::new(*col, DataType::Int32, true)
                        } else {
                            Field::new(*col, DataType::Utf8, true)
                        }
                    })
                    .collect::<Vec<_>>(),
            ))),
            table_name,
            vec!["range", "hash1", "hash2", "v1", "v2"],
            None,
            client.clone(),
            &[
                "+-------+-------+-------+------+------+",
                "| range | hash1 | hash2 | v1   | v2   |",
                "+-------+-------+-------+------+------+",
                "| range | 1     | a     | a11  | a22  |",
                "| range | 2     | b     | b111 | b222 |",
                "| range | 3     | c     | c111 | c222 |",
                "| range | 4     | d     | d1   | d2   |",
                "+-------+-------+-------+------+------+",
            ],
        )
        .await?;

        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                ["range", "v1", "hash1", "v2", "hash2"]
                    .iter()
                    .map(|col| {
                        if *col == "hash1" {
                            Field::new(*col, DataType::Int32, true)
                        } else {
                            Field::new(*col, DataType::Utf8, true)
                        }
                    })
                    .collect::<Vec<_>>(),
            ))),
            table_name,
            vec!["hash1", "v1", "v2"],
            None,
            client.clone(),
            &[
                "+-------+------+------+",
                "| hash1 | v1   | v2   |",
                "+-------+------+------+",
                "| 1     | a11  | a22  |",
                "| 2     | b111 | b222 |",
                "| 3     | c111 | c222 |",
                "| 4     | d1   | d2   |",
                "+-------+------+------+",
            ],
        )
        .await?;

        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                ["range", "v1", "hash1", "v2", "hash2"]
                    .iter()
                    .map(|col| {
                        if *col == "hash1" {
                            Field::new(*col, DataType::Int32, true)
                        } else {
                            Field::new(*col, DataType::Utf8, true)
                        }
                    })
                    .collect::<Vec<_>>(),
            ))),
            table_name,
            vec!["v1", "v2"],
            None,
            client.clone(),
            &[
                "+------+------+",
                "| v1   | v2   |",
                "+------+------+",
                "| a11  | a22  |",
                "| b111 | b222 |",
                "| c111 | c222 |",
                "| d1   | d2   |",
                "+------+------+",
            ],
        )
        .await?;

        check_upsert(
            RecordBatch::new_empty(SchemaRef::new(Schema::new(
                ["range", "v1", "hash1", "v2", "hash2"]
                    .iter()
                    .map(|col| {
                        if *col == "hash1" {
                            Field::new(*col, DataType::Int32, true)
                        } else {
                            Field::new(*col, DataType::Utf8, true)
                        }
                    })
                    .collect::<Vec<_>>(),
            ))),
            table_name,
            vec!["range", "v2"],
            None,
            client.clone(),
            &[
                "+-------+------+",
                "| range | v2   |",
                "+-------+------+",
                "| range | a22  |",
                "| range | b222 |",
                "| range | c222 |",
                "| range | d2   |",
                "+-------+------+",
            ],
        )
        .await
    }

    async fn test_merge_same_column_with_timestamp_type_i32_time() -> Result<()> {
        let dt1 = NaiveDate::from_ymd_opt(1000, 6, 14)
            .unwrap()
            .and_hms_micro_opt(8, 28, 53, 123456)
            .unwrap();
        let dt2 = NaiveDate::from_ymd_opt(1582, 6, 15)
            .unwrap()
            .and_hms_micro_opt(8, 28, 53, 123456)
            .unwrap();
        let dt3 = NaiveDate::from_ymd_opt(1900, 6, 16)
            .unwrap()
            .and_hms_micro_opt(8, 28, 53, 123456)
            .unwrap();
        let dt4 = NaiveDate::from_ymd_opt(2018, 6, 17)
            .unwrap()
            .and_hms_micro_opt(8, 28, 53, 123456)
            .unwrap();

        let val1 = dt1.and_utc().timestamp_micros();
        let val2 = dt2.and_utc().timestamp_micros();
        let val3 = dt3.and_utc().timestamp_micros();
        let val4 = dt4.and_utc().timestamp_micros();

        let table_name = "test_merge_same_column_with_timestamp_type_i64_time";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32_and_timestamp(
                vec!["range", "hash", "value", "timestamp"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
                vec![val1, val2, val3, val4],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "timestamp"]
                    .into_iter()
                    .map(|name| {
                        if name == "timestamp" {
                            Field::new(
                                name,
                                DataType::Timestamp(TimeUnit::Microsecond, None),
                                true,
                            )
                        } else {
                            Field::new(name, DataType::Int32, true)
                        }
                    })
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]],
            ),
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
            ],
        )
        .await
    }

    async fn test_merge_different_columns_with_timestamp_type_i32_time() -> Result<()> {
        let dt1 = NaiveDate::from_ymd_opt(1000, 6, 14)
            .unwrap()
            .and_hms_micro_opt(8, 28, 53, 123456)
            .unwrap();
        let _dt2 = NaiveDate::from_ymd_opt(1582, 6, 15)
            .unwrap()
            .and_hms_micro_opt(8, 28, 53, 123456)
            .unwrap();
        let dt3 = NaiveDate::from_ymd_opt(1900, 6, 16)
            .unwrap()
            .and_hms_micro_opt(8, 28, 53, 123456)
            .unwrap();
        let dt4 = NaiveDate::from_ymd_opt(2018, 6, 17)
            .unwrap()
            .and_hms_micro_opt(8, 28, 53, 123456)
            .unwrap();

        let val1 = dt1.and_utc().timestamp_micros();
        let _val2 = _dt2.and_utc().timestamp_micros();
        let val3 = dt3.and_utc().timestamp_micros();
        let val4 = dt4.and_utc().timestamp_micros();

        let table_name = "merge_different_columns_with_timestamp_type_i32_time";
        let client = Arc::new(MetaDataClient::from_env().await?);

        init_table(
            create_batch_i32(
                vec!["range", "hash", "value"],
                vec![
                    &[20201101, 20201101, 20201101, 20201102],
                    &[1, 2, 3, 4],
                    &[1, 2, 3, 4],
                ],
            ),
            table_name,
            SchemaRef::new(Schema::new(
                ["range", "hash", "value", "name", "timestamp"]
                    .into_iter()
                    .map(|name| {
                        if name == "timestamp" {
                            Field::new(
                                name,
                                DataType::Timestamp(TimeUnit::Microsecond, None),
                                true,
                            )
                        } else {
                            Field::new(name, DataType::Int32, true)
                        }
                    })
                    .collect::<Vec<Field>>(),
            )),
            vec!["hash".to_string()],
            vec!["range".to_string()],
            client.clone(),
        )
        .await?;

        check_upsert(
            create_batch_i32_and_timestamp(
                vec!["range", "hash", "name", "timestamp"],
                vec![&[20201101, 20201101, 20201101], &[1, 3, 4], &[11, 33, 44]],
                vec![val1, val3, val4],
            ),
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
            ],
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partial_upsert_enforces_stored_not_null_without_materializing() -> Result<()>
    {
        use crate::{cli::CoreArgs, create_lakesoul_session_ctx};
        use lakesoul_io::file_format::PhysicalFormat;

        let client = Arc::new(MetaDataClient::from_env().await?);
        for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
            let table_name =
                format!("write_constraints_{}", uuid::Uuid::new_v4().simple());
            let table_schema = Arc::new(Schema::new(vec![
                Field::new("part", DataType::Int32, true),
                Field::new("id", DataType::Int32, false),
                Field::new("required", DataType::Int32, false),
                Field::new("v", DataType::Int32, true),
                Field::new("w", DataType::Int32, true),
            ]));
            create_table_with_file_format(
                client.clone(),
                &table_name,
                LakeSoulIOConfigBuilder::new()
                    .with_schema(table_schema)
                    .with_primary_keys(vec!["id".into()])
                    .with_range_partitions(vec!["part".into()])
                    .build(),
                format,
            )
            .await?;
            let table = LakeSoulTable::for_name(&table_name).await?;
            let ctx = create_lakesoul_session_ctx(client.clone(), &CoreArgs::default())?;
            // Nullable source fields with valid values satisfy stored constraints.
            table
                .execute_upsert(create_batch_i32(
                    vec!["part", "id", "required", "v", "w"],
                    vec![&[1], &[1], &[100], &[10], &[20]],
                ))
                .await?;
            // Source order differs from metadata order; omitted v must stay absent.
            table
                .execute_upsert(create_batch_i32(
                    vec!["required", "id", "part", "w"],
                    vec![&[200], &[1], &[1], &[21]],
                ))
                .await?;
            let rows = ctx
                .sql(&format!("SELECT * FROM {table_name}"))
                .await?
                .collect()
                .await?;
            assert_batches_eq(
                &table_name,
                &[
                    "+------+----+----------+----+----+",
                    "| part | id | required | v  | w  |",
                    "+------+----+----------+----+----+",
                    "| 1    | 1  | 200      | 10 | 21 |",
                    "+------+----+----------+----+----+",
                ],
                &rows,
            );

            // Explicit NULL clears v, unlike materialized/coalescing upsert.
            let updates = create_batch_optional_i32(
                vec!["part", "id", "required", "v"],
                vec![
                    &[Some(1), Some(1)],
                    &[Some(1), Some(2)],
                    &[Some(300), Some(400)],
                    &[None, Some(40)],
                ],
            );
            table.upsert_dataframe(ctx.read_batch(updates)?).await?;

            for id in [1, 3] {
                let missing =
                    create_batch_i32(vec!["part", "id", "v"], vec![&[1], &[id], &[999]]);
                assert!(table.execute_upsert(missing).await.is_err());
                let null_required = create_batch_optional_i32(
                    vec!["part", "id", "required"],
                    vec![&[Some(1)], &[Some(id)], &[None]],
                );
                assert!(table.execute_upsert(null_required).await.is_err());
            }
            for names in [vec!["id", "required"], vec!["part", "required"]] {
                assert!(
                    table
                        .execute_upsert(create_batch_i32(names, vec![&[1], &[999]],))
                        .await
                        .is_err()
                );
            }
            let null_key = create_batch_optional_i32(
                vec!["part", "id", "required"],
                vec![&[Some(1)], &[None], &[Some(999)]],
            );
            assert!(table.execute_upsert(null_key).await.is_err());
            let wrong_type = create_batch_str_or_i32(
                vec!["part", "id", "required"],
                vec![
                    &[StrOrI32::V2(1)],
                    &[StrOrI32::V2(3)],
                    &[StrOrI32::V1("not an integer")],
                ],
            );
            assert!(
                table
                    .upsert_dataframe(ctx.read_batch(wrong_type)?)
                    .await
                    .is_err()
            );

            let rows = ctx
                .sql(&format!("SELECT * FROM {table_name} ORDER BY id"))
                .await?
                .collect()
                .await?;
            assert_batches_eq(
                &table_name,
                &[
                    "+------+----+----------+----+----+",
                    "| part | id | required | v  | w  |",
                    "+------+----+----------+----+----+",
                    "| 1    | 1  | 300      |    | 21 |",
                    "| 1    | 2  | 400      | 40 |    |",
                    "+------+----+----------+----+----+",
                ],
                &rows,
            );
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_upsert_rejects_nested_vector_and_decimal_parameter_changes()
    -> Result<()> {
        use crate::{cli::CoreArgs, create_lakesoul_session_ctx};
        use arrow::array::{Array, Decimal128Array, FixedSizeListArray, StructArray};
        use arrow::datatypes::Float32Type;
        use lakesoul_io::file_format::PhysicalFormat;

        let batch = |id: i32, size: i32, precision: u8, scale: i8, amount: i128| {
            let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                [Some(vec![Some(1.0); size as usize])],
                size,
            );
            let amounts = Decimal128Array::from(vec![amount])
                .with_precision_and_scale(precision, scale)
                .unwrap();
            let payload = StructArray::from(vec![
                (
                    Arc::new(Field::new("vec", vectors.data_type().clone(), true)),
                    Arc::new(vectors) as ArrayRef,
                ),
                (
                    Arc::new(Field::new("amount", amounts.data_type().clone(), true)),
                    Arc::new(amounts) as ArrayRef,
                ),
            ]);
            RecordBatch::try_from_iter(vec![
                ("id", Arc::new(Int32Array::from(vec![id])) as ArrayRef),
                ("payload", Arc::new(payload) as ArrayRef),
            ])
            .unwrap()
        };
        let client = Arc::new(MetaDataClient::from_env().await?);
        for format in [PhysicalFormat::Parquet, PhysicalFormat::VortexCompact] {
            let name = format!("write_types_{}", uuid::Uuid::new_v4().simple());
            let seed = batch(1, 8, 10, 2, 123);
            create_table_with_file_format(
                client.clone(),
                &name,
                LakeSoulIOConfigBuilder::new()
                    .with_schema(seed.schema())
                    .with_primary_keys(vec!["id".into()])
                    .build(),
                format,
            )
            .await?;
            let table = LakeSoulTable::for_name(&name).await?;
            table.execute_upsert(seed).await?;
            let ctx = create_lakesoul_session_ctx(client.clone(), &CoreArgs::default())?;
            for invalid in [
                batch(2, 4, 10, 2, 123),
                batch(2, 8, 38, 2, 999_999_999_999),
                batch(2, 8, 10, 3, 123),
            ] {
                assert!(
                    table
                        .upsert_dataframe(ctx.read_batch(invalid)?)
                        .await
                        .is_err()
                );
            }
            let rows = ctx
                .sql(&format!(
                    "SELECT id, payload['vec'] AS vec, payload['amount'] AS amount \
                     FROM {name}"
                ))
                .await?
                .collect()
                .await?;
            assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            let row = rows.iter().find(|batch| batch.num_rows() != 0).unwrap();
            assert_eq!(
                row.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                1
            );
            let vectors = row
                .column(1)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            assert_eq!(vectors.value_length(), 8);
            let amounts = row
                .column(2)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            assert_eq!(amounts.precision(), 10);
            assert_eq!(amounts.scale(), 2);
            assert_eq!(amounts.value(0), 123);
        }
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_all_cases() -> Result<()> {
        test_merge_same_column_i32().await?;
        test_merge_different_column_i32().await?;
        test_merge_different_columns_and_filter_by_non_selected_columns_i32().await?;
        test_merge_different_columns_and_filter_partial_rows_i32().await?;
        test_merge_one_file_with_empty_batch_i32().await?;
        test_merge_multi_files_with_empty_batch_i32().await?;
        test_upsert_without_range_partitions_i32().await?;
        test_upsert_with_multiple_range_and_hash_partitions_i32().await?;
        test_filter_requested_columns_upsert_1_times_i32().await?;
        test_filter_requested_columns_upsert_2_times_i32().await?;
        test_filter_requested_columns_upsert_3_times_i32().await?;
        test_select_requested_columns_without_hash_columns_upsert_1_times_i32().await?;
        test_select_requested_columns_without_hash_columns_upsert_2_times_i32().await?;
        test_derange_hash_key_and_data_schema_order_int_type_upsert_1_times_i32().await?;
        test_derange_hash_key_and_data_schema_order_int_type_upsert_2_times_i32().await?;
        test_derange_hash_key_and_data_schema_order_int_type_upsert_3_times_i32().await?;
        test_derange_hash_key_and_data_schema_order_string_type_upsert_1_times_i32()
            .await?;
        test_derange_hash_key_and_data_schema_order_string_type_upsert_2_times_i32()
            .await?;
        test_derange_hash_key_and_data_schema_order_string_type_upsert_3_times_i32()
            .await?;
        test_create_table_with_hash_key_disordered().await?;
        test_merge_same_column_with_timestamp_type_i32_time().await?;
        test_merge_different_columns_with_timestamp_type_i32_time().await?;
        Ok(())
    }
}
