// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::assert_batches_eq;
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::writer::async_writer::{
    AsyncBatchWriter, AsyncSendableMutableLakeSoulWriter,
};
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

use crate::Result;
use crate::catalog::{create_io_config_builder, create_table};
use crate::cli::CoreArgs;
use crate::create_lakesoul_session_ctx;
use crate::lakesoul_table::LakeSoulTable;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
    ]))
}

fn batch(ids: &[i32], scores: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(scores.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn unique_table_name(prefix: &str) -> String {
    format!("{}_{}", prefix, uuid::Uuid::new_v4().simple())
}

async fn write_batch_to_table(
    client: MetaDataClientRef,
    table: &LakeSoulTable,
    physical_format: PhysicalFormat,
    batch: RecordBatch,
) -> Result<()> {
    let io_config = create_io_config_builder(
        client,
        Some(table.table_name()),
        false,
        table.table_namespace(),
        Default::default(),
        Default::default(),
    )
    .await?
    .with_physical_format(physical_format)
    .build();

    let mut writer =
        AsyncSendableMutableLakeSoulWriter::from_io_config(io_config).await?;
    writer.write_record_batch(batch).await?;
    let outputs = Box::new(writer).flush_and_close().await?;
    table.commit_flush_result(outputs).await
}

async fn create_table_with_batches(
    client: MetaDataClientRef,
    table_name: &str,
    batches: Vec<(PhysicalFormat, RecordBatch)>,
) -> Result<()> {
    let io_config = LakeSoulIOConfigBuilder::new()
        .with_schema(test_schema())
        .build();
    create_table(client.clone(), table_name, io_config).await?;

    let table = LakeSoulTable::for_name(table_name).await?;
    for (physical_format, batch) in batches {
        write_batch_to_table(client.clone(), &table, physical_format, batch).await?;
    }

    Ok(())
}

async fn collect_sql(table_name: &str, sql: &str) -> Result<Vec<RecordBatch>> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let ctx = create_lakesoul_session_ctx(client, &CoreArgs::default())?;
    Ok(ctx
        .sql(sql.replace("{table}", table_name).as_str())
        .await?
        .collect()
        .await?)
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_select_reads_vortex_table() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table_name = unique_table_name("catalog_vortex_select");
    create_table_with_batches(
        client,
        &table_name,
        vec![(PhysicalFormat::Vortex, batch(&[3, 1, 2], &[30, 10, 20]))],
    )
    .await?;

    let results =
        collect_sql(&table_name, "SELECT id, score FROM {table} ORDER BY id").await?;

    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | score |",
            "+----+-------+",
            "| 1  | 10    |",
            "| 2  | 20    |",
            "| 3  | 30    |",
            "+----+-------+",
        ],
        &results
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_select_reads_mixed_parquet_and_vortex_table() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table_name = unique_table_name("catalog_mixed_select");
    create_table_with_batches(
        client,
        &table_name,
        vec![
            (PhysicalFormat::Parquet, batch(&[2, 1], &[20, 10])),
            (PhysicalFormat::Vortex, batch(&[4, 3], &[40, 30])),
        ],
    )
    .await?;

    let results =
        collect_sql(&table_name, "SELECT id, score FROM {table} ORDER BY id").await?;

    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | score |",
            "+----+-------+",
            "| 1  | 10    |",
            "| 2  | 20    |",
            "| 3  | 30    |",
            "| 4  | 40    |",
            "+----+-------+",
        ],
        &results
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_vortex_filter_can_reference_non_projected_column() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table_name = unique_table_name("catalog_vortex_filter_projection");
    create_table_with_batches(
        client,
        &table_name,
        vec![(PhysicalFormat::Vortex, batch(&[1, 2, 3], &[5, 15, 25]))],
    )
    .await?;

    let results = collect_sql(
        &table_name,
        "SELECT id FROM {table} WHERE score > 10 ORDER BY id",
    )
    .await?;

    #[rustfmt::skip]
    assert_batches_eq!(
      &["+----+",
        "| id |",
        "+----+",
        "| 2  |",
        "| 3  |",
        "+----+",],
        &results
    );
    Ok(())
}
