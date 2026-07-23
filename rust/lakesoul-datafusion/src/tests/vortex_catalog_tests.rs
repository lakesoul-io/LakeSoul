// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::assert_batches_eq;
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::collect;
use lakesoul_io::config::{
    LakeSoulIOConfigBuilder, OPTION_KEY_CDC_COLUMN, OPTION_KEY_STABLE_SORT,
};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::writer::async_writer::{
    AsyncBatchWriter, AsyncSendableMutableLakeSoulWriter,
};
use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};
use proto::proto::entity::TableInfo;

use crate::Result;
use crate::catalog::{
    LakeSoulTableProperty, create_io_config_builder, create_table,
    format_table_info_partitions,
};
use crate::cli::CoreArgs;
use crate::create_lakesoul_session_ctx;
use crate::lakesoul_table::LakeSoulTable;
use crate::ser::arrow_java::schema_to_metadata_parts;

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

fn cdc_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("score", DataType::Int32, false),
        Field::new("op", DataType::Utf8, true),
    ]))
}

fn cdc_batch(ids: &[i32], scores: &[i32], ops: &[&str]) -> RecordBatch {
    RecordBatch::try_new(
        cdc_schema(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(scores.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(ops.to_vec())) as ArrayRef,
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

async fn create_primary_key_table_with_batches(
    client: MetaDataClientRef,
    table_name: &str,
    batches: Vec<(PhysicalFormat, RecordBatch)>,
) -> Result<()> {
    let io_config = LakeSoulIOConfigBuilder::new()
        .with_schema(test_schema())
        .with_primary_keys(vec![String::from("id")])
        .build();
    create_table(client.clone(), table_name, io_config).await?;

    let table = LakeSoulTable::for_name(table_name).await?;
    for (physical_format, batch) in batches {
        write_batch_to_table(client.clone(), &table, physical_format, batch).await?;
    }

    Ok(())
}

async fn create_cdc_table(client: MetaDataClientRef, table_name: &str) -> Result<()> {
    let primary_keys = vec!["id".to_string()];
    let io_config = LakeSoulIOConfigBuilder::new()
        .with_schema(cdc_schema())
        .with_primary_keys(primary_keys.clone())
        .with_option(OPTION_KEY_CDC_COLUMN, "op")
        .with_option(OPTION_KEY_STABLE_SORT, "true")
        .build();

    let target_schema = io_config.target_schema();
    let (table_schema, table_schema_arrow_ipc, table_schema_arrow_ipc_json_hash) =
        schema_to_metadata_parts(target_schema.as_ref());

    client
        .create_table(TableInfo {
            table_id: format!("table_{}", uuid::Uuid::new_v4()),
            table_name: table_name.to_string(),
            table_path: format!(
                "file://{}/default/{}",
                std::env::current_dir()?.to_string_lossy(),
                table_name
            ),
            table_schema,
            table_schema_arrow_ipc,
            table_schema_arrow_ipc_json_hash,
            table_namespace: "default".to_string(),
            properties: serde_json::to_string(&LakeSoulTableProperty {
                hash_bucket_num: Some(String::from("4")),
                cdc_change_column: Some(String::from("op")),
                use_cdc: Some(String::from("true")),
                ..Default::default()
            })?,
            partitions: format_table_info_partitions(&[], &primary_keys),
            domain: "public".to_string(),
        })
        .await?;

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
async fn catalog_mixed_format_scan_preserves_commit_order_for_use_last() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table_name = unique_table_name("catalog_mixed_order");
    create_primary_key_table_with_batches(
        client,
        &table_name,
        vec![
            (PhysicalFormat::Parquet, batch(&[1], &[10])),
            (PhysicalFormat::Vortex, batch(&[1], &[20])),
            (PhysicalFormat::Parquet, batch(&[1], &[30])),
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
            "| 1  | 30    |",
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

#[tokio::test(flavor = "multi_thread")]
async fn catalog_scan_accepts_filter_column_outside_projection() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table_name = unique_table_name("catalog_filter_only_column");
    create_table_with_batches(
        client.clone(),
        &table_name,
        vec![
            (PhysicalFormat::Parquet, batch(&[1, 2], &[5, 15])),
            (PhysicalFormat::Vortex, batch(&[3, 4], &[25, 35])),
        ],
    )
    .await?;

    let ctx = create_lakesoul_session_ctx(client, &CoreArgs::default())?;
    let provider = ctx.table_provider(table_name).await?;
    let projection = vec![0];
    let filters = vec![col("score").gt(lit(10_i32))];
    let plan = provider
        .scan(&ctx.state(), Some(&projection), &filters, None)
        .await?;
    let results = collect(plan, ctx.task_ctx()).await?;

    #[rustfmt::skip]
    assert_batches_eq!(
      &[ "+----+",
         "| id |",
         "+----+",
         "| 2  |",
         "| 3  |",
         "| 4  |",
         "+----+",],
        &results
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_cdc_scan_filters_delete_tombstones_before_projection() -> Result<()> {
    let client = Arc::new(MetaDataClient::from_env().await?);
    let table_name = unique_table_name("catalog_cdc_delete_filter");
    create_cdc_table(client.clone(), &table_name).await?;

    let table = LakeSoulTable::for_name(&table_name).await?;
    write_batch_to_table(
        client.clone(),
        &table,
        PhysicalFormat::Parquet,
        cdc_batch(&[1, 2], &[10, 30], &["insert", "insert"]),
    )
    .await?;
    write_batch_to_table(
        client,
        &table,
        PhysicalFormat::Parquet,
        cdc_batch(&[1], &[20], &["delete"]),
    )
    .await?;

    let results =
        collect_sql(&table_name, "SELECT id, score FROM {table} ORDER BY id").await?;

    assert_batches_eq!(
        &[
            "+----+-------+",
            "| id | score |",
            "+----+-------+",
            "| 2  | 30    |",
            "+----+-------+",
        ],
        &results
    );
    Ok(())
}
