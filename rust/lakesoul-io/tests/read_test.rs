use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow_cast::pretty::print_batches;
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use datafusion_expr::{col, lit};
use lakesoul_io::{
    config::{LakeSoulIOConfig, OPTION_KEY_FILE_FILTER_PUSHDOWN},
    reader::LakeSoulReader,
    utils::{gen_random_batch, lakesoul_file_name, random_str},
    writer::create_writer_with_io_config,
};
use tempfile::env::temp_dir;
use tempfile::tempdir;

#[test_log::test(tokio::test)]
async fn test_read_for_one_partition() {
    // write
    let dir = temp_dir();
    let writer_id = random_str(16);
    let file_name = lakesoul_file_name(&writer_id, 0);
    let mut schema_builder = SchemaBuilder::new();
    schema_builder.push(Field::new("hash", DataType::Utf8, true));
    schema_builder.push(Field::new("op", DataType::Utf8, true));

    let schema = SchemaRef::new(schema_builder.finish());
    let mut conf_builder = LakeSoulIOConfig::builder()
        .with_file(
            dir.join("range=range1")
                .join(file_name)
                .into_os_string()
                .into_string()
                .unwrap(),
        )
        .with_primary_key("hash")
        .with_hash_bucket_num("1")
        .with_schema(schema.clone())
        .with_object_store_option("fs.s3a.path.style.access", "false")
        .with_object_store_option("fs.defaultFS", "file:///");
    let mut writer = create_writer_with_io_config(conf_builder.clone().build())
        .await
        .unwrap();
    writer
        .write_record_batch(gen_random_batch(schema.clone(), 3, 0.0))
        .await
        .unwrap();
    writer.flush_and_close().await.unwrap();
    // reader
    // add partition
    let mut schema_builder = SchemaBuilder::from(schema.as_ref());
    schema_builder.push(Field::new("range", DataType::Utf8, true));
    conf_builder = conf_builder
        .with_schema(Arc::new(schema_builder.finish()))
        .with_option("is_compacted", "false")
        .with_option("skip_merge_on_read", "false")
        .with_default_column_value("range", "range1");

    let mut reader = LakeSoulReader::new(conf_builder.build()).unwrap();

    reader.start().await.unwrap();

    let mut batches = vec![];

    while let Some(Ok(batch)) = reader.next_rb().await {
        batches.push(batch);
    }

    print_batches(&batches).unwrap();
}

#[test_log::test(tokio::test)]
async fn test_read_vortex_file_sink() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("test.vortex")
        .into_os_string()
        .into_string()
        .unwrap();
    let batch = int64_batch("id", [3, 2, 1]);

    write_batch(path.clone(), batch.clone()).await;

    let batches = read_batches(vec![path], batch.schema(), vec![]).await;
    let mut values = int64_values(&batches, "id");
    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3]);
}

#[test_log::test(tokio::test)]
async fn test_read_mixed_parquet_and_vortex_with_filter() {
    let dir = tempdir().unwrap();
    let parquet_path = dir
        .path()
        .join("part-0.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let vortex_path = dir
        .path()
        .join("part-1.vortex")
        .into_os_string()
        .into_string()
        .unwrap();

    let parquet_batch = int64_batch("id", [1, 2]);
    let vortex_batch = int64_batch("id", [3, 4]);

    write_batch(parquet_path.clone(), parquet_batch.clone()).await;
    write_batch(vortex_path.clone(), vortex_batch.clone()).await;

    let batches = read_batches(
        vec![parquet_path, vortex_path],
        parquet_batch.schema(),
        vec![col("id").gt(lit(2_i64))],
    )
    .await;
    let mut values = int64_values(&batches, "id");
    values.sort_unstable();
    assert_eq!(values, vec![3, 4]);
}

#[test_log::test(tokio::test)]
async fn test_read_mixed_parquet_and_vortex_with_primary_key_merge() {
    let dir = tempdir().unwrap();
    let parquet_path = dir
        .path()
        .join("pk-0.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let vortex_path = dir
        .path()
        .join("pk-1.vortex")
        .into_os_string()
        .into_string()
        .unwrap();

    let parquet_batch = int64_batch("id", [1, 3]);
    let vortex_batch = int64_batch("id", [2, 4]);
    let primary_keys = vec!["id".to_string()];

    write_batch_with_primary_keys(
        parquet_path.clone(),
        parquet_batch.clone(),
        primary_keys.clone(),
    )
    .await;
    write_batch_with_primary_keys(
        vortex_path.clone(),
        vortex_batch.clone(),
        primary_keys.clone(),
    )
    .await;

    let batches = read_batches_with_primary_keys(
        vec![parquet_path, vortex_path],
        parquet_batch.schema(),
        vec![],
        primary_keys,
    )
    .await;
    assert_eq!(int64_values(&batches, "id"), vec![1, 2, 3, 4]);
}

#[test_log::test(tokio::test)]
async fn test_read_filter_on_non_projected_column_preserves_projection() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("projected-filter.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let batch = two_int64_batch("id", [1, 2, 3], "score", [5, 15, 25]);
    let target_schema =
        SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    write_batch(path.clone(), batch).await;

    let batches = read_batches(
        vec![path],
        target_schema,                      // id
        vec![col("score").gt(lit(10_i64))], // score > 10
    )
    .await;

    assert_single_column_schema(&batches, "id");
    assert_eq!(int64_values(&batches, "id"), vec![2, 3]);
}

#[test_log::test(tokio::test)]
async fn test_read_parquet_pushdown_preserves_projection() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("projected-filter-pushdown.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let batch = two_int64_batch("id", [1, 2, 3], "score", [5, 15, 25]);
    let target_schema =
        SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    write_batch(path.clone(), batch).await;

    let batches = read_batches_with_options(
        vec![path],
        target_schema,
        vec![col("score").gt(lit(10_i64))],
        vec![],
        true,
    )
    .await;

    assert_single_column_schema(&batches, "id");
    assert_eq!(int64_values(&batches, "id"), vec![2, 3]);
}

#[test_log::test(tokio::test)]
async fn test_read_vortex_filter_pushdown_on_non_projected_column_preserves_projection() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("projected-filter.vortex")
        .into_os_string()
        .into_string()
        .unwrap();
    let batch = two_int64_batch("id", [1, 2, 3], "score", [5, 15, 25]);
    let target_schema =
        SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    write_batch(path.clone(), batch).await;

    let batches = read_batches_with_options(
        vec![path],
        target_schema,
        vec![col("score").gt(lit(10_i64))],
        vec![],
        true,
    )
    .await;

    assert_single_column_schema(&batches, "id");
    assert_eq!(int64_values(&batches, "id"), vec![2, 3]);
}

fn int64_batch<const N: usize>(name: &str, values: [i64; N]) -> RecordBatch {
    RecordBatch::try_from_iter([(
        name,
        Arc::new(Int64Array::from_iter_values(values)) as ArrayRef,
    )])
    .unwrap()
}

fn two_int64_batch<const N: usize>(
    left_name: &str,
    left_values: [i64; N],
    right_name: &str,
    right_values: [i64; N],
) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            left_name,
            Arc::new(Int64Array::from_iter_values(left_values)) as ArrayRef,
        ),
        (
            right_name,
            Arc::new(Int64Array::from_iter_values(right_values)) as ArrayRef,
        ),
    ])
    .unwrap()
}

async fn write_batch(path: String, batch: RecordBatch) {
    write_batch_with_primary_keys(path, batch, vec![]).await;
}

async fn write_batch_with_primary_keys(
    path: String,
    batch: RecordBatch,
    primary_keys: Vec<String>,
) {
    let writer_conf = LakeSoulIOConfig::builder()
        .with_file(path)
        .with_thread_num(2)
        .with_batch_size(2)
        .with_schema(batch.schema())
        .with_primary_keys(primary_keys)
        .build();
    let mut writer = create_writer_with_io_config(writer_conf).await.unwrap();
    writer.write_record_batch(batch).await.unwrap();
    writer.flush_and_close().await.unwrap();
}

async fn read_batches(
    paths: Vec<String>,
    schema: SchemaRef,
    filters: Vec<datafusion_expr::Expr>,
) -> Vec<RecordBatch> {
    read_batches_with_primary_keys(paths, schema, filters, vec![]).await
}

async fn read_batches_with_primary_keys(
    paths: Vec<String>,
    schema: SchemaRef,
    filters: Vec<datafusion_expr::Expr>,
    primary_keys: Vec<String>,
) -> Vec<RecordBatch> {
    read_batches_with_options(paths, schema, filters, primary_keys, false).await
}

async fn read_batches_with_options(
    paths: Vec<String>,
    schema: SchemaRef,
    filters: Vec<datafusion_expr::Expr>,
    primary_keys: Vec<String>,
    file_filter_pushdown: bool,
) -> Vec<RecordBatch> {
    let mut reader_conf = LakeSoulIOConfig::builder()
        .with_files(paths)
        .with_thread_num(2)
        .with_batch_size(2)
        .with_schema(schema)
        .with_primary_keys(primary_keys)
        .with_filters(filters);
    if file_filter_pushdown {
        reader_conf = reader_conf.with_option(OPTION_KEY_FILE_FILTER_PUSHDOWN, "true");
    }
    let reader_conf = reader_conf.build();
    let mut reader = LakeSoulReader::new(reader_conf).unwrap();
    reader.start().await.unwrap();

    let mut batches = vec![];
    while let Some(batch) = reader.next_rb().await {
        batches.push(batch.unwrap());
    }
    batches
}

fn assert_single_column_schema(batches: &[RecordBatch], column: &str) {
    assert!(!batches.is_empty());
    for batch in batches {
        assert_eq!(batch.schema().fields().len(), 1);
        assert_eq!(batch.schema().field(0).name(), column);
    }
}

fn int64_values(batches: &[RecordBatch], column: &str) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|batch| {
            let index = batch.schema().index_of(column).unwrap();
            let array = batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..array.len()).map(|row| array.value(row))
        })
        .collect()
}

// TODO(jiax)
