use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow_cast::pretty::print_batches;
use arrow_schema::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use datafusion_expr::{col, lit};
use lakesoul_io::{
    config::{
        LakeSoulIOConfig, OPTION_KEY_FILE_FILTER_PUSHDOWN, OPTION_KEY_IS_COMPACTED,
        OPTION_KEY_SKIP_MERGE_ON_READ,
    },
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

// --- Error path tests ---

#[test_log::test(tokio::test)]
async fn test_reader_empty_files_error() {
    let schema =
        SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let conf = LakeSoulIOConfig::builder()
        .with_files(Vec::<String>::new())
        .with_schema(schema)
        .build();
    let result = LakeSoulReader::new(conf);
    assert!(result.is_err());
}

// --- Hash bucket skip optimization tests ---

#[test_log::test(tokio::test)]
async fn test_hash_bucket_skip_optimization_skips_when_no_match() {
    let dir = tempdir().unwrap();
    // file name pattern "part-*_0001.parquet" → hash_bucket_id = 1
    let file_name = lakesoul_file_name("test", 1);
    let path = dir
        .path()
        .join(file_name)
        .into_os_string()
        .into_string()
        .unwrap();
    let schema =
        SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = int64_batch("id", [10, 20]);

    // write file
    let writer_conf = LakeSoulIOConfig::builder()
        .with_file(path.clone())
        .with_schema(schema.clone())
        .with_primary_key("id")
        .with_hash_bucket_num("1")
        .build();
    let mut writer = create_writer_with_io_config(writer_conf).await.unwrap();
    writer.write_record_batch(batch).await.unwrap();
    writer.flush_and_close().await.unwrap();

    // read with OR-conjunctive filter on PK, hash_bucket_num=1, skip_merge_on_read
    // all values hash to `any % 1 = 0`, but file has hash_bucket_id=1
    // since {0} doesn't contain 1, reader should skip → empty result
    let reader_conf = LakeSoulIOConfig::builder()
        .with_file(path)
        .with_schema(schema)
        .with_primary_key("id")
        .with_hash_bucket_num("1")
        .with_filters(vec![
            col("id").eq(lit(10_i64)).or(col("id").eq(lit(20_i64))),
        ])
        .with_option(OPTION_KEY_SKIP_MERGE_ON_READ, "true")
        .build();
    let mut reader = LakeSoulReader::new(reader_conf).unwrap();
    reader.start().await.unwrap();

    let mut row_count = 0;
    while let Some(Ok(batch)) = reader.next_rb().await {
        row_count += batch.num_rows();
    }
    assert_eq!(
        row_count, 0,
        "reader should skip when hash bucket doesn't match"
    );
}

#[test_log::test(tokio::test)]
async fn test_hash_bucket_skip_optimization_reads_when_match() {
    let dir = tempdir().unwrap();
    // file name pattern "part-*_0000.parquet" → hash_bucket_id = 0
    let file_name = lakesoul_file_name("test", 0);
    let path = dir
        .path()
        .join(file_name)
        .into_os_string()
        .into_string()
        .unwrap();
    let schema =
        SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = int64_batch("id", [10, 20]);

    // write file
    let writer_conf = LakeSoulIOConfig::builder()
        .with_file(path.clone())
        .with_schema(schema.clone())
        .with_primary_key("id")
        .with_hash_bucket_num("1")
        .build();
    let mut writer = create_writer_with_io_config(writer_conf).await.unwrap();
    writer.write_record_batch(batch).await.unwrap();
    writer.flush_and_close().await.unwrap();

    // all values hash to `any % 1 = 0`, file has hash_bucket_id=0
    // {0} contains 0, so reader should NOT skip → data returned
    let reader_conf = LakeSoulIOConfig::builder()
        .with_file(path)
        .with_schema(schema)
        .with_primary_key("id")
        .with_hash_bucket_num("1")
        .with_filters(vec![
            col("id").eq(lit(10_i64)).or(col("id").eq(lit(20_i64))),
        ])
        .with_option(OPTION_KEY_SKIP_MERGE_ON_READ, "true")
        .build();
    let mut reader = LakeSoulReader::new(reader_conf).unwrap();
    reader.start().await.unwrap();

    let mut values = vec![];
    while let Some(Ok(batch)) = reader.next_rb().await {
        values.extend(int64_values(&[batch], "id"));
    }
    values.sort_unstable();
    assert_eq!(values, vec![10, 20]);
}

// --- Filter with zero results ---

#[test_log::test(tokio::test)]
async fn test_filter_zero_results_preserves_schema() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("zero-result.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let batch = int64_batch("id", [1, 2, 3]);
    let schema = batch.schema();

    write_batch(path.clone(), batch).await;

    // filter matches nothing
    let batches =
        read_batches(vec![path], schema.clone(), vec![col("id").gt(lit(100_i64))]).await;

    // when filter matches nothing, reader may return 0 batches or empty batches
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "zero rows should match");
    // verify schema is preserved in each batch
    for batch in &batches {
        assert_eq!(batch.schema().field(0).name(), "id");
    }
}

// --- Multi-file merge-on-read ---

#[test_log::test(tokio::test)]
async fn test_multi_file_merge_on_read_deduplication() {
    let dir = tempdir().unwrap();
    let p1_path = dir
        .path()
        .join("merge-1.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let p2_path = dir
        .path()
        .join("merge-2.parquet")
        .into_os_string()
        .into_string()
        .unwrap();

    // File 1: id=[1, 2, 3], score=[100, 200, 300]
    // File 2: id=[2, 3, 4], score=[250, 350, 450]
    let batch1 = two_int64_batch("id", [1, 2, 3], "score", [100, 200, 300]);
    let batch2 = two_int64_batch("id", [2, 3, 4], "score", [250, 350, 450]);
    let primary_keys = vec!["id".to_string()];

    let schema = batch1.schema();
    write_batch_with_primary_keys(p1_path.clone(), batch1, primary_keys.clone()).await;
    write_batch_with_primary_keys(p2_path.clone(), batch2, primary_keys.clone()).await;

    let batches = read_batches_with_primary_keys(
        vec![p1_path, p2_path],
        schema,
        vec![],
        primary_keys,
    )
    .await;

    let ids = int64_values(&batches, "id");
    assert_eq!(ids, vec![1, 2, 3, 4], "merged PKs should be 1,2,3,4");
}

// --- Compacted data read ---

#[test_log::test(tokio::test)]
async fn test_read_compacted_data() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("compacted.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let batch = two_int64_batch("id", [1, 2, 3], "score", [10, 20, 30]);

    write_batch(path.clone(), batch.clone()).await;

    // read with is_compacted=true and skip_merge_on_read=true
    let reader_conf = LakeSoulIOConfig::builder()
        .with_files(vec![path])
        .with_schema(batch.schema())
        .with_option(OPTION_KEY_IS_COMPACTED, "true")
        .with_option(OPTION_KEY_SKIP_MERGE_ON_READ, "true")
        .with_batch_size(2)
        .with_thread_num(1)
        .build();
    let mut reader = LakeSoulReader::new(reader_conf).unwrap();
    reader.start().await.unwrap();

    let mut batches = vec![];
    while let Some(batch) = reader.next_rb().await {
        batches.push(batch.unwrap());
    }
    let ids = int64_values(&batches, "id");
    assert_eq!(ids, vec![1, 2, 3]);
}

// --- Mixed column types ---

#[test_log::test(tokio::test)]
async fn test_read_mixed_column_types() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("mixed-types.parquet")
        .into_os_string()
        .into_string()
        .unwrap();

    let schema = SchemaRef::new(Schema::new(vec![
        Field::new("int_col", DataType::Int64, false),
        Field::new("float_col", DataType::Float64, false),
        Field::new("str_col", DataType::Utf8, false),
        Field::new("bool_col", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values([1_i64, 2, 3])),
            Arc::new(Float64Array::from_iter_values([1.5_f64, 2.5, 3.5])),
            Arc::new(StringArray::from_iter_values(["a", "b", "c"])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ],
    )
    .unwrap();

    let writer_conf = LakeSoulIOConfig::builder()
        .with_file(path.clone())
        .with_schema(schema.clone())
        .build();
    let mut writer = create_writer_with_io_config(writer_conf).await.unwrap();
    writer.write_record_batch(batch).await.unwrap();
    writer.flush_and_close().await.unwrap();

    let batches = read_batches(vec![path], schema.clone(), vec![]).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);
    // verify schema has all 4 columns
    let result_schema = &batches[0].schema();
    assert_eq!(result_schema.fields().len(), 4);

    // verify each column values across all batches
    let mut ints: Vec<i64> = vec![];
    let mut floats: Vec<f64> = vec![];
    let mut strs: Vec<String> = vec![];
    let mut bools: Vec<bool> = vec![];

    for batch in &batches {
        for i in 0..batch.num_rows() {
            ints.push(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i),
            );
            floats.push(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(i),
            );
            strs.push(
                batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i)
                    .to_string(),
            );
            bools.push(
                batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(i),
            );
        }
    }
    assert_eq!(ints, vec![1, 2, 3]);
    assert_eq!(floats, vec![1.5, 2.5, 3.5]);
    assert_eq!(
        strs,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(bools, vec![true, false, true]);
}

// --- Read with range partition and default column value ---

#[test_log::test(tokio::test)]
async fn test_read_multiple_range_partitions() {
    let dir = tempdir().unwrap();
    let writer_id = random_str(8);

    let schema = SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    // Write two partitions
    for (range_val, id_offset) in [("range1", 0_i64), ("range2", 100_i64)] {
        let file_name = lakesoul_file_name(&writer_id, 0);
        let file_path = dir
            .path()
            .join(format!("range={}", range_val))
            .join(file_name);
        std::fs::create_dir_all(file_path.parent().unwrap()).unwrap();

        let write_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values([1 + id_offset, 2 + id_offset])),
                Arc::new(StringArray::from_iter_values(["x", "y"])),
            ],
        )
        .unwrap();

        let writer_conf = LakeSoulIOConfig::builder()
            .with_file(file_path.into_os_string().into_string().unwrap())
            .with_schema(schema.clone())
            .with_hash_bucket_num("1")
            .with_object_store_option("fs.defaultFS", "file:///")
            .build();
        let mut writer = create_writer_with_io_config(writer_conf).await.unwrap();
        writer.write_record_batch(write_batch).await.unwrap();
        writer.flush_and_close().await.unwrap();
    }

    // Read both partitions
    let mut read_schema_builder = SchemaBuilder::from(schema.as_ref());
    read_schema_builder.push(Field::new("range", DataType::Utf8, true));

    let files = vec![
        dir.path()
            .join("range=range1")
            .join(lakesoul_file_name(&writer_id, 0))
            .into_os_string()
            .into_string()
            .unwrap(),
        dir.path()
            .join("range=range2")
            .join(lakesoul_file_name(&writer_id, 0))
            .into_os_string()
            .into_string()
            .unwrap(),
    ];

    let reader_conf = LakeSoulIOConfig::builder()
        .with_files(files)
        .with_schema(Arc::new(read_schema_builder.finish()))
        .with_range_partitions(vec!["range".to_string()])
        .with_default_column_value("range", "range1")
        .with_hash_bucket_num("1")
        .with_object_store_option("fs.defaultFS", "file:///")
        .with_option(OPTION_KEY_IS_COMPACTED, "false")
        .with_option(OPTION_KEY_SKIP_MERGE_ON_READ, "false")
        .build();

    let mut reader = LakeSoulReader::new(reader_conf).unwrap();
    reader.start().await.unwrap();

    let mut ids = vec![];
    while let Some(Ok(batch)) = reader.next_rb().await {
        ids.extend(int64_values(&[batch], "id"));
    }
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 101, 102]);
}

// --- Vortex-only tests for feature parity with Parquet ---

#[test_log::test(tokio::test)]
async fn test_vortex_multi_file_merge_on_read() {
    let dir = tempdir().unwrap();
    let v1_path = dir
        .path()
        .join("merge-1.vortex")
        .into_os_string()
        .into_string()
        .unwrap();
    let v2_path = dir
        .path()
        .join("merge-2.vortex")
        .into_os_string()
        .into_string()
        .unwrap();

    let batch1 = two_int64_batch("id", [1, 2, 3], "score", [100, 200, 300]);
    let batch2 = two_int64_batch("id", [2, 3, 4], "score", [250, 350, 450]);
    let schema = batch1.schema();
    let primary_keys = vec!["id".to_string()];

    write_batch_with_primary_keys(v1_path.clone(), batch1, primary_keys.clone()).await;
    write_batch_with_primary_keys(v2_path.clone(), batch2, primary_keys.clone()).await;

    let batches = read_batches_with_primary_keys(
        vec![v1_path, v2_path],
        schema,
        vec![],
        primary_keys,
    )
    .await;

    let ids = int64_values(&batches, "id");
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[test_log::test(tokio::test)]
async fn test_vortex_hash_bucket_skip_optimization() {
    let dir = tempdir().unwrap();
    // hash_bucket_id = 1 (from file name)
    let file_name = "part-test_0001.vortex";
    let path = dir
        .path()
        .join(file_name)
        .into_os_string()
        .into_string()
        .unwrap();
    let schema =
        SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = int64_batch("id", [10, 20]);

    let writer_conf = LakeSoulIOConfig::builder()
        .with_file(path.clone())
        .with_schema(schema.clone())
        .with_primary_key("id")
        .with_hash_bucket_num("1")
        .build();
    let mut writer = create_writer_with_io_config(writer_conf).await.unwrap();
    writer.write_record_batch(batch).await.unwrap();
    writer.flush_and_close().await.unwrap();

    // hash_bucket_num=1 → all values hash to 0; file has hash_bucket_id=1
    // {0} does not contain 1 → reader should skip → empty result
    let reader_conf = LakeSoulIOConfig::builder()
        .with_file(path)
        .with_schema(schema)
        .with_primary_key("id")
        .with_hash_bucket_num("1")
        .with_filters(vec![
            col("id").eq(lit(10_i64)).or(col("id").eq(lit(20_i64))),
        ])
        .with_option("skip_merge_on_read", "true")
        .build();
    let mut reader = LakeSoulReader::new(reader_conf).unwrap();
    reader.start().await.unwrap();

    let mut row_count = 0;
    while let Some(Ok(batch)) = reader.next_rb().await {
        row_count += batch.num_rows();
    }
    assert_eq!(row_count, 0);
}

#[test_log::test(tokio::test)]
async fn test_vortex_filter_zero_results_preserves_schema() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("zero.vortex")
        .into_os_string()
        .into_string()
        .unwrap();
    let batch = int64_batch("id", [1, 2, 3]);
    let schema = batch.schema();

    write_batch(path.clone(), batch).await;

    let batches =
        read_batches(vec![path], schema.clone(), vec![col("id").gt(lit(100_i64))]).await;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
    for batch in &batches {
        assert_eq!(batch.schema().field(0).name(), "id");
    }
}

#[test_log::test(tokio::test)]
async fn test_vortex_mixed_column_types() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("types.vortex")
        .into_os_string()
        .into_string()
        .unwrap();

    let schema = SchemaRef::new(Schema::new(vec![
        Field::new("int_col", DataType::Int64, false),
        Field::new("float_col", DataType::Float64, false),
        Field::new("str_col", DataType::Utf8, false),
        Field::new("bool_col", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values([1_i64, 2, 3])),
            Arc::new(Float64Array::from_iter_values([1.5_f64, 2.5, 3.5])),
            Arc::new(StringArray::from_iter_values(["a", "b", "c"])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
        ],
    )
    .unwrap();

    write_batch_with_primary_keys(path.clone(), batch, vec![]).await;

    let batches = read_batches(vec![path], schema.clone(), vec![]).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    let result_schema = &batches[0].schema();
    assert_eq!(result_schema.fields().len(), 4);

    let mut ints: Vec<i64> = vec![];
    let mut floats: Vec<f64> = vec![];
    let mut strs: Vec<String> = vec![];
    let mut bools: Vec<bool> = vec![];

    for batch in &batches {
        let str_col = batch.column_by_name("str_col").unwrap();
        for i in 0..batch.num_rows() {
            ints.push(
                batch
                    .column_by_name("int_col")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i),
            );
            floats.push(
                batch
                    .column_by_name("float_col")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(i),
            );
            strs.push(match str_col.data_type() {
                DataType::Utf8 => str_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i)
                    .to_string(),
                DataType::Utf8View => {
                    use arrow_array::StringViewArray;
                    str_col
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .unwrap()
                        .value(i)
                        .to_string()
                }
                _ => unreachable!(),
            });
            bools.push(
                batch
                    .column_by_name("bool_col")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(i),
            );
        }
    }
    assert_eq!(ints, vec![1, 2, 3]);
    assert_eq!(floats, vec![1.5, 2.5, 3.5]);
    assert_eq!(
        strs,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(bools, vec![true, false, true]);
}

#[test_log::test(tokio::test)]
async fn test_vortex_with_primary_key_sort() {
    let dir = tempdir().unwrap();
    let path = dir
        .path()
        .join("pk-sort.vortex")
        .into_os_string()
        .into_string()
        .unwrap();
    let schema = SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values([3_i64, 1, 2])),
            Arc::new(StringArray::from_iter_values(["c", "a", "b"])),
        ],
    )
    .unwrap();

    write_batch_with_primary_keys(path.clone(), batch, vec!["id".to_string()]).await;

    let batches = read_batches_with_primary_keys(
        vec![path],
        schema.clone(),
        vec![],
        vec!["id".to_string()],
    )
    .await;

    let ids = int64_values(&batches, "id");
    assert_eq!(ids, vec![1, 2, 3]);
}

// --- Mixed Parquet+Vortex tests ---

#[test_log::test(tokio::test)]
async fn test_mixed_hash_bucket_skip_optimization() {
    let dir = tempdir().unwrap();
    // Both files have hash_bucket_id=0 (from file name suffix _0000)
    let parquet_path = dir
        .path()
        .join("part-mixed_0000.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let vortex_path = dir
        .path()
        .join("part-mixed_0000.vortex")
        .into_os_string()
        .into_string()
        .unwrap();
    let schema =
        SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = int64_batch("id", [10, 20]);

    // Write Parquet file (hash_bucket_id=0)
    let writer_conf = LakeSoulIOConfig::builder()
        .with_file(parquet_path.clone())
        .with_schema(schema.clone())
        .with_primary_key("id")
        .with_hash_bucket_num("1")
        .build();
    let mut writer = create_writer_with_io_config(writer_conf).await.unwrap();
    writer.write_record_batch(batch.clone()).await.unwrap();
    writer.flush_and_close().await.unwrap();

    // Write Vortex file (hash_bucket_id=0)
    let writer_conf = LakeSoulIOConfig::builder()
        .with_file(vortex_path.clone())
        .with_schema(schema.clone())
        .with_primary_key("id")
        .with_hash_bucket_num("1")
        .build();
    let mut writer = create_writer_with_io_config(writer_conf).await.unwrap();
    writer.write_record_batch(batch).await.unwrap();
    writer.flush_and_close().await.unwrap();

    // hash_bucket_num=1 → all values hash to {0}
    // Both files have hash_bucket_id=0, which is in {0} → both read
    // Verifies hash bucket metadata works across mixed Parquet+Vortex formats
    let reader_conf = LakeSoulIOConfig::builder()
        .with_files(vec![parquet_path, vortex_path])
        .with_schema(schema)
        .with_primary_key("id")
        .with_hash_bucket_num("1")
        .with_filters(vec![
            col("id").eq(lit(10_i64)).or(col("id").eq(lit(20_i64))),
        ])
        .with_option(OPTION_KEY_SKIP_MERGE_ON_READ, "true")
        .build();
    let mut reader = LakeSoulReader::new(reader_conf).unwrap();
    reader.start().await.unwrap();

    let mut values = vec![];
    while let Some(Ok(batch)) = reader.next_rb().await {
        values.extend(int64_values(&[batch], "id"));
    }
    values.sort_unstable();
    assert_eq!(values, vec![10, 10, 20, 20]); // both files contribute
}

#[test_log::test(tokio::test)]
async fn test_mixed_filter_on_non_projected_column() {
    let dir = tempdir().unwrap();
    let parquet_path = dir
        .path()
        .join("mixed-proj-0.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let vortex_path = dir
        .path()
        .join("mixed-proj-1.vortex")
        .into_os_string()
        .into_string()
        .unwrap();

    let batch = two_int64_batch("id", [1, 2, 3], "score", [5, 15, 25]);
    let target_schema =
        SchemaRef::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

    write_batch(parquet_path.clone(), batch.clone()).await;
    write_batch(vortex_path.clone(), batch.clone()).await;

    let batches = read_batches_with_options(
        vec![parquet_path, vortex_path],
        target_schema,                      // id only
        vec![col("score").gt(lit(10_i64))], // score > 10
        vec![],
        true, // file_filter_pushdown
    )
    .await;

    assert_single_column_schema(&batches, "id");
    let mut ids = int64_values(&batches, "id");
    ids.sort_unstable();
    assert_eq!(ids, vec![2, 2, 3, 3]); // rows with score=15,25 from both files
}

#[test_log::test(tokio::test)]
async fn test_mixed_column_types() {
    let dir = tempdir().unwrap();
    let parquet_path = dir
        .path()
        .join("mixed-cols-0.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let vortex_path = dir
        .path()
        .join("mixed-cols-1.vortex")
        .into_os_string()
        .into_string()
        .unwrap();

    let schema = SchemaRef::new(Schema::new(vec![
        Field::new("int_col", DataType::Int64, false),
        Field::new("float_col", DataType::Float64, false),
        Field::new("str_col", DataType::Utf8, false),
        Field::new("bool_col", DataType::Boolean, false),
    ]));

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values([1_i64, 2])),
            Arc::new(Float64Array::from_iter_values([1.5_f64, 2.5])),
            Arc::new(StringArray::from_iter_values(["a", "b"])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values([3_i64, 4])),
            Arc::new(Float64Array::from_iter_values([3.5_f64, 4.5])),
            Arc::new(StringArray::from_iter_values(["c", "d"])),
            Arc::new(BooleanArray::from(vec![false, true])),
        ],
    )
    .unwrap();

    write_batch(parquet_path.clone(), batch1).await;
    write_batch(vortex_path.clone(), batch2).await;

    let batches =
        read_batches(vec![parquet_path, vortex_path], schema.clone(), vec![]).await;
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 4);

    let result_schema = &batches[0].schema();
    assert_eq!(result_schema.fields().len(), 4);

    let mut ints: Vec<i64> = vec![];
    let mut floats: Vec<f64> = vec![];
    let mut strs: Vec<String> = vec![];
    let mut bools: Vec<bool> = vec![];

    for batch in &batches {
        let str_col = batch.column_by_name("str_col").unwrap();
        for i in 0..batch.num_rows() {
            ints.push(
                batch
                    .column_by_name("int_col")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(i),
            );
            floats.push(
                batch
                    .column_by_name("float_col")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(i),
            );
            strs.push(match str_col.data_type() {
                DataType::Utf8 => str_col
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(i)
                    .to_string(),
                DataType::Utf8View => {
                    use arrow_array::StringViewArray;
                    str_col
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .unwrap()
                        .value(i)
                        .to_string()
                }
                _ => unreachable!(),
            });
            bools.push(
                batch
                    .column_by_name("bool_col")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .value(i),
            );
        }
    }
    ints.sort_unstable();
    floats.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    strs.sort();
    bools.sort_unstable();
    assert_eq!(ints, vec![1, 2, 3, 4]);
    assert_eq!(floats, vec![1.5, 2.5, 3.5, 4.5]);
    assert_eq!(
        strs,
        vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string()
        ]
    );
    assert_eq!(bools, vec![false, false, true, true]);
}

#[test_log::test(tokio::test)]
async fn test_mixed_filter_zero_results() {
    let dir = tempdir().unwrap();
    let parquet_path = dir
        .path()
        .join("mixed-zero-0.parquet")
        .into_os_string()
        .into_string()
        .unwrap();
    let vortex_path = dir
        .path()
        .join("mixed-zero-1.vortex")
        .into_os_string()
        .into_string()
        .unwrap();

    let batch = int64_batch("id", [1, 2, 3]);
    let schema = batch.schema();

    write_batch(parquet_path.clone(), batch.clone()).await;
    write_batch(vortex_path.clone(), batch.clone()).await;

    let batches = read_batches(
        vec![parquet_path, vortex_path],
        schema.clone(),
        vec![col("id").gt(lit(100_i64))],
    )
    .await;

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
    for batch in &batches {
        assert_eq!(batch.schema().field(0).name(), "id");
    }
}
