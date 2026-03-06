use std::sync::Arc;

use arrow_cast::pretty::print_batches;
use arrow_schema::{DataType, Field, SchemaBuilder, SchemaRef};
use lakesoul_io::{
    config::LakeSoulIOConfig,
    reader::LakeSoulReader,
    utils::{create_random_batch, lakesoul_file_name, random_str},
    writer::create_writer_with_io_config,
};
use tempfile::env::temp_dir;

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
        .write_record_batch(create_random_batch(schema.clone(), 3, 0.0))
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
