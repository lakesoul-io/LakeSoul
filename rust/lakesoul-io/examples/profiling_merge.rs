use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_array::{ArrayRef, Int64Array, StringArray};
use arrow_schema::SchemaRef;
use datafusion::error::Result;
use rand::Rng;
use std::sync::Arc;
use tokio::runtime::Builder;
use tokio::time::Instant;
use rand::distributions::DistString;

use lakesoul_io::lakesoul_io_config::LakeSoulIOConfigBuilder;
use lakesoul_io::lakesoul_reader::{LakeSoulReader, SyncSendableMutableLakeSoulReader};
use lakesoul_io::lakesoul_writer::SyncSendableMutableLakeSoulWriter;

struct LinearPKGenerator {
    a: i64,
    b: i64,
    current: i64,
}

impl LinearPKGenerator {
    fn new(a: i64, b: i64) -> Self {
        LinearPKGenerator { a, b, current: 0 }
    }

    fn next_pk(&mut self) -> i64 {
        let pk = self.a * self.current + self.b;
        self.current += 1;
        pk
    }
}

fn create_batch(num_columns: usize, num_rows: usize, str_len: usize, pk_generator: &mut Option<LinearPKGenerator>) -> RecordBatch {
    let mut rng = rand::thread_rng();
    let mut len_rng = rand::thread_rng();
    let mut iter = vec![];
    if let Some(generator) = pk_generator {
        let pk_iter = (0..num_rows).map(|_| uuid::Builder::from_bytes_le((generator.next_pk() as u128).to_le_bytes()).as_uuid().to_string());
        iter.push((
            "pk".to_string(),
            Arc::new(StringArray::from_iter_values(pk_iter)) as ArrayRef,
            true,
        ));
    }
    for i in 0..num_columns {
        iter.push((
            format!("col_{}", i),
            Arc::new(StringArray::from(
                (0..num_rows)
                    .into_iter()
                    .map(|_| {
                        rand::distributions::Alphanumeric
                            .sample_string(&mut rng, len_rng.gen_range(str_len..str_len * 3))
                    })
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            true,
        ));
    }
    RecordBatch::try_from_iter_with_nullable(iter).unwrap()

}

fn create_schema(num_columns: usize, with_pk: bool) -> SchemaRef {
    let mut fields = vec![];
    if with_pk {
        fields.push(Field::new("pk", DataType::Utf8, true));
    }
    for i in 0..num_columns {
        fields.push(Field::new(format!("col_{}", i), DataType::Utf8, true));
    }
    Arc::new(Schema::new(fields))
}

fn main() -> Result<()> {
    let num_batch = 128;
    let num_rows = 512;
    let num_columns = 16;
    let str_len = 4;
    let temp_dir = std::env::current_dir()?.join("temp_dir");
    let with_pk = true;
    let file_num = 50;
    let to_write_schema = create_schema(num_columns, with_pk);
    
    for i in 0..file_num {

        let mut generator = if with_pk { Some(LinearPKGenerator::new(i + 2, 0)) } else { None } ;
        // let to_write = create_batch(num_columns, num_rows, str_len, &mut generator);
        let path = temp_dir
            .clone()
            .join(format!("test{}.parquet", i))
            .into_os_string()
            .into_string()
            .unwrap();
        dbg!(&path);
        let writer_conf = LakeSoulIOConfigBuilder::new()
            .with_files(vec![path.clone()])
            // .with_prefix(tempfile::tempdir()?.into_path().into_os_string().into_string().unwrap())
            .with_thread_num(2)
            .with_batch_size(num_rows)
            // .with_max_row_group_size(2000)
            // .with_max_row_group_num_values(4_00_000)
            .with_schema(to_write_schema.clone())
            .with_primary_keys(
                vec!["pk".to_string()]
            )
            // .with_aux_sort_column("col2".to_string())
            // .with_option(OPTION_KEY_MEM_LIMIT, format!("{}", 1024 * 1024 * 48))
            // .set_dynamic_partition(true)
            .with_hash_bucket_num(4)
            // .with_max_file_size(1024 * 1024 * 32)
            .build();

        let mut writer = SyncSendableMutableLakeSoulWriter::try_new(
            writer_conf, 
            Builder::new_multi_thread().enable_all().build().unwrap()
        )?;

        let start = Instant::now();
        for _ in 0..num_batch {
            // let once_start = Instant::now();
            writer.write_batch(create_batch(num_columns, num_rows, str_len, &mut generator))?;
            // println!("write batch once cost: {}", once_start.elapsed().as_millis());
        }
        let flush_start = Instant::now();
        writer.flush_and_close()?;
        println!("write into file {} cost: {}ms", path, flush_start.elapsed().as_millis());
    }

    let reader_conf = LakeSoulIOConfigBuilder::new()
        .with_files((0..file_num).map(|i| temp_dir.join(format!("test{}.parquet", i)).into_os_string().into_string().unwrap()).collect::<Vec<_>>())
        .with_thread_num(2)
        .with_batch_size(num_rows)
        .with_schema(to_write_schema.clone())
        .with_primary_keys(vec!["pk".to_string()])
        .build();

    let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
    let lakesoul_reader = LakeSoulReader::new(reader_conf)?;
    let mut reader = SyncSendableMutableLakeSoulReader::new(lakesoul_reader, runtime);
    reader.start_blocked();
    let start = Instant::now();
    let mut rb_count = 0;
    while let Some(rb) = reader.next_rb_blocked() {
        let rb = rb.unwrap();
        rb_count += rb.num_rows();
        // dbg!(&rb.column_by_name("pk").unwrap());
    }
    if file_num == 2 {
        assert_eq!(rb_count, num_rows * num_batch * 2 - num_rows * num_batch / 3 - 1);
    } 
    println!("time cost: {:?}ms", start.elapsed().as_millis()); // ms
    
    Ok(())
} 