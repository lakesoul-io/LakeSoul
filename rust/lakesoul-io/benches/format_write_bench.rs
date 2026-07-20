// SPDX-FileCopyrightText: LakeSoul 2026 Contributors
//
// SPDX-License-Identifier: Apache-2.0

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use criterion::{Criterion, criterion_group, criterion_main};
use parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use vortex::{
    VortexSessionDefault,
    array::{arrow::ArrowSessionExt, stream::ArrayStreamAdapter},
    compressor::BtrBlocksCompressorBuilder,
    file::{WriteOptionsSessionExt, WriteStrategyBuilder},
    io::session::RuntimeSessionExt,
    session::VortexSession,
};

mod common;

/// use mimalloc for benchmarking
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// Keep these defaults aligned with LakeSoulIOConfig and
// lakesoul-io/src/writer/mod.rs::parquet_options.
const LAKESOUL_DEFAULT_BATCH_SIZE: usize = 8192;
const LAKESOUL_DEFAULT_MAX_ROW_GROUP_SIZE: usize = 250_000;
const LAKESOUL_DEFAULT_MAX_ROW_GROUP_NUM_VALUES: usize = 2_147_483_647;

fn lakesoul_parquet_writer_properties(schema: &SchemaRef) -> WriterProperties {
    let field_count = schema.fields().len().max(1);
    // Target row group size: 250,000 rows.
    // However, if the schema is wide such that:
    //     250,000 * num_columns > max_allowed_values
    // then reduce the row group size to:
    //     max_allowed_values / num_columns
    // while ensuring it is never smaller than batch_size.
    let max_row_group_size = if LAKESOUL_DEFAULT_MAX_ROW_GROUP_SIZE * field_count
        > LAKESOUL_DEFAULT_MAX_ROW_GROUP_NUM_VALUES
    {
        LAKESOUL_DEFAULT_BATCH_SIZE
            .max(LAKESOUL_DEFAULT_MAX_ROW_GROUP_NUM_VALUES / field_count)
    } else {
        LAKESOUL_DEFAULT_MAX_ROW_GROUP_SIZE
    };

    WriterProperties::builder()
        .set_write_batch_size(LAKESOUL_DEFAULT_BATCH_SIZE)
        .set_max_row_group_size(max_row_group_size)
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .set_dictionary_enabled(false)
        .build()
}

// only test write performance
fn parquet_write(schema: SchemaRef, batches: &[RecordBatch]) {
    let mut buf = Vec::new();
    let props = lakesoul_parquet_writer_properties(&schema);
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();
}

fn vortex_write_tokio(schema: SchemaRef, batches: &[RecordBatch]) {
    let mut buf = Vec::new();
    let batches = batches.to_vec();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(16)
        .build()
        .unwrap();
    rt.block_on(async move {
        let session = VortexSession::default().with_tokio();
        let dtype = session.arrow().from_arrow_schema(schema.as_ref()).unwrap();
        let import_schema = schema;
        let arrow_session = session.clone();
        let stream =
            futures::stream::iter(batches.into_iter().map(move |record_batch| {
                arrow_session
                    .arrow()
                    .from_arrow_record_batch(record_batch, &import_schema)
            }));

        session
            .write_options()
            .write(&mut buf, ArrayStreamAdapter::new(dtype, stream))
            .await
            .unwrap();
    });
}

fn vortex_compact_write_tokio(schema: SchemaRef, batches: &[RecordBatch]) {
    let mut buf = Vec::new();
    let batches = batches.to_vec();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(16)
        .build()
        .unwrap();
    rt.block_on(async move {
        let session = VortexSession::default().with_tokio();
        let dtype = session.arrow().from_arrow_schema(schema.as_ref()).unwrap();
        let import_schema = schema;
        let arrow_session = session.clone();
        let stream =
            futures::stream::iter(batches.into_iter().map(move |record_batch| {
                arrow_session
                    .arrow()
                    .from_arrow_record_batch(record_batch, &import_schema)
            }));

        let write_options = session.write_options().with_strategy(
            WriteStrategyBuilder::default()
                .with_btrblocks_builder(
                    BtrBlocksCompressorBuilder::default().with_compact(),
                )
                .build(),
        );
        write_options
            .write(&mut buf, ArrayStreamAdapter::new(dtype, stream))
            .await
            .unwrap();
    });
}

fn bench_format(c: &mut Criterion) {
    let (schema, batches) = common::create_q20_like_batches(1000, 10_000);
    let mut group = c.benchmark_group("Format_Write_Comparison");
    group.sample_size(10);

    // parquet
    group.bench_function("parquet-write", |b| {
        b.iter(|| parquet_write(schema.clone(), &batches))
    });
    // vortex
    group.bench_function("vortex-write-tokio", |b| {
        b.iter(|| vortex_write_tokio(schema.clone(), &batches))
    });
    // vortex-compact
    group.bench_function("vortex-compact-write-tokio", |b| {
        b.iter(|| vortex_compact_write_tokio(schema.clone(), &batches))
    });

    group.finish();
}

criterion_group!(benches, bench_format);
criterion_main!(benches);
