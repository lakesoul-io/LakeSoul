// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Regression test: vortex files are written with small row blocks and a
//! small data block target, so a random row read (the candidate rows of a
//! vector search, a pk lookup) fetches little per row.
//!
//! The vortex writer defaults (8192 rows per block, 1 MiB data blocks) are
//! tuned for sequential analytics; a scattered candidate set would read most
//! of a wide payload column for a handful of rows.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lakesoul_io::config::{
    LakeSoulIOConfigBuilder, OPTION_KEY_VORTEX_DATA_BLOCK_TARGET_BYTES,
    OPTION_KEY_VORTEX_ROW_BLOCK_SIZE,
};
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::reader::LakeSoulReader;
use lakesoul_io::writer::create_writer_with_io_config;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use vortex::VortexSessionDefault;
use vortex::array::memory::MemorySessionExt;
use vortex::file::{OpenOptionsSessionExt, VortexFile};
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::layout::LayoutRef;
use vortex::session::VortexSession;

/// Rows written by the test: several 8192-row writer blocks.
const ROWS: i64 = 24_576;

fn rows_batch() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int64Array::from_iter_values(0..ROWS));
    // A payload wide enough that the writer default (1 MiB data blocks) can
    // pack many rows into one block.
    let payload: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..ROWS).map(|id| format!("payload-{id}-{}", "x".repeat(96))),
    ));
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ])),
        vec![ids, payload],
    )
    .expect("batch construction failed")
}

/// Write `rows_batch()` to `path`, with `options` overriding the write layout.
async fn write_vortex_file(path: &str, options: &[(&str, &str)]) {
    let batch = rows_batch();
    let mut builder = LakeSoulIOConfigBuilder::new()
        .with_files(vec![path.to_string()])
        .with_primary_key("id".to_string())
        .with_schema(batch.schema())
        .with_batch_size(8192)
        .with_physical_format(PhysicalFormat::Vortex);
    for (key, value) in options {
        builder = builder.with_option(*key, *value);
    }
    let mut writer = create_writer_with_io_config(builder.build())
        .await
        .expect("writer creation failed");
    writer
        .write_record_batch(batch)
        .await
        .expect("batch write failed");
    writer.flush_and_close().await.expect("flush failed");
}

async fn open_vortex_file(path: &str) -> VortexFile {
    let session = VortexSession::default().with_tokio();
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix("/").unwrap());
    let location = StorePath::from_absolute_path(path).unwrap();
    let reader = Arc::new(ObjectStoreReadAt::new_with_allocator(
        store,
        location,
        session.handle(),
        session.allocator(),
    ));
    session
        .open_options()
        .open_read(reader)
        .await
        .expect("open failed")
}

/// Number of leaf layouts under the column `field`: the data blocks the read
/// path can fetch individually.
fn count_blocks(layout: &LayoutRef, path: &mut Vec<String>, field: &str) -> usize {
    let children = layout.children().unwrap_or_default();
    if children.is_empty() {
        return usize::from(path.iter().any(|name| name == field));
    }
    let names: Vec<String> = layout.child_names().map(|name| name.to_string()).collect();
    let mut blocks = 0;
    for (index, child) in children.into_iter().enumerate() {
        let name = names.get(index).cloned().unwrap_or_default();
        path.push(name);
        blocks += count_blocks(&child, path, field);
        path.pop();
    }
    blocks
}

/// Every row of the file can be read back, so the smaller blocks stay valid.
async fn read_ids(path: &str) -> Vec<i64> {
    let config = LakeSoulIOConfigBuilder::new()
        .with_files(vec![path.to_string()])
        .with_primary_key("id".to_string())
        .with_schema(rows_batch().schema())
        .with_physical_format(PhysicalFormat::Vortex)
        .build();
    let mut reader = LakeSoulReader::new(config).expect("reader creation failed");
    reader.start().await.expect("reader start failed");
    let mut ids = Vec::new();
    while let Some(batch) = reader.next_rb().await {
        let batch = batch.expect("read failed");
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column");
        ids.extend(values.values().iter().copied());
    }
    ids.sort_unstable();
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vortex_files_use_small_row_blocks() {
    // Files are kept where a manual probe (the vortex probes' `PROBE_FILE`)
    // can read them back when the variable is set.
    let keep_dir = std::env::var("VORTEX_ROW_BLOCKS_DIR").ok();
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let dir = keep_dir.unwrap_or_else(|| temp_dir.path().display().to_string());
    std::fs::create_dir_all(&dir).ok();
    let default_path = format!("{dir}/default.vortex");
    let writer_default_path = format!("{dir}/writer_default.vortex");

    // The writer's own defaults: 8192 rows per block, 1 MiB data blocks.
    write_vortex_file(
        &writer_default_path,
        &[
            (OPTION_KEY_VORTEX_ROW_BLOCK_SIZE, "8192"),
            (OPTION_KEY_VORTEX_DATA_BLOCK_TARGET_BYTES, "1048576"),
        ],
    )
    .await;
    write_vortex_file(&default_path, &[]).await;

    let writer_default = open_vortex_file(&writer_default_path).await;
    let small_blocks = open_vortex_file(&default_path).await;
    let writer_default_blocks =
        count_blocks(writer_default.footer().layout(), &mut Vec::new(), "payload");
    let small_blocks_count =
        count_blocks(small_blocks.footer().layout(), &mut Vec::new(), "payload");
    let writer_default_size = std::fs::metadata(&writer_default_path)
        .map(|meta| meta.len())
        .unwrap_or_default();
    let small_blocks_size = std::fs::metadata(&default_path)
        .map(|meta| meta.len())
        .unwrap_or_default();
    println!(
        "payload blocks: writer default {writer_default_blocks}, small layout \
         {small_blocks_count}; file bytes: {writer_default_size} vs {small_blocks_size}"
    );
    assert!(
        writer_default_blocks >= 1,
        "the writer default must write at least one block"
    );
    assert!(
        small_blocks_count >= writer_default_blocks * 3,
        "the default layout must split the payload into many blocks: \
         {small_blocks_count} vs {writer_default_blocks}"
    );

    let ids = read_ids(&default_path).await;
    assert_eq!(ids.len(), ROWS as usize, "rows read back");
    assert_eq!(ids.first(), Some(&0), "first row");
    assert_eq!(ids.last(), Some(&(ROWS - 1)), "last row");
}
