// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Regression test: merge-on-read over multiple vortex files must deduplicate
//! primary keys exactly.
//!
//! The merge-on-read k-way merge requires every input stream to be sorted by
//! the primary key. The vortex file source reads row-group splits
//! concurrently and, unless the scan declares an output ordering, emits
//! batches in completion order — silently breaking the merge's deduplication
//! and leaking duplicate rows (e2e symptom: `COUNT(*)` exceeds the number of
//! unique primary keys after an upsert).
//!
//! This test writes three vortex files whose id spaces fully overlap (like
//! repeated upsert batches of the same table), with multiple row-group splits
//! per file like production data, then reads all files with
//! `primary_keys=["id"]` and asserts the merged count equals the number of
//! unique ids. It fails without the ordering declaration in
//! `MergeParquetExec::new`.

use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::reader::LakeSoulReader;
use lakesoul_io::writer::create_writer_with_io_config;

/// Number of distinct ids shared by all files.
const N: i64 = 1_000_000;

fn ids_batch(values: impl IntoIterator<Item = i64>) -> RecordBatch {
    let col: ArrayRef =
        Arc::new(Int64Array::from(values.into_iter().collect::<Vec<_>>()));
    RecordBatch::try_from_iter(vec![("id", col)]).expect("batch construction failed")
}

/// Write a vortex file containing `ids`, sorted by `id` by the sort writer
/// (same as the LakeSoul upsert write path). Row groups of 100k rows produce
/// multiple splits per file, like production files.
async fn write_vortex_file(path: &str, ids: RecordBatch) {
    let conf = LakeSoulIOConfigBuilder::new()
        .with_files(vec![path.to_string()])
        .with_primary_key("id".to_string())
        .with_schema(ids.schema())
        .with_batch_size(8192)
        .with_max_row_group_size(100_000)
        .with_physical_format(PhysicalFormat::VortexCompact)
        .build();
    let mut writer = create_writer_with_io_config(conf)
        .await
        .expect("writer creation failed");
    writer
        .write_record_batch(ids)
        .await
        .expect("batch write failed");
    writer.flush_and_close().await.expect("flush failed");
}

#[tokio::test]
async fn merge_on_read_dedups_overlapping_vortex_files() {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    // Three files with fully overlapping id spaces, like three upsert batches
    // of the same table. Every row beyond the first occurrence is a duplicate
    // that merge-on-read must remove.
    let files: Vec<String> = (0..3)
        .map(|i| {
            temp_dir
                .path()
                .join(format!("batch{i}.vortex"))
                .into_os_string()
                .into_string()
                .unwrap()
        })
        .collect();
    for f in &files {
        // unsorted runs so the sort writer must actually sort
        write_vortex_file(f, ids_batch((1..=N).chain((1..=N).rev()))).await;
    }

    // Read through the exact production merge-on-read path.
    let conf = LakeSoulIOConfigBuilder::new()
        .with_files(files)
        .with_primary_keys(vec!["id".to_string()])
        .with_schema(Arc::new(Schema::new(vec![Field::new(
            "id",
            DataType::Int64,
            false,
        )])))
        .with_batch_size(8192)
        .with_thread_num(4)
        .with_hash_bucket_num("2".to_string())
        .build();
    let mut reader = LakeSoulReader::new(conf).expect("reader creation failed");
    reader.start().await.expect("reader start failed");

    let mut count = 0usize;
    while let Some(rb) = reader.next_rb().await {
        count += rb.expect("batch read failed").num_rows();
    }

    assert_eq!(
        count, N as usize,
        "merge-on-read must deduplicate overlapping primary keys exactly"
    );
}
