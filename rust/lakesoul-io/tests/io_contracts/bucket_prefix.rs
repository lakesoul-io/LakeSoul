// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tests for the IVM bucket layout: a table may bucket by a prefix of its
//! merge key (`hash_partitioning_columns`) while merging on the full key.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, col, lit};
use lakesoul_io::{
    config::{LakeSoulIOConfig, OPTION_KEY_SKIP_MERGE_ON_READ},
    helpers::{compute_scalar_hash, extract_hash_bucket_id},
    reader::LakeSoulReader,
    writer::create_writer_with_io_config,
};
use tempfile::tempdir;

const BUCKETS: u32 = 4;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("row_id", DataType::Int64, false),
        Field::new("v", DataType::Utf8, true),
    ]))
}

fn batch(rows: &[(i64, i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|row| row.2))),
        ],
    )
    .unwrap()
}

fn bucket_of(value: i64) -> u32 {
    compute_scalar_hash(&ScalarValue::Int64(Some(value))) % BUCKETS
}

fn list_files(dir: &Path) -> Vec<String> {
    let mut files = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|ext| ext == "parquet" || ext == "vortex")
        })
        .map(|path| path.into_os_string().into_string().unwrap())
        .collect::<Vec<_>>();
    files.sort();
    files
}

async fn write_bucket_rows(prefix: &str, rows: &[(i64, i64, &str)]) {
    let config = LakeSoulIOConfig::builder()
        .with_prefix(prefix.to_string())
        .with_thread_num(2)
        .with_batch_size(2)
        .with_schema(schema())
        .with_primary_keys(vec!["k".to_string(), "row_id".to_string()])
        .with_hash_partitioning_columns(vec!["k".to_string()])
        .with_hash_bucket_num(BUCKETS.to_string())
        .set_dynamic_partition(true)
        .build();
    let mut writer = create_writer_with_io_config(config).await.unwrap();
    writer.write_record_batch(batch(rows)).await.unwrap();
    writer.flush_and_close().await.unwrap();
}

async fn read_rows(
    files: Vec<String>,
    filters: Vec<Expr>,
    skip_merge_on_read: bool,
) -> Vec<(i64, i64)> {
    let config = LakeSoulIOConfig::builder()
        .with_files(files)
        .with_thread_num(2)
        .with_batch_size(2)
        .with_schema(schema())
        .with_primary_keys(vec!["k".to_string(), "row_id".to_string()])
        .with_hash_partitioning_columns(vec!["k".to_string()])
        .with_hash_bucket_num(BUCKETS.to_string())
        .with_filters(filters)
        .with_option(
            OPTION_KEY_SKIP_MERGE_ON_READ,
            if skip_merge_on_read { "true" } else { "false" },
        )
        .build();
    let mut reader = LakeSoulReader::new(config).unwrap();
    reader.start().await.unwrap();

    let mut rows = Vec::new();
    while let Some(batch) = reader.next_rb().await {
        let batch = batch.unwrap();
        let k = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let row_id = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend(
            (0..batch.num_rows()).map(|index| (k.value(index), row_id.value(index))),
        );
    }
    rows.sort_unstable();
    rows
}

fn bucket_suffixes(files: &[String]) -> Vec<u32> {
    files
        .iter()
        .map(|file| extract_hash_bucket_id(file).expect("bucket id in file name"))
        .collect()
}

#[test_log::test(tokio::test)]
async fn bucket_prefix_keeps_merge_key_and_shared_bucket() {
    let dir = tempdir().unwrap();
    let prefix = format!("file://{}", dir.path().display());

    // Pick two keys with different buckets, and a first key whose bucket
    // differs from the bucket of `row_id = 2` so the pruning test below is
    // meaningful.
    let k1 = (1..100)
        .find(|key| bucket_of(*key) != bucket_of(2))
        .unwrap();
    let k2 = (1..100)
        .find(|key| bucket_of(*key) != bucket_of(k1))
        .unwrap();

    write_bucket_rows(&prefix, &[(k1, 1, "a"), (k2, 1, "b")]).await;
    let first_files = list_files(dir.path());
    let first_suffixes = bucket_suffixes(&first_files);
    assert!(
        first_suffixes.contains(&bucket_of(k1)),
        "k1={k1} must land in bucket {}, got {first_suffixes:?}",
        bucket_of(k1)
    );
    assert!(
        first_suffixes.contains(&bucket_of(k2)),
        "k2={k2} must land in bucket {}, got {first_suffixes:?}",
        bucket_of(k2)
    );

    write_bucket_rows(&prefix, &[(k1, 2, "c")]).await;
    let second_new_files = list_files(dir.path())
        .into_iter()
        .filter(|file| !first_files.contains(file))
        .collect::<Vec<_>>();
    assert_eq!(second_new_files.len(), 1);
    // the same key prefix must hash into the same bucket as before
    assert_eq!(bucket_suffixes(&second_new_files), vec![bucket_of(k1)]);

    // merge-on-read keeps both rows of k1 (the merge key is the full PK)
    let all_files = list_files(dir.path());
    let rows = read_rows(all_files, vec![], false).await;
    assert_eq!(rows, vec![(k1, 1), (k1, 2), (k2, 1)]);
}

#[test_log::test(tokio::test)]
async fn bucket_pruning_requires_the_bucket_columns() {
    let dir = tempdir().unwrap();
    let prefix = format!("file://{}", dir.path().display());

    let k = (1..100)
        .find(|key| bucket_of(*key) != bucket_of(2))
        .unwrap();
    write_bucket_rows(&prefix, &[(k, 2, "c")]).await;
    let files = list_files(dir.path());
    assert_eq!(bucket_suffixes(&files), vec![bucket_of(k)]);

    // Filtering on `row_id` alone must not prune the file: bucket pruning may
    // only key off the bucket column `k`. Pruning on `row_id` would hash it to
    // another bucket and skip the reader, losing this row.
    let rows = read_rows(
        files.clone(),
        vec![col("row_id").in_list(vec![lit(2_i64)], false)],
        true,
    )
    .await;
    assert_eq!(rows, vec![(k, 2)]);

    // Filtering on the bucket column is still allowed to prune.
    let rows = read_rows(
        files.clone(),
        vec![col("k").in_list(vec![lit(k)], false)],
        true,
    )
    .await;
    assert_eq!(rows, vec![(k, 2)]);
}
