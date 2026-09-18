// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end: build text index splits from parquet data and search them
//! through `LakeSoulReader` with exact candidate verification.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lakesoul_common::IndexKind;
use lakesoul_io::config::LakeSoulIOConfig;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_io::reader::LakeSoulReader;
use lakesoul_io::text::builder::TextShardIndexBuilder;
use lakesoul_io::writer::create_writer_with_io_config;
use lakesoul_text::TextIndexConfig;
use object_store::local::LocalFileSystem;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("body", DataType::Utf8, true),
    ]))
}

fn batch(rows: &[(u64, &str)]) -> RecordBatch {
    let ids: Vec<u64> = rows.iter().map(|(id, _)| *id).collect();
    let texts: Vec<&str> = rows.iter().map(|(_, text)| *text).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(UInt64Array::from(ids)) as ArrayRef,
            Arc::new(StringArray::from(texts)) as ArrayRef,
        ],
    )
    .unwrap()
}

async fn write(prefix: &str, batch: &RecordBatch) -> Vec<String> {
    let config = LakeSoulIOConfig::builder()
        .with_prefix(prefix.to_string())
        .with_schema(schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1")
        .with_thread_num(1)
        .with_batch_size(8)
        .build();
    let mut writer = create_writer_with_io_config(config).await.unwrap();
    writer.write_record_batch(batch.clone()).await.unwrap();
    writer
        .flush_and_close()
        .await
        .unwrap()
        .into_iter()
        .map(|output| output.file_path)
        .collect()
}

fn config() -> TextIndexConfig {
    TextIndexConfig {
        column_name: "body".to_string(),
        tokenizer: "jieba".to_string(),
        with_positions: true,
        stored: false,
    }
}

async fn build_split(index_prefix: &str, files: Vec<String>) -> ResolvedIndex {
    let outcome = TextShardIndexBuilder::new(
        Arc::new(LocalFileSystem::new()),
        config(),
        files,
        "id".to_string(),
        HashMap::new(),
        None,
    )
    .build()
    .await
    .unwrap();
    assert_eq!(
        outcome.index_prefix.trim_end_matches('/'),
        index_prefix.trim_end_matches('/')
    );
    let segments = outcome.segments_json().unwrap();
    ResolvedIndex {
        kind: IndexKind::Text,
        index_prefix: outcome.index_prefix,
        commit_id: 1,
        generation: 1,
        version: 1,
        header: outcome.header,
        segments,
    }
}

async fn read_ids(
    prefix: &str,
    files: Vec<String>,
    target_schema: SchemaRef,
    query: &str,
    resolved: Vec<ResolvedIndex>,
) -> Vec<u64> {
    let config = LakeSoulIOConfig::builder()
        .with_prefix(prefix.to_string())
        .with_files(files)
        .with_schema(target_schema)
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1")
        .with_thread_num(1)
        .with_batch_size(8)
        .with_option("text_search_column", "body")
        .with_option("text_search_query", query)
        .with_option("text_search_top_k", "10")
        .with_resolved_index_shards(resolved)
        .build();
    let mut reader = LakeSoulReader::new(config).unwrap();
    reader.start().await.unwrap();
    let mut ids = vec![];
    while let Some(record) = reader.next_rb().await {
        let record = record.unwrap();
        let values = record
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        for i in 0..record.num_rows() {
            ids.push(values.value(i));
        }
    }
    ids.sort_unstable();
    ids
}

/// The split of the inserted data is stale for the updated row: the index
/// candidate must be verified against the current text and dropped.
#[tokio::test]
async fn stale_update_is_not_returned() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().to_string_lossy().into_owned();

    let inserted = write(&prefix, &batch(&[(1, "apple pie"), (2, "banana bread")])).await;
    let updated = write(&prefix, &batch(&[(1, "cherry tart")])).await;
    let index_prefix = format!("{prefix}/_text_index/body/-5/0");

    // Two splits: the original insert and the incremental update split.
    let first = build_split(&index_prefix, inserted.clone()).await;
    let second = build_split(&index_prefix, updated.clone()).await;
    let mut resolved = first.clone();
    let mut segments = first.segments.as_array().cloned().unwrap_or_default();
    segments.extend(second.segments.as_array().cloned().unwrap_or_default());
    resolved.segments = serde_json::Value::Array(segments);

    let all_files: Vec<String> = inserted.iter().chain(updated.iter()).cloned().collect();
    let target = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));

    // "apple" only existed in the superseded version of id=1.
    let ids = read_ids(
        &prefix,
        all_files.clone(),
        target.clone(),
        "apple",
        vec![resolved.clone()],
    )
    .await;
    assert!(ids.is_empty(), "stale text matched: {ids:?}");

    // The current text of id=1 and the untouched row are found.
    let ids = read_ids(
        &prefix,
        all_files.clone(),
        target.clone(),
        "cherry",
        vec![resolved.clone()],
    )
    .await;
    assert_eq!(ids, vec![1]);
    let ids = read_ids(
        &prefix,
        all_files.clone(),
        target.clone(),
        "banana",
        vec![resolved],
    )
    .await;
    assert_eq!(ids, vec![2]);
}

/// The text column is read for verification even when the caller does not
/// project it, and it is removed from the output again.
#[tokio::test]
async fn hidden_text_column_is_dropped_and_chinese_matches() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().to_string_lossy().into_owned();
    let files = write(
        &prefix,
        &batch(&[(10, "机器学习与向量检索"), (11, "全文检索使用倒排索引")]),
    )
    .await;
    let index_prefix = format!("{prefix}/_text_index/body/-5/0");
    let resolved = build_split(&index_prefix, files.clone()).await;

    // The caller only asks for the primary key: verification still needs the
    // text column internally.
    let target = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
    let ids = read_ids(&prefix, files, target, "倒排索引", vec![resolved]).await;
    assert_eq!(ids, vec![11]);
}

/// Special characters from the jieba analyzer do not leak into the output
/// schema when the caller projected the text column itself.
#[tokio::test]
async fn projected_text_column_is_preserved() {
    let dir = tempfile::tempdir().unwrap();
    let prefix = dir.path().to_string_lossy().into_owned();
    let files = write(&prefix, &batch(&[(7, "hello world")])).await;
    let index_prefix = format!("{prefix}/_text_index/body/-5/0");
    let resolved = build_split(&index_prefix, files.clone()).await;

    let config = LakeSoulIOConfig::builder()
        .with_prefix(prefix.clone())
        .with_files(files)
        .with_schema(schema())
        .with_primary_keys(vec!["id".to_string()])
        .with_hash_bucket_num("1")
        .with_thread_num(1)
        .with_option("text_search_column", "body")
        .with_option("text_search_query", "hello")
        .with_option("text_search_top_k", "10")
        .with_resolved_index_shards(vec![resolved])
        .build();
    let mut reader = LakeSoulReader::new(config).unwrap();
    reader.start().await.unwrap();
    while let Some(record) = reader.next_rb().await {
        let record = record.unwrap();
        assert_eq!(
            record
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "body"]
        );
        let texts = record
            .column_by_name("body")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(texts.value(0), "hello world");
    }
}
