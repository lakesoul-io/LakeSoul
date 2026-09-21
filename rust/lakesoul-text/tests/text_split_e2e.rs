// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! End-to-end test: build a text split, upload it to an object store,
//! download/materialize it in the split cache, and search it.

use std::sync::Arc;

use lakesoul_text::{
    SplitCache, TextIndexConfig, search_index, search_splits, write_split,
};
use object_store::ObjectStore;
use object_store::memory::InMemory;

const BUDGET: usize = 32 * 1024 * 1024;

fn config() -> TextIndexConfig {
    TextIndexConfig {
        column_name: "body".to_string(),
        tokenizer: "jieba".to_string(),
        with_positions: true,
        stored: false,
    }
}

fn english_docs() -> Vec<(u64, String)> {
    vec![
        (1, "The Old Man and the Sea".to_string()),
        (2, "The quick brown fox jumps over the lazy dog".to_string()),
        (3, "Rust is a systems programming language".to_string()),
    ]
}

fn chinese_docs() -> Vec<(u64, String)> {
    vec![
        (10, "机器学习与向量检索".to_string()),
        (11, "LakeSoul 是一个云原生湖仓框架".to_string()),
        (12, "全文检索使用倒排索引".to_string()),
    ]
}

#[tokio::test]
async fn build_upload_materialize_and_search_english() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = "table/_text_index/body/-5/0";

    let entry = write_split(&store, prefix, &config(), &english_docs(), BUDGET)
        .await
        .unwrap();
    assert_eq!(entry.num_docs, 3);
    assert!(entry.file_size > 0);
    assert!(entry.filename.ends_with(".split"));

    let cache_dir = tempfile::tempdir().unwrap();
    let cache = SplitCache::new(cache_dir.path());
    let index = cache.open(&store, prefix, &entry).await.unwrap();

    let hits = search_index(&index, "old man sea", 3).unwrap();
    assert_eq!(hits[0].id, 1, "top hit should be the old man");

    let hits = search_index(&index, "systems programming", 3).unwrap();
    assert_eq!(hits[0].id, 3);

    // The materialized directory is reused on the next open.
    let reopened = cache.open(&store, prefix, &entry).await.unwrap();
    assert_eq!(search_index(&reopened, "fox", 1).unwrap()[0].id, 2);
}

#[tokio::test]
async fn chinese_text_is_segmented_by_jieba() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = "table/_text_index/body/part=1/0";

    let entry = write_split(&store, prefix, &config(), &chinese_docs(), BUDGET)
        .await
        .unwrap();

    let cache_dir = tempfile::tempdir().unwrap();
    let cache = SplitCache::new(cache_dir.path());
    let index = cache.open(&store, prefix, &entry).await.unwrap();

    let hits = search_index(&index, "机器学习", 3).unwrap();
    assert_eq!(hits[0].id, 10);

    let hits = search_index(&index, "倒排索引", 3).unwrap();
    assert_eq!(hits[0].id, 12);
}

#[tokio::test]
async fn multiple_splits_merge_into_a_global_top_k() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = "table/_text_index/body/-5/0";

    let english = write_split(&store, prefix, &config(), &english_docs(), BUDGET)
        .await
        .unwrap();
    let chinese = write_split(&store, prefix, &config(), &chinese_docs(), BUDGET)
        .await
        .unwrap();

    let cache_dir = tempfile::tempdir().unwrap();
    let cache = SplitCache::new(cache_dir.path());
    let indexes = vec![
        cache.open(&store, prefix, &english).await.unwrap(),
        cache.open(&store, prefix, &chinese).await.unwrap(),
    ];

    // "old" only appears in the English split, "机器学习" only in the Chinese
    // one; a merged search returns hits from both.
    let hits = search_splits(&indexes, "old 机器学习", 10, 5).unwrap();
    let ids: Vec<u64> = hits.iter().map(|hit| hit.id).collect();
    assert!(ids.contains(&1), "english hit missing: {ids:?}");
    assert!(ids.contains(&10), "chinese hit missing: {ids:?}");
}

#[tokio::test]
async fn large_batches_are_merged_into_a_single_segment() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = "table/_text_index/body/-5/0";
    let config = TextIndexConfig {
        column_name: "body".to_string(),
        tokenizer: "en_stem".to_string(),
        with_positions: false,
        stored: false,
    };

    // More text than the minimum memory budget forces the writer to flush
    // several segments while indexing; the split must still be a single
    // segment and keep every live document.
    let filler = "lakesoul distributed lakehouse storage engine ".repeat(6);
    let docs: Vec<(u64, String)> = (0..80_000)
        .map(|id| (id, format!("document {id} {filler}")))
        .collect();
    let entry = write_split(&store, prefix, &config, &docs, 15_000_000)
        .await
        .unwrap();
    assert_eq!(entry.num_docs, docs.len() as u64);

    let cache_dir = tempfile::tempdir().unwrap();
    let cache = SplitCache::new(cache_dir.path());
    let index = cache.open(&store, prefix, &entry).await.unwrap();
    assert_eq!(
        index.searchable_segment_ids().unwrap().len(),
        1,
        "the split must be force-merged into one segment"
    );

    let hits = search_index(&index, "distributed lakehouse", 5).unwrap();
    assert_eq!(hits.len(), 5);
}

#[tokio::test]
async fn duplicate_primary_keys_keep_the_last_document() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let prefix = "table/_text_index/body/-5/0";

    // A rebuilt shard reads overlapping versions from several data files;
    // only the last document of each primary key may survive.
    let docs = vec![
        (1, "apple pie".to_string()),
        (2, "banana bread".to_string()),
        (1, "cherry tart".to_string()),
        (1, "date cake".to_string()),
    ];
    let entry = write_split(&store, prefix, &config(), &docs, BUDGET)
        .await
        .unwrap();
    assert_eq!(entry.num_docs, 2, "superseded documents must be deleted");

    let cache_dir = tempfile::tempdir().unwrap();
    let cache = SplitCache::new(cache_dir.path());
    let index = cache.open(&store, prefix, &entry).await.unwrap();

    assert!(search_index(&index, "apple", 10).unwrap().is_empty());
    assert!(search_index(&index, "cherry", 10).unwrap().is_empty());
    assert_eq!(search_index(&index, "date", 10).unwrap()[0].id, 1);
    assert_eq!(search_index(&index, "banana", 10).unwrap()[0].id, 2);
}
