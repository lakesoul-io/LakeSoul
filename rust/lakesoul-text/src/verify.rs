// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Exact re-verification of text index candidates.
//!
//! An index search returns candidate primary keys from the *indexed* text,
//! which may be a stale version of a row (the row was updated or deleted
//! after the split was written).  The vector index tolerates this because
//! candidates are re-ranked by exact distance to the raw vector; a text
//! query instead needs an exact boolean pass over the **current** rows,
//! applied after merge-on-read resolved the row versions (and after CDC
//! delete tombstones were dropped).
//!
//! [`matching_scores`] builds a tiny in-memory Tantivy index over the
//! current `(primary key, text)` rows of one batch and evaluates the same
//! query with the same tokenizer, returning the current BM25 scores of the
//! rows that really match.

use std::collections::{HashMap, HashSet};

use tantivy::collector::TopDocs;
use tantivy::schema::TantivyDocument;
use tantivy::{Index, IndexWriter};

use crate::config::TextIndexConfig;
use crate::error::Result;
use crate::schema::{PK_FIELD, TextSchema};
use crate::tokenizer::register_tokenizers;

/// Smallest memory budget Tantivy accepts for an index writer.
const VERIFY_WRITER_MEMORY_BUDGET: usize = 15_000_000;

/// Exact matching rows of one candidate batch.
///
/// Null or empty texts are treated as "no text" and never match.  This is
/// the predicate the reader applies after merge-on-read (and after CDC
/// delete tombstones are dropped) before returning index candidates.
pub fn matching_ids(
    config: &TextIndexConfig,
    rows: &[(u64, Option<String>)],
    query: &str,
) -> Result<HashSet<u64>> {
    Ok(matching_scores(config, rows, query)?.into_keys().collect())
}

/// Like [`matching_ids`], but also returns the row's BM25 score inside the
/// batch.
///
/// The scores only rank rows within this candidate batch (Tantivy builds
/// per-index statistics), so callers that need a stable global ranking
/// should keep the index search scores or re-rank explicitly.
pub fn matching_scores(
    config: &TextIndexConfig,
    rows: &[(u64, Option<String>)],
    query: &str,
) -> Result<HashMap<u64, f32>> {
    let mut matched = HashMap::new();
    if rows.is_empty() {
        return Ok(matched);
    }

    let text_schema = TextSchema::build(config);
    let index = Index::create_in_ram(text_schema.schema.clone());
    register_tokenizers(&index);
    let mut writer: IndexWriter =
        index.writer_with_num_threads(1, VERIFY_WRITER_MEMORY_BUDGET)?;
    for (id, text) in rows {
        let Some(text) = text.as_deref().filter(|text| !text.is_empty()) else {
            continue;
        };
        let mut document = TantivyDocument::new();
        document.add_u64(text_schema.pk_field, *id);
        document.add_text(text_schema.text_field, text);
        writer.add_document(document)?;
    }
    writer.commit()?;

    let searcher = index.reader()?.searcher();
    let parsed = crate::search::parse_user_query(&index, text_schema.text_field, query);
    let top_docs =
        searcher.search(&parsed, &TopDocs::with_limit(rows.len()).order_by_score())?;
    for (score, address) in top_docs {
        let segment = searcher.segment_reader(address.segment_ord);
        let column = segment.fast_fields().u64(PK_FIELD)?;
        if let Some(id) = column.first(address.doc_id) {
            matched.insert(id, score);
        }
    }

    writer.wait_merging_threads()?;
    Ok(matched)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> TextIndexConfig {
        TextIndexConfig {
            column_name: "body".to_string(),
            tokenizer: "jieba".to_string(),
            with_positions: true,
            stored: false,
        }
    }

    #[test]
    fn stale_text_does_not_match() {
        // The index returned id=1 because its *old* text matched "apple";
        // the current row was updated to "banana" and must not come back.
        let rows = vec![
            (1, Some("banana".to_string())),
            (2, Some("apple pie".to_string())),
        ];
        let matched = matching_scores(&config(), &rows, "apple").unwrap();
        assert!(!matched.contains_key(&1), "stale text matched: {matched:?}");
        assert!(matched.contains_key(&2));
    }

    #[test]
    fn chinese_text_is_verified_with_the_same_tokenizer() {
        let rows = vec![
            (1, Some("机器学习与向量检索".to_string())),
            (2, Some("全文检索使用倒排索引".to_string())),
        ];
        let matched = matching_scores(&config(), &rows, "机器学习").unwrap();
        assert_eq!(matched.keys().copied().collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn null_and_empty_text_never_match() {
        let rows = vec![(1, None), (2, Some(String::new()))];
        let matched = matching_scores(&config(), &rows, "anything").unwrap();
        assert!(matched.is_empty());
    }

    #[test]
    fn all_currently_matching_rows_are_returned() {
        let rows = vec![
            (1, Some("apple apple apple".to_string())),
            (2, Some("apple".to_string())),
            (3, Some("banana".to_string())),
        ];
        let matched = matching_ids(&config(), &rows, "apple").unwrap();
        assert_eq!(
            matched.into_iter().collect::<HashSet<_>>(),
            HashSet::from([1, 2])
        );
    }
}
