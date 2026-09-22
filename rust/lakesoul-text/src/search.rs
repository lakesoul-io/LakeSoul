// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Searching text index splits.

use std::cmp::Ordering;
use std::collections::HashMap;

use tantivy::Index;
use tantivy::Term;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, EmptyQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{Field, FieldType, IndexRecordOption};
use tantivy::tokenizer::TokenStream;
use tracing::debug;

use crate::TextError;
use crate::error::Result;
use crate::schema::{PK_FIELD, TextSchema};

/// A matching row: its primary key and BM25 score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TextHit {
    pub id: u64,
    pub score: f32,
}

/// Query-syntax characters that switch parsing to [`QueryParser`].
fn has_query_syntax(query: &str) -> bool {
    const SYNTAX: &[char] = &[
        '"', '(', ')', '[', ']', '{', '}', '+', '-', '*', '~', ':', '^', '\\',
    ];
    if query.chars().any(|c| SYNTAX.contains(&c)) {
        return true;
    }
    query
        .split_whitespace()
        .any(|word| matches!(word, "AND" | "OR" | "NOT"))
}

/// Build an OR query over the terms the field analyzer produces.
///
/// This is the `match` semantics of a search engine: the text is analyzed
/// into terms and any term may match (BM25 sums the term scores).  It is
/// required for Chinese, where [`QueryParser`] turns a whitespace-free
/// sequence of tokens into an exact phrase query and loses nearly all
/// recall.
fn analyzed_query(index: &Index, field: Field, query: &str) -> Box<dyn Query> {
    let schema = index.schema();
    let entry = schema.get_field_entry(field);
    let FieldType::Str(text_options) = entry.field_type() else {
        return Box::new(EmptyQuery);
    };
    let Some(indexing) = text_options.get_indexing_options() else {
        return Box::new(EmptyQuery);
    };
    let Some(mut analyzer) = index.tokenizers().get(indexing.tokenizer()) else {
        return Box::new(EmptyQuery);
    };
    let mut stream = analyzer.token_stream(query);
    let mut terms: Vec<Term> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    while stream.advance() {
        let token = stream.token();
        if token.text.is_empty() || !seen.insert(token.text.clone()) {
            continue;
        }
        terms.push(Term::from_field_text(field, &token.text));
    }
    let clauses: Vec<(Occur, Box<dyn Query>)> = terms
        .into_iter()
        .map(|term| {
            let query: Box<dyn Query> =
                Box::new(TermQuery::new(term, IndexRecordOption::WithFreqs));
            (Occur::Should, query)
        })
        .collect();
    if clauses.is_empty() {
        return Box::new(EmptyQuery);
    }
    Box::new(BooleanQuery::new(clauses))
}

/// Parse a search query.
///
/// Explicit query syntax (`AND`/`OR`/`NOT`, quoted phrases, parentheses,
/// field prefixes) goes through Tantivy's parser; clauses that are malformed
/// are dropped instead of failing the whole query.  Everything else is
/// treated as natural-language text and analyzed into OR-combined terms.
pub(crate) fn parse_user_query(
    index: &Index,
    text_field: Field,
    query: &str,
) -> Box<dyn Query> {
    if !has_query_syntax(query) {
        return analyzed_query(index, text_field, query);
    }
    let parser = QueryParser::for_index(index, vec![text_field]);
    let (parsed, errors) = parser.parse_query_lenient(query);
    if !errors.is_empty() {
        debug!(
            query,
            dropped = errors.len(),
            "ignored malformed query clauses"
        );
    }
    parsed
}

/// Search one split and return its top `top_k` hits by BM25 score.
pub fn search_index(index: &Index, query: &str, top_k: usize) -> Result<Vec<TextHit>> {
    if top_k == 0 {
        return Ok(Vec::new());
    }
    let text_schema = TextSchema::resolve(&index.schema())?;
    let reader = index.reader()?.searcher();
    let parsed = parse_user_query(index, text_schema.text_field, query);
    let top_docs =
        reader.search(&parsed, &TopDocs::with_limit(top_k).order_by_score())?;

    let mut hits = Vec::with_capacity(top_docs.len());
    for (score, address) in top_docs {
        let segment = reader.segment_reader(address.segment_ord);
        let column = segment.fast_fields().u64(PK_FIELD)?;
        let id = column.first(address.doc_id).ok_or_else(|| {
            TextError::Invalid(format!(
                "text index hit at doc {} has no primary key",
                address.doc_id
            ))
        })?;
        hits.push(TextHit { id, score });
    }
    Ok(hits)
}

/// Merge per-split hits into a global top-k.
///
/// Scores of different splits are not strictly comparable (per-split BM25
/// statistics); the highest score per primary key is kept and the result is
/// sorted by descending score with the primary key as a tie breaker.
pub fn merge_hits(hits: impl IntoIterator<Item = TextHit>, top_k: usize) -> Vec<TextHit> {
    let mut best: HashMap<u64, f32> = HashMap::new();
    for hit in hits {
        let entry = best.entry(hit.id).or_insert(f32::NEG_INFINITY);
        if hit.score > *entry {
            *entry = hit.score;
        }
    }
    let mut merged: Vec<TextHit> = best
        .into_iter()
        .map(|(id, score)| TextHit { id, score })
        .collect();
    merged.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.id.cmp(&b.id))
    });
    merged.truncate(top_k);
    merged
}

/// Search every split and return the merged top-k.
pub fn search_splits(
    indexes: &[Index],
    query: &str,
    top_k_per_split: usize,
    top_k: usize,
) -> Result<Vec<TextHit>> {
    let mut all = Vec::new();
    for index in indexes {
        all.extend(search_index(index, query, top_k_per_split)?);
    }
    Ok(merge_hits(all, top_k))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_keeps_highest_score_per_id_and_sorts() {
        let hits = vec![
            TextHit { id: 1, score: 0.5 },
            TextHit { id: 2, score: 0.9 },
            TextHit { id: 1, score: 1.5 },
            TextHit { id: 3, score: 1.5 },
        ];
        let merged = merge_hits(hits, 2);
        assert_eq!(merged.len(), 2);
        // 1 and 3 tie on 1.5; the smaller id wins the tie breaker.
        assert_eq!(merged[0], TextHit { id: 1, score: 1.5 });
        assert_eq!(merged[1], TextHit { id: 3, score: 1.5 });
    }

    #[test]
    fn merge_truncates_to_top_k() {
        let merged = merge_hits(
            (0..10).map(|id| TextHit {
                id,
                score: id as f32,
            }),
            3,
        );
        assert_eq!(
            merged.iter().map(|hit| hit.id).collect::<Vec<_>>(),
            [9, 8, 7]
        );
    }
}
