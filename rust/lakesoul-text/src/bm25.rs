// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Corpus-level BM25 statistics and consistent cross-shard scoring.
//!
//! Tantivy scores each index with its own document frequencies and average
//! field length.  A sharded text index therefore ranks with per-shard
//! statistics, which makes the merged order approximate.  This module
//! collects the statistics of every shard, adds the corpus-level numbers for
//! exact candidate scoring over current rows, and computes one BM25 score per
//! document against the **global** statistics — the `dfs_query_then_fetch`
//! behaviour of a distributed search engine.
//!
//! Only plain (OR-term) queries can be scored this way; queries with
//! explicit syntax keep Tantivy's parser and per-shard scores.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use tantivy::schema::FieldType;
use tantivy::tokenizer::{TextAnalyzer, TokenStream};
use tantivy::{Index, Term};

use crate::config::TextIndexConfig;
use crate::error::{Result, TextError};
use crate::schema::TextSchema;
use crate::search::has_query_syntax;
use crate::tokenizer::register_tokenizers;

/// BM25 term-frequency saturation (`k1`) and length-normalization (`b`)
/// parameters, matching Tantivy's defaults.
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;

/// Corpus-level statistics over every indexed document.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CorpusStats {
    /// Number of documents.
    pub num_docs: u64,
    /// Total number of text tokens across all documents.
    pub total_tokens: u64,
    /// Document frequency of the query terms (term → documents containing it).
    pub doc_freq: HashMap<String, u64>,
}

impl CorpusStats {
    /// Fold another shard's statistics into this one.
    pub fn merge(&mut self, other: CorpusStats) {
        self.num_docs += other.num_docs;
        self.total_tokens += other.total_tokens;
        for (term, frequency) in other.doc_freq {
            *self.doc_freq.entry(term).or_insert(0) += frequency;
        }
    }

    /// Average document length in tokens (`0.0` for an empty corpus).
    pub fn average_doc_len(&self) -> f64 {
        if self.num_docs == 0 {
            0.0
        } else {
            self.total_tokens as f64 / self.num_docs as f64
        }
    }

    /// Whether the statistics describe an empty corpus.
    pub fn is_empty(&self) -> bool {
        self.num_docs == 0
    }
}

/// Whether a query can be scored with global BM25 statistics.
pub fn is_plain_query(query: &str) -> bool {
    !has_query_syntax(query)
}

/// One analyzed token with its byte offsets in the source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenSpan {
    pub text: String,
    pub start: usize,
    pub end: usize,
}

/// Tokenize `text` with the analyzer of `config`, keeping byte offsets.
pub fn token_spans(config: &TextIndexConfig, text: &str) -> Result<Vec<TokenSpan>> {
    let mut analyzer = analyzer_for(config)?;
    let mut stream = analyzer.token_stream(text);
    let mut spans = Vec::new();
    while stream.advance() {
        let token = stream.token();
        if !token.text.is_empty() {
            spans.push(TokenSpan {
                text: token.text.clone(),
                start: token.offset_from,
                end: token.offset_to,
            });
        }
    }
    Ok(spans)
}

/// Tokenize `text` with the analyzer of `config`.
pub fn tokenize(config: &TextIndexConfig, text: &str) -> Result<Vec<String>> {
    let mut analyzer = analyzer_for(config)?;
    let mut stream = analyzer.token_stream(text);
    let mut tokens = Vec::new();
    while stream.advance() {
        let token = stream.token();
        if !token.text.is_empty() {
            tokens.push(token.text.clone());
        }
    }
    Ok(tokens)
}

/// Validate that a plain query contains at least one token.
pub fn query_terms(config: &TextIndexConfig, query: &str) -> Result<Vec<String>> {
    let mut terms = tokenize(config, query)?;
    terms.dedup();
    Ok(terms)
}

/// Collect the statistics of one Tantivy index (all splits of a shard).
pub fn collect_index_stats(index: &Index, terms: &[String]) -> Result<CorpusStats> {
    let text_schema = TextSchema::resolve(&index.schema())?;
    let reader = index.reader()?;
    let searcher = reader.searcher();
    let mut stats = CorpusStats {
        num_docs: searcher.num_docs(),
        total_tokens: 0,
        doc_freq: HashMap::new(),
    };
    for segment in searcher.segment_readers() {
        stats.total_tokens += segment
            .inverted_index(text_schema.text_field)?
            .total_num_tokens();
    }
    for term in terms {
        let query_term = Term::from_field_text(text_schema.text_field, term);
        let frequency = searcher.doc_freq(&query_term)?;
        if frequency > 0 {
            stats.doc_freq.insert(term.clone(), frequency);
        }
    }
    Ok(stats)
}

/// Statistics contributed by a set of `(primary key, text)` rows that are
/// not part of any index commit (the deferred-maintenance tail).
pub fn stats_for_rows(
    config: &TextIndexConfig,
    rows: &[(u64, Option<String>)],
) -> Result<CorpusStats> {
    let mut stats = CorpusStats::default();
    for (_, text) in rows {
        let Some(text) = text.as_deref().filter(|text| !text.is_empty()) else {
            continue;
        };
        let tokens = tokenize(config, text)?;
        if tokens.is_empty() {
            continue;
        }
        stats.num_docs += 1;
        stats.total_tokens += tokens.len() as u64;
        let mut seen = std::collections::HashSet::new();
        for token in tokens {
            if seen.insert(token.clone()) {
                *stats.doc_freq.entry(token).or_insert(0) += 1;
            }
        }
    }
    Ok(stats)
}

/// Global BM25 scores of the rows that match a plain query.
///
/// The score of a document is the same no matter which shard returned it,
/// because every term's document frequency and the average document length
/// come from `stats`.
pub fn bm25_scores(
    config: &TextIndexConfig,
    rows: &[(u64, Option<String>)],
    query: &str,
    stats: &CorpusStats,
) -> Result<HashMap<u64, f32>> {
    if !is_plain_query(query) {
        return Ok(HashMap::new());
    }
    let terms = query_terms(config, query)?;
    bm25_scores_with_terms(config, rows, &terms, stats)
}

/// Like [`bm25_scores`], with query terms already analyzed (a query-time
/// analyzer override analyzes the query text differently from the rows,
/// which keep the index-time analyzer).
pub fn bm25_scores_with_terms(
    config: &TextIndexConfig,
    rows: &[(u64, Option<String>)],
    terms: &[String],
    stats: &CorpusStats,
) -> Result<HashMap<u64, f32>> {
    let mut scores = HashMap::new();
    if rows.is_empty() || stats.is_empty() || terms.is_empty() {
        return Ok(scores);
    }
    let average_len = stats.average_doc_len();
    let num_docs = stats.num_docs as f64;
    for (id, text) in rows {
        let Some(text) = text.as_deref().filter(|text| !text.is_empty()) else {
            continue;
        };
        let tokens = tokenize(config, text)?;
        if tokens.is_empty() {
            continue;
        }
        let doc_len = tokens.len() as f64;
        let mut frequencies: HashMap<&str, u64> = HashMap::new();
        for token in &tokens {
            *frequencies.entry(token.as_str()).or_insert(0) += 1;
        }
        let mut score = 0.0f64;
        let mut matched = false;
        for term in terms {
            let Some(frequency) = frequencies.get(term.as_str()).copied() else {
                continue;
            };
            matched = true;
            let doc_freq = stats.doc_freq.get(term).copied().unwrap_or(0) as f64;
            let idf = (1.0 + (num_docs - doc_freq + 0.5) / (doc_freq + 0.5)).ln();
            let frequency = frequency as f64;
            let denominator =
                frequency + BM25_K1 * (1.0 - BM25_B + BM25_B * doc_len / average_len);
            score += idf * frequency * (BM25_K1 + 1.0) / denominator;
        }
        if matched {
            scores.insert(*id, score as f32);
        }
    }
    Ok(scores)
}

/// Analyzer registry: building one (especially jieba) is not free, and the
/// analyzer only depends on the tokenizer name.
fn analyzer_for(config: &TextIndexConfig) -> Result<TextAnalyzer> {
    static ANALYZERS: OnceLock<Mutex<HashMap<String, TextAnalyzer>>> = OnceLock::new();
    let cache = ANALYZERS.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(analyzer) = cache
        .lock()
        .expect("analyzer cache poisoned")
        .get(&config.tokenizer)
        .cloned()
    {
        return Ok(analyzer);
    }
    let text_schema = TextSchema::build(config);
    let index = Index::create_in_ram(text_schema.schema.clone());
    register_tokenizers(&index);
    let schema = index.schema();
    let entry = schema.get_field_entry(text_schema.text_field);
    let FieldType::Str(options) = entry.field_type() else {
        return Err(TextError::Invalid(
            "the text field is not a string field".to_string(),
        ));
    };
    let Some(indexing) = options.get_indexing_options() else {
        return Err(TextError::Invalid(
            "the text field is not indexed".to_string(),
        ));
    };
    let analyzer = index
        .tokenizers()
        .get(indexing.tokenizer())
        .ok_or_else(|| {
            TextError::Invalid(format!(
                "tokenizer '{}' is not registered",
                indexing.tokenizer()
            ))
        })?;
    cache
        .lock()
        .expect("analyzer cache poisoned")
        .insert(config.tokenizer.clone(), analyzer.clone());
    Ok(analyzer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::split::write_split;

    fn config() -> TextIndexConfig {
        TextIndexConfig {
            column_name: "text".to_string(),
            tokenizer: "default".to_string(),
            with_positions: true,
            stored: false,
        }
    }

    fn stats_of(documents: &[(u64, String)], terms: &[String]) -> CorpusStats {
        let text_schema = TextSchema::build(&config());
        let index = Index::create_in_ram(text_schema.schema.clone());
        register_tokenizers(&index);
        let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
        for (id, text) in documents {
            let mut document = tantivy::schema::TantivyDocument::new();
            document.add_u64(text_schema.pk_field, *id);
            document.add_text(text_schema.text_field, text);
            writer.add_document(document).unwrap();
        }
        writer.commit().unwrap();
        collect_index_stats(&index, terms).unwrap()
    }

    #[test]
    fn merged_stats_score_like_the_single_index() {
        let left = vec![
            (1u64, "apple pie recipe".to_string()),
            (2u64, "banana bread recipe".to_string()),
        ];
        let right = vec![
            (3u64, "apple apple tart".to_string()),
            (4u64, "cherry cake".to_string()),
        ];
        let terms = vec!["apple".to_string(), "recipe".to_string()];
        let mut merged = stats_of(&left, &terms);
        merged.merge(stats_of(&right, &terms));

        let all: Vec<(u64, String)> = left.iter().chain(right.iter()).cloned().collect();
        let baseline = stats_of(&all, &terms);
        assert_eq!(merged, baseline);

        let rows = vec![
            (1u64, Some("apple pie recipe".to_string())),
            (3u64, Some("apple apple tart".to_string())),
        ];
        let merged_scores =
            bm25_scores(&config(), &rows, "apple recipe", &merged).unwrap();
        let baseline_scores =
            bm25_scores(&config(), &rows, "apple recipe", &baseline).unwrap();
        assert_eq!(merged_scores, baseline_scores);
        // "recipe" is rarer than "apple", so doc 1 must win.
        assert!(merged_scores[&1] > merged_scores[&3], "{merged_scores:?}");
    }

    #[test]
    fn row_stats_include_the_unindexed_tail() {
        let tail = vec![
            (9u64, Some("apple apple pie".to_string())),
            (10u64, Some("banana bread".to_string())),
        ];
        let stats = stats_for_rows(&config(), &tail).unwrap();
        assert_eq!(stats.num_docs, 2);
        assert_eq!(stats.total_tokens, 5);
        assert_eq!(stats.doc_freq.get("apple"), Some(&1));
        assert_eq!(stats.doc_freq.get("banana"), Some(&1));

        let scores = bm25_scores(&config(), &tail, "apple", &stats).unwrap();
        assert!(scores[&9] > scores.get(&10).copied().unwrap_or(0.0));
        assert!(!scores.contains_key(&10));
    }

    #[test]
    fn syntax_queries_are_not_scored_globally() {
        let stats = CorpusStats {
            num_docs: 1,
            total_tokens: 2,
            doc_freq: HashMap::from([("apple".to_string(), 1)]),
        };
        let rows = vec![(1u64, Some("apple pie".to_string()))];
        assert!(
            bm25_scores(&config(), &rows, "\"apple pie\"", &stats)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn tokenize_matches_the_split_analyzer() {
        let tokens = tokenize(&config(), "Apple pie recipe").unwrap();
        // The default tokenizer lowercases.
        assert_eq!(tokens, vec!["apple", "pie", "recipe"]);
        let _ = write_split;
    }
}
