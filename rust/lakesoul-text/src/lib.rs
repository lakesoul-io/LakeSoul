// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! LakeSoul full-text index core.
//!
//! Builds Tantivy-based, self-contained index splits for a text column and
//! searches them.  The crate is deliberately independent of the LakeSoul IO
//! and metadata layers: a split is built from `(primary key, text)` rows,
//! persisted as a single immutable object, and searched standalone.
//!
//! ```text
//! {table dir}/_text_index/{column}/{partition_desc}/{bucket_id}/{split_id}.split
//! ```

pub mod bm25;
pub mod config;
pub mod error;
pub mod schema;
pub mod search;
pub mod split;
pub mod tokenizer;
pub mod verify;

pub use bm25::{
    CorpusStats, bm25_scores, bm25_scores_with_terms, collect_index_stats,
    is_plain_query, query_terms, stats_for_rows, tokenize,
};
pub use config::TextIndexConfig;
pub use error::{Result, TextError};
pub use schema::{PK_FIELD, TEXT_FIELD, TextSchema};
pub use search::{TextHit, merge_hits, search_index, search_index_with, search_splits};
pub use split::{
    DEFAULT_WRITER_MEMORY_BUDGET, ENV_SPLIT_CACHE_DIR, SPLIT_FORMAT_VERSION, SplitCache,
    TextSplitEntry, drift_exceeds_threshold, write_split,
};
pub use tokenizer::{SUPPORTED_TOKENIZERS, is_supported, register_tokenizers};
pub use verify::{
    matching_ids, matching_ids_with, matching_scores, matching_scores_with,
};

/// Re-exported so index integrations can name an opened split's index type
/// without depending on Tantivy directly.
pub use tantivy;
