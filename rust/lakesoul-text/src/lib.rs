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

pub mod config;
pub mod error;
pub mod schema;
pub mod search;
pub mod split;
pub mod tokenizer;
pub mod verify;

pub use config::TextIndexConfig;
pub use error::{Result, TextError};
pub use schema::{PK_FIELD, TEXT_FIELD, TextSchema};
pub use search::{TextHit, merge_hits, search_index, search_splits};
pub use split::{
    DEFAULT_WRITER_MEMORY_BUDGET, ENV_SPLIT_CACHE_DIR, SPLIT_FORMAT_VERSION, SplitCache,
    TextSplitEntry, write_split,
};
pub use tokenizer::{SUPPORTED_TOKENIZERS, is_supported, register_tokenizers};
pub use verify::{matching_ids, matching_scores};

/// Re-exported so index integrations can name an opened split's index type
/// without depending on Tantivy directly.
pub use tantivy;
