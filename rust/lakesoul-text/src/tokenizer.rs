// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Tokenizer registration for text index splits.
//!
//! Tantivy pre-registers `default`, `raw`, `en_stem` and `whitespace`; this
//! module adds the Chinese word-segmentation tokenizer `jieba` and must be
//! called both when a split is created and when it is opened for search
//! (the tokenizer is not persisted in the index metadata).

use tantivy::Index;
use tantivy::tokenizer::{LowerCaser, TextAnalyzer};
use tantivy_jieba::JiebaTokenizer;

/// Tokenizer names accepted by [`TextIndexConfig`](crate::TextIndexConfig).
pub const SUPPORTED_TOKENIZERS: &[&str] =
    &["jieba", "default", "en_stem", "whitespace", "raw"];

/// Whether a tokenizer name is supported.
pub fn is_supported(name: &str) -> bool {
    SUPPORTED_TOKENIZERS.contains(&name)
}

/// Register the crate's custom tokenizers on an index.
///
/// The `jieba` analyzer is jieba word segmentation followed by
/// lowercasing, so mixed Chinese/English text matches case-insensitively.
pub fn register_tokenizers(index: &Index) {
    let manager = index.tokenizers();
    let jieba = TextAnalyzer::builder(JiebaTokenizer::default())
        .filter(LowerCaser)
        .build();
    manager.register("jieba", jieba);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jieba_is_supported_and_registered() {
        assert!(is_supported("jieba"));
        assert!(is_supported("default"));
        assert!(!is_supported("nope"));

        let schema = tantivy::schema::SchemaBuilder::new().build();
        let index = Index::create_in_ram(schema);
        register_tokenizers(&index);
        assert!(index.tokenizers().get("jieba").is_some());
    }
}
