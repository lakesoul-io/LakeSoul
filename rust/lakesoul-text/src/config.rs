// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Text index configuration.

use serde::Deserialize;

use crate::error::{Result, TextError};

/// Text index configuration, parsed from a `text_index_columns` table
/// property entry.
///
/// ```json
/// [{"column": "body", "tokenizer": "jieba", "with_positions": true}]
/// ```
///
/// Fields:
/// - `column` (required): text column to index.
/// - `tokenizer` (default `"jieba"`): tokenizer name registered on the
///   index (`"jieba"`, `"default"`, `"en_stem"`, `"whitespace"`, `"raw"`).
/// - `with_positions` (default true): index term positions so phrase
///   queries work; disabling shrinks the index but drops phrase support.
/// - `stored` (default false): also store the original text in the index
///   (needed for snippets, not for search).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextIndexConfig {
    /// Text column name (the Arrow schema field).
    pub column_name: String,
    /// Tokenizer registered on the index.
    pub tokenizer: String,
    /// Whether to index term positions (phrase queries).
    pub with_positions: bool,
    /// Whether to store the original text in the index.
    pub stored: bool,
}

impl Default for TextIndexConfig {
    fn default() -> Self {
        Self {
            column_name: String::new(),
            tokenizer: DEFAULT_TOKENIZER.to_string(),
            with_positions: true,
            stored: false,
        }
    }
}

/// Tokenizer used when a config does not name one.
pub const DEFAULT_TOKENIZER: &str = "jieba";

fn default_tokenizer() -> String {
    DEFAULT_TOKENIZER.to_string()
}

fn default_with_positions() -> bool {
    true
}

fn default_stored() -> bool {
    false
}

/// Intermediate type deserializing one `text_index_columns` JSON entry.
#[derive(Debug, Deserialize)]
struct JsonEntry {
    column: String,
    #[serde(default = "default_tokenizer")]
    tokenizer: String,
    #[serde(default = "default_with_positions")]
    with_positions: bool,
    #[serde(default = "default_stored")]
    stored: bool,
}

impl From<JsonEntry> for TextIndexConfig {
    fn from(entry: JsonEntry) -> Self {
        Self {
            column_name: entry.column,
            tokenizer: entry.tokenizer,
            with_positions: entry.with_positions,
            stored: entry.stored,
        }
    }
}

impl TextIndexConfig {
    /// Parse a `text_index_columns` property value.
    ///
    /// The value may be a single JSON object, a JSON array, or a JSON string
    /// containing either.  Empty or missing values yield an empty list.
    pub fn parse_json(value: &str) -> Result<Vec<Self>> {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }
        let json: serde_json::Value = serde_json::from_str(trimmed)?;
        let json = match json {
            serde_json::Value::String(inner) => {
                let inner = inner.trim();
                if inner.is_empty() {
                    return Ok(Vec::new());
                }
                serde_json::from_str(inner)?
            }
            other => other,
        };
        let configs: Vec<TextIndexConfig> = match json {
            serde_json::Value::Array(_) => {
                let entries: Vec<JsonEntry> = serde_json::from_value(json)?;
                entries.into_iter().map(Into::into).collect()
            }
            serde_json::Value::Object(_) => {
                let entry: JsonEntry = serde_json::from_value(json)?;
                vec![entry.into()]
            }
            other => {
                return Err(TextError::Invalid(format!(
                    "text_index_columns must be a JSON object or array, got {other}"
                )));
            }
        };
        for config in &configs {
            config.validate()?;
        }
        Ok(configs)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.column_name.trim().is_empty() {
            return Err(TextError::Invalid(
                "text index column name must not be empty".to_string(),
            ));
        }
        if self.tokenizer.trim().is_empty() {
            return Err(TextError::Invalid(format!(
                "text index column '{}' must name a tokenizer",
                self.column_name
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_jieba_with_positions() {
        let configs = TextIndexConfig::parse_json(r#"[{"column":"body"}]"#).unwrap();
        assert_eq!(configs.len(), 1);
        assert_eq!(configs[0].column_name, "body");
        assert_eq!(configs[0].tokenizer, "jieba");
        assert!(configs[0].with_positions);
        assert!(!configs[0].stored);
    }

    #[test]
    fn parses_single_object_and_overrides() {
        let configs = TextIndexConfig::parse_json(
            r#"{"column":"title","tokenizer":"en_stem","with_positions":false}"#,
        )
        .unwrap();
        assert_eq!(configs[0].tokenizer, "en_stem");
        assert!(!configs[0].with_positions);
    }

    #[test]
    fn parses_a_json_string_containing_an_array() {
        let raw = serde_json::to_string(r#"[{"column":"body"}]"#).unwrap();
        let configs = TextIndexConfig::parse_json(&raw).unwrap();
        assert_eq!(configs[0].column_name, "body");
    }

    #[test]
    fn empty_input_yields_no_configs() {
        assert!(TextIndexConfig::parse_json("").unwrap().is_empty());
        assert!(TextIndexConfig::parse_json("  ").unwrap().is_empty());
    }

    #[test]
    fn rejects_empty_column() {
        let error = TextIndexConfig::parse_json(r#"[{"column":""}]"#).unwrap_err();
        assert!(error.to_string().contains("column"), "{error}");
    }
}
