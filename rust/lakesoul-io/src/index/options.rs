// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Reader-option parsing for index searches.
//!
//! Each kind selects its index with `{kind.option_prefix()}_column` and
//! `{kind.option_prefix()}_query`, plus `..._top_k` (default 10).  Kind
//! specific extras (e.g. `vector_search_nprobe`) are read by the kind's own
//! search code from the same option map.

use lakesoul_common::IndexKind;

use crate::config::LakeSoulIOConfig;

/// A search request parsed from the reader options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    pub kind: IndexKind,
    /// Indexed column to search.
    pub column: String,
    /// Raw query string; parsed by the kind's search code.
    pub query: String,
    /// Maximum number of candidates to retrieve.
    pub top_k: usize,
}

/// Option key of a kind-specific setting, e.g. `vector_search_nprobe`.
pub fn option_key(kind: IndexKind, suffix: &str) -> String {
    format!("{}_{}", kind.option_prefix(), suffix)
}

impl SearchRequest {
    /// Parse the request of one kind, if the reader options select it.
    ///
    /// A request is selected only when both the column and query options
    /// are present, so an accidentally set column never turns into a search
    /// against an empty query.
    pub fn from_config(config: &LakeSoulIOConfig, kind: IndexKind) -> Option<Self> {
        let column = config.option(&option_key(kind, "column"))?;
        let query = config.option(&option_key(kind, "query"))?;
        let top_k = config
            .option(&option_key(kind, "top_k"))
            .and_then(|value| value.parse().ok())
            .unwrap_or(10);
        Some(Self {
            kind,
            column,
            query,
            top_k,
        })
    }
}

/// Parse every index search request selected by the reader options.
pub fn parse_search_requests(config: &LakeSoulIOConfig) -> Vec<SearchRequest> {
    IndexKind::ALL
        .into_iter()
        .filter_map(|kind| SearchRequest::from_config(config, kind))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LakeSoulIOConfigBuilder;

    #[test]
    fn parses_vector_request_with_default_top_k() {
        let config = LakeSoulIOConfigBuilder::new()
            .with_option("vector_search_column".to_string(), "vec".to_string())
            .with_option("vector_search_query".to_string(), "1,2".to_string())
            .build();
        let requests = parse_search_requests(&config);
        assert_eq!(
            requests,
            vec![SearchRequest {
                kind: IndexKind::Vector,
                column: "vec".to_string(),
                query: "1,2".to_string(),
                top_k: 10,
            }]
        );
    }

    #[test]
    fn ignores_a_column_without_query() {
        let config = LakeSoulIOConfigBuilder::new()
            .with_option("vector_search_column".to_string(), "vec".to_string())
            .build();
        assert!(parse_search_requests(&config).is_empty());
    }

    #[test]
    fn parses_top_k_override() {
        let config = LakeSoulIOConfigBuilder::new()
            .with_option("text_search_column".to_string(), "body".to_string())
            .with_option("text_search_query".to_string(), "hello".to_string())
            .with_option("text_search_top_k".to_string(), "42".to_string())
            .build();
        let requests = parse_search_requests(&config);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].kind, IndexKind::Text);
        assert_eq!(requests[0].top_k, 42);
    }
}
