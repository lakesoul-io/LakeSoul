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
#[derive(Debug, Clone, PartialEq)]
pub struct SearchRequest {
    pub kind: IndexKind,
    /// Indexed column to search.
    pub column: String,
    /// Raw query string; parsed by the kind's search code.
    pub query: String,
    /// Maximum number of candidates to retrieve.
    pub top_k: usize,
    /// Fusion weight used when several requests of one kind are combined
    /// (candidate sets are intersected and the scores are summed weighted).
    pub weight: f32,
    /// Per-request metric override; `None` uses the request-level option.
    pub metric: Option<String>,
}

/// One entry of the `{kind}_search_requests` JSON option.
#[derive(Debug, Clone, serde::Deserialize)]
struct RawSearchRequest {
    column: String,
    query: String,
    #[serde(default)]
    top_k: Option<usize>,
    #[serde(default)]
    weight: Option<f32>,
    #[serde(default)]
    metric: Option<String>,
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
            weight: 1.0,
            metric: None,
        })
    }
}

/// Parse one kind's requests: the `{kind}_search_requests` JSON list when
/// present (several columns, fused), else the single-column shorthand.
pub fn requests_for_kind(
    config: &LakeSoulIOConfig,
    kind: IndexKind,
) -> Vec<SearchRequest> {
    if let Some(raw) = config.option(&option_key(kind, "requests")) {
        match parse_request_list(config, kind, &raw) {
            Ok(requests) if !requests.is_empty() => return requests,
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(
                    "ignoring invalid {}_search_requests: {error}",
                    kind.option_prefix()
                );
            }
        }
    }
    SearchRequest::from_config(config, kind)
        .into_iter()
        .collect()
}

fn parse_request_list(
    config: &LakeSoulIOConfig,
    kind: IndexKind,
    raw: &str,
) -> Result<Vec<SearchRequest>, serde_json::Error> {
    let default_top_k = config
        .option(&option_key(kind, "top_k"))
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    let default_metric = config.option(&option_key(kind, "metric"));
    let entries: Vec<RawSearchRequest> = serde_json::from_str(raw)?;
    Ok(entries
        .into_iter()
        .map(|entry| SearchRequest {
            kind,
            column: entry.column,
            query: entry.query,
            top_k: entry.top_k.unwrap_or(default_top_k),
            weight: entry.weight.unwrap_or(1.0),
            metric: entry.metric.or_else(|| default_metric.clone()),
        })
        .collect())
}

/// Parse every index search request selected by the reader options.
pub fn parse_search_requests(config: &LakeSoulIOConfig) -> Vec<SearchRequest> {
    IndexKind::ALL
        .into_iter()
        .flat_map(|kind| requests_for_kind(config, kind))
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
                weight: 1.0,
                metric: None,
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
    fn parses_multi_column_requests() {
        let config = LakeSoulIOConfigBuilder::new()
            .with_option(
                "vector_search_requests".to_string(),
                r#"[
                    {"column":"text_vec","query":"0.1,0.2","weight":0.7},
                    {"column":"image_vec","query":"0.3,0.4","top_k":42,"weight":0.3,"metric":"L2"}
                ]"#
                .to_string(),
            )
            .with_option("vector_search_top_k".to_string(), "25".to_string())
            .build();
        let requests = parse_search_requests(&config);
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].column, "text_vec");
        assert_eq!(requests[0].top_k, 25, "request-level top_k is the default");
        assert_eq!(requests[0].weight, 0.7);
        assert_eq!(requests[0].metric, None);
        assert_eq!(requests[1].column, "image_vec");
        assert_eq!(requests[1].top_k, 42);
        assert!((requests[1].weight - 0.3).abs() < 1e-6);
        assert_eq!(requests[1].metric.as_deref(), Some("L2"));
    }

    #[test]
    fn invalid_request_list_falls_back_to_the_shorthand() {
        let config = LakeSoulIOConfigBuilder::new()
            .with_option("vector_search_requests".to_string(), "not json".to_string())
            .with_option("vector_search_column".to_string(), "vec".to_string())
            .with_option("vector_search_query".to_string(), "1,2".to_string())
            .build();
        let requests = parse_search_requests(&config);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].column, "vec");
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
