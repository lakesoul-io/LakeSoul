// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Shared index resolution helpers for the PyO3 bindings.
//!
//! The caller-side reader passes `{kind}_search_*` options; this module
//! resolves the index commits behind the data files from the metadata
//! catalog and holds reader leases for them, so the native reader can search
//! without any catalog access of its own.

use std::sync::Arc;
use std::time::Duration;

use lakesoul_common::IndexKind;
use lakesoul_io::index::IndexLease;
use lakesoul_io::index::commit::ResolvedIndex;
use lakesoul_metadata::index_catalog::{CatalogSegment, IndexCatalog, IndexCommitView};

/// Find an option by key in the reader option list.
pub(crate) fn option_value<'a>(
    options: &'a Option<Vec<(String, String)>>,
    key: &str,
) -> Option<&'a str> {
    options
        .as_ref()?
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

/// TTL of a reader lease; env `LAKESOUL_INDEX_LEASE_TTL_SECS` (with the
/// legacy vector-specific variable as fallback).
pub(crate) fn lease_ttl() -> Duration {
    let seconds = std::env::var("LAKESOUL_INDEX_LEASE_TTL_SECS")
        .ok()
        .or_else(|| std::env::var("LAKESOUL_VECTOR_INDEX_LEASE_TTL_SECS").ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(300);
    Duration::from_secs(seconds)
}

/// Lease owner identity: `hostname:pid`.
pub(crate) fn lease_owner() -> String {
    format!(
        "{}:{}",
        std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
        std::process::id()
    )
}

/// Parent directory of a data file URL, scheme preserved.
pub(crate) fn derive_prefix_from_url(url: &str) -> String {
    let url = url.trim_end_matches('/');
    // file:// URL: use path-based parent
    if let Some(rest) = url.strip_prefix("file://") {
        return std::path::Path::new(rest)
            .parent()
            .and_then(|p| p.to_str())
            .map(|s| format!("file://{}", s))
            .unwrap_or_else(|| url.to_string());
    }
    // s3:// or s3a:// URL: find parent directory after the bucket
    for scheme in &["s3://", "s3a://"] {
        if let Some(rest) = url.strip_prefix(scheme) {
            // rest = "bucket/prefix/file.parquet" or just "bucket/file.parquet"
            let parts: Vec<&str> = rest.splitn(2, '/').collect();
            if parts.len() < 2 {
                // Just bucket, no path — return scheme + bucket
                return format!("{}{}", scheme, rest);
            }
            let bucket = parts[0]; // "bucket"
            let path = parts[1]; // "prefix/file.parquet" or "file.parquet"
            return match std::path::Path::new(path).parent().and_then(|p| p.to_str()) {
                Some(parent) if !parent.is_empty() => {
                    format!("{}{}/{}", scheme, bucket, parent)
                }
                _ => format!("{}{}", scheme, bucket),
            };
        }
    }
    // Fallback for unknown schemes
    url.to_string()
}

/// Resolve and lease the index commits of the shards behind `file_urls`.
///
/// A request is selected only when both `{kind}_search_column` and
/// `{kind}_search_query` are present.  `to_segments` converts the kind's
/// catalog segments into the io transport payload (JSON); a `None` result
/// skips the shard with a warning instead of failing the read.
pub(crate) async fn resolve_index_shards<S, C>(
    kind: IndexKind,
    catalog: &IndexCatalog<S>,
    file_urls: &[String],
    options: &Option<Vec<(String, String)>>,
    to_segments: C,
) -> (Vec<ResolvedIndex>, Vec<Arc<IndexLease>>)
where
    S: CatalogSegment,
    C: Fn(&IndexCommitView<S>) -> Option<serde_json::Value>,
{
    let column_key = format!("{}_column", kind.option_prefix());
    let query_key = format!("{}_query", kind.option_prefix());
    let Some(column) = option_value(options, &column_key) else {
        return (Vec::new(), Vec::new());
    };
    if option_value(options, &query_key).is_none() {
        return (Vec::new(), Vec::new());
    }
    let Some(first) = file_urls.first() else {
        return (Vec::new(), Vec::new());
    };
    let table_prefix = derive_prefix_from_url(first);
    let prefixes =
        lakesoul_io::index::prefix::derive_index_prefixes(file_urls, &table_prefix, kind, column);
    let mut shards = Vec::with_capacity(prefixes.len());
    let mut leases = Vec::with_capacity(prefixes.len());
    for (index_prefix, _bucket) in prefixes {
        let view = match catalog.resolve_cached(&index_prefix).await {
            Ok(Some(view)) => view,
            Ok(None) => continue,
            Err(error) => {
                log::warn!("failed to resolve {kind} index at '{index_prefix}': {error}");
                continue;
            }
        };
        match catalog
            .acquire_lease(&index_prefix, lease_ttl(), &lease_owner())
            .await
        {
            Ok(Some(handle)) => leases.push(Arc::new(IndexLease::new(handle))),
            Ok(None) => {}
            Err(error) => {
                log::warn!("failed to lease {kind} index at '{index_prefix}': {error}");
            }
        }
        let Some(segments) = to_segments(&view) else {
            log::warn!("failed to serialize {kind} index segments at '{index_prefix}'");
            continue;
        };
        shards.push(ResolvedIndex {
            kind,
            index_prefix,
            commit_id: view.commit_id,
            generation: view.generation,
            version: view.version,
            header: view.header,
            segments,
        });
    }
    (shards, leases)
}
