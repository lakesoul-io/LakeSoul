// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Object-store prefix derivation for index shards.
//!
//! An index shard lives next to the data files it indexes:
//! `{table_prefix}/{kind.directory()}/{column}/{partition_desc}/{bucket_id}/`.
//! The prefix is derived from the data file paths so a reader can locate the
//! index without any extra catalog lookup.

use std::collections::HashSet;
use std::path::Path;

use lakesoul_common::IndexKind;

/// Derive the index shard prefixes from data file paths and a table prefix.
///
/// Extracts the partition directory and hash bucket from each file path and
/// returns one `(index_prefix, bucket_id)` pair per distinct shard, in file
/// order.
pub fn derive_index_prefixes(
    file_paths: &[String],
    table_prefix: &str,
    kind: IndexKind,
    column: &str,
) -> Vec<(String, u32)> {
    let is_s3 = !file_paths.is_empty()
        && (file_paths[0].starts_with("s3://") || file_paths[0].starts_with("s3a://"));
    let prefix = strip_scheme(table_prefix);
    // For S3 paths the first component is the bucket name.  Strip it
    // because the ObjectStore already knows the bucket.
    let store_prefix: &str = if is_s3 {
        prefix
            .split_once('/')
            .map(|(_, rest)| rest)
            .unwrap_or(prefix)
    } else {
        prefix
    };
    let mut seen: HashSet<(String, u32)> = HashSet::new();
    let mut result = Vec::new();
    for file_path in file_paths {
        let Some(bucket_id) = crate::helpers::extract_hash_bucket_id(file_path) else {
            continue;
        };
        let clean_path = strip_scheme(file_path);
        // Strip the bucket from S3 clean paths too, for consistent
        // relative-path computation.
        let store_clean_path: &str = if is_s3 {
            clean_path
                .split_once('/')
                .map(|(_, rest)| rest)
                .unwrap_or(clean_path)
        } else {
            clean_path
        };
        let relative = store_clean_path
            .strip_prefix(store_prefix)
            .unwrap_or(store_clean_path)
            .trim_start_matches('/');
        let parent_dir = Path::new(relative)
            .parent()
            .and_then(|p| p.to_str())
            .unwrap_or("");
        let partition_desc = if parent_dir.is_empty() {
            "-5".to_string()
        } else {
            parent_dir.to_string()
        };
        let key = (partition_desc, bucket_id);
        if seen.insert(key.clone()) {
            result.push((
                format!(
                    "{}/{}/{}/{}/{}",
                    store_prefix.trim_end_matches('/'),
                    kind.directory(),
                    column,
                    key.0,
                    key.1
                ),
                bucket_id,
            ));
        }
    }
    result
}

/// Derive the index store prefix for the shard containing `file_paths`.
///
/// All files of a shard share the same partition directory, so the first
/// file determines the shard's `{kind.directory()}/{column}/...` prefix
/// (matching how the search path locates the index).
pub fn shard_index_prefix(
    file_paths: &[String],
    kind: IndexKind,
    column: &str,
) -> String {
    let prefix = file_paths
        .first()
        .and_then(|url| {
            let clean = strip_scheme(url);
            Path::new(clean.trim_end_matches('/'))
                .parent()?
                .to_str()
                .map(|s| s.to_string())
        })
        .unwrap_or_default();
    derive_index_prefixes(file_paths, &prefix, kind, column)
        .first()
        .map(|(prefix, _)| prefix.clone())
        .unwrap_or_else(|| format!("{}/{column}/-5/0/", kind.directory()))
}

/// Strip the URL scheme from a path; the object store already knows the
/// scheme and, for S3, the bucket is handled separately by callers.
pub fn strip_scheme(path: &str) -> &str {
    path.trim_start_matches("file://")
        .trim_start_matches("s3://")
        .trim_start_matches("s3a://")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_prefixes_from_local_files() {
        let files = vec![
            "/tmp/table/part=1/part-x_0.parquet".to_string(),
            "/tmp/table/part=1/part-y_0.parquet".to_string(),
        ];
        let prefixes =
            derive_index_prefixes(&files, "/tmp/table", IndexKind::Vector, "vec");
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0].0, "/tmp/table/_vector_index/vec/part=1/0");
        assert_eq!(prefixes[0].1, 0);
    }

    #[test]
    fn derive_prefixes_strips_s3_bucket() {
        let files = vec!["s3://bucket/table/part-x_0.parquet".to_string()];
        let prefixes =
            derive_index_prefixes(&files, "s3://bucket/table", IndexKind::Vector, "vec");
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0].0, "table/_vector_index/vec/-5/0");
    }

    #[test]
    fn prefixes_use_the_kind_directory() {
        let files = vec!["/tmp/table/part-x_3.parquet".to_string()];
        let prefixes =
            derive_index_prefixes(&files, "/tmp/table", IndexKind::Text, "body");
        assert_eq!(prefixes[0].0, "/tmp/table/_text_index/body/-5/3");
    }

    #[test]
    fn shard_prefix_falls_back_without_bucket_ids() {
        let files = vec!["/tmp/table/part-x.parquet".to_string()];
        let prefix = shard_index_prefix(&files, IndexKind::Vector, "vec");
        assert_eq!(prefix, "_vector_index/vec/-5/0/");
    }
}
