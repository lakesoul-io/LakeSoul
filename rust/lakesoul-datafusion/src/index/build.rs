// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Shared build orchestration for index kinds.
//!
//! The kind-specific drivers keep their own builder invocation (each kind
//! has different config and artifact types), but share how committed files
//! are grouped into shards, how delta/rebuild is planned and how the built
//! segments are published.

use std::collections::HashMap;

use lakesoul_common::IndexKind;
use lakesoul_io::helpers::extract_hash_bucket_id;
use lakesoul_io::index::prefix::shard_index_prefix;
use lakesoul_metadata::index_catalog::{
    CatalogSegment, CommitMode, IndexCatalog, IndexCommitView,
};
use rootcause::report;

use crate::Result;

/// Files of one index shard, grouped by range partition and hash bucket.
#[derive(Debug, Clone)]
pub struct ShardFiles {
    pub partition_desc: String,
    pub bucket: u32,
    pub files: Vec<String>,
}

/// Group the newly committed files of a write by `(partition_desc, bucket)`.
///
/// Files from different range partitions must never share a shard.  The
/// result is sorted by `(partition_desc, bucket)` so builds are
/// deterministic.
pub fn group_shard_files(
    partition_files: &HashMap<String, (Vec<String>, u64)>,
) -> Vec<ShardFiles> {
    let mut shards: HashMap<(String, u32), Vec<String>> = HashMap::new();
    for (partition_desc, (files, _)) in partition_files {
        for file in files {
            if let Some(bucket) = extract_hash_bucket_id(file) {
                shards
                    .entry((partition_desc.clone(), bucket))
                    .or_default()
                    .push(file.clone());
            }
        }
    }
    let mut grouped: Vec<ShardFiles> = shards
        .into_iter()
        .map(|((partition_desc, bucket), files)| ShardFiles {
            partition_desc,
            bucket,
            files,
        })
        .collect();
    grouped.sort_by(|a, b| {
        a.partition_desc
            .cmp(&b.partition_desc)
            .then(a.bucket.cmp(&b.bucket))
    });
    grouped
}

/// Group every active file of a table into its shard prefix.
pub fn group_files_by_shard(
    all_files: &[String],
    kind: IndexKind,
    column: &str,
) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for file in all_files {
        if extract_hash_bucket_id(file).is_none() {
            continue;
        }
        let prefix = shard_index_prefix(std::slice::from_ref(file), kind, column);
        map.entry(prefix).or_default().push(file.clone());
    }
    map
}

/// The file set, base commit and commit mode chosen for one shard build.
#[derive(Debug, Clone)]
pub struct ShardBuildPlan<S> {
    pub files: Vec<String>,
    pub base: Option<IndexCommitView<S>>,
    pub mode: CommitMode,
}

/// Decide how to build one shard from its resolved commit.
///
/// * rebuild requested → re-read all of the shard's active files without a
///   base,
/// * commit exists → append the new files as a delta,
/// * no commit → build fresh from the shard's active files.
pub fn plan_shard_build<S: Clone>(
    resolved: Option<&IndexCommitView<S>>,
    should_rebuild: bool,
    full_shard_files: Option<Vec<String>>,
    bucket_files: Vec<String>,
) -> ShardBuildPlan<S> {
    match (resolved, should_rebuild) {
        (_, true) => ShardBuildPlan {
            files: full_shard_files.unwrap_or(bucket_files),
            base: None,
            mode: CommitMode::Rebuild,
        },
        (Some(view), false) => ShardBuildPlan {
            files: bucket_files,
            base: Some(view.clone()),
            mode: CommitMode::Delta,
        },
        (None, false) => ShardBuildPlan {
            files: full_shard_files.unwrap_or(bucket_files),
            base: None,
            mode: CommitMode::Rebuild,
        },
    }
}

/// Publish a built shard's segments if it produced any.
///
/// Returns whether a commit was published.
pub async fn commit_if_non_empty<S: CatalogSegment>(
    catalog: &IndexCatalog<S>,
    index_prefix: &str,
    header: &[u8],
    segments: &[S],
    mode: CommitMode,
) -> Result<bool> {
    if segments.is_empty() {
        return Ok(false);
    }
    catalog
        .commit(index_prefix, header, segments, mode)
        .await
        .map_err(|error| {
            report!("failed to commit index at '{index_prefix}': {error}")
        })?;
    Ok(true)
}
