// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Shared garbage collection for index kinds.
//!
//! The catalog knows which files the retained commits reference; this module
//! sweeps the shard directory for unreferenced files older than the grace
//! period.  The file-naming filter is kind-specific (vector segment files
//! vs text split files), so it is passed in by the caller.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use lakesoul_metadata::index_catalog::{
    CatalogSegment, IndexCatalog, normalize_index_prefix,
};
use object_store::ObjectStore;
use object_store::ObjectStoreExt;

use crate::Result;

/// Default grace period before superseded index files may be deleted.
pub const DEFAULT_GC_GRACE_SECONDS: u64 = 3600;

/// Environment variable overriding how often the write path runs its
/// amortized shard GC (every N writes, non-zero grace only).
pub const ENV_INDEX_GC_EVERY: &str = "LAKESOUL_INDEX_GC_EVERY";

/// Default write-path GC interval (every 16th write per process).
pub const DEFAULT_GC_EVERY_WRITES: u64 = 16;

/// Whether a write-path shard GC should run now.
///
/// A write can only make its just-committed files superseded, and the grace
/// period protects those; with a non-zero grace period every write would
/// otherwise run a full shard-directory scan that cannot collect anything.
/// Amortize it over writes instead.  With grace zero the caller explicitly
/// asked for immediate cleanup, so it runs on every write.
pub fn should_run_write_gc(grace_seconds: u64) -> bool {
    if grace_seconds == 0 {
        return true;
    }
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let every = std::env::var(ENV_INDEX_GC_EVERY)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_GC_EVERY_WRITES);
    let write = WRITES.fetch_add(1, Ordering::Relaxed);
    write.is_multiple_of(every)
}

/// Knobs for an explicit garbage collection run.
#[derive(Debug, Clone)]
pub struct IndexGcOptions {
    pub grace_seconds: u64,
    pub keep_generations: usize,
    /// Delete control-plane rows of shards whose directory is gone.
    pub drop_orphan_shards: bool,
}

impl Default for IndexGcOptions {
    fn default() -> Self {
        Self {
            grace_seconds: DEFAULT_GC_GRACE_SECONDS,
            keep_generations: 1,
            drop_orphan_shards: true,
        }
    }
}

/// What a garbage collection run did.
#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct IndexGcReport {
    pub shards_scanned: usize,
    pub commits_deleted: u64,
    pub objects_deleted: u64,
    pub bytes_deleted: u64,
    pub orphan_shards_deleted: u64,
}

/// List the objects of a shard that may be removed: files of this index kind
/// that are unreferenced and last modified before the grace cutoff.
async fn sweep_shard_objects<F: Fn(&str) -> bool>(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    retained: &HashSet<String>,
    grace: Duration,
    keep_file: &F,
) -> Result<(u64, u64)> {
    use futures::StreamExt;
    let path = object_store::path::Path::from(prefix.trim_end_matches('/'));
    let cutoff = chrono::DateTime::<chrono::Utc>::from(SystemTime::now() - grace);
    let mut deleted_objects = 0u64;
    let mut deleted_bytes = 0u64;
    let mut stream = store.list(Some(&path));
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        let name = meta.location.filename().unwrap_or_default().to_string();
        // Only ever delete our own index files.
        if !keep_file(&name) {
            continue;
        }
        if retained.contains(&name) {
            continue;
        }
        if meta.last_modified > cutoff {
            continue;
        }
        store.delete(&meta.location).await?;
        deleted_objects += 1;
        deleted_bytes += meta.size;
    }
    Ok((deleted_objects, deleted_bytes))
}

async fn shard_directory_is_empty(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> Result<bool> {
    use futures::StreamExt;
    let path = object_store::path::Path::from(prefix.trim_end_matches('/'));
    let mut stream = store.list(Some(&path));
    Ok(stream.next().await.is_none())
}

/// Garbage collect one shard: drop expired leases and superseded
/// generations in the catalog, then delete the objects they referenced.
pub async fn gc_shard_now<S: CatalogSegment, F: Fn(&str) -> bool>(
    store: &Arc<dyn ObjectStore>,
    catalog: &IndexCatalog<S>,
    prefix: &str,
    grace: Duration,
    keep_generations: usize,
    keep_file: &F,
) -> Result<IndexGcReport> {
    let mut report = IndexGcReport::default();
    let plan = catalog.gc_shard(prefix, grace, keep_generations).await?;
    report.commits_deleted = plan.deleted_commit_rows;
    if plan.deleted_commit_rows == 0 && plan.removed_filenames.is_empty() {
        return Ok(report);
    }
    let retained: HashSet<String> = plan.retained_filenames.into_iter().collect();
    let (objects, bytes) =
        sweep_shard_objects(store, prefix, &retained, grace, keep_file).await?;
    report.objects_deleted = objects;
    report.bytes_deleted = bytes;
    Ok(report)
}

/// Garbage collect the index files of a table.
///
/// Drops expired leases and superseded generations from the catalog and
/// deletes their files once they are older than the grace period, so readers
/// that resolved an older commit keep working.  Shards whose directory is
/// gone (dropped partitions) have their control-plane rows removed as well.
pub async fn gc_index_shards<S: CatalogSegment, F: Fn(&str) -> bool>(
    store: &Arc<dyn ObjectStore>,
    catalog: &IndexCatalog<S>,
    table_path: &str,
    options: &IndexGcOptions,
    keep_file: &F,
) -> Result<IndexGcReport> {
    let grace = Duration::from_secs(options.grace_seconds);
    let table_prefix = normalize_index_prefix(table_path);
    let shards = catalog.list_shards_under(&table_prefix).await?;
    let mut report = IndexGcReport::default();
    for prefix in shards {
        report.shards_scanned += 1;
        let plan = gc_shard_now(
            store,
            catalog,
            &prefix,
            grace,
            options.keep_generations,
            keep_file,
        )
        .await?;
        report.commits_deleted += plan.commits_deleted;
        report.objects_deleted += plan.objects_deleted;
        report.bytes_deleted += plan.bytes_deleted;
        if options.drop_orphan_shards {
            let has_commit = catalog.resolve(&prefix).await?.is_some();
            if !has_commit && shard_directory_is_empty(store, &prefix).await? {
                catalog.delete_shard(&prefix).await?;
                report.orphan_shards_deleted += 1;
            }
        }
    }
    Ok(report)
}
