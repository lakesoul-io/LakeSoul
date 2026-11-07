// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Optional short-TTL memoization of the metadata queries every statement
//! repeats.
//!
//! Planning one vector-search statement resolves the table (`table_info` from
//! PostgreSQL) and, while building the scan, lists the table's partitions —
//! two PG round trips of ~0.2-0.4 ms each.  `LAKESOUL_METADATA_TTL_MS` bounds
//! how long a statement may reuse the previous answer:
//!
//! * unset or `0` (default) - disabled, every statement sees the current
//!   metadata, exactly as before;
//! * `N` - a statement may reuse the table info / partition list fetched less
//!   than `N` milliseconds ago, so a concurrent DDL or commit becomes visible
//!   within `N` ms instead of immediately.
//!
//! The cache is process-local and keyed by the metadata client instance, the
//! namespace and the table, so separate warehouses or test clients never share
//! entries.  Measured on the GloVe single-query path (`--rerank index` and
//! `exact`): ~0.36 ms per statement, i.e. +26% index-only / +13% exact QPS at
//! `LAKESOUL_METADATA_TTL_MS=200` with recall unchanged.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use lakesoul_metadata_proto::entity::PartitionInfo;

use crate::lakesoul_table::LakeSoulTable;

/// The configured freshness window, `None` when the cache is disabled.
fn ttl() -> Option<Duration> {
    static TTL: LazyLock<Option<Duration>> = LazyLock::new(|| {
        std::env::var("LAKESOUL_METADATA_TTL_MS")
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .map(Duration::from_millis)
    });
    *TTL
}

struct Entry<T> {
    at: Instant,
    value: T,
}

struct TtlCache<T> {
    entries: Mutex<HashMap<String, Entry<T>>>,
}

impl<T: Clone> TtlCache<T> {
    fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }

    fn get(&self, key: &str, ttl: Duration) -> Option<T> {
        let entries = self.entries.lock().ok()?;
        let entry = entries.get(key)?;
        (entry.at.elapsed() < ttl).then(|| entry.value.clone())
    }

    fn put(&self, key: String, value: T) {
        if let Ok(mut entries) = self.entries.lock() {
            // Bound the map: one entry per table is expected, so a large map
            // means many tables were queried in this process; expire old ones
            // instead of growing without limit.
            if entries.len() > 4096 {
                entries.retain(|_, entry| entry.at.elapsed() < Duration::from_secs(60));
            }
            entries.insert(key, Entry { at: Instant::now(), value });
        }
    }
}

static TABLES: LazyLock<TtlCache<Arc<LakeSoulTable>>> =
    LazyLock::new(TtlCache::new);
static PARTITIONS: LazyLock<TtlCache<Arc<Vec<PartitionInfo>>>> =
    LazyLock::new(TtlCache::new);

/// Resolve one table, reusing an entry fetched less than the configured TTL
/// ago.  `key` must identify the metadata client instance and the table.
pub(crate) async fn table<E, F>(
    key: String,
    fetch: F,
) -> Result<Arc<LakeSoulTable>, E>
where
    F: Future<Output = Result<LakeSoulTable, E>>,
{
    let Some(ttl) = ttl() else {
        return fetch.await.map(Arc::new);
    };
    if let Some(cached) = TABLES.get(&key, ttl) {
        return Ok(cached);
    }
    let table = Arc::new(fetch.await?);
    TABLES.put(key, Arc::clone(&table));
    Ok(table)
}

/// The current partition list of one table, reusing an entry fetched less
/// than the configured TTL ago.
pub(crate) async fn partition_info<E, F>(
    key: String,
    fetch: F,
) -> Result<Arc<Vec<PartitionInfo>>, E>
where
    F: Future<Output = Result<Vec<PartitionInfo>, E>>,
{
    let Some(ttl) = ttl() else {
        return fetch.await.map(Arc::new);
    };
    if let Some(cached) = PARTITIONS.get(&key, ttl) {
        return Ok(cached);
    }
    let partitions = Arc::new(fetch.await?);
    PARTITIONS.put(key, Arc::clone(&partitions));
    Ok(partitions)
}

/// Cache key of a table entry: metadata client instance + namespace + name.
pub(crate) fn table_key(
    client: &lakesoul_metadata::MetaDataClientRef,
    namespace: &str,
    name: &str,
) -> String {
    format!("{:p}:{namespace}:{name}", Arc::as_ptr(client))
}

/// Cache key of a partition-list entry: metadata client instance + table id.
pub(crate) fn partition_key(
    client: &lakesoul_metadata::MetaDataClientRef,
    table_id: &str,
) -> String {
    format!("{:p}:{table_id}", Arc::as_ptr(client))
}
