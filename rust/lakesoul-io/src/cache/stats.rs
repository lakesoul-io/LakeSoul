// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cache stats
use std::sync::Arc;
use std::{
    fmt::Debug,
    sync::atomic::{AtomicU64, Ordering},
};

// Re-established by the schema-only store wrapper around requests and stream
// polls, including reads dispatched onto Vortex's separate I/O runtime.
tokio::task_local! {
    pub(crate) static SCHEMA_INFERENCE_TIMING: Arc<SchemaInferenceTiming>;
}

#[derive(Debug, Default)]
pub(crate) struct SchemaInferenceTiming {
    pub(crate) files_with_reads: AtomicU64,
    pub(crate) store_read_ns: AtomicU64,
    pub(crate) range_calls: AtomicU64,
    pub(crate) range_ns: AtomicU64,
    pub(crate) requested_bytes: AtomicU64,
    pub(crate) head_ns: AtomicU64,
    pub(crate) range_head_ns: AtomicU64,
    pub(crate) page_hits: AtomicU64,
    pub(crate) page_misses: AtomicU64,
    pub(crate) remote_ns: AtomicU64,
    pub(crate) remote_bytes: AtomicU64,
    pub(crate) lookup_read_ns: AtomicU64,
    pub(crate) cache_write_ns: AtomicU64,
    pub(crate) cache_put_writes: CachePutCounters,
    pub(crate) cache_put_nops: CachePutCounters,
}

impl SchemaInferenceTiming {
    pub(crate) fn log_summary(&self) {
        let ms =
            |counter: &AtomicU64| counter.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        tracing::info!(
            files_with_reads = self.files_with_reads.load(Ordering::Relaxed),
            store_read_sum_ms = ms(&self.store_read_ns),
            range_calls = self.range_calls.load(Ordering::Relaxed),
            range_sum_ms = ms(&self.range_ns),
            requested_bytes = self.requested_bytes.load(Ordering::Relaxed),
            get_opts_head_sum_ms = ms(&self.head_ns),
            range_head_sum_ms = ms(&self.range_head_ns),
            page_hits = self.page_hits.load(Ordering::Relaxed),
            page_misses = self.page_misses.load(Ordering::Relaxed),
            remote_sum_ms = ms(&self.remote_ns),
            remote_bytes = self.remote_bytes.load(Ordering::Relaxed),
            lookup_read_sum_ms = ms(&self.lookup_read_ns),
            cache_write_sum_ms = ms(&self.cache_write_ns),
            "scan planning schema summary"
        );
        self.cache_put_writes.log_summary("write");
        self.cache_put_nops.log_summary("nop");
    }
}

// Stage order is shared with `PutTiming` in disk_cache.rs, which asserts at
// compile time that its `PutStage` enum has exactly this many variants. Fixed-
// size counters avoid per-page allocations or per-page log events.
pub(crate) const CACHE_PUT_STAGE_COUNT: usize = 10;

pub(crate) const CACHE_PUT_STAGES: [&str; CACHE_PUT_STAGE_COUNT] = [
    "prepare",
    "entry_wait",
    "path_prepare",
    "copy",
    "blocking_queue",
    "open",
    "write",
    "resume",
    "commit",
    "bookkeeping",
];

#[derive(Debug, Default)]
pub(crate) struct CachePutCounters {
    count: AtomicU64,
    bytes: AtomicU64,
    total_ns: AtomicU64,
    max_ns: AtomicU64,
    stage_ns: [AtomicU64; CACHE_PUT_STAGE_COUNT],
    stage_max_ns: [AtomicU64; CACHE_PUT_STAGE_COUNT],
    residual_ns: AtomicU64,
    residual_max_ns: AtomicU64,
    overlap_count: AtomicU64,
}

impl CachePutCounters {
    /// `total_ns` is wall time from the start of the put and `stages` partition
    /// it: `PutTiming::mark` stores the interval since the previous mark, so the
    /// stage sum plus `residual_ns` (time no mark brackets, e.g. blocking-task
    /// start-up) equals the total. `overlap_count` staying at zero is the
    /// invariant check; a non-zero value means two stages measured the same
    /// interval and the stage breakdown must not be trusted.
    pub(crate) fn record(
        &self,
        bytes: u64,
        total_ns: u64,
        stages: [u64; CACHE_PUT_STAGE_COUNT],
    ) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        self.total_ns.fetch_add(total_ns, Ordering::Relaxed);
        self.max_ns.fetch_max(total_ns, Ordering::Relaxed);
        let accounted: u64 = stages.iter().sum();
        if accounted > total_ns {
            self.overlap_count.fetch_add(1, Ordering::Relaxed);
        }
        let residual = total_ns.saturating_sub(accounted);
        self.residual_ns.fetch_add(residual, Ordering::Relaxed);
        self.residual_max_ns.fetch_max(residual, Ordering::Relaxed);
        for (index, ns) in stages.into_iter().enumerate() {
            self.stage_ns[index].fetch_add(ns, Ordering::Relaxed);
            self.stage_max_ns[index].fetch_max(ns, Ordering::Relaxed);
        }
    }

    fn log_summary(&self, outcome: &'static str) {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            return;
        }
        let stage_sum_ns: [u64; CACHE_PUT_STAGE_COUNT] =
            std::array::from_fn(|i| self.stage_ns[i].load(Ordering::Relaxed));
        let stage_max_ns: [u64; CACHE_PUT_STAGE_COUNT] =
            std::array::from_fn(|i| self.stage_max_ns[i].load(Ordering::Relaxed));
        tracing::info!(
            outcome,
            count,
            bytes = self.bytes.load(Ordering::Relaxed),
            total_sum_ms = self.total_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            total_max_ms = self.max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            stages = ?CACHE_PUT_STAGES,
            ?stage_sum_ns,
            ?stage_max_ns,
            residual_sum_ns = self.residual_ns.load(Ordering::Relaxed),
            residual_max_ns = self.residual_max_ns.load(Ordering::Relaxed),
            overlap_count = self.overlap_count.load(Ordering::Relaxed),
            "scan planning cache put summary"
        );
    }
}

use tracing::warn;

/// Cache read stats.
pub trait CacheReadStats: Sync + Send + Debug {
    /// Total reads on the cache.
    fn total_reads(&self) -> u64;

    /// Total misses on the cache.
    fn total_misses(&self) -> u64;

    /// Total bytes served from the cache.
    fn total_hit_bytes(&self) -> u64;

    /// Total bytes served after a cache miss.
    fn total_miss_bytes(&self) -> u64;

    /// Total bytes fetched from the inner store to fill cache misses.
    fn total_insert_bytes(&self) -> u64;

    /// Total query time
    fn total_query_time(&self) -> u64;

    /// Total data size
    fn total_data_size(&self) -> u64;

    /// Increase total reads by 1.
    fn inc_total_reads(&self);

    /// Increase total misses by 1.
    fn inc_total_misses(&self);

    /// Increase hit bytes by val.
    fn inc_hit_bytes(&self, val: u64);

    /// Increase miss bytes by val.
    fn inc_miss_bytes(&self, val: u64);

    /// Increase insert bytes by val.
    fn inc_insert_bytes(&self, val: u64);

    /// Increase total query time by val.
    fn inc_total_query_time(&self, val: u64);

    fn inc_total_data_size(&self, val: u64);
}

/// Cache capacity stats.
pub trait CacheCapacityStats {
    fn max_capacity(&self) -> u64;

    fn set_max_capacity(&self, val: u64);

    fn usage(&self) -> u64;

    fn set_usage(&self, val: u64);

    fn inc_usage(&self, val: u64);

    fn sub_usage(&self, val: u64);
}

pub trait CacheStats: CacheCapacityStats + CacheReadStats {}

#[derive(Debug)]
pub struct AtomicIntCacheStats {
    total_reads: AtomicU64,
    total_misses: AtomicU64,
    total_hit_bytes: AtomicU64,
    total_miss_bytes: AtomicU64,
    total_insert_bytes: AtomicU64,
    max_capacity: AtomicU64,
    capacity_usage: AtomicU64,
    total_query_times: AtomicU64,
    total_data_size: AtomicU64,
}

/// Atomic integer cache stats.
impl AtomicIntCacheStats {
    pub fn new() -> Self {
        Self {
            total_misses: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            total_hit_bytes: AtomicU64::new(0),
            total_miss_bytes: AtomicU64::new(0),
            total_insert_bytes: AtomicU64::new(0),
            max_capacity: AtomicU64::new(0),
            capacity_usage: AtomicU64::new(0),
            total_query_times: AtomicU64::new(0),
            total_data_size: AtomicU64::new(0),
        }
    }
}

impl Default for AtomicIntCacheStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheReadStats for AtomicIntCacheStats {
    fn total_misses(&self) -> u64 {
        self.total_misses.load(Ordering::Acquire)
    }

    fn total_reads(&self) -> u64 {
        self.total_reads.load(Ordering::Acquire)
    }

    fn total_hit_bytes(&self) -> u64 {
        self.total_hit_bytes.load(Ordering::Acquire)
    }

    fn total_miss_bytes(&self) -> u64 {
        self.total_miss_bytes.load(Ordering::Acquire)
    }

    fn total_insert_bytes(&self) -> u64 {
        self.total_insert_bytes.load(Ordering::Acquire)
    }

    fn total_query_time(&self) -> u64 {
        self.total_query_times.load(Ordering::Acquire)
    }

    fn total_data_size(&self) -> u64 {
        self.total_data_size.load(Ordering::Acquire)
    }

    fn inc_total_reads(&self) {
        self.total_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_total_misses(&self) {
        self.total_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_hit_bytes(&self, val: u64) {
        self.total_hit_bytes.fetch_add(val, Ordering::Relaxed);
    }

    fn inc_miss_bytes(&self, val: u64) {
        self.total_miss_bytes.fetch_add(val, Ordering::Relaxed);
    }

    fn inc_insert_bytes(&self, val: u64) {
        self.total_insert_bytes.fetch_add(val, Ordering::Relaxed);
    }

    fn inc_total_query_time(&self, val: u64) {
        self.total_query_times.fetch_add(val, Ordering::Relaxed);
    }

    fn inc_total_data_size(&self, val: u64) {
        self.total_data_size.fetch_add(val, Ordering::Relaxed);
    }
}

impl CacheCapacityStats for AtomicIntCacheStats {
    fn max_capacity(&self) -> u64 {
        self.max_capacity.load(Ordering::Acquire)
    }

    fn set_max_capacity(&self, val: u64) {
        self.max_capacity.store(val, Ordering::Relaxed);
    }

    fn usage(&self) -> u64 {
        self.capacity_usage.load(Ordering::Acquire)
    }

    fn set_usage(&self, val: u64) {
        self.capacity_usage.store(val, Ordering::Relaxed);
    }

    fn inc_usage(&self, val: u64) {
        self.capacity_usage.fetch_add(val, Ordering::Relaxed);
    }

    fn sub_usage(&self, val: u64) {
        let res = self
            .capacity_usage
            .fetch_update(Ordering::Acquire, Ordering::Relaxed, |current| {
                if current < val {
                    warn!(
                        "cannot decrement cache usage. current val = {:?} and decrement = {:?}",
                        current, val
                    );
                    None
                } else {
                    Some(current - val)
                }
            });
        if let Err(e) = res {
            warn!("error setting cache usage: {:?}", e);
        }
    }
}

impl CacheStats for AtomicIntCacheStats {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_put_counters_partition_the_total() {
        let counters = CachePutCounters::default();
        let mut stages = [0; CACHE_PUT_STAGE_COUNT];
        stages[0] = 40;
        stages[5] = 60;
        counters.record(4096, 120, stages);

        assert_eq!(counters.count.load(Ordering::Relaxed), 1);
        assert_eq!(counters.bytes.load(Ordering::Relaxed), 4096);
        assert_eq!(counters.total_ns.load(Ordering::Relaxed), 120);
        assert_eq!(counters.max_ns.load(Ordering::Relaxed), 120);
        assert_eq!(counters.residual_ns.load(Ordering::Relaxed), 20);
        assert_eq!(counters.residual_max_ns.load(Ordering::Relaxed), 20);
        assert_eq!(counters.overlap_count.load(Ordering::Relaxed), 0);
        assert_eq!(counters.stage_ns[5].load(Ordering::Relaxed), 60);
        assert_eq!(counters.stage_max_ns[0].load(Ordering::Relaxed), 40);

        // Stages that do not fit inside the total are a measurement bug: the
        // residual stays clamped at zero and the overlap counter flags it.
        counters.record(0, 10, [7; CACHE_PUT_STAGE_COUNT]);

        assert_eq!(counters.count.load(Ordering::Relaxed), 2);
        assert_eq!(counters.total_ns.load(Ordering::Relaxed), 130);
        assert_eq!(counters.residual_ns.load(Ordering::Relaxed), 20);
        assert_eq!(counters.residual_max_ns.load(Ordering::Relaxed), 20);
        assert_eq!(counters.overlap_count.load(Ordering::Relaxed), 1);
        assert_eq!(counters.stage_ns[0].load(Ordering::Relaxed), 47);
        assert_eq!(counters.stage_max_ns[0].load(Ordering::Relaxed), 40);
    }
}
