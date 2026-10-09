// SPDX-FileCopyrightText: LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Cache stats
use std::{
    fmt::Debug,
    sync::atomic::{AtomicU64, Ordering},
};

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
