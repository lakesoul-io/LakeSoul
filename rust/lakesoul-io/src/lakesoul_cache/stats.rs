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

    /// Total hits
    fn total_misses(&self) -> u64;

    /// Increase total reads by 1.
    fn inc_total_reads(&self);

    /// Increase total hits by 1.
    fn inc_total_misses(&self);
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
    max_capacity: AtomicU64,
    capacity_usage: AtomicU64,
}

/// Atomic integer cache stats.
impl AtomicIntCacheStats {
    pub fn new() -> Self {
        Self {
            total_misses: AtomicU64::new(0),
            total_reads: AtomicU64::new(0),
            max_capacity: AtomicU64::new(0),
            capacity_usage: AtomicU64::new(0),
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

    fn inc_total_reads(&self) {
        self.total_reads.fetch_add(1, Ordering::Relaxed);
    }

    fn inc_total_misses(&self) {
        self.total_misses.fetch_add(1, Ordering::Relaxed);
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
