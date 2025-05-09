//! Disk cache Builder.
//!

use std::time::Duration;

use super::{DiskCache, DEFAULT_PAGE_SIZE, DEFAULT_TIME_TO_IDLE};

/// Builder for [`DiskCache`]
pub struct DiskCacheBuilder {
    capacity: usize,
    page_size: usize,

    time_to_idle: Duration,
}

impl DiskCacheBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            page_size: DEFAULT_PAGE_SIZE,
            time_to_idle: DEFAULT_TIME_TO_IDLE,
        }
    }

    /// Set the page size.
    pub fn page_size(&mut self, size: usize) -> &mut Self {
        self.page_size = size;
        self
    }

    /// If an entry has been idle longer than `time_to_idle` seconds,
    /// it will be evicted.
    ///
    /// Default is 30 minutes.
    pub fn time_to_idle(&mut self, tti: Duration) -> &mut Self {
        self.time_to_idle = tti;
        self
    }

    #[must_use]
    pub fn build(&self) -> DiskCache {
        DiskCache::with_params(self.capacity, self.page_size, self.time_to_idle)
    }
}
