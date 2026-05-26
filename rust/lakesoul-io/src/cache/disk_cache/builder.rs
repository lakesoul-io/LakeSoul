//! Disk cache Builder.
//!

use std::{path::PathBuf, time::Duration};

use super::{DEFAULT_PAGE_SIZE, DEFAULT_TIME_TO_IDLE, DiskCache};

/// Builder for [`DiskCache`]
pub struct DiskCacheBuilder {
    capacity: usize,
    page_size: usize,
    cache_path: Option<PathBuf>,

    time_to_idle: Duration,
}

impl DiskCacheBuilder {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            page_size: DEFAULT_PAGE_SIZE,
            cache_path: None,
            time_to_idle: DEFAULT_TIME_TO_IDLE,
        }
    }

    /// Set the page size.
    pub fn page_size(&mut self, size: usize) -> &mut Self {
        self.page_size = size;
        self
    }

    /// Set the directory used to store disk cache files.
    pub fn cache_path<T: Into<PathBuf>>(&mut self, path: T) -> &mut Self {
        self.cache_path = Some(path.into());
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
        DiskCache::with_params(
            self.capacity,
            self.page_size,
            self.time_to_idle,
            self.cache_path.clone(),
        )
    }
}
