// SPDX-FileCopyrightText: 2026 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Process memory allocator. The executable owns `#[global_allocator]`;
//! LakeSoul libraries never install one. The default `jemalloc` feature uses
//! jemalloc with its built-in defaults: no heap profiling, no decay tuning
//! (`MALLOC_CONF` still applies at startup). `--no-default-features` builds
//! with the system allocator.
//!
//! Keep the jemalloc crates out of shared libraries: the workspace enables
//! `unprefixed_malloc_on_supported_platforms`, so any binary that links
//! `tikv-jemalloc-sys` in the same build would have its `malloc` replaced.

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Logs which allocator serves this process, as reported by the allocator.
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
pub(crate) fn log_active() {
    match jemalloc::identity() {
        Ok(id) => tracing::info!(
            allocator = "jemalloc",
            version = id.version,
            config_prof = id.config_prof,
            background_thread = id.background_thread,
            narenas = id.narenas,
            dirty_decay_ms = id.dirty_decay_ms,
            muzzy_decay_ms = id.muzzy_decay_ms,
            allocated_bytes = id.allocated,
            "memory allocator"
        ),
        Err(e) => {
            tracing::warn!(allocator = "jemalloc", error = %e, "memory allocator: mallctl failed")
        }
    }
}

/// Reader of jemalloc's allocated/active/resident bytes for the memory
/// sampler. MIBs are resolved once; each call refreshes jemalloc's
/// statistics epoch first.
#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
pub(crate) fn stats_reader()
-> impl FnMut() -> Option<lakesoul_observability::AllocatorStats> + Send + 'static {
    let mibs = jemalloc::StatsMibs::new()
        .inspect_err(|e| tracing::warn!(error = %e, "jemalloc statistics unavailable"))
        .ok();
    move || {
        mibs.as_ref()?
            .read()
            .inspect_err(
                |e| tracing::debug!(error = %e, "jemalloc statistics read failed"),
            )
            .ok()
    }
}

#[cfg(not(all(feature = "jemalloc", not(target_env = "msvc"))))]
pub(crate) fn log_active() {
    tracing::info!(allocator = "system", "memory allocator");
}

/// The system allocator reports no statistics.
#[cfg(not(all(feature = "jemalloc", not(target_env = "msvc"))))]
pub(crate) fn stats_reader()
-> impl FnMut() -> Option<lakesoul_observability::AllocatorStats> + Send + 'static {
    || None
}

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
mod jemalloc {
    use tikv_jemalloc_ctl::{Result, epoch, epoch_mib, opt, raw, stats, version};

    pub(super) struct Identity {
        pub version: &'static str,
        pub config_prof: bool,
        pub background_thread: bool,
        pub narenas: u32,
        pub dirty_decay_ms: isize,
        pub muzzy_decay_ms: isize,
        /// Bytes jemalloc currently has allocated; non-zero only if it has
        /// served this process's allocations so far.
        pub allocated: usize,
    }

    pub(super) fn identity() -> Result<Identity> {
        epoch::advance()?;
        // SAFETY: NUL-terminated mallctl names whose C types are bool,
        // ssize_t and ssize_t.
        let (config_prof, dirty_decay_ms, muzzy_decay_ms) = unsafe {
            (
                raw::read::<bool>(b"config.prof\0")?,
                raw::read::<isize>(b"opt.dirty_decay_ms\0")?,
                raw::read::<isize>(b"opt.muzzy_decay_ms\0")?,
            )
        };
        Ok(Identity {
            // `version` is the raw C string, NUL included.
            version: version::read()?.trim_end_matches('\0'),
            config_prof,
            background_thread: opt::background_thread::read()?,
            narenas: opt::narenas::read()?,
            dirty_decay_ms,
            muzzy_decay_ms,
            allocated: stats::allocated::read()?,
        })
    }

    pub(super) struct StatsMibs {
        epoch: epoch_mib,
        allocated: stats::allocated_mib,
        active: stats::active_mib,
        resident: stats::resident_mib,
    }

    impl StatsMibs {
        pub(super) fn new() -> Result<Self> {
            Ok(Self {
                epoch: epoch::mib()?,
                allocated: stats::allocated::mib()?,
                active: stats::active::mib()?,
                resident: stats::resident::mib()?,
            })
        }

        pub(super) fn read(&self) -> Result<lakesoul_observability::AllocatorStats> {
            self.epoch.advance()?;
            Ok(lakesoul_observability::AllocatorStats {
                allocated: self.allocated.read()? as u64,
                active: self.active.read()? as u64,
                resident: self.resident.read()? as u64,
            })
        }
    }
}

#[cfg(all(test, feature = "jemalloc", not(target_env = "msvc")))]
mod tests {
    use std::ffi::c_void;
    use std::hint::black_box;

    use tikv_jemalloc_ctl::thread;

    const SIZE: usize = 1 << 20;

    unsafe extern "C" {
        fn malloc(size: usize) -> *mut c_void;
        fn free(ptr: *mut c_void);
    }

    /// Bytes jemalloc has ever allocated on this thread while `f` runs.
    fn jemalloc_bytes_during(f: impl FnOnce()) -> u64 {
        let allocated = thread::allocatedp::read().unwrap();
        let before = allocated.get();
        f();
        allocated.get() - before
    }

    #[test]
    fn rust_heap_is_served_by_jemalloc() {
        let bytes = jemalloc_bytes_during(|| drop(black_box(vec![1u8; SIZE])));
        assert!(bytes >= SIZE as u64, "jemalloc saw {bytes} bytes");
    }

    #[test]
    fn c_malloc_is_served_by_jemalloc() {
        let bytes = jemalloc_bytes_during(|| unsafe {
            let p = black_box(malloc(SIZE));
            assert!(!p.is_null());
            free(p);
        });
        assert!(bytes >= SIZE as u64, "jemalloc saw {bytes} bytes");
    }
}
