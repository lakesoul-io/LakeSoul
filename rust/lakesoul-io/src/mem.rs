/// jemalloc statistics. This crate never installs an allocator: executables
/// own `#[global_allocator]` (`postgres-lakesoul`, `lakesoul-worker`,
/// `lakesoul-console`), and these numbers are meaningful only in a process
/// that uses jemalloc.
#[cfg(feature = "jemalloc")]
pub mod jemalloc {
    use tikv_jemalloc_ctl::{epoch, stats};

    pub fn resident_memory() -> usize {
        epoch::advance().unwrap();
        stats::resident::read().unwrap()
    }
}

pub mod pool;
