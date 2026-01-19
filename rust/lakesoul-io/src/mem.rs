use tikv_jemalloc_ctl::{epoch, stats};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(feature = "jemalloc")]
mod jemalloc {
    use tikv_jemalloc_ctl::{epoch, stats};

    pub fn print_memory_stats() {
        // 必须先调用 epoch::advance() 才能刷新缓存中的统计数据
        epoch::advance().unwrap();
        let allocated = stats::allocated::read().unwrap();
        let resident = stats::resident::read().unwrap();
        let active = stats::active::read().unwrap();
        let metadata = stats::metadata::read().unwrap();

        println!("--- mem status---");
        println!("Allocated: {} MB", allocated / 1024 / 1024);
        println!("Resident: {} MB", resident / 1024 / 1024);
        println!("Active: {} MB", active / 1024 / 1024);
        println!("Metadata: {} MB", metadata / 1024 / 1024);
    }
}
