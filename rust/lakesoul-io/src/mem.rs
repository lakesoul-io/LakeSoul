#[cfg(feature = "jemalloc")]
pub mod jemalloc {

    #[cfg(not(target_env = "msvc"))]
    #[global_allocator]
    static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

    #[allow(non_upper_case_globals)]
    #[unsafe(export_name = "malloc_conf")]
    pub static malloc_conf: &[u8] =
        b"prof:true,prof_gdump:true,prof_prefix:/data/jiax_space/LakeSoul/rust/peak\0";
    use tikv_jemalloc_ctl::{epoch, stats};

    pub fn _print_memory_stats() {
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

    pub async fn _dump_flamegraph() {
        let prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap();
        let mut ctl = prof_ctl.lock().await;
        if !ctl.activated() {
            return;
        }
        let svg = ctl.dump_flamegraph().unwrap();
        std::fs::write("./flamegraph.svg", svg).unwrap();
    }

    pub fn resident_memory() -> usize {
        epoch::advance().unwrap();
        stats::resident::read().unwrap()
    }
}

pub mod pool;
