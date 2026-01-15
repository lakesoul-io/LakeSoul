use tikv_jemalloc_ctl::{epoch, stats};
use tikv_jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub fn print_memory_stats() {
    // 必须先调用 epoch::advance() 才能刷新缓存中的统计数据
    epoch::advance().unwrap();

    let allocated = stats::allocated::read().unwrap();
    let resident = stats::resident::read().unwrap();
    let active = stats::active::read().unwrap();
    let metadata = stats::metadata::read().unwrap();

    println!("--- 内存统计 ---");
    println!("已分配 (Allocated): {} MB", allocated / 1024 / 1024);
    println!("常驻 (Resident): {} MB", resident / 1024 / 1024);
    println!("活跃 (Active): {} MB", active / 1024 / 1024);
    println!("元数据 (Metadata): {} MB", metadata / 1024 / 1024);
}

#[cfg(test)]
mod tests {
    use crate::mem::print_memory_stats;

    #[test]
    fn mmmmmmmm_test() {
        let v = vec![1024; 100000];
        print_memory_stats();
    }
}
