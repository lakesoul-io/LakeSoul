use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use lakesoul_io::cache::disk_cache::DiskCache;
use lakesoul_io::cache::paging::PageCache;
use object_store::path::Path;
use rand::Rng;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tempfile::TempDir;

const PAGE_SIZE: usize = 64 * 1024;

fn make_cache(capacity: usize) -> (Arc<DiskCache>, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let cache = DiskCache::with_path(capacity, PAGE_SIZE, dir.path().to_path_buf());
    (Arc::new(cache), dir)
}

fn random_data(size: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    (0..size).map(|_| rng.random::<u8>()).collect()
}

fn prefill(cache: &DiskCache, num_pages: usize, location: &Path) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for i in 0..num_pages {
            let data = random_data(PAGE_SIZE);
            cache
                .put(location, i as u32, bytes::Bytes::from(data))
                .await
                .unwrap();
        }
    });
}

// ── Single-threaded throughput ───────────────────────────────────

fn bench_put(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (cache, _dir) = make_cache(256 * 1024 * 1024);
    let location = Path::from("bench_put");

    c.bench_function("cache_put_64k_page", |b| {
        b.iter(|| {
            let data = random_data(PAGE_SIZE);
            let bytes = bytes::Bytes::from(data.clone());
            rt.block_on(async {
                black_box(cache.put(&location, 0, bytes).await.unwrap());
            });
            // Invalidate so next put doesn't short-circuit
            rt.block_on(async { cache.invalidate(&location).await.unwrap() });
        });
    });
}

fn bench_get_hit(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (cache, _dir) = make_cache(256 * 1024 * 1024);
    let location = Path::from("bench_get_hit");
    prefill(&cache, 1, &location);

    c.bench_function("cache_get_hit_64k_page", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(cache.get(&location, 0).await.unwrap().unwrap());
            });
        });
    });
}

fn bench_get_range_hit(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (cache, _dir) = make_cache(256 * 1024 * 1024);
    let location = Path::from("bench_range_hit");
    prefill(&cache, 1, &location);

    c.bench_function("cache_get_range_hit_4k_of_64k", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(
                    cache
                        .get_range(&location, 0, 0..4096)
                        .await
                        .unwrap()
                        .unwrap(),
                );
            });
        });
    });
}

fn bench_get_miss(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (cache, _dir) = make_cache(256 * 1024 * 1024);
    let location = Path::from("bench_get_miss");

    c.bench_function("cache_get_miss", |b| {
        b.iter(|| {
            rt.block_on(async {
                black_box(cache.get(&location, 0).await.unwrap());
            });
        });
    });
}

// ── Concurrent throughput ────────────────────────────────────────

fn bench_concurrent_reads(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let num_pages = 500usize;
    let (cache, _dir) = make_cache(256 * 1024 * 1024);
    let location = Path::from("bench_concurrent_read");
    prefill(&cache, num_pages, &location);

    let num_threads: Vec<usize> = vec![1, 4, 16];
    for &threads in &num_threads {
        let cache = cache.clone();
        let loc = location.clone();
        let name = format!("cache_concurrent_read_{}_threads", threads);

        c.bench_function(&name, |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::new();
                    for _ in 0..threads {
                        let cache = cache.clone();
                        let loc = loc.clone();
                        let mut rng = StdRng::from_os_rng();
                        handles.push(tokio::task::spawn(async move {
                            for _ in 0..200 {
                                let page_id = rng.random_range(0..num_pages as u32);
                                let _ = cache.get(&loc, page_id).await;
                                black_box(());
                            }
                        }));
                    }
                    for h in handles {
                        h.await.unwrap();
                    }
                });
            });
        });
    }
}

fn bench_concurrent_mixed(c: &mut Criterion) {
    let num_pages: usize = 400;
    let num_readers: usize = 12;
    let num_writers: usize = 4;
    let capacity = 64 * 1024 * 1024; // 64MB
    let (cache, _dir) = make_cache(capacity);
    let location = Path::from("bench_concurrent_mixed");
    prefill(&cache, num_pages / 2, &location);

    let rt = tokio::runtime::Runtime::new().unwrap();
    c.bench_function("cache_concurrent_mixed_read_write", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();

                for _ in 0..num_readers {
                    let cache = cache.clone();
                    let loc = location.clone();
                    let mut rng = StdRng::from_os_rng();
                    handles.push(tokio::task::spawn(async move {
                        for _ in 0..100 {
                            let page_id = rng.random_range(0..num_pages as u32);
                            let _ = cache.get(&loc, page_id).await;
                        }
                    }));
                }

                for offset in 0..num_writers {
                    let cache = cache.clone();
                    handles.push(tokio::task::spawn(async move {
                        let write_loc = Path::from(format!("bench_write_{}", offset));
                        for page_id in 0u32..50 {
                            let data = random_data(PAGE_SIZE);
                            let _ = cache
                                .put(&write_loc, page_id, bytes::Bytes::from(data))
                                .await;
                        }
                    }));
                }

                for h in handles {
                    h.await.unwrap();
                }
                black_box(());
            });
        });
    });
}

// ── Eviction stress ──────────────────────────────────────────────

fn bench_eviction_stress(c: &mut Criterion) {
    let capacity = 128 * 1024; // 128KB, holds only 2 pages of 64KB
    let (cache, _dir) = make_cache(capacity);
    let location = Path::from("bench_eviction");
    prefill(&cache, 2, &location);

    let rt = tokio::runtime::Runtime::new().unwrap();
    let write_page_id = Arc::new(AtomicU64::new(2));
    c.bench_function("cache_eviction_stress_continuous_insert", |b| {
        b.iter(|| {
            rt.block_on(async {
                let pid = write_page_id.fetch_add(1, Ordering::Relaxed) as u32;
                let data = random_data(PAGE_SIZE);
                let _ = cache
                    .put(&location, pid, bytes::Bytes::from(data))
                    .await;
                let _ = cache.get(&location, 0).await;
                black_box(());
            });
        });
    });
}

// ── Stability: long-running random access ────────────────────────

fn bench_stability_extended(c: &mut Criterion) {
    let capacity = 16 * 1024 * 1024; // 16MB, ~256 pages
    let num_pages: usize = 256;
    let (cache, _dir) = make_cache(capacity);
    let location = Path::from("bench_stability");

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        for i in 0..num_pages / 2 {
            let data = random_data(PAGE_SIZE);
            cache
                .put(&location, i as u32, bytes::Bytes::from(data))
                .await
                .unwrap();
        }
    });

    let num_threads = 8;
    let ops_per_thread = 2000usize;

    c.bench_function("cache_stability_8t_16000ops", |b| {
        b.iter(|| {
            rt.block_on(async {
                let errors = Arc::new(AtomicU64::new(0));
                let mut handles = Vec::new();

                for _ in 0..num_threads {
                    let cache = cache.clone();
                    let loc = location.clone();
                    let errors = errors.clone();
                    let mut rng = StdRng::from_os_rng();
                    handles.push(tokio::task::spawn(async move {
                        for _ in 0..ops_per_thread {
                            let choice = rng.random_range(0..100);
                            let pid = rng.random_range(0..(num_pages as u32 * 2));
                            let result = match choice {
                                0..=59 => {
                                    cache
                                        .get_range(&loc, pid, 0..PAGE_SIZE.min(4096))
                                        .await
                                        .map(|_| ())
                                }
                                60..=89 => {
                                    let loc2 = loc.clone();
                                    cache
                                        .get_with(&loc2, pid, async move {
                                            Ok(bytes::Bytes::from(random_data(PAGE_SIZE)))
                                        })
                                        .await
                                        .map(|_| ())
                                }
                                _ => {
                                    let data = random_data(PAGE_SIZE);
                                    cache
                                        .put(&loc, pid, bytes::Bytes::from(data))
                                        .await
                                }
                            };
                            if let Err(_e) = result {
                                errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }));
                }

                for h in handles {
                    h.await.unwrap();
                }

                let errs = errors.load(Ordering::Relaxed);
                assert_eq!(errs, 0, "{} errors in stability run", errs);
                black_box(errs);
            });
        });
    });
}

// ── Invalidate + re-access pattern ───────────────────────────────

fn bench_invalidate_cycle(c: &mut Criterion) {
    let capacity = 8 * 1024 * 1024;
    let (cache, _dir) = make_cache(capacity);

    let rt = tokio::runtime::Runtime::new().unwrap();

    c.bench_function("cache_invalidate_and_repopulate", |b| {
        b.iter(|| {
            rt.block_on(async {
                let location = Path::from(format!("cycle_{}", rand::rng().random::<u64>()));

                for i in 0u32..50 {
                    let data = random_data(PAGE_SIZE);
                    cache
                        .put(&location, i, bytes::Bytes::from(data))
                        .await
                        .unwrap();
                }

                for i in 0u32..10 {
                    let _ = cache.get(&location, i).await.unwrap();
                }

                cache.invalidate(&location).await.unwrap();
                black_box(());
            });
        });
    });
}

criterion_group! {
    name = cache_bench;
    config = Criterion::default()
        .sample_size(50)
        .measurement_time(std::time::Duration::from_secs(10));
    targets =
        bench_put,
        bench_get_hit,
        bench_get_range_hit,
        bench_get_miss,
        bench_concurrent_reads,
        bench_concurrent_mixed,
        bench_eviction_stress,
        bench_stability_extended,
        bench_invalidate_cycle,
}
criterion_main!(cache_bench);
