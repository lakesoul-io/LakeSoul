// SPDX-FileCopyrightText: 2025 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

//! Quick probe (ignored): how much does a per-file pk -> row locator save
//! over the current `id IN (...)` filter pushdown?
//!
//! Compares, per data file:
//!   (a) scan [id, vec] with the candidate InList pushed down (current path)
//!   (b) scan [id] only (the "locate" pass of a row locator)
//!   (c) row-index fetch of the matched rows with [id, vec] (the "take" pass)
//!   (d) in-memory pk -> row lookup (simulating a cached per-file pk index)
//!
//! Usage:
//!   PROBE_DIR=/tmp/vector-profile2/work/e5_gist PROBE_ITERS=10 \
//!     cargo test -p lakesoul-io --test vortex_pk_probe -- --ignored --nocapture
//! or PROBE_FILE=/path/to/file.vortex

use std::sync::Arc;
use std::time::Instant;

use futures::StreamExt;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::memory::MemorySessionExt;
use vortex::array::stream::ArrayStreamExt;
use vortex::buffer::Buffer;
use vortex::buffer::ByteBufferMut;
use vortex::dtype::Nullability;
use vortex::expr::{Expression, get_item, list_contains, lit, root};
use vortex::file::{
    OpenOptionsSessionExt, VortexFile, WriteOptionsSessionExt, WriteStrategyBuilder,
};
use vortex::io::object_store::ObjectStoreReadAt;
use vortex::io::session::RuntimeSessionExt;
use vortex::scalar::Scalar;
use vortex::session::VortexSession;

fn probe_paths() -> Vec<std::path::PathBuf> {
    if let Ok(file) = std::env::var("PROBE_FILE") {
        return vec![std::path::PathBuf::from(file)];
    }
    let dir = std::env::var("PROBE_DIR").expect("set PROBE_FILE or PROBE_DIR");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|e| e == "vortex"))
        .collect();
    files.sort();
    files
}

fn open_options() -> (Arc<dyn ObjectStore>, VortexSession) {
    (
        Arc::new(LocalFileSystem::new_with_prefix("/").unwrap()),
        VortexSession::default(),
    )
}

async fn open_file(
    store: &Arc<dyn ObjectStore>,
    path: &std::path::Path,
    session: &VortexSession,
) -> VortexFile {
    let location = StorePath::from_absolute_path(path).unwrap();
    let reader = Arc::new(ObjectStoreReadAt::new_with_allocator(
        store.clone(),
        location,
        session.handle(),
        session.allocator(),
    ));
    session.open_options().open_read(reader).await.unwrap()
}

fn in_list_expr(ids: &[i64]) -> Expression {
    let elements: Vec<Scalar> = ids
        .iter()
        .map(|&v| Scalar::primitive(v, Nullability::NonNullable))
        .collect();
    let list = Scalar::list(elements[0].dtype().clone(), elements, Nullability::Nullable);
    list_contains(lit(list), get_item("id", root()))
}

async fn drain(
    stream: impl futures::Stream<Item = vortex::error::VortexResult<vortex::array::ArrayRef>>,
) -> usize {
    futures::pin_mut!(stream);
    let mut rows = 0;
    while let Some(item) = stream.next().await {
        rows += item.unwrap().len();
    }
    rows
}

async fn read_pks(file: &VortexFile, session: &VortexSession) -> Vec<i64> {
    let ctx = &mut session.create_execution_ctx();
    let array = file
        .scan()
        .unwrap()
        .with_projection(get_item("id", root()))
        .into_array_stream()
        .unwrap()
        .read_all()
        .await
        .unwrap();
    let pk = array.execute::<PrimitiveArray>(ctx).unwrap();
    pk.as_slice::<i64>().to_vec()
}

macro_rules! bench {
    ($iters:expr, $body:expr) => {{
        for _ in 0..2 {
            let _ = $body.await;
        }
        let t = Instant::now();
        for _ in 0..$iters {
            let _ = $body.await;
        }
        t.elapsed().as_secs_f64() * 1000.0 / $iters as f64
    }};
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run manually against a real vortex data file"]
async fn probe_in_list_vs_row_locator() {
    let iters: usize = std::env::var("PROBE_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let candidates_n: usize = std::env::var("PROBE_CANDIDATES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let paths = probe_paths();
    let (store, session) = open_options();

    // Setup (untimed): open files, read pk columns, pick candidates, build
    // the in-memory pk arrays that a per-file row locator would cache.
    let open_start = Instant::now();
    let mut files = Vec::new();
    for p in &paths {
        files.push(open_file(&store, p, &session).await);
    }
    let open_ms = open_start.elapsed().as_secs_f64() * 1000.0 / paths.len() as f64;

    let mut pk_arrays: Vec<Vec<i64>> = Vec::new();
    for file in &files {
        pk_arrays.push(read_pks(file, &session).await);
    }
    let total_rows: usize = pk_arrays.iter().map(|p| p.len()).sum();
    for pk in &pk_arrays {
        assert!(pk.windows(2).all(|w| w[0] <= w[1]), "pk column not sorted");
    }

    // Candidates sampled from the union of all pks, spread across files.
    let mut state = 0x1234_5678_9abc_def0u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as usize
    };
    let mut candidates: Vec<i64> = Vec::new();
    while candidates.len() < candidates_n {
        let file = &pk_arrays[next() % pk_arrays.len()];
        let pk = file[next() % file.len()];
        if !candidates.contains(&pk) {
            candidates.push(pk);
        }
    }
    candidates.sort_unstable();

    // Row indices per file for the candidates (the lookup a row locator does).
    let mut found_per_file: Vec<Vec<u64>> = Vec::with_capacity(files.len());
    for pk in &pk_arrays {
        let mut indices: Vec<u64> = candidates
            .iter()
            .filter_map(|c| pk.binary_search(c).ok().map(|i| i as u64))
            .collect();
        indices.sort_unstable();
        found_per_file.push(indices);
    }
    let found: usize = found_per_file.iter().map(|v| v.len()).sum();

    let expr = in_list_expr(&candidates);

    let in_list_ms = bench!(iters, async {
        let mut rows = 0;
        for file in &files {
            let stream = file
                .scan()
                .unwrap()
                .with_projection(root())
                .with_filter(expr.clone())
                .into_array_stream()
                .unwrap();
            rows += drain(stream).await;
        }
        rows
    });

    let pk_scan_ms = bench!(iters, async {
        let mut rows = 0;
        for file in &files {
            let stream = file
                .scan()
                .unwrap()
                .with_projection(get_item("id", root()))
                .into_array_stream()
                .unwrap();
            rows += drain(stream).await;
        }
        rows
    });

    let take_ms = bench!(iters, async {
        let mut rows = 0;
        for (file, indices) in files.iter().zip(&found_per_file) {
            if indices.is_empty() {
                continue;
            }
            let stream = file
                .scan()
                .unwrap()
                .with_projection(root())
                .with_row_indices(Buffer::from_iter(indices.iter().copied()))
                .into_array_stream()
                .unwrap();
            rows += drain(stream).await;
        }
        rows
    });

    let full_scan_ms = bench!(iters, async {
        let stream = files[0]
            .scan()
            .unwrap()
            .with_projection(root())
            .into_array_stream()
            .unwrap();
        drain(stream).await
    });

    let id_in_list_ms = bench!(iters, async {
        let stream = files[0]
            .scan()
            .unwrap()
            .with_projection(get_item("id", root()))
            .with_filter(expr.clone())
            .into_array_stream()
            .unwrap();
        drain(stream).await
    });

    let id_take_ms = bench!(iters, async {
        let mut rows = 0;
        for (file, indices) in files.iter().zip(&found_per_file) {
            if indices.is_empty() {
                continue;
            }
            let stream = file
                .scan()
                .unwrap()
                .with_projection(get_item("id", root()))
                .with_row_indices(Buffer::from_iter(indices.iter().copied()))
                .into_array_stream()
                .unwrap();
            rows += drain(stream).await;
        }
        rows
    });

    let biggest = pk_arrays
        .iter()
        .enumerate()
        .max_by_key(|(_, p)| p.len())
        .map(|(i, _)| i)
        .unwrap();
    let biggest_len = pk_arrays[biggest].len();
    let range_start = (biggest_len / 2).saturating_sub(50).min(biggest_len - 100) as u64;
    let contiguous = (range_start..range_start + 100).collect::<Vec<_>>();
    let contiguous_range_ms = bench!(iters, async {
        files[biggest]
            .scan()
            .unwrap()
            .with_projection(root())
            .with_row_range(range_start..range_start + 100)
            .into_array_stream()
            .unwrap()
            .read_all()
            .await
            .unwrap()
            .len()
    });
    let contiguous_indices_ms = bench!(iters, async {
        files[biggest]
            .scan()
            .unwrap()
            .with_projection(root())
            .with_row_indices(Buffer::from_iter(contiguous.iter().copied()))
            .into_array_stream()
            .unwrap()
            .read_all()
            .await
            .unwrap()
            .len()
    });

    let lookup_ms = bench!(iters, async {
        let mut hits = 0;
        for pk in &pk_arrays {
            hits += candidates
                .iter()
                .filter(|c| pk.binary_search(c).is_ok())
                .count();
        }
        hits
    });

    let reopen_take_ms = bench!(iters, async {
        let mut rows = 0;
        for (path, indices) in paths.iter().zip(&found_per_file) {
            if indices.is_empty() {
                continue;
            }
            let file = open_file(&store, path, &session).await;
            let stream = file
                .scan()
                .unwrap()
                .with_projection(root())
                .with_row_indices(Buffer::from_iter(indices.iter().copied()))
                .into_array_stream()
                .unwrap();
            rows += drain(stream).await;
        }
        rows
    });

    // Layout sensitivity: rewrite the same data with smaller row blocks and
    // measure random row-index access and full scans.
    let all = files[biggest]
        .scan()
        .unwrap()
        .with_projection(root())
        .into_array_stream()
        .unwrap()
        .read_all()
        .await
        .unwrap();
    let mut layout_results = Vec::new();
    for block in [1024usize, 256, 64] {
        let strategy = WriteStrategyBuilder::default()
            .with_row_block_size(block)
            .build();
        let mut buf = ByteBufferMut::empty();
        session
            .write_options()
            .with_strategy(strategy)
            .write(&mut buf, all.to_array_stream())
            .await
            .unwrap();
        let small = session.open_options().open_buffer(buf).unwrap();
        let take = bench!(iters, async {
            small
                .scan()
                .unwrap()
                .with_projection(root())
                .with_row_indices(Buffer::from_iter(found_per_file[0].iter().copied()))
                .into_array_stream()
                .unwrap()
                .read_all()
                .await
                .unwrap()
                .len()
        });
        let full = bench!(iters, async {
            small
                .scan()
                .unwrap()
                .with_projection(root())
                .into_array_stream()
                .unwrap()
                .read_all()
                .await
                .unwrap()
                .len()
        });
        layout_results.push((block, take, full));
    }

    println!("\n── vortex pk locator probe ──");
    println!(
        "files={} rows={} candidates={} matched={} iters={}",
        files.len(),
        total_rows,
        candidates_n,
        found,
        iters
    );
    println!("open file (cold, once per file):   {open_ms:.2} ms/file");
    println!("(e) [id,vec] full scan:            {full_scan_ms:.2} ms/query");
    println!("(a) [id,vec] + InList pushdown:    {in_list_ms:.2} ms/query");
    println!("(f) [id]      + InList pushdown:    {id_in_list_ms:.2} ms/query");
    println!("(g) [id]      row-index take:       {id_take_ms:.2} ms/query");
    println!("(h) [id,vec] contiguous row range:  {contiguous_range_ms:.2} ms/query");
    println!("(i) [id,vec] contiguous row idx:    {contiguous_indices_ms:.2} ms/query");
    println!("(b) [id] locate pass:              {pk_scan_ms:.2} ms/query");
    println!("(d) in-memory pk lookup:           {lookup_ms:.2} ms/query");
    println!(
        "(c) row-index take [id,vec]:       {take_ms:.2} ms/query (file handles cached)"
    );
    println!(
        "(b+d+c) locator total:             {:.2} ms/query",
        pk_scan_ms + lookup_ms + take_ms
    );
    println!(
        "(d+c) locator with cached pk col:  {:.2} ms/query",
        lookup_ms + take_ms
    );
    println!("(c) with reopen per query:         {reopen_take_ms:.2} ms/query");
    println!(
        "     speedup (a)/(b+d+c): {:.1}x",
        in_list_ms / (pk_scan_ms + lookup_ms + take_ms)
    );
    println!(
        "     speedup (a)/(d+c):   {:.1}x",
        in_list_ms / (lookup_ms + take_ms)
    );
    for (block, take, full) in &layout_results {
        println!(
            "     block={block:<5} random take={take:.2} ms  full scan={full:.2} ms"
        );
    }
    assert!(found > 0, "no candidates matched any file");
}
