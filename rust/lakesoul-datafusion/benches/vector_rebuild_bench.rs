// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

//! Vector index benchmark: build / search / stream / trigger.
//!
//! Dataset files are read directly from fvecs/ivecs (no download); the
//! LakeSoul table data files are written in **vortex** format
//! (`PhysicalFormat::Vortex`).
//!
//! Skeleton scope (this file):
//!   * fvecs/ivecs readers
//!   * brute-force ground truth + recall / QPS measurement
//!   * metrics collection (wall time, RSS, index size, cluster stats)
//!   * `build` and `search` scenarios
//!   * `stream` / `trigger` scenarios are stubs (policy wiring comes next)
//!
//! Usage:
//!   cargo bench -p lakesoul-datafusion --bench vector_rebuild_bench -- \
//!       --scenario build \
//!       --base ~/program/opensource/rabitq-rs/data/gist/gist_base.fvecs \
//!       --query ~/program/opensource/rabitq-rs/data/gist/gist_query.fvecs \
//!       --gt ~/program/opensource/rabitq-rs/data/gist/gist_groundtruth.ivecs \
//!       --limit 100000 --nlist 256 --work-dir /tmp/lakesoul_test/vector_bench/gist
//!
//!   cargo bench -p lakesoul-datafusion --bench vector_rebuild_bench -- \
//!       --scenario search --reuse --work-dir /tmp/lakesoul_test/vector_bench/gist \
//!       --base ... --query ... --gt ...

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{FixedSizeListBuilder, Float32Builder, RecordBatch, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::vector::builder::{VectorShardIndexBuilder, shard_index_prefix};
use lakesoul_io::writer::create_writer_with_io_config;
use lakesoul_vector::{
    IvfRabitqIndex, ManifestStore, Metric, RotatorType, SearchParams, VectorIndexConfig,
    cluster_stats, index_stats,
};
use object_store::local::LocalFileSystem;
use serde_json::{Value, json};

const PK_COLUMN: &str = "id";
const VEC_COLUMN: &str = "vec";
const DEFAULT_WORK_DIR: &str = "/tmp/lakesoul_test/vector_bench";
const WRITE_BATCH_SIZE: usize = 8192;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Build,
    Search,
    Stream,
    Trigger,
}

#[derive(Debug, Clone)]
struct Args {
    scenario: Scenario,
    base: PathBuf,
    query: PathBuf,
    gt: Option<PathBuf>,
    learn: Option<PathBuf>,
    /// Cap on base vectors (scaling curves). `None` = all.
    limit: Option<usize>,
    /// Cap on query vectors.
    n_queries: usize,
    nlist: usize,
    total_bits: usize,
    metric: Metric,
    top_k: usize,
    /// nprobe values swept by `search`.
    nprobe_sweep: Vec<usize>,
    /// nprobe used by non-sweep measurements.
    nprobe: usize,
    threads: usize,
    seed: u64,
    work_dir: PathBuf,
    out: Option<PathBuf>,
    /// Reuse an existing index in `work_dir` instead of rebuilding it.
    reuse: bool,
    /// Print full usage.
    help: bool,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            scenario: Scenario::Build,
            base: PathBuf::new(),
            query: PathBuf::new(),
            gt: None,
            learn: None,
            limit: None,
            n_queries: 100,
            nlist: 256,
            total_bits: 7,
            metric: Metric::L2,
            top_k: 10,
            nprobe_sweep: vec![1, 4, 16, 64, 128, 256],
            nprobe: 64,
            threads: 16,
            seed: 42,
            work_dir: PathBuf::from(DEFAULT_WORK_DIR),
            out: None,
            reuse: false,
            help: false,
        }
    }
}

fn usage() -> String {
    format!(
        r#"vector_rebuild_bench — LakeSoul IVF+RaBitQ index benchmark

USAGE:
  cargo bench -p lakesoul-datafusion --bench vector_rebuild_bench -- [OPTIONS]

OPTIONS:
  --scenario <build|search|stream|trigger>  Scenario to run (default: build)
  --base <path.fvecs>                       Base vectors (required)
  --query <path.fvecs>                      Query vectors (required)
  --gt <path.ivecs>                         Ground truth (optional; brute-forced if absent)
  --learn <path.fvecs>                      Extra vectors for stream updates (optional)
  --limit <N>                               Cap base vectors (default: all)
  --n-queries <N>                           Cap queries used (default: 100)
  --nlist <N>                               IVF clusters (default: 256)
  --total-bits <N>                          RaBitQ total bits, 1-16 (default: 7)
  --metric <l2|ip>                          Distance metric (default: l2)
  --top-k <N>                               Recall@k / search top-k (default: 10)
  --nprobe <N>                              nprobe for non-sweep measurements (default: 64)
  --nprobe-sweep <a,b,c>                    nprobe sweep for `search` (default: 1,4,16,64,128,256)
  --threads <N>                             Worker threads / RAYON_NUM_THREADS (default: 16)
  --seed <N>                                Random seed (default: 42)
  --work-dir <path>                         Data + index directory (default: {DEFAULT_WORK_DIR})
  --out <path.json>                         Write the result summary as JSON
  --reuse                                   Reuse an existing index in --work-dir
  --help                                    Show this help
"#
    )
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        let flag = argv[i].as_str();
        let mut value = |name: &str| -> Result<String, String> {
            i += 1;
            argv.get(i)
                .cloned()
                .ok_or_else(|| format!("missing value for {name}"))
        };
        match flag {
            "--scenario" => {
                args.scenario = match value(flag)?.as_str() {
                    "build" => Scenario::Build,
                    "search" => Scenario::Search,
                    "stream" => Scenario::Stream,
                    "trigger" => Scenario::Trigger,
                    other => return Err(format!("unknown scenario: {other}")),
                };
            }
            "--base" => args.base = PathBuf::from(value(flag)?),
            "--query" => args.query = PathBuf::from(value(flag)?),
            "--gt" => args.gt = Some(PathBuf::from(value(flag)?)),
            "--learn" => args.learn = Some(PathBuf::from(value(flag)?)),
            "--limit" => {
                args.limit = Some(
                    value(flag)?
                        .parse()
                        .map_err(|e| format!("bad --limit: {e}"))?,
                )
            }
            "--n-queries" => {
                args.n_queries = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --n-queries: {e}"))?
            }
            "--nlist" => {
                args.nlist = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --nlist: {e}"))?
            }
            "--total-bits" => {
                args.total_bits = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --total-bits: {e}"))?
            }
            "--metric" => {
                args.metric = match value(flag)?.to_lowercase().as_str() {
                    "l2" => Metric::L2,
                    "ip" | "innerproduct" => Metric::InnerProduct,
                    other => return Err(format!("unknown metric: {other}")),
                }
            }
            "--top-k" => {
                args.top_k = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --top-k: {e}"))?
            }
            "--nprobe" => {
                args.nprobe = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --nprobe: {e}"))?
            }
            "--nprobe-sweep" => {
                args.nprobe_sweep = value(flag)?
                    .split(',')
                    .map(|s| {
                        s.trim()
                            .parse::<usize>()
                            .map_err(|e| format!("bad --nprobe-sweep: {e}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
            "--threads" => {
                args.threads = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --threads: {e}"))?
            }
            "--seed" => {
                args.seed = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --seed: {e}"))?
            }
            "--work-dir" => args.work_dir = PathBuf::from(value(flag)?),
            "--out" => args.out = Some(PathBuf::from(value(flag)?)),
            "--reuse" => args.reuse = true,
            // `cargo bench` appends `--bench` (and `cargo test` `--test`) to
            // harness = false targets; ignore them.
            "--bench" | "--test" => {}
            "--help" | "-h" => args.help = true,
            other => return Err(format!("unknown argument: {other}")),
        }
        i += 1;
    }
    if !args.help
        && (args.base.as_os_str().is_empty() || args.query.as_os_str().is_empty())
    {
        return Err("--base and --query are required (see --help)".to_string());
    }
    Ok(args)
}

// ---------------------------------------------------------------------------
// fvecs / ivecs readers
// ---------------------------------------------------------------------------

/// Row-major fvecs contents: `n` vectors of `dim` f32 values.
#[derive(Debug, Clone)]
struct Fvecs {
    dim: usize,
    n: usize,
    data: Vec<f32>,
}

impl Fvecs {
    fn query(&self, i: usize) -> &[f32] {
        &self.data[i * self.dim..(i + 1) * self.dim]
    }
}

/// Row-major ivecs contents: `n` rows of `k` i32 values.
#[derive(Debug, Clone)]
struct Ivecs {
    k: usize,
    n: usize,
    data: Vec<i32>,
}

impl Ivecs {
    fn row(&self, i: usize) -> &[i32] {
        &self.data[i * self.k..(i + 1) * self.k]
    }
}

fn read_fvecs(path: &Path, limit: Option<usize>) -> Result<Fvecs, String> {
    let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut dim_buf = [0u8; 4];
    let mut data: Vec<f32> = Vec::new();
    let mut dim: Option<usize> = None;
    loop {
        if let (Some(d), Some(limit)) = (dim, limit)
            && data.len() / d >= limit
        {
            break;
        }
        // Each record starts with its own dim header.
        match reader.read_exact(&mut dim_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("read {}: {e}", path.display())),
        }
        let record_dim = i32::from_le_bytes(dim_buf);
        if record_dim <= 0 || record_dim > 100_000 {
            return Err(format!(
                "invalid fvecs dim {record_dim} in {}",
                path.display()
            ));
        }
        let record_dim = record_dim as usize;
        let d = *dim.get_or_insert(record_dim);
        if record_dim != d {
            return Err(format!(
                "inconsistent fvecs dim {record_dim} (expected {d}) in {}",
                path.display()
            ));
        }
        let mut record = vec![0u8; d * 4];
        reader
            .read_exact(&mut record)
            .map_err(|e| format!("read record of {}: {e}", path.display()))?;
        data.extend(
            record
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| f32::from_le_bytes(*c)),
        );
    }
    let dim = dim.ok_or_else(|| format!("empty fvecs file {}", path.display()))?;
    Ok(Fvecs {
        dim,
        n: data.len() / dim,
        data,
    })
}

fn read_ivecs(path: &Path, limit: Option<usize>) -> Result<Ivecs, String> {
    let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut reader = BufReader::with_capacity(1 << 20, file);
    let mut k_buf = [0u8; 4];
    let mut data: Vec<i32> = Vec::new();
    let mut k: Option<usize> = None;
    loop {
        if let (Some(kk), Some(limit)) = (k, limit)
            && data.len() / kk >= limit
        {
            break;
        }
        match reader.read_exact(&mut k_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(format!("read {}: {e}", path.display())),
        }
        let record_k = i32::from_le_bytes(k_buf);
        if record_k <= 0 || record_k > 100_000 {
            return Err(format!("invalid ivecs k {record_k} in {}", path.display()));
        }
        let record_k = record_k as usize;
        let kk = *k.get_or_insert(record_k);
        if record_k != kk {
            return Err(format!(
                "inconsistent ivecs k {record_k} (expected {kk}) in {}",
                path.display()
            ));
        }
        let mut record = vec![0u8; kk * 4];
        reader
            .read_exact(&mut record)
            .map_err(|e| format!("read record of {}: {e}", path.display()))?;
        data.extend(
            record
                .as_chunks::<4>()
                .0
                .iter()
                .map(|c| i32::from_le_bytes(*c)),
        );
    }
    let k = k.ok_or_else(|| format!("empty ivecs file {}", path.display()))?;
    Ok(Ivecs {
        k,
        n: data.len() / k,
        data,
    })
}

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

struct Dataset {
    base: Fvecs,
    queries: Fvecs,
    /// Provided or brute-forced ground truth, aligned with `queries`.
    gt: Ivecs,
    /// Extra vectors usable as the stream update pool (optional).
    #[allow(dead_code)]
    learn: Option<Fvecs>,
    gt_source: &'static str,
}

impl Dataset {
    fn dim(&self) -> usize {
        self.base.dim
    }
}

fn load_dataset(args: &Args) -> Result<Dataset, String> {
    println!(
        "loading base {} (limit {:?}) ...",
        args.base.display(),
        args.limit
    );
    let base = read_fvecs(&args.base, args.limit)?;
    println!(
        "loading query {} (limit {}) ...",
        args.query.display(),
        args.n_queries
    );
    let queries = read_fvecs(&args.query, Some(args.n_queries))?;
    if queries.dim != base.dim {
        return Err(format!(
            "dim mismatch: base {} vs query {}",
            base.dim, queries.dim
        ));
    }
    let learn = match &args.learn {
        Some(path) => {
            println!("loading learn {} ...", path.display());
            let learn = read_fvecs(path, None)?;
            if learn.dim != base.dim {
                return Err(format!(
                    "dim mismatch: base {} vs learn {}",
                    base.dim, learn.dim
                ));
            }
            Some(learn)
        }
        None => None,
    };

    let (gt, gt_source) = match &args.gt {
        Some(path) => {
            println!("loading ground truth {} ...", path.display());
            let gt = read_ivecs(path, Some(queries.n))?;
            if gt.n < queries.n {
                return Err(format!(
                    "ground truth has {} rows, need {}",
                    gt.n, queries.n
                ));
            }
            (gt, "provided")
        }
        None => {
            println!(
                "computing brute-force ground truth ({} queries, k={}) ...",
                queries.n, args.top_k
            );
            let t = Instant::now();
            let gt = brute_force_gt(&base, &queries, args.top_k, args.threads);
            println!("brute force done in {:.2}s", t.elapsed().as_secs_f64());
            (gt, "brute_force")
        }
    };

    println!(
        "dataset: {} base x {}d, {} queries, gt k={} ({}), learn={}",
        base.n,
        base.dim,
        queries.n,
        gt.k,
        gt_source,
        learn.as_ref().map(|l| l.n).unwrap_or(0)
    );
    Ok(Dataset {
        base,
        queries,
        gt,
        learn,
        gt_source,
    })
}

// ---------------------------------------------------------------------------
// Brute-force ground truth
// ---------------------------------------------------------------------------

/// Exact L2 top-k for every query, computed in parallel over `threads`.
fn brute_force_gt(base: &Fvecs, queries: &Fvecs, k: usize, threads: usize) -> Ivecs {
    let dim = base.dim;
    let nq = queries.n;
    let k = k.min(base.n).max(1);
    let mut out = vec![0i32; nq * k];
    let nthreads = threads.min(nq).max(1);
    let queries_per_thread = nq.div_ceil(nthreads);

    std::thread::scope(|scope| {
        for (t, out_chunk) in out.chunks_mut(queries_per_thread * k).enumerate() {
            let q0 = t * queries_per_thread;
            let qn = out_chunk.len() / k;
            let base_data = &base.data;
            let query_data = &queries.data;
            scope.spawn(move || {
                let mut dists: Vec<(f32, u32)> = Vec::with_capacity(base.n);
                for qi in 0..qn {
                    let q = &query_data[(q0 + qi) * dim..(q0 + qi + 1) * dim];
                    dists.clear();
                    for vi in 0..base.n {
                        let v = &base_data[vi * dim..(vi + 1) * dim];
                        let mut d = 0f32;
                        for j in 0..dim {
                            let x = q[j] - v[j];
                            d += x * x;
                        }
                        dists.push((d, vi as u32));
                    }
                    dists.select_nth_unstable_by(k - 1, |a, b| a.0.total_cmp(&b.0));
                    dists[..k].sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
                    for (j, (_, id)) in dists[..k].iter().enumerate() {
                        out_chunk[qi * k + j] = *id as i32;
                    }
                }
            });
        }
    });

    Ivecs {
        k,
        n: nq,
        data: out,
    }
}

// ---------------------------------------------------------------------------
// Recall / search metrics
// ---------------------------------------------------------------------------

fn recall_at_k(predicted: &[u64], truth: &[i32], k: usize) -> f64 {
    let gt: std::collections::HashSet<u64> =
        truth.iter().take(k).map(|&x| x as u64).collect();
    if k == 0 {
        return 0.0;
    }
    let hits = predicted
        .iter()
        .take(k)
        .filter(|id| gt.contains(id))
        .count();
    hits as f64 / k as f64
}

#[derive(Debug, Clone, Default)]
struct SearchMetrics {
    nprobe: usize,
    recall_at_k: f64,
    qps: f64,
    batch_ms: f64,
    p50_ms: f64,
    p99_ms: f64,
    mean_ms: f64,
}

fn measure_search(
    index: &IvfRabitqIndex,
    dataset: &Dataset,
    top_k: usize,
    nprobe: usize,
) -> SearchMetrics {
    let params = SearchParams::new(top_k, nprobe);
    let query_refs: Vec<&[f32]> = (0..dataset.queries.n)
        .map(|i| dataset.queries.query(i))
        .collect();

    // Multi-threaded throughput (batch_search uses the rayon pool).
    let t = Instant::now();
    let batch = index.batch_search(&query_refs, params);
    let batch_ms = t.elapsed().as_secs_f64() * 1000.0;
    let qps = if batch_ms > 0.0 {
        dataset.queries.n as f64 / (batch_ms / 1000.0)
    } else {
        f64::INFINITY
    };

    // Recall over the batch results.
    let mut recall_sum = 0.0;
    let mut n_ok = 0usize;
    for (i, result) in batch.iter().enumerate() {
        if let Ok(results) = result {
            let predicted: Vec<u64> = results.iter().map(|r| r.id).collect();
            recall_sum += recall_at_k(&predicted, dataset.gt.row(i), top_k);
            n_ok += 1;
        }
    }
    let recall_at_k = if n_ok > 0 {
        recall_sum / n_ok as f64
    } else {
        0.0
    };

    // Single-threaded latency.
    let mut latencies = Vec::with_capacity(dataset.queries.n);
    for q in &query_refs {
        let t = Instant::now();
        let _ = index.search(q, params);
        latencies.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    latencies.sort_by(|a, b| a.total_cmp(b));
    let percentile = |p: f64| -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }
        let idx = ((latencies.len() as f64 - 1.0) * p).round() as usize;
        latencies[idx]
    };
    let mean_ms = latencies.iter().sum::<f64>() / latencies.len().max(1) as f64;

    SearchMetrics {
        nprobe,
        recall_at_k,
        qps,
        batch_ms,
        p50_ms: percentile(0.50),
        p99_ms: percentile(0.99),
        mean_ms,
    }
}

// ---------------------------------------------------------------------------
// Metrics: RSS / disk
// ---------------------------------------------------------------------------

fn status_kb(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status.lines().find_map(|line| {
        let (key, rest) = line.split_once(':')?;
        if key == field {
            rest.trim().trim_end_matches(" kB").trim().parse().ok()
        } else {
            None
        }
    })
}

fn rss_mb() -> f64 {
    status_kb("VmRSS").unwrap_or(0) as f64 / 1024.0
}

fn peak_rss_mb() -> f64 {
    status_kb("VmHWM").unwrap_or(0) as f64 / 1024.0
}

fn dir_size_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    let mut total = 0u64;
    for entry in entries.flatten() {
        let p = entry.path();
        if p.is_dir() {
            total += dir_size_bytes(&p);
        } else if let Ok(meta) = entry.metadata() {
            total += meta.len();
        }
    }
    total
}

fn index_size_bytes(work_dir: &Path) -> u64 {
    dir_size_bytes(&work_dir.join("_vector_index"))
}

// ---------------------------------------------------------------------------
// LakeSoul (vortex) data files
// ---------------------------------------------------------------------------

fn vector_schema(dim: usize) -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(PK_COLUMN, DataType::UInt64, false),
        Field::new(
            VEC_COLUMN,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
            ),
            false,
        ),
    ]))
}

fn vector_batch(
    schema: &SchemaRef,
    ids: &[u64],
    vectors: &[f32],
    dim: usize,
) -> Result<RecordBatch, String> {
    debug_assert_eq!(ids.len() * dim, vectors.len());
    let id_array = UInt64Array::from(ids.to_vec());
    let mut builder = FixedSizeListBuilder::new(Float32Builder::new(), dim as i32);
    for vector in vectors.chunks_exact(dim) {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    let vec_array = builder.finish();
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(vec_array)],
    )
    .map_err(|e| format!("build record batch: {e}"))
}

/// Write `ids`/`vectors` as a LakeSoul table data file in **vortex** format.
///
/// Returns the produced file paths (one per flush).
async fn write_vortex_data(
    work_dir: &Path,
    dim: usize,
    ids: &[u64],
    vectors: &[f32],
) -> Result<Vec<String>, String> {
    let schema = vector_schema(dim);
    let config = LakeSoulIOConfigBuilder::new()
        .with_prefix(work_dir.to_string_lossy().to_string())
        .with_schema(schema.clone())
        .with_physical_format(PhysicalFormat::Vortex)
        .with_hash_bucket_num("1")
        .with_batch_size(WRITE_BATCH_SIZE)
        .build();
    let mut writer = create_writer_with_io_config(config)
        .await
        .map_err(|e| format!("create vortex writer: {e}"))?;

    for chunk_start in (0..ids.len()).step_by(WRITE_BATCH_SIZE) {
        let chunk_end = (chunk_start + WRITE_BATCH_SIZE).min(ids.len());
        let batch = vector_batch(
            &schema,
            &ids[chunk_start..chunk_end],
            &vectors[chunk_start * dim..chunk_end * dim],
            dim,
        )?;
        writer
            .write_record_batch(batch)
            .await
            .map_err(|e| format!("write batch: {e}"))?;
    }
    let outputs = writer
        .flush_and_close()
        .await
        .map_err(|e| format!("flush vortex writer: {e}"))?;
    Ok(outputs.into_iter().map(|o| o.file_path).collect())
}

/// List the vortex data files directly under `work_dir` (sorted for
/// deterministic shard prefix derivation).
fn find_vortex_files(work_dir: &Path) -> Vec<String> {
    let mut files: Vec<String> = std::fs::read_dir(work_dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| p.extension().map(|e| e == "vortex").unwrap_or(false))
                .map(|p| p.to_string_lossy().to_string())
                .collect()
        })
        .unwrap_or_default();
    files.sort();
    files
}

fn vector_index_config(args: &Args, dim: usize) -> VectorIndexConfig {
    VectorIndexConfig {
        column_name: VEC_COLUMN.to_string(),
        dim,
        nlist: args.nlist,
        total_bits: args.total_bits,
        metric: args.metric,
        rotator_type: RotatorType::FhtKacRotator,
        seed: args.seed,
        use_faster_config: true,
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: 1.0,
    }
}

fn manifest_store(files: &[String]) -> ManifestStore {
    let prefix = shard_index_prefix(files, VEC_COLUMN);
    ManifestStore::new(Arc::new(LocalFileSystem::new()), prefix)
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

async fn run_build(args: &Args, dataset: &Dataset) -> Result<Value, String> {
    let dim = dataset.dim();
    let work_dir = &args.work_dir;
    std::fs::create_dir_all(work_dir).map_err(|e| format!("mkdir: {e}"))?;

    // Deterministic re-runs: drop any stale index and data files unless the
    // caller explicitly wants to reuse them.
    if !args.reuse {
        let index_dir = work_dir.join("_vector_index");
        if index_dir.exists() {
            std::fs::remove_dir_all(&index_dir).map_err(|e| format!("clean: {e}"))?;
        }
        for file in find_vortex_files(work_dir) {
            let _ = std::fs::remove_file(file);
        }
    }

    // 1. Write the base dataset as LakeSoul vortex files.
    let ids: Vec<u64> = (0..dataset.base.n as u64).collect();
    let t = Instant::now();
    let files = write_vortex_data(work_dir, dim, &ids, &dataset.base.data).await?;
    let data_write_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!(
        "wrote {} vortex file(s) in {:.1}ms: {:?}",
        files.len(),
        data_write_ms,
        files
    );

    // 2. Build the vector index (fresh).
    let rss_before = rss_mb();
    let store = Arc::new(LocalFileSystem::new());
    let builder = VectorShardIndexBuilder::new(
        store,
        vector_index_config(args, dim),
        files.clone(),
        PK_COLUMN.to_string(),
        HashMap::new(),
        Some(format!("file://{}", work_dir.display())),
    );
    let t = Instant::now();
    builder
        .build()
        .await
        .map_err(|e| format!("index build failed: {e:?}"))?;
    let build_ms = t.elapsed().as_secs_f64() * 1000.0;
    let rss_after = rss_mb();

    // 3. Collect index stats.
    let mstore = manifest_store(&files);
    let stats = index_stats(&mstore)
        .await
        .map_err(|e| format!("index_stats: {e}"))?;
    let clusters = cluster_stats(&mstore)
        .await
        .map_err(|e| format!("cluster_stats: {e}"))?
        .unwrap_or_default();
    let max_cluster_ratio = clusters
        .iter()
        .map(|c| c.delta_ratio())
        .fold(0.0f32, f32::max);

    let index_bytes = index_size_bytes(work_dir);
    let summary = json!({
        "scenario": "build",
        "format": "vortex",
        "dim": dim,
        "n_base": dataset.base.n,
        "n_queries": dataset.queries.n,
        "gt_source": dataset.gt_source,
        "config": {
            "nlist": args.nlist,
            "total_bits": args.total_bits,
            "metric": format!("{:?}", args.metric),
            "threads": args.threads,
        },
        "data_write_ms": data_write_ms,
        "build_ms": build_ms,
        "rss_before_mb": rss_before,
        "rss_after_mb": rss_after,
        "peak_rss_mb": peak_rss_mb(),
        "index_size_bytes": index_bytes,
        "index_stats": stats.as_ref().map(|s| json!({
            "base_segments": s.base_segments,
            "delta_segments": s.delta_segments,
            "base_vectors": s.base_vectors,
            "delta_vectors": s.delta_vectors,
        })),
        "clusters": clusters.len(),
        "max_cluster_delta_ratio": max_cluster_ratio,
        "data_files": files,
    });
    print_summary(&summary);
    Ok(summary)
}

async fn run_search(args: &Args, dataset: &Dataset) -> Result<Value, String> {
    let dim = dataset.dim();
    let work_dir = &args.work_dir;
    std::fs::create_dir_all(work_dir).map_err(|e| format!("mkdir: {e}"))?;

    let files = find_vortex_files(work_dir);
    if files.is_empty() {
        return Err(format!(
            "no vortex data files under {} — run --scenario build first",
            work_dir.display()
        ));
    }

    let mstore = manifest_store(&files);
    let has_index = lakesoul_vector::rabitq::manifest::manifest_exists(&mstore).await;
    if !has_index {
        if args.reuse {
            return Err(format!(
                "no index under {} (and --reuse was given)",
                work_dir.display()
            ));
        }
        println!("no index found; building fresh ...");
        VectorShardIndexBuilder::new(
            Arc::new(LocalFileSystem::new()),
            vector_index_config(args, dim),
            files.clone(),
            PK_COLUMN.to_string(),
            HashMap::new(),
            Some(format!("file://{}", work_dir.display())),
        )
        .build()
        .await
        .map_err(|e| format!("index build failed: {e:?}"))?;
    }

    // Load the index and measure load time.
    let t = Instant::now();
    let index = IvfRabitqIndex::load_from_v4(&mstore)
        .await
        .map_err(|e| format!("load index: {e}"))?;
    let load_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!(
        "loaded index ({} vectors, {} clusters) in {:.1}ms",
        index.len(),
        index.cluster_count(),
        load_ms
    );

    let mut sweep = Vec::new();
    for &nprobe in &args.nprobe_sweep {
        let m = measure_search(&index, dataset, args.top_k, nprobe);
        println!(
            "nprobe={:<4} recall@{}={:.4} qps={:>10.1} p50={:.3}ms p99={:.3}ms",
            m.nprobe, args.top_k, m.recall_at_k, m.qps, m.p50_ms, m.p99_ms
        );
        sweep.push(json!({
            "nprobe": m.nprobe,
            "recall_at_k": m.recall_at_k,
            "qps": m.qps,
            "batch_ms": m.batch_ms,
            "p50_ms": m.p50_ms,
            "p99_ms": m.p99_ms,
            "mean_ms": m.mean_ms,
        }));
    }

    let summary = json!({
        "scenario": "search",
        "format": "vortex",
        "dim": dim,
        "n_base": dataset.base.n,
        "n_queries": dataset.queries.n,
        "top_k": args.top_k,
        "gt_source": dataset.gt_source,
        "load_ms": load_ms,
        "index_size_bytes": index_size_bytes(work_dir),
        "sweep": sweep,
    });
    print_summary(&summary);
    Ok(summary)
}

async fn run_stream(_args: &Args, _dataset: &Dataset) -> Result<Value, String> {
    Err("scenario 'stream' is not implemented yet (skeleton stage)".to_string())
}

async fn run_trigger(_args: &Args, _dataset: &Dataset) -> Result<Value, String> {
    Err("scenario 'trigger' is not implemented yet (skeleton stage)".to_string())
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

fn print_summary(summary: &Value) {
    println!("\n── summary ──");
    println!(
        "{}",
        serde_json::to_string_pretty(summary).unwrap_or_default()
    );
}

fn write_out(path: &Path, summary: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir: {e}"))?;
    }
    std::fs::write(path, serde_json::to_string_pretty(summary).unwrap())
        .map_err(|e| format!("write {}: {e}", path.display()))
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("error: {e}\n");
            eprintln!("{}", usage());
            std::process::exit(2);
        }
    };
    if args.help {
        println!("{}", usage());
        return;
    }
    if std::env::var("RAYON_NUM_THREADS").is_err() {
        // SAFETY: single-threaded at this point, before any rayon pool exists.
        unsafe { std::env::set_var("RAYON_NUM_THREADS", args.threads.to_string()) };
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(args.threads.min(8))
        .build()
        .expect("tokio runtime");

    let result: Result<Value, String> = runtime.block_on(async {
        let dataset = load_dataset(&args)?;
        match args.scenario {
            Scenario::Build => run_build(&args, &dataset).await,
            Scenario::Search => run_search(&args, &dataset).await,
            Scenario::Stream => run_stream(&args, &dataset).await,
            Scenario::Trigger => run_trigger(&args, &dataset).await,
        }
    });

    match result {
        Ok(summary) => {
            if let Some(out) = &args.out
                && let Err(e) = write_out(out, &summary)
            {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
