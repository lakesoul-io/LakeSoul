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

use arrow::array::{
    FixedSizeListBuilder, Float32Builder, Int64Array, ListBuilder, RecordBatch,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{
    CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
};
use datafusion::datasource::memory::MemTable;
use lakesoul_datafusion::cli::CoreArgs;
use lakesoul_datafusion::udf::vector_search_marker::LakeSoulVectorSearchOptions;
use lakesoul_datafusion::vector_index::{
    VectorIndexTableConfig, auto_build_vector_index, vector_index_columns_to_json,
};
use lakesoul_io::config::LakeSoulIOConfigBuilder;
use lakesoul_io::file_format::PhysicalFormat;
use lakesoul_io::vector::builder::{VectorShardIndexBuilder, shard_index_prefix};
use lakesoul_io::writer::create_writer_with_io_config;
use lakesoul_vector::rabitq::manifest::resolve_view;
use lakesoul_vector::{
    IvfRabitqIndex, ManifestStore, Metric, RotatorType, SearchParams, VectorIndexConfig,
    cluster_stats, index_stats,
};
use object_store::local::LocalFileSystem;
use rand::SeedableRng;
use rand::rngs::StdRng;
use serde_json::{Value, json};

const PK_COLUMN: &str = "id";
const VEC_COLUMN: &str = "vec";
const DEFAULT_WORK_DIR: &str = "/tmp/lakesoul_test/vector_bench";
const WRITE_BATCH_SIZE: usize = 8192;
/// `max_delta_ratio` used to force a rebuild through the real auto policy
/// (`always` and the periodic rounds): any non-zero delta exceeds it.
const FORCE_REBUILD_RATIO: f32 = 1e-9;

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Build,
    Search,
    Stream,
    Trigger,
    /// End-to-end DataFusion SQL: write through INSERT, search through
    /// `ORDER BY array_distance(...) LIMIT k` (index candidates + exact
    /// re-rank).  Requires PostgreSQL metadata.
    Sql,
}

/// Rebuild policy exercised by the `stream`/`trigger` scenarios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Policy {
    /// `rebuild_mode: "none"` — incremental delta segments only.
    None,
    /// `rebuild_mode: "auto"` with `--max-delta-ratio`.
    Auto,
    /// Incremental, with a forced rebuild every `--period` rounds.
    Periodic,
    /// Rebuild on every round (forced through the auto policy).
    Always,
}

/// How the per-round update vectors are generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Drift {
    /// Uniformly sampled from the update pool (learn set or base).
    Uniform,
    /// Sampled from a small region around a random anchor (skewed growth).
    Skew,
    /// Uniform samples translated by a fixed offset (distribution shift).
    Shift,
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
    // ---- stream / trigger ----
    policy: Policy,
    max_delta_ratio: f32,
    /// Periodic policy: force a rebuild every N rounds.
    period: usize,
    rounds: usize,
    per_round: usize,
    drift: Drift,
    /// `shift` drift: offset magnitude as a fraction of the mean vector norm.
    drift_strength: f32,
    /// Measure recall/QPS every N rounds (brute-force GT is recomputed).
    checkpoint_every: usize,
    /// Checkpoint queries follow the drift distribution (default).  With
    /// `--static-queries` the dataset's fixed queries are used instead.
    query_drift: bool,
    /// Table name for the SQL scenario.
    table: String,
    /// Physical format for the SQL scenario's table ("parquet" or "vortex").
    sql_format: String,
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
            policy: Policy::Auto,
            max_delta_ratio: 1.0,
            period: 3,
            rounds: 10,
            per_round: 10_000,
            drift: Drift::Uniform,
            drift_strength: 0.5,
            checkpoint_every: 1,
            query_drift: true,
            table: "vec_bench_sql".to_string(),
            sql_format: "vortex".to_string(),
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
  --scenario <build|search|stream|trigger|sql>  Scenario to run (default: build)
  --base <path.fvecs>                       Base vectors (required)
  --query <path.fvecs>                      Query vectors (required)
  --gt <path.ivecs>                         Ground truth (optional; brute-forced if absent)
  --learn <path.fvecs>                      Update pool for stream (default: base vectors)
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

STREAM / TRIGGER:
  --policy <none|auto|periodic|always>      Rebuild policy (default: auto)
  --max-delta-ratio <F>                     Auto policy threshold (default: 1.0)
  --period <N>                              Periodic policy: rebuild every N rounds (default: 3)
  --rounds <N>                              Update rounds (default: 10)
  --per-round <N>                           Vectors written per round (default: 10000)
  --drift <uniform|skew|shift>              Update distribution (default: uniform)
  --drift-strength <F>                      Shift offset / mean-norm fraction (default: 0.5)
  --checkpoint-every <N>                    Recall/QPS checkpoint every N rounds (default: 1)
  --static-queries                          Measure recall on the dataset's fixed queries
                                            (default: checkpoint queries follow the drift)
  --table <name>                            Table name for the SQL scenario (default: vec_bench_sql)
  --sql-format <parquet|vortex>              Data file format for the SQL scenario (default: vortex)

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
                    "sql" => Scenario::Sql,
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
            "--table" => args.table = value(flag)?,
            "--sql-format" => args.sql_format = value(flag)?,
            "--policy" => {
                args.policy = match value(flag)?.to_lowercase().as_str() {
                    "none" => Policy::None,
                    "auto" => Policy::Auto,
                    "periodic" => Policy::Periodic,
                    "always" => Policy::Always,
                    other => return Err(format!("unknown policy: {other}")),
                }
            }
            "--max-delta-ratio" => {
                args.max_delta_ratio = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --max-delta-ratio: {e}"))?
            }
            "--period" => {
                args.period = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --period: {e}"))?
            }
            "--rounds" => {
                args.rounds = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --rounds: {e}"))?
            }
            "--per-round" => {
                args.per_round = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --per-round: {e}"))?
            }
            "--drift" => {
                args.drift = match value(flag)?.to_lowercase().as_str() {
                    "uniform" => Drift::Uniform,
                    "skew" => Drift::Skew,
                    "shift" => Drift::Shift,
                    other => return Err(format!("unknown drift: {other}")),
                }
            }
            "--drift-strength" => {
                args.drift_strength = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --drift-strength: {e}"))?
            }
            "--checkpoint-every" => {
                args.checkpoint_every = value(flag)?
                    .parse()
                    .map_err(|e| format!("bad --checkpoint-every: {e}"))?
            }
            "--static-queries" => args.query_drift = false,
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
    if matches!(args.scenario, Scenario::Stream | Scenario::Trigger) {
        if args.per_round == 0 {
            return Err("--per-round must be > 0".to_string());
        }
        if args.rounds == 0 {
            return Err("--rounds must be > 0".to_string());
        }
        if args.period == 0 {
            return Err("--period must be > 0".to_string());
        }
        if args.checkpoint_every == 0 {
            return Err("--checkpoint-every must be > 0".to_string());
        }
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
        // The search scenario computes the GT over the vectors actually
        // stored in the index (base + deltas), so defer it.
        None if args.scenario == Scenario::Search => (
            Ivecs {
                k: args.top_k,
                n: 0,
                data: Vec::new(),
            },
            "indexed_files",
        ),
        None => {
            println!(
                "computing brute-force ground truth ({} queries, k={}) ...",
                queries.n, args.top_k
            );
            let t = Instant::now();
            let gt =
                brute_force_gt(&base.data, base.dim, &queries, args.top_k, args.threads);
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
///
/// `base` is the flat row-major current vector set (`base.len() / dim`
/// vectors); the returned ids are row indices.
fn brute_force_gt(
    base: &[f32],
    dim: usize,
    queries: &Fvecs,
    k: usize,
    threads: usize,
) -> Ivecs {
    let base_n = base.len() / dim;
    let nq = queries.n;
    let k = k.min(base_n).max(1);
    let mut out = vec![0i32; nq * k];
    let nthreads = threads.min(nq).max(1);
    let queries_per_thread = nq.div_ceil(nthreads);

    std::thread::scope(|scope| {
        for (t, out_chunk) in out.chunks_mut(queries_per_thread * k).enumerate() {
            let q0 = t * queries_per_thread;
            let qn = out_chunk.len() / k;
            let query_data = &queries.data;
            scope.spawn(move || {
                let mut dists: Vec<(f32, u32)> = Vec::with_capacity(base_n);
                for qi in 0..qn {
                    let q = &query_data[(q0 + qi) * dim..(q0 + qi + 1) * dim];
                    dists.clear();
                    for vi in 0..base_n {
                        let v = &base[vi * dim..(vi + 1) * dim];
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

fn measure_index(
    index: &IvfRabitqIndex,
    queries: &Fvecs,
    gt: &Ivecs,
    top_k: usize,
    nprobe: usize,
) -> SearchMetrics {
    let params = SearchParams::new(top_k, nprobe);
    let query_refs: Vec<&[f32]> = (0..queries.n).map(|i| queries.query(i)).collect();

    // Multi-threaded throughput (batch_search uses the rayon pool).
    let t = Instant::now();
    let batch = index.batch_search(&query_refs, params);
    let batch_ms = t.elapsed().as_secs_f64() * 1000.0;
    let qps = if batch_ms > 0.0 {
        queries.n as f64 / (batch_ms / 1000.0)
    } else {
        f64::INFINITY
    };

    // Recall over the batch results.
    let mut recall_sum = 0.0;
    let mut n_ok = 0usize;
    for (i, result) in batch.iter().enumerate() {
        if let Ok(results) = result {
            let predicted: Vec<u64> = results.iter().map(|r| r.id).collect();
            recall_sum += recall_at_k(&predicted, gt.row(i), top_k);
            n_ok += 1;
        }
    }
    let recall_at_k = if n_ok > 0 {
        recall_sum / n_ok as f64
    } else {
        0.0
    };

    // Single-threaded latency.
    let mut latencies = Vec::with_capacity(queries.n);
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

/// List the LakeSoul data files directly under `work_dir` (vortex from the
/// index-level scenarios, parquet from the SQL scenario), sorted for
/// deterministic shard prefix derivation.
fn find_data_files(work_dir: &Path) -> Vec<String> {
    let mut files: Vec<String> = std::fs::read_dir(work_dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| e.path())
                .filter(|p| {
                    p.extension()
                        .map(|e| e == "vortex" || e == "parquet")
                        .unwrap_or(false)
                })
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

/// Every vector-index shard prefix for `files` (indices are stored per
/// `(partition_desc, hash bucket)`).
fn all_shard_prefixes(files: &[String], column: &str) -> Vec<String> {
    let base = files
        .first()
        .and_then(|u| {
            let u = u
                .trim_start_matches("file://")
                .trim_start_matches("s3://")
                .trim_start_matches("s3a://");
            std::path::Path::new(u.trim_end_matches('/'))
                .parent()?
                .to_str()
                .map(|s| s.to_string())
        })
        .unwrap_or_default();
    lakesoul_io::vector::search::derive_index_prefixes(files, &base, column)
        .into_iter()
        .map(|(prefix, _)| prefix)
        .collect()
}

/// Aggregate [`RoundStats`] over every shard of the table's index.
async fn collect_all_shard_stats(files: &[String]) -> Result<Value, String> {
    let mut shards = 0usize;
    let mut generation = 0u64;
    let mut base_vectors = 0usize;
    let mut delta_vectors = 0usize;
    let mut max_cluster_delta_ratio = 0.0f32;
    let mut violating_clusters = 0usize;
    for prefix in all_shard_prefixes(files, VEC_COLUMN) {
        let mstore = ManifestStore::new(Arc::new(LocalFileSystem::new()), prefix);
        let stats = collect_round_stats(&mstore).await?;
        shards += 1;
        generation = generation.max(stats.generation);
        base_vectors += stats.base_vectors;
        delta_vectors += stats.delta_vectors;
        max_cluster_delta_ratio =
            max_cluster_delta_ratio.max(stats.max_cluster_delta_ratio);
        violating_clusters += stats.violating_clusters;
    }
    Ok(json!({
        "shards": shards,
        "generation": generation,
        "base_vectors": base_vectors,
        "delta_vectors": delta_vectors,
        "max_cluster_delta_ratio": max_cluster_delta_ratio,
        "violating_clusters": violating_clusters,
    }))
}

/// Read every vector stored in the LakeSoul data files under `work_dir`
/// back through the native reader, ordered by id (ids are dense 0..n).
async fn read_indexed_vectors(
    files: &[String],
    work_dir: &Path,
    dim: usize,
) -> Result<Fvecs, String> {
    let urls: Vec<String> = files
        .iter()
        .map(|f| {
            if f.starts_with("file://") {
                f.clone()
            } else {
                format!("file://{f}")
            }
        })
        .collect();
    let config = LakeSoulIOConfigBuilder::new()
        .with_files(urls)
        .with_prefix(work_dir.to_string_lossy().to_string())
        .with_schema(vector_schema(dim))
        .with_primary_keys(vec![PK_COLUMN.to_string()])
        .build();
    let mut reader = lakesoul_io::reader::LakeSoulReader::new(config)
        .map_err(|e| format!("create reader: {e}"))?;
    reader
        .start()
        .await
        .map_err(|e| format!("reader start: {e}"))?;

    let mut rows: Vec<(u64, Vec<f32>)> = Vec::new();
    while let Some(batch) = reader.next_rb().await {
        let batch = batch.map_err(|e| format!("read batch: {e}"))?;
        if batch.num_rows() == 0 {
            continue;
        }
        let id_col = batch
            .column_by_name(PK_COLUMN)
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| format!("missing u64 column '{PK_COLUMN}'"))?;
        let vec_col = batch
            .column_by_name(VEC_COLUMN)
            .and_then(|c| {
                c.as_any()
                    .downcast_ref::<arrow::array::FixedSizeListArray>()
            })
            .ok_or_else(|| format!("missing FixedSizeList column '{VEC_COLUMN}'"))?;
        for i in 0..batch.num_rows() {
            let values = vec_col.value(i);
            let floats = values
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .ok_or_else(|| "vector values must be Float32".to_string())?;
            rows.push((id_col.value(i), floats.values().to_vec()));
        }
    }

    let max_id = rows.iter().map(|(id, _)| *id).max().unwrap_or(0) as usize;
    let mut data = vec![0f32; (max_id + 1) * dim];
    let mut seen = vec![false; max_id + 1];
    for (id, vector) in &rows {
        let start = *id as usize * dim;
        data[start..start + dim].copy_from_slice(vector);
        seen[*id as usize] = true;
    }
    if seen.iter().any(|s| !s) {
        return Err(format!(
            "non-contiguous vector ids in {} ({} rows read)",
            work_dir.display(),
            rows.len()
        ));
    }
    Ok(Fvecs {
        dim,
        n: max_id + 1,
        data,
    })
}

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------

async fn run_build(args: &Args, dataset: &Dataset) -> Result<Value, String> {
    let dim = dataset.dim();
    let work_dir = prepare_work_dir(&args.work_dir)?;

    // Deterministic re-runs: drop any stale index and data files unless the
    // caller explicitly wants to reuse them.
    if !args.reuse {
        let index_dir = work_dir.join("_vector_index");
        if index_dir.exists() {
            std::fs::remove_dir_all(&index_dir).map_err(|e| format!("clean: {e}"))?;
        }
        for file in find_data_files(&work_dir) {
            let _ = std::fs::remove_file(file);
        }
    }

    // 1. Write the base dataset as LakeSoul vortex files.
    let ids: Vec<u64> = (0..dataset.base.n as u64).collect();
    let t = Instant::now();
    let files = write_vortex_data(&work_dir, dim, &ids, &dataset.base.data).await?;
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

    let index_bytes = index_size_bytes(&work_dir);
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
    let work_dir = prepare_work_dir(&args.work_dir)?;

    let files = find_data_files(&work_dir);
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

    // Ground truth for the indexed dataset (base + deltas): when no
    // `--gt` is given, read every vector back from the vortex files and
    // brute-force the top-k over them.
    let gt = if dataset.gt.n == dataset.queries.n {
        dataset.gt.clone()
    } else {
        let all = read_indexed_vectors(&files, &work_dir, dim).await?;
        println!(
            "computing brute-force GT over {} indexed vectors ...",
            all.n
        );
        brute_force_gt(&all.data, dim, &dataset.queries, args.top_k, args.threads)
    };

    let mut sweep = Vec::new();
    for &nprobe in &args.nprobe_sweep {
        let m = measure_index(&index, &dataset.queries, &gt, args.top_k, nprobe);
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
        "index_size_bytes": index_size_bytes(&work_dir),
        "sweep": sweep,
    });
    print_summary(&summary);
    Ok(summary)
}

// ---------------------------------------------------------------------------
// Stream / trigger helpers
// ---------------------------------------------------------------------------

fn metric_str(metric: Metric) -> &'static str {
    match metric {
        Metric::L2 => "L2",
        Metric::InnerProduct => "IP",
    }
}

fn policy_name(policy: Policy) -> &'static str {
    match policy {
        Policy::None => "none",
        Policy::Auto => "auto",
        Policy::Periodic => "periodic",
        Policy::Always => "always",
    }
}

fn drift_name(drift: Drift) -> &'static str {
    match drift {
        Drift::Uniform => "uniform",
        Drift::Skew => "skew",
        Drift::Shift => "shift",
    }
}

/// The table-property entry consumed by the real `auto_build_vector_index`
/// policy path.
fn table_config(
    args: &Args,
    dim: usize,
    rebuild_mode: &str,
    max_delta_ratio: f32,
) -> VectorIndexTableConfig {
    VectorIndexTableConfig {
        column: VEC_COLUMN.to_string(),
        dim,
        nlist: args.nlist,
        total_bits: args.total_bits,
        metric: metric_str(args.metric).to_string(),
        rotator_type: "FhtKac".to_string(),
        seed: args.seed,
        use_faster_config: true,
        rebuild_mode: rebuild_mode.to_string(),
        max_delta_ratio,
    }
}

/// Per-round index state collected from the manifest.
#[derive(Debug, Clone)]
struct RoundStats {
    generation: u64,
    clusters: usize,
    base_vectors: usize,
    delta_vectors: usize,
    delta_segments: usize,
    shard_delta_ratio: f32,
    max_cluster_delta_ratio: f32,
    violating_clusters: usize,
}

impl RoundStats {
    fn to_json(&self) -> Value {
        json!({
            "generation": self.generation,
            "clusters": self.clusters,
            "base_vectors": self.base_vectors,
            "delta_vectors": self.delta_vectors,
            "delta_segments": self.delta_segments,
            "shard_delta_ratio": self.shard_delta_ratio,
            "max_cluster_delta_ratio": self.max_cluster_delta_ratio,
            "violating_clusters": self.violating_clusters,
        })
    }
}

async fn collect_round_stats(mstore: &ManifestStore) -> Result<RoundStats, String> {
    let stats = index_stats(mstore)
        .await
        .map_err(|e| format!("index_stats: {e}"))?
        .unwrap_or_default();
    let clusters = cluster_stats(mstore)
        .await
        .map_err(|e| format!("cluster_stats: {e}"))?
        .unwrap_or_default();
    let max_cluster_delta_ratio = clusters
        .iter()
        .map(|c| c.delta_ratio())
        .fold(0.0f32, f32::max);
    let violating_clusters = clusters.iter().filter(|c| c.delta_ratio() > 1.0).count();
    let generation = resolve_view(mstore)
        .await
        .map_err(|e| format!("resolve_view: {e}"))?
        .map(|v| v.generation)
        .unwrap_or(0);
    Ok(RoundStats {
        generation,
        clusters: clusters.len(),
        base_vectors: stats.base_vectors,
        delta_vectors: stats.delta_vectors,
        delta_segments: stats.delta_segments,
        shard_delta_ratio: stats.delta_ratio(),
        max_cluster_delta_ratio,
        violating_clusters,
    })
}

/// Update sampler holding the pool plus the pre-computed skew candidates and
/// the fixed shift offset.
struct UpdateSampler {
    pool: Arc<Vec<f32>>,
    pool_n: usize,
    dim: usize,
    skew_rows: Vec<u32>,
    shift_offset: Vec<f32>,
    rng: StdRng,
}

impl UpdateSampler {
    /// Build a sampler over `pool` with a shared skew region and shift
    /// direction, so update and query samplers target the same distribution.
    fn new(
        dataset: &Dataset,
        pool: Arc<Vec<f32>>,
        pool_n: usize,
        rng_seed: u64,
        skew_rows: Vec<u32>,
        shift_offset: Vec<f32>,
    ) -> Self {
        Self {
            pool,
            pool_n,
            dim: dataset.dim(),
            skew_rows,
            shift_offset,
            rng: StdRng::seed_from_u64(rng_seed),
        }
    }

    /// Sample `count` vectors according to the configured drift.
    fn sample(&mut self, count: usize) -> Vec<f32> {
        use rand::Rng;
        let dim = self.dim;
        let mut out = Vec::with_capacity(count * dim);
        for _ in 0..count {
            let row = if self.skew_rows.is_empty() {
                self.rng.random_range(0..self.pool_n)
            } else {
                let idx = self.rng.random_range(0..self.skew_rows.len());
                self.skew_rows[idx] as usize
            };
            let src = &self.pool[row * dim..(row + 1) * dim];
            if self.shift_offset.is_empty() {
                out.extend_from_slice(src);
            } else {
                out.extend(src.iter().zip(&self.shift_offset).map(|(a, b)| a + b));
            }
        }
        out
    }
}

/// Build the update and query samplers over the shared drift definition
/// (same skew region / shift direction, different random streams).
fn make_samplers(args: &Args, dataset: &Dataset) -> (UpdateSampler, UpdateSampler) {
    let dim = dataset.dim();
    let (pool, pool_n) = match &dataset.learn {
        Some(learn) => (learn.data.clone(), learn.n),
        None => (dataset.base.data.clone(), dataset.base.n),
    };
    let pool = Arc::new(pool);
    let skew_rows = if args.drift == Drift::Skew {
        make_skew_rows(
            &pool,
            pool_n,
            dim,
            &mut StdRng::seed_from_u64(args.seed ^ 0xA1),
        )
    } else {
        Vec::new()
    };
    let shift_offset = if args.drift == Drift::Shift {
        make_shift_offset(
            &pool,
            pool_n,
            dim,
            args.drift_strength,
            &mut StdRng::seed_from_u64(args.seed ^ 0xB2),
        )
    } else {
        Vec::new()
    };
    (
        UpdateSampler::new(
            dataset,
            pool.clone(),
            pool_n,
            args.seed ^ 0x9e37_79b9_7f4a_7c15,
            skew_rows.clone(),
            shift_offset.clone(),
        ),
        UpdateSampler::new(
            dataset,
            pool,
            pool_n,
            args.seed ^ 0xdead_beef,
            skew_rows,
            shift_offset,
        ),
    )
}

/// Candidate rows for skewed growth: the closest `pool_n / 20` vectors to a
/// random anchor, i.e. a small concentrated region of the space.
fn make_skew_rows(pool: &[f32], pool_n: usize, dim: usize, rng: &mut StdRng) -> Vec<u32> {
    use rand::Rng;
    let anchor = rng.random_range(0..pool_n);
    let anchor_vec = &pool[anchor * dim..(anchor + 1) * dim];
    let mut dists: Vec<(f32, u32)> = (0..pool_n)
        .map(|i| {
            let v = &pool[i * dim..(i + 1) * dim];
            let d: f32 = v
                .iter()
                .zip(anchor_vec)
                .map(|(a, b)| {
                    let x = a - b;
                    x * x
                })
                .sum();
            (d, i as u32)
        })
        .collect();
    let keep = (pool_n / 20).max(1);
    dists.select_nth_unstable_by(keep - 1, |a, b| a.0.total_cmp(&b.0));
    dists.truncate(keep);
    dists.iter().map(|(_, i)| *i).collect()
}

/// Fixed translation for shift drift: a random unit direction scaled by
/// `strength * mean_vector_norm`.
fn make_shift_offset(
    pool: &[f32],
    pool_n: usize,
    dim: usize,
    strength: f32,
    rng: &mut StdRng,
) -> Vec<f32> {
    use rand::Rng;
    let mut dir: Vec<f32> = (0..dim).map(|_| rng.random::<f32>() * 2.0 - 1.0).collect();
    let norm = dir.iter().map(|x| x * x).sum::<f32>().sqrt().max(1e-9);
    for x in &mut dir {
        *x /= norm;
    }
    let sample = pool_n.clamp(1, 1024);
    let mean_norm = (0..sample)
        .map(|i| {
            let v = &pool[i * dim..(i + 1) * dim];
            v.iter().map(|x| x * x).sum::<f32>().sqrt()
        })
        .sum::<f32>()
        / sample as f32;
    let scale = strength * mean_norm;
    dir.iter().map(|x| x * scale).collect()
}

/// Create `work_dir` and return its absolute path (the local object store
/// resolves prefixes against the filesystem root, so relative paths must be
/// canonicalized before use).
fn prepare_work_dir(work_dir: &Path) -> Result<PathBuf, String> {
    std::fs::create_dir_all(work_dir)
        .map_err(|e| format!("mkdir {}: {e}", work_dir.display()))?;
    std::fs::canonicalize(work_dir)
        .map_err(|e| format!("canonicalize {}: {e}", work_dir.display()))
}

/// Remove any stale index and vortex data files from `work_dir`.
fn clean_work_dir(work_dir: &Path) -> Result<(), String> {
    let index_dir = work_dir.join("_vector_index");
    if index_dir.exists() {
        std::fs::remove_dir_all(&index_dir).map_err(|e| format!("clean: {e}"))?;
    }
    for file in find_data_files(work_dir) {
        let _ = std::fs::remove_file(file);
    }
    Ok(())
}

/// Recall recorded at `round`, or the most recent checkpoint before it.
fn recall_at_round(
    series: &[(usize, f32, f32, Option<f64>)],
    round: Option<usize>,
) -> Value {
    let Some(round) = round else {
        return Value::Null;
    };
    series
        .iter()
        .take_while(|(r, _, _, _)| *r <= round)
        .filter_map(|(_, _, _, recall)| *recall)
        .last()
        .map(|r| json!(r))
        .unwrap_or(Value::Null)
}

async fn run_stream_inner(
    args: &Args,
    dataset: &Dataset,
    trigger_only: bool,
) -> Result<Value, String> {
    let dim = dataset.dim();
    if args.reuse {
        return Err("--reuse is not supported for stream/trigger".to_string());
    }
    let work_dir = prepare_work_dir(&args.work_dir)?;
    clean_work_dir(&work_dir)?;

    // 1. Base data + fresh index through the real auto policy path.
    let ids: Vec<u64> = (0..dataset.base.n as u64).collect();
    let t = Instant::now();
    let mut all_files =
        write_vortex_data(&work_dir, dim, &ids, &dataset.base.data).await?;
    let base_write_ms = t.elapsed().as_secs_f64() * 1000.0;

    let mut base_files: HashMap<String, (Vec<String>, u64)> = HashMap::new();
    base_files.insert("-5".to_string(), (all_files.clone(), dataset.base.n as u64));
    let t = Instant::now();
    auto_build_vector_index(
        &[table_config(args, dim, "auto", args.max_delta_ratio)],
        &[PK_COLUMN.to_string()],
        &HashMap::new(),
        &base_files,
        Some(all_files.as_slice()),
    )
    .await
    .map_err(|e| format!("base index build failed: {e}"))?;
    let base_build_ms = t.elapsed().as_secs_f64() * 1000.0;

    // 2. Update/query samplers over the shared drift definition.
    let (mut sampler, mut query_sampler) = make_samplers(args, dataset);

    let mstore = manifest_store(&all_files);
    let mut vectors = dataset.base.data.clone();
    let mut next_id = dataset.base.n as u64;

    let mut rounds_json: Vec<Value> = Vec::new();
    // (round, max cluster ratio, shard ratio, recall at that round)
    let mut series: Vec<(usize, f32, f32, Option<f64>)> = Vec::new();
    let mut rebuilds = 0usize;
    let mut total_data_ms = 0.0f64;
    let mut total_index_ms = 0.0f64;
    let mut min_recall = f64::INFINITY;
    let mut last_recall: Option<f64> = None;

    for round in 1..=args.rounds {
        // 2a. Write this round's update vectors as a new vortex file.
        let updates = sampler.sample(args.per_round);
        let update_ids: Vec<u64> = (next_id..next_id + args.per_round as u64).collect();
        next_id += args.per_round as u64;
        let t = Instant::now();
        let new_files = write_vortex_data(&work_dir, dim, &update_ids, &updates).await?;
        let data_write_ms = t.elapsed().as_secs_f64() * 1000.0;
        all_files.extend(new_files.iter().cloned());
        vectors.extend_from_slice(&updates);

        // 2b. Map the policy onto the real auto-build configuration and call
        //     `auto_build_vector_index` (the production decision path).
        let (rebuild_mode, ratio) = if trigger_only {
            ("none", args.max_delta_ratio)
        } else {
            match args.policy {
                Policy::None => ("none", args.max_delta_ratio),
                Policy::Auto => ("auto", args.max_delta_ratio),
                Policy::Always => ("auto", FORCE_REBUILD_RATIO),
                Policy::Periodic => {
                    if round % args.period == 0 {
                        ("auto", FORCE_REBUILD_RATIO)
                    } else {
                        ("none", args.max_delta_ratio)
                    }
                }
            }
        };
        let gen_before = collect_round_stats(&mstore).await?.generation;
        let mut round_files: HashMap<String, (Vec<String>, u64)> = HashMap::new();
        round_files.insert("-5".to_string(), (new_files, args.per_round as u64));
        let t = Instant::now();
        auto_build_vector_index(
            &[table_config(args, dim, rebuild_mode, ratio)],
            &[PK_COLUMN.to_string()],
            &HashMap::new(),
            &round_files,
            Some(all_files.as_slice()),
        )
        .await
        .map_err(|e| format!("round {round} index update failed: {e}"))?;
        let index_update_ms = t.elapsed().as_secs_f64() * 1000.0;
        total_data_ms += data_write_ms;
        total_index_ms += index_update_ms;

        let stats = collect_round_stats(&mstore).await?;
        let rebuilt = stats.generation > gen_before;
        if rebuilt {
            rebuilds += 1;
        }

        // 2c. Recall/QPS checkpoint (brute-force GT over the current data).
        let mut search = Value::Null;
        let mut recall_here: Option<f64> = None;
        if round % args.checkpoint_every == 0 || round == args.rounds {
            // By default the checkpoint queries follow the drift (new data is
            // what gets queried); `--static-queries` keeps the dataset queries.
            let queries = if args.query_drift {
                Fvecs {
                    dim,
                    n: dataset.queries.n,
                    data: query_sampler.sample(dataset.queries.n),
                }
            } else {
                dataset.queries.clone()
            };
            let gt = brute_force_gt(&vectors, dim, &queries, args.top_k, args.threads);
            let t = Instant::now();
            let index = IvfRabitqIndex::load_from_v4(&mstore)
                .await
                .map_err(|e| format!("load index: {e}"))?;
            let load_ms = t.elapsed().as_secs_f64() * 1000.0;
            let m = measure_index(&index, &queries, &gt, args.top_k, args.nprobe);
            min_recall = min_recall.min(m.recall_at_k);
            last_recall = Some(m.recall_at_k);
            recall_here = Some(m.recall_at_k);
            search = json!({
                "recall_at_k": m.recall_at_k,
                "qps": m.qps,
                "p50_ms": m.p50_ms,
                "p99_ms": m.p99_ms,
                "load_ms": load_ms,
            });
        }
        series.push((
            round,
            stats.max_cluster_delta_ratio,
            stats.shard_delta_ratio,
            recall_here,
        ));

        println!(
            "round {round:>2}: n_total={:<8} gen={} rebuilt={:<5} \
             idx={:>8.1}ms data={:>7.1}ms cluster_ratio={:.3} shard_ratio={:.3}{}",
            vectors.len() / dim,
            stats.generation,
            rebuilt,
            index_update_ms,
            data_write_ms,
            stats.max_cluster_delta_ratio,
            stats.shard_delta_ratio,
            recall_here
                .map(|r| format!(" recall@{}={r:.4}", args.top_k))
                .unwrap_or_default(),
        );

        rounds_json.push(json!({
            "round": round,
            "n_total": vectors.len() / dim,
            "data_write_ms": data_write_ms,
            "index_update_ms": index_update_ms,
            "rebuilt": rebuilt,
            "stats": stats.to_json(),
            "search": search,
        }));
    }

    let total_vectors = (dataset.base.n + args.rounds * args.per_round) as f64;
    let amortized_vps = total_vectors / ((total_data_ms + total_index_ms) / 1000.0);
    let mut summary = json!({
        "scenario": if trigger_only { "trigger" } else { "stream" },
        "policy": if trigger_only { "none" } else { policy_name(args.policy) },
        "drift": drift_name(args.drift),
        "max_delta_ratio": args.max_delta_ratio,
        "period": args.period,
        "rounds": args.rounds,
        "per_round": args.per_round,
        "dim": dim,
        "n_base": dataset.base.n,
        "n_queries": dataset.queries.n,
        "top_k": args.top_k,
        "nprobe": args.nprobe,
        "base_write_ms": base_write_ms,
        "base_build_ms": base_build_ms,
        "rounds_data": rounds_json,
        "summary": {
            "rebuilds": rebuilds,
            "total_data_write_ms": total_data_ms,
            "total_index_update_ms": total_index_ms,
            "amortized_vectors_per_sec": amortized_vps,
            "min_recall": if min_recall.is_finite() { json!(min_recall) } else { Value::Null },
            "final_recall": last_recall,
            "final_index_size_bytes": index_size_bytes(&work_dir),
            "peak_rss_mb": peak_rss_mb(),
        },
    });

    if trigger_only {
        // Compare when the per-cluster rule and the old shard-level rule
        // would first fire, and the recall at each trigger point.
        let mut analysis = Vec::new();
        for threshold in [0.25f32, 0.5, 1.0, 2.0] {
            let cluster_first = series
                .iter()
                .find(|(_, cluster, _, _)| *cluster > threshold)
                .map(|(round, _, _, _)| *round);
            let shard_first = series
                .iter()
                .find(|(_, _, shard, _)| *shard > threshold)
                .map(|(round, _, _, _)| *round);
            analysis.push(json!({
                "threshold": threshold,
                "cluster_first_round": cluster_first,
                "cluster_recall_at_trigger": recall_at_round(&series, cluster_first),
                "shard_first_round": shard_first,
                "shard_recall_at_trigger": recall_at_round(&series, shard_first),
            }));
        }
        summary["trigger_analysis"] = json!(analysis);
    }

    print_summary(&summary);
    Ok(summary)
}

async fn run_stream(args: &Args, dataset: &Dataset) -> Result<Value, String> {
    run_stream_inner(args, dataset, false).await
}

// ---------------------------------------------------------------------------
// SQL scenario (end-to-end DataFusion: write + index-backed search)
// ---------------------------------------------------------------------------

fn core_args() -> CoreArgs {
    CoreArgs {
        warehouse_prefix: None,
        endpoint: None,
        s3_bucket: None,
        s3_access_key: None,
        s3_secret_key: None,
        s3_virtual_host_style: false,
        worker_threads: 2,
    }
}

/// Record batch matching the SQL table schema (`id BIGINT`, `vec FLOAT[]`).
fn sql_vector_batch(
    ids: &[i64],
    vectors: &[f32],
    dim: usize,
) -> Result<RecordBatch, String> {
    let id_array = Int64Array::from(ids.to_vec());
    let mut builder = ListBuilder::new(Float32Builder::new());
    for vector in vectors.chunks_exact(dim) {
        for value in vector {
            builder.values().append_value(*value);
        }
        builder.append(true);
    }
    let list = builder.finish();
    let schema = Arc::new(Schema::new(vec![
        Field::new(PK_COLUMN, DataType::Int64, false),
        Field::new(
            VEC_COLUMN,
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            false,
        ),
    ]));
    RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(list)])
        .map_err(|e| format!("build sql batch: {e}"))
}

fn query_literal(dataset: &Dataset, i: usize) -> String {
    dataset
        .queries
        .query(i)
        .iter()
        .map(|v| v.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

async fn explain_text(
    ctx: &datafusion::prelude::SessionContext,
    sql: &str,
) -> Result<String, String> {
    let batches = ctx
        .sql(sql)
        .await
        .map_err(|e| format!("explain: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("explain: {e}"))?;
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .map(|t| t.to_string())
        .map_err(|e| format!("format explain: {e}"))
}

/// End-to-end DataFusion SQL benchmark: create a LakeSoul table with the
/// vector index property, write vectors through `INSERT ... SELECT`, then
/// query with `ORDER BY array_distance(...) LIMIT k`.  The query is served by
/// the index (candidate ids) plus an exact re-rank over the candidate rows in
/// DataFusion; this scenario therefore measures the full write + search path.
///
/// Requires a running PostgreSQL metadata service (same default as the
/// integration tests).
async fn run_sql(args: &Args, dataset: &Dataset) -> Result<Value, String> {
    let dim = dataset.dim();
    let work_dir = prepare_work_dir(&args.work_dir)?;
    clean_work_dir(&work_dir)?;
    let table_name = args.table.clone();

    let client = Arc::new(
        lakesoul_datafusion::MetaDataClient::from_env()
            .await
            .map_err(|e| format!("sql scenario requires PostgreSQL metadata: {e}"))?,
    );
    let _ = client.drop_table(&table_name, "default").await;

    let core = core_args();
    let ctx = lakesoul_datafusion::create_lakesoul_session_ctx(client.clone(), &core)
        .map_err(|e| format!("create session: {e}"))?;
    // The LakeSoul catalog only accepts LakeSoul tables, so the INSERT source
    // data lives in a separate in-memory catalog (`src.public.*`).
    let src_catalog = Arc::new(MemoryCatalogProvider::new());
    let src_schema = Arc::new(MemorySchemaProvider::new());
    src_catalog
        .register_schema("public", src_schema.clone())
        .map_err(|e| format!("register schema: {e}"))?;
    ctx.register_catalog("src", src_catalog as Arc<dyn CatalogProvider>);

    // 1. Create the table through SQL DDL with the vector index property.
    let config = VectorIndexTableConfig {
        column: VEC_COLUMN.to_string(),
        dim,
        nlist: args.nlist,
        total_bits: args.total_bits,
        metric: metric_str(args.metric).to_string(),
        rotator_type: "FhtKac".to_string(),
        seed: args.seed,
        use_faster_config: true,
        rebuild_mode: "auto".to_string(),
        max_delta_ratio: args.max_delta_ratio,
    };
    let property = vector_index_columns_to_json(std::slice::from_ref(&config));
    let create_sql = format!(
        "CREATE EXTERNAL TABLE \"lakesoul\".default.{table_name} (\
            id BIGINT NOT NULL PRIMARY KEY, \
            vec FLOAT[] NOT NULL\
         ) STORED AS LAKESOUL LOCATION '{}' \
         OPTIONS ('vector_index_columns' '{property}', 'hashBucketNum' '1', \
                  'file_format' '{}')",
        work_dir.display(),
        args.sql_format
    );
    ctx.sql(&create_sql)
        .await
        .map_err(|e| format!("create table: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("create table: {e}"))?;

    // 2. Write the base data through `INSERT ... SELECT` from an in-memory
    //    table: this goes through the DataFusion planner, the LakeSoul sink,
    //    and the post-commit auto index build.
    let base_ids: Vec<i64> = (0..dataset.base.n as i64).collect();
    let base_batch = sql_vector_batch(&base_ids, &dataset.base.data, dim)?;
    let src = Arc::new(
        MemTable::try_new(base_batch.schema(), vec![vec![base_batch.clone()]])
            .map_err(|e| format!("memtable: {e}"))?,
    );
    src_schema
        .register_table("bench_src".to_string(), src)
        .map_err(|e| format!("register table: {e}"))?;
    let t = Instant::now();
    ctx.sql(&format!(
        "INSERT INTO \"lakesoul\".default.{table_name} \
         SELECT id, vec FROM src.public.bench_src"
    ))
    .await
    .map_err(|e| format!("base insert plan: {e}"))?
    .collect()
    .await
    .map_err(|e| format!("base insert: {e}"))?;
    let base_insert_ms = t.elapsed().as_secs_f64() * 1000.0;
    println!("base insert (SQL): {base_insert_ms:.0}ms");

    // 3. Optional extra rounds of SQL inserts (with the configured drift).
    let mut vectors = dataset.base.data.clone();
    let mut round_insert_ms: Vec<f64> = Vec::new();
    if args.rounds > 0 {
        let (mut sampler, _) = make_samplers(args, dataset);
        let mut next_id = dataset.base.n as i64;
        for round in 1..=args.rounds {
            let updates = sampler.sample(args.per_round);
            let ids: Vec<i64> = (next_id..next_id + args.per_round as i64).collect();
            next_id += args.per_round as i64;
            let batch = sql_vector_batch(&ids, &updates, dim)?;
            let name = format!("bench_src_r{round}");
            let src = Arc::new(
                MemTable::try_new(batch.schema(), vec![vec![batch.clone()]])
                    .map_err(|e| format!("memtable: {e}"))?,
            );
            src_schema
                .register_table(name.clone(), src)
                .map_err(|e| format!("register table: {e}"))?;
            let t = Instant::now();
            ctx.sql(&format!(
                "INSERT INTO \"lakesoul\".default.{table_name} \
                 SELECT id, vec FROM src.public.{name}"
            ))
            .await
            .map_err(|e| format!("round {round} insert plan: {e}"))?
            .collect()
            .await
            .map_err(|e| format!("round {round} insert: {e}"))?;
            let ms = t.elapsed().as_secs_f64() * 1000.0;
            println!("round {round} insert (SQL): {ms:.0}ms");
            round_insert_ms.push(ms);
            vectors.extend_from_slice(&updates);
        }
    }

    // 4. Ground truth over the current dataset.
    let gt = brute_force_gt(&vectors, dim, &dataset.queries, args.top_k, args.threads);

    // 5. Search session with the nprobe extension.
    let session_config = lakesoul_datafusion::create_lakesoul_session_config()
        .map_err(|e| format!("session config: {e}"))?
        .with_extension(Arc::new(LakeSoulVectorSearchOptions {
            nprobe: args.nprobe,
        }));
    let search_ctx = lakesoul_datafusion::create_lakesoul_session_ctx_with_config(
        client.clone(),
        &core,
        session_config,
    )
    .map_err(|e| format!("search session: {e}"))?;

    // 6. EXPLAIN must show the index-candidate + exact-rerank exec node.
    let explain_sql = format!(
        "EXPLAIN VERBOSE select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{}]) limit {}",
        query_literal(dataset, 0),
        args.top_k
    );
    let explain = explain_text(&search_ctx, &explain_sql).await?;
    let uses_vector_index_exec = explain.contains("LakeSoulVectorSearchExec");
    // Per-operator timings for the same query (diagnostic).
    let analyze_sql = format!(
        "EXPLAIN ANALYZE select id from \"lakesoul\".default.{table_name} \
         order by array_distance(vec, ARRAY[{}]) limit {}",
        query_literal(dataset, 0),
        args.top_k
    );
    let explain_analyze = explain_text(&search_ctx, &analyze_sql).await?;

    // 7. End-to-end SQL latency and recall, one query at a time.
    let nq = dataset.queries.n;
    let mut latencies = Vec::with_capacity(nq);
    let mut recall_sum = 0.0;
    for qi in 0..nq {
        let sql = format!(
            "select id from \"lakesoul\".default.{table_name} \
             order by array_distance(vec, ARRAY[{}]) limit {}",
            query_literal(dataset, qi),
            args.top_k
        );
        let t = Instant::now();
        let batches = search_ctx
            .sql(&sql)
            .await
            .map_err(|e| format!("query: {e}"))?
            .collect()
            .await
            .map_err(|e| format!("query: {e}"))?;
        latencies.push(t.elapsed().as_secs_f64() * 1000.0);
        let mut predicted = Vec::new();
        for batch in &batches {
            if let Some(arr) = batch.column(0).as_any().downcast_ref::<Int64Array>() {
                predicted.extend(arr.values().iter().map(|&v| v as u64));
            }
        }
        recall_sum += recall_at_k(&predicted, gt.row(qi), args.top_k);
    }
    latencies.sort_by(|a, b| a.total_cmp(b));
    let percentile = |p: f64| -> f64 {
        if latencies.is_empty() {
            return 0.0;
        }
        latencies[((latencies.len() as f64 - 1.0) * p).round() as usize]
    };
    let total_ms: f64 = latencies.iter().sum();
    let qps = if total_ms > 0.0 {
        nq as f64 / (total_ms / 1000.0)
    } else {
        f64::INFINITY
    };

    let index_state = collect_all_shard_stats(&find_data_files(&work_dir))
        .await
        .unwrap_or(Value::Null);

    let summary = json!({
        "scenario": "sql",
        "table": table_name,
        "dim": dim,
        "n_base": dataset.base.n,
        "n_rounds": args.rounds,
        "n_queries": nq,
        "top_k": args.top_k,
        "nprobe": args.nprobe,
        "base_insert_ms": base_insert_ms,
        "round_insert_ms": round_insert_ms,
        "recall_at_k": recall_sum / nq.max(1) as f64,
        "qps": qps,
        "mean_ms": total_ms / nq.max(1) as f64,
        "p50_ms": percentile(0.50),
        "p99_ms": percentile(0.99),
        "uses_vector_index_exec": uses_vector_index_exec,
        "index_size_bytes": index_size_bytes(&work_dir),
        "index_state": index_state,
        "peak_rss_mb": peak_rss_mb(),
        "explain": explain,
        "explain_analyze": explain_analyze,
    });
    print_summary(&summary);
    Ok(summary)
}

async fn run_trigger(args: &Args, dataset: &Dataset) -> Result<Value, String> {
    run_stream_inner(args, dataset, true).await
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
            Scenario::Sql => run_sql(&args, &dataset).await,
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
