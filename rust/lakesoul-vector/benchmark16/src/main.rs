// SPDX-License-Identifier: Apache-2.0
//! Native, two-pass ANN-Benchmarks baseline, derived from /tmp/vbench-common.
//! No full training matrix, persisted index, Python, or HDF5 library is needed.

mod hdf5;

use futures::stream::Stream;
use hdf5::Hdf5File;
use lakesoul_vector::{
    IdAndVecBatch, IndexKey, IvfRabitqBuilder, IvfRabitqIndex, Metric, RotatorType,
    SearchParams, SearchResult,
};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::{Command, ExitCode};
use std::sync::{Condvar, Mutex, mpsc};
use std::task::{Context, Poll};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const BATCH_ROWS: usize = 10_000;
const NLIST: usize = 1024;
const SEED: u64 = 42;
const TOP_K: usize = 100;
const QUERY_COUNT: usize = 1000;
const WARMUP_COUNT: usize = 10;
const MIB: f64 = 1024.0 * 1024.0;
const COMPILED_MANIFEST: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));

#[derive(Debug)]
struct Config {
    h5: PathBuf,
    out: PathBuf,
    dataset: String,
    version: String,
    source_label: String,
    angular: bool,
    bits: usize,
    workers: usize,
    nprobes: Vec<usize>,
}

const HELP: &str = "Native lakesoul-vector baseline (one build per process)
  --h5 PATH            ANN-Benchmarks HDF5 file (required)
  --out PATH           New JSON output file (required; refuses overwrite)
  --version LABEL      Library/comparison label, e.g. new or old13cd5ffd (required)
  --library-source TEXT  Optional operator source label (default: --version)
  --dataset NAME       Default: HDF5 filename stem
  --metric l2|angular  Inferred for *-euclidean/*-angular dataset names; ip aliases angular
  --bits 1|3|7         Default: 7; run each width in a separate process
  --workers 16|1       Default: exactly 16 scoped OS query threads; 1 is a control
  --nprobes N,N,...    Default: 16,128,512; overrides are labelled as controls
Fixed: all train rows; batch=10000; nlist=1024; seed=42; faster_config=false;
       FhtKacRotator; inner parallelism=1; top_k=100; first 1000 queries; warmup=10.
Use RAYON_NUM_THREADS=16 for report-compatible build concurrency.
";

type BenchResult<T> = Result<T, String>;

impl Config {
    fn parse() -> BenchResult<Option<Self>> {
        let mut args = std::env::args().skip(1);
        let mut h5 = None;
        let mut out = None;
        let mut dataset = None;
        let mut version = None;
        let mut source_label = None;
        let mut metric = None;
        let mut bits = 7;
        let mut workers = 16;
        let mut nprobes = vec![16, 128, 512];
        let mut seen = HashSet::new();
        while let Some(name) = args.next() {
            if name == "--help" || name == "-h" {
                return Ok(None);
            }
            if !seen.insert(name.clone()) {
                return Err(format!("duplicate option {name}"));
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {name}"))?;
            match name.as_str() {
                "--h5" => h5 = Some(PathBuf::from(value)),
                "--out" => out = Some(PathBuf::from(value)),
                "--dataset" => dataset = Some(value),
                "--version" => version = Some(value),
                "--library-source" => source_label = Some(value),
                "--metric" => metric = Some(value.to_ascii_lowercase()),
                "--bits" => bits = value.parse().map_err(|_| "invalid --bits")?,
                "--workers" => {
                    workers = value.parse().map_err(|_| "invalid --workers")?
                }
                "--nprobes" => {
                    nprobes = value
                        .split(',')
                        .map(|n| n.parse().map_err(|_| format!("invalid nprobe {n:?}")))
                        .collect::<BenchResult<_>>()?;
                }
                _ => return Err(format!("unknown option {name}")),
            }
        }
        let h5 = h5.ok_or("--h5 is required")?;
        let out = out.ok_or("--out is required")?;
        let version = version
            .filter(|s| !s.trim().is_empty())
            .ok_or("--version is required")?;
        let dataset = dataset.unwrap_or_else(|| {
            h5.file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned()
        });
        let angular = match metric.as_deref() {
            Some("angular" | "ip") => true,
            Some("l2" | "euclidean") => false,
            Some(other) => return Err(format!("unsupported metric {other:?}")),
            None if dataset.to_ascii_lowercase().contains("angular") => true,
            None if dataset.to_ascii_lowercase().contains("euclidean") => false,
            None => {
                return Err(
                    "cannot infer metric; use --metric l2 or --metric angular".into()
                );
            }
        };
        if dataset.to_ascii_lowercase().contains("angular") && !angular {
            return Err(
                "angular datasets require normalized InnerProduct (--metric angular)"
                    .into(),
            );
        }
        if ![1, 3, 7].contains(&bits) {
            return Err("--bits must be 1, 3, or 7".into());
        }
        if ![1, 16].contains(&workers) {
            return Err("--workers must be 16 (baseline) or 1 (control)".into());
        }
        if nprobes.is_empty() || nprobes.iter().any(|n| *n == 0 || *n > NLIST) {
            return Err(format!("nprobes must be between 1 and {NLIST}"));
        }
        let distinct: HashSet<_> = nprobes.iter().collect();
        if distinct.len() != nprobes.len() {
            return Err("duplicate nprobes are not allowed".into());
        }
        Ok(Some(Self {
            h5,
            out,
            dataset,
            source_label: source_label.unwrap_or_else(|| version.clone()),
            version,
            angular,
            bits,
            workers,
            nprobes,
        }))
    }
}

fn normalize_flat(flat: &mut [f32], dim: usize) {
    // Same f32 norm and zero-vector policy as the report's starting harness.
    for row in flat.chunks_exact_mut(dim) {
        let norm = row.iter().map(|v| v * v).sum::<f32>().sqrt();
        if norm > 0.0 {
            let inv = norm.recip();
            for value in row {
                *value *= inv;
            }
        }
    }
}

fn read_train_batch(
    file: &mut Hdf5File,
    start: usize,
    count: usize,
    dim: usize,
    angular: bool,
) -> BenchResult<IdAndVecBatch> {
    let (mut vectors, cols) = file
        .read_f32_flat_range("train", start, count)
        .map_err(|e| format!("read train rows {start}..{}: {e}", start + count))?;
    if cols != dim || vectors.len() != count * dim {
        return Err(format!("unexpected train shape at row {start}"));
    }
    if angular {
        normalize_flat(&mut vectors, dim);
    }
    let ids = (start..start + count)
        .map(|id| IndexKey::new((id as u64).to_le_bytes()))
        .collect();
    Ok(IdAndVecBatch { ids, vectors })
}

/// The builder owns at most one raw training batch through this stream. Its
/// bounded reservoir and internal build buffers belong to the library itself.
struct BatchStream {
    file: Hdf5File,
    pos: usize,
    total: usize,
    dim: usize,
    angular: bool,
}

impl BatchStream {
    fn new(path: &Path, total: usize, dim: usize, angular: bool) -> Self {
        Self {
            file: Hdf5File::open(path).unwrap_or_else(|e| {
                panic!("open second-pass HDF5 {}: {e}", path.display())
            }),
            pos: 0,
            total,
            dim,
            angular,
        }
    }
}

impl Stream for BatchStream {
    type Item = IdAndVecBatch;

    fn poll_next(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.pos == this.total {
            return Poll::Ready(None);
        }
        let count = BATCH_ROWS.min(this.total - this.pos);
        // Stream<Item=IdAndVecBatch> cannot carry an I/O error. Panic rather
        // than silently ending the stream and returning an incomplete index.
        // main catches it, writes a failed JSON record, and exits unsuccessfully.
        let batch =
            read_train_batch(&mut this.file, this.pos, count, this.dim, this.angular)
                .unwrap_or_else(|e| panic!("{e}"));
        this.pos += count;
        if this.pos % 1_000_000 == 0 || this.pos == this.total {
            eprintln!("pass2 rows={}/{}", this.pos, this.total);
        }
        Poll::Ready(Some(batch))
    }
}

fn decode_id(key: &IndexKey) -> BenchResult<u64> {
    let bytes: [u8; 8] = key.as_bytes().try_into().map_err(|_| {
        format!(
            "expected an 8-byte benchmark ID, got {} bytes",
            key.as_bytes().len()
        )
    })?;
    Ok(u64::from_le_bytes(bytes))
}

fn proc_memory() -> BenchResult<Value> {
    let text = std::fs::read_to_string("/proc/self/status")
        .map_err(|e| format!("read /proc/self/status: {e}"))?;
    let field = |name: &str| -> BenchResult<u64> {
        let rest = text
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .ok_or_else(|| format!("missing {name} in /proc/self/status"))?;
        let mut fields = rest.split_whitespace();
        let value = fields
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| format!("invalid {name} in /proc/self/status"))?;
        if fields.next() != Some("kB") {
            return Err(format!("unexpected units for {name}"));
        }
        Ok(value)
    };
    let hwm = field("VmHWM:")?;
    let rss = field("VmRSS:")?;
    Ok(
        json!({"available": true, "vm_hwm_kib": hwm, "vm_rss_kib": rss,
        "vm_hwm_mib": hwm as f64 / 1024.0, "vm_rss_mib": rss as f64 / 1024.0}),
    )
}

fn memory_snapshot() -> Value {
    proc_memory().unwrap_or_else(|e| json!({"available": false, "error": e}))
}

fn reset_peak_rss() -> Value {
    let before = memory_snapshot();
    match std::fs::write("/proc/self/clear_refs", "5") {
        Ok(()) => json!({
            "reset_success": true, "method": "write 5 to /proc/self/clear_refs",
            "success_definition": "kernel accepted the reset write; snapshots show the observed HWM/RSS",
            "before": before, "after": memory_snapshot(),
        }),
        Err(error) => {
            eprintln!(
                "WARNING: VmHWM reset FAILED: {error}; search peak is not isolated from earlier stages"
            );
            json!({"reset_success": false, "method": "write 5 to /proc/self/clear_refs",
                "error": error.to_string(), "before": before, "after": memory_snapshot()})
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum GateState {
    Waiting,
    Start,
    Cancel,
}

struct Observation {
    query_index: usize,
    search_ns: u64,
    results: Vec<SearchResult>,
}

struct WorkerBatch {
    observations: Vec<Observation>,
    last_search_end: Instant,
}

struct SearchRun {
    batches: Vec<WorkerBatch>,
    report_wall_seconds: f64,
    post_gate_wall_seconds: f64,
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).into()
    } else {
        "non-string panic payload".into()
    }
}

/// One execution yields both wall-clock denominators. Workers keep results
/// private; decoding, deduplication, and recall run after all search timing.
fn measure_queries(
    index: &IvfRabitqIndex,
    queries: &[Vec<f32>],
    params: SearchParams,
    workers: usize,
) -> BenchResult<SearchRun> {
    let gate = (Mutex::new(GateState::Waiting), Condvar::new());
    let (ready_tx, ready_rx) = mpsc::channel();
    let report_start = Instant::now();
    let (batches, gate_start) = std::thread::scope(|scope| -> BenchResult<_> {
        let mut handles = Vec::with_capacity(workers);
        let mut failure = None;
        for worker in 0..workers {
            let gate = &gate;
            let ready = ready_tx.clone();
            let handle = std::thread::Builder::new()
                .name(format!("bench16-{worker}"))
                .spawn_scoped(scope, move || -> BenchResult<WorkerBatch> {
                    let mut observations =
                        Vec::with_capacity(queries.len().div_ceil(workers));
                    // No fallible search occurs before readiness/start. Dropping
                    // every sender also lets the coordinator detect a startup panic.
                    ready
                        .send(())
                        .map_err(|e| format!("worker readiness: {e}"))?;
                    drop(ready);
                    let mut state =
                        gate.0.lock().map_err(|e| format!("gate lock: {e}"))?;
                    while *state == GateState::Waiting {
                        state =
                            gate.1.wait(state).map_err(|e| format!("gate wait: {e}"))?;
                    }
                    if *state == GateState::Cancel {
                        return Err("worker cancelled after startup failure".into());
                    }
                    drop(state);
                    let mut last_search_end = Instant::now();
                    for query_index in (worker..queries.len()).step_by(workers) {
                        let start = Instant::now();
                        let results = index
                            .search(&queries[query_index], params)
                            .map_err(|e| format!("query {query_index}: {e}"))?;
                        last_search_end = Instant::now();
                        let search_ns = u64::try_from(
                            last_search_end.duration_since(start).as_nanos(),
                        )
                        .map_err(|_| "per-query latency overflow".to_string())?;
                        observations.push(Observation {
                            query_index,
                            search_ns,
                            results,
                        });
                    }
                    Ok(WorkerBatch {
                        observations,
                        last_search_end,
                    })
                });
            match handle {
                Ok(handle) => handles.push((worker, handle)),
                Err(error) => {
                    failure = Some(format!("spawn query worker {worker}: {error}"));
                    break;
                }
            }
        }
        drop(ready_tx);
        if failure.is_none() {
            for _ in 0..workers {
                if let Err(error) = ready_rx.recv() {
                    failure = Some(format!(
                        "query worker failed before the start gate: {error}"
                    ));
                    break;
                }
            }
        }
        let gate_start = Instant::now();
        // Release successful spawns even on startup failure, so joins cannot
        // leave workers stuck at a barrier. Never return before joining all.
        {
            let mut state = gate.0.lock().unwrap_or_else(|poison| poison.into_inner());
            *state = if failure.is_none() {
                GateState::Start
            } else {
                GateState::Cancel
            };
            gate.1.notify_all();
        }
        let mut batches = Vec::with_capacity(handles.len());
        for (worker, handle) in handles {
            match handle.join() {
                Ok(Ok(batch)) => batches.push(batch),
                Ok(Err(error)) => {
                    failure.get_or_insert(format!("worker {worker}: {error}"));
                }
                Err(payload) => {
                    failure.get_or_insert(format!(
                        "worker {worker} panicked: {}",
                        panic_message(payload)
                    ));
                }
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok((batches, gate_start))
    })?;
    // Includes thread creation, gate coordination, timing/result collection,
    // and joins, as the original MT wall-clock boundary did. No recall here.
    let report_wall_seconds = report_start.elapsed().as_secs_f64();
    let last_end = batches
        .iter()
        .map(|b| b.last_search_end)
        .max()
        .ok_or("no workers completed")?;
    let post_gate_wall_seconds = last_end.duration_since(gate_start).as_secs_f64();
    Ok(SearchRun {
        batches,
        report_wall_seconds,
        post_gate_wall_seconds,
    })
}

/// Nearest-rank percentile (ceil(p*N), one-based); values are actual query times.
fn percentile(sorted: &[u64], percent: usize) -> Option<u64> {
    if sorted.is_empty() || !(1..=100).contains(&percent) {
        return None;
    }
    Some(sorted[(sorted.len() * percent).div_ceil(100) - 1])
}

fn summarize(
    run: SearchRun,
    ground_truth: &[HashSet<u64>],
    train_rows: usize,
) -> BenchResult<Value> {
    let mut observations: Vec<_> = run
        .batches
        .into_iter()
        .flat_map(|batch| batch.observations)
        .collect();
    observations.sort_unstable_by_key(|observation| observation.query_index);
    if observations.len() != QUERY_COUNT
        || observations
            .iter()
            .enumerate()
            .any(|(i, observation)| i != observation.query_index)
    {
        return Err(
            "query coverage is not exactly the first 1000 queries once each".into(),
        );
    }
    let mut timings = Vec::with_capacity(QUERY_COUNT);
    let mut per_query = Vec::with_capacity(QUERY_COUNT);
    let mut total_hits = 0;
    let mut duplicate_returned_ids = 0;
    for observation in observations {
        let mut ids = HashSet::with_capacity(observation.results.len());
        for result in &observation.results {
            let id = decode_id(&result.id)?;
            if id >= train_rows as u64 {
                return Err(format!(
                    "query {} returned out-of-range ID {id}",
                    observation.query_index
                ));
            }
            ids.insert(id);
        }
        if observation.results.len() > TOP_K {
            return Err(format!(
                "query {} returned more than top_k results",
                observation.query_index
            ));
        }
        let duplicates = observation.results.len() - ids.len();
        let hits = ids
            .intersection(&ground_truth[observation.query_index])
            .count();
        total_hits += hits;
        duplicate_returned_ids += duplicates;
        timings.push(observation.search_ns);
        per_query.push(json!({"query_index": observation.query_index, "search_ns": observation.search_ns,
            "returned_count": observation.results.len(), "unique_returned_count": ids.len(),
            "duplicate_returned_ids": duplicates, "hits": hits, "recall_at_100": hits as f64 / TOP_K as f64}));
    }
    timings.sort_unstable();
    Ok(json!({
        "query_count": QUERY_COUNT,
        "report_wall_seconds": run.report_wall_seconds,
        "report_qps": QUERY_COUNT as f64 / run.report_wall_seconds,
        "post_start_gate_wall_seconds": run.post_gate_wall_seconds,
        "post_start_gate_qps": QUERY_COUNT as f64 / run.post_gate_wall_seconds,
        "latency_ns": {"definition": "each index.search call only; excludes start gate, ID decoding and recall",
            "percentile_method": "nearest rank ceil(p*N)",
            "p50": percentile(&timings, 50), "p95": percentile(&timings, 95),
            "p99": percentile(&timings, 99), "min": timings.first(), "max": timings.last(),
            "mean": timings.iter().map(|n| *n as f64).sum::<f64>() / QUERY_COUNT as f64},
        "recall_at_100": total_hits as f64 / (QUERY_COUNT * TOP_K) as f64,
        "unique_hits": total_hits, "ground_truth_entries": QUERY_COUNT * TOP_K,
        "duplicate_returned_ids": duplicate_returned_ids, "per_query": per_query,
    }))
}

fn command_text(directory: &Path, program: &str, args: &[&str]) -> Value {
    match Command::new(program)
        .args(args)
        .current_dir(directory)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .output()
    {
        Ok(output) if output.status.success() => json!({"success": true,
            "stdout": String::from_utf8_lossy(&output.stdout).trim_end()}),
        Ok(output) => json!({"success": false, "exit_code": output.status.code(),
            "stderr": String::from_utf8_lossy(&output.stderr).trim_end()}),
        Err(error) => json!({"success": false, "error": error.to_string()}),
    }
}

fn library_source_info(config: &Config) -> BenchResult<Value> {
    // Both the checked-in manifest and Lead's generated manifest use the same
    // inline path dependency. Resolve relative to the *compiled* manifest,
    // which need not be this source directory when comparing old/new trees.
    let line = COMPILED_MANIFEST
        .lines()
        .map(str::trim)
        .find(|line| {
            line.starts_with("lakesoul-vector ") || line.starts_with("lakesoul-vector=")
        })
        .ok_or(
            "keep lakesoul-vector as an inline path dependency in the generated manifest",
        )?;
    let path_value = line
        .split_once("path")
        .and_then(|(_, suffix)| suffix.split_once('='))
        .map(|(_, value)| value.trim_start())
        .ok_or("missing lakesoul-vector dependency path")?;
    let quote = path_value.chars().next().ok_or("empty library path")?;
    if quote != '\'' && quote != '"' {
        return Err("library dependency path must be quoted".into());
    }
    let path_text = path_value[1..]
        .split(quote)
        .next()
        .ok_or("invalid library dependency path")?;
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(path_text)
        .canonicalize()
        .map_err(|e| format!("resolve library dependency path {path_text:?}: {e}"))?;
    Ok(json!({
        "operator_label": config.source_label, "resolved_dependency_path": source,
        "compiled_manifest_directory": env!("CARGO_MANIFEST_DIR"),
        "compiled_manifest": COMPILED_MANIFEST,
        "git_head_at_run": command_text(&source, "git", &["rev-parse", "HEAD"]),
        "git_status_at_run": command_text(&source, "git", &["status", "--porcelain", "--untracked-files=normal",
            "--", ".", ":(exclude)benchmark16", ":(top)Cargo.toml"]),
        "provenance_note": "Manifest embedded at compile time; git state observed at process startup. Build and run against unchanged source.",
    }))
}

fn write_report(file: &mut File, report: &Value) -> BenchResult<()> {
    file.seek(SeekFrom::Start(0))
        .map_err(|e| format!("seek output: {e}"))?;
    file.set_len(0)
        .map_err(|e| format!("truncate output: {e}"))?;
    serde_json::to_writer_pretty(&mut *file, report)
        .map_err(|e| format!("write JSON: {e}"))?;
    file.write_all(b"\n")
        .and_then(|_| file.flush())
        .map_err(|e| format!("flush output: {e}"))
}

fn execute(config: &Config, report: &mut Value, output: &mut File) -> BenchResult<()> {
    report["library"] = library_source_info(config)?;
    report["environment"] = json!({
        "available_logical_cpus": std::thread::available_parallelism().map(|n| n.get()).ok(),
        "rayon_threads": rayon::current_num_threads(), "tokio_worker_threads": 16,
        "rayon_num_threads_env": std::env::var("RAYON_NUM_THREADS").ok(),
        "rustflags_at_run": std::env::var("RUSTFLAGS").ok(),
        "rustflags_at_compile": option_env!("RUSTFLAGS"),
        "encoded_rustflags_at_compile": option_env!("CARGO_ENCODED_RUSTFLAGS"),
        "rustc_at_run": command_text(Path::new("."), "rustc", &["--version"]),
        "uname": command_text(Path::new("."), "uname", &["-a"]),
        "build_note": "Use the same release compiler, dependency resolutions and -C target-cpu=native for both versions; runtime RUSTFLAGS do not prove build flags.",
    });
    write_report(output, report)?;

    let load_start = Instant::now();
    let mut file = Hdf5File::open(&config.h5).map_err(|e| format!("open HDF5: {e}"))?;
    let (train_rows, dim) = file
        .f32_shape("train")
        .map_err(|e| format!("train shape: {e}"))?;
    let (query_rows, query_dim) = file
        .f32_shape("test")
        .map_err(|e| format!("test shape: {e}"))?;
    let (neighbor_rows, neighbor_cols) = file
        .i32_shape("neighbors")
        .map_err(|e| format!("neighbors shape: {e}"))?;
    if train_rows < NLIST
        || dim == 0
        || query_dim != dim
        || query_rows < QUERY_COUNT
        || neighbor_rows < QUERY_COUNT
        || neighbor_cols < TOP_K
    {
        return Err(format!(
            "invalid dataset shapes: train={train_rows}x{dim}, test={query_rows}x{query_dim}, neighbors={neighbor_rows}x{neighbor_cols}"
        ));
    }
    let mut queries = file
        .read_f32_rows("test", QUERY_COUNT)
        .map_err(|e| format!("load test: {e}"))?;
    let neighbors = file
        .read_i32_rows("neighbors", QUERY_COUNT)
        .map_err(|e| format!("load neighbors: {e}"))?;
    drop(file);
    if config.angular {
        for query in &mut queries {
            normalize_flat(query, dim);
        }
    }
    let mut ground_truth = Vec::with_capacity(QUERY_COUNT);
    for (query, row) in neighbors.iter().enumerate() {
        let mut ids = HashSet::with_capacity(TOP_K);
        for &id in row.iter().take(TOP_K) {
            if id < 0 || id as usize >= train_rows || !ids.insert(id as u64) {
                return Err(format!(
                    "official neighbors query {query} has invalid or duplicate top-100 ID {id}"
                ));
            }
        }
        ground_truth.push(ids);
    }
    drop(neighbors);
    report["data"] = json!({"h5_path": config.h5.canonicalize().map_err(|e| e.to_string())?,
        "h5_bytes": std::fs::metadata(&config.h5).map_err(|e| e.to_string())?.len(),
        "train_rows": train_rows, "dimension": dim, "test_rows": query_rows,
        "neighbors_rows": neighbor_rows, "neighbors_columns": neighbor_cols,
        "source_query_and_ground_truth_load_seconds": load_start.elapsed().as_secs_f64()});
    eprintln!(
        "loaded {} rows={train_rows} dim={dim} bits={} workers={}",
        config.dataset, config.bits, config.workers
    );
    write_report(output, report)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(16)
        .build()
        .map_err(|e| format!("create Tokio runtime: {e}"))?;
    let total_build_start = Instant::now();
    let mut builder = IvfRabitqBuilder::new(
        dim,
        NLIST,
        config.bits,
        if config.angular {
            Metric::InnerProduct
        } else {
            Metric::L2
        },
        RotatorType::FhtKacRotator,
        SEED,
        false,
        0,
    );
    let builder_setup_seconds = total_build_start.elapsed().as_secs_f64();
    let first_pass_start = Instant::now();
    let mut reader =
        Hdf5File::open(&config.h5).map_err(|e| format!("open first-pass HDF5: {e}"))?;
    let mut pos = 0;
    while pos < train_rows {
        let count = BATCH_ROWS.min(train_rows - pos);
        builder
            .insert_batch(read_train_batch(
                &mut reader,
                pos,
                count,
                dim,
                config.angular,
            )?)
            .map_err(|e| format!("reservoir insert at row {pos}: {e}"))?;
        pos += count;
        if pos % 1_000_000 == 0 || pos == train_rows {
            eprintln!("pass1 rows={pos}/{train_rows}");
        }
    }
    drop(reader);
    let first_pass_seconds = first_pass_start.elapsed().as_secs_f64();
    let second_pass_start = Instant::now();
    let index = runtime
        .block_on(
            builder
                .build(|| BatchStream::new(&config.h5, train_rows, dim, config.angular)),
        )
        .map_err(|e| format!("build index: {e}"))?;
    let second_pass_seconds = second_pass_start.elapsed().as_secs_f64();
    let total_build_seconds = total_build_start.elapsed().as_secs_f64();
    let build_peak = memory_snapshot(); // before warmup or any measured search
    if index.len() != train_rows {
        return Err(format!(
            "built {} rows; expected all {train_rows}",
            index.len()
        ));
    }
    report["build"] = json!({"builder_setup_seconds": builder_setup_seconds,
        "first_pass_seconds": first_pass_seconds, "second_pass_seconds": second_pass_seconds,
        "report_build_seconds": second_pass_seconds, "total_seconds": total_build_seconds,
        "pass_sum_seconds": first_pass_seconds + second_pass_seconds,
        "index_vectors": index.len(), "padded_dimension": index.padded_dim(),
        "index_memory_bytes": index.memory_usage(), "index_memory_mib": index.memory_usage() as f64 / MIB,
        "peak_before_any_search": build_peak,
        "second_pass_definition": "builder.build wall time including k-means, HDF5 open/read, normalization, rotation, assignment, quantization and in-memory flush"});
    eprintln!(
        "built first_pass={first_pass_seconds:.3}s second_pass={second_pass_seconds:.3}s total={total_build_seconds:.3}s"
    );
    write_report(output, report)?;

    for &nprobe in &config.nprobes {
        let mut params = SearchParams::new(TOP_K, nprobe);
        params.parallelism = 1; // No nested per-probe Rayon query concurrency.
        let warmup_start = Instant::now();
        for query in queries.iter().take(WARMUP_COUNT) {
            let _ = index
                .search(query, params)
                .map_err(|e| format!("warmup nprobe={nprobe}: {e}"))?;
        }
        let warmup_seconds = warmup_start.elapsed().as_secs_f64();
        let reset = reset_peak_rss();
        let run = measure_queries(&index, &queries, params, config.workers)?;
        // Snapshot immediately after timing, before recall/statistics allocate.
        let search_peak = memory_snapshot();
        let mut result = summarize(run, &ground_truth, train_rows)?;
        result["nprobe"] = json!(nprobe);
        result["workers"] = json!(config.workers);
        result["inner_parallelism"] = json!(params.parallelism);
        result["warmup_queries"] = json!(WARMUP_COUNT);
        result["warmup_seconds"] = json!(warmup_seconds);
        result["search_peak_isolated"] =
            json!(reset["reset_success"] == true && search_peak["available"] == true);
        result["peak_reset"] = reset;
        result["search_peak_before_recall"] = search_peak;
        eprintln!(
            "search nprobe={nprobe} workers={} wall={:.6}s qps={:.1} gate_qps={:.1} recall={:.6} p95={:.3}ms reset={}",
            config.workers,
            result["report_wall_seconds"].as_f64().unwrap(),
            result["report_qps"].as_f64().unwrap(),
            result["post_start_gate_qps"].as_f64().unwrap(),
            result["recall_at_100"].as_f64().unwrap(),
            result["latency_ns"]["p95"].as_u64().unwrap() as f64 / 1e6,
            result["peak_reset"]["reset_success"]
        );
        report["searches"].as_array_mut().unwrap().push(result);
        write_report(output, report)?;
    }
    Ok(())
}

fn main() -> ExitCode {
    let config = match Config::parse() {
        Ok(Some(config)) => config,
        Ok(None) => {
            print!("{HELP}");
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("{error}\n{HELP}");
            return ExitCode::FAILURE;
        }
    };
    let mut output = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&config.out)
    {
        Ok(file) => file,
        Err(error) => {
            eprintln!("create new output {}: {error}", config.out.display());
            return ExitCode::FAILURE;
        }
    };
    let mut report = json!({
        "schema_version": 1, "status": "running", "benchmark": "native_hdf5_ivf_rabitq_baseline",
        "version": config.version, "dataset": config.dataset,
        "started_unix_seconds": SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
        "argv": std::env::args().collect::<Vec<_>>(),
        "parameters": {"bits": config.bits, "nlist": NLIST, "seed": SEED, "faster_config": false,
            "rotator": "FhtKacRotator", "key_encoding": "zero-based train row as 8-byte little-endian u64",
            "all_train_rows": true, "batch_rows": BATCH_ROWS, "query_offset": 0,
            "query_count": QUERY_COUNT, "top_k": TOP_K, "warmup_queries": WARMUP_COUNT,
            "nprobes": config.nprobes, "query_workers": config.workers, "inner_parallelism": 1,
            "metric": if config.angular { "InnerProduct" } else { "L2" },
            "normalize_train_and_queries": config.angular},
        "report_default_query_parameters": config.workers == 16 && config.nprobes == [16, 128, 512],
        "methodology": {
            "query_partition": "worker t handles t,t+workers,...; exactly one search per query",
            "report_wall": "one run; before scoped thread creation through all joins, including start gate; excludes recall",
            "post_start_gate_wall": "same run; just before gate release through last completed search; excludes creation/joins",
            "latency": "actual per-query search calls; no gate waiting, ID decoding or recall",
            "recall": "unique returned IDs intersect official neighbors[query][0..100]; after all searches and RSS snapshot",
            "rss": "build VmHWM before warmup; search reset after 10 warmups, then one worker-count run, snapshot before recall",
            "historical_difference": "original /tmp/vbench-common ST wall included recall; its MT wall did not. This harness excludes recall for both worker counts, retains results, and adds timing instrumentation/start gate. Historical search RSS covered ST+MT; this measures only the selected worker count.",
            "repeat_policy": "one dataset/bits/version per process; externally run two interleaved rounds old-new then new-old; min build, max QPS and max RSS as in report",
            "historical_old": "report used 4989d850; old13cd5ffd is a separate upsert control and must retain its own label"},
        "searches": [],
    });
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        execute(&config, &mut report, &mut output)
    }));
    let error = match outcome {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(payload) => Some(format!("benchmark panicked: {}", panic_message(payload))),
    };
    report["status"] = json!(if error.is_some() {
        "failed"
    } else {
        "complete"
    });
    if let Some(error) = &error {
        report["error"] = json!(error);
        eprintln!("FAILED: {error}");
    }
    if let Err(error) = write_report(&mut output, &report) {
        eprintln!("FAILED: {error}");
        return ExitCode::FAILURE;
    }
    if error.is_some() {
        ExitCode::FAILURE
    } else {
        eprintln!("JSON {}", config.out.display());
        ExitCode::SUCCESS
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_decoding_preserves_all_64_bits_and_rejects_other_widths() {
        for id in [0, 1, 255, 256, 1 << 40, u64::MAX] {
            assert_eq!(decode_id(&IndexKey::new(id.to_le_bytes())).unwrap(), id);
        }
        for bytes in [&[][..], &[1][..], &[0; 7][..], &[0; 9][..]] {
            assert!(decode_id(&IndexKey::new(bytes)).is_err());
        }
        assert_eq!(
            decode_id(&IndexKey::new([1, 2, 0, 0, 0, 0, 0, 0])).unwrap(),
            513
        );
    }

    #[test]
    fn nearest_rank_percentiles_handle_small_samples_and_ties() {
        assert_eq!(percentile(&[], 50), None);
        assert_eq!(percentile(&[42], 99), Some(42));
        assert_eq!(percentile(&[1, 2, 3, 4], 50), Some(2));
        assert_eq!(percentile(&[1, 1, 1, 9, 9], 95), Some(9));
        let values: Vec<u64> = (1..=1000).collect();
        assert_eq!(percentile(&values, 50), Some(500));
        assert_eq!(percentile(&values, 95), Some(950));
        assert_eq!(percentile(&values, 99), Some(990));
        assert_eq!(percentile(&values, 0), None);
        assert_eq!(percentile(&values, 101), None);
    }
}
