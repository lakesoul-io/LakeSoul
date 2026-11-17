# Native HDF5 baseline with 16 query workers

This standalone binary starts from `/tmp/vbench-common/src/main.rs` and its
native Rust HDF5 reader. It implements the measurement parameters in the vector
benchmark report. Build and run serially; one process handles one dataset,
bit width, library version, and repetition. It creates no index files.

## Build and run

From the LakeSoul repository root, Lead can run:

```sh
RUSTFLAGS='-C target-cpu=native' cargo -q build --release \
  --manifest-path rust/lakesoul-vector/benchmark16/Cargo.toml \
  --target-dir /tmp/lakesoul-bench16-target-new

RAYON_NUM_THREADS=16 /tmp/lakesoul-bench16-target-new/release/lakesoul-vector-benchmark16 \
  --h5 /path/to/sift-128-euclidean.hdf5 \
  --version new --library-source 'current working tree' \
  --bits 7 --workers 16 \
  --out /tmp/sift-new-b7-rep1.json
```

The output parent directory must exist. `--out` must name a new file; existing
results are never overwritten. Failed runs exit unsuccessfully and leave a
JSON record with `status: "failed"` and the error where output is writable.
The file is checkpointed after loading, building, and each nprobe.

The manifest contains its own `[workspace]`. Since this directory is directly
inside `rust/lakesoul-vector`, `lakesoul-vector = { path = ".." }` resolves to
the crate, not to the repository's `rust/` directory. The enclosing workspace
is rooted at the repository root. Do not add this package to that workspace or
modify the repository lockfile. Cargo will generate a separate harness lockfile
when Lead builds; preserve and reuse the same resolved dependencies for A/B.

Use `--metric angular` for angular files without `angular` in the dataset label;
`--metric ip` is an alias and also normalizes. Names containing `angular` infer
normalized InnerProduct; names containing `euclidean` infer L2. Ambiguous names
require `--metric`. `--dataset` changes the displayed label. Angular normalization
uses the starting harness's f32 norm and leaves zero vectors unchanged.

## The same source against the old crate

The requested upsert control is revision **13cd5ffd** in the future worktree
`/tmp/lakesoul-bench16-old`; the historical report's old **4989d850** is a
different comparison. Keep the labels distinct.

Lead can create an external temporary manifest, for example
`/tmp/lakesoul-bench16-manifest-old/Cargo.toml`, with:

```toml
[package]
name = "lakesoul-vector-benchmark16"
version = "0.1.0"
edition = "2024"
publish = false

[workspace]
resolver = "3"

[[bin]]
name = "lakesoul-vector-benchmark16"
path = "/home/zhibin/LakeSoul/rust/lakesoul-vector/benchmark16/src/main.rs"

[dependencies]
lakesoul-vector = { path = "/tmp/lakesoul-bench16-old/rust/lakesoul-vector" }
futures = "0.3"
tokio = { version = "1", features = ["rt-multi-thread"] }
rayon = "1.8"
serde_json = "1.0"
```

Build with that `--manifest-path`, a separate target directory, and the same
release flags. Run with `--version old13cd5ffd --library-source 'upsert control 13cd5ffd'`.
No version-specific `cfg` is needed. The harness uses only the shared builder,
index, search, and key APIs. Both revisions expose `SearchParams.parallelism`.

Keep the path dependency inline and on one line, as above: provenance resolves
it from the manifest embedded in the binary. JSON includes the full compiled
manifest, its directory, the resolved library directory, the supplied labels,
and read-only Git HEAD/status observations at process startup. Build and run
against unchanged source: the Git observation describes startup state, not a
cryptographic fingerprint of a previously built binary. Runtime compiler and
RUSTFLAGS are labelled as runtime observations; they do not prove build flags.
Preserve the build command/toolchain and generated lockfile with the run records.

## Fixed baseline and controls

- Train: every row, two scans in 10,000-row batches, never a full train matrix.
- Builder: nlist 1024, seed 42, FhtKacRotator, `faster_config=false`, key kind 0.
  The shared library builder supplies 15 Lloyd iterations and a 65,536-row
  reservoir; the harness does not substitute an algorithm or sample fewer rows.
- Bits: `--bits 1`, `3`, or `7`; run each in a fresh process.
- Search: nprobes 16,128,512; top_k 100; first 1,000 test rows; official first
  100 neighbor IDs. Missing rows, invalid IDs, or duplicate ground-truth IDs fail.
- Warmup: first 10 test rows, sequentially, before each nprobe measurement.
- Concurrency: exactly 16 scoped OS workers, strided queries, inner parallelism 1.
  `--workers 1` provides the same scoped-thread instrumentation as a control.
  `--nprobes` overrides are also controls and change the JSON default-parameter
  flag. No extra MT mode or second MT search execution is used.

Use `RAYON_NUM_THREADS=16` for report-compatible build concurrency. The actual
Rayon thread count and logical CPU count are recorded; the Tokio runtime has
16 worker threads. Query work is explicitly assigned to the scoped OS workers.
No CPU affinity or cache eviction is forced.

## Timing, recall, and memory

```text
HDF5 test + neighbors -> normalized queries + official truth (outside timings)
HDF5 train, pass 1 -> batch -> normalize -> builder reservoir
HDF5 train, pass 2 -> batch -> normalize -> builder.build -> in-memory index
index -> 10 warmups -> reset VmHWM -> start 16 threads -> ready/start gate
      -> 1000 timed searches -> join all -> capture VmHWM -> unique-ID recall
      -> query latency statistics + JSON
```

`build.report_build_seconds` equals `build.second_pass_seconds`, including
K-Means, opening and reading the source, normalization, rotation, assignment,
quantization, append, and in-memory flush. The first pass includes source open,
read, normalization, and reservoir insertion. `build.total_seconds` also
includes builder setup and the small gap between pass timers. File pages are
usually warm for pass 2, as in the report; no cold-cache claim is made.

Each nprobe records two wall times from the **same** search execution:

- `report_wall_seconds` / `report_qps`: starts before scoped thread creation and
  ends after all joins. Includes startup coordination, result collection, and
  joins; excludes recall and statistics.
- `post_start_gate_wall_seconds` / `post_start_gate_qps`: starts immediately
  before releasing ready workers and ends at the last completed search.
  Includes gate release/scheduling, and excludes creation and joins.

Neither denominator is a sum of worker or per-query durations. Each query has
an actual `Instant` pair around `index.search`; p50/p95/p99 use nearest rank
`ceil(p*N)` over those samples, in nanoseconds. They exclude gate waiting,
ID decoding, and recall. Raw per-query timings and recall counts are saved.
Worker-owned results are retained until after the wall timers and RSS snapshot;
this adds a bounded result-buffer cost and timing instrumentation relative to
the original MT loop, which discarded results. Wall time includes collecting
those observations. Account for this when comparing absolute historical QPS/RSS.

Recall is the intersection of unique returned row IDs and the official top-100
IDs, divided by 100 for each query and by 100,000 in aggregate. Eight-byte
little-endian decoding is strict; malformed/out-of-range IDs fail, and duplicate
returned IDs are counted without inflating recall. Every spawned worker is
joined even after errors, and panics/search errors make the run fail.

The original starting program included recall in its sequential wall timer and
omitted recall in MT. This harness excludes recall for both worker counts. Its
single-worker control is therefore not identical to the historical ST metric.

Build memory is `/proc/self/status` VmHWM immediately after the build and before
any searches. After each warmup, the harness writes `5` to
`/proc/self/clear_refs`, records whether the kernel accepted the write, and
captures before/after HWM/RSS. Search memory is captured after joins and before
recall. Reset failures produce an explicit warning, `reset_success=false`, and
`search_peak_isolated=false`; an unreset process peak is never labelled an
isolated search peak. Missing procfs data is reported as unavailable with an
error, never as zero. This search peak covers the selected worker count only;
the historical report's peak covered its sequential plus MT loops.

The inherited minimal HDF5 reader supports ANN-Benchmarks superblock v0,
object-header v1, legacy group tables, and contiguous uncompressed little-endian
32-bit matrices. It is not a general HDF5 implementation. Compressed/chunked or
newer-format files must be converted externally or rejected; query/source I/O
never occurs in the search timer.

For report aggregation, Lead should run two interleaved rounds old->new then
new->old, in fresh processes for each dataset/bits/version. Take the smaller
second-pass build time, larger QPS for each wall metric separately, and larger
RSS across repetitions. Keep raw JSON and do not mix post-gate QPS with the
thread-creation-inclusive QPS.

## Checks for Lead

No build, unit test execution, or benchmark run was performed while creating
this harness. To execute its small statistics/ID-decoding unit tests:

```sh
cargo -q test --manifest-path rust/lakesoul-vector/benchmark16/Cargo.toml
```

Build/test serially with the other benchmark work. The tests cover nearest-rank
percentiles (including ties/small samples) and strict 64-bit ID decoding.
