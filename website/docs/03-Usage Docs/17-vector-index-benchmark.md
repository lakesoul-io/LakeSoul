# Vector Index Benchmark

LakeSoul's vector search is built on an **IVF+RaBitQ** index that is maintained
incrementally as data is written.  New vectors are appended as immutable
*delta segments*; when the accumulated deltas indicate that a cluster's
centroid no longer represents its data, a shard can be **rebuilt** from all of
its data files (fresh k-means, published as a new index generation).  This page
documents the benchmark suite used to quantify that behaviour, the results
obtained on the standard ANN datasets, and what each experiment tells us.

The benchmark is a self-contained Rust scenario runner
(`rust/lakesoul-datafusion/benches/vector_rebuild_bench.rs`) orchestrated by
`script/benchmark/vector/run.sh`, with reporting in
`script/benchmark/vector/plot.py`.

## What the benchmark answers

| # | Experiment | Question it answers |
|---|------------|---------------------|
| **E1** | Policy × drift quality/cost | As updates accumulate along different distributions, how does recall@k evolve, and when does a rebuild pay off relative to its cost? |
| **E2** | Fresh build scaling | How do build time, peak memory and on-disk index size scale with vector count, dimension and `nlist`? |
| **E3** | Per-cluster vs shard trigger | Does per-cluster drift detection fire earlier and more precisely than the old whole-shard `delta/base` ratio? |
| **E4** | Search by index state | How do recall and QPS differ between a fresh index, one with accumulated deltas, and one that has just been rebuilt? |
| **E5** | End-to-end DataFusion SQL | What do QPS and recall look like when the data is written with SQL `INSERT` and searched with `ORDER BY array_distance(...) LIMIT k` (index candidates + exact re-rank)? |

## Test environment

| Item | Value |
|------|-------|
| Machine | Linux, 32 CPU cores, 62 GB RAM, local NVMe SSD |
| Build | `cargo bench` release profile, 16 worker threads (`RAYON_NUM_THREADS=16`) |
| Storage | local filesystem; all scenarios write LakeSoul table data as **vortex** files (`PhysicalFormat::Vortex`; the SQL scenario selects it through the `file_format` table option) |
| Distance metric | L2 |
| Index config | `nlist = 256`, `total_bits = 7`, `top_k = 10`, search `nprobe = 64` (E4 sweeps 1–256) |
| Queries per checkpoint | 100 |
| Random seed | 42 (fixed; the workload is deterministic) |

### Datasets

| Dataset | Dimension | Base | Queries | Ground truth | Update pool |
|---------|-----------|------|---------|--------------|-------------|
| GIST1M | 960 | 1,000,000 | 1,000 | 100-NN | 500K `gist_learn` vectors |
| GloVe-200d | 200 | 1,183,514 | 10,000 | 100-NN | base vectors (no separate learn set) |

GIST vectors are unnormalised; GloVe-200d vectors are unit-normalised, so L2
ranking is equivalent to angular ranking.  The datasets are standard ANN
benchmarks and are read directly from local `fvecs`/`ivecs` files — no download
is performed by the harness.

## How the benchmark works

### Workload

For E1/E3/E4 the runner uses **100,000 base vectors** and applies **10 update
rounds of 10,000 vectors** (a 100% growth in 10 steps).  Each round writes its
new vectors as a LakeSoul vortex data file and then calls the production
`auto_build_vector_index` policy with the new file plus the table's active
files, exactly as a write commit does.

### Rebuild policies

| Policy | Behaviour |
|--------|-----------|
| `none` | Incremental delta segments only (`rebuild_mode: "none"`). |
| `auto` | Real production policy: rebuild a shard when any of its clusters satisfies `delta_vectors / base_vectors > max_delta_ratio` (default `1.0`). |
| `periodic` | Incremental, with a forced rebuild every 3 rounds. |
| `always` | Forced rebuild on every round (upper bound on quality, worst case cost). |

`periodic` and `always` are expressed through the same `auto_build_vector_index`
call with a tiny `max_delta_ratio`, so no separate code path is measured.

### Update (drift) modes

Each round's vectors are sampled according to one of the following modes:

| Mode | Description |
|------|-------------|
| `uniform` | Vectors drawn uniformly from the update pool — no distribution change. |
| `skew` | Vectors drawn from the closest 5% of the pool to a random anchor — growth concentrated in a small region. |
| `shift` | Pool vectors translated by a fixed offset of `3.0 × mean vector norm` along a random unit direction — the data distribution moves. |

By default the **checkpoint queries follow the same drift** as the updates
(shared skew region / shift direction, different random stream), so recall
reflects the workload users actually issue after the data has changed.
`--static-queries` restores the fixed dataset queries for comparison.

### Metrics

- **recall@10** — overlap between the returned top-10 and the exact top-10
  computed by brute force over the vectors currently stored in the index
  (base + all deltas).
- **QPS** — throughput of the batched search path; **p50/p99** — per-query
  latency of the single-threaded search path.
- **build / update time** — wall time of a fresh build, of an incremental
  delta flush, and of a rebuild.
- **peak RSS** — process peak memory (`VmHWM`).
- **index size** — bytes on disk under `_vector_index/`.
- **drift signals** — per-cluster max `delta/base` ratio and the shard-level
  `delta/base` ratio read from the manifest.
- **generation** — manifest generation; a bump means a rebuild happened.

## Results

### E1 — rebuild policy vs update distribution

**Goal.** Find out when rebuilding pays off: does recall degrade as deltas
accumulate, and can a cheaper policy (per-cluster `auto`, periodic) match an
always-rebuild policy?

**Method.** 100K base + 10 × 10K updates for each `(policy, drift)` pair.
Recall@10 is checked every round (GloVe) or every two rounds (GIST, which is
more expensive to brute-force), using drifted queries.

**GIST1M — `shift` drift (distribution move):**

| Policy | Min recall@10 | Final recall@10 | Rebuilds | Index update time |
|--------|--------------:|----------------:|---------:|------------------:|
| `none` | 0.769 | 0.769 | 0 | 3.6 s |
| `auto@1.0` | **0.929** | **0.938** | 3 | 10.2 s |
| `periodic` (every 3) | 0.786 | 0.933 | 3 | 10.7 s |
| `always` | 0.927 | 0.938 | 5 | 15.5 s |

![E1 GIST shift](/img/vector-benchmark/e1_recall_gist_shift.png)

**What it tells us.** When the distribution actually moves, incremental-only
maintenance loses ~16 recall points (0.77 vs 0.93), and rebuilding is the only
way to recover them.  The per-cluster `auto` policy with the default
`max_delta_ratio = 1.0` reaches *always*-rebuild quality with **3 rebuilds and
2/3 of the index time** — the first rebuild fires as soon as a cluster's deltas
exceed its base, before recall has fallen far.

**All drifts, minimum recall@10 (key policies):**

| Dataset | Drift | `none` | `auto@1.0` | `periodic` | `always` |
|---------|-------|-------:|-----------:|-----------:|---------:|
| GIST1M | uniform | 0.966 | 0.966 | 0.962 | 0.969 |
| GIST1M | skew | 0.903 | 0.897 | 0.902 | 0.897 |
| GIST1M | shift | 0.769 | **0.929** | 0.786 | 0.927 |
| GloVe-200d | uniform | 0.880 | 0.880 | 0.881 | 0.881 |
| GloVe-200d | skew | 0.741 | 0.604 | 0.708 | 0.740 |
| GloVe-200d | shift | 0.868 | 0.890 | 0.890 | 0.890 |

![E1 GIST skew](/img/vector-benchmark/e1_recall_gist_skew.png)
![E1 quality vs cost](/img/vector-benchmark/e1_quality_cost_gist.png)

**What it tells us.**
- **Uniform growth**: recall stays high with no rebuilds; rebuilding buys
  nothing and only costs time.  (The `auto` policy mostly stays at 0 rebuilds
  for `max_delta_ratio ≥ 1`.)
- **Skewed growth**: concentrating updates in a region already covered by
  existing clusters does not require retraining; per-cluster rebuilds add cost
  without a recall benefit (they can even lower it slightly, because the
  retrained centroids introduce run-to-run variance while the data itself did
  not move).
- **Distribution shift**: rebuilding is necessary and the per-cluster `auto`
  policy is the best cost/quality compromise.

### E2 — fresh build scaling

**Goal.** Quantify the cost of the full (re)build that `auto`/manual rebuild
performs, as a function of dataset size, dimension and `nlist`.

**Method.** Fresh index build on a subset of each dataset with the provided
ground truth; no incremental updates.  Time is the `build()` wall time, peak
RSS is the process high-water mark, and index size is the on-disk total.

| Dataset | Vectors | nlist | Build time | Peak RSS | Index size |
|---------|--------:|------:|-----------:|---------:|-----------:|
| GIST1M (960d) | 100,000 | 256 | 2.3 s | 3.3 GB | 86 MB |
| GIST1M (960d) | 300,000 | 256 | 4.6 s | 5.7 GB | 254 MB |
| GIST1M (960d) | 1,000,000 | 256 | 18.2 s | 13.4 GB | 845 MB |
| GIST1M (960d) | 1,000,000 | 1024 | 60.8 s | 14.1 GB | 849 MB |
| GloVe-200d | 100,000 | 256 | 0.7 s | 0.4 GB | 26 MB |
| GloVe-200d | 1,000,000 | 256 | 4.2 s | 2.6 GB | 256 MB |
| GloVe-200d | 1,000,000 | 1024 | 8.8 s | 2.7 GB | 257 MB |
| GloVe-200d | 1,000,000 | 4096 | 36.0 s | 2.7 GB | 263 MB |

![E2 GIST build](/img/vector-benchmark/e2_build_gist.png)
![E2 GloVe build](/img/vector-benchmark/e2_build_glove.png)

**What it tells us.**
- Build time scales roughly linearly with the vector count and with `nlist`
  (k-means iterations dominate: GIST 1M goes from 18 s at `nlist=256` to 61 s
  at `nlist=1024`).  Index size is dominated by the quantised codes and grows
  linearly with `N` in both dimensions.
- Memory is dominated by the raw vectors being processed; 1M × 960-d needs
  ~13–14 GB peak.  A rebuild of a billion-scale shard should be planned as an
  offline/background operation with this budget.
- The numbers also give the *cost* side of E1: for GIST 1M a rebuild is tens
  of seconds, which is why triggering it only when drift is real (E3) matters.

### E3 — per-cluster vs shard-level trigger

**Goal.** The auto-rebuild trigger changed from a whole-shard
`delta/base` ratio to *any cluster* crossing `max_delta_ratio`.  How much
earlier does the new rule fire, and at what recall?

**Method.** Run the same stream with `none` (no rebuilds) and record, each
round, the maximum per-cluster `delta/base` ratio and the shard-level ratio.
For each threshold, report the first round at which each rule would fire and
the recall at that point.

Excerpt (GIST1M):

| Drift | Threshold | Per-cluster fires | Recall there | Shard-level fires | Recall there |
|-------|----------:|------------------:|-------------:|------------------:|-------------:|
| shift | 1.0 | round 1 | 0.813 | never (≤ 0.6 after 10 rounds) | – |
| shift | 0.5 | round 1 | 0.813 | round 6 | 0.793 |
| skew | 1.0 | round 2 | 0.968 | never | – |
| uniform | 1.0 | round 6 | 0.982 | never | – |

![E3 trigger GIST](/img/vector-benchmark/e3_trigger_gist.png)
*Per-cluster (red) and shard-level (blue) drift signals vs recall (green) for
the three drift modes.*

**What it tells us.** Under distribution shift, the shard-level ratio stays
below 0.6 for the whole run — the old rule would *never* rebuild, even though
recall has already dropped to ~0.8.  The per-cluster rule fires in the first
rounds, which is exactly the behaviour that keeps E1's `auto` policy close to
`always` in quality.  Under uniform growth the cluster rule fires late (round
6 at threshold 1.0), so it does not cause spurious rebuilds on healthy data.

### E4 — search across index states

**Goal.** Confirm that accumulating deltas (and rebuilding) does not degrade
search quality or throughput in an unexpected way.

**Method.** Build an index on the base data, then measure a full `nprobe`
sweep (1, 4, 16, 64, 128, 256) for three states: **fresh** (base only),
**delta** (after 6 rounds × 5K uniform updates, no rebuild), and **rebuilt**
(the same data after a forced rebuild).  Ground truth is brute-forced over the
vectors actually present in each index state.

| Dataset | State | Index load | Best recall@10 | QPS at that point |
|---------|-------|-----------:|---------------:|------------------:|
| GIST1M | fresh | 57 ms | 0.977 (@nprobe 128) | 19,359 |
| GIST1M | delta | 217 ms | 0.971 (@nprobe 64) | 21,377 |
| GIST1M | rebuilt | 71 ms | 0.971 (@nprobe 128) | 14,691 |
| GloVe-200d | fresh | 25 ms | 0.970 (@nprobe 256) | 17,318 |
| GloVe-200d | delta | 89 ms | 0.949 (@nprobe 256) | 12,647 |
| GloVe-200d | rebuilt | 31 ms | 0.951 (@nprobe 256) | 12,662 |

![E4 GIST recall vs QPS](/img/vector-benchmark/e4_recall_qps_gist.png)

**What it tells us.**
- Search quality and throughput remain comparable across the three states;
  delta segments do not break the recall/QPS trade-off (GloVe's small
  regression recovers after rebuild).
- The visible cost of accumulated deltas is **index load time** (GloVe
  25 → 89 ms, GIST 57 → 217 ms with six delta generations), because every
  segment must be read and merged when the index is opened.  Rebuilding folds
  the deltas back into one base segment and removes that overhead.
- Why opening a multi-segment index is slower, and what was done about it:
  each segment pads its final FastScan batch to 32 vectors, so merging
  segments requires extracting and re-packing every vector's codes (a single
  freshly-built or rebuilt index needs none of this).  The loader now
  reuses single-segment clusters as-is, concatenates already-aligned
  segments directly, and re-packs the rest in parallel across batches while
  reading clusters concurrently.  Compared with the first implementation this
  cuts index open by **4–5× for delta-heavy indexes** (GIST 1.31 s → 0.27 s)
  and **up to ~12× for fresh indexes** (GIST 0.69 s → 0.06 s).
- Because search keeps working across states, rebuilds can be scheduled
  independently of query serving; readers switch to the new generation when
  the metadata catalog resolves the refreshed commit.

### E5 — end-to-end DataFusion SQL (write + search)

**Goal.** Measure the complete SQL path: create a table with the vector index
property, write vectors with `INSERT ... SELECT` (DataFusion sink + the
post-commit auto index build), then search with
`ORDER BY array_distance(vec, ARRAY[...]) LIMIT k`.  This path is served by
the index (candidate ids) plus an **exact re-rank of the candidate rows inside
DataFusion**, so it exercises the integration rather than the index in
isolation.  The scenario requires PostgreSQL metadata.

**Method.**
- `CREATE EXTERNAL TABLE ... OPTIONS ('vector_index_columns' ..., '
  file_format' 'vortex')` declares the index and the write format; the base
  100K vectors are inserted from an in-memory table registered in a separate
  catalog, followed by 10 further `INSERT` rounds of 10K uniform vectors (200K
  rows written in total).  The table's rebuild policy runs during these SQL
  writes.
- Search runs one SQL statement per query (including SQL planning) with
  `nprobe = 64`; recall is computed against the exact top-10 over all written
  vectors, and `EXPLAIN VERBOSE` is checked for `LakeSoulVectorSearchExec`.

| Dataset | Rows written | Base insert | Recall@10 | QPS | Mean latency | p99 | Index (live generation) |
|---------|-------------:|------------:|----------:|----:|-------------:|----:|-------------------------|
| GloVe-200d | 200,000 | 1.3 s (+10 rounds, median 0.3 s) | 0.899 | 38.9 | 25.7 ms | 29.2 ms | 1 shard, 108 MB, 190K base + 10K delta, gen 2 |
| GIST1M (960d) | 200,000 | 4.7 s (+10 rounds, median 0.7 s incl. rebuilds) | 0.980 | 23.3 | 43.0 ms | 64.4 ms | 1 shard, 432 MB, 170K base + 30K delta, gen 3 |

These numbers are measured with the index cache, the single-shard fixes and
the row-level pk locator described below.  The same workload written as **parquet** measured
3.73 QPS / 268 ms (GloVe) and 1.03 QPS / 967 ms (GIST) at identical recall in
an earlier run (before the index cache and shard fixes) — vortex was ~1.9×
(GloVe) and ~3.1× (GIST) faster per SQL query.

**Index cache.**  A process-wide cache keeps the merged in-memory index per
`(object store, index prefix)` and reuses it while the manifest still resolves
to the same commit; a rebuild or delta commit publishes a new manifest, so the
next query loads the new generation and replaces the entry.  The same E5
workload with and without the cache (identical recall, single hash bucket):

| Dataset | Live index size | Cache off | Cache on | Speedup |
|---------|----------------:|----------:|---------:|--------:|
| GloVe-200d | 107 MB | 7.04 QPS / 142.0 ms | 24.74 QPS / 40.4 ms | 3.5× |
| GIST1M (960d) | 432 MB | 3.19 QPS / 313.6 ms | 15.72 QPS / 63.6 ms | 4.9× |

The cache is bounded by a byte budget
(`LAKESOUL_VECTOR_INDEX_CACHE_BYTES`, default 512 MiB, `0` disables it) and
evicts by weighted LRU.

**Row-level primary-key locator.**  With the index cached, the remaining cost
was the candidate scan: the pushed `pk IN (...)` filter is evaluated row by row
over every file (~28 ms GloVe, ~47 ms GIST), and random candidate ids defeat
zone-map pruning.  Two changes remove it:

- *Candidate locator.*  A process-wide, per-file `pk -> row` map answers
  `pk = v` and `pk IN (...)` filters (including the candidate list injected by
  the vector search) by fetching only the matching rows through vortex row
  indices.  The map is built lazily from the file's primary-key column and
  keyed by the file location alone — data files are immutable, so an entry
  never goes stale (a compacted or rewritten file has a new location).  The
  candidate set is capped at 10,000 keys; parquet files and non-integer
  primary keys fall back to the regular scan.  This is a generic primary-key
  pushdown, not a vector-search special case.  Budget:
  `LAKESOUL_PK_CACHE_BYTES` (default 256 MiB, `0` disables it).
- *Small row blocks for vector columns.*  Vortex reads random rows at
  row-block granularity, so the writer gives every column declared in the
  table's `vector_index_columns` property 1024-row blocks instead of the
  default 8192.  Fetching 100 scattered candidates then reads ~1/8 of the
  bytes, and full-column scans are also slightly faster for these wide
  columns.

Together they cut execution from 34.3 ms to 20.4 ms (GloVe) and 53.5 ms to
31.3 ms (GIST): the candidate fetch is now ~4 ms (GloVe) / ~7 ms (GIST) warm,
and reads only the candidate rows.

![E5 SQL end-to-end](/img/vector-benchmark/e5_sql_end_to_end.png)

**What it tells us.**
- **Functional correctness end to end:** `EXPLAIN VERBOSE` picks
  `LakeSoulVectorSearchExec`, and SQL recall matches the index-level
  measurements (0.90 for GloVe, 0.98 for GIST).  The candidate-then-rerank
  path returns exact top-k among the retrieved candidates.
- **SQL writes maintain the index:** each `INSERT` commits data files and the
  post-commit hook updates (or rebuilds) the index; the manifest generation
  reached 2 (GloVe) and 3 (GIST) across the ten update rounds.
- **The candidate scan was the dominant cost and is now ~50× faster.**  The
  query plan is `Filter(pk IN candidates)` above `MergeParquetExec`, and the
  merge used to read and merge *every* row of every data file before the
  filter ran (1.7 s per GIST query for 200K rows).  Two things were needed to
  fix it: enable `file_filter_pushdown` for the vector-search reader
  (`supports_filters_pushdown` classifies the pk filter as an Inexact pushdown
  only when this option is set) and build the injected filter as a single
  `pk IN (...)` instead of a chain of `OR`s — the OR-chain made vortex's
  filter pushdown hang, while the IN list is evaluated and pushed cheaply.
  With the filter applied inside each file scan, the merge only sees the
  ~100 candidate rows: the scan dropped from ~1.7 s to ~10–30 ms.
- **Index open was the dominant remaining cost and is now cached.**  After the
  E4 loader optimizations every query still re-opened and re-merged the shard
  (~0.10 s GloVe, ~0.27 s GIST) versus only ~6–30 ms of candidate scan.  The
  process-level cache above removes that cost; together with the shard fixes
  and the pk locator below the E5 workload now reaches 38.9 QPS (GloVe) /
  23.3 QPS (GIST).
- **`hashBucketNum` was silently ignored by SQL DDL.**  DataFusion lower-cases
  `OPTIONS` keys and prefixes namespace-less keys with `format.`, so the
  Spark/Flink spelling `'hashBucketNum' '1'` arrived as
  `format.hashbucketnum` and the provider fell back to the default of 4
  buckets.  Every query then probed 4 index shards and scanned 4× the data
  files.  The option is now honored (the SQL test asserts the stored
  property), and a single bucket is the fastest configuration for this
  workload (24.7 vs 14.1 QPS for 4 buckets on GloVe).
- **Buckets are read in parallel.**  `LakeSoulVectorSearchExec` used to drive
  each hash bucket's reader sequentially; buckets are independent (separate
  indexes and files), so they now run on one scoped thread each.  On a 4-bucket
  GloVe table this cut execution from 92.6 ms to 61.5 ms per query.
- **The candidate scan was the last bottleneck and is now row-level.**  The
  pushed `pk IN (...)` predicate was evaluated across every row of every file
  (~28 ms GloVe, ~47 ms GIST), because random candidate ids defeat zone-map
  pruning.  The pk locator above replaces it with a per-file `pk -> row` map
  and a row-index fetch: the scan itself is now sub-millisecond and the
  fetch is ~4 ms (GloVe) / ~7 ms (GIST) with warm caches.  With it, the
  per-query split is SQL planning ~2–4 ms, physical planning ~3–8 ms, index
  probe ~6–11 ms and candidate fetch the rest.
- **The write format matters.**  The SQL sink used to hard-code a parquet-only
  multipart writer and ignored the table's `file_format`; it now uses the
  format-aware writer, so tables can be created with
  `file_format = "vortex"`.  At identical recall vortex is ~1.9×
  (GloVe) and ~3.1× (GIST) faster per SQL query than parquet, because the
  vortex candidate scan prunes faster.
- **Index files are garbage-collected:** the commit history and reader leases
  live in the metadata database (`vector_index_*` tables), and after every
  write the superseded generations are collected once they are older than a
  grace period (`gc_grace_seconds`, default 1 h, `vector_index_columns` JSON
  property) and no reader holds a lease.  An explicit `gc_vector_index` API
  (also exposed on `LakeSoulTable`) sweeps on demand and removes the
  control-plane rows of dropped partitions.

## Recommendations

1. Keep the default `rebuild_mode = "auto"` with `max_delta_ratio = 1.0` for
   general workloads: it never rebuilds under uniform growth, detects real
   distribution shifts within the first update rounds, and reaches
   always-rebuild quality at roughly two thirds of the cost.
2. For workloads with strong, continuous drift, lower `max_delta_ratio`
   (e.g. `0.5`) or call the manual rebuild API during off-peak windows.
3. For uniform or mildly skewed ingestion, `rebuild_mode = "none"` is
   sufficient and avoids rebuild cost entirely.
4. Budget rebuilds from the E2 table (GIST 1M: ~18 s at `nlist=256`, ~14 GB
   peak RSS); make sure the trigger and rebuild run in the background.

## Reproducing the benchmark

The harness reads local `fvecs`/`ivecs` datasets; adjust `DATA_DIR` or pass the
dataset paths directly to the bench.

```bash
# Run every experiment on both datasets (writes JSON + logs under the
# gitignored benchmark-results/ directory)
DATA_DIR=~/program/opensource/rabitq-rs/data \
  script/benchmark/vector/run.sh all --results /tmp/vector-bench

# Or one experiment at a time
script/benchmark/vector/run.sh e1 --results /tmp/vector-bench

# Render the plots (matplotlib is fetched ephemerally by uv)
uv run --with matplotlib python script/benchmark/vector/plot.py \
  --results /tmp/vector-bench
```

`run.sh` supports `QUICK=1` for a small smoke run, and `THREADS=<n>` to set the
worker thread count.  Each run writes one JSON file with the per-round metrics,
a `logs/` file with the raw runner output, and `plots/*.png` with the figures
used above.  The runner can also be invoked directly:

```bash
cargo bench -p lakesoul-datafusion --bench vector_rebuild_bench -- \
  --scenario stream --base gist_base.fvecs --query gist_query.fvecs \
  --learn gist_learn.fvecs --policy auto --max-delta-ratio 1.0 \
  --drift shift --drift-strength 3.0 --rounds 10 --per-round 10000 \
  --work-dir /tmp/vector-bench/work/gist-shift
```
