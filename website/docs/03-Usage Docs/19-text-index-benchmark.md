# Text Index Benchmark

LakeSoul's full-text search is built on an **inverted index** (Tantivy) that
is maintained incrementally as data is written.  Every write produces
immutable index *deltas* alongside the data files; compaction merges them
into fresh splits and removes the entries of deleted rows.  A search reads
the per-split BM25 top-k, merges the candidates by primary key, and then
**exactly re-verifies** each candidate against the current row text before
returning it.  This page documents the benchmark suite used to quantify that
behaviour on real IR corpora, the results obtained, and what each experiment
tells us.

The benchmark is a self-contained Rust scenario runner
(`rust/lakesoul-datafusion/benches/text_index_bench.rs`) orchestrated by
`script/benchmark/text/run.sh`, with dataset preparation in
`script/benchmark/text/download.py`, figures in
`script/benchmark/text/plot.py` and the gateway scenario in
`script/benchmark/text/gateway_bench.py`.

## What the benchmark answers

| # | Experiment | Question it answers |
|---|------------|---------------------|
| **T1** | Rebuild policy × update pattern | Do appends, same-key rewrites, deletes and topic drift degrade retrieval, and when does rebuilding the shard pay off? |
| **T2** | Fresh build | How do analyzer (`jieba` / `default` / `en_stem`), positions and stored text affect build speed, index size and quality?  How does the build scale to millions of documents? |
| **T3** | Rebuild trigger | When does the production drift trigger fire, and does drift predict quality loss? |
| **T4** | Search by index state | How do latency and recall differ between a fresh index, one with accumulated deltas, and one that has just been rebuilt, as the candidate count per shard is swept? |
| **T5** | Shard count | What does fanning the index out over more shards cost in build time and query latency? |
| **T6** | End-to-end DataFusion SQL | What do insert throughput and `text_match` + `text_score` query latency look like through the SQL path? |
| **T7** | End-to-end ES gateway | What do bulk write and `_search` look like over the Elasticsearch-compatible HTTP API? |

## Test environment

| Item | Value |
|------|-------|
| Machine | Linux, 32 CPU cores, 62 GB RAM, local NVMe SSD |
| Build | `cargo bench` release profile, 16 worker threads |
| Storage | local filesystem; table data as Parquet, one file per hash bucket |
| Tokenizer | `jieba` unless the experiment varies it |
| Index config | `with_positions = true`, `stored = false` |
| Search | BM25, global top-k by score, exact verification on, 100 candidates per shard |
| Metric | nDCG@10, Recall@100, MRR@10 against the dataset qrels; Recall@10 against the exact single-index BM25 baseline |
| Seed | 42 (fixed; the workload is deterministic) |

### Datasets

| Dataset | Documents | Judged queries | Role |
|---------|----------:|---------------:|------|
| SciFact (BEIR) | 5,183 | 339 | smoke test (`run.sh quick`) |
| MS MARCO | 8.8M | 7,437 dev | main English benchmark; subsets of 100K/300K/1M/2M documents |
| T2Retrieval | 118,605 | 22,812 dev | Chinese benchmark (first 100K documents) |

The datasets are normalized into `corpus.jsonl` / `queries.jsonl` /
`qrels.tsv` by `download.py`, which streams the MTEB mirrors; the Rust
harness itself never touches the network.  The harness restricts the qrels
to documents inside the loaded subset and evaluates only queries that have
at least one judged document there, so the metrics stay meaningful on
subsets (in the MS MARCO 100K subset, 34 queries are judged).

## How the benchmark works

### Workload

The incremental experiments use **100,000 base documents** and **10 update
rounds of 10,000 documents**.  Each round writes its documents as a LakeSoul
data file and then calls the production `auto_build_text_index` policy with
the new file plus the table's active files, exactly as a write commit does.

### Update patterns

| Pattern | Description |
|---------|-------------|
| `append` | New primary keys — the corpus grows. |
| `rewrite` | Existing primary keys rewritten with new text — the corpus stays the same size, the index accumulates new versions. |
| `delete` | Previously written documents are deleted (CDC tombstones) — the corpus shrinks. |
| `topic-shift` | New documents sampled from a different part of the corpus — the vocabulary shifts away from the queries. |

### Rebuild policies

| Policy | Behaviour |
|--------|-----------|
| `none` | Incremental deltas only (`rebuild_mode: "none"`). |
| `auto@r` | Production policy: rebuild a shard when its `delta / base` documents ratio exceeds `max_delta_ratio = r`. |
| `periodic (3)` | Incremental, with a forced rebuild every 3 rounds. |
| `always` | Forced rebuild every round (upper bound on cost). |

`periodic` and `always` are expressed through the same
`auto_build_text_index` call with a tiny `max_delta_ratio`, so no separate
code path is measured.

### Metrics

- **nDCG@10 / MRR@10 / Recall@100** — retrieval quality against the dataset
  qrels of the current live rows.
- **Recall@10 vs exact BM25** — agreement between what the index retrieved
  and an exact single-index BM25 over the same live rows; a value below 100%
  isolates candidate loss, a value of 100% means the index order equals the
  exact BM25 order.
- **p50 / p95 / p99, QPS** — query latency through the production scan path.
- **build / update time** — wall time of a fresh build, of one incremental
  index update, and of a rebuild; **docs/s** normalizes it.
- **peak RSS** — process peak memory (`VmHWM`).
- **bytes/doc and index size** — bundle bytes under `_text_index/`.
- **drift signal** — `delta / base` document ratio of the shard catalog.
- **dropped stale** — candidates dropped by the exact verification pass.

## Results

### T1 — rebuild policy vs update pattern

**Goal.** Find out whether an incrementally maintained text index loses
quality as versions accumulate, and whether a cheaper policy can replace
rebuilding.

**Method.** 100K base + 10 × 10K updates, quality measured every round
against the qrels (`recall@10` vs exact BM25 and nDCG@10).

| Policy | Rebuilds | Index update (total) | Min recall@10 vs BM25 | Final nDCG@10 | Final recall@100 | Index size |
|--------|---------:|---------------------:|----------------------:|--------------:|-----------------:|-----------:|
| `none` | 0 | 1.3 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `auto@1.0` | 0 | 1.2 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `auto@2.0` | 0 | 1.3 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `auto@0.5` | 1 | 3.6 s | 100.0% | 0.337 | 82.4% | 56 MB |
| `auto@0.25` | 2 | 7.7 s | 100.0% | 0.336 | 82.4% | 77 MB |
| `periodic (3)` | 3 | 9.2 s | 100.0% | 0.336 | 82.4% | 98 MB |
| `always` | 5 | 13.5 s | 100.0% | 0.336 | 82.4% | 141 MB |

Ten write rounds of 10K documents cost **~1.3 s** of incremental index
maintenance in total; a rebuild costs ~2.5 s and 33 MB.  Rebuilding does
**not** improve retrieval quality: the incremental index keeps the exact
BM25 order (100% recall@10 vs the exact baseline), because verification
re-checks every candidate against the current text.  The policy default
`auto@1.0` never fires in this workload and pays nothing; the aggressive
policies add 3–10× the maintenance cost and 2–4× the disk for a slightly
lower nDCG@10 — rebuilding the corpus loses the better document statistics
of the larger index.

**Update pattern (auto@1.0):**

| Pattern | Rebuilds | Index update (total) | Min recall@10 vs BM25 | Final nDCG@10 | Final recall@100 | Index size |
|---------|---------:|---------------------:|----------------------:|--------------:|-----------------:|-----------:|
| `append` | 0 | 1.2 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `rewrite` | 0 | 1.2 s | 100.0% | 0.378 | 82.4% | 34 MB |
| `topic-shift` | 0 | 1.2 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `delete` | 0 | 0.0 s | 90.6% | 0.644 | 82.4% | 33 MB |

Appends, same-key rewrites and topic drift never lose a candidate.
The delete pattern is the only one that dips (90.6% in the last round,
where 90% of the corpus had been deleted and ~89 of every query's 100
candidates were stale versions that verification dropped); a rebuild after
bulk deletes removes those entries and restores full candidate headroom.
Note that nDCG rises as the corpus shrinks — BM25 simply gets easier with
fewer distractors.

![T1 policy recall and cost](/img/text-benchmark/t1_msmarco_policy_recall_cost.png)

### T2 — fresh build

**Analyzer, positions and stored text (MS MARCO 100K):**

| Tokenizer | Positions | Stored | Build docs/s | Bytes/doc | Build time | nDCG@10 | Recall@100 | p50 |
|-----------|-----------|--------|-------------:|----------:|-----------:|--------:|-----------:|----:|
| `jieba` | yes | no | 39,921 | 343.2 | 2.5 s | 0.350 | 82.4% | 8.1 ms |
| `jieba` | no | no | 44,526 | 227.1 | 2.2 s | 0.350 | 82.4% | 9.2 ms |
| `jieba` | yes | yes | 34,496 | 535.1 | 2.9 s | 0.350 | 82.4% | 8.0 ms |
| `default` | yes | no | 93,087 | 269.7 | 1.1 s | 0.354 | 85.3% | 7.3 ms |
| `default` | no | no | 79,027 | 215.9 | 1.3 s | 0.354 | 85.3% | 7.5 ms |
| `en_stem` | yes | no | 44,747 | 261.3 | 2.2 s | **0.368** | 82.4% | 8.4 ms |
| `en_stem` | no | no | 48,904 | 208.5 | 2.0 s | **0.368** | 82.4% | 8.4 ms |

- `en_stem` gives the best English quality (nDCG 0.368 vs 0.354 for
  `default` and 0.350 for `jieba`), for 30–50% slower builds than `default`.
- **Positions** cost 15–30% of index size and support phrase queries; they
  can be disabled when only term matching is needed.
- **Stored** costs ~190 bytes/document and buys nothing for the search path
  (the reader takes the text from the data files for verification); keep it
  off unless a client reads the stored text from the index directly.
- `jieba` on English text pays segmentation cost without quality gain —
  choose the analyzer per language.

**Chinese (T2Retrieval, first 100K documents):**

| Tokenizer | Build time | Docs/s | Bytes/doc | nDCG@10 | Recall@100 | p50 | QPS |
|-----------|-----------:|-------:|----------:|--------:|-----------:|----:|----:|
| `jieba` | 20.1 s | 4,968 | 2,497 | **0.642** | **80.5%** | 29.9 ms | 30 |
| `default` | 42.4 s | 2,356 | 2,760 | 0.016 | 1.6% | 0.9 ms | 397 |

Chinese requires `jieba`: the `default` analyzer keeps a whole Chinese run
as a single token, so it matches almost nothing (and is faster only because
those huge tokens are cheap to index and search).

**Scaling (MS MARCO, `jieba` + positions):**

| Docs | Build time | Docs/s | Data write | Bytes/doc | Index size | Peak RSS | nDCG@10 | Recall@100 | p50 | QPS |
|-----:|-----------:|-------:|-----------:|----------:|-----------:|---------:|--------:|-----------:|----:|----:|
| 100,000 | 2.5 s | 39,921 | 0.3 s | 343.2 | 33 MB | 552 MB | 0.350 | 82.4% | 8.1 ms | 94 |
| 300,000 | 9.0 s | 33,262 | 0.8 s | 338.8 | 97 MB | 1.3 GB | 0.279 | 76.5% | 9.7 ms | 72 |
| 1,000,000 | 30.3 s | 32,953 | 1.9 s | 334.4 | 319 MB | 3.4 GB | 0.202 | 67.6% | 12.1 ms | 45 |
| 2,000,000 | 63.3 s | 31,605 | 3.6 s | 332.7 | 635 MB | 6.9 GB | 0.190 | 58.8% | 17.1 ms | 26 |

The index stays slightly above 300 bytes/document (a third of the raw
Parquet text); build time and memory grow linearly (2M documents build in
63 s with 6.9 GB peak RSS).  Search latency doubles from 100K to 2M
documents.  Quality decreases with corpus size because MS MARCO qrels are
sparse relative to the larger document pool — the index is not losing
candidates (recall@10 vs the exact BM25 baseline stays 100%).

![T2 parameter matrix](/img/text-benchmark/t2_params_matrix.png)

![T2 build scaling](/img/text-benchmark/t2_msmarco_scaling.png)

### T3 — rebuild trigger

**Goal.** When does the production drift signal fire, and does it predict
quality loss?

| Pattern | First round with `delta/base >= 1.0` | Rebuild round |
|---------|-------------------------------------:|--------------:|
| `append` | 10 | — |
| `rewrite` | 10 | — |
| `topic-shift` | 10 | — |

With 10% growth per round, the **write-volume** ratio reaches 1.0 only at
round 10 (the shard holds 100K base and 100K delta documents) and the run
ends before the next write pushes it above the default threshold.  The
ratio counts written documents, so `rewrite` and `delete` accumulate it the
same way even though the live corpus does not grow, and `topic-shift`
changes vocabulary but not the ratio at all.  For the text index this
signal is therefore a **cost heuristic** (it bounds delta build work), not
a quality alarm: the exact verification pass keeps retrieval quality
independent of how stale the index is.

![T3 drift and rebuild](/img/text-benchmark/t3_msmarco_trigger.png)

### T4 — search by index state

**Goal.** Measure the read path against a fresh index, an index with three
rounds of deltas, and a just-rebuilt index, sweeping the candidate count
per shard.

The three states hold 130,000 live documents: `fresh` (index built once),
`delta` (100K base + 30K appended, 7 splits), `rebuilt` (the same data
after a production rebuild, 1 split).

| State | Candidates/shard | Recall@10 vs exact BM25 | Recall@100 | p50 | QPS | Verify |
|-------|-----------------:|------------------------:|-----------:|----:|----:|-------:|
| fresh | 10 | 100.0% | 61.8% | 4.7 ms | 196 | 3.3 ms |
| fresh | 20 | 100.0% | 73.5% | 4.5 ms | 220 | 3.3 ms |
| fresh | 40 | 100.0% | 73.5% | 5.2 ms | 186 | 3.8 ms |
| fresh | **100** | 100.0% | 82.4% | 8.2 ms | 126 | 5.7 ms |
| fresh | 200 | 100.0% | 82.4% | 12.5 ms | 82 | 9.1 ms |
| delta | 10 | 100.0% | 61.8% | 4.5 ms | 196 | 3.0 ms |
| delta | 100 | 100.0% | 82.4% | 8.5 ms | 114 | 5.7 ms |
| delta | 200 | 100.0% | 82.4% | 13.0 ms | 74 | 8.3 ms |
| rebuilt | 10 | 100.0% | 58.8% | 5.0 ms | 188 | 3.3 ms |
| rebuilt | 100 | 100.0% | 82.4% | 8.5 ms | 117 | 6.0 ms |
| rebuilt | 200 | 100.0% | 82.4% | 16.7 ms | 64 | 11.3 ms |
| fresh, no verify | 100 | 100.0% | 82.4% | **2.6 ms** | 335 | — |

- The index order equals the exact BM25 order at **any** candidate count
  (recall@10 vs exact BM25 is 100% even at 10 candidates): BM25 ranking is
  local enough that a per-split top-k merge reproduces it.
- Recall@100 saturates at ~100 candidates, which is exactly the production
  default; 200 candidates buy nothing and cost ~50% more latency.
- Verification costs 3–6 ms per query (it rebuilds a tiny in-memory index
  over the scanned candidate rows) but keeps stale index entries invisible;
  without it the query is 3× faster.  Disable it only when stale hits are
  acceptable.
- Fully rebuilt is **not faster to search** than a fresh or delta index
  (slightly slower here): a rebuilt single split is not measurably faster to
  search than several delta splits — the reason to rebuild is compacting
  the index and dropping deleted entries, not query speed.

![T4 index states](/img/text-benchmark/t4_msmarco_search_states.png)

![T4 verification cost](/img/text-benchmark/t4_msmarco_verify_cost.png)

### T5 — shard count

**Goal.** Measure build and search when the same 100K documents are spread
over 1, 4, 16 and 64 shards (hash buckets), with 100 candidates per shard.

| Shards | Build time | Docs/s | Index size | Splits | nDCG@10 | Recall@100 | p50 | QPS |
|-------:|-----------:|-------:|-----------:|-------:|--------:|-----------:|----:|----:|
| 1 | 2.8 s | 36,163 | 33 MB | 1 | 0.350 | 82.4% | 9.6 ms | 100 |
| 4 | 2.9 s | 34,903 | 37 MB | 4 | 0.352 | 82.4% | 21.1 ms | 44 |
| 16 | 4.2 s | 24,049 | 41 MB | 16 | 0.314 | 82.4% | 67.9 ms | 14 |
| 64 | 22.9 s | 4,371 | 45 MB | 64 | 0.319 | 82.4% | 236.8 ms | 4 |

Query latency grows almost linearly with the number of shards — every shard
is searched and its candidates fetched and verified, and the candidate
budget multiplies (100 per shard).  Build throughput collapses at 64 shards
because each shard has its own writer and Tantivy indexing overhead is
per-split.  Per-split BM25 statistics also make the merged ranking slightly
approximate (nDCG 0.314–0.352 against 0.350 for a single shard).  Use as
few shards as the write parallelism requires; sharding is not a way to make
search faster.

![T5 shard count](/img/text-benchmark/t5_msmarco_shards.png)

### T6 — end-to-end DataFusion SQL

**Goal.** Run the production write and query path through Spark-style SQL:
`INSERT` base and incremental batches, then
`SELECT ... WHERE text_match(content, '...') ORDER BY text_score(content, '...') DESC LIMIT 10`
with the index declared through the `text_index_columns` table option.

| Measurement | Value |
|-------------|-------|
| Base insert (100K rows) | 3.4 s (29,101 rows/s) |
| Incremental insert (10K rows) | 0.16 s / 0.14 s |
| Query p50 / QPS | 117.7 ms / 8.4 |
| nDCG@10 / Recall@100 | 0.350 / 82.4% |
| Index used by the planner | yes (`EXPLAIN` checked) |

The SQL path delivers the same quality as the scan API, but ~14× the
latency (117 ms vs 8 ms) because each query goes through the full
DataFusion plan and batch pipeline rather than the direct candidate path of
the scan API.  Latency-sensitive clients should use the scan API for scoring
and the SQL path for filtering/joins.

![T6 SQL](/img/text-benchmark/t6_sql.png)

### T7 — ES-compatible gateway

**Goal.** Drive the Elasticsearch-compatible gateway over HTTP: bulk-write
20K MS MARCO documents, run `match` searches, then two rewrite rounds of
2,000 documents and re-measure.

| Measurement | Value |
|-------------|-------|
| Bulk write | 1,161 docs/s (20,000 in 17.2 s) |
| Search p50 / QPS | 347 ms / 3.3 |
| nDCG@10 / Recall@100 | 0.604 / 85.7% |
| Rewrite round 1 | 2,000 docs at 1,271 docs/s; quality unchanged |
| Rewrite round 2 | 2,000 docs at 1,240 docs/s; quality unchanged |

A later gateway optimization pass re-measured the same 20K/4-bucket
setup at 1.0-1.2K docs/s bulk and ~0.42 s search p50 (run-to-run
variance); the one-bucket configuration documented in the
[gateway performance tuning](18-es-compatible-gateway.md#performance-tuning)
writes ~2.9K docs/s and answers at p50 ~76 ms; with the gateway's deferred index maintenance the same table writes ~12.9K docs/s and still answers at p50 ~46 ms once the background build catches up.  Quality matches the engine (the judged MS MARCO subset is small, so the
absolute nDCG is higher than on the full corpus).  Writes and searches both
pay the merge-on-read plus index-maintenance cost per HTTP request, and the
default 4-bucket layout multiplies search latency by the shard count; the
gateway is a compatibility layer for WeKnora-style clients, not a
latency-optimized endpoint.

![T7 gateway](/img/text-benchmark/t7_gateway.png)

## What the benchmark found

Two engine bugs surfaced while running the full matrix; both are fixed in
the same series as this page:

1. **Split build merge race.**  Building a 2M-document shard could fail
   with Tantivy's *"segments that were merged could not be found"*: the
   background merge policy retired segments between the id lookup and the
   forced merge.  Background merges are now disabled while a split is
   built, and the explicit merge sees a stable segment list.
2. **Chinese queries were parsed as phrases.**  Tantivy turns a
   whitespace-free literal into a phrase query when positions are indexed,
   so a Chinese sentence only matched documents containing that exact
   sentence.  T2Retrieval nDCG was 0.038 with almost no candidates;
   analyzing plain query text into OR-combined terms raised it to 0.642
   (recall@100 80.5%), matching an independent jieba BM25 baseline.

## Conclusions

- **Let the index accumulate deltas.** Text search quality does not decay
  with stale index entries because verification is exact; the production
  `auto@1.0` default rebuilds only under write volumes far beyond the
  measured workload and costs nothing when it does not fire.
- **Rebuild for disk, not for quality.** Rebuilds compact many small splits
  and remove deleted documents' entries (the delete pattern is the only one
  whose candidate headroom drops).  They do not make queries faster and
  slightly change nDCG through fresh statistics.
- **Pick the analyzer by language.** `en_stem` for English, `jieba` for
  Chinese (mandatory), and disable positions and stored text unless phrases
  or direct stored-text access are needed.
- **Keep shards few.** Latency grows ~linearly with shard count and
  per-split BM25 statistics make the global ranking approximate; start at
  1 shard per partition and shard only as write parallelism requires.
- **Candidate count 100 is the sweet spot.** Recall@100 saturates there;
  more candidates cost latency and buy nothing on these corpora.
- **Use the scan API for latency.** SQL adds ~100 ms of planning and batch
  verification overhead per query; the HTTP gateway is a compatibility
  layer, not the fastest path.

## Reproducing

```sh
# 1. Datasets (streamed from the Hugging Face MTEB mirrors once)
uv run --with datasets python script/benchmark/text/download.py scifact \
    --out ~/data/lakesoul-text-bench/scifact
uv run --with datasets python script/benchmark/text/download.py msmarco \
    --out ~/data/lakesoul-text-bench/msmarco-100000 --limit 100000

# 2. Scenario runner (needs PostgreSQL metadata)
LAKESOUL_PG_URL=... script/benchmark/text/run.sh quick   # SciFact smoke (T1-T6)
LAKESOUL_PG_URL=... script/benchmark/text/run.sh all     # full matrix (T1-T7)

# 3. Figures
uv run --with matplotlib python script/benchmark/text/plot.py \
    --results benchmark-results/text-<timestamp>
```

Useful scenario switches: `--scenario build|search|stream|trigger|sql`,
`--limit`, `--query-limit`, `--tokenizer`, `--no-positions`, `--stored`,
`--shards`, `--candidates`, `--candidate-sweep`, `--policy`,
`--max-delta-ratio`, `--drift`, `--rounds`, `--per-round`, `--work-dir`
(reuse a built table with `search`) and `--out` (JSON results).

## See also

- [Text search in Python/Daft](11-lakesoul-python/08-text-search.md) for
  the API, the SQL surface and the ranking functions.
- [ES-compatible gateway](18-es-compatible-gateway.md) for the HTTP
  contract used by T7.
- [Vector index benchmark](17-vector-index-benchmark.md) for the analogous
  study of the ANN index (where stale candidates *do* cost recall).
