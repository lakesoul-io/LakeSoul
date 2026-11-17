---
sidebar_position: 4
---

# Vector index benchmarks

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

This page summarises what the LakeSoul vector index achieves on the standard
ANN datasets and what the rebuild policy costs. Two benchmark families are
reported:

- **Build + search at scale** — four datasets, three query interfaces, MinIO
  object storage and a small cache budget (this section).
- **Rebuild policy on drift** — recall and rebuild cost as data grows and
  shifts (the E1/E2 section at the end).

Both are measured with the same production index implementation; all search
paths exercise the real IVF + RaBitQ index, the native reader and the
merge-on-read data path.

## Method summary

| Item | Value |
| --- | --- |
| Storage | MinIO (S3-compatible), local container |
| Machine | 16 vCPU (8 physical cores × 2 threads), 60 GiB RAM, AVX-512, not CPU-pinned |
| Cache | L1 512 MiB, L2 1 GiB (each configuration starts a new process) |
| Index | IVF + RaBitQ, `bits=7`, 4 hash buckets |
| Search | `top_k=100`, recall@100 against exact ground truth |
| Datasets | SIFT-128 (1M × 128), GloVe-200 (1,183,514 × 200), GIST-960 (1M × 960), Deep-Image-96 (9.99M × 96) |
| Target parameters | SIFT: `nlist=256, nprobe=64`; GloVe: `nlist=1024, nprobe=512` |
| Single query | 200 real query vectors, including the first (cold) query |
| Batch | one native batch over all query vectors: 10,000 for SIFT/GloVe/Deep, 1,000 for GIST |
| Interfaces | Python native, DataFusion SQL, LakeSoul pgwire |
| Exact re-rank | `exact_overfetch=1` (candidates re-ranked against the stored vectors) |

Index-only rows order candidates by the quantized index distance and never read
the vector column. Exact rows fetch the candidate vectors and re-rank them.

## Final measurement (Python native, quiet window)

Single-query rows report the first query latency separately from the p50 of the
steady-state queries; batch QPS is the full native batch wall time.

| Dataset | Order | nprobe | Single QPS | First query | p50 | Batch QPS | Recall@100 |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| SIFT-128 | index | 64 | 204.0 | 320 ms | 3.30 ms | 4,382.6 | 0.9747 |
| SIFT-128 | exact | 64 | 56.3 | 662 ms | 10.49 ms | 1,403.8 | 0.9997–0.9999 |
| GloVe-200 | index | 512 | 54.4 | 644 ms | 7.21 ms | 1,195.4 | 0.9718 |
| GloVe-200 | exact | 512 | 20.5 | 1,065 ms | 17.44 ms | 658.3 | 0.9917–0.9919 |

## Published range across the three interfaces

The same configurations measured through Python native, DataFusion SQL and
pgwire. Ranges are min–max over the interfaces.

| Dataset | Single index QPS | Single exact QPS | Batch index QPS | Batch exact QPS | Recall@100 (index / exact) |
| --- | ---: | ---: | ---: | ---: | ---: |
| SIFT-128 | 206–242 | 58.4–60.5 | 3,617–4,759 | 1,336–1,479 | 0.9747 / 0.9997–0.9999 |
| GloVe-200 | 56.0–56.8 | 19.6–21.4 | 1,173–1,227 | 658.3–682.0 | 0.9712 / 0.9917–0.9919 |
| GIST-960 | 9.9–13.0 | 2.4–2.5 | 1,005–1,179 | 35.1–35.3 | 0.9802 / 0.9980 |
| Deep-Image-96 | 17.5–17.6 | 4.5–4.6 | 107–110 | 25.2–25.5 | 0.9503 / 0.9707 |

## What the numbers say

- **Recall is stable at the default budget.** With `bits=7`, `recall@100` is
  0.97–0.98 for index-only search on all four datasets at the target `nprobe`;
  exact re-rank lifts SIFT to 0.9997+ and GloVe to 0.9917+.
- **Index-only is the throughput mode.** Exact re-rank reads the candidate
  vectors and costs 2.7–3.6× single-query throughput (SIFT 204 → 56 QPS,
  GloVe 54 → 20 QPS) for the last fraction of a recall point.
- **Native batch is an order of magnitude faster than single queries.**
  One batch reaches 4,382 QPS (SIFT index), 1,404 QPS (SIFT exact), 1,195 QPS
  (GloVe index) and 658 QPS (GloVe exact); the batch path shares one snapshot,
  one index load and one ranking pass across all queries.
- **Dimension and scale dominate latency.** GIST-960d and Deep-Image-96 (9.99M
  rows) reach 9.9–13.0 and 17.5–17.6 single index QPS; their exact batch paths
  are still 35.1–35.3 and 25.2–25.5 QPS.
- **SQL tracks native single-query performance; batch through SQL is slower
  than native batch.** DataFusion and pgwire single queries land in the same
  range as the Python native path (the SQL paths avoid per-row Python
  conversion, so they are often slightly faster), while the native batch entry
  remains the highest-throughput path.
- **L2 is the limit for exact re-rank.** The 512 MiB / 1 GiB configuration is
  sufficient for index-only search; exact batch reads many candidate blocks
  and improves with a larger L2 (see the detailed pages below).

### Detailed measurements

- [Four-dataset benchmark on local files (SIFT, GloVe, GIST, Deep-Image;
  Python, DataFusion, pgvector)](/docs/engine/vector-search/benchmark-local-file)
- [Large-scale MinIO benchmark, L1 8 GiB / L2 16 GiB](/docs/engine/vector-search/benchmark-minio-large)
- [Large-scale MinIO benchmark, L1 512 MiB / L2 1 GiB](/docs/engine/vector-search/benchmark-minio-large-512m1g)

These pages contain the per-dataset build times, cache-size sweeps, latency
percentiles and interface comparisons behind the tables above.

## Rebuild policy benchmark (E1/E2)

The large-scale numbers above measure build and search. The second benchmark
family measures what happens while data keeps arriving: how recall evolves as
deltas accumulate along different distributions, and when a rebuild pays off.

Environment for these experiments: 32 CPU cores, 62 GB RAM, local NVMe, data
written as vortex files. Index `nlist=256`, `bits=7`, `top_k=10`,
`nprobe=64`, 100 checkpoint queries, fixed seed; 100,000 base vectors plus 10
rounds of 10,000 updates (100% growth in 10 steps). Recall@10 is measured
against brute force over the vectors currently in the index (base + deltas),
with checkpoint queries following the same drift as the updates.

### E1 — rebuild policy vs update distribution

Policies: `none` (delta segments only), `auto` (rebuild a shard when any cluster
satisfies `delta_vectors / base_vectors > max_delta_ratio`, default `1.0`),
`periodic` (rebuild every 3 rounds) and `always` (rebuild every round).

GIST1M under distribution **shift**:

| Policy | Min recall@10 | Final recall@10 | Rebuilds | Index update time |
| --- | ---: | ---: | ---: | ---: |
| `none` | 0.769 | 0.769 | 0 | 3.6 s |
| `auto@1.0` | **0.929** | **0.938** | 3 | 10.2 s |
| `periodic` (every 3) | 0.786 | 0.933 | 3 | 10.7 s |
| `always` | 0.927 | 0.938 | 5 | 15.5 s |

Minimum recall@10 across drift modes (key policies):

| Dataset | Drift | `none` | `auto@1.0` | `periodic` | `always` |
| --- | --- | ---: | ---: | ---: | ---: |
| GIST1M | uniform | 0.966 | 0.966 | 0.962 | 0.969 |
| GIST1M | skew | 0.903 | 0.897 | 0.902 | 0.897 |
| GIST1M | shift | 0.769 | **0.929** | 0.786 | 0.927 |
| GloVe-200d | uniform | 0.880 | 0.880 | 0.881 | 0.881 |
| GloVe-200d | skew | 0.741 | 0.604 | 0.708 | 0.740 |
| GloVe-200d | shift | 0.868 | 0.890 | 0.890 | 0.890 |

Findings:

- **Uniform growth:** incremental deltas keep recall (0.966 GIST, 0.880 GloVe)
  and rebuilding buys nothing — the `auto` policy stays at zero rebuilds for
  `max_delta_ratio ≥ 1`.
- **Skewed growth:** updates concentrate in an already-covered region; retraining
  adds cost without a recall benefit and can even lose a little recall to
  run-to-run variance.
- **Distribution shift:** incremental-only maintenance loses about 16 recall
  points on GIST (0.769 vs 0.938). The per-cluster `auto` policy recovers
  always-rebuild quality with 3 rebuilds and about two thirds of the time
  (10.2 s vs 15.5 s), because the first rebuild fires as soon as one cluster's
  deltas exceed its base.
- Drift is detected per cluster, not per shard, so a real shift is caught in the
  first rounds while healthy data does not trigger spurious rebuilds.

### E2 — fresh build scaling

Fresh index build on a subset of each dataset; time is the `build()` wall time,
peak RSS is the process high-water mark, index size is the on-disk total.

| Dataset | Vectors | nlist | Build time | Peak RSS | Index size |
| --- | ---: | ---: | ---: | ---: | ---: |
| GIST1M (960d) | 100,000 | 256 | 2.3 s | 3.3 GB | 86 MB |
| GIST1M (960d) | 300,000 | 256 | 4.6 s | 5.7 GB | 254 MB |
| GIST1M (960d) | 1,000,000 | 256 | 18.2 s | 13.4 GB | 845 MB |
| GIST1M (960d) | 1,000,000 | 1024 | 60.8 s | 14.1 GB | 849 MB |
| GloVe-200d | 100,000 | 256 | 0.7 s | 0.4 GB | 26 MB |
| GloVe-200d | 1,000,000 | 256 | 4.2 s | 2.6 GB | 256 MB |
| GloVe-200d | 1,000,000 | 1024 | 8.8 s | 2.7 GB | 257 MB |
| GloVe-200d | 1,000,000 | 4096 | 36.0 s | 2.7 GB | 263 MB |

Findings:

- Build time scales roughly linearly with the vector count and with `nlist`
  (k-means iterations dominate: GIST 1M goes from 18 s at `nlist=256` to 61 s
  at `nlist=1024`). Index size is dominated by the quantized codes.
- Peak memory is dominated by the raw vectors being processed; 1M × 960-d needs
  roughly 13–14 GB. Plan a rebuild of a very large shard as a background job
  with that budget.

### Recommendations

1. Keep `rebuild_mode="auto"` with `max_delta_ratio=1.0` for general workloads:
   it does not rebuild under uniform growth, detects real distribution shifts in
   the first rounds, and reaches always-rebuild quality at roughly two thirds of
   the cost.
2. For strong, continuous drift, lower `max_delta_ratio` (for example `0.5`) or
   call `table.rebuild_vector_index()` during off-peak windows.
3. For uniform or mildly skewed ingestion, `rebuild_mode="none"` is sufficient
   and avoids rebuild cost entirely.
4. Budget rebuilds from the E2 table (GIST 1M: about 18 s and 14 GB peak at
   `nlist=256`) and run them in the background.

## Next

- [Configuration](configuration.md) — index parameters, cache tiers and
  environment variables.
- [API reference](api.md) — the Python, SQL and pgwire surface.
