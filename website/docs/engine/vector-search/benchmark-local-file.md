---
sidebar_position: 6
sidebar_label: "Benchmark: local file (SSD + memory)"
---

# Vector Index Benchmark (Local File)

> This is the **Local File** (local filesystem) variant, covering build + index search.
> This benchmark covers **build + index search only**; it does not test upserts. The MinIO (three-tier cache) variant is [MinIO benchmark](/docs/engine/vector-search/benchmark-minio-large).

## 1. Summary

The four standard ANN datasets all reach **recall@100 ≥ 0.97** with RaBitQ `bits=7`, at the cost of a larger `nprobe`.
The main trade-offs are: `bits=7` vs `bits=1`, cache size (especially for exact re-rank), and interface.

| Dataset | Config | recall | index single QPS | index batch QPS |
|---|---|---:|---:|---:|
| SIFT-128 | nlist=256, bits=7, nprobe=64 | 0.970 | 379.4 | 4128.9 |
| GloVe-200 | nlist=1024, bits=7, nprobe=448 | 0.971 | 178.2 | 1294.3 |
| GIST-960 | nlist=256, bits=7, nprobe=32 | 0.974 | 157.7 | 1709.2 |
| Deep-Image-96 | nlist=1024, bits=7, nprobe=32 | 0.971 | 22.9 | 121.7 |

## 2. Method

| Item | Value |
|---|---|
| Storage | local file store (not MinIO/S3) |
| Index | IVF + RaBitQ, `hash_bucket_num=4` |
| top_k | 100 |
| recall | recall@100 against HDF5 ground truth |
| RSS | build/query process `VmRSS`; pgvector service RSS reported separately |
| Build | separate process, excludes write-phase peak |
| Cleanup | drop table and delete the local index/cache after each dataset |

| Dataset | Base | Queries | GT | dim | metric |
|---|---:|---:|---:|---:|---|
| SIFT-128 | 1,000,000 | 10,000 | 100-NN | 128 | L2 |
| GloVe-200 | 1,183,514 | 10,000 | 100-NN | 200 | cosine |
| GIST-960 | 1,000,000 | 1,000 | 100-NN | 960 | L2 |
| Deep-Image-96 | 9,990,000 | 10,000 | 100-NN | 96 | cosine |

`QPS / mean ms / RSS MB / recall` is the cell format in the cache and interface tables.
Single-query QPS is steady-state: the first query is timed separately; batch QPS is the average of two rounds.

**What NQ means**: NQ is the number of query vectors per measurement (the first
NQ test queries). Recall is the top-100 average over those queries and
`QPS = NQ / elapsed`. NQ is neither a concurrency setting nor a batch size.
This run uses NQ=10 for SIFT/GIST/Deep and NQ=20 for GloVe; larger NQ is more
stable but slower.


### Machine and measured resources

| Item | Value |
|---|---|
| CPU | Intel `General Purpose Processor` (KVM vCPU), 1 socket, 8 physical cores, 2 threads/core, **16 logical CPUs** |
| ISA | AVX2, AVX-512 (including AVX-512 VNNI/BF16) |
| Memory | **60 GiB**, no swap |
| cgroup limits | no CPU/memory limit |
| Test storage | MinIO runs as a local container; L2 cache is on local SSD |
| Concurrency | `hash_bucket_num=4`; native batch `parallelism=1`; pgvector `--worker-threads 4` |
| Measured build | SIFT MinIO: average **237% CPU ≈ 2.4 logical cores**, peak RSS **485 MB** |
| Measured query | SIFT MinIO index-only, NQ=10: average **349% CPU ≈ 3.5 logical cores**, peak RSS **397 MB** |

Notes:

- Build/query processes are not pinned to fixed CPUs; the core counts above are
  derived from `/usr/bin/time -v` average CPU percentages, not a cgroup quota.
- Build uses the `hash_bucket_num=4` shard layout; `CARGO_BUILD_JOBS=8` only
  affects compilation, not benchmark runtime CPU use.
- Single queries use roughly 3–4 logical cores. Native batch has one thread per
  bucket (4), plus merge threads (`min(NQ, available cores)`); the short batch
  averages about 3.5 cores.
- Build RSS is roughly 0.5–1.45 GB; query RSS is roughly 0.3–2.2 GB on this
  60 GiB machine.

## 3. Build

| Dataset | nlist | bits | build s | build RSS MB | write s |
|---|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 1 | 3.6 | 481 | 3.0 |
| SIFT-128 | 256 | 7 | 4.1 | 542 | 3.0 |
| SIFT-128 | 1024 | 1 | 6.2 | 325 | 3.0 |
| SIFT-128 | 1024 | 7 | 7.2 | 385 | 3.0 |
| GloVe-200 | 256 | 1 | 8.3 | 703 | 7.8 |
| GloVe-200 | 256 | 7 | 9.3 | 767 | 7.8 |
| GloVe-200 | 1024 | 1 | 13.6 | 685 | 7.8 |
| GloVe-200 | 1024 | 7 | 14.0 | 763 | 7.8 |
| GloVe-200 | 2048 | 7 | 24.3 | 770 | 7.8 |
| GIST-960 | 256 | 1 | 34.1 | 791 | 30.1 |
| GIST-960 | 256 | 7 | 37.1 | 1077 | 30.1 |
| GIST-960 | 1024 | 1 | 51.2 | 638 | 30.1 |
| GIST-960 | 1024 | 7 | 54.2 | 804 | 30.1 |
| Deep-Image-96 | 1024 | 1 | 93.9 | 1207 | 31.4 |
| Deep-Image-96 | 1024 | 7 | 97.1 | 1442 | 31.4 |
| Deep-Image-96 | 4096 | 1 | 136.4 | 1038 | 31.4 |
| Deep-Image-96 | 4096 | 7 | 138.5 | 1207 | 31.4 |

Build RSS is the build process only. GloVe/Deep use row-normalized vectors; SIFT/GIST use raw L2.

**What `build s` includes**: timing starts at `build_table_index` and covers
reading the table, KMeans/quantisation, index segment writes and the PG index
commit/lease. It **does not include** the preceding `write_arrow` table write;
that is the separate `write s` column.

## 4. Retrieval quality and throughput

`bits=1` is compact but cannot reach 0.97 recall at these nprobe values.
`bits=7` reaches 0.97 with a larger nprobe. Full sweeps are in Appendix A.

### Target configurations

| Dataset | Config | recall | index single | index batch | exact single | exact batch |
|---|---|---:|---:|---:|---:|---:|
| SIFT-128 | nlist=256, bits=7, nprobe=64 | 0.970 | 379.4 | 4128.9 | 50.1 | 2307.1 |
| GloVe-200 | nlist=1024, bits=7, nprobe=448 | 0.971 | 178.2 | 1294.3 | 23.6 | 915.3 |
| GIST-960 | nlist=256, bits=7, nprobe=32 | 0.974 | 157.7 | 1709.2 | 13.4 | 1019.1 |
| Deep-Image-96 | nlist=1024, bits=7, nprobe=32 | 0.971 | 22.9 | 121.7 | 8.6 | 138.9 |

SIFT: `nlist=256,bits=7,nprobe=64`; GloVe: `nlist=1024,bits=7,nprobe=448`;
GIST: `nlist=256,bits=7,nprobe=32`; Deep: `nlist=1024,bits=7,nprobe=32`.
All configurations scan nprobe upward from 8; GloVe first reaches 0.97 at nprobe=448.

## 5. Cache size

Format: **single QPS / mean ms / RSS MB / recall**; batch uses the same format.
All four use the target config above.

### SIFT-128

| cache | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| 512m | 332.1 / 3.01 / 381 / 0.970 | 4201.3 / 0.24 / 383 / 0.970 | 43.0 / 23.24 / 731 / 1.000 | 148.3 / 6.74 / 1059 / 1.000 |
| 1g | 326.4 / 3.06 / 381 / 0.970 | 4232.3 / 0.24 / 383 / 0.970 | 46.4 / 21.57 / 818 / 1.000 | 223.1 / 4.48 / 1188 / 1.000 |
| 8g | 379.4 / 2.64 / 380 / 0.970 | 4128.9 / 0.24 / 382 / 0.970 | 50.1 / 19.98 / 887 / 1.000 | 2307.1 / 0.43 / 890 / 1.000 |
| off | 5.4 / 185.91 / 381 / 0.970 | 106.6 / 9.38 / 383 / 0.970 | — | — |

### GloVe-200

| cache | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| 512m | 87.5 / 11.43 / 651 / 0.971 | 1304.9 / 0.77 / 653 / 0.971 | 15.1 / 66.28 / 1019 / 0.988 | 28.0 / 35.72 / 1348 / 0.988 |
| 1g | 156.6 / 6.39 / 558 / 0.971 | 1276.7 / 0.78 / 559 / 0.971 | 22.7 / 44.14 / 1100 / 0.988 | 139.1 / 7.19 / 1429 / 0.988 |
| 8g | 178.2 / 5.61 / 558 / 0.971 | 1294.3 / 0.77 / 559 / 0.971 | 23.6 / 42.32 / 1424 / 0.988 | 915.3 / 1.09 / 1427 / 0.988 |

### GIST-960

| cache | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| 512m | 75.5 / 13.25 / 1117 / 0.974 | 1732.8 / 0.58 / 1118 / 0.974 | 5.2 / 190.49 / 1686 / 0.990 | 13.7 / 73.12 / 1950 / 0.990 |
| 1g | 160.3 / 6.24 / 1115 / 0.974 | 1762.8 / 0.57 / 1117 / 0.974 | 6.7 / 148.45 / 1890 / 0.990 | 58.9 / 16.97 / 2154 / 0.990 |
| 8g | 157.7 / 6.34 / 1113 / 0.974 | 1709.2 / 0.59 / 1115 / 0.974 | 13.4 / 74.68 / 1990 / 0.990 | 1019.1 / 0.98 / 1993 / 0.990 |
| off | 3.1 / 327.81 / 1128 / 0.974 | 31.2 / 32.04 / 1132 / 0.974 | — | — |

### Deep-Image-96

| cache | index single | index batch | exact single | exact batch |
|---|---|---|---|---|
| 512m | 20.9 / 47.77 / 967 / 0.971 | 22.8 / 43.85 / 1722 / 0.971 | 8.5 / 118.06 / 1300 / 0.989 | 7.1 / 141.38 / 1991 / 0.989 |
| 1g | 21.0 / 47.56 / 1064 / 0.971 | 127.8 / 7.82 / 1095 / 0.971 | 9.0 / 111.68 / 1556 / 0.989 | 63.3 / 15.79 / 1858 / 0.989 |
| 8g | 22.9 / 43.72 / 1042 / 0.971 | 121.7 / 8.22 / 1052 / 0.971 | 8.6 / 115.94 / 1870 / 0.989 | 138.9 / 7.20 / 1987 / 0.989 |
| off | 21.9 / 45.57 / 540 / 0.971 | 29.9 / 33.45 / 623 / 0.971 | — | — |

Observations:

- Index-only is not very sensitive to cache size, except that 512 MiB can add latency.
- Exact re-rank is dominated by the block cache: 512 MiB -> 1 GiB -> 8 GiB gives large batch gains (SIFT 148 -> 223 -> 2307 QPS; GIST 14 -> 59 -> 1019; Deep 7.1 -> 63 -> 139).
- Without any cache, index-only still works and exact single-query falls back.
  Native exact batch also falls back to the normal reader now: SIFT nprobe=64, NQ=5
  gives about 15.7 QPS at recall 1.0 (temporary key-index path in `pk_locator`).

## 6. Interfaces

Target configuration, cache 8 GiB; format **single QPS / mean ms / RSS MB / recall**.
DataFusion batch means a loop of SQL queries; Python batch is the native batch entry.

### SIFT-128

| Interface | rerank | single | batch | server RSS MB |
|---|---|---|---|---:|
| python | index | 379.4 / 2.64 / 380 / 0.970 | 4128.9 / 0.24 / 382 / 0.970 | — |
| python | exact | 50.1 / 19.98 / 887 / 1.000 | 2307.1 / 0.43 / 890 / 1.000 | — |
| datafusion | index | 450.7 / 2.22 / 381 / 0.970 | 478.6 / 2.09 / 381 / 0.970 | — |
| datafusion | exact | 49.9 / 20.06 / 874 / 1.000 | 396.8 / 2.52 / 874 / 1.000 | — |
| pgvector | index | 390.6 / 2.56 / 64 / 0.970 | 440.2 / 2.27 / 65 / 0.970 | 304 |
| pgvector | exact | 100.0 / 10.00 / 64 / 1.000 | 189.6 / 5.27 / 65 / 1.000 | 853 |

### GloVe-200

| Interface | rerank | single | batch | server RSS MB |
|---|---|---|---|---:|
| python | index | 178.2 / 5.61 / 558 / 0.971 | 1294.3 / 0.77 / 559 / 0.971 | — |
| python | exact | 23.6 / 42.32 / 1424 / 0.988 | 915.3 / 1.09 / 1427 / 0.988 | — |
| datafusion | index | 187.8 / 5.32 / 557 / 0.971 | 214.3 / 4.67 / 557 / 0.971 | — |
| datafusion | exact | 24.9 / 40.14 / 1416 / 0.988 | 203.2 / 4.92 / 1416 / 0.988 | — |
| pgvector | index | 196.0 / 5.10 / 68 / 0.971 | 220.1 / 4.54 / 68 / 0.971 | 476 |
| pgvector | exact | 44.0 / 22.74 / 68 / 0.988 | 99.7 / 10.03 / 68 / 0.988 | 1463 |

### GIST-960

| Interface | rerank | single | batch | server RSS MB |
|---|---|---|---|---:|
| python | index | 157.7 / 6.34 / 1113 / 0.974 | 1709.2 / 0.59 / 1115 / 0.974 | — |
| python | exact | 13.4 / 74.68 / 1990 / 0.990 | 1019.1 / 0.98 / 1993 / 0.990 | — |
| datafusion | index | 170.9 / 5.85 / 1114 / 0.974 | 182.8 / 5.47 / 1115 / 0.974 | — |
| datafusion | exact | 13.6 / 73.80 / 1990 / 0.990 | 159.9 / 6.26 / 1992 / 0.990 | — |
| pgvector | index | 190.5 / 5.25 / 60 / 0.974 | 217.6 / 4.60 / 60 / 0.974 | 1046 |
| pgvector | exact | 4.1 / 243.59 / 60 / 0.990 | 33.6 / 29.73 / 60 / 0.990 | 3949 |

### Deep-Image-96

| Interface | rerank | single | batch | server RSS MB |
|---|---|---|---|---:|
| python | index | 22.9 / 43.72 / 1042 / 0.971 | 121.7 / 8.22 / 1052 / 0.971 | — |
| python | exact | 8.6 / 115.94 / 1870 / 0.989 | 138.9 / 7.20 / 1987 / 0.989 | — |
| datafusion | index | 21.7 / 46.04 / 1053 / 0.971 | 96.6 / 10.36 / 1125 / 0.971 | — |
| datafusion | exact | 8.4 / 119.75 / 1890 / 0.989 | 88.8 / 11.26 / 2200 / 0.989 | — |
| pgvector | index | 22.3 / 44.81 / 63 / 0.971 | 94.0 / 10.64 / 63 / 0.971 | 902 |
| pgvector | exact | 4.2 / 238.81 / 63 / 0.989 | 11.9 / 84.30 / 63 / 0.989 | 3825 |

pgvector is a separate service process (`postgres-lakesoul`, 4 worker threads).
Python and DataFusion are in-process. pgvector exact re-rank needs much more service RSS,
reaching about 3.9 GB for GIST and 3.8 GB for Deep.

## 7. Parallelism and bucket count

SIFT, target config, Python native, cache 8 GiB, NQ=20:

| parallelism | index single QPS | index batch QPS |
|---:|---:|---:|
| 1 | 378.6 | 4301.7 |
| 2 | 325.4 | 2643.1 |
| 4 | 379.4 | 3190.7 |

The native batch path already probes buckets in parallel; extra `parallelism` oversubscribes
the short batch. Keep `hash_bucket_num × vector_search.parallelism` near the CPU count.

## 8. Recommendations

| Goal | Recommendation |
|---|---|
| best recall / QPS balance | bits=7, nprobe at the 0.97 knee; see target table |
| minimum memory | 512 MiB cache: index-only works, exact is slow |
| exact re-rank | 8 GiB L1/L2 cache; 512 MiB is not enough for batch |
| Python native | use `batch_search`; set `parallelism=1` for short batches |
| SQL | DataFusion single-query is close to native; batch through SQL is slower than native batch |
| pgvector | convenient service interface, but exact is expensive and needs several GB RSS |

## 9. Limitations

- Local file store, not S3/MinIO; absolute QPS differs from remote-storage runs.
- NQ is 10 for most result tables (SIFT parameter sweep uses 20); QPS is steady-state.
- The GloVe target config is nlist=1024,bits=7,nprobe=448; scanning nprobe upward from 8,
  nprobe=448 is the first value reaching recall>=0.97 (NQ=20, recall 0.9715).
- pgvector cache sizes were measured only on the GloVe target config (512 MiB/1 GiB/8 GiB);
  other pgvector interface rows use 8 GiB.
- Reported `index_bytes` is omitted because old index generations were still on disk
  while building multiple configurations; build time/RSS are not affected.
- The English and Chinese pages share this data set; regenerate both from the raw JSONL if needed.

## 10. Raw results

- [raw/sift_query.jsonl](https://github.com/lakesoul-io/LakeSoul/blob/main/rust/lakesoul-vector/ann-benchmarks/vector-index-2026-11-11/raw/sift_query.jsonl)
- [raw/glove.jsonl](https://github.com/lakesoul-io/LakeSoul/blob/main/rust/lakesoul-vector/ann-benchmarks/vector-index-2026-11-11/raw/glove.jsonl)
- [raw/glove_target.jsonl](https://github.com/lakesoul-io/LakeSoul/blob/main/rust/lakesoul-vector/ann-benchmarks/vector-index-2026-11-11/raw/glove_target.jsonl)
- [raw/glove_target_fix.jsonl](https://github.com/lakesoul-io/LakeSoul/blob/main/rust/lakesoul-vector/ann-benchmarks/vector-index-2026-11-11/raw/glove_target_fix.jsonl)
- [raw/gist.jsonl](https://github.com/lakesoul-io/LakeSoul/blob/main/rust/lakesoul-vector/ann-benchmarks/vector-index-2026-11-11/raw/gist.jsonl)
- [raw/deep.jsonl](https://github.com/lakesoul-io/LakeSoul/blob/main/rust/lakesoul-vector/ann-benchmarks/vector-index-2026-11-11/raw/deep.jsonl)

## Appendix A: full index-only parameter sweep

Cache 8 GiB, Python native, `nprobe` sweep. Values are QPS / recall.

### SIFT-128

| nlist | bits | nprobe | recall | single QPS | batch QPS |
|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 8 | 0.517 | 506.8 | 7999.1 |
| 256 | 1 | 16 | 0.531 | 488.0 | 7457.1 |
| 256 | 1 | 32 | 0.524 | 475.8 | 6616.4 |
| 256 | 1 | 64 | 0.524 | 445.3 | 5573.8 |
| 256 | 1 | 128 | 0.524 | 434.6 | 4498.9 |
| 256 | 7 | 8 | 0.862 | 457.2 | 6445.2 |
| 256 | 7 | 16 | 0.949 | 409.8 | 5508.6 |
| 256 | 7 | 32 | 0.967 | 395.1 | 4849.1 |
| 256 | 7 | 64 | 0.970 | 391.1 | 4214.7 |
| 256 | 7 | 128 | 0.970 | 359.9 | 3308.2 |
| 1024 | 1 | 8 | 0.495 | 508.0 | 7490.5 |
| 1024 | 1 | 16 | 0.529 | 509.0 | 7217.4 |
| 1024 | 1 | 32 | 0.553 | 438.6 | 5931.0 |
| 1024 | 1 | 64 | 0.551 | 437.1 | 5993.7 |
| 1024 | 1 | 128 | 0.549 | 441.8 | 5520.5 |
| 1024 | 7 | 8 | 0.675 | 467.9 | 6185.5 |
| 1024 | 7 | 16 | 0.826 | 430.1 | 6102.4 |
| 1024 | 7 | 32 | 0.932 | 430.2 | 5361.2 |
| 1024 | 7 | 64 | 0.964 | 379.3 | 4702.7 |
| 1024 | 7 | 128 | 0.972 | 355.7 | 4203.0 |

### GloVe-200

| nlist | bits | nprobe | recall | single QPS | batch QPS |
|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 8 | 0.404 | 411.4 | 6585.8 |
| 256 | 1 | 16 | 0.442 | 389.8 | 5870.8 |
| 256 | 1 | 32 | 0.451 | 375.8 | 5000.8 |
| 256 | 1 | 64 | 0.455 | 335.9 | 3908.0 |
| 256 | 1 | 128 | 0.452 | 292.5 | 2868.2 |
| 256 | 1 | 256 | 0.448 | 245.1 | 1995.2 |
| 256 | 7 | 8 | 0.691 | 343.0 | 4220.1 |
| 256 | 7 | 16 | 0.799 | 316.3 | 3258.1 |
| 256 | 7 | 32 | 0.878 | 267.2 | 2478.8 |
| 256 | 7 | 64 | 0.940 | 229.5 | 1827.7 |
| 256 | 7 | 128 | 0.969 | 185.3 | 1337.1 |
| 256 | 7 | 256 | 0.975 | 146.2 | 1016.5 |
| 1024 | 1 | 8 | 0.425 | 391.3 | 5869.6 |
| 1024 | 1 | 16 | 0.444 | 389.4 | 5658.9 |
| 1024 | 1 | 32 | 0.461 | 380.3 | 5164.5 |
| 1024 | 1 | 64 | 0.468 | 371.3 | 4678.9 |
| 1024 | 1 | 128 | 0.466 | 334.5 | 3903.0 |
| 1024 | 1 | 256 | 0.463 | 311.6 | 3022.2 |
| 1024 | 7 | 8 | 0.634 | 334.1 | 5011.2 |
| 1024 | 7 | 16 | 0.709 | 338.5 | 4309.6 |
| 1024 | 7 | 32 | 0.802 | 290.7 | 2586.0 |
| 1024 | 7 | 64 | 0.876 | 283.6 | 2760.0 |
| 1024 | 7 | 128 | 0.926 | 239.8 | 2142.5 |
| 1024 | 7 | 256 | 0.957 | 192.2 | 1550.4 |
| 2048 | 7 | 256 | 0.927 | 228.8 | 1868.7 |
| 2048 | 7 | 512 | 0.959 | 187.5 | 1358.0 |
| 2048 | 7 | 1024 | 0.973 | 152.9 | 1049.6 |

### GIST-960

| nlist | bits | nprobe | recall | single QPS | batch QPS |
|---:|---:|---:|---:|---:|---:|
| 1024 | 7 | 64 | 0.966 | 146.4 | 1747.5 |
| 1024 | 7 | 128 | 0.980 | 144.3 | 1475.1 |
| 1024 | 7 | 256 | 0.981 | 120.8 | 1095.8 |
| 1024 | 7 | 512 | 0.981 | 108.2 | 801.3 |
| 1024 | 7 | 1024 | 0.981 | 91.2 | 587.9 |
| 256 | 1 | 8 | 0.681 | 225.3 | 3055.8 |
| 256 | 1 | 16 | 0.721 | 213.5 | 2729.8 |
| 256 | 1 | 32 | 0.740 | 192.8 | 2170.9 |
| 256 | 1 | 64 | 0.742 | 176.0 | 1604.4 |
| 256 | 1 | 128 | 0.744 | 141.3 | 1137.9 |
| 256 | 7 | 8 | 0.827 | 184.3 | 2511.4 |
| 256 | 7 | 16 | 0.932 | 169.9 | 2039.0 |
| 256 | 7 | 32 | 0.974 | 163.5 | 1788.7 |
| 256 | 7 | 64 | 0.980 | 136.3 | 1342.3 |
| 256 | 7 | 128 | 0.981 | 122.6 | 1007.9 |
| 1024 | 1 | 8 | 0.588 | 210.3 | 2688.7 |
| 1024 | 1 | 16 | 0.681 | 188.8 | 2583.1 |
| 1024 | 1 | 32 | 0.735 | 193.9 | 2434.3 |
| 1024 | 1 | 64 | 0.758 | 188.5 | 2096.3 |
| 1024 | 1 | 128 | 0.764 | 168.3 | 1692.0 |
| 1024 | 7 | 8 | 0.635 | 164.0 | 2354.8 |
| 1024 | 7 | 16 | 0.790 | 166.6 | 2236.9 |
| 1024 | 7 | 32 | 0.905 | 152.6 | 1493.4 |

### Deep-Image-96

| nlist | bits | nprobe | recall | single QPS | batch QPS |
|---:|---:|---:|---:|---:|---:|
| 1024 | 1 | 8 | 0.522 | 113.3 | 1104.6 |
| 1024 | 1 | 16 | 0.527 | 67.3 | 587.4 |
| 1024 | 1 | 32 | 0.530 | 36.7 | 303.2 |
| 1024 | 1 | 64 | 0.529 | 21.6 | 179.3 |
| 1024 | 7 | 8 | 0.904 | 66.8 | 437.3 |
| 1024 | 7 | 16 | 0.943 | 34.9 | 225.3 |
| 1024 | 7 | 32 | 0.971 | 21.4 | 115.8 |
| 1024 | 7 | 64 | 0.975 | 13.2 | 65.3 |
| 4096 | 1 | 8 | 0.514 | 72.0 | 1669.1 |
| 4096 | 1 | 16 | 0.538 | 40.1 | 1268.6 |
| 4096 | 1 | 32 | 0.550 | 23.2 | 860.0 |
| 4096 | 1 | 64 | 0.552 | 12.5 | 534.7 |
| 4096 | 7 | 8 | 0.809 | 65.5 | 1041.4 |
| 4096 | 7 | 16 | 0.898 | 36.3 | 682.9 |
| 4096 | 7 | 32 | 0.952 | 20.3 | 347.3 |
| 4096 | 7 | 64 | 0.967 | 11.2 | 190.1 |
