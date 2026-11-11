# Vector Index Benchmark (MinIO / Three-Tier Cache)

> Fresh MinIO benchmark, November 2026. Build and search only; upserts are not tested.
> The local-filesystem variant is kept separately in [17-vector-index-benchmark-local-file.md](17-vector-index-benchmark-local-file.md).

## 1. Summary

LakeSoul's three-tier cache is exercised here with the table data and index in MinIO,
the local disk cache in `/tmp`, and the in-memory index/block caches in the query process.

| Dataset | Config | recall | index single | index batch | exact single | exact batch |
|---|---|---:|---:|---:|---:|---:|
| SIFT-128 | nlist=256, bits=7, nprobe=64 | 0.975 | 363.9 | 2757.8 | 14.6 | 1793.8 |
| GloVe-200 | nlist=1024, bits=7, nprobe=448 | 0.971 | 161.2 | 1235.3 | 11.0 | 916.3 |
| GIST-960 | nlist=256, bits=7, nprobe=32 | 0.974 | 164.0 | 1731.5 | 6.2 | 1027.2 |
| Deep-Image-96 | nlist=1024, bits=7, nprobe=32 | 0.971 | 15.9 | 119.0 | 4.3 | 141.1 |

The target column is the **smallest nprobe found by scanning upward from 8**
that reached recall@100 >= 0.97 for the chosen nlist, not the maximum nprobe.

## 2. Three-tier cache

| Tier | Location | Contents |
|---|---|---|
| L1 | process memory | whole index/cluster cache, decoded block/snapshot caches |
| L2 | local SSD directory (`LAKESOUL_CACHE_PATH`) | index segments, decoded vector blocks, snapshots |
| L3 | MinIO (`s3://lakesoul-bench2`) | table data files and index segments |

- `LAKESOUL_CACHE_SIZE=0` disables L2; `LAKESOUL_VECTOR_MEMORY_BYTES=0` disables the L1 budget.
- L1 only still benefits index-only because the index is re-read from MinIO on first use and then kept in memory.
- L2 only does not help index-only: the index cache is in L1. It does help exact re-rank because decoded vector blocks are cached on disk.
- L1+L2 is the normal production configuration.
- First-query latency includes the MinIO read/promotion; steady-state QPS is measured after it.


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

## 3. Method

| Item | Value |
|---|---|
| Storage | MinIO `lakesoul-bench2`, endpoint `http://127.0.0.1:9000` |
| Index | IVF + RaBitQ, `hash_bucket_num=4` |
| top_k | 100 |
| recall | HDF5 ground truth, recall@100 |
| Cache matrix | off, L1-only, L2-only, 512 MiB, 1 GiB, 8 GiB |
| Cleanup | drop table and delete MinIO prefix + local cache after each dataset |

| Dataset | Base | Queries | dim | metric |
|---|---:|---:|---:|---|
| SIFT-128 | 1,000,000 | 10,000 | 128 | L2 |
| GloVe-200 | 1,183,514 | 10,000 | 200 | cosine |
| GIST-960 | 1,000,000 | 1,000 | 960 | L2 |
| Deep-Image-96 | 9,990,000 | 10,000 | 96 | cosine |

## 4. Build

| Dataset | nlist | bits | build s | build RSS MB | write s |
|---|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 15.5 | 515 | 3.1 |
| GloVe-200 | 1024 | 7 | 55.5 | 804 | 12.9 |
| GIST-960 | 256 | 7 | 53.5 | 1040 | 42.3 |
| Deep-Image-96 | 1024 | 7 | 153.6 | 1447 | 56.1 |

Build reads/writes go through MinIO; compare with the local-file page for the storage effect.

## 5. Cache results

Cells are `QPS / first ms / RSS MB / recall`. `first ms` is the first query (cold);
steady-state QPS follows.

### SIFT-128

**Index only**

| cache | L1 / L2 | single (QPS / first ms / RSS MB / recall) | batch (QPS / first ms / RSS MB / recall) |
|---|---|---|---|
| off |  | 4.0 / 340 / 383 / 0.975 | 39.0 / 256 / 384 / 0.975 |
| L1 only | 8GiB / L2 0 | 339.3 / 313 / 387 / 0.975 | 3002.7 / 4 / 389 / 0.975 |
| L2 only | L1 0 / 16GiB | 3.9 / 338 / 384 / 0.975 | 39.4 / 252 / 386 / 0.975 |
| L1+L2 512MiB | 512MiB / 512MiB | 300.3 / 311 / 386 / 0.975 | 3082.3 / 4 / 388 / 0.975 |
| L1+L2 1GiB | 1GiB / 1GiB | 278.7 / 987 / 384 / 0.975 | 3171.7 / 4 / 386 / 0.975 |
| L1+L2 8GiB | 8GiB / 16GiB | 363.9 / 318 / 384 / 0.975 | 2757.8 / 4 / 387 / 0.975 |

**Exact re-rank**

| cache | L1 / L2 | single (QPS / first ms / RSS MB / recall) | batch (QPS / first ms / RSS MB / recall) |
|---|---|---|---|
| off |  | 2.8 / 427 / 582 / 1.000 | — |
| L1 only | 8GiB / L2 0 | 4.0 / 399 / 562 / 1.000 | — |
| L2 only | L1 0 / 16GiB | 3.1 / 620 / 447 / 1.000 | 29.2 / 334 / 453 / 1.000 |
| L1+L2 512MiB | 512MiB / 512MiB | 11.9 / 597 / 640 / 1.000 | 177.7 / 68 / 770 / 1.000 |
| L1+L2 1GiB | 1GiB / 1GiB | 14.5 / 641 / 775 / 1.000 | 311.4 / 34 / 901 / 1.000 |
| L1+L2 8GiB | 8GiB / 16GiB | 14.6 / 609 / 812 / 1.000 | 1793.8 / 7 / 815 / 1.000 |

### GloVe-200

**Index only**

| cache | L1 / L2 | single (QPS / first ms / RSS MB / recall) | batch (QPS / first ms / RSS MB / recall) |
|---|---|---|---|
| off |  | 2.0 / 654 / 568 / 0.971 | 39.0 / 544 / 571 / 0.971 |
| L1 only | 8GiB / L2 0 | 180.0 / 615 / 564 / 0.971 | 1257.4 / 17 / 568 / 0.971 |
| L2 only | L1 0 / 16GiB | 2.0 / 664 / 569 / 0.971 | 38.7 / 516 / 571 / 0.971 |
| L1+L2 512MiB | 512MiB / 512MiB | 69.4 / 625 / 661 / 0.971 | 1269.7 / 19 / 663 / 0.971 |
| L1+L2 1GiB | 1GiB / 1GiB | 151.8 / 661 / 565 / 0.971 | 1254.0 / 18 / 568 / 0.971 |
| L1+L2 8GiB | 8GiB / 16GiB | 161.2 / 643 / 567 / 0.971 | 1235.3 / 17 / 568 / 0.971 |

**Exact re-rank**

| cache | L1 / L2 | single (QPS / first ms / RSS MB / recall) | batch (QPS / first ms / RSS MB / recall) |
|---|---|---|---|
| off |  | 1.4 / 940 / 632 / 0.988 | — |
| L1 only | 8GiB / L2 0 | 13.3 / 889 / 1413 / 0.988 | — |
| L2 only | L1 0 / 16GiB | 1.6 / 1009 / 639 / 0.988 | 27.3 / 713 / 645 / 0.988 |
| L1+L2 512MiB | 512MiB / 512MiB | 5.7 / 1000 / 1050 / 0.988 | 12.1 / 1619 / 1303 / 0.988 |
| L1+L2 1GiB | 1GiB / 1GiB | 10.9 / 1013 / 1037 / 0.988 | 123.9 / 172 / 1447 / 0.988 |
| L1+L2 8GiB | 8GiB / 16GiB | 11.0 / 1012 / 1417 / 0.988 | 916.3 / 26 / 1420 / 0.988 |

### GIST-960

**Index only**

| cache | L1 / L2 | single (QPS / first ms / RSS MB / recall) | batch (QPS / first ms / RSS MB / recall) |
|---|---|---|---|
| off |  | 2.0 / 811 / 1104 / 0.974 | 20.1 / 492 / 1107 / 0.974 |
| L1 only | 8GiB / L2 0 | 160.9 / 794 / 1099 / 0.974 | 1678.4 / 7 / 1101 / 0.974 |
| L2 only | L1 0 / 16GiB | 2.0 / 831 / 1106 / 0.974 | 20.2 / 488 / 1107 / 0.974 |
| L1+L2 512MiB | 512MiB / 512MiB | 16.2 / 811 / 1327 / 0.974 | 1617.0 / 239 / 1330 / 0.974 |
| L1+L2 1GiB | 1GiB / 1GiB | 56.0 / 803 / 1103 / 0.974 | 1730.2 / 7 / 1105 / 0.974 |
| L1+L2 8GiB | 8GiB / 16GiB | 164.0 / 805 / 1097 / 0.974 | 1731.5 / 7 / 1100 / 0.974 |

**Exact re-rank**

| cache | L1 / L2 | single (QPS / first ms / RSS MB / recall) | batch (QPS / first ms / RSS MB / recall) |
|---|---|---|---|
| off |  | 0.4 / 2489 / 1407 / 0.990 | — |
| L1 only | 8GiB / L2 0 | 0.5 / 2449 / 1353 / 0.990 | — |
| L2 only | L1 0 / 16GiB | 1.5 / 1157 / 1170 / 0.990 | 16.2 / 619 / 1180 / 0.990 |
| L1+L2 512MiB | 512MiB / 512MiB | 2.4 / 1152 / 1671 / 0.990 | 5.7 / 1868 / 1909 / 0.990 |
| L1+L2 1GiB | 1GiB / 1GiB | 3.3 / 1660 / 1772 / 0.990 | 35.1 / 223 / 2016 / 0.990 |
| L1+L2 8GiB | 8GiB / 16GiB | 6.2 / 1136 / 1982 / 0.990 | 1027.2 / 13 / 1985 / 0.990 |

### Deep-Image-96

**Index only**

| cache | L1 / L2 | single (QPS / first ms / RSS MB / recall) | batch (QPS / first ms / RSS MB / recall) |
|---|---|---|---|
| off |  | 14.9 / 374 / 287 / 0.971 | 16.5 / 612 / 386 / 0.971 |
| L1 only | 8GiB / L2 0 | 15.7 / 405 / 867 / 0.971 | 108.8 / 103 / 930 / 0.971 |
| L2 only | L1 0 / 16GiB | 15.1 / 370 / 285 / 0.971 | 16.3 / 620 / 360 / 0.971 |
| L1+L2 512MiB | 512MiB / 512MiB | 15.4 / 395 / 612 / 0.971 | 15.3 / 678 / 1404 / 0.971 |
| L1+L2 1GiB | 1GiB / 1GiB | 16.0 / 393 / 868 / 0.971 | 118.4 / 97 / 969 / 0.971 |
| L1+L2 8GiB | 8GiB / 16GiB | 15.9 / 907 / 869 / 0.971 | 119.0 / 104 / 922 / 0.971 |

**Exact re-rank**

| cache | L1 / L2 | single (QPS / first ms / RSS MB / recall) | batch (QPS / first ms / RSS MB / recall) |
|---|---|---|---|
| off |  | 0.7 / 1852 / 2598 / 0.989 | — |
| L1 only | 8GiB / L2 0 | 1.4 / 1748 / 2542 / 0.989 | — |
| L2 only | L1 0 / 16GiB | 4.3 / 1520 / 589 / 0.989 | 14.5 / 702 / 690 / 0.989 |
| L1+L2 512MiB | 512MiB / 512MiB | 4.3 / 1531 / 1292 / 0.989 | 4.2 / 2357 / 1992 / 0.989 |
| L1+L2 1GiB | 1GiB / 1GiB | 4.3 / 1551 / 1441 / 0.989 | 57.0 / 192 / 1743 / 0.989 |
| L1+L2 8GiB | 8GiB / 16GiB | 4.3 / 2206 / 1873 / 0.989 | 141.1 / 101 / 1967 / 0.989 |

### Cache findings

- SIFT index-only: cache off 4.0 QPS; L1-only 339 QPS; L1+L2 up to 364 QPS.
- GloVe: cache off 2.0 QPS; L1-only 180 QPS; L1+L2 161 QPS (nprobe=448 reads many clusters).
- GIST: cache off 2.0 QPS; L1-only 161 QPS; L1+L2 164 QPS.
- Deep-Image is lazy-indexed: cache off 14.9 QPS, L1-only 15.7, L1+L2 15.9;
  the working set is already dominated by the nprobe cluster load.
- Exact is where L2 matters: SIFT exact batch 1794 (8 GiB) vs no cache measured in the
  local fallback test at 15.7 QPS; GIST 1027; GloVe 916; Deep 141.
- The first query is much slower than steady state on MinIO; L2 cold-fill is visible in `first ms`.

### Exact batch without cache

After the `pk_locator` fallback change, native exact batch no longer fails when
`LAKESOUL_VECTOR_CACHE=0`: it builds a one-shot key index and reads decoded blocks
straight from the data files. Local SIFT nprobe=64, NQ=5: **15.7 QPS, recall 1.0**
(was `exact batch search could not open data file`). MinIO cache-off exact batch
was not re-measured after the change; the table marks those cells `—`.

## 6. Interfaces

Target configuration, L1+L2 8 GiB. `DataFusion` batch is a SQL loop; Python is the
native batch entry. pgvector is a separate service process.

### SIFT-128

| Interface | rerank | single QPS | batch QPS | server RSS MB |
|---|---|---:|---:|---:|
| python | index | 363.9 | 2757.8 | — |
| python | exact | 14.6 | 1793.8 | — |
| datafusion | index | 427.9 | 496.2 | — |
| datafusion | exact | 14.2 | 381.8 | — |
| pgvector | index | 392.7 | 452.4 | 312 |
| pgvector | exact | 32.2 | 178.3 | 850 |

### GloVe-200

| Interface | rerank | single QPS | batch QPS | server RSS MB |
|---|---|---:|---:|---:|
| python | index | 161.2 | 1235.3 | — |
| python | exact | 11.0 | 916.3 | — |
| datafusion | index | 192.5 | 214.0 | — |
| datafusion | exact | 11.4 | 210.5 | — |
| pgvector | index | 201.4 | 222.4 | 484 |
| pgvector | exact | 24.2 | 102.0 | 1472 |

### GIST-960

| Interface | rerank | single QPS | batch QPS | server RSS MB |
|---|---|---:|---:|---:|
| python | index | 164.0 | 1731.5 | — |
| python | exact | 6.2 | 1027.2 | — |
| datafusion | index | 170.9 | 182.5 | — |
| datafusion | exact | 5.7 | 154.0 | — |
| pgvector | index | 208.8 | 229.2 | 1035 |
| pgvector | exact | 1.8 | 28.7 | 3853 |

### Deep-Image-96

| Interface | rerank | single QPS | batch QPS | server RSS MB |
|---|---|---:|---:|---:|
| python | index | 15.9 | 119.0 | — |
| python | exact | 4.3 | 141.1 | — |
| datafusion | index | 16.2 | 105.1 | — |
| datafusion | exact | 4.3 | 100.0 | — |
| pgvector | index | 16.0 | 100.5 | 791 |
| pgvector | exact | 1.8 | 11.4 | 3795 |

## 7. Conclusions

1. With data/index in MinIO, the three tiers are distinguishable:
   cache off is 4 QPS (SIFT index), L1-only is 339, L1+L2 is 164–364 depending on table.
2. L2 matters most for exact: exact batch grows to 141–1794 QPS with L1+L2, versus
   roughly 15 QPS for the uncached reader fallback.
3. L1 is still required for index-only; L2 alone does not cache the loaded index.
4. MinIO build time is storage-bound: SIFT 15.5 s, GloVe 55.5 s, GIST 53.5 s,
   Deep 153.6 s; build RSS stays 0.5–1.45 GB.
5. The target configurations all reach recall>=0.97 with nprobe found by an upward scan;
   GloVe uses nlist=1024,nprobe=448, not the rejected nlist=4096,nprobe=2048.

## 8. Raw results

- [minio.jsonl](/benchmark-data/vector-index-minio-2026-11-11/minio.jsonl)

The local-filesystem full parameter sweep remains at
[17-vector-index-benchmark-local-file.md](17-vector-index-benchmark-local-file.md).
