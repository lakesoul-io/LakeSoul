# Vector Index Benchmark (MinIO / Three-Tier Cache)

> MinIO benchmark, November 2026. Build and search only; no upserts.
> Local-filesystem full sweep: [17-vector-index-benchmark-local-file.md](17-vector-index-benchmark-local-file.md).

## 1. Results at a glance

All four datasets reach recall@100 >= 0.97 with `bits=7`; nprobe is the
**first value found when scanning upward from 8**, not a maximum value.

| Dataset | nlist | bits | nprobe | NQ | index recall | index single QPS | index batch QPS | exact recall | exact single QPS | exact batch QPS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 64 | 10 | 0.975 | 363.9 | 2757.8 | 1.000 | 14.6 | 1793.8 |
| GloVe-200 | 1024 | 7 | 448 | 20 | 0.971 | 161.2 | 1235.3 | 0.988 | 11.0 | 916.3 |
| GIST-960 | 256 | 7 | 32 | 10 | 0.974 | 164.0 | 1731.5 | 0.990 | 6.2 | 1027.2 |
| Deep-Image-96 | 1024 | 7 | 32 | 10 | 0.971 | 15.9 | 119.0 | 0.989 | 4.3 | 141.1 |

## 2. Test environment

| Item | Value |
|---|---|
| CPU | Intel `General Purpose Processor` (KVM), 1 socket, 8 cores, 2 threads/core, 16 logical CPUs |
| Memory | 60 GiB, no swap |
| Storage | MinIO local container `s3://lakesoul-bench2`; L2 on local SSD |
| Index | IVF + RaBitQ, `hash_bucket_num=4` |
| Query | top_k=100, HDF5 ground-truth recall@100 |
| Build/query CPU | not pinned; measured SIFT build 237% (~2.4 cores, 485 MB peak RSS), SIFT query 349% (~3.5 cores, 397 MB peak RSS) |
| Concurrency | native batch 4 bucket threads x `parallelism=1`; pgvector 4 worker threads |
| Cleanup | drop table and delete MinIO prefix + local cache after each dataset |

| Dataset | Base | Query | dim | metric |
|---|---:|---:|---:|---|
| SIFT-128 | 1,000,000 | 10,000 | 128 | L2 |
| GloVe-200 | 1,183,514 | 10,000 | 200 | cosine |
| GIST-960 | 1,000,000 | 1,000 | 960 | L2 |
| Deep-Image-96 | 9,990,000 | 10,000 | 96 | cosine |

### Three-tier cache

| Tier | Location | Contents |
|---|---|---|
| L1 | process memory | loaded index/cluster cache, decoded blocks, snapshots |
| L2 | local SSD `LAKESOUL_CACHE_PATH` | index segments, decoded vector blocks, snapshots |
| L3 | MinIO | table data and index segments |

`LAKESOUL_CACHE_SIZE=0` disables L2. L1-only helps index search; L2-only does
not cache the loaded index and mainly helps exact re-rank.

## 3. Build

| Dataset | nlist | bits | build s | build RSS MB | write s |
|---|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 15.5 | 515 | 3.1 |
| GloVe-200 | 1024 | 7 | 55.5 | 804 | 12.9 |
| GIST-960 | 256 | 7 | 53.5 | 1040 | 42.3 |
| Deep-Image-96 | 1024 | 7 | 153.6 | 1447 | 56.1 |

## 4. Cache and retrieval

**Parameters are fixed per dataset and shown here; recall is independent of
cache configuration.** Detailed cache matrix below:

### SIFT-128

Parameters: `nlist=256`, `bits=7`, `nprobe=64`, `top_k=100`, `NQ=10`.

| cache | L1 / L2 | index single | index batch | index recall | exact single | exact batch | exact recall | query RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| off | — / — | 4.0 | 39.0 | 0.975 | 2.8 | — | 1.000 | 383 |
| L1 only | 8GiB / 0 | 339.3 | 3002.7 | 0.975 | 4.0 | — | 1.000 | 387 |
| L2 only | 0 / 16GiB | 3.9 | 39.4 | 0.975 | 3.1 | 29.2 | 1.000 | 384 |
| L1+L2 512MiB | 512MiB / 512MiB | 300.3 | 3082.3 | 0.975 | 11.9 | 177.7 | 1.000 | 386 |
| L1+L2 1GiB | 1GiB / 1GiB | 278.7 | 3171.7 | 0.975 | 14.5 | 311.4 | 1.000 | 384 |
| L1+L2 8GiB | 8GiB / 16GiB | 363.9 | 2757.8 | 0.975 | 14.6 | 1793.8 | 1.000 | 384 |

### GloVe-200

Parameters: `nlist=1024`, `bits=7`, `nprobe=448`, `top_k=100`, `NQ=20`.

| cache | L1 / L2 | index single | index batch | index recall | exact single | exact batch | exact recall | query RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| off | — / — | 2.0 | 39.0 | 0.971 | 1.4 | 5.4 | 0.988 | 568 |
| L1 only | 8GiB / 0 | 180.0 | 1257.4 | 0.971 | 13.3 | 919.4 | 0.988 | 564 |
| L2 only | 0 / 16GiB | 2.0 | 38.7 | 0.971 | 1.6 | 27.3 | 0.988 | 569 |
| L1+L2 512MiB | 512MiB / 512MiB | 69.4 | 1269.7 | 0.971 | 5.7 | 12.1 | 0.988 | 661 |
| L1+L2 1GiB | 1GiB / 1GiB | 151.8 | 1254.0 | 0.971 | 10.9 | 123.9 | 0.988 | 565 |
| L1+L2 8GiB | 8GiB / 16GiB | 161.2 | 1235.3 | 0.971 | 11.0 | 916.3 | 0.988 | 567 |

### GIST-960

Parameters: `nlist=256`, `bits=7`, `nprobe=32`, `top_k=100`, `NQ=10`.

| cache | L1 / L2 | index single | index batch | index recall | exact single | exact batch | exact recall | query RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| off | — / — | 2.0 | 20.1 | 0.974 | 0.4 | — | 0.990 | 1104 |
| L1 only | 8GiB / 0 | 160.9 | 1678.4 | 0.974 | 0.5 | — | 0.990 | 1099 |
| L2 only | 0 / 16GiB | 2.0 | 20.2 | 0.974 | 1.5 | 16.2 | 0.990 | 1106 |
| L1+L2 512MiB | 512MiB / 512MiB | 16.2 | 1617.0 | 0.974 | 2.4 | 5.7 | 0.990 | 1327 |
| L1+L2 1GiB | 1GiB / 1GiB | 56.0 | 1730.2 | 0.974 | 3.3 | 35.1 | 0.990 | 1103 |
| L1+L2 8GiB | 8GiB / 16GiB | 164.0 | 1731.5 | 0.974 | 6.2 | 1027.2 | 0.990 | 1097 |

### Deep-Image-96

Parameters: `nlist=1024`, `bits=7`, `nprobe=32`, `top_k=100`, `NQ=10`.

| cache | L1 / L2 | index single | index batch | index recall | exact single | exact batch | exact recall | query RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| off | — / — | 14.9 | 16.5 | 0.971 | 0.7 | — | 0.989 | 287 |
| L1 only | 8GiB / 0 | 15.7 | 108.8 | 0.971 | 1.4 | — | 0.989 | 867 |
| L2 only | 0 / 16GiB | 15.1 | 16.3 | 0.971 | 4.3 | 14.5 | 0.989 | 285 |
| L1+L2 512MiB | 512MiB / 512MiB | 15.4 | 15.3 | 0.971 | 4.3 | 4.2 | 0.989 | 612 |
| L1+L2 1GiB | 1GiB / 1GiB | 16.0 | 118.4 | 0.971 | 4.3 | 57.0 | 0.989 | 868 |
| L1+L2 8GiB | 8GiB / 16GiB | 15.9 | 119.0 | 0.971 | 4.3 | 141.1 | 0.989 | 869 |

Notes:

- `off`: `LAKESOUL_VECTOR_CACHE=0`, no L1/L2. The exact-batch cells are `—`
  because MinIO cache-off exact batch was not re-measured after the reader
  fallback; local SIFT measured 15.7 QPS after the fix.
- `L1 only`: 8 GiB memory, L2 disabled.
- `L2 only`: L1 budget 0, 16 GiB local disk cache.
- Index recall does not change with cache; exact recall is slightly higher.

## 5. Interfaces

Target configuration, L1+L2 8 GiB. DataFusion batch is a SQL loop; Python is the
native batch path; pgvector is a separate service.

### SIFT-128

| Interface | index single | index batch | exact single | exact batch | server RSS MB |
|---|---:|---:|---:|---:|---:|
| python | 363.9 | 2757.8 | 14.6 | 1793.8 | — |
| datafusion | 427.9 | 496.2 | 14.2 | 381.8 | — |
| pgvector | 392.7 | 452.4 | 32.2 | 178.3 | 312 |

### GloVe-200

| Interface | index single | index batch | exact single | exact batch | server RSS MB |
|---|---:|---:|---:|---:|---:|
| python | 161.2 | 1235.3 | 11.0 | 916.3 | — |
| datafusion | 192.5 | 214.0 | 11.4 | 210.5 | — |
| pgvector | 201.4 | 222.4 | 24.2 | 102.0 | 484 |

### GIST-960

| Interface | index single | index batch | exact single | exact batch | server RSS MB |
|---|---:|---:|---:|---:|---:|
| python | 164.0 | 1731.5 | 6.2 | 1027.2 | — |
| datafusion | 170.9 | 182.5 | 5.7 | 154.0 | — |
| pgvector | 208.8 | 229.2 | 1.8 | 28.7 | 1035 |

### Deep-Image-96

| Interface | index single | index batch | exact single | exact batch | server RSS MB |
|---|---:|---:|---:|---:|---:|
| python | 15.9 | 119.0 | 4.3 | 141.1 | — |
| datafusion | 16.2 | 105.1 | 4.3 | 100.0 | — |
| pgvector | 16.0 | 100.5 | 1.8 | 11.4 | 791 |

## 6. What the numbers say

1. Index-only requires L1: SIFT 4.0 (off) -> 339 (L1 only) -> 364 (L1+L2).
2. L2 is needed by exact re-rank: SIFT exact batch 1794 at 8 GiB, GIST 1027,
   GloVe 916, Deep 141; L2-only only reaches 16–29 QPS on batch.
3. Deep-Image is lazy-indexed; its QPS stays around 15–16 because nprobe cluster
   loading dominates, and extra cache gives little.
4. MinIO build is storage-bound: SIFT 15.5 s, GloVe 55.5 s, GIST 53.5 s,
   Deep 153.6 s; build RSS 0.5–1.45 GB.
5. `nlist=4096, nprobe=2048` is not used; GloVe reached 0.97 at
   `nlist=1024, nprobe=448`.

## 7. Limitations

- Results are MinIO-local; remote S3 adds network latency.
- NQ is 10 except GloVe (20).
- MinIO cache-off/L1-only exact batch was not re-measured after the fallback.
- Query RSS is process RSS; pgvector server RSS is separate.

## 8. Raw data

- [minio.jsonl](/benchmark-data/vector-index-minio-2026-11-11/minio.jsonl)

