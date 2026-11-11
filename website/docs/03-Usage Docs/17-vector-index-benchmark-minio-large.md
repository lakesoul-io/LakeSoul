# Large-Scale Vector Benchmark (MinIO)

> Single query uses **200 query vectors**; batch uses **all query vectors**
> (10,000 for SIFT/GloVe/Deep, 1,000 for GIST). MinIO storage, build+search only.

## 1. Method

| Item | Value |
|---|---|
| Storage | MinIO `s3://lakesoul-bench2` |
| Cache | L1 8 GiB + L2 16 GiB, unless marked "off" |
| Index | IVF + RaBitQ, `hash_bucket_num=4` |
| Single | first query excluded, then 200 queries |
| Batch | every query vector in one native batch (or one SQL/pgvector pass) |
| P50/P90/P99 | single: real per-query timings; batch: 100-query chunk-normalized approximation |
| CPU/memory | 16 logical CPUs / 8 physical cores / 60 GiB; not pinned |
| Cleanup | drop table + delete MinIO prefix/cache after each dataset |

| Dataset | nlist | bits | build s | build RSS MB | write s |
|---|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 14.5 | 496 | 3.0 |
| GloVe-200 | 1024 | 7 | 58.6 | 830 | 13.2 |
| GIST-960 | 256 | 7 | 53.2 | 1068 | 41.6 |
| Deep-Image-96 | 1024 | 7 | 147.0 | 1434 | 56.4 |

## 2. Python native

### Single query (200)

| Dataset | rerank | nlist | bits | nprobe | single_nq | QPS | P50 ms | P90 ms | P99 ms | recall |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 256 | 7 | 64 | 200 | 391.3 | 2.53 | 2.74 | 3.25 | 0.975 |
| SIFT-128 | exact | 256 | 7 | 64 | 200 | 134.7 | 2.92 | 11.47 | 107.39 | 1.000 |
| GloVe-200 | index | 1024 | 7 | 512 | 200 | 197.1 | 5.20 | 6.21 | 6.81 | 0.971 |
| GloVe-200 | exact | 1024 | 7 | 512 | 200 | 63.4 | 6.15 | 32.25 | 165.91 | 0.991 |
| GIST-960 | index | 256 | 7 | 64 | 200 | 157.1 | 6.38 | 6.68 | 6.99 | 0.980 |
| GIST-960 | exact | 256 | 7 | 64 | 200 | 18.4 | 29.87 | 126.58 | 274.37 | 0.999 |
| Deep-Image-96 | index | 1024 | 7 | 64 | 200 | 33.9 | 19.85 | 51.74 | 134.63 | 0.969 |
| Deep-Image-96 | exact | 1024 | 7 | 64 | 200 | 13.0 | 41.46 | 183.63 | 389.11 | 0.989 |

### Batch (all query vectors)

| Dataset | rerank | nprobe | batch_nq | QPS | P50 ms | P90 ms | P99 ms | recall |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 10000 | 7268.6 | 0.16 | 0.17 | 0.19 | 0.975 |
| SIFT-128 | exact | 64 | 10000 | 4052.5 | 0.29 | 0.31 | 0.33 | 1.000 |
| GloVe-200 | index | 512 | 10000 | 1786.4 | 0.66 | 0.68 | 0.72 | 0.972 |
| GloVe-200 | exact | 512 | 10000 | 1362.4 | 0.82 | 0.87 | 0.95 | 0.992 |
| GIST-960 | index | 64 | 1000 | 1896.1 | 0.50 | 0.52 | 0.52 | 0.979 |
| GIST-960 | exact | 64 | 1000 | 418.1 | 1.96 | 2.32 | 3.05 | 0.998 |
| Deep-Image-96 | index | 64 | 10000 | 73.6 | 13.59 | 14.04 | 14.42 | 0.969 |
| Deep-Image-96 | exact | 64 | 10000 | 70.7 | 15.32 | 15.69 | 16.21 | 0.990 |

### Cache off (index only)

| Dataset | index single QPS | P50 ms | P90 ms | P99 ms | recall | batch QPS | batch P50 | batch recall |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | 3.9 | 255.78 | 260.37 | 268.47 | 0.975 | 2496.6 | 2.68 | 0.975 |
| GloVe-200 | 2.0 | 503.48 | 512.11 | 532.88 | 0.971 | 907.0 | 5.68 | 0.972 |

## 3. DataFusion

| Dataset | rerank | nlist | bits | nprobe | single QPS | single P50 | single P90 | single P99 | single recall | batch QPS | batch P50 | batch P90 | batch P99 | batch recall | server RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 256 | 7 | 64 | 489.7 | 2.04 | 2.20 | 2.46 | 0.975 | 479.2 | 2.10 | 2.25 | 2.32 | 0.975 | — |
| SIFT-128 | exact | 256 | 7 | 64 | 138.8 | 2.60 | 11.99 | 112.44 | 1.000 | 402.5 | 2.55 | 2.66 | 2.75 | 1.000 | — |
| GloVe-200 | index | 1024 | 7 | 512 | 216.3 | 4.73 | 5.88 | 6.65 | 0.971 | 223.9 | 4.50 | 4.66 | 4.86 | 0.972 | — |
| GloVe-200 | exact | 1024 | 7 | 512 | 66.3 | 5.67 | 31.78 | 139.82 | 0.991 | 198.9 | 5.10 | 5.30 | 5.40 | 0.992 | — |
| GIST-960 | index | 256 | 7 | 64 | 165.1 | 6.04 | 6.39 | 7.43 | 0.980 | 170.5 | 5.91 | 6.15 | 6.40 | 0.979 | — |
| GIST-960 | exact | 256 | 7 | 64 | 20.5 | 28.79 | 121.41 | 228.10 | 0.999 | 79.7 | 10.40 | 11.01 | 15.25 | 0.998 | — |

## 4. pgvector

| Dataset | rerank | nlist | bits | nprobe | single QPS | single P50 | single P90 | single P99 | single recall | batch QPS | batch P50 | batch P90 | batch P99 | batch recall | server RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 256 | 7 | 64 | 463.2 | 2.13 | 2.33 | 3.09 | 0.975 | 455.5 | 2.21 | 2.32 | 2.35 | 0.975 | 312 |
| SIFT-128 | exact | 256 | 7 | 64 | 151.9 | 5.33 | 5.78 | 27.42 | 1.000 | 186.9 | 5.23 | 5.41 | 5.57 | 1.000 | 856 |
| GloVe-200 | index | 1024 | 7 | 512 | 217.9 | 4.67 | 5.69 | 6.52 | 0.971 | 221.2 | 4.47 | 4.66 | 4.95 | 0.972 | 482 |
| GloVe-200 | exact | 1024 | 7 | 512 | 76.6 | 10.24 | 11.35 | 62.99 | 0.992 | 101.7 | 9.71 | 9.94 | 10.23 | 0.992 | 1476 |
| GIST-960 | index | 256 | 7 | 64 | 199.5 | 4.96 | 5.43 | 6.36 | 0.980 | 206.7 | 4.71 | 4.85 | 4.86 | 0.979 | 1040 |
| GIST-960 | exact | 256 | 7 | 64 | 12.5 | 45.01 | 73.15 | 898.12 | 0.999 | 23.7 | 42.20 | 43.02 | 43.78 | 0.998 | 5355 |

Deep-Image is omitted from the DataFusion/pgvector tables: a full 10k batch at
its ~16 QPS / ~1.8 QPS exact rates would take many minutes to hours.

## 5. Notes

- Batch P50/P90/P99 are normalized from 100-query chunks; they include batch
  scheduling overhead and are not true per-query service times.
- GIST has only 1,000 query vectors, so its batch uses 1,000.
- GloVe uses nprobe=512 after a 200-query check; nprobe=448 gave 0.9682 recall.
- Deep-Image nprobe=64 gives 0.9687 index recall; exact 0.9895.
- All tables were released after each dataset.

## 6. Raw data

- [raw/sift.jsonl](/benchmark-data/vector-index-minio-large-2026-11-11/sift.jsonl)
- [raw/glove.jsonl](/benchmark-data/vector-index-minio-large-2026-11-11/glove.jsonl)
- [raw/gist.jsonl](/benchmark-data/vector-index-minio-large-2026-11-11/gist.jsonl)
- [raw/deep.jsonl](/benchmark-data/vector-index-minio-large-2026-11-11/deep.jsonl)
