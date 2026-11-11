# Large-Scale MinIO Benchmark (L1 512MiB / L2 1GiB)

> Same large-scale protocol as the 8GiB run, but **L1=512MiB, L2=1GiB**.
> Single query = 200 vectors; batch = all vectors (10k/1k). Build+search only.

## 1. Method

| Item | Value |
|---|---|
| L1 memory | **512 MiB** (`LAKESOUL_VECTOR_MEMORY_BYTES=512MiB`) |
| L2 disk | **1 GiB** (`LAKESOUL_CACHE_SIZE=1GiB`) |
| Storage | MinIO `s3://lakesoul-bench2` |
| Single | 200 queries |
| Batch | all queries |
| Percentiles | single real; batch 100-query chunk normalized |
| Cleanup | drop table + MinIO prefix/cache after each dataset |

| Dataset | nlist | bits | build s | build RSS MB | write s |
|---|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 14.9 | 530 | 3.3 |
| GloVe-200 | 1024 | 7 | 57.0 | 802 | 13.2 |
| GIST-960 | 256 | 7 | 53.8 | 1059 | 43.4 |
| Deep-Image-96 | 1024 | 7 | 147.4 | 1397 | 55.6 |

## 2. Python native

### Single (200)

| Dataset | rerank | nlist | bits | nprobe | single QPS | P50 | P90 | P99 | recall |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 256 | 7 | 64 | 335.0 | 2.97 | 3.25 | 3.46 | 0.975 |
| SIFT-128 | exact | 256 | 7 | 64 | 71.2 | 9.99 | 17.90 | 104.02 | 1.000 |
| GloVe-200 | index | 1024 | 7 | 512 | 62.6 | 7.10 | 9.01 | 150.21 | 0.971 |
| GloVe-200 | exact | 1024 | 7 | 512 | 23.6 | 16.73 | 137.90 | 255.38 | 0.991 |
| GIST-960 | index | 256 | 7 | 64 | 10.9 | 7.78 | 254.08 | 260.95 | 0.980 |
| GIST-960 | exact | 256 | 7 | 64 | 2.4 | 402.58 | 418.57 | 921.49 | 0.999 |

### Batch (all)

| Dataset | rerank | batch_nq | batch QPS | P50 | P90 | P99 | recall |
|---|---|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 10000 | 7204.3 | 0.17 | 0.19 | 0.22 | 0.975 |
| SIFT-128 | exact | 10000 | 377.7 | 2.92 | 3.04 | 3.15 | 1.000 |
| GloVe-200 | index | 10000 | 1398.2 | 0.73 | 2.14 | 2.34 | 0.972 |
| GloVe-200 | exact | 10000 | 251.6 | 5.08 | 5.66 | 5.93 | 0.992 |
| GIST-960 | index | 1000 | 1326.9 | 4.21 | 4.50 | 4.53 | 0.979 |
| GIST-960 | exact | 1000 | 15.7 | 70.95 | 72.32 | 73.63 | 0.998 |

## 3. DataFusion

| Dataset | rerank | nprobe | single QPS | batch QPS | single P50 | single P99 | recall | server RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 376.8 | 381.5 | 2.64 | 3.02 | 0.975 | — |
| SIFT-128 | exact | 64 | 73.3 | 106.5 | 9.69 | 112.64 | 1.000 | — |
| GloVe-200 | index | 512 | 72.1 | 65.0 | 6.34 | 138.82 | 0.972 | — |
| GloVe-200 | exact | 512 | 24.8 | 34.1 | 16.16 | 261.03 | 0.992 | — |
| GIST-960 | index | 64 | 11.1 | 8.1 | 7.37 | 264.55 | 0.979 | — |
| GIST-960 | exact | 64 | — | — | — | — | — | — |

## 4. pgvector

| Dataset | rerank | nprobe | single QPS | batch QPS | single P50 | single P99 | recall | server RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 354.4 | 357.5 | 2.77 | 3.48 | 0.975 | 310 |
| SIFT-128 | exact | 64 | — | — | — | — | — | — |
| GloVe-200 | index | 512 | 67.6 | 67.4 | 6.43 | 169.81 | 0.972 | 879 |
| GloVe-200 | exact | 512 | — | — | — | — | — | — |
| GIST-960 | index | 64 | — | — | — | — | — | — |
| GIST-960 | exact | 64 | — | — | — | — | — | — |

## 5. Notes

- Deep-Image is not in the query tables: the 10k full batch at 512MiB/1GiB did
  not finish within the 30-minute budget; build completed (147s).
- pgvector exact for SIFT/GloVe and all GIST pgvector results are omitted because
  the 1GiB block cache made the full batch exceed the budget.
- Batch percentiles are chunk-normalized; they include batch scheduling.
- Every completed dataset was released after its run.

## 6. Raw data

- [raw/sift.jsonl](/benchmark-data/vector-index-minio-large-512m1g-2026-11-11/sift-512m1g.jsonl)
- [raw/glove.jsonl](/benchmark-data/vector-index-minio-large-512m1g-2026-11-11/glove-512m1g.jsonl)
- [raw/gist.jsonl](/benchmark-data/vector-index-minio-large-512m1g-2026-11-11/gist-512m1g.jsonl)
- [raw/deep.jsonl](/benchmark-data/vector-index-minio-large-512m1g-2026-11-11/deep-512m1g.jsonl)
