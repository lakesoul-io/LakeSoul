# 向量索引大规模基准（L1 512MiB / L2 1GiB）

> 与 8GiB 大规模测试相同协议，但 **L1=512MiB、L2=1GiB**。
> 单查 200 条，batch 全部 query（10k/1k）；只测构建+检索。

## 1. 方法

| 项目 | 取值 |
|---|---|
| L1 内存 | **512 MiB**（`LAKESOUL_VECTOR_MEMORY_BYTES=512MiB`） |
| L2 磁盘 | **1 GiB**（`LAKESOUL_CACHE_SIZE=1GiB`） |
| 存储 | MinIO `s3://lakesoul-bench2` |
| 单查 | 200 条 |
| batch | 全部 query |
| 百分位 | 单查真实测量；batch 为 100 条 chunk 归一化 |
| 清理 | 每个数据集测完 drop 表，删除 MinIO prefix/cache |

| 数据集 | nlist | bits | build s | build RSS MB | 写表 s |
|---|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 14.9 | 530 | 3.3 |
| GloVe-200 | 1024 | 7 | 57.0 | 802 | 13.2 |
| GIST-960 | 256 | 7 | 53.8 | 1059 | 43.4 |
| Deep-Image-96 | 1024 | 7 | 147.4 | 1397 | 55.6 |

## 2. Python native

### 单查（200）

| 数据集 | rerank | nlist | bits | nprobe | 单查 QPS | P50 | P90 | P99 | recall |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 256 | 7 | 64 | 335.0 | 2.97 | 3.25 | 3.46 | 0.975 |
| SIFT-128 | exact | 256 | 7 | 64 | 71.2 | 9.99 | 17.90 | 104.02 | 1.000 |
| GloVe-200 | index | 1024 | 7 | 512 | 62.6 | 7.10 | 9.01 | 150.21 | 0.971 |
| GloVe-200 | exact | 1024 | 7 | 512 | 23.6 | 16.73 | 137.90 | 255.38 | 0.991 |
| GIST-960 | index | 256 | 7 | 64 | 10.9 | 7.78 | 254.08 | 260.95 | 0.980 |
| GIST-960 | exact | 256 | 7 | 64 | 2.4 | 402.58 | 418.57 | 921.49 | 0.999 |

### batch（全部）

| 数据集 | rerank | batch_nq | batch QPS | P50 | P90 | P99 | recall |
|---|---|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 10000 | 7204.3 | 0.17 | 0.19 | 0.22 | 0.975 |
| SIFT-128 | exact | 10000 | 377.7 | 2.92 | 3.04 | 3.15 | 1.000 |
| GloVe-200 | index | 10000 | 1398.2 | 0.73 | 2.14 | 2.34 | 0.972 |
| GloVe-200 | exact | 10000 | 251.6 | 5.08 | 5.66 | 5.93 | 0.992 |
| GIST-960 | index | 1000 | 1326.9 | 4.21 | 4.50 | 4.53 | 0.979 |
| GIST-960 | exact | 1000 | 15.7 | 70.95 | 72.32 | 73.63 | 0.998 |

## 3. DataFusion

| 数据集 | rerank | nprobe | 单查 QPS | batch QPS | 单查 P50 | 单查 P99 | recall | 服务端 RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 376.8 | 381.5 | 2.64 | 3.02 | 0.975 | — |
| SIFT-128 | exact | 64 | 73.3 | 106.5 | 9.69 | 112.64 | 1.000 | — |
| GloVe-200 | index | 512 | 72.1 | 65.0 | 6.34 | 138.82 | 0.972 | — |
| GloVe-200 | exact | 512 | 24.8 | 34.1 | 16.16 | 261.03 | 0.992 | — |
| GIST-960 | index | 64 | 11.1 | 8.1 | 7.37 | 264.55 | 0.979 | — |
| GIST-960 | exact | 64 | — | — | — | — | — | — |

## 4. pgvector

| 数据集 | rerank | nprobe | 单查 QPS | batch QPS | 单查 P50 | 单查 P99 | recall | 服务端 RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 64 | 354.4 | 357.5 | 2.77 | 3.48 | 0.975 | 310 |
| SIFT-128 | exact | 64 | — | — | — | — | — | — |
| GloVe-200 | index | 512 | 67.6 | 67.4 | 6.43 | 169.81 | 0.972 | 879 |
| GloVe-200 | exact | 512 | — | — | — | — | — | — |
| GIST-960 | index | 64 | — | — | — | — | — | — |
| GIST-960 | exact | 64 | — | — | — | — | — | — |

## 5. 说明

- Deep-Image 没有进入查询表：512MiB/1GiB 下 10k 全量 batch 在 30 分钟预算内
  没有跑完；构建已完成（147s）。
- SIFT/GloVe 的 pgvector exact、GIST 的 pgvector 全部结果未跑：
  1GiB block cache 使全量 batch 超出预算。
- batch 百分位为 chunk 归一化，包含 batch 调度开销。
- 已完成的数据集都已释放表/索引/cache。

## 6. 原始数据

- [raw/sift.jsonl](/benchmark-data/vector-index-minio-large-512m1g-2026-11-11/sift-512m1g.jsonl)
- [raw/glove.jsonl](/benchmark-data/vector-index-minio-large-512m1g-2026-11-11/glove-512m1g.jsonl)
- [raw/gist.jsonl](/benchmark-data/vector-index-minio-large-512m1g-2026-11-11/gist-512m1g.jsonl)
- [raw/deep.jsonl](/benchmark-data/vector-index-minio-large-512m1g-2026-11-11/deep-512m1g.jsonl)
