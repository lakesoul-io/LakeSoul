# 向量索引基准测试（MinIO / 三级缓存）

> 2026-11 MinIO 基准；只测构建和检索，不测 upsert。
> 本地文件系统完整参数扫描见 [17-vector-index-benchmark-local-file.md](17-vector-index-benchmark-local-file.md)。

## 1. 结果一览

四个数据集都用 `bits=7` 达到 recall@100 ≥ 0.97；nprobe 是从 8 开始
**逐步增大找到的第一个满足值**，不是最大值。

下面默认是 **Python native** 接口；DataFusion 和 pgvector 分节单列。

| 数据集 | nlist | bits | nprobe | NQ | index recall | index 单查 QPS | index batch QPS | exact recall | exact 单查 QPS | exact batch QPS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 64 | 10 | 0.975 | 363.9 | 2757.8 | 1.000 | 14.6 | 1793.8 |
| GloVe-200 | 1024 | 7 | 448 | 20 | 0.971 | 161.2 | 1235.3 | 0.988 | 11.0 | 916.3 |
| GIST-960 | 256 | 7 | 32 | 10 | 0.974 | 164.0 | 1731.5 | 0.990 | 6.2 | 1027.2 |
| Deep-Image-96 | 1024 | 7 | 32 | 10 | 0.971 | 15.9 | 119.0 | 0.989 | 4.3 | 141.1 |

## 2. 测试环境

| 项目 | 取值 |
|---|---|
| CPU | Intel `General Purpose Processor`（KVM），1 socket、8 物理核、2 threads/core、16 逻辑 CPU |
| 内存 | 60 GiB，无 swap |
| 存储 | MinIO 本机容器 `s3://lakesoul-bench2`；L2 在本地 SSD |
| 索引 | IVF + RaBitQ，`hash_bucket_num=4` |
| 查询 | top_k=100，HDF5 ground truth recall@100 |
| CPU 使用 | 不绑核；SIFT 构建 237%（约 2.4 核，485 MB），SIFT 查询 349%（约 3.5 核，397 MB） |
| 并发 | Python batch 4 个 bucket 线程 × `parallelism=1`；pgvector 4 worker |
| 清理 | 每个数据集测完 drop 表，删除 MinIO prefix 和本地 cache |

| 数据集 | Base | Query | dim | 距离 |
|---|---:|---:|---:|---|
| SIFT-128 | 1,000,000 | 10,000 | 128 | L2 |
| GloVe-200 | 1,183,514 | 10,000 | 200 | cosine |
| GIST-960 | 1,000,000 | 1,000 | 960 | L2 |
| Deep-Image-96 | 9,990,000 | 10,000 | 96 | cosine |

### 三级缓存

| 层级 | 位置 | 内容 |
|---|---|---|
| L1 | 进程内存 | 已加载 index/cluster cache、decoded block、snapshot |
| L2 | 本地 SSD | index segment、decoded vector block、snapshot |
| L3 | MinIO | 表数据和 index segment |

`LAKESOUL_CACHE_SIZE=0` 关闭 L2。L1-only 对 index 有效；L2-only 主要服务 exact。

## 3. 构建

| 数据集 | nlist | bits | build s | build RSS MB | 写表 s |
|---|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 15.5 | 515 | 3.1 |
| GloVe-200 | 1024 | 7 | 55.5 | 804 | 12.9 |
| GIST-960 | 256 | 7 | 53.5 | 1040 | 42.3 |
| Deep-Image-96 | 1024 | 7 | 153.6 | 1447 | 56.1 |

## 4. Python native 接口

每个数据集参数固定，recall 与缓存配置无关。

### SIFT-128

参数：`nlist=256`、`bits=7`、`nprobe=64`、`top_k=100`、`NQ=10`。

| cache | L1 / L2 | index 单查 QPS | index batch QPS | index recall | exact 单查 QPS | exact batch QPS | exact recall | 查询 RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| off | — / — | 4.0 | 39.0 | 0.975 | 2.5 | 6.3 | 1.000 | 383 |
| L1 only | 8GiB / 0 | 339.3 | 3002.7 | 0.975 | 18.0 | 1765.7 | 1.000 | 387 |
| L2 only | 0 / 16GiB | 3.9 | 39.4 | 0.975 | 3.1 | 29.2 | 1.000 | 384 |
| L1+L2 512MiB | 512MiB / 512MiB | 300.3 | 3082.3 | 0.975 | 11.9 | 177.7 | 1.000 | 386 |
| L1+L2 1GiB | 1GiB / 1GiB | 278.7 | 3171.7 | 0.975 | 14.5 | 311.4 | 1.000 | 384 |
| L1+L2 8GiB | 8GiB / 16GiB | 363.9 | 2757.8 | 0.975 | 14.6 | 1793.8 | 1.000 | 384 |

### GloVe-200

参数：`nlist=1024`、`bits=7`、`nprobe=448`、`top_k=100`、`NQ=20`。

| cache | L1 / L2 | index 单查 QPS | index batch QPS | index recall | exact 单查 QPS | exact batch QPS | exact recall | 查询 RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| off | — / — | 2.0 | 39.0 | 0.971 | 1.4 | 5.4 | 0.988 | 568 |
| L1 only | 8GiB / 0 | 180.0 | 1257.4 | 0.971 | 13.3 | 919.4 | 0.988 | 564 |
| L2 only | 0 / 16GiB | 2.0 | 38.7 | 0.971 | 1.6 | 27.3 | 0.988 | 569 |
| L1+L2 512MiB | 512MiB / 512MiB | 69.4 | 1269.7 | 0.971 | 5.7 | 12.1 | 0.988 | 661 |
| L1+L2 1GiB | 1GiB / 1GiB | 151.8 | 1254.0 | 0.971 | 10.9 | 123.9 | 0.988 | 565 |
| L1+L2 8GiB | 8GiB / 16GiB | 161.2 | 1235.3 | 0.971 | 11.0 | 916.3 | 0.988 | 567 |

### GIST-960

参数：`nlist=256`、`bits=7`、`nprobe=32`、`top_k=100`、`NQ=10`。

| cache | L1 / L2 | index 单查 QPS | index batch QPS | index recall | exact 单查 QPS | exact batch QPS | exact recall | 查询 RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| off | — / — | 2.0 | 20.1 | 0.974 | 0.4 | — | 0.990 | 1104 |
| L1 only | 8GiB / 0 | 160.9 | 1678.4 | 0.974 | 0.5 | — | 0.990 | 1099 |
| L2 only | 0 / 16GiB | 2.0 | 20.2 | 0.974 | 1.5 | 16.2 | 0.990 | 1106 |
| L1+L2 512MiB | 512MiB / 512MiB | 16.2 | 1617.0 | 0.974 | 2.4 | 5.7 | 0.990 | 1327 |
| L1+L2 1GiB | 1GiB / 1GiB | 56.0 | 1730.2 | 0.974 | 3.3 | 35.1 | 0.990 | 1103 |
| L1+L2 8GiB | 8GiB / 16GiB | 164.0 | 1731.5 | 0.974 | 6.2 | 1027.2 | 0.990 | 1097 |

### Deep-Image-96

参数：`nlist=1024`、`bits=7`、`nprobe=32`、`top_k=100`、`NQ=10`。

| cache | L1 / L2 | index 单查 QPS | index batch QPS | index recall | exact 单查 QPS | exact batch QPS | exact recall | 查询 RSS MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| off | — / — | 14.9 | 16.5 | 0.971 | 0.7 | — | 0.989 | 287 |
| L1 only | 8GiB / 0 | 15.7 | 108.8 | 0.971 | 1.4 | — | 0.989 | 867 |
| L2 only | 0 / 16GiB | 15.1 | 16.3 | 0.971 | 4.3 | 14.5 | 0.989 | 285 |
| L1+L2 512MiB | 512MiB / 512MiB | 15.4 | 15.3 | 0.971 | 4.3 | 4.2 | 0.989 | 612 |
| L1+L2 1GiB | 1GiB / 1GiB | 16.0 | 118.4 | 0.971 | 4.3 | 57.0 | 0.989 | 868 |
| L1+L2 8GiB | 8GiB / 16GiB | 15.9 | 119.0 | 0.971 | 4.3 | 141.1 | 0.989 | 869 |

SIFT 的 cache-off/L1-only exact batch 已在 reader fallback 后补测：
off 6.3 QPS，L1-only 1765.7 QPS。GIST/Deep 的 `—` 没有在修改后重测。

## 5. DataFusion 接口

相同目标参数；batch 是 SQL 查询循环，不是 native batch。

| 数据集 | nlist | bits | nprobe | NQ | index recall | index 单查 | index batch | exact recall | exact 单查 | exact batch | RSS MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 64 | 10 | 0.975 | 427.9 | 496.2 | 1.000 | 14.2 | 381.8 | client 384 |
| GloVe-200 | 1024 | 7 | 448 | 20 | 0.971 | 192.5 | 214.0 | 0.988 | 11.4 | 210.5 | client 567 |
| GIST-960 | 256 | 7 | 32 | 10 | 0.974 | 170.9 | 182.5 | 0.990 | 5.7 | 154.0 | client 1099 |
| Deep-Image-96 | 1024 | 7 | 32 | 10 | 0.971 | 16.2 | 105.1 | 0.989 | 4.3 | 100.0 | client 869 |

## 6. pgvector 接口

独立 `postgres-lakesoul` 服务，4 worker threads。`RSS MB` 是服务端 RSS
（index 单查一行）；exact 会占用更多服务端内存。

| 数据集 | nlist | bits | nprobe | NQ | index recall | index 单查 | index batch | exact recall | exact 单查 | exact batch | RSS MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 64 | 10 | 0.975 | 392.7 | 452.4 | 1.000 | 32.2 | 178.3 | server 312 |
| GloVe-200 | 1024 | 7 | 448 | 20 | 0.971 | 201.4 | 222.4 | 0.988 | 24.2 | 102.0 | server 484 |
| GIST-960 | 256 | 7 | 32 | 10 | 0.974 | 208.8 | 229.2 | 0.990 | 1.8 | 28.7 | server 1035 |
| Deep-Image-96 | 1024 | 7 | 32 | 10 | 0.971 | 16.0 | 100.5 | 0.989 | 1.8 | 11.4 | server 791 |

## 7. 结论

1. index-only 必须有 L1：SIFT cache off 4.0 QPS → L1-only 339 → L1+L2 364。
2. exact 依赖 block cache：SIFT exact batch 8GiB 下 1794 QPS，GIST 1027，
   GloVe 916，Deep 141；L2-only batch 只有 16–29 QPS。
3. Deep-Image 走 lazy index，QPS 约 15–16。
4. MinIO 构建受存储限制：SIFT 15.5s、GloVe 55.5s、GIST 53.5s、Deep 153.6s；
   build RSS 0.5–1.45 GB。
5. GloVe 使用 `nlist=1024, nprobe=448`，不使用 `nlist=4096, nprobe=2048`。

## 8. 限制

- 本机 MinIO；远端 S3 会增加网络延迟。
- 除 GloVe NQ=20 外，其余 NQ=10。
- GIST/Deep 的 cache-off/L1-only exact batch 未在 fallback 后重测。
- pgvector 的 RSS 是服务端 RSS，不是客户端。

## 9. 原始结果

- [minio.jsonl](/benchmark-data/vector-index-minio-2026-11-11/minio.jsonl)

