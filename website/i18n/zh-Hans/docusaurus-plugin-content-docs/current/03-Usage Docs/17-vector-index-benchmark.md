# 向量索引基准测试（MinIO / 三级缓存）

> 2026-11 基于 MinIO 重新测试；只测构建和检索，不测 upsert。
> 本地文件系统版本单独保留在 [17-vector-index-benchmark-local-file.md](17-vector-index-benchmark-local-file.md)。

## 1. 结论摘要

本页用 **MinIO 作为 L3**、本地 SSD 作为 L2、进程内存作为 L1，完整跑三级缓存。
数据文件和索引都放在 `s3://lakesoul-bench2`。

| 数据集 | 配置 | recall | index 单查 | index batch | exact 单查 | exact batch |
|---|---|---:|---:|---:|---:|---:|
| SIFT-128 | nlist=256, bits=7, nprobe=64 | 0.975 | 363.9 | 2757.8 | 14.6 | 1793.8 |
| GloVe-200 | nlist=1024, bits=7, nprobe=448 | 0.971 | 161.2 | 1235.3 | 11.0 | 916.3 |
| GIST-960 | nlist=256, bits=7, nprobe=32 | 0.974 | 164.0 | 1731.5 | 6.2 | 1027.2 |
| Deep-Image-96 | nlist=1024, bits=7, nprobe=32 | 0.971 | 15.9 | 119.0 | 4.3 | 141.1 |

目标配置的 nprobe 不是随手取最大值，而是**从 8 开始逐步增大**，
找到的第一个使 recall@100 ≥ 0.97 的取值。

## 2. 三级缓存

| 层级 | 位置 | 内容 |
|---|---|---|
| L1 | 进程内存 | 完整 index/cluster cache、decoded block、snapshot |
| L2 | 本地 SSD 目录（`LAKESOUL_CACHE_PATH`） | index segment、decoded vector block、snapshot |
| L3 | MinIO（`s3://lakesoul-bench2`） | 表数据文件和 index segment |

- `LAKESOUL_CACHE_SIZE=0` 关闭 L2；`LAKESOUL_VECTOR_MEMORY_BYTES=0` 关闭 L1 预算。
- L1-only 对 index-only 仍有效：首次从 MinIO 读入后常驻内存。
- L2-only 对 index-only 基本无效，因为 index cache 在 L1；L2 主要服务 exact 的向量块缓存。
- 生产配置通常是 L1+L2。
- `first ms` 是首次查询（含 MinIO 读取/缓存提升），QPS 是之后稳态。


### 机器与实测资源

| 项目 | 取值 |
|---|---|
| CPU | Intel `General Purpose Processor`（KVM 虚拟 CPU），1 socket，8 物理核，2 threads/core，**16 逻辑 CPU** |
| 指令集 | AVX2、AVX-512（含 AVX-512 VNNI/BF16） |
| 内存 | **60 GiB**，无 swap |
| cgroup 限制 | 未设置 CPU/内存上限 |
| 测试存储 | MinIO 为本机容器；L2 cache 在本地 SSD |
| 并发配置 | `hash_bucket_num=4`；native batch `parallelism=1`；pgvector `--worker-threads 4` |
| 构建实测 | SIFT MinIO：平均 **237% CPU ≈ 2.4 逻辑核**，峰值 RSS **485 MB** |
| 查询实测 | SIFT MinIO index-only，NQ=10：平均 **349% CPU ≈ 3.5 逻辑核**，峰值 RSS **397 MB** |

说明：

- 构建/查询进程没有绑定固定 CPU 核；上面的“用了多少核”是 `/usr/bin/time -v`
  的平均 CPU 百分比换算的逻辑核数，不是 cgroup 配额。
- 构建尝试使用 `hash_bucket_num=4` 的 shard 并行；`CARGO_BUILD_JOBS=8`
  只影响编译过程，不影响基准运行时的 CPU 使用。
- 单查大约使用 3–4 个逻辑核；native batch 的 bucket 线程为 4，
  merge 线程数取 `min(NQ, 可用核数)`，所以短 batch 的实测平均约 3.5 核。
- build RSS 范围约 0.5–1.45 GB；query RSS 范围约 0.3–2.2 GB；
  机器总内存 60 GiB。

## 3. 测试方法

| 项目 | 取值 |
|---|---|
| 存储 | MinIO `lakesoul-bench2`，`http://127.0.0.1:9000` |
| 索引 | IVF + RaBitQ，`hash_bucket_num=4` |
| top_k | 100 |
| recall | HDF5 ground truth recall@100 |
| 缓存矩阵 | off、L1-only、L2-only、512MiB、1GiB、8GiB |
| 清理 | 每个数据集测完 drop 表，删除 MinIO prefix 和本地 cache |

| 数据集 | Base | Query | dim | 距离 |
|---|---:|---:|---:|---|
| SIFT-128 | 1,000,000 | 10,000 | 128 | L2 |
| GloVe-200 | 1,183,514 | 10,000 | 200 | cosine |
| GIST-960 | 1,000,000 | 1,000 | 960 | L2 |
| Deep-Image-96 | 9,990,000 | 10,000 | 96 | cosine |

## 4. 构建

| 数据集 | nlist | bits | build s | build RSS MB | 写表 s |
|---|---:|---:|---:|---:|---:|
| SIFT-128 | 256 | 7 | 15.5 | 515 | 3.1 |
| GloVe-200 | 1024 | 7 | 55.5 | 804 | 12.9 |
| GIST-960 | 256 | 7 | 53.5 | 1040 | 42.3 |
| Deep-Image-96 | 1024 | 7 | 153.6 | 1447 | 56.1 |

构建读写都经过 MinIO；与 local-file 页面对比可以看存储层的影响。

## 5. 缓存结果

每格为 **QPS / first ms / RSS MB / recall**；`first ms` 为首次查询（冷启动），
QPS 为之后稳态。

### SIFT-128

**index-only**

| cache | L1 / L2 | 单查（QPS / first ms / RSS MB / recall） | batch（QPS / first ms / RSS MB / recall） |
|---|---|---|---|
| off |  | 4.0 / 340 / 383 / 0.975 | 39.0 / 256 / 384 / 0.975 |
| L1 only | 8GiB / L2 0 | 339.3 / 313 / 387 / 0.975 | 3002.7 / 4 / 389 / 0.975 |
| L2 only | L1 0 / 16GiB | 3.9 / 338 / 384 / 0.975 | 39.4 / 252 / 386 / 0.975 |
| L1+L2 512MiB | 512MiB / 512MiB | 300.3 / 311 / 386 / 0.975 | 3082.3 / 4 / 388 / 0.975 |
| L1+L2 1GiB | 1GiB / 1GiB | 278.7 / 987 / 384 / 0.975 | 3171.7 / 4 / 386 / 0.975 |
| L1+L2 8GiB | 8GiB / 16GiB | 363.9 / 318 / 384 / 0.975 | 2757.8 / 4 / 387 / 0.975 |

**exact**

| cache | L1 / L2 | 单查（QPS / first ms / RSS MB / recall） | batch（QPS / first ms / RSS MB / recall） |
|---|---|---|---|
| off |  | 2.8 / 427 / 582 / 1.000 | — |
| L1 only | 8GiB / L2 0 | 4.0 / 399 / 562 / 1.000 | — |
| L2 only | L1 0 / 16GiB | 3.1 / 620 / 447 / 1.000 | 29.2 / 334 / 453 / 1.000 |
| L1+L2 512MiB | 512MiB / 512MiB | 11.9 / 597 / 640 / 1.000 | 177.7 / 68 / 770 / 1.000 |
| L1+L2 1GiB | 1GiB / 1GiB | 14.5 / 641 / 775 / 1.000 | 311.4 / 34 / 901 / 1.000 |
| L1+L2 8GiB | 8GiB / 16GiB | 14.6 / 609 / 812 / 1.000 | 1793.8 / 7 / 815 / 1.000 |

### GloVe-200

**index-only**

| cache | L1 / L2 | 单查（QPS / first ms / RSS MB / recall） | batch（QPS / first ms / RSS MB / recall） |
|---|---|---|---|
| off |  | 2.0 / 654 / 568 / 0.971 | 39.0 / 544 / 571 / 0.971 |
| L1 only | 8GiB / L2 0 | 180.0 / 615 / 564 / 0.971 | 1257.4 / 17 / 568 / 0.971 |
| L2 only | L1 0 / 16GiB | 2.0 / 664 / 569 / 0.971 | 38.7 / 516 / 571 / 0.971 |
| L1+L2 512MiB | 512MiB / 512MiB | 69.4 / 625 / 661 / 0.971 | 1269.7 / 19 / 663 / 0.971 |
| L1+L2 1GiB | 1GiB / 1GiB | 151.8 / 661 / 565 / 0.971 | 1254.0 / 18 / 568 / 0.971 |
| L1+L2 8GiB | 8GiB / 16GiB | 161.2 / 643 / 567 / 0.971 | 1235.3 / 17 / 568 / 0.971 |

**exact**

| cache | L1 / L2 | 单查（QPS / first ms / RSS MB / recall） | batch（QPS / first ms / RSS MB / recall） |
|---|---|---|---|
| off |  | 1.4 / 940 / 632 / 0.988 | — |
| L1 only | 8GiB / L2 0 | 13.3 / 889 / 1413 / 0.988 | — |
| L2 only | L1 0 / 16GiB | 1.6 / 1009 / 639 / 0.988 | 27.3 / 713 / 645 / 0.988 |
| L1+L2 512MiB | 512MiB / 512MiB | 5.7 / 1000 / 1050 / 0.988 | 12.1 / 1619 / 1303 / 0.988 |
| L1+L2 1GiB | 1GiB / 1GiB | 10.9 / 1013 / 1037 / 0.988 | 123.9 / 172 / 1447 / 0.988 |
| L1+L2 8GiB | 8GiB / 16GiB | 11.0 / 1012 / 1417 / 0.988 | 916.3 / 26 / 1420 / 0.988 |

### GIST-960

**index-only**

| cache | L1 / L2 | 单查（QPS / first ms / RSS MB / recall） | batch（QPS / first ms / RSS MB / recall） |
|---|---|---|---|
| off |  | 2.0 / 811 / 1104 / 0.974 | 20.1 / 492 / 1107 / 0.974 |
| L1 only | 8GiB / L2 0 | 160.9 / 794 / 1099 / 0.974 | 1678.4 / 7 / 1101 / 0.974 |
| L2 only | L1 0 / 16GiB | 2.0 / 831 / 1106 / 0.974 | 20.2 / 488 / 1107 / 0.974 |
| L1+L2 512MiB | 512MiB / 512MiB | 16.2 / 811 / 1327 / 0.974 | 1617.0 / 239 / 1330 / 0.974 |
| L1+L2 1GiB | 1GiB / 1GiB | 56.0 / 803 / 1103 / 0.974 | 1730.2 / 7 / 1105 / 0.974 |
| L1+L2 8GiB | 8GiB / 16GiB | 164.0 / 805 / 1097 / 0.974 | 1731.5 / 7 / 1100 / 0.974 |

**exact**

| cache | L1 / L2 | 单查（QPS / first ms / RSS MB / recall） | batch（QPS / first ms / RSS MB / recall） |
|---|---|---|---|
| off |  | 0.4 / 2489 / 1407 / 0.990 | — |
| L1 only | 8GiB / L2 0 | 0.5 / 2449 / 1353 / 0.990 | — |
| L2 only | L1 0 / 16GiB | 1.5 / 1157 / 1170 / 0.990 | 16.2 / 619 / 1180 / 0.990 |
| L1+L2 512MiB | 512MiB / 512MiB | 2.4 / 1152 / 1671 / 0.990 | 5.7 / 1868 / 1909 / 0.990 |
| L1+L2 1GiB | 1GiB / 1GiB | 3.3 / 1660 / 1772 / 0.990 | 35.1 / 223 / 2016 / 0.990 |
| L1+L2 8GiB | 8GiB / 16GiB | 6.2 / 1136 / 1982 / 0.990 | 1027.2 / 13 / 1985 / 0.990 |

### Deep-Image-96

**index-only**

| cache | L1 / L2 | 单查（QPS / first ms / RSS MB / recall） | batch（QPS / first ms / RSS MB / recall） |
|---|---|---|---|
| off |  | 14.9 / 374 / 287 / 0.971 | 16.5 / 612 / 386 / 0.971 |
| L1 only | 8GiB / L2 0 | 15.7 / 405 / 867 / 0.971 | 108.8 / 103 / 930 / 0.971 |
| L2 only | L1 0 / 16GiB | 15.1 / 370 / 285 / 0.971 | 16.3 / 620 / 360 / 0.971 |
| L1+L2 512MiB | 512MiB / 512MiB | 15.4 / 395 / 612 / 0.971 | 15.3 / 678 / 1404 / 0.971 |
| L1+L2 1GiB | 1GiB / 1GiB | 16.0 / 393 / 868 / 0.971 | 118.4 / 97 / 969 / 0.971 |
| L1+L2 8GiB | 8GiB / 16GiB | 15.9 / 907 / 869 / 0.971 | 119.0 / 104 / 922 / 0.971 |

**exact**

| cache | L1 / L2 | 单查（QPS / first ms / RSS MB / recall） | batch（QPS / first ms / RSS MB / recall） |
|---|---|---|---|
| off |  | 0.7 / 1852 / 2598 / 0.989 | — |
| L1 only | 8GiB / L2 0 | 1.4 / 1748 / 2542 / 0.989 | — |
| L2 only | L1 0 / 16GiB | 4.3 / 1520 / 589 / 0.989 | 14.5 / 702 / 690 / 0.989 |
| L1+L2 512MiB | 512MiB / 512MiB | 4.3 / 1531 / 1292 / 0.989 | 4.2 / 2357 / 1992 / 0.989 |
| L1+L2 1GiB | 1GiB / 1GiB | 4.3 / 1551 / 1441 / 0.989 | 57.0 / 192 / 1743 / 0.989 |
| L1+L2 8GiB | 8GiB / 16GiB | 4.3 / 2206 / 1873 / 0.989 | 141.1 / 101 / 1967 / 0.989 |

### 缓存观察

- SIFT index-only：cache off 4.0 QPS；L1-only 339；L1+L2 最高 364。
- GloVe：cache off 2.0 QPS；L1-only 180；L1+L2 161（nprobe=448 访问 cluster 多）。
- GIST：cache off 2.0 QPS；L1-only 161；L1+L2 164。
- Deep-Image 走 lazy index：cache off 14.9 QPS，L1-only 15.7，L1+L2 15.9；
  nprobe 选中的 cluster 工作集占主导，L2 收益小。
- exact 是 L2 的主战场：SIFT exact batch 8GiB 时 1794 QPS；
  GIST 1027；GloVe 916；Deep 141。

### 无缓存 exact batch

`pk_locator` 修改后，`LAKESOUL_VECTOR_CACHE=0` 的 native exact batch 不再报
`could not open data file`：它构建一次性 key index，并直接从数据文件读取/解码候选块。
本地 SIFT nprobe=64、NQ=5：**15.7 QPS，recall 1.0**。
MinIO 的 cache-off exact batch 在修改后没有重新测，因此表中该行为 `—`。

## 6. 接口

目标配置，L1+L2 8GiB。DataFusion batch 是 SQL 循环；Python 是 native batch；
pgvector 是独立服务进程。

### SIFT-128

| 接口 | rerank | 单查 QPS | batch QPS | 服务端 RSS MB |
|---|---|---:|---:|---:|
| python | index | 363.9 | 2757.8 | — |
| python | exact | 14.6 | 1793.8 | — |
| datafusion | index | 427.9 | 496.2 | — |
| datafusion | exact | 14.2 | 381.8 | — |
| pgvector | index | 392.7 | 452.4 | 312 |
| pgvector | exact | 32.2 | 178.3 | 850 |

### GloVe-200

| 接口 | rerank | 单查 QPS | batch QPS | 服务端 RSS MB |
|---|---|---:|---:|---:|
| python | index | 161.2 | 1235.3 | — |
| python | exact | 11.0 | 916.3 | — |
| datafusion | index | 192.5 | 214.0 | — |
| datafusion | exact | 11.4 | 210.5 | — |
| pgvector | index | 201.4 | 222.4 | 484 |
| pgvector | exact | 24.2 | 102.0 | 1472 |

### GIST-960

| 接口 | rerank | 单查 QPS | batch QPS | 服务端 RSS MB |
|---|---|---:|---:|---:|
| python | index | 164.0 | 1731.5 | — |
| python | exact | 6.2 | 1027.2 | — |
| datafusion | index | 170.9 | 182.5 | — |
| datafusion | exact | 5.7 | 154.0 | — |
| pgvector | index | 208.8 | 229.2 | 1035 |
| pgvector | exact | 1.8 | 28.7 | 3853 |

### Deep-Image-96

| 接口 | rerank | 单查 QPS | batch QPS | 服务端 RSS MB |
|---|---|---:|---:|---:|
| python | index | 15.9 | 119.0 | — |
| python | exact | 4.3 | 141.1 | — |
| datafusion | index | 16.2 | 105.1 | — |
| datafusion | exact | 4.3 | 100.0 | — |
| pgvector | index | 16.0 | 100.5 | 791 |
| pgvector | exact | 1.8 | 11.4 | 3795 |

## 7. 结论

1. 数据/索引放 MinIO 后，三级缓存区别明显：
   SIFT index-only cache off 4.0 QPS、L1-only 339、L1+L2 最高 364。
2. exact 最依赖 L2：L1+L2 下 exact batch 达到 141–1794 QPS；
   无缓存 reader fallback 约 15 QPS。
3. index-only 必须有 L1；L2-only 不缓存已加载 index。
4. MinIO 构建受存储限制：SIFT 15.5s、GloVe 55.5s、GIST 53.5s、Deep 153.6s；
   build RSS 0.5–1.45GB。
5. 目标配置均达到 recall≥0.97，且 nprobe 是向上扫描得到的最小值；
   GloVe 使用 nlist=1024,nprobe=448，不再使用 nlist=4096,nprobe=2048。

## 8. 原始结果

- [minio.jsonl](/benchmark-data/vector-index-minio-2026-11-11/minio.jsonl)

本地文件系统的完整参数扫描保留在
[17-vector-index-benchmark-local-file.md](17-vector-index-benchmark-local-file.md)。
