# ANN Benchmarks on LakeSoul vector index (2026-11-10)

## 1. 目的与范围

在 MinIO 上端到端测试 4 个 ann-benchmarks 数据集，覆盖：

- index-only 与 exact 重排；
- 单查询 Python `LakeSoulTable.search`；
- native batch `LakeSoulSession.batch_search`；
- DataFusion SQL 单查询；
- pgvector 兼容服务单查询；
- 不同 `nlist` / `nprobe` / `nbits` 组；
- 每组 QPS 与进程 RSS。

数据文件：

```text
/home/zhibin/ann-benchmarks/deep-image-96-angular.hdf5
/home/zhibin/ann-benchmarks/gist-960-euclidean.hdf5
/home/zhibin/ann-benchmarks/glove-200-angular.hdf5
/home/zhibin/ann-benchmarks/sift-128-euclidean.hdf5
```

## 2. 数据集与距离

| 数据集 | 行数 | dim | 名称距离 | LakeSoul 处理 |
|---|---:|---:|---|---|
| deep-image-96-angular | 9,990,000 | 96 | angular/cosine | train/test L2 归一化，索引/查询用 IP；cosine 排序与 IP 同序 |
| gist-960-euclidean | 1,000,000 | 960 | Euclidean | L2 |
| glove-200-angular | 1,183,514 | 200 | angular/cosine | train/test L2 归一化，IP |
| sift-128-euclidean | 1,000,000 | 128 | Euclidean | L2 |

说明：angular 数据集不能用未归一化的原始向量直接跑 L2；本实验先对 train/test 做
逐行 L2 归一化，再用 inner product 排序（cosine 距离 `1 - dot` 的排序等价）。

## 3. 实验环境与协议

- 16 核 / 60 GiB 内存；MinIO 本地 `127.0.0.1:9000`；PostgreSQL 元数据；
- LakeSoul 4 个 hash bucket；top_k=100、exact_overfetch=1；
- 每个数据集写一张表（4 bucket），用 `build_table_index(..., rebuild=True)` 按配置
  重建完整 index generation；
- SIFT/GloVe/GIST 每配置 200 条真实 query，Deep-Image 100 条（10M 行，exact 较慢）；
- Python/DataFusion：`LAKESOUL_CACHE=1`、cache 8 GiB、L1 block 512 MiB、
  `LAKESOUL_VECTOR_BLOCK_ROWS=256`；
- pgvector 服务：`--worker-threads 8`、cache 2 GiB、L1 block 512 MiB；
- RSS 是 Python 进程 / pgvector 服务进程的 `/proc/<pid>/status` VmRSS；批量数据为
  warm 3 轮 QPS；index-only 默认、exact 显式开启；
- 所有表数据只在 MinIO，无 payload 文件。

参数组：

| 数据集 | 组1 | 组2 | 组3 |
|---|---|---|---|
| SIFT/GloVe/GIST | nlist=256,bits=1,nprobe=8/16/32 | nlist=1024,bits=1,nprobe=16/32/64 | nlist=1024,bits=7,nprobe=16/32/64 |
| Deep-Image | nlist=1024,bits=1,nprobe=16/32 | nlist=4096,bits=1,nprobe=16/32 | nlist=4096,bits=7,nprobe=16/32 |

已记录的 index build 时间（其他配置未单独计时）：

- SIFT：baseline 256/1 12.1 s，1024/1 42.9 s，1024/7 44.7 s；
- GloVe：baseline 17.1 s；
- GIST：baseline 31.0 s；
- Deep-Image：baseline 112.9 s。

## 4. SIFT-128 Euclidean

| nlist | bits | nprobe | index single QPS | index single RSS MB | index batch QPS | index batch RSS MB | exact single QPS | exact single RSS MB | exact batch QPS | exact batch RSS MB |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 8 | 530.2 | 269.8 | 20689.0 | 280.1 | 156.6 | 814.3 | 6215.5 | 819.9 |
| 256 | 1 | 16 | 517.9 | 267.8 | 17117.4 | 278.8 | 153.1 | 815.1 | 5951.5 | 820.9 |
| 256 | 1 | 32 | 494.2 | 268.7 | 14326.8 | 280.0 | 153.2 | 820.2 | 5258.8 | 825.4 |
| 1024 | 1 | 16 | 494.2 | 273.2 | 18283.2 | 284.7 | 145.0 | 825.5 | 6082.0 | 831.7 |
| 1024 | 1 | 32 | 484.3 | 273.6 | 16182.7 | 285.0 | 146.8 | 825.6 | 5916.2 | 830.2 |
| 1024 | 1 | 64 | 457.0 | 273.3 | 10205.9 | 285.8 | 142.4 | 823.8 | 4912.9 | 827.8 |
| 1024 | 7 | 16 | 392.7 | 384.0 | 13086.2 | 394.3 | 136.1 | 927.3 | 4746.1 | 932.7 |
| 1024 | 7 | 32 | 385.1 | 382.5 | 11045.8 | 393.8 | 132.6 | 929.9 | 4878.9 | 934.0 |
| 1024 | 7 | 64 | 379.7 | 381.7 | 8993.5 | 393.1 | 132.6 | 929.9 | 4472.6 | 934.0 |

### DataFusion SQL 单查询（该组最后一行）

| nlist/bits/nprobe | SQL index QPS | SQL exact QPS | SQL exact RSS MB |
|---|---:|---:|---:|
| 1024/1/32 | 590.5 | 414.0 | 826.9 |

## 5. GloVe-200 Angular

| nlist | bits | nprobe | index single QPS | index single RSS MB | index batch QPS | index batch RSS MB | exact single QPS | exact single RSS MB | exact batch QPS | exact batch RSS MB |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 8 | 410.1 | 308.4 | 14716.1 | 319.2 | 78.4 | 1106.3 | 1653.9 | 1698.0 |
| 256 | 1 | 16 | 408.2 | 308.7 | 10396.6 | 319.3 | 80.2 | 1046.4 | 1629.8 | 1865.0 |
| 256 | 1 | 32 | 383.1 | 307.7 | 8778.7 | 318.5 | 80.3 | 1125.0 | 1549.9 | 1969.4 |
| 1024 | 1 | 16 | 394.0 | 314.7 | 11226.7 | 325.0 | 77.1 | 1103.1 | 1633.6 | 1991.5 |
| 1024 | 1 | 32 | 383.4 | 314.4 | 8971.6 | 325.3 | 75.6 | 1215.0 | 1607.4 | 2110.4 |
| 1024 | 1 | 64 | 372.4 | 314.8 | 7816.8 | 326.1 | 76.6 | 1079.3 | 1481.4 | 2095.1 |
| 1024 | 7 | 16 | 348.0 | 552.8 | 7169.9 | 564.5 | 75.9 | 1402.5 | 1386.8 | 2156.3 |
| 1024 | 7 | 32 | 321.5 | 552.0 | 5584.7 | 562.2 | 76.0 | 1330.3 | 1466.1 | 2114.8 |
| 1024 | 7 | 64 | 309.0 | 552.6 | 4171.2 | 564.5 | 75.9 | 1301.0 | 1301.8 | 2179.5 |

### DataFusion SQL 单查询（该组最后一行）

| nlist/bits/nprobe | SQL index QPS | SQL exact QPS | SQL exact RSS MB |
|---|---:|---:|---:|
| 1024/1/32 | 436.2 | 137.2 | 1956.0 |

## 6. GIST-960 Euclidean

| nlist | bits | nprobe | index single QPS | index single RSS MB | index batch QPS | index batch RSS MB | exact single QPS | exact single RSS MB | exact batch QPS | exact batch RSS MB |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 256 | 1 | 8 | 228.8 | 378.6 | 5860.0 | 394.2 | 24.6 | 1473.9 | 87.5 | 2097.3 |
| 256 | 1 | 16 | 215.3 | 380.0 | 4648.9 | 395.7 | 24.6 | 1372.6 | 87.7 | 2118.8 |
| 256 | 1 | 32 | 200.9 | 383.8 | 3249.1 | 399.1 | 23.8 | 1521.8 | 86.5 | 2175.3 |
| 1024 | 1 | 16 | 219.6 | 397.8 | 4223.1 | 412.2 | 24.5 | 1472.5 | 88.0 | 2116.5 |
| 1024 | 1 | 32 | 217.2 | 397.4 | 3853.1 | 411.6 | 24.3 | 1758.4 | 86.5 | 2496.5 |
| 1024 | 1 | 64 | 205.5 | 397.9 | 2794.5 | 414.0 | 24.3 | 1502.4 | 86.4 | 2063.7 |
| 1024 | 7 | 16 | 25.6 | 1340.5 | 670.8 | 1354.2 | 4.8 | 2532.2 | 77.5 | 3234.9 |
| 1024 | 7 | 32 | 23.9 | 1343.7 | 537.2 | 1355.0 | 4.9 | 2380.6 | 78.0 | 3044.3 |
| 1024 | 7 | 64 | 23.2 | 1340.4 | 628.7 | 1355.1 | 5.0 | 2364.4 | 76.5 | 3110.0 |

### DataFusion SQL 单查询（该组最后一行）

| nlist/bits/nprobe | SQL index QPS | SQL exact QPS | SQL exact RSS MB |
|---|---:|---:|---:|
| 1024/1/32 | 239.6 | 30.1 | 2053.7 |

## 7. Deep-Image-96 Angular

| nlist | bits | nprobe | index single QPS | index single RSS MB | index batch QPS | index batch RSS MB | exact single QPS | exact single RSS MB | exact batch QPS | exact batch RSS MB |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1024 | 1 | 16 | 2.5 | 1178.6 | 123.1 | 1181.9 | 1.9 | 2035.5 | 99.2 | 2677.1 |
| 1024 | 1 | 32 | 2.5 | 1426.2 | 124.2 | 1429.7 | 1.9 | 2241.9 | 100.8 | 2876.9 |
| 4096 | 1 | 16 | 1.7 | 1449.2 | 85.7 | 1457.8 | 1.5 | 2223.2 | 71.3 | 2876.5 |
| 4096 | 1 | 32 | 1.7 | 1448.6 | 85.2 | 1457.1 | 1.5 | 2265.5 | 72.4 | 2876.3 |
| 4096 | 7 | 16 | 0.7 | 2272.8 | 45.7 | 2281.8 | 0.6 | 3129.4 | 41.4 | 3751.5 |
| 4096 | 7 | 32 | 0.7 | 2272.4 | 45.5 | 2281.5 | 0.6 | 3093.8 | 41.5 | 3710.2 |

### DataFusion SQL 单查询（该组最后一行）

| nlist/bits/nprobe | SQL index QPS | SQL exact QPS | SQL exact RSS MB |
|---|---:|---:|---:|
| 4096/1/32 | 1.7 | 1.0 | 3098.4 |

## 8. pgvector 兼容服务单查询（当前 index 为 bits=7 组）

| dataset | nlist/bits | nprobe | index QPS | exact QPS | service RSS start/index/exact MB |
|---|---:|---:|---:|---:|---:|
| sift | 1024/7 | 32 | 636.9 | 184.6 | 365.7 / 366.5 / 855.8 |
| glove | 1024/7 | 32 | 498.1 | 22.0 | 856.1 / 1248.5 / 2081.4 |
| gist | 1024/7 | 32 | 26.0 | 0.6 | 2081.2 / 5557.2 / 6088.4 |
| deep | 4096/7 | 32 | 0.5 | 0.3 | 6088.4 / 13261.0 / 14374.2 |

## 9. 主要观察

1. **index-only 单查询**：SIFT 约 230–530 QPS，GloVe 约 300–410 QPS，GIST 约 23–229 QPS，Deep 约 0.7–2.5 QPS；主要受维度和数据量影响。
2. **native batch 明显高于逐 query**：SIFT index batch 约 9k–20.7k QPS，GloVe 约 4.2k–14.7k，GIST 约 0.6k–5.9k，Deep 约 45–124 QPS。
3. **exact 重排显著更慢且 RSS 更高**：尤其 GIST-960 与 Deep-Image；GIST exact batch 约 76–92 QPS、RSS 2.0–3.1 GB；Deep exact 更低。
4. **bits=7 的 index 明显更大、更慢**：GIST 1024/7 单查询 index 仅 ~23–26 QPS，RSS 1.34 GB，而 bits=1 约 200+ QPS；bits 提升 recall 的代价需要按数据规模评估。
5. **nprobe 增大通常降 QPS**，但可提高候选召回；本报告只记录 QPS/RSS，未计算 recall。
6. **pgvector 单查询**：SIFT/GloVe ~500–640 QPS index、GIST ~26 QPS、Deep ~0.45 QPS；exact pgvector 在 GIST/Deep 上非常慢（~0.27–0.57 QPS），服务 RSS 可达 5.5–14.4 GB。
7. **Deep-Image 10M** 的 exact/pgvector 是明显的资源瓶颈；大批量优先 index-only / native batch，exact 只在必要查询使用。

## 10. 复现

原始数据：`results.jsonl`（本目录），每行一个 run，包含 first latency、QPS、RSS。
脚本在 `scripts/`：

```bash
cd python
# 需要先准备 MinIO/PostgreSQL，并修改 common.py 的 bucket/credentials
PYTHONPATH=src .venv/bin/python ../rust/lakesoul-vector/ann-benchmarks/scripts/prepare.py sift
PYTHONPATH=src .venv/bin/python ../rust/lakesoul-vector/ann-benchmarks/scripts/build.py sift 1024 1
PYTHONPATH=src .venv/bin/python ../rust/lakesoul-vector/ann-benchmarks/scripts/bench.py sift 1024 1 32 sql
```

pgvector：

```bash
./rust/target/release/postgres-lakesoul --port 6543 --worker-threads 8   --warehouse-prefix s3://lakesoul-bench/ --endpoint http://127.0.0.1:9000   --s3-bucket lakesoul-bench --s3-access-key minioadmin --s3-secret-key minioadmin   --s3-virtual-host-style=false
PG_SERVICE_PID=<pid> python scripts/pgbench.py sift 32
```

## 11. 备注

- 本实验是一轮参数扫描，不包含 recall@100；QPS 与 index 参数、cache 状态、机器负载相关；
- exact 的 block cache 是 warm 状态；冷查询会更慢；
- 每次切换 index 配置都会重建 index generation，切换后 block cache 可能保留旧候选块；
- Deep-Image 的 NQ=100，其他数据集 NQ=200；批量 QPS 为 3 轮 warm 平均；
- pgvector 结果使用当时最后的 bits=7 index 配置（SIFT/GloVe/GIST nlist=1024、
  Deep nlist=4096）；pgvector 服务是在**同一个进程内顺序**跑完四个数据集，前一个
  数据集的 index/block cache 可能仍占用内存，Deep 的服务端 RSS/QPS 受此影响，不能
  当作 fresh-process 数据；pgvector 服务二进制为当时已有的 `f464e5be` 构建。
