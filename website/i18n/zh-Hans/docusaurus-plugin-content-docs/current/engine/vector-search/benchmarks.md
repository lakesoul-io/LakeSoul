---
sidebar_position: 4
sidebar_label: 性能基准
---

# 向量检索性能基准

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

本文给出 LakeSoul 向量检索的实测性能：Python 原生入口的最终结果、已发布的大规模范围、相对精确扫描（exact）的收益，以及重建策略实验的结论。配置方法见[配置](configuration.md)，接口见 [API 参考](api.md)。

## 测试方法

| 项目 | 取值 |
|---|---|
| 存储 | MinIO（本地 S3 兼容服务） |
| 缓存 | L1 = 512 MiB（`LAKESOUL_VECTOR_MEMORY_BYTES`），L2 = 1 GiB（`LAKESOUL_CACHE_SIZE`） |
| 分桶 | 4 个 hash bucket |
| 索引 | RaBitQ `total_bits = 7`，`top_k = 100` |
| 查询入口 | Python 原生入口（`table.search` / `table.search_batch`） |
| 单查 | 200 条 query 逐条执行 |
| 批量 | 全量 query 一次原生 batch（SIFT / GloVe 各 10k 条） |
| SIFT-128 | `nlist = 256`，`nprobe = 64` |
| GloVe-200 | `nlist = 1024`，`nprobe = 512` |

- **单查 QPS / 首查 / p50**：200 条查询的吞吐、第一条查询（冷缓存）延迟、单查中位延迟；
- **batch QPS**：一次原生批量调用处理全部 query 的吞吐（原生入口复用同一份快照、cluster 和 FastScan block）；
- **recall**：返回 top-k 与对当前全部向量做精确 top-k 的交集比例；
- 下表为安静窗口（无其它负载）的最终测量。

## 最终结果（Python 原生入口）

| 数据集 | rerank | nlist | nprobe | 单查 QPS | 首查 | p50 | batch QPS | recall |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| SIFT-128 | index | 256 | 64 | **204.0** | 320 ms | 3.30 ms | **4,382.6** | 0.9747 |
| SIFT-128 | exact | 256 | 64 | 56.3 | 662 ms | 10.49 ms | 1,403.8 | 0.9997–0.9999 |
| GloVe-200 | index | 1024 | 512 | **54.4** | 644 ms | 7.21 ms | **1,195.4** | 0.9718 |
| GloVe-200 | exact | 1024 | 512 | 20.5 | 1,065 ms | 17.44 ms | 658.3 | 0.9917–0.9919 |

## 已发布的大规模范围

更大规模（GIST-960、Deep-Image-96 等）在相同方法下的已发布范围，用于判断量级和跨数据集趋势；不同轮次/环境的数值会有小幅波动：

| 数据集 | 单查 index | 单查 exact | batch index | batch exact | recall（index / exact） |
|---|---:|---:|---:|---:|---|
| SIFT-128 | 206–242 | — | 3,617–4,759 | — | 见上表 |
| GloVe-200 | 56.0–56.8 | — | 1,173–1,227 | — | 见上表 |
| GIST-960 | 9.9–13.0 | 2.4–2.5 | 1,005–1,179 | 35.1–35.3 | 0.9802 / 0.9980 |
| Deep-Image-96 | 17.5–17.6 | 4.5–4.6 | 107–110 | 25.2–25.5 | 0.9503 / 0.9707 |

（SIFT / GloVe 的 recall 见上表：index 约 0.97，exact 约 0.99 以上。）

## 结论

- **index-only vs exact**：index-only 不读向量列，单查约快 2.7–3.6×（SIFT 204.0 vs 56.3，GloVe 54.4 vs 20.5），batch 约快 1.8–3.1×；代价是召回略低（SIFT 0.9747 vs 0.9997–0.9999，GloVe 0.9718 vs 0.9917–0.9919），距离是量化距离。exact 的首查明显更重（SIFT 320→662 ms，GloVe 644→1,065 ms），因为首查要构建候选 block 缓存。
- **batch 收益最大**：原生 batch 一次调用完成整批 query，约是单查的 20 倍以上（SIFT 4,382.6 vs 204.0，GloVe 1,195.4 vs 54.4）；批量离线评估、召回评估优先用原生 batch，而不是循环单查。
- **recall 由 nprobe / total_bits 决定**：表中 index 模式 recall 约 0.95–0.98，exact 重排后 0.99 以上；需要更高召回时优先增大 `nprobe`（延迟线性上升），其次提高 `total_bits` 或重建索引。
- **缓存决定尾延迟**：首查需要从对象存储加载索引段/候选 block（数百毫秒级），热查询进入 L1/L2 后 p50 降到毫秒级；exact 的 L2 预算必须覆盖候选 block 工作集，否则吞吐会骤降。缓存层级与预算见[配置](configuration.md)。

## 重建策略实验（压缩摘要）

索引在写入时增量维护：新向量追加为不可变的 delta segment；`rebuild_mode = auto`（默认 `max_delta_ratio = 1.0`）会在 shard 内**任一 cluster** 的 `delta/base` 超过阈值时，对该 shard 的全部数据文件重新训练并发布新 generation。

**E1 — 重建策略 vs 更新分布。** 100K base + 10 × 10K 更新，三种 drift 模式（`uniform` 均匀、`skew` 集中到小区域、`shift` 整体平移），四种策略对比 recall@10 与成本。GIST1M `shift`：

| 策略 | 最小 recall@10 | 最终 recall@10 | 重建次数 | 索引更新耗时 |
|---|---:|---:|---:|---:|
| `none`（只追加 delta） | 0.769 | 0.769 | 0 | 3.6 s |
| `auto@1.0`（默认） | **0.929** | **0.938** | 3 | 10.2 s |
| `periodic`（每 3 轮） | 0.786 | 0.933 | 3 | 10.7 s |
| `always`（每轮重建） | 0.927 | 0.938 | 5 | 15.5 s |

![E1 GIST shift 的 recall 与重建触发](/img/vector-benchmark/e1_recall_gist_shift.png)

结论：

- **均匀增长**：不重建 recall 依然很高，重建没有收益、只增加成本（`auto` 基本不触发）；
- **偏斜增长**：更新集中在已有簇覆盖的区域，重训质心没有召回收益，甚至引入方差；
- **分布移动**：`none` 损失约 16 个 recall 点（0.77 vs 0.93），重建是必要的；逐簇 `auto@1.0` 以 3 次重建、约 always 的 2/3 索引耗时达到 always 的质量。

**E2 — 全量构建扩展性。** 全量重建/首次构建的墙钟时间、峰值内存与索引大小随数据量、维度、`nlist` 的变化（节选）：

| 数据集 | 向量数 | nlist | 构建时间 | 峰值 RSS | 索引大小 |
|---|---:|---:|---:|---:|---:|
| GloVe-200d | 1,000,000 | 256 | 4.2 s | 2.6 GB | 256 MB |
| GloVe-200d | 1,000,000 | 1024 | 8.8 s | 2.7 GB | 257 MB |
| GIST1M (960d) | 1,000,000 | 256 | 18.2 s | 13.4 GB | 845 MB |
| GIST1M (960d) | 1,000,000 | 1024 | 60.8 s | 14.1 GB | 849 MB |

![E2 全量构建扩展性（GloVe）](/img/vector-benchmark/e2_build_glove.png)

结论：

- 构建时间大致随向量数与 `nlist` 线性增长（k-means 迭代占主导）；索引大小主要由量化码决定，随行数与维度线性增长；
- 峰值内存主要由待处理的原始向量决定（1M × 960 维约 13–14 GB），亿级 shard 的重建应按离线/后台任务规划资源；
- 这组数字同时是 E1 的成本侧：GIST 1M 一次重建需要几十秒，因此"只在真正漂移时触发"很重要——逐簇 `auto` 能在 recall 明显下降前触发。

## 详细基准

更大规模的完整方法与原始数据见：

- [大规模 MinIO 基准（L1 512MiB / L2 1GiB）](benchmark-minio-large-512m1g.md)：SIFT / GloVe / GIST / Deep 的构建、单查、batch、recall 与缓存对比；
- [本地文件大规模基准](benchmark-local-file.md)：本地文件系统上的同规模基准，用于对比对象存储路径。

缓存层级、block 大小与预算的含义见[配置](configuration.md)；接口与查询参数见 [API 参考](api.md)。
