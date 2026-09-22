# 文本索引 Benchmark

LakeSoul 的全文检索基于 **倒排索引**（Tantivy），随数据写入增量维护。每次写入都会在数据文件之外生成不可变的索引 *delta*；compaction 将它们合并成新的 split，并清除被删除行的索引项。检索时先取每个 split 的 BM25 top-k，按主键合并候选，再对候选行的当前文本做**精确校验**后才返回。本文记录用于量化该行为的 benchmark 套件、在真实检索语料上的实测结果，以及每个实验的结论。

Benchmark 由一个自包含的 Rust 场景执行器
（`rust/lakesoul-datafusion/benches/text_index_bench.rs`）实现，由
`script/benchmark/text/run.sh` 编排，数据准备在
`script/benchmark/text/download.py`，出图在
`script/benchmark/text/plot.py`，网关场景在
`script/benchmark/text/gateway_bench.py`。

## Benchmark 回答的问题

| # | 实验 | 回答的问题 |
|---|------|-----------|
| **T1** | 重建策略 × 更新模式 | 追加、同主键重写、删除、主题漂移会不会让检索退化？什么时候重建 shard 才划算？ |
| **T2** | 全新构建 | 分词器（`jieba` / `default` / `en_stem`）、位置索引与原文存储如何影响构建速度、索引大小与质量？构建如何扩展到百万级文档？ |
| **T3** | 重建触发 | 生产环境的漂移触发条件何时命中？漂移能否预测质量下降？ |
| **T4** | 按索引状态检索 | 全新索引、含累积 delta 的索引、刚重建的索引，在候选数扫描下的延迟与召回差异？ |
| **T5** | Shard 数量 | 索引拆分到更多 shard 后，构建时间与查询延迟的代价是什么？ |
| **T6** | 端到端 DataFusion SQL | 通过 SQL 路径写入的吞吐，以及 `text_match` + `text_score` 查询延迟如何？ |
| **T7** | 端到端 ES 网关 | 通过 Elasticsearch 兼容 HTTP API 的批量写入与 `_search` 表现如何？ |

## 测试环境

| 项目 | 值 |
|------|----|
| 机器 | Linux，32 核 CPU，62 GB 内存，本地 NVMe SSD |
| 构建 | `cargo bench` release，16 个工作线程 |
| 存储 | 本地文件系统；表数据为 Parquet，每个 hash bucket 一个文件 |
| 分词器 | 除专门对比外均为 `jieba` |
| 索引配置 | `with_positions = true`，`stored = false` |
| 检索 | BM25，按分数全局 top-k，开启精确校验，每 shard 100 个候选 |
| 指标 | 基于数据集 qrels 的 nDCG@10、Recall@100、MRR@10；以及相对精确单索引 BM25 基线的 Recall@10 |
| 随机种子 | 42（固定，负载确定） |

### 数据集

| 数据集 | 文档数 | 有标注查询 | 用途 |
|--------|-------:|-----------:|------|
| SciFact (BEIR) | 5,183 | 339 | 冒烟测试（`run.sh quick`） |
| MS MARCO | 8.8M | 7,437 dev | 英文主实验；取 100K/300K/1M/2M 文档子集 |
| T2Retrieval | 118,605 | 22,812 dev | 中文实验（取前 100K 文档） |

`download.py` 将数据流式转换为 `corpus.jsonl` / `queries.jsonl` /
`qrels.tsv`（来自 Hugging Face 的 MTEB 镜像），Rust 执行器本身不访问网络。执行器会把 qrels 限制在已加载子集内的文档上，并只评估在子集内至少有一个已标注文档的查询，使子集上的指标仍然有意义（MS MARCO 100K 子集中有 34 个查询带标注）。

## Benchmark 的工作方式

### 负载

增量实验使用 **100,000 篇基础文档** 和 **10 轮 × 10,000 篇更新**。每轮把新文档写成 LakeSoul 数据文件，然后以新文件加表内活跃文件调用生产环境的 `auto_build_text_index` 策略，与一次写入提交完全一致。

### 更新模式

| 模式 | 说明 |
|------|------|
| `append` | 新主键——语料增长。 |
| `rewrite` | 用新文本重写已有主键——语料规模不变，索引累积新版本。 |
| `delete` | 删除此前写入的文档（CDC 墓碑）——语料收缩。 |
| `topic-shift` | 新文档取自语料中不同主题区域——词汇逐渐偏离查询。 |

### 重建策略

| 策略 | 行为 |
|------|------|
| `none` | 只做增量 delta（`rebuild_mode: "none"`）。 |
| `auto@r` | 生产策略：当 shard 的 `delta / base` 文档比超过 `max_delta_ratio = r` 时重建。 |
| `periodic (3)` | 增量维护，每 3 轮强制重建一次。 |
| `always` | 每轮强制重建（成本上界）。 |

`periodic` 与 `always` 也通过同一 `auto_build_text_index` 调用（把 `max_delta_ratio` 设得极小）表达，因此测量的是同一条代码路径。

### 指标

- **nDCG@10 / MRR@10 / Recall@100**：基于当前活跃行对应 qrels 的检索质量。
- **Recall@10 vs 精确 BM25**：索引结果与「同一批活跃行上的精确单索引 BM25」的一致度；低于 100% 说明候选有损失，等于 100% 说明索引排序与精确 BM25 排序一致。
- **p50 / p95 / p99、QPS**：走生产 scan 路径的查询延迟与吞吐。
- **构建/更新时间**：全新构建、单次增量索引维护、单次重建的墙钟时间；用 **docs/s** 归一化。
- **峰值 RSS**：进程峰值内存（`VmHWM`）。
- **bytes/doc 与索引大小**：`_text_index/` 下的 bundle 字节数。
- **漂移信号**：shard catalog 的 `delta / base` 文档比。
- **丢弃的陈旧候选（dropped stale）**：被精确校验丢弃的候选数。

## 结果

### T1 — 重建策略 × 更新模式

**目标**：确认增量维护的文本索引是否会随版本累积而质量下降，以及能否用更便宜的策略代替重建。

**方法**：100K 基础 + 10 × 10K 更新，每轮都基于 qrels 测质量（相对精确 BM25 的 recall@10 与 nDCG@10）。

| 策略 | 重建次数 | 索引维护耗时（总计） | 最低 recall@10 vs BM25 | 最终 nDCG@10 | 最终 recall@100 | 索引大小 |
|------|--------:|---------------------:|-----------------------:|-------------:|----------------:|--------:|
| `none` | 0 | 1.3 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `auto@1.0` | 0 | 1.2 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `auto@2.0` | 0 | 1.3 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `auto@0.5` | 1 | 3.6 s | 100.0% | 0.337 | 82.4% | 56 MB |
| `auto@0.25` | 2 | 7.7 s | 100.0% | 0.336 | 82.4% | 77 MB |
| `periodic (3)` | 3 | 9.2 s | 100.0% | 0.336 | 82.4% | 98 MB |
| `always` | 5 | 13.5 s | 100.0% | 0.336 | 82.4% | 141 MB |

十轮共 100K 文档的增量索引维护总计只花 **约 1.3 s**；一次重建约 2.5 s 与 33 MB。重建**并不会**提升检索质量：增量索引始终与精确 BM25 排序一致（相对基线 recall@10 100%），因为校验会用当前文本重查每个候选。默认策略 `auto@1.0` 在该负载下从不触发，也不产生任何成本；更激进的策略带来 3–10 倍的维护成本和 2–4 倍的磁盘占用，却让 nDCG@10 略降——重建后文档统计量变小，反而损失了质量。

**更新模式（auto@1.0）：**

| 模式 | 重建次数 | 索引维护耗时（总计） | 最低 recall@10 vs BM25 | 最终 nDCG@10 | 最终 recall@100 | 索引大小 |
|------|--------:|---------------------:|-----------------------:|-------------:|----------------:|--------:|
| `append` | 0 | 1.2 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `rewrite` | 0 | 1.2 s | 100.0% | 0.378 | 82.4% | 34 MB |
| `topic-shift` | 0 | 1.2 s | 100.0% | 0.350 | 82.4% | 34 MB |
| `delete` | 0 | 0.0 s | 90.6% | 0.644 | 82.4% | 33 MB |

追加、同主键重写与主题漂移全程不丢候选。只有删除模式出现回落（最后一轮 90.6%：此时 90% 的语料已被删除，每个查询的 100 个候选中约 89 个是被校验丢弃的陈旧版本）；批量删除后做一次重建可以清掉这些条目、恢复完整候选余量。注意 nDCG 随语料缩小而升高——干扰文档变少，BM25 本身变简单了。

![T1 策略与成本](/img/text-benchmark/t1_msmarco_policy_recall_cost.png)

### T2 — 全新构建

**分词器、位置与原文存储（MS MARCO 100K）：**

| 分词器 | 位置 | 存储原文 | 构建 docs/s | bytes/doc | 构建耗时 | nDCG@10 | Recall@100 | p50 |
|--------|------|---------|------------:|----------:|--------:|--------:|-----------:|----:|
| `jieba` | 是 | 否 | 39,921 | 343.2 | 2.5 s | 0.350 | 82.4% | 8.1 ms |
| `jieba` | 否 | 否 | 44,526 | 227.1 | 2.2 s | 0.350 | 82.4% | 9.2 ms |
| `jieba` | 是 | 是 | 34,496 | 535.1 | 2.9 s | 0.350 | 82.4% | 8.0 ms |
| `default` | 是 | 否 | 93,087 | 269.7 | 1.1 s | 0.354 | 85.3% | 7.3 ms |
| `default` | 否 | 否 | 79,027 | 215.9 | 1.3 s | 0.354 | 85.3% | 7.5 ms |
| `en_stem` | 是 | 否 | 44,747 | 261.3 | 2.2 s | **0.368** | 82.4% | 8.4 ms |
| `en_stem` | 否 | 否 | 48,904 | 208.5 | 2.0 s | **0.368** | 82.4% | 8.4 ms |

- 英文质量最好的是 `en_stem`（nDCG 0.368，`default` 0.354、`jieba` 0.350），构建比 `default` 慢 30–50%。
- **位置索引**占索引体积的 15–30%，用于短语查询；只需要词项匹配时可以关闭。
- **存储原文**每篇约 190 字节，对检索路径没有收益（校验文本来自数据文件）；除非客户端直接从索引读原文，否则应关闭。
- `jieba` 处理英文要付分词成本却没有质量收益——分词器应按语言选择。

**中文（T2Retrieval，前 100K 文档）：**

| 分词器 | 构建耗时 | docs/s | bytes/doc | nDCG@10 | Recall@100 | p50 | QPS |
|--------|--------:|-------:|----------:|--------:|-----------:|----:|----:|
| `jieba` | 20.1 s | 4,968 | 2,497 | **0.642** | **80.5%** | 29.9 ms | 30 |
| `default` | 42.4 s | 2,356 | 2,760 | 0.016 | 1.6% | 0.9 ms | 397 |

中文必须使用 `jieba`：`default` 会把整段连续中文当成一个词项，几乎匹配不到东西（它更快只是因为这种超长词项的索引与检索都便宜）。

**规模扩展（MS MARCO，`jieba` + 位置）：**

| 文档数 | 构建耗时 | docs/s | 数据写入 | bytes/doc | 索引大小 | 峰值 RSS | nDCG@10 | Recall@100 | p50 | QPS |
|-------:|--------:|-------:|--------:|----------:|--------:|--------:|--------:|-----------:|----:|----:|
| 100,000 | 2.5 s | 39,921 | 0.3 s | 343.2 | 33 MB | 552 MB | 0.350 | 82.4% | 8.1 ms | 94 |
| 300,000 | 9.0 s | 33,262 | 0.8 s | 338.8 | 97 MB | 1.3 GB | 0.279 | 76.5% | 9.7 ms | 72 |
| 1,000,000 | 30.3 s | 32,953 | 1.9 s | 334.4 | 319 MB | 3.4 GB | 0.202 | 67.6% | 12.1 ms | 45 |
| 2,000,000 | 63.3 s | 31,605 | 3.6 s | 332.7 | 635 MB | 6.9 GB | 0.190 | 58.8% | 17.1 ms | 26 |

索引稳定在每篇 300 字节出头（约为原始 Parquet 文本的三分之一）；构建时间与内存线性增长（2M 文档 63 s、峰值 6.9 GB）。检索延迟从 100K 到 2M 翻倍。质量随语料增大而下降是因为 MS MARCO 的标注相对更大的文档池更稀疏——索引没有丢候选（相对精确 BM25 的 recall@10 始终 100%）。

![T2 参数矩阵](/img/text-benchmark/t2_params_matrix.png)

![T2 构建规模](/img/text-benchmark/t2_msmarco_scaling.png)

### T3 — 重建触发

**目标**：生产环境的漂移信号何时触发？它能否预测质量下降？

| 模式 | 首次 `delta/base >= 1.0` 的轮次 | 实际重建轮次 |
|------|-------------------------------:|------------:|
| `append` | 10 | — |
| `rewrite` | 10 | — |
| `topic-shift` | 10 | — |

每轮 10% 增长时，**写入量**比值到第 10 轮才到达 1.0（shard 内 100K base + 100K delta），本轮结束时还没有超过默认阈值，因此没有触发重建。该比值统计的是写入文档数：`rewrite` 和 `delete` 的活跃语料不增长，比值却同样累积，`topic-shift` 改变词汇但完全不改变比值。对文本索引而言这是一个**成本启发式**（约束 delta 构建量），而不是质量警报：精确校验让检索质量与索引陈旧程度无关。

![T3 漂移与重建](/img/text-benchmark/t3_msmarco_trigger.png)

### T4 — 按索引状态检索

**目标**：在全新索引、累积三轮 delta 的索引、刚重建的索引三种状态下，扫描每 shard 候选数，测读路径。

三种状态都是 130,000 活跃文档：`fresh`（一次构建）、`delta`（100K 基础 + 30K 追加，7 个 split）、`rebuilt`（同一批数据经生产重建，1 个 split）。

| 状态 | 候选/shard | Recall@10 vs 精确 BM25 | Recall@100 | p50 | QPS | 校验 |
|------|----------:|-----------------------:|-----------:|----:|----:|-----:|
| fresh | 10 | 100.0% | 61.8% | 4.7 ms | 196 | 3.3 ms |
| fresh | 20 | 100.0% | 73.5% | 4.5 ms | 220 | 3.3 ms |
| fresh | 40 | 100.0% | 73.5% | 5.2 ms | 186 | 3.8 ms |
| fresh | **100** | 100.0% | 82.4% | 8.2 ms | 126 | 5.7 ms |
| fresh | 200 | 100.0% | 82.4% | 12.5 ms | 82 | 9.1 ms |
| delta | 10 | 100.0% | 61.8% | 4.5 ms | 196 | 3.0 ms |
| delta | 100 | 100.0% | 82.4% | 8.5 ms | 114 | 5.7 ms |
| delta | 200 | 100.0% | 82.4% | 13.0 ms | 74 | 8.3 ms |
| rebuilt | 10 | 100.0% | 58.8% | 5.0 ms | 188 | 3.3 ms |
| rebuilt | 100 | 100.0% | 82.4% | 8.5 ms | 117 | 6.0 ms |
| rebuilt | 200 | 100.0% | 82.4% | 16.7 ms | 64 | 11.3 ms |
| fresh，关闭校验 | 100 | 100.0% | 82.4% | **2.6 ms** | 335 | — |

- 在**任意**候选数下索引排序都与精确 BM25 一致（候选数取 10 时 recall@10 vs 精确 BM25 仍为 100%）：BM25 排序足够局部化，按 split 取 top-k 再合并即可复现。
- Recall@100 在约 100 个候选时饱和，正好是生产默认值；取 200 没有收益，延迟却多约 50%。
- 校验每查询 3–6 ms（对扫描到的候选行重建一个内存小索引），但能让陈旧的索引项对客户端不可见；关闭后查询快 3 倍。只有在陈旧命中可接受时才应关闭。
- 刚重建的索引检索**并不更快**（本例略慢）：一次重建的价值在于压缩 split、清除已删除条目的索引项，而不是加速查询。

![T4 索引状态](/img/text-benchmark/t4_msmarco_search_states.png)

![T4 校验开销](/img/text-benchmark/t4_msmarco_verify_cost.png)

### T5 — Shard 数量

**目标**：同样 100K 文档分布到 1、4、16、64 个 shard（hash bucket），每 shard 100 候选，测构建与检索。

| Shards | 构建耗时 | docs/s | 索引大小 | Splits | nDCG@10 | Recall@100 | p50 | QPS |
|-------:|--------:|-------:|--------:|-------:|--------:|-----------:|----:|----:|
| 1 | 2.8 s | 36,163 | 33 MB | 1 | 0.350 | 82.4% | 9.6 ms | 100 |
| 4 | 2.9 s | 34,903 | 37 MB | 4 | 0.352 | 82.4% | 21.1 ms | 44 |
| 16 | 4.2 s | 24,049 | 41 MB | 16 | 0.314 | 82.4% | 67.9 ms | 14 |
| 64 | 22.9 s | 4,371 | 45 MB | 64 | 0.319 | 82.4% | 236.8 ms | 4 |

查询延迟几乎随 shard 数线性增长——每个 shard 都要检索、取候选并校验，候选预算还按 shard 数倍增（每 shard 100）。64 个 shard 时构建吞吐骤降，因为每个 shard 有独立 writer，Tantivy 的索引开销按 split 计。按 split 的 BM25 统计量还让合并排序略偏（nDCG 0.314–0.352 对单 shard 的 0.350）。shard 数按写入并行度取尽可能少；分片不是加速检索的手段。

![T5 Shard 数量](/img/text-benchmark/t5_msmarco_shards.png)

### T6 — 端到端 DataFusion SQL

**目标**：走生产写入与查询路径（Spark 风格 SQL）：`INSERT` 基础与增量批次，然后用 `text_index_columns` 表选项声明索引，执行
`SELECT ... WHERE text_match(content, '...') ORDER BY text_score(content, '...') DESC LIMIT 10`。

| 测量项 | 值 |
|--------|----|
| 基础写入（100K 行） | 3.4 s（29,101 行/s） |
| 增量写入（10K 行） | 0.16 s / 0.14 s |
| 查询 p50 / QPS | 117.7 ms / 8.4 |
| nDCG@10 / Recall@100 | 0.350 / 82.4% |
| 规划器是否使用索引 | 是（用 `EXPLAIN` 验证） |

SQL 路径的质量与 scan API 一致，但延迟约为其 14 倍（117 ms 对 8 ms），因为每次查询都要经过完整的 DataFusion 计划与批处理管线，而不是 scan API 的直接候选路径。对延迟敏感的客户端应用 scan API 取分，用 SQL 做过滤/连接。

![T6 SQL](/img/text-benchmark/t6_sql.png)

### T7 — ES 兼容网关

**目标**：通过 HTTP 驱动 Elasticsearch 兼容网关：批量写入 20K 篇 MS MARCO 文档，执行 `match` 检索，再做两轮各 2,000 篇重写并复测。

| 测量项 | 值 |
|--------|----|
| 批量写入 | 1,161 docs/s（20,000 篇 17.2 s） |
| 检索 p50 / QPS | 347 ms / 3.3 |
| nDCG@10 / Recall@100 | 0.604 / 85.7% |
| 重写第 1 轮 | 2,000 篇，1,271 docs/s；质量不变 |
| 重写第 2 轮 | 2,000 篇，1,240 docs/s；质量不变 |

后续的网关优化又复测了同样的 20K/4-bucket 配置：批量写入约 1.0–1.2K docs/s、检索 p50 约 0.42 s（存在运行间波动）；[网关性能调参](18-es-compatible-gateway.md#性能调参)中记录的 1 bucket 配置写入约 2.9K docs/s、检索 p50 约 76 ms；开启网关的延迟索引维护（deferred）后同一张表写入约 12.9K docs/s，后台追平后检索 p50 约 46 ms。质量与引擎一致（带标注的 MS MARCO 子集较小，因此绝对 nDCG 高于全语料）。每次 HTTP 写入与检索都要付 merge-on-read 加索引维护的成本，默认 4 个 bucket 的布局又把检索延迟按 shard 数放大；网关面向 WeKnora 类客户端的兼容性，而不是延迟优化端点。

![T7 网关](/img/text-benchmark/t7_gateway.png)

## Benchmark 发现的问题

跑完整矩阵的过程中暴露了两个引擎缺陷，均已在本次系列中修复：

1. **Split 构建时后台合并的竞态。** 构建 200 万文档的 shard 时可能失败并报 Tantivy 的 *"segments that were merged could not be found"*：后台 merge 策略在读取 segment 列表与执行强制合并之间退休了这些 segment。现在构建 split 期间禁用后台合并，显式合并看到的是稳定的 segment 列表。
2. **中文查询被解析为短语。** 当字段带位置索引时，Tantivy 会把不含空白的字面量转成短语查询，于是中文整句只能匹配包含该整句的文档。T2Retrieval 的 nDCG 只有 0.038、几乎没有候选；将普通查询文本按分词结果做 OR 组合后升到 0.642（recall@100 80.5%），与独立的 jieba BM25 基线一致。

## 结论

- **让索引累积 delta 即可。** 由于校验是精确的，陈旧索引项不会降低文本检索质量；生产默认 `auto@1.0` 只在远超本文负载的写入量下才重建，不触发时零成本。
- **重建是为了磁盘，不是为了质量。** 重建压缩大量小 split 并清除已删除条目的索引项（删除模式是唯一候选余量下降的模式）；它不会让查询更快，还会因统计量变化微调 nDCG。
- **分词器按语言选择。** 英文用 `en_stem`，中文必须用 `jieba`；除非需要短语查询或直接从索引读原文，否则关闭位置索引与原文存储。
- **少用 shard。** 延迟随 shard 数近似线性增长，按 split 的 BM25 统计量还会让全局排序变近似；每个分区从 1 个 shard 起步，只有写入并行度需要时才增加。
- **候选数 100 是甜点。** Recall@100 在此饱和；更多候选只增延迟，在本文语料上无收益。
- **低延迟用 scan API。** SQL 每查询多约 100 ms 的计划与批处理开销；HTTP 网关是兼容层，不是最快路径。

## 复现

```sh
# 1. 数据集（首次从 Hugging Face MTEB 镜像流式下载）
uv run --with datasets python script/benchmark/text/download.py scifact \
    --out ~/data/lakesoul-text-bench/scifact
uv run --with datasets python script/benchmark/text/download.py msmarco \
    --out ~/data/lakesoul-text-bench/msmarco-100000 --limit 100000

# 2. 场景执行器（需要 PostgreSQL 元数据）
LAKESOUL_PG_URL=... script/benchmark/text/run.sh quick   # SciFact 冒烟（T1-T6）
LAKESOUL_PG_URL=... script/benchmark/text/run.sh all     # 全量矩阵（T1-T7）

# 3. 出图
uv run --with matplotlib python script/benchmark/text/plot.py \
    --results benchmark-results/text-<timestamp>
```

常用场景参数：`--scenario build|search|stream|trigger|sql`、`--limit`、`--query-limit`、`--tokenizer`、`--no-positions`、`--stored`、`--shards`、`--candidates`、`--candidate-sweep`、`--policy`、`--max-delta-ratio`、`--drift`、`--rounds`、`--per-round`、`--work-dir`（配合 `search` 复用已构建的表）与 `--out`（JSON 结果）。

## 参见

- [Python/Daft 文本检索](11-lakesoul-python/08-text-search.md)：API、SQL 接口与排序函数。
- [ES 兼容网关](18-es-compatible-gateway.md)：T7 使用的 HTTP 契约。
- [向量索引 Benchmark](17-vector-index-benchmark.md)：ANN 索引的同类研究（那里陈旧候选**确实**会损失召回）。
