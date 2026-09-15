# 向量索引基准测试

LakeSoul 的向量检索基于 **IVF+RaBitQ** 索引，并在数据写入过程中增量维护：新向量以不可变的
*delta segment* 追加；当累积的增量表明某个簇的聚类中心已不能代表其数据时，可基于该 shard 的
全部数据文件执行**重建**（重新训练 k-means，并以新的索引 generation 发布）。本文档说明用于
量化该行为的基准测试套件、在标准 ANN 数据集上的测试结果，以及每项测试反映出的结论。

基准测试由一个自包含的 Rust 场景运行器实现
（`rust/lakesoul-datafusion/benches/vector_rebuild_bench.rs`），由
`script/benchmark/vector/run.sh` 编排，`script/benchmark/vector/plot.py` 负责绘图。

## 测试目标

| # | 实验 | 回答的问题 |
|---|------|-----------|
| **E1** | 策略 × drift 的质量/成本 | 随着增量数据按不同分布累积，recall@k 如何变化？重建相对其成本何时划算？ |
| **E2** | 全量构建扩展性 | 构建时间、峰值内存、索引磁盘大小随向量数、维度、`nlist` 如何变化？ |
| **E3** | 逐簇 vs 整 shard 触发 | 逐簇漂移检测是否比旧的整 shard `delta/base` 比值更早、更准确地触发？ |
| **E4** | 不同索引状态的检索 | 全新索引、累积 delta 的索引、刚重建的索引，在 recall 与 QPS 上有何差异？ |
| **E5** | DataFusion SQL 端到端 | 用 SQL `INSERT` 写入数据、再用 `ORDER BY array_distance(...) LIMIT k`（索引取候选 + 精确精排）检索时，QPS 与 recall 如何？ |

## 测试环境

| 项目 | 取值 |
|------|------|
| 机器 | Linux，32 核 CPU，62 GB 内存，本地 NVMe SSD |
| 构建 | `cargo bench` release profile，16 个工作线程（`RAYON_NUM_THREADS=16`） |
| 存储 | 本地文件系统；所有场景的 LakeSoul 表数据都以 **vortex** 格式写入（`PhysicalFormat::Vortex`；SQL 场景通过 `file_format` 表选项选择） |
| 距离度量 | L2 |
| 索引配置 | `nlist = 256`、`total_bits = 7`、`top_k = 10`、检索 `nprobe = 64`（E4 扫描 1–256） |
| 每次 checkpoint 查询数 | 100 |
| 随机种子 | 42（固定，工作负载可复现） |

### 数据集

| 数据集 | 维度 | Base | 查询 | Ground truth | 更新池 |
|--------|------|------|------|--------------|--------|
| GIST1M | 960 | 1,000,000 | 1,000 | 100-NN | `gist_learn` 500K 向量 |
| GloVe-200d | 200 | 1,183,514 | 10,000 | 100-NN | base 向量（无单独 learn 集） |

GIST 向量未归一化；GloVe-200d 向量已单位化，因此 L2 排序等价于 angular 排序。
两个都是标准 ANN 基准数据集，运行器直接读取本地 `fvecs`/`ivecs` 文件，不进行下载。

## 测试方法

### 工作负载

E1/E3/E4 使用 **100,000 条 base 向量**，随后进行 **10 轮 × 10,000 条更新**（10 步内增长
100%）。每一轮把新向量写成一个 LakeSoul vortex 数据文件，然后调用生产环境的
`auto_build_vector_index` 策略（传入新文件与表的全部有效文件），与一次写入提交完全一致。

### 重建策略

| 策略 | 行为 |
|------|------|
| `none` | 仅追加 delta segment（`rebuild_mode: "none"`）。 |
| `auto` | 真实生产策略：shard 内**任一簇**满足 `delta_vectors / base_vectors > max_delta_ratio`（默认 `1.0`）即重建。 |
| `periodic` | 增量更新，每 3 轮强制重建一次。 |
| `always` | 每轮强制重建（质量上界、成本最差）。 |

`periodic`/`always` 也通过同一条 `auto_build_vector_index` 调用（使用极小的
`max_delta_ratio`）实现，不测任何独立代码路径。

### 更新（drift）模式

每轮向量按以下模式之一采样：

| 模式 | 说明 |
|------|------|
| `uniform` | 从更新池均匀采样——分布不变。 |
| `skew` | 从距离随机 anchor 最近的 5% 池向量中采样——增长集中在很小的区域。 |
| `shift` | 池向量沿随机单位方向平移 `3.0 × 平均向量模长`——整体分布发生移动。 |

默认情况下，**checkpoint 查询跟随与更新相同的 drift**（共享 skew 区域/平移方向，使用不同的
随机流），因此 recall 反映的是数据变化后用户实际发起的查询；`--static-queries` 可切换回数据集
固定的查询以作对比。

### 指标

- **recall@10** — 返回的 top-10 与对索引中当前全部向量（base + 所有 delta）暴力精确
  top-10 的交集比例。
- **QPS** — 批量检索吞吐；**p50/p99** — 单线程单查询延迟。
- **build / update time** — 全量构建、增量 delta flush、重建的墙钟时间。
- **peak RSS** — 进程峰值内存（`VmHWM`）。
- **index size** — `_vector_index/` 下的磁盘字节数。
- **drift 信号** — manifest 中读取的逐簇最大 `delta/base` 比与 shard 级 `delta/base` 比。
- **generation** — manifest 代数；发生增长即表示执行了重建。

## 测试结果

### E1 — 重建策略与更新分布

**目标。** 弄清重建何时划算：随着 delta 累积，recall 是否退化？更便宜的策略（逐簇 `auto`、
周期性）能否达到 always-rebuild 的质量？

**方法。** 对每个 `(策略, drift)` 组合运行 100K base + 10 × 10K 更新。每个 checkpoint
（GloVe 每轮、GIST 每两轮，因为 GIST 的精确 GT 计算更贵）用跟随 drift 的查询测 recall@10。

**GIST1M — `shift`（分布移动）：**

| 策略 | 最小 recall@10 | 最终 recall@10 | 重建次数 | 索引更新耗时 |
|------|---------------:|---------------:|---------:|-------------:|
| `none` | 0.769 | 0.769 | 0 | 3.6 s |
| `auto@1.0` | **0.929** | **0.938** | 3 | 10.2 s |
| `periodic`（每 3 轮） | 0.786 | 0.933 | 3 | 10.7 s |
| `always` | 0.927 | 0.938 | 5 | 15.5 s |

![E1 GIST shift](/img/vector-benchmark/e1_recall_gist_shift.png)

**结论。** 当分布真正移动时，纯增量维护会损失约 16 个 recall 点（0.77 vs 0.93），只有重建
能恢复。默认 `max_delta_ratio = 1.0` 的逐簇 `auto` 策略以 **3 次重建、约 always 的 2/3
索引耗时**达到 always 的质量——首次重建在某簇的 delta 超过其 base 时立即触发，此时 recall
尚未大幅下降。

**各 drift 的最小 recall@10（关键策略）：**

| 数据集 | Drift | `none` | `auto@1.0` | `periodic` | `always` |
|--------|-------|-------:|-----------:|-----------:|---------:|
| GIST1M | uniform | 0.966 | 0.966 | 0.962 | 0.969 |
| GIST1M | skew | 0.903 | 0.897 | 0.902 | 0.897 |
| GIST1M | shift | 0.769 | **0.929** | 0.786 | 0.927 |
| GloVe-200d | uniform | 0.880 | 0.880 | 0.881 | 0.881 |
| GloVe-200d | skew | 0.741 | 0.604 | 0.708 | 0.740 |
| GloVe-200d | shift | 0.868 | 0.890 | 0.890 | 0.890 |

![E1 GIST skew](/img/vector-benchmark/e1_recall_gist_skew.png)
![E1 质量 vs 成本](/img/vector-benchmark/e1_quality_cost_gist.png)

**结论。**
- **均匀增长**：不重建 recall 依然很高，重建没有收益、只增加成本（`max_delta_ratio ≥ 1`
  时 `auto` 基本保持 0 次重建）。
- **偏斜增长**：更新集中在已有簇覆盖的区域时无需重训；逐簇重建增加成本却无 recall 收益
  （甚至可能略降，因为数据本身没有移动，重训聚类中心反而引入方差）。
- **分布移动**：重建是必要的，逐簇 `auto` 是成本/质量折中的最佳选择。

### E2 — 全量构建扩展性

**目标。** 量化 `auto`/手动重建所执行的全量构建成本随数据规模、维度与 `nlist` 的变化。

**方法。** 在各数据集的子集上做 fresh 构建（使用数据集自带 ground truth，避免暴力计算）。
时间为 `build()` 墙钟时间，峰值为进程 RSS 高水位，索引大小为磁盘总量。

| 数据集 | 向量数 | nlist | 构建时间 | 峰值 RSS | 索引大小 |
|--------|-------:|------:|---------:|---------:|---------:|
| GIST1M (960d) | 100,000 | 256 | 2.3 s | 3.3 GB | 86 MB |
| GIST1M (960d) | 300,000 | 256 | 4.6 s | 5.7 GB | 254 MB |
| GIST1M (960d) | 1,000,000 | 256 | 18.2 s | 13.4 GB | 845 MB |
| GIST1M (960d) | 1,000,000 | 1024 | 60.8 s | 14.1 GB | 849 MB |
| GloVe-200d | 100,000 | 256 | 0.7 s | 0.4 GB | 26 MB |
| GloVe-200d | 1,000,000 | 256 | 4.2 s | 2.6 GB | 256 MB |
| GloVe-200d | 1,000,000 | 1024 | 8.8 s | 2.7 GB | 257 MB |
| GloVe-200d | 1,000,000 | 4096 | 36.0 s | 2.7 GB | 263 MB |

![E2 GIST 构建](/img/vector-benchmark/e2_build_gist.png)
![E2 GloVe 构建](/img/vector-benchmark/e2_build_glove.png)

**结论。**
- 构建时间大致随向量数与 `nlist` 线性增长（k-means 迭代占主导：GIST 1M 从
  `nlist=256` 的 18 s 增至 `nlist=1024` 的 61 s）。索引大小主要由量化码决定，随 `N` 和
  维度线性增长。
- 内存主要由待处理的原始向量决定；1M × 960 维峰值约 13–14 GB。亿级 shard 的重建应按
  离线/后台任务规划这样的资源预算。
- 这组数字同时给出了 E1 的**成本侧**：GIST 1M 一次重建需要几十秒，因此"只在真正漂移时
  触发"（E3）非常重要。

### E3 — 逐簇触发 vs 整 shard 触发

**目标。** 自动重建的触发条件从整 shard 的 `delta/base` 比值改为**任一簇**超过
`max_delta_ratio`。新规则提前了多少？触发时的 recall 是多少？

**方法。** 用 `none` 策略跑相同的流（不重建），每轮记录逐簇最大 `delta/base` 比与 shard
级比值；对每个阈值给出两种规则首次触发的轮次及该轮 recall。

GIST1M 摘要：

| Drift | 阈值 | 逐簇触发 | 该轮 recall | Shard 级触发 | 该轮 recall |
|-------|-----:|---------:|------------:|-------------:|------------:|
| shift | 1.0 | 第 1 轮 | 0.813 | 10 轮内从未（≤ 0.6） | – |
| shift | 0.5 | 第 1 轮 | 0.813 | 第 6 轮 | 0.793 |
| skew | 1.0 | 第 2 轮 | 0.968 | 从未 | – |
| uniform | 1.0 | 第 6 轮 | 0.982 | 从未 | – |

![E3 GIST 触发](/img/vector-benchmark/e3_trigger_gist.png)
*逐簇（红）与 shard 级（蓝）漂移信号，以及 recall（绿）随轮次的变化。*

**结论。** 分布移动时，shard 级比值整轮运行都低于 0.6——旧规则**永远不会**触发，即使
recall 已降到约 0.8；逐簇规则在前几轮即触发，这正是 E1 中 `auto` 策略接近 `always`
质量的原因。均匀增长下逐簇规则触发很晚（阈值 1.0 时第 6 轮），因此不会在健康数据上造成
不必要的重建。

### E4 — 不同索引状态的检索

**目标。** 确认累积 delta（以及重建）不会以意外方式损害检索质量或吞吐。

**方法。** 先在 base 数据上构建索引，然后对三种状态做完整 `nprobe` 扫描
（1, 4, 16, 64, 128, 256）：**fresh**（仅 base）、**delta**（6 轮 × 5K 均匀更新、
不重建）、**rebuilt**（同样数据强制重建）。Ground truth 对每种状态索引中实际存在的
向量暴力计算。

| 数据集 | 状态 | 索引加载 | 最佳 recall@10 | 该点 QPS |
|--------|------|---------:|---------------:|---------:|
| GIST1M | fresh | 57 ms | 0.977 (@nprobe 128) | 19,359 |
| GIST1M | delta | 217 ms | 0.971 (@nprobe 64) | 21,377 |
| GIST1M | rebuilt | 71 ms | 0.971 (@nprobe 128) | 14,691 |
| GloVe-200d | fresh | 25 ms | 0.970 (@nprobe 256) | 17,318 |
| GloVe-200d | delta | 89 ms | 0.949 (@nprobe 256) | 12,647 |
| GloVe-200d | rebuilt | 31 ms | 0.951 (@nprobe 256) | 12,662 |

![E4 GIST recall vs QPS](/img/vector-benchmark/e4_recall_qps_gist.png)

**结论。**
- 三种状态的检索质量与吞吐相当；delta segment 不会破坏 recall/QPS 折中（GloVe 的小幅
  下降在重建后恢复）。
- 累积 delta 的可见成本是**索引加载时间**（6 代 delta 后：GloVe 25 → 89 ms、GIST
  57 → 217 ms），因为打开索引时需要读取并合并所有 segment。重建把 delta 折叠回单个 base
  segment，消除了这部分开销。
- 为什么多 segment 时打开索引更慢、以及相应优化：每个 segment 的最后一个 FastScan batch 会
  补零到 32 个向量，因此合并必须逐向量解包并重打包编码（全新构建/刚重建的索引不需要）。
  加载器现在对单 segment 簇直接复用、对已按 32 对齐的段直接拼接、其余在读取各簇的同时按
  batch 并行重打包。相比最初实现，**delta 较多的索引打开提速 4–5×**（GIST 1.31 s → 0.27 s），
  **全新索引最多约 12×**（GIST 0.69 s → 0.06 s）。
- 由于检索在各状态下都能正常工作，重建可以独立于查询服务进行调度；reader 在元数据
  catalog 解析到新 commit 时切换到新 generation。

### E5 — DataFusion SQL 端到端（写入 + 检索）

**目标。** 度量完整的 SQL 链路：用带向量索引属性的 `CREATE TABLE` 建表，用
`INSERT ... SELECT` 写入向量（DataFusion sink + 提交后的自动索引维护），再用
`ORDER BY array_distance(vec, ARRAY[...]) LIMIT k` 检索。该链路由索引（候选 id）加
**DataFusion 中对候选行的精确精排**组成，因此测试的是集成行为而非孤立的索引。该场景需要
PostgreSQL 元数据服务。

**方法。**
- `CREATE EXTERNAL TABLE ... OPTIONS ('vector_index_columns' ..., 'file_format'
  'vortex')` 同时声明索引与写入格式；先从一个注册在独立 catalog 的内存表插入 10 万条 base
  向量，随后再执行 10 轮、每轮 1 万条的均匀向量 `INSERT`（共写入 20 万行）。表属性的重建
  策略在这些 SQL 写入过程中生效。
- 检索为每个查询执行一条 SQL（包含 SQL 规划），`nprobe = 64`；recall 以全部已写入向量的
  精确 top-10 为基准，并用 `EXPLAIN VERBOSE` 校验 `LakeSoulVectorSearchExec`。

| 数据集 | 写入行数 | base 插入 | recall@10 | QPS | 平均延迟 | p99 | 索引（当前 generation） |
|--------|---------:|----------:|----------:|----:|---------:|----:|-------------------------|
| GloVe-200d | 200,000 | 1.3 s（另 10 轮，中位 0.3 s） | 0.899 | 38.9 | 25.7 ms | 29.2 ms | 1 分片，108 MB，19 万 base + 1 万 delta，gen 2 |
| GIST1M (960d) | 200,000 | 4.7 s（另 10 轮，中位 0.7 s，含重建轮） | 0.980 | 23.3 | 43.0 ms | 64.4 ms | 1 分片，432 MB，17 万 base + 3 万 delta，gen 3 |

以上数字已包含下文的索引缓存、单分片修复与主键行级定位。同一工作负载若写成 **parquet**，在早前一轮
（尚未引入索引缓存与分片修复时）recall 相同的情况下测得 GloVe 3.73 QPS / 268 ms、
GIST 1.03 QPS / 967 ms —— 即每次 SQL 查询 vortex 约快 1.9×（GloVe）/ 3.1×（GIST）。

**索引缓存。** 进程级缓存按 `(object store, 索引前缀)` 保留已合并的内存索引：只要
manifest 仍解析到同一 commit 就直接复用；重建或增量提交会发布新 manifest，下一次查询即
加载新 generation 并替换缓存。同一 E5 工作负载开/关缓存的对比（recall 相同，单 hash 分桶）：

| 数据集 | 当前索引大小 | 关闭缓存 | 开启缓存 | 加速 |
|--------|-------------:|---------:|---------:|-----:|
| GloVe-200d | 107 MB | 7.04 QPS / 142.0 ms | 24.74 QPS / 40.4 ms | 3.5× |
| GIST1M (960d) | 432 MB | 3.19 QPS / 313.6 ms | 15.72 QPS / 63.6 ms | 4.9× |

缓存以字节预算为上限（`LAKESOUL_VECTOR_INDEX_CACHE_BYTES`，默认 512 MiB，`0` 表示禁用），
按加权 LRU 淘汰。

**主键行级定位。** 索引缓存之后，剩余开销就是候选扫描：下推的 `pk IN (...)` 需要逐行扫过
所有文件（GloVe 约 28 ms、GIST 约 47 ms），而随机的候选 id 让 zone map 剪枝失效。两个改
动消除了这部分开销：

- **候选定位器。** 进程级、按文件维护 `pk -> 行号` 映射，`pk = v` 与 `pk IN (...)`（包括
  向量检索注入的候选列表）通过 vortex 行号索引只取匹配行。映射在首次访问时从文件主键列
  惰性构建，key 只用文件 location —— 数据文件不可变，条目不会失效（compaction/重写产生
  新 location 即新条目）。候选集合上限 10,000；parquet 文件与非整数主键自动回退普通扫描。
  这是通用的主键下推，不限于向量检索。预算：`LAKESOUL_PK_CACHE_BYTES`（默认 256 MiB，
  `0` 禁用）。
- **向量列使用更小行块。** vortex 按行块粒度随机读取，因此写入时对表属性
  `vector_index_columns` 中声明的列使用 1024 行/块（默认 8192），取 100 个分散候选的读取
  量约为 1/8；对这些宽列的全列扫描也略有加速。

两者叠加后执行时间从 34.3 ms 降到 20.4 ms（GloVe）、53.5 ms 降到 31.3 ms（GIST）：候选
取行（缓存热）约 4 ms（GloVe）/ 7 ms（GIST），且只读取候选行。

![E5 SQL 端到端](/img/vector-benchmark/e5_sql_end_to_end.png)

**结论。**
- **端到端功能正确**：`EXPLAIN VERBOSE` 命中 `LakeSoulVectorSearchExec`，SQL 检索的
  recall 与索引级测量一致（GloVe 0.90、GIST 0.98）；"候选 → 精排"路径返回候选中的精确
  top-k。
- **SQL 写入会维护索引**：每次 `INSERT` 提交数据文件后，提交钩子会增量更新或重建索引；
  10 轮更新后 manifest generation 分别达到 2（GloVe）和 3（GIST）。
- **候选扫描曾是主要瓶颈，现已提速约 50×。** 查询计划是
  `Filter(pk IN candidates)` 压在 `MergeParquetExec` 之上，而 merge 之前会把**每个数据文件
  的所有行**读出并合并后才过滤（GIST 20 万行每查询约 1.7 s）。修复需要两点：为向量检索
  reader 打开 `file_filter_pushdown`（只有开启该选项，`supports_filters_pushdown` 才会把 pk
  过滤判定为 Inexact 下推），并把注入过滤构造成单个 `pk IN (...)` 而不是一串 `OR`——OR 链会
  让 vortex 的过滤下推卡死，IN 列表则既廉价又能下推。过滤进入每个文件的扫描后，merge 只需
  处理约 100 行候选：扫描从约 1.7 s 降到约 10–30 ms。
- **索引打开曾是最主要的剩余开销，现已被缓存消除。** 经过 E4 的加载器优化后，每次查询
  仍需重新打开并合并索引分片（GloVe 约 0.10 s、GIST 约 0.27 s），而候选扫描仅约
  6–30 ms。上文的进程级缓存消除了这部分开销；叠加下文的单分片修复与主键定位器后，
  E5 达到 38.9 QPS（GloVe）/ 23.3 QPS（GIST）。
- **SQL DDL 曾静默忽略 `hashBucketNum`。** DataFusion 会把 `OPTIONS` 的 key 转成小写并为
  不带命名空间的 key 加上 `format.` 前缀，因此 Spark/Flink 大小写写法
  `'hashBucketNum' '1'` 实际变成 `format.hashbucketnum`，provider 回退到默认 4 个分桶：
  每次查询要探测 4 个索引分片并扫描 4 倍数据文件。现已正确解析（SQL 测试会断言落库属性）；
  对本负载单分桶也是最快配置（GloVe 24.7 QPS，4 分桶为 14.1 QPS）。
- **分桶并行读取。** `LakeSoulVectorSearchExec` 原先串行驱动每个 hash 分桶的 reader；各分桶
  的索引与数据文件相互独立，现在每个分桶跑在独立的 scoped 线程上。GloVe 4 分桶表每次查询
  的执行时间从 92.6 ms 降到 61.5 ms。
- **候选扫描这最后一道瓶颈已被行级定位取代。** 原先 `pk IN (...)` 需要逐行扫过每个文件
  （GloVe 约 28 ms、GIST 约 47 ms），因为随机候选 id 无法利用 zone map 剪枝。上文的
  主键定位器改为按文件的 `pk -> 行号` 映射 + 行号取回：扫描本身降到亚毫秒，取行在缓存热
  时约 4 ms（GloVe）/ 7 ms（GIST）。当前每次查询拆分约为：SQL 规划 2–4 ms、物理规划
  3–8 ms、索引探测 6–11 ms，其余为候选取行。
- **写入格式有影响。** SQL sink 此前硬编码仅支持 parquet 的 multipart writer、忽略表的
  `file_format`；现在使用支持多格式的 writer，建表可指定
  `file_format = "vortex"`。在 recall 相同的前提下，每次 SQL 查询 vortex 比 parquet 约快
  1.9×（GloVe）/ 3.1×（GIST），因为 vortex 的候选扫描剪枝更快。
- **索引文件会被垃圾回收**：commit 历史与读租约存放在元数据库的 `vector_index_*` 表中，
  每次写入后，被取代且超过宽限期（`gc_grace_seconds`，默认 1 小时，配置在
  `vector_index_columns` JSON 属性里）且没有活跃租约的 generation 会被清理。另有显式
  `gc_vector_index` API（`LakeSoulTable` 上也可用）用于按需清理，并删除已 drop 分区的
  控制面记录。

## 建议

1. 通用负载保持默认 `rebuild_mode = "auto"`、`max_delta_ratio = 1.0`：均匀增长下不会
   重建；真正的分布移动会在最初几轮更新内被检测到；以约 always 的 2/3 成本达到其质量。
2. 对持续强漂移的负载，降低 `max_delta_ratio`（如 `0.5`），或在低峰期调用手动重建 API。
3. 均匀或轻微偏斜的写入使用 `rebuild_mode = "none"` 即可，完全避免重建成本。
4. 按 E2 表格为重建预留资源（GIST 1M：`nlist=256` 时约 18 s、峰值约 14 GB），并确保触发
   与重建在后台执行。

## 复现基准测试

运行器读取本地 `fvecs`/`ivecs` 数据集；可通过 `DATA_DIR` 或直接向 bench 传路径来调整。

```bash
# 在两个数据集上运行全部实验（JSON 与日志写入已被 gitignore 的
# benchmark-results/ 目录）
DATA_DIR=~/program/opensource/rabitq-rs/data \
  script/benchmark/vector/run.sh all --results /tmp/vector-bench

# 或者单独运行某个实验
script/benchmark/vector/run.sh e1 --results /tmp/vector-bench

# 绘图（uv 会临时拉取 matplotlib）
uv run --with matplotlib python script/benchmark/vector/plot.py \
  --results /tmp/vector-bench
```

`run.sh` 支持 `QUICK=1`（小规模冒烟）与 `THREADS=<n>`（工作线程数）。每次运行会写入一个
包含逐轮指标的 JSON、`logs/` 原始日志，以及 `plots/*.png` 图表。也可以直接调用运行器：

```bash
cargo bench -p lakesoul-datafusion --bench vector_rebuild_bench -- \
  --scenario stream --base gist_base.fvecs --query gist_query.fvecs \
  --learn gist_learn.fvecs --policy auto --max-delta-ratio 1.0 \
  --drift shift --drift-strength 3.0 --rounds 10 --per-round 10000 \
  --work-dir /tmp/vector-bench/work/gist-shift
```
