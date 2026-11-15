> 缓存配置已收敛为 5 个公共变量（`LAKESOUL_VECTOR_CACHE`、
> `LAKESOUL_VECTOR_MEMORY_BYTES`、`LAKESOUL_CACHE_PATH`、`LAKESOUL_CACHE_SIZE`、
> `LAKESOUL_VECTOR_LAZY_INDEX`），旧变量仅作兼容；见
> [Warm Cache](warm-cache.md)。

# LakeSoul fresh index-only QPS 分析与向量路径优化方案

> 本方案针对 SIFT-128 fresh index-only 约 12.7 QPS 的 LakeSoul 端到端现象。`LakeSoul 向量索引改进前后基准测试报告.md` 仅参考其线程/测量说明，不照搬其中的核心 QPS 数字、bits/nprobe 或单索引负载。方案只优化向量检索，不改变湖仓表数据、事务、快照、Upsert/Delete、merge-on-read 或其他索引语义。

## 1. Fresh QPS 低的主要原因

已知场景：约 900K base，fresh index，4 buckets，index-only p50 约 78 ms、QPS 约 12.7；此时没有历史 delta/mutation。

### 1.1 当前 index-only 仍经过表读取

Daft 的 `rerank="index"` 分支投影主键和保留的向量距离，不读向量列；但仍执行 `scan.to_daft()` 并收集结果。Native Reader 启动时先做向量候选搜索，接着依候选主键构建并执行 LakeSoul 物理读取计划，再向读出的行附加索引距离。[Daft index-only 分支](../../../../python/src/lakesoul/daft/search.py#L154-L188)；[候选 key 生成](../../../../rust/lakesoul-io/src/reader.rs#L673-L705)；[Reader 物理计划](../../../../rust/lakesoul-io/src/reader.rs#L479-L497)。

因此这里的 index-only 是“**索引距离排序、不回读向量 payload**”，不是“绕过表存储、直接从 ANN 返回主键”。单独运行 `IvfRabitqIndex::search()` 不包含这个回表/合并/Daft collect 部分。

### 1.2 这次基准很可能没有启用 PK locator

向量索引 cache 与 PK locator 所依赖的共享磁盘 cache 是两套配置：

- `LAKESOUL_INDEX_MEMORY_BYTES` 控制内存中的 vector/text index cache（旧名 `LAKESOUL_INDEX_CACHE_BYTES`）；
- PK locator 要求 `LAKESOUL_CACHE` 已设置，且 `LAKESOUL_CACHE_SIZE` 对应的 cache capacity 非零。

[PK locator 的开关](../../../../rust/lakesoul-io/src/pk_locator.rs#L107-L153)；[独立 disk-cache 初始化](../../../../rust/lakesoul-io/src/cache/mod.rs#L68-L102)。Vortex 格式本身不会自动开启 locator。

此前 SIFT 基准记录了 `LAKESOUL_INDEX_MEMORY_BYTES=8GiB`（当时的旧名是 `LAKESOUL_INDEX_CACHE_BYTES`），但没有记录/设置 `LAKESOUL_CACHE`。在所述运行环境 `LAKESOUL_CACHE` 未设置的前提下，`try_build_key_inputs_encoded()` 会在 locator cache gate 处返回 `None`；`file_filter_pushdown` 默认也是关闭的，候选 PK Filter 留在普通扫描/merge 路径。[encoded key locator gate](../../../../rust/lakesoul-io/src/pk_locator.rs#L2056-L2068)；[过滤下推默认值](../../../../rust/lakesoul-io/src/config/mod.rs#L311-L315)；[下推判断](../../../../rust/lakesoul-io/src/session.rs#L833-L863)。

这种路径会读取四个 bucket 文件组中的主键列并参与 merge/过滤，尽管最终只输出少量候选、没有读取向量列。对 900K 行表，这比单纯的内存 ANN 查询多了主要工作量，很可能是 fresh 78ms 的首要来源。**这是根据脚本环境和分支条件的推断；仍应通过 locator profile 或读取字节/行数验证，不应仅凭 QPS 声称其占据了多少毫秒。**

> 如果当时运行进程通过其他启动脚本预设了 `LAKESOUL_CACHE`，则上述 gate 推断不成立；基准应把该变量、`LAKESOUL_CACHE_SIZE`、locator hit/miss 和读取行数字段记录到结果中。

### 1.3 一条 fresh 查询还包含控制面与 Daft 编排

每次 `LakeSoulTable.scan(...).to_daft()` 构建表 schema、当前文件/分区计划，然后 Daft 按 partition/hash bucket 创建读取 task。[Scan config 构建](../../../../python/src/lakesoul/catalog.py#L1694-L1722)；[每个 scan partition 一个 Daft task](../../../../python/src/lakesoul/daft/source.py#L96-L132)。每个 Reader 解析并租用向量 shard，再转换 segment 描述；Python 路径每个 shard 都执行 `resolve_cached()` 和 `acquire_lease()`。[Python resolve/lease](../../../../python/src/index.rs#L151-L187)。

fresh 有 4,096 个段描述。这份元数据的复制和传输会产生额外工作，但数量本身不能证明其占据 78ms 的最大部分。向量索引 cache 命中只说明不再从对象存储加载完整索引，不代表 schema/scan plan、PG commit 检查、lease、segment transport、PK 行读取、Daft merge/collect 均免费。

### 1.4 线程数不是单一参数

这类完整查询中至少有 Daft runner 线程、Daft bucket task、Reader/Tokio worker、单次 ANN 的 probe parallelism 和多请求并发度。原 SIFT 基准是一个 Python worker 顺序发 query，Daft runner 配置 16；这不等于 16 条 ANN 同时执行。参考报告的 Rust QPS 则是内存单索引的纯核心查询，其线程语义和工作范围不同，绝对值不可直接相比。

LakeSoul 内层 `vector_search_parallelism` 默认为 1；只有该值大于 1 且 nprobe 大于 1 时，IVF 才把 probes 分组并行执行。[选项默认值](../../../../rust/lakesoul-io/src/vector/search.rs#L412-L458)；[并行与顺序 probe 分支](../../../../rust/lakesoul-vector/src/rabitq/ivf/mod.rs#L1581-L1605)。faer/Rayon 在 K-means 构建中使用多核，不会自动使每次 search 也运行于所有核心。[K-means Rayon matmul](../../../../rust/lakesoul-vector/src/rabitq/kmeans.rs#L478-L499)。

## 2. 已确认与待测的边界

| 事项 | 当前结论 |
|---|---|
| index-only 是否读向量 payload | 不读向量列；仍需从 LakeSoul 表路径取得候选主键行 |
| PK locator 是否仅靠 Vortex 自动启用 | 否；另需 `LAKESOUL_CACHE` 和有效 disk cache |
| 现有 benchmark 是否肯定启用了 locator | 记录显示只设置向量 index cache；locator 未设置时会关闭。但必须以实际 worker 环境核验 |
| warm index cache 是否还逐 segment 从存储加载 | 否；warm hit 返回缓存索引 Arc |
| warm query 是否仍有 O(segment metadata) 工作 | 是；metadata view clone、segment list transport/转换仍发生。存在两次 JSON Value→typed segment 解码；这是优化目标，不等于已测出其耗时 |
| SQL deferred-payload 是否解释 Daft index-only p50 | 否；那是 DataFusion SQL 的另一执行模式，不适用于当前 Daft 查询 |
| “全部物理 delta files 每条 ANN 分别打开” | 否；索引在缓存 miss/load 时合并 segments 为 per-cluster 内存结构，warm ANN 不逐段打开文件 |
| 元数据工作是否独占了大多数 78ms | 尚无分段 profile，不能下结论 |

## 3. 仅向量检索的低风险优化

### 3.1 已改动：热命中复用解析后的 segment coverage

在 `lakesoul-io` 向量搜索内部，将 segment JSON→typed 解码与 covered-file basename 集合构建移动到 loaded-index cache miss 的 loader。将 mask/index、typed metric/dimension 和 coverage 放进同一个按 vector shard 当前 commit id 缓存的内部 entry。`search_matching_shards()` 使用同一个 `Arc` 执行 ANN 和 coverage 判断，避免每次热查询重复两次 segment 解码和 coverage set 重建。

保持原有 public search API，header/key-kind 验证仍逐请求进行；cache 仍以 commit id 识别更新；mutation 读取失败仍回退不使用索引；无效 segment metadata 仍报错；pending files 仍按当前请求文件列表计算。空 coverage 仍表示 legacy/unknown，不误判成所有当前文件未覆盖。表 snapshot、Reader、租约、持久格式、写入/事务均不改变。

这只移除已确认的重复向量 metadata 工作，**是否显著降低 78ms 需端到端复测**。新增内存 coverage set 已纳入索引 cache 的估算容量。

### 3.1 首次 GloVe DataFusion SQL index-only 实测（fresh，无 Upsert）

通过仓库内的 `rust/lakesoul-datafusion/benches/vector_rebuild_bench.rs --scenario sql`，走真实 DataFusion SQL `INSERT` 建 GloVe 全量 1,183,514 行 Vortex 表并自动建索引，然后执行 200 条官方 test query；`LakeSoulVectorSearchExec(order_by=true)` 已由 explain 核实。数据使用 4 buckets、每 shard `nlist=1024`、`total_bits=7`、`top_k=100`、`search_parallelism=1`。GloVe/angular 向量逐行 L2 归一化，以 L2 index-only 排序（单位向量上和 cosine/angular nearest-neighbor 排序等价）。表数据位于 `/dev/shm`，所以此项是内存文件系统 E2E，不与本地 ext4 的旧测值混算。

| PK locator 配置 | nprobe | QPS | p50 | p99 | DataFusion plan 均值 | physical plan 均值 | executor 均值 | Recall@100 | peak RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 关闭（原运行环境，`LAKESOUL_CACHE` 未设置） | 16 | 17.35 | 56.00 ms | 69.40 ms | 1.28 ms | 1.58 ms | 54.77 ms | 0.63805 | 5,612.5 MiB |
| 开启，`LAKESOUL_CACHE_SIZE=1GiB` | 16 | 23.95 | 41.76 ms | 49.75 ms | 1.27 ms | 1.56 ms | 38.93 ms | 0.63805 | 5,920.6 MiB |
| 开启 | 128 | 23.47 | 41.81 ms | 54.58 ms | 1.29 ms | 1.57 ms | 39.74 ms | 0.83635 | 5,964.1 MiB |
| 开启 | 512 | 22.26 | 44.69 ms | 52.00 ms | 1.28 ms | 1.58 ms | 42.08 ms | 0.95455 | 5,946.4 MiB |

PK locator on/off 是对**缓存设置**的对照，不是 segment-cache 代码更改前后的 A/B。开 locator 后 p50 -25.4%、executor 均值 -28.9%、QPS +38.0%，recall 不变，peak RSS +308 MiB（5.5%）。`LAKESOUL_PK_PROFILE=1` 的四 bucket 记录每条查询定位共 200 个 key / 200 行，并报告 1 个文件；locator 第一次建立的本地 PK 索引每 bucket `cache` 约 28–33 ms，warm `cache` 为几十微秒，而 Vortex `take` 约 16–23 ms。说明 full-table PK 扫描确有贡献，但 warm locator 后每 query 的候选行物化仍要约 19 ms；78/56 ms 不能全部归因给分段元数据。

这些 SQL 运行是**串行 200 个 query**；DataFusion benchmark 的 `--threads=16` / Rayon 设置并不意味着并行提交了 16 个 SQL 查询。locator-on 的 GloVe single-index Rust 内核参考是 1,604 QPS（1 thread）/16,239 QPS（16 OS search workers），与当前 four-shard SQL table scan 的 QPS 不属于同一层。当前端到端 QPS 仍远低于目标。

结果文件：`/tmp/lakesoul-vector-query-opt-20261105/glove-datafusion-sql.json`（locator-off）、`glove-datafusion-pk-on-r2.json`（locator-on nprobe16）、`glove-datafusion-pk-on-np128.json`、`glove-datafusion-pk-on-np512.json`；locator-on per-file 诊断在同目录对应 log。数据库精确 table/path/shard/commit/lease 核验均为 0；locator cache、Vortex table/index 文件和中间 GloVe fvecs 已清理，官方 HDF5、JSON 与日志保留。清理前查询 PG 验证表和索引记录均为零。

### 3.2 下一步建议：先测量、再优化控制面

1. 在同一 fresh 表、同一查询/worker 设置下记录 locator off/on；收集 `LAKESOUL_PK_PROFILE`、locator hit/miss、读文件数/行数/bytes。
2. 对 Daft 计时加入 schema/scan-plan、resolver/lease、segment transport、ANN、PK locator/merge、to_pylist/collect 的阶段记录。对比不启用 Upsert 的 fresh 与 10×10K 后的文件、segment 数和读取行数。
3. 若 locator warm 命中使主表读取大幅缩短，先将其作为单独、正确启用的配置优化，再评审 cache lifecycle/资源预算；不为追求指标而静默改变所有用户默认值。
4. 对 vector shard 的 immutable resolved view / typed segment transport 做向量专属复用，保留 commit ID、schema/key-layout、mutation head/version 和覆盖语义。不要先全局修改 metadata catalog/cache API。
5. 将向量结果的 candidate keys reuse 到后续需要的 payload 路径，避免重复 ANN；必须固定同一表数据 snapshot 和候选排序。先针对纯 index-only，不扩大到普通过滤或多列索引查询。
6. Rust 内核、LakeSoulReader、DataFusion SQL、Daft 端到端分别报告；并发度写清 Python worker、Daft threads、Reader/Tokio threads、ANN parallelism。相同参数下比较，不拿内核 QPS 作为端到端目标值。

`with_parallelism>1` 可降低单查询 probe 时间，但多个并发查询时会造成线程超卖，需单独测量。它不是重复元数据转换和 PK/行读取的替代品。

## 4. 湖仓语义保护

- 不改变事实数据的落盘、schema、分区/哈希桶、事务提交、版本可见性和 merge-on-read。
- 不绕过主表扫描后将旧索引候选当成最终结果；cache metadata 必须针对正确 commit/snapshot。
- 不移除 leases 或 GC 约束。`is_loaded()` 返回布尔值不是 pin，不能单凭该检查认定文件可安全无租约访问。
- 不从旧的 `data_files` coverage 字段假设完整覆盖；未覆盖 tail、删除、稀疏 vector updates 必须保持现有 fallback。
- 向量内部优化发生错误要保持现有错误/回退类别；文本索引和非向量 scan 不改变。
- 不增加新的 SQL/metadata schema 字段作为本阶段必要条件。

## 5. 测试与资源规则

先跑向量搜索相关单元/E2E，再以全新 SIFT table 对照优化前后的 fresh index-only 同一 API、同 query IDs、同 nprobe、同候选数、同缓存状态；然后测更新后。Daft 用例明确是单 worker/16 Daft threads还是多进程；不可把 Daft threads 写成 16 路查询 worker。

SIFT 是首个诊断组；locator off/on 分成不同进程/运行组。若之后运行 GIST/GloVe/Deep，仍按数据集顺序执行，不并行占用同一机器，避免 RSS/页缓存相互污染。

每组结束时停止并确认全部查询子进程退出，释放进程级 vector/disk cache；使用唯一 run-owned table name/path。成功、异常、timeout 均执行 public drop-table，再安全地删除**本组精确路径**，核验 cleanup 状态并保存结果。表清理为 metadata warning 或失败时标记不完整并停止后续测试；不通配清理共享目录、数据库其他表、别的进程缓存或全局 OS cache。原始 HDF5、脚本、源码基线和结果不删。
