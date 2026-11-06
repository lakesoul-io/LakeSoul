# DataFusion 向量 QPS 接近 Rust 内核的方案（第二版）

> 本文件是第一份 [端到端 QPS 分析方案](./端到端向量检索QPS分析与优化方案.md) 的延续，专门说明如何满足“先用 GloVe 验证，index-only、不做 Upsert，并把 QPS 提升到接近纯 Rust”的目标。上一份方案仍保留为诊断和缓存/PK locator 对照记录。实施仍只限向量检索；不改变 LakeSoul 表格式、PG/ACID 提交、快照、merge-on-read、PK 语义或其他索引。

## 0. 结论先行

如果每个向量都单独走一次 `DataFrame.scan().to_daft().collect()`、一次 Reader/ANN、一次结果取回，那么无论把 segment JSON 少解码一次，还是让 Rust/Rayon 多开线程，都不能消除**每向量一次的 PG/plan/Reader/结果物化成本**。已有 GloVe locator 实测从 17.35 提到 23.95 QPS，证明 locator 有用，但与 Rust 内核参考仍有巨大差距；那 38% 是 locator 配置增益，不是代码 A/B 增益。

因此方案提供两种明确工作模式：

| 模式 | 对用户的语义 | 查询准备/PG次数 | 返回路径 | 目标/限制 |
|---|---|---:|---|---|
| 单条 latest 查询 | 每次调用解析执行时当前可见快照 | 仍按调用进行版本校验 | 必须保持现有表可见性/回表语义 | 优化固定成本，但**不承诺接近纯内存 Rust QPS** |
| **DataFusion 批量向量查询（主优化目标）** | 一个 DataFusion statement/批次显式固定一个 LakeSoul snapshot | 每批准备一次、解析/租用四个当前 shard 一次 | coverage 证明成立的 index-only 才可直接输出 `id+distance`；否则原 Reader 回表 | 共享固定工作后，争取接近同一四分片内存 Rust 基线 |

不能把同一快照下 1,000 条 query 的批量吞吐宣传成 1,000 个彼此独立的 latest 事务。API、SQL 文档和 benchmark 都要标出 `snapshot_id / batch_id`。

## 1. 为什么要新增一个 DataFusion 批量 vector operator

当前 Rust SQL benchmark 逐 query 循环调用 `ctx.sql()`、`create_physical_plan()` 和 `collect()`，而且测的是**单线程串行 SQL**；纯 Rust 报告的 16-thread QPS 则是把 1,000 query 分给 16 个 OS threads，对照口径不同。现有 `search_parallelism=16` 只是单条 query 的 probe 并行，**不是 16 条 query 并发**。现有 `LakeSoulVectorSearchExec` 还在 Reader 周围逐 bucket 建配置并读取候选结果。

主优化必须把“重复为每一个 query 构造完整 DataFusion/LakeSoul scan”变成“一份 DataFusion physical plan 处理一个查询批次”，并让 DataFusion executor 调度多条 query 的 ANN。单纯缓存 Reader 对象会复用已消费的 stream，是错误做法；单纯开 16 个 Tokio futures 也不能保证同步 ANN 在 16 个 OS worker 上运行。

## 2. API 与 DataFusion physical plan

新增一条可选入口，例如：

```sql
SELECT * FROM vector_search_batch(
  TABLE lake.default.glove_vectors,
  queries => $queries,             -- Arrow FixedSizeList<Float32>[200]
  query_ids => $query_ids,         -- UInt32，用于恢复输入顺序
  top_k => 100,
  nprobe => 16,
  rerank => 'index',
  snapshot => $snapshot
);
```

具体 SQL 语法由 DataFusion 现有 table-function/table-valued-UDF 扩展方式确定，不把 API 名称当作实现前提。关键契约：

- 输入为 Arrow query batch，不对每个 vector 单独调用 SQL/Reader。
- 返回 `query_id, id, distance` 三列 Arrow 数据；距离按 metric 排序、query_id 保留分组。
- 所有 query 显式绑定同一不可变的 LakeSoul data/index snapshot；SQL statement 结束后释放 snapshot/lease。
- 新入口可选且只针对向量查询；现有 `SELECT ... array_distance ...`、Daft `vector_search()` 和普通 scan 默认行为不变。
- DataFusion 将 query batch 分配给有界的 16 个 native query workers；单条 IVF probe `parallelism=1`，防止 `16 worker × 16 probe threads` 超卖。
- PG catalog resolve、schema/file-group 计划、lease 和 typed segment transport 在批入口准备时只做一次，而不是每个 query 一次。不能跳过 latest 版本检查；减少的是**每个 batch**的重复开销，不是无条件复用陈旧元数据。
- 每个 partition/hash-bucket task 读取当前 vector index shard 一次；内存中保留当前 shard 的 `Arc<IvfRabitqIndex>` 及 mutation mask。多条 query 在同一个 handle 上并行调用现有 `IvfRabitqIndex::search()`。
- 只保留每个 query 每个 shard 的本地 top-k（带确定性 tie-break），然后 DataFusion 合并四个 shard 的结果为全局 top-k；不得收集或拼接完整索引 vectors。

四 bucket 的基础索引仍是四个子索引；批量查询是在这四个子索引上并行执行并全局合并，不重建为一个全表索引。

## 3. index-only 直接返回的严格正确性门槛

当前 index-only 仍需 Reader 按候选 PK 读取表行。直接返回索引中的主键和距离可省去这项 row materialization，但只有证明“本 SQL snapshot 中应出现的向量行，恰好由这个 index commit/mutation state 完整表达”时才允许。

资格校验在每个 shard 的 snapshot handle 创建时做一次，至少要求：

1. query 限定于索引记录的 partition/hash bucket；索引 header key-layout/schema 与目标表完全匹配；
2. table data snapshot 和被解析的 vector index commit 通过现有 catalog 关系可绑定；索引 delta/readiness 覆盖该 snapshot 内所有有效的 vector-bearing 数据文件；无未覆盖 tail；
3. Upsert/Delete/CDC 影响已完整进入相同 commit 的 mutation head/visibility state；不透明 legacy coverage、缺失 mutation 或无法识别的删除一律判定不合格；
4. 无需要表行求值的普通 filter、payload/exact distance、sparse non-vector 更新、或不支持的 multi-column/partition 语义；
5. 使用确定的全局排序和 candidate merge；不把局部 shard top-k 当作全局结果。

有任一条件不能证实时，DataFusion 落回**现有完整 LakeSoulReader/merge-on-read 路径**，结果必须与当前实现一致。不得仅靠 `data_files` 字符串恰好匹配、cache 命中或 TTL 推断 correctness；不为了 fast path 删除 lease、跳过 snapshot/version fencing 或假设无 CDC。

本阶段不为通过资格校验增加数据库 schema 字段。如果当前 catalog metadata 不足以证明某类 snapshot coverage，则这类表暂不启用直接返回，继续使用 fallback。任何需要持久化新的 coverage watermark/跨系统原子提交才能启用更多表的提案，都另立设计审查。

## 4. 数据流与缓存生命周期

```text
DataFusion SQL query batch + explicit LakeSoul snapshot
  │
  ├─ resolve current schema/files/partition+bucket groups once
  ├─ resolve index commit + lease once per shard; check data coverage/version
  ├─ get/load vector-only index Arc and mutation mask once per shard
  │
  └─ 16 bounded native query workers:
       query[0..B] × (4 existing bucket indexes, search_parallelism=1)
          │
          ├─ eligible, no-filter index-only → (query_id, key, score) Arrow batches
          └─ otherwise → unchanged LakeSoulReader PK lookup + MOR + filters
                │
                └─ DataFusion stable global top-k / Arrow output
```

内存策略：

- 使用现有 process-level index cache；16 workers 共享同一进程中的 `Arc` shard entries，不派生 16 份全量 index。
- 缓存覆盖信息只属于 immutable vector-index commit，跟 commit id 一起失效；当前 query 的 active-file set 不放进 commit cache。
- 把新覆盖 HashSet 的估算 bytes 计入 index-cache weight；内存预算达到上限时按 LRU 淘汰并在下个批次按同一版本重新加载。
- batch query buffer 只包含 `B×dim` 输入向量、task state 和最多 `B×4×top_k` 的候选上界；大批量实现流式 `RecordBatch` 分块，不把整份训练表或全量 query 复制进每个 worker。
- 每个 query 完成即释放 query-local LUT/heap/work buffers；index handle 由 snapshot/batch 生命周期持有。
- 对 object-store 文件只在 cache miss/load 时读取；重复 query 不重新读/合并全部 segment 文件。

## 5. 现有兼容优化与本次验证数据

已做的一项低风险 vector-only 优化在 [Rust 向量搜索文件](../../rust/lakesoul-io/src/vector/search.rs)：热 cache hit 复用 typed shard、per-commit covered-file basename 集合，避免重复 segment JSON 解码和 coverage 集合构造；保留 public API、mutation fallback、Reader、写入和 PG/事务语义不变。4 个 focused Rust tests 已通过。

DataFusion/GloVe 的现有单-query fresh locator 对照（200 query，L2-normalized GloVe，四个 full shard，DataFusion SQL `LakeSoulVectorSearchExec(order_by=true)`，无 Upsert）：

| PK locator | nprobe=16 QPS | p50 | p99 | recall@100 | peak RSS |
|---|---:|---:|---:|---:|---:|
| off | 17.35 | 56.00 ms | 69.40 ms | 0.63805 | 5,612.5 MiB |
| on，1 GiB disk-cache limit | 23.95 | 41.76 ms | 49.75 ms | 0.63805 | 5,920.6 MiB |

locator on 是配置对照，不是上面 vector cache code 的收益。locator 的 profile 显示 200 key/文件 fetch，warm 每 query 的 `take` 仍约 16–23 ms。nprobe=128/512 的 locator-on QPS 分别为 23.47/22.26，Recall 为 0.83635/0.95455。locator-off 的 locator 尚未启用不是靠参数名推断，而是此前 worker 的 `LAKESOUL_CACHE` 环境变量缺失；这次 profile 通过显式 cache-on/off worker 环境作区分。

这些单-query SQL 数值仍比 Rust core 低；它们尚非批量 vector operator 的结果，也**不能**证明仅靠 cache patch 就已接近 Rust。现有所有结果和逐 bucket 日志保留在 `/tmp/lakesoul-vector-query-opt-20261105/`；临时表/segments/cache 已按唯一路径清理。

## 6. 分阶段实现与性能门槛

### 阶段 A：完成 cache 优化 A/B

先用相同 Rust `lakesoul-datafusion --bench vector_rebuild_bench --scenario sql`、相同 GloVe normalized fixtures、同一4-bucket新表、200 query/nprobe16/128/512、rerank index，比较归档旧 `vector/search.rs` 与当前 patch。记录 QPS、p50/p99、RSS、locator profile、resolved metadata/segment decode计数。若 QPS无可重复提升或 RSS超出事前给出的增量预算，则只回滚本 patch，不触动 Reader/table实现。

### 阶段 B：DataFusion 批量 index-only operator

开发 query-batch physical execution path，入口解析一个真实 snapshot；逐 shard加载/校验一次；使用有界16 native workers共享 index Arc；eligible index-only 直接返回 Arrow ids/scores；不符合任何 consistency gate 的请求沿旧Reader路径运行。保留 single-query latest API；batch API 明确它的所有 query 共享同一快照，不能谎称每条是独立最新事务。

### 阶段 C：有限实验矩阵

仅用 GloVe-200、初始 fresh 表、L2-normalized angular、index-only、无 Upsert/无 row filter/payload，按顺序执行：

1. pure `lakesoul-vector` Rust 1 worker 与16 worker；
2. 当前 DataFusion SQL/Reader 200 single-query baseline；
3. 阶段 B 的 DataFusion single snapshot batch，16 query workers；
4. locator off/on 和 index-cache冷/热独立对照。

4 shard、每 shard nlist1024 与 single-index Rust core 本身工作量不同。必须增加一个**完全相同四 shard**的Rust matched control（同一nlist/nprobe/query ids/top-k、相同 global merge），再评价“接近 Rust”。不拿不同CPU、不同query数、`nprobe` 或进程并发的QPS相除。每个变体至少做3次交错独立运行并保留所有样本，报告中位数、范围，不只挑最好值。

## 7. 验收条件

- Correctness：与同一冻结 snapshot 的旧 SQL/Reader path 比较每个 query 的 top-100 ID/顺序及 score 容差；覆盖考试重点是 deletes、upserts、mutation fallback、cache commit替换、unsupported-filter fallback。
- Performance：报告用户可见的 batch wall-QPS、每 query p50/p95/p99、pure-core matched four-shard QPS，以及 plan/snapshot-preparation 时间；把 single-query E2E 与 snapshot batch QPS分开标注。
- RSS：报告单 DataFusion进程 VmHWM/PSS、索引常驻、最大batch查询临时buffer；fast path不得把完整 GloVe base copies 或 16份index 载入内存。阶段 B 相对 matched Rust control 的新增常驻RSS预设预算 10%（包含 coverage metadata）；超过预算就缩小/按需重建 coverage，而不是静默扩大cache。
- 不降低nprobe、top-k、查询数、bits或向量精度，不让partial segment coverage query命中fast path；性能未提升时回滚对应向量patch。
- reader/datawriter/catalog其它表行为及现有向量Upsert snapshot/result通过回归用例。第一阶段只对Glove fresh index-only做性能判断。

## 8. 每组测试资源清理

每个GloVe变体独立唯一表名和scratch/cache路径；按顺序执行并留相同代码/配置/fixture hash。完成后等待所有query/build进程退出、释放reader/index Arc/lease；使用公开table drop API清元数据，逐项核验该表名/path、shard、commit、lease计数为0；只删除经过规范化与run-token ownership检查的本组目录和PK disk-cache路径。清理失败则停止后续矩阵并保留cleanup error；不通配删除、不清共享Cargo target、不清HDF5、不全局drop OS caches。任何比较都把locator是否启用、LAKESOUL_CACHE_SIZE/PATH、索引cache额度、CPU/Daft/Rust/Rayon线程、build/index hashes写入JSON/log。

## 9. 第一轮实测、结论与修正后的计划（GloVe-200，index-only）

### 9.1 阶段 A 的 A/B 结果（同一台机器、交错 3 轮、相同 fixture/参数）

对比对象：`vector_rebuild_bench_old`（HEAD 的 `rust/lakesoul-io/src/vector/search.rs`）与 `vector_rebuild_bench_new`（已解码 shard + coverage 复用的 patch）。两二进制都以相同 `RUSTFLAGS='-C target-cpu=native'` 构建，sha256 分别为 `bcf36de6…`、`14d73928…`；GloVe fixture 为 L2-normalize 后 fvecs/ivecs（`3266ce5d…`/`83036e32…`/`9b675bc5…`）。4 hash bucket、nlist 1024/shard、bits 7、top-100、nprobe 16、rerank index、locator on、index cache 8 GiB、200 条 SQL single-query。

| 轮次 | old QPS | new QPS | old p50 | new p50 | old peak RSS | new peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 23.29 | 23.91 | 42.12 ms | 41.47 ms | 5,996 MiB | 6,090 MiB |
| 2 | 23.52 | 23.57 | 42.25 ms | 42.09 ms | 6,122 MiB | 6,063 MiB |
| 3 | 22.70 | 23.75 | 43.84 ms | 42.12 ms | 6,134 MiB | 5,895 MiB |
| 中位数 | 23.29 | 23.75 | — | — | 6,122 MiB | 6,063 MiB |

均值 23.17 → 23.74 QPS（**+2.5%**），三轮 new 全部高于 old（样本不重叠），Recall@100 完全相同（0.63805），RSS 无上升（略降 0.6%）。结论：cache patch 有真实但**极小**的收益，**不足以**支撑“逼近 Rust core”的目标；它不是主要瓶颈。按门槛规则暂不回滚该 patch，但若最终组合方案相对 baseline 无可重复提升，则本 patch 一并回滚。

### 9.2 决定性证据：39 ms 的 exec 花在取行，且投影是全列

`LAKESOUL_PK_PROFILE=1` 在 locator-on 运行中给出每 query、每 bucket 的原始数据（804 行样本，去掉首轮冷启动）：

```
pk_locator: keys=200 files=1 pruned=0 rows=200 cache=28.8µs take=19.5ms total=20.2ms
pk_locator: keys=200 files=1 pruned=0 rows=200 cache=31.3µs take=22.5ms total=23.1ms
```

- locator 的 key 索引已是 warm（`cache ≈ 30 µs`），**不是**元数据/索引重复加载问题；
- 真正的成本是 `take`——**只取 200 行却花 17–22 ms**；
- 定位到 `rust/lakesoul-io/src/pk_locator.rs::take_rows`：`root().bind(file.dtype())` 把**整个文件的全部列**作为 vortex 投影，随后才在 batch 上按 `projection_names` 丢弃多余列。200 个随机行索引几乎覆盖整个文件的 chunk，于是每条 query 都要解码该 bucket 的 `id` + **200 维向量列**（约 295k 行 × 800 B ≈ 236 MB/bucket），4 个 bucket 并行也仍要 17–22 ms；
- `EXPLAIN ANALYZE` 与 `pk_locator total` 的差额说明 exec 中还包含每桶 reader 启动（ANN、filter 注入、plan 构建）与 `build_configs` 的串行部分，待下面阶段分解。

### 9.3 修正后的实施顺序

1. **（已实施）窄投影取行**：`take_rows` 改为 `select(FieldNames::from_iter(projection_names), root())`，只解码调用方请求的列。对 `select id` 的 index-only 查询，取行只读主键列（295k × 8 B ≈ 2.4 MB），预期 `take` 从 17–22 ms 降到 1 ms 量级；不改变任何输出行/列语义，parquet 路径不受影响。
2. **（诊断）每桶 create/start/scan 分解**：bench 增加 opt-in `RUST_LOG` subscriber，配合已有 `LAKESOUL_VECTOR_SEARCH_PROFILE=1`，测量 reader 创建、启动（ANN + filter 注入 + plan）与扫描三段占比，以及 `exec − 单桶` 的串行残差（`build_configs` 中每桶一次 `resolve_cached`）。
3. 依据分解结果决定后续项：`build_configs` 的 4 次串行 `resolve_cached`（每次一条 PG `select commit_id`）是否可并行/合并为一次批量轮询；ANN 与 reader 启动的其它一次性开销。
4. 每轮只用 GloVe-200 fresh index-only、4 bucket 复测，交错 3 轮，比较 QPS/p50/p99/RSS/Recall；无提升或 RSS 上升即回滚该项。

### 9.4 验收与清理（沿用第 6–8 节门槛）

- 每项优化独立可回滚，最终以“相对 baseline 的可重复 QPS 提升 + RSS 不升 + Recall 不变”为准；
- 每组测试仍使用唯一表名/scratch/pkcache 路径，测试后核验 PG 中 table/shards/commit/lease 计数为 0，并删除本组 `/dev/shm` 目录；
- 不修改 Reader/table/transaction/snapshot/MOR 语义；本轮唯一生产改动为 `vector/search.rs`（cache patch）与 `pk_locator.rs::take_rows`（窄投影）。

## 10. 单 query 阶段成果（GloVe-200，index-only，4 bucket）

逐轮改动、证据与回滚决定记录在
[`向量查询优化日志.md`](./向量查询优化日志.md)；已提交（未 push）：
`b5874e9d`（已解码 shard 复用）、`b5ab6932`（取行窄列投影）、`cf9e1a31`（bench tracing）、
`a10cccce`（index cache 身份 = `(kind, index_prefix)`）、`11a78144`（各桶 commit 批量解析）。

| 指标 | HEAD 基线 | 当前 | 变化 |
|---|---:|---:|---:|
| QPS（200 串行 single-query） | 23.78（中位） | 61.80（中位） | **+160%（2.6×）** |
| p50 / p99 | 41.9 / 47.4 ms | 16.2 / 17.8 ms | −61% / −63% |
| exec_mean | 39.3 ms | 13.6 ms | −65% |
| peak RSS | 6089 MiB | 5956 MiB（中位） | 不升 |
| Recall@100 | 0.63805 | 0.63805 | 不变 |

三处根因（都在向量检索路径内，未触碰 Reader/表/事务/snapshot/MOR 语义）：

1. `pk_locator::take_rows` 对 candidate 行做**整文件全列投影**，每条 query、每个桶都要解码
   200 维向量列（≈236 MB/bucket）→ 只投影调用方请求的列后，`take` 由 17–22 ms 降到 0.7 ms；
2. index cache key 里混入了 object store 的 Display，而 DataFusion 侧拿到的是
   `MonitoredObjectStore(...)`、native reader 侧是裸 store ⇒ `is_loaded` 永远 false，
   每条 query、每个桶都白申请一次 PG reader lease（≈2 ms）→ key 改为与 catalog 一致的
   `(kind, index_prefix)`；
3. 每桶各做一次 commit 解析与一次 runtime 交接 → 合并为 1 次批量查询 + 1 次交接。

当前单 query ≈16.2 ms 仍未接近纯 Rust core（1604 QPS/1 线程，0.62 ms/query），差值来自
每 query 的 SQL/物理计划（≈2.6 ms）、4 桶 reader 构建（桶内 start≈4.8 ms）、
`block_in_place` runtime 交接（≈2×2.6 ms）等**固定成本**。下一步方向见日志第 4 节：
先把 setup 移入返回的 stream 以去掉交接，再按阶段 B 做同 snapshot 批次 API 摊薄固定成本，
并用“同样 4 shard 的 Rust matched control”做同口径对比。

## 11. 端到端 vs 纯 Rust：CPU 与并发口径（实测）

用 bench 新增的 `--concurrency` 与 `/proc/self/stat` CPU 统计（GloVe-200，index-only，
4 bucket，nprobe 16，200 query，locator on）：

| 口径 | QPS | p50 | 每 query CPU |
|---|---:|---:|---:|
| 串行 1 query 在飞（当前基准） | 60.2–61.6 | 15.9–16.6 ms | **42–43 ms** |
| 2 并发 | 95.1–95.5 | 19.8–20.1 ms | — |
| 4 并发 | 120.9–125.3 | 29.0–30.0 ms | — |
| 8 并发 | 129.4–133.5 | 51.8–52.6 ms | 50.8–51.1 ms |
| 纯 Rust core（1 线程 / 16 线程） | 1604 / 16239 | 0.62 ms | 0.62 ms |

结论：

1. **每条 e2e query 消耗约 42 ms CPU**，而纯 Rust 的内存索引 ANN 只要 0.62 ms ——
   e2e 的 CPU 成本是 ANN 的 ~68 倍；ANN 在 e2e 里只占 0.6–2 ms 延迟（nprobe 扫描已证明）。
   这 42 ms = SQL/物理计划（~2.6 ms 单线程）+ 4 个 bucket reader 的构建/ANN/取行/合并
   （4 线程并行，每桶 start 4.8 + scan 1.0 ms）+ 索引读取与 locator 开销。
2. **并发只到 ~2.1×（61 → 130 QPS）就饱和**，而 8 并发时 CPU 利用率只有约 43%
   （131 QPS × 51 ms ≈ 6.9/16 核）：瓶颈不是 CPU 饱和，而是每 query 的多线程 reader
   结构与运行时/锁的争用（`LAKESOUL_IO_WORKER_THREADS=16` 与 `RAYON=1` 均无改善）。
3. 所以单 query 路径已经接近它自己的天花板；要“逼近 Rust”必须**把固定成本按批摊薄**，
   这正是阶段 B 的批处理入口。

## 12. 阶段 B 细化：同 snapshot 的多 query 批量入口

**语义契约**：一个批次里的所有 query 共享**同一个 snapshot 与 index commit**；结果带
`query_index` 列。批次之间不承诺一致性；不改变表/事务/snapshot/MOR 任何语义，不绕过
delete/CDC/ACL。批次入口是新增 API，现有单 query SQL 路径保持不变。

**实现路径**（按依赖顺序）：

1. `lakesoul-vector` core：`search_batch(queries: &[Vec<f32>], nprobe, k)` —— 逐 query 扫同一批
   已解码的 list/codes（后续可做多 query SIMD 距离），返回 `[Vec<VectorHit>]`；
2. `lakesoul-io` reader：`OPTION_KEY_VECTOR_SEARCH_QUERIES`（N 条 query）+ 输出
   `query_index` 列；merge-on-read、PK locator 批量取行（N×200 key 一次 take）、
   每 query 分组 top-k；
3. `lakesoul-datafusion`：`VectorSearchRequest` 增加多 query 字段，exec 每批次每桶只做一次
   解析/租约/reader 构建；批次输出按 `query_index` 分组；
4. API 入口：先做 Rust/Python 的 `table.batch_vector_search(queries, column, top_k, nprobe)`，
   SQL 侧的表函数后续再加；
5. 验证：同一 GloVe-200 与单 query 结果逐 query 比对 ID/顺序；QPS/CPU/RSS 对比
   单 query（61 QPS）与 8 并发（130 QPS）。

**成本模型（按当前实测推算）**：每批次固定 ≈ 2.6（计划）+ 5（4 桶 reader 构建）+
1（resolve/lease）≈ 9 ms；每 query 边际 ≈ ANN 0.6 + 批量取行 1–2.4 + 排序 0.2 ≈
**1.8–3 ms**。N=16 批次 ≈ 9+16×2.4 ≈ 47 ms ⇒ **≈2.9 ms/query（≈340 QPS，单 worker）**；
取行批量化到 1.2 ms/query 时 ≈1.8 ms/query（≈560 QPS）；多 worker 并发批次可再约 2×
（受 CPU/HT 与内存带宽限制）⇒ 目标 **700–1000 QPS**，即纯 Rust 16-worker 量级的 5–6%。

**回滚门槛**：Recall/ID 与单 query 完全一致、RSS 不升、QPS 提升可重复；任一条不满足即回滚
该路径，保留已提交的单 query 优化。

## 13. 阶段 B 首版落地：批量入口默认 64（已实现并验证）

R11 的设计已落地，实测（GloVe-200、4 bucket、nprobe 16、index-only、locator on；
**构建与搜索分成两个进程**，搜索进程不加载 947 MB base 向量）：

| 搜索配置 | QPS | p50 | CPU/query | 查询期峰值 RSS | Recall@100 |
|---|---:|---:|---:|---:|---:|
| 单 query SQL（原路径） | 60.1 | 16.4 ms | 43.0 ms | 577 MB | 0.63805 |
| 批量 = 1 | 124.0 | 8.06 ms | 16.3 ms | 577 MB | 0.63805 |
| **批量 = auto（默认 64）** | **1449.0** | **0.687 ms** | **3.25 ms** | **591 MB** | 0.63805 |
| 批量 = 64 | 1454.4 | 0.685 ms | 3.20 ms | 589 MB | 0.63805 |

- 默认批量 `DEFAULT_BATCH_SIZE = 64`：16→64 每 query 成本从 4.0 ms 降到 3.2 ms；
  200（整批）因校验候选并集过大退化到 57 QPS，故 32–64 是实测最优区间，取 64。
- 语义：批次内所有 query 共享同一个 snapshot / index commit（调用方选择 snapshot 即选择
  一致性点）；结果与单 query 路径逐 query 的 top-100 完全一致（Recall 相同）；校验读仍走
  LakeSoulReader，merge-on-read/删除/CDC 不变。
- 内存：查询期 RSS 0.58–0.59 GB（其中解码索引 361 MB）；**5.2 GB 是建表+进程内建索引的
  构建期峰值**，不属于查询路径；纯 Rust core 的 3.6 GB 峰值同样包含数据集加载。
- 与纯 Rust 的差距：单线程 1604 QPS 的 ~90%；CPU/query 3.2 ms vs 0.62 ms（ANN 本身），
  剩余为候选行校验读（locator take）与按 query 合并排序。
- 下一步：把批量入口接到 `LakeSoulTableProvider`（由 provider 提供 snapshot 文件列表，
  当前低层 API 由调用方传入文件列表）、补 provider 级集成测试、Python/Daft 绑定；
  以及在批内进一步降低校验读成本（locator 批量 take / 只对需要验证的行取行）。

## 14. 批量入口接入 provider + 安全校验跳过（实测）

- `LakeSoulTableProvider::vector_batch_search(state, request)`：provider 用当前 snapshot 列文件，
  调用方无需自己传文件列表，“同一 snapshot”契约由 provider 保证。
- 批量候选默认走校验读（LakeSoulReader，保证 merge-on-read/删除语义）；仅当
  「无 CDC 列 + 无 mutation chain + commit 覆盖该桶全部 active 文件」时跳过校验读，
  该判定有单测；`LAKESOUL_VECTOR_BATCH_FORCE_VERIFY=1` 可强制回退用于验证。
- 实测（构建/搜索分离、warmup 后各 4 次、默认批量 64）：

| 模式 | QPS 中位 | p50 | CPU/query | 查询期 RSS | Recall@100 |
|---|---:|---:|---:|---:|---:|
| 批量 + 跳过校验（默认） | **2155** | 0.462 ms | 2.18 ms | 548–551 MB | 0.63805 |
| 批量 + 强制校验 | 1399 | 0.712 ms | 3.30 ms | 576–585 MB | 0.63805 |
| 单 query SQL | 61.1 | 16.1 ms | 42.1 ms | 574 MB | 0.63805 |

相对纯 Rust：单线程 1604 QPS 的 134%，16 线程 16239 QPS 的 13%。剩余优化点：
批内 ANN 之外的合并/解码开销、payload/非 id-only 查询、以及多批次并发上限。

## 15. 批量路径当前最优（默认自适应）

| 配置（构建/搜索分离，GloVe-200，nprobe16，index-only） | QPS | p50 | CPU/query | 查询期 RSS | Recall |
|---|---:|---:|---:|---:|---:|
| 单 query SQL（原路径） | 60.6 | 16.2 ms | 42.1 ms | 582 MB | 0.63805 |
| 批量·默认（无需校验 → 整批） | **3594** | 0.26–0.28 ms | 1.8–1.95 ms | 556–558 MB | 0.63805 |
| 批量·默认（需校验 → 64/批） | 2221 | 0.45 ms | 3.15 ms | 615 MB | 0.63805 |

对照纯 Rust（同数据集、单 shard、nprobe=16）：单线程 single-query p50 0.582 ms
≈ **1684 QPS**；16 线程 `batch_search` **15250 QPS**。即批量入口已达纯 Rust 单线程的
2.13×、16 线程 batch 的 24%，相对单 query 湖仓路径 59×。

后续空间：每 query 仍有约 1.8 ms CPU（ANN 0.62 ms + 候选合并/解码/校验），且
3594 QPS × 1.8 ms ≈ 6.5/16 核 —— 还有约 2× 的核利用空间（多批次并发或批内更细并行）；
另需把入口暴露到 Python/Daft 并补 provider 级集成测试。

## 16. 单查询 index-only 直返（nbits=1 基线）

用户确认基准索引保持 **`total_bits=1`**（不再试 7 bit），因此 §15 之前 bits=7 的数值只作历史
参照；bits=1 下同一 harness 的基线是：单 query SQL 61.3 QPS、p50 16.0 ms、CPU/query 41.5 ms、
查询期 RSS 337 MB；批量入口 4537 QPS、0.217 ms、1.15 ms、315 MB；索引 103 MB；Recall@100
0.42035（1-bit 码的固有精度，接受保留）。

§10 的单 query 优化把每次查询的 42 ms CPU 里绝大部分定位为**固定成本**：每个 bucket 每次
查询都新建 reader（每 reader 一个 tokio Runtime，4.47 ms）、`get_table_schema` +
`build_physical_plan`（4.81 ms）、扫描候选行（3.20 ms），而 ANN 只有 0.1 ms。既然候选集合
完全由索引决定，index-only 排序下的 PK-only 投影就不需要打开数据文件。

**实现**（R17）：

- 门槛（`index_only_eligible`，全部满足才生效，否则整条查询回落到原 reader 路径）：
  `rerank='index'`（`order_by`）、单向量列、无 CDC 列、无 range partition、输出列 ⊆ 主键列、
  每个 shard 通过覆盖证明 `can_skip_verification`（无 mutation chain、index 覆盖本快照的
  全部 active 文件）。门槛与批量入口完全一致，因此不引入新的语义分支。
- 执行：一次 `resolve_cached_many` → 每 shard `search_typed_shard` → 按 key 去重取最优 →
  按索引距离排序 → `KeyCodec::decode` 直接生成主键列 → 返回单 batch。全程不建 Reader/物理计划。
- `lakesoul-io` 新增 `search_typed_shard`：DataFusion 侧本来就持有 catalog 解析后的
  `Vec<VectorSegmentEntry>`，直接传 `IndexHeader` + `Vec<SegmentEntry>`，不再每 query 把
  约 1024 个 segment 序列化成 JSON 再解析（这一步单独占 4.7 ms/query）。
- 回退开关：`LAKESOUL_VECTOR_INDEX_ONLY=0` 强制 reader 路径；profile 开关
  `LAKESOUL_VECTOR_SEARCH_PROFILE=1`。

| 版本（bits=1，200 query） | QPS | p50 | CPU/query | 查询期 RSS | Recall |
|---|---:|---:|---:|---:|---:|
| 基线 reader 路径 | 61.3 | 16.0 ms | 41.5 ms | 337 MB | 0.42035 |
| R17a index-only（JSON 传输） | 118.4 | 8.25 ms | 7.9 ms | 237 MB | 0.42035 |
| **R17b index-only + typed shard** | **183.3** | **5.26 ms** | **4.9 ms** | **235 MB** | **0.42035** |

单查询 3.0×、CPU/query 8.4× 更低、RSS −30%，批量路径不受影响；用
`LAKESOUL_VECTOR_INDEX_ONLY=0` 复测 reader 路径为 58.4 QPS / 16.6 ms / Recall 0.42035，
说明回退路径与基线一致。

**下一步**（按收益排序）：

1. 单查询的 exec 残值 2.8 ms：`resolve_cached_many` 0.78 ms + 4 shard 检索约 1.8 ms
   （可并发 + 缓存已加载 shard 以跳过 segment 转换）。
2. logical plan 1.07 ms + physical plan 1.44 ms：每条 SQL 都要重新解析快照/列文件；
   单查询要再上一个台阶，需要“一个 statement 固定一个 snapshot 的多次查询”显式入口
   （与 §12 的批量入口同一思路，只是 batch=1..N 由调用方声明）。
3. 批量入口：1.15 ms CPU/query 中 ANN 约 0.6 ms，其余是候选合并/解码；14 核利用空间仍在。

## 17. 批量入口按桶并行（nbits=1）

§15/§16 的批量入口把 4 个桶放进 `try_join_all`。每个桶的 `batch_search` 是**同步** CPU 工作，
同一个 task 轮询时 4 个桶实际是串行的：profile 显示 1.15 ms/query 的 CPU 只用掉约 5.3/16 核。
把“每桶探测 + 排序”放到各自的 OS 线程、按 query 切分跨桶合并之后：

| 版本（bits=1、200 query、6 次） | QPS 中位 | p50 | CPU/query | 查询期 RSS | Recall |
|---|---:|---:|---:|---:|---:|
| R17 串行探测 | 4331 | 0.226–0.238 ms | 1.10 ms | 238–241 MB | 0.42035 |
| **R18 按桶并行** | **8512** | **0.110–0.124 ms** | 1.15 ms | 239–242 MB | 0.42035 |

并行的前提是 lakesoul-io 暴露线程安全的 `LoadedShard`（同步 `search_batch`）：批入口先把
shard 装好，再把桶分给线程，避免把 async future 搬过线程边界。跳过校验时也不再构建候选
并集（覆盖证明已保证候选是活行）。强制校验路径复测 1090–1191 QPS / Recall 0.42035，
与跳过校验一致。

**当前口径**：批量 CPU/query 1.15 ms ≈ 4 个 shard 的 ANN 本身（0.275 ms/query/shard），
DataFusion 侧只剩 ~5% 开销；纯 Rust 上限 ≈ 14 k QPS，实测 8.5 k（60%），差额为每 chunk
固定成本（load/merge/probe 尾巴）和 provider 每批的 snapshot 列文件开销。单查询在 R17
之后是 183 QPS（5.26 ms：plan 1.07 + physical plan 1.44 + exec 2.8），瓶颈已转移到每条
SQL 的规划/快照解析。

## 18. 单查询按 shard 并行 + 缓存命中直取

R17 之后单查询的 exec 还剩 2.8 ms，其中 shard 装载/搜索 1.77 ms（4 个 shard 串行、ANN 只占
0.1 ms/shard），且缓存命中时还在克隆 ~1024 条 segment。把 shard 先装好、`search_batch` 放到
4 个线程上并行，并新增 `cached_typed_shard`（命中缓存直接返回 `LoadedShard`，不再克隆 segment
列表、不解析 header，`key_kind` 不匹配即失效）之后：

| 版本（bits=1、200 query） | 单 query QPS | p50 | CPU/query | 查询期 RSS | 批量 QPS | Recall |
|---|---:|---:|---:|---:|---:|---:|
| R17 | 183.3 | 5.26 ms | 4.9 ms | 235 MB | 4331 | 0.42035 |
| **R19** | **227–233** | **4.1 ms** | **4.6 ms** | **233–237 MB** | **8816–8950** | 0.42035 |

单查询相对最初 reader 路径（61.3 QPS / 16.0 ms）为 3.7×；批量相对 R18 再 +4%。剩下
4.1 ms 中 logical plan 1.07 + physical plan 1.44 已是主项（每 query 的快照/列文件解析），
要继续提升需要显式 snapshot 复用入口。

## 19. 缓存命中共享视图 + 批量单次解析

`resolve_cached_many` 命中 `VIEW_CACHE` 后仍克隆整个 `IndexCommitView`（每 shard ~1024 条
segment，含 data_files 字符串），这是单查询 exec 里 0.75–0.9 ms、批量每批 1.08 ms 的主要原因；
批量入口还在 batch-size 判定与 chunk 各解析一次 shard。改成返回 `Arc<IndexCommitView>`（缓存条目
本身即 Arc）、批量入口每次调用只解析一次之后：

| 版本（bits=1、200 query） | 单 query QPS | p50 | CPU/query | 批量 QPS 中位 | 查询期 RSS | Recall |
|---|---:|---:|---:|---:|---:|---:|
| R19 | 227–233 | 4.1 ms | 4.6 ms | 8890 | 233–241 MB | 0.42035 |
| **R20** | **264–274** | **3.4–3.5 ms** | **4.0 ms** | **9466** | **233–238 MB** | 0.42035 |

单查询相对本轮开始（61.3 QPS / 16.0 ms）为 4.4×；批量相对 R17（4331）为 2.2×。批量合并线程数
8 实测更差（7.3–7.8k），保持全核。剩余：单查询 3.4 ms 中 logical plan 1.07 + physical plan
1.44 仍是主项。

## 20. 批量探测与排序分阶段

R20 的批量快路径是"每桶一个线程 = ANN + 排序"，但排序（逐 query 去重 + 主键解码）只能在
每桶单线程上跑，4 个桶只铺 4 核，16 核里 12 个空转。拆成「按桶并行探测 → 按 query 铺满
全部核排序」两个阶段后（两版各 6 次交错）：

| 版本（bits=1、200 query） | 批量 QPS 中位 | QPS 区间 | p50 | CPU/query | 查询期 RSS | Recall |
|---|---:|---|---:|---:|---:|---:|
| R20 | 8387 | 6463–9844 | 0.107 ms | 1.1–1.25 ms | 237–240 MB | 0.42035 |
| **R21** | **10943** | 8572–11117 | 0.089–0.093 ms | 1.1–1.25 ms | 237–240 MB | 0.42035 |

批量中位 **+30.5%**。此时每批 200 query 的 chunk 为 load 0.18 + probe 20.4 + merge 0.89 ms；
探测阶段 18.5 ms 对理想 13.75 ms（220 ms CPU / 16 核）≈ 73% 并行效率，差距来自 SIMD 满载
下的频率与内存带宽，不是调度问题。

## 21. 与纯 Rust 天花板对照（bits=1）

新增 `--scenario rust-shard-batch`：用 catalog 直接加载表里的 4 个 shard，按「每 shard 一线程
`batch_search` + 每 query 跨 shard 合并」的手写 Rust 流程测量，作为 DataFusion 路径的天花板
（3 次，bits=1、200 query、nprobe 16、top_k 100、16 rayon 线程）：

| 指标 | 纯 Rust 参考 | DataFusion（R21/R22） |
|---|---:|---:|
| 检索段 QPS | 13.4–13.8k | — |
| 检索 + 合并 | ≈10.7k（合并单线程 4.0 ms） | **11.2–11.5k（端到端）** |
| Recall@100 | 0.42035 | 0.42035 |
| 单线程单 query（4 shard）p50 | 0.73–0.78 ms | 单 query SQL 3.46 ms |

即：**批量入口端到端已达纯 Rust 检索段的 82%、纯 Rust「检索+合并」的 105%**（我们的合并并行），
Recall 与参考逐位一致。单查询 SQL 里真正属于检索的只有约 1.0 ms（exec），其余 2.4 ms 是每条
SQL 的 logical/physical planning 与快照列文件。

R22 另把排序与跨桶合并融合成同一个并行区：批量中位 10943 → **11206**（6 次，+2.4%），且抖动
收窄（最小值 8572 → 10867）。

## 22. 向量扫描复用不可变 partition 版本文件列表

单查询的 physical planning 里 `list_files_for_scan` 每次都要重做 2 次 PG 查询 + 每文件一次 HEAD
（0.49 ms）。partition 版本发布后其数据文件不可变，因此向量扫描路径改走
`list_files_for_vector_scan`：**每次仍解析当前 partition 版本**（保证快照新鲜度），但
`(table_id, partition_desc, version)` 的文件列表走 512 项进程内 LRU（≈0.5 MB），按版本号失效；
普通扫描与文本索引扫描完全不变。

| 版本（bits=1、200 query） | 单 query QPS | p50 | physical plan | CPU/query | 批量 QPS | Recall |
|---|---:|---:|---:|---:|---:|---:|
| R22 | 269 | 3.5 ms | 1.44–1.54 ms | 4.0 ms | 11206 | 0.42035 |
| **R23** | **301** | **3.12–3.20 ms** | **1.13–1.17 ms** | **3.65–3.8 ms** | **11547** | 0.42035 |

单查询 +10%（相对本轮会话开始 61.3 QPS 为 **4.9×**），批量 +3%；RSS 234–238 MB 不变。
单查询剩余的 3.15 ms = logical plan 1.02–1.08 + physical plan 1.13–1.17 + exec 1.0，
其中 planning 部分（约 2.1 ms）已是纯 SQL/DataFusion 开销。

## 23. 硬件上限复核与批量路径的定位

本机 `lscpu`：KVM 虚拟机、**8 物理核 / 16 超线程（SMT2）**，不是 16 物理核。实测
`RAYON_NUM_THREADS=8` 时批量 8.8–9.5k QPS、`=16` 时 11.6–11.7k（SMT 约 +28%），说明 16 线程仍在
线性收益区。按批量端到端 17.1 ms/200 query、CPU/query 1.05 ms 折算 ≈ **13.2 个逻辑核**在用，
纯 Rust 参考场景检索段 ≈ 14.5 个 → DataFusion 批量路径的**检索阶段已达纯 Rust 同流程的 ~91%**；
端到端 11.7k 对纯 Rust「检索+单线程合并」10.7k 为 **109%**。批量路径已贴近硬件上限，剩余为每批
约 2 ms 的固定成本（列文件 0.2 + index 解析 0.3 + 句柄装载 0.2 + 排序合并 ~1.2）。

R24 的收尾清理（零拷贝 key 解码 + 并行取句柄）让批量中位 11384 → **11738（+3.1%）**，单查询持平。
单查询仍是 3.1–3.2 ms，其中 logical/physical planning ≈ 2.1 ms 是每条 SQL 语句的固有成本。

## 24. 单查询：按快照文件集复用已解析 shard

单查询的 exec 每个 query 都要重新 resolve index 提交（0.305 ms）并取 4 个 shard 句柄
（0.201 ms），而压测场景里同一条 SQL 模板在同一快照上连续执行 200 次。由于向量扫描的 exec 是
按 planning 阶段的文件组构造的、数据文件不可变，**同一文件集等价于同一批行**：把「文件集 →
（index 提交视图, 已加载 shard）」缓存起来即可复用，数据提交会改变文件集从而自动 miss；
缓存命中后仍逐 shard 重跑覆盖证明，证明不成立即回落 reader 路径；`LAKESOUL_VECTOR_SNAPSHOT_CACHE=0`
可关闭。另把覆盖证明所需的 covered 文件名预先去重存好，4 个 shard 的探测改投共享 blocking 池。

| 配置（bits=1、200 query、6 次） | 单 query QPS 中位 | p50 | exec 均值 | CPU/query | RSS | Recall |
|---|---:|---:|---:|---:|---:|---:|
| 关闭缓存（≈修改前） | 296 | 3.12–3.23 ms | — | 3.7 ms | 233–235 MB | 0.42035 |
| 仅快照缓存 | 320 | 2.91–2.97 ms | 0.69–0.77 ms | 3.6–3.75 ms | 232–235 MB | 0.42035 |
| **全部（+covered 预存 + blocking 池）** | **343** | **2.72–2.86 ms** | **0.50–0.53 ms** | **3.35–3.5 ms** | **232–234 MB** | **0.42035** |

单查询 **+15%**（相对最初 reader 路径 61.3 QPS/16.0 ms 为 5.6×），RSS 未升、Recall 不变；
批量入口不受影响。剩余 2.74 ms 中 planning 占 ~2.35 ms（86%），属于"一条 SQL 一次查询"的固有
成本；exec 只剩 ~0.51 ms。

## 25. 单查询：执行计划快路径 + 表 provider 复用

用 profile 把单查询 2.72 ms 拆细之后，两个"每条语句都重复"的成本浮出来：默认物理 planner 的
逐节点转换/物理优化器（~0.16 ms）和 catalog 的 `table()` 查找（0.398 ms = 表信息 PG 查询
0.215 + provider 构建 0.183）。对应的两项优化：

1. **执行计划快路径**（`LAKESOUL_VECTOR_FAST_PLAN`，默认开）：优化后的逻辑计划恒为
   `Limit(fetch) -> TableScan(projection, filters=[marker])`（index-only 已吃掉排序），因此直接
   `provider.scan()` + `GlobalLimitExec`，跳过默认 planner；必须验证计划树含向量 exec 且输出
   schema 与逻辑计划一致，否则回退。
2. **表 provider 复用**（`LAKESOUL_TABLE_PROVIDER_CACHE`，默认开）：每条语句仍从 PG 读
   `TableInfo`，只有元数据与 catalog options 指纹都没变时才复用已构建的 provider。

| 版本（bits=1、200 query） | 单 query QPS 中位 | p50 | Recall |
|---|---:|---:|---:|
| R25 | 349 | 2.70–2.78 ms | 0.42035 |
| + 执行计划快路径 | 362（+4.9%） | 2.596 ms | 0.42035 |
| **+ 表 provider 复用** | **392（+8.9%）** | **2.407 ms** | 0.42035 |

本轮合计 **+12%**，相对最初 reader 路径（61.3 QPS / 16.0 ms）为 6.4×。剩余 2.4 ms = logical
optimizer ~0.86 + analyzer 剩余 ~0.75（含 0.215 ms 的表信息 PG 查询，出自快照新鲜度要求）+
物理规划 ~0.16 + exec ~0.51。

## 26. 大批次（10,000 query）：分块与有界并发

用 10,000 个查询（从 base 均匀取样、分块 BLAS 精确 GT）验证批量入口的伸缩性：

| 配置（bits=1、10k query） | QPS | p50 | peak RSS | CPU/query |
|---|---:|---:|---:|---:|
| 整批（10k 一个 chunk） | 13226–14211 | 0.067–0.072 ms | **547 MB** | 0.99 ms |
| **分块 1024 + 最多 2 chunk 在飞** | **13190–13801** | 0.069–0.073 ms | **349–351 MB** | 0.99 ms |
| 200 query（对照） | 11559 | 0.083 ms | 237 MB | 1.05 ms |

结论：**批量 10,000 query 的 QPS 比 200 query 更高（约 +15–18%）** —— 每批的固定成本（快照列文件、
index 解析、句柄装载、合并启动）被摊薄，CPU/query 从 1.05 ms 降到 0.99 ms；但把 10k 个查询同时
留在内存里会让 peak RSS 从 238 MB 涨到 547 MB。因此 fast path 的批量上限设为 1024 个 query、
最多 2 个 chunk 同时在飞：QPS 保持 ~13.4k，peak RSS 回到 350 MB。分块 512 或 2048 都会掉到
11–13k，256 掉到 11k；两个参数都可用
`LAKESOUL_VECTOR_BATCH_CHUNK_QUERIES` / `LAKESOUL_VECTOR_BATCH_CHUNKS_IN_FLIGHT` 覆盖。

## 27. 单查询 planning 分阶段实测（含两次无收益回滚）

`--scenario plan-probe`（bench 内置诊断）把每条语句拆成 parse / analyze / logical optimizer /
physical planner，并对比 200 维向量、2 维向量与普通 limit 三种形态（p50，GloVe 表）：

| 阶段 | 200 维向量 | 2 维向量 | `select id limit 100` |
|---|---:|---:|---:|
| parse | 0.215 ms | 0.048 ms | 0.028 ms |
| analyze | 0.607 ms | 0.270 ms | 0.226 ms |
| logical optimizer | **0.835 ms** | 0.185 ms | 0.047 ms |
| physical planner | 0.242 ms | 0.218 ms | 0.826 ms |
| 合计 | 1.90 ms | 0.72 ms | 1.13 ms |

- 200 维字面量让 planning 多 ~1.1 ms，其中 logical optimizer 占 0.65 ms；
- 物理规划我们反而比普通扫描快 0.58 ms（R26 的 fast plan）；
- 两次尝试均无收益并已回滚：把 pushdown 规则前置（optimizer 0.835→0.843 ms）、
  marker 只携带 query 句柄（0.810 ms）；pushdown 规则本身每条语句仅 ~11 µs。
- 下一步方向：在 analyzer 阶段把全字面量 `ARRAY[...]` 折叠成单个 `List` 字面量（自定义
  coercion），或提供"固定快照 + 多次查询"入口。
