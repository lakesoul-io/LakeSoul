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
