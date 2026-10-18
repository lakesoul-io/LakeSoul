# 10M 近实时数仓场景：IVM 性能路线（P0–P5）

状态：P0.1 进行中（基准与内存基线）；其余条目为计划。
本文以 2026-10 的实测数据（B0–B3、P1a/P1b、R1）和业务约束为基础，
是 `ROADMAP.md` §G 在目标场景下的展开；两者冲突时以本文为准。

## 1. 目标场景

| 维度 | 目标 |
|---|---|
| 事实表 | 1000 万行，主键点查/更新 |
| 维表 | 3–5 张，各约 1 万行，很少更新但会更新 |
| 维表 fanout | 一个维表 key 可对应万级事实行；维表更新可能影响 10 万–100 万事实行 |
| 变更 | 每 5–10 分钟一批，事实表 1%–5%（10 万–50 万行） |
| 聚合 | GROUP BY 维表属性，分组约 1 万；以 SUM/COUNT/AVG 为主，少量 MIN/MAX/COUNT(DISTINCT) |
| 时延 | 刷新在批次间隔内完成（5% 时目标 < 2–3 分钟） |
| 内存 | 不能像 Flink 一样为每个 key 维护本地状态；单次刷新峰值内存有界 |
| 执行 | 集群、分布式 + 多线程；按 hash bucket 拆分任务 |
| 读取 | source 读取与中间计算必须走 DataFusion 物理计划，不把数据整体读入驱动端内存 |
| compaction | 状态表由写入方主动 compaction；用户可见表仍由外部 compaction 任务处理 |

关键推论：

- 维表万级 → **broadcast join**，不需要 shuffle 维表；
- 事实 delta 10 万–50 万行 → 每轮的主要输入是 delta，而不是全表；
- 聚合分组万级 → 可合并聚合（SUM/COUNT/AVG）的状态很小，是增量的最佳场景；
- 10 万行 delta 会覆盖几乎所有万级分组 → MIN/MAX/DISTINCT 的「按组重算」容易退化为全量，必须允许代价回退；
- 1%–5% 的随机 key 会命中每个数据文件 → 文件级裁剪有限，**文件布局/compaction 与写入减少**比点查更关键；
- 「点查」主要用在：join MV 按事实 PK 取旧行、聚合状态按 group key 取旧状态、未来的 FK 反查。

## 2. 当前实现与目标的 gap

### G0（架构级，最高优先级）：扫描/写入边界仍会整体物化

- `IvmTable::read_current*` 返回 `Vec<RecordBatch>`，注册为 `MemTable`；刷新输出最后
  `collect()` 到驱动端再 `append_batches`。
- 计算本身是 DataFusion 物理计划，但 **scan 与 sink 不是**：10M 事实表、3–5 张维表、
  10M join MV 都可能在驱动端被整体物化；无法满足「不自己读到内存里」和集群执行。
- 需要：`IvmTable` 的 TableProvider（current / before-window / delta file-list 三种 scan，
  下推 filters/projection/bucket），以及流式的 DataSink/writer physical plan；复用
  `lakesoul-io/src/session.rs` 已有的 provider、bucket pruning、pk locator 能力。

### G1（业务收益最大）：多级维表 join 仍是全量读

- `refresh_lookup_chain` 每轮对每个 source 执行 `read_current_projected`，再 replay 整条链；
  `refresh_multi_join` 同样全量读每个 source。
- 1M 的 chain 基准每轮仍扫描约 500 万行；10M × 3–5 维就是每轮数千万行。
- 目标：事实 delta 驱动；维表广播；只对 delta 行做链式 join；join MV 写 delete+insert。

### G2：维表更新路径

- 没有按 FK 反查事实的索引/状态表；`pk_locator` 只对事实 PK 生效。
- 维表更新低频但 fanout 大；默认维护一张 10M 行的 FK 状态表不划算（每轮事实变更都要额外写）。
- 策略：先用代价模型估算受影响事实行数，超过阈值直接全量重算；只有实测证明维表更新频繁
  且回退代价过高时，才引入 FK 状态表/二级索引。

### G3：1%–5% 的 key 集超过现有过滤上限

- `MAX_KEY_FILTERS = 4096`、`MAX_PK_CANDIDATES = 10_000`；10M × 1% = 10 万 keys。
- 事实 delta 驱动时不需要对事实表做 key IN 过滤；需要大 key 集的是：
  - join MV 的旧行读取（delete 行）——用 delta 与 MV 做 **semi-join**，而不是字面量 IN；
  - 维表更新时的 affected fact 探测。
- 实现上要把「字面量 IN」改成「delta 表参与的 semi-join / 分块流式 probe」。

### G4：聚合在万级分组下的分化

- SUM/COUNT/AVG：`sum_count` 用 delta + before 行 + 万级组状态合并，路径正确、成本低。
- MIN/MAX/COUNT(DISTINCT)：10 万行以上 delta 会触达几乎所有万级分组；按组重算会读 join MV
  的大量行，接近全量。可选 value-count 状态表，或按代价回退到全量聚合。
- 验收上必须区分「可合并」与「不可合并」，不要求所有聚合都增量。

### G5：点查/有序过滤未接入刷新路径

- `pk_locator` 支持 PK 等值/IN/复合键前缀，索引 mmap + 对象存储缓存；但只接在
  `session.rs` 的 DataFusion provider 路径，IVM 低层 reader 未使用；候选集上限 1 万。
- 需要：P0.2 把 provider/locator 接进刷新；状态表按 group key 分桶；用户 MV 的
  compaction 仍外部做。

### G6：bucket 并行/分布式

- 事实 10M、每轮 10 万–50 万行；单机多线程先做，但设计要按 bucket 切分：
  delta 按事实 PK 的 bucket 分区、维表广播、join MV 按 bucket 写、聚合按 group key
  局部聚合后合并万级状态。
- 测试覆盖 `target_partitions` 8/16 与桶数 4/64/256。

### G7：状态表 compaction

- R1 的写入合并只解决「一次刷新内多 batch → 多文件」；多轮刷新仍累积小文件。
- 状态表需要写入方在文件数/小文件比例超阈值时，读当前状态 → 重写新 generation →
  原子切换，并与 epoch/游标协议配合；用户表继续外部 compaction。

## 3. 目标架构

```
                 ┌──────────────────────────────┐
   fact delta ──►│ DataFusion physical plan      │
   (100k-500k)   │  scan(delta files)            │
                 │  broadcast join dims (10k)    │──► join MV delta (delete+insert)
                 │  semi-join MV old rows        │       按 bucket 流式写
                 └──────────────┬───────────────┘
                                │ join MV delta
                                ▼
                 ┌──────────────────────────────┐
                 │ DataFusion aggregation plan   │
                 │  group by dim attrs (10k)     │──► aggregate MV delta
                 │  merge with group state (10k) │       状态表写入方 compaction
                 └──────────────────────────────┘
```

- 所有 scan 都是 TableProvider：current、before-window、delta file-list 三种；
  filters、projection、bucket 下推。
- 维表广播；事实 delta 是探测侧。
- join MV 必须刷新（用户可见），只写 delta 对应的 delete+insert；旧行用 delta 与 MV 的
  semi-join 取，而不是全表读。
- 聚合从 join MV delta 更新；可合并聚合直接合并万级状态；不可合并聚合走 value-count 或
  代价回退。
- 输出用 DataSink，按 bucket 分区写；驱动端不 collect。
- 状态表 compaction 由写入方在 epoch 边界触发。

## 4. 工作流

### P0.1（进行中）：10M 星型 + 聚合基准与内存基线

- 新增独立基准（`examples/ivm_bench_star.rs`），不改业务逻辑：
  - fact 1M/5M/10M；3 张维表各 1 万行；分组约 1 万；
  - 两步视图：多级 lookup chain（join MV）+ `SUM/COUNT` 聚合；
  - 变更：fact 1%/3%/5%，dim 0/0.1%/1%，维表 fanout 均匀/热点；
  - 布局：`--batch` 8k（碎片）与 256k（模拟 compaction）；Parquet；
  - 指标：join step / agg step 刷新 p50/p95、全量重建耗时、`VmHWM` 峰值、文件数；
  - 可选 `--verify` 在小规模做增量 vs 全量内容对拍。
- 产出：当前实现（collect/MemTable 路径）在 1M/5M/10M 下的内存与时延基线，确认 G0/G1 的
  真实占比。

### P0.1 结果（2026-10，release，本地存储，32 核，默认 DataFusion 线程）

星型 3 维（各 1 万行）+ `SUM/COUNT` 聚合（100 组），事实变更 1%/5%：

| 场景 | 增量 join | 增量 agg | 增量合计 | 全量 join | 全量 agg | 全量合计 | 峰值 RSS |
|---|---|---|---|---|---|---|---|
| 10M / 1% | 5.0–5.7s | 2.8–3.4s | **7.8–9.0s** | 4.9s | 1.4s | **6.3s** | 11.0GB |
| 10M / 5% | 7.5s | 9.6s | **17.2s** | 5.4s | 1.3s | **6.7s** | 9.5GB |
| 10M / 1%，256k 批 | 4.2–5.3s | 2.6–2.7s | **6.9–7.9s** | 4.6s | 1.5s | **6.1s** | 10.4GB |
| 1M / 1% | 0.6s | 0.3–0.5s | **0.9–1.1s** | 0.6s | 0.2s | **0.8s** | 1.5GB |

（1M/10M 的增量结果是 2–3 轮的中位数；全量是同一数据上 `rebuild_*` 的耗时；每次增量与全量
的聚合结果都做了行级校验。）

结论：

1. **当前实现在 10M/1% 和 5% 下增量都比全量慢**。join 仍读全部源表（G1）；5% 时聚合的
   before/state 读取用 50 万 key 的过滤，耗时 9.6s，而全量聚合只读 10M 行 join MV（1.3s，G3/G4）。
2. **峰值内存 ~10–11GB**：10M 事实表 + 10M join MV 被 materialize/collect（G0）。
3. **compaction 有用但不够**：事实表文件 636→168，join 仍 4.2–5.3s；10M 行的扫描/写入是主成本，
   文件数只是其中一部分。更适合放大到更大规模或远端存储时再评估。
4. 发现并修复了 `key_filters` 的 O(n²) 去重：10 万 key 的 bootstrap 聚合从 **10.6s 降到 122ms**
   （`Vec::contains` → `HashSet`）。这是 1%–5% 大 key 集下所有过滤路径的基础修复。
5. 基线证明 P0.2（DataFusion provider/流式）和 P1（事实 delta 驱动的多级 join）是下一步的
   正确优先级；P2 需要区分可合并聚合与不可合并聚合，并避免大 key 集的字面量 IN。

基准命令（本地）：

```sh
cargo run --release -p lakesoul-ivm --example ivm_bench_star -- \
  --rows 10000000 --dim-rows 10000 --change-ratio 0.05 --rounds 1 --warmup 1 \
  --storage local --verify
```

### P0.2：DataFusion provider + DataSink

- `IvmTable` 实现 TableProvider（current/before/delta），下推 filters/projection；
  读取走 `lakesoul-io/src/session.rs` 的 provider（含 bucket pruning、pk locator）。
- 刷新输出改为流式 writer physical plan/DataSink，按 bucket 写，驱动端不 collect。
- 验收：10M/5% 峰值内存有界（目标 < 8GB，按机器调整）；读路径无 `Vec<RecordBatch>` 全表物化。

#### P0.2a 结果：流式 provider（基础设施）

- `IvmTable::stream_current` 逐批读取 current 状态；`IvmTableProvider` 的 `Raw` 模式改为
  可重复执行的流式 `IvmStreamExec`（每次 execute 启动新的 reader 线程 + 有界 channel），
  不再在 provider 内 `collect` 整表；`refresh_lookup_chain` 的事实基表 `now` 改走该 provider。
- 10M/1% 复测：增量 7.5–8.7s、峰值 ~10.9GB（改造前 7.8–9.0s、11.0GB），**没有实质改善**。
- 原因：真正占内存/时间的是 chain 的 **before 全量读**、事实变更时的 `every_base_row`
  （affected = 全部 base keys）、以及 10M 行 replay 的 `collect()`；只把 `now` 流式化不够。
- 结论：P0.2 的流式基础设施保留；下一步必须做 **P1（事实 delta 驱动的链）**，同时把
  before/affected/输出改为 delta 范围，再接流式 sink。P0.2 的内存验收由 P1 一并完成。

### P1：事实 delta 驱动的多级 join

- 广播维表；只读事实 delta；链式 replay 仅处理受影响行；join MV 写 delete+insert。
- 旧行用 delta semi-join join MV（或 before 事实行），避免全表读 MV。
- 验收：10M/1% join 刷新增量 ≥ 2× 全量；5% 不慢于全量（否则代价回退）。

### P2：聚合更新

- join MV delta 上的万级分组聚合；SUM/COUNT/AVG 状态合并。
- MIN/MAX/DISTINCT：value-count 状态或代价回退；区分可合并/不可合并。
- 验收：10M/1% 可合并聚合增量 ≥ 2× 全量；不可合并允许回退但不慢于全量 1.0×。

### P3：维表更新与代价模型

- 估算 fanout/受影响事实行数；默认全量或全表 probe 回退；实测值得时再做 FK 状态表/索引。
- 代价模型输入：delta 行数、维表 fanout、分组数、文件数、MV 行数、内存。
- 验收：维表更新不出现 >2× 的性能悬崖；错误回退率可观测。

### P4：状态表 compaction 与布局

- 状态表写入方 compaction（文件数/小文件阈值、新 generation、原子切换）；
- 用户表外部 compaction；文件大小目标与 bucket 数按集群并行度设计。
- 验收：100 轮后状态表文件数稳定；单文件大小接近目标。

### P5：分布式/多线程

- bucket 级任务拆分、维表广播、分区聚合、状态合并；单机多线程先验证，再做分布式调度。
- 验收：8/16 线程接近线性扩展；bucket 数 64/256 下无热点。

## 5. 基准设计

### 5.1 数据

- `fact(k, fk1, fk2, fk3, v, op)`：1M/5M/10M；FK 均匀或热点。
- `dim1..dim3(k, a, b, op)`：各 1 万行；`a` 取 100 个值，两维组合约 1 万分组。
- 事实变更：1%/3%/5%，mix 1:1:1 与 9:1:0；维表变更：0/0.1%/1%。

### 5.2 视图

1. `V1`：fact + 1 dim（对照）。
2. `V2`：fact + 3 dims lookup chain（join MV）。
3. `V3`：V2 + `GROUP BY a1, a2` + `SUM(v), COUNT(v)`。
4. `V4`：V2 + `MIN/MAX` 或 `COUNT(DISTINCT)`。
5. `V5`：V3 + 维表属性更新。

### 5.3 矩阵与指标

- 规模 1M/5M/10M；线程 `target_partitions` 默认/8/16；桶 4/64/256；
  文件 `--batch` 8k/256k；Parquet（后续 Vortex）。
- 指标：join/agg 两步 p50/p95；全量重建；`files_read/rows_scanned/rows_written`；
  `VmHWM` 峰值；状态表/MV 文件数；服务侧点查（后续）。
- 消融：多级 join 全源读 vs delta 驱动；维表更新全表 probe vs 回退；聚合类型；
  compaction/cache；collect vs provider 流式。

### 5.4 验收

- 10M/5%/V3：< 2–3 分钟，峰值内存 < 8GB（按机器调整），增量 ≥ 1.0× 全量。
- 10M/1%/V3：增量 ≥ 2× 全量（P1/P2 达成后）。
- 10M/1%/V4：允许回退，但不慢于全量 1.0×。
- 状态表 100 轮文件数稳定；点查 p99 < 100ms（热 cache，P4 后）。

## 6. 风险与开放问题

- 内存预算未定（8/16/32GB）：决定 P0.2 是否必须 spill。
- MIN/MAX/DISTINCT 占比未定：决定 value-count 状态表是否值得。
- 维表 fanout 分布未定：决定 FK 索引/状态表是否值得，或直接代价回退。
- 集群调度接口未定：P5 需要与现有调度/compaction 服务对接。
