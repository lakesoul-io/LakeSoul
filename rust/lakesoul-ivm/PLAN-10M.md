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
| compaction | 内部 state/MV 统一由外部 `NewCompactionTask` 压缩；refresh 内不做 compaction |

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
- 状态表 compaction 由外部 `NewCompactionTask` 在 refresh 之外触发。

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

#### P1a 结果：base-only 快路径（2026-10）

- 只有事实基表变化时：`affected = base delta keys`；base `now` = 过滤后的 delta；维表读当前态
  （万级）；join MV 按 affected 键过滤读取；不再全量读 before/base/MV，也不再走
  `every_base_row` 的 10M replay。
- 10M/1%：join 5.0–5.7s → **1.9–2.7s**；总增量 7.8–9.0s → **4.4–6.2s**；峰值 RSS
  11.0GB → **6.1GB**。
- 10M/5%：join 7.5s → **4.9s**；总增量 17.2s → **14.6s**；峰值 RSS 9.5GB → 6.2GB；
  但聚合步骤仍 **9.7s**（全量 1.5s），所以 5% 仍慢于全量 6.9s。
- 结论：P1 解决了 join 的全量读；**聚合的 before/state 读取成为下一瓶颈**（100k–500k keys
  的 IN 过滤），P2 必须让聚合直接使用 join MV 的 delete+insert delta 更新组状态。
- 维表变更/混合变更仍走通用路径（低频，代价回退）；全量/rebuild 仍走 materialized 路径
  （P0.2 的流式 sink 尚未覆盖）。

### P2：聚合更新

- join MV delta 上的万级分组聚合；SUM/COUNT/AVG 状态合并。
- MIN/MAX/DISTINCT：value-count 状态或代价回退；区分可合并/不可合并。
- 验收：10M/1% 可合并聚合增量 ≥ 2× 全量；不可合并允许回退但不慢于全量 1.0×。

#### P2a 结果：聚合直接消费 join MV 的 changelog delta（2026-10）

- 内部 MV 源且 delta 中确实含 delete 行时：读**未合并**的 delta，`new_agg` 来自 insert、
  `old_agg` 来自 delete，跳过 before 状态读取；多轮窗口内的版本对会自然抵消。
- rebuild 写的是 insert-only 快照（没有 delete 行），自动回退到 before 路径，避免重复累加
  （`view_chain` 自愈测试覆盖）。
- 10M/1%：聚合 2.5–3.4s → **0.10s**；总增量 4.4–6.2s → **2.0–2.1s**；全量 6.3s；约 3×。
- 10M/5%：聚合 9.7s → **0.15s**；总增量 14.6s → **5.3s**；全量 6.8s。
- 峰值 RSS 6.6–6.8GB。
- 剩余：bootstrap/rebuild 仍走 materialized 路径（5.8–7.4s / 6–9s），流式 sink 待补；
  维表更新/mixed 变更仍走通用路径（低频，下一步按代价回退）。

### P3：维表更新与代价模型

- 估算 fanout/受影响事实行数；默认全量或全表 probe 回退；实测值得时再做 FK 状态表/索引。
- 代价模型输入：delta 行数、维表 fanout、分组数、文件数、MV 行数、内存。
- 验收：维表更新不出现 >2× 的性能悬崖；错误回退率可观测。

#### P3a 结果：维表变更回退（2026-10）

- **通用增量路径在维表变更时不可用**：10M/1% fact + 0.1%/1% 维表更新实测 **195s、峰值 52GB**，
  且增量结果比全量少 3.3 万行（正确性 bug）。
- 回退策略：任何 step source（维表）变化时直接 `rebuild_lookup_chain`；上游 rebuild 写出的是
  insert-only 全量快照（>1M 行），下游聚合检测到后走 `rebuild_sum_count`，避免按 1000 万 key
  读 before 状态。
- 结果：10M/1% fact + 0.1%/1% 维表：总刷新 **7.3–7.5s**，峰值 **6.9GB**，与全量重建结果一致。
- 维表更新低频，~7s 可接受；后续若需要，再按 fanout/代价选择部分增量或全量。旧的通用
  dim 增量路径保留但不再默认使用，需修复后才能作为低成本选项。

#### P0.2b 结果：流式输出到状态写入（2026-10）

- chain/row 的最终写改为 `DataFrame::execute_stream` + 每 100 万行分块 `append_batches`；
  驱动端不再 `collect` 整个输出（首次/重建的 1000 万行）。
- 10M/1%：bootstrap 峰值 RSS **6.7GB → 3.2GB**、总耗时 12–14s → **8.1s**；全量 rebuild 峰值
  **6.6GB → 5.1GB**、6.8s；增量总 2.9–3.4s（join 比 collect 版略慢 ~0.6s），仍约 2× 于全量。
- 峰值（含全量）5.2GB；增量校验通过。
- 剩余：聚合/其他形状的输出仍 collect（输出小）；bootstrap 的**输入** delta 仍 materialize
  （10M 行），后续可把初始刷新也改为 provider 流式扫描 + 流式 delta。

#### P0.2c 结果：执行层输入/输出全面流式化（2026-10）

- `source_baseline` 只保留 `PartitionFiles`、游标和版本；所有 rebuild（join、aggregate、
  window、union、semi/anti、row、multi-join、lookup chain、fallback）直接从 provider 流式
  读取，不再把整表读成 `Vec<RecordBatch>` 再注册 `MemTable`。
- 增量路径的 current / before-window / as-of 以及显式窗口文件组读取也统一走
  `IvmTableProvider`；显式文件组通过 `IvmTableProvider::groups` 扫描，保留 CDC tombstone，
  与旧 `read_partition_files` 语义一致。
- 聚合、join、union、semi/anti、window、row 的输出改为 `append_stream`/`write_dataframe`
  流式写入，驱动端不再 `collect` 整份输出。
- affected-key / group-key / 维表也不再 `collect`：它们以 DataFrame/Provider 形式注册，
  由 DataFusion 的 semi-join / broadcast join 消费；刷新路径的中间步骤不再把数据收进 driver。
  代价是失去 `IN` 列表的 reader 裁剪，部分状态读取会扫描整表/整分区，但内存不随源表/MV
  总行数增长，且不再有 driver 端峰值。
- 附带修复：reader 的 range partition 列不再重复加入 `target_schema`（分区列由
  `table_partition_cols` 追加）；否则 `day = ...` 这类分区谓词会报
  `Ambiguous reference to unqualified field day`。

#### P5b 结果：rebuild 桶并行（2026-10）

- `source_baseline` 拆出只取文件/版本/游标的 metadata 版本；rebuild 不再先整表读一遍
  baseline、再读一遍数据。
- 唯一键 chain 的 rebuild 也按当前文件的 bucket 后缀并行：MV 已 truncate，每个桶只写自己的
  insert 行；万级维表只读一次并广播。
- 4 桶、10M：bootstrap 7.7s → **3.6s**（峰值 2.7GB）；全量 rebuild 6.8s → **2.5s**；
  维表更新（rebuild）7.3s → **3.3s**；增量 1% 仍 0.8–1.1s、5% 3.5s。
- **交叉点变化**：5% 时全量（2.6s）已快于增量（3.5s）；代价回退应在约 3–5% 以上选择全量，
  1% 增量仍约 2–3×。这直接验证了「不是所有情况都追求增量」。
- 剩余：聚合 rebuild 约 1.4s 未桶并行（可按 group 分区）；跨节点任务调度。

#### P3b 结果：代价回退 + 快照识别（2026-10）

- 运行时维护每个 view 的 live rows：bootstrap/rebuild 时设置为写入行数，增量刷新按
  inserts - deletes 调整；进程重启后未知，直到下一次 rebuild/bootstrap。
- 唯一键 chain 的 base-only 刷新按 `delta_rows / live_rows` 估算：≥ **4%** 直接调用
  bucket-parallel `rebuild_lookup_chain`，否则走增量。
- 聚合的快照识别改为「window added files 数 == source current files 数」→ 直接
  `rebuild_sum_count`，不再为了判断快照而扫描 10M 行。
- 10M/4 桶：1% 增量 **0.84–1.21s**（全量 2.8s，约 2.3–3.3×）；5% 回退 **2.80s**
  （原增量 3.5s、全量 2.9s）；维表更新 **2.63s**；bootstrap **2.7s**（聚合 1.35s）；
  峰值 2.0–4.0GB。
- 交叉点约 4%：1% 增量、5% 全量。所有结果与全量重建校验一致。
- 注意：live rows 是 MV 逻辑行数的近似（唯一键 chain ≈ 基表行数）；重启后的首次刷新在
  未知时保守走增量，直到 rebuild/bootstrap 建立估计。

### P4：状态表 compaction 与布局

- 状态表与用户表统一由外部 `NewCompactionTask` compaction（文件数/小文件阈值、
  新 generation、原子切换）；refresh 内不做 compaction；
- 文件大小目标与 bucket 数按集群并行度设计。
- 验收：100 轮后状态表文件数稳定；单文件大小接近目标。

#### P4 结果：state 自动 compaction + MV 外部压缩（2026-10）

- **原语**：`IvmTable::compact` 读当前合并状态 → 丢弃 CDC 墓碑、`rowKinds=insert`、
  `__ivm_epoch=0` → 按桶重写 → `metadata.commit_compaction`（输入分区 + 输出文件，
  一次 `CompactionCommit` 替换分区快照；旧文件由保留清理回收）。
- **state/MV**：生产由外部 `NewCompactionTask` 压缩。早期 refresh 前按文件数阈值
  自动压缩 state 的逻辑（`LAKESOUL_IVM_STATE_COMPACT_FILES`）已在 Phase 3 删除；
  state/MV 都只在 refresh 之外由外部任务压缩，压缩耗时**不计入** refresh 的
  join/agg/total。
- **1M/1%/4 桶/Vortex，100 轮（每 10 轮 out-of-band 压缩）**：
  - 平均 join **319ms**、agg **98ms**、total **417ms**；无 compaction 时平均 total
    1641ms，第 100 轮 2711ms（比 0.5s 全量重建慢 5.4×）。
  - 每次 MV compaction 约 **0.9s**（物化 1M 行），在轮次之外执行，refresh 无尖峰。
- **10M / RustFS（127.0.0.1:9000，Vortex，4 桶，`LAKESOUL_CACHE=1`），20 轮（每 10
  轮 out-of-band 压缩）**：
  - 第 1–10 轮：join **525–644ms**、total **764–1105ms**；压缩后第 11–20 轮：
    join 533–874ms、total 697–2313ms（一次 agg 1439ms 的抖动，来自压缩后的缓存/内存）。
  - 每次 MV compaction **9.5–10.8s**（物化 10M 行，RSS 升到 ~4.3GB），属于外部服务
    成本，**不计入** refresh 延迟；全量重建 2.5s。
  - 无 compaction 的 10M 基线约 join 630ms / total 950ms，20 轮/80 文件时 join 仍
    ~630ms——对象存储 I/O 底噪掩盖了文件增长，10M 的 MV 压缩间隔可以更宽。
- **阈值结论**：外部 compaction 建议从 **32–64 文件** 开始（1M/4 桶约每 10 轮 =
  40 文件；10M 可偏 64 以减少 ~10s 的重写次数）。compaction 耗时不属于 IVM
  refresh 性能，评测时单独统计。
- 结论：MV/state 文件增长是 100 轮漂移的主因；外部按文件数阈值压缩可把 refresh 的
  增量读延迟稳定在数百 ms，避免增量退化为全量的数倍慢。

#### P5a 结果：bucket 并行刷新（2026-10）

- 条件：唯一键 chain、源/MV 桶数相同、空 bucket columns（按主键哈希）、非分区、无未知文件后缀。
- 按事实 delta 文件的 bucket 后缀分组；每个桶独立 replay + 过滤读该桶 MV + 写该桶输出；
  万级维表只读一次并广播；桶任务用 `join_all` 并发；所有桶完成后统一提交 epoch。
- 4 桶、10M/1%：join 2.6–2.8s → **0.77–1.12s**；总增量 2.9–3.4s → **0.86–1.22s**；
  峰值 3.5GB。
- 4 桶、10M/5%：join 5.2s → **3.0s**；总 5.3s → **3.2s**；峰值 6.4GB → **3.5GB**。
- 8 桶本地略慢（文件数约翻倍：fact 1272、MV 152），分布式可用更多桶；本机推荐 4 桶。
- 回退：维表变更、1:N 步骤、分区表、桶数/主键不一致、未知后缀 → 单计划路径。
- 下一步 P5b：rebuild 也按桶并行；聚合按 group 分区；跨节点任务调度。

#### R6 结果：状态读的 pk 索引（2026-10，格式决定成败）

问题：增量 join 为产生 delete 行的 before-image，要按 affected keys 读取 join MV
（状态）；文件按主键哈希分桶、桶内按主键排序，但随机 key 的 `IN` 谓词无法做任何
文件/row-group 裁剪，1%–5% 时等价于整表扫描。

改动（`lakesoul-io/src/pk_locator.rs`）：

- 新增 `MmapIndex::rows_with_prefixes`：把候选 key 排序后与每个文件的已排序索引
  **一次归并**（`rows + keys`），替代每 key 两次二分（`keys * log(rows)`）；候选数
  ≥64 时启用。
- `extract_key_constraints_with_cap` + `LakeSoulIOConfig::{max_pk_candidates,
  bulk_pk_lookup}`：候选上限与 bulk 从常量变成配置，IVM 过滤读可传入更大的上限。
  索引对 **Vortex 表默认开启**（vortex sink 每列 1024 行一个 block），Parquet 默认
  关闭（`LAKESOUL_IVM_STATE_INDEX=1` 可强制开启）。
- 单元测试：`bulk_merge_answers_many_sorted_prefixes`、
  `bulk_merge_matches_binary_search_on_many_keys`（5000 key 与逐 key 二分对拍）。
- benchmark：`ivm_bench_star --format parquet|vortex|vortex-compact`。

10M star、本地存储、4 桶、1 warmup + 2–3 轮（join p50，ms；全量为同格式重建）：

| 变更比例 | Parquet 扫描+IN | Parquet+索引 | **Vortex+索引** | Vortex-compact+索引 | 全量重建 |
|---|---|---|---|---|---|
| 0.01% | 367–422 | 639–674 | **182–204** | — | 2.0–2.8s |
| 0.1% | 390–457 | 757–904 | **266–279** | — | 2.1–2.7s |
| 1% | 756–1258 | 965–990 | **402–421** | 635–662 | 2.0–2.8s |
| 3% | — | — | **707–721** | — | 2.2s |
| 5% | 2862–2890（回退） | — | 2103–2160（回退） | — | 2.3s |

结论：

- **Parquet**：索引每一档都更慢。`LAKESOUL_PK_PROFILE=1` 显示 `take` 为 340–740ms
  （150–1700 命中行、3–8 个文件）：page 有数万行，0.01%–1% 的随机 key 几乎每页都有
  命中，row selection 跳不掉 page；索引只省比较次数，没省 page 读。
- **Vortex**：索引快 2–3×。1024 行 block 让 scattered candidate 只读命中的 block，
  `take` 降到 45–170ms；链总耗时 0.25–0.27s / 0.33–0.35s / 0.48–0.56s / 0.79–0.80s
  （0.01% / 0.1% / 1% / 3%），对 2.0–2.2s 的全量重建是 **4×（1%）到 8×（0.01%）**。
  Vortex 连全量重建也更快（2.0–2.2s vs Parquet 2.6–2.8s，主要省在聚合重算）。
- **Vortex-compact（zstd）** 仍优于 Parquet，但比未压缩慢 ~1.6×（1%：635–662ms vs
  402–421ms），是「体积/IO vs 解压 CPU」的取舍。
- 索引需要 `LAKESOUL_CACHE`（本地索引缓存）；未开启时 locator 自动回退为扫描，
  1% 的 Vortex join 为 742–1285ms（与 Parquet 扫描同级）。**目标场景建议：
  Vortex + LAKESOUL_CACHE**。
- **回退阈值按格式标定**（`lookup_chain`，`LAKESOUL_IVM_REBUILD_RATIO` 可覆盖）：
  Parquet 4%，Vortex **8%**。Vortex 上强制两条路径对比（链总耗时）：

  | 变更比例 | 增量 | 回退重建 |
  |---|---|---|
  | 3% | 0.79–0.80s | — |
  | 5% | **1.18–1.37s** | 2.20–2.27s |
  | 6% | **1.29–1.35s** | 2.38–2.45s |
  | 7% | **1.51–1.63s** | 2.37–2.45s |
  | 8% | 1.83–2.34s | **1.69–1.93s** |

  交叉点在 8% 附近；5–7% 走增量比原 4% 阈值快 1.5–1.7×。回退路径的聚合耗时在
  0.09–0.97s 之间波动（快照识别/页缓存），所以阈值是经验值，8% 是保守取值。
- **桶数**：1% 下 8 桶（583/550ms）比 4 桶（402–421ms）慢，文件数翻倍（1272 vs 636）；
  单机推荐 4 桶，更多桶留给分布式。
- 所以之前「状态读只能全量扫」的结论是 **Parquet page 粒度**造成的；换成 Vortex 后
  key-filtered 状态读在 7% 以内都优于整表重建。

#### P6 结果：分区级刷新、列裁剪与对象存储（2026-10）

- **分区级刷新现状**：
  - source 侧始终是分区级的：`get_table_changelog` 只返回发生变化的分区，
    `window.added_files` 只含这些分区的文件；
  - MV 侧裁剪：`SumCount` 已实现（`mv_partition_restriction`，测试
    `a_partitioned_mv_refresh_prunes_unchanged_partitions`）；本轮为 `LookupChain`
    的 base-only 增量补上（`lookup_chain_mv_partitions`）。条件：MV 分区列 ⊆ 源分区列、
    源主键包含所有源分区列（key 不跨分区）、MV 主键包含分区列；
  - 已补齐 `Row`、`Window`/`TopK`/`SemiAnti`/`Union*` 和两表 keyed Join：
    `mv_partitions_for_source` / `mv_partitions_for_source_mapped` 的条件是 MV 分区列
    映射到源分区列、源主键含源分区列（key 不跨分区）、MV 主键含分区列；Row 还要求该列
    由视图原样物化，Join/MultiJoin 用 source column -> 输出列/别名映射。SemiAnti 仅在
    右侧无变更时按左侧 delta 的 key 过滤状态读并裁剪 MV；Union 取各变更分支的并集；
    Join 仅在一侧变更时裁剪（两侧都变时另一侧的匹配行可在任意分区）；MultiJoin 在
    base 源变更时按 base 分区裁剪，单源变更且 base 分区列是到 source 1 的 join key 时，
    从变更源 delta 的 join-key 值出发，逐级过滤中间源 current 状态并取出下一级 key，
    最终得到受影响的 base 分区（要求变更源的 join key 在其主键内；可变更 join key 属于
    重建场景，保持全读）。无主键（append-only）的两表 join 仍要求单分区
    （`ensure_unpartitioned`）；Fallback 有分区局部重算。
- **列裁剪**：`SUM/COUNT` 的 before-state 读已按列下推（`sum_count_source_projection`
  + `read_before_window_filtered_projected`）；10M、8 个 payload 列、1% 变更下刷新
  1401/1351ms → 1332/1172ms（约 9–12%）。窄表无可裁剪列。
- **对象存储**：RustFS 1M/1% 下 Parquet 反而最快（join 235–250ms），Vortex 252–330ms，
  vortex-compact 413–415ms；此时 MV 只有 4 个文件，固定开销主导，索引无 block 可跳。
  10M 本地 NVMe 上 Vortex+索引的 2–3× 优势来自状态读；对象存储上需要在 10M 重测，
  且 compact/未压缩的选择取决于带宽 vs 读延迟。
- **DataFusion plan（部分完成）**：`IvmTableProvider::scan` 对所有模式返回流式
  `IvmStreamExec`（不再整表物化）；投影下推到 reader，CDC 墓碑在 merge-on-read 之后
  过滤；只把**主键谓词**下推给 reader（`Inexact`，其余谓词交给 DataFusion 的
  `FilterExec`，避免 merge 之前过滤掉某个 key 的最新版本）。fallback recompute 和
  消费端查询因此不再在 driver 端聚齐整表。刷新 SQL 仍把输入注册成 `MemTable`
  （同一个 `mv` 被 SQL 引用多次时 provider 会重复扫描），下一步是单 plan 重写。

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

## 7. 多聚合 state：全量基线、增量点查与 compaction

### 7.1 全量基线要持久化

全量基准不能只执行 `COUNT(*)`；需要把同一 source epoch 的
`GROUP BY` 输出真正写到持久化参考表：

- schema 与 MV 一致（group keys + measures）；
- 使用与 MV 相同的 LakeSoul writer 和线程配置；
- 计时包含聚合、writer 排序/分区、文件与 metadata commit；
- 与增量 MV 按 row/group key 对比，而不是只比较 group 数。

### 7.2 增量 state 用流式 PK lookup

state 表的主键是 `ivm_group_row`（Arrow Row 二进制），并且按该列 bucket。
增量不能用 driver collect 全部 affected keys，也不能让 state 全分区扫描。

目标执行路径：

```text
contrib stream (affected groups, bounded batches)
  -> group_row key batch
  -> state provider / LakeSoulReader bulk PK filter
  -> matched state rows
  -> per-batch state/MV merge
  -> append_stream / bounded append
```

约束：

- keys 不回到 driver，不构造全局 IN 列表；
- state reader 走 `with_state_key_lookup` / `bulk_pk_lookup`；
- 按 changed range partition 裁剪 state；
- affected 比例过高时允许回退全 state/分区扫描，但要有成本阈值；
- 用 `rows_scanned` 证明 state 读量接近 affected groups，而不是 state 总行数。

### 7.3 compaction

- state/MV：统一由外部 `NewCompactionTask` 触发，refresh 代码不包含 compaction。
- 测试统一通过外部 listener 观察 compaction 结果，不再在 refresh 内按文件数
  自动压缩；`IvmTable::compact` 只作为外部任务的等价原语，其单元测试保留。
- 测试关注 compaction 后 state/MV 文件数是否稳定，以及下一次增量 lookup
  是否仍正确。

### 7.4 首轮实现结果

- 增量 `refresh_multi_agg_state` 已改为消费 `contrib` batch stream：
  - 每个 batch 从 `ivm_group_row` 构造 batch-local PK 过滤；
  - `state` 按 changed range partition + PK filter 读取；
  - 在同一个 DataFusion session 中注册 batch-local contrib/state，执行
    state merge；
  - state/MV 输出按有界 chunk 写入，state 与 MV 并行 append。
- 不把完整 key 集合 collect 到 driver；每个 batch 的 key 集合和 state 结果
  都是有界的。
- 小分区真实 delta：25 个窗口全部 `equal=true`，单窗口约 1.2–3.7s。
- 在测试窗口之间显式执行 state/MV compaction：
  - 第 10 轮后约 3.6s；
  - 第 20 轮后约 3.3s；
  - compaction 不计入 refresh 计时；
  - compaction 后继续增量，最终仍 `equal=true`。
- 仍需补：
  - state reader 的 `rows_scanned` 指标，证明 PK lookup 命中而不是全分区扫描；
  - full-day 真实/合成增量与持久化 full_ref 对比；
  - 100 轮文件数与 compaction 稳定性。

### 7.5 persisted full_ref 的首轮结果

测试 harness 新增一个持久化 `full_ref` 表：同一 source epoch 的 61-key
`GROUP BY` 结果通过 LakeSoul writer 写成 Vortex，包含聚合和提交时间。

4 threads、16 buckets、8GB session pool：

| 项目 | 耗时 |
|---|---:|
| 全量聚合 `COUNT(*)`（未物化） | 9.0s |
| 持久化 full_ref（一份 3.75M 行） | **11.8s** |
| bootstrap（state + MV 两份输出） | **29.6s** |
| bootstrap 聚合到第一批输出 | 8.6s |
| bootstrap state/MV 并行写入 | 20.1s |

full_ref 行数与 MV 均为 3,755,984，逐行 hash `equal=true`。
小分区 full_ref：494,478 行，物化 0.86s，`equal=true`。

因此公平比较下：

- 全量物化一份结果约 11.8s；
- bootstrap 两份输出约 29.6s，约 **2.5×**；
- 之前 7–9s 与 bootstrap 的 5× 差异主要来自“COUNT 未物化”和“两份写入”，
  不是 group-by 本身。

后续优化重点仍是：

1. 评估 state 只写一份、MV 异步生成；
2. 复用一次排序写两种 schema；
3. 减少 15 次 writer chunk 的建立/排序/提交开销；
4. 检查 MV bucket 倾斜。

### 7.6 full-day 增量首测：暴露 source 全分区扫描

full-day（source 10.9M 行、3.76M group、state/MV/full_ref 各 3.76M 行）
使用 1% 合成 update/delete/insert 后，增量 refresh **失败**：

- 32GB cgroup：OOM kill；
- 48GB cgroup：DataFusion 报错：

```text
HashJoinInput ... can_spill: false
greedy(used: 6.4 GB, pool_size: 6.4 GB)
```

错误发生在 refresh 内部，`multi-agg keyed incremental finished` 尚未打印。

结论：

- state 的流式 PK lookup 已经生效；
- 但 source 的 `before` 和 `current` 仍然按 changed range partition 全量读取，
  再与 delta keys 做 `LeftSemi`；
- full-day 的 10.9M source 行 + 1% delta keys 仍会产生多个 1GB 级
  HashJoinInput，无法靠扩大 heap 解决；
- 这也说明 source 侧也必须做 **流式 source PK lookup**，不能只优化 state。

下一步：

1. 对 delta stream 按 batch 提取 source primary-key keys；
2. 通过 source reader 的 PK filter 只读 affected rows：
   - before version 用 `read_before_window_filtered`；
   - current 用 changed partition + PK filter；
3. 每个 delta batch 生成有界 contributions，合并后再走现有 state PK lookup；
4. 重新测 full-day 1%、5%；
5. 如果 source delta 本身已经包含完整的新值，可进一步去掉 current source 的全量读。

### 7.7 full-day 1% 增量：source PK lookup + state 分桶并发扫描

在 7.6 的失败之后补了两层优化：

- source 不再把 changed range partition 与 delta keys 做 `LeftSemi`；delta
  stream 按有界 key chunk 取 source before rows，new rows 直接取 delta 的
  `insert` 行（去掉 source current 全量读）。
- state lookup 改走 `IvmTableProvider` 的 lazy scan，而不再直接调用
  `read_current_filtered`。provider 会按 hash bucket 把 scan 拆成多个
  DataFusion execution partition；每个 partition 的 Vortex reader 自动使用
  PK locator。contributions 按
  `LAKESOUL_IVM_STATE_LOOKUP_BATCH_ROWS`（默认 131072）合并成一个有界
  batch 后再发起 state scan。

full-day 1% 实测（source 10.9M、3.76M group；16 buckets；target partitions=4；
48GB cgroup；cache 开启；`LAKESOUL_PK_PROFILE=1`）：

```text
first incremental:
  source before lookup: keys=130998 files=64 total=1.3s
  source chunk total:   1.7s
  state lookup:         keys=119300, 16 bucket partitions,
                        files=15/partition, total=5.9s
  state/MV write:       <1s
  refresh total:        8.5s

second incremental (PK index cached):
  source before lookup: 1.9s
  state lookup:         4.0s
  refresh total:        6.7s
```

对比优化前（state 每个 8192-key batch 都重新打开 256 个 state 文件）：

```text
first incremental state read:  65.5s
second incremental state read: 59.9s
```

结论：

- bucket 并发 + 一次大 key batch 让 state lookup 从 ~60s 降到 4–6s；
- PK index 缓存确实有效，第二次 state lookup 只需 4.0s；
- 当前每个 bucket partition 仍会收到完整的 119k key filter，下一步可以按
  bucket 预先分流 key，进一步降低内存和 locator 候选量；
- 32GB cgroup 在第二次增量前仍会 OOM，48GB 可以完成；内存峰值需要继续
  优化，不能把 48GB 当成生产结论；
- full-day 1% 的最终 full/MV 等价校验和 5% 压测仍未在本轮完成。
### 7.8 按 bucket 预分流 state key，并复查内存

7.7 的实现仍然把完整的 119k key filter 传给每个 bucket partition。每个
partition 都会构造一份 `InList`，并用完整的 key 集去调用 PK locator，
既浪费内存也浪费候选计算。

现在在 recompute 侧先用与 writer 相同的 `compute_scalar_hash` 把
`__ivm_group_row` 分到 bucket，再通过 provider 的 bucket filter 路由：
每个 bucket partition 只拿到自己的约 7.5k 个 key；某个 bucket 没有 key 时
直接使用 `false` filter，不再退化成全量 scan。provider 仍保留 DataFusion
的完整 filter，保证结果正确。

full-day 1% 复测（32GB cgroup，其余配置同 7.7）：

```text
first incremental:
  source before lookup: 1.7s
  state lookup:         2.76s
  refresh total:        5.63s

second incremental (PK index cached):
  source before lookup: 2.2s
  state lookup:         2.66s
  refresh total:        5.61s
```

每次 state lookup 的 profile 从：

```text
keys=119k files=16 total≈0.7–1.1s ×16
```

变为：

```text
keys≈7.5k files=16 total≈1.4–1.7s ×16
```

把完整 key 集复制到 16 个 partition 是 32GB 下第二次增量 OOM 的重要内存
放大点；按 bucket 预分流后，32GB cgroup 可以完成两轮增量，不再需要 48GB。
剩余内存占用主要来自：

- 有界的 119k contributions batch；
- 每个 bucket 的 state 结果约 7.5k 行，最后合并成 119k 行；
- 每个 DataFusion session 的 8GB 内存池上限（实际按需分配）；
- state PK index 走本地 cache/mmap，不占 heap。

下一步：5% 的最终等值校验，以及把 source 的 before key 也按 bucket 预分流
（source 当前已经只有约 1.7s，优先级较低）。

### 7.9 5% 增量结果

同一 full-day fixture，32GB cgroup，16 buckets：

```text
source chunks: 128,920 / 128,047 / 22,899 keys
state keys:    237,933
state lookup:  5.7s
refresh total: 10.4s
```

这次运行没有 OOM。full-day 1% 的最终 full/MV 等价校验在 48GB 下通过
（full_hashes == mv_hashes，3,753,875 个 group）；32GB 下增量 refresh 本身
完成了，但 pilot 紧接着执行的全量校验 aggregate 在 32GB 时 OOM，因此不能把
“32GB 能完成校验”写成结论。生产 refresh 的内存峰值和校验/查询的内存峰值需要
分开评估。

### 7.10 source before 按 bucket 预分流、流式校验与分阶段内存

source 的 before 读取也改为：

- 从 `before_window_groups` 一次解析窗口前的文件组；
- 用 source primary key 的 `compute_scalar_hash` 按 bucket 分流；
- 通过 `IvmTableProvider::groups(...).with_bucket_filters(...)` 让每个
  bucket partition 只拿本桶 key，并自动走 Vortex PK locator。

full-day 1% profile：

```text
source bucket partitions: keys≈1.3k–12k files=4 total≈0.11–0.15s ×16
source chunk:             0.9s
state lookup:             2.07s
refresh total:            3.7s
```

校验不再把 3.75M 个 md5 字符串收进 `Vec` 再排序，而是流式读取哈希结果，
累加两个独立的 64-bit digest 和行数；full/MV digest 相等即通过。这样校验
不需要在 driver 上保留全量哈希集合。

full-day 1% 分阶段 RSS（32GB cgroup，pilot 进程自身采样；start/peak/end）：

| 阶段 | start | peak | end |
|---|---:|---:|---:|
| initial full aggregate | 9.6GB | 20.6GB | 13.0GB |
| bootstrap refresh | 13.1GB | 20.3GB | 13.0GB |
| bootstrap validation | 15.8GB | 24.5GB | 16.5GB |
| incremental refresh | 19.2GB | 19.3GB | 19.0GB |
| incremental validation | 19.0GB | 23.2GB | 20.9GB |

结论：

- 流式校验后，32GB cgroup 可以完成 full-day 1% 的最终 full/MV 校验；
- refresh 本身的峰值增量很小（约 0.1GB），但校验阶段仍有约 4GB 峰值增量；
- process baseline 已包含 source load、bootstrap 表和 DataFusion session，不能把
  peak 直接当成单个阶段 allocations；
- 5% 已补测；refresh/validation 仍需继续观察，见 7.11。

### 7.11 5% source bucket + 流式校验

5% 增量复测（source bucket、流式校验、32GB）：

```text
source chunks: 130,233 / 129,220 / 20,413 keys
source chunk:  2.2s / 2.5s / 2.8s
state keys:    237,933
state lookup:  7.05s
refresh total: 16.52s
validation:    full/mv rows=3,744,932, digest equal
memory:
  refresh:     start 23.6GB, peak 30.1GB, end 26.6GB
  validation:  start 26.6GB, peak 31.1GB, end 27.6GB
```

5% 在 32GB cgroup 下完成了 refresh 和流式校验，但峰值已经接近上限；生产上
不能假设 5% 变化仍能保持较大 headroom。

### 7.12 内存配置复查与 jemalloc

配置复查结论：

- DataFusion refresh session 使用 `LAKESOUL_IVM_MEMORY_LIMIT_MB`（本次 8192，
  对应 6.4GB pool），并新增 `LAKESOUL_IVM_BATCH_SIZE`（本次 4096）；
- LakeSoul writer 的 spill pool 由 `LAKESOUL_IVM_WRITE_POOL_SIZE` 控制（本次
  2048MB），state/MV 并行写时各自一个 writer；
- reader spill pool 由 `LAKESOUL_IVM_READ_POOL_SIZE` 控制（本次 512MB），
  注意它是 **每个 reader/partition** 的预算，16 个 bucket 的理论上限是
  16×512MB，不是全局 512MB；
- 实测把 write pool 降到 512MB 时，bootstrap 的 ExternalSorter 需要
  508.4MB，但 pool 只剩 502MB，直接报 `Resources exhausted`；writer pool
  至少要留出约 1GB 余量，2048MB 可稳定通过。

使用：

```sh
export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
export MALLOC_CONF=background_thread:true
```

并设置 batch=4096、read pool=512MB、write pool=2048MB 后，full-day 复测：

1%：

| 阶段 | start | peak | end |
|---|---:|---:|---:|
| initial full aggregate | 1.9GB | 14.1GB | 4.1GB |
| bootstrap refresh | 4.9GB | 16.5GB | 1.6GB |
| bootstrap validation | 2.1GB | 15.6GB | 1.9GB |
| incremental refresh | 6.7GB | 6.7GB | 2.9GB |
| incremental validation | 2.9GB | 15.6GB | 2.0GB |

5%：

| 阶段 | start | peak | end |
|---|---:|---:|---:|
| incremental refresh | 8.0GB | 19.9GB | 4.1GB |
| incremental validation | 4.1GB | 16.1GB | 1.8GB |

与没有 jemalloc、batch=8192、read/write pool=2048MB 的旧结果相比：

- 1% incremental refresh peak 从 19.3GB 降到 6.7GB；
- 1% validation peak 从 23.2GB 降到 15.6GB；
- 5% refresh peak 从 30.1GB 降到 19.9GB；
- 5% validation peak 从 31.1GB 降到 16.1GB。

这说明此前“偏高”很大一部分是 allocator 保留已释放页造成的 RSS 碎片，
而不是 DataFusion/writer 的活跃内存。jemalloc 会及时归还页面，配合更小的
batch 和 reader pool 后内存明显下降。生产环境仍应把 reader pool 按 bucket
数计入总预算，不能只按单个 512MB 计算。

