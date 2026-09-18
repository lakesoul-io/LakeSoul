# LakeSoul IVM 基础能力补齐计划

> 状态：生效中。本文件是基础能力补齐阶段的实施计划；IVM 上层设计见附录 A。
> 基线：`lakesoul-ivm` worktree，`ca83ed1d`（含 #883/#884/#885）。
> DataFusion 54→55 升级由其他同事负责，本计划不包含、不依赖该升级。

## 0. 背景与边界

为 IVM（增量物化视图）落地补齐 **LakeSoul 核心缺失能力**。改动落在
`rust/lakesoul-metadata`、`rust/lakesoul-io`、PostgreSQL schema 与测试层面。
IVM 上层（调度、delta DAG、状态表协议）作为后续阶段，本计划为其定义接口约束。

### 已确认决策

1. **bucket 前缀**：走内部表属性方案，不改元数据模型；JVM 侧 v1 不做拒绝逻辑，
   后续由 JVM 在 `list tables` 时过滤掉内部表使其不可见。
2. **changelog**：Rust 侧按 version 区间消费，timestamp 仅作水位/发现；显式返回
   `requires_rebuild` 与 `deleted_partitions`，与 JVM 现有语义有意分歧。
3. **保留策略**：v1 只做文档约束，不做 cursor-aware GC。
4. 先写本计划，再从 P0-1（as-of 读 API）开始实现。

## 1. 能力矩阵与优先级

| 能力 | 现状 | 优先级 |
|---|---|---|
| as-of / 历史快照读（Rust API） | DAO SQL 已存在（`lib.rs:362-392,501-516`），无 `MetaDataClient` 封装，无整表 as-of | P0 |
| changelog / 增量读（Rust API） | Scala 有实现（含 4 个已知缺陷），Rust 侧完全缺失 | P0 |
| 桶列与合并键解耦（bucket = key 前缀） | 不存在，`TableInfo.partitions` 的 hash 段 == 合并键 | P0 |
| Rust commit 乐观并发（OCC） | TODO 空实现，会静默丢更新（`metadata_client.rs:628`） | P1 |
| 保留/清理与 cursor 约束 | 默认不清理；opt-in TTL 会删历史 | P1（文档级） |
| 点查泛化（任意 probe 列） | 仅 Vortex、单 Int32/64 PK、10k 上限、无落盘索引 | P2（v1 不依赖） |
| 行组大小表属性 | 已可编程配置（`max_row_group_size` 等），无需核心改动 | — |
| epoch 标记 | 不需要核心字段，IVM 自建 `ivm.epochs` 映射 | — |

## 2. P0-1 as-of / 历史快照读

**现状**

- `get_all_partition_info`（`rust/lakesoul-metadata/src/metadata_client.rs:1080`）走
  `DISTINCT ON ... version DESC` 只取最新。
- 历史查询 DAO 已实现但 Rust 无调用者：精确版本、版本区间、时间戳区间、
  latest-up-to-time 标量（`rust/lakesoul-metadata/src/lib.rs:362-392,501-516`）。
- 选定 `PartitionInfo` 后的文件解析已是快照正确的（`metadata_client.rs:1029-1060,1237-1258`）。

**改动**

1. 新 DAO：`ListPartitionByTableIdAndTimestamp`，按分区取 `timestamp <= $2` 的最新版本：

   ```sql
   select distinct on (table_id, partition_desc)
       table_id, partition_desc, version, commit_op, snapshot, timestamp, expression, domain
   from partition_info
   where table_id = $1::TEXT and timestamp <= $2::BIGINT
   order by table_id desc, partition_desc desc, version desc
   ```

   边界用 `<=`，与 JVM `getLastedVersionUptoTime` 一致；`partition_info.timestamp`
   为 DB 毫秒时钟（`script/meta_init.sql:86-98`）。

2. `MetaDataClient` 新方法：

   ```rust
   async fn get_all_partition_info_as_of(&self, table_id, as_of_ms) -> Result<Vec<PartitionInfo>>
   async fn get_partition_info_as_of(&self, table_id, partition_desc, as_of_ms)
       -> Result<Option<PartitionInfo>>
   ```

3. 复用 `active_data_files` 解析 as-of 文件集；IVM 用显式文件列表构造
   `LakeSoulIOConfig`，v1 不改 `TableProvider::scan`（后续可选加 provider option）。

**验收**

- v1..v5 各版本快照下，as-of t3 文件集 == 该版本 snapshot 解析结果。
- 与 JVM 同参数结果一致。
- 分区在 t3 尚未创建/已删除的边界行为明确。

## 3. P0-2 changelog 增量读

**现状（JVM 语义，需镜像）**

- 窗口 `[start,end)` 落在 `partition_info.timestamp`；baseline = `timestamp < start`
  的最新版本，其 snapshot 做减集。
- `CompactionCommit` 的 `snapshot[0]`（compaction 产物）排除，`snapshot[1:]` 作为
  并发 append 保留。
- `UpdateCommit` 出现在窗口中间 → 返回空；出现在首位 → 整体透传。
- 无 baseline → 返回 end 时刻全量快照。
- 结果只含 add 文件（del 仅用于压制同路径 add）。

**JVM 已知缺陷（Rust 版修正）**

1. `getPartitionsFromTimestamp/Version` 无 `ORDER BY`，首行/baseline 逻辑依赖升序。
2. 窗口中出现 `UpdateCommit` 静默返回空，调用方 cursor 照常推进 → 丢窗口。
3. 分区删除（`DeleteCommit`/空 snapshot）对增量读不可见。
4. 毫秒时间戳碰撞使边界不可靠。

**Rust API（按 version 消费，timestamp 只做水位/发现）**

```rust
pub struct PartitionChangelog {
    pub partition_desc: String,
    pub added_files: Vec<DataFileInfo>, // add-only，按 commit 顺序
    pub partition_deleted: bool,        // 窗口内 DeleteCommit
    pub requires_rebuild: bool,         // 窗口内 UpdateCommit 或 baseline 丢失
    pub to_version: i64,                // 新 cursor
    pub to_timestamp: i64,              // to_version 的 partition_info.timestamp
}

pub struct IncrementalWindow {
    pub partitions: Vec<PartitionChangelog>,
    pub added_files: Vec<DataFileInfo>,  // 扁平化
    pub deleted_partitions: Vec<String>,
    pub requires_rebuild: bool,          // 任一分区需要重建
}

async fn get_partition_changelog(
    &self, table_id, partition_desc,
    from_version_exclusive: i64, to_version_inclusive: i64,
) -> Result<PartitionChangelog>

async fn get_incremental_files(
    &self, table_id, partition_desc: Option<&str>,
    from_version_exclusive: i64, to_version_inclusive: i64,
) -> Result<IncrementalWindow>
```

- 版本区间查询复用 `ListPartitionVersionByTableIdAndPartitionDescAndVersionRange`
  （已补 `order by version`），baseline 用 P0-1 的精确版本查询。
- `partition_desc=None` 时对 `get_all_partition_info` 列出的每个当前分区各取一段窗口，
  并把窗口上界收敛到该分区最新版本；被物理删除的分区需按 desc 显式查询才能感知。
- `requires_rebuild=true` 或 `partition_deleted=true` 时不返回 `added_files`
  （避免把不完整窗口当 changelog 应用）。
- 表级一次扫描 DAO（替代 N 次分区查询）为后续优化，v1 用 per-partition 查询保证语义。

**实施记录（已完成）**

- 新 DAO：`SelectOnePartitionVersionByTableIdAndDescAndTimestamp`、
  `ListPartitionByTableIdAndTimestamp`（P0-1）；版本区间补 `order by version`。
- `MetaDataClient::get_partition_info_by_version` / `get_partition_changelog` /
  `get_incremental_files`，helper `active_added_files`（镜像 JVM `filterFiles`）。
- 集成测试 `rust/lakesoul-metadata/tests/incremental_read.rs`（7 个场景）。
- 顺带修复：`commit_data` 的 Update/Compaction/Delete 分支缺少 snapshot 容器，
  导致 `TransactionInsertPartitionInfo` 把最后一个真实分区 pop 掉、版本未落库。

**验收**

- 构造 append-only 链 / 中途 compaction / 中途 UpdateCommit / 分区删除 /
  同 ms 多次提交 / cursor 重放 各场景。
- append-only 场景与 JVM `getIncrementalPartitionDataInfo` 结果逐文件一致。
- 修正场景断言 rebuild 信号而非空集。

## 4. P0-3 桶列与合并键解耦（bucket = key 前缀）

**现状**

hash 桶列 == 合并键 == `TableInfo.partitions` 的 hash 段
（`rust/lakesoul-io/src/writer/async_writer/partitioning_writer.rs:224-235`、
`rust/lakesoul-datafusion/src/lakesoul_table/helpers.rs:41-77`、
`rust/lakesoul-datafusion/src/datasource/table_provider.rs:242-294`），
无法表达"按 join_key 分桶、按 (join_key,row_id) 合并"。

**方案（内部属性，不动元数据模型、v1 不暴露用户）**

1. 新表属性 `lakesoul.ivm.bucket_columns`（逗号分隔）+ `lakesoul.ivm.internal=true`；
   经 `properties` JSON 存取，无 PG DDL 变更。
2. `LakeSoulIOConfig` 增加 `hash_partitioning_columns`（默认 = `primary_keys`）；
   writer 的 `Partitioning::Hash` 改用它，**排序键仍是 range ++ primary_keys ++ aux**
   （保证合并键有序）。
3. reader 桶裁剪（`rust/lakesoul-io/src/reader.rs:164-225`）改用 bucket 列；
   仅当所有 bucket 列被等值/IN 谓词钉住时才裁剪。
4. `create_io_config_builder_from_table_info` 读取属性并校验：必须为 `primary_keys`
   的前缀，否则报错。
5. JVM 可见性：v1 不实现拒绝逻辑；后续由 JVM 在 `list tables` 时按
   `lakesoul.ivm.internal` 过滤内部表（另行安排）。
6. 行组大小由 IVM 构造 IOConfig 时直接传 `max_row_group_size`，不做表属性。

**验收**

- PK=(k,row_id)、bucket=(k) 的表：同 k 不同批次写入落在同一桶/文件。
- 按 k 过滤只读该桶；merge-on-read 同 k 多行共存；与全量结果 EXCEPT ALL 为空。
- 前缀校验负例报错。

**实施记录（已完成）**

- `LakeSoulIOConfig` 新增 `hash_partitioning_columns`，getter
  `hash_partitioning_columns_slice()` 空值时回退 `primary_keys`；builder
  `with_hash_partitioning_columns`。
- writer（`partitioning_writer.rs`）hash 分区改用 bucket 列；排序键保持
  `range ++ primary_keys ++ aux`，满足 `RepartitionByRangeAndHashExec` 对
  "range+hash 是输入序前缀" 的要求。
- reader（`reader.rs`）桶裁剪改用 bucket 列，并新增门控：**仅当 bucket 列恰好
  1 个** 且过滤器把该列钉住时才裁剪（现有裁剪按单列标量哈希，多列 bucket 会
  误裁剪；`row_id` 这类合并键列不再触发裁剪）。
- `LakeSoulTableProperty` 新增 `lakesoul.ivm.internal` /
  `lakesoul.ivm.bucket_columns`；`create_io_config_builder_from_table_info`
  校验 "internal=true" 与 "bucket 列是 primary keys 前缀"，否则报错。
- 测试：`rust/lakesoul-io/tests/bucket_prefix.rs`（同 k 跨批次同桶 + 合并键
  保留；过滤 row_id 不被误裁剪）、config 单测 2 个、datafusion helpers 单测 4 个。
- JVM 可见性按决策延后：后续 JVM 在 `list tables` 按 `lakesoul.ivm.internal`
  过滤内部表。

## 5. P1-1 Rust commit 乐观并发（OCC）

**现状**

`metadata_client.rs:594-641` 冲突分支为空：read_version != cur_version 时保留当前
（别人的）snapshot、version+1 提交，**新文件永不发布**（静默丢更新）；
`transaction_insert_partition_info` 只返回行数，PK 冲突时重试同 payload 最终报
PG 唯一键错误。JVM 侧有完整实现（`lakesoul-common/src/main/java/com/dmetasoul/lakesoul/meta/DBManager.java:559-711`）。

**改动（镜像 JVM）**

1. `commit_data` 冲突分支：调 `ListCommitOpsBetweenVersions(read+1, cur)` 判定
   - 仅 Append/Merge → 合并 snapshot（`updateSubmitPartitionSnapshot` 语义：
     mine ++ (cur − read)）
   - 含 UpdateCommit → 报错（IVM 视为需重建）
   - 含单个单元素 CompactionCommit → 折叠重试；否则报错/跳过该分区
2. `transaction_insert_partition_info` 检测 SQLSTATE 23505 返回冲突标记；
   有界重试（`MAX_COMMIT_ATTEMPTS`）。
3. 新增 DAO wrapper：`ListCommitOpsBetweenVersions`（SQL 已在 `lib.rs:385-388`）。

**验收**

并发 append+append、append+compaction、update+append 三组测试在
`rust/lakesoul-metadata/tests/` 下无丢文件、snapshot 正确、版本单调。

**实施记录（已完成）**

- `commit_data` 改为有界重试循环（`MAX_COMMIT_ATTEMPTS = 5`，对齐
  `DBConfig.MAX_COMMIT_ATTEMPTS`）：每轮重新读 `cur_map` 并规划分区行。
- 冲突判定复用现有返回语义：`TransactionInsertPartitionInfo` 在唯一键冲突时
  已 rollback 并返回 `Ok(0)`，因此按 "返回行数 != 期望行数" 判定冲突并重试，
  无需改动 lib.rs 的 JNI 行为（原计划的 SQLSTATE 23505 检测由此替代）。
- `plan_partition_commit` 镜像 `DBManager.commitData` 及其
  `appendConflict`/`mergeConflict`/`updateConflict`/`compactionConflict`：
  - Append/Merge：跨轮次缓存计划行（`planned`），仅在本分区无新提交时复用；
    当前 op 为 Delete 时报错。
  - Update：中间含 Update，或含多个 op 且有 Compaction → 报错；中间是单个
    无并发追加的 Compaction → 折叠；否则 `submitted ++ (cur − read)` 合并。
  - Compaction：中间含 Update/Compaction → 跳过该分区；否则合并快照。
  - Delete：保持原语义（要求 read 提供 desc，版本 +1、清空 snapshot）。
  - 首次提交的分区版本从 -1 起算（对齐 JVM `getOrCreateCurPartitionInfo`）。
- 新增 `merge_submitted_snapshot`（JVM `updateSubmitPartitionSnapshot` 语义）与
  `get_commit_ops_between_versions`（`ListCommitOpsBetweenVersions` DAO wrapper）。
- 测试 `rust/lakesoul-metadata/tests/commit_occ.rs` 6 例：4 写者并发 append
  无丢文件；Compaction/Update 与并发 Append 合并；stale Update 与 Update 冲突
  报错；stale Compaction 在有 Update 时跳过；stale Update 越过带并发追加的
  Compaction 报错。

## 6. P1-2 保留策略与 cursor 约束

- 事实：默认**不自动删除**历史；唯一风险来自 opt-in 的 `partition.ttl` /
  `compaction.ttl` / 异步清理 job / `cleanOldCompaction`。
- v1 策略：文档约束"IVM 消费的表不配置上述 TTL"，不做 cursor-aware GC。
- 后续（P2）：新建 `ivm.cursors` 水位检查点，清理前校验 `min(cursor) - grace`；
  可参考 `vector_index_lease` 模式（`rust/lakesoul-metadata/src/vector_index.rs:65,149`）。

**实施记录（已完成）**：约束写入 crate 级文档
（`rust/lakesoul-ivm/src/lib.rs` 的 `# Retention` 一节）：IVM 消费的表必须保持默认
保留策略，不配置 `partition.ttl` / `compaction.ttl` / `dataExpiredTime`，也不启用
`cleanOldCompaction`；cursor-aware GC 留待后续。

## 7. P2 预研项（明确暂缓）

- `pk_locator` 泛化（任意列/字符串/parquet/非唯一键）：v1 join 用"桶裁剪 +
  row-group min/max + sort-merge"，不依赖点查。
- `data_commit_info.timestamp` 索引、`DataCommitInfo.timestamp` 秒/毫秒不一致
  （`rust/lakesoul-datafusion/src/catalog/mod.rs:309` vs
  `metadata_client.rs:798`）归一化。
- JNI DAO offset 错位（`lakesoul-common/src/main/java/com/dmetasoul/lakesoul/meta/jnr/NativeUtils.java:57`
  `+10` 实际命中 `SelectTableDomainById`）——Java "按表查最新 commit" 路径失效，
  独立小 fix。
- as-of 参数下沉到 `TableProvider::scan` / provider options。

## 8. 里程碑

| 阶段 | 内容 | 验收 |
|---|---|---|
| F0 | P0-1 as-of API + P0-2 changelog API + 测试 | 与 JVM append-only 结果一致；修正场景返回 rebuild/删除分区信号 |
| F1 | P1-1 OCC + 并发测试 | 三组并发场景无丢更新 |
| F2 | P0-3 bucket=key 前缀（writer/reader/属性校验） | 状态表读写与 EXCEPT ALL 通过 |
| F3 | IVM 冒烟：单表 SUM/COUNT 增量刷新 + join 状态表读写 | MV 与全量查询双向 EXCEPT ALL 为空 |

**F3 实施记录（已完成冒烟切片）**

- 新 crate `rust/lakesoul-ivm`：
  - `metadata`：PG schema `ivm`（`views` / `cursors`）及 CRUD；DDL 用
    `do $$ ... exception when duplicate_schema/duplicate_table` 保证并发初始化安全。
  - `table`：内部表创建（`lakesoul.ivm.internal=true`、bucket 前缀属性、parquet）+
    `append_batch`（keyed 表走 partitioning writer + stable sort，一次提交
    delete/insert）+ `read_files`/`read_current`（MOR）。
  - `runtime`：`SumCountView` 声明式视图（`SUM`/`COUNT` over append-only 源表）。
    `refresh_sum_count` 按分区 cursor 消费 changelog（P0-2），用 DataFusion 聚合 delta，
    与 MV 当前状态合并后写 `delete(old) + insert(new)`，提交成功后再推进 cursor。
- 测试 `tests/aggregate_refresh.rs`：
  1. 两轮增量刷新后 MV 状态 == 源表全量 `GROUP BY`（含第二轮同 key 更新）；
     cursor 版本/时间戳正确、无新提交时 refresh 为 no-op；
  2. PK=(k,row_id)、bucket=(k) 状态表跨批次写入后 MOR 读回全部行（F3 的
     "join 状态表读写"部分）。
- 已知缺口（下一步）：MV 提交与 cursor 更新之间无原子性，crash 可能重放窗口；
  epoch 幂等（`__ivm_epoch` + `ivm.epochs` 发布）尚未实现；SQL 视图前端未开始。

**Crash 重放保护实施记录（已完成）**

- epoch 改为窗口确定性哈希（`window_epoch`：FNV-1a over view_id + 排序后的
  `(source_table_id, partition_desc, to_version)`），同一窗口重试得到同一 epoch，
  不依赖时钟且与窗口一一对应。
- sum/count：状态读取同时取每组的 `__ivm_epoch`；构建写入批次时，若某组状态
  epoch 已等于当前窗口 epoch，说明该窗口已应用，跳过该组（幂等），避免
  crash 后重放导致重复计数。
- join：输出行携带 `__ivm_epoch`；append 前扫描输出已有 epoch
  （`applied_output_epochs`），命中则跳过本次 append，只推进 cursor。
- 测试：模拟"数据已提交、cursor 未推进"（把 cursor 回拨后重跑）——
  sum/count 状态与 MV 版本号不变、返回 epoch 相同；join 输出不重复、epoch 相同。
- 仍未完成：`ivm.epochs`（epoch → commit_id 发布，需要 commit API 返回 commit id）
  与 `ivm.states`；join 输出的 epoch 扫描目前是全量读，后续可用 epoch 索引表替代。

**Join 增量刷新实施记录（已完成冒烟切片）**

- `JoinView`（inner equi-join，两侧均 append-only，未分区）：每个窗口计算
  `ΔL ⋈ R_before + L_before ⋈ ΔR + ΔL ⋈ ΔR`（inclusion-exclusion），
  `before` 用 P0-1 的 as-of 读（`IvmTable::read_as_of`）按各自 cursor 时间戳重建。
- 输出表 append-only（`join_key, left_value, right_value, __ivm_epoch`）；
  只要两侧只增，每个 join pair 恰好产生一次，累计输出恒等于 `L_now ⋈ R_now`。
- 测试 `tests/join_refresh.rs`：两轮双边窗口（第二轮三项都有贡献）后逐行等于
  全量 join；无新提交时 no-op；每侧 cursor 正确推进。
- 已知缺口：仅支持 inner join + Int64 key/value + 未分区 + append-only 源；
  非 append-only 源需要带 retraction 的 join delta（inclusion-exclusion 配合
  状态表），留待下一阶段。

## 9. 风险与开放问题

1. bucket 前缀属性为"IVM 内部表"专用，JVM 引擎误读会得到错误结果 → 需要
   `internal` 标记 + JVM `list tables` 过滤（后续）。
2. changelog API 与 JVM 语义有意分歧（version 消费、rebuild 信号），需在文档里
   写清楚，避免两套实现漂移。
3. `partition_info.timestamp` 是 DB 时钟，多实例时钟一致性影响水位 W；版本消费
   可消除正确性依赖，但 W 仍用于调度。
4. OCC 重试与 PG 事务隔离（当前 READ COMMITTED）需并发测试覆盖。

## 附录 A. IVM 上层设计（后续阶段，摘要）

- **表模型**：MV 输出表（PK=输出键，含 `__ivm_cnt/__ivm_epoch/rowKinds`）、
  join 状态表（PK=(join_key,row_id)，bucket=join_key 前缀）、
  `ivm_aggval/aggdistinct/match`、`ivm.views/cursors/epochs/states`（PG schema `ivm`）。
- **刷新协议**：到期 view → 读窗口（P0-2）→ 拓扑序 delta 计算 → 一次 append commit
  （同 key delete 前 insert 后，复用
  `rust/lakesoul-io/src/physical_plan/self_incremental_index_column.rs` stable sort）
  → 推进 version cursor → 失败置 dirty 全量重建。
- **算子**：投影/Filter/Union ALL 透传；SUM/COUNT/AVG 状态即 MV；MIN/MAX 有删除用
  值状态表；DISTINCT/COUNT(DISTINCT) 用计数状态；Window 按受影响分区 delete+insert
  重算；Join 用 inclusion–exclusion（`Δ = Σ_{S≠∅} (-1)^{|S|-1} ⋈_{i∈S} ΔR_i ⋈_{i∉S} State_i`，
  N≤3~4），join key = 源 PK 直读源表，否则建状态表。
- **调度/执行**：Rust tokio 调度 + IVM 自建 scan 算子（显式文件列表 + as-of/changelog），
  不阻塞 DataFusion 升级；PG `CREATE/REFRESH MATERIALIZED VIEW` 表面在
  `rust/postgres-lakesoul` 上后续对接。
