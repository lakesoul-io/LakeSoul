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
  epoch 幂等尚未实现（后续记录与 `EPOCH.md` 已给出方案）；SQL 视图前端未开始。

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
- 仍未完成：按 `EPOCH.md` 的设计落地"元数据优先"的 epoch 协议——
  `ivm.epochs`（单调 epoch + `window_key` 去重 + `mv_versions_before` 比较）替代
  join 的全量 epoch 扫描；`ivm.states`、SQL 视图前端仍未开始。

**Epoch 协议实施记录（已完成 `EPOCH.md` 步骤 1–3）**

- `ivm.views` 增加 `last_epoch` / `generation`；新增 `ivm.epochs`
  （`view_id, generation, epoch` 主键，`window_key` 唯一索引，
  `to_versions` / `mv_versions_before` / `mv_versions` / `status`）。
- `IvmMetadata`：`begin_epoch`（已 committed → 跳过；pending → 恢复；否则用
  `views.last_epoch` 分配单调 epoch 并插入 pending）、`mark_epoch_committed`、
  `get_epoch`、`list_committed_epochs`、`max_committed_to_versions`，
  以及测试/恢复辅助 `set_epoch_pending`。
- runtime：`window_key` 规范串取代哈希 epoch；刷新前一次 `ivm.epochs` 点查 +
  一次分区版本比较即可判定"是否已应用"，正常重放与 pending 恢复都不再读数据；
  窗口下界必须等于该源已提交的最大 `to_version`，否则报错要求重建（cursor 回退
  超过上一窗口时不再静默重复）。
- 删除 join 的 `applied_output_epochs` 全量扫描；`__ivm_epoch` 列保留为审计/兜底。
- 测试：`tests/epoch_protocol.rs`（pending 已写跳过、pending 未写应用、
  回退越界报错）与 join 的 pending 跳过用例。
- 未完成：`ivm.states`、SQL 视图前端；consumer 水位 GC 见 `EPOCH.md` §9。

**Rebuild 与消费者读取实施记录（已完成 `EPOCH.md` 步骤 4–5）**

- `IvmMetadata`：`set_view_status` / `view_status` / `bump_generation` /
  `delete_cursors` / `latest_committed_epoch`。
- `IvmTable`：`truncate`（空 snapshot 的 CompactionCommit 清分区，避免 Delete 后
  无法再 Merge/Append）、`read_at_versions`（按 epoch 记录的 MV 版本读快照）。
- `IvmRuntime`：`rebuild_sum_count` / `rebuild_join`（rebuilding → generation+1 →
  删 cursor → truncate → 读源全量状态重算 → `rebuild:<generation>` epoch 提交 →
  cursor 重置到最新 → active）；`view_state_at_epoch` / `latest_epoch` 供消费者
  按 epoch 定位一致快照。
- 测试 `tests/rebuild.rs`：重建后状态==全量聚合、cursor/generation/window_key 正确、
  重建后仅消费新提交；join 重建输出==全量 join；`view_state_at_epoch` 能分别读出
  两个 epoch 的历史快照；回退报错后 rebuild 恢复。
- 未完成：consumer 水位 GC（`ivm.consumers`）、SQL 视图前端。

**Upsert 源支持实施记录（已完成）**

- `refresh_sum_count` / `rebuild_sum_count` 不再要求源是 append-only；源带主键时按
  upsert 语义维护：
  - delta 用 `read_files`（按主键 MOR 合并）读取窗口内变更文件的**最终版本**（同
    窗口多次更新只算最后一次）；
  - 旧值用 P0-1 as-of 读窗口起点快照，按主键 semi-join 出"键发生变化的旧行"；
  - `delta = aggregate(new rows) - aggregate(changed old rows)`；含
    `rowKinds='delete'` 的 delta 行只参与旧值回收、不计入新值；
  - 全量重建本就按主键合并读取，天然支持 keyed 源。
- 注意事项：delta 的删除识别依赖源表存在字面量 `rowKinds` 列（大小写敏感，SQL
  里用精确列名）；表属性 `lakesoul_cdc_change_column` 尚未接入；join 仍要求两侧
  append-only 且未分区。
- 测试 `tests/upsert_refresh.rs`：值更新、组迁移（group 变化）、同窗口同键多次
  更新、`rowKinds='delete'`（含删除不存在的键）、keyed 源 rebuild，均与全量聚合
  交叉验证。

**MIN/MAX 实施记录（已完成）**

- 新增 `MinMaxView`：MV 表 `(group, value, rowKinds, __ivm_epoch)`（PK=group），
  值分布状态表 `(group, value, value_count, rowKinds, __ivm_epoch)`
  （PK=(group,value)，bucket=group）。
- 刷新：窗口 delta → `(group,value)` 计数变化（append-only 直接 +1；keyed 源用
  as-of 旧值 semi-join 回收）→ 应用状态表（逐行 delete(old)+insert(new)）→
  受影响组从状态重算极值 → 写 MV（delete(old)+insert(new)）。
- 幂等：状态行携带 `__ivm_epoch`，重放时同 epoch 的键跳过状态变更；MV 重写是
  同主键同值的幂等写，因此 crash 在状态与 MV 之间也不会重复计数。
- `rebuild_min_max`：清空 MV 与状态表，从源全量重建计数与极值，发布
  `rebuild:<generation>`。
- 测试 `tests/min_max_refresh.rs`：append-only 源的 MIN 与 MAX、keyed 源的
  更新/删除/组清空、窗口重放不重复、rebuild 后仅消费新提交。
- 未完成：`ivm.states` 状态表注册（当前由调用方创建并持有 handle）、
  Window、SEMI/ANTI、join upsert。

**DISTINCT 聚合实施记录（已完成）**

- `MIN/MAX` 与 `COUNT(DISTINCT)`/`SUM(DISTINCT)` 共用值分布状态表：schema
  统一命名为 `value_count_mv_schema` / `value_count_state_schema`
  （旧名 `min_max_*_schema` 保留为别名）。
- 新增 `DistinctAggView`（`ViewSpec::DistinctAgg`，`DistinctAggKind::{Count,Sum}`）：
  刷新/重建复用同一套"值计数 → 受影响组重算"逻辑，仅 MV 取值不同
  （distinct 个数 / distinct 值之和）。
- 内部重构：`refresh_value_count` / `rebuild_value_count` 接受
  `ValueCountView` 借用描述与 `ValueAgg`，`MinMaxView` 与 `DistinctAggView`
  都是薄封装。
- 测试 `tests/distinct_agg_refresh.rs`：同源上 COUNT(DISTINCT) 与
  SUM(DISTINCT) 的更新/删除/组清空、窗口重放、rebuild，均与
  `count/sum(distinct ...)` 全量查询交叉验证。
- 未完成：多列 / 非 Int64 group-key 与 value（当前要求 Int64）、
  `SELECT DISTINCT` 多列投影、Window、SEMI/ANTI、join upsert。

**CDC change column 实施记录（已完成）**

- `IvmTableOptions::with_cdc_column` + `IvmTable.cdc_column`：创建表时写入
  `lakesoul_cdc_change_column` 属性并校验列存在；源表没有配置时回退到内部
  `rowKinds` 列（兼容既有约定）。
- 源侧删除过滤统一走 `change_column(source)` + `filter_deletes`：
  sum/count 的 append-only 与重建路径、upsert 的新值/旧值两侧、min/max 与
  distinct 的计数路径。
- 顺带修复两个 tombstone 正确性缺陷：
  1. `rebuild_sum_count` 对 keyed 源会把 MOR 存活的 `delete` 墓碑计入全量；
  2. upsert 旧值回收会把墓碑当存在行再回收一次（删除后重新插入同一 key 时
     多减一次）。
- 测试 `tests/cdc_column.rs`：自定义列名 `op` 的更新/删除、删除后 rebuild、
  删除后重新插入（两个回归点）、min/max 的 CDC 删除、属性持久化、
  未在 schema 中的 cdc 列报错。
- 未完成：`update_before`/`update_after` 语义目前依赖主键 MOR 合并折叠
  （append-only CDC 源不支持）；多列 key、Window、SEMI/ANTI、join upsert。

**Window 实施记录（已完成 v1：ROW_NUMBER）**

- 新增 `WindowView`（`ViewSpec::Window`、`WindowFunction::RowNumber`）：
  MV 表 `(partition keys..., source PK..., row_number, rowKinds, __ivm_epoch)`，
  PK = partition keys + source PK，bucket = partition keys（用到了 P0-3 的
  bucket 前缀）；`window_mv_schema(partition_keys, row_keys)`。
- 刷新（分区级重算）：delta → 受影响 partition 集合（delta 行的 partition +
  变更行在 MV 里所在旧 partition，覆盖分区迁移）→ 读源当前状态、DataFusion
  `row_number() over (partition by ... order by ..., <PK>)` 重算 → 对比 MV
  逐行 delete/insert；源中已消失的行删除对应 MV 行；行携带 epoch 保证重放幂等。
- `rebuild_window`：清空 MV，从源全量重排，发布 `rebuild:<generation>`。
- 校验：源必须有主键、partition/order 列必须存在、partition/PK 列必须 Int64；
  order 自动追加源主键保证并列时确定性。
- 测试 `tests/window_refresh.rs`：插入中间行、order 值更新、删除、跨分区迁移、
  窗口重放、rebuild，均与全量 `row_number()` 交叉验证；负例（无主键源、
  不存在的 order 列）。
- 未完成：仅 `ROW_NUMBER`（RANK/DENSE_RANK、聚合窗口函数未做）；分区重算是
  O(受影响分区 + 全量源读)，后续可按 partition 裁剪源读取。

**SEMI/ANTI 实施记录（已完成 v1）**

- 新增 `SemiAntiView`（`ViewSpec::SemiAnti`、`anti: bool`）：MV = 左表全列 +
  `rowKinds` + `__ivm_epoch`，PK = 左表主键；`semi_anti_mv_schema(left_schema)`。
- 刷新（受影响左行重算）：affected = ΔL 的左键 ∪（L_before ⋈ ΔR 的左键）；
  对 affected 键重写 MV：`delete(旧行) + insert(当前命中状态)`（SEMI 命中一
  行、ANTI 未命中一行）；L/R 的删除墓碑在匹配前用 `filter_deletes` 过滤，
  但 MV 旧行保留墓碑用于 delete；行携带 epoch 保证重放幂等。
- 两侧都用 DataFusion：LeftSemi/LeftAnti join 计算 matched/affected，`union`
  拼 inserts/deletes，全部集合运算在 DataFrame 层完成（输出列类型不受限）。
- `rebuild_semi_anti`：清空 MV，从两侧全量状态重算，发布 `rebuild:<generation>`；
  校验左表必须有主键、join key 两侧存在、两侧未分区。
- 测试 `tests/semi_anti_refresh.rs`：SEMI 的匹配出现/消失、payload 更新、
  左行删除、窗口重放、rebuild；ANTI 的匹配出现/消失，均与全量 EXISTS/NOT EXISTS
  交叉验证。
- 未完成：仅 inner equi join（多条件 join key 已支持，非等值条件未做）、
  输出固定为左表全列（投影下推未做）；join upsert（inclusion-exclusion +
  状态表）仍是后续。



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
