# LakeSoul IVM 基础能力补齐计划

> 状态：基础能力与 IVM 运行时均已合入 upstream/main；本文件保留实施记录并维护
> 进度总览与后续路线。设计细节见 `EPOCH.md` 与各节实施记录。

## 进度总览

### 已合入的 PR

| PR | 内容 |
|---|---|
| #888 | 元数据基础：as-of 读、version-based changelog、bucket 列 = PK 前缀、Rust commit OCC |
| #890 | IVM 运行时：`ivm` schema、SUM/COUNT（append-only）、epoch 幂等协议、rebuild、epoch 快照读取 |
| #896 | SUM/COUNT 支持主键（upsert）源：delta 最终版本 + as-of 旧值回收 |
| #903 | MIN/MAX：值计数状态表 |
| #905 | COUNT(DISTINCT)/SUM(DISTINCT)：复用值计数状态 |
| #908 | CDC change column 配置化 + tombstone 正确性修复 |
| #914 | ROW_NUMBER：受影响分区重算 |
| #917 | SEMI/ANTI：受影响左行重算 |

### 算子 × 源支持矩阵

| 算子 | 源要求 | 状态方式 |
|---|---|---|
| SUM / COUNT | append-only 或 keyed；多列、任意类型 key，SUM 需数值 | MV 即状态，delete+insert |
| MIN / MAX | 同上；value 任意可比较类型 | 值计数状态表 |
| COUNT / SUM(DISTINCT) | 同上；value 任意可比较/可哈希类型 | 值计数状态表 |
| ROW_NUMBER / RANK / DENSE_RANK / SUM / COUNT OVER | keyed（需主键）+ 未分区；partition 列任意可排序类型、可多列；ranking 需 order 列，聚合可整体（无 order）或按 SQL 默认 frame running | 分区级重算，刷新按受影响分区裁剪源读取 |
| INNER JOIN | 两侧 append-only 或两侧 keyed + 未分区；join key 可多列、任意相等比较类型，payload 任意 | append-only 源用 inclusion-exclusion；keyed 源输出按左右行身份键控，受影响 pair delete+insert |
| SEMI / ANTI | 左 keyed，右 append-only/keyed；等值 join key 可多列，另可加任意 `= <> < <= > >=` 左右列条件（含纯非等值）；输出左列可投影 | 受影响左行 delete+insert；源读取按需投影 |
| 投影/Filter（`RowView`） | 源 keyed 或 append-only；输出列可投影，过滤条件 `= <> < <= > >=`（含 NULL 判断） | keyed 源按受影响主键 delete+insert（通过过滤才插入）；append-only 只追加 |
| UNION ALL（`UnionAllView`） | 多源同 schema 且全 keyed 或全 append-only | keyed 输出按 `(__ivm_source, PK)` 键控、delete+insert；append-only 追加 |
| TOP-K（`TopKView`） | keyed（需主键）+ 未分区；group/order 列任意可排序类型；输出列可投影（须含 group 与主键） | 受影响 group 内 `row_number() <= k` 重算，按 `(group, PK)` delete+insert；并列用源主键确定 |
| join upsert、SELECT DISTINCT、NTILE/LAG/LEAD 等窗口算子 | — | **未支持** |

### 路线图

- **P1**：~~通用类型（多列、非 Int64）group key 与 value~~、~~JOIN 支持 keyed 源~~、
  ~~`ivm.states` 注册表~~、~~Window 扩展（RANK/DENSE_RANK/聚合窗口、源按分区裁剪）~~、
  ~~SEMI/ANTI 扩展（非等值、投影下推）~~、~~投影/Filter/Union ALL 视图~~、
  ~~TOP-K~~（P1 已完成）→ P2 见下
- **P2**：~~consumer 水位 GC（`ivm.consumers`）~~（已完成；cursor-aware retention 联动待做）→
  ~~JVM `list tables` 过滤 internal 表~~、~~epoch 发布 commit_id~~、
  ~~as-of 下沉 TableProvider~~、~~changelog 表级单扫描~~（已完成）→
  changelog 表级单扫描 → CDC `update_before`/`update_after` → 聚合状态按 key/桶裁剪、
  `pk_locator` 泛化 → `DataCommitInfo` 时间单位与 JNI DAO offset 小修
- **P3**：SQL 前端（SQL → 逻辑计划改写）与 tokio 调度器（interval/拓扑序、级联 MV、
  dirty 自动重建）→ PG `CREATE/REFRESH MATERIALIZED VIEW` 表面（postgres-lakesoul）
  （P2 已全部收尾）。SQL 增量入口的后续路线（WHERE、测试深度、算子扩展、框架扩展
  checklist）见 §10；M1（spec 驱动）/M2（形状分析）/M3（专用执行器）/M4（sqllogictest +
  差分 oracle）均已合并。

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

- ~~`pk_locator` 泛化（任意列/字符串/parquet/非唯一键）~~：已按 vortex 路线完成类型化
  泛化（见下方 Step 1 记录）；parquet 明确不在范围内（v1 join 用"桶裁剪 +
  row-group min/max + sort-merge"，不依赖点查）。
- ~~`DataCommitInfo.timestamp` 秒/毫秒不一致~~（已完成：hash sink 提交路径改为毫秒并加
  回归测试；`data_commit_info.timestamp` 索引未做——目前只有 JVM 列表的
  `order by timestamp` 使用，需要时再走迁移补）。
- ~~JNI DAO offset 错位~~（已完成：补齐 Rust `SelectOneDataCommitInfoByTableId`
  （query-one +13）并让 Java 枚举指向同一 code、`paramsNum=1`；顺带把
  `DAO_TYPE_*_OFFSET` 常量改为 final，消除"先引用枚举类型"时的循环 `<clinit>`；
  Java 侧加了 code/参数一致性测试）。
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



**通用类型 group key / value 实施记录（已完成）**

- 聚合视图（SUM/COUNT、MIN/MAX、COUNT/SUM(DISTINCT)）支持多列、任意类型的
  group key，value 类型也放开（SUM 需数值；MIN/MAX/DISTINCT 任意可比较/可哈希）。
- 刷新流程改为 SQL 管道：delta 聚合、as-of 旧值回收、与状态/MV 的全外连接与
  coalesce 全部下推到 DataFusion，避免了 Rust 侧的按类型累加与行组装；
  `(group, value)` 与 `group` 的 map 只保留在 SQL 中。
- 幂等：状态/MV 行仍带 `__ivm_epoch`，SQL 中按 epoch 过滤已应用的键；同一 key 的
  delete/insert 通过 `order by keys, rowKinds` 保证删除在前。
- 新增通用 schema 构造函数：`sum_count_mv_schema_for`、
  `value_count_state_schema_for`、`value_count_mv_schema_for`、
  `min_max_mv_schema_for`、`distinct_agg_mv_schema_for`；旧的 Int64 单列 schema
  保留为兼容包装。
- JOIN 同样改为 DataFrame/类型通用：join key 支持多列、任意相等比较类型，payload
  任意；输出列名为 join key 原名 + `left_value`/`right_value`，schema 由
  `join_view_schema_for` 推导（旧 `join_view_schema` 已移除）。
- WINDOW 去掉 Int64 限制：partition key 任意非空类型（可多列），order key 任意可排序
  类型；row_number 由 SQL 计算并 cast bigint，MV schema 由 `window_mv_schema_for`
  推导；delete/insert 用 `order by pks, rowKinds` 保证顺序。
- 测试：`tests/generic_keys.rs`（多列 Utf8 key SUM(Int32)/COUNT、字符串 MIN/
  COUNT(DISTINCT)）与 `tests/generic_join_window.rs`（两列 Utf8 join key + 字符串/
  Int32 payload、字符串 partition/order 的 ROW_NUMBER），均与 SQL 交叉验证；
  SEMI/ANTI 增加 delete-before-insert 排序。
- 未完成：非数值 SUM、非等值 join 条件。

**NULL 语义实施记录（已完成）**

- 探针 `tests/null_probe.rs` 验证内部表允许 NULL 合并键（写入、跨提交 upsert、
  同批 delete+insert、MOR 读回均正确），group/partition key 因此直接放开可空。
- NULL 作为合法分组值：聚合/window 的 delta↔状态、`already`/`affected`/`active`
  键匹配全部改用 `IS NOT DISTINCT FROM`（NULL 等于 NULL）；JOIN、SEMI/ANTI 保持
  `=`，NULL key 不匹配，与 SQL 一致。注意 DataFusion 中 `IS NOT DISTINCT FROM`
  的右操作数会吞掉后续 `AND`，每个等值需整体加括号。
- 值语义：`count(distinct value)` / `sum(distinct value)` 忽略 NULL；MIN/MAX
  忽略 NULL，全 NULL 组结果为 NULL；SUM 全 NULL 组结果为 NULL。
- SUM/COUNT MV 增加隐藏列 `__ivm_nonnull_count`（非 NULL 值计数），`sum_v` 仅在
  计数 > 0 时非空，从而区分“和为 0”与“全 NULL”；MV/状态 schema 的 sum/value 列
  改为可空，键列通过 `key_fields` 保留源列可空性（不再强制非空）。
- 行身份仍要求非空：window 的源主键、SEMI/ANTI 的左主键仍校验；group/partition
  key 不再校验。
- 测试 `tests/null_semantics.rs`：NULL 组的 SUM/COUNT（全 NULL 组、增删改、
  rebuild）、NULL 值的 MIN/COUNT(DISTINCT)/SUM(DISTINCT)、NULL join key 的
  JOIN/SEMI/ANTI、NULL 分区的 ROW_NUMBER，均与 SQL 交叉验证。

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

**Join keyed 源实施记录（已完成）**

- 两侧都带主键时 `JoinView` 走 keyed 路径：输出 schema 由
  `keyed_join_view_schema_for` 给出，除 join key 与 payload 外还含隐藏列
  `__left_pk_<pk>` / `__right_pk_<pk>`（输出主键，`keyed_join_output_primary_keys`
  生成）以及 `rowKinds`/`__ivm_epoch`。
- 刷新：受影响 pair = ΔL/ΔR 的主键集合，输出行按 `(左 PK, 右 PK)` 定位；
  `delete(old) + insert(current)` 只重写受影响 pair，其中 current 为
  `L_affected ⋈ R_now ∪ L_now ⋈ R_affected`（union distinct 去重，覆盖两侧
  同窗口变化的 pair）；写入按 pair + rowKinds 排序保证 delete 在前，同 epoch
  的 pair 跳过，replay 幂等。
- `rebuild_join` 对当前两侧状态做全量 inner join 重写输出（过滤 CDC tombstone）。
- 混用 keyed/append-only 源、可空行身份、输出 schema/PK 不匹配都会在
  `validate_join_view` 报错；join key 相等比较仍为 `=`，NULL 不匹配。
- 测试 `tests/join_keyed.rs`：双侧 upsert/delete、join key 在 NULL/非 NULL 间
  迁移、payload 更新、两侧同窗口对齐、多列 join key（Utf8 + Int64）、rebuild
  以及与 SQL inner join 的逐行对照；另有混合源/可空主键校验测试。

**`ivm.states` 注册表实施记录（已完成）**

- 新增 PG 表 `ivm.states(view_id, role, table_id, table_name, namespace,
  table_path, created_at)`，主键 `(view_id, role)`；角色 `StateRole::{Mv, State}`
  （mv = MV 输出兼状态，state = 值计数状态表）。
- `IvmMetadata::{register_state, get_state, list_states}`；`delete_view` 一并
  清理状态注册。
- 每个 `register_*_view`（SUM/COUNT、MIN/MAX、DISTINCT、WINDOW、SEMI/ANTI、
  JOIN）在 `upsert_view` 之前注册内部表，因此冲突报错不会改动 `ivm.views`
  里的 spec；重复注册同一张表幂等。
- 绑定保护：同一 `(view, role)` 首次注册后，再用不同 table_id 注册同一个
  view id 会报错，避免两个视图静默共用/切换状态表。
- `IvmRuntime::list_states(view_id)` 暴露注册表查询。
- 测试 `tests/states_registry.rs`：各类视图的角色与 table_id 注册、重复刷新与
  rebuild 幂等、冲突表被拒绝且原绑定保留、`delete_view` 清理注册。

**Window RANK/DENSE_RANK 实施记录（已完成）**

- `WindowFunction` 增加 `Rank`/`DenseRank`：`sql_name()` 生成 SQL 函数名，
  `column_name()` 决定 MV 列名（`row_number`/`rank`/`dense_rank`），后两者导出为
  `IVM_RANK_COLUMN`/`IVM_DENSE_RANK_COLUMN`。
- `window_ranking_mv_schema_for(source, partitions, row_keys, function)` 生成对应
  schema，`window_mv_schema_for` 委托为 `RowNumber` 兼容包装；
  `WindowView::new_with_function` 指定函数，`new` 保持 ROW_NUMBER。
- `window_ranking_cte` 按函数生成 `cast(fn() over (...) as bigint)`；仅
  ROW_NUMBER 继续把源主键追加到 ORDER BY 以保证确定性，RANK/DENSE_RANK 使用
  声明的排序（并列名次相同），与 SQL 语义一致。
- 刷新/重建沿用分区级 delete+insert 重算；测试 `tests/window_rank.rs` 覆盖并列
  名次（含只按主键无法区分并列的反例）、并列插入/排序更新/删除、双分区、
  rebuild、与 SQL `RANK()/DENSE_RANK()` 对照、spec 中函数持久化。

**Window 聚合函数与分区裁剪实施记录（已完成）**

- `WindowFunction` 增加 `Sum`/`Count`：SUM 需要 value 列且结果可空（`sum_v`），
  COUNT 支持 `count(1)`（`value_column = None`）或 `count(value)`（`count_v`，非空）。
  `WindowFunction` 增加 `is_aggregate()`、`Default = RowNumber`；`ViewSpec::Window`
  的 `function`/`value_column` 带 `serde(default)`，旧 view spec 反序列化仍为
  ROW_NUMBER。
- `WindowView::new_aggregate` + `window_aggregate_mv_schema_for`：没有 order 列时
  计算整分区聚合，有 order 列时用 SQL 默认 frame（RANGE UNBOUNDED PRECEDING..
  CURRENT ROW，含并列）；`window_function_cte` 按函数生成窗口表达式，ranking
  函数仍是 `cast(fn() over (...) as bigint)`。
- 源按分区裁剪：刷新改为两阶段。阶段一仅用 delta + MV 计算受影响分区
  （`window_affected_cte`/`window_affected_sql`，复用原 CTE 前缀），
  `partition_filters` 把分区值（含 NULL）转成 DataFusion `Expr`（IN/IS NULL），
  `IvmTable::read_current_filtered` 将过滤下推到 LakeSoul reader；阶段二只对
  过滤后的源行做窗口重算。rebuild 仍读全量。
- 测试 `tests/window_aggregate.rs`：SUM/COUNT 的 running（含并列）与整分区、
  NULL 值/全 NULL 分区、更新/跨分区迁移/删除、rebuild、与 SQL 对照；以及
  `read_current_filtered` 的等值/IN/false 过滤读数验证。

**SEMI/ANTI 扩展实施记录（已完成）**

- 新增 `CompareOp`/`SemiAntiCondition`（`left_column op right_column`，`= <> < <= > >=`）
  与 `SemiAntiView::new_with_conditions`；`join_keys` 仍是等值键，条件里 `=` 合并
  进 join key（可哈希），其余作为 join filter。纯非等值（无 join key）也可用。
- 匹配由 `semi_anti_join` 统一：右表投影为 `__ivm_right_*` 别名，避免两侧同名列
  歧义；`= ` 条件进 `on`，其他进 filter。
- 受影响集合修正：右表变化时，除 delta 自身外，还按窗口起始 as-of 读出变化主键的
  **旧版本**，与 delta 版本合并后再 semi-match 左旧行。这样右表 join key 原地更新
  （旧键匹配消失）与比较条件翻转（如 `v < w` 中 w 变小）都能正确回收。
- 投影下推：`SemiAntiView::output_columns`（默认全部左列，必须含左主键）+
  `semi_anti_mv_schema_for`；刷新时左/右两侧都读成所需列（reader 级投影，
  `IvmTable::{read_current_projected, read_files_projected, read_as_of_projected}`），
  右侧只读 join key/条件列/主键/CDC 列，左侧只读输出列 + 条件列 + 主键 + CDC。
- 测试 `tests/semi_anti_ext.rs`：非等值（含条件翻转、右表等值键更新、删除）、纯
  非等值 ANTI、投影输出与 `read_current_projected`、校验负例；均与 SQL
  `EXISTS/NOT EXISTS` 对照并覆盖 rebuild。

**投影/Filter/Union ALL 实施记录（已完成）**

- 新增 `RowView`（单源投影 + 过滤）与 `UnionAllView`（多源 UNION ALL）、
  `LiteralValue`/`FilterCondition`（列与字面量比较，NULL 用 `=`/`<>` 判断）、
  `row_mv_schema_for`、`union_all_mv_schema_for` 与 `IVM_SOURCE_COLUMN`。
- `RowView`：keyed 源按受影响主键 delete+insert（只有通过过滤的行才插入，旧行总
  先收回），append-only 源只追加通过过滤的 delta；两种源都支持 rebuild。
- `UnionAllView`：要求所有源 schema 一致且同为 keyed 或同为 append-only；keyed
  输出按 `(__ivm_source, 主键)` 键控，逐源 delete+insert，epoch 幂等；append-only
  输出直接追加。`__ivm_source` 记录源序号。
- `ivm.states` 注册表覆盖两种新视图（mv 角色）；`ViewSpec` 新增 `Row`/`UnionAll`
  并带 serde 默认；`ViewSpec` 去掉了 `Eq`（新增的浮点字面量不满足）。
- 测试 `tests/row_union_views.rs`：投影+过滤（更新双向穿越过滤、删除、插入、
  rebuild、SQL 对照）、append-only 源的字符串/NULL 过滤、keyed union-all 的
  更新/删除/重建、append-only union-all、以及校验负例。

**TOP-K 实施记录（已完成）**

- 新增 `TopKView`（group_keys/order_keys/limit/output_columns）与
  `top_k_mv_schema_for`；输出 schema = 投影列 + rowKinds + epoch，MV 主键为源主键。
- 语义：每 group 取 `row_number() over (partition by group order by order_keys,
  源主键) <= limit`，并列由主键确定性打散；输出列必须包含 group 与源主键。
- 刷新沿用 window 的受影响分区思路：affected = delta 的 group ∪ 变化主键在 MV 中
  的旧 group；受影响 group 内重算并 delete+insert，epoch 幂等；rebuild 全量重算。
- 测试 `tests/top_k.rs`：新行挤入/挤出、排序更新导致掉出、删除后递补、并列按主键
  打散、投影列（去掉 payload）、跨 group 迁移、rebuild、与 SQL `row_number()` 对照、
  校验负例（limit<=0、缺 group/order、非 keyed 源、输出缺列）。

**Consumer 水位 GC 实施记录（已完成）**

- 新增 PG 表 `ivm.consumers(view_id, consumer_id, last_epoch, updated_at)` 与
  `Consumer` 类型；`IvmMetadata::{upsert_consumer, list_consumers, delete_consumer,
  consumer_watermark}`，`IvmRuntime` 提供同名包装；`delete_view` 一并清理。
- `gc_epochs(view_id, grace)` 删除
  `status='committed' and epoch < min(last_epoch) - grace` 的 epoch 行；无消费者时
  返回 0（保留全部），pending 行永不删除。
- 测试 `tests/consumers_gc.rs`：水位的注册/前移/删除、GC 只删水位以下的 committed
  行、GC 后新窗口仍能刷新且 MV 与 SQL 一致、被 pin 的 epoch 保留、pending 行不被
  删除、grace 延迟删除、无消费者不删、`delete_view` 清理消费者。
- 说明：本地全并发测试偶发 PG SERIALIZABLE（40001）冲突，属既有元数据提交重试
  问题；`--test-threads<=2` 稳定，CI 低并发同样适用。

**并发冲突重试实施记录（已完成）**

- 背景：docker-compose 测试环境把 PostgreSQL 设为 `serializable`
  （`default_transaction_isolation=serializable`），并发 refresh/commit 之间会产生
  SQLSTATE 40001（serialization failure）与 40P01（deadlock），此前默认高并发下
  每轮全量测试有 2–3 个偶发失败。
- `IvmMetadata`：新增 `execute_rw`/`query_opt_rw`/`batch_execute_rw`，对
  40001/40P01 做最多 10 次指数退避 + 抖动重试；所有 IVM 元数据写入
  （views/cursors/epochs/states/consumers 与 DDL）都改走这些入口。
- `lakesoul-metadata::commit_data`：OCC 重试循环把 40001/40P01 也视为可重试
  （退避后进入下一轮），而不仅是 `inserted != expected`；`get_table_domain`
  空结果从 panic 改为 `NotFound` 错误。
- 测试 `tests/concurrency.rs`：8 路并发 refresh 各自与 SQL 结果一致；全量套件在
  默认高并发下 legacy 连跑 2 次、V2 跑 1 次均 25 个二进制全绿（修复前默认并发
  每轮有 2–3 个偶发失败）。

**Epoch 发布 commit_id 实施记录（已完成）**

- `ivm.epochs` 增加 `commit_ids jsonb not null default '[]'`（建表 + `alter table ...
  add column if not exists` 升级），`EpochRecord.commit_ids` 暴露。
- `lakesoul-metadata::commit_data_files{,_with_commit_op}` 返回本次提交产生的
  commit id 列表（每个 partition 一个，标准 UUID 字符串）；`IvmTable::append_batch`
  返回该列表。
- 各 refresh/rebuild 窗口内收集 MV 输出表的 commit id 并随 `mark_epoch_committed`
  写入；`begin_window` 的 pending 恢复沿用记录里的 ids；pending epoch 初始为空。
- 测试 `tests/epoch_commit_ids.rs`：每个 epoch 的 ids 非空且为 UUID、跨 epoch 不复用、
  rebuild（新 generation）同样发布、pending 初始为空。
- 说明：消费者可用 `commit_ids` + `mv_versions` 双重定位快照；按 commit id 读取的
  下沉 API（`view_state_at_commit`）留待后续。

**JVM `list tables` 过滤 internal 表实施记录（已完成）**

- 所有列表路径不再返回内部 IVM 表（`table_info.properties` 含
  `lakesoul.ivm.internal`）：
  - Rust JNI DAO（`rust/lakesoul-metadata/src/lib.rs`）：`ListAllTablePath`、
    `ListAllPathTablePathByNamespace`、`ListTableNameByNamespace`、
    `ListTableNamesByDomain` 加 `not exists (select 1 from table_info ... properties::text
    like '%lakesoul.ivm.internal%')`；
  - Java JDBC（`TablePathIdDao.listAllPath`/`listAllPathByNamespace`、
    `TableNameIdDao.listAllNameByNamespace`/`listAllNamesByDomain`）同样加
    `not exists` 过滤。
- 不新增 DAO 类型/偏移（Java `CodedDaoType` 不变）；内部表仍可被逐表查询、
  compaction 等按 id 访问，只是不再出现在列表里。
- 测试：Rust `rust/lakesoul-metadata/tests/list_tables_filter.rs`（覆盖四个 JNI
  查询）与 Java `ListTablesFilterTest`（JDBC 四个列表方法）；`lakesoul-common`
  模块测试与 `lakesoul-metadata` 串行测试全绿。

**as-of 下沉 TableProvider 实施记录（已完成）**

- 新增 `IvmTableProvider`（DataFusion `TableProvider`）与
  `IvmReadMode::{Current, AsOf(ms), AtVersions(versions)}`；
  `IvmRuntime::{table_provider, table_provider_as_of, table_provider_at_versions,
  table_provider_at_epoch}` 提供便捷构造（共享连接池）。
- `scan` 把查询投影下推到 `IvmTable::{read_current_projected, read_as_of_projected,
  read_at_versions_projected}`（自动补 merge key 与 CDC 列），读回后过滤 CDC
  tombstone 并按请求列投影，返回 `MemorySourceConfig` 计划；过滤条件交给 DataFusion
  在上层执行（默认不支持 pushdown）。
- `LakeSoulReader` 非 `Sync`，读取 future 不是 `Send`；`scan` 在 blocking 池中用独立
  current-thread runtime 执行读取，避免非 Send 类型跨线程（reader 变 Send/Sync 后可
  内联）。
- `IvmTable` 增加 `read_at_versions_projected`；`MetaDataClient` 增加 `Clone`
  （内部 Arc 池共享）。
- 测试 `tests/table_provider.rs`：当前态/指定 epoch/as-of 与 `view_state_at_epoch`
  对照、投影与过滤、CDC tombstone 隐藏；全套 27 个二进制 legacy/V2 全绿。
- 未完成：changelog 表级单扫描（`collect_source_window` 仍按分区增量读取）。

**changelog 表级单扫描实施记录（已完成）**

- metadata 侧新增两个表级查询（DAO 编码取列表区间内的保留值，不占用 JNI
  已用偏移）：`ListPartitionVersionsByTableIdAndMinVersion` 一次取回所有变化分区
  （含各自 cursor 所在版本）的全部 `partition_info` 行；
  `ListDataCommitInfoByTableIdAndCommitIds` 一次取回所有引用的 data commit。
- `MetaDataClient::get_table_changelog(table_id, from_versions)` 在内存中按分区
  切分版本行、定位 baseline、处理 update/delete/compaction 语义、收集 commit id，
  再按请求顺序分组计算 `active_added_files`。
- `collect_source_window` 改用它：每条源窗口由 O(分区) 次元数据往返降为固定 2 次
  （分区版本 + commit 文件）；`requires_rebuild`/`partition_deleted`/identity/cursor
  推进等语义保持不变。
- 测试 `rust/lakesoul-metadata/tests/table_changelog.rs` 与逐分区 API 对照：
  多分区全量、按 cursor 增量、删除分区、update 触发 rebuild；IVM 全量 27 个二进制
  legacy/V2 全绿。

**CDC `update_before`/`update_after` 实施记录（已完成）**

- 语义：`insert`/`update_after` 是生效版本，`delete`/`update_before` 是撤回；
  `update_before` 撤回旧版本，直到同一变更的 `update_after` 生效。
- keyed 源：`source_delete_filter`（SQL）与 `filter_deletes`（DataFrame）同时排除
  `delete` 与 `update_before`。MOR 折叠同一提交的 before+after 对；单独的
  `update_before` 通过 as-of 旧值回收把行收回，`update_after` 到达后再插入新版本。
- append-only CDC 源（无合并键、不折叠）：SUM/COUNT 的 delta/重建与值计数状态改为
  **带符号聚合**（`source_retract_condition` + `signed_delta_exprs`）：
  `insert`/`update_after` 计入 +value/+1，`delete`/`update_before` 计入
  -value/-1；重建时净计数 ≤ 0 的组不写入，`sum_v` 在无非空值时保持 NULL。
- window/top-k 等按状态计算同样使用不含 `update_before` 的 live 过滤。
- 测试 `tests/cdc_update_markers.rs`：append-only 的 update 对（含同窗口
  delete+update 与带符号 SQL 对照）、MIN 值状态、rebuild；keyed 的 MOR 折叠、
  单独 `update_before` 撤回、`update_after` 恢复、rebuild。
- 范围说明：无主键的 row/union 视图仍无法按行撤回（无行身份），保持文档现状。

**聚合状态按 key/桶裁剪实施记录（已完成）**

- 刷新窗口先算出候选受影响分组（`affected_groups_sql`）：delta 的分组 ∪ keyed 源中
  变更主键的旧分组（旧值按 live 过滤）。
- 据此对读取做 key 过滤（`key_filters`：`IN` + `IS NULL`，NULL 分组也可用）：
  - SUM/COUNT：`old` 按 delta 主键过滤（新增 `IvmTable::read_as_of_filtered`）、
    MV 按候选分组过滤（`read_current_filtered`）；
  - MIN/MAX/DISTINCT：`old` 同样按主键过滤，值状态表与 MV 都按候选分组过滤
    （写 delta 用第一次裁剪读，`state_now`/MV 用同一 filters 重读）。
- 过滤下推到 LakeSoul reader：行级过滤 + 桶裁剪（状态表 bucket=group 前缀、单桶列
  命中时生效）。`partition_filters` 泛化为 `key_filters`，窗口分区裁剪沿用。
- 测试 `tests/state_pruning.rs`：60 个分组、更新/删除少量分组后 SUM 与 MIN 的
  全部分组仍与 SQL 一致、rebuild 正常；全量 29 个二进制 legacy/V2 全绿。

**Vortex 内部表支持实施记录（pk_locator Step 0，已完成）**

- `IvmTableOptions::with_file_format(PhysicalFormat)`（默认 parquet）+ `IvmTable.file_format`；
  建表属性 `file_format` 同步写入，`append_batch` 与 `read_files_with_options` 使用表格式
  （`truncate`/rebuild 等元数据路径与格式无关），`lakesoul_ivm` 重导出 `PhysicalFormat`。
- 测试 `tests/vortex_tables.rs`：parquet 源 + vortex MV/状态表（SUM/COUNT、MIN/MAX、
  ROW_NUMBER），覆盖插入/upsert/删除（MOR）、`read_current_filtered` 的 group 过滤
  （含 `(g,value)` 状态表的键前缀过滤）、rebuild，并校验写侧文件扩展名为 `.vortex`。
- 结论：vortex 内部表在 IVM 全链路可用，是后续 pk_locator 泛化（类型/复合键/前缀）
  的前提；当前 locator 仍只支持单列 Int32/64 且过滤需精确命中 PK 列，点取收益待
  Step 1 泛化后生效。

**pk_locator 泛化实施记录（Step 1，已完成）**

- 键表示与提取：`KeyValue`（Bool / Int / UInt / Float（规范化 bits） / Utf8 / Binary /
  Decimal128；Date32/64、Time32/64、Timestamp、Duration 归一为 Int）+ `KeyConstraint`
  （`extract_key_constraints`）：按主键前缀逐列提取有限集（`=`、`IN`、同列 `OR`；同列
  合取取交集，类型族不匹配或矛盾则忽略/回退），取最长的"每列都有限"的前缀并做笛卡尔
  积（上限 `MAX_PK_CANDIDATES`）。**只有主键前缀可下推**：payload 谓词不能下推，否则
  MOR 可能丢键的最新版本；候选集始终是完整谓词的超集，扫描之上的 FilterExec 仍生效。
- 每文件索引：`KeyIndex` = `(复合键, row)` 排序数组，二分区间支持全键与任意前缀查找，
  非唯一键返回全部行；`build_cached_file` 用 vortex `select` 只投影键列、`execute_arrow`
  转 Arrow 后按列类型编码（NULL 键不建索引），列实际类型与表 schema 不符时整体报错回退。
  缓存键 = 文件位置 + 前缀列集合；索引字节数按内联条目 + 字符串/二进制堆负载估算。
- 读取侧：`session.rs` 与 `lakesoul-datafusion` 的 `table_provider.rs` 改为
  `extract_key_constraints` + `try_build_key_inputs`；单列整型保留 min/max 统计裁剪；
  VortexSession 惰性构造（parquet 路径零开销）。IVM 过滤已全部经 `with_filters` 下传，
  `(g,value)` 状态表前缀、SUM MV 全键、window 分区前缀等直接受益。
- 测试：
  - `rust/lakesoul-io/src/pk_locator.rs` 单测：字符串键、类型不匹配回落、复合键全键/前缀、
    无前缀/矛盾合取回落、复合候选上限、索引全键/前缀/重复键/乱序输入（17 个）。
  - `rust/lakesoul-datafusion/src/tests/pk_locator_tests.rs` e2e：新增字符串主键与复合主键
    前缀用例（点取、IN、缺失键、残留谓词、MOR 更新合并）；`LAKESOUL_PK_PROFILE=1` 输出
    证实索引点取与缓存命中（如复合前缀 `g='g1'`：6 行来自 5 个文件，二次查询缓存命中）。
  - 回归：IVM 全量 legacy + V2（`--test-threads=2 --skip concurrent_refreshes_converge`）
    全绿；`concurrent_refreshes_converge` 本身在基线上也偶发 PG 40001
    （8 路 serializable 写、5 次重试耗尽），与本次改动无因果。

**pk_locator mmap/磁盘缓存实施记录（Phase A，已完成）**

- 索引格式：`MmapIndex`（本地文件只读 `mmap`）——header（magic/版本/行数/blob 长度/键布局指纹）
  + arrow-Row 编码的键字节 blob + `u32` offsets。条目位置即行号（写侧保证主键有序，构建时复检），
  前缀查找返回连续区间，非唯一键天然覆盖；键编码与写侧排序一致（`RowConverter` + 默认
  `SortOptions`），复合键是各列编码的拼接，故"列前缀 = 字节前缀"。
- 容量共享：不新增开关/预算。索引作为"整文件条目"存入进程唯一的 object store `DiskCache`
  （`crate::cache::get_lakesoul_cache()`），与页缓存共用 moka 实例、目录
  （`LAKESOUL_CACHE_PATH`，默认 `lakesoul_cache_dir`）与容量（`LAKESOUL_CACHE_SIZE`），
  统一 LRU 淘汰，由 `LAKESOUL_CACHE` 开关控制。`DiskCache` 新增
  `get_file_entry`/`insert_file_entry`/`temporary_path` 等整文件 API；淘汰 unlink 不影响
  已映射页（open fd/inode），启动清目录只导致重建。原 `LAKESOUL_PK_CACHE_BYTES` 与堆内索引
  缓存已移除。
- 读取路径：`get_or_load_file` 先查磁盘条目（命中则校验并 mmap），未命中才扫描 vortex 键列、
  用 `RowConverter` 编码并流式写临时文件后插入；`try_build_key_inputs` 用同一 converter 一次性
  编码候选再二分。vortex 文件句柄仍用小型堆缓存（1024 项）摊销 footer 读取。
- 阈值与回退：`MIN_INDEX_ROWS=4096`（低于 writer 常见的 ~8k 行/文件）。决策覆盖整次查询：
  所有候选文件都小于阈值才回退全扫，混合的小 upsert 文件不再拖累大文件的点取。单条索引上限为
  共享容量的 1/8；键序不符/类型不符/超限/损坏文件进入负缓存并回退扫描。
- 收益（spike benchmark，见下）：每 100 万行不可回收 anon 内存 96–119 MiB → 0.1–4 MiB，
  索引主体转为可回收的 page cache（13–23 B/行）；点查不降反升（Int64 2.2×、复合 3.6×，
  前缀 1.9×，构建也略快）。
- 测试：`pk_locator.rs` 单测（前缀/重复键、乱序拒绝、损坏 header、候选编码）；`disk_cache.rs`
  单测（整文件条目、命名空间、共享容量淘汰、`invalidate` 连带清理）；datafusion e2e 三个用例
  统一打开 `LAKESOUL_CACHE`，首个数据文件 6 万行（writer 切成 ~8.5k 行/文件，验证混合文件与
  缓存命中），`LAKESOUL_PK_PROFILE` 显示首次构建 ~10ms、随后 ~100µs；IVM legacy/V2 全量 +
  `vortex_tables` 开缓存全绿。

**pk_locator Step 2：IVM 刷新端到端量化与阈值修正（已完成）**

- 场景：`tests/pk_locator_refresh.rs`（新增，ignored，大小/模式由 env 控制）——全 vortex
  源/MV/值状态表，32 次 append × 8192 行 = 26.2 万行、128 个源文件，10 轮"改 8 个 key"的
  增量刷新；对比开/关 locator（`LAKESOUL_CACHE`）。
- 稳态增量刷新耗时（中位数）：SUM **110ms vs 179ms（1.6×）**，MIN **162ms vs 237ms（1.5×）**；
  读量（`LAKESOUL_PK_PROFILE`）：PK 点取 `rows=8`（只取受影响行），值状态前缀
  `rows≈1024`（每桶一行），其余候选文件被统计裁剪；索引缓存 458 hits / 42 builds。
- 首窗全量刷新两模式相当（85–92s，固定开销主导；该窗口候选 key 数超过
  `MAX_PK_CANDIDATES`，源 `old` 读本就回退全扫）。冷全量不是 locator 的目标场景；该固定
  开销（大窗口下刷新本身的耗时）另列为观察项。
- **阈值修正**：IVM writer 每次 append 按写线程分区写出多个文件（实测 8192 行 → 约 5 个
  ~1.6k 行文件）。原"单文件行数 ≥ `MIN_INDEX_ROWS`"的判据会把所有 IVM 文件排除（实测
  locator 完全未启用、hits=misses=0）。已改为"候选文件**总行数** ≥ 4096 才启用，否则全扫"，
  达标后小 upsert 文件也一并索引；提前退出使大表判据开销为 O(1) 次 footer 读。
- 结论：收益集中在稳态增量刷新；`MIN_INDEX_ROWS`/`MAX_INDEX_CACHE_RATIO` 暂无需再调，
  写时预计算（Phase B）仍无必要。

**SQL 增量执行入口 M1：按 spec 驱动刷新与注册表扩展（已完成）**

- 背景：SQL 层不做 `CREATE/REFRESH MATERIALIZED VIEW` DDL，改为一个专用执行入口把
  `INSERT INTO` 解释为增量维护（首跑全量、其后按源 changelog 增量），`INSERT OVERWRITE`
  保持全量覆盖语义。M1 先补"spec 即状态"的基础设施。
- `IvmTable::from_table_info` + `IvmRuntime::open_table/open_table_by_id`：按名称/id 从元数据
  重建内部或用户表（schema 优先 Arrow IPC、回退 JSON；解析 PK、桶列、CDC 列、物理格式）。
- `IvmRuntime::refresh_spec(&ViewSpec)`：从规范 JSON 打开全部相关表并调用对应的 `refresh_*`，
  并保留 `ivm.views.refresh_interval_ms`（否则 `register_*` 会用 0 覆盖调度区间）；
  新增 `ViewSpec::view_id()`。
- `ivm.views` 增列 `definition_hash` / `source_sql`（幂等 alter），新增
  `view_refresh_interval_ms` / `set_view_definition` / `view_definition`；
  `ivm.states` 新增 `unregister_state` —— 注册表对"同一 role 换表"保持**报错拒绝**，
  重建视图必须先注销再注册（正是定义变化清理旧 state 表所需的语义）。
- 测试 `tests/spec_dispatch.rs`：open 往返（name/id、各属性）、spec 驱动首跑+增量刷新、
  注册表替换语义、定义 hash/SQL 往返；IVM 全量（`--test-threads=1`）32 个测试二进制全绿。

**SQL 增量执行入口 M2a：聚合类形状分析（已完成）**

- 新增 `rust/lakesoul-ivm/src/sql.rs`：`analyze_select(plan, tables, request) -> AnalyzedView
  { spec, definition_hash }`，从 SELECT 的逻辑计划推导 `ViewSpec`（`tables` 提供源表的
  name→IvmTable 解析，支持裸名/`schema.table`）。
- 覆盖形状：投影/过滤（`Row`，支持 `=`/`<>`/`<`/`<=`/`>`/`>=`、`IS [NOT] NULL`、
  字面量在左右两侧）、SUM/COUNT（`SUM(v)`、`SUM(v)+COUNT(*)`、纯 `COUNT(*)`、
  无 GROUP BY 的全局聚合）、MIN/MAX（需 value-count state 表，由执行器创建并传入）、
  `COUNT(DISTINCT)`/`SUM(DISTINCT)`。
- 明确报错：AVG、SELECT DISTINCT、聚合上的 WHERE/HAVING、窗口、join、计算列等
  （M2b/M2c 切片）；不支持形状一律报错，绝不回退成普通 append。
- `definition_hash`：规范 spec JSON 的 FNV-1a，形状变化触发重建。
- 单测（不依赖 PG，直接构造逻辑计划）8 例：各聚合形状、过滤/空值/反向比较、
  不支持形状、hash 稳定性与形状敏感性。

**SQL 增量执行入口 M2b：窗口与 TOP-K 形状分析（已完成）**

- 窗口视图：`Projection -> Window` 单窗口函数（ROW_NUMBER/RANK/DENSE_RANK、
  `SUM/COUNT OVER`），提取 PARTITION BY / ORDER BY/聚合列；聚合窗口只接受运行时维护的两种
  frame（无 ORDER BY 的整分区、有 ORDER BY 的 SQL 默认 running frame），其他 frame 明确报错；
  排序窗口必须有 ORDER BY；窗口之上只允许直接选列或选窗口表达式本身。
- TOP-K：识别 `WHERE rn <= k`（子查询里 `row_number() OVER (PARTITION BY .. ORDER BY ..)`）
  形状，rank 列 = filter 输入作用域中源表没有的列；输出列从上层投影提取（不得包含 rank 列）；
  全局 `ORDER BY ... LIMIT` 暂不支持（运行时要求 group/order 键）。
- 单测新增 4 例（三种排序窗口、聚合窗口与 frame 校验、TOP-K、窗口不支持形状）；
  合计 12 例。

**SQL 增量执行入口 M2c：Join / SEMI-ANTI / UNION ALL 形状分析（已完成）**

- 内连接：仅 INNER；等值键从 `Join.on`（优化后）与 `Join.filter` 的等值合取中提取，
  要求两侧同名列；从选择列表按"左/右各一个 payload 列"提取 `left_value`/`right_value`
  （列归属优先看 relation 别名，其次看 schema 唯一性）；非等值条件一律报错。
- SEMI/ANTI：要求传入**优化后**的计划（DataFusion 会把 `EXISTS`/`NOT EXISTS`/`IN`
  decorrelate 成 `LeftSemi`/`LeftAnti`）；等值键入 `join_keys`，额外列比较条件归一化为
  `left.col op right.col`（按 relation/schema 判定左右并翻转操作符）；输出列取 join 左侧
  schema（投影下推进 TableScan 时也不会丢列）。
- UNION ALL：所有分支必须选择源的**全部列且顺序一致**（投影可能被下推进 scan，按
  `projected_schema` 校验），所有源 schema 必须一致；输出投影不得改名/换序。
- 明确报错：LEFT/RIGHT/FULL join、异名 join 键、内连接带非等值条件、UNION 分支投影裁剪。
- 单测新增 5 例（内连接、semi/anti、带额外条件的 semi、union all、不支持形状），
  合计 17 例；模块文档注明"计划需先经优化器"。

**SQL 增量执行入口 M3：专用执行器（已完成）**

- 新增 `rust/lakesoul-ivm/src/executor.rs`：`IvmSqlExecutor`（可选 `with_session`）只接受单条
  `INSERT INTO`/`INSERT OVERWRITE ... SELECT`；流程 = sqlparser 解析 → 收集并打开源表
  （无 session 时自建 SessionContext 并用 `IvmTableProvider` 注册）→ 逻辑计划 + 优化器 →
  形状分析 → 目标表 schema/键校验。
- 视图身份 = 目标表 `table_id`；定义哈希（FNV-1a，已归一化生成的 state 表 id）写入
  `ivm.views.definition_hash/source_sql`（存原始语句）。
- 行为：无定义 → `rebuild_spec` 全量建立并记录定义（Bootstrap）；哈希相同 → `refresh_spec`
  增量（Incremental）；哈希变化 → 先注销并 drop 旧的非 Mv state 表，再 `rebuild_spec`
  （generation+1、cursor 重置、清空 MV 均由 rebuild 内部完成）并更新定义（Rebuild）；
  `INSERT OVERWRITE` 同定义 → `rebuild_spec`（Overwrite），定义变化时同 Rebuild。
- value-count state 表按需创建（schema 匹配则复用、失配则重建），命名/路径由 MV 派生；
  运行时新增 `rebuild_spec`（与 `refresh_spec` 共用抽出的 `spec_view`）。
- 测试 `tests/sql_executor.rs` 4 例：bootstrap+增量、MIN→MAX 定义变化（state 表被替换且仅一份）、
  OVERWRITE 重算并恢复增量（同 key 变更按 upsert 语义验证）、非法语句/列清单/形状/schema
  不匹配报错；IVM 全量 33 个测试二进制（`--test-threads=1`）全绿。

**SQL 增量执行入口 M4：sqllogictest 与差分 oracle（已完成）**

- 引入 `sqllogictest`（dev-dep）与 harness `tests/sqllogic.rs`：INSERT 走专用执行器，其余 SQL
  走注册了源/MV provider 的 SessionContext；源变更用
  `/*ivm-append <table>: k=..,g=..,v=..,op=..; ...*/` 指令（provider 只读）。harness 用私有
  current-thread runtime + `block_on`（`AsyncDB` 要求 Send，而 LakeSoul reader 仅 Send 不 Sync）。
- `.slt` 套件：`sum_count.slt`（bootstrap/增量/删除/幂等重跑）、`min_max.slt`（MIN→MAX 定义
  变化重建后继续增量）、`errors.slt`（VALUES/AVG/聚合 WHERE/多语句/schema 不匹配报错且不影响
  已注册定义）。
- 差分 oracle `tests/sql_oracle.rs`：SUM/COUNT 与 MIN 各 10 轮随机插入/更新/删除，每轮
  `INSERT INTO` 后与"源上全量定义查询"逐行比对，且每题连续执行两次验证幂等。
- 已知限制（oracle 暴露）：聚合视图的 INSERT 定义里暂不支持 WHERE（M2 起明确报错），过滤需用
  `delete` 撤回语义表达；列入后续。
- 全量 35 个测试二进制（`--test-threads=1`）全绿。

## 9. 风险与开放问题

1. bucket 前缀属性为"IVM 内部表"专用，JVM 引擎误读会得到错误结果 → 需要
   `internal` 标记 + JVM `list tables` 过滤（后续）。
2. changelog API 与 JVM 语义有意分歧（version 消费、rebuild 信号），需在文档里
   写清楚，避免两套实现漂移。
3. `partition_info.timestamp` 是 DB 时钟，多实例时钟一致性影响水位 W；版本消费
   可消除正确性依赖，但 W 仍用于调度。
4. ~~OCC 重试与 PG 事务隔离需并发测试覆盖~~（已完成：40001/40P01 退避重试 +
   `tests/concurrency.rs`；serializable 测试环境稳定）。

## 10. SQL 增量入口后续路线（WHERE、测试深度、算子扩展）

> 决策（已确认）：① 过滤谓词直接来自 SQL 解析，表示形式（DataFusion `Expr` 或逻辑计划）
> 均可，**不为旧格式做兼容**；② HAVING 下一步单独做；③ 本轮只做 W1（聚合 WHERE）与
> W2（窗口/TOP-K/UNION 分支 WHERE）；④ AVG 等算子放后续批次；⑤ slt 的 action 断言用
> `/*ivm-expect-action ...*/` 指令；⑥ 核心诉求是**框架能否承接未来算子**，见 §10.1。

### 10.1 框架可扩展性评估（本轮重点）

**现有扩展点（新增一个算子/形状的清单）**

1. runtime：typed view struct + `to_spec()` + `refresh_*`/`rebuild_*`（SQL 构造与状态维护）+
   `*_mv_schema_for` + `validate_*`；
2. `ViewSpec` 新 variant（serde）+ `view_id()`；
3. runtime `spec_view()` 与 `refresh_spec`/`rebuild_spec` 分发；
4. analyzer（`sql.rs`）：形状匹配 + 明确错误；
5. executor（`executor.rs`）：`expected_mv_schema` 增加一支；
6. 测试：单测 + `*.slt` + 差分 oracle 场景。

**结论：分层方向正确，扩展成本可控**

- ✅ `ViewSpec` 即状态：跨进程/入口可复现，executor 与 runtime 解耦，新增算子只改上面的
  1–6 点中的指定位置。
- ✅ 状态注册表（`ivm.states`）+ generation/cursor/epoch 统一承载重建与定义变化；
  `spec_view()` 让"只用 spec 驱动"成为默认路径。
- ✅ analyzer 与 executor 的形状覆盖是"白名单式"的：不支持就明确报错，绝不静默退化。
- ⚠️ 需要先补齐的共性能力（按本计划顺序）：
  a. **过滤/谓词表示与注入点**（W0/W1/W2）：目前 `FilterCondition` 只支持单列-字面量比较，
     且只有 `Row` 有过滤；改为通用 SQL 谓词并统一注入 delta/old/rebuild/state/affected。
  b. **表达式/投影**：计算列、`CAST`、函数、`CASE` 目前一律拒绝；未来需要"表达式白名单 +
     可序列化表示"（与 a 相同的基础设施）。
  c. **窗口函数注册表**：`WindowFunction` 是封闭枚举，LAG/LEAD/NTILE 等逐个需要 runtime
     实现（受影响分区重算语义）。
  d. **连接模型**：目前仅单层 join、同名列、每侧一个 payload；多表/外连接需要新的 spec
     形态（join 树/条件列表）。
  e. **视图级联**：MV 作为另一个 MV 的源（cascading views）尚未验证；调度器需要保证
     拓扑序刷新。
- 结论：本计划先补 (a) 并顺手验证 (e)（已在 §10.19 验证）；b–e 作为已知扩展点登记在 §10.4。

### 10.2 W0：过滤谓词表示（与 W1 同一个 PR）

- analyzer 从逻辑计划提取 `Expr::Filter` 谓词（或等价 AST），**spec 中存可解析 SQL 文本**
  `filter: Option<String>`（用 `datafusion::sql::unparser::Unparser` 渲染，保证可回注到运行
  时的字符串 SQL）；**移除 `FilterCondition`/`Vec<FilterCondition>` 的旧路径**（无兼容负担），
  `Row` 视图一并迁移；`CompareOp` 保留给 `SemiAntiCondition`。
- 校验：谓词只引用源列、无子查询/聚合/窗口/volatile；运行时把文本嵌入 SQL，规划或执行失败
  即报错（不静默）。
- 覆盖 Operand：`= <> < <= > >=`、`IN`、`BETWEEN`、`LIKE`、`IS [NOT] NULL`、
  `AND/OR/NOT`、简单 `CAST`/常量表达式。

### 10.3 W1（聚合 WHERE）与 W2（窗口/TOP-K/UNION WHERE）

**W1：SUM/COUNT、MIN/MAX、DISTINCT（+ `Row` 复用）**

- 语义要点：keyed 源是 upsert，变更贡献 = `f(new)·P(new) − f(old)·P(old)`，因此
  **delta 按新行过滤、old 按旧行过滤**；`affected_groups` 取"新命中分组 ∪ 旧命中分组"。
- 注入点：
  - `sum_count_refresh_sql`：keyed 的 `delta` 聚合与 `old_agg/old_changed`；append-only 的
    signed delta（`where` 或 `case when P`）与 old 读；`sum_count_rebuild_sql` 源读；
  - `refresh_value_count` 内联 SQL（MIN/MAX/DISTINCT）：state 增量/撤回、MV 计算、rebuild；
  - `affected_groups_sql`：`delta_groups` 与 `old_groups` 分别过滤。
- analyzer：接受 `Aggregate` 下方的 `Filter`（含 `Filter` 在 `Projection`/`Aggregate` 之间的
  常见排布），提取谓词；不再报"聚合上的 WHERE"。

**W2：Window / Top-K / UNION ALL**

- Window：`window_affected_cte`、`window_refresh_sql`、`window_rebuild_sql` 的源读先过滤
  再开窗（语义 = `SELECT ... OVER (...) FROM (SELECT * FROM src WHERE P)`）。
- Top-K：`top_k_affected_cte`、`top_k_computed_cte`、`top_k_refresh_sql`、
  `top_k_rebuild_sql` 加过滤（先过滤再取 `row_number() <= k`）。
- UNION ALL：spec 由 `source_table_ids: Vec<String>` 改为
  `sources: Vec<UnionSource{ table_id, filter: Option<String> }>`（新格式）；分支各自过滤，
  仍要求各分支输出 schema 一致。

### 10.4 测试深度（随 W1/W2 同步）

- **H1 fixtures**：slt harness 支持自定义源 schema 与多张表；`ivm-append` 扩展
  Float64/Boolean/Date32/Timestamp/Decimal128/NULL 与多列 key。
- **H2 action 断言**：`/*ivm-expect-action bootstrap|incremental|rebuild|overwrite*/`，
  与执行结果比对（当前 slt 未验证 action）。
- **H4/H5 覆盖**：带 WHERE 的 SUM/COUNT、MIN/MAX、COUNT(DISTINCT)/SUM(DISTINCT)、窗口、
  TOP-K、UNION ALL 的 slt；同算子"不同 WHERE → rebuild"用例；差分 oracle 把现有
  "delete 撤回"场景换成真 WHERE，并为每个新形状加 10 轮随机变更场景。
- **H6 错误矩阵**：子查询谓词、引用不存在列、`HAVING`、`OR` 中混入聚合等非法形态逐条
  `statement error`，并确认无副作用（executor 已保证先校验后动 state）。

### 10.5 不支持算子清单（backlog，按优先级）

> 该表在 2026-10 按当前实现重写；已完成项见 §10.6 起的实施记录（0.x 未列）。

| 组 | 缺口 | 建议 |
|---|---|---|
| 连接/集合 | 多路链中的外连接剩余形状（keyed 1:1 链含非基表键已实现，见 §10.92/§10.93；右侧一对多、bushy 树仍拒绝）；`INTERSECT`/`EXCEPT` 的重复计数放宽（可空键已支持，匹配计数差异仍拒绝）；pair 谓词只能引用已物化列 | 设计级扩展（多视图链 / 空安全 join） |
| 子查询/CTE | `DISTINCT`/多聚合的相关子查询、聚合右输入的嵌套/不透明派生表 | 随多视图链一起做 |
| 入口/表 | **分区源表**（需打通 `lakesoul-io` 分区值读取与 IVM 读取路径） | 跨模块，独立立项 |
| 类型 | Float/Decimal/Date/Boolean/Timestamp 的聚合/分组/去重与 UNION 已系统覆盖（typed slt + 差分 oracle）；剩余：Decimal 超出 Decimal128(38) 的 SUM 溢出、嵌套类型（List/Struct） | 随需求做 |
| 表达式 | 每个聚合族各自的小形状（如 `ANY_VALUE` 等顺序相关函数按需明确拒绝） | 按需接线 |

### 10.6 本轮 PR 拆分

1. **PR-1**：W0 + W1 + H1/H2 + 聚合类 slt/oracle（一个 PR）。
2. **PR-2**：W2 + 窗口/TOP-K/UNION slt/oracle。
3. 后续单独立项：HAVING、AVG、SELECT DISTINCT、窗口扩展、外连接/多表 join、
   子查询/CTE、M5 文档与指标（复用 #959 观测）。文档见 §10.42、指标见 §10.46（均已完成）。

### 10.7 PR-1 实施记录（W0 + W1 + H1/H2）

- **W0 已落地**：`ViewSpec::Row/SumCount/MinMax/DistinctAgg` 存 `filter: Option<String>`；
  analyzer 用 `Unparser` 渲染谓词并剥离关系名，回注运行时 `delta/old/src` 别名下都能解析；
  `FilterCondition`/`LiteralValue` 已删除，`CompareOp` 仅剩 `SemiAntiCondition` 使用。
  谓词校验拒绝聚合/窗口/子查询；引用不存在的列在 `SessionState::create_logical_expr`
  解析（`validate_*` 与执行前）时报错。
- **W1 已落地**：注入点与 §10.3 一致——keyed 的 delta 过滤新行、`old_changed/old_agg`
  过滤旧行；append-only 的 signed delta 直接 `where`；`sum_count_rebuild_sql` 与
  value-count 的 state 增量/重建同步过滤；`affected_groups_sql` 对 `delta_groups` 与
  `old_groups` 分别过滤（`delta_pks` 不过滤，保证被改 key 的旧值能被正确撤回）。
- **analyzer 形状**：接受 `Aggregate -> (Projection|Filter)* -> TableScan` 链，并读取
  `TableScan.filters` 与 `scan.projection`（优化器把过滤/投影下推进 scan 的形态）；
  HAVING、子查询谓词仍明确报错。
- **H1/H2 已落地**：`/*ivm-expect-action bootstrap|incremental|rebuild|overwrite*/`；
  `ivm-append` 支持 Float64/Boolean/Date32/Timestamp/Decimal128/NULL 与多列 key fixture；
  fixture 抽取为 `run_script_for_source`，可传自定义源 schema/主键。
- **测试**：`sum_count_where.slt`、`min_max_where.slt`、`row_where.slt`、
  `sum_count_multi_key.slt`；2 个真 WHERE 差分 oracle（SUM/COUNT `v > 30`、
  MIN `v > 30 AND g <> 'g1'`）；analyzer 优化计划（scan 下推）单测。全量 IVM 套件
  35 个测试二进制 / 113 个测试通过。
- **语义变化**：过滤谓词里的 `= NULL` 现在是 SQL 语义（结果为未知）；需要判空请写
  `IS NULL`。旧 `LiteralValue::Null + Eq` 的"= NULL 即 IS NULL"特例随旧格式一并移除。

### 10.8 PR-2 实施记录（W2：Window / Top-K / UNION ALL 的 WHERE）

- **spec**：`ViewSpec::Window` 与 `ViewSpec::TopK` 增加 `filter: Option<String>`；
  `ViewSpec::UnionAll` 由 `source_table_ids: Vec<String>` 改为
  `sources: Vec<UnionSourceSpec { table_id, filter }>`（新格式，无兼容）；
  typed 侧新增 `UnionSource { table, filter }`，`UnionAllView.sources: Vec<UnionSource>`。
- **analyzer**：原 `collect_aggregate_source` 泛化为
  `collect_filtered_source(plan, tables, shape)`，aggregate / window / top-k 共用，
  接受 `(Projection|Filter)* -> TableScan` 链并收集 `TableScan.filters`；
  UNION ALL 的 `union_branch` 改为返回 `(IvmTable, Option<String>)`，用
  `plan.schema()` 与源 schema 比对，保持"分支必须按源列顺序全列输出"的校验。
- **语义（关键）**：
  - Window/Top-K：谓词注入 `window_function_cte` / `top_k_computed_cte` 的源读
    （即"先过滤、后开窗/排名"）。affected partitions/groups 仍用**未过滤**的 delta 与
    MV 旧行推导——行离开谓词时其旧分区/旧分组必须重算并删除 MV 行；多余的重算幂等无害。
  - UNION ALL：keyed 分支的 `affected`（去重 pk）来自未过滤 delta（离开谓词的行也要删）；
    插入行用过滤后的当前行；append-only 分支直接过滤 delta；rebuild 过滤当前状态。
- **顺带修复**：executor `expected_mv_schema` 的 Window 分支原先把 `order_keys`
  当作 MV 的 row keys，实际 MV 以**源主键**为 row keys（runtime 一直如此）；
  window view 此前没有 SQL 入口端到端用例，故未暴露。现已修正。
- **测试**：`window_where.slt`、`window_aggregate_where.slt`、`top_k_where.slt`、
  `union_where.slt`、`union_append_where.slt`（含 action 断言）；差分 oracle 泛化为多源
  （`__SRC0__/__SRC__` 占位符）并新增 window / top-k / union ALL 三个 10 轮随机场景，
  每个 oracle 断言 MV 在 10 轮内非空；slt harness 新增 `run_script_for_sources`、
  `SltSource::{keyed, append_only}` 多源 fixture。全量 IVM 套件 35 个测试二进制 /
  126 个测试通过。
- 语义变化：UNION ALL 的持久化格式改为 `sources`（旧 spec 不兼容，符合"无旧格式兼容"决策）。

### 10.9 HAVING 实施记录（PR-3）

- **spec**：`ViewSpec::{SumCount, MinMax, DistinctAgg}` 增加 `having: Option<String>`；
  谓词渲染在 **MV 列** 上（`sum_v`/`count_v`/`__ivm_nonnull_count`/`value` + 分组键），
  例如 `sum_v > 10 AND g <> 'x'`；typed 视图增加 `having` 与 `with_having`。
- **analyzer**：识别 `Filter* -> Aggregate`（`HAVING`，可带外层 `Projection`，MIN/MAX 的
  投影会被优化器去掉）与 top-k 的 rank filter 区分；plan 中 HAVING 以“聚合表达式显示名”
  的隐藏列（如 `sum(src.v)`）引用聚合，因此按 `Aggregate.aggr_expr` 的显示名建立
  聚合→MV 列映射后再渲染；未物化的聚合（如 SUM 视图上的 MAX、COUNT(v)）明确报错。
- **运行时语义**：
  - MIN/MAX/DISTINCT：value-count **state 本身是完整的**，只在 `mv_ins` 上加 HAVING 条件、
    rebuild 外层过滤即可，天然支持“之前不达标的分组重新达标”。
  - SUM/COUNT：MV 中可能缺少不达标的分组，delta 无法推出分组全量，因此 HAVING 视图的
    增量刷新额外按受影响分组**裁剪读取当前源状态**（`read_current_filtered` + group filters）
    计算 `group_now`，用“受影响分组列表（delta ∪ old）”删除旧 MV 行、用 `group_now` 过滤
    HAVING 后插入；append-only 源保留撤回标记并做 signed 聚合。非 HAVING 路径不变。
- **rebuild**：三种聚合的 rebuild 都在聚合结果外层套 HAVING 过滤。
- **发现并记录**（已在 §10.11 修复）：`COUNT(DISTINCT)`/`SUM(DISTINCT)` 经 SQL 入口曾因
  优化器把 distinct 聚合改写成嵌套聚合
  (`Aggregate(count(alias1)) -> Aggregate(groupBy=[g, alias1])`) 而失败。
- **测试**：`having.slt`（阈值双向跨越、分组离开/回归、COUNT+分组键、WHERE+HAVING、
  删除导致失败）、`min_max_having.slt`、`having_append.slt`（append-only 撤回标记与
  update_before/after）；oracle 新增 SUM/COUNT HAVING 与 MIN HAVING 两个 10 轮随机场景；
  analyzer 单测覆盖优化计划、聚合仅出现在 HAVING、未物化聚合拒绝。全量 IVM 套件
  35 个测试二进制 / 131 个测试通过。

### 10.10 AVG 实施记录（PR-4）

- **spec**：`ViewSpec::SumCount` 增加 `average: bool`（`serde(default)`，缺省 false），复用
  sum/count 状态机；`SumCountView.average` + `with_average()`。Analyzer 识别 `AVG(col)`，
  允许与 `SUM(col)`/`COUNT(*)` 同列混用（必须同一 value column），与 MIN/MAX/DISTINCT
  混用明确报错。
- **schema**：`avg_mv_schema_for` = 分组键 + `sum_v` + `count_v` + `__ivm_nonnull_count` +
  `avg_v`（Float64）。AVG 仅支持数值类型（Int/UInt/Float）；Decimal、字符串等在创建/刷新
  校验时明确报错（`avg_result_type`）。
- **运行时**：增量与重建 SQL 在原有 sum/count 列后追加
  `case when n_nonnull > 0 then cast(sum as double) / cast(n_nonnull as double) else null end
  as avg_v`；append-only 源对 signed 聚合做同样处理；HAVING 可引用 `avg_v`（映射
  `avg(col)` → `avg_v`），未开 average 的视图引用 AVG 仍报"未物化"。
- **优化器细节**：优化后的计划把 `avg(v)` 规范化为 `avg(CAST(v AS Float64))`，而 HAVING
  隐藏列名仍是未加 cast 的 `avg(src.v)`；因此聚合参数提取需解开 cast，HAVING 映射同时
  登记原显示名与去 cast 归一化名。
- **测试**：`avg.slt`（更新/删除/HAVING 阈值进出）、`avg_null.slt`（NULL 忽略、全 NULL 组
  为 NULL）、oracle 新增 AVG 的 10 轮随机场景；analyzer 单测覆盖混用拒绝与优化计划形态。
  全量 IVM 套件 35 个测试二进制 / 136 个测试通过。
- **备注（用户要求，后续单独实现）**：用户可见/全量批量读取 MV 或普通表时，应**默认过滤**
  `rowKinds`/`op` 的 `delete`（及 `update_before`）标记，即提供"逻辑读"模式。当前 provider
  忠实返回 merge-on-read 后的最新物理行（被删 key 的最新行就是 delete 标记），所以显式查询
  需要 `WHERE "rowKinds" = 'insert'`（或运行时内部的 `filter_deletes`）。实现时要考虑默认
  开关的位置（provider / reader / table API）、与 AsOf/AtVersions 读的交互，以及运行时内部
  读是否保持 raw。

### 10.11 DISTINCT 的 SQL 入口修复（PR-5）

- **背景**：优化器把单个 `DISTINCT` 聚合改写成两层
  `Aggregate(count(alias1)) -> Aggregate(groupBy=[g, v AS alias1])`；此前 analyzer 直接报
  "an aggregate over an aggregate"，`COUNT(DISTINCT)`/`SUM(DISTINCT)` 只能走 runtime API。
- **修复**：`analyze_aggregate` 先识别该两层形态（`distinct_split`）：内层 `aggr_expr` 为空、
  内层分组 = 外层分组 + 恰好一个 `value AS alias`；WHERE/源过滤从**内层输入**收集；外层
  `count(alias)`/`sum(alias)` 还原为 `DistinctAggKind` 与 value column。raw 与优化计划产出
  同一 spec（definition_hash 一致）。
- **HAVING**：`HavingColumns::Distinct` 增加 `alias` 字段，`count(alias1)`（非 distinct）映射
  到 `value`；直接形态 `count(DISTINCT v)` 仍按原逻辑。混用/未物化照旧明确报错。
- **测试**：`distinct_agg.slt`（bootstrap、更新/删除、HAVING 阈值跌出与回归）、
  `distinct_sum.slt`；oracle 新增 COUNT(DISTINCT) 与 SUM(DISTINCT) 两个 10 轮随机场景；
  analyzer 单测覆盖拆分形态、WHERE、HAVING、raw/优化 spec 等价与混用拒绝。
- **仍待办**：无 GROUP BY 的 `SELECT DISTINCT`（值计数状态可复用，backlog 单独立项）。
  全量 IVM 套件 35 个测试二进制 / 141 个测试通过。

### 10.12 SELECT DISTINCT 实施记录（PR-6）

- **计划形态**：优化后 `SELECT DISTINCT` 就是一个"无聚合函数的分组"
  `Aggregate: groupBy=[[cols...]], aggr=[[]]`；raw 计划是 `Distinct::All(Projection ...)`，
  两条路径都已支持（`DISTINCT ON` 明确报错）。
- **映射**：直接复用 count-only 的 SumCount 视图——`group_keys = DISTINCT 列`、
  `value_column=None`、`having=None`。`count_v` 归零即删除分组，恰好等价于"该组合不再有行"，
  因此**无需任何运行时改动**；`SELECT DISTINCT g` 与 `SELECT g, COUNT(*) GROUP BY g` 产出
  同一 spec 与 definition_hash（同一份状态）。
- **限制**：DISTINCT 表达式必须是普通列（计算列报错）；分组键可空性与既有 group-by 视图
  一致（MV 主键列要求非空）。
- **顺带补回**：#973 遗漏的 HAVING analyzer 单测（此前只有 slt/oracle 覆盖）在本 PR 补回：
  `analyzes_having`、`analyzes_having_on_the_optimized_plan`、`rejects_unmaterialized_having`。
- **测试**：`select_distinct.slt`（单列：bootstrap、值随删除消失、WHERE 变化 rebuild、值回归）、
  `select_distinct_rows.slt`（多列：组合替换、重复组合删除后仍保留）；oracle 新增
  `SELECT DISTINCT v`（值列高频变化）10 轮随机场景；analyzer 单测覆盖 raw/优化一致、
  多列 + WHERE、与显式 count(*) 定义等价、计算列拒绝。
  全量 IVM 套件 35 个测试二进制 / 148 个测试通过。

### 10.13 逻辑读默认过滤 tombstone（PR-7）

- **背景**：`IvmTableProvider` 早已隐藏 `delete` tombstone（`drop_tombstones`，含无 CDC 列时的
  `rowKinds` 回退），但 ① `update_before` 未过滤；② 没有关闭过滤的入口，无法查看物理标记。
- **变更**：
  - 逻辑读默认过滤 `delete` 与 `update_before`（与运行时 `filter_deletes` 同一判据）；
    `Current`/`AsOf`/`AtVersions` 都返回逻辑行。
  - 新增 `IvmReadMode::Raw` 与 `IvmTableProvider::raw` / `IvmRuntime::table_provider_raw`，
    返回 merge-on-read 后的物理行（保留 tombstone），供调试与工具使用。
  - 运行时内部读取（`IvmTable::read_current*` 等）保持 raw 不变，不影响增量算法。
- **语义**：keyed 表 merge-on-read 后每 key 仅剩最新行——被删 key 的最新行就是 tombstone，
  逻辑读丢弃它；append-only changelog 的 `delete`/`update_before` 标记同样按行丢弃。
- **测试**：`table_provider.rs` 新增用例：append-only 源 5 行（insert / update_before /
  update_after / insert / delete）→ 逻辑读 3 行、raw 5 行；MV 组被删除后逻辑读 1 行、raw 2 行
  （`rowKinds='delete'` 只在 raw 可见）。既有 `provider_hides_cdc_tombstones` 继续覆盖 `delete`。
  全量 IVM 套件 35 个测试二进制 / 149 个测试通过。

### 10.14 LAG/LEAD 实施记录（PR-8）

- **选择**：先做 LAG/LEAD——语义不依赖窗口 frame（ROWS/RANGE、INCLUDE/EXCLUDE 都不影响），
  边界清晰；`FIRST_VALUE/LAST_VALUE/NTH_VALUE` 与 frame 语义绑定，留到"自定义 frame"批次。
- **spec**：`ViewSpec::Window` 增加 `window_args: Option<String>`（`lag(v, 2, 0)` → `"2, 0"`，
  分析期用 Unparser 渲染字面量）；typed `WindowView.window_args` + `with_window_args`。
- **analyzer**：WindowUDF `lag`/`lead` → `WindowFunction::{Lag,Lead}`；值参数必须是普通列，
  offset 必须是非负整数字面量，default 必须是字面量；表达式值、列 offset、负 offset、
  超过两个额外参数都明确报错；非聚合函数要求 ORDER BY（既有规则）。
- **运行时**：`window_function_cte` 生成
  `{lag|lead}(value[, args]) over (partition by ... order by ...)`；LAG/LEAD 与 ROW_NUMBER 一样
  在 ORDER BY 后追加源主键以确定性打破并列；MV 列名 `lag_v`/`lead_v`，类型取源列、可空；
  新增 `window_value_mv_schema_for`，executor 的 `expected_mv_schema` 变为
  ranking / aggregate / value 三分支。
- **测试**：`window_lag.slt`（bootstrap、更新、offset+default 定义变化 rebuild、删除）、
  `window_lead.slt`（bootstrap、行移动后重算）；oracle 新增 `LAG(v, 1, 0)` 的 10 轮随机场景；
  analyzer 单测覆盖 raw/优化一致、offset/default 渲染与各类拒绝。
  全量 IVM 套件 35 个测试二进制 / 153 个测试通过。

### 10.15 自定义 frame 与 FIRST_VALUE/LAST_VALUE/NTH_VALUE（PR-9）

- **frame**：`ViewSpec::Window` 增加 `window_frame: Option<String>`——仅当显式 frame 与声明的
  ORDER BY 默认 frame 不同时存储（分析期渲染为 SQL 文本）；typed `WindowView.window_frame`
  + `with_window_frame`。运行时把它拼进 OVER 子句；frame 始终落在分区内，"受影响分区整体
  重算"模型成立，因此 ROWS/RANGE/GROUPS 都可维护。忽略 frame 的函数（排名、LAG/LEAD）不输出
  frame，避免与它们追加的主键 tie-breaker 冲突（RANGE + offset 只允许一个 ORDER BY 键）。
- **值函数**：`WindowFunction::{FirstValue, LastValue, NthValue}`；MV 列 `first_value_v` /
  `last_value_v` / `nth_value_v`，类型取源列、可空；NTH_VALUE 的行号是必需的正整数字面量，
  存入 `window_args`；值必须是普通列。
- **选择**：自定义 frame 与 frame 相关的值函数一起交付；FIRST/LAST/NTH **不**追加主键
  tie-breaker，生成的 SQL 与用户声明完全一致（并列行的歧义由用户负责）。
- **测试**：`window_first_value.slt`（全分区 frame：首值进入/删除）、`window_last_value.slt`、
  `window_nth_value.slt`、`window_sum_frame.slt`（running ROWS frame 的增量重算）；
  oracle 新增 FIRST_VALUE + 全分区 frame 的 10 轮随机场景；analyzer 单测覆盖默认/显式 frame
  的存储与渲染、NTH_VALUE 参数校验与各类拒绝。
  全量 IVM 套件 35 个测试二进制 / 159 个测试通过。

### 10.16 NTILE/PERCENT_RANK/CUME_DIST 实施记录（PR-10）

- **函数**：`WindowFunction::{Ntile, PercentRank, CumeDist}`；NTILE 的桶数是必需的正整数
  字面量（用 `positive_integer_arg` 存入 `window_args`）；MV 列 `ntile`（Int64；DataFusion 的
  ntile 返回 UInt64，SQL 里 cast 成 bigint）、`percent_rank`/`cume_dist`（Float64）。
- **排名列类型**：`window_ranking_mv_schema_for` 改用 `rank_result_type`（PercentRank/CumeDist
  → Float64，其余 → Int64），列非空。
- **frame**：三者都忽略 frame（`uses_frame()` 为 false），不输出 frame；NTILE 追加主键
  tie-breaker（分桶确定），PERCENT_RANK/CUME_DIST 不追加（并列行必须共享同一个排名/分布值）。
- **测试**：`window_ntile.slt`（分桶随排序变化）、`window_percent_rank.slt`、
  `window_cume_dist.slt`（5 行唯一序，步长 0.25/0.2 便于精确断言）；oracle 新增 NTILE(3) 的
  10 轮随机场景；analyzer 单测覆盖桶数校验与缺少 ORDER BY 的拒绝。
  全量 IVM 套件 35 个测试二进制 / 164 个测试通过。

### 10.17 窗口聚合的 FILTER（PR-11）

- **spec**：`ViewSpec::Window` 增加 `window_filter: Option<String>`（分析期用 `render_filter`
  渲染 FILTER 谓词）；typed `WindowView.window_filter` + `with_window_filter`。
- **analyzer**：移除 "FILTER on a window function" 的拒绝；仅聚合窗口（SUM/COUNT）允许 FILTER，
  非聚合窗口明确报错；FILTER 谓词复用过滤器渲染管线（剥离关系名、禁聚合/子查询）。
- **运行时**：`window_function_cte` 生成 `sum(v) filter (where P) over (...)`、
  `count(1) filter (where P) over (...)`；`validate_window_view` 对源 schema 解析 FILTER。
- **语义**：FILTER 只影响聚合输入行，行本身仍在窗口内（frame 位置不变），受影响分区整体重算
  天然覆盖 FILTER 成员变化；分区内无匹配行时 SUM 为 NULL。
- **测试**：`window_filter.slt`（成员进入/移除、NULL 和）、oracle 新增 SUM ... FILTER 的
  10 轮随机场景（阈值随更新变化）；analyzer 单测覆盖 SUM/COUNT FILTER 的渲染。
  全量 IVM 套件 35 个测试二进制 / 167 个测试通过。

### 10.18 无 PARTITION BY 的全局窗口（PR-12）

- **spec**：`partition_keys` 允许为空——窗口作用于整张表；`WindowParts`/analyzer 不再要求
  PARTITION BY（top-k 仍要求 PARTITION BY + ORDER BY，全局 top-k 未开放）。
- **SQL 生成**：OVER 子句按需拼装（`over ()` / `over (order by ...)` / 带 frame 的变体）；
  computed / insert / delete / rebuild 的投影片段在无分区键时省略。
- **刷新语义**：全局窗口没有"受影响分区"概念：`window_affected_sql` 返回常量查询，刷新时读取
  全量源、删除全部 active 行并重算插入（每次窗口即全量重算，符合全局语义）；`already`
  反连接仍保证重放幂等。
- **测试**：`window_global.slt`（全局 ROW_NUMBER、更新后整体重编号）、`window_global_sum.slt`
  （全局 running ROWS frame）；oracle 新增全局 ROW_NUMBER 的 10 轮随机场景；analyzer 单测覆盖
  `over ()` 与 `over (order by ...)`；"单视图多窗口"改为新的拒绝断言。
  全量 IVM 套件 35 个测试二进制 / 171 个测试通过。

### 10.19 MV 级联验证（PR-13）

- **结论**：MV 作为下游视图的源表已经可用，**无需运行时改动**：
  - 下游视图把上游 MV 当作普通 keyed 源；MV 没有显式 CDC 列时 `change_column` 回退到
    `rowKinds`，上游写下的 `delete` 撤回行会像其它 CDC 标记一样在下游读取时被过滤。
  - 下游增量刷新的窗口/游标机制对上游 MV 同样生效；上游更新/删除经两层传播。
  - 重放（无新文件）在上游/下游都是 no-op；SQL 入口同样可用（executor 自动注册上游 MV）。
- **契约**：调度方（或未来的调度器）必须按拓扑序刷新；本 PR 只验证语义，不引入调度。
- **测试**：新增 `tests/cascading_views.rs`：
  1. 运行时两层 SUM/COUNT 链（bootstrap、更新、删除、重放 no-op）；
  2. SQL executor 路径的两层链；
  3. 聚合 MV → 全局 ROW_NUMBER 窗口视图（跨算子族的级联）。
- 全量 IVM 套件 36 个测试二进制 / 174 个测试通过。

### 10.20 VARIANCE/STDDEV 实施记录（PR-14）

- **函数**：`VAR_SAMP`（DataFusion 规范名 `var`）、`VAR_POP`、`STDDEV_SAMP`（`stddev`）、
  `STDDEV_POP`；新增 `VarianceKind` 与 spec `ViewSpec::Variance`（group_keys、value_column、
  statistic、filter、having）。
- **状态策略**：DataFusion 用 Welford 算法（m2/mean/count），无法用有符号 delta 合并，因此刷新时
  **按受影响分组从当前源重算**（复用 `affected_groups_sql` + `key_filters` 的分组裁剪）；MV 只存
  分组键 + 派生列（`variance_v` / `stddev_v`，Float64 可空：样本统计量在 <2 个值时 NULL），
  rebuild 直接全量聚合。该策略保证结果与原生聚合一致（oracle 可用）。
- **analyzer 细节**：单个 variance 聚合的参数是内联 `var(CAST(v AS Float64))`；出现多个聚合或
  HAVING 时优化器会把 cast hoist 成投影（`__common_expr_1 AS v`），所以 `variance_argument`
  同时解开 Cast 与 Alias，`collect_filtered_source` 接受仅含 `CAST(column)` 的投影；HAVING 把
  `var`/`var_pop`/`stddev`/`stddev_pop` 映射到派生列。与其它聚合族混用明确报错。
- **测试**：`variance.slt`（VAR_SAMP 的 bootstrap/NULL、更新、删除、HAVING 重建与增量）、
  `stddev.slt`（STDDEV_SAMP → STDDEV_POP 定义切换、WHERE、过滤后单值 → 0）；oracle 新增
  VAR_SAMP 的 10 轮随机场景（连续 3 次运行稳定）；analyzer 单测覆盖四种函数、WHERE/HAVING
  与混用拒绝。
  全量 IVM 套件 36 个测试二进制 / 178 个测试通过。
### 10.21 窗口 `IGNORE NULLS`（PR-15）

- **问题**：分析器此前完全忽略 `WindowFunctionParams.null_treatment`，`LAG(v) IGNORE NULLS`
  会被静默按默认的 `RESPECT NULLS` 维护（结果错误）。DataFusion 对值窗口函数实现了
  `IGNORE NULLS`（lead_lag / nth_value），对排名与聚合窗口则只是语法上的 no-op。
- **变更**：`WindowParts` / `ViewSpec::Window` / `WindowView` 增加 `ignore_nulls: bool`
  （serde default）；仅值函数（LAG/LEAD/FIRST_VALUE/LAST_VALUE/NTH_VALUE）保留该标志，
  排名/聚合窗口在分析期归一化为 false；`window_function_cte` 对值函数生成
  `lag(v[, args]) ignore nulls over (...)`。
- **语义**：偏移量只累计非 NULL 行；MV 列本就可空，重算模型对可空值同样成立。
- **测试**：`window_ignore_nulls.slt`（可空 v：bootstrap、更新为 NULL、链延长、
  `LAG(v, 2) IGNORE NULLS` 定义变化 rebuild）；analyzer 单测覆盖值函数保留、
  `RESPECT NULLS`/默认保持 false、排名与聚合窗口归一化。
  全量 IVM 套件 36 个测试二进制 / 180 个测试通过。

### 10.22 MEDIAN 实施记录（PR-16）

- **策略**：中位数同样无法用有符号 delta 合并，复用 variance 的"按受影响分组从当前源重算"机制。
  本 PR 把该机制抽成通用 `RecomputeParts`（view_id/source/mv/group_keys/value_column/aggregate/
  column/filter/having）、`recompute_refresh_sql` / `recompute_rebuild_sql` /
  `validate_recompute_view` 与 `refresh_recomputed` / `rebuild_recomputed` 引擎；variance 与
  median 共用，variance 的公开 API 与行为保持不变。
- **类型**：`median_result_type`——整数会被 DataFusion 强制转成 Float64（结果 Float64），
  Float32/Float64 保持原宽；其它类型（如 Decimal）在创建/刷新时明确报错。MV 派生列名
  `median_v`，可空。
- **spec/analyzer**：`ViewSpec::Median` + `MedianView`（group_keys、value_column、filter、having）；
  分析器识别 `median(...)`（参数复用 `recomputed_argument` 解开 Cast/Alias），HAVING 把
  `median` 映射到 `median_v`；与其它聚合族混用报错。
- **测试**：`median.slt`（奇数/偶数中位数、更新、删除、HAVING 重建与分组回归）；oracle 新增
  MEDIAN 的 10 轮随机场景（中位数与行序无关，增量结果与全量逐位一致）；analyzer 单测覆盖
  WHERE/HAVING 与混用拒绝。
  全量 IVM 套件 36 个测试二进制 / 184 个测试通过。

### 10.23 单视图多窗口（PR-17）

- **问题**：`ViewSpec::Window` 此前只能承载一个窗口函数（`function`/`value_column`/
  `window_*` 顶层字段），`row_number() over w, rank() over w` 这类共享同窗口的多列会被拒绝。
- **计划形态**：DataFusion 把共享同一 `PARTITION BY`/`ORDER BY` 的窗口函数放进**一个**
  `WindowAggr` 节点（每列可有自己的 frame）；不同 spec 会形成 `Window -> Window` 链，
  本期仍不支持并在分析期明确报错。
- **spec/typed**：新增 `WindowColumn { function, value_column, window_args, window_filter,
  ignore_nulls, window_frame, column }`（serde，旧顶层字段移除、无兼容层）；
  `ViewSpec::Window` 与 `WindowView` 改为 `columns: Vec<WindowColumn>`。typed 侧保留单列
  便捷构造（`new`/`new_with_function`/`new_aggregate`，`with_window_*` 作用于唯一列），
  新增 `new_with_columns`。
- **列名**：优先取投影别名（未命名时回退函数默认名，如 `sum_v`）；解析器同时处理 DataFusion
  的双层别名与优化计划中"列名 = 窗口表达式显示文本"的引用形态，并忽略自动生成的别名；
  同名列（如同窗口两个 `SUM` 未加别名）在分析期报错。
- **tie-breaker**：仅当**所有**列都需要时才把源主键追加进 ORDER BY（`ROW_NUMBER`/`LAG`/
  `LEAD`/`NTILE` 需要；`RANK`/`DENSE_RANK`/`PERCENT_RANK`/`CUME_DIST` 与带 frame 的聚合
  不需要，追加会破坏并列语义或 RANGE 的 peer 定义）。
- **schema**：新增 `window_columns_mv_schema_for` 逐列推导类型（SUM → 值类型可空；
  COUNT/排名 → Int64 非空；`PERCENT_RANK`/`CUME_DIST` → Float64 非空；值函数 → 源列类型
  可空）；既有三个单列 schema 函数改为其包装，公开 API 不变。
- **顺手修复**：`DESC` / `NULLS FIRST` 排序此前被静默按 `ASC NULLS LAST` 维护（结果错误），
  现于分析期拒绝；并移除 PR-16 遗漏在 `sql.rs` 的 `probe_median` 探针测试。
- **测试**：`window_multi.slt`（SUM + COUNT + ROW_NUMBER 同窗、更新/删除增量）；oracle
  多窗口全量差分；typed 运行时 `multi_column_window_refreshes_and_rebuilds`（refresh 与
  rebuild 一致）；analyzer 单测覆盖共享 spec、逐列 frame、别名、重名/不同 spec/DESC 拒绝。
  全量 IVM 套件 36 个测试二进制 / 186 个测试通过。

### 10.24 `COUNT(column)` 非空计数（PR-18）

- **问题**：`COUNT(column)` 此前直接报错（提示改用 `COUNT(*)`）。SUM/COUNT 的 MV 本来就维护
  `__ivm_nonnull_count`（`SUM(v)` 的非空计数，也是 AVG 的分母），只缺 SELECT 入口。
- **spec/typed**：`ViewSpec::SumCount` 与 `SumCountView` 增加 `count_column: Option<String>`
  （serde default；构建器 `with_count_column`）。为 `None` 时非空计数回退到 `value_column`，
  既有 SUM 视图的语义与序列化保持不变。
- **语义**：非空计数只有一个累加器，因此 `COUNT(col)` 必须与 `SUM`/`AVG` 使用同一列
  （不同列在分析期报错）；`COUNT(*)` 与 `COUNT(col)` 可并存（分别落到 `count_v` 与
  `__ivm_nonnull_count`）；只有 `COUNT(col)` 的视图 `value_column` 为 `None`，`sum_v` 恒为 0。
- **运行时**：`signed_delta_exprs` 拆分 value/count 两个列参数（有符号 SUM 用 value 列、
  有符号非空计数用 count 列），rebuild 与 HAVING 的 `count(col)` 同样按 count 列聚合；
  校验合并到 `validate_sum_count_view`。
- **测试**：`count_column.slt`（keyed：值↔NULL 更新、删除、HAVING、定义变化 rebuild）、
  `count_column_append.slt`（append-only changelog：delete/update 标记撤回）；差分 oracle
  扩展出 `run_oracle_with`（可选 schema 与可空值），新增 `COUNT(v)` 的可空随机负载；
  analyzer 单测覆盖 count-only、与 SUM 共用、列不一致拒绝与 HAVING 映射。
  全量 IVM 套件 36 个测试二进制 / 190 个测试通过。

### 10.25 聚合 `FILTER (WHERE ...)`（PR-19）

- **问题**：`SUM(v) FILTER (WHERE ...)` 此前直接报错。把 FILTER 直接折进视图过滤是错的：
  SQL 里 FILTER 只影响聚合输入、不删除分组，折叠会让"无匹配行"的分组消失（结果应为 NULL）。
- **语义**：采用**条件聚合**——行数 `count_v` 不过滤（分组存在性不变），SUM 与非空计数带
  `FILTER (WHERE ...)`，AVG = 条件 SUM / 条件非空计数；无匹配行的分组保留为 NULL 值 + 行数。
- **spec/typed**：`ViewSpec::SumCount` / `SumCountView` 增加 `aggregate_filter: Option<String>`
  （serde default；构建器 `with_aggregate_filter`）。
- **analyzer**：仅值聚合（`SUM`/`AVG`/`COUNT(column)`）可带 FILTER 且必须共用同一个谓词；
  `COUNT(*) FILTER`、混用不同谓词、其它聚合族（MIN/MAX/VARIANCE/MEDIAN/DISTINCT）带 FILTER
  一律明确报错。优化器会把共享谓词 hoist 成投影（`v > 5 AS __common_expr_1`），分析器用
  `resolve_hoisted` 解析回源列表达式并允许这种标量投影；HAVING 中的同一聚合按解析后的表达式名
  映射到物化列，不同 FILTER 的 HAVING 报错。
- **运行时**：`signed_delta_exprs` 接受聚合谓词（有符号 SUM/非空计数加 `FILTER`，行数不加）；
  keyed 的 new/old 聚合、HAVING 的 `group_now` 与 rebuild 都改用条件表达式。
- **测试**：`aggregate_filter.slt`（无匹配行的分组保留 NULL、更新跨阈值、HAVING、WHERE+FILTER）、
  两个差分 oracle（`SUM/COUNT(*) FILTER`、可空列的 `COUNT(v) FILTER`）、analyzer 单测覆盖
  混用与其它族的拒绝。
  全量 IVM 套件 36 个测试二进制 / 194 个测试通过。

### 10.26 窗口/Top-K 的 `DESC` 与 `NULLS FIRST/LAST`（PR-20）

- **问题**：排序方向此前完全没有记录（PR-17 起对非升序直接报错），`row_number() over
  (order by ts desc)` 这类"取每组最新一条"的常见写法无法使用。
- **spec/typed**：`ViewSpec::Window`/`TopK` 与 `WindowView`/`TopKView` 增加
  `order_by: Vec<String>`（serde default；构建器 `with_order_by`）——存**渲染后的排序项**
  （如 `"v" desc nulls first`）；为空时回退到既有 `order_keys` 的升序列引用，因此既有 spec
  序列化与行为不变。校验要求 `order_by` 非空时与 `order_keys` 等长。
- **analyzer**：`render_order_key` 把排序项渲染为：升序 `NULLS LAST`（默认）→ 裸列名；
  其余显式写 `asc|desc nulls first|last` 并引用标识符。同一 WindowAggr 节点内的多列仍要求
  排序完全一致（含方向与 NULL 位置）。
- **运行时**：新增 `order_items` 统一取排序项，窗口与 top-k 都用它拼 `ORDER BY`；tie-break
  仍按需追加源主键。
- **测试**：`window_desc.slt`（降序排名 + 更新/删除 + `DESC NULLS LAST` 重建，可空源）、
  `top_k_desc.slt`（降序 top-1 与 limit 变化重建）；两个差分 oracle（降序 `ROW_NUMBER`、
  降序 top-k，参考侧用 `v DESC, k` 与运行时 tie-break 对齐）；analyzer 单测覆盖方向/NULL
  位置渲染、默认归一化为空与 top-k 透传。
  全量 IVM 套件 36 个测试二进制 / 199 个测试通过。

### 10.27 `STRING_AGG(value, delimiter ORDER BY ...)`（PR-21）

- **问题**：`STRING_AGG` 顺序相关、无法用有符号 delta 合并，但复用 variance/median 的
  "按受影响分组从当前源重算"策略即可与全量一致；前提是排序写在聚合内部（`ORDER BY`），
  否则重算时输入顺序不稳定、增量与重建可能不同。
- **spec/typed**：新增 `ViewSpec::StringAgg` + `StringAggView`（group_keys、value_column、
  渲染后的 `delimiter` 字面量、渲染后的 `order_by`、filter、having）。派生列名
  `string_agg_<value>`，类型 `LargeUtf8` 可空（与 DataFusion 累加器一致）。
- **recompute 引擎泛化**：`RecomputeParts.aggregate`（函数名）改为 `aggregate_call`
  （完整渲染调用），variance/median 分别渲染 `var("v")` / `median("v")`；
  `StringAggView::parts()` 渲染 `string_agg("s", ',' order by "k")`。
- **analyzer**：识别 `STRING_AGG(value, delimiter ORDER BY ...)`：value 必须是纯字符串列、
  delimiter 必须是字符串字面量、聚合内必须带 `ORDER BY`（否则明确报错），不能与其它聚合
  混用；HAVING 按 value/delimiter/order 结构化比对映射到派生列。
- **顺手修复**：HAVING 映射新增"按聚合输出字段名"（`aggregate.schema.fields()`）映射，
  修掉渲染文本空格差异导致的多参聚合（STRING_AGG）HAVING 匹配失败。
- **测试**：`string_agg.slt`（自定义可空字符串 schema：bootstrap、值/顺序更新、NULL 跳过与
  全 NULL 分组、删除、HAVING、delimiter+DESC+WHERE 重建、重建后增量）；差分 oracle
  （`string_agg(g, '|' order by k)` 随机负载 10 轮全量对比）；analyzer 单测覆盖渲染
  （delimiter/方向）、缺少 ORDER BY、非字面量 delimiter 与混用拒绝。
  全量 IVM 套件 36 个测试二进制 / 202 个测试通过。

### 10.28 Row 视图的计算列（PR-22）

- **能力**：`SELECT k, v * 2 AS v2, CASE WHEN v > 5 THEN 'big' ELSE 'small' END AS bucket
  FROM src` 现在可以直接物化——投影里的标量表达式（算术/CAST/CASE/函数）按别名落成 MV 列。
- **spec/typed**：`ViewSpec::Row` / `RowView` 增加 `output_exprs: Vec<String>`（与
  `output_columns` 平行；为空表示全部按原列投影，既有 spec 与行为不变）；构建器
  `with_output_exprs`。
- **analyzer**：投影表达式渲染为 SQL（复用过滤谓词的 `Unparser` 渲染并剥离关系名）；
  未命名（没有 `AS`）的计算列明确报错；纯列投影保持紧凑，列改名存原列名。
- **schema**：新增 `row_expr_mv_schema_for`（旧 `row_mv_schema_for` 成为其无表达式包装）：
  用 DataFusion 逐个规划表达式取类型与可空性，列数不匹配时报错。
- **运行时**：`row_projection` 解析存储表达式（`create_logical_expr`）并按 MV 列名别名，
  用于 insert 与 rebuild；**删除路径改为按 MV 列名取数**（计算列在 MV 里是同名结果列，
  不能在 MV 上重算源表达式）；keyed 视图要求源主键列原样投影（拒绝 `k + 1 AS k`）。
- **测试**：`row_exprs.slt`（bootstrap、更新、删除、`WHERE`+表达式定义变化 rebuild）；
  analyzer 单测覆盖表达式/改名/纯列与 `WHERE` 组合。
  全量 IVM 套件 36 个测试二进制 / 204 个测试通过。

### 10.29 SUM/AVG 的聚合参数表达式（PR-23）

- **能力**：`SUM(v * 2)`、`AVG(v * 2)`（以及 `SUM(CASE WHEN ... THEN ... END)` 这样的条件
  聚合）现在可以直接物化——SUM 家族（含 COUNT(*) 机制）保留一个"值"，既可以是纯列，也可以
  是标量表达式。
- **spec/typed**：`ViewSpec::SumCount` / `SumCountView` 增加 `value_expr: Option<String>`
  （与 `value_column` 互斥；serde default）。纯列参数仍走 `value_column`，既有 spec 与行为
  不变；构建器 `with_value_expr`。
- **analyzer**：新增 `SumValue::{Column, Expr}` 统一解析 `SUM`/`AVG` 参数（解析 hoisted
  别名、去掉优化器加上的数值 CAST）；SUM/AVG 必须引用同一个值；`COUNT(column)` 不能与表达式
  值共存（非空计数只有一个累加器）；HAVING 中同值（纯列或表达式）的 SUM/AVG 映射到物化列。
- **schema**：新增 `sum_expr_mv_schema_for`（规划表达式取类型并做 SUM/AVG 数值校验）；抽出
  `expression_type` 与 `sum_count_schema_for`，`sum_count_mv_schema_for` / `avg_mv_schema_for`
  行为不变。
- **运行时**：`sum_count_value_sql` 统一取渲染值；`signed_delta_exprs` 改为接收已渲染的值
  （append-only 的 delete/update 标记按表达式有符号撤回）；keyed 的 new/old 聚合、HAVING 的
  `group_now` 与 rebuild（含非 keyed 分支）全部复用。
- **测试**：`sum_expr.slt`（bootstrap、表达式更新、删除、HAVING 增量、WHERE+表达式 rebuild）、
  `sum_expr_append.slt`（append-only 的 delete/update 标记撤回）；差分 oracle（`SUM(v * 2)` +
  `COUNT(*)` 全量对比）；analyzer 单测覆盖表达式/共享值/HAVING 映射与拒绝。
  全量 IVM 套件 36 个测试二进制 / 208 个测试通过。

### 10.30 `ARRAY_AGG(value ORDER BY ...)`（PR-24）

- **能力**：`ARRAY_AGG(value ORDER BY keys)` 直接把分组内的值收集成 List 列（延续
  STRING_AGG 的 recompute 策略；聚合内 `ORDER BY` 必填以保证确定性）。
- **spec/typed**：新增 `ViewSpec::ArrayAgg` + `ArrayAggView`（group_keys、value_column、
  渲染后的 order_by、filter；不支持 HAVING，遇到明确报错）。派生列 `array_agg_<value>`，
  类型 `List<value>` 可空（与 DataFusion 的累加器一致）。
- **运行时**：复用 `RecomputeParts`，渲染 `array_agg("v" order by "k")`；
  `refresh_array_agg` / `rebuild_array_agg` / `register_array_agg_view` 与 STRING_AGG 对称；
  schema 由 `array_agg_mv_schema_for` 推导。
- **analyzer**：值必须是纯列（任意类型）、聚合内必须带 ORDER BY，不能与其它聚合族混用；
  HAVING 不支持。`array_agg` 与 `string_agg` 一样从顶部 ORDER BY 拒绝中豁免。
- **测试**：`array_agg.slt`（bootstrap、更新重排、删除、DESC+WHERE 重建）、差分 oracle
  （随机负载 10 轮全量对比）、analyzer 单测（渲染/缺 ORDER BY/混用拒绝）。
  全量 IVM 套件 36 个测试二进制 / 211 个测试通过。

### 10.31 MIN/MAX 的参数表达式（PR-25）

- **能力**：`MIN(v * 2)`、`MAX(CASE WHEN ... THEN v END)` 等直接物化；MIN/MAX 保留用户在
  表达式里写的 CAST（不做 SUM/AVG 那样的数值强制转换剥离）。
- **spec/typed**：`ViewSpec::MinMax` / `MinMaxView` 的 `value_column` 变为 `Option<String>`
  并新增 `value_expr: Option<String>`（serde default，互斥；构建器 `with_value_expr`）。
  新增 `value_count_state_expr_schema_for` / `min_max_expr_mv_schema_for`（复用统一
  `expression_type` 规划表达式类型与可空性）；`ValueCountView` 增加 `value_expr`，
  refresh/rebuild 与值 SQL 统一走 `value_count_value_sql`。
- **analyzer**：`AggValue`（原 `SumValue`）统一承载"纯列或渲染表达式"；`value_argument`
  可选剥离优化器 CAST（SUM/AVG 剥离，MIN/MAX 不剥离）；**HAVING 对 MIN/MAX 改为结构化
  比对参数**——此前只要函数名与参数个数匹配就会映射，`HAVING MIN(w)` 会错误命中
  `MIN(v)` 的物化列。
- **测试**：`min_expr.slt`（表达式 bootstrap、更新、删除最小值、条件 MAX + WHERE 重建）、
  差分 oracle（`MIN(v * 2)` 随机负载全量对比）、analyzer 单测（表达式/条件聚合/HAVING
  同参数映射与不同参数拒绝）。
  全量 IVM 套件 36 个测试二进制 / 214 个测试通过。

### 10.32 STRING_AGG/ARRAY_AGG 的参数表达式（PR-26）

- **能力**：`STRING_AGG(CAST(v AS TEXT), '|' ORDER BY k)`、`ARRAY_AGG(v * 2 ORDER BY k)`
  等直接物化；字符串聚合保留用户写的 CAST（数值转字符串的常见用法）。
- **spec/typed**：`ViewSpec::StringAgg/ArrayAgg` 与 typed view 的 `value_column` 变为
  `Option<String>` 并新增 `value_expr`（互斥；构建器 `with_value_expr`）。派生列名：纯列为
  `string_agg_<col>` / `array_agg_<col>`，表达式为 `string_agg_value` / `array_agg_value`
  （`string_agg_output_column` / `array_agg_output_column`）。
- **schema/运行时**：新增 `string_agg_expr_mv_schema_for` / `array_agg_expr_mv_schema_for`
  （规划表达式：字符串类型校验 / 任意类型 → `List<type>`）；`parts()` 统一用
  `aggregate_value_sql`，refresh/rebuild 用 `aggregate_value_type` 校验。
- **analyzer**：两个分支改用 `value_argument(..., strip_cast=false)`；STRING_AGG 的 HAVING
  改为结构化比对参数（值 + delimiter + order），不再只看列名。
- **测试**：`string_agg_expr.slt`（CAST 值的 bootstrap/更新/删除 + HAVING 同 cast 重建）、
  两个差分 oracle（`STRING_AGG(CAST(v AS VARCHAR), '|')`、`ARRAY_AGG(v * 2)`）、analyzer
  单测（cast 保留、array_agg 表达式）。
  全量 IVM 套件 36 个测试二进制 / 218 个测试通过。

### 10.33 GROUP BY 表达式（PR-27，SUM/COUNT 家族）

- **能力**：`SELECT v % 10 AS bucket, SUM(v) FROM src GROUP BY bucket`（以及
  `date_trunc('day', ts)` 这类）直接物化：分组键可以是标量表达式，`GROUP BY` 允许引用
  SELECT 别名。
- **spec/typed**：`ViewSpec::SumCount` / `SumCountView` 增加 `group_exprs: Vec<String>`
  （与 `group_keys` 平行；为空表示全部为普通列；构建器 `with_group_exprs`）。
- **analyzer**：分组表达式从 Aggregate 的 `group_expr` 解析，别名通过 SELECT 投影匹配
  （复用 `projection_alias`，并解开 hoisted 别名）；表达式必须有别名；HAVING 中引用分组
  别名/输出字段名时按 Aggregate schema 映射到 MV 键列。
- **schema**：抽出 `group_key_fields`（普通列或按 `expression_type` 规划表达式，并拒绝与源列
  重名）；新增 `sum_count_groups_mv_schema_for`（同时支持分组表达式与值表达式，旧的
  `sum_count_mv_schema_for`/`sum_expr_mv_schema_for`/`avg_mv_schema_for` 均委托它）。
- **运行时**：新增 `project_group_keys`——在注册 delta/old/src/rebuild 基线前把计算出的键
  列 `with_column` 进批次（含空批次补 schema），下游 SQL（分组、join、HAVING）无需改动即
  可用；计算键无法用于源裁剪，此时跳过 `key_filters`（读全表保证正确）。
- **测试**：`group_expr.slt`（表达式分组 bootstrap、更新换桶、删除清空桶、HAVING+WHERE
  重建）、差分 oracle（`v % 10` 分组随机负载全量对比）、analyzer 单测（别名、混合键、HAVING
  映射、缺别名拒绝）。
  全量 IVM 套件 36 个测试二进制 / 221 个测试通过。

### 10.34 LEFT lookup join（PR-28）

- **能力**：`SELECT l.jk, l.lv, r.rv FROM fact l LEFT JOIN dim r ON l.jk = r.jk`——星型模型的
  事实表 ⋈ 维表：左表每一行保留其引用的右表 payload（无匹配为 NULL）。
- **约束**：两侧都必须有主键，且**右表以连接键为主键**（lookup 唯一），因此左行至多一个匹配、
  输出以左表主键为键（不会出现 NULL 主键）；连接键仍需两侧同名（沿用既有 join 规范）。
- **spec/typed**：新增 `ViewSpec::LookupJoin` + `LookupJoinView`（left/right/output、join_keys、
  left_value、right_value）；schema `lookup_join_view_schema_for` 输出
  `join_keys + left_value + right_value(可空) + 左主键 + rowKinds + epoch`。
- **analyzer**：`JoinType::Left` 分支校验右表主键集合等于连接键，复用 `join_values` 选 payload；
  非法形状（右表非该键、非等值条件）明确报错。
- **运行时**：keyed 路径——受影响左行 = Δ左表的主键 ∪ 连接键出现在 Δ右表的当前左行；
  对这些行 `delete(旧) + insert(用当前右表左连接的新值)`，epoch 幂等；右侧删除/更新会把
  引用它的左行重算为 NULL 或新值；rebuild 用两侧基线做同样的左连接。
- **测试**：typed `lookup_join.rs`（bootstrap、右表新增填 NULL、右表更新、右表删除回 NULL、
  左表换键、左表删除、rebuild 一致，逐步与 SQL 左连接对拍）、`left_join.slt`（SQL 入口端到端）、
  analyzer 单测（合法形状与右表非该键拒绝）。
  全量 IVM 套件 36 个测试二进制 / 224 个测试通过。

### 10.35 通用 LEFT JOIN（PR-29）

- **能力**：右侧非唯一的 `LEFT JOIN`（两侧均为 keyed 源）：左行保留**所有**匹配的右行，
  无匹配时输出一行 NULL 填充的 pair。
- **spec/typed**：新增 `ViewSpec::LeftJoin` + `LeftJoinView`；schema
  `left_join_view_schema_for`（与 keyed inner join 同形状，但连接键保留左表可空性、
  右 payload 与右主键别名可空）。analyzer 的 `JoinType::Left` 分支：右表以连接键为主键时
  仍走更省的 `LookupJoin`，否则在两侧 keyed 的前提下生成 `LeftJoin`（未 keyed 报错）。
- **运行时**：pair 键控（`__left_pk_*` + `__right_pk_*`，右主键可空）；刷新时对受影响左行
  **整体重写**（删除其全部旧 pair 再插入当前左连接的全部 pair），受影响左行 = Δ左表主键 ∪
  连接键出现在 Δ右表的当前左行；重放时同样的删除+插入天然幂等（无需 epoch 守卫）。
  `keyed_join_projection` / `keyed_join_output_columns` 抽出 `PairJoin` 复用给 inner 与 left。
- **验证**：右主键为 NULL 的 pair 在 MV 合并路径可用（与 value-count 状态列可空同理），
  typed 测试逐步与 SQL 左连接对拍。
- **测试**：typed `left_join.rs`（多匹配、NULL 连接键、右表更新/删除、新匹配填 NULL、
  左表换键/删除、rebuild 一致）、`left_join_multi.slt`（SQL 入口端到端）、analyzer 单测
  （lookup 与通用两种形状 + 未 keyed 拒绝）。
  全量 IVM 套件 36 个测试二进制 / 226 个测试通过。

### 10.36 FULL JOIN（PR-30）

- **能力**：两侧 keyed 的 `FULL JOIN`：匹配 pair + 左右各自未匹配的行（NULL 填充）。
- **spec/typed**：新增 `ViewSpec::FullJoin` + `FullJoinView`；schema `full_join_view_schema_for`：
  连接键、两侧 payload 以及**两侧**主键别名都可空（未匹配的右行没有左主键）。
  `keyed_join_schema_for` 抽出 `JoinSchema::{Inner,Left,Full}` 控制可空性，
  `keyed_join_projection` 增加 `keys_from_right`（未匹配右行的连接键取自右表）。
- **运行时**：受影响左行 = Δ左主键 ∪ 连接键出现在 Δ右表的当前左行；受影响右行对称。
  左路对受影响左行做左连接、右路对受影响右行做右连接，两路 union + distinct 后
  删除（按左主键或右主键匹配全部旧 pair）再插入；rebuild 用两侧基线同样两路合成。
  注意 LakeSoul 内部 writer 拒绝**声明为非空**的列出现 NULL——FULL JOIN 的左右主键别名
  因此在 schema 里必须声明为可空（Arrow nullability）。
- **测试**：typed `full_join.rs`（左/右未匹配、NULL 连接键、右删除使左行转为未匹配、
  新左行匹配未匹配右行、左删除、rebuild，逐步与 SQL FULL JOIN 对拍）、`full_join.slt`
  （SQL 入口端到端）、analyzer 单测（形状 + 未 keyed 拒绝）。
  全量 IVM 套件 36 个测试二进制 / 229 个测试通过。

### 10.37 RIGHT JOIN（PR-31）

- **能力**：`A RIGHT JOIN B` 与 `B LEFT JOIN A` 等价，本 PR 在分析器直接把两侧交换后复用
  `LookupJoin` / `LeftJoin`：
  - 原左表以连接键为主键（lookup 唯一）→ 交换后仍走 `LookupJoin`；
  - 否则两侧 keyed → 交换后走 pair-keyed `LeftJoin`（未匹配的右侧→左主键别名保留、原左表
    主键别名可空）。
- **约定**：输出列仍按"保留侧在左"的方向命名（`left_value` = 被保留侧 payload）；
  schema/运行时零新增（复用 PR-28/29 的两种左连接实现）。
- **analyzer**：`JoinType::Left`/`Right` 共用 `analyze_outer_join(kind, ...)` 助手；
  非等值条件、未 keyed 明确报错。
- **测试**：analyzer 单测（lookup 与 pair-keyed 两种交换结果）、`right_join.slt`
  （维度行保留、同一维度多事实、事实删除/换键、NULL 填充，SQL 入口端到端）。
  全量 IVM 套件 36 个测试二进制 / 231 个测试通过。

### 10.38 其它聚合族的 `GROUP BY` 表达式（PR-32）

- **能力**：`GROUP BY <expr>` 从 SUM/COUNT 家族推广到全部聚合族——Variance/Stddev、
  Median、`STRING_AGG`、`ARRAY_AGG` 都接受计算分组键（与 SumCount 同一套 `project_group_keys`
  机制，键列在注册 delta/old/src 前注入批次）。
- **spec/typed**：`ViewSpec::{Variance,Median,StringAgg,ArrayAgg}` 增加
  `group_exprs: Vec<String>`（serde default，空 = 全是普通列）；typed view 同步字段 +
  `with_group_exprs`；`RecomputeParts` 携带 `group_exprs`。
- **schema**：新增 `variance_groups_mv_schema_for` / `median_groups_mv_schema_for` /
  `string_agg_groups_mv_schema_for` / `array_agg_groups_mv_schema_for`（键字段走
  `group_key_fields`，同时支持计算键 + 计算 value），旧的 `*_mv_schema_for` 委托空表达式；
  executor 统一走 groups 版本。
- **运行时**：`validate_recompute_view` 在有表达式时跳过源列校验、改用 `group_key_fields`；
  `refresh_recomputed` 对 delta/old/src 三个注册点投影计算键，计算键无法裁剪源时跳过多余的
  `key_filters` 剪枝（`filters` 置空）；rebuild 对基线批次同样投影。
- **测试**：analyzer 单测（四个家族 + 普通列与表达式混合）、`variance_group_expr.slt`
  （keyed 源跨桶更新/删除/HAVING/WHERE，SQL 入口端到端）、4 个差分 oracle
  （variance/median/`ARRAY_AGG`/`STRING_AGG`，其中 string/array 同时带计算值与计算键）。
  全量 IVM 套件（lib + 37 个集成测试二进制 + doctest）238 个测试通过、0 失败。

### 10.39 VARIANCE/MEDIAN 参数表达式（PR-33）

- **能力**：`VAR_*`/`STDDEV_*`/`MEDIAN` 的参数可以是标量表达式（此前只接受普通列）。
  解析复用 SUM/AVG 的 `value_argument`：解析优化器 hoist 的别名、丢弃为数值强制添加的
  外层 CAST，普通列仍走列形式。
- **spec/typed**：`ViewSpec::{Variance,Median}` 的 `value_column` 变为 `Option<String>` 并新增
  `value_expr`；typed view 同步字段 + `with_value_expr`；`RecomputeParts.aggregate_call`
  用 `aggregate_value_sql` 渲染 `var(...)`/`median(...)`。
- **schema**：`variance_groups_mv_schema_for` / `median_groups_mv_schema_for` 增加
  `value_column`/`value_expr` 参数（用 `aggregate_value_type` 推导结果类型），旧函数委托；
  executor 统一传入两者；刷新/重建前的类型校验同步改为 `aggregate_value_type`。
- **测试**：analyzer（`var_samp(v * 2)`、`median(v * 2)`、普通列保持列形式）、
  `variance_expr.slt`（`VAR_POP(v * 2)`：bootstrap/删除/更新/HAVING rebuild/增量）、
  两个差分 oracle（variance/median 的表达式参数）。
  全量 IVM 套件（lib + 37 个集成测试二进制 + doctest）242 个测试通过、0 失败。

### 10.40 UNION 去重（PR-34）

- **能力**：`SELECT ... UNION SELECT ...`（去重）。MV 每行保存一个 distinct 值及其出现次数
  （`count_v`）；刷新按带符号 delta 调整计数，计数降到 0 的行从 MV 删除。
- **spec/typed**：新增 `ViewSpec::UnionDistinct` + `UnionDistinctView`（复用
  `UnionSource`/`UnionSourceSpec`）；MV schema
  `union_distinct_mv_schema_for(schema, change_column)` = 数据列（不含 CDC change 列）
  + `count_v` + rowKinds + epoch，主键为数据列。
- **analyzer**：`UNION` 的优化计划是 `Aggregate(groupBy=全部列, aggr=[]) -> Union`，原始计划是
  `Distinct(Union)`，两种形状都识别；分支必须按顺序选择**全部数据列**（CDC change 列由内部
  维护、不进入 distinct key）；要求分支 schema 一致且全部 keyed 或全部 append-only。
- **运行时**：
  - append-only 分支：delta 与 rebuild 都按
    `sum(case when op in ('delete','update_before') then -1 else 1 end)` 聚合（无 CDC 列则
    `count(1)`），与 SUM/COUNT 的 append-only 重建约定一致。
  - keyed 分支：新值 +1（排除 retract 行）、变更主键的 as-of 旧值 -1（复用 sum_count 的
    `key_filters` + as-of 读）；rebuild 对 merge-on-read 基线 `count(1)`（去掉 tombstone）。
  - 刷新 SQL 把 delta 计数与当前 active 计数合并，受影响 key 先 delete 再 insert；失败重试时
    本 epoch 的部分写入不计入基线但会被 delete 清理，重放幂等。
- **测试**：analyzer（keyed 形状、per-branch 谓词、混合 keyed/append-only 拒绝）、
  `union_distinct.slt`（keyed 两源：跨源重复计数、更新/删除/重新插入、谓词 rebuild、增量）、
  `union_distinct_append.slt`（append-only CDC 两源：delete 标记、delete+insert 替换、
  谓词 rebuild、增量）、差分 oracle（两 keyed 源 vs 全量 union + count）。
  全量 IVM 套件（lib + 37 个集成测试二进制 + doctest）246 个测试通过、0 失败。

### 10.41 ORDER BY 表达式（窗口/TOP-K 与 STRING_AGG/ARRAY_AGG）（PR-35）

- **能力**：窗口（含 TOP-K）的 `ORDER BY` 与 `STRING_AGG`/`ARRAY_AGG` 的聚合内 `ORDER BY`
  接受标量表达式，例如 `row_number() over (partition by g order by v % 10)`、
  `string_agg(x, ',' order by v % 10, k)`。
- **analyzer**：窗口/TOP-K 的排序项逐项解析——普通列保留列名；表达式把 `render_filter` 文本
  存入 `order_keys`（用于校验），`order_by` 额外包一层括号并带方向
  （`(v % 10) desc nulls first`）；聚合排序走扩展后的 `render_order_items`。
- **运行时**：`order_keys` 校验由"必须是源列"放宽为"源列或可用 `parse_filter` 解析的表达式"
  （`validate_window_view` 与 `validate_top_k_view` 各一处）；`order_items` 无需改动
  （表达式时 `order_by` 非空，不会被当作标识符引用）。
- **测试**：analyzer（窗口、混合方向、STRING_AGG/ARRAY_AGG、TOP-K）、
  `window_order_expr.slt`（bootstrap/更新改变桶序/删除压缩/谓词 rebuild/平局由主键打破）、
  差分 oracle（窗口 `ORDER BY v % 10`、`STRING_AGG(... order by v % 10, k)`）。
  全量 IVM 套件（lib + 37 个集成测试二进制 + doctest）250 个测试通过、0 失败。

### 10.42 M5 文档：`rust/lakesoul-ivm/README.md`（PR-36）

- **内容**：面向使用者的 README：
  - 分层结构（runtime / sql+executor / provider）与 SQL 入口语义（`INSERT INTO` 增量维护、
    `INSERT OVERWRITE` 全量、目标表必须存在且 schema 匹配、定义变化触发 rebuild、action）；
  - 支持形状总表（投影/过滤、`SELECT DISTINCT`、SUM/COUNT/AVG、MIN/MAX、DISTINCT 聚合、
    VAR/STDDEV、MEDIAN、STRING_AGG、ARRAY_AGG、窗口、TOP-K、内连接/lookup/LEFT/FULL/RIGHT、
    UNION ALL、UNION、semi/anti）与派生列名表（`sum_v`/`count_v`/`avg_v`/`__ivm_*` 等）；
  - 源约定（keyed vs append-only、CDC change 列、retention 约束）、逻辑读
    （provider 的 Current/AsOf/AtVersions/Raw 模式）、刷新语义（游标/epoch/重放/rebuild）、
    Rust API 示例、限制清单与测试命令。
- **同步**：`lib.rs` 顶部文档加 README 链接；PLAN §10.6 的 M5 条目标注文档已完成
  （指标/观测复用 #959 仍留待后续）。
- **验证**：纯文档改动，全量 IVM 套件保持（lib + 37 个集成测试二进制 + doctest）
  250 个测试通过、0 失败。

### 10.43 UNION 分支投影裁剪/改名（PR-37）

- **能力**：`UNION ALL`/`UNION` 的分支可以做列裁剪、改名与计算列（各分支输出
  schema 的名称与类型需一致），例如
  `SELECT k, g, v * 2 AS amount FROM a UNION ALL SELECT k, g, v AS amount FROM b`。
- **spec/typed**：`UnionSourceSpec`/`UnionSource` 增加 `columns`（输出列，空=全部源列）
  与 `exprs`（与 `columns` 平行的渲染表达式，空=全部普通列）；新增
  `union_output_schema_for(source_schema, columns, exprs)`；
  `union_distinct_mv_schema_for` 改为接收分支输出 schema；typed
  `UnionSource::with_projection`。
- **analyzer**：`union_branch` 复用行投影解析（`collect_row`），并校验各分支输出 schema
  一致（名+类型）；keyed 的 `UNION ALL` 分支必须把主键作为普通列保留（否则运行时的
  pair 匹配失效）；`UNION` 分支不得投影 CDC change 列（含表达式引用，逐表达式解析检查）。
- **运行时**：
  - `UNION ALL`：每分支按 `columns`/`exprs` 构建 DataFrame 投影（表达式经
    `create_logical_expr` 解析并 alias 到输出列）；append-only CDC 源的标记翻译为 MV 的
    `rowKinds`（`delete`/`update_before` → `delete`），即使 CDC 列不在投影里逻辑读也能过滤；
    rebuild 路径同样使用投影。
  - `UNION`：每分支先在子查询里投影再做计数聚合（append-only 的带符号计数在投影内计算，
    keyed 的新/旧两侧各自投影），rebuild 同理。
- **测试**：analyzer（append-only 投影/改名/计算列、keyed 丢主键拒绝、UNION 投影与 CDC
  列拒绝）、`union_projection.slt`（keyed 两源：计算列、更新/删除/谓词 rebuild/增量）、
  差分 oracle（keyed `UNION` 投影 + 计数）。
  全量 IVM 套件（lib + 37 个集成测试二进制 + doctest）253 个测试通过、0 失败。

### 10.44 类型覆盖：Float64 / Decimal / Date / Boolean（PR-38）

- **动机**：运行时对类型是泛化的，但此前只有行视图有 `row_types.slt` 的系统覆盖；
  聚合、DISTINCT、MIN/MAX、窗口与重算家族的 Float64/Decimal128/Date32/Boolean
  路径缺少验证。
- **新增 slt**（复用 `typed_source_schema`：k Int64、v Float64 可空、flag Boolean、
  d Decimal128(10,2) 可空、day Date32、op）：
  - `typed_sum_avg.slt`：`SUM/COUNT/AVG(v)` 按 Boolean 分组——NULL 不计入 sum 与非空计数
    （`__ivm_nonnull_count`）、删除清空分组、HAVING rebuild 与增量；
  - `typed_min_max.slt`：`MIN/MAX(d)` 按 Date32 分组——Decimal 比较、MIN→MAX 定义变化重建
    （同 schema）、删除最大值回退、谓词 rebuild；
  - `typed_distinct.slt`：`COUNT(DISTINCT d)` 按 Boolean 分组——Decimal 状态键与跨分组
    重复值；
  - `typed_window.slt`：`ROW_NUMBER() ... PARTITION BY flag ORDER BY d DESC, day`——
    Boolean 分区、可空 Decimal 排序（DESC 默认 NULLS FIRST）、Date tie-break、更新/删除/
    谓词 rebuild/增量；
  - `typed_variance.slt`：`VAR_SAMP(v)` 按 Boolean 分组——重算家族的 Float64 与单值
    NULL 样本。
- **验证**：全量 IVM 套件（lib + 37 个集成测试二进制 + doctest）258 个测试通过、
  0 失败。

### 10.45 CTE / 派生表支持与验证（PR-39）

- **能力**：`WITH` 与派生表在优化器内联后按内层形状维护
  （`WITH filtered AS (SELECT ... WHERE ...) SELECT g, SUM(v) FROM filtered GROUP BY g`、
  `SELECT ... FROM (SELECT ...) t`）；CTE 体内可以含聚合/窗口，只要外层是普通列引用
  （如 `WITH totals AS (SELECT g, SUM(v) AS s ...) SELECT g, s FROM totals`）。
- **入口修复**：`IvmSqlExecutor` 的关系收集器现在记录查询里的 CTE 别名
  （`pre_visit_query`），引用 CTE 的单段关系不再被当成表打开——此前
  `INSERT INTO mv WITH x AS (...) SELECT ...` 会报 `table default.x not found`。
- **非目标**（仍在限制清单）：标量/相关子查询、聚合之上的计算列
  （`SELECT s * 2 FROM (SELECT SUM(v) AS s ...) t`）。
- **测试**：analyzer（CTE 过滤体、聚合体、派生表、窗口派生表）、`cte.slt`
  （bootstrap、进入过滤的更新、删除、阈值变化 rebuild、增量、派生表等价定义走增量）、
  差分 oracle（CTE vs `WHERE` 参考）。全量 IVM 套件（lib + 37 个集成测试二进制 +
  doctest）261 个测试通过、0 失败。

### 10.46 M5 指标 / 观测（PR-40）

- **能力**：IVM 运行时与 SQL 入口通过 `metrics` crate 输出低基数指标（沿用 lakesoul-io
  的 `lakesoul_*` 命名，任意 recorder 可导出，例如 Prometheus exporter）：
  - `lakesoul_ivm_statements_total{action}` /
    `lakesoul_ivm_statement_duration_seconds{action}`（executor，含 `action=error`）；
  - `lakesoul_ivm_refreshes_total{kind,result}` /
    `lakesoul_ivm_refresh_duration_seconds{kind}`（`refresh_spec`，`result` 为
    `applied`/`noop`）；
  - `lakesoul_ivm_rebuilds_total{kind}` / `lakesoul_ivm_rebuild_duration_seconds{kind}`
    （`rebuild_spec`）；
  - `lakesoul_ivm_epochs_total{kind}`（已提交 epoch）。
- **实现**：新模块 `observability.rs`（`Once` 注册描述 + 记录函数）；
  `ViewSpec::kind()` 提供视图种类标签；`IvmExecutionAction::label()`；
  `IvmSqlExecutor::execute` 拆出 `execute_inner` 后计时并记录。
- **测试**：`tests/metrics.rs` 用本地 recorder（最小 `Recorder`/`CounterFn`/`HistogramFn` 实现）
  断言 bootstrap、两次 incremental（applied/noop）、rebuild 生命周期下的计数与直方图观测次数。
  全量 IVM 套件（lib + 38 个集成测试二进制 + doctest）262 个测试通过、0 失败。

### 10.47 CROSS JOIN（PR-41）

- **能力**：两侧 keyed 的 `CROSS JOIN`（含 `FROM a, b` 逗号写法，DataFusion 计划为
  `Join { on: [], join_type: Inner }`）；输出按左侧主键别名 + 右侧主键别名组成 pair 键，
  payload 为两侧各一列。
- **spec/typed**：新增 `ViewSpec::CrossJoin` + `CrossJoinView`（left/right/output、
  left_value、right_value）与 `cross_join_view_schema_for`（无连接键；两侧主键别名非空）；
  运行时复用 `keyed_join_projection`/`PairJoin`（`join_keys` 为空时改用
  `LogicalPlanBuilder::cross_join`）。
- **analyzer**：`on` 为空且无 filter 的内连接走 `analyze_cross_join`（要求两侧 keyed）；
  payload 从 SELECT 列表解析，若优化器把投影下推掉则从 join 输出 schema 推导（要求命名
  不歧义）；带 `WHERE` 的 cross join 明确报错。
- **运行时**：刷新把「左侧变更主键 × 当前右侧」与「当前左侧 × 右侧变更主键」两路 pair
  union + distinct（同一窗口两侧都变时去重），删除受影响 identity 的旧 pair
  （delete + insert 幂等，无需 epoch 守卫）；rebuild 用两侧基线做全量叉积。
- **测试**：analyzer（cross join 与逗号写法、要求 keyed、带 WHERE 拒绝）、typed
  `tests/cross_join.rs`（bootstrap、左右插入/更新/删除、同一窗口两侧同时变更、no-op 刷新、
  rebuild，逐步与 SQL cross join 对拍）、`cross_join.slt`（SQL 入口端到端）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）265 个测试通过、0 失败。

### 10.48 SQL 入口的 SEMI/ANTI join（EXISTS/IN）（PR-42）

- **能力**：`WHERE [NOT] EXISTS (SELECT ...)` 与 `IN (SELECT ...)` 经优化器去关联后计划为
  `LeftSemi`/`LeftAnti` join，直接复用运行时已有的 `SemiAntiView`（右侧可为 keyed 或
  append-only 源、支持等值键 + 额外比较条件、输出左侧列子集）。
- **实现**：无需运行时改动；补齐 SQL 入口的验证与文档。注意：分析器在**优化计划**上接受该
  形状（原始计划因子查询过滤报错，但 executor 始终分析优化计划）。
- **测试**：analyzer（EXISTS/NOT EXISTS/IN、额外比较条件）、`semi_anti.slt`（bootstrap、
  右侧启用/禁用组、左侧进入/离开、IN 等价定义走增量、切到 NOT EXISTS rebuild、反向增量）、
  两个差分 oracle（semi join 按非唯一键匹配、anti join）。README 的支持形状表更新为 SQL 写法，
  限制条目改为标量子查询。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）269 个测试通过、0 失败。

### 10.49 MIN/MAX 与 DISTINCT 的 GROUP BY 表达式（PR-43）

- **能力**：`GROUP BY <expr>` 从 SumCount 与 recompute 家族推广到 value-count 家族
  （MIN/MAX、`COUNT/SUM(DISTINCT)`）。
- **spec/typed**：`ViewSpec::MinMax`/`DistinctAgg` 增加 `group_exprs`；typed view 同步
  字段 + `with_group_exprs`；`ValueCountView` 携带 `group_exprs`。
- **schema**：新增 `min_max_groups_mv_schema_for` / `distinct_agg_groups_mv_schema_for` /
  `value_count_groups_state_schema_for`（键字段走 `group_key_fields`，状态表与 MV 都支持
  计算键）；旧函数委托空表达式；executor 统一走 groups 版本。
- **运行时**：`refresh_value_count`/`rebuild_value_count` 把计算键投影进 delta/old/src 批次
  （`project_group_keys`），计算键无法裁剪时跳过 `key_filters` 剪枝，`affected` 注册用
  `key_schema_for`。
- **analyzer**：DISTINCT 的优化器分组拆分（inner `group by keys, value` + outer count）
  在计算键下会把键 hoist 为 `group_alias_N`——`distinct_split` 现在把「外层引用的别名」
  识别为分组键，`analyze_aggregate` 把这些别名合并进 hoisted 表达式映射，从而还原出
  `bucket = (v % 10)` 的键/表达式。
- **测试**：analyzer（MIN/MAX、DISTINCT、普通列与表达式混合）、`min_max_group_expr.slt`
  （跨桶更新、删除清空分组、MIN→MAX 重建、增量）、`distinct_group_expr.slt`（distinct 值
  更新、删除降计数、跨桶移动）、两个差分 oracle。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）274 个测试通过、0 失败。

### 10.50 窗口链式多 clause（不同 PARTITION BY/ORDER BY）（PR-44）

- **能力**：一条语句里的多个窗口若 clause 不同（优化器计划为链式 `WindowAggr`，中间可能有投影），
  按 `ViewSpec::MultiWindow` 维护：MV 以源主键为键，把各 clause 的分区键物化为值列，再加所有
  窗口列。
- **spec/typed**：新增 `WindowGroupSpec`（partition/order/order_by/columns）与
  `ViewSpec::MultiWindow` + `MultiWindowView`；schema `multi_window_mv_schema_for`。
- **运行时**：刷新 SQL 一条完成——`affected`（delta 主键 ∪ 各 clause 分区变化的 MV 行）→
  `computed`（单条 SELECT 里对源表同时计算各 clause 的窗口表达式，复用抽出的
  `window_over_base` tie-breaker 逻辑）→ 受影响 identity 的 delete + insert（epoch 去重）；
  rebuild 对全量源计算；校验复用抽出的 `validate_window_column`。
- **analyzer**：`analyze_window` 沿 `peel` 收集链上的所有 Window 节点（允许中间投影），逐节点
  复用 `window_function_spec`（对顶层 projection 解析别名），单 clause 仍走原有 `ViewSpec::Window`。
- **测试**：analyzer（两种不同 clause、单 clause 仍为 Window）、`multi_window.slt`（两个不同
  clause：更新/删除/谓词 rebuild/增量）、差分 oracle。README 支持清单更新、限制条目删除。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）277 个测试通过、0 失败。

### 10.51 LOOKUP JOIN 的异名键（PR-45）

- **能力**：lookup `LEFT JOIN` 允许左右键不同名（`ON f.dim_id = d.id`）；右表仍须以自己的连接键
  为主键。
- **spec/typed**：`ViewSpec::LookupJoin` 增加 `right_keys`（空=与左键同名）；`LookupJoinView`
  同步字段、`with_right_keys` 与 `right_join_keys()`。
- **运行时**：`lookup_join_projection` 右侧按 `right_join_keys` 选择并别名 `__right_{左键}`；
  刷新时 delta 右侧取右键、左表与变更键的 semi join 使用（左键, 右键）；校验比较右表主键与右键。
- **analyzer**：连接键收集改为 (左,右) 名对（`on` 与 filter 等值条件都按 `side_of` 归位）；
  内/全/semi/anti 仍要求同名并给出明确错误；LEFT/RIGHT 分支把键对传入 `analyze_outer_join`
  （RIGHT 交换两侧时同时交换键），同名键保持 `right_keys` 为空以保持定义紧凑。
- **测试**：analyzer（异名 lookup 接受、同名保持紧凑、异名 inner 拒绝）、`lookup_join_names.slt`
  （bootstrap/NULL 填充/维度更新/删除回 NULL/事实换键/删除）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）278 个测试通过、0 失败。

### 10.52 缺口固化：带过滤的 join 输入与已知语义限制（PR-46）

- **探针结论**：join 一侧的 `WHERE` 会被优化器下推到 join 之下（`Filter` 包住扫描），
  带过滤的派生表同理；跨侧谓词进入 `join.filter`。这些形状此前均被拒绝，且**不会静默忽略**谓词。
- **改动**：
  - `join_input` 对 `Filter` 输入给出明确错误（"a join input with a WHERE clause (or a filtered
    derived table) is not supported yet"）；
  - 新增 `rejects_filtered_join_inputs` 回归测试（普通 join 侧过滤、过滤派生表、CROSS JOIN 跨侧
    谓词），把"拒绝而非静默出错"的行为固定下来；
  - README 限制清单补充：带过滤的 join 输入；`GROUPING SETS/ROLLUP/CUBE`、`COUNT(DISTINCT a,b)`、
    `SELECT DISTINCT ON`（上游计划可产生但运行时不维护）；append-only CDC 源的 `UNION ALL`
    删除标记无法撤回原始 insert 行（无行标识），需要撤回时应用 keyed 源。
- **验证**：全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）279 个测试通过、0 失败。

### 10.53 加固：GROUPING SETS 明确报错、未维护形状回归测试与 UNION 类型覆盖（PR-47）

- **报错改进**：`GROUP BY ROLLUP/CUBE/GROUPING SETS` 此前落到"GROUP BY expressions need an
  alias"的含糊错误；分组键解析现在先识别 `Expr::GroupingSet`，明确返回
  "GROUPING SETS / ROLLUP / CUBE are not supported yet"。
- **回归测试**：新增 `rejects_unmaintained_shapes`，固定以下形状为"明确拒绝而非静默出错"——
  聚合之上的计算列/标量子查询、三种 grouping set、多参数 `COUNT(DISTINCT a, b)`、
  `DISTINCT ON`（计划为 `first_value` 聚合）。
- **类型覆盖**：新增 `union_types.slt`——`UNION ALL` 跨越 Float64/Decimal128/Date32 列
  （keyed 源，投影保留主键），覆盖 bootstrap、更新、删除、分支谓词 rebuild 与增量，
  补齐 joins/union 的类型矩阵缺口。
- **验证**：全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）281 个测试通过、0 失败。

### 10.54 LOOKUP JOIN 的左侧过滤（PR-48）

- **能力**：lookup `LEFT JOIN` 的**左侧**可以带 `WHERE`（`... WHERE f.amount > 0`）；过滤后的左侧行
  进入/离开视图由主键受影响集自然处理（delta 主键含所有变更行，投影按过滤后的当前左侧状态重算）。
  右侧过滤会被优化器改写为 inner join（其它 join 形状）→ 仍明确拒绝。
- **spec/typed**：`ViewSpec::LookupJoin` 增加 `left_filter`；`LookupJoinView` +
  `with_left_filter`；校验解析该谓词。
- **analyzer**：`join_input` 现在返回 `JoinInput { table, alias, filter }`（识别左侧的 `Filter` 与
  扫描下推过滤，渲染为无限定列名）；lookup 情形保留左过滤、拒绝右过滤；inner/full/semi/anti 与
  CROSS JOIN 对任何一侧过滤都给出原有明确错误（不会静默忽略谓词）。
- **运行时**：刷新与重建把左过滤应用到当前左侧状态（delta 的受影响主键提取不过滤，保证离开过滤的
  行也能撤回其 pair）。
- **测试**：analyzer（lookup 左过滤被保留；inner/full/semi/anti 与 CROSS 的过滤仍拒绝）、
  `lookup_join_filter.slt`（bootstrap、进入/离开过滤、维度更新流入、过滤变化 rebuild）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）282 个测试通过、0 失败。

### 10.55 INNER JOIN 双侧过滤（PR-49）

- **能力**：inner join 的两侧都可以带 `WHERE`（`... JOIN dim d ON ... WHERE f.amount > 0 AND d.active`）。
  优化器把谓词下推到 join 之下（`Filter` 或扫描下推过滤），analyzer 将其保留在 `ViewSpec::Join`
  的 `left_filter`/`right_filter`；行的进入/离开过滤由主键受影响集自然处理（delta 主键/键不过滤，
  当前状态与 pair 投影按过滤后的两侧重算）。
- **运行时**：keyed 路径过滤 `left_now`/`right_now`；append-only 路径过滤 delta 与 as-of before
  状态；`rebuild_join` 过滤基线。`apply_side_filter`/`filtered_frame` 两个辅助函数统一处理。
- **语义说明**：FULL JOIN + 单侧过滤会被 DataFusion 改写为 LEFT/RIGHT join（NULL 侧行被过滤掉），
  因而走进 lookup/inner 支持路径；跨侧谓词（`a.v > b.v`）改写为 inner join + 非等值条件 → 仍明确拒绝。
  SEMI/ANTI 的侧过滤下推后仍明确拒绝。
- **拒绝范围收窄**：`rejects_filtered_join_inputs` 现在只覆盖 semi/anti 与 CROSS JOIN 的跨侧谓词；
  新增 `analyzes_filtered_inner_join_inputs`（双侧过滤 + 过滤派生表）。
- **测试**：analyzer、`join_filters.slt`（bootstrap、左右行进入/离开过滤、删除、过滤变化 rebuild）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）284 个测试通过、0 失败。

### 10.56 多列 COUNT(DISTINCT a, b)（PR-50）

- **能力**：`SELECT g, COUNT(DISTINCT a, b) FROM src GROUP BY g`；优化计划保留为单个
  Aggregate（不像单列那样 split），analyzer 将参数列保存为 `DistinctAgg.value_columns`。
- **运行时**：多列 distinct 没有可合并的「签名值状态」；DataFusion 的
  `count(DISTINCT a, b)` 计划可生成但**执行未实现**（`COUNT DISTINCT with multiple arguments`），
  因此 `refresh_distinct_agg`/`rebuild_distinct_agg` 走 recompute 家族：按受影响分组从当前源
  重算。`RecomputeParts` 增加 `distinct_columns`，SQL 生成改为对
  `select distinct 分组键, 值列... from src [where ...]` 的 `count(case when 值列均非空 then 1 end)`，
  既保留 SQL 的 NULL 语义（任一列为 NULL 的元组不计数），又让「所有元组为 NULL」的分组保留 0。
- **明确拒绝**：多列 distinct + `HAVING`、+ `FILTER`、或**无 GROUP BY**（全局聚合）→ 明确报错
  （recompute 家族的全局聚合另属 backlog）；analyzer 测试同步更新 `rejects_unmaintained_shapes`。
- **测试**：analyzer（spec 的 `value_columns`、HAVING/FILTER/全局拒绝）、
  `multi_distinct.slt`（bootstrap、重复元组、跨分组移动、元组坍缩、删除到空分组、分组回归、
  定义变化 rebuild）。全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）286 个测试通过、0 失败。

### 10.57 集合运算/空值感知 join 的明确拒绝（PR-51）

- **背景（正确性修复）**：`INTERSECT ALL`/`EXCEPT ALL` 会被 DataFusion 计划为
  `LeftSemi/LeftAnti Join`，与 `IN`/`EXISTS` 形状相同（此前 analyzer 会**静默按 semi/anti 维护**），
  但语义不同：集合运算把 NULL 视为相等（`null_equality = NullEqualsNull`），且 `ALL` 变体需要
  `min(cl, cr)` / `cl - cr` 的计数语义；`IS NOT DISTINCT FROM` 谓词同样计划为空值感知 join。
  维护的 semi/anti 视图用等值比较（NULL 永不匹配）且每个左行保留一行，会给出**错误结果**。
- **修复**：`analyze_join` 在任何 join 形状判断之前检查 `join.null_equality`，对
  `NullEqualsNull` 明确报错（"INTERSECT/EXCEPT (or a null-aware join predicate) is not maintained"）。
  该检查同时让 `INTERSECT`/`EXCEPT`（distinct，左输入为 Aggregate）在进入 `join_input` 之前获得
  清晰错误。
- **测试**：`rejects_set_operations` 覆盖 `INTERSECT`、`EXCEPT`、两个 `ALL` 变体、
  `IS NOT DISTINCT FROM` 内连接与空值感知 `EXISTS`。
- **backlog 更新**：集合运算若要支持需引入空值感知匹配（`IS NOT DISTINCT FROM`）与按行计数状态；
  全局聚合（无 GROUP BY）目前在运行时被 "needs at least one group key" 明确拒绝，支持它需要
  跨聚合家族改造（记录在案）。
- **验证**：全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）287 个测试通过、0 失败。

### 10.58 SEMI/ANTI JOIN 双侧过滤（PR-52）

- **能力**：semi/anti（`EXISTS`/`NOT EXISTS`/`IN`）两侧都可带谓词：外层 `WHERE f.x > 1` 过滤左源，
  子查询内的谓词（`d.rv > 10`）过滤右源；任一过滤的进入/离开都会使匹配生效或失效。
- **analyzer**：`join_input` 扩展为可穿透**列裁剪的 `Projection`**（仅接受纯列投影，拒绝别名/
  计算投影），且允许 `Projection`/`Filter` 任意交错；semi/anti 分支不再拒绝侧过滤，写入
  `ViewSpec::SemiAnti` 的 `left_filter`/`right_filter`。副作用：带侧过滤的 CROSS JOIN 现在给出
  「a join input with a WHERE clause ...」的准确错误（此前是 join input must be a table）。
- **运行时**：`SemiAntiView` 增加过滤字段与 builder；刷新时两侧投影按需补上过滤列
  （新 `filter_columns` 辅助从谓词提取列）；当前状态（`left_now`/`right_now`）应用过滤，
  delta/as-of-before 不应用（作为受影响集的超集，安全）；重建同样过滤基线；校验解析谓词。
- **测试**：analyzer（`analyzes_filtered_semi_anti_inputs`：EXISTS 与 IN 两侧过滤；
  `rejects_filtered_join_inputs` 仅保留 CROSS JOIN 跨侧谓词）、`semi_anti_filter.slt`
  （bootstrap、左/右行进入与离开过滤、键变更、右删除、过滤变化 rebuild + ANTI 变体）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）289 个测试通过、0 失败。

### 10.59 CROSS JOIN 与 pair-keyed LEFT JOIN 的侧过滤（PR-53）

- **能力**：`CROSS JOIN` 任一侧、pair-keyed `LEFT JOIN`（右侧非 join key 主键）的保留侧都可带
  `WHERE`；进入/离开过滤的行会新增/撤回其 pair（LEFT JOIN 无匹配时保持 NULL 填充对）。
  至此除 FULL JOIN（优化器会把单侧过滤重写为 LEFT/RIGHT）与「右侧过滤会被改写成 inner join」
  的情形外，所有被支持的 join 形状都支持侧过滤。
- **analyzer**：`analyze_cross_join` 不再拒绝侧过滤；`analyze_outer_join` 的 pair 分支把
  `left_filter`/`right_filter` 写入 `ViewSpec::LeftJoin`（RIGHT 分支交换后自然复用）。
- **运行时**：`ViewSpec::CrossJoin`/`ViewSpec::LeftJoin` 增加过滤字段（typed builder 同步）；
  刷新对当前 `left_now`/`right_now` 应用过滤（delta 受影响标识不应用），重建过滤基线；校验解析谓词。
- **测试**：analyzer（`analyzes_filtered_pair_joins`：cross 双侧、pair-left 左过滤）、
  `cross_join_filter.slt`（双侧进出过滤、删除、过滤变化 rebuild）、`left_join_filter.slt`
  （进入过滤产生 NULL 填充对、键变更、右更新、离开过滤撤回、重建）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）292 个测试通过、0 失败。

### 10.60 非等值 join 条件（pair payload 谓词）（PR-54）

- **能力**：inner join 的额外非等值条件（`ON l.k = r.k AND l.amount < r.limit`）与 CROSS JOIN 的跨侧谓词
  （`WHERE l.lo <= r.hi`）都作为 **pair 谓词**维护：仅物化满足谓词的 pair，任一 payload 变化会增删受影响
  pair；条件变化（含方向翻转）触发 rebuild。
- **analyzer**：`render_pair_conditions` 把 `join.filter` 的非等值比较渲染为 `left_value <op> right_value`
  （只允许两侧分别对应各自 payload 列，其它列明确拒绝）；`CROSS JOIN + WHERE` 不再整体拒绝而是交给
  `analyze_cross_join` 解析跨侧谓词。`rejects_unsupported_join_and_union_shapes` 相应收窄（改为非 payload 条件）。
- **spec/typed**：`ViewSpec::Join`/`ViewSpec::CrossJoin` 增加 `pair_filter`；typed builder `with_pair_filter`。
- **运行时**：`PairJoin` 增加 `pair_filter`；`keyed_join_projection`（keyed 路径）与 `join_projection`
  （append-only 路径）在输出投影前对 join 结果应用该谓词；新的 `apply_pair_filter` 用 join 结果 schema 解析；
  校验器按 payload 列类型解析谓词。受影响 pair 逻辑不变（谓词是两行身份的纯函数）。
- **测试**：analyzer（inner theta、cross theta、非 payload 条件拒绝）、`theta_join.slt`、
  `cross_theta_join.slt`（bootstrap、payload 变化增删 pair、删除、条件变化 rebuild）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）295 个测试通过、0 失败。

### 10.61 多列 COUNT(DISTINCT) 的 HAVING（PR-55）

- **能力**：多列 distinct 视图支持 `HAVING COUNT(DISTINCT a, b) > n`（此前明确拒绝）；分组跨越阈值
  时增量进入/离开 MV。
- **analyzer**：`HavingColumns` 新增 `MultiDistinct { columns }`，`having_column` 精确匹配
  `count(distinct <columns...>)` 并映射到 MV 的 `value` 列；多列分支改为调用 `render_having`。
  `FILTER` 仍明确拒绝（与 recompute 家族一致）。
- **测试**：analyzer（HAVING 渲染为 `value > 1`，FILTER 仍拒绝）、`multi_distinct.slt` 追加
  HAVING 定义（rebuild）与分组跨阈值（incremental）。全量 IVM 套件（lib + 39 个集成测试二进制 +
  doctest）295 个测试通过、0 失败。

### 10.62 INNER JOIN 异名连接键（PR-56）

- **能力**：inner join 支持两侧键名不同（`ON f.dim_id = d.id`），此前仅 lookup LEFT JOIN 支持；
  MV 输出保留左侧键名，两侧键列都不作为 payload。
- **spec/typed**：`ViewSpec::Join`/`JoinView` 增加 `right_keys`（空=同名），typed builder `with_right_keys`；
  `PairJoin` 携带 `right_keys`。
- **analyzer**：Inner 分支不再拒绝异名键；payload 排除两侧键名（`key_names` 并集）；FULL JOIN 仍拒绝
  （错误信息更新为「仅 inner join 与 lookup LEFT JOIN 支持」）。`analyzes_lookup_left_join` 的
  「异名键被拒绝」断言改为 FULL JOIN 形态。
- **运行时**：`keyed_join_projection` 与 `join_projection`（append-only）按 `right_keys` 投影右侧键列
  （仍别名为 `__right_<左键名>`）；校验器按 `right_keys` 检查右侧列与类型（长度需与 `join_keys` 平行）。
- **测试**：analyzer（异名 inner 的 join_keys/right_keys/payload；同名保持紧凑）、
  `inner_join_names.slt`（bootstrap、事实键变更、维度键变更、删除、定义变化 rebuild）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）297 个测试通过、0 失败。

### 10.63 新形状的差分 oracle 覆盖（PR-57）

- **oracle 差分层**：为近几轮新增的 join/聚合形状补随机差分测试（每轮随机增删改 + 全量重算对照 +
  幂等重放）：
  - `oracle_inner_join_names`：异名 inner 键（`a.k = b.v`）；
  - `oracle_theta_join`：inner join + pair 非等值条件 + 侧过滤；
  - `oracle_multi_distinct`：多列 `COUNT(DISTINCT v, k)`（参照实现因 DataFusion 不执行多参数
    count distinct，改用等价拼接表达式）；
  - `oracle_semi_anti_filters`：semi join 双侧过滤。
- **harness 扩展**：随机轮次前可注入初始行（`run_oracle_seeded` + `SeedRow`），否则随机数据很难
  产生「两源共享键」或「键值恰好相等」的匹配，视图恒为空；`run_oracle_with` 委托给种子版本
  （空种子保持原行为）。
- **未覆盖**：cross join 的跨侧谓词随机 oracle 因两侧列名相同（schema 共享）会触发 payload 歧义，
  暂由 slt 覆盖。全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）301 个测试通过、0 失败。

### 10.64 INTERSECT/EXCEPT 的受控子集（PR-58）

- **能力**：`INTERSECT`/`EXCEPT`（distinct 与 `ALL` 变体）在**语义等价子集**上维护：所有 join 列在两侧
  均不可空（NULL 永不出现，空值感知等价性退化为等值）且左行在 join 元组上唯一（左主键被 join 列覆盖，
  因而 `min(cl, cr)`/`cl - cr` 退化为存在性）。该子集也覆盖不可空列上的空值感知 `EXISTS` 谓词。
- **analyzer**：`analyze_set_operation` 替换原先的整体拒绝；剥掉 distinct 变体左输入的 no-op
  `Aggregate`（要求其分组列与 join 列集合一致、无聚合）；提取 `semi_anti_output_columns` 供普通
  semi/anti 分支与集合运算共用；不满足守卫的形状（可空列、主键未覆盖、distinct 分组列不一致、
  空值感知 inner join、残余条件）仍明确拒绝。
- **运行时**：无需改动（复用 `SemiAnti`）。
- **测试**：analyzer（4 个变体 + 空值感知 EXISTS 正向；4 类拒绝）、`set_ops.slt`（bootstrap、左/右更新、
  新匹配行、删两侧、EXCEPT/INTERSECT ALL/EXCEPT ALL 的定义循环）、两个种子 oracle
  （`oracle_intersect`/`oracle_except`，与 DataFusion 原生集合运算差分对照）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）305 个测试通过、0 失败。

### 10.65 全局聚合（无 GROUP BY，SUM/COUNT/AVG）（PR-59）

- **能力**：`SELECT SUM(v), COUNT(*), AVG(v) FROM src [WHERE ...] [HAVING ...]` 无 GROUP BY 的全局聚合：
  单行 MV（无主键）随每次刷新整行重写（重算 + truncate + 写入）；空源保持一行 `NULL/0`，HAVING 不满足时
  MV 为空。
- **运行时**：`validate_sum_count_view` 允许空 key 列表；`sum_count_rebuild_sql` 抽出
  `key_select`/`group_by_clause` 支持空 key（grouped 行为不变）；`refresh_sum_count` 对空 key 走全局分支
  （读当前源 → truncate MV → 重算 SQL 单行写入）。MV 无主键时 delete 标记无法按主键合并，因此必须
  truncate 而不是 delete+insert。
- **范围**：仅 SUM/COUNT/AVG 家族；其它聚合家族（MIN/MAX、DISTINCT、方差、中位数等）的全局聚合仍以
  “needs at least one group key” 明确拒绝（README 记录）。
- **测试**：`global_sum.slt`（bootstrap/更新/删空/复活/HAVING 重建与增量）、
  `oracle_global_sum`（随机差分：SUM/COUNT/AVG 与全量重算对照）。全量 IVM 套件
  （lib + 39 个集成测试二进制 + doctest）307 个测试通过、0 失败。

### 10.66 全局 MIN/MAX 与 DISTINCT（PR-60）

- **能力**：`SELECT MIN(v) FROM src`、`MAX(v)`、`COUNT(DISTINCT v)`、`SUM(DISTINCT v)`（可带 WHERE/HAVING）
  无 GROUP BY 的全局聚合，沿用 PR-59 的「单行 MV + 每次刷新整表重写」模式。
- **运行时**：`refresh_value_count`/`rebuild_value_count` 的空 key 分支：读当前源/基线 → truncate MV →
  新 `value_count_global_sql`（`min/max/count(distinct)/sum(distinct)` 直接作用于源列或表达式，可带 HAVING）
  单行写入；value-count 状态表保持为空（全局聚合不需要增量状态，与多列 distinct 的做法一致）。
  校验允许空 key。
- **测试**：`global_min_max.slt`（MIN→MAX 定义变化 rebuild、删除移动最值、空源 NULL 行）、
  `global_distinct.slt`（COUNT DISTINCT + 过滤定义 rebuild）、`global_distinct_sum.slt`
  （SUM DISTINCT，注意其 value 列可空需独立 MV schema）、两个随机 oracle（global MIN / global COUNT DISTINCT）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）312 个测试通过、0 失败。

### 10.67 全局方差/中位数/STRING_AGG/ARRAY_AGG（PR-61）

- **能力**：recompute 家族（VAR/STDDEV/MEDIAN/STRING_AGG/ARRAY_AGG）的无 GROUP BY 全局聚合，沿用
  PR-59/60 的「单行 MV + 每次刷新整表重写」；空源保留单行（聚合为 NULL，除 count 外），HAVING 不满足
  时 MV 为空。至此所有受支持聚合的全局形态都可用。
- **运行时**：`validate_recompute_view` 允许空 key；`refresh_recomputed`/`rebuild_recomputed` 增加全局
  分支（读当前源/基线 → truncate MV → 新 `recompute_global_sql`：`{aggregate_call} as {column}` 直接
  作用于源（含 ORDER BY 的 string/array agg）单行写入）。recompute 家族本无状态表，无需其它状态处理。
- **测试**：`global_variance.slt`、`global_median.slt`、`global_string_agg.slt`、`global_array_agg.slt`
  （各含增量步骤 + 过滤定义 rebuild）、`oracle_global_variance`、`oracle_global_median`。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）318 个测试通过、0 失败。

### 10.68 多分支 UNION 覆盖 + backlog 刷新（PR-62）

- **测试**：三分支 `UNION ALL`/`UNION` 的 slt（分支增删改 + `__ivm_source` 标记）与随机差分 oracle
  （3 源）；全局 `STRING_AGG`/`ARRAY_AGG` 的随机差分 oracle。此前的 2 分支测试未覆盖 3+ 分支
  （实现本就支持单 Union 节点多输入，本轮以测试固化）。
- **文档**：PLAN §10.5「不支持算子清单」按当前实现重写——已完成项移除，剩余缺口收敛为
  GROUPING SETS、三表 join/多 payload、标量/相关子查询、分区源表、极端类型与个别聚合函数。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）324 个测试通过、0 失败。

### 10.69 GROUPING SETS / ROLLUP / CUBE（PR-63）

- **能力**：`GROUP BY GROUPING SETS/ROLLUP/CUBE`（含 `GROUP BY g, ROLLUP(v)` 这类被优化器展开的混合写法）
  在 keyed 源上维护 `SUM`/`COUNT(*)/COUNT(列)/AVG`（可带 WHERE/HAVING）；每个分组集物化在**同一张 MV**
  里，以 `__ivm_grouping`（集合序号）+ 扁平键列（未分组的键为 NULL）为键；刷新按 `affected`（delta/old
  的扁平键元组，复用 `affected_groups_sql`）对每个集合重算受影响分组，删除旧行并写入新行。
- **spec/typed/schema**：`ViewSpec::GroupingSets` + `GroupingSetsView`（`new` + 各 builder）；
  `grouping_sets_mv_schema_for`（`__ivm_grouping` 非空、所有键强制可空、SUM/COUNT/AVG 列）；
  新增常量 `IVM_GROUPING_COLUMN` 并导出。
- **analyzer**：`analyze_grouping_sets` 解析三种 GroupingSet 形态（ROLLUP 前缀集、CUBE 全子集、显式集合），
  展平键为首见顺序并记录索引集合；聚合仅接受 SUM/COUNT/AVG（FILTER/DISTINCT/其它函数明确拒绝）；
  key 必须为纯列；HAVING 复用 `HavingColumns::SumCount`。`having_aggregate` 扩展为可跳过 Filter 与
  Aggregate 之间的纯投影（分组集的 HAVING 计划形态），`rejects_unmaintained_shapes` 移除旧的
  “GROUPING SETS 明确拒绝”断言。
- **运行时**：`refresh_grouping_sets`（读 delta/old/当前源/MV → 每集合的 delete/insert 分支 union all，
  `order by 键, "rowKinds"` 保证同键删除先于插入，`already` 守卫按 epoch 幂等）与
  `rebuild_grouping_sets`（每集合 `group by` 直算）；分发/指标/执行器 schema 同步。
- **测试**：analyzer（ROLLUP、CUBE 子集、显式集合、HAVING、AVG、混合写法展开、三类拒绝）、
  `grouping_sets.slt`/`grouping_sets_multi.slt`（明细/小计/总计、增删改、等价定义增量 vs 集合变化
  rebuild）、随机差分 oracle（ROLLUP 与 DataFusion 原生重算对照）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）328 个测试通过、0 失败。

### 10.70 BOOL_AND / BOOL_OR（PR-64）

- **能力**：`BOOL_AND(flag)`/`BOOL_OR(flag)`（列或表达式，可带 WHERE/HAVING）分组维护；沿用
  recompute 家族（按受影响分组从当前源重算），NULL 输入由 DataFusion 语义忽略，空分组/全 NULL 分组的
  行为与全量重算一致。
- **spec/typed/schema**：`ViewSpec::BoolAgg` + `BoolAggView`（`new`/`new_with_group_keys` + builder）、
  `BoolAggKind`（`bool_and`/`bool_or`）、`bool_agg_output_column`（`bool_and_<v>`/`bool_and_value`）、
  `bool_agg_mv_schema_for`/`bool_agg_groups_mv_schema_for`（Boolean 可空列）。
- **analyzer**：聚合循环新增分支（单参数、无 DISTINCT/FILTER，混用检查覆盖全部家族）、
  `HavingColumns::BoolAgg` 映射 HAVING 到派生列；执行器 schema 与分发/指标同步。
- **测试**：analyzer（表达式参数、HAVING 映射、非 Boolean 拒绝）、`bool_agg.slt`/`bool_or.slt`
  （NULL 忽略、更新翻转、分组消失、过滤定义 rebuild）、随机差分 oracle（表达式参数 + WHERE）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）332 个测试通过、0 失败。

### 10.71 oracle 种子可变 + GROUPING SETS 覆盖扩展（PR-65）

- **bug 修复（由新覆盖发现）**：`render_having` 用 `aggregate.group_expr.len()` 定位聚合输出列，但
  GROUPING SETS 的聚合 schema 在分组列与聚合列之间还有隐藏的 `__grouping_id` 列 → HAVING 的映射
  整体错位（`HAVING SUM(v)` 被映射到 `avg_v`，导致分组集 HAVING 过滤错误）。改为从 schema **末尾**
  定位聚合输出（`fields.len() - aggr_expr.len()`），对所有 HAVING 路径生效；既有 HAVING 测试全绿。
- **测试基建**：oracle 的随机流可配置（`IVM_ORACLE_SEED`/`IVM_ORACLE_ROUNDS`），每个 oracle 把 tag
  混入种子保证单次运行覆盖不同数据流；自定义种子的稀疏数据（参考结果全程为空）不再触发
  “view stayed empty” 断言（每轮仍与全量重算对照）。默认种子下断言保持不变。
- **覆盖扩展**：GROUPING SETS 增加 CUBE（两键：明细 + 两个单键小计 + 总计）与 HAVING+AVG（阈值
  进出）随机差分 oracle；多轮多种子深跑（`ROUNDS=30`，5 个种子）未再发现其它问题。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）334 个测试通过、0 失败。

### 10.72 APPROX_DISTINCT（PR-66）

- **能力**：`APPROX_DISTINCT(v)`（列或表达式，可带 WHERE/HAVING，含无 GROUP BY 的全局形态）分组维护；
  沿用 recompute 家族。DataFusion 的 HLL sketch 更新与行序无关，因此增量（按受影响分组重算）与全量
  rebuild 的结果**完全一致**（多个种子 oracle 验证；小基数下估计恰好精确）。
- **spec/typed/schema**：`ViewSpec::ApproxDistinct` + `ApproxDistinctView`、`approx_distinct_output_column`
  （`approx_distinct_<v>`/`approx_distinct_value`）、`approx_distinct_mv_schema_for`/`_groups_`（UInt64，
  可空以容纳全 NULL 分组）；`HavingColumns::ApproxDistinct` 映射 HAVING；执行器/分发/导出同步。
- **测试**：analyzer（列/表达式、HAVING 映射、与其它聚合混用拒绝）、`approx_distinct.slt`
  （bootstrap、新值上升、删除下降、过滤定义 rebuild）、随机差分 oracle（多种子）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）337 个测试通过、0 失败。

### 10.73 APPROX_PERCENTILE_CONT（PR-67）

- **能力**：`APPROX_PERCENTILE_CONT(v, p)`（p 为字面量；列或表达式；可带 WHERE/HAVING）分组维护，沿用
  recompute 家族。t-digest 与 HLL 一样与行序无关（探针验证：升序/降序/打乱结果一致；多种子 oracle
  与全量重算逐组一致）。
- **spec/typed/schema**：`ViewSpec::ApproxPercentile` + `ApproxPercentileView`、
  `approx_percentile_output_column`（`approx_percentile_cont_<v>`）、`approx_percentile_mv_schema_for`/
  `_groups_`（Float64 可空）；analyzer 剥离优化器的 Float64 数值强制转换（与 SUM/AVG 一致，参数保持
  列名）；`HavingColumns::ApproxPercentile` 分别比较取值与百分位字面量；执行器/分发/导出同步。
- **测试**：analyzer（字面量校验、HAVING 映射、非字面量与被混用拒绝）、`approx_percentile.slt`
  （bootstrap、大值上移中位数、过滤定义 rebuild）、随机差分 oracle（多种子）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）340 个测试通过、0 失败。

### 10.74 SQL 函数补全：标量聚合族 + GROUPING()（PR-68）

- **确定性标量聚合族**（通用 spec `ViewSpec::ComputedAgg` + `ComputedAggView`，参数为「列/表达式」对，
  可带百分位字面量；结果类型 `Value`/`Float64`/`UInt64`）：
  - `BIT_AND`/`BIT_OR`/`BIT_XOR(v)`（结果同首参类型）；
  - `CORR`/`COVAR_SAMP`/`COVAR_POP(y, x)`、`REGR_SLOPE`/`REGR_INTERCEPT`/`REGR_COUNT`/`REGR_R2`/
    `REGR_AVGX`/`REGR_AVGY`/`REGR_SXX`/`REGR_SYY`/`REGR_SXY(y, x)`（Float64，`REGR_COUNT` 为 UInt64）；
  - `PERCENTILE_CONT(v, p)`（精确分位）、`APPROX_MEDIAN(v)`、`APPROX_PERCENTILE_CONT_WITH_WEIGHT(v, w, p)`。
  维护沿用 recompute 家族（按受影响分组重算），列名 `<function>_<参数标签...>`；HAVING 由
  `HavingColumns::ComputedAgg` 逐参数匹配；执行器 schema/分发同步。
- **GROUPING()**：`GROUPING(key)` 被优化器改写为隐藏列 `__grouping_id` 上的按位表达式；分组集
  analyzer 识别单键掩码形态并物化为 **每集合常量列**（集合包含该键→0，聚合掉→1），列名取显式别名或
  `grouping_<key>`；`Projection→Aggregate` 的纯列检查对分组集投影放宽；多键/复合 `GROUPING()` 仍明确拒绝。
- **明确拒绝**：`ANY_VALUE`、聚合形态的 `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE` 等顺序相关函数仍按
  “aggregate function …” 报错（不静默）。
- **测试**：analyzer（18 个函数 + 列名 + HAVING + 非字面量百分位/混用拒绝；GROUPING 正反向）、
  `bit_agg.slt`、`grouping_sets_grouping.slt`（明细/总计与 GROUPING 标记、增删改、重建）、表驱动 oracle
  （18 个函数对全量重算，逐组一致）、GROUPING oracle。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）345 个测试通过、0 失败。

### 10.75 宽投影 join（每侧多列 payload）（PR-69）

- **spec/typed 扩展**：`JoinSide`/`JoinOutputColumn { side, column, name }`；`ViewSpec::Join` /
  `ViewSpec::CrossJoin`、`JoinView`/`CrossJoinView` 新增 `output_columns`（空 = 既有紧凑形态
  `left_value`/`right_value`，完全向后兼容）；`PairJoin` 增加 `output_columns`（Left/Full 传空）。
- **投影**：宽形态下中间列为 `__left_<name>`/`__right_<name>`，最终输出按选择顺序物化为 `name`
  （取 SELECT 别名，否则源列名）；紧凑形态逐字节不变。键位对齐（`keyed_join_output_columns`）、
  schema（`wide_join_view_schema_for` / `wide_keyed_join_view_schema_for`）、校验
  （`wide_output_fields`、`validate_wide_pair_filter`）与执行器期望 schema 同步；DELETE/重放/rebuild
  逻辑不感知 payload，无需改动。
- **analyzer**：`join_payloads` 统一分类 SELECT 列表（跳过 join 键、解析侧别、别名做列名），
  单侧 1 列走紧凑、其余走宽；投影被优化器裁剪时（`Projection` 消失）用 `DFSchema` 限定字段名
  重建（`join_payloads_from_fields`，内连接因此也支持该形态）；每侧至少一列、列名唯一（运行期
  再兜底），宽形态的非等值条件渲染为 `"__left_x" < "__right_y"`（`render_wide_pair_conditions`）。
- **范围**：内连接（keyed + append-only）与 CROSS JOIN 支持宽投影；LEFT/FULL/RIGHT/lookup 暂保持
  紧凑形态（明确报“one payload column”类错误）。
- **测试**：analyzer（宽内连接列名/顺序/pair filter、宽 cross、每侧缺列拒绝）、`wide_join.slt` 与
  `cross_join_wide.slt`（增删改与重建）、`generic_join_window` 追加 append-only 宽 join（三个
  inclusion-exclusion 项各覆盖）、`oracle_wide_inner_join_matches_full_recompute`（带侧过滤的逐轮差分）。
  README 形状表/限制清单同步。全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）351 个测试通过、0 失败。

### 10.76 三表及以上 join（多路内连接，PR-70）

- **扁平化而非中间表**：优化器对多表 join 统一输出左深/任意树（括号写法也会重排），
  analyzer 后序展平为线性链 `((S0 ⋈ S1) ⋈ S2)`；每个等值键对连接两个源且左端在链中靠前
  （`MultiJoinKey`），跨源非等值条件（`MultiJoinCondition`）与单源谓词（并入该源侧过滤）
  分开收集；中间裁剪 Projection/Filter 均被穿透，别名可在链路任意位置（`join_input` 重构）。
- **新 spec `ViewSpec::MultiJoin` + `MultiJoinView`**：`sources`（表 + 侧过滤）、`keys`、`columns`
  （输出列 = 源下标 + 列 + 物化名）、`conditions`；MV 以**每个源的行标识**为主键
  （`__pk<i>_<key>`），宽输出直接复用 10.75 的列命名；schema helper
  （`multi_join_mv_schema_for` / `multi_join_append_schema_for` / `multi_join_primary_keys`）与
  执行器期望 schema 同步。键/条件渲染为中间列 `__c<i>_<col>`，投影与连接在
  `multi_join_frame` 内一次完成。
- **刷新**：keyed 路径取各源 changed 行（current ⋈semi delta），对每个变化源做一次
  「该源 changed × 其余 current」的 N 路连接并去重，删除按各源受影响标识反连接该源的 MV 元组；
  append-only 路径只对**本窗口变化的源集合**枚举非空子集（2^c-1 项），
  子集取 Δ、补集取 before，逐项连接后 union（项间天然不重叠）。重建路径同构并带 epoch/rebuild 状态机。
- **范围**：3–4 个源、内连接（树中出现 LEFT/RIGHT/FULL 明确拒绝）、全 keyed 或全 append-only、
  同/异名键、侧过滤、跨源条件；明确拒绝 >4 源。
- **测试**：analyzer（键/条件/侧过滤与左连接拒绝）、`multi_join.slt`（三源 keyed 增删改与重建）、
  `multi_join_append.slt`（append-only 三类 inclusion-exclusion 覆盖 + 带过滤重建）、
  `oracle_multi_join_matches_full_recompute`（三源 keyed 随机轮差分）。
  README 形状表/限制清单同步。全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）355 个测试通过、0 失败。

### 10.77 outer/lookup join 的宽投影（PR-71）

- **spec/typed 扩展**：`ViewSpec::LeftJoin`/`FullJoin`/`LookupJoin`、`LeftJoinView`/`FullJoinView`/
  `LookupJoinView` 增加 `output_columns`（serde 省略，既有紧凑 spec/定义哈希不变）；
  LEFT/FULL 与 inner 共用 `keyed_join_projection`（`PairJoin` 透传），lookup 的
  `lookup_join_projection`/`lookup_join_output_columns` 增加宽分支。
- **schema nullability**：`wide_output_fields_with` 支持按侧强制可空；新增
  `wide_outer_join_view_schema_for`（键取左源可空性、输出与两侧标识全部可空）与
  `wide_lookup_join_view_schema_for`（左列保持源可空性、右列强制可空、左标识非空）。
- **analyzer**：`analyze_outer_join`（LEFT/RIGHT/lookup）与 FULL 分支改用统一的
  `join_payloads` + `compact_or_wide`（单侧 1 列走紧凑、其余走宽、每侧至少一列）；执行器
  期望 schema 四条 outer/lookup 分支按 `output_columns` 选择宽/紧凑 helper；校验
  （`validate_left_join_view`/`validate_lookup_join_view`，FULL 经克隆委托）增加宽形态列校验。
- **范围**：LEFT/RIGHT/FULL/lookup 的宽输出（RIGHT 仍按「保留侧在左」的 lookup/pair-LEFT 处理）；
  外连接的侧过滤支持范围不变（FULL 仍不接受输入过滤）。
- **测试**：analyzer（lookup/full/pair-LEFT 宽形态与紧凑回退）、`wide_lookup_join.slt`（NULL 补位、
  维度增删改、事实键迁移、过滤重建）、`wide_full_join.slt`（双侧未匹配与补位）、
  `oracle_wide_left_join_matches_full_recompute`（pair-keyed 宽 LEFT 随机轮差分）。
  README 形状表/限制清单同步。全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）359 个测试通过、0 失败。

### 10.78 outer pair join 的异名键与输入过滤（PR-72）

- **异名键**：`ViewSpec::LeftJoin`/`FullJoin` 与 typed view 新增 `right_keys`（serde 省略）；
  `parts()` 透传给 `keyed_join_projection`（沿用 `effective_right_keys`）。FULL 刷新里
  「变化键 × 另一侧当前态」的半连接改为左键名/右键名分别解析（`right_join_key_names`），
  不再假定两侧同名。analyzer 移除「异名键仅限 inner/lookup」限制，pair-keyed LEFT/RIGHT/FULL
  均支持；校验器按并行键对校验存在性与类型。
- **输入过滤**：`ViewSpec::FullJoin`/`FullJoinView` 新增 `left_filter`/`right_filter`，
  刷新与重建对 current/delta/baseline 帧统一叠加 `apply_side_filter`；analyzer 移除 FULL 的
  「join input with a WHERE」拒绝（derived table / 下推过滤两侧均可）。剩余拒绝：
  lookup LEFT 的右侧过滤。
- **侧别感知的 payload 分类**：`join_payloads`/`join_payloads_from_fields` 改为按左/右键列表
  分别跳过（此前按合并列表跳名，异名键时会把另一侧同名 payload 误判为键）；outer/FULL 的
  裁剪投影（`Projection` 被优化器移除）也走 `join_payloads_from_fields` 兜底。
- **测试**：analyzer（FULL 异名键+双侧 derived 过滤、移除过时拒绝断言）、
  `full_join_names_filter.slt`（异名键 FULL 的增删与带过滤重建）。README 形状表/限制清单同步。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）360 个测试通过、0 失败。

### 10.79 lookup LEFT JOIN 的右侧输入过滤（PR-73）

- **spec/typed**：`ViewSpec::LookupJoin`/`LookupJoinView` 新增 `right_filter`（serde 省略）与
  `with_right_filter`；`to_spec`/`spec_view`/执行器路径同步。
- **运行时**：`refresh_lookup_join`/`rebuild_lookup_join` 的 `right_now` 经 `apply_side_filter`
  过滤；受影响键仍取自**未过滤**的 delta（进入/离开过滤区都会重写引用它的左侧行），
  重写投影使用过滤后的 `right_now`，因此匹配被过滤掉的左侧行保持 NULL。校验器解析过滤表达式。
- **analyzer**：移除 lookup 右侧过滤的拒绝，改由 `join_input` 收集并透传；至此所有 join 形态的
  输入过滤（inner/cross/semi-anti/LEFT/RIGHT/FULL/lookup，任一侧）均已支持，README 限制清单
  对应条目删除。
- **测试**：analyzer（lookup 右侧 derived table 过滤）、`lookup_join_right_filter.slt`
  （维度行进入/离开过滤区导致 NULL 填充/补齐、新事实行、过滤变化重建）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）361 个测试通过、0 失败。

### 10.80 多路 join 的无键（CROSS）步骤与源数上限（PR-74）

- **无键步骤**：多路链中某个源没有键对即视为 CROSS 步骤——`multi_join_frame` 用
  `LogicalPlanBuilder::cross_join` 连接累积帧与该源（`FROM a, b, c` 与 `a JOIN b ON ... , c`
  混合形状均可）；校验器移除「每源至少一个键对」「至少一个键」两条约束（纯交叉链合法）。
- **源数上限**：4 → 8（append-only 刷新只对**本窗口变化**的源枚举子集，平均窗口仍是 ~2 项）。
- **正确性**：keyed 刷新的受影响集合以各源行标识为基准，与是否存在等值键无关；append-only
  子集分解同理，因此交叉步骤无需特殊处理。
- **测试**：analyzer（纯交叉链与混合链）、`multi_cross_join.slt`（三源交叉的增删改）、
  `oracle_multi_cross_join_matches_full_recompute`（随机轮差分）。README 形状表/限制清单与
  §10.5 backlog 同步刷新。全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）364 个测试通过、0 失败。

### 10.81 同一语句混合多种聚合 kind（PR-75）

- **触发**：`analyze_aggregate` 取**选择列表引用**的聚合（裁剪投影为 None 时即全部），
  若 >1 且不是「至多一个 SUM/COUNT/AVG」的组合则路由到 `analyze_multi_agg`；
  HAVING 额外引入的计划聚合仍由原 family 路径处理，保持「HAVING 必须引用已物化聚合」的报错。
  混合语句中的 DISTINCT 聚合（优化器会改写成嵌套分组）在 `distinct_split` 之前明确拒绝。
- **spec/typed**：`ViewSpec::MultiAgg` + `MultiAggSpec { call, column, result }`（result 为可移植类型编码
  `encode_data_type`/`decode_data_type`，覆盖数值/字符串/时间/Decimal/List）+ `MultiAggView`
  （`aggregates: Vec<(call, column, DataType)>`）；schema helper `multi_agg_mv_schema_for`
  支持分组与全局（空键）两种形态；执行器期望 schema 同步。
- **维护**：复用 recompute 家族——`RecomputeParts` 增加 `extra_aggregates`，三个 SQL 构造器
  （group_now/refresh/rebuild/global）改为多列列表；`refresh_recomputed`/`rebuild_recomputed`
  未改动即可支持 N 个聚合（受影响分组重算 + 删除/插入）。
- **列命名**：`<函数>_<参数标签>`（如 `sum_v`、`min_v`、`bit_and_v`），`COUNT(*)` 物化为 `count`；
  重名明确报错。HAVING 通过「输出字段名/表达式显示/渲染调用」三重映射到 MV 列。
- **支持集**：SUM/COUNT/AVG/MIN/MAX/方差族/MEDIAN/APPROX_*/STRING_AGG/ARRAY_AGG/BOOL_*/分位/位与回归族；
  FILTER、多参数 DISTINCT 明确拒绝。
- **测试**：analyzer（SUM+MIN+COUNT+HAVING、MIN+MAX+MEDIAN、单族回退、DISTINCT/FILTER 拒绝；
  更新 5 处旧的“混合即拒绝”断言为 MultiAgg）、`mixed_aggregates.slt`（键组增删改、组消失、
  HAVING 重建）、`mixed_global_aggregates.slt`（全局混合）、
  `oracle_mixed_aggregates_matches_full_recompute`（随机轮差分）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）368 个测试通过、0 失败。

### 10.82 runtime.rs 按算子模块化拆分（PR-76）

- **动机**：`src/runtime.rs` 已达 ~1.8 万行，单文件难以导航与评审。按 SQL 算子拆分为
  `src/runtime/` 子模块，公共 API（`lib.rs` 的 `pub use`、`IvmRuntime` 方法与视图类型）保持不变。
- **模块划分**（新增 `pub use` 汇聚到 `runtime`，外部路径不变）：
  - `runtime.rs`（父，~3.0k）：`IvmRuntime` 核心（建表/打开/epoch/窗口/游标/baseline）、
    `ViewSpec`/`SpecView`、view_id/kind、`spec_view`、refresh/rebuild 分发、编码与共享助手
    （引号/过滤/键/分组/`affected_groups_sql`/`dataframe` 等）。
  - `runtime/aggregates.rs`（~2.9k）：SUM/COUNT/AVG、MIN/MAX、DISTINCT 聚合、value-count 状态、
    GROUPING SETS/ROLLUP/CUBE。
  - `runtime/recompute.rs`（~2.7k）：recompute 家族（方差/中位数/布尔/近似/字符串/数组/计算聚合/混合聚合）
    与 `RecomputeParts` 机制。
  - `runtime/joins.rs`（~4.0k）：pair join（inner/lookup LEFT/pair-keyed LEFT/RIGHT/FULL/CROSS + 宽投影）。
  - `runtime/multi_join.rs`（~0.9k）：多路链（3–8 源、交叉步骤）。
  - `runtime/semi_anti.rs`（~0.8k）：EXISTS/IN 与 INTERSECT/EXCEPT 子集。
  - `runtime/windows.rs`（~2.1k）：窗口/链式窗口/TOP-K。
  - `runtime/rows.rs`（~0.5k）：行投影；`runtime/unions.rs`（~1.1k）：UNION ALL/UNION。
- **可见性**：子模块 `use super::*;` 继承父模块私有助手；跨模块共享项（`RecomputeParts` 及字段、
  `validate_recompute_view`、`refresh_recomputed`/`rebuild_recomputed`、`apply_pair_filter`、
  `wide_pair_alias`）标注 `pub(super)`/`pub(crate)`；父模块以 `pub use self::<mod>::*;` 重导出，
  保持 `crate::runtime::X` 与 `lib.rs` 公共导出路径不变。
- **验证**：顶层条目名集合与拆分前完全一致（无遗漏/新增）；fmt、clippy 干净；
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）368 个测试通过、0 失败。

### 10.83 sql.rs 按算子模块化拆分（PR-77）

- **动机**：`src/sql.rs` 9.4k 行（其中 4.2k 为单测），按 SQL 算子拆分到 `src/sql/` 子模块；
  `analyze_select` 等公共入口保持在父模块，外部路径不变。
- **模块划分**：
  - `sql.rs`（父，~0.9k）：`AnalyzeRequest`/`AnalyzedView`、`analyze_select` 分发、
    `definition_hash`、过滤与表达式共享助手（`render_filter`/`strip_relations`/`validate_filter`、
    `column_of`/`strip_alias`/`projection_alias`、`collect_filtered_source`、`having_aggregate`、
    `compare_op` 等）与入口类测试。
  - `sql/aggregates.rs`（~3.9k）：`analyze_aggregate`、`HAVING` 渲染/重写、`GROUPING SETS`、
    混合聚合，以及 29 个聚合单测。
  - `sql/joins.rs`（~1.9k）：pair join（inner/lookup/pair-keyed left/right/full/cross、宽投影、
    pair 谓词）+ semi/anti 分支与 19 个 join 单测。
  - `sql/multi_join.rs`（~0.4k）：join 树展平（后序、无键 CROSS 步骤、侧过滤/条件分类）。
  - `sql/set_ops.rs`（~0.2k）：INTERSECT/EXCEPT；`sql/unions.rs`（~0.5k）：UNION ALL/UNION。
  - `sql/windows.rs`（~1.3k）：窗口/链式窗口/TOP-K 与序号渲染；`sql/rows.rs`（~0.2k）：行投影/
    SELECT DISTINCT。
  - `sql/test_helpers.rs`（~0.1k，`#[cfg(test)] pub(crate) mod`）：`schema`/`source_table`/`plan`/
    `analyze*`/`normalized` 等共享 fixture，各模块的单测子模块通过 `use crate::sql::test_helpers::*;`
    复用（原 94 个测试按算子随模块迁移）。
- **可见性**：父模块以私有 `use self::<mod>::*;` 汇聚子模块；跨模块调用的分析函数
  （`analyze_aggregate`/`analyze_join`/`analyze_multi_join`/`analyze_set_operation`/`analyze_union*`/
  `analyze_window`/`try_analyze_top_k`/`analyze_row`/`analyze_distinct_rows`/`is_grouping_projection`/
  `join_input`+`JoinInput` 字段/`semi_anti_output_columns`/`render_order_key`/`render_order_expr`）
  标注 `pub(super)`。
- **验证**：条目名集合与拆分前完全一致；fmt/clippy 干净；全量 IVM 套件
  （lib + 39 个集成测试二进制 + doctest）368 个测试通过、0 失败。

### 10.84 `DISTINCT ON`（PR-78）

- **计划形态**：优化器把 `SELECT DISTINCT ON (keys) ... [ORDER BY ...]` 规划为
  `Aggregate(groupBy=keys, aggr=[first_value(col) ORDER BY ...])`（带 ORDER BY 时上方还有一个输出
  `Sort`；无 ORDER BY 时为裸 `first_value`）。因此复用 10.81 的 **MultiAgg（按受影响分组重算）**
  机制，无需新的 spec。
- **确定性**：analyzer 渲染 `first_value(arg order by <用户排序>, <源主键 asc>)`——主键仅用于
  并列打破（与 TOP-K 运行时追加主键一致），保证增量与全量重算结果一致；**仅支持有主键的源**，
  append-only 源明确拒绝；`FIRST_VALUE` 参数为分组键的聚合被跳过（其值即键），全为键时明确拒绝
  并提示改用 `SELECT DISTINCT`。
- **入口**：`analyze_select` 新增 `Projection -> Sort -> Aggregate(first_value…)` 分支（仅在该形态下
  接受顶层 Sort；普通聚合上的 ORDER BY 仍按原样拒绝）。
- **支持集**：`multi_agg_supported` 增加 `first_value`，聚合内 ORDER BY 允许列表加入 `FIRST_VALUE`。
- **测试**：analyzer（带/不带 ORDER BY、主键并列、多列、键-only 拒绝、append-only 拒绝、
  普通 ORDER BY 仍拒绝；更新一处旧的“DISTINCT ON 拒绝”断言）、`distinct_on.slt`（选中行随更新/
  删除/新增迁移、过滤重建重建）、`oracle_distinct_on_matches_full_recompute`（随机轮差分）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）371 个测试通过、0 失败。

### 10.85 `GROUPING SETS`/`ROLLUP`/`CUBE` 支持任意聚合组合（PR-79）

- **spec/typed**：`ViewSpec::GroupingSets` 与 `GroupingSetsView` 增加 `aggregates: Vec<MultiAggSpec>`（serde
  省略）；空 = 既有 SUM/COUNT/AVG 增量布局（完全兼容），非空 = 每集合重算的通用聚合列表
  （列名规则与 MultiAgg 一致：`<函数>_<参数>`）。
- **analyzer**：`analyze_grouping_sets` 先判断「全部为非 DISTINCT/FILTER/ORDER BY 的 SUM/COUNT/AVG」→ 走
  原增量路径；否则复用从 `analyze_multi_agg` 抽出的 `parse_multi_aggregates` 构建通用列表，
  HAVING 经 `render_multi_agg_having` 映射；`GROUPING()` 列解析抽为
  `grouping_set_projection_columns` 两条路径共用。聚合结果类型改从 aggregate schema **末尾**定位
  （ROLLUP 的隐藏 `__grouping_id` 位于键与聚合之间）。
- **运行时**：`grouping_sets_columns`/`group_now`/`rebuild_sql` 按 `aggregates` 是否为空分支
  （删除/插入统一走 `grouping_sets_columns`，故只需这三处）；schema helper
  `grouping_sets_mv_schema_for` 增加 `aggregates` 参数；校验器校验列名唯一；执行器解码类型。
- **测试**：analyzer（ROLLUP 混合聚合 + HAVING、SUM/COUNT/AVG 回退、DISTINCT 拒绝；更新一处旧断言）、
  `grouping_sets_mixed_aggregates.slt`（更新/删除整组/新增组/过滤重建）、
  `oracle_grouping_sets_mixed_aggregates_matches_full_recompute`（随机轮差分）。
- **注**：修复过程中曾误改既有 `sqllogic_grouping_sets_multi` 注册与 slt（已恢复）；新增 slt 命名为
  `grouping_sets_mixed_aggregates`。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）374 个测试通过、0 失败。

### 10.86 投影/过滤视图支持非相关标量子查询（PR-80）

- **能力**：`SELECT ... FROM src WHERE v > (SELECT AVG(v) FROM dim)` —— 过滤器中的
  非相关标量子查询（全局聚合、无 `GROUP BY`），比较可为 `=, <>, <, <=, >, >=`
  任一侧、可与普通谓词 AND 组合；外层源需 keyed。
- **analyzer**：`collect_row` 增加 scalar 累积器；新 `render_scalar_filter` 按合取项拆分，
  标量项 -> `(子查询 SQL) <op> <外层表达式>`（子查询经 `plan_to_sql` 渲染为自包含 SQL、
  `render_scalar_subquery` 收集其表到 `RowScalarSpec`，拒绝相关子查询 / `GROUP BY` /
  非聚合子查询）；select-list 标量子查询与 UNION 分支内的标量子查询明确报错。
  spec/typed：`ViewSpec::Row.scalar` + `RowScalarView`（SQL 名 + 表）。
- **运行时**：`refresh_row`/`rebuild_row` 把子查询表按 SQL 名注册为 MemTable
  （`read_current` + `filter_deletes`），并把它们的游标与源一起收集/推进；`scalar_changed`
  时 `affected` 取「当前源键 ∪ MV 键」以重算全部键（源仍走 delta 路径）。过滤器解析改为
  `parse_scalar_filter`：把源临时注册后 `create_logical_plan("select * from <tmp> where <filter>")`
  并取出 Filter 谓词（`create_logical_expr` 的空 table map 无法解析子查询表），再剥离关系限定名
  （`Expr::ScalarSubquery` 在 TreeNode 中是叶子，子查询计划不受影响）。
- **测试**：analyzer（两侧比较、AND 组合、四类拒绝）、`scalar_subquery.slt`
  （bootstrap、子查询表增删驱动全量重算、源更新走 delta、阈值下降后回插）、
  `oracle_scalar_subquery_matches_full_recompute`（双源随机轮差分，另跑 seed 3/17/91 × 25 轮）。
  全量 IVM 套件（lib + 39 个集成测试二进制 + doctest）378 个测试通过、0 失败。

### 10.87 `GROUPING SETS`/`ROLLUP`/`CUBE` 支持键表达式与普通 append-only 源（PR-81）

- **键表达式**：`GROUP BY ROLLUP(g, v % 10 AS bucket)` —— 每个集合项可为普通列或带别名的表达式；
  分析器按集合项复用名字/表达式约定（普通列沿用列名，表达式取 select 别名并以 `render_filter`
  渲染，`group_keys == group_exprs` 时压缩为空以保持既有 spec 不变）；未起别名的表达式明确报错。
- **运行时**：`group_key_fields` 放宽为「表达式等于键名且键名是源列」的普通列沿用源字段
  （混合键中普通列不再被误判为复用列名），其余命名冲突仍然拒绝；`refresh_grouping_sets` /
  `rebuild_grouping_sets` 用 `project_group_keys` 投影 delta/old/src 后再注册，使 SQL 能引用
  计算键。
- **append-only 源**：`affected` 按源是否有主键选择 `affected_groups_sql(keyed)`；普通
  append-only（无 change 列）可维护；带删除/更新标记的 append-only changelog 无法在被重算的
  当前态里回收旧行，因此分析器与校验器都**明确拒绝**（错误信息注明 cannot retract）。
- **测试**：analyzer（ROLLUP 单计算键、混合 `GROUPING SETS ((g, bucket), (g), ())`、重命名普通列、
  未别名报错、append-only changelog 拒绝；更新一处旧断言）、`grouping_sets_expr.slt`
  （混合键 ROLLUP 的更新/整组删除/新组）、`grouping_sets_append.slt`（普通 append-only + 计算键
  CUBE 的追加/过滤重建）、`oracle_grouping_sets_expression_keys_matches_full_recompute`
  （另跑 seed 5/23/77 × 25 轮）。全量 IVM 套件 382 个测试通过、0 失败。

### 10.88 `INTERSECT`/`EXCEPT` 支持可空键（空安全匹配，PR-82）

- **能力**：集合操作/空安全半连接中的连接列可以为空 —— `NULL` 与 `NULL` 匹配，与 SQL 语义一致；
  左侧行在连接列上仍须唯一（主键覆盖），否则明确拒绝。
- **实现**：`ViewSpec::SemiAnti` / `SemiAntiView` 增加 `null_safe`（serde 省略 false，非空键的定义哈希
  不变）；`semi_anti_join` 在 `null_safe` 时改用
  `LogicalPlanBuilder::join_detailed(..., NullEquality::NullEqualsNull)`，其余键连接（左侧主键）仍为
  普通等值；分析器按两侧字段可空性设置 `null_safe` 并移除原来的可空键拒绝。
- **测试**：analyzer（可空键 → `null_safe = true`，非空键保持 false；删除旧的拒绝断言）、
  `set_ops_nullable.slt`（NULL 命中/失配/再命中、EXCEPT 的 NULL 匹配、右侧删除）、
  `oracle_intersect_nullable_matches_full_recompute`（nullable 值 + NULL 种子，另跑 seed 11/42/99 × 25 轮）。
  全量 IVM 套件 384 个测试通过、0 失败。

### 10.89 相关标量子查询（WHERE 比较，聚合右输入，PR-83）

- **形状**：`WHERE v OP (SELECT AGG(w) FROM dim u WHERE u.k = s.k)` 规划为
  `LeftSemi Join(s.k = __scalar_sq_1.k, Filter: <比较>)`，右侧是
  `SubqueryAlias -> Projection -> Aggregate(groupBy=相关键)`。
- **spec/typed**：`ViewSpec::SemiAnti` / `SemiAntiView` 增加 `right_aggregate`（渲染后的聚合调用）、
  `right_keys`（与 `join_keys` 对齐的右侧分组键）与 `match_predicate`（把聚合输出改名为
  `__ivm_right_agg` 后的比较谓词）；均为 serde 省略，非相关子查询的定义哈希不变。
- **analyzer**：`analyze_join` 在派发前识别「右侧为聚合」的连接；`LeftSemi` 走
  `analyze_correlated_scalar`（校验单个聚合、相关键与分组一致、列单输出、连接键为普通列，
  渲染聚合调用与比较谓词；聚合输出按子查询投影里非键字段识别），其他连接类型与
  选择列表相关子查询、`DISTINCT`/多聚合重写明确拒绝。
- **运行时**：`semi_aggregate_match` 先按相关键对右侧做聚合（输出别名 `__ivm_right_agg`），再以
  半连接 + 过滤谓词求匹配；受影响键取「左 delta 键 ∪ 右侧变化键」（`right_aggregate` 模式对
  append-only 右侧用 delta 键，对 keyed 右侧用主键回查旧键）；投影列集合补上聚合参数列与
  谓词左列；校验器解析聚合与谓词（类型由逻辑推断）。
- **测试**：analyzer（基本形状、异名键 + 双侧表达式 + 子查询过滤、选择列表与 DISTINCT 拒绝）、
  `correlated_scalar.slt`（bootstrap、无匹配键=NULL、平均值升降、源更新/删除、子查询过滤重建）、
  `oracle_correlated_scalar_matches_full_recompute`（keyed 双源 + 种子，另跑 seed 7/31/64 × 30 轮）。
  全量 IVM 套件 387 个测试通过、0 失败。

### 10.90 选择列表相关标量子查询（左连接聚合，PR-84）

- **形状**：`SELECT s.k, ..., (SELECT AGG(u.v) FROM dim u WHERE u.k = s.k) AS m FROM src s` 规划为
  `Projection(s.k, __scalar_sq_1.agg AS m) -> Left Join(s.k = __scalar_sq_1.k) -> 聚合右输入`。
- **spec/typed**：新增 `ViewSpec::LeftAggregate` 与 `LeftAggregateView`（`join_keys`/`right_keys`/
  `right_aggregate`/`aggregate_column`/`output_columns`/两侧过滤），MV schema
  `left_aggregate_mv_schema_for`（左输出列 + 聚合列 + rowKinds + epoch）。
- **analyzer**：聚合右输入的派发按连接类型分支 —— `LeftSemi` 走相关比较（§10.89），`Left` 走
  `analyze_left_aggregate`：要求无残余条件、单个聚合、相关键 == 分组键、外层投影为普通左列 +
  恰好一次标量值（需要别名或沿用列名）、左主键保留在输出列中；计算表达式紧邻标量值明确拒绝。
- **运行时**：`refresh_left_aggregate` 以「左 delta 主键 ∪ 右侧变化键命中的左行」为受影响集合，
  先按相关键聚合右侧（键列加 `__ivm_right_` 前缀别名避免与左侧同名列冲突、聚合别名
  `__ivm_right_agg`），再以左连接取值（无匹配键为 NULL），最后 delete+insert 重写受影响主键；
  `rebuild_left_aggregate` 从两侧当前态整体重建。
- **测试**：analyzer（形状字段断言、选择列表从旧拒绝改为正例、计算表达式/DISTINCT 拒绝）、
  `left_aggregate.slt`（NULL 值、键获得/失去行、源更新/删除、子查询过滤重建）、
  `oracle_left_aggregate_matches_full_recompute`（种子双源，另跑 seed 9/44/82 × 30 轮）。
  全量 IVM 套件 389 个测试通过、0 失败。

### 10.91 多路链中的外连接：现状与设计（PR-85）

- **现状**：三表以上的扁平链只支持 inner/cross；链中出现 `LEFT`/`RIGHT`/`FULL` 步骤时分析器
  **明确拒绝**（错误信息说明「链式外连接未维护，可用两表外连接或逐级 lookup 视图」），
  不会静默产出与全量重算不同的结果。本 PR 补上该拒绝的专门测试与设计记录。
- **设计（下一项实现，暂名 `LookupChain`）**：
  - **形状**：左深链 `base [LEFT|INNER] JOIN s1 ON s1.<k> = base.<k> [LEFT|INNER] JOIN s2 ...`，
    所有源 keyed，每个右表以连接键为其唯一键（1:1 lookup，与两表 LookupJoin 的既有约束一致）；
    步骤键只引用**基表**列（星型/维表 lookup 的常见形状），先做这一子集，其余形状继续拒绝。
  - **MV**：基表输出列 + 各步骤 payload（外表步骤可空）+ `rowKinds`/`__ivm_epoch`；
    主键 = 基表主键（1:1 下每条基表行至多一行）。
  - **刷新**：受影响基表主键 = 基表 delta 主键 ∪「各变化源的新旧键值经基表列半连接得到的
    基表主键」（右侧变化取 `delta ∪ as-of(pks)` 的键值，覆盖键变更与删除）；对受影响行按当前
    各源做 `base LEFT/INNER JOIN s1 ...` 重算，delete+insert 重写。
  - **重建**：从各源当前态整体按链左连接重算。
  - **后续扩展**：右侧一对多（非 1:1）的外连接、步骤键引用前序非基表列、与 inner 步骤混合的
    链式副作用、computed 输出列。
- **验证计划**：analyzer 形状/拒绝矩阵、`lookup_chain.slt`（维表插入/更新/删除、基表更新与删除、
    多步 payload、空 payload 的 NULL 语义）、差分 oracle（种子随机轮）。

### 10.92 `LookupChain`：多路链中的外连接（keyed 1:1 星型链，PR-86）

- **能力**：`a LEFT JOIN b ON b.k = a.k LEFT JOIN c ON c.k = a.k`（左深、步骤为 `INNER`/`LEFT`、
  每步右表以连接键为唯一键、步骤键引用基表列、可选步骤 filter）；每条基表行一行，缺失步骤的
  payload 为 NULL。
- **spec/typed**：`ViewSpec::LookupChain`（`sources`（含 filter）/`steps`（`left`、`keys`、
  `right_keys`）/`output_columns`（source/column/name））+ `LookupChainView`；MV schema
  `lookup_chain_mv_schema_for`（步骤列在其前缀含 LEFT 时可空）。
- **analyzer**：`join_tree_has_outer_step` 把含外步骤的多源链路由到 `analyze_lookup_chain`：
  左深收集步骤、同名列连接、基表键校验、右表主键 == 连接键、bushy/right/full/非基表键/残余条件
  明确拒绝；输出列按关系名解析来源（未限定名要求唯一）。
- **运行时**：受影响基表主键 = 基表 delta 主键 ∪「每个变化步骤的 `delta ∪ as-of(主键)` 键值
  经基表半连接得到的基表主键」；随后按当前各源重放链（步骤列加 `__ivm_chain_<i>_*` 别名避免
  冲突、LEFT 步骤 NULL 补位）并 delete+insert 重写；重建时从各源当前态整体重放。
- **测试**：analyzer（形状字段、right/full/异名键/非基表键拒绝）、`lookup_chain.slt`
  （维表插入/更新/删除、基表更新/删除、多步独立变化、过滤源重建）、
  `oracle_lookup_chain_matches_full_recompute`（三源种子，另跑 seed 13/57/88 × 30 轮）。
  全量 IVM 套件 392 个测试通过、0 失败。

### 10.93 `LookupChain` 步骤键引用前序源（PR-87）

- **能力**：步骤键可引用任意**更早**源的列（不限基表），如
  `a LEFT JOIN b ON b.k = a.k LEFT JOIN c ON c.v = b.v`（`c` 由 `b` 产出的值查找）；
  键名不再要求同名（只要求类型一致）。
- **spec**：`LookupChainStep` 增加 `key_sources`（每个键的来源源序号，空 = 全基表），serde 省略。
- **运行时**：重放时左侧键用 `__ivm_chain_<i>_<col>`（或基表列名）解析到前序源列；受影响映射分两条路径 ——
  若该步骤的任一前序源（含基表）在本窗口也变化，保守地把「基表当前键 ∪ MV 键 ∪ 基表 delta 键」全部重算；
  否则用**前缀重放**（当前态、前序不变）把变化键经 `__ivm_changed_<k>` 别名半连接到基表主键。
  步骤投影与重放帧额外带上「被后续步骤键引用的列」。
- **测试**：analyzer（`c.k = b.k` 的 `key_sources == [1]`）、`lookup_chain.slt` 改为
  `__DIM2__` 以 `v` 为主键并执行 `c.v = b.v`（中间步骤取值变化导致末步失配、基表与步骤同窗口变化、
  末步删除、过滤源重建）、oracle 第三步改为 `c.k = b.k`（seed 21/66/5 × 30 轮，并修掉
  「保守路径丢失已删除基表键」的缺陷）。全量 IVM 套件 392 个测试通过、0 失败。

### 10.94 逻辑读去 tombstone 过滤 + `COUNT(*)` 修复（PR-88）

- **背景**：slt/oracle 里所有查询都带 `WHERE "rowKinds" = 'insert'`，而最终用户不应加这种内部列过滤；
  `IvmReadMode::Current`（默认）本就通过 `drop_tombstones` 隐藏 CDC 墓碑行。
- **改动**：删除 122 个 slt 文件中的 541 处与 `sql_oracle.rs` 的 85 处 `WHERE "rowKinds" = 'insert'`
  （concurrency/consumers_gc 各 1 处），让测试走用户视角的逻辑读；
- **顺带修复**：去掉过滤后暴露出 provider 的真实缺陷 —— `SELECT COUNT(*) FROM mv` 触发空投影扫描，
  `project_batches` 在 0 列 schema 上用 `RecordBatch::try_new` 抛
  "must either specify a row count or at least one column"；改为
  `try_new_with_options(..., row_count = Some(batch.num_rows()))` 保留行数。
- **测试**：全量 IVM 套件 392 个测试通过、0 失败；`concurrent_refreshes_converge` 本次不再跳过
  （本地 1.28s 通过）。
- **后续**：CDC 语义一期（append-only CDC × recompute 族的拒绝矩阵）见
  `/home/chenxu/.opencode/plan/ivm-cdc-semantics.md`。

### 10.95 投影/过滤视图跳过 append-only CDC 的墓碑行（PR-89）

- **缺陷**：`refresh_row` 的 append-only 分支直接追加 delta，未过滤 CDC 墓碑行
  （`delete`/`update_before`）——这些标记会被物化成一条新的活跃行（同一内容出现两次）。
- **修复**：追加前套用 `filter_deletes(delta, change_column(source))`；keyed 分支不受影响
  （它只从 delta 取受影响主键），普通 append-only（无 CDC 列）为 no-op。
- **语义**：append-only 源没有行标识，删除标记无法撤回对应的 insert 行（既有文档化限制），
  但「标记不得成为行」是明确的；重新审视 recompute 族对该源的拒绝矩阵列入
  `/home/chenxu/.opencode/plan/ivm-cdc-semantics.md` 的一期工作。
- **测试**：`row_append_cdc.slt`（insert 两行 → delete 一行 → 视图仍两行，删除标记不成行）。
  全量 IVM 套件 393 个测试通过、0 失败。

### 10.96 MV 主键存在性校验（PR-90，ROADMAP §A1）

- **契约**：MV 由用户建表、主键由用户在 `CREATE TABLE` 时声明（对标 Flink `PRIMARY KEY ...
  NOT ENFORCED` + `SinkUpsertMaterializer`：引擎不校验键的语义，只按该键物化）；我们的
  merge-on-read 就是同一角色的读路径放置（删除行是从 MV 按身份列选出的完整行）。
- **缺口与修复**：执行器原先只校验 schema、不看主键，无键 MV 会静默退化为 append 语义。
  现在在执行器（`expected_mv_schema` 旁边）增加校验：键ed 语句（引用的表都有主键）且
  **不是全局聚合**时，目标 MV 必须声明非空主键，否则报错说明「视图的刷新写入删除标记，
  需要 MV 主键作为合并键」。
- **不变量**：不要求主键包含身份列、不校验唯一性（用户负责，已写入 README：典型陷阱是
  多分支 `UNION ALL` 需投影分支列并入主键）；全局聚合（单行整表重写）豁免；引用了
  append-only 源的语句按 ROADMAP §A3 暂不纳入契约、跳过校验。
- **测试**：`sql_executor.rs::rejects_keyless_target_for_keyed_views`（键ed 语句 + 无键 MV
  → 报错；全局聚合无键 MV → 通过；append-only 源 + 无键 MV → 通过）。
  全量 IVM 套件 395 个测试通过、0 失败。

### 10.97 keyed CDC 契约与测试（PR-91，ROADMAP §A2）

- **契约固定**（写入 README「Sources」）：变更列只承认四个标记 `insert`/`update_after`/
  `update_before`/`delete`（其它值按活跃版本处理）；keyed 源按最高版本 merge：
  同窗口配对折叠为新版本、落单的 `update_before` 先撤回直至 `update_after` 到达（可跨窗口）、
  乱序到达时以最后版本为准、**主键变更 = delete(旧) + insert(新)**、同键多个活跃版本取最新
  （写入端对同键保持输入顺序）。
- **测试**：`cdc_update_markers.rs` 新增 `keyed_cdc_contract_corners`，用 `SumCountView`
  覆盖四个角：乱序 pair（撤回胜出，且重建一致）、主键变更（delete+insert 折叠）、
  同键重复活跃行（取最新）、域外 op 值（按活跃版本处理）；既有
  `keyed_update_markers_fold_and_lone_before_retracts` 与 append-only signed 用例保留。
  全量 IVM 套件 396 个测试通过、0 失败。
- **说明**：数据值层面的 op 校验不做（读取热路径代价高）；契约以文档 + 测试固定，
  写入端应按四值约定落盘。
- **后续收敛**（§10.102，PR-96）：实际摄入只产出 `insert`/`update`/`delete`（before 镜像落成
  `delete`），契约已收敛为三标记，本文的 `update_before`/`update_after` 条目仅作历史记录。

### 10.98 非契约源：分析器直接拒绝 append-only 源（PR-92，ROADMAP §A3）

- **契约**：只支持有主键的源；append-only / append-only CDC 源不再支持（不设开关）。
  `analyze_select` 在任何形状分派之前检查语句引用的每张表，缺主键即报错
  （"source table X has no primary key: incremental views need keyed sources"）。
- **连带调整**：
  - 执行器 A1 校验去掉「append-only 语句跳过」的分支（不再可达）；
  - 删除 8 个 SQL 级 append-only 用例与其 slt（sum_expr_append、count_column_append、
    having_append、union_distinct_append、union_append_where、grouping_sets_append、
    multi_join_append、row_append_cdc）及 `SltSource::append_only(_cdc)` 辅助；
  - `left_aggregate` / `correlated_scalar` 的维表从 append-only 改为 keyed（主键 `(k, v)`，
    保持多行/键的 avg 语义），slt 的维表写入补 `op=insert`；
  - `sql/unions.rs` 的 append-only 投影用例改为拒绝断言；
  - README 全面更新（形状表、Sources、A1 段、限制清单：append-only 归并为一条）。
- **保留**：运行时非 keyed 分支与其**直接构造 typed view** 的单元测试暂留（不经过分析器），
  作为后续一次性删除死代码的独立重构；SLT/oracle 层面已无 append-only 入口。
- **测试**：全量 IVM 套件 388 个测试通过、0 失败（删除 8 个 SQL 级用例、1 个子用例改为拒绝）。

### 10.99 视图链：可复用的拓扑刷新与语句内 opt-in（PR-93，ROADMAP §B2，方案 D）

- **默认不隐式刷新**：读上游 MV 的语句仍只读取其当前状态（保持原有被动语义，避免"写 mv3
  却改了 mv1"的隐式副作用与时间旅行破坏）。
- **调度器 API**：`IvmRuntime::refresh_view_chain(view_id)` —— 从目标视图出发按拓扑序
  （显式栈的后序遍历、环安全、每个表一次）刷新自身及其上游，返回 `(view_id, epoch)` 列表
  （`None` = 无变化），供调度器一次推进整条链。
- **语句内 opt-in**：`IvmSqlExecutor::with_refresh_upstream(true)` 时，语句先对其引用的每个
  注册视图调用 `refresh_view_chain`，再执行本语句；默认 `false`。
- **实现**：`ViewSpec::source_table_ids()` 汇总 spec 的源表 id（无需重新解析上游 SQL）；
  拓扑遍历与刷新逻辑放在 runtime（不依赖 session）。
- **测试**：`cascading_views.rs::chained_views_refresh_on_demand`：三层链默认叶子语句**不**
  推进上游；`refresh_view_chain(mv3)` 一次推进三层（含 mv1 epoch 非空断言）；opt-in 语句
  再次追加后经整链得到正确结果。全量 IVM 套件 389 个测试通过、0 失败。

### 10.100 分区源表一期：分区值注入与按分区 before-state（PR-94，ROADMAP §B9）

- **表结构**：`IvmTable`/`IvmTableOptions` 新增 `range_partition_columns`；建表写
  `partitions = "range;hash"` 并校验分区列不重复、在 schema 内；打开表时从
  `TableInfo.partitions` 解析。分区列属于逻辑 schema，数据文件不含它们。
- **分组读取**：`PartitionFiles { partition_desc, files }` +
  `read_partition_files(_projected)`；每个分区组单独读，`with_range_partitions` +
  `with_default_column_value`（对齐 DataFusion provider 注入语义）；未分区表仍单次读取，
  merge 范围不变。`read_current`/`read_as_of`/`read_at_versions` 全部改为按分区组读取。
- **changelog 窗口**：`SourceWindow.added_files` 变成按分区组，并新增
  `before_versions: partition_desc -> 上次消费版本`；新增 `read_before_window(_filtered/
  _projected)` 按分区 pin 版本读取 before-state（未触及分区读当前态；首次消费分区为空），
  替换 keyed 路径上的 `read_as_of(*, before_timestamp)`（sum/count 族、grouping sets、
  union distinct、recompute、semi-anti、left-aggregate、lookup chain）。
- **非 keyed 旧路径**：append-only join / multi-join 的 inclusion-exclusion 需要单一
  as-of 时间戳，仍保留 `ensure_unpartitioned`；keyed 视图与 rebuild 的 guard 全部移除。
- **写入**：`append_batch` 在声明分区列时启用 `with_range_partitions`，writer 自动剥离
  分区列并生成 `partition_desc`/子目录（测试与二期 MV 分区复用）。
- **IO 修复**：`MergeParquetExec::new` 与 `new_with_inputs` 对齐——注入的常量列
  （`default_column_value`）视为分区列，不因输入文件缺失而标成 nullable，否则分区列
  可空性与表 schema 不一致（DataFusion `MemTable` 直接报 schema mismatch）。
- **测试**：`tests/partitioned_sources.rs` 3 个：按 `day` 分组聚合跨增量/删除/新分区/
  staggered 游标/`INSERT OVERWRITE` rebuild；行视图 `WHERE day = ...`；分区源 keyed join
  的增量更新与删除。全量 IVM **392 passed / 0 failed**；lakesoul-io **219 passed /
  0 failed**；fmt/clippy 干净。

### 10.101 LookupChain 支持 1:N 右侧（PR-95，ROADMAP §B3）

- **语义**：步骤右表按自身主键去重后，join key 不等于其主键即 1:N；一条基表行产出多行
  链行（LEFT 无匹配产出 NULL 补齐行）。1:1 步骤行为与 schema 不变。
- **行标识**：每个 1:N 步骤在 MV 输出列之后追加一列非空 Utf8 `__step<source>_id`：匹配行
  按「`v<octet_length>:<文本>`」对主键各列做长度前缀拼接（无碰撞、确定性；整值为 NULL 的
  LEFT 未匹配为 `n...n` 哨兵）。`lookup_chain_mv_schema_for` 据此扩展 schema；
  `validate_lookup_chain_view` 要求 MV 主键**包含**基表主键 + 各 1:N 步骤标识列（缺失明确
  拒绝，多余列由用户负责，沿用 A1 口径），并校验 `unique` 与 right_keys/右表主键一致。
- **刷新**：受影响集合仍按基表键集合计算（基表 delta + 每个变化步骤经前缀重放命中 old/new
  keys 的基表行；前序源同窗口变化则全量），重放自然展开多行；插入/删除按完整标识
  （基表键 + 标识列）去重并保持重试幂等；排序按完整标识 + `rowKinds`，保证同标识
  delete 先于 insert；rebuild 走同一套行表达式。
- **分析器**：`sql/multi_join.rs` 去掉「right_keys == 右表主键」拒绝，改为
  `unique = (right_keys == 右表主键)` 写入 spec（serde 默认 true，旧 spec 兼容）；right/full、
  bushy、>8 源等形状限制不变。步骤键引用前序源（含 1:N 步骤的列）继续支持。
- **测试**：`tests/lookup_chain_1n.rs` 2 个（LEFT 1:N 多匹配/删到 NULL 补齐/改 join key/
  基表删除/rebuild；INNER 1:N 丢行与恢复）；`sql_oracle` 新增
  `oracle_lookup_chain_1n_matches_full_recompute`（SQL 全链随机增删改对比全量 LEFT JOIN
  语义）；`analyzes_lookup_chain` 单测更新（1:N 与「1:N 步骤列作后续键」均接受）。
  全量 IVM **395 passed / 0 failed**；fmt/clippy 干净。

### 10.102 CDC 契约收敛为三标记 insert/update/delete（PR-96）

- **背景**：LakeSoul Flink CDC 摄入（`FlinkUtil.rowKindToOperation`）把 `UPDATE_AFTER` 落成
  `update`、`UPDATE_BEFORE` 落成 `delete`，静态数据只有 `insert`/`update`/`delete`；IVM 原
  四标记契约靠"未知值按 live"兜底才对 `update` 生效，契约与事实不统一（且 lone `update_before`
  的撤回角落真实摄入不会产生）。
- **变更**：撤回标记只认 `delete`——`filter_deletes`、provider 墓碑隐藏、`union distinct`
  的 retract 判定、`source_delete_filter`/`source_retract_condition` 统一为 `<> 'delete'` /
  `= 'delete'`，与 IO 层 `cdc_delete_predicate` 完全一致；`update` 上升为契约值（同键新版本，
  merge-on-read 高版本胜出）；未知值仍按 live。
- **语义注记**：同键 update 不需要 before 镜像；键变更由摄入把 before 镜像落成 `delete`(旧键)
  + `update`/`insert`(新键) 表达；append-only 源的 signed 聚合把 `delete` 记负贡献。
- **测试**：`cdc_update_markers.rs` 重写为三标记（append-only signed；keyed update 折叠、
  delete 撤回、update 复活；角落：insert→update、update→delete、键变更 delete+update、
  重复活跃行取最新、未知值 `upsert` 按 live、rebuild 一致）；`table_provider`、
  `partitioned_sources`、`lookup_chain_1n` 的 update 对改为单条 `update`。全量 IVM 395
  passed / 0 failed；fmt/clippy 干净。

### 10.103 inner/cross join 的非等值条件支持未物化列（PR-97，ROADMAP §B3）

- **能力**：`SELECT l.k, l.v, r.v AS rv FROM l JOIN r ON l.k = r.k AND l.g < r.g` 这类
  「条件引用未进 select list 的列」不再被拒；条件列自动物化为**隐藏 wide payload**，命名
  `__ivm_cond_<side>_<column>`（`join_condition_column_name`，导出以便构造 MV schema）。
- **判定**：先看 compact（每侧一个 payload）是否满足全部非等值条件都恰好比较
  `left_value`/`right_value`（`pair_compatible`，任一侧顺序均可）；不满足则走 wide：
  保留 select payload 顺序，再按条件顺序追加缺失的条件列（`materialize_condition_columns`
  去重），随后沿用既有 `render_wide_pair_conditions` 与 wide keyed join 运行时——
  运行时零改动，MV schema/重放/撤回都自动带上隐藏列。
- **注意**：切换到 wide 后输出名必须唯一（原本 compact 允许两侧同名 payload），
  重复名会以「materialized twice; add distinct aliases」明确拒绝。
- **测试**：`analyzes_theta_joins` 更新（隐藏列进入 `output_columns`，pair_filter 引用
  隐藏别名）；新增 `oracle_theta_join_hidden_columns_matches_full_recompute`：条件
  `l.v < r.v` 而输出只含 `g`，隐藏列为 v/v，随机更新使匹配集合翻转，对比全量 SQL 语义。
  全量 IVM 396 passed / 0 failed；fmt/clippy 干净。

### 10.104 两源 inner join 的表达式连接键（PR-98，ROADMAP §B3）

- **能力**：`SELECT l.k, l.v, r.v FROM l JOIN r ON l.k = r.k AND l.v + 1 = r.v`——等值键的任一侧
  不是普通列时，渲染成表达式（`render_expression`，去限定符），两侧分别求值后参与 join。
- **表示**：`ViewSpec::Join`/`JoinView` 新增 `key_exprs: JoinKeyExprs`（与 `join_keys` 位置对齐；
  普通键为空串）。表达式键在 `join_keys` 里以隐藏名 `__ivm_key_<n>` 出现（`join_key_expression_name`），
  MV schema 的类型由 `expression_type` 从对应侧表达式推导（两侧类型必须一致）。
- **运行时**：`join_side_key_columns` 在 `keyed_join_projection` 与 `join_projection` 的首次
  select 里把表达式求值并 alias（左侧 `key`、右侧 `__right_<key>`），其余 join/撤回/排序逻辑
  零改动；`validate_join_view` 校验长度、两侧类型与（keyed 路径的）输出 schema。
- **schema helper**：新增 `keyed_join_view_schema_with_keys`、`wide_keyed_join_view_schema_with_keys`、
  `join_view_schema_with_keys`、`wide_join_view_schema_with_keys`（带 `key_exprs`），既有 66 个
  调用点保持原签名（委托空表达式）。
- **范围**：仅两源 inner join；lookup/outer/multi-way/chain 的表达式键仍拒绝（错误信息不变）。
- **测试**：分析器用例固定 `join_keys`/`key_exprs` 与两侧渲染；oracle
  `oracle_join_key_expression_matches_full_recompute`（`a.k = b.k AND a.v + 1 = b.v`，随机更新
  翻转匹配）。全量 IVM 397 passed / 0 failed；fmt/clippy 干净。

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
