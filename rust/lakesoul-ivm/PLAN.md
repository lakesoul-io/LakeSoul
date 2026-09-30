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
  （P2 已全部收尾）

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

## 9. 风险与开放问题

1. bucket 前缀属性为"IVM 内部表"专用，JVM 引擎误读会得到错误结果 → 需要
   `internal` 标记 + JVM `list tables` 过滤（后续）。
2. changelog API 与 JVM 语义有意分歧（version 消费、rebuild 信号），需在文档里
   写清楚，避免两套实现漂移。
3. `partition_info.timestamp` 是 DB 时钟，多实例时钟一致性影响水位 W；版本消费
   可消除正确性依赖，但 W 仍用于调度。
4. ~~OCC 重试与 PG 事务隔离需并发测试覆盖~~（已完成：40001/40P01 退避重试 +
   `tests/concurrency.rs`；serializable 测试环境稳定）。

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
