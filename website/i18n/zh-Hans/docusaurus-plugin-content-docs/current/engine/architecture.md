---
sidebar_position: 2
sidebar_label: 架构与表格式
---

# 架构与表格式

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

这一页讲 SQL 表面之下的设计：提交、元数据和数据文件如何组织，使同一张表同时支持行级更新、增量读取、增量计算和检索。30 秒版本见[架构](../get-started/architecture.md)。

## 设计目标

- **对象存储是唯一事实来源。** 数据持久且便宜；计算无状态，替换计算节点不需要搬文件。
- **行级变更不重写整表。** 一次更新写 delta 文件，而不是复制一份新表。
- **提交日志就是 changelog。** 增量读取和增量计算消费同一批让数据可见的提交。
- **索引与数据同生命周期。** 向量和全文索引是表的一部分，不是另一套需要同步的系统。
- **物理格式可以选择。** 表可以在不重写历史的前提下更换文件格式。

## 三个平面

```
计算面
  ├─ 提交 / 冲突解决 ──► 控制面（PostgreSQL）
  └─ 读取 ─────────────► 数据面（对象存储）
```

控制面保存 catalog、分区版本、数据提交、快照和索引；数据面是对象存储里的 base/delta 文件和索引段；计算面是 DataFusion、PostgreSQL 前端、Python SDK 和连接器。控制面是一个数据库，不是一组 manifest 文件。这个选择让分区版本冲突检测、行级 merge-on-read 和索引 lease 成为可能；代价是 PostgreSQL 成为部署的一部分。

## 元数据模型

| 表 | 职责 |
| --- | --- |
| `namespace` | 命名空间 |
| `table_info` | 表路径、Arrow schema、分区列、属性 |
| `table_name_id` / `table_path_id` | 名称/路径到 `table_id` 的查找 |
| `partition_info` | 每个分区的版本、快照（`commit_id[]`）、提交类型 |
| `data_commit_info` | 一次提交的文件操作，以及是否已发布 |
| `table_snapshot` / `snapshot_commit` / `table_snapshot_tag` | 表级快照与 tag |
| `index_shard` / `index_commit` / `index_lease` | 二级索引控制面 |

三个概念承载了大部分设计：

- **分区版本。** 分区是并发单位。`partition_info` 为每个分区保存单调递增的 `version`；提交时比较自己读到的版本和当前 head。
- **快照。** `partition_info.snapshot` 是一组 `commit_id`。读取解析的是这组提交，而不是“最新文件”。
- **提交。** `data_commit_info` 记录一次提交的 `file_ops` 和 `committed` 标记。分区发布新版本、并把它的数据提交标记为已发布，在同一个事务里完成，提交才真正可见。

## 提交协议

```
writer:
  写不可变文件 ──► 对象存储
  commit_data(partition, read_version, file_ops, commit_op)
        │
        ▼
  PostgreSQL 事务:
    insert partition_info (version + 1, snapshot = commit ids)
    update data_commit_info set committed = true
        │
        ▼
  唯一键冲突 / 版本过期 = 冲突 ──► 解决、重试或失败
```

新的分区版本和它引用的数据提交在**同一个 PostgreSQL 事务**里发布。并发写者输在分区版本的唯一键上，而不是表级锁上。重放时如果提交都已发布，返回 `already committed`；读到过期版本时，会基于期间落地的提交做合并或报冲突。

不同操作的快照迁移规则不同：

| 操作 | 快照规则 |
| --- | --- |
| Append / Merge | 在当前快照上追加新的 commit id |
| Update | 替换快照；过期读会被 rebase 或报冲突 |
| Compaction | 读到的版本是当前版本时替换快照；期间有更新则跳过该分区 |
| Delete | 清空快照并 bump 版本 |

当分区的 delta 版本积累到阈值时，通过 PostgreSQL `NOTIFY` 触发 compaction；读者不需要等待它。

## 表格式

### Base 文件与 delta 文件

主键写入追加 **delta 文件**，并在 `data_commit_info.file_ops` 里记录为 `add`。删除记录为 `delete`，或在 CDC 变更列里写 tombstone。读取时收集快照对应的文件操作，按主键合并：最新提交胜出，合并后 tombstone 消失。

`file_exist_cols` 记录文件实际包含哪些列。这是**部分列 upsert** 的基础：只带 `(id, score)` 的 delta 不会覆盖旧文件里的 `name`；缺失列表示“未贡献”，而不是 NULL。

`compaction` 按大小分层把 delta 文件合并成更大的 base 文件，并发布 `CompactionCommit`。它降低读放大，同时不阻塞读写。

### 主键、分区与 CDC

- **哈希桶。** `hash_bucket_num` 把主键表拆成多个桶，每个桶是独立的合并单元。
- **范围分区。** `PARTITIONED BY` 在哈希桶之上再加一层范围维度。读取会裁剪分区和桶；写入只碰一个分区。
- **CDC 变更列。** `rowKinds`（或 `lakesoul_cdc_change_column` 指定的列）记录 insert/delete 语义，也是增量 SQL 目标表能够撤回行的基础。

### 可插拔的物理格式

`file_format` 是**每次写入**的选择，不是表的锁死属性：

| 值 | 适用场景 |
| --- | --- |
| `parquet` | 需要互操作、回滚，或外部 reader |
| `vortex` | 读性能和随机行访问最重要 |
| `vortex-compact` | 默认；更小的文件，使用 Zstd/PCodec 一类编码 |

同一个快照可以混用 Parquet 和 Vortex。读取端按格式分组，构建同一个逻辑扫描；merge-on-read、主键合并、分区裁剪和快照语义都与格式无关。完整矩阵和升级边界见[物理文件格式](../operations/physical-file-formats.md)。

### Vortex：随机访问与多模态

Vortex 是直接在有编码的列上操作的压缩列式格式，支持投影和谓词下推。普通 Vortex 表使用 Vortex Writer 的默认值：每个 row block 8192 行、data block 目标 1 MiB，以保持压缩比。表属性 `vector_index_columns` 中声明的列则默认使用 1024 行 block 和 128 KiB 的 data block 目标，因为向量检索读取的是这些宽列上分散的候选行。这个布局按列生效、可按表配置；其他列仍使用 Writer 默认值。

Vortex 也承载大二进制 payload。视频帧、机器人 episode、tensor 以 blob 列与元数据放在一起；`BlobRef` 和 `materialize_blob` 让引用和 payload 处于同一个快照。具身数据集不需要再为媒体准备一个对象存储、为元数据准备第三套系统。

### 主键定位器：高性能点查

`pk = value` 和 `pk IN (...)` 不扫全表。每个数据文件有一份**按主键排序的 per-file 索引**：

- key 用 `arrow-row` 编码（与 writer 排序时相同的保序编码），一次查找返回连续的行区间；
- 索引惰性构建，以 `mmap` 文件形式存放在对象存储磁盘缓存里，进程内查询共享；
- 数据文件不可变：重写或 compaction 会产生新文件和新 locator，索引不会陈旧；
- 复合主键支持**前缀查找**（`(g, value)` 上的 `g = 'x'`）。

```
pk = 42
  ├─ per-file 有序主键索引（mmap，对象存储缓存）
  ├─ 候选行区间
  │    ├─ Vortex：直接按 row index 读取
  │    └─ Parquet：row selection + page index，跳过无关 page
  └─ 只对这些行做 merge-on-read，最新版本胜出
```

定位器刻意返回匹配行的**超集**：只使用 key 前缀，因为丢掉一个满足 payload 条件的行，可能会丢掉某个 key 的最新版本。merge 的正确性不依赖定位器给出精确集合。

Vortex 和 Parquet 都支持：Vortex 直接按 row index 读，Parquet 使用 row selection 和 page index。这就是“扫描所有 delta 文件再合并”和“先定位行，再合并”的区别。

## 读路径

| 路径 | 解析方式 | 使用方 |
| --- | --- | --- |
| 快照读 | `partition_info.snapshot` → `data_commit_info.file_ops` → merge-on-read | SQL 查询、时间旅行 |
| 增量读 | `data_commit_info` 的提交区间 → changelog | Spark/Flink 增量查询、增量 SQL |
| 索引扫描 | `index_commit` 的 generation/version → 索引段 | 向量与全文检索 |

## 二级索引也是格式的一部分

索引控制面与元数据共用数据库：

- `index_shard` 用 `(kind, index_prefix)` 标识一个 shard；
- `index_commit` 保存不可变的 `(generation, version)`、索引 header 和 segment 列表；全量重建发布新 generation，增量 flush 发布 `version + 1`；
- `index_lease` 在读者加载某个 generation 期间保活，避免 GC 删掉正在读取的文件。

索引数据存放在表旁边的对象存储里。向量索引是 IVF + RaBitQ，全文索引是 Tantivy BM25。两者都在数据提交之后维护，并由读取表的同一个引擎查询。见[向量索引管理](vector-search/index-management.md)和[文本索引管理](full-text-search/index-management.md)。

## 建立在 commit log 上的增量计算

表的提交序列本身就是 changelog。增量 SQL 为每个目标表保存游标，只消费游标之后的提交，识别发生变化的分区，只写受影响的行。让数据对快照读可见的同一批提交，就是增量计算的输入；引擎内部写入不需要额外的 CDC 管道。

见[增量 SQL](incremental-sql/overview.md)。

## 为什么不直接用 Iceberg？

Iceberg 是一个开放的表布局规范：manifest、snapshot、分区演进和广泛的引擎互操作。它很好地解决了那一层的问题，LakeSoul 并不试图取代这个规范。

LakeSoul 解决的是另一层：一个跑在对象存储上、需要行级更新、可消费的 changelog、索引和 PostgreSQL 端点的引擎。换个说法：

| | Iceberg | LakeSoul |
| --- | --- | --- |
| 元数据 | manifest 与 snapshot 文件 | PostgreSQL 中的分区版本与提交行 |
| 行级更新 | delete files，由外部引擎合并 | 主键 delta 文件 + merge-on-read |
| 增量 changelog | 外部引擎计算 snapshot diff | commit log 直接消费 |
| 二级索引 | 外部系统 | 引擎内的索引 catalog |
| 物理格式 | Parquet / ORC / Avro | Parquet / Vortex / Vortex Compact，按写入选择 |
| 多模态 payload | binary 列；没有 blob 语义 | Vortex blob 列 + `BlobRef` |
| 点查 | delete files + 引擎扫描；没有主键定位器 | per-file 有序定位器，直接读行区间 |
| 提交并发 | catalog / 对象存储协调 | PostgreSQL 事务里的分区版本 CAS |

这些能力不在 Iceberg 规范里。你可以在 Iceberg 之上把它们搭出来——delete files、外部索引服务、CDC 管道和独立的向量数据库——但那时你是在运维多套系统并自己做一致性。LakeSoul 把它们做成了一套设计。

**需要一种被很多引擎读取的被动表格式时，Iceberg 是更好的选择。** 需要一个在对象存储上做行级更新、增量计算和检索，并愿意运行它的 catalog 的引擎时，LakeSoul 是更好的选择。

代价是真实的：catalog 依赖 PostgreSQL；读取需要 LakeSoul 引擎或它的 native reader；格式不是公开规范；Vortex 比 Parquet 新，外部 reader 更少。这些是有意的取舍，不是遗漏。

## 设计边界

- 元数据由 PostgreSQL 提供，catalog 是部署的一部分。
- 外部 catalog 互通还在路线图上；今天其他引擎通过 LakeSoul 连接器或 native IO 读取。
- Vortex 是新写入的默认格式，但 Parquet 仍是互操作和回滚路径。LakeSoul 3.x 不能读 Vortex，见[兼容性参考](../reference/compatibility.md)。
- 增量 SQL、向量检索和全文检索各有自己的限制；这一页描述共享的存储设计，不是所有功能边界。

## 下一步

- [引擎概览](overview.md)
- [PostgreSQL 前端](postgres/overview.md)
- [增量 SQL](incremental-sql/overview.md)
- [向量检索](vector-search/overview.md)
- [物理文件格式](../operations/physical-file-formats.md)
