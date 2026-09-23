# LakeSoul 快照 / Tag / 保留（P0）设计

状态：已评审（2026-09-22）。Phase A+B 同版本发布；branch 不在范围内。

## 0. 目标与非目标

目标：

- Python 时间旅行：按 `timestamp` / `snapshot_id` / `tag` 读取历史版本；
- tag 作为保留 pin：被 pin 的 snapshot 不参与清理；
- Flink Clean Job 感知 pin，compaction 产物与被 pin 文件不进入 discard 表；
- blob（R1）：pack 独立于数据文件、引用清单（`.blobref`）、orphan GC；
- manifest 独立表：样本视图可查询、跨快照可复现。

非目标：branch、快照回滚/写分支、Spark SQL 时间旅行语法、跨引擎血缘。

## 1. 调研结论（blob）

- **Lance Blob v2**（`docs/src/guide/blob.md`）：blob 列是逻辑 struct（内联 `data` 或
  `uri`+`position`/`size`）；大 payload 存独立 `.blob` 文件并被引用，compaction/更新只重写
  引用、不重写字节；`read_blobs`（整读）与 `take_blobs`（`BlobFile` 惰性/range）。
- **Paimon**（`docs/docs/primary-key-table/blob-storage.md`）：数据文件存 `BlobDescriptor`；
  payload 存不可变、可共享的 `.managed.blob` pack；每个数据文件一个 `.blobref` sidecar
  记录其引用的 pack 清单，随数据文件生命周期（`extraFiles`）走；compaction 只重建
  sidecar、不拷贝 payload；GC 为独立的 `remove_orphan_blobs`：扫描所有保留
  snapshot/tag/branch 的 sidecar，删除无引用且早于 `older_than`（默认 1 天）的 pack，
  两次收集且拓扑变化则中止；sidecar 缺失/损坏时不删除任何 pack；snapshot 过期只删数据
  文件 + sidecar。
- tag 语义（Paimon `maintenance/manage-tags.mdx`）：tag 保留 snapshot 的 manifest 与数据
  文件，独立于 snapshot 过期。

结论：LakeSoul 采用 R1（引用模型），compaction 不重写 blob 字节。

## 2. 元数据模型（PG）

```sql
-- 表级快照：创建时把每个分区的最新版本与 commit ids 固化下来
create table if not exists table_snapshot
(
    table_id    text   not null,
    snapshot_id bigserial,
    created_at  bigint not null,           -- PG 服务端 now() epoch ms
    description text,
    primary key (table_id, snapshot_id)
);

-- 权威 pin 来源：快照包含的每个 (partition, version, commit)
create table if not exists snapshot_commit
(
    table_id       text   not null,
    snapshot_id    bigint not null,
    partition_desc text   not null,
    version        int    not null,
    commit_id      uuid   not null,
    primary key (table_id, snapshot_id, partition_desc, commit_id)
);
create index if not exists snapshot_commit_commit_id on snapshot_commit (table_id, commit_id);
create index if not exists snapshot_commit_version on snapshot_commit (table_id, partition_desc, version);

-- tag：名字 -> snapshot；expire_at 为空表示永不过期
create table if not exists table_snapshot_tag
(
    table_id    text   not null,
    tag         text   not null,
    snapshot_id bigint not null,
    created_at  bigint not null,
    expire_at   bigint,
    primary key (table_id, tag)
);

-- 物化 pin 标记（Flink Clean Job 直接过滤；由 snapshot/tag 创建/删除同事务重算）
alter table data_commit_info add column if not exists pinned boolean not null default false;
alter table partition_info   add column if not exists pinned boolean not null default false;
create index if not exists data_commit_info_pinned on data_commit_info (table_id, pinned);
```

创建快照 = 单条 `INSERT ... SELECT`（`partition_info` 每分区最新版本 + `unnest(snapshot)`），
PG MVCC 一致读、无锁；`snapshot_id` 用 `bigserial` 全局序列（每表内按 id 单调，避免每表
`max()+1` 竞态）。

pin 维护：创建 snapshot/tag 后对涉及的 commit/version `SET pinned = true`；drop 时用
`EXISTS (SELECT 1 FROM snapshot_commit ...)` 重算，避免多 tag 引用同一 commit 时的引用计数问题。

## 3. 读取语义（Python 先行）

- `LakeSoulScan.options(snapshot=..., tag=..., timestamp=..., time_zone=...)`，优先级
  `tag` > `snapshot` > `timestamp`；
- `timestamp` 接受 `datetime.date` / `datetime.datetime` / ISO-8601 `str`（另保留 epoch ms
  `int` 逃生）：
  - 带时区（aware datetime / 带 offset 字符串）：直接使用，忽略 `time_zone`；
  - 不带时区：必须显式传 `time_zone`（IANA 名，如 `"Asia/Shanghai"`），否则 `ValueError`；
  - `date` 解释为该时区当天 `00:00:00`；
  - 语义：`partition_info.timestamp <= T` 的每分区最新版本（事务时间，PG 服务端时钟）；
- `snapshot/tag`：取 `snapshot_commit` 行，按 (partition_desc, commit_ids) 直接调用
  `get_data_files_of_single_partition` 解析文件（不依赖旧 `partition_info` 行是否仍在）；
- `timestamp`：DAO `ListPartitionByTableIdAndTimestamp`（`DAO_TYPE_QUERY_LIST_OFFSET + 17`）；
- `EmbodiedDataset`、Daft `read_samples`/`read_gop_frames` 接收 scan，自动继承；
- pickle/`to_scan_config` 固定 snapshot/tag/timestamp，DataLoader worker 读同一版本。

## 4. Flink Clean Job 集成（只此一处清理）

现状（`lakesoul-flink/src/main/java/org/apache/flink/lakesoul/entry/clean/`）：

| 路径 | 行为 | 改动 |
|---|---|---|
| `CompactionBroadcastProcessFunction` → `CleanUtils.deleteFileAndDataCommitInfo` / `cleanPartitionInfo` | 删旧版本文件、`data_commit_info`、`partition_info` | 加 `AND pinned = false`；删文件前确认 commit 未 pinned |
| `LakeSoulTable.newCompaction.commitMetadata`（Spark）写 `discard_compressed_file_info` | 旧 compaction 文件无条件入 discard 表 | 被 pin 的文件不入 discard 表 |
| `DiscardFileDeleteFunction` | 按 TTL 删 discard 行与文件 | 兜底跳过 pinned |
| `TtlBroadcastProcessFunction.dropPartition` → `DBManager.deleteMetaPartitionInfo` | 整分区版本/文件/目录递归删除 | 分区内存在 pinned 版本/commit 则跳过 |
| 手动删除 | 无 | `drop_tag` / `drop_snapshot(purge=True)` 解除 pin；`purge` 立即回收 |

顺带修复（调研发现）：

1. compaction 清理删除 `data_commit_info` 前未检查更新版本是否仍引用该 commit（merge-on-read 共享 commit 会被误删）；
2. `dropPartition` 递归删目录但未清理该分区的 `discard_compressed_file_info` 行。

## 5. Blob R1（引用模型）

- pack 命名改为不可变共享：`<table_path>/_blob/<column>/<uuid>.blob`（tagged 引用格式不变：
  `0x01 || crc32 || length || offset || pack_path`）；
- native writer 落数据文件时写 `<data_file>.blobref`（JSON：pack 路径列表 + size），路径约定
  归属数据文件，无需改 `file_ops`；
- compaction：
  - 读侧 `blob_materialize=false` 透传引用；
  - 写侧收集输出行引用，生成新 `.blobref`，不拷贝 payload；
  - `moveFileToLevel` / `DelayedCopyCommitProtocol` 搬数据文件时同步搬 `.blobref`；
  - 使用 Spark Parquet writer 的旧 compaction 路径需强制 native writer（或拒绝 blob 表）；
- GC：`vacuum_blobs(table, older_than=1d, dry_run=True)`（Python/native）：
  1. 收集所有 live snapshot/tag 引用的数据文件；
  2. 读每个数据文件的 `.blobref`，得到 used pack 集合；
  3. 删除无引用且 mtime 早于 `older_than` 的 pack；
  4. 两次收集、拓扑变化则中止；任何 sidecar 缺失/损坏则整体不删；
- snapshot 过期/删除只删数据文件 + `.blobref`，pack 由 `vacuum_blobs` 回收；
- 宽限期默认 1 天（对齐 Paimon）。

## 6. Manifest（Phase D）

- sibling 表 `<table>__manifests`（普通 LakeSoul 表，用户可直接 scan/SQL）：
  `manifest`(名称), `snapshot_id`, `episode_id`, `anchor`, `rank`, `params`(JSON 字符串), `created_at`；
- 一个 manifest = 按 `manifest` 过滤的行集；
- `EmbodiedDataset.from_manifest(...)` 按 `snapshot_id` 打开数据表复现样本；测试断言
  compaction/新版本后逐字节一致。

## 7. 阶段

- **Phase A+B（同版本发布）**：DDL/迁移、Rust DAO/proto、Python API 与读取、Flink pin 化、
  手动 purge、测试（Python + Flink 集成）；
- **Phase C**：blob R1（pack 命名、`.blobref`、compaction 透传、`vacuum_blobs`）；
- **Phase D**：manifest sibling 表 + SDK + 复现测试 + 文档。

## 8. 风险

1. 旧表兼容：`pinned` 列默认 false，读取路径不受影响；blob pack 命名变更仅影响未发布的
   Python/native blob 功能（tagged 引用自描述，旧 pack 仍可读，GC 需同时识别两种命名）；
2. Flink Clean Job 的 `dataExpiredTime`/`ontimer_interval` 默认值与文档不一致（既有问题），
   本设计不改默认，仅加 pin 过滤；
3. `vacuum_blobs` 依赖 sidecar 完整性；缺失即整体跳过（Paimon 安全规则）；
4. 并发：快照创建单语句一致读；pin 重算与 tag/snapshot 创建/删除同事务。
