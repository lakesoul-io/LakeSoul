# LakeSoul 具身数据支持 · 总体文档

- 状态：M1/M2/M4 主体完成；P0（snapshot/tag/pin）已合并（PR #925）；Blob R1（pack/`.blobref`/vacuum）已实现（PR #936）；Phase D manifest 已实现（catalog API + `from_manifest` + Daft `read_samples` + 治理）。
- 面向读者：使用/维护具身数据能力的开发者。用户向教程见 website 的 [Embodied Data](../website/docs/03-Usage%20Docs/20-embodied-data.md)（英文），设计细节见 [embodied-snapshot-tag-design.md](./embodied-snapshot-tag-design.md) 与 [embodied-plan.md](./embodied-plan.md)。

## 0. 目标与非目标

- **目标**：以"episode 内顺序 + 窗口随机起点"的方式支持视频+状态的多模态训练数据；视频与状态透明地存进 LakeSoul 表，Python 侧提供导入、采样、解码、对齐、训练闭环；读取可复现（快照/标签/时间戳）。
- **非目标**：全局行级随机访问、分支（branch）、跨引擎血缘。

## 1. 总体架构

```
                 ┌─────────────────── 导入 ───────────────────┐
 LeRobot v3 ───▶ │ import_lerobot / import_lerobot_gop (Daft) │
 MCAP(JSON/protobuf) ├ import_mcap / import_mcap (Daft)         │
 自研数据 ─────▶ │ write_arrow / write_daft / Ray Datasink    │
                 └──────────────┬─────────────────────────────┘
                                │  (native writer, 可选 blob_columns 外置)
                                ▼
                 ┌──────── LakeSoul 表（metadata: PG）────────┐
                 │ <table>          逐 tick 行（状态/动作/…） │
                 │ <table>_gops     GOP 包 + 帧索引           │
                 │ <table>_frames   帧 → GOP 位置映射         │
                 │ <data_file_dir>/_blob/<column>/<uuid>.blob 外置 pack
                 │ <data_file>.blobref                 pack 引用
                 └──────────────┬─────────────────────────────┘
                                │
        ┌───────────────────────┼───────────────────────────┐
        ▼                       ▼                           ▼
 EmbodiedDataset         read_samples / read_gop_frames   Spark / Flink SQL
 (窗口/秒窗口/副流对齐)   (Daft 分布式，懒执行)              (Parquet/Vortex)
        │                       │
        ▼                       ▼
 torch Dataset            GopVideo / EpisodeGopVideo
 (prefetch, shard)        (按 GOP 解码 + 缓存)
```

## 2. 数据模型

### 2.1 表结构（GOP 布局）

`video_layout="gop"` 时导入器创建三张表（默认 `physical_format="vortex"`）：

| 表 | 内容 | 关键列 |
|---|---|---|
| `<table>` | 逐 tick 行（状态、动作、可选逐帧图片 bytes、`episode_id`） | `episode_id`（分区） |
| `<table>_gops` | 每个 GOP 一段压缩视频 + 帧索引 | `episode_id, camera, gop_index, timestamp, codec, num_frames, frame_timestamps, frame_offsets, frame_lengths, data` |
| `<table>_frames` | 帧 → GOP 内位置映射 | `episode_id, camera, frame_index, gop_index, gop_position, timestamp, byte_offset, byte_length` |

`video_layout="frames"`（默认）只把逐帧图片存进主表，不建 `_gops`/`_frames`。

### 2.2 关键概念

- **episode**：一段连续采集（`episode_id` 分区），窗口不跨 episode；
- **anchor / window**：样本以锚点行 + 相对行区间 `Window(start, end)` 定义，`start` 含、`end` 不含，负值指向锚点之前；也支持秒窗口（`time_column`，默认 `timestamp`）；
- **stride**：锚点步长；`boundary=skip|clamp` 处理 episode 边界窗口；
- **副流对齐**：`SecondaryStream` 按时间戳把另一张表对齐到主表行（`nearest/backward/forward`，`null/skip` 缺失语义）；
- **GOP**：一段关键帧开头的压缩视频；帧解码只解所需 GOP，并在 `EpisodeGopVideo` 内按 `(camera, gop_index)` 缓存。

## 3. 功能矩阵与实现状态

| 能力 | 入口 | 状态 |
|---|---|---|
| 窗口采样（顺序消费 + 随机起点、epoch 洗牌、rank/worker 分片） | `EmbodiedDataset`、`iter_epoch` | 完成 |
| PyTorch 适配（IterableDataset、shuffle buffer、读前 prefetch） | `lakesoul.embodied.torch.Dataset` | 完成 |
| 存储策略（vortex 默认、blob/binary 列 identity 压缩、`vector_columns`） | 表属性/写入选项 | 完成 |
| Loader 性能（真实 prefetch、readahead、`prefetch=N`） | `torch.Dataset(prefetch=...)` | 完成 |
| Blob 外置（`blob_columns`、R1 pack/`.blobref`、零拷贝引用读） | `blob.py`、`BlobRef` | 完成 |
| LeRobot v3 导入（状态/动作 + 视频） | `import_lerobot` / `import_lerobot_gop` | 完成 |
| MCAP 导入（JSON + protobuf，多 topic 对齐） | `import_mcap`、`build_*` | 完成 |
| GOP 布局与读取 | `_gops`/`_frames`、`GopVideo`、`decode_gop` | 完成 |
| Daft 分布式（导入/窗口采样/GOP 解码、Ray runner） | `lakesoul.embodied.daft` | 完成 |
| Ray 写入（`LakeSoulDatasink`，含自动 vacuum 钩子） | `lakesoul.ray` | 完成 |
| 时间语义（timestamp/snapshot/tag、时区规则） | `scan.options(...)` | 完成（Python） |
| 快照/标签与 pin-aware 保留 | `catalog.create_snapshot/create_tag/...` | 完成（PR #925） |
| 自动/手动 pack GC | `blob_vacuum_interval`、`vacuum_blobs` | 完成（PR #936） |
| sample manifest（sibling 表 + `from_manifest` + Daft `read_samples` + 复现测试） | `catalog.create_manifest` / `EmbodiedDataset.from_manifest` / `read_samples(manifest=...)` | 完成 |

## 4. 关键机制

### 4.1 Blob R1（引用模型）

- pack 不可变共享：数据文件所在目录下的 `_blob/<column>/<uuid>.blob`（分区表每个分区目录一份 `_blob` 树，vacuum 递归扫描表下全部 `_blob`）；
- 每个数据文件写 `<data_file>.blobref`（JSON：`{"version":1,"packs":[...]}`），路径约定归属数据文件，不改 `file_ops`；
- tagged 行内表示：inline `0x00 || raw`；external `0x01 || crc32(u32 LE) || length(u32 LE) || offset(u64 LE) || pack_path`；
- 表属性：`blob_columns = {"<col>": {"mode": "auto|inline|external", "inline_threshold": 16384, "pack_target_bytes": 268435456}}`（默认 auto / 16 KiB / 256 MiB；超过 `pack_target_bytes` 时开始新 pack，`0` 表示不滚动）；
- 默认读会物化 blob；`reader_options={"blob_materialize": "false"}` 保留 tagged 值，用 `BlobRef.parse(value).read(offset, size)` 做范围读（整读校验 CRC32）；
- SQL 引擎 glue：Spark/Flink 的 native 读写自动透传表属性 `blob_columns`（Spark 读侧由 `LakeSoulScanBuilder` 注入、`NativeIOUtils` 转发；Flink 由 `FlinkUtil.setIOConfigs` 转发），写入即 tag/外置、读取默认物化，`blob_materialize=false` 保留 tagged；CompactBucketIO 显式排除该选项，避免 compaction 二次编码；
- compaction 透传：读侧不物化、写侧把输入 sidecar 的 pack 并集写到每个输出文件的 `.blobref`（保守并集，不拷贝 payload）；`moveFileToLevel`/`DelayedCopyCommitProtocol` 搬数据文件时同步搬 sidecar。

### 4.2 保留与清理（两件事分清楚）

- **数据文件/元数据**：旧版本文件、`data_commit_info`、discard 文件由 compaction + Flink Clean Job（TTL、pin-aware）负责；快照/标签 pin 住的数据不会被清理。
- **pack GC**：`vacuum_blobs(catalog, table, older_than=1d, dry_run=True)` 收集"最新分区版本 + 所有 snapshot/tag"引用的数据文件 → 读 sidecar 得 used pack 集合 → 删除无引用且 mtime 早于 grace 的 pack；两次收集、live 集合变化或缺 sidecar 则整体中止。宽限期默认 1 天。
- **自动 vacuum**：blob 表在提交后按分区版本计数触发，`blob_vacuum_interval`（默认 20，`0` 关闭）为倍数时执行一次真实 vacuum；失败只告警不影响写。**pack GC 不依赖 Flink Clean Job**。
- **安全宽限**：写入方先上传 pack、后 commit 数据文件，因此非 dry-run 且 grace < 1 小时会被拒绝（除非显式 `allow_short_grace=True`）；默认宽限期 1 天。
- 手动 `purge(older_than=..., dry_run=True)` 仍可用于删除未 pin 的旧版本与文件。

### 4.3 时间语义与可复现

- `scan.options(timestamp=..., time_zone=..., snapshot=..., tag=...)`：三者互斥（同时传报错）；`time_zone` 只与 `timestamp` 搭配；
- `timestamp` 接受毫秒整数、`datetime`、ISO-8601 字符串、`date`；带时区的值忽略 `time_zone`，naive 值必须显式传 IANA `time_zone`，否则报错；
- `tag > snapshot > timestamp` 的优先级仅作为设计意图记录；当前实现要求显式区分，不自动挑选；
- `EmbodiedDataset` 可 pickle，反序列化后固定同一版本配置；配合 `set_epoch(epoch)` 保证洗牌可复现。

### 4.4 Sample manifest（Phase D）

- sibling 表 `<table>__manifests`（按 `manifest` 分区）：`manifest, snapshot_id, episode_id, anchor(order_by 列值, int64), rank, params(JSON), created_at`；
- `catalog.create_manifest(table, manifest, samples, ...)`：`samples` 至少含 `episode_id, anchor`，`rank` 缺省自动编号；未给 snapshot 时自动创建快照；`params` 记录 `window/stride/boundary/seed/time_column/streams/video` 等读配置；
- `EmbodiedDataset.from_manifest(table, manifest, ...)`：按 `snapshot_id` 固定读取，显式锚点（值→行号映射），默认 `rank` 顺序、`shuffle=True` 可选，支持 `iter_epoch(rank, world_size)` 分片与 pickle 序列化；
- 治理：`drop_snapshot` 拒绝删除被 manifest 引用的快照；`drop_table` 级联删除 sibling 表；锚点缺失/未知 manifest 直接报错不静默跳过；`create_manifest(overwrite=True)` 通过重写 sibling 表实现；
- Daft：`read_samples(scan, manifest="eval")` 或 `read_samples(manifest="eval", table=..., catalog=...)`，输出含 `rank`；分布式输出顺序不保证，需要时按 `rank` 排序；

## 5. Python API 索引

```python
from lakesoul.embodied import (
    EmbodiedDataset, Window, BOUNDARY_SKIP, BOUNDARY_CLAMP,
    GopVideo, SecondaryStream, align,
    import_lerobot, import_mcap, ImportSummary,
)
from lakesoul.embodied.daft import (          # 需 lakesoul[daft]
    import_lerobot, import_lerobot_gop, import_mcap, read_samples, read_gop_frames,
)
from lakesoul.embodied.torch import Dataset as EmbodiedTorchDataset  # 需 lakesoul[torch]
from lakesoul import BlobRef, materialize_blob
from lakesoul.vacuum import vacuum_blobs, VacuumResult
```

- 依赖 extras：`embodied`（`av`/`pillow`/`mcap`）、`daft`、`torch`、`ray`；完整安装 `lakesoul[all]`。
- 导入器签名（单机）：`import_lerobot(source, *, table, path, catalog=None, namespace=None, episodes=None, cameras=None, include_video=True, video_layout="frames"|"gop", image_format="JPEG", image_quality=90, physical_format="vortex", properties=None, overwrite=False)`；`properties`（如 `blob_columns`）在单机与 Daft 导入器（LeRobot/MCAP）均支持，按各建表列过滤。
- `import_mcap(source, *, table, path, ..., columns=None, cameras=None, row_topic=None, tolerance=0.02, video_layout=...)`；一个 MCAP 文件 = 一个 episode 分区。
- 采样：`EmbodiedDataset(scan, window={...}, stride=1, episodes=None, boundary="skip", seed=0, time_column=None, streams=(), video=GopVideo(...), video_window=None)`、`iter_epoch(epoch=None, rank=None, world_size=None)`。

## 6. 示例、基准与测试

- 端到端示例（含导入/训练/基准命令）：[`python/examples/embodied/README.md`](../python/examples/embodied/README.md)；
- 基准：`script/benchmark/embodied/run_benchmark.py`（全表扫描 vs 窗口采样）、`run_video_layout_benchmark.py`（frames/GOP 布局、`--with-blob` 外置对比）、`lerobot_source.py`（合成 LeRobot v3 数据集）；
- 测试：`python/tests/embodied/`（dataset/torch/lerobot/mcap/align/daft/ray），`python/tests/arrow/test_lakesoul_write.py`（blob 写读/零拷贝）、`python/tests/arrow/test_vacuum_blobs.py`、`python/tests/arrow/test_blob_vacuum_trigger.py`、`python/tests/metadata/test_snapshot_tag.py`、`python/tests/metadata/test_time_travel.py`；
- 端到端 CI：`script/ci/compaction_clean_e2e.py` 的 blob 场景验证 sidecar 迁移、pack 引用、自动/手动 vacuum 与 clean job 删 sidecar。

## 7. 限制与注意事项

1. **旧 Spark Parquet writer 路径不支持 blob**：blob 表依赖 native writer；该遗留路径计划移除，不做拦截；
2. `read_samples`/`read_gop_frames` 位于 `lakesoul.embodied.daft` 子模块，不在 `lakesoul.embodied` 顶层导出；
3. 单机 LeRobot 导入仅支持 v3.0；MCAP 支持 JSON 与 protobuf（FileDescriptorSet）；
4. `align()` 物化 API 为 P1，读时副流对齐推荐直接用 `EmbodiedDataset(streams=...)`；
5. blob GC 依赖 sidecar 完整性：live 文件缺 `.blobref` 时 vacuum 整体跳过（安全优先）。

## 8. 相关文档

- [具身数据使用教程（website，英文）](../website/docs/03-Usage%20Docs/20-embodied-data.md)
- [快照/标签/pin 与 Blob R1 设计](./embodied-snapshot-tag-design.md)
- [执行计划与历史状态](./embodied-plan.md)
- [冗余数据清理（含 blob pack vacuum）](../website/docs/03-Usage%20Docs/09-clean-redundant-data.md)
- [Python SDK 文档](../website/docs/03-Usage%20Docs/11-lakesoul-python/01-overview.md)
