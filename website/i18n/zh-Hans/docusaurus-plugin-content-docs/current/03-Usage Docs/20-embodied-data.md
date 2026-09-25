# 具身数据：视频、状态与训练窗口

LakeSoul 的具身数据支持把多模态机器人/训练数据（状态/动作 tick + 压缩视频的 episode）存储为普通 LakeSoul 表，并以训练窗口的方式读取。视频以 GOP 索引的字节区间（或逐帧图片）保存，大二进制列可以外置到共享 blob pack，读取可以固定到某个快照或标签，保证严格可复现。

本页是总体使用指南；设计细节见
[`docs/embodied-snapshot-tag-design.md`](https://github.com/lakesoul-io/LakeSoul/blob/main/docs/embodied-snapshot-tag-design.md)，
可运行示例见
[`python/examples/embodied`](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples/embodied)。

## 概念

| 术语 | 含义 |
|---|---|
| Episode | 一段连续录制；以 `episode_id` 分区存储，窗口不跨 episode。 |
| Anchor / window | 样本 = 锚点行 + 相对它的行区间，例如 `Window(-2, 0)`；区间左闭右开，负值指向锚点之前。 |
| Stride | episode 内锚点之间的步长。 |
| Boundary | `skip` 丢弃越过 episode 边界的窗口；`clamp` 截断。 |
| GOP | 一组帧（关键帧 + 后续帧）；按 GOP 解码并缓存。 |
| Blob pack | 存放外置二进制值的不可变共享文件，由 tagged 行值引用。 |

导入器使用 `video_layout="gop"` 时会创建三张表：

| 表 | 内容 |
|---|---|
| `<table>` | 每个 tick 一行（状态/动作/…，`episode_id` 分区） |
| `<table>_gops` | `episode_id, camera, gop_index, timestamp, codec, num_frames, frame_timestamps, frame_offsets, frame_lengths, data` |
| `<table>_frames` | `episode_id, camera, frame_index, gop_index, gop_position, timestamp, byte_offset, byte_length` |

## 安装

```shell
pip install "lakesoul[embodied]"   # 视频解码（av、pillow）+ MCAP（mcap）
pip install "lakesoul[daft]"       # 分布式导入与读取
pip install "lakesoul[torch]"      # PyTorch 适配
```

导入器默认写 `vortex`；需要时传 `physical_format="parquet"`。

## 快速开始

### 导入 LeRobot v3 数据集

```python
import json
from lakesoul import LakeSoulCatalog
from lakesoul.embodied import import_lerobot

catalog = LakeSoulCatalog.from_env()
summary = import_lerobot(
    "/data/lerobot/pusht",
    table="pusht",
    path="s3://bucket/lakesoul/pusht",
    catalog=catalog,
    video_layout="gop",                 # "frames" 只保留逐帧图片
    cameras=["cam_high", "cam_wrist"],  # 可选，默认全部相机
    # 可选：把 GOP 字节外置到共享 pack
    properties={"blob_columns": json.dumps({"data": {"mode": "external"}})},
)
print(summary)
```

导入器会创建主表以及 `<table>_gops`、`<table>_frames`。

### 导入 MCAP 录制

```python
from lakesoul.embodied import import_mcap

import_mcap(
    "/data/recordings/episode-001.mcap",
    table="mcap_demo",
    path="s3://bucket/lakesoul/mcap_demo",
    columns={"observation.state": "control_tick:state", "reward": "control_tick:reward"},
    cameras={"cam_high": "camera_high"},
    row_topic="control_tick",
    tolerance=0.02,
    video_layout="gop",
)
```

一个 MCAP 文件对应一个 episode 分区；表格类数据按 `tolerance` 秒做最近邻对齐。支持 JSON 与 protobuf（FileDescriptorSet）消息。

### 用 Daft 分布式导入

```python
import json

from lakesoul.embodied.daft import import_lerobot_gop

import_lerobot_gop(
    "/data/lerobot/pusht",
    table="pusht_daft",
    path="s3://bucket/lakesoul/pusht_daft",
    properties={"blob_columns": json.dumps({"data": {"mode": "external"}})},
)
```

`lakesoul.embodied.daft` 还提供 `import_lerobot`（frames 布局）、`import_mcap`、`read_samples` 与 `read_gop_frames`。
所有导入器（单机与 Daft、LeRobot 与 MCAP）都接受 `properties`（如 `blob_columns`），条目会按各建表的实际列过滤。

## 读取训练样本

### 单进程：`EmbodiedDataset`

```python
import numpy as np
from lakesoul.embodied import BOUNDARY_CLAMP, EmbodiedDataset, GopVideo, Window

dataset = EmbodiedDataset(
    table.scan(),
    window={"observation_state": Window(-2, 0), "action": Window(0, 2)},
    stride=1,
    seed=42,
    boundary=BOUNDARY_CLAMP,
    video=GopVideo(catalog.table("pusht_gops"), catalog.table("pusht_frames")),
    video_window=(0, 4),                  # 第一路相机的解码帧
)

dataset.set_epoch(3)                       # 确定性重洗
for sample in dataset:                     # dict[str, np.ndarray]
    print(sample["observation_state"].shape, sample["action"].shape)
```

- 用秒级窗口寻址：`window={"state": (-1.0, 0.0)}` 搭配 `time_column="timestamp"`；
- 用 `episodes=["ep000001"]` 限定 episode；
- 分布式分片：`for sample in dataset.iter_epoch(rank=rank, world_size=world_size)`；
- `EmbodiedDataset` 可 pickle：DataLoader worker 恢复后仍是同一版本配置。

### 分布式：`read_samples`

```python
from lakesoul.embodied.daft import read_samples

df = read_samples(
    table.scan(),
    window={"observation_state": (-2, 0), "action": (0, 2)},
    stride=1,
    seed=0,
)
rows = df.collect().to_pylist()            # 列：episode_id、anchor 及各窗口对应的 list 列
```

### 解码视频帧

```python
from lakesoul.embodied import GopVideo
from lakesoul.embodied.daft import read_gop_frames

gops = catalog.table("pusht_gops")
frames = catalog.table("pusht_frames")

# 单进程、按 episode（内部按 GOP 解码并有缓存）
video = GopVideo(gops, frames).for_episode("ep000001")
images = video.frames(0, 32)               # {camera: (n_frames, H, W, 3)}

# 分布式：每个 (episode, camera, frame) 一行，image 为编码后的 JPEG 字节
decoded = read_gop_frames(gops.scan(), frames.scan()).collect().to_pylist()
raw = read_gop_frames(gops.scan(), frames.scan(), cameras=["cam"], image_format=None)
```

### 对齐副流

```python
from lakesoul.embodied import EmbodiedDataset, SecondaryStream

dataset = EmbodiedDataset(
    table.scan(),
    window={"state": (-2, 0), "action": (0, 2)},
    streams=[
        SecondaryStream(
            catalog.table("rewards").scan(),
            on="timestamp",
            by="episode_id",
            columns=["reward"],
            tolerance=0.02,
            direction="nearest",           # nearest | backward | forward
            missing="null",                # null | skip
        )
    ],
)
```

`lakesoul.embodied.align` 还提供显式的 `align(left_scan, right_scan, into=...)`，用于物化对齐后的表。

### PyTorch 训练循环

```python
from torch.utils.data import DataLoader
from lakesoul.embodied.torch import Dataset as EmbodiedTorchDataset

torch_dataset = EmbodiedTorchDataset(dataset, shuffle_buffer=1_000, prefetch=8)
loader = DataLoader(torch_dataset, batch_size=64, num_workers=4)

for epoch in range(args.epochs):
    torch_dataset.set_epoch(epoch)
    for batch in loader:
        ...
```

`prefetch` 在后台线程预读；`shuffle_buffer` 在带种子的缓冲区内洗牌；`DataLoader` worker 与 `torch.distributed` rank 会自动分片。

## 存储布局与 blob 外置

### 共享 pack 与 `.blobref` sidecar

外置值存放在数据文件旁边的不可变 pack 中：

```
<data_file_dir>/_blob/<column>/<uuid>.blob     # 不可变 pack
<data_file>.blobref                            # JSON {"version":1,"packs":[...]}
```

pack 位于数据文件自身所在目录的 `_blob/` 下，因此分区表会按分区目录各有一份 `_blob` 树（例如
`<table>/episode_id=ep000001/_blob/data/...`）。每个数据文件在自己的 sidecar 里记录所引用的
pack；vacuum 会递归扫描表下所有 `_blob/` 目录，因此分区表的 pack 同样会被回收。compaction 透传
tagged 引用、不重写 blob 字节，并为输出重建 sidecar（输入 sidecar 的保守并集）；文件移动时
sidecar 随数据文件一起搬移，保证引用始终有效。clean job 删除数据文件时会一并删除 sidecar。

Spark SQL 与 Flink SQL 也走 native reader/writer：向 blob 表写入会自动 tag 并外置，扫描默认物化 payload；传入 reader 选项
`blob_materialize = 'false'`（例如 `spark.read.option("blob_materialize", "false")`）可保留 tagged 引用并延迟读取 pack。旧 Spark Parquet writer 路径不支持 blob 表。

### `blob_columns`

```python
properties={"blob_columns": json.dumps({
    "frame": {"mode": "external"},                   # auto | inline | external
    "thumbnail": {"mode": "auto", "inline_threshold": 16384},
})}
```

默认值：`mode="auto"`（大于 `inline_threshold` 时外置）、`inline_threshold` 16 KiB、`pack_target_bytes` 256 MiB。
当下一个值会使当前 pack 超过 `pack_target_bytes` 时会开始新 pack（`0` 表示每次写入只用单个 pack）。

### 用 `BlobRef` 做延迟读取

```python
import json
from lakesoul import BlobRef

scan = catalog.scan("pusht").options(reader_options={"blob_materialize": "false"})
values = scan.to_arrow_table().column("frame").to_pylist()
ref = BlobRef.parse(values[0])
tiny = ref.read(offset=0, size=16)     # 从 pack 做范围读
full = ref.materialize()               # 整值读取（校验 CRC32）
```

## 时间旅行与可复现

用快照或标签固定精确的表状态，或按时间戳读取：

```python
import datetime as dt

snapshot_id = catalog.create_snapshot("pusht", "before-experiment")
catalog.create_tag("pusht", "v1", snapshot_id)

ids = catalog.scan("pusht").options(snapshot=snapshot_id).to_arrow_table()
ids = catalog.scan("pusht").options(tag="v1").to_arrow_table()
ids = catalog.scan("pusht").options(
    timestamp=dt.datetime(2026, 9, 1, tzinfo=dt.timezone.utc)
).to_arrow_table()
```

- `timestamp`、`snapshot`、`tag` 三者互斥；
- `time_zone` 只能与 `timestamp` 搭配（IANA 名称）。自带时区的时间戳会忽略它；naive datetime 与 `date` 必须显式传入；
- 快照与标签会 pin 住对应数据，清理不会删除它们；
- `catalog.drop_tag(...)` / `catalog.drop_snapshot(...)` 解除 pin（有标签指向时 `drop_snapshot` 会拒绝）；
- `catalog.purge(table, older_than=..., dry_run=True)` 删除超过宽限期且未被 pin 的旧版本与文件。

## 样本 manifest

manifest 保存一份可复现的样本清单：每行一个样本，包含 episode、锚点（`order_by` 列值）、全局
`rank` 和 JSON 格式的 `params`。它存放在 sibling 表 `<table>__manifests` 中，并绑定数据表的一个
快照，因此 compaction 与后续写入之后仍能复现同样的样本。

```python
import pyarrow as pa

samples = pa.table({
    "episode_id": ["ep000001", "ep000000"],
    "anchor": [2, 4],                      # frame_index 取值
    "rank": [0, 1],                        # 可选的全局顺序
})
info = catalog.create_manifest(
    "pusht", "eval-v1", samples,
    params={"window": {"state": [0, 2]}, "stride": 2, "seed": 3},
)

dataset = EmbodiedDataset.from_manifest("pusht", "eval-v1")  # 固定到 info.snapshot_id
for sample in dataset:                 # 默认按 rank 顺序；训练可用 shuffle=True
    ...

catalog.list_manifests("pusht")        # ManifestInfo(manifest, snapshot_id, rows, created_at)
catalog.drop_manifest("pusht", "eval-v1")
```

`create_manifest` 在未提供 snapshot 时自动创建；有 manifest 引用时 `drop_snapshot` 会拒绝；
删除基表会级联删除 `<table>__manifests`。锚点缺失或 manifest 不存在会直接报错，不会静默跳过样本。

## 维护

### Compaction

Compaction 只重写行、不重写 blob 负载：引用原样透传，输出 sidecar 列出输入 pack（保守并集），sidecar 随文件在 compaction 层级间移动。读取时用 `blob_materialize=false` 保持引用为惰性。

### Blob pack vacuum

没有被任何 live 数据文件引用（对照最新分区版本与所有快照/标签）且超过一天宽限期的 pack 会被删除。blob 表会在写入成功后自动执行：每 `blob_vacuum_interval` 个分区版本一次（默认 `20`；表属性设为 `0` 关闭）：

```python
properties={"blob_columns": json.dumps({"data": {"mode": "external"}}),
            "blob_vacuum_interval": "50"}
```

vacuum 会收集两次 live 集合，一旦集合变化或 live 文件缺少 sidecar 就整体中止、不删任何东西，因此与并发写入同时运行是安全的。由于写入方先上传 pack、后提交引用它的数据文件，非
dry-run 且宽限期短于一小时的删除会被拒绝，除非显式传 `allow_short_grace=True`。也可以手动执行（默认 dry run）：

```python
from lakesoul.vacuum import vacuum_blobs

report = vacuum_blobs(catalog, table, dry_run=True)
print(report)
```

:::tip
blob pack 回收**不依赖** Flink clean job；clean job 仍负责清理过期数据文件、commit 元数据与 discard 记录。
:::

### 保留与清理

快照/标签 pin 住的版本永远不会被清理；每个分区的最新版本始终保留。blob 部分见
[Clean up unreferenced blob packs](./09-clean-redundant-data.md)。

## 基准与示例

- 可运行示例：
  [`python/examples/embodied`](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples/embodied)
  （`generate_data.py`、`train.py` 及导入片段）；
- 基准脚本：
  [`script/benchmark/embodied`](https://github.com/lakesoul-io/LakeSoul/tree/main/script/benchmark/embodied)
  —— 全表扫描 vs 窗口采样（`run_benchmark.py`）、frames vs GOP 布局及 `--with-blob` 外置对比（`run_video_layout_benchmark.py`）。

## 限制与路线

- Daft 的 `read_samples(..., manifest=...)` 尚未提供，暂用 `EmbodiedDataset.from_manifest`；
- 旧 Spark Parquet writer 路径不支持 blob 表（需要 native writer）；该路径计划移除而不是加拦截；
- `read_samples` / `read_gop_frames` 位于 `lakesoul.embodied.daft`，需要 `daft` extra；
- 单进程 LeRobot 导入支持 v3.0 数据集；MCAP 支持 JSON 与 protobuf。
