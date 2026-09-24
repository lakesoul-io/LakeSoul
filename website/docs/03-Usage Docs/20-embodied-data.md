# Embodied Data: Videos, States and Training Windows

LakeSoul's embodied-data support stores multimodal robot/training data — episodes of state and
action ticks plus compressed video — in ordinary LakeSoul tables, and reads it back as training
windows. Video is kept as GOP-indexed byte ranges (or per-frame images), large binary columns can
be externalized into shared blob packs, and reads can be pinned to a snapshot or tag for exact
reproducibility.

This page is the overall guide; the design is described in
[`docs/embodied-snapshot-tag-design.md`](https://github.com/lakesoul-io/LakeSoul/blob/main/docs/embodied-snapshot-tag-design.md)
and runnable examples live in
[`python/examples/embodied`](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples/embodied).

## Concepts

| Term | Meaning |
|---|---|
| Episode | One recording; stored as a `episode_id` partition. Windows never cross episodes. |
| Anchor / window | A sample is an anchor row plus row ranges relative to it, e.g. `Window(-2, 0)`. Bounds are half-open; negative values point before the anchor. |
| Stride | Steps between anchors inside an episode. |
| Boundary | `skip` drops windows that run past an episode edge; `clamp` clips them. |
| GOP | A group of pictures (keyframe + frames). Frames are decoded per GOP and cached. |
| Blob pack | An immutable shared file holding externalized binary values, referenced by tagged row values. |

Importers with `video_layout="gop"` create three tables:

| Table | Content |
|---|---|
| `<table>` | one row per tick (state/action/..., `episode_id` partition) |
| `<table>_gops` | `episode_id, camera, gop_index, timestamp, codec, num_frames, frame_timestamps, frame_offsets, frame_lengths, data` |
| `<table>_frames` | `episode_id, camera, frame_index, gop_index, gop_position, timestamp, byte_offset, byte_length` |

## Installation

```shell
pip install "lakesoul[embodied]"   # video decode (av, pillow) + MCAP (mcap)
pip install "lakesoul[daft]"       # distributed import and reads
pip install "lakesoul[torch]"      # PyTorch adapter
```

The importers write `vortex` by default; pass `physical_format="parquet"` when needed.

## Quick start

### Import a LeRobot v3 dataset

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
    video_layout="gop",                 # "frames" keeps per-frame images only
    cameras=["cam_high", "cam_wrist"],  # optional, defaults to all cameras
    # externalize the GOP bytes into shared packs (optional)
    properties={"blob_columns": json.dumps({"data": {"mode": "external"}})},
)
print(summary)
```

The importer creates the main table plus `<table>_gops` and `<table>_frames`.

### Import an MCAP recording

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

One MCAP file becomes one episode partition; tabular values are nearest-anchored within
`tolerance` seconds. JSON and protobuf (FileDescriptorSet) messages are supported.

### Distributed import with Daft

```python
from lakesoul.embodied.daft import import_lerobot_gop

import_lerobot_gop(
    "/data/lerobot/pusht",
    table="pusht_daft",
    path="s3://bucket/lakesoul/pusht_daft",
)
```

`lakesoul.embodied.daft` also provides `import_lerobot` (frames layout), `import_mcap`,
`read_samples` and `read_gop_frames`.

## Reading training samples

### Single process: `EmbodiedDataset`

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
    video_window=(0, 4),                  # decoded frames for the first camera
)

dataset.set_epoch(3)                       # deterministic reshuffle
for sample in dataset:                     # dict[str, np.ndarray]
    print(sample["observation_state"].shape, sample["action"].shape)
```

- Address windows in seconds with `window={"state": (-1.0, 0.0)}` plus `time_column="timestamp"`;
- Restrict episodes with `episodes=["ep000001"]`;
- Distributed sharding: `for sample in dataset.iter_epoch(rank=rank, world_size=world_size)`;
- `EmbodiedDataset` is picklable: a dataloader worker restores the same version configuration.

### Distributed: `read_samples`

```python
from lakesoul.embodied.daft import read_samples

df = read_samples(
    table.scan(),
    window={"observation_state": (-2, 0), "action": (0, 2)},
    stride=1,
    seed=0,
)
rows = df.collect().to_pylist()            # columns: episode_id, anchor, one list column per window
```

### Decode video frames

```python
from lakesoul.embodied import GopVideo
from lakesoul.embodied.daft import read_gop_frames

gops = catalog.table("pusht_gops")
frames = catalog.table("pusht_frames")

# single process, per episode (GOP decode cache inside the episode)
video = GopVideo(gops, frames).for_episode("ep000001")
images = video.frames(0, 32)               # {camera: (n_frames, H, W, 3)}

# distributed, one row per (episode, camera, frame); image is encoded JPEG bytes
decoded = read_gop_frames(gops.scan(), frames.scan()).collect().to_pylist()
raw = read_gop_frames(gops.scan(), frames.scan(), cameras=["cam"], image_format=None)
```

### Align secondary streams

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

`lakesoul.embodied.align` also exposes an explicit `align(left_scan, right_scan, into=...)` API
to materialize an aligned table.

### PyTorch training loop

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

`prefetch` reads ahead on a background thread; `shuffle_buffer` shuffles within a seeded buffer;
`DataLoader` workers and `torch.distributed` ranks are sharded automatically.

## Storage layout and blob externalization

### Shared packs and `.blobref` sidecars

Externalized values are stored in immutable packs shared by the table:

```
<table>/_blob/<column>/<uuid>.blob     # immutable pack
<data_file>.blobref                    # JSON {"version":1,"packs":[...]}
```

Every data file records the packs it references in its own sidecar. Compaction passes tagged
references through without rewriting blob bytes and regenerates the sidecars (a conservative
union of the inputs), and file moves copy the sidecar together with the data file, so references
stay valid. The cleanup job deletes a sidecar when it deletes its data file.

### `blob_columns`

```python
properties={"blob_columns": json.dumps({
    "frame": {"mode": "external"},                   # auto | inline | external
    "thumbnail": {"mode": "auto", "inline_threshold": 16384},
})}
```

Defaults: `mode="auto"` (externalize when larger than `inline_threshold`), `inline_threshold`
16 KiB, `pack_target_bytes` 256 MiB (currently informational).

### Deferred reads with `BlobRef`

```python
import json
from lakesoul import BlobRef

scan = catalog.scan("pusht").options(reader_options={"blob_materialize": "false"})
values = scan.to_arrow_table().column("frame").to_pylist()
ref = BlobRef.parse(values[0])
tiny = ref.read(offset=0, size=16)     # range read from the pack
full = ref.materialize()               # whole value (CRC32-verified)
```

## Time travel and reproducibility

Pin an exact table state with a snapshot or tag, or read as of a timestamp:

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

- `timestamp`, `snapshot` and `tag` are mutually exclusive;
- `time_zone` is only valid with `timestamp` (an IANA name). Timestamps with their own zone
  ignore it; naive datetimes and dates require it;
- snapshots and tags pin their data, so cleanup never removes them;
- `catalog.drop_tag(...)` / `catalog.drop_snapshot(...)` release the pin (`drop_snapshot` refuses
  while a tag still points at it);
- `catalog.purge(table, older_than=..., dry_run=True)` removes unpinned versions and files older
  than the grace period.

## Maintenance

### Compaction

Compaction rewrites rows but not blob payloads: references pass through, output sidecars list the
input packs (conservative union), and sidecars move with files between compaction levels. Reading
with `blob_materialize=false` keeps the references lazy.

### Blob pack vacuum

Packs that no live data file references — checked against the latest partition versions plus all
snapshots and tags — are deleted after a one-day grace period. Blob tables do this automatically
after a successful write, once every `blob_vacuum_interval` partition versions (default `20`;
set the table property to `0` to disable):

```python
properties={"blob_columns": json.dumps({"data": {"mode": "external"}}),
            "blob_vacuum_interval": "50"}
```

The vacuum collects the live set twice and aborts without deleting anything if it changed or a
live file is missing its sidecar, so it is safe with concurrent writers. It can also be run
manually (dry run by default):

```python
from lakesoul.vacuum import vacuum_blobs

report = vacuum_blobs(catalog, table, dry_run=True)
print(report)
```

:::tip
Blob pack garbage collection does **not** depend on the Flink clean job. The clean job still
removes expired data files, commit metadata and discard entries.
:::

### Retention and cleanup

Snapshot/tag-pinned versions are never cleaned; the latest partition version is always kept.
See [Clean up unreferenced blob packs](./09-clean-redundant-data.md#clean-up-unreferenced-blob-packs)
for the blob section of the cleanup guide.

## Benchmarks and examples

- Runnable examples:
  [`python/examples/embodied`](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples/embodied)
  (`generate_data.py`, `train.py`, plus import snippets);
- Benchmark scripts:
  [`script/benchmark/embodied`](https://github.com/lakesoul-io/LakeSoul/tree/main/script/benchmark/embodied)
  — full-scan baseline vs window sampling (`run_benchmark.py`), frames vs GOP layouts with
  `--with-blob` externalization (`run_video_layout_benchmark.py`).

## Limitations and roadmap

- Manifests (a `<table>__manifests` sibling table and an `EmbodiedDataset.from_manifest` API for
  durable sample addresses across snapshots) are designed but **not implemented** yet;
- Daft importers do not accept `properties`; blob externalization at import time is currently
  available through the single-process `import_lerobot(properties=...)`;
- The legacy Spark Parquet writer path does not support blob tables (native writer required); it
  is planned for removal rather than guarded;
- `read_samples` / `read_gop_frames` live in `lakesoul.embodied.daft` and require the `daft`
  extra;
- Single-process LeRobot import supports v3.0 datasets; MCAP supports JSON and protobuf.
