# Embodied data examples

Synthetic embodied episodes with `state` / `action` vectors and per-frame
`image` bytes, laid out with one partition per episode (the M1 hard
constraint) so episode selection prunes files at metadata level.

## Setup

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test

cd python
uv sync --group dev        # or `uv run maturin develop` for an in-place build
```

## Generate data

```sh
python python/examples/embodied/generate_data.py \
    --table embodied_demo --episodes 8 --ticks 512 --overwrite
```

## Import a LeRobot v3 dataset

```python
from lakesoul.embodied import import_lerobot

summary = import_lerobot(
    "/path/to/lerobot_dataset",  # v3.0 directory with meta/info.json
    table="robot_episodes",
    path="file:///tmp/lakesoul-embodied/robot_episodes",
    cameras=["cam_high", "cam_wrist"],  # optional, defaults to all cameras
)
print(summary)
```

Video decoding needs the `embodied` extra: `pip install "lakesoul[embodied]"`.

Pass `video_layout="gop"` to keep the source H.264 packets instead of decoded
per-frame JPEGs; the importer then creates `<table>_gops` (raw Annex-B GOPs)
and `<table>_frames` (frame to GOP/offset index). `EmbodiedDataset` can consume
them directly:

```python
from lakesoul.embodied import EmbodiedDataset, GopVideo

video = GopVideo(
    catalog.table("robot_episodes_gops"),
    catalog.table("robot_episodes_frames"),
)
dataset = EmbodiedDataset(
    catalog.table("robot_episodes").scan(),
    window={"observation_state": (-4, 0), "action": (0, 4)},
    video=video,               # decoded frames follow video_window
    video_window="observation_state",
)
```

Frames can also be decoded manually with
`lakesoul.embodied.video.decode_gop_range`.

## Import an MCAP recording

```python
from lakesoul.embodied import import_mcap

summary = import_mcap(
    "ep01.mcap",
    table="robot_episodes",
    path="file:///tmp/lakesoul-embodied/robot_episodes",
    columns={  # column -> topic[:field.path]
        "observation_state": "state",
        "action": "commands:position",
    },
    cameras={"cam_high": "camera_high"},  # base64 frame payloads
    row_topic="control_tick",
)
```

JSON and protobuf messages are supported; protobuf payloads are decoded
through the FileDescriptorSet embedded in the MCAP file (for example
``foxglove.CompressedVideo``). Other encodings are rejected with the list of
available topics.

Pass `video_layout="gop"` to group H.264/HEVC camera access units into
Annex-B GOPs (`<table>_gops` / `<table>_frames`, consumable by `GopVideo`).
Recorders should set per-channel message `sequence` numbers so decode order
is preserved; JPEG/PNG cameras should keep the default `frames` layout.

## Distributed import with Daft

```python
from lakesoul.embodied.daft import import_lerobot

summary = import_lerobot(
    "/path/to/lerobot_dataset",
    table="robot_episodes",
    path="file:///tmp/lakesoul-embodied/robot_episodes",
    cameras=["cam_high"],
)
```

`daft.datasets.lerobot` scans and decodes on the Daft runner and the LakeSoul
Daft sink writes files in parallel with a single driver commit. On the default
native runner the pipeline is correct but single-process; a Ray (or other
distributed) runner parallelizes it without code changes.

Use `lakesoul.embodied.daft.import_lerobot_gop` for the GOP layout: ticks come
from the Daft LeRobot reader and `<table>_gops` / `<table>_frames` are built by
a Daft class UDF that demuxes each video shard once per worker.

## Train

```sh
python python/examples/embodied/train.py --table embodied_demo --epochs 2
```

The script selects episodes, builds state/action windows around anchor ticks
and feeds them through `lakesoul.embodied.torch.Dataset` (shuffle buffer +
prefetch) into a linear model.

## Compare with the full-scan baseline

```sh
python benchmark/embodied/run_benchmark.py --episodes 8 --ticks 512 \
    --output /tmp/m1_5.json
```
