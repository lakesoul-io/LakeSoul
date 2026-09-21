# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Generate a local LeRobot v3 dataset used by the video-layout benchmark.

Frames use a moving gradient so the source MP4 compresses like real footage
(random noise would inflate the source itself). The layout follows the standard
LeRobot v3 conventions: one shared data parquet shard, one shared MP4 shard per
camera and chunked episode metadata with ``from_timestamp`` offsets.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

CAMERA = "observation.images.cam"
STATE_DIM = 4
ACTION_DIM = 2


@dataclass(frozen=True)
class SourceInfo:
    root: Path
    episodes: int
    ticks: int
    fps: int
    width: int
    height: int
    mp4_bytes: int
    parquet_bytes: int

    @property
    def frames(self) -> int:
        return self.episodes * self.ticks

    @property
    def raw_rgb_bytes(self) -> int:
        return self.frames * self.width * self.height * 3


def _gradient_frame(index: int, width: int, height: int) -> np.ndarray:
    base = (np.add.outer(np.arange(height), np.arange(width)) % 256).astype(np.uint8)
    shifted = ((base.astype(np.int32) + index * 3) % 256).astype(np.uint8)
    channels = [
        shifted,
        ((shifted.astype(np.int32) + 40) % 256).astype(np.uint8),
        ((shifted.astype(np.int32) + 80) % 256).astype(np.uint8),
    ]
    return np.stack(channels, axis=-1).astype(np.uint8)


def _write_video(
    path: Path, *, frames: int, fps: int, width: int, height: int, keyint: int
) -> None:
    import av

    path.parent.mkdir(parents=True, exist_ok=True)
    container = av.open(str(path), mode="w", format="mp4")
    stream = container.add_stream("libx264", rate=fps)
    stream.width = width
    stream.height = height
    stream.pix_fmt = "yuv420p"
    stream.options = {
        "x264-params": f"keyint={keyint}:min-keyint={keyint}:scenecut=0",
    }
    for index in range(frames):
        video_frame = av.VideoFrame.from_ndarray(
            _gradient_frame(index, width, height), format="rgb24"
        )
        for packet in stream.encode(video_frame):
            container.mux(packet)
    for packet in stream.encode():
        container.mux(packet)
    container.close()


def write_source(
    root: Path,
    *,
    episodes: int,
    ticks: int,
    fps: int = 10,
    width: int = 128,
    height: int = 128,
    keyint: int = 16,
    seed: int = 0,
) -> SourceInfo:
    root.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(seed)
    total = episodes * ticks

    state = rng.standard_normal((total, STATE_DIM)).astype(np.float32)
    action = rng.standard_normal((total, ACTION_DIM)).astype(np.float32)
    frame_index: list[int] = []
    timestamp: list[float] = []
    episode_index: list[int] = []
    task_index: list[int] = []
    global_index = list(range(total))
    for episode in range(episodes):
        frame_index.extend(range(ticks))
        timestamp.extend(position / fps for position in range(ticks))
        episode_index.extend([episode] * ticks)
        task_index.extend([episode % 2] * ticks)

    data_path = root / "data" / "chunk-000" / "file-000.parquet"
    data_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "observation.state": pa.FixedSizeListArray.from_arrays(
                    pa.array(state.reshape(-1), type=pa.float32()), STATE_DIM
                ),
                "action": pa.FixedSizeListArray.from_arrays(
                    pa.array(action.reshape(-1), type=pa.float32()), ACTION_DIM
                ),
                "timestamp": pa.array(timestamp, type=pa.float32()),
                "frame_index": pa.array(frame_index, type=pa.int64()),
                "episode_index": pa.array(episode_index, type=pa.int64()),
                "task_index": pa.array(task_index, type=pa.int64()),
                "index": pa.array(global_index, type=pa.int64()),
            }
        ),
        data_path,
    )

    video_path = root / "videos" / CAMERA / "chunk-000" / "file-000.mp4"
    _write_video(
        video_path,
        frames=total,
        fps=fps,
        width=width,
        height=height,
        keyint=keyint,
    )

    episode_columns: dict[str, pa.Array] = {
        "episode_index": pa.array(range(episodes), type=pa.int64()),
        "tasks": pa.array([[f"task-{episode % 2}"] for episode in range(episodes)]),
        "length": pa.array([ticks] * episodes, type=pa.int64()),
        "dataset_from_index": pa.array(
            [episode * ticks for episode in range(episodes)], type=pa.int64()
        ),
        "dataset_to_index": pa.array(
            [(episode + 1) * ticks for episode in range(episodes)], type=pa.int64()
        ),
        "data/chunk_index": pa.array([0] * episodes, type=pa.int64()),
        "data/file_index": pa.array([0] * episodes, type=pa.int64()),
        "meta/episodes/chunk_index": pa.array([0] * episodes, type=pa.int64()),
        "meta/episodes/file_index": pa.array([0] * episodes, type=pa.int64()),
        f"videos/{CAMERA}/chunk_index": pa.array([0] * episodes, type=pa.int64()),
        f"videos/{CAMERA}/file_index": pa.array([0] * episodes, type=pa.int64()),
        f"videos/{CAMERA}/from_timestamp": pa.array(
            [episode * ticks / fps for episode in range(episodes)], type=pa.float32()
        ),
        f"videos/{CAMERA}/to_timestamp": pa.array(
            [(episode + 1) * ticks / fps for episode in range(episodes)],
            type=pa.float32(),
        ),
    }
    episodes_path = root / "meta" / "episodes" / "chunk-000" / "file-000.parquet"
    episodes_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(episode_columns), episodes_path)

    info = {
        "codebase_version": "v3.0",
        "fps": fps,
        "total_episodes": episodes,
        "total_frames": total,
        "total_tasks": 2,
        "chunks_size": 1000,
        "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        "video_path": "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4",
        "features": {
            "observation.state": {"dtype": "float32", "shape": [STATE_DIM]},
            "action": {"dtype": "float32", "shape": [ACTION_DIM]},
            "timestamp": {"dtype": "float32", "shape": [1]},
            "frame_index": {"dtype": "int64", "shape": [1]},
            "episode_index": {"dtype": "int64", "shape": [1]},
            "index": {"dtype": "int64", "shape": [1]},
            "task_index": {"dtype": "int64", "shape": [1]},
            CAMERA: {
                "dtype": "video",
                "shape": [height, width, 3],
                "info": {"video.fps": fps, "video.codec": "h264"},
            },
        },
    }
    info_path = root / "meta" / "info.json"
    info_path.write_text(json.dumps(info), encoding="utf-8")

    parquet_bytes = sum(path.stat().st_size for path in root.rglob("*.parquet"))
    return SourceInfo(
        root=root,
        episodes=episodes,
        ticks=ticks,
        fps=fps,
        width=width,
        height=height,
        mp4_bytes=video_path.stat().st_size,
        parquet_bytes=parquet_bytes,
    )


__all__: Sequence[str] = ["CAMERA", "SourceInfo", "write_source"]
