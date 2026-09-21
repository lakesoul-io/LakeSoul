# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import io
import json
from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import uuid4

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset, GopVideo, import_lerobot
from lakesoul.embodied.video import decode_gop_range

FPS = 10
EPISODES = ((0, 5, 0), (1, 4, 0), (2, 6, 1))
STATE_DIM = 3
ACTION_DIM = 2
CAMERA = "observation.images.cam"


def _table_name(prefix: str) -> str:
    return f"lerobot_{prefix}_{uuid4().hex[:8]}"


def _float_lists(rows: int, dim: int, seed: int) -> pa.Array:
    values = np.arange(rows * dim, dtype=np.float32).reshape(rows, dim) + seed * 100
    return pa.FixedSizeListArray.from_arrays(
        pa.array(values.reshape(-1), type=pa.float32()), dim
    )


def _write_dataset(root: Path, *, with_video: bool = False) -> dict:
    root.mkdir(parents=True, exist_ok=True)
    shards: dict[int, list[tuple[int, int]]] = {}
    for index, length, shard in EPISODES:
        shards.setdefault(shard, []).append((index, length))

    global_offset = 0
    episode_rows: dict[int, tuple[int, int]] = {}
    for shard, shard_episodes in shards.items():
        state_values: list[pa.Array] = []
        action_values: list[pa.Array] = []
        frame_index: list[int] = []
        timestamp: list[float] = []
        episode_index: list[int] = []
        task_index: list[int] = []
        global_index: list[int] = []
        for index, length in shard_episodes:
            episode_rows[index] = (global_offset, length)
            state_values.append(_float_lists(length, STATE_DIM, index))
            action_values.append(_float_lists(length, ACTION_DIM, index))
            frame_index.extend(range(length))
            timestamp.extend(position / FPS for position in range(length))
            episode_index.extend([index] * length)
            task_index.extend([index % 2] * length)
            global_index.extend(range(global_offset, global_offset + length))
            global_offset += length
        columns = {
            "observation.state": pa.chunked_array(state_values),
            "action": pa.chunked_array(action_values),
            "timestamp": pa.array(timestamp, type=pa.float32()),
            "frame_index": pa.array(frame_index, type=pa.int64()),
            "episode_index": pa.array(episode_index, type=pa.int64()),
            "task_index": pa.array(task_index, type=pa.int64()),
            "index": pa.array(global_index, type=pa.int64()),
        }
        data_path = root / "data" / "chunk-000" / f"file-{shard:03d}.parquet"
        data_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.table(columns), data_path)

    episode_columns: dict[str, pa.Array] = {
        "episode_index": pa.array([index for index, _, _ in EPISODES], type=pa.int64()),
        "tasks": pa.array([[f"task-{index % 2}"] for index, _, _ in EPISODES]),
        "length": pa.array([length for _, length, _ in EPISODES], type=pa.int64()),
        "dataset_from_index": pa.array(
            [episode_rows[index][0] for index, _, _ in EPISODES], type=pa.int64()
        ),
        "dataset_to_index": pa.array(
            [
                episode_rows[index][0] + episode_rows[index][1]
                for index, _, _ in EPISODES
            ],
            type=pa.int64(),
        ),
        "data/chunk_index": pa.array([0] * len(EPISODES), type=pa.int64()),
        "data/file_index": pa.array(
            [shard for _, _, shard in EPISODES], type=pa.int64()
        ),
        "meta/episodes/chunk_index": pa.array([0] * len(EPISODES), type=pa.int64()),
        "meta/episodes/file_index": pa.array([0] * len(EPISODES), type=pa.int64()),
    }
    if with_video:
        episode_columns[f"videos/{CAMERA}/chunk_index"] = pa.array(
            [0] * len(EPISODES), type=pa.int64()
        )
        episode_columns[f"videos/{CAMERA}/file_index"] = pa.array(
            [0] * len(EPISODES), type=pa.int64()
        )
        episode_columns[f"videos/{CAMERA}/from_timestamp"] = pa.array(
            [episode_rows[index][0] / FPS for index, _, _ in EPISODES],
            type=pa.float32(),
        )
        episode_columns[f"videos/{CAMERA}/to_timestamp"] = pa.array(
            [
                (episode_rows[index][0] + episode_rows[index][1]) / FPS
                for index, _, _ in EPISODES
            ],
            type=pa.float32(),
        )
        _write_video(root / "videos" / CAMERA / "chunk-000" / "file-000.mp4")
    episodes_path = root / "meta" / "episodes" / "chunk-000" / "file-000.parquet"
    episodes_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(episode_columns), episodes_path)

    features = {
        "observation.state": {"dtype": "float32", "shape": [STATE_DIM]},
        "action": {"dtype": "float32", "shape": [ACTION_DIM]},
        "timestamp": {"dtype": "float32", "shape": [1]},
        "frame_index": {"dtype": "int64", "shape": [1]},
        "episode_index": {"dtype": "int64", "shape": [1]},
        "index": {"dtype": "int64", "shape": [1]},
        "task_index": {"dtype": "int64", "shape": [1]},
    }
    if with_video:
        features[CAMERA] = {
            "dtype": "video",
            "shape": [8, 8, 3],
            "info": {"video.fps": FPS, "video.codec": "mpeg4"},
        }
    info = {
        "codebase_version": "v3.0",
        "fps": FPS,
        "total_episodes": len(EPISODES),
        "total_frames": sum(length for _, length, _ in EPISODES),
        "total_tasks": 2,
        "chunks_size": 1000,
        "data_path": "data/chunk-{chunk_index:03d}/file-{file_index:03d}.parquet",
        "video_path": "videos/{video_key}/chunk-{chunk_index:03d}/file-{file_index:03d}.mp4",
        "features": features,
    }
    info_path = root / "meta" / "info.json"
    info_path.parent.mkdir(parents=True, exist_ok=True)
    info_path.write_text(json.dumps(info), encoding="utf-8")
    return info


def _write_video(path: Path, frames: int = 15) -> None:
    av = pytest.importorskip("av")
    codec = "libx264" if "libx264" in av.codecs_available else "mpeg4"
    path.parent.mkdir(parents=True, exist_ok=True)
    container = av.open(str(path), mode="w", format="mp4")
    stream = container.add_stream(codec, rate=FPS)
    stream.width = 8
    stream.height = 8
    stream.pix_fmt = "yuv420p"
    if codec == "libx264":
        # Force short GOPs so the fixture has several keyframes.
        stream.options = {"x264-params": "keyint=4:min-keyint=4:scenecut=0"}
    for index in range(frames):
        pixels = np.full((8, 8, 3), index * 10, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(pixels, format="rgb24")
        for packet in stream.encode(frame):
            container.mux(packet)
    for packet in stream.encode():
        container.mux(packet)
    container.close()


def _catalog_table(table_name: str, catalog: LakeSoulCatalog):
    return catalog.table(table_name)


def test_import_lerobot_reads_episodes_and_trains(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("plain")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_lerobot(
            root,
            table=table_name,
            path=table_path,
            physical_format="parquet",
        )
        assert summary.episodes == 3
        assert summary.rows == 15
        assert summary.video_frames == 0
        assert "observation_state" in summary.columns
        assert "action" in summary.columns

        table = _catalog_table(table_name, catalog)
        scanned = table.scan().to_arrow_table()
        assert scanned.num_rows == 15
        assert set(scanned.column("episode_id").to_pylist()) == {
            "ep000000",
            "ep000001",
            "ep000002",
        }

        dataset = EmbodiedDataset(
            table.scan(),
            window={"observation_state": (0, 2), "action": (0, 2)},
            stride=1,
            episodes=["ep000001"],
        )
        samples = list(dataset.iter_epoch(0))
        assert len(samples) == 3
        assert samples[0]["observation_state"].shape == (2, STATE_DIM)
        assert samples[0]["action"].shape == (2, ACTION_DIM)
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_import_lerobot_rejects_legacy_version(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root)
    info_path = root / "meta" / "info.json"
    info = json.loads(info_path.read_text())
    info["codebase_version"] = "v2.1"
    info_path.write_text(json.dumps(info))

    with pytest.raises(ValueError, match="v3"):
        import_lerobot(
            root,
            table=_table_name("legacy"),
            path=(tmp_path / "lake").as_uri(),
            catalog=object(),  # type: ignore[arg-type]
        )


def test_import_lerobot_video_frames(tmp_path: Path) -> None:
    pytest.importorskip("av")
    pytest.importorskip("PIL")

    root = tmp_path / "dataset"
    _write_dataset(root, with_video=True)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("video")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_lerobot(
            root,
            table=table_name,
            path=table_path,
            physical_format="parquet",
        )
        assert summary.video_frames == 15
        assert "cam" in summary.columns

        table = _catalog_table(table_name, catalog)
        scanned = table.scan().to_arrow_table()
        images = scanned.column("cam").to_pylist()
        assert len(images) == 15
        assert all(image for image in images)

        from PIL import Image

        decoded = Image.open(io.BytesIO(images[0]))
        assert decoded.size == (8, 8)
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_import_lerobot_gop_layout(tmp_path: Path) -> None:
    av = pytest.importorskip("av")
    if "libx264" not in av.codecs_available:
        pytest.skip("libx264 is required for the GOP fixture")

    root = tmp_path / "dataset"
    _write_dataset(root, with_video=True)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("gop")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_lerobot(
            root,
            table=table_name,
            path=table_path,
            physical_format="parquet",
            video_layout="gop",
        )
        assert summary.tables == (
            table_name,
            f"{table_name}_gops",
            f"{table_name}_frames",
        )
        assert summary.rows == 15
        assert summary.video_frames == 15

        ticks = catalog.table(table_name).scan().to_arrow_table()
        assert "cam" not in ticks.column_names

        gops = catalog.table(f"{table_name}_gops").scan().to_arrow_table()
        frames = catalog.table(f"{table_name}_frames").scan().to_arrow_table()
        assert frames.num_rows == 15
        assert set(gops.column("camera").to_pylist()) == {"cam"}

        selection = pc.equal(frames.column("episode_id"), "ep000001")
        decoded = decode_gop_range(
            gops.filter(pc.equal(gops.column("episode_id"), "ep000001")),
            frames.filter(selection),
            0,
            4,
        )
        assert len(decoded) == 4
        assert [float(frame[0, 0, 0]) for frame in decoded] == pytest.approx(
            [50.0, 60.0, 70.0, 80.0], abs=6.0
        )
        episode_timestamps = frames.filter(selection).column("timestamp").to_pylist()
        assert episode_timestamps == pytest.approx([0.5, 0.6, 0.7, 0.8], abs=1e-6)

        ticks_table = catalog.table(table_name)
        video = GopVideo(
            catalog.table(f"{table_name}_gops"),
            catalog.table(f"{table_name}_frames"),
        )
        dataset = EmbodiedDataset(
            ticks_table.scan(),
            window={"observation_state": (-2, 0), "action": (0, 2)},
            stride=1,
            episodes=["ep000001"],
            video=video,
        )
        samples = list(dataset.iter_epoch(0))
        assert len(samples) == 1
        assert samples[0]["observation_state"].shape == (2, STATE_DIM)
        assert samples[0]["cam"].shape == (2, 8, 8, 3)
        assert [float(frame[0, 0, 0]) for frame in samples[0]["cam"]] == pytest.approx(
            [50.0, 60.0], abs=6.0
        )
    finally:
        catalog.drop_table(f"{table_name}_frames", if_exists=True)
        catalog.drop_table(f"{table_name}_gops", if_exists=True)
        catalog.drop_table(table_name, if_exists=True)


def test_import_lerobot_unknown_camera(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root, with_video=True)

    with pytest.raises(ValueError, match="unknown camera"):
        import_lerobot(
            root,
            table=_table_name("camera"),
            path=(tmp_path / "lake").as_uri(),
            catalog=object(),  # type: ignore[arg-type]
            cameras=["missing"],
        )


def test_import_lerobot_overwrite_guard(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("overwrite")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        import_lerobot(
            root, table=table_name, path=table_path, physical_format="parquet"
        )
        with pytest.raises(ValueError, match="already exists"):
            import_lerobot(
                root, table=table_name, path=table_path, physical_format="parquet"
            )
        import_lerobot(
            root,
            table=table_name,
            path=table_path,
            physical_format="parquet",
            overwrite=True,
        )
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_import_lerobot_gop_blob_external(tmp_path: Path) -> None:
    root = tmp_path / "dataset"
    _write_dataset(root, with_video=True)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("gop_blob")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_lerobot(
            root,
            table=table_name,
            path=table_path,
            cameras=["cam"],
            video_layout="gop",
            physical_format="parquet",
            properties={"blob_columns": json.dumps({"data": {"mode": "external"}})},
        )
        assert summary.tables == (
            table_name,
            f"{table_name}_gops",
            f"{table_name}_frames",
        )

        gops_table = catalog.table(f"{table_name}_gops")
        gops_path = Path(unquote(urlparse(gops_table.path).path))
        packs = sorted(gops_path.rglob("*.blob"))
        assert packs, "expected per-file blob packs"
        raw_rows = [
            row
            for file in gops_path.rglob("*.parquet")
            for row in pq.read_table(file).to_pylist()
        ]
        assert raw_rows
        assert all(row["data"][0] == 1 for row in raw_rows)

        video = GopVideo(
            catalog.table(f"{table_name}_gops"), catalog.table(f"{table_name}_frames")
        )
        decoded = video.for_episode("ep000001").frames(0, 4)
        assert [float(frame[0, 0, 0]) for frame in decoded["cam"]] == pytest.approx(
            [50.0, 60.0, 70.0, 80.0], abs=6.0
        )
    finally:
        catalog.drop_table(f"{table_name}_frames", if_exists=True)
        catalog.drop_table(f"{table_name}_gops", if_exists=True)
        catalog.drop_table(table_name, if_exists=True)
