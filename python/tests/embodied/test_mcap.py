# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import base64
import json
from pathlib import Path
from uuid import uuid4

import numpy as np
import pytest

from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset, GopVideo, import_mcap
from lakesoul.embodied.video import demux_gops

FPS = 10
TICKS = 6
FAKE_JPEG = b"\xff\xd8fake-jpeg-frame\xff\xd9"


def _table_name(prefix: str) -> str:
    return f"mcap_{prefix}_{uuid4().hex[:8]}"


def _write_mcap(
    path: Path,
    *,
    ticks: int = TICKS,
    camera_offset: float = 0.005,
    tick_encoding: str = "json",
) -> None:
    writer_module = pytest.importorskip("mcap.writer")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        writer = writer_module.Writer(stream)
        writer.start()
        schema_id = writer.register_schema(
            name="JsonLog", encoding="jsonschema", data=b"{}"
        )
        tick_channel = writer.register_channel(
            topic="control_tick",
            message_encoding=tick_encoding,
            schema_id=schema_id,
        )
        camera_channel = writer.register_channel(
            topic="camera_high",
            message_encoding="json",
            schema_id=schema_id,
        )
        for index in range(ticks):
            timestamp = int(index * 1e9 / FPS)
            tick = {
                "state": [float(index), float(index + 1), float(index + 2)],
                "action": [0.1 * index, 0.2 * index],
                "reward": 0.5 + index,
            }
            writer.add_message(
                tick_channel,
                log_time=timestamp,
                publish_time=timestamp,
                data=json.dumps(tick).encode(),
            )
            camera_timestamp = timestamp + int(camera_offset * 1e9)
            frame = {
                "format": "jpeg",
                "data": base64.b64encode(FAKE_JPEG).decode(),
            }
            writer.add_message(
                camera_channel,
                log_time=camera_timestamp,
                publish_time=camera_timestamp,
                data=json.dumps(frame).encode(),
            )
        writer.finish()


def test_import_mcap_tabular_and_camera(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("full")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_mcap(
            source,
            table=table_name,
            path=table_path,
            columns={
                "observation.state": "control_tick:state",
                "action": "control_tick:action",
                "reward": "control_tick:reward",
            },
            cameras={"cam_high": "camera_high"},
            row_topic="control_tick",
            physical_format="parquet",
        )
        assert summary.episodes == 1
        assert summary.rows == TICKS
        assert summary.video_frames == TICKS
        assert "observation_state" in summary.columns
        assert "cam_high" in summary.columns

        table = catalog.table(table_name)
        scanned = table.scan().to_arrow_table()
        assert scanned.num_rows == TICKS
        assert scanned.column("observation_state").to_pylist()[0] == [
            0.0,
            1.0,
            2.0,
        ]
        assert scanned.column("action").to_pylist()[3] == pytest.approx([0.3, 0.6])
        assert scanned.column("reward").to_pylist() == pytest.approx(
            [0.5 + index for index in range(TICKS)]
        )
        assert scanned.column("cam_high").to_pylist() == [FAKE_JPEG] * TICKS

        dataset = EmbodiedDataset(
            table.scan(),
            window={"observation_state": (0, 2), "action": (0, 2)},
            stride=1,
            episodes=["ep01"],
        )
        samples = list(dataset.iter_epoch(0))
        assert len(samples) == TICKS - 1
        assert samples[0]["observation_state"].shape == (2, 3)
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_import_mcap_tolerance_too_small(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source, camera_offset=0.05)

    with pytest.raises(ValueError, match="no message within"):
        import_mcap(
            source,
            table=_table_name("tolerance"),
            path=(tmp_path / "lake").as_uri(),
            columns={"reward": "control_tick:reward"},
            cameras={"cam_high": "camera_high"},
            row_topic="control_tick",
            tolerance=0.001,
            catalog=object(),  # type: ignore[arg-type]
        )


def test_import_mcap_rejects_unknown_encoding(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source, tick_encoding="cdr")

    with pytest.raises(ValueError, match="only JSON and protobuf"):
        import_mcap(
            source,
            table=_table_name("cdr"),
            path=(tmp_path / "lake").as_uri(),
            columns={"reward": "control_tick:reward"},
            catalog=object(),  # type: ignore[arg-type]
        )


def _tick_file_descriptor():
    from google.protobuf import descriptor_pb2

    descriptor = descriptor_pb2.FileDescriptorProto(
        name="demo/tick.proto", package="demo", syntax="proto3"
    )
    descriptor.dependency.append("google/protobuf/timestamp.proto")
    mode = descriptor.enum_type.add()
    mode.name = "Mode"
    unknown = mode.value.add()
    unknown.name = "MODE_UNKNOWN"
    unknown.number = 0
    running = mode.value.add()
    running.name = "MODE_RUN"
    running.number = 1

    message = descriptor.message_type.add()
    message.name = "Tick"

    def add_field(name, number, field_type, label, type_name=None):
        field = message.field.add()
        field.name = name
        field.number = number
        field.type = field_type
        field.label = label
        if type_name is not None:
            field.type_name = type_name

    add_field("state", 1, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE, 3)
    add_field("action", 2, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE, 3)
    add_field("reward", 3, descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE, 1)
    add_field("label", 4, descriptor_pb2.FieldDescriptorProto.TYPE_STRING, 1)
    add_field(
        "mode",
        5,
        descriptor_pb2.FieldDescriptorProto.TYPE_ENUM,
        1,
        ".demo.Mode",
    )
    add_field("data", 6, descriptor_pb2.FieldDescriptorProto.TYPE_BYTES, 1)
    add_field(
        "stamp",
        7,
        descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        1,
        ".google.protobuf.Timestamp",
    )
    return descriptor


def _tick_message_class(descriptor):
    from google.protobuf import (
        descriptor_pb2,
        descriptor_pool,
        message_factory,
        timestamp_pb2,  # noqa: F401
    )

    pool = descriptor_pool.DescriptorPool()
    timestamp = descriptor_pool.Default().FindFileByName(
        "google/protobuf/timestamp.proto"
    )
    pool.Add(descriptor_pb2.FileDescriptorProto.FromString(timestamp.serialized_pb))
    pool.Add(descriptor)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("demo.Tick"))


def _write_protobuf_mcap(path: Path, *, ticks: int = TICKS) -> None:
    writer_module = pytest.importorskip("mcap.writer")

    from google.protobuf import descriptor_pb2

    descriptor = _tick_file_descriptor()
    tick_class = _tick_message_class(descriptor)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        writer = writer_module.Writer(stream)
        writer.start()
        file_set = descriptor_pb2.FileDescriptorSet()
        file_set.file.add().CopyFrom(descriptor)
        schema_id = writer.register_schema(
            name="demo.Tick",
            encoding="protobuf",
            data=file_set.SerializeToString(),
        )
        tick_channel = writer.register_channel(
            topic="control_tick",
            message_encoding="protobuf",
            schema_id=schema_id,
        )
        camera_channel = writer.register_channel(
            topic="camera_high",
            message_encoding="protobuf",
            schema_id=schema_id,
        )
        for index in range(ticks):
            timestamp = int(index * 1e9 / FPS)
            tick = tick_class(
                state=[float(index), float(index + 1), float(index + 2)],
                action=[0.1 * index, 0.2 * index],
                reward=0.5 + index,
                label=f"tick-{index}",
                mode=1,
            )
            tick.stamp.seconds = index
            writer.add_message(
                tick_channel,
                log_time=timestamp,
                publish_time=timestamp,
                data=tick.SerializeToString(),
            )
            writer.add_message(
                camera_channel,
                log_time=timestamp + 200_000,
                publish_time=timestamp + 200_000,
                data=tick_class(data=FAKE_JPEG, label="cam").SerializeToString(),
            )
        writer.finish()


def test_import_mcap_protobuf_tabular_and_camera(tmp_path: Path) -> None:
    source = tmp_path / "ep02.mcap"
    _write_protobuf_mcap(source)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("protobuf")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_mcap(
            source,
            table=table_name,
            path=table_path,
            columns={
                "observation_state": "control_tick:state",
                "action": "control_tick:action",
                "reward": "control_tick:reward",
                "label": "control_tick:label",
                "mode": "control_tick:mode",
                "stamp": "control_tick:stamp",
            },
            cameras={"cam_high": "camera_high"},
            row_topic="control_tick",
            physical_format="parquet",
        )
        assert summary.rows == TICKS
        assert summary.video_frames == TICKS

        scanned = catalog.table(table_name).scan().to_arrow_table()
        assert scanned.column("observation_state").to_pylist()[1] == [1.0, 2.0, 3.0]
        assert scanned.column("reward").to_pylist() == pytest.approx(
            [0.5 + index for index in range(TICKS)]
        )
        assert scanned.column("label").to_pylist() == [
            f"tick-{index}" for index in range(TICKS)
        ]
        assert scanned.column("mode").to_pylist() == ["MODE_RUN"] * TICKS
        assert scanned.column("stamp").to_pylist() == pytest.approx(
            [float(index) for index in range(TICKS)]
        )
        assert scanned.column("cam_high").to_pylist() == [FAKE_JPEG] * TICKS
    finally:
        catalog.drop_table(table_name, if_exists=True)


def _write_h264(path: Path, frames: int = 12) -> None:
    av = pytest.importorskip("av")
    if "libx264" not in av.codecs_available:
        pytest.skip("libx264 is required for the GOP fixture")
    path.parent.mkdir(parents=True, exist_ok=True)
    container = av.open(str(path), mode="w", format="mp4")
    stream = container.add_stream("libx264", rate=FPS)
    stream.width = 8
    stream.height = 8
    stream.pix_fmt = "yuv420p"
    stream.options = {"x264-params": "keyint=4:min-keyint=4:scenecut=0"}
    for index in range(frames):
        pixels = np.full((8, 8, 3), index * 10, dtype=np.uint8)
        frame = av.VideoFrame.from_ndarray(pixels, format="rgb24")
        for packet in stream.encode(frame):
            container.mux(packet)
    for packet in stream.encode():
        container.mux(packet)
    container.close()


def _h264_annexb_frames(tmp_path: Path, frames: int = 12):
    mp4 = tmp_path / "camera.mp4"
    _write_h264(mp4, frames)
    collected = []
    for gop in demux_gops(mp4):
        # Offsets follow decode order; MCAP records access units the same way.
        for ref in sorted(gop.frames, key=lambda frame: frame.offset):
            collected.append(
                (
                    ref.timestamp,
                    gop.data[ref.offset : ref.offset + ref.length],
                )
            )
    return collected


def _write_h264_mcap(path: Path, frames) -> None:
    writer_module = pytest.importorskip("mcap.writer")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as stream:
        writer = writer_module.Writer(stream)
        writer.start()
        schema_id = writer.register_schema(
            name="JsonLog", encoding="jsonschema", data=b"{}"
        )
        tick_channel = writer.register_channel(
            topic="control_tick", message_encoding="json", schema_id=schema_id
        )
        camera_channel = writer.register_channel(
            topic="camera_high", message_encoding="json", schema_id=schema_id
        )
        for index, (timestamp, payload) in enumerate(frames):
            log_time = int(timestamp * 1e9)
            writer.add_message(
                tick_channel,
                log_time=log_time,
                publish_time=log_time,
                data=json.dumps({"reward": 0.5 + index}).encode(),
            )
            frame = {
                "format": "h264",
                "data": base64.b64encode(payload).decode(),
            }
            writer.add_message(
                camera_channel,
                log_time=log_time,
                publish_time=log_time,
                sequence=index,
                data=json.dumps(frame).encode(),
            )
        writer.finish()


def test_import_mcap_gop_layout(tmp_path: Path) -> None:
    frames = _h264_annexb_frames(tmp_path)
    source = tmp_path / "ep_gop.mcap"
    _write_h264_mcap(source, frames)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("gop")
    table_path = (tmp_path / "lake" / table_name).as_uri()

    try:
        summary = import_mcap(
            source,
            table=table_name,
            path=table_path,
            columns={"reward": "control_tick:reward"},
            cameras={"cam_high": "camera_high"},
            row_topic="control_tick",
            video_layout="gop",
            physical_format="parquet",
        )
        assert summary.tables == (
            table_name,
            f"{table_name}_gops",
            f"{table_name}_frames",
        )
        assert summary.video_frames == len(frames)

        ticks_table = catalog.table(table_name)
        ticks = ticks_table.scan().to_arrow_table()
        assert "cam_high" not in ticks.column_names

        video = GopVideo(
            catalog.table(f"{table_name}_gops"),
            catalog.table(f"{table_name}_frames"),
        )
        decoded = video.for_episode(source.stem).frames(0, 4)
        assert decoded["cam_high"].shape == (4, 8, 8, 3)
        assert [float(frame[0, 0, 0]) for frame in decoded["cam_high"]] == (
            pytest.approx([0.0, 10.0, 20.0, 30.0], abs=6.0)
        )

        dataset = EmbodiedDataset(
            ticks_table.scan(),
            window={"reward": (-2, 0)},
            video=video,
            boundary="clamp",
        )
        samples = list(dataset.iter_epoch(0))
        assert samples
        assert all(
            sample["cam_high"].shape[0] == sample["reward"].shape[0]
            for sample in samples
        )
    finally:
        catalog.drop_table(f"{table_name}_frames", if_exists=True)
        catalog.drop_table(f"{table_name}_gops", if_exists=True)
        catalog.drop_table(table_name, if_exists=True)


def test_import_mcap_gop_rejects_jpeg(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source)

    with pytest.raises(ValueError, match="video_layout='frames'"):
        import_mcap(
            source,
            table=_table_name("jpeg"),
            path=(tmp_path / "lake").as_uri(),
            columns={"reward": "control_tick:reward"},
            cameras={"cam_high": "camera_high"},
            row_topic="control_tick",
            video_layout="gop",
            catalog=object(),  # type: ignore[arg-type]
        )


def test_import_mcap_unknown_topic(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source)

    with pytest.raises(ValueError, match="not found"):
        import_mcap(
            source,
            table=_table_name("missing"),
            path=(tmp_path / "lake").as_uri(),
            columns={"reward": "missing_topic:reward"},
            catalog=object(),  # type: ignore[arg-type]
        )


def test_import_mcap_overwrite_guard(tmp_path: Path) -> None:
    source = tmp_path / "ep01.mcap"
    _write_mcap(source)
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("overwrite")
    table_path = (tmp_path / "lake" / table_name).as_uri()
    kwargs = {
        "table": table_name,
        "path": table_path,
        "columns": {"reward": "control_tick:reward"},
        "row_topic": "control_tick",
        "physical_format": "parquet",
    }

    try:
        import_mcap(source, **kwargs)
        with pytest.raises(ValueError, match="already exists"):
            import_mcap(source, **kwargs)
        import_mcap(source, overwrite=True, **kwargs)
    finally:
        catalog.drop_table(table_name, if_exists=True)
