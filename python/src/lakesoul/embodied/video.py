# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Video helpers shared by the embodied importers.

Two layouts are supported:

* per-frame: decode a video and encode each frame as JPEG/PNG bytes;
* GOP: split a video into self-contained Annex-B groups of pictures (one
  keyframe plus its following frames) and keep a frame index that points into
  the raw packet payloads, so no decode/re-encode is needed at import time.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa

EPISODE_COLUMN = "episode_id"
_ANNEXB_FILTERS = {
    "h264": "h264_mp4toannexb",
    "hevc": "hevc_mp4toannexb",
    "vvc": "vvc_mp4toannexb",
}
_RAW_FORMATS = {"h264": "h264", "hevc": "hevc"}
_START_CODE_3 = b"\x00\x00\x01"
_START_CODE_4 = b"\x00\x00\x00\x01"

GOPS_SCHEMA = pa.schema(
    [
        pa.field(EPISODE_COLUMN, pa.string(), nullable=False),
        pa.field("camera", pa.string(), nullable=False),
        pa.field("gop_index", pa.int64(), nullable=False),
        pa.field("timestamp", pa.float64(), nullable=False),
        pa.field("codec", pa.string(), nullable=False),
        pa.field("num_frames", pa.int64(), nullable=False),
        pa.field("frame_timestamps", pa.list_(pa.float64())),
        pa.field("frame_offsets", pa.list_(pa.int64())),
        pa.field("frame_lengths", pa.list_(pa.int64())),
        pa.field("data", pa.binary(), nullable=False),
    ]
)
FRAMES_SCHEMA = pa.schema(
    [
        pa.field(EPISODE_COLUMN, pa.string(), nullable=False),
        pa.field("camera", pa.string(), nullable=False),
        pa.field("frame_index", pa.int64(), nullable=False),
        pa.field("gop_index", pa.int64(), nullable=False),
        pa.field("gop_position", pa.int64(), nullable=False),
        pa.field("timestamp", pa.float64(), nullable=False),
        pa.field("byte_offset", pa.int64(), nullable=False),
        pa.field("byte_length", pa.int64(), nullable=False),
    ]
)


@dataclass(frozen=True)
class FrameRef:
    """One frame inside a GOP."""

    position: int
    timestamp: float
    offset: int
    length: int


@dataclass(frozen=True)
class GopRecord:
    """A self-contained GOP: raw packets plus per-frame index."""

    index: int
    timestamp: float
    codec: str
    data: bytes
    frames: tuple[FrameRef, ...]


def encode_frames(
    path: str | Path,
    timestamps: list[float],
    *,
    tolerance: float,
    image_format: str,
    quality: int,
) -> list[bytes]:
    """Decode ``path`` and encode the frame nearest to every timestamp."""
    import av

    container = av.open(str(path))
    try:
        stream = container.streams.video[0]
        stream.thread_type = "AUTO"
        container.seek(int(max(0.0, timestamps[0] - tolerance) * av.time_base))
        frames: list[bytes | None] = [None] * len(timestamps)
        pending = 0
        for frame in container.decode(stream):
            time_seconds = float(frame.pts * stream.time_base)
            while (
                pending < len(timestamps)
                and time_seconds >= timestamps[pending] - tolerance
            ):
                frames[pending] = encode_image(
                    frame.to_image(), image_format=image_format, quality=quality
                )
                pending += 1
            if pending >= len(timestamps):
                break
    finally:
        container.close()

    missing = [index for index, frame in enumerate(frames) if frame is None]
    if missing:
        raise ValueError(
            f"could not decode {len(missing)} frames from {path} "
            f"(first missing index: {missing[0]})"
        )
    return [frame for frame in frames if frame is not None]


def encode_image(image: Any, *, image_format: str, quality: int) -> bytes:
    buffer = io.BytesIO()
    normalized = image_format.upper()
    options = {"quality": quality} if normalized == "JPEG" else {}
    image.save(buffer, format=normalized, **options)
    return buffer.getvalue()


def demux_gops(path: str | Path) -> list[GopRecord]:
    """Split ``path`` into Annex-B GOPs with a presentation-order frame index."""
    import av

    with av.open(str(path)) as container:
        stream = container.streams.video[0]
        codec = stream.codec_context.name
        filter_name = _ANNEXB_FILTERS.get(codec)
        if filter_name is None:
            raise ValueError(
                f"video codec {codec!r} cannot be written as GOPs; use "
                "video_layout='frames'"
            )
        bit_filter = av.bitstream.BitStreamFilterContext(filter_name, stream)
        packets = []
        for packet in container.demux(stream):
            if packet.size == 0:
                continue
            packets.extend(bit_filter.filter(packet))
        packets.extend(bit_filter.filter(None))
        time_base = stream.time_base
        entries = []
        for packet in packets:
            if packet.pts is None:
                raise ValueError(f"packet without a timestamp in {path}")
            entries.append(
                (float(packet.pts * time_base), bytes(packet), bool(packet.is_keyframe))
            )

    gops: list[GopRecord] = []
    data = b""
    frames: list[tuple[float, int, int]] = []
    for timestamp, payload, is_keyframe in entries:
        if is_keyframe:
            if data:
                gops.append(_build_gop(len(gops), data, frames, codec))
            data = b""
            frames = []
        elif not data:
            raise ValueError(
                f"the first packet of {path} is not a keyframe; cannot build "
                "self-contained GOPs"
            )
        frames.append((timestamp, len(data), len(payload)))
        data += payload
    if data:
        gops.append(_build_gop(len(gops), data, frames, codec))
    if not gops:
        raise ValueError(f"no video packets found in {path}")
    return gops


def _build_gop(
    index: int, data: bytes, frames: list[tuple[float, int, int]], codec: str
) -> GopRecord:
    ordered = sorted(frames, key=lambda frame: frame[0])
    refs = tuple(
        FrameRef(position=position, timestamp=timestamp, offset=offset, length=length)
        for position, (timestamp, offset, length) in enumerate(ordered)
    )
    return GopRecord(
        index=index,
        timestamp=refs[0].timestamp,
        codec=codec,
        data=data,
        frames=refs,
    )


def decode_gop(data: bytes, *, codec: str = "h264") -> list[np.ndarray]:
    """Decode one self-contained GOP into RGB frames in presentation order."""
    import av

    format_name = _RAW_FORMATS.get(codec)
    if format_name is None:
        raise ValueError(f"cannot decode GOP codec {codec!r}")
    with av.open(io.BytesIO(data), format=format_name) as container:
        stream = container.streams.video[0]
        stream.thread_type = "AUTO"
        return [frame.to_ndarray(format="rgb24") for frame in container.decode(stream)]


def select_episode_frames(
    gops: list[GopRecord],
    *,
    from_timestamp: float,
    length: int,
    fps: float,
) -> list[tuple[GopRecord, FrameRef]]:
    """Pick the frames of one episode out of a demuxed video file."""
    tolerance = 0.5 / fps
    flattened = [(gop, frame) for gop in gops for frame in gop.frames]
    start = next(
        (
            index
            for index, (_, frame) in enumerate(flattened)
            if frame.timestamp >= from_timestamp - tolerance
        ),
        None,
    )
    if start is None or start + length > len(flattened):
        raise ValueError(
            f"video provides {len(flattened) - (start or 0)} frames from "
            f"{from_timestamp}s but the episode needs {length}"
        )
    return flattened[start : start + length]


def decode_gop_range(
    gops_table: pa.Table,
    frames_table: pa.Table,
    start_frame: int,
    end_frame: int,
) -> list[np.ndarray]:
    """Decode frames ``[start_frame, end_frame)`` from one episode/camera.

    Both tables are expected to be pre-filtered to a single episode and
    camera; they are the ``<table>_gops`` / ``<table>_frames`` tables written
    by the importers.
    """
    gop_rows = {int(row["gop_index"]): row for row in gops_table.to_pylist()}
    selected = [
        row
        for row in frames_table.to_pylist()
        if start_frame <= int(row["frame_index"]) < end_frame
    ]
    if not selected:
        return []
    result: list[np.ndarray | None] = [None] * (end_frame - start_frame)
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in selected:
        grouped.setdefault(int(row["gop_index"]), []).append(row)
    for gop_index, rows in grouped.items():
        gop_row = gop_rows.get(gop_index)
        if gop_row is None:
            raise KeyError(f"gop {gop_index} is not present in the gops table")
        decoded = decode_gop(gop_row["data"], codec=gop_row["codec"])
        for row in rows:
            position = int(row["gop_position"])
            result[int(row["frame_index"]) - start_frame] = decoded[position]
    missing = [index for index, frame in enumerate(result) if frame is None]
    if missing:
        raise ValueError(f"could not decode frames at offsets {missing}")
    return [frame for frame in result if frame is not None]


__all__ = [
    "EPISODE_COLUMN",
    "FRAMES_SCHEMA",
    "GOPS_SCHEMA",
    "EpisodeGopVideo",
    "FrameLocation",
    "FrameRef",
    "GopRecord",
    "GopVideo",
    "build_gops_from_annexb",
    "decode_gop",
    "decode_gop_range",
    "demux_gops",
    "encode_frames",
    "encode_image",
    "is_keyframe",
    "looks_like_annexb",
    "normalize_codec",
    "select_episode_frames",
    "table_from_columns",
    "to_annexb",
]


@dataclass(frozen=True)
class FrameLocation:
    """Where one input frame ended up in the GOP tables."""

    gop_index: int
    position: int
    timestamp: float
    offset: int
    length: int


def table_from_columns(schema: pa.Schema, columns: dict[str, list[Any]]) -> pa.Table:
    arrays = [
        pa.array(columns[name], type=schema.field(name).type) for name in schema.names
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


def looks_like_annexb(data: bytes) -> bool:
    return data.startswith((_START_CODE_3, _START_CODE_4))


def normalize_codec(codec: str) -> str:
    """Map container/format spellings onto ``h264``/``hevc``."""
    normalized = codec.lower().replace(".", "").replace("-", "")
    if normalized in {"h264", "avc", "avc1", "x264"}:
        return "h264"
    if normalized in {"hevc", "h265", "hvc1", "hev1"}:
        return "hevc"
    return normalized


def to_annexb(data: bytes, *, codec: str) -> bytes:
    """Return ``data`` as Annex-B, converting AVCC length-prefixed NALs."""
    if looks_like_annexb(data):
        return data
    units = _avcc_units(data)
    if not units:
        raise ValueError(
            f"camera payload is neither Annex-B nor AVCC ({codec}); cannot build GOPs"
        )
    return b"".join(_START_CODE_4 + unit for unit in units)


def _avcc_units(data: bytes) -> list[bytes]:
    units: list[bytes] = []
    offset = 0
    while offset + 4 <= len(data):
        length = int.from_bytes(data[offset : offset + 4], "big")
        offset += 4
        if length <= 0 or offset + length > len(data):
            return []
        units.append(data[offset : offset + length])
        offset += length
    return units if offset == len(data) and units else []


def _split_annexb(data: bytes) -> list[bytes]:
    units: list[bytes] = []
    current = bytearray()
    index = 0
    size = len(data)
    while index < size:
        if data[index : index + 3] == _START_CODE_3:
            if current:
                units.append(bytes(current))
                current = bytearray()
            index += 3
            continue
        if data[index : index + 4] == _START_CODE_4:
            if current:
                units.append(bytes(current))
                current = bytearray()
            index += 4
            continue
        current.append(data[index])
        index += 1
    if current:
        units.append(bytes(current))
    return units


def is_keyframe(data: bytes, *, codec: str) -> bool:
    """True when an Annex-B access unit contains an IDR/IRAP NAL."""
    annexb = to_annexb(data, codec=codec)
    for unit in _split_annexb(annexb):
        if not unit:
            continue
        if codec == "h264":
            if unit[0] & 0x1F == 5:
                return True
        elif codec == "hevc":
            if 16 <= (unit[0] >> 1) & 0x3F <= 23:
                return True
        else:
            raise ValueError(f"keyframe detection does not support codec {codec!r}")
    return False


def build_gops_from_annexb(
    frames: list[tuple[float, bytes]],
    *,
    codec: str,
) -> tuple[list[GopRecord], list[FrameLocation | None]]:
    """Group per-frame Annex-B access units into self-contained GOPs.

    Frames before the first keyframe cannot be decoded standalone and are
    skipped (their location is ``None``). Byte payloads stay in input
    (decode) order while the per-frame index is sorted by timestamp, so
    decoded frames come out in presentation order.
    """
    gops: list[GopRecord] = []
    locations: list[FrameLocation | None] = [None] * len(frames)
    data = b""
    pending: list[tuple[float, int, int]] = []
    pending_inputs: list[int] = []

    def close_gop() -> None:
        if not data:
            return
        gop = _build_gop(len(gops), data, pending, codec)
        for (timestamp, offset, length), input_index in zip(
            pending, pending_inputs, strict=True
        ):
            reference = next(
                frame
                for frame in gop.frames
                if frame.offset == offset and frame.length == length
            )
            locations[input_index] = FrameLocation(
                gop_index=gop.index,
                position=reference.position,
                timestamp=timestamp,
                offset=offset,
                length=length,
            )
        gops.append(gop)

    for input_index, (timestamp, payload) in enumerate(frames):
        annexb = to_annexb(payload, codec=codec)
        if is_keyframe(annexb, codec=codec):
            close_gop()
            data = b""
            pending = []
            pending_inputs = []
        elif not data:
            continue
        pending.append((timestamp, len(data), len(annexb)))
        pending_inputs.append(input_index)
        data += annexb
    close_gop()
    if not gops:
        raise ValueError("no keyframe found in the camera stream")
    return gops, locations


class GopVideo:
    """GOP video source for :class:`~lakesoul.embodied.EmbodiedDataset`.

    ``gops`` and ``frames`` are the ``<table>_gops`` / ``<table>_frames``
    tables produced by the importers with ``video_layout="gop"``. Each episode
    is read once per dataset instance and decoded GOPs are cached while the
    unit is being consumed.
    """

    def __init__(
        self,
        gops: Any,
        frames: Any,
        *,
        cameras: tuple[str, ...] | list[str] | None = None,
        episode_column: str = "episode_id",
    ) -> None:
        self._gops = gops
        self._frames = frames
        self._cameras = tuple(cameras) if cameras is not None else None
        self._episode_column = episode_column
        self._tables: dict[str, tuple[pa.Table, pa.Table]] = {}

    @property
    def episode_column(self) -> str:
        return self._episode_column

    @property
    def cameras(self) -> tuple[str, ...] | None:
        return self._cameras

    def for_episode(self, episode_id: str) -> EpisodeGopVideo:
        key = str(episode_id)
        cached = self._tables.get(key)
        if cached is None:
            partitions = {self._episode_column: key}
            gops = self._gops.scan(partitions=partitions).to_arrow_table()
            frames = self._frames.scan(partitions=partitions).to_arrow_table()
            cached = (gops, frames)
            self._tables[key] = cached
        return EpisodeGopVideo(cached[0], cached[1], cameras=self._cameras)


class EpisodeGopVideo:
    """One episode's GOP rows with a decoded-frame cache."""

    def __init__(
        self,
        gops: pa.Table,
        frames: pa.Table,
        *,
        cameras: tuple[str, ...] | list[str] | None = None,
    ) -> None:
        self._gops = {int(row["gop_index"]): row for row in gops.to_pylist()}
        self._frames = frames.to_pylist()
        detected = tuple(sorted({str(row["camera"]) for row in self._frames}))
        self._cameras = tuple(cameras) if cameras is not None else detected
        missing = [camera for camera in self._cameras if camera not in detected]
        if missing:
            raise ValueError(
                f"cameras {missing} are not part of the gops/frames tables "
                f"(available: {list(detected)})"
            )
        self._decoded: dict[str, dict[int, list[np.ndarray]]] = {}

    @property
    def cameras(self) -> tuple[str, ...]:
        return self._cameras

    def frames(self, start_frame: int, end_frame: int) -> dict[str, np.ndarray]:
        """Decoded frames per camera for ``[start_frame, end_frame)``."""
        result: dict[str, np.ndarray] = {}
        for camera in self._cameras:
            rows = [
                row
                for row in self._frames
                if row["camera"] == camera
                and start_frame <= int(row["frame_index"]) < end_frame
            ]
            if rows:
                result[camera] = self._decode(camera, rows, start_frame, end_frame)
        return result

    def _decode(
        self,
        camera: str,
        rows: list[dict[str, Any]],
        start_frame: int,
        end_frame: int,
    ) -> np.ndarray:
        decoded = self._decoded.setdefault(camera, {})
        result: list[np.ndarray | None] = [None] * (end_frame - start_frame)
        for row in rows:
            gop_index = int(row["gop_index"])
            if gop_index not in decoded:
                gop = self._gops.get(gop_index)
                if gop is None:
                    raise KeyError(f"gop {gop_index} is not present in the gops table")
                decoded[gop_index] = decode_gop(gop["data"], codec=gop["codec"])
            position = int(row["gop_position"])
            result[int(row["frame_index"]) - start_frame] = decoded[gop_index][position]
        missing = [index for index, frame in enumerate(result) if frame is None]
        if missing:
            raise ValueError(f"could not decode frames at offsets {missing}")
        return np.stack([frame for frame in result if frame is not None])
