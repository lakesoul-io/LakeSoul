# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Import MCAP recordings into a LakeSoul table.

Scope: MCAP files whose messages are JSON encoded (``message_encoding ==
"json"``), which covers Foxglove/ROS JSON recordings and hand-written test
logs. Protobuf-encoded messages are not decoded yet and are rejected with the
list of available topics.

Rows are anchored on one topic (``row_topic``, defaulting to the topic of the
first configured column); every other topic is attached to the nearest anchor
within ``tolerance`` seconds. Tabular values are taken from JSON numbers,
arrays, booleans or strings; camera values must be base64 strings (for example
Foxglove's ``CompressedVideo``/``CompressedImage`` ``data`` field) and are
stored as per-frame bytes.

Example:
    import_mcap(
        "ep01.mcap",
        table="robot_episodes",
        path="file:///tmp/lakesoul/robot_episodes",
        columns={"observation_state": "state", "action_pos": "commands:position"},
        cameras={"cam_high": "camera_high"},
        row_topic="control_tick",
    )
"""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa

from lakesoul.catalog import LakeSoulCatalog

from .importer import ImportSummary, prepare_table, resolve_names

EPISODE_COLUMN = "episode_id"
_IMAGE_KEYS = ("data", "image", "frame", "bytes")


@dataclass(frozen=True)
class _Message:
    timestamp: float
    payload: Any


def import_mcap(
    source: str | Path,
    *,
    table: str,
    path: str | Path,
    catalog: LakeSoulCatalog | None = None,
    namespace: str | None = None,
    columns: Mapping[str, str] | None = None,
    cameras: Mapping[str, str] | None = None,
    row_topic: str | None = None,
    episode_id: str | None = None,
    tolerance: float = 0.02,
    physical_format: str = "vortex",
    overwrite: bool = False,
) -> ImportSummary:
    """Import one MCAP file as one episode partition.

    Args:
        source: MCAP file path.
        table: LakeSoul table name to create.
        path: storage path (URI or local path) for the new table.
        catalog: catalog handle; defaults to ``LakeSoulCatalog.from_env()``.
        namespace: optional catalog namespace.
        columns: tabular outputs, mapping column name to ``"topic"`` or
            ``"topic:field.path"``.
        cameras: image outputs, mapping column name to a topic whose JSON
            payload contains a base64 image/video frame.
        row_topic: topic that defines the frames; defaults to the topic of the
            first ``columns`` entry.
        episode_id: partition value; defaults to the file stem.
        tolerance: maximum seconds between a frame and a matched message.
        physical_format: LakeSoul physical format for the written files.
        overwrite: drop and recreate the table when it already exists.
    """
    catalog = catalog or LakeSoulCatalog.from_env()
    root = Path(source).expanduser().resolve()
    if not root.exists():
        raise FileNotFoundError(f"MCAP file not found: {root}")

    columns = dict(columns or {})
    cameras = dict(cameras or {})
    if not columns and not cameras:
        raise ValueError("at least one tabular column or camera is required")
    column_names = resolve_names(list(columns))
    camera_names = resolve_names(list(cameras))
    if set(column_names) & set(camera_names):
        raise ValueError(f"column names collide: {column_names} vs {camera_names}")

    specs = {
        column_names[index]: columns[original] for index, original in enumerate(columns)
    }
    camera_specs = {
        camera_names[index]: cameras[original] for index, original in enumerate(cameras)
    }
    parsed_columns = {
        column: _parse_spec(column, spec) for column, spec in specs.items()
    }
    parsed_cameras = {
        column: _parse_spec(column, spec, allow_field=False)
        for column, spec in camera_specs.items()
    }
    wanted_topics = {topic for topic, _ in parsed_columns.values()} | {
        topic for topic, _ in parsed_cameras.values()
    }
    if row_topic is None:
        first_column = next(iter(parsed_columns), None)
        if first_column is None:
            raise ValueError("row_topic is required when only cameras are configured")
        row_topic = parsed_columns[first_column][0]
    wanted_topics.add(row_topic)

    messages, available = _read_messages(root, wanted_topics)
    for topic in wanted_topics:
        if topic not in messages:
            raise ValueError(
                f"topic {topic!r} not found in {root.name}; available topics: "
                f"{sorted(available)}"
            )

    anchor_times = sorted(message.timestamp for message in messages[row_topic])
    if not anchor_times:
        raise ValueError(f"row topic {row_topic!r} has no messages")

    resolved_episode = episode_id if episode_id is not None else root.stem
    arrays: dict[str, pa.Array] = {
        EPISODE_COLUMN: pa.array(
            [resolved_episode] * len(anchor_times), type=pa.string()
        ),
        "timestamp": pa.array(anchor_times, type=pa.float64()),
        "frame_index": pa.array(
            np.arange(len(anchor_times), dtype=np.int64), type=pa.int64()
        ),
    }
    fields = [
        pa.field(EPISODE_COLUMN, pa.string(), nullable=False),
        pa.field("timestamp", pa.float64(), nullable=False),
        pa.field("frame_index", pa.int64(), nullable=False),
    ]

    for column, (topic, field) in parsed_columns.items():
        matched = _nearest(messages[topic], anchor_times, tolerance)
        values = [
            None if message is None else _extract(message.payload, field)
            for message in matched
        ]
        if all(value is None for value in values):
            raise ValueError(
                f"column {column!r} has no message within {tolerance}s of "
                f"topic {row_topic!r}; increase tolerance"
            )
        data_type, array = _infer_array(column, values)
        fields.append(pa.field(column, data_type))
        arrays[column] = array

    video_frames = 0
    for column, (topic, _) in parsed_cameras.items():
        matched = _nearest(messages[topic], anchor_times, tolerance)
        images = [
            None if message is None else _image_bytes(column, message.payload)
            for message in matched
        ]
        if all(image is None for image in images):
            raise ValueError(
                f"camera {column!r} has no message within {tolerance}s of "
                f"topic {row_topic!r}; increase tolerance"
            )
        video_frames += sum(image is not None for image in images)
        fields.append(pa.field(column, pa.binary()))
        arrays[column] = pa.array(images, type=pa.binary())

    schema = pa.schema(fields)
    episode_table = pa.Table.from_arrays(
        [arrays[name] for name in schema.names], schema=schema
    )

    resolved_namespace = namespace or catalog.namespace
    prepare_table(catalog, table, resolved_namespace, overwrite)
    table_handle = catalog.create_table(
        table,
        path=path,
        schema=schema,
        namespace=resolved_namespace,
        partition_by=(EPISODE_COLUMN,),
    )
    table_handle.write_arrow(episode_table, format=physical_format)
    return ImportSummary(
        table=table_handle.name,
        path=table_handle.path,
        episodes=1,
        rows=len(anchor_times),
        video_frames=video_frames,
        columns=tuple(schema.names),
    )


def _parse_spec(
    column: str, spec: str, *, allow_field: bool = True
) -> tuple[str, str | None]:
    topic, separator, field = spec.partition(":")
    if not topic:
        raise ValueError(f"column {column!r} has an empty topic spec {spec!r}")
    if separator and not allow_field:
        raise ValueError(f"camera {column!r} must reference a topic without a field")
    return topic, field or None


def _read_messages(
    root: Path, wanted_topics: set[str]
) -> tuple[dict[str, list[_Message]], dict[str, str]]:
    from mcap.reader import make_reader

    messages: dict[str, list[_Message]] = {}
    available: dict[str, str] = {}
    with root.open("rb") as stream:
        reader = make_reader(stream)
        for _, channel, message in reader.iter_messages():
            available[channel.topic] = channel.message_encoding
            if channel.topic not in wanted_topics:
                continue
            if channel.message_encoding != "json":
                raise ValueError(
                    f"topic {channel.topic!r} uses encoding "
                    f"{channel.message_encoding!r}; only JSON messages are supported"
                )
            payload = json.loads(message.data.decode("utf-8"))
            messages.setdefault(channel.topic, []).append(
                _Message(timestamp=message.log_time / 1e9, payload=payload)
            )
    for topic_messages in messages.values():
        topic_messages.sort(key=lambda message: message.timestamp)
    return messages, available


def _nearest(
    messages: list[_Message], timestamps: list[float], tolerance: float
) -> list[_Message | None]:
    matched: list[_Message | None] = []
    start = 0
    for timestamp in timestamps:
        while (
            start < len(messages) and messages[start].timestamp < timestamp - tolerance
        ):
            start += 1
        best: _Message | None = None
        index = start
        while (
            index < len(messages) and messages[index].timestamp <= timestamp + tolerance
        ):
            candidate = messages[index]
            if best is None or abs(candidate.timestamp - timestamp) < abs(
                best.timestamp - timestamp
            ):
                best = candidate
            index += 1
        matched.append(best)
    return matched


def _extract(payload: Any, field: str | None) -> Any:
    if field is None:
        if isinstance(payload, dict):
            if "data" in payload and len(payload) == 1:
                return payload["data"]
            raise ValueError(
                "message is a JSON object; reference a field with "
                f"'topic:field' (available keys: {sorted(payload)})"
            )
        return payload
    value = payload
    for part in field.split("."):
        if not isinstance(value, dict) or part not in value:
            raise ValueError(f"field {field!r} not found in message payload")
        value = value[part]
    return value


def _image_bytes(column: str, payload: Any) -> bytes:
    value = payload
    if isinstance(value, dict):
        for key in _IMAGE_KEYS:
            if key in value:
                value = value[key]
                break
        else:
            raise ValueError(
                f"camera {column!r} payload has no base64 field "
                f"(looked for {_IMAGE_KEYS})"
            )
    if not isinstance(value, str):
        raise TypeError(
            f"camera {column!r} payload must be a base64 string, got {type(value)}"
        )
    try:
        return base64.b64decode(value)
    except ValueError as error:
        raise ValueError(f"camera {column!r} payload is not valid base64") from error


def _infer_array(column: str, values: list[Any]) -> tuple[pa.DataType, pa.Array]:
    present = [value for value in values if value is not None]
    if not present:
        return pa.null(), pa.nulls(len(values))
    if all(isinstance(value, bool) for value in present):
        return pa.bool_(), pa.array(values, type=pa.bool_())
    if all(
        isinstance(value, (int, np.integer)) and not isinstance(value, bool)
        for value in present
    ):
        return pa.int64(), pa.array(values, type=pa.int64())
    if all(
        isinstance(value, (int, float, np.number)) and not isinstance(value, bool)
        for value in present
    ):
        return pa.float64(), pa.array(values, type=pa.float64())
    if all(isinstance(value, str) for value in present):
        return pa.string(), pa.array(values, type=pa.string())
    if all(
        isinstance(value, (list, tuple))
        and all(
            isinstance(item, (int, float, np.number)) and not isinstance(item, bool)
            for item in value
        )
        for value in present
    ):
        dimensions = {len(value) for value in present}
        if len(dimensions) != 1:
            raise ValueError(
                f"column {column!r} has arrays of varying length: {sorted(dimensions)}"
            )
        (dimension,) = dimensions
        mask = [value is None for value in values]
        flattened = [
            float(item)
            for value in values
            for item in (value if value is not None else [0.0] * dimension)
        ]
        data_type = pa.list_(pa.float64(), dimension)
        if any(mask):
            array = pa.FixedSizeListArray.from_arrays(
                pa.array(flattened, type=pa.float64()),
                type=data_type,
                mask=pa.array(mask),
            )
        else:
            array = pa.FixedSizeListArray.from_arrays(
                pa.array(flattened, type=pa.float64()), type=data_type
            )
        return data_type, array
    kinds = sorted({type(value).__name__ for value in present})
    raise ValueError(f"column {column!r} has unsupported value types: {kinds}")


__all__ = ["import_mcap"]
