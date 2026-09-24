# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Import MCAP recordings into a LakeSoul table.

Scope: MCAP files whose messages are JSON (``message_encoding == "json"``) or
protobuf encoded. Protobuf messages are decoded through the FileDescriptorSet
embedded in the file's schemas, which covers Foxglove schemas such as
``foxglove.CompressedVideo``.

Rows are anchored on one topic (``row_topic``, defaulting to the topic of the
first configured column); every other topic is attached to the nearest anchor
within ``tolerance`` seconds. Tabular values are taken from numbers, arrays,
booleans or strings; camera values come from base64 strings (JSON) or bytes
fields (protobuf) and are stored as per-frame bytes.

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
from google.protobuf import (
    descriptor_pb2,
    descriptor_pool,
    duration_pb2,
    empty_pb2,
    message_factory,
    timestamp_pb2,
    wrappers_pb2,
)
from google.protobuf.message import DecodeError

from lakesoul.catalog import LakeSoulCatalog

from .importer import (
    ImportSummary,
    filter_properties,
    prepare_table,
    resolve_names,
    sibling_path,
)
from .video import (
    FRAMES_SCHEMA,
    GOPS_SCHEMA,
    FrameLocation,
    GopRecord,
    build_gops_from_annexb,
    looks_like_annexb,
    normalize_codec,
    table_from_columns,
)

EPISODE_COLUMN = "episode_id"
_IMAGE_KEYS = ("data", "image", "frame", "bytes")


@dataclass(frozen=True)
class _Message:
    timestamp: float
    payload: Any
    arrival: int = 0
    sequence: int = 0


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
    video_layout: str = "frames",
    physical_format: str = "vortex",
    properties: Mapping[str, str] | None = None,
    overwrite: bool = False,
    _build_only: bool = False,
) -> ImportSummary | pa.Table:
    """Import one MCAP file as one episode partition.

    Args:
        source: MCAP file path.
        table: LakeSoul table name to create.
        path: storage path (URI or local path) for the new table.
        catalog: catalog handle; defaults to ``LakeSoulCatalog.from_env()``.
        namespace: optional catalog namespace.
        columns: tabular outputs, mapping column name to ``"topic"`` or
            ``"topic:field.path"``.
        cameras: image outputs, mapping column name to a topic whose payload
            contains an image/video frame (base64 string for JSON, bytes field
            for protobuf).
        row_topic: topic that defines the frames; defaults to the topic of the
            first ``columns`` entry.
        episode_id: partition value; defaults to the file stem.
        tolerance: maximum seconds between a frame and a matched message.
        video_layout: ``"frames"`` stores each matched frame payload as bytes;
            ``"gop"`` groups Annex-B H.264/HEVC access units into
            ``<table>_gops`` / ``<table>_frames`` tables that
            ``lakesoul.embodied.GopVideo`` can decode.
        physical_format: LakeSoul physical format for the written files.
        properties: extra table properties, e.g. ``blob_columns`` to externalize
            binary columns; entries are filtered per created table.
        overwrite: drop and recreate the table when it already exists.

    ``_build_only`` is used by the distributed importer: it returns the
    frames-layout episode table without touching metadata.
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

    if video_layout not in {"frames", "gop"}:
        raise ValueError("video_layout must be 'frames' or 'gop'")
    gop_layout = video_layout == "gop" and bool(parsed_cameras)

    video_frames = 0
    camera_streams: list[_CameraStream] = []
    if gop_layout:
        for column, (topic, _) in parsed_cameras.items():
            camera_streams.append(_camera_stream(column, messages[topic]))
    else:
        for column, (topic, _) in parsed_cameras.items():
            matched = _nearest(messages[topic], anchor_times, tolerance)
            images = [
                None if message is None else _image_payload(column, message.payload)[0]
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
    if _build_only:
        if gop_layout:
            gop_columns, frame_columns = _gop_rows(
                resolved_episode, camera_streams, anchor_times, tolerance
            )
            return _BuiltEpisode(
                ticks=episode_table, gops=gop_columns, frames=frame_columns
            )
        return _BuiltEpisode(ticks=episode_table)

    resolved_namespace = namespace or catalog.namespace
    gops_table = f"{table}_gops"
    frames_table = f"{table}_frames"
    prepare_table(catalog, table, resolved_namespace, overwrite)
    if gop_layout:
        prepare_table(catalog, gops_table, resolved_namespace, overwrite)
        prepare_table(catalog, frames_table, resolved_namespace, overwrite)
    table_handle = catalog.create_table(
        table,
        path=path,
        schema=schema,
        namespace=resolved_namespace,
        partition_by=(EPISODE_COLUMN,),
        properties=filter_properties(properties, schema),
    )
    table_handle.write_arrow(episode_table, format=physical_format)
    tables = (table_handle.name,)
    if gop_layout:
        gops_handle = catalog.create_table(
            gops_table,
            path=sibling_path(path, "_gops"),
            schema=GOPS_SCHEMA,
            namespace=resolved_namespace,
            partition_by=(EPISODE_COLUMN,),
            properties=filter_properties(properties, GOPS_SCHEMA),
        )
        frames_handle = catalog.create_table(
            frames_table,
            path=sibling_path(path, "_frames"),
            schema=FRAMES_SCHEMA,
            namespace=resolved_namespace,
            partition_by=(EPISODE_COLUMN,),
            properties=filter_properties(properties, FRAMES_SCHEMA),
        )
        video_frames = _write_gop_tables(
            gops_handle,
            frames_handle,
            resolved_episode,
            camera_streams,
            anchor_times,
            tolerance,
            physical_format,
        )
        tables = (table_handle.name, gops_handle.name, frames_handle.name)
    return ImportSummary(
        table=table_handle.name,
        path=table_handle.path,
        episodes=1,
        rows=len(anchor_times),
        video_frames=video_frames,
        columns=tuple(schema.names),
        tables=tables,
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
    decoder = _ProtobufDecoder()
    arrival = 0
    with root.open("rb") as stream:
        reader = make_reader(stream)
        for schema, channel, message in reader.iter_messages():
            available[channel.topic] = channel.message_encoding
            if channel.topic not in wanted_topics:
                continue
            payload = _decode_payload(
                decoder, schema, channel.message_encoding, channel.topic, message.data
            )
            messages.setdefault(channel.topic, []).append(
                _Message(
                    timestamp=message.log_time / 1e9,
                    payload=payload,
                    arrival=arrival,
                    sequence=message.sequence,
                )
            )
            arrival += 1
    for topic_messages in messages.values():
        topic_messages.sort(key=lambda message: message.timestamp)
    return messages, available


def _decode_payload(
    decoder: _ProtobufDecoder,
    schema: Any,
    encoding: str,
    topic: str,
    data: bytes,
) -> Any:
    if encoding == "json":
        return json.loads(data.decode("utf-8"))
    if encoding == "protobuf":
        if schema is None:
            raise ValueError(f"protobuf topic {topic!r} has no embedded schema")
        return decoder.decode(schema, data)
    raise ValueError(
        f"topic {topic!r} uses encoding {encoding!r}; only JSON and protobuf "
        "messages are supported"
    )


# Importing these modules registers the file descriptors in
# ``descriptor_pool.Default()``; the decoder copies them into its own pool.
_WELL_KNOWN_MODULES = (duration_pb2, empty_pb2, timestamp_pb2, wrappers_pb2)
_WELL_KNOWN_FILES = tuple(
    f"google/protobuf/{name}.proto"
    for name in ("timestamp", "duration", "empty", "wrappers")
)


class _ProtobufDecoder:
    """Decode protobuf messages using the schemas embedded in an MCAP file."""

    def __init__(self) -> None:
        self._pool = descriptor_pool.DescriptorPool()
        self._classes: dict[tuple[int, str], type] = {}
        self._registered: set[str] = set()
        self._add_well_known()

    def _add_well_known(self) -> None:
        default = descriptor_pool.Default()
        for name in _WELL_KNOWN_FILES:
            try:
                file_descriptor = default.FindFileByName(name)
            except KeyError:
                continue
            self._pool.Add(
                descriptor_pb2.FileDescriptorProto.FromString(
                    file_descriptor.serialized_pb
                )
            )
            self._registered.add(name)

    def decode(self, schema: Any, data: bytes) -> Any:
        message_class = self._message_class(schema)
        return _proto_to_python(message_class.FromString(data))

    def _message_class(self, schema: Any) -> type:
        key = (id(schema), schema.name)
        cached = self._classes.get(key)
        if cached is not None:
            return cached
        try:
            file_protos = descriptor_pb2.FileDescriptorSet.FromString(schema.data).file
        except (DecodeError, ValueError):
            file_protos = [descriptor_pb2.FileDescriptorProto.FromString(schema.data)]
        for file_proto in file_protos:
            if file_proto.name in self._registered:
                continue
            try:
                self._pool.Add(file_proto)
            except (TypeError, ValueError) as error:
                raise ValueError(
                    f"cannot load protobuf schema {schema.name!r}: {error}"
                ) from error
            self._registered.add(file_proto.name)
        descriptor = self._pool.FindMessageTypeByName(schema.name)
        message_class = message_factory.GetMessageClass(descriptor)
        self._classes[key] = message_class
        return message_class


def _nearest(
    messages: list[_Message], timestamps: list[float], tolerance: float
) -> list[_Message | None]:
    indices = _nearest_index(
        [message.timestamp for message in messages], timestamps, tolerance
    )
    return [None if index is None else messages[index] for index in indices]


def _nearest_index(
    candidates: list[float], timestamps: list[float], tolerance: float
) -> list[int | None]:
    matched: list[int | None] = []
    start = 0
    for timestamp in timestamps:
        while start < len(candidates) and candidates[start] < timestamp - tolerance:
            start += 1
        best: int | None = None
        index = start
        while index < len(candidates) and candidates[index] <= timestamp + tolerance:
            if best is None or abs(candidates[index] - timestamp) < abs(
                candidates[best] - timestamp
            ):
                best = index
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


def _image_payload(column: str, payload: Any) -> tuple[bytes, str | None]:
    """Return ``(bytes, format)`` for a camera payload.

    ``format`` is the payload-declared video/image format when present
    (``foxglove.CompressedVideo`` carries one), otherwise ``None``.
    """
    value = payload
    image_format: str | None = None
    if isinstance(value, dict):
        raw_format = value.get("format")
        if isinstance(raw_format, str):
            image_format = raw_format
        for key in _IMAGE_KEYS:
            if key in value:
                value = value[key]
                break
        else:
            raise ValueError(
                f"camera {column!r} payload has no bytes field "
                f"(looked for {_IMAGE_KEYS})"
            )
    if isinstance(value, (bytes, bytearray)):
        return bytes(value), image_format
    if not isinstance(value, str):
        raise TypeError(
            f"camera {column!r} payload must be bytes or a base64 string, "
            f"got {type(value)}"
        )
    try:
        return base64.b64decode(value), image_format
    except ValueError as error:
        raise ValueError(f"camera {column!r} payload is not valid base64") from error


@dataclass
class _BuiltEpisode:
    """Frames-layout ticks plus optional GOP side rows, built without IO."""

    ticks: pa.Table
    gops: dict[str, list[Any]] | None = None
    frames: dict[str, list[Any]] | None = None


@dataclass
class _CameraStream:
    column: str
    codec: str
    gops: list[GopRecord]
    locations: list[FrameLocation | None]
    sorted_indices: list[int]
    sorted_timestamps: list[float]


def _camera_stream(column: str, messages: list[_Message]) -> _CameraStream:
    # H.264 access units must stay in decode order. Recorders number messages
    # per channel (``sequence``); when the file has none, fall back to log-time
    # order, which is only safe for streams without B-frame reordering.
    sequences = {message.sequence for message in messages}
    if len(sequences) > 1:
        ordered = sorted(messages, key=lambda item: (item.sequence, item.arrival))
    else:
        ordered = sorted(messages, key=lambda item: (item.timestamp, item.arrival))
    entries: list[tuple[float, bytes]] = []
    codec: str | None = None
    for message in ordered:
        data, image_format = _image_payload(column, message.payload)
        if image_format is not None:
            resolved = normalize_codec(image_format)
            if codec is None:
                codec = resolved
            elif codec != resolved:
                raise ValueError(
                    f"camera {column!r} mixes formats {codec!r} and {resolved!r}"
                )
        entries.append((message.timestamp, data))
    if not entries:
        raise ValueError(f"camera {column!r} has no messages")
    if codec is None:
        codec = "h264" if looks_like_annexb(entries[0][1]) else "unknown"
    if codec not in {"h264", "hevc"}:
        raise ValueError(
            f"camera {column!r} uses {codec!r}; video_layout='gop' supports "
            "h264/hevc, use video_layout='frames' for JPEG/PNG or other formats"
        )
    gops, locations = build_gops_from_annexb(entries, codec=codec)
    order = sorted(range(len(entries)), key=lambda index: entries[index][0])
    return _CameraStream(
        column=column,
        codec=codec,
        gops=gops,
        locations=locations,
        sorted_indices=order,
        sorted_timestamps=[entries[index][0] for index in order],
    )


def _gop_rows(
    episode_id: str,
    streams: list[_CameraStream],
    anchor_times: list[float],
    tolerance: float,
) -> tuple[dict[str, list[Any]], dict[str, list[Any]]]:
    gop_columns: dict[str, list[Any]] = {name: [] for name in GOPS_SCHEMA.names}
    frame_columns: dict[str, list[Any]] = {name: [] for name in FRAMES_SCHEMA.names}
    for stream in streams:
        matched = _nearest_index(stream.sorted_timestamps, anchor_times, tolerance)
        for gop in stream.gops:
            gop_columns[EPISODE_COLUMN].append(episode_id)
            gop_columns["camera"].append(stream.column)
            gop_columns["gop_index"].append(gop.index)
            gop_columns["timestamp"].append(gop.timestamp)
            gop_columns["codec"].append(stream.codec)
            gop_columns["num_frames"].append(len(gop.frames))
            gop_columns["frame_timestamps"].append(
                [frame.timestamp for frame in gop.frames]
            )
            gop_columns["frame_offsets"].append([frame.offset for frame in gop.frames])
            gop_columns["frame_lengths"].append([frame.length for frame in gop.frames])
            gop_columns["data"].append(gop.data)
        for anchor_index, index in enumerate(matched):
            if index is None:
                raise ValueError(
                    f"camera {stream.column!r} has no message within {tolerance}s "
                    f"of anchor {anchor_index}; increase tolerance"
                )
            entry_index = stream.sorted_indices[index]
            location = stream.locations[entry_index]
            if location is None:
                raise ValueError(
                    f"camera {stream.column!r} frame at "
                    f"{stream.sorted_timestamps[index]} precedes the first "
                    "keyframe; trim the recording or use video_layout='frames'"
                )
            frame_columns[EPISODE_COLUMN].append(episode_id)
            frame_columns["camera"].append(stream.column)
            frame_columns["frame_index"].append(anchor_index)
            frame_columns["gop_index"].append(location.gop_index)
            frame_columns["gop_position"].append(location.position)
            frame_columns["timestamp"].append(location.timestamp)
            frame_columns["byte_offset"].append(location.offset)
            frame_columns["byte_length"].append(location.length)
    return gop_columns, frame_columns


def _write_gop_tables(
    gops_handle: Any,
    frames_handle: Any,
    episode_id: str,
    streams: list[_CameraStream],
    anchor_times: list[float],
    tolerance: float,
    physical_format: str,
) -> int:
    gop_columns, frame_columns = _gop_rows(episode_id, streams, anchor_times, tolerance)
    gops_handle.write_arrow(
        table_from_columns(GOPS_SCHEMA, gop_columns), format=physical_format
    )
    frames_handle.write_arrow(
        table_from_columns(FRAMES_SCHEMA, frame_columns), format=physical_format
    )
    return len(frame_columns["frame_index"])


def _proto_to_python(message: Any) -> Any:
    """Convert a protobuf message into Python values.

    ``google.protobuf.Timestamp`` becomes seconds (float), bytes stay bytes,
    enums become names, repeated fields become lists and map fields become
    dicts. Unlike ``MessageToDict`` this keeps 64-bit integers as ints and
    bytes as bytes.
    """
    if message.DESCRIPTOR.full_name == "google.protobuf.Timestamp":
        return message.seconds + message.nanos / 1e9
    result: dict[str, Any] = {}
    for field, value in message.ListFields():
        if field.label == field.LABEL_REPEATED:
            if (
                field.type == field.TYPE_MESSAGE
                and field.message_type.GetOptions().map_entry
            ):
                value_field = field.message_type.fields_by_name["value"]
                result[field.name] = {
                    key: _proto_value(value_field, item) for key, item in value.items()
                }
            else:
                result[field.name] = [_proto_value(field, item) for item in value]
        else:
            result[field.name] = _proto_value(field, value)
    return result


def _proto_value(field: Any, value: Any) -> Any:
    if field.type == field.TYPE_MESSAGE:
        return _proto_to_python(value)
    if field.type == field.TYPE_BYTES:
        return bytes(value)
    if field.type == field.TYPE_ENUM:
        return field.enum_type.values_by_number[value].name
    return value


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


def _built_episode(
    source: str | Path,
    *,
    columns: Mapping[str, str] | None,
    cameras: Mapping[str, str] | None,
    row_topic: str | None,
    episode_id: str | None,
    tolerance: float,
    video_layout: str,
) -> _BuiltEpisode:
    built = import_mcap(
        source,
        table="",
        path="",
        columns=columns,
        cameras=cameras,
        row_topic=row_topic,
        episode_id=episode_id,
        tolerance=tolerance,
        video_layout=video_layout,
        catalog=object(),  # type: ignore[arg-type]
        _build_only=True,
    )
    assert isinstance(built, _BuiltEpisode)
    return built


def build_frame_episode(
    source: str | Path,
    *,
    columns: Mapping[str, str] | None = None,
    cameras: Mapping[str, str] | None = None,
    row_topic: str | None = None,
    episode_id: str | None = None,
    tolerance: float = 0.02,
) -> pa.Table:
    """Build one MCAP file's frames-layout episode table without table IO."""
    return _built_episode(
        source,
        columns=columns,
        cameras=cameras,
        row_topic=row_topic,
        episode_id=episode_id,
        tolerance=tolerance,
        video_layout="frames",
    ).ticks


def build_gop_rows(
    source: str | Path,
    *,
    columns: Mapping[str, str] | None = None,
    cameras: Mapping[str, str] | None = None,
    row_topic: str | None = None,
    episode_id: str | None = None,
    tolerance: float = 0.02,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build one MCAP file's GOP and frames rows without table IO."""
    built = _built_episode(
        source,
        columns=columns,
        cameras=cameras,
        row_topic=row_topic,
        episode_id=episode_id,
        tolerance=tolerance,
        video_layout="gop",
    )
    assert built.gops is not None and built.frames is not None
    return _column_rows(built.gops), _column_rows(built.frames)


def _column_rows(columns: dict[str, list[Any]]) -> list[dict[str, Any]]:
    names = list(columns)
    return [
        dict(zip(names, values, strict=True))
        for values in zip(*(columns[name] for name in names), strict=True)
    ]


__all__ = ["build_frame_episode", "build_gop_rows", "import_mcap"]
