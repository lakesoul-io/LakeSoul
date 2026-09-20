# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Import a local LeRobotDataset v3.0 directory into a LakeSoul table.

The output layout follows the M1 hard constraints: one partition per episode
and time-ordered frames. Video is either decoded into per-frame JPEG/PNG bytes
(``video_layout="frames"``, one table) or kept as raw Annex-B GOPs with a
frame index (``video_layout="gop"``, ``<table>`` + ``<table>_gops`` +
``<table>_frames``), which avoids decoding and re-encoding altogether.
Tabular features keep their LeRobot names with dots replaced by underscores,
for example ``observation.state`` becomes ``observation_state``.

Only ``v3.0`` directories on local storage are supported; convert a ``v2.1``
dataset with LeRobot's ``convert_dataset_v21_to_v30`` script first.

Video decoding requires the optional ``av`` and ``Pillow`` dependencies
(``pip install lakesoul[embodied]``).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from lakesoul.catalog import LakeSoulCatalog

from .importer import ImportSummary, prepare_table, sanitize
from .video import GopRecord, demux_gops, encode_frames, select_episode_frames

EPISODE_COLUMN = "episode_id"
NON_FEATURE_COLUMNS = (
    "timestamp",
    "frame_index",
    "index",
    "episode_index",
    "task_index",
)
_REQUIRED_EPISODE_COLUMNS = (
    "episode_index",
    "length",
    "dataset_from_index",
    "dataset_to_index",
    "data/chunk_index",
    "data/file_index",
)
GOPS_TABLE_SUFFIX = "_gops"
FRAMES_TABLE_SUFFIX = "_frames"
_GOPS_SCHEMA = pa.schema(
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
_FRAMES_SCHEMA = pa.schema(
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
class _Feature:
    key: str
    column: str
    dtype: str
    shape: tuple[int, ...]

    @property
    def is_video(self) -> bool:
        return self.dtype == "video"


@dataclass(frozen=True)
class _Episode:
    index: int
    length: int
    tasks: tuple[str, ...]
    local_offset: int
    data_file: Path
    video_files: Mapping[str, Path]
    video_from_timestamp: Mapping[str, float]


def import_lerobot(
    source: str | Path,
    *,
    table: str,
    path: str | Path,
    catalog: LakeSoulCatalog | None = None,
    namespace: str | None = None,
    episodes: Sequence[int] | None = None,
    cameras: Sequence[str] | None = None,
    include_video: bool = True,
    video_layout: str = "frames",
    image_format: str = "JPEG",
    image_quality: int = 90,
    physical_format: str = "vortex",
    overwrite: bool = False,
) -> ImportSummary:
    """Import a local LeRobot v3.0 dataset into a new LakeSoul table.

    Args:
        source: dataset root containing ``meta/info.json``.
        table: LakeSoul table name to create.
        path: storage path (URI or local path) for the new table.
        catalog: catalog handle; defaults to ``LakeSoulCatalog.from_env()``.
        namespace: optional catalog namespace.
        episodes: episode indices to import; defaults to all episodes.
        cameras: video feature keys (or their last dotted component) to decode;
            defaults to every video feature.
        include_video: set ``False`` to skip video import entirely.
        video_layout: ``"frames"`` decodes and re-encodes every frame,
            ``"gop"`` stores raw Annex-B GOPs plus a frame index in the
            ``<table>_gops`` / ``<table>_frames`` tables.
        image_format: ``JPEG`` or ``PNG`` for per-frame image bytes.
        image_quality: JPEG quality (``frames`` layout only).
        physical_format: LakeSoul physical format for the written files.
        overwrite: drop and recreate the table when it already exists.
    """
    catalog = catalog or LakeSoulCatalog.from_env()
    root = Path(source).expanduser().resolve()
    info = _load_info(root)
    _check_version(info)

    features = _parse_features(info)
    video_features = [feature for feature in features if feature.is_video]
    if cameras is not None:
        video_features = _select_cameras(video_features, cameras)
    if not include_video:
        video_features = []
    data_features = [
        feature
        for feature in features
        if not feature.is_video and feature.key not in NON_FEATURE_COLUMNS
    ]

    schema = _build_schema(data_features, video_features)
    episodes_meta = _load_episodes(root, info, video_features)
    selected = _select_episodes(episodes_meta, episodes)
    if not selected:
        raise ValueError("no episodes selected for import")

    if video_layout not in {"frames", "gop"}:
        raise ValueError("video_layout must be 'frames' or 'gop'")
    gop_layout = video_layout == "gop" and bool(video_features)
    if include_video:
        _require_video_dependencies(video_features, needs_pillow=not gop_layout)

    schema = _build_schema(data_features, () if gop_layout else video_features)
    resolved_namespace = namespace or catalog.namespace
    gops_table = f"{table}{GOPS_TABLE_SUFFIX}"
    frames_table = f"{table}{FRAMES_TABLE_SUFFIX}"
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
    )
    gops_handle = frames_handle = None
    if gop_layout:
        gops_handle = catalog.create_table(
            gops_table,
            path=_sibling_path(path, GOPS_TABLE_SUFFIX),
            schema=_GOPS_SCHEMA,
            namespace=resolved_namespace,
            partition_by=(EPISODE_COLUMN,),
        )
        frames_handle = catalog.create_table(
            frames_table,
            path=_sibling_path(path, FRAMES_TABLE_SUFFIX),
            schema=_FRAMES_SCHEMA,
            namespace=resolved_namespace,
            partition_by=(EPISODE_COLUMN,),
        )

    table_path = table_handle.path
    rows = 0
    video_frames = 0
    gop_cache: dict[Path, list[GopRecord]] = {}
    for episode in selected:
        episode_table, decoded = _build_episode_table(
            episode,
            info,
            data_features,
            () if gop_layout else video_features,
            schema=schema,
            image_format=image_format,
            image_quality=image_quality,
        )
        table_handle.write_arrow(episode_table, format=physical_format)
        rows += episode_table.num_rows
        if gop_layout:
            video_frames += _write_gop_episode(
                gops_handle,
                frames_handle,
                episode,
                info,
                video_features,
                cache=gop_cache,
                physical_format=physical_format,
            )
        else:
            video_frames += decoded
    tables = (table_handle.name,)
    if gop_layout:
        tables = (table_handle.name, gops_handle.name, frames_handle.name)
    return ImportSummary(
        table=table_handle.name,
        path=table_path,
        episodes=len(selected),
        rows=rows,
        video_frames=video_frames,
        columns=tuple(schema.names),
        tables=tables,
    )


def _load_info(root: Path) -> dict[str, Any]:
    info_path = root / "meta" / "info.json"
    if not info_path.exists():
        raise FileNotFoundError(f"not a LeRobot dataset (missing {info_path})")
    with info_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _check_version(info: Mapping[str, Any]) -> None:
    version = str(info.get("codebase_version", ""))
    if not version.startswith("v3"):
        raise ValueError(
            f"unsupported LeRobot codebase_version {version!r}; only v3.x is "
            "supported (convert v2.1 datasets with LeRobot's "
            "convert_dataset_v21_to_v30 script)"
        )


def _parse_features(info: Mapping[str, Any]) -> list[_Feature]:
    features = []
    used: dict[str, str] = {}
    for key, spec in info.get("features", {}).items():
        dtype = str(spec.get("dtype", ""))
        shape = tuple(int(size) for size in spec.get("shape") or ())
        column = sanitize(key) if dtype != "video" else _video_column(key)
        existing = used.get(column)
        if existing is not None:
            raise ValueError(
                f"features {existing!r} and {key!r} map to the same column {column!r}"
            )
        used[column] = key
        features.append(_Feature(key=key, column=column, dtype=dtype, shape=shape))
    return features


def _video_column(key: str) -> str:
    return sanitize(key.rsplit(".", 1)[-1])


def _select_cameras(
    video_features: Sequence[_Feature], cameras: Sequence[str]
) -> list[_Feature]:
    by_key = {feature.key: feature for feature in video_features}
    by_column = {feature.column: feature for feature in video_features}
    selected = []
    for camera in cameras:
        feature = by_key.get(camera) or by_column.get(sanitize(camera))
        if feature is None:
            available = sorted(by_key)
            raise ValueError(f"unknown camera {camera!r}; available: {available}")
        selected.append(feature)
    return selected


def _arrow_type(feature: _Feature) -> pa.DataType:
    dtype = feature.dtype
    if feature.is_video:
        return pa.binary()
    if dtype.startswith("float"):
        base = pa.float32() if dtype == "float32" else pa.float64()
    elif dtype.startswith("int"):
        base = pa.int64()
    elif dtype.startswith("uint"):
        base = pa.uint64()
    elif dtype == "bool":
        base = pa.bool_()
    elif dtype == "string":
        base = pa.string()
    else:
        raise ValueError(f"unsupported feature dtype {dtype!r} for {feature.key!r}")
    if feature.shape:
        return pa.list_(base, int(np.prod(feature.shape)))
    return base


def _build_schema(
    data_features: Sequence[_Feature], video_features: Sequence[_Feature]
) -> pa.Schema:
    fields = [
        pa.field(EPISODE_COLUMN, pa.string(), nullable=False),
        pa.field("frame_index", pa.int64(), nullable=False),
        pa.field("timestamp", pa.float64()),
        pa.field("index", pa.int64()),
        pa.field("episode_index", pa.int64()),
        pa.field("task_index", pa.int64()),
    ]
    fields.extend(
        pa.field(feature.column, _arrow_type(feature))
        for feature in (*data_features, *video_features)
    )
    return pa.schema(fields)


def _load_episodes(
    root: Path,
    info: Mapping[str, Any],
    video_features: Sequence[_Feature],
) -> list[_Episode]:
    episodes_dir = root / "meta" / "episodes"
    files = sorted(episodes_dir.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"no episode metadata under {episodes_dir}; LeRobot v2.1 datasets "
            "must be converted to v3.0 first"
        )
    table = pa.concat_tables([pq.read_table(file) for file in files])
    missing = [
        name for name in _REQUIRED_EPISODE_COLUMNS if name not in table.column_names
    ]
    if missing:
        raise ValueError(f"episode metadata is missing columns: {missing}")

    rows = table.to_pylist()
    video_template = info.get("video_path")
    if video_features and not video_template:
        raise ValueError("dataset declares video features but no video_path template")
    file_starts: dict[tuple[int, int], int] = {}
    for row in rows:
        key = (int(row["data/chunk_index"]), int(row["data/file_index"]))
        start = int(row["dataset_from_index"])
        file_starts[key] = min(file_starts.get(key, start), start)

    episodes = []
    for row in rows:
        data_key = (int(row["data/chunk_index"]), int(row["data/file_index"]))
        video_files: dict[str, Path] = {}
        video_from: dict[str, float] = {}
        for feature in video_features:
            prefix = f"videos/{feature.key}"
            video_files[feature.key] = root / str(video_template).format(
                video_key=feature.key,
                chunk_index=int(row[f"{prefix}/chunk_index"]),
                file_index=int(row[f"{prefix}/file_index"]),
            )
            video_from[feature.key] = float(row[f"{prefix}/from_timestamp"])
        episodes.append(
            _Episode(
                index=int(row["episode_index"]),
                length=int(row["length"]),
                tasks=tuple(row.get("tasks") or ()),
                local_offset=int(row["dataset_from_index"]) - file_starts[data_key],
                data_file=root
                / str(info["data_path"]).format(
                    chunk_index=data_key[0], file_index=data_key[1]
                ),
                video_files=video_files,
                video_from_timestamp=video_from,
            )
        )
    return episodes


def _select_episodes(
    episodes: Sequence[_Episode], requested: Sequence[int] | None
) -> list[_Episode]:
    if requested is None:
        return list(episodes)
    by_index = {episode.index: episode for episode in episodes}
    selected = []
    for index in requested:
        episode = by_index.get(int(index))
        if episode is None:
            raise ValueError(f"episode {index} is not part of the dataset")
        selected.append(episode)
    return selected


def _require_video_dependencies(
    video_features: Sequence[_Feature], *, needs_pillow: bool
) -> None:
    if not video_features:
        return
    try:
        import av  # noqa: F401

        if needs_pillow:
            import PIL  # noqa: F401
    except ImportError as error:
        raise ImportError(
            "video import requires the optional 'av'"
            + (" and 'Pillow'" if needs_pillow else "")
            + " dependencies; install them with `pip install lakesoul[embodied]` "
            "or pass include_video=False"
        ) from error


def _build_episode_table(
    episode: _Episode,
    info: Mapping[str, Any],
    data_features: Sequence[_Feature],
    video_features: Sequence[_Feature],
    *,
    schema: pa.Schema,
    image_format: str,
    image_quality: int,
) -> tuple[pa.Table, int]:
    if not episode.data_file.exists():
        raise FileNotFoundError(f"data shard not found: {episode.data_file}")
    source = pq.read_table(episode.data_file)
    source = source.slice(episode.local_offset, episode.length)
    if source.num_rows != episode.length:
        raise ValueError(
            f"episode {episode.index} expects {episode.length} rows but "
            f"{episode.data_file} only provides {source.num_rows}"
        )

    episode_id = f"ep{episode.index:06d}"
    arrays: dict[str, pa.Array] = {
        EPISODE_COLUMN: pa.array([episode_id] * episode.length, type=pa.string()),
        "frame_index": _int_column(source, "frame_index"),
        "timestamp": _float64_column(source, "timestamp"),
        "index": _int_column(source, "index"),
        "episode_index": pa.array([episode.index] * episode.length, type=pa.int64()),
        "task_index": _int_column(source, "task_index"),
    }
    for feature in data_features:
        arrays[feature.column] = _feature_column(source, feature)
    decoded = 0
    for feature in video_features:
        images = _decode_video_frames(
            episode,
            feature,
            info,
            image_format=image_format,
            image_quality=image_quality,
        )
        arrays[feature.column] = pa.array(images, type=pa.binary())
        decoded += len(images)
    return (
        pa.Table.from_arrays([arrays[name] for name in schema.names], schema=schema),
        decoded,
    )


def _int_column(table: pa.Table, name: str) -> pa.Array:
    if name not in table.column_names:
        return pa.nulls(table.num_rows, type=pa.int64())
    return table.column(name).combine_chunks().cast(pa.int64())


def _float64_column(table: pa.Table, name: str) -> pa.Array:
    if name not in table.column_names:
        return pa.nulls(table.num_rows, type=pa.float64())
    return table.column(name).combine_chunks().cast(pa.float64())


def _feature_column(table: pa.Table, feature: _Feature) -> pa.Array:
    if feature.key not in table.column_names:
        raise ValueError(f"data shard is missing feature {feature.key!r}")
    column = table.column(feature.key).combine_chunks()
    target = _arrow_type(feature)
    if pa.types.is_fixed_size_list(target):
        # Parquet names the child ``element`` while the table schema uses
        # ``item``; rebuild so the array type matches the expected schema
        # exactly (Arrow type equality ignores child field names).
        values = column.flatten()
        if values.type != target.value_type:
            values = values.cast(target.value_type)
        if column.null_count:
            mask = pa.compute.is_null(column)
            return pa.FixedSizeListArray.from_arrays(values, type=target, mask=mask)
        return pa.FixedSizeListArray.from_arrays(values, type=target)
    if column.type == target:
        return column
    return column.cast(target)


def _decode_video_frames(
    episode: _Episode,
    feature: _Feature,
    info: Mapping[str, Any],
    *,
    image_format: str,
    image_quality: int,
) -> list[bytes]:
    fps = float(info["fps"])
    video_path = episode.video_files[feature.key]
    if not video_path.exists():
        raise FileNotFoundError(f"video shard not found: {video_path}")
    offset = episode.video_from_timestamp[feature.key]
    timestamps = [offset + index / fps for index in range(episode.length)]
    return encode_frames(
        video_path,
        timestamps,
        tolerance=0.5 / fps,
        image_format=image_format,
        quality=image_quality,
    )


def _sibling_path(path: str | Path, suffix: str) -> str:
    return f"{str(path).rstrip('/')}{suffix}"


def _write_gop_episode(
    gops_handle: Any,
    frames_handle: Any,
    episode: _Episode,
    info: Mapping[str, Any],
    video_features: Sequence[_Feature],
    *,
    cache: dict[Path, list[GopRecord]],
    physical_format: str,
) -> int:
    fps = float(info["fps"])
    episode_id = f"ep{episode.index:06d}"
    gop_columns: dict[str, list[Any]] = {name: [] for name in _GOPS_SCHEMA.names}
    frame_columns: dict[str, list[Any]] = {name: [] for name in _FRAMES_SCHEMA.names}
    for feature in video_features:
        video_path = episode.video_files[feature.key]
        if not video_path.exists():
            raise FileNotFoundError(f"video shard not found: {video_path}")
        gops = cache.get(video_path)
        if gops is None:
            gops = demux_gops(video_path)
            cache[video_path] = gops
        selected = select_episode_frames(
            gops,
            from_timestamp=episode.video_from_timestamp[feature.key],
            length=episode.length,
            fps=fps,
        )
        order: list[GopRecord] = []
        for gop, _ in selected:
            if all(existing is not gop for existing in order):
                order.append(gop)
        renumbered = {id(gop): index for index, gop in enumerate(order)}
        for gop in order:
            gop_columns[EPISODE_COLUMN].append(episode_id)
            gop_columns["camera"].append(feature.column)
            gop_columns["gop_index"].append(renumbered[id(gop)])
            gop_columns["timestamp"].append(gop.timestamp)
            gop_columns["codec"].append(gop.codec)
            gop_columns["num_frames"].append(len(gop.frames))
            gop_columns["frame_timestamps"].append(
                [frame.timestamp for frame in gop.frames]
            )
            gop_columns["frame_offsets"].append([frame.offset for frame in gop.frames])
            gop_columns["frame_lengths"].append([frame.length for frame in gop.frames])
            gop_columns["data"].append(gop.data)
        for frame_index, (gop, frame) in enumerate(selected):
            frame_columns[EPISODE_COLUMN].append(episode_id)
            frame_columns["camera"].append(feature.column)
            frame_columns["frame_index"].append(frame_index)
            frame_columns["gop_index"].append(renumbered[id(gop)])
            frame_columns["gop_position"].append(frame.position)
            frame_columns["timestamp"].append(frame.timestamp)
            frame_columns["byte_offset"].append(frame.offset)
            frame_columns["byte_length"].append(frame.length)
    gops_handle.write_arrow(
        _table_from(_GOPS_SCHEMA, gop_columns), format=physical_format
    )
    frames_handle.write_arrow(
        _table_from(_FRAMES_SCHEMA, frame_columns), format=physical_format
    )
    return len(frame_columns["frame_index"])


def _table_from(schema: pa.Schema, columns: dict[str, list[Any]]) -> pa.Table:
    arrays = [
        pa.array(columns[name], type=schema.field(name).type) for name in schema.names
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


__all__ = ["ImportSummary", "import_lerobot"]
