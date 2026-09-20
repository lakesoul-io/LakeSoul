# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Import a local LeRobotDataset v3.0 directory into a LakeSoul table.

The output layout follows the M1 hard constraints: one partition per episode,
time-ordered frames, and per-frame image bytes in binary columns (decoded from
the dataset's MP4 shards). Tabular features keep their LeRobot names with dots
replaced by underscores, for example ``observation.state`` becomes
``observation_state``.

Only ``v3.0`` directories on local storage are supported; convert a ``v2.1``
dataset with LeRobot's ``convert_dataset_v21_to_v30`` script first.

Video decoding requires the optional ``av`` and ``Pillow`` dependencies
(``pip install lakesoul[embodied]``).
"""

from __future__ import annotations

import io
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from lakesoul.catalog import LakeSoulCatalog, TableNotFoundError

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


@dataclass(frozen=True)
class ImportSummary:
    table: str
    path: str
    episodes: int
    rows: int
    video_frames: int
    columns: tuple[str, ...]


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
        include_video: set ``False`` to skip video decoding entirely.
        image_format: ``JPEG`` or ``PNG`` for per-frame image bytes.
        image_quality: JPEG quality.
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

    if include_video:
        _require_video_dependencies(video_features)

    resolved_namespace = namespace or catalog.namespace
    _prepare_table(catalog, table, resolved_namespace, overwrite)

    table_handle = catalog.create_table(
        table,
        path=path,
        schema=schema,
        namespace=resolved_namespace,
        partition_by=(EPISODE_COLUMN,),
    )
    table_path = table_handle.path
    rows = 0
    video_frames = 0
    for episode in selected:
        episode_table, decoded = _build_episode_table(
            episode,
            info,
            data_features,
            video_features,
            schema=schema,
            image_format=image_format,
            image_quality=image_quality,
        )
        table_handle.write_arrow(episode_table, format=physical_format)
        rows += episode_table.num_rows
        video_frames += decoded
    return ImportSummary(
        table=table_handle.name,
        path=table_path,
        episodes=len(selected),
        rows=rows,
        video_frames=video_frames,
        columns=tuple(schema.names),
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
        column = _sanitize(key) if dtype != "video" else _video_column(key)
        existing = used.get(column)
        if existing is not None:
            raise ValueError(
                f"features {existing!r} and {key!r} map to the same column {column!r}"
            )
        used[column] = key
        features.append(_Feature(key=key, column=column, dtype=dtype, shape=shape))
    return features


def _sanitize(name: str) -> str:
    sanitized = "".join(
        character if character.isalnum() or character == "_" else "_"
        for character in name
    )
    return sanitized.strip("_") or "column"


def _video_column(key: str) -> str:
    return _sanitize(key.rsplit(".", 1)[-1])


def _select_cameras(
    video_features: Sequence[_Feature], cameras: Sequence[str]
) -> list[_Feature]:
    by_key = {feature.key: feature for feature in video_features}
    by_column = {feature.column: feature for feature in video_features}
    selected = []
    for camera in cameras:
        feature = by_key.get(camera) or by_column.get(_sanitize(camera))
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


def _require_video_dependencies(video_features: Sequence[_Feature]) -> None:
    if not video_features:
        return
    try:
        import av  # noqa: F401
        import PIL  # noqa: F401
    except ImportError as error:
        raise ImportError(
            "video import requires the optional 'av' and 'Pillow' dependencies; "
            "install them with `pip install lakesoul[embodied]` or pass "
            "include_video=False"
        ) from error


def _prepare_table(
    catalog: LakeSoulCatalog,
    table: str,
    namespace: str,
    overwrite: bool,
) -> None:
    try:
        catalog.table(table, namespace=namespace)
    except TableNotFoundError:
        return
    if not overwrite:
        raise ValueError(f"table {table!r} already exists; pass overwrite=True")
    catalog.drop_table(table, namespace=namespace, if_exists=True)


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
    import av

    fps = float(info["fps"])
    video_path = episode.video_files[feature.key]
    if not video_path.exists():
        raise FileNotFoundError(f"video shard not found: {video_path}")
    offset = episode.video_from_timestamp[feature.key]
    timestamps = [offset + index / fps for index in range(episode.length)]
    tolerance = 0.5 / fps

    container = av.open(str(video_path))
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
                frames[pending] = _encode_image(
                    frame.to_image(), image_format=image_format, quality=image_quality
                )
                pending += 1
            if pending >= len(timestamps):
                break
    finally:
        container.close()

    missing = [index for index, frame in enumerate(frames) if frame is None]
    if missing:
        raise ValueError(
            f"could not decode {len(missing)} frames for episode "
            f"{episode.index} from {video_path}"
        )
    return [frame for frame in frames if frame is not None]


def _encode_image(image: Any, *, image_format: str, quality: int) -> bytes:
    buffer = io.BytesIO()
    normalized = image_format.upper()
    options = {"quality": quality} if normalized == "JPEG" else {}
    image.save(buffer, format=normalized, **options)
    return buffer.getvalue()


__all__ = ["ImportSummary", "import_lerobot"]
