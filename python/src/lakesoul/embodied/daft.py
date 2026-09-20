# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Distributed LeRobot import built on Daft.

``daft.datasets.lerobot`` scans and decodes the dataset (one row per frame),
and the LakeSoul Daft sink writes files on the workers with a single driver
commit. The single-machine :func:`lakesoul.embodied.import_lerobot` stays the
reference implementation; this module reuses its schema builders so both
produce the same table layout.

The Daft runner is used as-is: on the default native runner the pipeline is
correct but single-process; Ray (or another distributed runner) makes the scan,
decode and writes parallel without code changes.
"""

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from lakesoul.catalog import LakeSoulCatalog

from .importer import ImportSummary, prepare_table, sibling_path
from .lerobot import (
    NON_FEATURE_COLUMNS,
    _build_schema,
    _check_version,
    _load_episodes,
    _load_info,
    _parse_features,
    _select_cameras,
    _select_episodes,
)
from .video import (
    FRAMES_SCHEMA,
    GOPS_SCHEMA,
    GopRecord,
    demux_gops,
    select_episode_frames,
)

EPISODE_COLUMN = "episode_id"
GOPS_TABLE_SUFFIX = "_gops"
FRAMES_TABLE_SUFFIX = "_frames"


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
    physical_format: str = "vortex",
    sort_rows: bool = True,
    overwrite: bool = False,
) -> ImportSummary:
    """Import a local LeRobot v3.0 dataset through Daft.

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
        physical_format: LakeSoul physical format for the written files.
        sort_rows: sort each episode's rows by frame before writing so window
            row offsets match ``frame_index``; disable to skip the shuffle.
        overwrite: drop and recreate the table when it already exists.
    """
    import daft
    from daft import col, functions, lit
    from daft.datasets import lerobot

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

    dataframe = lerobot.read(
        str(root),
        load_video_frames=[feature.key for feature in video_features]
        if video_features
        else False,
    )
    if episodes is not None:
        dataframe = dataframe.where(
            col("episode_index").is_in([int(episode) for episode in episodes])
        )

    selections: list[Any] = [
        functions.concat(
            lit("ep"),
            functions.lpad(col("episode_index").cast(daft.DataType.string()), 6, "0"),
        ).alias(EPISODE_COLUMN),
        col("frame_index"),
        col("timestamp").cast(daft.DataType.float64()),
        col("index"),
        col("episode_index"),
        col("task_index"),
    ]
    selections.extend(
        col(feature.key).alias(feature.column) for feature in data_features
    )
    selections.extend(
        functions.encode_image(col(feature.key), image_format).alias(feature.column)
        for feature in video_features
    )
    dataframe = dataframe.select(*selections)
    if sort_rows:
        dataframe = dataframe.sort([EPISODE_COLUMN, "frame_index"])

    resolved_namespace = namespace or catalog.namespace
    prepare_table(catalog, table, resolved_namespace, overwrite)
    table_handle = catalog.create_table(
        table,
        path=path,
        schema=schema,
        namespace=resolved_namespace,
        partition_by=(EPISODE_COLUMN,),
    )
    result = table_handle.write_daft(dataframe, format=physical_format)
    rows = result.row_count
    return ImportSummary(
        table=table_handle.name,
        path=table_handle.path,
        episodes=len(result.partitions),
        rows=rows,
        video_frames=rows * len(video_features),
        columns=tuple(schema.names),
        tables=(table_handle.name,),
    )


__all__ = ["import_lerobot"]


def import_lerobot_gop(
    source: str | Path,
    *,
    table: str,
    path: str | Path,
    catalog: LakeSoulCatalog | None = None,
    namespace: str | None = None,
    episodes: Sequence[int] | None = None,
    cameras: Sequence[str] | None = None,
    physical_format: str = "vortex",
    overwrite: bool = False,
) -> ImportSummary:
    """Import a local LeRobot v3.0 dataset as GOP video tables through Daft.

    The ticks table is written from ``daft.datasets.lerobot`` exactly like
    :func:`import_lerobot`; ``<table>_gops`` / ``<table>_frames`` are built by a
    Daft class UDF that demuxes each video shard once per worker and groups the
    episode's frames into self-contained Annex-B GOPs, mirroring the
    single-machine ``video_layout="gop"`` layout.
    """
    import daft
    from daft import col, functions, lit
    from daft.datasets import lerobot

    catalog = catalog or LakeSoulCatalog.from_env()
    root = Path(source).expanduser().resolve()
    info = _load_info(root)
    _check_version(info)
    fps = float(info["fps"])

    features = _parse_features(info)
    video_features = [feature for feature in features if feature.is_video]
    if cameras is not None:
        video_features = _select_cameras(video_features, cameras)
    data_features = [
        feature
        for feature in features
        if not feature.is_video and feature.key not in NON_FEATURE_COLUMNS
    ]
    if not video_features:
        raise ValueError("gop layout requires at least one camera")
    tick_schema = _build_schema(data_features, ())

    episodes_meta = _load_episodes(root, info, video_features)
    selected = _select_episodes(episodes_meta, episodes)
    if not selected:
        raise ValueError("no episodes selected for import")

    resolved_namespace = namespace or catalog.namespace
    gops_table = f"{table}{GOPS_TABLE_SUFFIX}"
    frames_table = f"{table}{FRAMES_TABLE_SUFFIX}"
    prepare_table(catalog, table, resolved_namespace, overwrite)
    prepare_table(catalog, gops_table, resolved_namespace, overwrite)
    prepare_table(catalog, frames_table, resolved_namespace, overwrite)

    ticks_handle = catalog.create_table(
        table,
        path=path,
        schema=tick_schema,
        namespace=resolved_namespace,
        partition_by=(EPISODE_COLUMN,),
    )
    ticks_result = ticks_handle.write_daft(
        _ticks_frame(lerobot, root, data_features, episodes),
        format=physical_format,
    )

    work_rows = [
        {
            "episode_id": f"ep{episode.index:06d}",
            "camera": feature.column,
            "video_path": str(episode.video_files[feature.key]),
            "from_timestamp": float(episode.video_from_timestamp[feature.key]),
            "length": int(episode.length),
        }
        for episode in selected
        for feature in video_features
    ]
    work = daft.from_pydict(
        {name: [row[name] for row in work_rows] for name in work_rows[0]}
    )
    work = work.with_column(
        "payload",
        _GopBuilder()(
            col("video_path"),
            col("from_timestamp"),
            col("length"),
            lit(fps),
        ),
    )
    gops_frame = (
        work.select(
            EPISODE_COLUMN,
            "camera",
            functions.explode(col("payload")["gops"]).alias("row"),
        )
        .select(
            EPISODE_COLUMN,
            "camera",
            col("row")["gop_index"].alias("gop_index"),
            col("row")["timestamp"].alias("timestamp"),
            col("row")["codec"].alias("codec"),
            col("row")["num_frames"].alias("num_frames"),
            col("row")["frame_timestamps"].alias("frame_timestamps"),
            col("row")["frame_offsets"].alias("frame_offsets"),
            col("row")["frame_lengths"].alias("frame_lengths"),
            col("row")["data"].alias("data"),
        )
        .sort([EPISODE_COLUMN, "camera", "gop_index"])
    )
    frames_frame = (
        work.select(
            EPISODE_COLUMN,
            "camera",
            functions.explode(col("payload")["frames"]).alias("row"),
        )
        .select(
            EPISODE_COLUMN,
            "camera",
            col("row")["frame_index"].alias("frame_index"),
            col("row")["gop_index"].alias("gop_index"),
            col("row")["gop_position"].alias("gop_position"),
            col("row")["timestamp"].alias("timestamp"),
            col("row")["byte_offset"].alias("byte_offset"),
            col("row")["byte_length"].alias("byte_length"),
        )
        .sort([EPISODE_COLUMN, "camera", "frame_index"])
    )

    gops_handle = catalog.create_table(
        gops_table,
        path=sibling_path(path, GOPS_TABLE_SUFFIX),
        schema=GOPS_SCHEMA,
        namespace=resolved_namespace,
        partition_by=(EPISODE_COLUMN,),
    )
    frames_handle = catalog.create_table(
        frames_table,
        path=sibling_path(path, FRAMES_TABLE_SUFFIX),
        schema=FRAMES_SCHEMA,
        namespace=resolved_namespace,
        partition_by=(EPISODE_COLUMN,),
    )
    gops_handle.write_daft(gops_frame, format=physical_format)
    frames_result = frames_handle.write_daft(frames_frame, format=physical_format)
    return ImportSummary(
        table=ticks_handle.name,
        path=ticks_handle.path,
        episodes=len(ticks_result.partitions),
        rows=ticks_result.row_count,
        video_frames=frames_result.row_count,
        columns=tuple(tick_schema.names),
        tables=(ticks_handle.name, gops_handle.name, frames_handle.name),
    )


def _ticks_frame(lerobot: Any, root: Path, data_features: Any, episodes: Any) -> Any:
    from daft import col, functions, lit

    dataframe = lerobot.read(str(root), load_video_frames=False)
    if episodes is not None:
        dataframe = dataframe.where(
            col("episode_index").is_in([int(episode) for episode in episodes])
        )
    selections = [
        functions.concat(
            lit("ep"),
            functions.lpad(
                col("episode_index").cast(_daft().DataType.string()), 6, "0"
            ),
        ).alias(EPISODE_COLUMN),
        col("frame_index"),
        col("timestamp").cast(_daft().DataType.float64()),
        col("index"),
        col("episode_index"),
        col("task_index"),
    ]
    selections.extend(
        col(feature.key).alias(feature.column) for feature in data_features
    )
    return dataframe.select(*selections).sort([EPISODE_COLUMN, "frame_index"])


def _daft() -> Any:
    import daft

    return daft


def _gop_struct() -> Any:
    daft = _daft()
    types = daft.DataType
    return types.struct(
        {
            "gop_index": types.int64(),
            "timestamp": types.float64(),
            "codec": types.string(),
            "num_frames": types.int64(),
            "frame_timestamps": types.list(types.float64()),
            "frame_offsets": types.list(types.int64()),
            "frame_lengths": types.list(types.int64()),
            "data": types.binary(),
        }
    )


def _frame_struct() -> Any:
    daft = _daft()
    types = daft.DataType
    return types.struct(
        {
            "frame_index": types.int64(),
            "gop_index": types.int64(),
            "gop_position": types.int64(),
            "timestamp": types.float64(),
            "byte_offset": types.int64(),
            "byte_length": types.int64(),
        }
    )


def _payload_type() -> Any:
    daft = _daft()
    return daft.DataType.struct(
        {
            "gops": daft.DataType.list(_gop_struct()),
            "frames": daft.DataType.list(_frame_struct()),
        }
    )


@_daft().cls()
class _GopBuilder:
    """Build GOP rows for one (episode, camera) video slice."""

    def __init__(self) -> None:
        self._cache: dict[str, list[GopRecord]] = {}

    def __call__(
        self,
        video_path: str,
        from_timestamp: float,
        length: int,
        fps: float,
    ) -> _payload_type():
        gops = self._cache.get(video_path)
        if gops is None:
            gops = demux_gops(video_path)
            self._cache[video_path] = gops
        selected = select_episode_frames(
            gops,
            from_timestamp=from_timestamp,
            length=int(length),
            fps=float(fps),
        )
        order: list[GopRecord] = []
        for gop, _ in selected:
            if all(existing is not gop for existing in order):
                order.append(gop)
        renumbered = {id(gop): index for index, gop in enumerate(order)}
        gop_rows = [
            {
                "gop_index": renumbered[id(gop)],
                "timestamp": gop.timestamp,
                "codec": gop.codec,
                "num_frames": len(gop.frames),
                "frame_timestamps": [frame.timestamp for frame in gop.frames],
                "frame_offsets": [frame.offset for frame in gop.frames],
                "frame_lengths": [frame.length for frame in gop.frames],
                "data": gop.data,
            }
            for gop in order
        ]
        frame_rows = [
            {
                "frame_index": frame_index,
                "gop_index": renumbered[id(gop)],
                "gop_position": frame.position,
                "timestamp": frame.timestamp,
                "byte_offset": frame.offset,
                "byte_length": frame.length,
            }
            for frame_index, (gop, frame) in enumerate(selected)
        ]
        return {"gops": gop_rows, "frames": frame_rows}


__all__ = ["import_lerobot", "import_lerobot_gop"]
