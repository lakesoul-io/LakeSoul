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

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pyarrow as pa

from lakesoul.catalog import LakeSoulCatalog, LakeSoulScan

from .dataset import BOUNDARY_CLAMP, BOUNDARY_SKIP, Window
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


def read_samples(
    scan: LakeSoulScan,
    *,
    window: Mapping[str, Window | tuple[int, int]],
    stride: int = 1,
    order_by: str = "frame_index",
    episode_column: str = EPISODE_COLUMN,
    seed: int = 0,
    epoch: int = 0,
    boundary: str = BOUNDARY_SKIP,
) -> Any:
    """Distributed anchor-window samples as a lazy Daft DataFrame.

    ``window`` maps a column to its row range relative to the anchor, exactly
    like :class:`lakesoul.embodied.EmbodiedDataset` (rows, not seconds). The
    pipeline sorts each episode by ``order_by``, aggregates it on one worker,
    packs the windows in a Daft UDF and explodes the samples, so both the
    ordering and the memory footprint stay per episode.

    Returns a DataFrame with ``episode_id``, ``anchor`` (the anchor's
    ``order_by`` value) and one list column per window key. With
    ``boundary="skip"`` anchors whose window leaves the episode are dropped;
    with ``boundary="clamp"`` windows are clipped to the episode and only
    anchors with a non-empty window survive.
    """
    import daft
    from daft import col, func, functions

    from lakesoul.daft import read_lakesoul

    if boundary not in (BOUNDARY_SKIP, BOUNDARY_CLAMP):
        raise ValueError(f"boundary must be one of {(BOUNDARY_SKIP, BOUNDARY_CLAMP)}")
    if stride < 1:
        raise ValueError(f"stride must be positive, got {stride}")
    if not window:
        raise ValueError("window must define at least one column")

    windows: dict[str, Window] = {}
    for name, spec in window.items():
        resolved = (
            spec if isinstance(spec, Window) else Window(int(spec[0]), int(spec[1]))
        )
        windows[name] = resolved

    dataframe = read_lakesoul(scan)
    schema = dataframe.schema()
    names = list(windows)
    for name in names:
        if name not in schema.column_names():
            raise ValueError(f"window column {name!r} is not in the scan schema")
    if order_by not in schema.column_names():
        raise ValueError(f"order_by column {order_by!r} is not in the scan schema")

    sample_type = daft.DataType.struct(
        {
            "anchor": schema[order_by].dtype,
            **{name: daft.DataType.list(schema[name].dtype) for name in names},
        }
    )
    payload_type = daft.DataType.list(sample_type)

    def sampler(order_values: list, *window_values: list) -> list[dict[str, Any]]:
        import numpy as np

        count = len(order_values)
        anchors = np.arange(0, count, stride, dtype=np.int64)
        lower = min(item.start for item in windows.values())
        upper = max(item.end for item in windows.values())
        if boundary == BOUNDARY_SKIP and (lower < 0 or upper > 0):
            keep = (anchors + lower >= 0) & (anchors + upper <= count)
            anchors = anchors[keep]
        if anchors.size > 1:
            anchors = np.random.default_rng([seed, epoch]).permutation(anchors)
        samples = []
        for anchor in anchors.tolist():
            sample: dict[str, Any] = {"anchor": order_values[anchor]}
            for name, values in zip(names, window_values):
                item = windows[name]
                start = anchor + item.start
                end = anchor + item.end
                if boundary == BOUNDARY_CLAMP:
                    start = max(start, 0)
                    end = min(end, count)
                if end <= start:
                    sample = {}
                    break
                sample[name] = values[start:end]
            if sample:
                samples.append(sample)
        return samples

    aggregated = (
        dataframe.sort([episode_column, order_by])
        .groupby(episode_column)
        .agg(
            functions.list_agg(col(order_by)).alias(order_by),
            *[functions.list_agg(col(name)).alias(name) for name in names],
        )
    )
    sampler_udf = func(return_dtype=payload_type)(sampler)
    exploded = aggregated.with_column(
        "samples",
        sampler_udf(col(order_by), *[col(name) for name in names]),
    ).select(
        episode_column,
        functions.explode(col("samples")).alias("sample"),
    )
    return exploded.select(
        episode_column,
        col("sample")["anchor"].alias("anchor"),
        *[col("sample")[name].alias(name) for name in names],
    )


def read_gop_frames(
    gops: LakeSoulScan,
    frames: LakeSoulScan,
    *,
    cameras: Sequence[str] | None = None,
    image_format: str | None = "JPEG",
    image_quality: int = 90,
    episode_column: str = EPISODE_COLUMN,
) -> Any:
    """Decode GOP video into a lazy DataFrame with one row per frame.

    ``gops`` / ``frames`` are scans of the side tables written by the GOP
    importers. Each GOP is decoded once on a Daft worker (``decode_gop``), the
    frames are exploded and joined back to the frame index, so the result has
    ``episode_id``, ``camera``, ``frame_index``, ``timestamp``, ``width``,
    ``height`` and ``image`` (encoded bytes, or raw RGB when
    ``image_format=None``).
    """
    import daft
    from daft import col, func, functions

    from lakesoul.daft import read_lakesoul

    gops_frame = read_lakesoul(gops).select(
        episode_column, "camera", "gop_index", "codec", "data"
    )
    frames_frame = read_lakesoul(frames).select(
        episode_column,
        "camera",
        "gop_index",
        "gop_position",
        "frame_index",
        "timestamp",
    )
    if cameras is not None:
        wanted = [str(camera) for camera in cameras]
        gops_frame = gops_frame.where(col("camera").is_in(wanted))
        frames_frame = frames_frame.where(col("camera").is_in(wanted))

    payload = daft.DataType.list(
        daft.DataType.struct(
            {
                "position": daft.DataType.int64(),
                "width": daft.DataType.int64(),
                "height": daft.DataType.int64(),
                "image": daft.DataType.binary(),
            }
        )
    )

    def decode(data: bytes, codec: str) -> list[dict[str, Any]]:
        import numpy as np
        from PIL import Image

        from .video import decode_gop, encode_image

        decoded = []
        for position, array in enumerate(decode_gop(bytes(data), codec=str(codec))):
            height, width = int(array.shape[0]), int(array.shape[1])
            if image_format is None:
                image = np.ascontiguousarray(array).tobytes()
            else:
                image = encode_image(
                    Image.fromarray(array),
                    image_format=image_format,
                    quality=image_quality,
                )
            decoded.append(
                {
                    "position": position,
                    "width": width,
                    "height": height,
                    "image": image,
                }
            )
        return decoded

    decode_udf = func(return_dtype=payload)(decode)
    decoded = (
        gops_frame.with_column("decoded", decode_udf(col("data"), col("codec")))
        .select(
            episode_column,
            "camera",
            "gop_index",
            functions.explode(col("decoded")).alias("frame"),
        )
        .select(
            episode_column,
            "camera",
            "gop_index",
            col("frame")["position"].alias("gop_position"),
            col("frame")["image"].alias("image"),
            col("frame")["width"].alias("width"),
            col("frame")["height"].alias("height"),
        )
    )
    joined = frames_frame.join(
        decoded,
        on=[episode_column, "camera", "gop_index", "gop_position"],
        how="inner",
    )
    return joined.select(
        episode_column,
        "camera",
        "frame_index",
        "timestamp",
        "width",
        "height",
        "image",
    ).sort([episode_column, "camera", "frame_index"])


def import_mcap(
    source: str | Path | Sequence[str | Path],
    *,
    table: str,
    path: str | Path,
    columns: Mapping[str, str] | None = None,
    cameras: Mapping[str, str] | None = None,
    row_topic: str | None = None,
    tolerance: float = 0.02,
    video_layout: str = "frames",
    catalog: LakeSoulCatalog | None = None,
    namespace: str | None = None,
    physical_format: str = "vortex",
    overwrite: bool = False,
) -> ImportSummary:
    """Import MCAP files (frames layout) through Daft, one file per task.

    ``source`` is an MCAP file, a directory of ``*.mcap`` files, or a list of
    files. Each task builds one file's rows with the same helpers as
    :func:`lakesoul.embodied.import_mcap` (JSON and protobuf messages) and the
    rows are written together with a single Daft sink commit. With
    ``video_layout="gop"`` the camera access units are grouped into
    ``<table>_gops`` / ``<table>_frames`` like the single-machine importer.
    """
    import daft
    from daft import col, func, functions

    from .mcap import build_frame_episode

    if video_layout not in {"frames", "gop"}:
        raise ValueError("video_layout must be 'frames' or 'gop'")
    catalog = catalog or LakeSoulCatalog.from_env()
    files = _resolve_mcap_sources(source)
    if not files:
        raise ValueError("no MCAP files found for import")

    work = daft.from_pydict(
        {
            "source": [str(file) for file in files],
            "episode_id": [file.stem for file in files],
        }
    )
    if video_layout == "gop":
        return _import_mcap_gop(
            daft,
            func,
            col,
            functions,
            work,
            files,
            table=table,
            path=path,
            columns=columns,
            cameras=cameras,
            row_topic=row_topic,
            tolerance=tolerance,
            catalog=catalog,
            namespace=namespace or catalog.namespace,
            physical_format=physical_format,
            overwrite=overwrite,
        )

    sample = build_frame_episode(
        files[0],
        columns=columns,
        cameras=cameras,
        row_topic=row_topic,
        episode_id=files[0].stem,
        tolerance=tolerance,
    )
    schema = sample.schema
    payload = daft.DataType.list(
        daft.DataType.struct(
            {name: _arrow_to_daft(schema.field(name).type) for name in schema.names}
        )
    )

    def build(file_path: str, episode_id: str) -> list[dict[str, Any]]:
        return build_frame_episode(
            file_path,
            columns=columns,
            cameras=cameras,
            row_topic=row_topic,
            episode_id=episode_id,
            tolerance=tolerance,
        ).to_pylist()

    builder = func(return_dtype=payload)(build)
    dataframe = work.select(
        functions.explode(builder(col("source"), col("episode_id"))).alias("row")
    ).select(*[col("row")[name].alias(name) for name in schema.names])

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
    return ImportSummary(
        table=table_handle.name,
        path=table_handle.path,
        episodes=len(files),
        rows=result.row_count,
        video_frames=result.row_count * len(cameras or {}),
        columns=tuple(schema.names),
        tables=(table_handle.name,),
    )


def _import_mcap_gop(
    daft: Any,
    func: Any,
    col: Any,
    functions: Any,
    work: Any,
    files: list[Path],
    *,
    table: str,
    path: str | Path,
    columns: Mapping[str, str] | None,
    cameras: Mapping[str, str] | None,
    row_topic: str | None,
    tolerance: float,
    catalog: LakeSoulCatalog,
    namespace: str,
    physical_format: str,
    overwrite: bool,
) -> ImportSummary:
    from .mcap import build_frame_episode, build_gop_rows

    if not cameras:
        raise ValueError("video_layout='gop' requires at least one camera")

    tick_sample = build_frame_episode(
        files[0],
        columns=columns,
        cameras={},
        row_topic=row_topic,
        episode_id=files[0].stem,
        tolerance=tolerance,
    )
    tick_schema = tick_sample.schema
    gop_struct = {
        name: _arrow_to_daft(GOPS_SCHEMA.field(name).type) for name in GOPS_SCHEMA.names
    }
    frame_struct = {
        name: _arrow_to_daft(FRAMES_SCHEMA.field(name).type)
        for name in FRAMES_SCHEMA.names
    }
    payload = daft.DataType.struct(
        {
            "ticks": daft.DataType.list(
                daft.DataType.struct(
                    {
                        name: _arrow_to_daft(tick_schema.field(name).type)
                        for name in tick_schema.names
                    }
                )
            ),
            "gops": daft.DataType.list(daft.DataType.struct(gop_struct)),
            "frames": daft.DataType.list(daft.DataType.struct(frame_struct)),
        }
    )

    def build(file_path: str, episode_id: str) -> dict[str, Any]:
        ticks = build_frame_episode(
            file_path,
            columns=columns,
            cameras={},
            row_topic=row_topic,
            episode_id=episode_id,
            tolerance=tolerance,
        )
        gops, frames = build_gop_rows(
            file_path,
            columns=columns,
            cameras=cameras,
            row_topic=row_topic,
            episode_id=episode_id,
            tolerance=tolerance,
        )
        return {
            "ticks": ticks.to_pylist(),
            "gops": gops,
            "frames": frames,
        }

    builder = func(return_dtype=payload)(build)
    built = work.with_column("payload", builder(col("source"), col("episode_id")))
    tick_frame = built.select(
        functions.explode(col("payload")["ticks"]).alias("row")
    ).select(*[col("row")[name].alias(name) for name in tick_schema.names])
    gops_frame = (
        built.select(functions.explode(col("payload")["gops"]).alias("row"))
        .select(*[col("row")[name].alias(name) for name in GOPS_SCHEMA.names])
        .sort([EPISODE_COLUMN, "camera", "gop_index"])
    )
    frames_frame = (
        built.select(functions.explode(col("payload")["frames"]).alias("row"))
        .select(*[col("row")[name].alias(name) for name in FRAMES_SCHEMA.names])
        .sort([EPISODE_COLUMN, "camera", "frame_index"])
    )

    gops_table = f"{table}{GOPS_TABLE_SUFFIX}"
    frames_table = f"{table}{FRAMES_TABLE_SUFFIX}"
    prepare_table(catalog, table, namespace, overwrite)
    prepare_table(catalog, gops_table, namespace, overwrite)
    prepare_table(catalog, frames_table, namespace, overwrite)

    tick_handle = catalog.create_table(
        table,
        path=path,
        schema=tick_schema,
        namespace=namespace,
        partition_by=(EPISODE_COLUMN,),
    )
    ticks_result = tick_handle.write_daft(tick_frame, format=physical_format)
    gops_handle = catalog.create_table(
        gops_table,
        path=sibling_path(path, GOPS_TABLE_SUFFIX),
        schema=GOPS_SCHEMA,
        namespace=namespace,
        partition_by=(EPISODE_COLUMN,),
    )
    gops_handle.write_daft(gops_frame, format=physical_format)
    frames_handle = catalog.create_table(
        frames_table,
        path=sibling_path(path, FRAMES_TABLE_SUFFIX),
        schema=FRAMES_SCHEMA,
        namespace=namespace,
        partition_by=(EPISODE_COLUMN,),
    )
    frames_result = frames_handle.write_daft(frames_frame, format=physical_format)
    return ImportSummary(
        table=tick_handle.name,
        path=tick_handle.path,
        episodes=len(files),
        rows=ticks_result.row_count,
        video_frames=frames_result.row_count,
        columns=tuple(tick_schema.names),
        tables=(tick_handle.name, gops_handle.name, frames_handle.name),
    )


def _resolve_mcap_sources(source: str | Path | Sequence[str | Path]) -> list[Path]:
    if isinstance(source, (str, Path)):
        path = Path(source).expanduser().resolve()
        if path.is_dir():
            return sorted(item for item in path.glob("*.mcap") if item.is_file())
        if not path.exists():
            raise FileNotFoundError(f"MCAP file not found: {path}")
        return [path]
    files = [Path(item).expanduser().resolve() for item in source]
    for file in files:
        if not file.exists():
            raise FileNotFoundError(f"MCAP file not found: {file}")
    return files


def _arrow_to_daft(dtype: pa.DataType) -> Any:
    import daft

    types = daft.DataType
    if pa.types.is_int64(dtype):
        return types.int64()
    if pa.types.is_int32(dtype):
        return types.int32()
    if pa.types.is_float64(dtype):
        return types.float64()
    if pa.types.is_float32(dtype):
        return types.float32()
    if pa.types.is_boolean(dtype):
        return types.bool()
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return types.string()
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype):
        return types.binary()
    if pa.types.is_fixed_size_list(dtype):
        return types.fixed_size_list(
            _arrow_to_daft(dtype.value_type), int(dtype.list_size)
        )
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return types.list(_arrow_to_daft(dtype.value_type))
    raise ValueError(f"unsupported Arrow type for Daft import: {dtype}")


__all__ = [
    "import_lerobot",
    "import_lerobot_gop",
    "import_mcap",
    "read_gop_frames",
    "read_samples",
]
