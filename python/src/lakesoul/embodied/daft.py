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

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from lakesoul.catalog import LakeSoulCatalog

from .importer import ImportSummary, prepare_table
from .lerobot import (
    NON_FEATURE_COLUMNS,
    _build_schema,
    _check_version,
    _load_info,
    _parse_features,
    _select_cameras,
)

EPISODE_COLUMN = "episode_id"


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
