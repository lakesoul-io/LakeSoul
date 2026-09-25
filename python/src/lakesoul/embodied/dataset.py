# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Training-oriented dataset for embodied data.

The dataset consumes one table per stream set: every scan unit (a partition,
i.e. one episode when the table is partitioned by ``episode_id``) is read once
per epoch and sliced into window samples around anchor rows. Randomness is
derived from ``(seed, epoch, unit)`` so that every process computes the same
order without communication; ``iter_epoch`` shards units across ranks.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pyarrow as pa

from lakesoul.arrow import LakeSoulScanConfig, lakesoul_dataset
from lakesoul.catalog import LakeSoulCatalog, LakeSoulScan, LakeSoulTable
from lakesoul.metadata import LakeSoulScanPlanPartition

from .align import SecondaryStream, aligned_columns, load_stream_unit
from .video import GopVideo

BOUNDARY_SKIP = "skip"
BOUNDARY_CLAMP = "clamp"
_BOUNDARIES = (BOUNDARY_SKIP, BOUNDARY_CLAMP)


@dataclass(frozen=True)
class ManifestSelection:
    """Explicit sample anchors of one manifest, ordered by ``rank``.

    ``rows`` holds ``(episode_id, anchor, rank)`` where ``anchor`` is the
    ``order_by`` column value of the anchor row.
    """

    order_by: str
    episode_column: str
    rows: tuple[tuple[str, int, int], ...]


@dataclass(frozen=True)
class Window:
    """A row range relative to an anchor row.

    ``start`` is inclusive and ``end`` exclusive; negative values address rows
    before the anchor. Bounds are expressed in rows of the scanned table, so a
    table sampled at 50 Hz uses ``Window(-50, 0)`` for the last second.
    """

    start: int
    end: int

    def __post_init__(self) -> None:
        if self.end <= self.start:
            raise ValueError(
                f"window end must be greater than start, got [{self.start}, {self.end})"
            )


def _as_window(value: Window | tuple[int, int]) -> Window:
    if isinstance(value, Window):
        return value
    if isinstance(value, tuple) and len(value) == 2:
        return Window(int(value[0]), int(value[1]))
    raise TypeError(f"window must be a Window or a (start, end) tuple, got {value!r}")


def _is_seconds_window(value: Window | tuple[int, int] | tuple[float, float]) -> bool:
    if isinstance(value, Window):
        return False
    if isinstance(value, tuple) and len(value) == 2:
        return any(isinstance(bound, float) for bound in value)
    raise TypeError(f"window must be a Window or a (start, end) tuple, got {value!r}")


def _as_time_window(value: tuple[float, float]) -> tuple[float, float]:
    start, end = float(value[0]), float(value[1])
    if end <= start:
        raise ValueError(f"window end must be greater than start, got [{start}, {end})")
    return start, end


def _unit_partition_map(unit: LakeSoulScanPlanPartition) -> dict[str, str]:
    return dict(unit.partition_info)


def unit_key(unit: LakeSoulScanPlanPartition) -> str:
    """Deterministic identity of a scan unit across processes."""
    partition = ",".join(f"{k}={v}" for k, v in sorted(unit.partition_info))
    return "|".join((partition, *unit.files))


def _unit_seed(unit: LakeSoulScanPlanPartition) -> int:
    digest = hashlib.blake2b(unit_key(unit).encode(), digest_size=8).digest()
    return int.from_bytes(digest, "little")


def plan_anchor_order(
    *,
    rows: int,
    stride: int,
    seed: int,
    epoch: int,
    unit_seed: int,
    windows: Mapping[str, Window],
    boundary: str = BOUNDARY_SKIP,
) -> np.ndarray:
    """Return anchor rows of one unit in deterministic shuffled order.

    Anchors that cannot produce a full window are dropped when
    ``boundary="skip"``; with ``boundary="clamp"`` windows are clipped to the
    unit instead and only anchors with a non-empty window survive.
    """
    if rows < 0:
        raise ValueError(f"rows must be non-negative, got {rows}")
    if stride < 1:
        raise ValueError(f"stride must be positive, got {stride}")
    anchors = np.arange(0, rows, stride, dtype=np.int64)
    if anchors.size > 1:
        rng = np.random.default_rng([seed, epoch, unit_seed])
        anchors = rng.permutation(anchors)
    if boundary == BOUNDARY_SKIP:
        lower = min(window.start for window in windows.values())
        upper = max(window.end for window in windows.values())
        keep = (anchors + lower >= 0) & (anchors + upper <= rows)
        return anchors[keep]
    return anchors


class EmbodiedDataset:
    """Windows samples over a LakeSoul table for embodied training loops.

    ``scan`` carries the table, selected columns, filter and object store
    options. ``window`` maps a column name to the row range that makes up that
    stream of a sample, relative to the anchor row. ``episodes`` narrows the
    scan to the given values of the episode partition column.

    ``video`` optionally attaches decoded GOP frames (``GopVideo`` over the
    ``<table>_gops`` / ``<table>_frames`` tables): each sample gets one array
    per camera covering the ``video_window`` row range, which defaults to the
    first window column.
    """

    def __init__(
        self,
        scan: LakeSoulScan,
        *,
        window: Mapping[str, Window | tuple[int, int]],
        stride: int = 1,
        episodes: Sequence[str] | None = None,
        episode_column: str | None = None,
        boundary: str = BOUNDARY_SKIP,
        seed: int = 0,
        time_column: str | None = None,
        streams: Sequence[SecondaryStream] = (),
        video: GopVideo | None = None,
        video_window: str | tuple[int, int] | None = None,
    ) -> None:
        if stride < 1:
            raise ValueError(f"stride must be positive, got {stride}")
        if boundary not in _BOUNDARIES:
            raise ValueError(f"boundary must be one of {_BOUNDARIES}, got {boundary!r}")
        if not window:
            raise ValueError("window must define at least one column")

        self._scan: LakeSoulScan | None = scan
        self._window: dict[str, Window] = {}
        self._time_window: dict[str, tuple[float, float]] = {}
        for name, value in window.items():
            if _is_seconds_window(value):
                self._time_window[name] = _as_time_window(value)  # type: ignore[arg-type]
            else:
                self._window[name] = _as_window(value)
        self._time_column = time_column or "timestamp"
        self._streams = tuple(streams)
        self._stride = stride
        self._boundary = boundary
        self._seed = int(seed)
        self._epoch = 0
        self._video = video
        self._video_window = video_window
        self._config = scan.to_scan_config()

        schema_names = set(self._config.schema.names)
        unknown = (set(self._window) | set(self._time_window)) - schema_names
        if unknown:
            raise ValueError(
                f"window columns not found in table schema: {sorted(unknown)}"
            )
        if (
            self._time_window or self._streams
        ) and self._time_column not in schema_names:
            raise ValueError(
                f"seconds-based windows and secondary streams need the time "
                f"column {self._time_column!r} in the table schema"
            )

        self._validate_video_window()

        self._units = self._resolve_units(scan, episodes, episode_column)
        self._selection: ManifestSelection | None = None
        self._shuffle = False

    @classmethod
    def from_manifest(
        cls,
        table: LakeSoulTable | str,
        manifest: str,
        *,
        catalog: LakeSoulCatalog | None = None,
        namespace: str | None = None,
        window: Mapping[str, Window | tuple[int, int]] | None = None,
        stride: int | None = None,
        boundary: str | None = None,
        seed: int | None = None,
        time_column: str | None = None,
        streams: Sequence[SecondaryStream] | None = None,
        video: GopVideo | None = None,
        video_window: str | tuple[int, int] | None = None,
        shuffle: bool = False,
    ) -> EmbodiedDataset:
        """Open the samples of a manifest, pinned to the manifest's snapshot.

        Manifest ``params`` supply the window/stride/boundary/seed (and video
        or secondary-stream specs) written by :meth:`LakeSoulCatalog.create_manifest`;
        explicit arguments override them. Samples are yielded in ``rank`` order
        unless ``shuffle`` is set.
        """
        if isinstance(table, LakeSoulTable):
            handle = table
            catalog = handle.catalog
        else:
            catalog = catalog or LakeSoulCatalog.from_env()
            handle = catalog.table(str(table), namespace=namespace)
        rows = catalog.read_manifest(handle, manifest)
        snapshot_ids = set(rows.column("snapshot_id").to_pylist())
        if len(snapshot_ids) != 1:
            raise ValueError(
                f"manifest {manifest!r} rows disagree on snapshot_id: {snapshot_ids}"
            )
        snapshot_id = int(snapshot_ids.pop())
        params = json.loads(rows.column("params")[0].as_py())
        if window is None:
            raw = params.get("window") or {}
            window = {name: tuple(value) for name, value in raw.items()}
            if not window:
                raise ValueError(
                    "manifest has no window params; pass window= explicitly"
                )
        if stride is None:
            stride = int(params.get("stride", 1))
        if boundary is None:
            boundary = str(params.get("boundary", BOUNDARY_SKIP))
        if seed is None:
            seed = int(params.get("seed", 0))
        if time_column is None:
            time_column = params.get("time_column")
        if video is None and params.get("video"):
            spec = params["video"]
            video = GopVideo(
                catalog.table(spec["gops"], namespace=handle.namespace),
                catalog.table(spec["frames"], namespace=handle.namespace),
                cameras=tuple(spec.get("cameras") or ()) or None,
                episode_column=spec.get("episode_column", "episode_id"),
            )
            if video_window is None and spec.get("video_window") is not None:
                video_window = tuple(spec["video_window"])
        if streams is None and params.get("streams"):
            streams = tuple(
                SecondaryStream(
                    catalog.table(spec["table"], namespace=handle.namespace).scan(),
                    on=spec.get("on", "timestamp"),
                    by=spec.get("by", "episode_id"),
                    columns=spec.get("columns"),
                    tolerance=spec.get("tolerance", 0.02),
                    direction=spec.get("direction", "nearest"),
                    missing=spec.get("missing", "null"),
                    suffix=spec.get("suffix", ""),
                )
                for spec in params["streams"]
            )
        scan = handle.scan().options(snapshot=snapshot_id)
        dataset = cls(
            scan,
            window=window,
            stride=int(stride),
            boundary=str(boundary),
            seed=int(seed),
            time_column=time_column,
            streams=tuple(streams or ()),
            video=video,
            video_window=video_window,
        )
        ordered = sorted(
            zip(
                rows.column("episode_id").to_pylist(),
                rows.column("anchor").to_pylist(),
                rows.column("rank").to_pylist(),
            ),
            key=lambda item: int(item[2]),
        )
        dataset._selection = ManifestSelection(
            order_by=str(params.get("order_by", "frame_index")),
            episode_column=str(params.get("episode_column", "episode_id")),
            rows=tuple(
                (str(episode), int(anchor), int(rank))
                for episode, anchor, rank in ordered
            ),
        )
        dataset._shuffle = bool(shuffle)
        return dataset

    @property
    def scan(self) -> LakeSoulScan | None:
        return self._scan

    @property
    def window(self) -> Mapping[str, Window]:
        return dict(self._window)

    @property
    def epoch(self) -> int:
        return self._epoch

    @property
    def num_units(self) -> int:
        return len(self._units)

    def set_epoch(self, epoch: int) -> None:
        self._epoch = int(epoch)

    def __reduce__(self) -> tuple[Any, tuple[Any, ...]]:
        """Serialize via the resolved config so DataLoader ``spawn`` works."""
        return (
            _dataset_from_state,
            (
                self._config,
                self._units,
                self._window,
                self._stride,
                self._boundary,
                self._seed,
                self._video,
                self._video_window,
                self._time_window,
                self._time_column,
                self._streams,
                self._selection,
                self._shuffle,
            ),
        )

    def __iter__(self) -> Iterator[dict[str, np.ndarray]]:
        yield from self.iter_epoch(self._epoch)

    def iter_epoch(
        self,
        epoch: int | None = None,
        *,
        rank: int | None = None,
        world_size: int | None = None,
    ) -> Iterator[dict[str, np.ndarray]]:
        if epoch is None:
            epoch = self._epoch
        rank, world_size = _normalize_shard(rank, world_size)
        if self._selection is not None:
            yield from self._iter_manifest(epoch, rank, world_size)
            return
        order = np.random.default_rng([self._seed, epoch]).permutation(len(self._units))
        for index in order[rank::world_size]:
            unit = self._units[int(index)]
            yield from self._iter_unit(unit, epoch)

    def _iter_manifest(
        self, epoch: int, rank: int, world_size: int
    ) -> Iterator[dict[str, np.ndarray]]:
        selection = self._selection
        assert selection is not None
        rows = list(selection.rows)
        if self._shuffle:
            order = np.random.default_rng([self._seed, epoch]).permutation(len(rows))
            rows = [rows[int(index)] for index in order]
        rows = rows[rank::world_size]
        grouped: dict[str, list[int]] = {}
        first_rank: dict[str, int] = {}
        for episode, anchor, sample_rank in rows:
            grouped.setdefault(episode, []).append(anchor)
            first_rank.setdefault(episode, sample_rank)
        units = {
            _unit_partition_map(unit).get(selection.episode_column): unit
            for unit in self._units
        }
        for episode in sorted(grouped, key=lambda item: first_rank[item]):
            unit = units.get(episode)
            if unit is None:
                raise ValueError(
                    f"manifest episode {episode!r} not found in partition column "
                    f"{selection.episode_column!r}"
                )
            yield from self._iter_unit(unit, epoch, anchors=grouped[episode])

    def _validate_video_window(self) -> None:
        if isinstance(self._video_window, str):
            if self._video_window in self._time_window:
                raise ValueError(
                    "video windows currently need a row-based window; pass "
                    "video_window=(start, end) in rows or use a row window key"
                )
            if self._video_window not in self._window:
                raise ValueError(
                    f"video_window {self._video_window!r} is not one of the "
                    f"window columns {sorted(self._window)}"
                )
        elif isinstance(self._video_window, tuple) and (
            len(self._video_window) != 2
            or self._video_window[1] <= self._video_window[0]
        ):
            raise ValueError(
                f"video_window must be a (start, end) tuple with end > start, "
                f"got {self._video_window}"
            )

    def _video_offsets(self) -> tuple[int, int]:
        if isinstance(self._video_window, tuple):
            return self._video_window
        key = self._video_window
        if key is None:
            if not self._window:
                raise ValueError(
                    "video needs at least one row-based window column or an "
                    "explicit video_window=(start, end)"
                )
            key = next(iter(self._window))
        window = self._window[key]
        return window.start, window.end

    def _iter_unit(
        self,
        unit: LakeSoulScanPlanPartition,
        epoch: int,
        anchors: list[int] | None = None,
    ) -> Iterator[dict[str, np.ndarray]]:
        table = self._read_unit(unit)
        rows = table.num_rows
        episode_video = None
        video_start = video_end = 0
        if self._video is not None:
            episode_id = _unit_partition_map(unit).get(self._video.episode_column)
            if episode_id is None:
                raise ValueError(
                    f"video source requires the scanned table to be partitioned "
                    f"by {self._video.episode_column!r}"
                )
            episode_video = self._video.for_episode(episode_id)
            video_start, video_end = self._video_offsets()
        stream_tables = []
        for stream in self._streams:
            episode_id = _unit_partition_map(unit).get(stream.by)
            if episode_id is None:
                raise ValueError(
                    f"secondary streams need the scanned table to be "
                    f"partitioned by {stream.by!r}"
                )
            stream_tables.append(load_stream_unit(stream, episode_id))
        if anchors is None:
            anchor_rows = plan_anchor_order(
                rows=rows,
                stride=self._stride,
                seed=self._seed,
                epoch=epoch,
                unit_seed=_unit_seed(unit),
                windows=self._window,
                boundary=self._boundary,
            ).tolist()
        else:
            anchor_rows = self._resolve_manifest_anchors(table, anchors)
        needed = set(self._window) | set(self._time_window)
        columns = {name: table[name].combine_chunks() for name in needed}
        times = (
            table[self._time_column].combine_chunks().to_numpy(zero_copy_only=False)
            if self._time_window or self._streams
            else None
        )
        for anchor in anchor_rows:
            ranges: dict[str, tuple[int, int]] = {}
            if times is not None:
                for name, (start_seconds, end_seconds) in self._time_window.items():
                    anchor_time = float(times[anchor])
                    start = int(
                        np.searchsorted(times, anchor_time + start_seconds, side="left")
                    )
                    end = int(
                        np.searchsorted(times, anchor_time + end_seconds, side="left")
                    )
                    if end <= start:
                        ranges = {}
                        break
                    ranges[name] = (start, end)
                if self._time_window and not ranges:
                    continue
            for name, window in self._window.items():
                start = anchor + window.start
                end = anchor + window.end
                if self._boundary == BOUNDARY_CLAMP:
                    start = max(start, 0)
                    end = min(end, rows)
                if end <= start:
                    ranges = {}
                    break
                ranges[name] = (start, end)
            if not ranges:
                continue
            sample: dict[str, np.ndarray] = {
                name: _to_numpy(columns[name], start, end)
                for name, (start, end) in ranges.items()
            }
            if stream_tables:
                stream_start, stream_end = next(iter(ranges.values()))
                for stream, right in zip(self._streams, stream_tables):
                    aligned = aligned_columns(
                        stream,
                        right,
                        np.asarray(times[stream_start:stream_end], dtype=np.float64),
                    )
                    if aligned is None:
                        sample = {}
                        break
                    for name, values in aligned.items():
                        if name in sample:
                            raise ValueError(
                                f"secondary stream column {name!r} collides "
                                "with a primary column"
                            )
                        sample[name] = values
                if not sample:
                    continue
            if episode_video is not None:
                frame_start = max(anchor + video_start, 0)
                frame_end = min(anchor + video_end, rows)
                if frame_end > frame_start:
                    for camera, frames in episode_video.frames(
                        frame_start, frame_end
                    ).items():
                        if camera in sample:
                            raise ValueError(
                                f"video camera {camera!r} collides with a window "
                                "column of the same name"
                            )
                        sample[camera] = frames
            yield sample

    def _resolve_manifest_anchors(
        self, table: pa.Table, anchors: list[int]
    ) -> list[int]:
        selection = self._selection
        assert selection is not None
        column = selection.order_by
        if column not in table.column_names:
            raise ValueError(
                f"manifest order_by column {column!r} is missing from the scan"
            )
        values = table[column].combine_chunks()
        lookup: dict[int, int] = {}
        for index, value in enumerate(values.to_pylist()):
            if value is None:
                continue
            key = int(value)
            if key in lookup:
                raise ValueError(
                    f"order_by column {column!r} has duplicate value {key} in one "
                    "episode; manifest anchors must be unique"
                )
            lookup[key] = index
        missing = [anchor for anchor in anchors if anchor not in lookup]
        if missing:
            preview = missing[:5]
            raise ValueError(
                f"manifest anchors not found in order_by column {column!r}: "
                f"{preview}{'...' if len(missing) > len(preview) else ''}"
            )
        return [lookup[anchor] for anchor in anchors]

    def _read_unit(self, unit: LakeSoulScanPlanPartition) -> pa.Table:
        config = dataclasses.replace(self._config, scan_partitions=(unit,))
        dataset = lakesoul_dataset(config)
        columns = list(self._window) + list(self._time_window)
        if self._time_window or self._streams:
            columns.append(self._time_column)
        if self._selection is not None:
            columns.append(self._selection.order_by)
        columns = list(dict.fromkeys(columns))
        return dataset.to_table(columns=columns)

    def _resolve_units(
        self,
        scan: LakeSoulScan,
        episodes: Sequence[str] | None,
        episode_column: str | None,
    ) -> tuple[LakeSoulScanPlanPartition, ...]:
        units = list(scan.scan_plan())
        if episodes is not None:
            episode_column = self._resolve_episode_column(scan, episode_column)
            wanted = {str(episode) for episode in episodes}
            selected = [
                unit
                for unit in units
                if _unit_partition_map(unit).get(episode_column) in wanted
            ]
            found = {_unit_partition_map(unit).get(episode_column) for unit in selected}
            missing = wanted - found
            if missing:
                preview = sorted(missing)[:5]
                raise ValueError(
                    f"episodes not found in partition column {episode_column!r}: "
                    f"{preview}{'...' if len(missing) > len(preview) else ''}"
                )
            units = selected
        units.sort(key=unit_key)
        return tuple(units)

    @staticmethod
    def _resolve_episode_column(scan: LakeSoulScan, episode_column: str | None) -> str:
        if episode_column is not None:
            return episode_column
        partition_columns = tuple(scan.table.partition_by)
        if len(partition_columns) != 1:
            raise ValueError(
                "episode_column is required unless the table is partitioned by "
                f"exactly one column, got {partition_columns}"
            )
        return partition_columns[0]


def _to_numpy(column: pa.Array, start: int, end: int) -> np.ndarray:
    """Slice ``column`` to ``[start, end)`` as a numpy array.

    Fixed-size list columns (vectors) become 2-D arrays so downstream tensor
    conversion needs no object-array handling; other columns use Arrow's
    default conversion (binary columns become object arrays of bytes).
    """
    array = column.slice(start, end - start)
    if pa.types.is_fixed_size_list(array.type) and not pa.types.is_nested(
        array.type.value_type
    ):
        flattened = array.flatten().to_numpy(zero_copy_only=False)
        return flattened.reshape(len(array), array.type.list_size)
    return array.to_numpy(zero_copy_only=False)


def _dataset_from_state(
    config: LakeSoulScanConfig,
    units: tuple[LakeSoulScanPlanPartition, ...],
    window: dict[str, Window],
    stride: int,
    boundary: str,
    seed: int,
    video: GopVideo | None,
    video_window: str | tuple[int, int] | None,
    time_window: dict[str, tuple[float, float]],
    time_column: str,
    streams: tuple[SecondaryStream, ...],
    selection: ManifestSelection | None,
    shuffle: bool,
) -> EmbodiedDataset:
    dataset = object.__new__(EmbodiedDataset)
    dataset._scan = None
    dataset._window = dict(window)
    dataset._stride = stride
    dataset._boundary = boundary
    dataset._seed = seed
    dataset._epoch = 0
    dataset._config = config
    dataset._units = tuple(units)
    dataset._video = video
    dataset._video_window = video_window
    dataset._time_window = dict(time_window)
    dataset._time_column = time_column
    dataset._streams = tuple(streams)
    dataset._selection = selection
    dataset._shuffle = shuffle
    return dataset


def _normalize_shard(rank: int | None, world_size: int | None) -> tuple[int, int]:
    if rank is None and world_size is None:
        return 0, 1
    if rank is None or world_size is None:
        raise ValueError("rank and world_size must be both set or both unset")
    if world_size < 1:
        raise ValueError(f"world_size must be positive, got {world_size}")
    if not 0 <= rank < world_size:
        raise ValueError(f"rank {rank} is out of range for world_size {world_size}")
    return rank, world_size
