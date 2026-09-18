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
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pyarrow as pa

from lakesoul.arrow import LakeSoulScanConfig, lakesoul_dataset
from lakesoul.catalog import LakeSoulScan
from lakesoul.metadata import LakeSoulScanPlanPartition

BOUNDARY_SKIP = "skip"
BOUNDARY_CLAMP = "clamp"
_BOUNDARIES = (BOUNDARY_SKIP, BOUNDARY_CLAMP)


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
    ) -> None:
        if stride < 1:
            raise ValueError(f"stride must be positive, got {stride}")
        if boundary not in _BOUNDARIES:
            raise ValueError(f"boundary must be one of {_BOUNDARIES}, got {boundary!r}")
        if not window:
            raise ValueError("window must define at least one column")

        self._scan: LakeSoulScan | None = scan
        self._window: dict[str, Window] = {
            name: _as_window(value) for name, value in window.items()
        }
        self._stride = stride
        self._boundary = boundary
        self._seed = int(seed)
        self._epoch = 0
        self._config = scan.to_scan_config()

        schema_names = set(self._config.schema.names)
        unknown = set(self._window) - schema_names
        if unknown:
            raise ValueError(
                f"window columns not found in table schema: {sorted(unknown)}"
            )

        self._units = self._resolve_units(scan, episodes, episode_column)

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
        order = np.random.default_rng([self._seed, epoch]).permutation(len(self._units))
        for index in order[rank::world_size]:
            unit = self._units[int(index)]
            yield from self._iter_unit(unit, epoch)

    def _iter_unit(
        self, unit: LakeSoulScanPlanPartition, epoch: int
    ) -> Iterator[dict[str, np.ndarray]]:
        table = self._read_unit(unit)
        rows = table.num_rows
        anchors = plan_anchor_order(
            rows=rows,
            stride=self._stride,
            seed=self._seed,
            epoch=epoch,
            unit_seed=_unit_seed(unit),
            windows=self._window,
            boundary=self._boundary,
        )
        columns = {name: table[name].combine_chunks() for name in self._window}
        for anchor in anchors.tolist():
            sample: dict[str, np.ndarray] = {}
            for name, window in self._window.items():
                start = anchor + window.start
                end = anchor + window.end
                if self._boundary == BOUNDARY_CLAMP:
                    start = max(start, 0)
                    end = min(end, rows)
                if end <= start:
                    sample = {}
                    break
                sample[name] = _to_numpy(columns[name], start, end)
            if sample:
                yield sample

    def _read_unit(self, unit: LakeSoulScanPlanPartition) -> pa.Table:
        config = dataclasses.replace(self._config, scan_partitions=(unit,))
        dataset = lakesoul_dataset(config)
        return dataset.to_table(columns=list(self._window))

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
