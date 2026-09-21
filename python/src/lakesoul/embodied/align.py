# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Read-time alignment of secondary tables for embodied datasets.

The :class:`SecondaryStream` describes a table that is aligned to another
table's rows by timestamp (nearest/backward/forward within a tolerance), and
:func:`align` materializes that alignment as a ``pyarrow.Table`` or into a
LakeSoul table.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pyarrow as pa

from lakesoul.arrow import lakesoul_dataset
from lakesoul.catalog import LakeSoulScan

DIRECTIONS = ("nearest", "backward", "forward")
MISSING_MODES = ("null", "skip")


@dataclass(frozen=True)
class SecondaryStream:
    """A table aligned to the primary table's rows by timestamp.

    ``columns`` selects the aligned value columns (all non-key columns when
    ``None``). ``direction`` picks the matching row, ``tolerance`` bounds the
    distance in seconds and ``missing`` decides whether unmatched rows become
    nulls or drop the whole sample.
    """

    scan: LakeSoulScan
    on: str = "timestamp"
    by: str = "episode_id"
    columns: Sequence[str] | None = None
    tolerance: float = 0.02
    direction: str = "nearest"
    missing: str = "null"
    suffix: str = ""

    def __post_init__(self) -> None:
        if self.direction not in DIRECTIONS:
            raise ValueError(
                f"direction must be one of {DIRECTIONS}, got {self.direction!r}"
            )
        if self.missing not in MISSING_MODES:
            raise ValueError(
                f"missing must be one of {MISSING_MODES}, got {self.missing!r}"
            )
        if self.tolerance < 0:
            raise ValueError(f"tolerance must be non-negative, got {self.tolerance}")


@dataclass(frozen=True)
class StreamTable:
    """One episode of a secondary stream, time ordered."""

    timestamps: np.ndarray
    columns: dict[str, np.ndarray]


def aligned_index(
    timestamps: np.ndarray,
    target: float,
    *,
    direction: str = "nearest",
    tolerance: float = 0.02,
) -> int | None:
    """Index of the row aligned to ``target``, or ``None`` when out of range."""
    if len(timestamps) == 0:
        return None
    position = int(np.searchsorted(timestamps, target))
    candidates: list[int] = []
    if direction in ("nearest", "forward") and position < len(timestamps):
        candidates.append(position)
    if direction in ("nearest", "backward") and position > 0:
        candidates.append(position - 1)
    best: tuple[float, int] | None = None
    for candidate in candidates:
        delta = abs(float(timestamps[candidate]) - target)
        if delta > tolerance:
            continue
        if best is None or delta < best[0]:
            best = (delta, candidate)
    return None if best is None else best[1]


def load_stream_unit(stream: SecondaryStream, episode_id: str) -> StreamTable:
    """Read one episode of a secondary stream, sorted by its timestamp."""
    config = stream.scan.to_scan_config()
    units = [
        unit
        for unit in stream.scan.scan_plan()
        if dict(unit.partition_info).get(stream.by) == str(episode_id)
    ]
    if not units:
        raise ValueError(
            f"secondary stream has no {stream.by}={episode_id!r} partition; "
            "partition the table by the 'by' column"
        )
    columns = list(stream.columns) if stream.columns is not None else None
    if columns is not None:
        schema_names = set(config.schema.names)
        for required in (stream.on, stream.by):
            if required in schema_names and required not in columns:
                columns.append(required)
    tables = []
    for unit in units:
        unit_config = dataclasses.replace(config, scan_partitions=(unit,))
        tables.append(lakesoul_dataset(unit_config).to_table(columns=columns))
    table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]

    if stream.by in table.column_names:
        values = table[stream.by].to_pylist()
        mask = np.asarray([value == str(episode_id) for value in values], dtype=bool)
        if not mask.all():
            table = table.filter(pa.array(mask))

    order = np.argsort(table[stream.on].to_numpy(zero_copy_only=False), kind="stable")
    timestamps = np.asarray(
        table[stream.on].to_numpy(zero_copy_only=False), dtype=np.float64
    )[order]
    values = {
        name: table[name].to_numpy(zero_copy_only=False)[order]
        for name in table.column_names
        if name not in (stream.on, stream.by)
    }
    return StreamTable(timestamps=timestamps, columns=values)


def aligned_columns(
    stream: SecondaryStream,
    right: StreamTable,
    timestamps: np.ndarray,
    *,
    missing: str | None = None,
) -> dict[str, np.ndarray] | None:
    """Aligned arrays for every secondary column at ``timestamps``.

    Returns ``None`` when ``missing="skip"`` and any timestamp has no match.
    """
    mode = missing or stream.missing
    aligned: dict[str, np.ndarray] = {}
    for name, values in right.columns.items():
        selected: list[Any] = []
        any_missing = False
        for target in timestamps:
            index = aligned_index(
                right.timestamps,
                float(target),
                direction=stream.direction,
                tolerance=stream.tolerance,
            )
            if index is None:
                any_missing = True
                selected.append(None)
            else:
                selected.append(values[index])
        if any_missing and mode == "skip":
            return None
        aligned[f"{name}{stream.suffix}"] = np.asarray(selected)
    return aligned


def align(
    left_scan: LakeSoulScan,
    right_scan: LakeSoulScan,
    *,
    on: str = "timestamp",
    by: str = "episode_id",
    columns: Sequence[str] | None = None,
    tolerance: float = 0.02,
    direction: str = "nearest",
    missing: str = "null",
    suffix: str = "_r",
    into: Any | None = None,
) -> pa.Table:
    """Align ``right_scan`` to ``left_scan`` rows and return the joined table.

    The result appends one column per right-side column (``suffix`` applied)
    to the left table. Pass ``into=<LakeSoulTable>`` to also write the result
    with ``write_arrow``.
    """
    stream = SecondaryStream(
        scan=right_scan,
        on=on,
        by=by,
        columns=columns,
        tolerance=tolerance,
        direction=direction,
        missing=missing,
        suffix=suffix,
    )
    left_config = left_scan.to_scan_config()
    results: list[pa.Table] = []
    for unit in left_scan.scan_plan():
        episode_id = dict(unit.partition_info).get(by)
        if episode_id is None:
            raise ValueError(f"left table needs a {by!r} partition to align by episode")
        left_table = lakesoul_dataset(
            dataclasses.replace(left_config, scan_partitions=(unit,))
        ).to_table()
        right = load_stream_unit(stream, episode_id)
        left_times = np.asarray(
            left_table[on].to_numpy(zero_copy_only=False), dtype=np.float64
        )
        aligned = aligned_columns(stream, right, left_times, missing=missing)
        if aligned is None:
            continue
        arrays = list(left_table.columns)
        names = list(left_table.column_names)
        for name, array in aligned.items():
            arrays.append(pa.array(array))
            names.append(name)
        results.append(pa.Table.from_arrays(arrays, names=names))
    if not results:
        return left_table.schema.empty_table()
    table = pa.concat_tables(results)
    if into is not None:
        into.write_arrow(table)
    return table


__all__ = [
    "DIRECTIONS",
    "MISSING_MODES",
    "SecondaryStream",
    "StreamTable",
    "align",
    "aligned_columns",
    "aligned_index",
    "load_stream_unit",
]
