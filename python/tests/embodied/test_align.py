# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest

from lakesoul import LakeSoulCatalog
from lakesoul.embodied import EmbodiedDataset, SecondaryStream, align

PRIMARY_SCHEMA = pa.schema(
    [
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("frame_index", pa.int64(), nullable=False),
        pa.field("timestamp", pa.float64()),
        pa.field("state", pa.int64()),
    ]
)
STREAM_SCHEMA = pa.schema(
    [
        pa.field("episode_id", pa.string(), nullable=False),
        pa.field("timestamp", pa.float64()),
        pa.field("grip", pa.float64()),
    ]
)


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def _primary(rows: int = 6) -> pa.Table:
    values = list(range(rows))
    return pa.table(
        {
            "episode_id": pa.array(["ep1"] * rows, type=pa.string()),
            "frame_index": pa.array(values, type=pa.int64()),
            "timestamp": pa.array(
                [float(value) for value in values], type=pa.float64()
            ),
            "state": pa.array(values, type=pa.int64()),
        },
        schema=PRIMARY_SCHEMA,
    )


def _stream(timestamps: list[float], grips: list[float]) -> pa.Table:
    return pa.table(
        {
            "episode_id": pa.array(["ep1"] * len(timestamps), type=pa.string()),
            "timestamp": pa.array(timestamps, type=pa.float64()),
            "grip": pa.array(grips, type=pa.float64()),
        },
        schema=STREAM_SCHEMA,
    )


def _grips(primary, secondary, **kwargs) -> list[list[float]]:
    dataset = EmbodiedDataset(
        primary.scan(),
        window={"state": (0, 1)},
        streams=[
            SecondaryStream(
                secondary.scan(),
                columns=("grip",),
                tolerance=0.6,
                **kwargs,
            )
        ],
    )
    return [sample["grip"].tolist() for sample in dataset.iter_epoch(0)]


def test_secondary_stream_snapshot_and_tag_pin(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    primary_name = _table_name("align_p")
    secondary_name = _table_name("align_s")
    primary = catalog.create_table(
        primary_name,
        path=(tmp_path / primary_name).as_uri(),
        schema=PRIMARY_SCHEMA,
        partition_by=("episode_id",),
    )
    secondary = catalog.create_table(
        secondary_name,
        path=(tmp_path / secondary_name).as_uri(),
        schema=STREAM_SCHEMA,
        partition_by=("episode_id",),
    )

    try:
        primary.write_arrow(_primary(), format="parquet")
        secondary.write_arrow(
            _stream([0.5, 2.5, 4.5], [10.0, 20.0, 30.0]), format="parquet"
        )
        snapshot_id = catalog.create_snapshot(secondary)
        catalog.create_tag(secondary, "stream-v1", snapshot_id)
        secondary.write_arrow(_stream([1.0], [99.0]), format="parquet")

        live = _grips(primary, secondary)
        assert any(99.0 in row for row in live)
        pinned = _grips(primary, secondary, snapshot=snapshot_id)
        assert all(99.0 not in row for row in pinned)
        assert _grips(primary, secondary, tag="stream-v1") == pinned

        with pytest.raises(ValueError, match="mutually exclusive"):
            SecondaryStream(secondary.scan(), snapshot=snapshot_id, tag="stream-v1")
    finally:
        catalog.drop_table(primary_name, if_exists=True)
        catalog.drop_table(secondary_name, if_exists=True)


def test_align_skips_unmatched_rows(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    primary_name = _table_name("align_skip_p")
    secondary_name = _table_name("align_skip_s")
    primary = catalog.create_table(
        primary_name,
        path=(tmp_path / primary_name).as_uri(),
        schema=PRIMARY_SCHEMA,
        partition_by=("episode_id",),
    )
    secondary = catalog.create_table(
        secondary_name,
        path=(tmp_path / secondary_name).as_uri(),
        schema=STREAM_SCHEMA,
        partition_by=("episode_id",),
    )

    try:
        primary.write_arrow(_primary(), format="parquet")
        secondary.write_arrow(_stream([0.0, 2.0], [10.0, 20.0]), format="parquet")

        table = align(
            primary.scan(),
            secondary.scan(),
            columns=("grip",),
            tolerance=1.0,
            missing="skip",
        )
        assert table.column("state").to_pylist() == [0, 1, 2, 3]
        assert table.column("grip_r").to_pylist() == [10.0, 20.0, 20.0, 20.0]
    finally:
        catalog.drop_table(primary_name, if_exists=True)
        catalog.drop_table(secondary_name, if_exists=True)
