# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime as dt
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest

from lakesoul import LakeSoulCatalog

SCHEMA = pa.schema([pa.field("id", pa.int64(), nullable=False)])


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def _batch(value: int) -> pa.Table:
    return pa.table({"id": pa.array([value], type=pa.int64())}, schema=SCHEMA)


def _ids(table, **options) -> list[int]:
    scan = table.scan()
    if options:
        scan = scan.options(**options)
    return scan.to_arrow_table().column("id").to_pylist()


def test_snapshot_and_tag_reads(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("snap")
    table = catalog.create_table(name, path=(tmp_path / name).as_uri(), schema=SCHEMA)

    try:
        table.write_arrow(_batch(1), format="parquet")
        snapshot_id = catalog.create_snapshot(name, "first")
        assert snapshot_id > 0

        table.write_arrow(_batch(2), format="parquet")
        assert sorted(_ids(table)) == [1, 2]

        assert catalog.create_tag(name, "v1", snapshot_id) == snapshot_id
        assert _ids(table, snapshot=snapshot_id) == [1]
        assert _ids(table, tag="v1") == [1]

        second = catalog.create_snapshot(name, "second")
        assert sorted(_ids(table, snapshot=second)) == [1, 2]

        assert [item.snapshot_id for item in catalog.list_snapshots(name)] == [
            snapshot_id,
            second,
        ]
        tags = catalog.list_tags(name)
        assert [(item.tag, item.snapshot_id) for item in tags] == [("v1", snapshot_id)]
    finally:
        catalog.drop_table(name, if_exists=True)


def test_snapshot_drop_requires_tag_removal(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("snap_drop")
    table = catalog.create_table(name, path=(tmp_path / name).as_uri(), schema=SCHEMA)

    try:
        table.write_arrow(_batch(1), format="parquet")
        snapshot_id = catalog.create_tag(name, "keep")

        with pytest.raises(ValueError, match="tagged"):
            catalog.drop_snapshot(name, snapshot_id)

        assert catalog.drop_tag(name, "keep")
        assert catalog.drop_snapshot(name, snapshot_id)
        assert catalog.list_snapshots(name) == []

        with pytest.raises(ValueError, match="unknown tag"):
            _ids(table, tag="keep")
        with pytest.raises(ValueError, match="unknown snapshot"):
            _ids(table, snapshot=snapshot_id)

        assert _ids(table) == [1]
    finally:
        catalog.drop_table(name, if_exists=True)


def test_time_travel_options_are_exclusive(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("snap_excl")
    table = catalog.create_table(name, path=(tmp_path / name).as_uri(), schema=SCHEMA)

    try:
        table.write_arrow(_batch(1), format="parquet")
        snapshot_id = catalog.create_snapshot(name)

        with pytest.raises(ValueError, match="only one of"):
            _ids(table, snapshot=snapshot_id, tag="v1")
        with pytest.raises(ValueError, match="only one of"):
            _ids(
                table,
                timestamp=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
                snapshot=snapshot_id,
            )
    finally:
        catalog.drop_table(name, if_exists=True)
