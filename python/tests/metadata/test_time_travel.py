# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime as dt
import time
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pytest

from lakesoul import LakeSoulCatalog


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def _batch(schema: pa.Schema, value: int, label: str) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([value], type=pa.int64()),
            "label": pa.array([label], type=pa.string()),
        },
        schema=schema,
    )


def _scan_ids(table, **options) -> list[int]:
    scan = table.scan()
    if options:
        scan = scan.options(**options)
    return scan.to_arrow_table().column("id").to_pylist()


def test_timestamp_time_travel(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("tt")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string()),
        ]
    )
    table = catalog.create_table(name, path=(tmp_path / name).as_uri(), schema=schema)

    try:
        table.write_arrow(_batch(schema, 1, "first"), format="parquet")
        as_of = catalog._client.get_all_partition_info(table.id)[0].timestamp
        time.sleep(0.05)
        table.write_arrow(_batch(schema, 2, "second"), format="parquet")

        assert _scan_ids(table) == [1, 2]
        assert _scan_ids(table, timestamp=as_of) == [1]

        aware = dt.datetime.fromtimestamp(as_of / 1000, tz=dt.timezone.utc)
        assert _scan_ids(table, timestamp=aware) == [1]
        assert _scan_ids(table, timestamp=aware, time_zone="Asia/Shanghai") == [1]
        assert _scan_ids(table, timestamp=aware.isoformat()) == [1]
    finally:
        catalog.drop_table(name, if_exists=True)


def test_timestamp_time_travel_requires_explicit_time_zone(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("tt_tz")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string()),
        ]
    )
    table = catalog.create_table(name, path=(tmp_path / name).as_uri(), schema=schema)

    try:
        table.write_arrow(_batch(schema, 1, "first"), format="parquet")

        for value in (
            dt.datetime(2026, 1, 1),  # noqa: DTZ001 - naive on purpose
            dt.date(2026, 1, 1),
            "2026-01-01T00:00:00",
            "2026-01-01",
        ):
            with pytest.raises(ValueError, match="time zone"):
                _scan_ids(table, timestamp=value)

        with pytest.raises(ValueError, match="time_zone"):
            _scan_ids(table, time_zone="UTC")

        tomorrow = dt.datetime.now(dt.timezone.utc).date() + dt.timedelta(days=1)
        assert _scan_ids(table, timestamp=tomorrow, time_zone="UTC") == [1]
    finally:
        catalog.drop_table(name, if_exists=True)
