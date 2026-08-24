# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace
from typing import cast

import pytest

from lakesoul.catalog import LakeSoulScan, LakeSoulTable
from lakesoul.io import WriteResult


class FakeDaftDataFrame:
    pass


def _load_daft_module(monkeypatch: pytest.MonkeyPatch):
    fake_daft = SimpleNamespace(DataFrame=FakeDaftDataFrame)
    monkeypatch.setitem(sys.modules, "daft", fake_daft)
    monkeypatch.delitem(sys.modules, "lakesoul.daft", raising=False)
    return importlib.import_module("lakesoul.daft"), fake_daft


def test_catalog_daft_methods_require_optional_dependency(monkeypatch) -> None:
    import lakesoul.daft as daft_module

    def missing_import(name: str):
        if name == "daft":
            raise ImportError("missing daft")
        return importlib.import_module(name)

    monkeypatch.setattr(daft_module.importlib, "import_module", missing_import)

    with pytest.raises(ImportError, match=r"lakesoul\[daft\]"):
        LakeSoulScan.to_daft(SimpleNamespace())  # type: ignore
    with pytest.raises(ImportError, match=r"lakesoul\[daft\]"):
        LakeSoulTable.write_daft(SimpleNamespace(), SimpleNamespace())  # type: ignore


def test_import_lakesoul_daft_registers_dataframe_method_without_ray(
    monkeypatch,
) -> None:
    monkeypatch.delitem(sys.modules, "ray", raising=False)
    daft_module, fake_daft = _load_daft_module(monkeypatch)

    assert "ray" not in sys.modules
    assert fake_daft.DataFrame.write_lakesoul is daft_module.write_lakesoul


def test_read_lakesoul_creates_native_source(monkeypatch) -> None:
    import lakesoul.daft as daft_module
    import lakesoul.daft.source as source_module

    expected_dataframe = object()
    scan_config = object()
    seen = {}

    class FakeSource:
        def __init__(self, config, *, columns, partition_columns) -> None:
            seen["config"] = config
            seen["columns"] = columns
            seen["partition_columns"] = partition_columns

        def read(self):
            return expected_dataframe

    monkeypatch.setattr(source_module, "LakeSoulDataSource", FakeSource)
    scan = cast(
        LakeSoulScan,
        SimpleNamespace(
            columns=("id",),
            table=SimpleNamespace(partition_by=("region",)),
            to_scan_config=lambda: scan_config,
        ),
    )

    result = daft_module.read_lakesoul(scan)

    assert result is expected_dataframe
    assert seen == {
        "config": scan_config,
        "columns": ("id",),
        "partition_columns": ("region",),
    }


def test_daft_tasks_disable_arrow_resharding(monkeypatch) -> None:
    import asyncio

    import pyarrow as pa
    from daft.io.pushdowns import Pushdowns

    import lakesoul.daft.source as source_module
    from lakesoul.arrow import LakeSoulScanConfig
    from lakesoul.metadata import LakeSoulScanPlanPartition

    captured = []

    class FakeTask:
        def __init__(self, scan_config, **kwargs) -> None:
            captured.append((scan_config, kwargs))

    monkeypatch.setattr(source_module, "LakeSoulDataSourceTask", FakeTask)
    source = source_module.LakeSoulDataSource(
        LakeSoulScanConfig(
            table_name="target",
            namespace="analytics",
            schema=pa.schema([pa.field("id", pa.int64())]),
            partition_schema=None,
            scan_partitions=(
                LakeSoulScanPlanPartition(["file:///tmp/part-0.parquet"], ["id"]),
                LakeSoulScanPlanPartition(["file:///tmp/part-1.parquet"], ["id"]),
            ),
            partitions={},
            object_store_options={},
            rank=1,
            world_size=2,
        ),
        columns=("id",),
        partition_columns=(),
    )

    async def collect_tasks():
        return [task async for task in source.get_tasks(Pushdowns())]

    tasks = asyncio.run(collect_tasks())

    assert len(tasks) == 1
    assert captured[0][0].scan_partitions[0].files == ["file:///tmp/part-1.parquet"]
    assert captured[0][0].rank is None
    assert captured[0][0].world_size is None


def test_write_lakesoul_uses_native_sink_and_returns_write_result(
    monkeypatch,
) -> None:
    import lakesoul.daft as daft_module
    import lakesoul.daft.sink as sink_module

    expected = WriteResult(files=(), partitions={}, row_count=3)
    seen = {}

    class FakeSink:
        def __init__(self, table, **kwargs) -> None:
            seen["table"] = table
            seen["kwargs"] = kwargs
            self.result = None

    class FakeDataFrame:
        def write_sink(self, sink):
            seen["sink"] = sink
            sink.result = expected
            return object()

    monkeypatch.setattr(sink_module, "LakeSoulDataSink", FakeSink)
    table = cast(LakeSoulTable, object())

    result = daft_module.write_lakesoul(
        FakeDataFrame(),
        table,
        format="parquet",
        max_file_size=1024,
    )

    assert result is expected
    assert seen["table"] is table
    assert seen["sink"].result is expected
    assert seen["kwargs"]["format"] == "parquet"
    assert seen["kwargs"]["max_file_size"] == 1024
