# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from lakesoul.arrow import LakeSoulScanConfig
from lakesoul.catalog import LakeSoulScan, LakeSoulTable, TableWriteConfig
from lakesoul.io import FileInfo, WriteResult
from lakesoul.metadata import LakeSoulScanPlanPartition


class FakeDaftDataFrame:
    def __init__(self, arrow_items: list[Any] | None = None) -> None:
        self._arrow_items = arrow_items or []
        self.results_buffer_size = None

    def to_arrow_iter(self, *, results_buffer_size: int | str = "num_cpus"):
        self.results_buffer_size = results_buffer_size
        return iter(self._arrow_items)


def _load_daft_module(monkeypatch: pytest.MonkeyPatch):
    fake_daft = SimpleNamespace(DataFrame=FakeDaftDataFrame)

    def from_arrow(tables):
        return SimpleNamespace(tables=list(tables))

    fake_daft.from_arrow = from_arrow
    monkeypatch.setitem(sys.modules, "daft", fake_daft)
    monkeypatch.delitem(sys.modules, "lakesoul.daft", raising=False)
    return importlib.import_module("lakesoul.daft"), fake_daft


def _write_result(row_count: int) -> WriteResult:
    file_info = FileInfo(
        partition="-5",
        path="file:///tmp/target/part-0.parquet",
        size=10,
        existing_columns=("id",),
        row_count=row_count,
        other_info={},
    )
    return WriteResult(
        files=(file_info,),
        partitions={"-5": (file_info,)},
        row_count=row_count,
    )


def _fake_table(schema: pa.Schema):
    committed: list[WriteResult] = []
    table_config = TableWriteConfig(
        table_name="target",
        namespace="analytics",
        path="file:///tmp/target",
        schema=schema,
        primary_keys=(),
        partition_by=(),
        hash_bucket_num=1,
        format="parquet",
    )

    class FakeCatalog:
        def _merge_object_store_options(self, overrides):
            merged = {"endpoint": "default", "region": "us"}
            merged.update(dict(overrides or {}))
            return merged

        def _commit_write_result(self, table, result):
            committed.append(result)

    table = SimpleNamespace(
        name="target",
        namespace="analytics",
        catalog=FakeCatalog(),
        write_config=lambda format="parquet": table_config,
    )
    return table, committed


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


def test_read_lakesoul_splits_scan_partitions_and_preserves_scan_options(
    monkeypatch,
) -> None:
    daft_module, _ = _load_daft_module(monkeypatch)
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("part", pa.string())])
    partitions = (
        LakeSoulScanPlanPartition(["file:///tmp/p0.parquet"], ["id"]),
        LakeSoulScanPlanPartition(["file:///tmp/p1.parquet"], ["id"]),
    )
    scan_config = LakeSoulScanConfig(
        table_name="target",
        namespace="analytics",
        schema=schema,
        partition_schema=None,
        scan_partitions=partitions,
        partitions={},
        object_store_options={},
        rank=0,
        world_size=2,
    )
    expression = pc.field("part") == "north"
    seen: list[tuple[tuple[LakeSoulScanPlanPartition, ...], list[str], Any]] = []

    class FakeDataset:
        def __init__(self, config: LakeSoulScanConfig) -> None:
            self._config = config

        def to_batches(self, *, columns=None, filter=None):
            seen.append((self._config.scan_partitions, columns, filter)) # type: ignore
            value = 1 if self._config.scan_partitions[0] is partitions[0] else 2
            yield pa.record_batch([pa.array([value], type=pa.int64())], names=["id"])

    monkeypatch.setattr(
        daft_module,
        "lakesoul_dataset",
        lambda config: FakeDataset(config),
    )
    scan = SimpleNamespace(
        columns=("id",),
        expression=expression,
        to_scan_config=lambda: scan_config,
    )

    dataframe = daft_module.read_lakesoul(scan)

    assert [table.to_pydict() for table in dataframe.tables] == [
        {"id": [1]},
        {"id": [2]},
    ]
    assert [item[0] for item in seen] == [(partitions[0],), (partitions[1],)]
    assert all(item[1] == ["id"] for item in seen)
    assert all(item[2] is expression for item in seen)


def test_read_lakesoul_empty_table_keeps_projected_schema(monkeypatch) -> None:
    daft_module, _ = _load_daft_module(monkeypatch)
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("value", pa.string())])
    scan_config = LakeSoulScanConfig(
        table_name="target",
        namespace="analytics",
        schema=schema,
        partition_schema=None,
        scan_partitions=(LakeSoulScanPlanPartition([], []),),
        partitions={},
        object_store_options={},
    )
    monkeypatch.setattr(
        daft_module,
        "lakesoul_dataset",
        lambda config: pytest.fail("empty scans must not create Arrow datasets"),
    )
    scan = SimpleNamespace(
        columns=("id",),
        expression=None,
        to_scan_config=lambda: scan_config,
    )

    dataframe = daft_module.read_lakesoul(scan)

    assert len(dataframe.tables) == 1
    assert dataframe.tables[0].num_rows == 0
    assert dataframe.tables[0].schema == pa.schema([pa.field("id", pa.int64())])


def test_write_lakesoul_streams_tables_normalizes_metadata_and_commits(
    monkeypatch,
) -> None:
    daft_module, _ = _load_daft_module(monkeypatch)
    expected_schema = pa.schema(
        [pa.field("id", pa.int64())],
        metadata={b"lakesoul": b"schema"},
    )
    table_handle, committed = _fake_table(expected_schema)
    dataframe = FakeDaftDataFrame([pa.table({"id": [1, 2]})])
    writer_instances = []

    class FakeWriter:
        def __init__(self, config) -> None:
            self.config = config
            self.writes = []
            writer_instances.append(self)

        def write(self, data) -> int:
            self.writes.append(data)
            return data.num_rows

        def finish(self) -> WriteResult:
            return _write_result(sum(table.num_rows for table in self.writes))

        def abort(self) -> None:
            raise AssertionError("abort should not be called")

    monkeypatch.setattr(daft_module, "Writer", FakeWriter)

    result = daft_module.write_lakesoul(
        dataframe,
        table_handle,
        object_store_options={"endpoint": "override"},
        results_buffer_size=2,
    )

    assert result.row_count == 2
    assert dataframe.results_buffer_size == 2
    assert writer_instances[0].config.object_store_options == {
        "endpoint": "override",
        "region": "us",
    }
    assert writer_instances[0].writes[0].schema.equals(
        expected_schema,
        check_metadata=True,
    )
    assert committed == [result]


def test_write_lakesoul_skips_empty_tables_without_commit(monkeypatch) -> None:
    daft_module, _ = _load_daft_module(monkeypatch)
    schema = pa.schema([pa.field("id", pa.int64())])
    table_handle, committed = _fake_table(schema)
    dataframe = FakeDaftDataFrame([pa.Table.from_batches([], schema=schema)])

    def unexpected_writer(config):
        raise AssertionError("empty writes must not create a native writer")

    monkeypatch.setattr(daft_module, "Writer", unexpected_writer)

    result = daft_module.write_lakesoul(dataframe, table_handle)

    assert result.files == ()
    assert result.row_count == 0
    assert committed == []


def test_write_lakesoul_aborts_and_does_not_commit_on_failure(monkeypatch) -> None:
    daft_module, _ = _load_daft_module(monkeypatch)
    schema = pa.schema([pa.field("id", pa.int64())])
    table_handle, committed = _fake_table(schema)
    dataframe = FakeDaftDataFrame(
        [
            pa.table({"id": [1]}),
            pa.table({"other": [2]}),
        ]
    )
    writer_instances = []

    class FakeWriter:
        def __init__(self, config) -> None:
            self.aborted = False
            writer_instances.append(self)

        def write(self, data) -> int:
            return data.num_rows

        def finish(self) -> WriteResult:
            raise AssertionError("finish should not be called")

        def abort(self) -> None:
            self.aborted = True

    monkeypatch.setattr(daft_module, "Writer", FakeWriter)

    with pytest.raises(ValueError, match="schema does not match"):
        daft_module.write_lakesoul(dataframe, table_handle)

    assert writer_instances[0].aborted
    assert committed == []
