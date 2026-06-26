# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import importlib
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.compute as pc

from lakesoul.arrow import LakeSoulScanConfig
from lakesoul.metadata import LakeSoulScanPlanPartition
from lakesoul.ray.read_lakesoul import LakeSoulDatasource

from .conftest import TABLE_NAME_TEST_LFS, lakesoul_ray_dataset, lakesoul_scan

ray_read_module = importlib.import_module("lakesoul.ray.read_lakesoul")


def test_use_partition(ray_session):
    ds = lakesoul_ray_dataset(TABLE_NAME_TEST_LFS, partitions={"c2": "1"})
    assert ds.count() == 2
    ds = lakesoul_ray_dataset(TABLE_NAME_TEST_LFS, partitions={"c2": "2"})
    assert ds.count() == 2
    ds = lakesoul_ray_dataset(TABLE_NAME_TEST_LFS, partitions={"c2": "3"})
    assert ds.count() == 3


def test_get_read_tasks_count(ray_session):
    """get_read_tasks should return one ReadTask per data file."""
    source = LakeSoulDatasource(
        lakesoul_scan(TABLE_NAME_TEST_LFS, partitions={"c2": "1"}).to_scan_config()
    )
    tasks = source.get_read_tasks(parallelism=1)
    assert len(tasks) > 1


def test_read_lakesoul_forwards_filter_to_ray_datasource(monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("part", pa.string())])
    scan_config = _scan_config(schema)
    expression = pc.field("part") == "north"
    captured = {}

    def fake_read_datasource(datasource):
        captured["columns"] = datasource._columns
        captured["filter"] = datasource._filter
        captured["schema"] = datasource._schema
        return SimpleNamespace(_plan=None)

    monkeypatch.setattr(
        ray_read_module.ray.data, "read_datasource", fake_read_datasource
    )
    scan = SimpleNamespace(
        columns=("id",),
        expression=expression,
        to_scan_config=lambda: scan_config,
    )

    ray_read_module.read_lakesoul(scan)  # type: ignore

    assert captured["columns"] == ("id",)
    assert captured["filter"] is expression
    assert captured["schema"] == pa.schema([pa.field("id", pa.int64())])


def test_datasource_read_tasks_forward_filter_to_arrow_scan(monkeypatch):
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("part", pa.string())])
    scan_config = _scan_config(schema)
    expression = pc.field("part") == "north"
    source = LakeSoulDatasource(scan_config, columns=("id",), filter=expression)
    captured = {}

    def fake_read_lakesoul_scan(config, columns, filter):
        captured["scan_partitions"] = config.scan_partitions
        captured["rank"] = config.rank
        captured["world_size"] = config.world_size
        captured["columns"] = columns
        captured["filter"] = filter
        return iter([pa.table({"id": [1]})])

    monkeypatch.setattr(
        ray_read_module,
        "_read_lakesoul_scan",
        fake_read_lakesoul_scan,
    )

    tasks = source.get_read_tasks(parallelism=1)
    list(tasks[0].read_fn())

    assert captured["scan_partitions"] == (scan_config.scan_partitions[0],)
    assert captured["rank"] is None
    assert captured["world_size"] is None
    assert captured["columns"] == ("id",)
    assert captured["filter"] is expression


def _scan_config(schema: pa.Schema) -> LakeSoulScanConfig:
    return LakeSoulScanConfig(
        table_name="target",
        namespace="analytics",
        schema=schema,
        partition_schema=None,
        scan_partitions=(
            LakeSoulScanPlanPartition(["file:///tmp/part-0.parquet"], ["id"]),
        ),
        partitions={},
        object_store_options={},
        rank=0,
        world_size=2,
    )
