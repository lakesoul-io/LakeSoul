# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from typing import cast

import pyarrow as pa

import lakesoul.torch.dataset as dataset_module
from lakesoul.arrow import LakeSoulScanConfig
from lakesoul.catalog import LakeSoulScan
from lakesoul.torch.dataset import Dataset


class FakeScan:
    columns = ("id",)
    expression = None

    def __init__(self, scan_config: LakeSoulScanConfig) -> None:
        self._scan_config = scan_config

    def to_scan_config(self) -> LakeSoulScanConfig:
        return self._scan_config


def _fake_scan(scan_config: LakeSoulScanConfig) -> LakeSoulScan:
    return cast(LakeSoulScan, FakeScan(scan_config))


def _scan_config(
    *,
    rank: int | None = None,
    world_size: int | None = None,
) -> LakeSoulScanConfig:
    return LakeSoulScanConfig(
        table_name="target",
        namespace="analytics",
        schema=pa.schema([pa.field("id", pa.int64())]),
        partition_schema=None,
        scan_partitions=(),
        partitions={},
        object_store_options={},
        rank=rank,
        world_size=world_size,
    )


def _capture_arrow_dataset(monkeypatch, captured):
    class FakeArrowDataset:
        def to_batches(self, *, columns, filter):
            captured["columns"] = columns
            captured["filter"] = filter
            yield pa.record_batch({"id": [1, 2, 3]})

    def fake_lakesoul_dataset(scan_config):
        captured["scan_config"] = scan_config
        return FakeArrowDataset()

    monkeypatch.setattr(dataset_module, "lakesoul_dataset", fake_lakesoul_dataset)


def test_torch_dataset_reads_unsharded_scan(monkeypatch) -> None:
    captured = {}
    _capture_arrow_dataset(monkeypatch, captured)
    monkeypatch.setattr(dataset_module.torch.distributed, "is_available", lambda: True)
    monkeypatch.setattr(
        dataset_module.torch.distributed,
        "is_initialized",
        lambda: False,
    )

    batches = list(Dataset(_fake_scan(_scan_config())))

    assert sum(len(batch) for batch in batches) == 3
    assert captured["scan_config"].rank is None
    assert captured["scan_config"].world_size is None
    assert captured["columns"] == ["id"]
    assert captured["filter"] is None


def test_torch_dataset_applies_distributed_shard(monkeypatch) -> None:
    captured = {}
    _capture_arrow_dataset(monkeypatch, captured)
    monkeypatch.setattr(dataset_module.torch.distributed, "is_available", lambda: True)
    monkeypatch.setattr(
        dataset_module.torch.distributed,
        "is_initialized",
        lambda: True,
    )
    monkeypatch.setattr(dataset_module.torch.distributed, "get_rank", lambda: 1)
    monkeypatch.setattr(dataset_module.torch.distributed, "get_world_size", lambda: 4)

    list(Dataset(_fake_scan(_scan_config())))

    assert captured["scan_config"].rank == 1
    assert captured["scan_config"].world_size == 4


def test_torch_dataset_preserves_explicit_shard(monkeypatch) -> None:
    captured = {}
    _capture_arrow_dataset(monkeypatch, captured)
    monkeypatch.setattr(dataset_module.torch.distributed, "is_available", lambda: True)
    monkeypatch.setattr(
        dataset_module.torch.distributed,
        "is_initialized",
        lambda: True,
    )
    monkeypatch.setattr(dataset_module.torch.distributed, "get_rank", lambda: 1)
    monkeypatch.setattr(dataset_module.torch.distributed, "get_world_size", lambda: 4)

    list(Dataset(_fake_scan(_scan_config(rank=0, world_size=2))))

    assert captured["scan_config"].rank == 0
    assert captured["scan_config"].world_size == 2
