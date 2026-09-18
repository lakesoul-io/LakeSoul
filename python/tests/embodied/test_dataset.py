# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

import lakesoul.embodied.dataset as dataset_module
from lakesoul.arrow import LakeSoulScanConfig
from lakesoul.embodied import EmbodiedDataset, Window
from lakesoul.embodied.dataset import _as_window, plan_anchor_order
from lakesoul.metadata import LakeSoulScanPlanPartition

SCHEMA = pa.schema(
    [
        pa.field("state", pa.int64()),
        pa.field("action", pa.int64()),
        pa.field("image", pa.binary()),
    ]
)


def _table(rows: int, offset: int = 0) -> pa.Table:
    values = np.arange(offset, offset + rows, dtype=np.int64)
    return pa.table(
        {
            "state": pa.array(values, type=pa.int64()),
            "action": pa.array(values * 10, type=pa.int64()),
            "image": pa.array(
                [f"img-{value}".encode() for value in values], type=pa.binary()
            ),
        }
    )


def _unit(episode: str, file: str) -> LakeSoulScanPlanPartition:
    return LakeSoulScanPlanPartition(
        files=[file],
        primary_keys=[],
        partition_info=[("episode_id", episode)],
    )


class _FakeTable:
    partition_by = ("episode_id",)


class _FakeScan:
    def __init__(
        self, units: list[LakeSoulScanPlanPartition], schema: pa.Schema = SCHEMA
    ) -> None:
        self._units = tuple(units)
        self._config = LakeSoulScanConfig(
            table_name="episodes",
            namespace="default",
            schema=schema,
            partition_schema=None,
            scan_partitions=self._units,
            partitions={},
            object_store_options={},
        )
        self.table = _FakeTable()

    def scan_plan(self):
        return self._units

    def to_scan_config(self) -> LakeSoulScanConfig:
        return self._config


class _FakeArrowDataset:
    def __init__(self, table: pa.Table) -> None:
        self._table = table

    def to_table(self, columns=None, **kwargs) -> pa.Table:
        if columns is None:
            return self._table
        return self._table.select(columns)


def _install_reader(monkeypatch, tables: dict[str, pa.Table]) -> list[str]:
    reads: list[str] = []

    def fake_lakesoul_dataset(config: LakeSoulScanConfig) -> _FakeArrowDataset:
        (unit,) = config.scan_partitions
        reads.append(unit.files[0])
        return _FakeArrowDataset(tables[unit.files[0]])

    monkeypatch.setattr(dataset_module, "lakesoul_dataset", fake_lakesoul_dataset)
    return reads


def _dataset(
    monkeypatch,
    units: list[LakeSoulScanPlanPartition],
    tables: dict[str, pa.Table],
    **kwargs,
) -> tuple[EmbodiedDataset, list[str]]:
    reads = _install_reader(monkeypatch, tables)
    return EmbodiedDataset(_FakeScan(units), **kwargs), reads


def _anchor_of(sample: dict[str, np.ndarray]) -> int:
    """Anchors are recoverable because state is ``[anchor - 2, anchor)``."""
    return int(sample["state"][-1]) + 1


def test_iter_epoch_matches_naive_slicing(monkeypatch) -> None:
    unit = _unit("a", "file-a")
    dataset, _ = _dataset(
        monkeypatch,
        [unit],
        {"file-a": _table(10)},
        window={"state": (-2, 0), "action": (0, 2)},
        stride=3,
    )

    samples = list(dataset.iter_epoch(0))

    assert {tuple(sample["state"]) for sample in samples} == {(1, 2), (4, 5)}
    assert {tuple(sample["action"]) for sample in samples} == {(30, 40), (60, 70)}
    assert {_anchor_of(sample) for sample in samples} == {3, 6}


def test_seed_reproduces_and_epoch_reshuffles(monkeypatch) -> None:
    unit = _unit("a", "file-a")
    window = {"state": (0, 1)}
    tables = {"file-a": _table(200)}

    first, _ = _dataset(monkeypatch, [unit], tables, window=window, seed=7)
    second, _ = _dataset(monkeypatch, [unit], tables, window=window, seed=7)
    other, _ = _dataset(monkeypatch, [unit], tables, window=window, seed=8)

    epoch0 = [int(sample["state"][0]) for sample in first.iter_epoch(0)]
    assert epoch0 == [int(sample["state"][0]) for sample in second.iter_epoch(0)]
    assert epoch0 != [int(sample["state"][0]) for sample in other.iter_epoch(0)]
    assert epoch0 != [int(sample["state"][0]) for sample in first.iter_epoch(1)]
    assert sorted(epoch0) == list(range(200))


def test_rank_shards_are_disjoint_and_complete(monkeypatch) -> None:
    units = [_unit("a", "file-a"), _unit("b", "file-b")]
    tables = {"file-a": _table(10), "file-b": _table(6, offset=100)}
    window = {"state": (0, 1)}
    world_size = 3

    per_rank = []
    for rank in range(world_size):
        dataset, _ = _dataset(
            monkeypatch, units, tables, window=window, seed=3, stride=2
        )
        per_rank.append(
            {
                int(sample["state"][0])
                for sample in dataset.iter_epoch(0, rank=rank, world_size=world_size)
            }
        )

    union = set().union(*per_rank)
    assert sum(len(shard) for shard in per_rank) == len(union)
    assert union == set(range(0, 10, 2)) | set(range(100, 106, 2))


def test_rank_sharding_reads_only_assigned_units(monkeypatch) -> None:
    units = [_unit(name, f"file-{name}") for name in ("a", "b", "c", "d")]
    tables = {f"file-{name}": _table(4) for name in ("a", "b", "c", "d")}
    window = {"state": (0, 1)}

    reads_by_rank = []
    for rank in range(2):
        dataset, reads = _dataset(monkeypatch, units, tables, window=window, seed=1)
        list(dataset.iter_epoch(0, rank=rank, world_size=2))
        reads_by_rank.append(reads)

    first, second = reads_by_rank
    assert len(first) == len(second) == 2
    assert set(first).isdisjoint(second)
    assert set(first) | set(second) == {f"file-{name}" for name in ("a", "b", "c", "d")}


def test_boundary_skip_drops_and_clamp_keeps_edge_anchors(monkeypatch) -> None:
    unit = _unit("a", "file-a")
    tables = {"file-a": _table(10)}
    window = {"state": (-3, 1)}

    skipped, _ = _dataset(
        monkeypatch, [unit], tables, window=window, stride=9, boundary="skip"
    )
    assert [int(sample["state"][-1]) for sample in skipped.iter_epoch(0)] == [9]

    clamped, _ = _dataset(
        monkeypatch, [unit], tables, window=window, stride=9, boundary="clamp"
    )
    samples = sorted(sample["state"].tolist() for sample in clamped.iter_epoch(0))
    assert samples == [[0], [6, 7, 8, 9]]


def test_episode_filter_and_unknown_episode(monkeypatch) -> None:
    units = [_unit("a", "file-a"), _unit("b", "file-b")]
    tables = {"file-a": _table(4), "file-b": _table(4, offset=100)}
    dataset, reads = _dataset(
        monkeypatch,
        units,
        tables,
        window={"state": (0, 1)},
        episodes=["b"],
    )

    samples = list(dataset.iter_epoch(0))

    assert len(samples) == 4
    assert all(int(sample["state"][0]) >= 100 for sample in samples)
    assert reads == ["file-b"]

    with pytest.raises(ValueError, match="episodes not found"):
        _dataset(
            monkeypatch,
            units,
            tables,
            window={"state": (0, 1)},
            episodes=["missing"],
        )


def test_set_epoch_drives_iter(monkeypatch) -> None:
    unit = _unit("a", "file-a")
    dataset, _ = _dataset(
        monkeypatch, [unit], {"file-a": _table(50)}, window={"state": (0, 1)}
    )
    dataset.set_epoch(2)

    assert [int(sample["state"][0]) for sample in dataset] == [
        int(sample["state"][0]) for sample in dataset.iter_epoch(2)
    ]


def test_constructor_validation(monkeypatch) -> None:
    unit = _unit("a", "file-a")
    tables = {"file-a": _table(4)}

    with pytest.raises(ValueError, match="at least one column"):
        _dataset(monkeypatch, [unit], tables, window={})
    with pytest.raises(ValueError, match="not found in table schema"):
        _dataset(monkeypatch, [unit], tables, window={"missing": (0, 1)})
    with pytest.raises(ValueError, match="boundary"):
        _dataset(monkeypatch, [unit], tables, window={"state": (0, 1)}, boundary="pad")
    with pytest.raises(ValueError, match="stride"):
        _dataset(monkeypatch, [unit], tables, window={"state": (0, 1)}, stride=0)
    with pytest.raises(ValueError, match="both set or both unset"):
        list(
            _dataset(monkeypatch, [unit], tables, window={"state": (0, 1)})[
                0
            ].iter_epoch(0, rank=0)
        )
    with pytest.raises(ValueError, match="out of range"):
        list(
            _dataset(monkeypatch, [unit], tables, window={"state": (0, 1)})[
                0
            ].iter_epoch(0, rank=2, world_size=2)
        )


def test_dataset_pickle_roundtrip(monkeypatch) -> None:
    import pickle

    unit = _unit("a", "file-a")
    dataset, _ = _dataset(
        monkeypatch, [unit], {"file-a": _table(20)}, window={"state": (0, 1)}, seed=3
    )

    restored = pickle.loads(pickle.dumps(dataset))

    assert restored.num_units == dataset.num_units
    assert [int(sample["state"][0]) for sample in restored] == [
        int(sample["state"][0]) for sample in dataset
    ]


def test_fixed_size_list_columns_become_2d(monkeypatch) -> None:
    values = np.arange(12, dtype=np.float32).reshape(4, 3)
    table = pa.table(
        {
            "state": pa.FixedSizeListArray.from_arrays(
                pa.array(values.reshape(-1), type=pa.float32()), 3
            ),
            "action": pa.array(np.arange(4, dtype=np.float32)),
        }
    )
    _install_reader(monkeypatch, {"file-a": table})
    dataset = EmbodiedDataset(
        _FakeScan([_unit("a", "file-a")], schema=table.schema),
        window={"state": (0, 2), "action": (0, 2)},
    )

    sample = next(iter(dataset))

    assert sample["state"].shape == (2, 3)
    np.testing.assert_allclose(sample["state"], values[:2])
    assert sample["action"].shape == (2,)


def test_window_validation() -> None:
    with pytest.raises(ValueError, match="greater than start"):
        Window(0, 0)
    with pytest.raises(TypeError, match="Window or a"):
        _as_window((0, 1, 2))


def test_plan_anchor_order_is_deterministic() -> None:
    windows = {"state": Window(-2, 1)}
    first = plan_anchor_order(
        rows=100, stride=3, seed=1, epoch=0, unit_seed=42, windows=windows
    )
    second = plan_anchor_order(
        rows=100, stride=3, seed=1, epoch=0, unit_seed=42, windows=windows
    )

    np.testing.assert_array_equal(first, second)
    assert first.min() >= 2
    assert first.max() <= 99
    assert len(first) == len(np.unique(first))
