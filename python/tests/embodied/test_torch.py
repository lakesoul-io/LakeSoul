# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import numpy as np
import pytest

torch = pytest.importorskip("torch")

import lakesoul.embodied.torch as torch_module
from lakesoul.embodied.torch import Dataset


class FakeDataset:
    def __init__(self, num_samples: int = 10) -> None:
        self.epoch = 0
        self.calls: list[tuple[int, int, int]] = []
        self._num_samples = num_samples

    def set_epoch(self, epoch: int) -> None:
        self.epoch = epoch

    def iter_epoch(self, epoch: int, *, rank: int = 0, world_size: int = 1):
        self.calls.append((epoch, rank, world_size))
        for index in range(rank, self._num_samples, world_size):
            yield {
                "value": np.array([index], dtype=np.int64),
                "payload": np.array([f"row-{index}".encode()], dtype=object),
            }


class FakeWorker:
    def __init__(self, worker_id: int, num_workers: int) -> None:
        self.id = worker_id
        self.num_workers = num_workers


def _values(dataset: Dataset) -> list[int]:
    return [int(sample["value"][0]) for sample in dataset]


def _patch_shard(monkeypatch, rank: int, world_size: int) -> None:
    monkeypatch.setattr(torch_module, "_distributed_shard", lambda: (rank, world_size))


def test_samples_are_sharded_across_workers(monkeypatch) -> None:
    _patch_shard(monkeypatch, 0, 1)
    dataset = Dataset(FakeDataset(10))

    seen: list[int] = []
    for worker_id in range(2):
        monkeypatch.setattr(
            torch_module, "get_worker_info", lambda wid=worker_id: FakeWorker(wid, 2)
        )
        seen.extend(_values(dataset))

    assert sorted(seen) == list(range(10))


def test_rank_and_workers_combine_into_shard(monkeypatch) -> None:
    _patch_shard(monkeypatch, 1, 2)
    monkeypatch.setattr(torch_module, "get_worker_info", lambda: FakeWorker(1, 2))
    fake = FakeDataset(20)

    values = _values(Dataset(fake))

    assert fake.calls == [(0, 3, 4)]
    assert values == list(range(3, 20, 4))


def test_set_epoch_propagates(monkeypatch) -> None:
    _patch_shard(monkeypatch, 0, 1)
    fake = FakeDataset(4)
    dataset = Dataset(fake)

    dataset.set_epoch(5)
    list(dataset)

    assert fake.calls == [(5, 0, 1)]


def test_shuffle_buffer_keeps_all_samples_and_is_reproducible(monkeypatch) -> None:
    _patch_shard(monkeypatch, 0, 1)
    dataset = Dataset(FakeDataset(32), shuffle_buffer=8)

    first = _values(dataset)
    second = _values(dataset)

    assert first == second
    assert sorted(first) == list(range(32))
    assert first != list(range(32))


def test_numeric_columns_become_tensors(monkeypatch) -> None:
    _patch_shard(monkeypatch, 0, 1)
    dataset = Dataset(FakeDataset(1))

    sample = next(iter(dataset))

    assert isinstance(sample["value"], torch.Tensor)
    assert sample["value"].tolist() == [0]
    assert sample["payload"].dtype == object
    assert sample["payload"][0] == b"row-0"


def test_to_tensor_can_be_disabled(monkeypatch) -> None:
    _patch_shard(monkeypatch, 0, 1)
    dataset = Dataset(FakeDataset(1), to_tensor=False)

    sample = next(iter(dataset))

    assert isinstance(sample["value"], np.ndarray)
    assert isinstance(sample["payload"], np.ndarray)


def test_prefetch_preserves_order_and_count(monkeypatch) -> None:
    _patch_shard(monkeypatch, 0, 1)
    dataset = Dataset(FakeDataset(50), prefetch=4)

    assert _values(dataset) == list(range(50))


def test_prefetch_propagates_worker_errors(monkeypatch) -> None:
    _patch_shard(monkeypatch, 0, 1)

    class FailingDataset(FakeDataset):
        def iter_epoch(self, epoch: int, *, rank: int = 0, world_size: int = 1):
            yield {"value": np.array([1], dtype=np.int64)}
            raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        list(Dataset(FailingDataset(1), prefetch=2))


def test_read_only_arrays_are_copied_without_warning(monkeypatch) -> None:
    _patch_shard(monkeypatch, 0, 1)

    class ReadOnlyDataset(FakeDataset):
        def iter_epoch(self, epoch: int, *, rank: int = 0, world_size: int = 1):
            array = np.array([5], dtype=np.int64)
            array.setflags(write=False)
            yield {"value": array, "payload": np.array([b"x"], dtype=object)}

    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        sample = next(iter(Dataset(ReadOnlyDataset(1))))

    assert sample["value"].tolist() == [5]


def test_shuffle_buffer_validation() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        Dataset(FakeDataset(1), shuffle_buffer=-1)
    with pytest.raises(ValueError, match="prefetch"):
        Dataset(FakeDataset(1), prefetch=-1)
