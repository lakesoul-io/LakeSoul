# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""PyTorch adapter for :class:`lakesoul.embodied.EmbodiedDataset`.

Compared with ``lakesoul.torch.Dataset`` (which yields whole record batches of
one distributed shard), this adapter yields window samples, shards across both
``torch.distributed`` ranks and DataLoader workers, and can mix samples across
units with an optional shuffle buffer.
"""

from __future__ import annotations

import queue
import random
import threading
from collections.abc import Iterable, Iterator
from typing import Any

import numpy as np
import torch
import torch.distributed as dist
from torch.utils.data import IterableDataset, get_worker_info

from .dataset import EmbodiedDataset


class Dataset(IterableDataset):
    """Sample-level iterable dataset over an :class:`EmbodiedDataset`.

    Each sample is a mapping of stream/column name to a tensor (numeric columns)
    or numpy object array (binary/variable-length columns). Set the epoch with
    :meth:`set_epoch` to reshuffle deterministically; combine with
    ``torch.distributed`` and ``DataLoader(num_workers=...)`` and sharding is
    derived from ``rank * num_workers + worker_id``.
    """

    def __init__(
        self,
        dataset: EmbodiedDataset,
        *,
        shuffle_buffer: int = 0,
        prefetch: int = 0,
        to_tensor: bool = True,
    ) -> None:
        if shuffle_buffer < 0:
            raise ValueError(
                f"shuffle_buffer must be non-negative, got {shuffle_buffer}"
            )
        if prefetch < 0:
            raise ValueError(f"prefetch must be non-negative, got {prefetch}")
        self._dataset = dataset
        self._shuffle_buffer = shuffle_buffer
        self._prefetch = prefetch
        self._to_tensor = to_tensor

    @property
    def dataset(self) -> EmbodiedDataset:
        return self._dataset

    def set_epoch(self, epoch: int) -> None:
        self._dataset.set_epoch(epoch)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        rank, world_size = _distributed_shard()
        worker = get_worker_info()
        worker_id = worker.id if worker is not None else 0
        num_workers = worker.num_workers if worker is not None else 1

        shard_id = rank * num_workers + worker_id
        shard_count = world_size * num_workers
        samples: Iterable[dict[str, np.ndarray]] = self._dataset.iter_epoch(
            self._dataset.epoch,
            rank=shard_id,
            world_size=shard_count,
        )
        if self._shuffle_buffer:
            rng = random.Random(f"{self._dataset.epoch}:{shard_id}")
            samples = _shuffle_buffer(samples, self._shuffle_buffer, rng)
        if self._prefetch:
            samples = _prefetch(samples, self._prefetch)
        for sample in samples:
            yield _convert(sample) if self._to_tensor else sample


def _distributed_shard() -> tuple[int, int]:
    if not dist.is_available() or not dist.is_initialized():
        return 0, 1
    return dist.get_rank(), dist.get_world_size()


def _shuffle_buffer(
    samples: Iterable[dict[str, np.ndarray]],
    size: int,
    rng: random.Random,
) -> Iterator[dict[str, np.ndarray]]:
    buffer: list[dict[str, np.ndarray]] = []
    for sample in samples:
        if len(buffer) < size:
            buffer.append(sample)
            continue
        index = rng.randrange(size)
        yield buffer[index]
        buffer[index] = sample
    rng.shuffle(buffer)
    yield from buffer


def _prefetch(
    samples: Iterable[dict[str, np.ndarray]],
    size: int,
) -> Iterator[dict[str, np.ndarray]]:
    """Read up to ``size`` samples ahead in a background thread.

    Order is preserved; the first error is re-raised in the consumer. The
    producer is a daemon thread, so an abandoned iterator cannot keep the
    process alive.
    """
    items: queue.Queue[Any] = queue.Queue(maxsize=size)
    done = object()

    def produce() -> None:
        try:
            for sample in samples:
                items.put(sample)
        except Exception as error:
            items.put(error)
        finally:
            items.put(done)

    thread = threading.Thread(target=produce, name="lakesoul-prefetch", daemon=True)
    thread.start()
    while True:
        item = items.get()
        if item is done:
            return
        if isinstance(item, Exception):
            raise item
        yield item


def _convert(sample: dict[str, np.ndarray]) -> dict[str, Any]:
    converted: dict[str, Any] = {}
    for name, array in sample.items():
        if array.dtype == object:
            converted[name] = array
            continue
        if not array.flags.writeable:
            array = array.copy()
        converted[name] = torch.as_tensor(array)
    return converted
