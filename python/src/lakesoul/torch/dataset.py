# SPDX-FileCopyrightText: 2023,2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from dataclasses import replace

from typing import TYPE_CHECKING

import torch
import torch.distributed as dist
from lakesoul.arrow import lakesoul_dataset

if TYPE_CHECKING:
    from lakesoul.catalog import LakeSoulScan


class Dataset(torch.utils.data.IterableDataset):
    def __init__(self, scan: LakeSoulScan) -> None:
        self._scan = scan

    def __iter__(self):
        scan_config = self._scan.to_scan_config()
        if scan_config.rank is None and scan_config.world_size is None:
            distributed_shard = _distributed_shard()
            if distributed_shard is not None:
                rank, world_size = distributed_shard
                scan_config = replace(
                    scan_config,
                    rank=rank,
                    world_size=world_size,
                )

        dataset = lakesoul_dataset(scan_config)
        yield from dataset.to_batches(
            columns=(
                list(self._scan.columns) if self._scan.columns is not None else None
            ),
            filter=self._scan.expression,
        )


def _distributed_shard() -> tuple[int, int] | None:
    if not dist.is_available() or not dist.is_initialized():
        return None
    return dist.get_rank(), dist.get_world_size()
