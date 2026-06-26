# SPDX-FileCopyrightText: 2023,2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from typing import TYPE_CHECKING

import torch

if TYPE_CHECKING:
    from lakesoul.catalog import LakeSoulScan


class Dataset(torch.utils.data.IterableDataset):
    def __init__(self, scan: LakeSoulScan) -> None:
        self._scan = scan

    def __iter__(self):
        yield from self._scan.to_batches()
