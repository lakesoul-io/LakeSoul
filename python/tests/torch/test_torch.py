# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from lakesoul.torch.dataset import Dataset


def test_torch():
    ds = Dataset("part")
    row_cnt = 0
    for b in ds:
        row_cnt += len(b)
    assert row_cnt == 20000
