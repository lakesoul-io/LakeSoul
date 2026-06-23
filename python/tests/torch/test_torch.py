# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import pyarrow as pa

from lakesoul.torch.dataset import Dataset


def test_torch():
    class FakeScan:
        def to_batches(self):
            yield pa.record_batch({"id": [1, 2]})
            yield pa.record_batch({"id": [3]})

    ds = Dataset(FakeScan())
    row_cnt = 0
    for b in ds:
        row_cnt += len(b)
    assert row_cnt == 3
