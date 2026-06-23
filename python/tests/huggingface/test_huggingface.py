# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import pyarrow as pa

from lakesoul.huggingface import from_lakesoul


def test_from_lakesoul():
    class FakeScan:
        @property
        def schema(self):
            return pa.schema([pa.field("id", pa.int64())])

        def to_batches(self):
            yield pa.record_batch({"id": [1, 2]})
            yield pa.record_batch({"id": [3]})

    ds = from_lakesoul(FakeScan())
    cnt = 0
    for _ in ds:
        cnt += 1
    assert cnt == 3
