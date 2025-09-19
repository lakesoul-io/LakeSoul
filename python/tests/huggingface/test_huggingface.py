# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from lakesoul.huggingface import from_lakesoul


def test_from_lakesoul():
    ds = from_lakesoul("part")
    cnt = 0
    for _ in ds:
        cnt += 1
    assert cnt == 20000
