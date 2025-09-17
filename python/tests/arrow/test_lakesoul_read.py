# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from lakesoul.arrow import lakesoul_dataset
import pytest


def test_lakesoul_dataset():
    # before this make share tpch data(0.1) is ready
    lds = lakesoul_dataset("part", batch_size=1024)

    total_rows = 0
    total_cols = 0
    for batch in lds.to_batches():
        total_rows += batch.num_rows
        assert batch.num_columns == 9
        total_cols += batch.num_columns

    assert total_rows == 20000


def test_dataset_parameters():
    lds = lakesoul_dataset("part")
    with pytest.raises(NotImplementedError) as _:
        lds.scanner(columns=[])
    with pytest.raises(NotImplementedError) as _:
        lds.scanner(filter="")
    with pytest.raises(NotImplementedError) as _:
        lds.scanner(batch_readahead=1)
    with pytest.raises(NotImplementedError) as _:
        lds.scanner(fragment_readahead=1)
    with pytest.raises(NotImplementedError) as _:
        lds.scanner(fragment_scan_options="")
    with pytest.raises(NotImplementedError) as _:
        lds.scanner(cache_metadata=True)
    with pytest.raises(NotImplementedError) as _:
        lds.scanner(memory_pool=False)


def test_dataset_to_table():
    lds = lakesoul_dataset("part")
    table = lds.to_table()
    assert table.num_rows == 20000
