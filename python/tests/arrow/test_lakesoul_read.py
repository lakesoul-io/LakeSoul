# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0


import pyarrow as pa
import pytest
from duckdb import BinderException

from lakesoul.arrow import lakesoul_dataset


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
        _ = lds.scanner(columns=1)  # pyright: ignore[reportArgumentType]
    with pytest.raises(NotImplementedError) as _:
        _ = lds.scanner(columns=[1])  # pyright: ignore[reportArgumentType]
    with pytest.raises(NotImplementedError) as _:
        _ = lds.scanner(filter="")
    with pytest.raises(NotImplementedError) as _:
        _ = lds.scanner(batch_readahead=1)
    with pytest.raises(NotImplementedError) as _:
        _ = lds.scanner(fragment_readahead=1)
    with pytest.raises(NotImplementedError) as _:
        _ = lds.scanner(fragment_scan_options="")
    with pytest.raises(NotImplementedError) as _:
        _ = lds.scanner(cache_metadata=True)
    with pytest.raises(NotImplementedError) as _:
        _ = lds.scanner(memory_pool=False)


def test_dataset_to_table():
    lds = lakesoul_dataset("part")
    table = lds.to_table()
    assert table.num_rows == 20000


def test_invalid_filter():
    import pyarrow.compute as pc

    with pytest.raises(pa.ArrowInvalid) as _:
        filter = pc.field("no_in_schema") == 50
        scanner = lakesoul_dataset("part").scanner(filter=filter)
        _ = scanner.to_table()


def test_dataset_with_filter():
    import decimal

    import pyarrow.compute as pc

    filter = pc.field("p_size") == 50
    scanner = lakesoul_dataset("part").scanner(filter=filter)
    table = scanner.to_table()
    assert len(table) == 392

    val = pa.array([decimal.Decimal("1500.00")], type=pa.decimal128(15, 2))
    filter = pc.field("p_retailprice") >= val[0]
    scanner = lakesoul_dataset("part").scanner(filter=filter)
    table = scanner.to_table()
    assert len(table) == 8190

    lds = lakesoul_dataset("part")
    lds.filter(filter)
    scanner = lds.scanner()
    table = scanner.to_table()
    assert len(table) == 8190

    filter = (pc.field("p_retailprice") >= val[0]) & (pc.field("p_size") == 50)
    scanner = lakesoul_dataset("part").scanner(filter=filter)
    table = scanner.to_table()
    assert len(table) == 176


def test_fragment_with_filter():
    import decimal

    import pyarrow.compute as pc

    val = pa.array([decimal.Decimal("1500.00")], type=pa.decimal128(15, 2))
    filter = (pc.field("p_retailprice") >= val[0]) & (pc.field("p_size") == 50)
    fragment = list(lakesoul_dataset("part").get_fragments(filter=filter))[0]
    scanner = fragment.scanner()
    table = scanner.to_table()
    assert len(table) == 176


def test_filter_override():
    import decimal

    import pyarrow.compute as pc

    val = pa.array([decimal.Decimal("1500.00")], type=pa.decimal128(15, 2))
    decimal_filter = pc.field("p_retailprice") >= val[0]
    int_filter = pc.field("p_size") == 50
    # dataset
    lds = lakesoul_dataset("part")
    lds.filter(decimal_filter)
    scanner = lds.scanner(filter=int_filter)
    table = scanner.to_table()
    assert len(table) == 392
    # fragments
    fragment = list(lakesoul_dataset("part").get_fragments(filter=decimal_filter))[0]
    scanner = fragment.scanner(filter=int_filter)
    table = scanner.to_table()
    assert len(table) == 392


def test_prune_columns():
    # dataset
    cols = ["p_name"]
    scanner = lakesoul_dataset("part").scanner(columns=cols)
    table = scanner.to_table()
    assert len(table.columns) == 1

    # fragment
    fragment = list(lakesoul_dataset("part").get_fragments())[0]
    scanner = fragment.scanner(columns=cols)
    table = scanner.to_table()
    assert len(table.columns) == 1

    cols = ["p_name", "p_size"]
    scanner = lakesoul_dataset("part").scanner(columns=cols)
    table = scanner.to_table()
    assert len(table.columns) == 2

    fragment = list(lakesoul_dataset("part").get_fragments())[0]
    scanner = fragment.scanner(columns=cols)
    table = scanner.to_table()
    assert len(table.columns) == 2

    with pytest.raises(ValueError) as _:
        # no such field
        scanner = lakesoul_dataset("part").scanner(columns=["not existed"])
        table = scanner.to_table()
        _ = scanner.to_table()

    with pytest.raises(ValueError) as _:
        # no such field
        fragment = list(lakesoul_dataset("part").get_fragments())[0]
        scanner = fragment.scanner(columns=["not existed"])
        _ = scanner.to_table()

    with pytest.raises(ValueError) as _:
        # no such field
        fragment = list(lakesoul_dataset("part").get_fragments())[0]
        scanner = fragment.scanner(columns=["p_name", "not existed"])
        _ = scanner.to_table()


def test_duckdb_compatibility():
    import duckdb

    conn = duckdb.connect()
    _lds = lakesoul_dataset("part")
    results = conn.sql("select * from _lds").arrow().read_all()
    assert len(results) == 20000


def test_duckdb_compatibility_with_filter_and_projections():
    import duckdb

    conn = duckdb.connect()
    _lds = lakesoul_dataset("part")
    results = conn.execute("select * from _lds where p_size = 50").arrow().read_all()
    assert len(results) == 392

    results = conn.sql("select * from _lds where p_size = 50").arrow().read_all()
    assert len(results) == 392

    results = (
        conn.execute("select p_size,p_name from _lds where p_size = 50")
        .arrow()
        .read_all()
    )
    assert len(results) == 392
    expected = ["p_size", "p_name"]
    actual = [f.name for f in results.schema]
    assert actual == expected
    assert len(results.columns) == 2

    results = (
        conn.sql("select p_size, p_name from _lds where p_size = 50").arrow().read_all()
    )
    assert len(results) == 392
    expected = ["p_size", "p_name"]
    actual = [f.name for f in results.schema]
    assert actual == expected
    assert len(results.columns) == 2

    results = (
        conn.sql("select p_name,p_size from _lds where p_size = 50").arrow().read_all()
    )
    assert len(results) == 392
    expected = ["p_name", "p_size"]
    actual = [f.name for f in results.schema]
    assert actual == expected
    assert len(results.columns) == 2

    results = (
        conn.execute("select p_name,p_size from _lds where p_size = 50")
        .arrow()
        .read_all()
    )
    assert len(results) == 392
    expected = ["p_name", "p_size"]
    actual = [f.name for f in results.schema]
    assert actual == expected
    assert len(results.columns) == 2

    with pytest.raises(BinderException) as _:
        # no such field
        results = conn.execute("select not_existed from _lds").arrow().read_all()


def test_normal_lakesoul_table():
    lds = lakesoul_dataset("test_lfs", batch_size=1024)

    total_rows = 0
    for batch in lds.to_batches():
        total_rows += batch.num_rows

    assert total_rows == 1
