# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import decimal
import operator
from collections import Counter
from datetime import date
from io import StringIO

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from lakesoul import LakeSoulCatalog


_CATALOG: LakeSoulCatalog | None = None


def _catalog() -> LakeSoulCatalog:
    global _CATALOG
    if _CATALOG is None:
        _CATALOG = LakeSoulCatalog.from_env()
    return _CATALOG


def test_daft_catalog_discovers_real_tpch_table() -> None:
    pytest.importorskip("daft")
    from lakesoul.daft import LakeSoulDataCatalog

    catalog = LakeSoulDataCatalog(_catalog(), name="lakesoul")

    assert "default" in {str(namespace) for namespace in catalog.list_namespaces()}
    assert "default.part" in {str(identifier) for identifier in catalog.list_tables()}

    qualified_table = catalog.get_table("default.part")
    default_namespace_table = catalog.get_table("part")

    assert qualified_table.name == "part"
    assert default_namespace_table.lakesoul_table.id == (
        qualified_table.lakesoul_table.id
    )
    assert qualified_table.schema().to_pyarrow_schema().names == [
        "p_partkey",
        "p_name",
        "p_mfgr",
        "p_brand",
        "p_type",
        "p_size",
        "p_container",
        "p_retailprice",
        "p_comment",
    ]


def test_daft_session_resolves_real_current_namespace() -> None:
    daft = pytest.importorskip("daft")
    from lakesoul.daft import LakeSoulDataCatalog

    session = daft.Session()
    session.attach(LakeSoulDataCatalog(_catalog(), name="lakesoul"))
    session.use("lakesoul.default")

    table = session.get_table("part")

    assert table.name == "part"
    assert table.lakesoul_table.namespace == "default"


def test_daft_session_reads_real_tpch_table_through_catalog() -> None:
    daft = pytest.importorskip("daft")
    from lakesoul.daft import LakeSoulDataCatalog

    session = daft.Session()
    session.attach(LakeSoulDataCatalog(_catalog(), name="lakesoul"))
    dataframe = session.read_table(
        "lakesoul.default.part",
        columns=("p_partkey", "p_size"),
    )
    filtered = dataframe.where(dataframe["p_partkey"] <= 10)

    plan_output = StringIO()
    filtered.explain(show_all=True, file=plan_output)
    assert "Filter pushdown =" in plan_output.getvalue()

    actual = _daft_to_table(
        filtered,
        pa.schema({"p_partkey": pa.int64(), "p_size": pa.int32()}),
    )
    expected = (
        _catalog()
        .scan(
            "part",
            columns=("p_partkey", "p_size"),
            filter=pc.field("p_partkey") <= _typed_scalar(10, pa.int64()),
        )
        .to_arrow_table()
    )

    assert actual.num_rows == 10
    assert _row_counter(actual) == _row_counter(expected)


def test_daft_simple_filter_matches_arrow_reader() -> None:
    pytest.importorskip("daft")
    filter = pc.field("p_size") == 50
    scan = _catalog().scan(
        "part",
        columns=("p_name", "p_size"),
        filter=filter,
    )

    table = _daft_to_table(scan.to_daft(), scan.schema)

    assert table.num_rows == 392
    assert table.schema.names == ["p_name", "p_size"]
    assert table.column("p_size").combine_chunks().to_pylist() == [50] * 392


def test_daft_compound_decimal_filter_matches_arrow_reader() -> None:
    pytest.importorskip("daft")
    threshold = pa.array(
        [decimal.Decimal("1500.00")],
        type=pa.decimal128(15, 2),
    )[0]
    filter = (pc.field("p_retailprice") >= threshold) & (pc.field("p_size") == 50)
    scan = _catalog().scan(
        "part",
        columns=("p_size", "p_retailprice"),
        filter=filter,
    )

    table = _daft_to_table(scan.to_daft(), scan.schema)

    assert table.num_rows == 176
    assert table.schema.names == ["p_size", "p_retailprice"]
    assert table.column("p_size").combine_chunks().to_pylist() == [50] * 176


@pytest.mark.parametrize(
    ("comparison", "literal"),
    [
        (operator.eq, 100),
        (operator.ne, 100),
        (operator.lt, 100),
        (operator.le, 100),
        (operator.gt, 100),
        (operator.ge, 100),
    ],
    ids=["eq", "ne", "lt", "le", "gt", "ge"],
)
def test_daft_int64_comparison_operators_are_pushed_down(
    comparison,
    literal: int,
) -> None:
    """Cover the six basic comparison operators on a common Int64 column."""
    pytest.importorskip("daft")
    dataframe = _catalog().scan("part").to_daft()
    _assert_filter_pushdown_matches_arrow(
        dataframe,
        comparison(dataframe["p_partkey"], literal),
        comparison(
            pc.field("p_partkey"),
            _typed_scalar(literal, pa.int64()),
        ),
        required_columns=("p_partkey",),
    )


@pytest.mark.parametrize(
    "comparison",
    [operator.eq, operator.ne],
    ids=["eq", "ne"],
)
def test_daft_string_comparison_is_pushed_down(comparison) -> None:
    """Cover equality and inequality on a common Utf8 column."""
    pytest.importorskip("daft")
    dataframe = _catalog().scan("part").to_daft()
    _assert_filter_pushdown_matches_arrow(
        dataframe,
        comparison(dataframe["p_brand"], "Brand#11"),
        comparison(
            pc.field("p_brand"),
            _typed_scalar("Brand#11", pa.string()),
        ),
        required_columns=("p_brand",),
    )


def test_daft_decimal_int_literal_is_cast_and_pushed_down() -> None:
    """A normal Python int must match the Decimal128 column precision/scale."""
    pytest.importorskip("daft")
    dataframe = _catalog().scan("part").to_daft()
    _assert_filter_pushdown_matches_arrow(
        dataframe,
        dataframe["p_retailprice"] >= 1500,
        pc.field("p_retailprice") >= _decimal_scalar("1500.00"),
        required_columns=("p_retailprice",),
    )


@pytest.mark.parametrize("predicate_kind", ["between", "is-in"])
def test_daft_int64_set_predicates_are_pushed_down(
    predicate_kind: str,
) -> None:
    """Cover non-Decimal BETWEEN and IN through Daft's standard visitor."""
    pytest.importorskip("daft")
    dataframe = _catalog().scan("part").to_daft()
    field = pc.field("p_partkey")
    if predicate_kind == "between":
        predicate = dataframe["p_partkey"].between(100, 500)
        arrow_filter = (field >= _typed_scalar(100, pa.int64())) & (
            field <= _typed_scalar(500, pa.int64())
        )
    else:
        predicate = dataframe["p_partkey"].is_in([100, 200, 300])
        arrow_filter = field.isin(pa.array([100, 200, 300], type=pa.int64()))
    _assert_filter_pushdown_matches_arrow(
        dataframe,
        predicate,
        arrow_filter,
        required_columns=("p_partkey",),
    )


def test_daft_string_is_in_is_pushed_down() -> None:
    """Cover a non-numeric IN list on a TPCH Utf8 column."""
    pytest.importorskip("daft")
    dataframe = _catalog().scan("part").to_daft()
    values = ["Brand#11", "Brand#22", "Brand#33"]
    _assert_filter_pushdown_matches_arrow(
        dataframe,
        dataframe["p_brand"].is_in(values),
        pc.field("p_brand").isin(pa.array(values, type=pa.string())),
        required_columns=("p_brand",),
    )


def test_daft_nested_and_or_not_filter_is_pushed_down() -> None:
    """Cover nested AND/OR/NOT with Int64, Int32 and Utf8 leaves."""
    pytest.importorskip("daft")
    dataframe = _catalog().scan("part").to_daft()
    predicate = (
        dataframe["p_partkey"].between(100, 500) & (dataframe["p_size"] >= 10)
    ) | (
        dataframe["p_brand"].is_in(["Brand#11", "Brand#22"])
        & ~(dataframe["p_size"] == 50)
    )
    arrow_filter = (
        (pc.field("p_partkey") >= _typed_scalar(100, pa.int64()))
        & (pc.field("p_partkey") <= _typed_scalar(500, pa.int64()))
        & (pc.field("p_size") >= _typed_scalar(10, pa.int32()))
    ) | (
        pc.field("p_brand").isin(pa.array(["Brand#11", "Brand#22"], type=pa.string()))
        & ~(pc.field("p_size") == _typed_scalar(50, pa.int32()))
    )
    _assert_filter_pushdown_matches_arrow(
        dataframe,
        predicate,
        arrow_filter,
        required_columns=("p_partkey", "p_size", "p_brand"),
    )


@pytest.mark.parametrize("is_null", [True, False], ids=["is-null", "is-not-null"])
def test_daft_null_predicates_are_pushed_down(is_null: bool) -> None:
    """Cover IS NULL and IS NOT NULL even when TPCH contains no null values."""
    pytest.importorskip("daft")
    dataframe = _catalog().scan("part").to_daft()
    if is_null:
        predicate = dataframe["p_comment"].is_null()
        arrow_filter = pc.field("p_comment").is_null()
    else:
        predicate = dataframe["p_comment"].not_null()
        arrow_filter = pc.field("p_comment").is_valid()
    _assert_filter_pushdown_matches_arrow(
        dataframe,
        predicate,
        arrow_filter,
        required_columns=("p_comment",),
    )


def test_daft_date32_range_filter_is_pushed_down() -> None:
    """Cover common Date32 >= and < range comparisons."""
    pytest.importorskip("daft")
    dataframe = _catalog().scan("orders").to_daft()
    predicate = (dataframe["o_orderdate"] >= date(1995, 1, 1)) & (
        dataframe["o_orderdate"] < date(1996, 1, 1)
    )
    arrow_filter = (
        pc.field("o_orderdate") >= _typed_scalar(date(1995, 1, 1), pa.date32())
    ) & (pc.field("o_orderdate") < _typed_scalar(date(1996, 1, 1), pa.date32()))
    _assert_filter_pushdown_matches_arrow(
        dataframe,
        predicate,
        arrow_filter,
        table_name="orders",
        required_columns=("o_orderdate",),
    )


def test_daft_python_udf_filter_falls_back_to_daft() -> None:
    """A valid Daft-only predicate must not make the LakeSoul scan fail."""
    daft = pytest.importorskip("daft")

    @daft.udf(return_dtype=daft.DataType.bool())
    def is_even(values):
        return [value % 2 == 0 for value in values]

    scan = _catalog().scan("part", columns=("p_partkey",))
    dataframe = scan.to_daft()
    actual = _daft_to_table(
        dataframe.where(is_even(dataframe["p_partkey"])),
        scan.schema,
    )
    expected_values = [
        value
        for value in scan.to_arrow_table().column("p_partkey").to_pylist()
        if value % 2 == 0
    ]

    assert Counter(actual.column("p_partkey").to_pylist()) == Counter(expected_values)


def test_daft_shard_matches_lakesoul_arrow_shard() -> None:
    """Daft must apply LakeSoul's scan-partition sharding before task creation."""
    pytest.importorskip("daft")
    scan = _catalog().scan("part", columns=("p_partkey",)).shard(1, 2)

    actual = _daft_to_table(scan.to_daft(), scan.schema)
    expected = scan.to_arrow_table()

    assert Counter(actual.column("p_partkey").to_pylist()) == Counter(
        expected.column("p_partkey").to_pylist()
    )


def test_daft_partition_filter_prunes_test_lfs_and_matches_metadata_scan() -> None:
    """A predicate on test_lfs.c2 must become a Daft Partition Filter."""
    pytest.importorskip("daft")
    catalog = _catalog()
    dataframe = catalog.scan(
        "test_lfs",
        retain_partition_columns=True,
    ).to_daft()
    filtered = dataframe.where(dataframe["c2"] == 1)

    plan_output = StringIO()
    filtered.explain(show_all=True, file=plan_output)
    plan = plan_output.getvalue()
    assert "Partition Filter =" in plan
    assert "c2" in plan

    expected_scan = catalog.scan(
        "test_lfs",
        partitions={"c2": "1"},
        retain_partition_columns=True,
    )
    actual = _daft_to_table(filtered, expected_scan.schema)
    expected = expected_scan.to_arrow_table()

    assert actual.num_rows == 2
    assert actual.schema.names == expected.schema.names
    assert _row_counter(actual) == _row_counter(expected)


def _daft_to_table(dataframe, schema: pa.Schema) -> pa.Table:
    if hasattr(dataframe, "collect"):
        dataframe = dataframe.collect()
    if hasattr(dataframe, "to_arrow"):
        return dataframe.to_arrow()
    if hasattr(dataframe, "to_arrow_iter"):
        tables = list(dataframe.to_arrow_iter())
        if not tables:
            return pa.Table.from_batches([], schema=schema)
        return pa.concat_tables(tables, promote_options="default")
    raise RuntimeError("Daft DataFrame does not expose an Arrow export API")


def _decimal_scalar(value: str) -> pc.Expression:
    return pc.scalar(pa.scalar(decimal.Decimal(value), type=pa.decimal128(15, 2)))


def _typed_scalar(value: object, arrow_type: pa.DataType) -> pc.Expression:
    return pc.scalar(pa.scalar(value, type=arrow_type))


def _assert_filter_pushdown_matches_arrow(
    dataframe,
    predicate,
    arrow_filter: pc.Expression,
    *,
    table_name: str = "part",
    required_columns: tuple[str, ...] = (),
) -> None:
    """Assert both optimizer pushdown and complete result correctness."""
    filtered = dataframe.where(predicate)
    plan_output = StringIO()
    filtered.explain(show_all=True, file=plan_output)
    plan = plan_output.getvalue()

    assert "Filter pushdown =" in plan
    for column in required_columns:
        assert column in plan

    expected_scan = _catalog().scan(table_name, filter=arrow_filter)
    actual = _daft_to_table(filtered, expected_scan.schema)
    expected = expected_scan.to_arrow_table()

    assert actual.schema.names == expected.schema.names
    assert _row_counter(actual) == _row_counter(expected)


def _row_counter(table: pa.Table) -> Counter[tuple[object, ...]]:
    """Compare complete rows without relying on scan task output order."""
    names = table.schema.names
    return Counter(tuple(row[name] for name in names) for row in table.to_pylist())
