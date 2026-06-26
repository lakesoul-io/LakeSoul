# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import decimal

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
