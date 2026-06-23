# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

from compat.cases import CASES
from compat.engines import engine_registry
from compat.normalize import assert_table_matches, table_summary
from compat.run_matrix import _plan_tasks


def test_pk_upsert_expected_table_keeps_latest_primary_key() -> None:
    table = CASES["pk_upsert"].expected_table

    assert sorted(table.to_pylist(), key=lambda row: row["id"]) == [
        {"id": 1, "name": "alice", "value": 10},
        {"id": 2, "name": "bob-updated", "value": 200},
        {"id": 3, "name": "carol", "value": 30},
        {"id": 4, "name": "dave", "value": 40},
    ]


def test_partitioned_case_expected_table_applies_filter_and_projection() -> None:
    table = CASES["partitioned_append"].expected_table

    assert table.schema.names == ["id", "part", "value"]
    assert table.to_pylist() == [
        {"id": 1, "part": "north", "value": 10},
        {"id": 3, "part": "north", "value": 30},
    ]


def test_summary_is_row_order_independent() -> None:
    table = CASES["basic_append"].expected_table
    shuffled = table.take([2, 0, 1])

    actual, expected = assert_table_matches(shuffled, table)

    assert actual == expected
    assert table_summary(table, table.schema)["row_count"] == 3


def test_smoke_plan_keeps_matrix_bounded() -> None:
    engines = engine_registry()
    write_tasks, read_tasks = _plan_tasks(
        "smoke",
        ["basic_append", "partitioned_append", "pk_upsert", "schema_types"],
        ["spark", "flink", "pyarrow", "datafusion"],
        ["spark", "flink", "pyspark", "pyarrow", "datafusion", "ray", "daft"],
        engines,
    )

    assert ("flink", "basic_append") in write_tasks
    assert ("datafusion", "pk_upsert") in write_tasks
    assert ("spark", "daft", "basic_append") in read_tasks
    assert ("pyarrow", "ray", "partitioned_append") in read_tasks
    assert ("flink", "daft", "schema_types") not in read_tasks
