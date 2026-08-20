# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import json
from pathlib import Path

import pytest

from compat.cases import CASES
from compat.engines import engine_registry
from compat.normalize import assert_table_matches, table_summary
from compat.run_matrix import _plan_tasks, main


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


def test_format_cases_cover_every_supported_physical_format() -> None:
    assert {
        CASES[name].physical_format
        for name in (
            "format_parquet",
            "format_vortex",
            "format_vortex_compact",
        )
    } == {"parquet", "vortex", "vortex-compact"}


def test_recovery_rejects_manifest_without_successful_writes(
    tmp_path: Path,
) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "run_id": "empty",
                "storage": "file:///tmp/empty",
                "cases": [],
                "records": [],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(
        SystemExit,
        match="recovery manifest contains no successful table writes",
    ):
        main(
            [
                "--read-manifest",
                str(manifest),
                "--writers",
                "",
                "--readers",
                "",
                "--output-dir",
                str(tmp_path / "output"),
            ]
        )


def test_smoke_plan_keeps_matrix_bounded() -> None:
    engines = engine_registry()
    write_tasks, read_tasks = _plan_tasks(
        "smoke",
        ["basic_append", "partitioned_append", "pk_upsert", "schema_types"],
        ["spark", "flink", "pyarrow", "datafusion", "daft"],
        ["spark", "flink", "pyspark", "pyarrow", "datafusion", "ray", "daft"],
        engines,
    )

    assert ("flink", "basic_append") in write_tasks
    assert ("datafusion", "pk_upsert") in write_tasks
    assert ("daft", "basic_append") in write_tasks
    assert ("daft", "pk_upsert") in write_tasks
    assert ("spark", "daft", "basic_append") in read_tasks
    assert ("daft", "pyarrow", "pk_upsert") in read_tasks
    assert ("daft", "daft", "basic_append") in read_tasks
    assert ("pyarrow", "ray", "partitioned_append") in read_tasks
    assert ("flink", "daft", "schema_types") not in read_tasks
