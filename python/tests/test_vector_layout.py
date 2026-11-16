# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

import json
from types import SimpleNamespace

import pytest

from lakesoul.catalog import (
    LakeSoulTable,
    _apply_vector_layout_defaults,
    _has_vector_columns,
    _validate_vector_layout,
)


def test_has_vector_columns_reads_json_array_and_object() -> None:
    assert _has_vector_columns(
        {"vector_index_columns": json.dumps([{"column": "vec", "dim": 8}])}
    )
    assert _has_vector_columns(
        {"vector_index_columns": json.dumps({"column": "vec", "dim": 8})}
    )
    assert not _has_vector_columns({"vector_index_columns": "[]"})
    assert not _has_vector_columns({})
    assert not _has_vector_columns({"vector_index_columns": "not-json"})


def test_validate_vector_layout_accepts_positive_integers() -> None:
    _validate_vector_layout(
        {
            "vortex_row_block_size": "1024",
            "vortex_data_block_target_bytes": 131072,
        }
    )


def test_apply_vector_layout_defaults_only_for_vector_tables() -> None:
    vector_table: dict[str, object] = {
        "vector_index_columns": json.dumps([{"column": "vec", "dim": 8}])
    }
    _apply_vector_layout_defaults(vector_table)
    assert vector_table["vortex_row_block_size"] == "1024"
    assert vector_table["vortex_data_block_target_bytes"] == "131072"

    explicit: dict[str, object] = {
        "vector_index_columns": json.dumps([{"column": "vec", "dim": 8}]),
        "vortex_row_block_size": "4096",
    }
    _apply_vector_layout_defaults(explicit)
    assert explicit["vortex_row_block_size"] == "4096"
    assert explicit["vortex_data_block_target_bytes"] == "131072"

    plain: dict[str, object] = {}
    _apply_vector_layout_defaults(plain)
    assert plain == {}


@pytest.mark.parametrize(
    ("properties", "message"),
    [
        ({"vortex_row_block_size": "0"}, "vortex_row_block_size"),
        ({"vortex_row_block_size": "-1"}, "vortex_row_block_size"),
        ({"vortex_row_block_size": "many"}, "vortex_row_block_size"),
        (
            {"vortex_data_block_target_bytes": 0},
            "vortex_data_block_target_bytes",
        ),
    ],
)
def test_validate_vector_layout_rejects_invalid_values(
    properties: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _validate_vector_layout(properties)


def test_vector_layout_properties_become_writer_options() -> None:
    table = SimpleNamespace(
        properties={
            "vortex_row_block_size": "4096",
            "vortex_data_block_target_bytes": "262144",
            "hashBucketNum": "4",
        }
    )

    assert LakeSoulTable._vector_layout_write_options(table, None) == {
        "vortex_vector_row_block_size": "4096",
        "vortex_vector_data_block_target_bytes": "262144",
    }


def test_per_write_options_override_vector_layout_properties() -> None:
    table = SimpleNamespace(properties={"vortex_row_block_size": "1024"})

    assert LakeSoulTable._vector_layout_write_options(
        table, {"vortex_vector_row_block_size": "2048"}
    ) == {"vortex_vector_row_block_size": "2048"}
