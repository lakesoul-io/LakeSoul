# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

import pyarrow as pa
import pytest

from lakesoul import metadata


def test_create_table_forwards_metadata_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[object, ...]] = []

    def fake_create_table(*args: object) -> None:
        calls.append(args)

    monkeypatch.setattr(metadata, "_create_table", fake_create_table)

    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("part", pa.string()),
            pa.field("score", pa.float32()),
        ]
    )

    metadata.create_table(
        "events",
        namespace="analytics",
        table_path="file:///tmp/events",
        table_schema=schema,
        properties={"hashBucketNum": "4"},
        partitions={"range": ["part"], "hash": ["id"]},
        domain="public",
    )

    assert len(calls) == 1
    (
        table_name,
        namespace,
        table_path,
        table_schema_json,
        properties_json,
        partitions,
        domain,
    ) = calls[0]

    assert table_name == "events"
    assert namespace == "analytics"
    assert table_path == "file:///tmp/events"
    assert domain == "public"
    assert table_schema_json is schema
    assert properties_json == '{"hashBucketNum":"4"}'
    assert partitions == "part;id"


def test_create_table_rejects_unknown_partition_columns() -> None:
    schema = pa.schema([pa.field("id", pa.int64())])

    with pytest.raises(ValueError, match="partition column not in schema"):
        metadata.create_table(
            "events",
            table_path="file:///tmp/events",
            table_schema=schema,
            partitions={"range": ["missing"]},
        )


def test_create_table_ignores_unknown_partition_groups() -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    calls: list[tuple[object, ...]] = []

    def fake_create_table(*args: object) -> None:
        calls.append(args)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(metadata, "_create_table", fake_create_table)
    try:
        metadata.create_table(
            "events",
            table_path="file:///tmp/events",
            table_schema=schema,
            partitions={"bucket": ["id"]},
        )
    finally:
        monkeypatch.undo()

    assert len(calls) == 1
    assert calls[0][5] == ";"
