# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

import pyarrow as pa
import pytest

from lakesoul.metadata import NativeMetadataClient


class FakeInner:
    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []

    def create_table(self, *args: object) -> None:
        self.calls.append(args)


def test_create_table_forwards_metadata_arguments() -> None:
    inner = FakeInner()
    client = NativeMetadataClient._from_inner(inner)  # type: ignore[arg-type]
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("part", pa.string()),
            pa.field("score", pa.float32()),
        ]
    )

    client.create_table(
        "events",
        namespace="analytics",
        table_path="file:///tmp/events",
        table_schema=schema,
        properties={"hashBucketNum": "4"},
        partitions={"range": ["part"], "hash": ["id"]},
        domain="public",
    )

    assert len(inner.calls) == 1
    (
        table_name,
        namespace,
        table_path,
        table_schema,
        properties_json,
        partitions,
        domain,
    ) = inner.calls[0]

    assert table_name == "events"
    assert namespace == "analytics"
    assert table_path == "file:///tmp/events"
    assert domain == "public"
    assert table_schema is schema
    assert properties_json == '{"hashBucketNum":"4"}'
    assert partitions == "part;id"


def test_create_table_rejects_unknown_partition_columns() -> None:
    inner = FakeInner()
    client = NativeMetadataClient._from_inner(inner)  # type: ignore[arg-type]
    schema = pa.schema([pa.field("id", pa.int64())])

    with pytest.raises(ValueError, match="partition column not in schema"):
        client.create_table(
            "events",
            table_path="file:///tmp/events",
            table_schema=schema,
            partitions={"range": ["missing"]},
        )


def test_create_table_ignores_unknown_partition_groups() -> None:
    inner = FakeInner()
    client = NativeMetadataClient._from_inner(inner)  # type: ignore[arg-type]
    schema = pa.schema([pa.field("id", pa.int64())])

    client.create_table(
        "events",
        table_path="file:///tmp/events",
        table_schema=schema,
        partitions={"bucket": ["id"]},
    )

    assert len(inner.calls) == 1
    assert inner.calls[0][5] == ";"
