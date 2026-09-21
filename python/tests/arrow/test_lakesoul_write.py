# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import json
import zlib
from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakesoul import BlobRef, LakeSoulCatalog
from lakesoul.metadata.generated.entity_pb2 import AppendCommit


def test_arrow_write_basic_append_reads_back_with_arrow(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("arrow_basic")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string()),
            pa.field("score", pa.float64()),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=schema,
    )

    try:
        first = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "label": pa.array(["alpha", "beta"], type=pa.string()),
                "score": pa.array([1.5, None], type=pa.float64()),
            },
            schema=schema,
        )
        second = pa.table(
            {
                "id": pa.array([3], type=pa.int64()),
                "label": pa.array(["gamma"], type=pa.string()),
                "score": pa.array([-7.25], type=pa.float64()),
            },
            schema=schema,
        )

        first_result = table.write_arrow(first)
        second_result = table.write_arrow(second)
        actual = catalog.scan(table_name).to_arrow_table()

        assert first_result.row_count == 2
        assert second_result.row_count == 1
        assert _rows(actual) == _rows(pa.concat_tables([first, second]))
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_arrow_write_partitioned_append_reads_partition_with_arrow(
    tmp_path: Path,
) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("arrow_part")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("part", pa.string(), nullable=False),
            pa.field("value", pa.int32()),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=schema,
        partition_by=("part",),
    )

    try:
        data = pa.table(
            {
                "id": pa.array([1, 2, 3, 4], type=pa.int64()),
                "part": pa.array(["north", "south", "north", "east"], type=pa.string()),
                "value": pa.array([10, 20, 30, 40], type=pa.int32()),
            },
            schema=schema,
        )

        result = table.write_arrow(data)
        actual = catalog.scan(
            table_name,
            partitions={"part": "north"},
            columns=("id", "part", "value"),
            retain_partition_columns=True,
        ).to_arrow_table()

        assert result.row_count == 4
        assert _rows(actual) == [
            {"id": 1, "part": "north", "value": 10},
            {"id": 3, "part": "north", "value": 30},
        ]
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_arrow_write_mixed_case_create_uses_datafusion_case_folding(
    tmp_path: Path,
) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("Arrow_Mixed")
    create_schema = pa.schema(
        [
            pa.field("ID", pa.int64(), nullable=True),
            pa.field("Label", pa.string()),
        ]
    )
    write_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string()),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=create_schema,
        primary_keys=("ID",),
    )

    try:
        data = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "label": pa.array(["alpha", "beta"], type=pa.string()),
            },
            schema=write_schema,
        )

        table.write_arrow(data)
        actual = catalog.scan(table_name).to_arrow_table()

        assert table.name == table_name.lower()
        assert table.schema == write_schema
        assert table.hash_bucket_num == 4
        assert _rows(actual) == _rows(data)
        assert _data_commit_ops(catalog, table) == [AppendCommit]
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_arrow_write_pk_upsert_reads_latest_rows_with_arrow(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("arrow_pk")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("value", pa.int32()),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=schema,
        primary_keys=("id",),
        hash_bucket_num=2,
    )

    try:
        first = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "name": pa.array(["alice", "bob", "carol"], type=pa.string()),
                "value": pa.array([10, 20, 30], type=pa.int32()),
            },
            schema=schema,
        )
        second = pa.table(
            {
                "id": pa.array([2, 4], type=pa.int64()),
                "name": pa.array(["bob-updated", "dave"], type=pa.string()),
                "value": pa.array([200, 40], type=pa.int32()),
            },
            schema=schema,
        )

        table.write_arrow(first)
        table.write_arrow(second)
        actual = catalog.scan(table_name).to_arrow_table()

        assert _rows(actual) == [
            {"id": 1, "name": "alice", "value": 10},
            {"id": 2, "name": "bob-updated", "value": 200},
            {"id": 3, "name": "carol", "value": 30},
            {"id": 4, "name": "dave", "value": 40},
        ]
        assert set(_data_commit_ops(catalog, table)) == {AppendCommit}
    finally:
        catalog.drop_table(table_name, if_exists=True)


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex}"


def _rows(table: pa.Table) -> list[dict[str, object]]:
    return sorted(table.to_pylist(), key=lambda row: tuple(row.values()))


def _data_commit_ops(catalog: LakeSoulCatalog, table) -> list[int]:
    ops = []
    for partition in catalog._client.get_all_partition_info(table.id):
        for commit in catalog._client.get_table_single_partition_data_info(partition):
            ops.append(commit.commit_op)
    return ops


def test_blob_columns_property_roundtrip(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("blob")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("frame", pa.binary()),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=schema,
        properties={"blob_columns": json.dumps({"frame": {"mode": "external"}})},
    )

    try:
        data = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "frame": pa.array([b"tiny", b"0123456789"], type=pa.binary()),
            },
            schema=schema,
        )
        result = table.write_arrow(data, format="parquet")

        assert table.blob_columns == ("frame",)
        raw_path = Path(unquote(urlparse(result.files[0].path).path))
        raw_values = pq.read_table(raw_path).column("frame").to_pylist()
        assert [value[0] for value in raw_values] == [1, 1]
        assert Path(f"{raw_path}.frame.blob").read_bytes() == b"tiny0123456789"

        actual = catalog.scan(table_name).to_arrow_table()
        assert actual.column("frame").to_pylist() == [b"tiny", b"0123456789"]
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_blob_scan_can_defer_reads(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("blob_ref")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("frame", pa.binary()),
            pa.field("tag", pa.binary()),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=schema,
        properties={
            "blob_columns": json.dumps(
                {"frame": {"mode": "external"}, "tag": {"mode": "inline"}}
            )
        },
    )

    frames = [b"external-payload-0", b"external-payload-111"]
    tags = [b"tiny", b"inline"]
    try:
        table.write_arrow(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "frame": pa.array(frames, type=pa.binary()),
                    "tag": pa.array(tags, type=pa.binary()),
                },
                schema=schema,
            ),
            format="parquet",
        )

        materialized = catalog.scan(table_name).to_arrow_table()
        assert materialized.column("frame").to_pylist() == frames

        deferred = (
            catalog.scan(table_name)
            .options(reader_options={"blob_materialize": "false"})
            .to_arrow_table()
        )
        refs = [BlobRef.parse(value) for value in deferred.column("frame").to_pylist()]
        assert all(not ref.is_inline for ref in refs)
        for raw, ref in zip(frames, refs):
            assert ref.size == len(raw)
            assert ref.crc32 == zlib.crc32(raw) & 0xFFFFFFFF
            assert ref.read() == raw
            assert ref.read(2, 5) == raw[2:7]
            assert ref.materialize() == raw

        inline = [BlobRef.parse(value) for value in deferred.column("tag").to_pylist()]
        assert all(ref.is_inline for ref in inline)
        assert [ref.read() for ref in inline] == tags
    finally:
        catalog.drop_table(table_name, if_exists=True)


def test_blob_ref_rejects_malformed_values() -> None:
    with pytest.raises(ValueError, match="empty"):
        BlobRef.parse(b"")
    with pytest.raises(ValueError, match="unknown blob tag"):
        BlobRef.parse(b"\x02x")
    with pytest.raises(ValueError, match="truncated"):
        BlobRef.parse(b"\x01short")


def test_create_table_rejects_invalid_blob_columns(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("blob_invalid")
    schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])

    with pytest.raises(ValueError, match="not in the schema"):
        catalog.create_table(
            table_name,
            path=(tmp_path / table_name).as_uri(),
            schema=schema,
            properties={"blob_columns": json.dumps({"missing": {}})},
        )
    with pytest.raises(ValueError, match="auto\\|inline\\|external"):
        catalog.create_table(
            table_name,
            path=(tmp_path / table_name).as_uri(),
            schema=schema,
            properties={"blob_columns": json.dumps({"id": {"mode": "sometimes"}})},
        )
