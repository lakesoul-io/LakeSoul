# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import json
import os
import struct
import zlib
from pathlib import Path
from urllib.parse import unquote, urlparse
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lakesoul import BlobRef, LakeSoulCatalog


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
        assert _rows(actual) == _rows(data)
    finally:
        catalog.drop_table(table_name, if_exists=True)


@pytest.mark.parametrize("format", ["parquet", "vortex", "vortex-compact"])
def test_arrow_write_uses_stored_not_source_nested_constraints(
    tmp_path: Path, format: str
) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("nested_source_flags")
    stored = pa.schema(
        [
            pa.field("id", pa.int32(), nullable=False),
            pa.field("s", pa.struct([pa.field("x", pa.int32())])),
        ]
    )
    source_struct = pa.struct([pa.field("x", pa.int32(), nullable=False)])
    data = pa.table(
        {
            "id": pa.array([1, 2], pa.int32()),
            "s": pa.StructArray.from_arrays(
                [pa.array([None, 7], pa.int32())],
                fields=list(source_struct),
                mask=pa.array([True, False]),
            ),
        }
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=stored,
        primary_keys=("id",),
    )
    try:
        table.write_arrow(data, format=format)
        assert _rows(table.scan().to_arrow_table()) == [
            {"id": 1, "s": None},
            {"id": 2, "s": {"x": 7}},
        ]
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
    finally:
        catalog.drop_table(table_name, if_exists=True)


@pytest.mark.parametrize("format", ["parquet", "vortex", "vortex-compact"])
def test_arrow_sparse_upsert_preserves_absence_and_checks_stored_constraints(
    tmp_path: Path, format: str
) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("arrow_sparse")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("part", pa.string()),
            pa.field("required", pa.int64(), nullable=False),
            pa.field("label", pa.string()),
            pa.field("value", pa.int64()),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=schema,
        primary_keys=("id",),
        partition_by=("part",),
        hash_bucket_num=2,
    )
    try:
        # Source fields are nullable; actual supplied values satisfy NOT NULL.
        table.write_arrow(
            pa.table(
                {
                    "id": [1, 2],
                    "part": ["north", "north"],
                    "required": [1, 2],
                    "label": ["keep", "other"],
                    "value": [10, 20],
                }
            ),
            format=format,
        )
        partial = pa.table(
            {
                "id": [1, 3],
                "part": ["north", "north"],
                "required": [11, 3],
                "value": [100, 30],
            }
        )
        result = table.write_arrow(partial.to_reader(), format=format)
        assert all("label" not in file.existing_columns for file in result.files)
        if format == "parquet":
            assert all(
                "label" not in pq.read_schema(file.path).names for file in result.files
            )
        assert _rows(table.scan().to_arrow_table()) == [
            {"id": 1, "part": "north", "required": 11, "label": "keep", "value": 100},
            {"id": 2, "part": "north", "required": 2, "label": "other", "value": 20},
            {"id": 3, "part": "north", "required": 3, "label": None, "value": 30},
        ]

        explicit_null = pa.record_batch(
            {
                "id": pa.array([1], type=pa.int64()),
                "part": ["north"],
                "required": pa.array([12], type=pa.int64()),
                "label": pa.array([None], type=pa.string()),
            }
        )
        table.write_arrow(explicit_null, format=format)
        expected = [
            {"id": 1, "part": "north", "required": 12, "label": None, "value": 100},
            {"id": 2, "part": "north", "required": 2, "label": "other", "value": 20},
            {"id": 3, "part": "north", "required": 3, "label": None, "value": 30},
        ]
        assert _rows(table.scan().to_arrow_table()) == expected
        commits = _data_commit_ops(catalog, table)

        invalid = [
            pa.table({"part": ["north"], "required": [9]}),
            pa.table({"id": [1], "required": [9]}),
            pa.table({"id": [1], "part": ["north"]}),
            pa.table(
                {
                    "id": [1],
                    "part": ["north"],
                    "required": pa.array([None], type=pa.int64()),
                }
            ),
            # A required-field Arrow schema is not proof of non-null values.
            pa.Table.from_arrays(
                [
                    pa.array([1], type=pa.int64()),
                    pa.array(["north"]),
                    pa.array([None], type=pa.int64()),
                ],
                schema=pa.schema(
                    [schema.field(name) for name in ("id", "part", "required")]
                ),
            ),
        ]
        for data in invalid:
            with pytest.raises(ValueError, match="missing required|non-nullable"):
                table.write_arrow(data, format=format)
            assert _data_commit_ops(catalog, table) == commits
        assert _rows(table.scan().to_arrow_table()) == expected
    finally:
        catalog.drop_table(table_name, if_exists=True)


@pytest.mark.parametrize("format", ["parquet", "vortex", "vortex-compact"])
def test_arrow_write_checks_stored_nested_struct_constraints(
    tmp_path: Path, format: str
) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("arrow_nested_required")
    stored_payload = pa.struct([pa.field("count", pa.int64(), nullable=False)])
    input_payload = pa.struct([pa.field("count", pa.int64())])
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("payload", stored_payload),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=(tmp_path / table_name).as_uri(),
        schema=schema,
        primary_keys=("id",),
    )
    try:
        valid = pa.table(
            {"id": [1], "payload": pa.array([{"count": 7}], type=input_payload)}
        )
        table.write_arrow(valid, format=format)
        expected = [{"id": 1, "payload": {"count": 7}}]
        assert table.scan().to_arrow_table().to_pylist() == expected
        commits = _data_commit_ops(catalog, table)

        invalid = [
            pa.table({"id": [1]}),
            pa.table({"id": [1], "payload": pa.array([None], type=input_payload)}),
            pa.table(
                {"id": [1], "payload": pa.array([{"count": None}], type=input_payload)}
            ),
        ]
        for data in invalid:
            with pytest.raises(ValueError, match="missing required|non-nullable"):
                table.write_arrow(data, format=format)
            assert _data_commit_ops(catalog, table) == commits

        # Failure after an earlier accepted batch still must not publish files.
        reader = pa.RecordBatchReader.from_batches(
            valid.schema, [valid.to_batches()[0], invalid[-1].to_batches()[0]]
        )
        with pytest.raises(ValueError, match="non-nullable"):
            table.write_arrow(reader, format=format)
        assert _data_commit_ops(catalog, table) == commits
        assert table.scan().to_arrow_table().to_pylist() == expected
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
        sidecar = Path(f"{raw_path}.blobref")
        packs = json.loads(sidecar.read_text())["packs"]
        assert len(packs) == 1
        pack_path = Path(unquote(urlparse(packs[0]).path))
        assert pack_path.read_bytes() == b"tiny0123456789"

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


def test_blob_ref_reads_with_injected_filesystem(tmp_path: Path) -> None:
    payload = b"payload-bytes-0123456789"
    pack = tmp_path / "data.blob"
    pack.write_bytes(b"junk" + payload)

    def external(offset: int, length: int, crc32: int) -> bytes:
        header = struct.pack("<IIQ", crc32, length, offset)
        return b"\x01" + header + pack.as_uri().encode()

    filesystem = pa.fs.LocalFileSystem()
    ref = BlobRef.parse(external(4, len(payload), zlib.crc32(payload) & 0xFFFFFFFF))
    assert ref.read(filesystem=filesystem) == payload
    assert ref.read(2, 5, filesystem=filesystem) == payload[2:7]
    assert ref.materialize(filesystem=filesystem) == payload

    broken = BlobRef.parse(external(4, len(payload), 123))
    with pytest.raises(ValueError, match="checksum"):
        broken.read(filesystem=filesystem)


def test_blob_refs_on_object_storage() -> None:
    if os.environ.get("LAKESOUL_S3_TEST") != "1":
        pytest.skip("set LAKESOUL_S3_TEST=1 to enable S3 tests")

    from pyarrow.fs import S3FileSystem

    options = {
        "fs.s3a.access.key": "rustfsadmin",
        "fs.s3a.secret.key": "rustfsadmin",
        "fs.s3a.endpoint": "http://localhost:9000",
        "fs.s3a.path.style.access": "true",
    }
    catalog = LakeSoulCatalog.from_env(object_store_options=options)
    table_name = _table_name("blob_s3")
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("frame", pa.binary()),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=f"s3://lakesoul-test-bucket/{table_name}",
        schema=schema,
        properties={"blob_columns": json.dumps({"frame": {"mode": "external"}})},
    )
    filesystem = S3FileSystem(
        access_key=options["fs.s3a.access.key"],
        secret_key=options["fs.s3a.secret.key"],
        endpoint_override=options["fs.s3a.endpoint"],
        scheme="http",
    )
    try:
        payloads = [b"first-payload", b"second-payload-longer"]
        table.write_arrow(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "frame": pa.array(payloads, type=pa.binary()),
                },
                schema=schema,
            ),
            format="parquet",
        )

        scan = table.scan().options(reader_options={"blob_materialize": "false"})
        values = scan.to_arrow_table().column("frame").to_pylist()
        refs = [BlobRef.parse(value) for value in values]
        assert all(not ref.is_inline for ref in refs)
        assert [ref.read(filesystem=filesystem) for ref in refs] == payloads
        assert refs[1].read(1, 6, filesystem=filesystem) == payloads[1][1:7]
    finally:
        catalog.drop_table(table_name, if_exists=True)
        filesystem.delete_dir(f"lakesoul-test-bucket/{table_name}")


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


def test_blob_pack_target_rolls_packs(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    table_name = _table_name("blob_pack_target")
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
        properties={
            "blob_columns": json.dumps(
                {"frame": {"mode": "external", "pack_target_bytes": 4096}}
            )
        },
    )
    try:
        payloads = [bytes([index]) * 2000 for index in range(6)]
        for start in (0, 3):
            batch = pa.table(
                {
                    "id": pa.array(range(start, start + 3), type=pa.int64()),
                    "frame": pa.array(payloads[start : start + 3], type=pa.binary()),
                },
                schema=schema,
            )
            table.write_arrow(batch, format="parquet")

        scanned = catalog.scan(table_name).to_arrow_table()
        values = dict(
            zip(
                scanned.column("id").to_pylist(),
                scanned.column("frame").to_pylist(),
            )
        )
        assert values == {index: payloads[index] for index in range(6)}

        packs = sorted((tmp_path / table_name).rglob("_blob/frame/*.blob"))
        assert len(packs) >= 2, "a small pack_target_bytes must roll packs"
        assert max(pack.stat().st_size for pack in packs) <= 4096 + 2000

        listed: set[str] = set()
        for sidecar in (tmp_path / table_name).rglob("*.blobref"):
            listed.update(json.loads(sidecar.read_text())["packs"])
        assert len(listed) == len(packs)
    finally:
        catalog.drop_table(table_name, if_exists=True)
