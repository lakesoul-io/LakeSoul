# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

from types import SimpleNamespace

import daft
import pyarrow as pa
import pytest
from daft.catalog import Identifier, NotFoundError
from daft.io.partitioning import PartitionField, PartitionTransform
from daft.logical.schema import Field

from lakesoul.daft import LakeSoulDataCatalog, LakeSoulDataTable


class FakeLakeSoulTable:
    def __init__(self, namespace: str, name: str) -> None:
        self.namespace = namespace
        self.name = name
        self.schema = pa.schema({"id": pa.int64()})
        self.scan_options = None
        self.write_args = None

    def scan(self, **options):
        self.scan_options = options
        return SimpleNamespace(to_daft=lambda: daft.from_pydict({"id": [1]}))

    def write_daft(self, dataframe, **options):
        self.write_args = (dataframe, options)


class FakeLakeSoulCatalog:
    namespace = "default"

    def __init__(self) -> None:
        self.tables = {
            ("default", "part"): FakeLakeSoulTable("default", "part"),
            ("analytics", "events"): FakeLakeSoulTable("analytics", "events"),
            ("company.czods", "orders"): FakeLakeSoulTable("company.czods", "orders"),
        }
        self.dropped = None
        self.created = None

    def list_namespaces(self):
        return ("default", "analytics", "company.czods")

    def list_tables(self, namespace):
        return tuple(
            name
            for table_namespace, name in self.tables
            if table_namespace == namespace
        )

    def table(self, name, namespace):
        try:
            return self.tables[(namespace, name)]
        except KeyError as error:
            raise RuntimeError("table not found") from error

    def drop_table(self, name, namespace):
        if (namespace, name) not in self.tables:
            raise RuntimeError("table not found")
        self.dropped = (namespace, name)

    def create_table(self, name, **options):
        self.created = (name, options)
        table = FakeLakeSoulTable(options["namespace"], name)
        table.schema = options["schema"]
        self.tables[(options["namespace"], name)] = table
        return table


def test_daft_catalog_lists_and_resolves_lakesoul_tables() -> None:
    inner = FakeLakeSoulCatalog()
    catalog = LakeSoulDataCatalog(inner, name="lake")

    assert catalog.name == "lake"
    assert catalog.list_namespaces() == [
        Identifier("default"),
        Identifier("analytics"),
        Identifier("company", "czods"),
    ]
    assert catalog.list_tables() == [
        Identifier("default", "part"),
        Identifier("analytics", "events"),
        Identifier("company", "czods", "orders"),
    ]
    assert catalog.has_table("part")
    assert catalog.has_table("analytics.events")
    assert catalog.has_table("company.czods.orders")
    assert catalog.has_namespace("company.czods")
    assert not catalog.has_table("default.missing")

    table = catalog.get_table("analytics.events")
    assert isinstance(table, LakeSoulDataTable)
    assert table.name == "events"
    assert table.schema().to_pyarrow_schema() == pa.schema({"id": pa.int64()})


def test_daft_table_delegates_read_and_append() -> None:
    inner = FakeLakeSoulCatalog()
    table = LakeSoulDataCatalog(inner).get_table("part")

    dataframe = table.read(batch_size=128, retain_partition_columns=True)
    assert dataframe.to_pydict() == {"id": [1]}
    assert table.lakesoul_table.scan_options == {
        "batch_size": 128,
        "retain_partition_columns": True,
    }

    table.append(dataframe, format="parquet", max_file_size=1024)
    assert table.lakesoul_table.write_args == (
        dataframe,
        {"format": "parquet", "max_file_size": 1024},
    )


def test_daft_catalog_translates_missing_tables_and_drop() -> None:
    inner = FakeLakeSoulCatalog()
    catalog = LakeSoulDataCatalog(inner)

    with pytest.raises(NotFoundError, match="missing"):
        catalog.get_table("default.missing")

    catalog.drop_table("analytics.events")
    assert inner.dropped == ("analytics", "events")


def test_daft_catalog_creates_table_with_lakesoul_options() -> None:
    inner = FakeLakeSoulCatalog()
    catalog = LakeSoulDataCatalog(inner)
    arrow_schema = pa.schema(
        {
            "region": pa.string(),
            "id": pa.int64(),
        }
    )
    schema = daft.Schema.from_pyarrow_schema(arrow_schema)
    expected_arrow_schema = schema.to_pyarrow_schema()
    region_field = Field.create("region", daft.DataType.string())
    partition_field = PartitionField.create(
        region_field,
        source_field=region_field,
        transform=PartitionTransform.identity(),
    )

    table = catalog.create_table(
        "analytics.output",
        schema,
        properties={
            "location": "s3://test-bucket/warehouse/analytics/output",
            "primary_keys": ["id"],
            "hash_bucket_num": 8,
            "domain": "public",
            "table_properties": {"compression": "zstd"},
        },
        partition_fields=[partition_field],
    )

    assert isinstance(table, LakeSoulDataTable)
    assert inner.created == (
        "output",
        {
            "namespace": "analytics",
            "path": "s3://test-bucket/warehouse/analytics/output",
            "schema": expected_arrow_schema,
            "partition_by": ("region",),
            "primary_keys": ("id",),
            "hash_bucket_num": 8,
            "properties": {"compression": "zstd"},
            "domain": "public",
        },
    )


def test_daft_catalog_create_table_uses_explicit_location() -> None:
    inner = FakeLakeSoulCatalog()
    catalog = LakeSoulDataCatalog(inner)
    schema = daft.Schema.from_pyarrow_schema(pa.schema({"id": pa.int64()}))

    catalog.create_table(
        "default.external",
        schema,
        properties={"location": "s3://external-bucket/table"},
    )

    assert inner.created is not None
    assert inner.created[1]["path"] == "s3://external-bucket/table"


@pytest.mark.parametrize(
    ("properties", "message"),
    [
        (
            {"location": "file:///tmp/output", "hash_buket_num": 8},
            "hash_buket_num",
        ),
        (
            {"location": "file:///tmp/output", "hash_bucket_num": 0},
            "positive integer",
        ),
        (
            {"location": "file:///tmp/output", "primary_keys": "id"},
            "sequence of strings",
        ),
        (
            {
                "location": "file:///tmp/output",
                "table_properties": {"version": 1},
            },
            "mapping of strings",
        ),
    ],
)
def test_daft_catalog_validates_create_table_properties(
    properties,
    message,
) -> None:
    catalog = LakeSoulDataCatalog(FakeLakeSoulCatalog())
    schema = daft.Schema.from_pyarrow_schema(pa.schema({"id": pa.int64()}))

    with pytest.raises((TypeError, ValueError), match=message):
        catalog.create_table("output", schema, properties=properties)


def test_daft_catalog_create_table_requires_location() -> None:
    catalog = LakeSoulDataCatalog(FakeLakeSoulCatalog())
    schema = daft.Schema.from_pyarrow_schema(pa.schema({"id": pa.int64()}))

    with pytest.raises(ValueError, match="properties.*location"):
        catalog.create_table("output", schema)


def test_daft_catalog_rejects_transformed_partition_fields() -> None:
    catalog = LakeSoulDataCatalog(FakeLakeSoulCatalog())
    schema = daft.Schema.from_pyarrow_schema(
        pa.schema({"created_at": pa.timestamp("us")})
    )
    partition_field = PartitionField.create(
        Field.create("created_day", daft.DataType.date()),
        source_field=Field.create(
            "created_at",
            daft.DataType.timestamp("us"),
        ),
        transform=PartitionTransform.day(),
    )

    with pytest.raises(ValueError, match="identity partition"):
        catalog.create_table(
            "output",
            schema,
            properties={"location": "file:///tmp/output"},
            partition_fields=[partition_field],
        )


def test_daft_catalog_rejects_patterns() -> None:
    catalog = LakeSoulDataCatalog(FakeLakeSoulCatalog())

    with pytest.raises(NotImplementedError, match="pattern filtering"):
        catalog.list_tables("analytics.%")


def test_daft_catalog_rejects_unsupported_mutations() -> None:
    catalog = LakeSoulDataCatalog(FakeLakeSoulCatalog())
    table = catalog.get_table("part")

    with pytest.raises(NotImplementedError, match="overwrite"):
        table.overwrite(daft.from_pydict({"id": [2]}))
    with pytest.raises(NotImplementedError, match="namespace creation"):
        catalog.create_namespace("new_namespace")
