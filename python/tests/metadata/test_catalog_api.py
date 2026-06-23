# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from types import SimpleNamespace

import pyarrow as pa

import lakesoul.catalog as catalog_module
from lakesoul.catalog import LakeSoulCatalog, LakeSoulTable, PostgresMetadataConfig
from lakesoul.metadata import LakeSoulScanPlanPartition, TableInfo


class DummyNativeClient:
    instances: list["DummyNativeClient"] = []

    def __init__(
        self,
        config: str,
        secondary_config: str | None = None,
        max_retry: int = 3,
    ) -> None:
        self.config = config
        self.secondary_config = secondary_config
        self.max_retry = max_retry
        self.created: list[dict[str, object]] = []
        self.dropped: list[tuple[str, str]] = []
        self.commits: list[tuple[str, str, list[tuple[str, str, int, list[str]]]]] = []
        self.scan_partitions = (
            LakeSoulScanPlanPartition(["file:///tmp/events/part-0.parquet"], ["id"]),
        )
        self.schema = pa.schema(
            [pa.field("id", pa.int64()), pa.field("dt", pa.string())]
        )
        DummyNativeClient.instances.append(self)

    @classmethod
    def from_env(cls) -> "DummyNativeClient":
        return cls("from-env")

    def list_namespaces(self) -> tuple[str, ...]:
        return ("default", "analytics")

    def list_tables(self, namespace: str) -> tuple[str, ...]:
        return ("events",) if namespace == "analytics" else ()

    def get_table_info_by_name(
        self,
        table_name: str,
        namespace: str,
    ) -> TableInfo:
        return _table_info(table_name=table_name, table_namespace=namespace)

    def create_table(self, table_name: str, **kwargs: object) -> None:
        self.created.append({"table_name": table_name, **kwargs})

    def drop_table(self, table_name: str, namespace: str) -> None:
        self.dropped.append((table_name, namespace))

    def commit_data_files(
        self,
        table_name: str,
        namespace: str,
        files: list[tuple[str, str, int, list[str]]],
    ) -> None:
        self.commits.append((table_name, namespace, files))

    def get_partition_and_pk_cols(
        self, table_info: TableInfo
    ) -> tuple[list[str], list[str]]:
        parts = table_info.partitions.split(";")
        return (
            parts[0].split(",") if parts[0] else [],
            parts[1].split(",") if parts[1] else [],
        )

    def get_scan_plan_partitions(
        self,
        table_name: str,
        partitions: dict[str, str] | None = None,
        namespace: str = "default",
    ) -> tuple[LakeSoulScanPlanPartition, ...]:
        self.last_scan_request = (table_name, partitions or {}, namespace)
        return self.scan_partitions

    def get_schemas_by_table_name(
        self,
        table_name: str,
        namespace: str = "default",
        retain_partition_columns: bool = True,
    ) -> tuple[pa.Schema, None]:
        self.last_schema_request = (
            table_name,
            namespace,
            retain_partition_columns,
        )
        return self.schema, None


def _table_info(**kwargs: object) -> TableInfo:
    values = {
        "table_id": "table-id",
        "table_name": "events",
        "table_namespace": "analytics",
        "table_path": "file:///tmp/events",
        "table_schema": "{}",
        "properties": '{"hashBucketNum":"4","owner":"data"}',
        "partitions": "dt;id",
    }
    values.update(kwargs)  # type: ignore
    return TableInfo(**values)


def test_catalog_builds_native_client_from_explicit_pg_config(monkeypatch) -> None:
    DummyNativeClient.instances.clear()
    monkeypatch.setattr(catalog_module, "NativeMetadataClient", DummyNativeClient)

    catalog = LakeSoulCatalog(
        pg_url="jdbc:postgresql://localhost:5432/lakesoul_test?stringtype=unspecified",
        pg_username="lakesoul_test",
        pg_password="secret",
        namespace="analytics",
        object_store_options={"fs.s3a.endpoint": "http://localhost:9000"},
    )

    assert catalog.namespace == "analytics"
    assert catalog.object_store_options["fs.s3a.endpoint"] == "http://localhost:9000"
    assert DummyNativeClient.instances[0].config == (
        "host=localhost port=5432 dbname=lakesoul_test "
        "user=lakesoul_test password=secret"
    )


def test_catalog_accepts_metadata_config_and_secondary_url(monkeypatch) -> None:
    DummyNativeClient.instances.clear()
    monkeypatch.setattr(catalog_module, "NativeMetadataClient", DummyNativeClient)

    LakeSoulCatalog(
        PostgresMetadataConfig(
            url="postgresql://primary:5432/db",
            username="user",
            password="pw",
            secondary_url="postgresql://secondary:5433/db2",
            max_retry=7,
        )
    )

    client = DummyNativeClient.instances[0]
    assert client.config == "host=primary port=5432 dbname=db user=user password=pw"
    assert client.secondary_config == (
        "host=secondary port=5433 dbname=db2 user=user password=pw"
    )
    assert client.max_retry == 7


def test_catalog_merges_object_store_options(monkeypatch) -> None:
    monkeypatch.setattr(catalog_module, "NativeMetadataClient", DummyNativeClient)
    catalog = LakeSoulCatalog(
        pg_url="postgresql://localhost/db",
        pg_username="user",
        pg_password="pw",
        object_store_options={"endpoint": "a", "region": "us"},
    )

    assert catalog._merge_object_store_options({"endpoint": "b"}) == {
        "endpoint": "b",
        "region": "us",
    }


def test_create_table_forwards_catalog_client() -> None:
    client = DummyNativeClient("host=localhost port=5432 dbname=db user=u password=p")
    catalog = LakeSoulCatalog(namespace="analytics", _client=client)
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("dt", pa.string())])

    result = catalog.create_table(
        "events",
        path="/tmp/events",
        schema=schema,
        partition_by=["dt"],
        primary_keys=["id"],
        hash_bucket_num=8,
    )

    assert isinstance(result, LakeSoulTable)
    assert client.created[0]["table_name"] == "events"
    assert client.created[0]["namespace"] == "analytics"
    assert client.created[0]["properties"] == {"hashBucketNum": "8"}
    assert client.created[0]["partitions"] == {"range": ["dt"], "hash": ["id"]}


def test_table_properties_and_write_config(monkeypatch) -> None:
    table = LakeSoulTable(
        LakeSoulCatalog(_client=DummyNativeClient("host=localhost dbname=db")),
        _table_info(),
    )
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("dt", pa.string())])
    monkeypatch.setattr(LakeSoulTable, "schema", property(lambda self: schema))

    assert table.properties["owner"] == "data"
    assert table.partition_by == ("dt",)
    assert table.primary_keys == ("id",)
    assert table.hash_bucket_num == 4
    assert table.write_config().schema is schema


def test_write_arrow_commits_written_files(monkeypatch) -> None:
    client = DummyNativeClient("host=localhost dbname=db")
    catalog = LakeSoulCatalog(
        _client=client,
        object_store_options={"endpoint": "default", "region": "us"},
    )
    table = LakeSoulTable(catalog, _table_info(partitions=";"))
    schema = pa.schema([pa.field("id", pa.int64())])
    monkeypatch.setattr(LakeSoulTable, "schema", property(lambda self: schema))

    writer_configs = []

    class FakeWriter:
        def __init__(self, config) -> None:
            writer_configs.append(config)
            self.result = None

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback) -> bool:
            if exc_type is None:
                self.result = SimpleNamespace(
                    files=(
                        SimpleNamespace(
                            partition="-5",
                            path="file:///tmp/events/part-0.parquet",
                            size=10,
                            existing_columns=("id",),
                        ),
                    )
                )
            return False

        def write(self, data) -> int:
            return 1

    monkeypatch.setattr(catalog_module, "Writer", FakeWriter)

    result = table.write_arrow(
        pa.table({"id": [1]}),
        object_store_options={"endpoint": "override"},
    )

    assert result.files[0].path == "file:///tmp/events/part-0.parquet"
    assert writer_configs[0].object_store_options == {
        "endpoint": "override",
        "region": "us",
    }
    assert client.commits == [
        (
            "events",
            "analytics",
            [("-5", "file:///tmp/events/part-0.parquet", 10, ["id"])],
        )
    ]


def test_scan_resolves_catalog_context_to_scan_config() -> None:
    client = DummyNativeClient("host=localhost dbname=db")
    catalog = LakeSoulCatalog(
        _client=client,
        object_store_options={"endpoint": "default", "region": "us"},
    )
    table = LakeSoulTable(catalog, _table_info())

    scan_config = (
        table.scan(
            partitions={"dt": "2026-06-18"},
            object_store_options={"endpoint": "override"},
            retain_partition_columns=True,
        )
        .select("id")
        .to_scan_config()
    )

    assert scan_config.table_name == "events"
    assert scan_config.namespace == "analytics"
    assert scan_config.partitions == {"dt": "2026-06-18"}
    assert scan_config.object_store_options == {
        "endpoint": "override",
        "region": "us",
    }
    assert scan_config.scan_partitions == client.scan_partitions
    assert client.last_scan_request == (
        "events",
        {"dt": "2026-06-18"},
        "analytics",
    )
    assert client.last_schema_request == ("events", "analytics", True)
