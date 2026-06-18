# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from types import SimpleNamespace

import pyarrow as pa

import lakesoul.catalog as catalog_module
from lakesoul.catalog import LakeSoulCatalog, LakeSoulTable, PostgresMetadataConfig


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
        self.commits: list[tuple[str, str, list[tuple[str, str, int, list[str]]]]] = []
        DummyNativeClient.instances.append(self)

    def commit_data_files(
        self,
        table_name: str,
        namespace: str,
        files: list[tuple[str, str, int, list[str]]],
    ) -> None:
        self.commits.append((table_name, namespace, files))


def _table_info(**kwargs: object) -> SimpleNamespace:
    values = {
        "table_id": "table-id",
        "table_name": "events",
        "table_namespace": "analytics",
        "table_path": "file:///tmp/events",
        "table_schema": "{}",
        "properties": '{"hashBucketNum":"4","owner":"data"}',
        "partitions": "dt;id",
    }
    values.update(kwargs)
    return SimpleNamespace(**values)


def test_catalog_builds_native_client_from_explicit_pg_config(
    monkeypatch,
) -> None:
    DummyNativeClient.instances.clear()
    monkeypatch.setattr(catalog_module, "_NativeMetadataClient", DummyNativeClient)

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
    monkeypatch.setattr(catalog_module, "_NativeMetadataClient", DummyNativeClient)

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
    monkeypatch.setattr(catalog_module, "_NativeMetadataClient", DummyNativeClient)
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


def test_create_table_forwards_catalog_client(monkeypatch) -> None:
    client = DummyNativeClient("host=localhost port=5432 dbname=db user=u password=p")
    catalog = LakeSoulCatalog(namespace="analytics", _client=client)
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("dt", pa.string())])
    calls: list[dict[str, object]] = []

    def fake_create_table(*args: object, **kwargs: object) -> None:
        calls.append({"args": args, "kwargs": kwargs})

    monkeypatch.setattr(catalog_module, "_create_table", fake_create_table)
    monkeypatch.setattr(
        LakeSoulCatalog,
        "table",
        lambda self, name, namespace=None: ("table", name, namespace),
    )

    result = catalog.create_table(
        "events",
        path="/tmp/events",
        schema=schema,
        partition_by=["dt"],
        primary_keys=["id"],
        hash_bucket_num=8,
    )

    assert result == ("table", "events", "analytics")
    assert calls[0]["args"] == ("events",)
    assert calls[0]["kwargs"]["namespace"] == "analytics"
    assert calls[0]["kwargs"]["properties"] == {"hashBucketNum": "8"}
    assert calls[0]["kwargs"]["partitions"] == {"range": ["dt"], "hash": ["id"]}
    assert calls[0]["kwargs"]["_client"] is client


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


def test_write_arrow_rejects_non_append_mode() -> None:
    table = LakeSoulTable(
        LakeSoulCatalog(_client=DummyNativeClient("host=localhost dbname=db")),
        _table_info(),
    )

    try:
        table.write_arrow(pa.table({"id": [1]}), mode="overwrite")  # type: ignore[arg-type]
    except NotImplementedError as error:
        assert "append" in str(error)
    else:
        raise AssertionError("expected NotImplementedError")


def test_scan_passes_catalog_context_to_arrow_dataset(monkeypatch) -> None:
    client = DummyNativeClient("host=localhost dbname=db")
    catalog = LakeSoulCatalog(
        _client=client,
        object_store_options={"endpoint": "default", "region": "us"},
    )
    table = LakeSoulTable(catalog, _table_info())
    calls: list[dict[str, object]] = []

    def fake_lakesoul_dataset(*args: object, **kwargs: object) -> object:
        calls.append({"args": args, "kwargs": kwargs})
        return object()

    import lakesoul.arrow

    monkeypatch.setattr(lakesoul.arrow, "lakesoul_dataset", fake_lakesoul_dataset)

    table.scan(
        partitions={"dt": "2026-06-18"},
        object_store_options={"endpoint": "override"},
    ).to_arrow_dataset()

    assert calls[0]["args"] == ("events",)
    assert calls[0]["kwargs"]["namespace"] == "analytics"
    assert calls[0]["kwargs"]["partitions"] == {"dt": "2026-06-18"}
    assert calls[0]["kwargs"]["object_store_configs"] == {
        "endpoint": "override",
        "region": "us",
    }
    assert calls[0]["kwargs"]["metadata_client"] is client
