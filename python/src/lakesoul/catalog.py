# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.dataset as ds

from lakesoul._lib._metadata import _NativeMetadataClient
from lakesoul.io import IOConfig, Writer, WriteResult
from lakesoul.metadata import create_table as _create_table
from lakesoul.metadata import drop_table as _drop_table
from lakesoul.metadata.meta_ops import (
    LakeSoulScanPlanPartition,
    get_partition_and_pk_cols,
    get_scan_plan_partitions,
    get_table_info_by_name,
    list_namespaces as _list_namespaces,
    list_tables as _list_tables,
)
from lakesoul.metadata.generated.entity_pb2 import TableInfo

WriteMode = Literal["append"]
PhysicalFormat = Literal["parquet", "vortex"]


@dataclass(frozen=True, slots=True)
class PostgresMetadataConfig:
    """PostgreSQL metadata connection settings."""

    url: str
    username: str
    password: str
    secondary_url: str | None = None
    max_retry: int = 3

    def primary_config(self) -> str:
        return _pg_config_from_url(self.url, self.username, self.password)

    def secondary_config(self) -> str | None:
        if self.secondary_url is None:
            return None
        return _pg_config_from_url(self.secondary_url, self.username, self.password)


@dataclass(frozen=True, slots=True)
class TableWriteConfig:
    table_name: str
    namespace: str
    path: str
    schema: pa.Schema
    primary_keys: tuple[str, ...]
    partition_by: tuple[str, ...]
    hash_bucket_num: int
    format: PhysicalFormat


class LakeSoulCatalog:
    """Main Python entry point for LakeSoul table metadata and IO."""

    def __init__(
        self,
        metadata_config: PostgresMetadataConfig | None = None,
        *,
        pg_url: str | None = None,
        pg_username: str | None = None,
        pg_password: str | None = None,
        pg_secondary_url: str | None = None,
        max_retry: int = 3,
        namespace: str = "default",
        object_store_options: Mapping[str, str] | None = None,
        _client: _NativeMetadataClient | None = None,
    ) -> None:
        if not namespace:
            raise ValueError("namespace must not be empty")

        if _client is not None:
            client = _client
        else:
            if metadata_config is None:
                if pg_url is None or pg_username is None or pg_password is None:
                    raise ValueError(
                        "metadata_config or pg_url/pg_username/pg_password is required"
                    )
                metadata_config = PostgresMetadataConfig(
                    url=pg_url,
                    username=pg_username,
                    password=pg_password,
                    secondary_url=pg_secondary_url,
                    max_retry=max_retry,
                )
            client = _NativeMetadataClient(
                metadata_config.primary_config(),
                metadata_config.secondary_config(),
                metadata_config.max_retry,
            )

        self._client = client
        self._namespace = namespace
        self._object_store_options = MappingProxyType(
            _validate_string_mapping(
                "object_store_options", object_store_options or {}
            )
        )

    @classmethod
    def from_env(
        cls,
        *,
        namespace: str = "default",
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulCatalog:
        return cls(
            namespace=namespace,
            object_store_options=object_store_options,
            _client=_NativeMetadataClient.from_env(),
        )

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def object_store_options(self) -> Mapping[str, str]:
        return self._object_store_options

    def list_namespaces(self) -> tuple[str, ...]:
        return _list_namespaces(client=self._client)

    def list_tables(self, namespace: str | None = None) -> tuple[str, ...]:
        return _list_tables(self._resolve_namespace(namespace), client=self._client)

    def table(self, name: str, namespace: str | None = None) -> LakeSoulTable:
        namespace = self._resolve_namespace(namespace)
        table_info = get_table_info_by_name(name, namespace, client=self._client)
        return LakeSoulTable(self, table_info)

    def create_table(
        self,
        name: str,
        *,
        path: str | Path,
        schema: pa.Schema,
        namespace: str | None = None,
        partition_by: Sequence[str] = (),
        primary_keys: Sequence[str] = (),
        hash_bucket_num: int = 1,
        properties: Mapping[str, str] | None = None,
        domain: str = "public",
    ) -> LakeSoulTable:
        if not isinstance(schema, pa.Schema):
            raise TypeError("schema must be a pyarrow.Schema")
        _validate_columns("partition_by", partition_by, schema)
        _validate_columns("primary_keys", primary_keys, schema)
        if isinstance(hash_bucket_num, bool) or hash_bucket_num <= 0:
            raise ValueError("hash_bucket_num must be greater than zero")

        props = dict(properties or {})
        if hash_bucket_num != 1 or "hashBucketNum" not in props:
            props["hashBucketNum"] = str(hash_bucket_num)

        namespace = self._resolve_namespace(namespace)
        _create_table(
            name,
            namespace=namespace,
            table_path=path,
            table_schema=schema,
            properties=props,
            partitions={"range": list(partition_by), "hash": list(primary_keys)},
            domain=domain,
            _client=self._client,
        )
        return self.table(name, namespace)

    def drop_table(
        self,
        name: str,
        namespace: str | None = None,
        *,
        if_exists: bool = False,
    ) -> None:
        namespace = self._resolve_namespace(namespace)
        try:
            _drop_table(name, namespace, _client=self._client)
        except RuntimeError as error:
            if if_exists and "not found" in str(error).lower():
                return
            raise

    def _resolve_namespace(self, namespace: str | None) -> str:
        resolved = self._namespace if namespace is None else namespace
        if not resolved:
            raise ValueError("namespace must not be empty")
        return resolved

    def _merge_object_store_options(
        self,
        object_store_options: Mapping[str, str] | None,
    ) -> dict[str, str]:
        merged = dict(self._object_store_options)
        merged.update(
            _validate_string_mapping(
                "object_store_options", object_store_options or {}
            )
        )
        return merged

    def _commit_write_result(self, table: LakeSoulTable, result: WriteResult) -> None:
        files = [
            (
                file_info.partition,
                file_info.path,
                file_info.size,
                list(file_info.existing_columns),
            )
            for file_info in result.files
        ]
        if files:
            self._client.commit_data_files(table.name, table.namespace, files)


class LakeSoulTable:
    """A loaded LakeSoul table."""

    def __init__(self, catalog: LakeSoulCatalog, table_info: TableInfo) -> None:
        self._catalog = catalog
        self._table_info = table_info

    @property
    def catalog(self) -> LakeSoulCatalog:
        return self._catalog

    @property
    def name(self) -> str:
        return self._table_info.table_name

    @property
    def namespace(self) -> str:
        return self._table_info.table_namespace

    @property
    def id(self) -> str:
        return self._table_info.table_id

    @property
    def path(self) -> str:
        return self._table_info.table_path

    @property
    def schema(self) -> pa.Schema:
        from lakesoul._lib._utils import to_arrow_schema

        return to_arrow_schema(self._table_info.table_schema, None)

    @property
    def properties(self) -> Mapping[str, Any]:
        return MappingProxyType(_parse_table_properties(self._table_info.properties))

    @property
    def partition_by(self) -> tuple[str, ...]:
        partition_by, _ = get_partition_and_pk_cols(self._table_info)
        return tuple(partition_by)

    @property
    def primary_keys(self) -> tuple[str, ...]:
        _, primary_keys = get_partition_and_pk_cols(self._table_info)
        return tuple(primary_keys)

    @property
    def hash_bucket_num(self) -> int:
        raw_value = self.properties.get("hashBucketNum", "1")
        try:
            value = int(raw_value)
        except (TypeError, ValueError) as error:
            raise ValueError(
                f"invalid hashBucketNum table property: {raw_value!r}"
            ) from error
        if value <= 0:
            raise ValueError(f"invalid hashBucketNum table property: {raw_value!r}")
        return value

    def scan(
        self,
        *,
        partitions: Mapping[str, str] | None = None,
        columns: Sequence[str] | None = None,
        filter: ds.Expression | None = None,
        batch_size: int = 1024,
        thread_count: int = 1,
        retain_partition_columns: bool = False,
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulScan:
        return LakeSoulScan(
            table=self,
            partitions=dict(partitions or {}),
            columns=tuple(columns) if columns is not None else None,
            filter=filter,
            batch_size=batch_size,
            thread_count=thread_count,
            retain_partition_columns=retain_partition_columns,
            object_store_options=self._catalog._merge_object_store_options(
                object_store_options
            ),
        )

    def scan_plan(
        self,
        partitions: Mapping[str, str] | None = None,
    ) -> list[LakeSoulScanPlanPartition]:
        return get_scan_plan_partitions(
            self.name,
            partitions=dict(partitions or {}),
            namespace=self.namespace,
            client=self._catalog._client,
        )

    def write_config(
        self,
        *,
        format: PhysicalFormat = "parquet",
    ) -> TableWriteConfig:
        if format not in {"parquet", "vortex"}:
            raise ValueError("format must be 'parquet' or 'vortex'")
        return TableWriteConfig(
            table_name=self.name,
            namespace=self.namespace,
            path=self.path,
            schema=self.schema,
            primary_keys=self.primary_keys,
            partition_by=self.partition_by,
            hash_bucket_num=self.hash_bucket_num,
            format=format,
        )

    def write_arrow(
        self,
        data: pa.RecordBatch | pa.Table | pa.RecordBatchReader,
        *,
        mode: WriteMode = "append",
        format: PhysicalFormat = "parquet",
        batch_size: int = 8192,
        thread_num: int | None = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_options: Mapping[str, str] | None = None,
        options: Mapping[str, str] | None = None,
    ) -> WriteResult:
        _validate_write_mode(mode)
        write_config = self.write_config(format=format)
        writer_config = IOConfig(
            path=write_config.path,
            schema=write_config.schema,
            format=write_config.format,
            primary_keys=write_config.primary_keys,
            partition_by=write_config.partition_by,
            hash_bucket_num=write_config.hash_bucket_num,
            batch_size=batch_size,
            thread_num=thread_num,
            max_file_size=max_file_size,
            max_row_group_size=max_row_group_size,
            object_store_options=self._catalog._merge_object_store_options(
                object_store_options
            ),
            options=dict(options or {}),
        )
        with Writer(writer_config) as writer:
            writer.write(data)
        result = writer.result
        if result is None:
            raise RuntimeError("writer finished without a result")
        self._catalog._commit_write_result(self, result)
        return result

    def write_ray(
        self,
        dataset: Any,
        *,
        mode: WriteMode = "append",
        format: PhysicalFormat = "parquet",
        batch_size: int = 8192,
        thread_num: int | None = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_options: Mapping[str, str] | None = None,
        options: Mapping[str, str] | None = None,
        ray_remote_args: Mapping[str, Any] | None = None,
        concurrency: int | None = None,
    ) -> None:
        _validate_write_mode(mode)
        from lakesoul.ray.write_lakesoul import write_lakesoul_table

        write_lakesoul_table(
            dataset,
            self,
            format=format,
            batch_size=batch_size,
            thread_num=thread_num,
            max_file_size=max_file_size,
            max_row_group_size=max_row_group_size,
            object_store_options=object_store_options,
            options=options,
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
        )

    def drop(self, *, if_exists: bool = False) -> None:
        self._catalog.drop_table(self.name, self.namespace, if_exists=if_exists)


class LakeSoulScan:
    """A configured scan of a LakeSoul table."""

    def __init__(
        self,
        *,
        table: LakeSoulTable,
        partitions: dict[str, str],
        columns: tuple[str, ...] | None,
        filter: ds.Expression | None,
        batch_size: int,
        thread_count: int,
        retain_partition_columns: bool,
        object_store_options: Mapping[str, str],
    ) -> None:
        self._table = table
        self._partitions = partitions
        self._columns = columns
        self._filter = filter
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._retain_partition_columns = retain_partition_columns
        self._object_store_options = dict(object_store_options)

    @property
    def table(self) -> LakeSoulTable:
        return self._table

    def to_arrow_dataset(self) -> ds.Dataset:
        from lakesoul.arrow import lakesoul_dataset

        return lakesoul_dataset(
            self._table.name,
            batch_size=self._batch_size,
            thread_count=self._thread_count,
            partitions=self._partitions,
            retain_partition_columns=self._retain_partition_columns,
            namespace=self._table.namespace,
            object_store_configs=self._object_store_options,
            metadata_client=self._table.catalog._client,
        )

    def to_batches(self) -> Any:
        return self.to_arrow_dataset().to_batches(
            columns=list(self._columns) if self._columns is not None else None,
            filter=self._filter,
        )

    def to_reader(self) -> pa.RecordBatchReader:
        return self.to_arrow_dataset().scanner(
            columns=list(self._columns) if self._columns is not None else None,
            filter=self._filter,
        ).to_reader()

    def to_table(self) -> pa.Table:
        return self.to_arrow_dataset().to_table(
            columns=list(self._columns) if self._columns is not None else None,
            filter=self._filter,
        )

    def to_ray(self) -> Any:
        import ray

        return ray.data.from_arrow(self.to_table())


def connect(
    metadata_config: PostgresMetadataConfig | None = None,
    **kwargs: Any,
) -> LakeSoulCatalog:
    return LakeSoulCatalog(metadata_config, **kwargs)


def connect_from_env(
    *,
    namespace: str = "default",
    object_store_options: Mapping[str, str] | None = None,
) -> LakeSoulCatalog:
    return LakeSoulCatalog.from_env(
        namespace=namespace,
        object_store_options=object_store_options,
    )


def _pg_config_from_url(url: str, username: str, password: str) -> str:
    if not isinstance(url, str) or not url:
        raise ValueError("PostgreSQL url must not be empty")
    if not isinstance(username, str) or not username:
        raise ValueError("PostgreSQL username must not be empty")
    if not isinstance(password, str):
        raise TypeError("PostgreSQL password must be a string")

    parsed_url = url[5:] if url.startswith("jdbc:") else url
    parsed = urlparse(parsed_url)
    if parsed.scheme not in {"postgresql", "postgres"}:
        raise ValueError("PostgreSQL url must use postgresql or jdbc:postgresql")
    if not parsed.hostname:
        raise ValueError("PostgreSQL url must include a host")
    dbname = parsed.path.lstrip("/")
    if not dbname:
        raise ValueError("PostgreSQL url must include a database name")
    return (
        f"host={parsed.hostname} "
        f"port={parsed.port or 5432} "
        f"dbname={dbname} "
        f"user={username} "
        f"password={password}"
    )


def _validate_string_mapping(name: str, value: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    result = dict(value)
    if any(not isinstance(k, str) or not isinstance(v, str) for k, v in result.items()):
        raise TypeError(f"{name} must contain only string keys and values")
    return result


def _validate_columns(name: str, columns: Sequence[str], schema: pa.Schema) -> None:
    if isinstance(columns, (str, bytes)) or not isinstance(columns, Sequence):
        raise TypeError(f"{name} must be a sequence of strings")
    schema_names = set(schema.names)
    values = tuple(columns)
    if any(not isinstance(column, str) or not column for column in values):
        raise TypeError(f"{name} must contain non-empty strings")
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must not contain duplicate columns")
    for column in values:
        if column not in schema_names:
            raise ValueError(f"partition column not in schema: {column}")


def _parse_table_properties(properties: str) -> dict[str, Any]:
    if not properties:
        return {}
    try:
        value = json.loads(properties)
    except json.JSONDecodeError as error:
        raise ValueError("LakeSoul table properties are not valid JSON") from error
    if not isinstance(value, dict):
        raise ValueError("LakeSoul table properties must be a JSON object")
    return value


def _validate_write_mode(mode: str) -> None:
    if mode != "append":
        raise NotImplementedError("only append mode is supported")


__all__ = [
    "LakeSoulCatalog",
    "LakeSoulScan",
    "LakeSoulTable",
    "PostgresMetadataConfig",
    "TableWriteConfig",
    "connect",
    "connect_from_env",
]
