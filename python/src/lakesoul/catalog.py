# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal

import pyarrow as pa
import pyarrow.dataset as ds

from lakesoul._lib._utils import _schema_from_metadata_str
from lakesoul.io import IOConfig, Writer, WriteResult
from lakesoul.metadata import NativeMetadataClient, PostgresMetadataConfig, TableInfo

DEFAULT_SCAN_BATCH_SIZE: int = 2**10
WriteMode = Literal["append"]
PhysicalFormat = Literal["parquet", "vortex"]


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
    """Main Python entry point for LakeSoul metadata and table IO."""

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
        _client: Any | None = None,
    ) -> None:
        if not namespace:
            raise ValueError("namespace must not be empty")

        if _client is not None:
            client = _client
        elif metadata_config is None:
            if pg_url is None or pg_username is None or pg_password is None:
                client = NativeMetadataClient.from_env()
            else:
                metadata_config = PostgresMetadataConfig(
                    url=pg_url,
                    username=pg_username,
                    password=pg_password,
                    secondary_url=pg_secondary_url,
                    max_retry=max_retry,
                )
                client = NativeMetadataClient(
                    metadata_config.primary_config(),
                    metadata_config.secondary_config(),
                    metadata_config.max_retry,
                )
        else:
            client = NativeMetadataClient(
                metadata_config.primary_config(),
                metadata_config.secondary_config(),
                metadata_config.max_retry,
            )

        self._client = client
        self._namespace = namespace
        self._object_store_options = MappingProxyType(
            _validate_string_mapping("object_store_options", object_store_options or {})
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
            _client=NativeMetadataClient.from_env(),
        )

    @property
    def namespace(self) -> str:
        return self._namespace

    @property
    def object_store_options(self) -> Mapping[str, str]:
        return self._object_store_options

    def list_namespaces(self) -> tuple[str, ...]:
        return self._client.list_namespaces()

    def list_tables(self, namespace: str | None = None) -> tuple[str, ...]:
        return self._client.list_tables(self._resolve_namespace(namespace))

    def table(self, name: str, namespace: str | None = None) -> LakeSoulTable:
        namespace = self._resolve_namespace(namespace)
        table_info = self._client.get_table_info_by_name(name, namespace)
        return LakeSoulTable(self, table_info)

    def scan(
        self,
        name: str,
        *,
        namespace: str | None = None,
        partitions: Mapping[str, str] | None = None,
        columns: Sequence[str] | str | None = None,
        filter: ds.Expression | None = None,
        batch_size: int = DEFAULT_SCAN_BATCH_SIZE,
        thread_count: int = 1,
        rank: int | None = None,
        world_size: int | None = None,
        retain_partition_columns: bool = False,
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulScan:
        return self.table(name, namespace).scan(
            partitions=partitions,
            columns=columns,
            filter=filter,
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
            retain_partition_columns=retain_partition_columns,
            object_store_options=object_store_options,
        )

    def dataset(
        self,
        name: str,
        *,
        namespace: str | None = None,
        partitions: Mapping[str, str] | None = None,
        batch_size: int = DEFAULT_SCAN_BATCH_SIZE,
        thread_count: int = 1,
        rank: int | None = None,
        world_size: int | None = None,
        retain_partition_columns: bool = False,
        object_store_options: Mapping[str, str] | None = None,
    ) -> ds.Dataset:
        return self.scan(
            name,
            namespace=namespace,
            partitions=partitions,
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
            retain_partition_columns=retain_partition_columns,
            object_store_options=object_store_options,
        ).to_arrow_dataset()

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
        self._client.create_table(
            name,
            namespace=namespace,
            table_path=path,
            table_schema=schema,
            properties=props,
            partitions={"range": list(partition_by), "hash": list(primary_keys)},
            domain=domain,
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
            self._client.drop_table(name, namespace)
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
            _validate_string_mapping("object_store_options", object_store_options or {})
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
        return _schema_from_metadata_str(self._table_info.table_schema, None)

    @property
    def properties(self) -> Mapping[str, Any]:
        return MappingProxyType(_parse_table_properties(self._table_info.properties))

    @property
    def partition_by(self) -> tuple[str, ...]:
        partition_by, _ = self._catalog._client.get_partition_and_pk_cols(
            self._table_info
        )
        return tuple(partition_by)

    @property
    def primary_keys(self) -> tuple[str, ...]:
        _, primary_keys = self._catalog._client.get_partition_and_pk_cols(
            self._table_info
        )
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
        columns: Sequence[str] | str | None = None,
        filter: ds.Expression | None = None,
        batch_size: int = DEFAULT_SCAN_BATCH_SIZE,
        thread_count: int = 1,
        rank: int | None = None,
        world_size: int | None = None,
        retain_partition_columns: bool = False,
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulScan:
        return LakeSoulScan(
            table=self,
            partitions=_validate_string_mapping("partitions", partitions or {}),
            columns=_normalize_optional_columns(columns),
            filter=_validate_filter(filter),
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
            retain_partition_columns=retain_partition_columns,
            object_store_options=self._catalog._merge_object_store_options(
                object_store_options
            ),
        )

    def scan_plan(
        self,
        partitions: Mapping[str, str] | None = None,
    ) -> tuple[Any, ...]:
        return tuple(
            self._catalog._client.get_scan_plan_partitions(
                self.name,
                partitions=dict(partitions or {}),
                namespace=self.namespace,
            )
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
        from lakesoul.ray.write_lakesoul import write_lakesoul

        write_lakesoul(
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
    """A configured, lazily resolved scan of a LakeSoul table."""

    def __init__(
        self,
        *,
        table: LakeSoulTable,
        partitions: Mapping[str, str],
        columns: tuple[str, ...] | None,
        filter: ds.Expression | None,
        batch_size: int,
        thread_count: int,
        rank: int | None,
        world_size: int | None,
        retain_partition_columns: bool,
        object_store_options: Mapping[str, str],
    ) -> None:
        self._table = table
        self._partitions = dict(partitions)
        self._columns = columns
        self._filter = filter
        self._batch_size = batch_size
        self._thread_count = thread_count
        self._rank = rank
        self._world_size = world_size
        self._retain_partition_columns = retain_partition_columns
        self._object_store_options = dict(object_store_options)

    @property
    def table(self) -> LakeSoulTable:
        return self._table

    @property
    def partitions(self) -> Mapping[str, str]:
        return MappingProxyType(dict(self._partitions))

    @property
    def columns(self) -> tuple[str, ...] | None:
        return self._columns

    @property
    def expression(self) -> ds.Expression | None:
        return self._filter

    @property
    def schema(self) -> pa.Schema:
        scan_config = self.to_scan_config()
        if self._columns is None:
            return scan_config.schema
        from lakesoul.arrow.dataset import schema_projection

        return schema_projection(scan_config.schema, list(self._columns))

    def select(self, *columns: str | Sequence[str]) -> LakeSoulScan:
        return self._replace(columns=_normalize_variadic_columns(columns))

    def filter(self, expression: ds.Expression | None) -> LakeSoulScan:
        return self._replace(filter=_validate_filter(expression))

    def with_partitions(
        self,
        partitions: Mapping[str, str] | None = None,
        **overrides: str,
    ) -> LakeSoulScan:
        merged = dict(self._partitions)
        merged.update(_validate_string_mapping("partitions", partitions or {}))
        merged.update(_validate_string_mapping("partitions", overrides))
        return self._replace(partitions=merged)

    def shard(self, rank: int, world_size: int) -> LakeSoulScan:
        return self._replace(rank=rank, world_size=world_size)

    def options(
        self,
        *,
        batch_size: int | None = None,
        thread_count: int | None = None,
        retain_partition_columns: bool | None = None,
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulScan:
        updates: dict[str, Any] = {}
        if batch_size is not None:
            updates["batch_size"] = batch_size
        if thread_count is not None:
            updates["thread_count"] = thread_count
        if retain_partition_columns is not None:
            updates["retain_partition_columns"] = retain_partition_columns
        if object_store_options is not None:
            updates["object_store_options"] = (
                self._table.catalog._merge_object_store_options(object_store_options)
            )
        return self._replace(**updates)

    def scan_plan(self) -> tuple[Any, ...]:
        return tuple(
            self._table.catalog._client.get_scan_plan_partitions(
                self._table.name,
                partitions=self._partitions,
                namespace=self._table.namespace,
            )
        )

    def to_arrow_dataset(self) -> ds.Dataset:
        from lakesoul.arrow import lakesoul_dataset

        return lakesoul_dataset(self.to_scan_config())

    def to_scan_config(self) -> Any:
        return self._scan_config()

    def to_reader(self) -> pa.RecordBatchReader:
        return (
            self.to_arrow_dataset()
            .scanner(
                columns=list(self._columns) if self._columns is not None else None,
                filter=self._filter,
            )
            .to_reader()
        )

    def to_batches(self) -> Any:
        return self.to_arrow_dataset().to_batches(
            columns=list(self._columns) if self._columns is not None else None,
            filter=self._filter,
        )

    def to_table(self) -> pa.Table:
        return self.to_arrow_dataset().to_table(
            columns=list(self._columns) if self._columns is not None else None,
            filter=self._filter,
        )

    def to_ray(self) -> Any:
        from lakesoul.ray.read_lakesoul import read_lakesoul

        return read_lakesoul(self)

    def to_torch(self) -> Any:
        from lakesoul.torch import Dataset

        return Dataset(self)

    def to_huggingface(self) -> Any:
        from lakesoul.huggingface import from_lakesoul

        return from_lakesoul(self)

    def _scan_config(self) -> Any:
        from lakesoul.arrow import LakeSoulScanConfig

        client = self._table.catalog._client
        schema, partition_schema = client.get_schemas_by_table_name(
            self._table.name,
            namespace=self._table.namespace,
            retain_partition_columns=self._retain_partition_columns,
        )
        return LakeSoulScanConfig(
            table_name=self._table.name,
            namespace=self._table.namespace,
            schema=schema,
            partition_schema=partition_schema,
            scan_partitions=self.scan_plan(),
            partitions=dict(self._partitions),
            object_store_options=dict(self._object_store_options),
            batch_size=self._batch_size,
            thread_count=self._thread_count,
            rank=self._rank,
            world_size=self._world_size,
        )

    def _replace(self, **updates: Any) -> LakeSoulScan:
        values = {
            "table": self._table,
            "partitions": self._partitions,
            "columns": self._columns,
            "filter": self._filter,
            "batch_size": self._batch_size,
            "thread_count": self._thread_count,
            "rank": self._rank,
            "world_size": self._world_size,
            "retain_partition_columns": self._retain_partition_columns,
            "object_store_options": self._object_store_options,
        }
        values.update(updates)
        return LakeSoulScan(**values)


def _validate_string_mapping(name: str, value: Mapping[str, str]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    result = dict(value)
    if any(not isinstance(k, str) or not isinstance(v, str) for k, v in result.items()):
        raise TypeError(f"{name} must contain only string keys and values")
    return result


def _normalize_optional_columns(
    columns: Sequence[str] | str | None,
) -> tuple[str, ...] | None:
    if columns is None:
        return None
    if isinstance(columns, str):
        values = (columns,)
    elif isinstance(columns, Sequence):
        values = tuple(columns)
    else:
        raise TypeError("columns must be a string or a sequence of strings")
    if any(not isinstance(column, str) or not column for column in values):
        raise TypeError("columns must contain non-empty strings")
    if len(values) != len(set(values)):
        raise ValueError("columns must not contain duplicate values")
    return values


def _normalize_variadic_columns(columns: tuple[str | Sequence[str], ...]):
    if len(columns) == 1 and not isinstance(columns[0], str):
        return _normalize_optional_columns(columns[0])
    return _normalize_optional_columns(*columns)


def _validate_filter(expression: ds.Expression | None) -> ds.Expression | None:
    if expression is not None and not isinstance(expression, ds.Expression):
        raise TypeError("filter must be a pyarrow.dataset.Expression")
    return expression


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
            raise ValueError(f"{name} column not in schema: {column}")


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
]
