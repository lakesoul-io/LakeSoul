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
PhysicalFormat = Literal["parquet", "vortex"]
_ASCII_LOWER_TRANS = str.maketrans(
    {chr(code): chr(code + 32) for code in range(ord("A"), ord("Z") + 1)}
)


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
        table_info = self._client.get_table_info_by_name(
            _case_fold_identifier(name),
            namespace,
        )
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
        _validate_scan_runtime_options(
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
        )
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
        hash_bucket_num: int | None = None,
        properties: Mapping[str, str] | None = None,
        domain: str = "public",
    ) -> LakeSoulTable:
        if not isinstance(schema, pa.Schema):
            raise TypeError("schema must be a pyarrow.Schema")
        table_name = _case_fold_identifier(name)
        normalized_schema = _case_fold_schema(schema)
        normalized_partition_by = _case_fold_columns("partition_by", partition_by)
        normalized_primary_keys = _case_fold_columns("primary_keys", primary_keys)
        _validate_columns("partition_by", normalized_partition_by, normalized_schema)
        _validate_columns("primary_keys", normalized_primary_keys, normalized_schema)
        normalized_schema = _make_primary_keys_non_nullable(
            normalized_schema,
            normalized_primary_keys,
        )
        if hash_bucket_num is not None:
            _validate_hash_bucket_num(hash_bucket_num)

        props = dict(properties or {})
        if hash_bucket_num is not None:
            props["hashBucketNum"] = str(hash_bucket_num)
        elif normalized_primary_keys and "hashBucketNum" not in props:
            props["hashBucketNum"] = "4"

        namespace = self._resolve_namespace(namespace)
        self._client.create_table(
            table_name,
            namespace=namespace,
            table_path=path,
            table_schema=normalized_schema,
            properties=props,
            partitions={
                "range": list(normalized_partition_by),
                "hash": list(normalized_primary_keys),
            },
            domain=domain,
        )
        return self.table(table_name, namespace)

    def drop_table(
        self,
        name: str,
        namespace: str | None = None,
        *,
        if_exists: bool = False,
    ) -> None:
        namespace = self._resolve_namespace(namespace)
        name = _case_fold_identifier(name)
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
        retain_partition_columns: bool = True,
        object_store_options: Mapping[str, str] | None = None,
    ) -> LakeSoulScan:
        """
        when `filter` is not None and references the range column,
        retain_partition_columns` must be True.
        """
        return LakeSoulScan(
            table=self,
            partitions=_normalize_string_mapping("partitions", partitions or {}),
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
        format: PhysicalFormat = "parquet",
        batch_size: int = 8192,
        thread_num: int | None = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_options: Mapping[str, str] | None = None,
        options: Mapping[str, str] | None = None,
    ) -> WriteResult:
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

    def write_daft(
        self,
        dataframe: Any,
        *,
        format: PhysicalFormat = "parquet",
        batch_size: int = 8192,
        thread_num: int | None = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_options: Mapping[str, str] | None = None,
        options: Mapping[str, str] | None = None,
        results_buffer_size: int | Literal["num_cpus"] = "num_cpus",
    ) -> WriteResult:
        from lakesoul.daft import write_lakesoul

        return write_lakesoul(
            dataframe,
            self,
            format=format,
            batch_size=batch_size,
            thread_num=thread_num,
            max_file_size=max_file_size,
            max_row_group_size=max_row_group_size,
            object_store_options=object_store_options,
            options=options,
            results_buffer_size=results_buffer_size,
        )

    def build_vector_index(
        self,
        *,
        column: str | None = None,
        dim: int | None = None,
        nlist: int = 256,
        total_bits: int = 7,
        metric: str = "L2",
        partition_desc: str | None = None,
        partitions: Mapping[str, str] | None = None,
    ) -> dict:
        """Build or update the IVF+RaBitQ vector index for this table.

        The vector column is auto-detected from table properties
        (``vector_index_columns``), or can be overridden via *column*/*dim*.

        Args:
            column: Vector column name (auto-detected if omitted).
            dim: Vector dimension (auto-detected if omitted).
            nlist: Number of IVF clusters (default 256).
            total_bits: RaBitQ total bits (default 7).
            metric: Distance metric, ``"L2"`` or ``"IP"``.
            partition_desc: Build index for a single partition, e.g.
                ``"range=2024-01-01"``.  When omitted, builds for all
                partitions.
            partitions: Shorthand for partition_desc — a mapping of
                partition column names to values.  Only one of
                *partition_desc* or *partitions* may be specified.

        Returns:
            Dict with summary: ``{"status": "ok", "partitions": 2, ...}``.
        """
        if partition_desc is not None and partitions is not None:
            raise ValueError(
                "partition_desc and partitions are mutually exclusive"
            )

        # Auto-detect vector column from table properties
        vec_col, vec_dim = self._vector_column_info(column, dim)

        store_config = _default_object_store_config(
            catalog=self._catalog, table=self
        )

        if partition_desc is not None:
            return _build_vector_index_for_one(
                table=self, column=vec_col, dim=vec_dim,
                nlist=nlist, total_bits=total_bits, metric=metric,
                partition_desc=partition_desc, store_config=store_config,
            )

        if partitions is not None:
            # Construct partition_desc in the table's partition_by order
            part_cols = self.partition_by
            missing = [c for c in part_cols if c not in partitions]
            if missing:
                raise ValueError(
                    f"missing partition columns: {missing}"
                )
            desc = ",".join(f"{c}={partitions[c]}" for c in part_cols)
            return _build_vector_index_for_one(
                table=self, column=vec_col, dim=vec_dim,
                nlist=nlist, total_bits=total_bits, metric=metric,
                partition_desc=desc, store_config=store_config,
            )

        # Build for all partitions
        from lakesoul.vector_index import build_table_vector_index
        return build_table_vector_index(
            table_name=self.name, namespace=self.namespace,
            vector_column=vec_col, dim=vec_dim,
            nlist=nlist, total_bits=total_bits, metric=metric,
            store_config=store_config,
        )

    def _vector_column_info(
        self, column: str | None, dim: int | None,
    ) -> tuple[str, int]:
        props = dict(self.properties)
        raw = props.get("vector_index_columns", "")
        if raw:
            # Parse "col:dim:nlist:bits:metric" format
            parts = raw.split(":") if ":" in raw else [raw]
            col = column or parts[0]
            d = dim or (int(parts[1]) if len(parts) > 1 else 0)
        else:
            col = column
            d = dim or 0
        if col is None:
            raise ValueError(
                "vector column not specified and not found in table properties"
            )
        if d <= 0:
            raise ValueError(f"invalid vector dimension: {d}")
        return col, d

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
        _reader_options: Mapping[str, str] | None = None,
    ) -> None:
        _validate_scan_runtime_options(
            batch_size=batch_size,
            thread_count=thread_count,
            rank=rank,
            world_size=world_size,
        )
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
        self._reader_options = dict(_reader_options or {})

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
        merged.update(_normalize_string_mapping("partitions", partitions or {}))
        merged.update(_normalize_string_mapping("partitions", overrides))
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
        reader_options: Mapping[str, str] | None = None,
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
        if reader_options is not None:
            updates["_reader_options"] = dict(reader_options)
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

    def to_arrow_table(self) -> pa.Table:
        return self.to_arrow_dataset().to_table(
            columns=list(self._columns) if self._columns is not None else None,
            filter=self._filter,
        )

    def to_ray(self) -> Any:
        from lakesoul.ray.read_lakesoul import read_lakesoul

        return read_lakesoul(self)

    def to_daft(self) -> Any:
        from lakesoul.daft import read_lakesoul

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
            filter=self._filter,
            object_store_options=dict(self._object_store_options),
            batch_size=self._batch_size,
            thread_count=self._thread_count,
            rank=self._rank,
            world_size=self._world_size,
            reader_options=dict(self._reader_options),
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
            "_reader_options": self._reader_options,
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


def _normalize_string_mapping(name: str, value: Mapping[str, Any]) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be a mapping")
    result = {}
    for k, v in value.items():
        if not isinstance(k, str):
            raise TypeError(f"{name} keys must be strings")
        result[k] = str(v)
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


def _normalize_variadic_columns(
    columns: tuple[str | Sequence[str], ...],
) -> tuple[str, ...] | None:
    if len(columns) == 1 and not isinstance(columns[0], str):
        return _normalize_optional_columns(columns[0])
    return _normalize_optional_columns(columns)  # ty: ignore


def _validate_filter(expression: ds.Expression | None) -> ds.Expression | None:
    if expression is not None and not isinstance(expression, ds.Expression):
        raise TypeError("filter must be a pyarrow.dataset.Expression")
    return expression


def _validate_scan_runtime_options(
    *,
    batch_size: int,
    thread_count: int,
    rank: int | None,
    world_size: int | None,
) -> None:
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size <= 0
    ):
        raise ValueError(
            f"batch_size must be a positive integer; {batch_size} is invalid"
        )
    if (
        isinstance(thread_count, bool)
        or not isinstance(thread_count, int)
        or thread_count < 0
    ):
        raise ValueError(
            f"thread_count must be a non-negative integer; {thread_count} is invalid"
        )
    if rank is None and world_size is None:
        return
    if rank is None or world_size is None:
        raise ValueError("rank and world_size must be both set or both unset")
    if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
        raise ValueError(f"rank must be a non-negative integer; {rank} is invalid")
    if (
        isinstance(world_size, bool)
        or not isinstance(world_size, int)
        or world_size <= 0
    ):
        raise ValueError(
            f"world_size must be a positive integer; {world_size} is invalid"
        )
    if rank >= world_size:
        raise ValueError(f"rank {rank} is out of range; world_size = {world_size}")


def _case_fold_identifier(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError("identifier must be a non-empty string")
    return value.translate(_ASCII_LOWER_TRANS)


def _case_fold_columns(name: str, columns: Sequence[str]) -> tuple[str, ...]:
    if isinstance(columns, (str, bytes)) or not isinstance(columns, Sequence):
        raise TypeError(f"{name} must be a sequence of strings")
    values = tuple(_case_fold_identifier(column) for column in columns)
    if len(values) != len(set(values)):
        raise ValueError(f"{name} must not contain duplicate columns")
    return values


def _case_fold_schema(schema: pa.Schema) -> pa.Schema:
    fields = []
    names = []
    for field in schema:
        name = _case_fold_identifier(field.name)
        fields.append(
            pa.field(
                name,
                field.type,
                nullable=field.nullable,
                metadata=field.metadata,
            )
        )
        names.append(name)
    if len(names) != len(set(names)):
        raise ValueError("schema must not contain duplicate columns after case folding")
    return pa.schema(fields, metadata=schema.metadata)


def _make_primary_keys_non_nullable(
    schema: pa.Schema,
    primary_keys: Sequence[str],
) -> pa.Schema:
    primary_key_set = set(primary_keys)
    if not primary_key_set:
        return schema
    fields = [
        pa.field(
            field.name,
            field.type,
            nullable=False if field.name in primary_key_set else field.nullable,
            metadata=field.metadata,
        )
        for field in schema
    ]
    return pa.schema(fields, metadata=schema.metadata)


def _validate_hash_bucket_num(hash_bucket_num: int) -> None:
    if (
        isinstance(hash_bucket_num, bool)
        or not isinstance(hash_bucket_num, int)
        or hash_bucket_num <= 0
    ):
        raise ValueError("hash_bucket_num must be greater than zero")


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


def _default_object_store_config(
    catalog: LakeSoulCatalog,
    table: LakeSoulTable,
) -> dict:
    """Build store_config dict for vector index from table/catalog info."""
    # Detect local vs S3 from table path
    path = table.path
    if path.startswith("file://"):
        return {"type": "local"}
    opts = dict(catalog.object_store_options)
    # Extract S3 config from standard keys
    config: dict = {"type": "s3"}
    for src, dst in [
        ("fs.s3a.access.key", "access_key_id"),
        ("fs.s3a.secret.key", "secret_access_key"),
        ("fs.s3a.endpoint", "endpoint"),
        ("fs.s3a.endpoint.region", "region"),
    ]:
        if src in opts:
            config[dst] = opts[src]
    # Bucket from path
    if path.startswith("s3://") or path.startswith("s3a://"):
        rest = path.split("://", 1)[1]
        config["bucket"] = rest.split("/", 1)[0]
    return config


def _build_vector_index_for_one(
    *,
    table: LakeSoulTable,
    column: str,
    dim: int,
    nlist: int,
    total_bits: int,
    metric: str,
    partition_desc: str,
    store_config: dict,
) -> dict:
    from lakesoul.vector_index import build_partition_vector_index

    return build_partition_vector_index(
        table_name=table.name,
        namespace=table.namespace,
        partition_desc=partition_desc,
        vector_column=column,
        dim=dim,
        nlist=nlist,
        total_bits=total_bits,
        metric=metric,
        store_config=store_config,
    )


__all__ = [
    "LakeSoulCatalog",
    "LakeSoulScan",
    "LakeSoulTable",
    "PostgresMetadataConfig",
    "TableWriteConfig",
]
