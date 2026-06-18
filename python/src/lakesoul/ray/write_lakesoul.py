# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal
from urllib.parse import urlparse

import pyarrow as pa
from ray.data import Dataset
from ray.data.block import BlockAccessor
from ray.data.datasource import Datasink

from lakesoul._lib._metadata import commit_data_files
from lakesoul.io import IOConfig, Writer
from lakesoul.io import WriteResult as LakeSoulWriteResult

from ..metadata.meta_ops import (
    get_arrow_schema_by_table_name,
    get_partition_and_pk_cols,
    get_table_info_by_name,
)


@dataclass(frozen=True, slots=True)
class _TableWriteConfig:
    table_name: str
    namespace: str
    path: str
    schema: pa.Schema
    primary_keys: tuple[str, ...]
    partition_by: tuple[str, ...]
    hash_bucket_num: int
    format: Literal["parquet", "vortex"]


class LakeSoulDatasink(Datasink):
    """Ray Datasink that writes and commits an existing LakeSoul table."""

    def __init__(
        self,
        table_name: str,
        *,
        namespace: str = "default",
        format: Literal["parquet", "vortex"] = "parquet",
        batch_size: int = 8192,
        thread_num: int | None = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_configs: Mapping[str, str] | None = None,
        options: Mapping[str, str] | None = None,
        table: Any | None = None,
    ) -> None:
        if format not in ["parquet", "vortex"]:
            raise ValueError(f"Unsupported format: {format}")
        self._table_handle = table
        self._table = (
            _load_table_write_config(
                table_name=table_name,
                namespace=namespace,
                format=format,
            )
            if table is None
            else table.write_config(format=format)
        )
        self._batch_size = batch_size
        self._thread_num = thread_num
        self._max_file_size = max_file_size
        self._max_row_group_size = max_row_group_size
        self._object_store_configs = dict(object_store_configs or {})
        self._options = dict(options or {})

        # Validate all writer options on the driver before Ray starts remote tasks.
        IOConfig(
            path=self._table.path,
            schema=self._table.schema,
            format=self._table.format,
            primary_keys=self._table.primary_keys,
            partition_by=self._table.partition_by,
            hash_bucket_num=self._table.hash_bucket_num,
            batch_size=self._batch_size,
            thread_num=self._thread_num,
            max_file_size=self._max_file_size,
            max_row_group_size=self._max_row_group_size,
            object_store_options=self._object_store_configs,
            options=self._options,
        )

    @property
    def supports_distributed_writes(self) -> bool:
        scheme = urlparse(self._table.path).scheme.lower()
        return scheme not in {"", "file", "local"}

    def write(self, blocks: Iterable[Any], ctx: Any) -> LakeSoulWriteResult:
        del ctx
        writer: Writer | None = None
        try:
            for block in blocks:
                table = BlockAccessor.for_block(block).to_arrow()
                if table.num_rows == 0:
                    continue
                if writer is None:
                    writer = Writer(self._writer_config())
                writer.write(self._normalize_table(table))
            if writer is None:
                return LakeSoulWriteResult(
                    files=(),
                    partitions=MappingProxyType({}),
                    row_count=0,
                )
            return writer.finish()
        except BaseException:
            if writer is not None:
                writer.abort()
            raise

    def on_write_complete(self, write_result: Any) -> None:
        task_results = getattr(write_result, "write_returns", write_result)
        files = [
            (
                file_info.partition,
                file_info.path,
                file_info.size,
                list(file_info.existing_columns),
            )
            for task_result in task_results
            for file_info in task_result.files
        ]
        if files:
            if self._table_handle is None:
                commit_data_files(
                    self._table.table_name,
                    self._table.namespace,
                    files,
                )
            else:
                self._table_handle.catalog._client.commit_data_files(
                    self._table.table_name,
                    self._table.namespace,
                    files,
                )

    def _writer_config(self) -> IOConfig:
        return IOConfig(
            path=self._table.path,
            schema=self._table.schema,
            format=self._table.format,
            primary_keys=self._table.primary_keys,
            partition_by=self._table.partition_by,
            hash_bucket_num=self._table.hash_bucket_num,
            batch_size=self._batch_size,
            thread_num=self._thread_num,
            max_file_size=self._max_file_size,
            max_row_group_size=self._max_row_group_size,
            object_store_options=self._object_store_configs,
            options=self._options,
        )

    def _normalize_table(self, table: pa.Table) -> pa.Table:
        if not table.schema.equals(self._table.schema, check_metadata=False):
            raise ValueError(
                "Ray Dataset schema does not match LakeSoul table schema:\n"
                f"expected: {self._table.schema}\nactual: {table.schema}"
            )
        return pa.Table.from_arrays(table.columns, schema=self._table.schema)


def write_lakesoul(
    dataset: Dataset,
    table_name: str,
    *,
    namespace: str = "default",
    format: Literal["parquet", "vortex"] = "parquet",
    batch_size: int = 8192,
    thread_num: int | None = 1,
    max_file_size: int | None = None,
    max_row_group_size: int = 250_000,
    object_store_configs: Mapping[str, str] | None = None,
    options: Mapping[str, str] | None = None,
    ray_remote_args: Mapping[str, Any] | None = None,
    concurrency: int | None = None,
) -> None:
    """Write a Ray Dataset to an existing LakeSoul table."""
    if not isinstance(dataset, Dataset):
        raise TypeError("dataset must be a ray.data.Dataset")

    datasink = LakeSoulDatasink(
        table_name,
        namespace=namespace,
        format=format,
        batch_size=batch_size,
        thread_num=thread_num,
        max_file_size=max_file_size,
        max_row_group_size=max_row_group_size,
        object_store_configs=object_store_configs,
        options=options,
    )
    _validate_dataset_schema(dataset, datasink._table.schema)

    remote_args = dict(ray_remote_args or {})
    remote_args.setdefault("max_retries", 0)
    dataset.write_datasink(
        datasink,
        ray_remote_args=remote_args,
        concurrency=concurrency,
    )


def write_lakesoul_table(
    dataset: Dataset,
    table: Any,
    *,
    format: Literal["parquet", "vortex"] = "parquet",
    batch_size: int = 8192,
    thread_num: int | None = 1,
    max_file_size: int | None = None,
    max_row_group_size: int = 250_000,
    object_store_options: Mapping[str, str] | None = None,
    options: Mapping[str, str] | None = None,
    ray_remote_args: Mapping[str, Any] | None = None,
    concurrency: int | None = None,
) -> None:
    """Write a Ray Dataset through a LakeSoulTable handle."""
    if not isinstance(dataset, Dataset):
        raise TypeError("dataset must be a ray.data.Dataset")

    datasink = LakeSoulDatasink(
        table.name,
        namespace=table.namespace,
        format=format,
        batch_size=batch_size,
        thread_num=thread_num,
        max_file_size=max_file_size,
        max_row_group_size=max_row_group_size,
        object_store_configs=table.catalog._merge_object_store_options(
            object_store_options
        ),
        options=options,
        table=table,
    )
    _validate_dataset_schema(dataset, datasink._table.schema)

    remote_args = dict(ray_remote_args or {})
    remote_args.setdefault("max_retries", 0)
    dataset.write_datasink(
        datasink,
        ray_remote_args=remote_args,
        concurrency=concurrency,
    )


def _load_table_write_config(
    *,
    table_name: str,
    namespace: str,
    format: Literal["parquet", "vortex"],
) -> _TableWriteConfig:
    table_info = get_table_info_by_name(table_name, namespace)
    partition_by, primary_keys = get_partition_and_pk_cols(table_info)
    properties = _parse_table_properties(table_info.properties)

    raw_format = format
    if not isinstance(raw_format, str):
        raise ValueError(f"invalid LakeSoul table file format: {raw_format!r}")
    configured_format = raw_format.lower()
    if configured_format not in {"parquet", "vortex"}:
        raise ValueError(f"unsupported LakeSoul table file format: {configured_format}")

    raw_bucket_num = properties.get("hashBucketNum", "1")
    try:
        hash_bucket_num = max(1, int(raw_bucket_num))
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"invalid hashBucketNum table property: {raw_bucket_num!r}"
        ) from error

    return _TableWriteConfig(
        table_name=table_name,
        namespace=namespace,
        path=table_info.table_path,
        schema=get_arrow_schema_by_table_name(
            table_name=table_name,
            namespace=namespace,
            retain_partition_columns=True,
        ),
        primary_keys=tuple(primary_keys),
        partition_by=tuple(partition_by),
        hash_bucket_num=hash_bucket_num,
        format=configured_format,  # type: ignore
    )


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


def _validate_dataset_schema(dataset: Dataset, expected: pa.Schema) -> None:
    schema = dataset.schema()
    arrow_schema = getattr(schema, "base_schema", schema)
    if not isinstance(arrow_schema, pa.Schema):
        raise ValueError("Ray Dataset does not have an Arrow schema")
    if not arrow_schema.equals(expected, check_metadata=False):
        raise ValueError(
            "Ray Dataset schema does not match LakeSoul table schema:\n"
            f"expected: {expected}\nactual: {arrow_schema}"
        )


Dataset.write_lakesoul = write_lakesoul  # type: ignore
