# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

import pyarrow as pa
from ray.data import Dataset
from ray.data.block import BlockAccessor
from ray.data.datasource import Datasink

from lakesoul.io import IOConfig, Writer
from lakesoul.io import WriteResult as LakeSoulWriteResult

if TYPE_CHECKING:
    from lakesoul.catalog import LakeSoulTable


class LakeSoulDatasink(Datasink):
    """Ray Datasink that writes and commits through a LakeSoulTable handle."""

    def __init__(
        self,
        table: LakeSoulTable,
        *,
        format: Literal["parquet", "vortex", "vortex-compact"] = "vortex-compact",
        batch_size: int = 8192,
        thread_num: int | None = 1,
        max_file_size: int | None = None,
        max_row_group_size: int = 250_000,
        object_store_options: Mapping[str, str] | None = None,
        options: Mapping[str, str] | None = None,
    ) -> None:
        if format not in {"parquet", "vortex", "vortex-compact"}:
            raise ValueError("format must be 'parquet', 'vortex', or 'vortex-compact'")
        self._table_handle = table
        self._table = table.write_config(format=format)
        self._batch_size = batch_size
        self._thread_num = thread_num
        self._max_file_size = max_file_size
        self._max_row_group_size = max_row_group_size
        self._object_store_options = table.catalog._merge_object_store_options(
            object_store_options
        )
        self._options = dict(options or {})

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
            object_store_options=self._object_store_options,
            options=self._options,
        )

    def __getstate__(self) -> dict[str, Any]:
        state = dict(self.__dict__)
        state["_table_handle"] = None
        return state

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
                    partitions={},
                    row_count=0,
                )
            return writer.finish()
        except BaseException:
            if writer is not None:
                writer.abort()
            raise

    def on_write_complete(self, write_result: Any) -> None:
        if self._table_handle is None:
            raise RuntimeError("LakeSoul table handle is required to commit Ray writes")
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
            object_store_options=self._object_store_options,
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
    table: LakeSoulTable,
    *,
    format: Literal["parquet", "vortex", "vortex-compact"] = "vortex-compact",
    batch_size: int = 8192,
    thread_num: int | None = 1,
    max_file_size: int | None = None,
    max_row_group_size: int = 250_000,
    object_store_options: Mapping[str, str] | None = None,
    options: Mapping[str, str] | None = None,
    ray_remote_args: Mapping[str, Any] | None = None,
    concurrency: int | None = None,
) -> None:
    """Write a Ray Dataset to an existing LakeSoulTable."""
    if not isinstance(dataset, Dataset):
        raise TypeError("dataset must be a ray.data.Dataset")

    datasink = LakeSoulDatasink(
        table,
        format=format,
        batch_size=batch_size,
        thread_num=thread_num,
        max_file_size=max_file_size,
        max_row_group_size=max_row_group_size,
        object_store_options=object_store_options,
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


Dataset.write_lakesoul = write_lakesoul  # type: ignore[attr-defined]
