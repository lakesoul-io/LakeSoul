# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import logging
import os
import socket
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
from daft.io.sink import DataSink, WriteResult as DaftWriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

from lakesoul.catalog import TableWriteConfig
from lakesoul.io import IOConfig, Writer
from lakesoul.io import WriteResult as LakeSoulWriteResult

if TYPE_CHECKING:
    from lakesoul.catalog import LakeSoulTable


_LOG = logging.getLogger(__name__)


class LakeSoulDataSink(DataSink[LakeSoulWriteResult]):
    """Distributed Daft sink backed by one LakeSoul Writer per micropartition."""

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

        self._table_handle: LakeSoulTable | None = table
        self._table: TableWriteConfig = table.write_config(format=format)
        self._batch_size = batch_size
        self._thread_num = thread_num
        self._max_file_size = max_file_size
        self._max_row_group_size = max_row_group_size
        self._object_store_options = table.catalog._merge_object_store_options(
            object_store_options
        )
        self._options = dict(options or {})
        self._result: LakeSoulWriteResult | None = None

        # Validate the complete native writer configuration on the driver before
        # any distributed write task starts.
        self._writer_config()

    def __getstate__(self) -> dict[str, Any]:
        state = dict(self.__dict__)
        # Workers only write files. The metadata client remains on the driver and
        # is used by the original sink instance during finalize().
        state["_table_handle"] = None
        return state

    def name(self) -> str:
        return "LakeSoul Write"

    @property
    def result(self) -> LakeSoulWriteResult | None:
        """Return the committed result after Daft has finalized the write."""
        return self._result

    def schema(self) -> Schema:
        return Schema.from_pyarrow_schema(
            pa.schema(
                [
                    pa.field("files_written", pa.int64(), nullable=False),
                    pa.field("rows_written", pa.int64(), nullable=False),
                    pa.field("bytes_written", pa.int64(), nullable=False),
                ]
            )
        )

    def write(
        self,
        micropartitions: Iterator[MicroPartition],
    ) -> Iterator[DaftWriteResult[LakeSoulWriteResult]]:
        # Daft currently invokes this method with one micropartition. Keep the
        # implementation correct even if a future version passes more: every
        # micropartition owns an independent Writer and produces one result.
        # max_file_size therefore limits file rollover within that
        # micropartition; it does not accumulate across micropartitions.
        for micropartition in micropartitions:
            arrow_table = _normalize_table(micropartition.to_arrow(), self._table.schema)
            if arrow_table.num_rows == 0:
                yield _to_daft_write_result(_empty_write_result())
                continue

            writer = Writer(self._writer_config())
            try:
                writer.write(arrow_table)
                result = writer.finish()
            except BaseException:
                try:
                    writer.abort()
                except Exception:
                    _LOG.exception(
                        "Failed to abort LakeSoul writer after write failure"
                    )
                raise

            _LOG.info(
                "LakeSoul Daft sink wrote micropartition: host=%s pid=%s "
                "rows=%s files=%s paths=%s",
                socket.gethostname(),
                os.getpid(),
                result.row_count,
                len(result.files),
                [file_info.path for file_info in result.files],
            )
            yield _to_daft_write_result(result)

    def finalize(
        self,
        write_results: list[DaftWriteResult[LakeSoulWriteResult]],
    ) -> MicroPartition:
        if self._table_handle is None:
            raise RuntimeError(
                "LakeSoul table handle is required to commit Daft writes"
            )

        task_results = [write_result.result for write_result in write_results]
        result = _merge_write_results(task_results)
        _LOG.info(
            "LakeSoul Daft sink finalized: micropartitions=%s rows=%s files=%s",
            len(task_results),
            result.row_count,
            len(result.files),
        )
        if result.files:
            self._table_handle.catalog._commit_write_result(
                self._table_handle,
                result,
            )
        self._result = result

        return MicroPartition.from_pydict(
            {
                "files_written": pa.array([len(result.files)], type=pa.int64()),
                "rows_written": pa.array([result.row_count], type=pa.int64()),
                "bytes_written": pa.array(
                    [sum(file_info.size for file_info in result.files)],
                    type=pa.int64(),
                ),
            }
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


def _normalize_table(table: pa.Table, expected: pa.Schema) -> pa.Table:
    if table.schema.names != expected.names:
        raise ValueError(
            "Daft DataFrame schema does not match LakeSoul table schema:\n"
            f"expected: {expected}\nactual: {table.schema}\n"
            "cast the DataFrame columns to the table schema before writing"
        )

    arrays = []
    for actual_field, expected_field in zip(table.schema, expected):
        column = table.column(actual_field.name)
        if not expected_field.nullable and column.null_count:
            raise ValueError(
                "Daft DataFrame contains null values for a non-nullable "
                f"LakeSoul column: {expected_field.name!r}"
            )

        if actual_field.type == expected_field.type:
            arrays.append(column)
            continue

        if _is_compatible_daft_type(actual_field.type, expected_field.type):
            arrays.append(pc.cast(column, expected_field.type, safe=True))
            continue

        raise ValueError(
            "Daft DataFrame schema does not match LakeSoul table schema:\n"
            f"expected: {expected}\nactual: {table.schema}\n"
            f"incompatible column {actual_field.name!r}: "
            f"{actual_field.type} cannot be written as {expected_field.type}\n"
            "cast the DataFrame columns to the table schema before writing"
        )

    return pa.Table.from_arrays(arrays, schema=expected)


def _is_compatible_daft_type(
    actual: pa.DataType,
    expected: pa.DataType,
) -> bool:
    """Allow only Arrow representation changes introduced by Daft."""
    actual_is_string = pa.types.is_string(actual) or pa.types.is_large_string(
        actual
    )
    expected_is_string = pa.types.is_string(expected) or pa.types.is_large_string(
        expected
    )
    if actual_is_string and expected_is_string:
        return True

    actual_is_binary = pa.types.is_binary(actual) or pa.types.is_large_binary(
        actual
    )
    expected_is_binary = pa.types.is_binary(expected) or pa.types.is_large_binary(
        expected
    )
    return actual_is_binary and expected_is_binary


def _empty_write_result() -> LakeSoulWriteResult:
    return LakeSoulWriteResult(files=(), partitions={}, row_count=0)


def _to_daft_write_result(
    result: LakeSoulWriteResult,
) -> DaftWriteResult[LakeSoulWriteResult]:
    return DaftWriteResult(
        result=result,
        bytes_written=sum(file_info.size for file_info in result.files),
        rows_written=result.row_count,
    )


def _merge_write_results(
    write_results: list[LakeSoulWriteResult],
) -> LakeSoulWriteResult:
    files = tuple(
        file_info
        for write_result in write_results
        for file_info in write_result.files
    )
    partitions: dict[str, list[Any]] = {}
    for file_info in files:
        partitions.setdefault(file_info.partition, []).append(file_info)
    return LakeSoulWriteResult(
        files=files,
        partitions={
            partition: tuple(partition_files)
            for partition, partition_files in partitions.items()
        },
        row_count=sum(write_result.row_count for write_result in write_results),
    )
