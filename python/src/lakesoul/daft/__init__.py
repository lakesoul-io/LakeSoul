# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import importlib
import inspect
from collections.abc import Iterator, Mapping
from dataclasses import replace
from typing import TYPE_CHECKING, Any, Literal

import pyarrow as pa
import pyarrow.dataset as ds

from lakesoul.arrow import LakeSoulScanConfig, lakesoul_dataset
from lakesoul.arrow.dataset import schema_projection
from lakesoul.io import IOConfig, Writer, WriteResult

if TYPE_CHECKING:
    from lakesoul.catalog import LakeSoulScan, LakeSoulTable

__all__ = ["read_lakesoul", "write_lakesoul"]

_INSTALL_MESSAGE = (
    "Daft support requires the optional dependency. "
    "Install it with `pip install lakesoul[daft]` or run "
    "`uv sync --extra daft` from the python package."
)


def read_lakesoul(scan: LakeSoulScan) -> Any:
    """Read a LakeSoul scan as a Daft DataFrame."""
    daft = _require_daft()
    scan_config = scan.to_scan_config()
    return daft.from_arrow(
        _iter_lakesoul_tables(
            scan_config,
            columns=scan.columns,
            filter=scan.expression,
        )
    )


def write_lakesoul(
    dataframe: Any,
    table: LakeSoulTable,
    *,
    format: Literal["parquet", "vortex", "vortex-compact"] = "vortex-compact",
    batch_size: int = 8192,
    thread_num: int | None = 1,
    max_file_size: int | None = None,
    max_row_group_size: int = 250_000,
    object_store_options: Mapping[str, str] | None = None,
    options: Mapping[str, str] | None = None,
    results_buffer_size: int | Literal["num_cpus"] = "num_cpus",
) -> WriteResult:
    """Write a Daft DataFrame to an existing LakeSoulTable."""
    _require_daft()
    if format not in {"parquet", "vortex", "vortex-compact"}:
        raise ValueError("format must be 'parquet', 'vortex', or 'vortex-compact'")

    write_config = table.write_config(format=format)
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
        object_store_options=table.catalog._merge_object_store_options(
            object_store_options
        ),
        options=dict(options or {}),
    )

    writer: Writer | None = None
    try:
        for arrow_item in _to_arrow_iter(
            dataframe,
            results_buffer_size=results_buffer_size,
        ):
            arrow_table = _coerce_arrow_table(arrow_item)
            normalized = _normalize_table(arrow_table, write_config.schema)
            if normalized.num_rows == 0:
                continue
            if writer is None:
                writer = Writer(writer_config)
            writer.write(normalized)
        if writer is None:
            return WriteResult(files=(), partitions={}, row_count=0)
        result = writer.finish()
    except BaseException:
        if writer is not None:
            writer.abort()
        raise

    table.catalog._commit_write_result(table, result)
    return result


def _iter_lakesoul_tables(
    scan_config: LakeSoulScanConfig,
    *,
    columns: tuple[str, ...] | None,
    filter: ds.Expression | None,
) -> Iterator[pa.Table]:
    schema = (
        schema_projection(scan_config.schema, list(columns))
        if columns is not None
        else scan_config.schema
    )
    scan_partitions = [
        scan_partition
        for scan_partition in scan_config.scan_partitions
        if scan_partition.files
    ]
    if not scan_partitions:
        yield pa.Table.from_batches([], schema=schema)
        return

    yielded = False
    for scan_partition in scan_partitions:
        partition_config = replace(
            scan_config,
            scan_partitions=(scan_partition,),
            rank=None,
            world_size=None,
        )
        arrow_dataset = lakesoul_dataset(partition_config)
        for batch in arrow_dataset.to_batches(
            columns=list(columns) if columns is not None else None,
            filter=filter,
        ):
            yielded = True
            yield pa.Table.from_batches([batch], schema=schema)
    if not yielded:
        yield pa.Table.from_batches([], schema=schema)


def _to_arrow_iter(
    dataframe: Any,
    *,
    results_buffer_size: int | Literal["num_cpus"],
) -> Iterator[Any]:
    to_arrow_iter = getattr(dataframe, "to_arrow_iter", None)
    if to_arrow_iter is None:
        raise TypeError("dataframe must be a daft.DataFrame")

    if _supports_keyword(to_arrow_iter, "results_buffer_size"):
        return iter(to_arrow_iter(results_buffer_size=results_buffer_size))
    if results_buffer_size != "num_cpus":
        raise TypeError(
            "this Daft DataFrame.to_arrow_iter() does not support "
            "results_buffer_size"
        )
    return iter(to_arrow_iter())


def _coerce_arrow_table(value: Any) -> pa.Table:
    if isinstance(value, pa.Table):
        return value
    if isinstance(value, pa.RecordBatch):
        return pa.Table.from_batches([value])
    raise TypeError(
        "Daft DataFrame.to_arrow_iter() must yield pyarrow.Table or "
        "pyarrow.RecordBatch values"
    )


def _normalize_table(table: pa.Table, expected: pa.Schema) -> pa.Table:
    if table.schema.names != expected.names:
        raise ValueError(
            "Daft DataFrame schema does not match LakeSoul table schema:\n"
            f"expected: {expected}\nactual: {table.schema}"
        )

    columns = []
    for column, field in zip(table.columns, expected):
        if not field.nullable and column.null_count:
            raise ValueError(
                "Daft DataFrame schema does not match LakeSoul table schema:\n"
                f"column {field.name!r} contains nulls but is not nullable"
            )
        if not column.type.equals(field.type):
            try:
                column = column.cast(field.type, safe=True)
            except (pa.ArrowInvalid, pa.ArrowTypeError) as error:
                raise ValueError(
                    "Daft DataFrame schema does not match LakeSoul table schema:\n"
                    f"expected column {field.name!r}: {field.type}\n"
                    f"actual column {field.name!r}: {column.type}"
                ) from error
        columns.append(column)
    return pa.Table.from_arrays(columns, schema=expected)


def _supports_keyword(func: Any, name: str) -> bool:
    try:
        parameters = inspect.signature(func).parameters
    except (TypeError, ValueError):
        return False
    parameter = parameters.get(name)
    return parameter is not None and parameter.kind in {
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }


def _require_daft() -> Any:
    try:
        return importlib.import_module("daft")
    except ImportError as error:
        raise ImportError(_INSTALL_MESSAGE) from error


def _register_daft_dataframe_method() -> None:
    try:
        daft = _require_daft()
    except ImportError:
        return
    dataframe_type = getattr(daft, "DataFrame", None)
    if dataframe_type is not None:
        dataframe_type.write_lakesoul = write_lakesoul


_register_daft_dataframe_method()
