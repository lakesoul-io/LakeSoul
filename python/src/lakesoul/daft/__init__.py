# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal

from lakesoul.io import WriteResult

if TYPE_CHECKING:
    from lakesoul.catalog import LakeSoulScan, LakeSoulTable

__all__ = ["LakeSoulDataSink", "read_lakesoul", "write_lakesoul"]

_INSTALL_MESSAGE = (
    "Daft support requires the optional dependency. "
    "Install it with `pip install lakesoul[daft]` or run "
    "`uv sync --extra daft` from the python package."
)


def read_lakesoul(scan: LakeSoulScan) -> Any:
    """Read a LakeSoul scan as a Daft DataFrame."""
    _require_daft()
    from lakesoul.daft.source import LakeSoulDataSource

    scan_config = scan.to_scan_config()
    source = LakeSoulDataSource(
        scan_config,
        columns=scan.columns,
        partition_columns=scan.table.partition_by,
    )
    return source.read()


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
    """Write a Daft DataFrame through Daft's distributed DataSink API."""
    _require_daft()
    del results_buffer_size  # Kept for source compatibility; native sinks do not collect.

    from lakesoul.daft.sink import LakeSoulDataSink

    sink = LakeSoulDataSink(
        table,
        format=format,
        batch_size=batch_size,
        thread_num=thread_num,
        max_file_size=max_file_size,
        max_row_group_size=max_row_group_size,
        object_store_options=object_store_options,
        options=options,
    )
    write_sink = getattr(dataframe, "write_sink", None)
    if write_sink is None:
        raise TypeError("dataframe must be a daft.DataFrame")
    write_sink(sink)
    result = sink.result
    if result is None:
        raise RuntimeError("Daft sink completed without a LakeSoul write result")
    return result


def __getattr__(name: str) -> Any:
    if name == "LakeSoulDataSink":
        from lakesoul.daft.sink import LakeSoulDataSink

        return LakeSoulDataSink
    raise AttributeError(name)


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
