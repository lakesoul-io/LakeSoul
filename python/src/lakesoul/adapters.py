# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""Adapters that wrap a query result into another DataFrame framework.

Every LakeSoul query returns a :class:`pyarrow.Table` (zero-copy Arrow).  These
helpers only wrap that result for the surrounding ecosystem: they never scan
the LakeSoul table again, so the vector-index fast path and the lakehouse
semantics of the original query stay in effect.

Frameworks are optional imports; asking for one that is not installed raises an
:class:`ImportError` that names the package to install.
"""

from __future__ import annotations

from typing import Any, Literal

import pyarrow as pa

#: Accepted values of the ``to=`` keyword.
OutputFormat = Literal["pyarrow", "arrow", "daft", "ray", "duckdb"]


def to_pyarrow(result: pa.Table | pa.RecordBatch | list[pa.RecordBatch]) -> pa.Table:
    """Return the result as a :class:`pyarrow.Table` (the default format)."""
    if isinstance(result, pa.Table):
        return result
    if isinstance(result, pa.RecordBatch):
        return pa.Table.from_batches([result])
    return pa.Table.from_batches(list(result))


def to_daft(result: pa.Table | pa.RecordBatch) -> Any:
    """Wrap the result in a ``daft.DataFrame``.

    Requires the ``daft`` extra (``pip install 'lakesoul[daft]'``).
    """
    try:
        import daft
    except ImportError as error:  # pragma: no cover - depends on the environment
        raise ImportError(
            "daft is required for to='daft'; install it with "
            "`pip install 'lakesoul[daft]'`"
        ) from error
    return daft.from_arrow(to_pyarrow(result))


def to_ray_dataset(result: pa.Table | pa.RecordBatch) -> Any:
    """Wrap the result in a ``ray.data.Dataset``.

    Requires the ``ray`` extra (``pip install 'lakesoul[ray]'``).
    """
    try:
        import ray  # noqa: F401
        from ray import data as ray_data
    except ImportError as error:  # pragma: no cover - depends on the environment
        raise ImportError(
            "ray is required for to='ray'; install it with "
            "`pip install 'lakesoul[ray]'`"
        ) from error
    return ray_data.from_arrow(to_pyarrow(result))


def to_duckdb(
    result: pa.Table | pa.RecordBatch, *, connection: Any | None = None
) -> Any:
    """Register the result in DuckDB and return a lazy relation.

    Requires the ``duckdb`` extra (``pip install 'lakesoul[duckdb]'``).  Pass an
    existing ``connection`` to join the relation against other DuckDB data.
    """
    try:
        import duckdb
    except ImportError as error:  # pragma: no cover - depends on the environment
        raise ImportError(
            "duckdb is required for to='duckdb'; install it with "
            "`pip install 'lakesoul[duckdb]'`"
        ) from error
    database = connection if connection is not None else duckdb.connect()
    return database.from_arrow(to_pyarrow(result))


def convert(
    result: pa.Table | pa.RecordBatch | list[pa.RecordBatch],
    to: OutputFormat | None,
) -> Any:
    """Convert a result to the requested output format.

    Args:
        result: A LakeSoul query result (Arrow table or batches).
        to: ``None``/``"pyarrow"``/``"arrow"`` keep Arrow, ``"daft"``,
            ``"ray"`` and ``"duckdb"`` wrap it for those frameworks.

    Raises:
        ValueError: For an unknown format.
        ImportError: When the requested framework is not installed.
    """
    if to is None or to in ("pyarrow", "arrow"):
        return to_pyarrow(result)
    if to == "daft":
        return to_daft(result)
    if to == "ray":
        return to_ray_dataset(result)
    if to == "duckdb":
        return to_duckdb(result)
    raise ValueError(
        f"unknown output format {to!r}; expected one of "
        "'pyarrow', 'daft', 'ray', 'duckdb'"
    )
