# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""Vector similarity search through a Daft DataFrame.

``vector_search`` pushes the similarity filtering down to the native
reader: every Daft task (one per scan partition / hash bucket) runs an ANN
search against that bucket's IVF+RaBitQ index and only returns candidate
rows.  The candidates are then re-ranked by exact distance in Daft and
truncated to the global top-``k`` with a ``sort`` + ``limit``, so the
whole pipeline stays lazy and distributed.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import daft
from daft import col

if TYPE_CHECKING:
    from lakesoul.catalog import LakeSoulScan, LakeSoulTable

_DISTANCE_COLUMN = "vector_distance"


def vector_search(
    source: LakeSoulTable | LakeSoulScan,
    query: Sequence[float],
    *,
    top_k: int = 10,
    nprobe: int = 64,
    column: str | None = None,
    metric: str | None = None,
    extra_columns: Sequence[str] = (),
) -> Any:
    """Search the nearest ``top_k`` rows to ``query`` and return a Daft DataFrame.

    The similarity filtering is executed by the LakeSoul vector index: each
    per-bucket reader returns its approximate top-``top_k`` candidates, and
    the candidates are re-ranked by exact vector distance in Daft before the
    global top-``top_k`` is returned.

    Args:
        source: A :class:`lakesoul.catalog.LakeSoulTable` or a configured
            :class:`lakesoul.catalog.LakeSoulScan`.  When a scan is passed,
            its partition filter, projection, and runtime options are kept
            except that the primary key and the searched vector column are
            always read.
        query: The query vector.  Must be a sequence of floats whose length
            matches the index dimension.
        top_k: Number of rows to return (global, across buckets).
        nprobe: Number of IVF clusters to probe in each bucket.
        column: Vector column to search.  Defaults to the single vector
            column declared in the table's ``vector_index_columns`` property.
        metric: Distance metric, ``"L2"`` or ``"IP"``.  Defaults to the
            table property.
        extra_columns: Additional columns to return alongside the primary
            key and the vector column.

    Returns:
        A lazy ``daft.DataFrame`` with the top-``top_k`` rows (nearest first),
        ordered by distance.  The distance column itself is not included;
        pass ``source.scan(columns=[...])`` and use ``extra_columns`` for
        the columns to project.

    Raises:
        ValueError: If the table has no vector index configuration, the
            searched column is not indexed, the query dimension does not
            match, or multiple indexed columns require an explicit ``column``.
    """
    from lakesoul.catalog import LakeSoulScan, LakeSoulTable

    if not isinstance(source, (LakeSoulTable, LakeSoulScan)):
        raise TypeError("source must be a LakeSoulTable or LakeSoulScan")
    table = source if isinstance(source, LakeSoulTable) else source.table

    if top_k < 1:
        raise ValueError("top_k must be >= 1")
    if nprobe < 1:
        raise ValueError("nprobe must be >= 1")
    if isinstance(query, str):
        raise TypeError("query must be a sequence of floats, not a string")

    configs = table._vector_configs()
    if not configs:
        raise ValueError(
            "the table has no vector_index_columns property; "
            "create the table with vector_index=[...] to enable vector search"
        )
    if column is None:
        if len(configs) == 1:
            column = configs[0]["column"]
        else:
            raise ValueError(
                "multiple vector columns are indexed; set 'column' explicitly"
            )
    cfg = next((c for c in configs if c["column"] == column), None)
    if cfg is None:
        raise ValueError(f"column {column!r} is not declared in vector_index_columns")
    if metric is None:
        metric = cfg.get("metric", "L2")

    query = [float(v) for v in query]
    if not query:
        raise ValueError("query must not be empty")
    expected_dim = cfg.get("dim")
    if expected_dim is not None and len(query) != expected_dim:
        raise ValueError(
            f"query dimension {len(query)} does not match the index "
            f"dimension {expected_dim}"
        )

    primary_keys = table.primary_keys
    if not primary_keys:
        raise ValueError("a vector index requires a table with a primary key")
    pk_column = primary_keys[0]

    metric = "IP" if metric.upper() in ("IP", "INNERPRODUCT") else "L2"
    columns = _dedup_columns((pk_column, column, *extra_columns))
    reader_options = {
        "vector_search_query": ",".join(f"{v:.6f}" for v in query),
        "vector_search_column": column,
        "vector_search_top_k": str(top_k),
        "vector_search_nprobe": str(nprobe),
        "vector_search_metric": metric,
    }

    if isinstance(source, LakeSoulScan):
        scan = source._replace(columns=tuple(columns), _reader_options=reader_options)
    else:
        scan = source.scan(columns=tuple(columns)).options(
            reader_options=reader_options
        )

    dataframe = scan.to_daft()
    dataframe = dataframe.with_column(
        _DISTANCE_COLUMN, _exact_distance_udf(col(column), query, metric)
    )
    dataframe = dataframe.sort(_DISTANCE_COLUMN).limit(top_k)
    return dataframe.exclude(_DISTANCE_COLUMN)


def _dedup_columns(names: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(names))


def _exact_distance(vec: Any, query: Any, metric: Any) -> float:
    if vec is None:
        return float("inf")
    if metric == "IP":
        return float(-sum(a * b for a, b in zip(vec, query)))
    return float(sum((a - b) * (a - b) for a, b in zip(vec, query)))


_exact_distance_udf = daft.func(return_dtype=daft.DataType.float32())(_exact_distance)
