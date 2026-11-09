# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""Distributed vector search: driver-side merge over range partitions.

A LakeSoul table's range partitions are independent work units - the engine
already reads them in parallel inside one process.  This module fans those work
units out to several workers (threads by default, Ray/Dask when installed) and
merges their local top-k on the driver:

1. resolve the distinct partition values once (a tiny SQL query);
2. split them over the workers, giving each worker the user's filter AND its
   partitions' predicate;
3. every worker runs the ordinary ``table.search`` on its own session (same
   snapshot semantics: the merge-on-read view, latest version per key, CDC
   tombstones dropped), fetching ``top_k`` neighbours per partition;
4. the driver de-duplicates by primary key and keeps the global ``top_k``.

The merge happens on the driver, so the lakehouse contract is untouched: no
worker writes, nothing is re-sharded, and every worker reads the same table
snapshot through the regular scan.  SQL statements are still planned
single-node on the driver (routing them to the Rust distributed coordinator is
a later stage of the Python API design).
"""

from __future__ import annotations

import concurrent.futures
import json
import os
import threading
from dataclasses import dataclass
from typing import Any, Literal

import pyarrow as pa

from lakesoul.adapters import OutputFormat, convert
from lakesoul.exceptions import LakeSoulError
from lakesoul.session import LakeSoulSession

Engine = Literal["threads", "ray", "dask"]

#: Session cache used by the thread engine: one session per worker keeps the
#: metadata/index/block caches warm across queries.
_SESSION_CACHE: dict[str, LakeSoulSession] = {}
_SESSION_CACHE_LOCK = threading.Lock()


class DistributedSearchError(LakeSoulError):
    """A distributed search failed on one of its workers."""


@dataclass
class _WorkerSpec:
    """What one worker has to search: the SQL and the session settings."""

    sql: str
    metadata_config: str | None
    object_store_options: dict[str, str]
    vector_search: dict[str, Any]


def _session_key(spec: _WorkerSpec) -> str:
    return json.dumps(
        {
            "metadata_config": spec.metadata_config,
            "object_store_options": sorted(spec.object_store_options.items()),
            "vector_search": sorted(spec.vector_search.items()),
        },
        sort_keys=True,
    )


def _worker_session(spec: _WorkerSpec) -> LakeSoulSession:
    key = _session_key(spec)
    with _SESSION_CACHE_LOCK:
        session = _SESSION_CACHE.get(key)
    if session is not None:
        return session
    session = LakeSoulSession(
        metadata_config=spec.metadata_config,
        object_store_options=spec.object_store_options or None,
        vector_search=spec.vector_search or None,
    )
    with _SESSION_CACHE_LOCK:
        _SESSION_CACHE.setdefault(key, session)
        return _SESSION_CACHE[key]


def _run_partition_search(spec: _WorkerSpec) -> pa.Table:
    """Search one worker's partitions and return its local top-k.

    The driver hands over the fully built SQL (query vector, projection,
    partition predicate and the user's filter); the worker only runs it on its
    own session.  Module-level so Ray/Dask can pickle it.
    """
    session = _worker_session(spec)
    result = session.sql(spec.sql)
    if not isinstance(result, pa.Table):
        raise DistributedSearchError(
            "a worker returned a non-Arrow result; pass to=None"
        )
    return result


class DistributedSearch:
    """Distributed vector search over a table's range partitions.

    Args:
        table: The :class:`lakesoul.catalog.LakeSoulTable` to search.
        engine: ``"threads"`` (default, no extra dependency), ``"ray"`` or
            ``"dask"``.
        num_workers: Parallel workers; each gets a share of the partitions.
        session: Driver session (used for partition discovery and the
            single-node fallback); defaults to the catalog's session.
    """

    def __init__(
        self,
        table: Any,
        *,
        engine: Engine = "threads",
        num_workers: int = 4,
        session: LakeSoulSession | None = None,
    ) -> None:
        if engine not in ("threads", "ray", "dask"):
            raise ValueError(f"engine must be 'threads', 'ray' or 'dask', got {engine!r}")
        if int(num_workers) < 1:
            raise ValueError("num_workers must be >= 1")
        self._table = table
        self._engine: Engine = engine
        self._num_workers = int(num_workers)
        self._driver_session = table._search_session(session)

    # -- helpers -------------------------------------------------------------

    @property
    def partition_columns(self) -> tuple[str, ...]:
        return tuple(self._table.partition_by)

    @property
    def is_distributed(self) -> bool:
        """True when the table has range partitions to fan out."""
        return bool(self.partition_columns)

    def _sql_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (bytes, bytearray)):
            return "X'" + bytes(value).hex() + "'"
        text = str(value).replace("'", "''")
        return f"'{text}'"

    def _partition_predicates(self) -> list[str]:
        """One predicate per distinct partition (empty list = no partitions)."""
        columns = self.partition_columns
        if not columns:
            return []
        # Partition values are not stale metadata: they come from a scan of the
        # table itself, so a worker never searches a partition that is gone.
        quoted = ", ".join(self._table._sql_identifier(column) for column in columns)
        table_ref = (
            f'"lakesoul".{self._table._sql_identifier(self._table.namespace)}.'
            f"{self._table._sql_identifier(self._table.name)}"
        )
        rows = self._driver_session.sql(f"SELECT DISTINCT {quoted} FROM {table_ref}")
        if rows.num_rows == 0:
            return []
        values = [rows.column(column).to_pylist() for column in columns]
        predicates: list[str] = []
        for position in range(rows.num_rows):
            parts = [
                f"{self._table._sql_identifier(column)} = {self._sql_literal(values[index][position])}"
                if values[index][position] is not None
                else f"{self._table._sql_identifier(column)} IS NULL"
                for index, column in enumerate(columns)
            ]
            predicates.append(" AND ".join(parts))
        return predicates

    def _worker_spec(
        self,
        predicates: list[str],
        *,
        query: list[float],
        top_k: int,
        columns: list[str],
        metric: str | None,
        rerank: str,
        nprobe: int | None,
        exact_overfetch: int | None,
        partition_overfetch: int,
        filter: str | None,
    ) -> _WorkerSpec:
        partition_clause = " OR ".join(f"({predicate})" for predicate in predicates)
        combined = f"({partition_clause})" if partition_clause else None
        if filter:
            combined = f"({combined}) AND ({filter})" if combined else str(filter)
        table = self._table
        vector_search: dict[str, Any] = {}
        if rerank in ("exact", "index"):
            vector_search["rerank"] = rerank
        if nprobe is not None:
            vector_search["nprobe"] = int(nprobe)
        if exact_overfetch is not None:
            vector_search["exact_overfetch"] = int(exact_overfetch)
        catalog = table.catalog
        sql = table.to_sql(
            query,
            top_k=max(top_k, 1) * max(partition_overfetch, 1),
            metric=metric,
            filter=combined,
            columns=columns,
            with_distance=True,
        )
        return _WorkerSpec(
            sql=sql,
            metadata_config=getattr(catalog, "_metadata_config", None),
            object_store_options=dict(catalog.object_store_options),
            vector_search=vector_search,
        )

    def _run(self, specs: list[_WorkerSpec]) -> list[pa.Table]:
        if self._engine == "threads":
            if len(specs) == 1:
                return [_run_partition_search(specs[0])]
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=min(self._num_workers, len(specs))
            ) as executor:
                futures = [
                    executor.submit(_run_partition_search, spec) for spec in specs
                ]
                return [future.result() for future in futures]
        if self._engine == "ray":
            try:
                import ray
            except ImportError as error:  # pragma: no cover - env dependent
                raise ImportError(
                    "ray is required for engine='ray'; install it with "
                    "`pip install 'lakesoul[ray]'`"
                ) from error
            remote = ray.remote(_run_partition_search)
            return list(ray.get([remote.remote(spec) for spec in specs]))
        try:
            import dask
        except ImportError as error:  # pragma: no cover - env dependent
            raise ImportError(
                "dask is required for engine='dask'; install it with "
                "`pip install dask`"
            ) from error
        delayed = [dask.delayed(_run_partition_search)(spec) for spec in specs]
        return list(dask.compute(*delayed))

    def _merge(
        self,
        results: list[pa.Table],
        *,
        top_k: int,
        final_columns: list[str],
        pk_column: str,
        metric: str,
        with_distance: bool,
        schema_source: pa.Table,
    ) -> pa.Table:
        descending = metric.upper() != "L2"
        best: dict[Any, tuple[float, dict[str, Any]]] = {}
        for result in results:
            if result.num_rows == 0:
                continue
            rows = {name: result.column(name).to_pylist() for name in result.column_names}
            if "__lakesoul_vector_distance" not in rows:
                raise DistributedSearchError(
                    "a worker did not return its distance column"
                )
            distances = rows["__lakesoul_vector_distance"]
            for position in range(result.num_rows):
                key = rows[pk_column][position]
                distance = float(distances[position])
                previous = best.get(key)
                if previous is None or (
                    distance > previous[0] if descending else distance < previous[0]
                ):
                    best[key] = (
                        distance,
                        {
                            name: rows[name][position]
                            for name in final_columns
                            if name in rows
                        },
                    )
        ordered = sorted(
            best.values(), key=lambda item: item[0], reverse=descending
        )[:top_k]
        columns: dict[str, Any] = {}
        source_schema = {
            field.name: field for field in schema_source.schema
        }
        for name in final_columns:
            if name not in source_schema:
                raise DistributedSearchError(
                    f"column {name!r} was not returned by the workers"
                )
            columns[name] = pa.array(
                [row[name] for _, row in ordered],
                type=source_schema[name].type,
            )
        if with_distance:
            columns["__lakesoul_vector_distance"] = pa.array(
                [distance for distance, _ in ordered], type=pa.float32()
            )
        return pa.Table.from_pydict(columns)

    # -- public API ----------------------------------------------------------

    def search(
        self,
        query: Any,
        *,
        top_k: int = 10,
        filter: str | None = None,
        columns: Any = None,
        metric: str | None = None,
        rerank: str = "auto",
        nprobe: int | None = None,
        exact_overfetch: int | None = None,
        with_distance: bool = False,
        partition_overfetch: int = 2,
    ) -> Any:
        """Search every partition in parallel and merge the local top-k.

        Arguments mirror :meth:`LakeSoulTable.search`; ``partition_overfetch``
        fetches ``top_k * partition_overfetch`` neighbours per partition as
        merge margin.  The result is ordered by distance (L2 ascending, IP
        descending) and identical in shape to the single-node search.
        """
        query_values = [float(value) for value in query]
        if not query_values:
            raise ValueError("query must not be empty")
        if int(top_k) < 1:
            raise ValueError("top_k must be >= 1")
        if rerank not in ("auto", "exact", "index"):
            raise ValueError(
                f"rerank must be 'auto', 'exact' or 'index', got {rerank!r}"
            )
        if int(partition_overfetch) < 1:
            raise ValueError("partition_overfetch must be >= 1")
        table = self._table
        _, resolved_column, resolved_metric = table._vector_search_sql(
            query_values, column=None, top_k=10, metric=metric
        )
        metric_name = resolved_metric
        # The distance column orders the merge, so every worker asks for it.
        pk_column = table.primary_keys[0] if table.primary_keys else "id"
        if columns is None:
            final_columns = [
                field.name
                for field in table.schema
                if field.name != resolved_column
            ]
        else:
            final_columns = [str(name) for name in columns]
            unknown = [name for name in final_columns if name not in table.schema.names]
            if unknown:
                raise ValueError(f"unknown column(s) in columns: {unknown}")
        if pk_column not in final_columns:
            worker_columns = [pk_column, *final_columns]
        else:
            worker_columns = list(final_columns)

        predicates = self._partition_predicates()
        if not predicates:
            # No range partitions to fan out: a plain single-node search keeps
            # the semantics (and is the right thing for a small table).
            return table.search(
                query,
                top_k=top_k,
                metric=metric,
                rerank="exact" if rerank == "auto" else rerank,
                nprobe=nprobe,
                exact_overfetch=exact_overfetch,
                filter=filter,
                columns=final_columns,
                with_distance=with_distance,
                session=self._driver_session,
            )
        workers = min(self._num_workers, len(predicates))
        groups: list[list[str]] = [[] for _ in range(workers)]
        for position, predicate in enumerate(predicates):
            groups[position % workers].append(predicate)
        specs = [
            self._worker_spec(
                group,
                query=query_values,
                top_k=top_k,
                columns=worker_columns,
                metric=metric,
                rerank="exact" if rerank == "auto" else rerank,
                nprobe=nprobe,
                exact_overfetch=exact_overfetch,
                partition_overfetch=partition_overfetch,
                filter=filter,
            )
            for group in groups
            if group
        ]
        results = self._run(specs)
        reference = self._driver_session.sql(
            f'SELECT * FROM "lakesoul".{table._sql_identifier(table.namespace)}.'
            f"{table._sql_identifier(table.name)} LIMIT 0"
        )
        merged = self._merge(
            results,
            top_k=int(top_k),
            final_columns=final_columns,
            pk_column=pk_column,
            metric=metric_name,
            with_distance=with_distance,
            schema_source=reference,
        )
        return convert(merged, None)

    def sql(self, sql: str, *, to: OutputFormat | None = None) -> Any:
        """Run a lakehouse SQL statement (planned on the driver for now).

        Routing SQL through the Rust distributed coordinator (worker sessions
        with ``lakesoul-worker``) is the next stage of the design; until then
        the statement runs single-node on the driver, which is semantically
        identical but not distributed.
        """
        return convert(self._driver_session.sql(sql), to)
