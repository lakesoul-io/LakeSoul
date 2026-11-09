# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""LakeSoul query session: the in-process DataFusion engine for Python.

A :class:`LakeSoulSession` is a read-only query session over the LakeSoul
catalog.  Every vector search the Python package exposes is a query on this
session (``table.search(...)`` builds the same ``ORDER BY <distance> LIMIT k``
statement a user would write), so results follow the engine's lakehouse
semantics - snapshot isolation, merge-on-read (latest version, CDC delete
tombstones dropped), partition pruning - and use the vector-index fast paths.

The session also keeps the metadata/index/block caches warm, which matters
for repeated queries: create it once and reuse it.

The native binding is built only when the extension is built with the
``ci-unify`` feature (``maturin develop --features ci-unify``); without it this
class raises an informative :class:`ImportError` and the rest of the Python API
keeps working.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from lakesoul.adapters import OutputFormat, convert
from lakesoul.exceptions import LakeSoulError

try:  # pragma: no cover - exercised by the feature-on build
    from lakesoul._lib._session import NativeSession as _NativeSession
except ImportError:  # pragma: no cover - feature-off build
    _NativeSession = None  # type: ignore[assignment]

#: Session-level vector-search options accepted by ``set_vector_search``.
VECTOR_SEARCH_OPTIONS = frozenset(
    {"nprobe", "rerank", "parallelism", "deferred_payload", "exact_overfetch"}
)

_BUILD_HINT = (
    "the LakeSoul query session is not available in this build; rebuild the "
    "extension with `maturin develop --features ci-unify` (or install a wheel "
    "that includes the query session)"
)


class LakeSoulQueryError(LakeSoulError):
    """A query failed to plan or execute."""


@dataclass
class BatchSearchResult:
    """Result of :meth:`LakeSoulTable.search_batch`.

    Attributes:
        ids: One list of primary keys per query, nearest first.
        distances: Reserved for per-query distances; the current batch
            entries return the primary keys only (``None``).
    """

    ids: list[list[int]]
    distances: list[list[float]] | None = None

    def __len__(self) -> int:
        return len(self.ids)

    def to_arrow(self) -> pa.Table:
        """Return the result as ``(query_id, rank, id[, distance])`` rows."""
        query_ids: list[int] = []
        ranks: list[int] = []
        keys: list[int] = []
        scores: list[float] = []
        with_distance = self.distances is not None
        for query_id, query_ids_list in enumerate(self.ids):
            for rank, key in enumerate(query_ids_list):
                query_ids.append(query_id)
                ranks.append(rank)
                keys.append(int(key))
                if with_distance:
                    assert self.distances is not None
                    scores.append(float(self.distances[query_id][rank]))
        columns: dict[str, pa.Array] = {
            "query_id": pa.array(query_ids, type=pa.int64()),
            "rank": pa.array(ranks, type=pa.int64()),
            "id": pa.array(keys, type=pa.int64()),
        }
        if with_distance:
            columns["distance"] = pa.array(scores, type=pa.float32())
        return pa.table(columns)


def native_session_available() -> bool:
    """True when the extension was built with the query session."""
    return _NativeSession is not None


def _require_binding() -> type:
    if _NativeSession is None:  # pragma: no cover - feature-off build
        raise ImportError(_BUILD_HINT)
    return _NativeSession


def _normalize_queries(queries: Sequence[Sequence[float]]) -> list[list[float]]:
    normalized: list[list[float]] = []
    for index, query in enumerate(queries):
        values = [float(value) for value in query]
        if not values:
            raise ValueError(f"query {index} is empty")
        normalized.append(values)
    dimensions = {len(query) for query in normalized}
    if len(dimensions) > 1:
        raise ValueError(
            f"all queries must have the same dimension, got {sorted(dimensions)}"
        )
    return normalized


class LakeSoulSession:
    """Read-only DataFusion session over the LakeSoul catalog.

    Args:
        metadata_config: PostgreSQL connection string in the native
            ``host=... dbname=... user=... password=...`` form (the same value
            :class:`lakesoul.metadata.NativeMetadataClient` takes).  When
            omitted, ``LAKESOUL_PG_URL``/``LAKESOUL_PG_USERNAME``/
            ``LAKESOUL_PG_PASSWORD`` are used.
        secondary_config: Optional read-replica connection string.
        max_retry: Metadata statement retry budget.
        warehouse_prefix: Table-root prefix handed to the reader (usually
            unnecessary: tables carry their own path).
        target_partitions: DataFusion ``target_partitions`` (default 1).
        object_store_options: S3 options understood by the native reader
            (``fs.s3a.endpoint``/``bucket``/``access.key``/``secret.key``/
            ``path.style.access``).
        vector_search: Defaults for ``lakesoul_vector_search.*``:
            ``nprobe``, ``rerank`` (``"index"`` default / ``"exact"``),
            ``parallelism``, ``deferred_payload``, ``exact_overfetch``.
        distributed: Optional coordinator settings that route queries through
            the Rust distributed planner and its ``lakesoul-worker`` fleet:
            ``worker_urls`` (required list of ``grpc://host:port`` strings),
            ``target_partitions`` (default 8, must be >= 2),
            ``fallback_to_local`` (default ``False``) and
            ``bytes_per_partition``.  Without it the session plans every query
            single-node.  The worker processes must run the matching
            ``lakesoul-worker`` build (the protocol version is checked when the
            session is created).

    Example:
        >>> from lakesoul import LakeSoulSession
        >>> with LakeSoulSession.from_env(vector_search={"nprobe": 48}) as session:
        ...     rows = session.sql(
        ...         'SELECT id FROM "lakesoul".default.docs '
        ...         "ORDER BY array_distance(embedding, ARRAY[0.1, 0.2]) LIMIT 10"
        ...     )
    """

    def __init__(
        self,
        *,
        metadata_config: str | None = None,
        secondary_config: str | None = None,
        max_retry: int = 3,
        warehouse_prefix: str | None = None,
        target_partitions: int | None = None,
        object_store_options: Mapping[str, str] | None = None,
        vector_search: Mapping[str, Any] | None = None,
        distributed: Mapping[str, Any] | None = None,
    ) -> None:
        binding = _require_binding()
        config: dict[str, Any] = {}
        if warehouse_prefix is not None:
            config["warehouse_prefix"] = str(warehouse_prefix)
        if target_partitions is not None:
            config["target_partitions"] = int(target_partitions)
        if object_store_options:
            config["object_store_options"] = {
                str(key): str(value) for key, value in object_store_options.items()
            }
        if vector_search:
            self._validate_vector_search(vector_search)
            config["vector_search"] = dict(vector_search)
        if distributed is not None:
            config["distributed"] = self._validate_distributed(distributed)
        try:
            self._inner = binding(
                metadata_config,
                secondary_config,
                int(max_retry),
                config or None,
            )
        except Exception as error:  # noqa: BLE001 - re-raised as our error type
            raise LakeSoulQueryError(
                f"failed to create a LakeSoul query session: {error}"
            ) from error

    # -- construction helpers -------------------------------------------------

    @classmethod
    def from_env(cls, **kwargs: Any) -> "LakeSoulSession":
        """Create a session from the ``LAKESOUL_*`` environment variables."""
        return cls(**kwargs)

    @staticmethod
    def _validate_distributed(options: Mapping[str, Any]) -> dict[str, Any]:
        if not isinstance(options, Mapping):
            raise TypeError("distributed must be a mapping")
        allowed = {
            "worker_urls",
            "target_partitions",
            "fallback_to_local",
            "bytes_per_partition",
        }
        unknown = set(options) - allowed
        if unknown:
            raise ValueError(
                f"unknown distributed option(s) {sorted(unknown)}; "
                f"expected a subset of {sorted(allowed)}"
            )
        worker_urls = options.get("worker_urls")
        if (
            not isinstance(worker_urls, (list, tuple))
            or not worker_urls
            or not all(isinstance(url, str) and url for url in worker_urls)
        ):
            raise ValueError(
                "distributed.worker_urls must be a non-empty list of URL strings "
                "(for example 'grpc://worker1:50051')"
            )
        normalized: dict[str, Any] = {"worker_urls": [str(url) for url in worker_urls]}
        if "target_partitions" in options:
            target_partitions = int(options["target_partitions"])
            if target_partitions < 2:
                raise ValueError(
                    "distributed.target_partitions must be >= 2 "
                    "(a distributed session cannot run with one partition)"
                )
            normalized["target_partitions"] = target_partitions
        if "fallback_to_local" in options:
            normalized["fallback_to_local"] = bool(options["fallback_to_local"])
        if "bytes_per_partition" in options:
            bytes_per_partition = int(options["bytes_per_partition"])
            if bytes_per_partition <= 0:
                raise ValueError("distributed.bytes_per_partition must be > 0")
            normalized["bytes_per_partition"] = bytes_per_partition
        return normalized

    @staticmethod
    def _validate_vector_search(options: Mapping[str, Any]) -> None:
        unknown = set(options) - VECTOR_SEARCH_OPTIONS
        if unknown:
            raise ValueError(
                f"unknown vector_search option(s) {sorted(unknown)}; "
                f"expected a subset of {sorted(VECTOR_SEARCH_OPTIONS)}"
            )
        rerank = options.get("rerank")
        if rerank is not None and str(rerank).lower() not in ("exact", "index"):
            raise ValueError(
                f"vector_search rerank must be 'exact' or 'index', got {rerank!r}"
            )

    # -- query surface --------------------------------------------------------

    def sql(self, sql: str, *, to: OutputFormat | None = None) -> Any:
        """Execute one SQL statement and return the result.
        

        The statement runs on the LakeSoul-enabled DataFusion engine, so the
        full lakehouse query surface is available: filters, projections, joins,
        aggregates and time-travel-independent snapshot reads.  A vector search
        is just a statement of this form::

            SELECT id, title FROM "lakesoul".default.docs
            ORDER BY array_distance(embedding, ARRAY[...]) LIMIT 10

        The GIL is released while the query runs; the result is zero-copy
        Arrow.
        """
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        try:
            batches = self._inner.sql(sql)
        except Exception as error:  # noqa: BLE001
            raise LakeSoulQueryError(str(error)) from error
        return convert(pa.Table.from_batches(batches), to)

    def batch_search(
        self,
        table: str,
        queries: Sequence[Sequence[float]],
        *,
        namespace: str = "default",
        column: str,
        top_k: int = 10,
        nprobe: int = 64,
        parallelism: int = 1,
        batch_size: int = 0,
        exact: bool = False,
        exact_overfetch: int = 10,
        metric: str = "L2",
    ) -> BatchSearchResult:
        """Run the native snapshot batch entry: many queries, one snapshot.

        This is the fast batch path: every bucket's index shard is resolved
        once for the whole call and the ranking runs natively, with the same
        snapshot and merge-on-read semantics as the SQL path.  It returns one
        list of primary keys per query.

        The native entry supports a single Int64 primary key named ``id`` and
        defaults to index-only ordering (``exact=False``); ``exact=True``
        requires one data file per bucket and the `LAKESOUL_CACHE`
        decoded-block cache;
        :meth:`lakesoul.catalog.LakeSoulTable.search_batch` checks both and
        falls back to per-query SQL otherwise, so prefer the table method.
        """
        if queries is None or len(queries) == 0:
            return BatchSearchResult(ids=[])
        normalized = _normalize_queries(queries)
        try:
            ids = self._inner.vector_batch_search(
                str(table),
                str(namespace),
                str(column),
                normalized,
                int(top_k),
                int(nprobe),
                int(parallelism),
                int(batch_size),
                bool(exact),
                int(exact_overfetch),
                str(metric),
            )
        except Exception as error:  # noqa: BLE001
            raise LakeSoulQueryError(str(error)) from error
        return BatchSearchResult(ids=ids)

    def register_snapshot(
        self,
        table: str,
        *,
        namespace: str = "default",
        snapshot: int | None = None,
        tag: str | None = None,
        as_of: int | None = None,
    ) -> str:
        """Register a pinned (time-travel) view of a table and return its alias.

        Exactly one of ``snapshot`` (id from
        :meth:`lakesoul.catalog.LakeSoulCatalog.create_snapshot`), ``tag`` or
        ``as_of`` (Unix milliseconds) selects the point in time.  The alias is
        a queryable table name in this session::

            alias = session.register_snapshot("docs", snapshot=snapshot_id)
            try:
                session.sql(f"SELECT count(*) FROM {alias}")
            finally:
                session.unregister(alias)

        The view reads the partition versions frozen at that point with the
        regular merge-on-read semantics.  Its vector/text indexes are not used
        (they describe the latest files), so a vector ordering is still exact
        but runs as a plain scan.
        """
        try:
            return self._inner.register_snapshot_table(
                str(table),
                str(namespace),
                None if snapshot is None else int(snapshot),
                None if tag is None else str(tag),
                None if as_of is None else int(as_of),
            )
        except Exception as error:  # noqa: BLE001
            raise LakeSoulQueryError(str(error)) from error

    def unregister(self, name: str) -> None:
        """Drop a view registered by :meth:`register_snapshot`."""
        try:
            self._inner.unregister_table(str(name))
        except Exception as error:  # noqa: BLE001
            raise LakeSoulQueryError(str(error)) from error

    def explain(
        self, sql: str, *, verbose: bool = True, analyze: bool = False
    ) -> str:
        """Return ``EXPLAIN``/``EXPLAIN ANALYZE`` output for a statement.

        Use it to confirm that a vector search was pushed down: the plan
        contains ``LakeSoulVectorSearchExec(vec=..., metric=..., order_by=...)``.
        With ``analyze=True`` the statement actually runs (side-effect-free
        queries only).
        """
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        try:
            return self._inner.explain(sql, bool(verbose), bool(analyze))
        except Exception as error:  # noqa: BLE001
            raise LakeSoulQueryError(str(error)) from error

    def set_vector_search(self, **options: Any) -> None:
        """Update the session's ``lakesoul_vector_search.*`` options.

        Takes effect for the statements that follow on this session
        (``SET``-like semantics).  Recognised keys: ``nprobe``, ``rerank``,
        ``parallelism``, ``deferred_payload``, ``exact_overfetch``.
        """
        if not options:
            return
        self._validate_vector_search(options)
        try:
            self._inner.set_vector_search(dict(options))
        except Exception as error:  # noqa: BLE001
            raise LakeSoulQueryError(str(error)) from error

    @property
    def nprobe(self) -> int:
        """The session's current ``nprobe``."""
        return int(self._inner.nprobe)

    # -- lifecycle ------------------------------------------------------------

    def close(self) -> None:
        """Release the native session (caches are dropped with it)."""
        inner = getattr(self, "_inner", None)
        if inner is not None:
            inner.close()
            self._inner = None  # type: ignore[assignment]

    def __enter__(self) -> "LakeSoulSession":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        return "LakeSoulSession(<native query session>)"
