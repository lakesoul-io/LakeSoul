# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""LakeSoul query session: the in-process DataFusion engine for Python.

A :class:`LakeSoulSession` is a read-only query session over the LakeSoul
catalog.  Every vector search the Python package exposes is a query on this
session (``table.search(...)`` builds the same ``ORDER BY <distance> LIMIT k``
statement a user would write), so results follow the engine's lakehouse
semantics - snapshot isolation, merge-on-read (latest version, CDC delete
tombstones dropped), partition pruning - and use the vector-index fast paths.

The session also keeps the metadata/index/payload caches warm, which matters
for repeated queries: create it once and reuse it.

The native binding is built only when the extension is built with the
``ci-unify`` feature (``maturin develop --features ci-unify``); without it this
class raises an informative :class:`ImportError` and the rest of the Python API
keeps working.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow as pa

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


def _require_binding() -> type:
    if _NativeSession is None:  # pragma: no cover - feature-off build
        raise ImportError(_BUILD_HINT)
    return _NativeSession


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
            ``nprobe``, ``rerank`` (``"exact"``/``"index"``),
            ``parallelism``, ``deferred_payload``, ``exact_overfetch``.

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

    def sql(self, sql: str) -> pa.Table:
        """Execute one SQL statement and return a :class:`pyarrow.Table`.

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
        return pa.Table.from_batches(batches)

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
