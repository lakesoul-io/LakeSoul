# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""Distributed index build for Daft writes.

After a Daft dataframe has been written through :class:`LakeSoulDataSink`
and committed, this module builds/updates the configured secondary indexes
in a distributed manner: the per-bucket new-file information is materialised
as a Daft dataframe (one row per ``(partition_desc, bucket, column)``
shard), repartitioned by shard count, and a ``@daft.cls`` class-UDF actor
pool invokes the generic :func:`lakesoul.index.build_shard` for each row.
Daft places those rows across its executors, so index builds run in
parallel across shards.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import daft
from daft import col, cls

from lakesoul.index import (
    build_shard,
    configured_index_kinds,
    default_object_store_config,
    find_index_kind_spec,
    shard_build_rows,
    table_index_configs,
)

_LOG = logging.getLogger(__name__)


def _is_lakesoul_table(table: Any) -> bool:
    """Return True if ``table`` is a usable :class:`LakeSoulTable`."""
    return all(
        hasattr(table, name) for name in ("properties", "primary_keys", "catalog")
    )


def build_index_daft(
    table: Any,
    result: Any,
    *,
    cpus: float = 1,
) -> None:
    """Build/update configured indexes for a freshly committed Daft write.

    This is a best-effort post-write step only when ``table`` is a real
    :class:`LakeSoulTable`.  In unit-test / distributed edge cases where a
    placeholder is passed (or the write produced no files) it is skipped
    gracefully, because the data write has already succeeded.

    Args:
        table: The :class:`lakesoul.catalog.LakeSoulTable` that was written to.
        result: The :class:`lakesoul.io.WriteResult` produced by the Daft sink.
        cpus: CPU budget per UDF invocation (per shard).

    Raises:
        RuntimeError: If any shard's index build fails.
    """
    if not _is_lakesoul_table(table):
        _LOG.warning(
            "Skipping index auto-build: %s is not a LakeSoul table",
            type(table).__name__,
        )
        return

    kinds = [
        kind
        for kind in configured_index_kinds(table.properties)
        if find_index_kind_spec(kind) is not None
    ]
    if not kinds:
        return
    file_infos = list(result.files)
    if not file_infos:
        return

    store_config_json = json.dumps(
        default_object_store_config(catalog=table.catalog, table=table)
    )
    primary_keys = table.primary_keys
    if not primary_keys:
        # write_lakesoul validates this pre-commit; direct callers get a
        # clear error here instead of an IndexError on primary_keys[0].
        raise ValueError(
            "an index requires an id column: the table has index properties "
            "but no primary key"
        )
    pk_column = primary_keys[0]

    rows: dict[str, list[Any]] = {
        "file_paths": [],
        "store_config_json": [],
        "pk_column": [],
        "kind": [],
        "config_json": [],
    }
    for kind in kinds:
        configs = table_index_configs(table, kind)
        if not configs:
            continue
        kind_rows, _ = shard_build_rows(
            kind, configs, file_infos, store_config_json, pk_column
        )
        for key in rows:
            rows[key].extend(kind_rows[key])

    n_shards = len(rows["file_paths"])
    if n_shards == 0:
        return

    df = daft.from_pydict(rows)

    # Distribute one shard per partition so the build UDF can run across
    # multiple executors (a no-op on the native runner; effective on the Ray
    # runner).  The UDF is a @daft.cls whose actor pool size matches the shard
    # count, so Daft can place each shard on a separate worker.
    df = df.into_partitions(max(1, n_shards))

    udf = cls(
        _BuildIndexShard,
        cpus=cpus,
        max_concurrency=max(1, n_shards),
    )
    df = df.with_column(
        "status",
        udf()(
            col("file_paths"),
            col("store_config_json"),
            col("pk_column"),
            col("kind"),
            col("config_json"),
        ),
    )
    statuses = [row["status"] for row in df.collect().to_pylist()]

    failures = [s for s in statuses if s != "ok"]
    if failures:
        raise RuntimeError(
            f"index build failed for {len(failures)}/{len(statuses)} "
            f"shard(s): {failures[:3]}{'...' if len(failures) > 3 else ''}"
        )


class _BuildIndexShard:
    """A per-row Daft class-UDF that builds one index shard.

    One invocation (one row = one partition_desc x bucket x column shard)
    runs the kind's native builder through :func:`lakesoul.index.build_shard`.
    Returned status is ``"ok"`` or an ``"error: ..."`` string; any failure
    raises from :func:`build_index_daft`.
    """

    def __call__(
        self,
        file_paths: Any,
        store_config_json: Any,
        pk_column: Any,
        kind: Any,
        config_json: Any,
    ) -> str:
        try:
            return build_shard(
                str(kind),
                json.loads(store_config_json),
                list(file_paths),
                str(pk_column),
                json.loads(config_json),
            )
        except Exception as error:  # noqa: BLE001 - surface as a row status
            return f"error: {error}"


# Kept for callers written before the generic entry point.
build_vector_index_daft = build_index_daft
