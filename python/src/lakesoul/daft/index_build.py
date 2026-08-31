# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

"""Distributed vector-index build for Daft writes.

After a Daft dataframe has been written through :class:`LakeSoulDataSink`
and committed, this module builds/updates the configured vector indexes
in a distributed manner: the per-bucket new-file information is materialised
as a Daft dataframe (one row per ``(bucket, vector column)`` shard) and a
``@daft.func`` UDF invokes the native ``build_shard_vector_index`` for each
row.  Daft schedules those rows across its executors, so index builds run in
parallel across shards.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import daft
from daft import DataType, col, func

from lakesoul.vector_index import _extract_bucket_id

_LOG = logging.getLogger(__name__)


def _is_lakesoul_table(table: Any) -> bool:
    """Return True if ``table`` is a usable :class:`LakeSoulTable`."""
    return all(
        hasattr(table, name) for name in ("_vector_configs", "primary_keys", "catalog")
    )


def build_vector_index_daft(
    table: Any,
    result: Any,
    *,
    cpus: float = 1,
) -> None:
    """Build/update vector indexes for a freshly committed Daft write.

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
            "Skipping vector index auto-build: %s is not a LakeSoul table",
            type(table).__name__,
        )
        return

    configs = table._vector_configs()
    if not configs:
        return
    file_infos = list(result.files)
    if not file_infos:
        return

    from lakesoul.catalog import _default_object_store_config

    store_config_json = json.dumps(
        _default_object_store_config(catalog=table.catalog, table=table)
    )
    pk_column = table.primary_keys[0]

    # A LakeSoul vector shard is identified by (partition_desc, bucket_id).
    # Group on that key so files from different range partitions never share
    # a shard (the Rust builder derives the index location from the files'
    # parent directory and only uses the first one).
    shard_files: dict[tuple[str, int], list[str]] = {}
    for fi in file_infos:
        shard_files.setdefault(
            (fi.partition, _extract_bucket_id(fi.path)), []
        ).append(fi.path)

    rows: dict[str, list[Any]] = {
        "file_paths": [],
        "store_config_json": [],
        "pk_column": [],
        "column": [],
        "dim": [],
        "nlist": [],
        "total_bits": [],
        "metric": [],
        "rotator_type": [],
        "seed": [],
        "use_faster_config": [],
    }
    for _partition_desc, _bid in sorted(shard_files):
        bfiles = shard_files[(_partition_desc, _bid)]
        for cfg in configs:
            rows["file_paths"].append(sorted(bfiles))
            rows["store_config_json"].append(store_config_json)
            rows["pk_column"].append(pk_column)
            rows["column"].append(cfg["column"])
            rows["dim"].append(cfg["dim"])
            rows["nlist"].append(cfg.get("nlist", 256))
            rows["total_bits"].append(cfg.get("total_bits", 7))
            rows["metric"].append(cfg.get("metric", "L2"))
            rows["rotator_type"].append(cfg.get("rotator_type", "FhtKac"))
            rows["seed"].append(cfg.get("seed", 42))
            rows["use_faster_config"].append(cfg.get("use_faster_config", True))

    df = daft.from_pydict(rows)
    udf = func(
        _build_index_udf,
        return_dtype=DataType.string(),
        on_error="raise",
        cpus=cpus,
    )
    df = df.with_column(
        "status",
        udf(
            col("file_paths"),
            col("store_config_json"),
            col("pk_column"),
            col("column"),
            col("dim"),
            col("nlist"),
            col("total_bits"),
            col("metric"),
            col("rotator_type"),
            col("seed"),
            col("use_faster_config"),
        ),
    )
    statuses = [row["status"] for row in df.collect().to_pylist()]

    failures = [s for s in statuses if s != "ok"]
    if failures:
        raise RuntimeError(
            f"vector index build failed for {len(failures)}/{len(statuses)} "
            f"shard(s): {failures[:3]}{'...' if len(failures) > 3 else ''}"
        )


def _build_index_udf(
    file_paths: Any,
    store_config_json: Any,
    pk_column: Any,
    column: Any,
    dim: Any,
    nlist: Any,
    total_bits: Any,
    metric: Any,
    rotator_type: Any,
    seed: Any,
    use_faster_config: Any,
) -> str:
    """Per-row UDF: build one vector index shard (one partition x bucket of one column).

    Called once per row by Daft; ``file_paths`` is the list of newly written
    parquet files for that shard.
    """
    from lakesoul._lib.vector import build_shard_vector_index

    try:
        return build_shard_vector_index(
            store_config=json.loads(store_config_json),
            file_paths=list(file_paths),
            pk_column=str(pk_column),
            vector_column=str(column),
            dim=int(dim),
            nlist=int(nlist),
            total_bits=int(total_bits),
            metric=str(metric),
            rotator_type=str(rotator_type),
            seed=int(seed),
            use_faster_config=bool(use_faster_config),
        )
    except Exception as error:  # noqa: BLE001 - surface as a row status
        return f"error: {error}"
