# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""LakeSoul vector index orchestration.

This module is the vector kind's thin layer on top of
:mod:`lakesoul.index`: it maps the vector configuration onto the native
IVF+RaBitQ builder and exposes the user-facing build entry points.

Usage::

    from lakesoul.vector_index import build_partition_vector_index

    build_partition_vector_index(
        table_name="my_table",
        namespace="default",
        partition_desc="range=2024-01-01",
        vector_column="embedding",
        dim=768,
        nlist=256,
        store_config={"type": "s3", "bucket": "...", ...},
    )
"""

from __future__ import annotations

from typing import Any

from . import index as _index
from .index import (
    IndexKindSpec,
    ShardInfo,
    build_partition_index,
    build_table_index,
    extract_bucket_id as _extract_bucket_id,
    group_files_by_shard as _group_files_by_shard,
    rename_summary_key as _rename_summary_key,
)

# Re-exported for compatibility with earlier imports.
__all__ = [
    "ShardInfo",
    "build_partition_vector_index",
    "build_table_vector_index",
    "rerank_by_distance",
]


def _build_shard(
    store_config: Any,
    file_paths: list[str],
    pk_column: str,
    config: Any,
    rebuild: bool,
) -> str:
    """Build one vector index shard through the native PyO3 binding."""
    from ._lib.vector import build_shard_vector_index, rebuild_shard_vector_index

    builder = rebuild_shard_vector_index if rebuild else build_shard_vector_index
    return builder(
        store_config=store_config,
        file_paths=list(file_paths),
        pk_column=pk_column,
        vector_column=config["column"],
        dim=config["dim"],
        nlist=config.get("nlist", 256),
        total_bits=config.get("total_bits", 7),
        metric=config.get("metric", "L2"),
        rotator_type=config.get("rotator_type", "FhtKac"),
        seed=config.get("seed", 42),
        use_faster_config=config.get("use_faster_config", True),
    )


def _validate(
    configs: Any,
    schema: Any,
    primary_keys: Any,
) -> None:
    from .catalog import _validate_vector_index_configs

    _validate_vector_index_configs(configs, schema, primary_keys)


def _parse(raw: str) -> list[dict[str, Any]]:
    from ._lib.vector import parse_vector_index_configs

    return list(parse_vector_index_configs(raw))


VECTOR_KIND_SPEC = IndexKindSpec(
    name="vector",
    property_key="vector_index_columns",
    parse_configs=_parse,
    validate=_validate,
    build_shard=_build_shard,
    required_params=("dim",),
)
_index.register_index_kind(VECTOR_KIND_SPEC)


def _vector_config(
    vector_column: str,
    dim: int,
    nlist: int,
    total_bits: int,
    metric: str,
    rotator_type: str,
    seed: int,
    use_faster_config: bool,
) -> dict[str, Any]:
    return {
        "column": vector_column,
        "dim": dim,
        "nlist": nlist,
        "total_bits": total_bits,
        "metric": metric,
        "rotator_type": rotator_type,
        "seed": seed,
        "use_faster_config": use_faster_config,
    }


def _as_vector_result(result: dict[str, Any]) -> dict[str, Any]:
    """Rename the generic ``column`` summary key for vector callers."""
    return _rename_summary_key(result, "vector_column")


def build_partition_vector_index(
    table_name: str,
    namespace: str,
    partition_desc: str,
    vector_column: str,
    dim: int,
    nlist: int = 256,
    total_bits: int = 7,
    metric: str = "L2",
    rotator_type: str = "FhtKac",
    seed: int = 42,
    use_faster_config: bool = True,
    store_config: dict[str, Any] | None = None,
    rebuild: bool = False,
) -> dict[str, Any]:
    """Build (or update, or rebuild) the vector index for a single partition
    of a table.

    This is the main entry point for background index building.  It:

    1. Looks up table metadata (schema, PK columns, table path) from PG.
    2. Queries the latest partition version's data files.
    3. Groups files by hash bucket.
    4. Calls the Rust IVF+RaBitQ builder for each (partition, bucket) shard.

    Args:
        table_name: LakeSoul table name.
        namespace: LakeSoul namespace (default ``"default"``).
        partition_desc: Partition descriptor string, e.g. ``"range=2024-01-01"``.
        vector_column: Name of the vector column in the Arrow schema.
        dim: Vector dimension.
        nlist: Number of IVF clusters (default 256).
        total_bits: RaBitQ total bits (default 7).
        metric: Distance metric, ``"L2"`` or ``"IP"`` (InnerProduct).
        rotator_type: Rotation type, ``"FhtKac"`` or ``"Matrix"``.
        seed: Random seed.
        use_faster_config: Enable fast quantization.
        store_config: Dict with S3/local storage credentials:
            ``{"type": "s3", "bucket": "...", "region": "...",
               "access_key_id": "...", "secret_access_key": "...", ...}``.
            If not provided, reads ``LAKESOUL_OBJECT_STORE_*`` env vars.
        rebuild: When true, every shard is rebuilt from scratch (fresh IVF
            k-means over all of the shard's active data files, published as
            a new index generation) instead of receiving an incremental
            delta update.  This is the way to re-train centroids after the
            data distribution has drifted.

    Returns:
        Dict with summary::

            {
              "status": "ok",
              "shards_total": 4,
              "shards_succeeded": 4,
              "table_path": "s3://bucket/table",
              "vector_column": "embedding",
              "partition_desc": "range=2024-01-01",
            }

    Raises:
        RuntimeError: If any shard's index build fails.
    """
    result = build_partition_index(
        "vector",
        table_name=table_name,
        namespace=namespace,
        partition_desc=partition_desc,
        config=_vector_config(
            vector_column,
            dim,
            nlist,
            total_bits,
            metric,
            rotator_type,
            seed,
            use_faster_config,
        ),
        store_config=store_config,
        rebuild=rebuild,
    )
    return _as_vector_result(result)


def build_table_vector_index(
    table_name: str,
    namespace: str,
    vector_column: str,
    dim: int,
    nlist: int = 256,
    total_bits: int = 7,
    metric: str = "L2",
    rotator_type: str = "FhtKac",
    seed: int = 42,
    use_faster_config: bool = True,
    store_config: dict[str, Any] | None = None,
    rebuild: bool = False,
) -> dict[str, Any]:
    """Build (or rebuild) vector index for ALL partitions of a table.

    Convenience wrapper around ``build_partition_vector_index`` that
    iterates over all existing partitions of the table.  When *rebuild* is
    true every shard is rebuilt from scratch (fresh k-means).

    Returns:
        Dict with per-partition results.
    """
    result = build_table_index(
        "vector",
        table_name=table_name,
        namespace=namespace,
        configs=[
            _vector_config(
                vector_column,
                dim,
                nlist,
                total_bits,
                metric,
                rotator_type,
                seed,
                use_faster_config,
            )
        ],
        store_config=store_config,
        rebuild=rebuild,
    )
    return _as_vector_result(result)


def rerank_by_distance(
    table: "pa.Table",
    query: "np.ndarray",
    vector_column: str,
    top_k: int,
    metric: str = "L2",
) -> "pa.Table":
    """Re-rank candidate rows by exact vector distance to the query.

    After per-bucket ANN search returns candidate rows, use this to
    compute exact distances and pick the true top-K.

    Args:
        table: Candidate table (must contain *vector_column*).
        query: Query vector, shape ``[D]``.
        vector_column: Name of the vector column in *table*.
        top_k: Number of rows to return.
        metric: ``"L2"`` (Euclidean) or ``"IP"`` (Inner Product).

    Returns:
        A ``pyarrow.Table`` with exactly ``min(top_k, len(table))`` rows,
        sorted by distance (ascending for L2, descending for IP).
    """
    import numpy as np
    import pyarrow as pa

    if top_k <= 0:
        return table.slice(0, 0)

    # Extract vectors as [N, D] numpy array
    vec_col = table.column(vector_column)
    n = len(vec_col)
    if n == 0:
        return table

    dim = len(vec_col[0].values)
    vectors = np.empty((n, dim), dtype=np.float32)
    for i in range(n):
        vectors[i] = vec_col[i].values.to_numpy()

    # Compute distances
    metric_lower = metric.upper()
    if metric_lower == "L2":
        diff = vectors - query.astype(np.float32)
        dists = np.sum(diff * diff, axis=1)
        top_indices = np.argsort(dists)[:top_k]
    elif metric_lower in ("IP", "INNER_PRODUCT", "COSINE"):
        # Inner product: larger = more similar
        dists = np.dot(vectors, query.astype(np.float32))
        top_indices = np.argsort(dists)[::-1][:top_k]
    else:
        raise ValueError(f"Unknown metric: {metric}")

    return table.take(pa.array(top_indices.tolist(), type=pa.int64()))
