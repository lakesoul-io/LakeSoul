# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""LakeSoul vector index builder orchestration.

Given a table name, partition, and vector column configs, this module:
1. Queries PG metadata for partition info and data files
2. Groups files by (partition_desc, hash_bucket_id)
3. Calls the Rust ``build_shard_vector_index`` PyO3 binding per shard

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

import json
import re
import collections
from dataclasses import dataclass
from typing import Sequence

from .metadata.dao import (
    select_table_info_by_table_name,
    get_partition_info_by_table_id_and_desc,
    list_data_commit_info,
)

from .metadata.meta_ops import (
    get_table_info_by_name,
    get_partition_and_pk_cols,
    get_table_single_partition_data_info,
    get_all_partition_info,
)

from .metadata.const import DaoType
from .metadata.native_client import query
from ._lib.vector import build_shard_vector_index


@dataclass
class ShardInfo:
    """A set of files belonging to one (partition, bucket) shard."""
    partition_desc: str
    bucket_id: int
    file_paths: list[str]
    primary_keys: list[str]


def _extract_bucket_id(file_path: str) -> int:
    """Extract hash bucket id from a parquet file path.

    File names follow the pattern ``part-{random}_{bucket_id:0>4}.parquet``.
    """
    match = re.search(r".*_(\d+)(?:\..*)?$", file_path)
    if not match:
        raise ValueError(
            f"Cannot determine bucket id from file name {file_path}"
        )
    return int(match.group(1))


def _group_files_by_shard(
    table_id: str,
    partition_desc: str,
    pk_cols: list[str],
) -> list[ShardInfo]:
    """Query PG for a partition's current data files and group by bucket."""
    # Get the latest partition info
    partition_infos = get_partition_info_by_table_id_and_desc(
        table_id, partition_desc
    )
    if not partition_infos:
        return []

    # Find latest version
    latest = max(partition_infos, key=lambda p: p.version)

    # Collect all files, grouped by bucket
    bucket_files: dict[int, list[str]] = collections.defaultdict(list)
    data_commits = list_data_commit_info(
        latest.table_id, latest.partition_desc, latest.snapshot
    )
    for commit in data_commits:
        for file_op in commit.file_ops:
            if file_op.file_op == "add":
                bid = _extract_bucket_id(file_op.path)
                bucket_files[bid].append(file_op.path)

    return [
        ShardInfo(
            partition_desc=partition_desc,
            bucket_id=bid,
            file_paths=paths,
            primary_keys=pk_cols,
        )
        for bid, paths in sorted(bucket_files.items())
    ]


def build_partition_vector_index(
    table_name: str,
    namespace: str,
    partition_desc: str,
    vector_column: str,
    dim: int,
    nlist: int = 256,
    total_bits: int = 7,
    metric: str = "L2",
    store_config: dict | None = None,
) -> dict:
    """Build (or update) the vector index for a single partition of a table.

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
        store_config: Dict with S3/local storage credentials:
            ``{"type": "s3", "bucket": "...", "region": "...",
               "access_key_id": "...", "secret_access_key": "...", ...}``.
            If not provided, reads ``LAKESOUL_OBJECT_STORE_*`` env vars.

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
    store_config = store_config or _default_store_config()

    # 1. Get table metadata from PG
    table_info = get_table_info_by_name(table_name, namespace)
    _, pk_cols = get_partition_and_pk_cols(table_info)
    if not pk_cols:
        raise ValueError(
            f"Table '{table_name}' has no primary key columns defined. "
            f"Vector index requires a u64 primary key."
        )
    pk_column = pk_cols[0]  # Use first PK column as vector ID

    table_path = table_info.table_path

    # 2. Group files by (partition, bucket)
    shards = _group_files_by_shard(
        table_info.table_id, partition_desc, pk_cols
    )
    if not shards:
        return {
            "status": "ok",
            "shards_total": 0,
            "shards_succeeded": 0,
            "table_path": table_path,
            "vector_column": vector_column,
            "partition_desc": partition_desc,
            "message": "no data files found",
        }

    # 3. Build index for each shard
    succeeded = 0
    failed = 0
    for shard in shards:
        index_prefix = (
            f"{table_path}/_vector_index/{vector_column}/"
            f"{shard.partition_desc}/{shard.bucket_id}/"
        )
        try:
            result = build_shard_vector_index(
                store_config=store_config,
                file_paths=shard.file_paths,
                pk_column=pk_column,
                vector_column=vector_column,
                dim=dim,
                nlist=nlist,
                total_bits=total_bits,
                metric=metric,
                index_prefix=index_prefix,
            )
            if result == "ok":
                succeeded += 1
            else:
                failed += 1
        except Exception as e:
            print(f"ERROR building index for {index_prefix}: {e}")
            failed += 1

    if failed > 0:
        raise RuntimeError(
            f"Vector index build failed for {failed}/{len(shards)} shards "
            f"of partition '{partition_desc}'"
        )

    return {
        "status": "ok",
        "shards_total": len(shards),
        "shards_succeeded": succeeded,
        "table_path": table_path,
        "vector_column": vector_column,
        "partition_desc": partition_desc,
    }


def build_table_vector_index(
    table_name: str,
    namespace: str,
    vector_column: str,
    dim: int,
    nlist: int = 256,
    total_bits: int = 7,
    metric: str = "L2",
    store_config: dict | None = None,
) -> dict:
    """Build vector index for ALL partitions of a table.

    Convenience wrapper around ``build_partition_vector_index`` that
    iterates over all existing partitions of the table.

    Returns:
        Dict with per-partition results.
    """
    store_config = store_config or _default_store_config()
    table_info = get_table_info_by_name(table_name, namespace)
    partition_infos = get_all_partition_info(table_info.table_id)

    results = []
    for pinfo in partition_infos:
        result = build_partition_vector_index(
            table_name=table_name,
            namespace=namespace,
            partition_desc=pinfo.partition_desc,
            vector_column=vector_column,
            dim=dim,
            nlist=nlist,
            total_bits=total_bits,
            metric=metric,
            store_config=store_config,
        )
        results.append(result)

    return {
        "status": "ok",
        "table_name": table_name,
        "vector_column": vector_column,
        "partitions_total": len(results),
        "partitions_processed": sum(1 for r in results if r["status"] == "ok"),
        "details": results,
    }


def _default_store_config() -> dict:
    """Build store config from environment variables."""
    import os
    config: dict = {"type": "s3"}
    mapping = {
        "AWS_ACCESS_KEY_ID": "access_key_id",
        "AWS_SECRET_ACCESS_KEY": "secret_access_key",
        "AWS_REGION": "region",
        "AWS_ENDPOINT": "endpoint",
        "LAKESOUL_S3_BUCKET": "bucket",
        "LAKESOUL_S3_REGION": "region",
    }
    for env_key, config_key in mapping.items():
        val = os.environ.get(env_key)
        if val:
            config[config_key] = val
    return config
