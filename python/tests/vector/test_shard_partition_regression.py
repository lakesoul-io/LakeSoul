# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Regression: auto-build keeps (partition_desc, hash_bucket_id) as the shard key.

A single write that produces files for multiple range partitions with the same
hash bucket id must build a *separate* vector index shard for each
``(partition_desc, bucket_id)`` combination.  Before the fix, the Arrow and
Daft auto-build paths grouped only by bucket id, so files from different range
partitions merged into one shard and the Rust builder wrote everything into the
first partition's index (leaving the other partition's index missing / empty on
scan).

Requires a live PostgreSQL metadata store.
"""

from __future__ import annotations

import os
import struct

import daft
import numpy as np
import pyarrow as pa
import pytest


DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DIM = 200
TRAIN_PATH = os.path.join(DATA_DIR, "train_700.fvecs")
DATES = ("2026-08-25", "2026-08-26")


def read_fvecs(path: str, n: int | None = None) -> np.ndarray:
    with open(path, "rb") as f:
        dim = struct.unpack("<i", f.read(4))[0]
        f.seek(0)
        vs = 4 + dim * 4
        n = min(n or os.path.getsize(path) // vs, os.path.getsize(path) // vs)
        data = np.zeros((n, dim), dtype=np.float32)
        for i in range(n):
            struct.unpack("<i", f.read(4))
            data[i] = struct.unpack(f"<{dim}f", f.read(dim * 4))
        return data


def _schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.uint64(), False),
            pa.field("dt", pa.string(), False),
            pa.field("vec", pa.list_(pa.field("item", pa.float32()), DIM), False),
        ]
    )


def _make_table(vectors: np.ndarray) -> pa.Table:
    schema = _schema()
    ids = pa.array(range(len(vectors)), type=pa.uint64())
    dts = pa.array([DATES[i % 2] for i in range(len(vectors))], type=pa.string())
    vec_col = pa.FixedSizeListArray.from_arrays(
        pa.array(vectors.flatten(), type=pa.float32()), DIM
    )
    return pa.Table.from_arrays([ids, dts, vec_col], schema=schema)


def _assert_per_partition_indexes(table_path: str, result) -> int:
    """Assert every (partition, bucket) produced by the write has its own index."""
    from lakesoul.vector_index import _extract_bucket_id

    shards = {}
    for fi in result.files:
        bucket = _extract_bucket_id(fi.path)
        shards[(fi.partition, bucket)] = fi.path

    partitions = {p for p, _ in shards}
    assert len(partitions) >= 2, (
        f"expected >=2 partitions in a single write, got {partitions} "
        f"(files: {[f.path for f in result.files]})"
    )

    missing = []
    local_path = (
        table_path.replace("file://", "").replace("s3://", "").replace("s3a://", "")
    )
    for partition, bucket in shards:
        # The index lives under the reader's per-partition prefix:
        #   {table}/{partition}/_vector_index/vec/-5/{bucket}  (partitioned)
        #   {table}/_vector_index/vec/-5/{bucket}              (non-partitioned)
        segments = [local_path]
        if partition != "-5":
            segments.append(partition)
        segments += ["_vector_index", "vec", "-5", str(bucket)]
        idx_dir = os.path.join(*segments)
        if not os.path.exists(os.path.join(idx_dir, "LATEST")):
            missing.append((partition, bucket, idx_dir))
    assert not missing, (
        f"missing per-(partition,bucket) indexes: {missing}. "
        "Every partition x bucket must have its own shard index."
    )
    return len(shards)


def _run_case(table, vectors: np.ndarray, write_call) -> int:
    tbl = _make_table(vectors)
    result = write_call(tbl)
    return _assert_per_partition_indexes(table.path, result)


def test_write_arrow_partition_shard_regression() -> None:
    from lakesoul import LakeSoulCatalog

    import shutil

    cat = LakeSoulCatalog(
        pg_url=os.environ.get(
            "LAKESOUL_PG_URL",
            "postgresql://lakesoul_test:lakesoul_test@localhost:5432/lakesoul_test",
        ),
        pg_username="lakesoul_test",
        pg_password="lakesoul_test",
    )
    vectors = read_fvecs(TRAIN_PATH, 80)
    table_name = "vec_partition_arrow_regression"
    table_path = f"/tmp/lakesoul_test/{table_name}"
    try:
        cat.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)

    table = cat.create_table(
        table_name,
        path=f"file://{table_path}",
        schema=_schema(),
        primary_keys=["id"],
        partition_by=["dt"],
        hash_bucket_num=4,
        vector_index=[
            {"column": "vec", "dim": DIM, "nlist": 8, "total_bits": 7, "metric": "L2"}
        ],
    )
    try:
        n_shards = _run_case(
            table,
            vectors,
            lambda tbl: table.write_arrow(
                tbl,
                batch_size=8192,
                thread_num=2,
            ),
        )
        print(f"write_arrow regression: {n_shards} per-(partition,bucket) shard(s)")
    finally:
        table.drop()


def test_write_daft_partition_shard_regression() -> None:
    from lakesoul import LakeSoulCatalog

    import shutil

    cat = LakeSoulCatalog(
        pg_url=os.environ.get(
            "LAKESOUL_PG_URL",
            "postgresql://lakesoul_test:lakesoul_test@localhost:5432/lakesoul_test",
        ),
        pg_username="lakesoul_test",
        pg_password="lakesoul_test",
    )
    vectors = read_fvecs(TRAIN_PATH, 80)
    table_name = "vec_partition_daft_regression"
    table_path = f"/tmp/lakesoul_test/{table_name}"
    try:
        cat.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)

    table = cat.create_table(
        table_name,
        path=f"file://{table_path}",
        schema=_schema(),
        primary_keys=["id"],
        partition_by=["dt"],
        hash_bucket_num=4,
        vector_index=[
            {"column": "vec", "dim": DIM, "nlist": 8, "total_bits": 7, "metric": "L2"}
        ],
    )
    try:
        n_shards = _run_case(
            table,
            vectors,
            lambda tbl: table.write_daft(
                daft.from_arrow(tbl),
                batch_size=8192,
                thread_num=2,
            ),
        )
        print(f"write_daft regression: {n_shards} per-(partition,bucket) shard(s)")
    finally:
        table.drop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
