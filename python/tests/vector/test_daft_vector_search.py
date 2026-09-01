# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Daft vector similarity search: index-filtered top-k via ``to_daft``.

Validates that a Daft query can use the vector index for similarity
top-k filtering:

- ``scan.options(reader_options={...}).to_daft()`` returns per-bucket
  candidates (the native reader filters each bucket by ``pk IN (...)``);
- ``lakesoul.daft.vector_search`` merges and re-ranks the candidates in
  Daft (exact distance UDF + global ``sort``/``limit``) and returns the
  global top-k without a distance column;
- metric/IP, multi-column, validation, and partition-pruning behavior.

Requires a live PostgreSQL metadata store.
"""

from __future__ import annotations

import os
import shutil
import struct

import numpy as np
import pyarrow as pa
import pytest


DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DIM = 200
TRAIN_PATH = os.path.join(DATA_DIR, "train_700.fvecs")
TEST_PATH = os.path.join(DATA_DIR, "test_5.fvecs")
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


def _vector_schema(*, partition_dt: bool = False) -> pa.Schema:
    fields = [pa.field("id", pa.uint64(), False)]
    if partition_dt:
        fields.append(pa.field("dt", pa.string(), False))
    fields.append(pa.field("vec", pa.list_(pa.field("item", pa.float32()), DIM), False))
    return pa.schema(fields)


def _make_batch(vectors: np.ndarray, *, partition_dt: bool = False) -> pa.RecordBatch:
    schema = _vector_schema(partition_dt=partition_dt)
    arrays = [pa.array(range(len(vectors)), type=pa.uint64())]
    if partition_dt:
        arrays.append(
            pa.array([DATES[i % 2] for i in range(len(vectors))], type=pa.string())
        )
    vec_col = pa.FixedSizeListArray.from_arrays(
        pa.array(vectors.flatten(), type=pa.float32()), DIM
    )
    arrays.append(vec_col)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def _brute_force_topk(
    vectors: np.ndarray, query: np.ndarray, k: int, metric: str
) -> list[int]:
    if metric == "IP":
        scores = vectors @ query
        return np.argsort(scores)[-k:][::-1].tolist()
    dists = np.sum((vectors - query) ** 2, axis=1)
    return np.argsort(dists)[:k].tolist()


def _catalog():
    from lakesoul import LakeSoulCatalog

    return LakeSoulCatalog(
        pg_url=os.environ.get(
            "LAKESOUL_PG_URL",
            "postgresql://lakesoul_test:lakesoul_test@localhost:5432/lakesoul_test",
        ),
        pg_username="lakesoul_test",
        pg_password="lakesoul_test",
    )


def _create_and_write(
    vectors: np.ndarray,
    *,
    metric: str = "L2",
    partition_dt: bool = False,
) -> tuple[object, object, str]:
    cat = _catalog()
    table_name = f"vec_daft_search_{metric.lower()}"
    if partition_dt:
        table_name += "_part"
    table_path = f"/tmp/lakesoul_test/{table_name}"
    try:
        cat.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)

    table = cat.create_table(
        table_name,
        path=f"file://{table_path}",
        schema=_vector_schema(partition_dt=partition_dt),
        primary_keys=["id"],
        partition_by=["dt"] if partition_dt else (),
        hash_bucket_num=4,
        vector_index=[
            {"column": "vec", "dim": DIM, "nlist": 8, "total_bits": 7, "metric": metric}
        ],
    )
    table.write_arrow(_make_batch(vectors, partition_dt=partition_dt))
    return cat, table, table_path


def test_daft_vector_search_candidates() -> None:
    """to_daft() + reader_options returns per-bucket ANN candidates."""
    train = read_fvecs(TRAIN_PATH, 300)
    query = read_fvecs(TEST_PATH, 1)[0]
    _, table, table_path = _create_and_write(train)
    try:
        candidates = (
            table.scan()
            .options(
                reader_options={
                    "vector_search_query": ",".join(f"{v:.6f}" for v in query),
                    "vector_search_top_k": "3",
                    "vector_search_nprobe": "8",
                }
            )
            .to_daft()
            .collect()
            .to_arrow()
        )
        ids = candidates.column("id").to_pylist()
        assert len(ids) > 0, "expected at least one candidate row"
        assert len(ids) <= 12, f"expected <= 4 buckets x 3 candidates, got {len(ids)}"
        assert all(0 <= i < 300 for i in ids)
        assert "vec" in candidates.column_names
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_daft_vector_search_global_topk() -> None:
    """vector_search returns the exact global top-k, nearest first, no distance col."""
    from lakesoul.daft import vector_search

    train = read_fvecs(TRAIN_PATH, 300)
    query = read_fvecs(TEST_PATH, 1)[0]
    top_k = 3
    _, table, table_path = _create_and_write(train)
    try:
        result = vector_search(table, query.tolist(), top_k=top_k, nprobe=8)
        collected = result.collect().to_arrow()
        assert collected.column_names == ["id", "vec"], collected.column_names
        ids = collected.column("id").to_pylist()
        assert len(ids) == top_k

        truth = _brute_force_topk(train, query, top_k, "L2")
        assert ids[0] == truth[0], f"nearest {ids[0]} != truth {truth[0]}"
        recall = len(set(ids) & set(truth)) / top_k
        assert recall >= 0.5, f"Recall@{top_k} too low: {recall:.2f}"
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_daft_vector_search_ip() -> None:
    """IP metric: highest dot product first."""
    from lakesoul.daft import vector_search

    train = read_fvecs(TRAIN_PATH, 300)
    query = read_fvecs(TEST_PATH, 2)[1]
    top_k = 1
    _, table, table_path = _create_and_write(train, metric="IP")
    try:
        result = vector_search(table, query.tolist(), top_k=top_k, nprobe=8)
        ids = result.collect().to_arrow().column("id").to_pylist()
        truth = _brute_force_topk(train, query, top_k, "IP")
        assert ids == truth, f"IP top-1 {ids} != truth {truth}"
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_daft_vector_search_scan_partition_pruning() -> None:
    """A LakeSoulScan restricts the search to its range partitions."""
    from lakesoul.daft import vector_search

    train = read_fvecs(TRAIN_PATH, 120)
    query = read_fvecs(TEST_PATH, 1)[0]
    _, table, table_path = _create_and_write(train, partition_dt=True)
    try:
        scan = table.scan(partitions={"dt": DATES[0]}).options(reader_options={})
        result = vector_search(
            scan, query.tolist(), top_k=5, nprobe=8, extra_columns=["dt"]
        )
        collected = result.collect().to_arrow()
        dts = collected.column("dt").to_pylist()
        ids = collected.column("id").to_pylist()
        assert len(ids) > 0
        assert all(d == DATES[0] for d in dts), f"rows outside the partition: {dts}"
        assert all(i % 2 == 0 for i in ids), "rows must belong to the dt partition"
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_daft_vector_search_validation() -> None:
    from lakesoul.daft import vector_search

    train = read_fvecs(TRAIN_PATH, 60)
    query = read_fvecs(TEST_PATH, 1)[0]
    cat = _catalog()

    table_name = "vec_daft_search_noidx"
    table_path = f"/tmp/lakesoul_test/{table_name}"
    try:
        cat.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)
    no_index = cat.create_table(
        table_name,
        path=f"file://{table_path}",
        schema=_vector_schema(),
        primary_keys=["id"],
        hash_bucket_num=4,
    )
    try:
        with pytest.raises(ValueError, match="vector_index_columns"):
            vector_search(no_index, query.tolist())
    finally:
        no_index.drop()
        shutil.rmtree(table_path, ignore_errors=True)

    _, table, table_path = _create_and_write(train)
    try:
        with pytest.raises(ValueError, match="dimension"):
            vector_search(table, [0.0] * (DIM + 1))
        with pytest.raises(ValueError, match="not declared"):
            vector_search(table, query.tolist(), column="other")
        with pytest.raises(TypeError, match="LakeSoulTable"):
            vector_search(object(), query.tolist())
        with pytest.raises(TypeError, match="floats"):
            vector_search(table, "0.1,0.2")
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_daft_vector_search_multiple_columns() -> None:
    """Multiple indexed columns require an explicit column."""
    from lakesoul.daft import vector_search

    train = read_fvecs(TRAIN_PATH, 60)
    query = read_fvecs(TEST_PATH, 1)[0]
    cat = _catalog()
    table_name = "vec_daft_search_multi"
    table_path = f"/tmp/lakesoul_test/{table_name}"
    try:
        cat.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)

    schema = pa.schema(
        [
            pa.field("id", pa.uint64(), False),
            pa.field("vec_a", pa.list_(pa.field("item", pa.float32()), DIM), False),
            pa.field("vec_b", pa.list_(pa.field("item", pa.float32()), DIM), False),
        ]
    )
    vec_col = pa.FixedSizeListArray.from_arrays(
        pa.array(train.flatten(), type=pa.float32()), DIM
    )
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(range(len(train)), type=pa.uint64()),
            vec_col,
            vec_col,
        ],
        schema=schema,
    )
    table = cat.create_table(
        table_name,
        path=f"file://{table_path}",
        schema=schema,
        primary_keys=["id"],
        hash_bucket_num=2,
        vector_index=[
            {
                "column": "vec_a",
                "dim": DIM,
                "nlist": 4,
                "total_bits": 7,
                "metric": "L2",
            },
            {
                "column": "vec_b",
                "dim": DIM,
                "nlist": 4,
                "total_bits": 7,
                "metric": "L2",
            },
        ],
    )
    table.write_arrow(batch)
    try:
        with pytest.raises(ValueError, match="explicitly"):
            vector_search(table, query.tolist())
        ids = (
            vector_search(table, query.tolist(), column="vec_b", top_k=3, nprobe=4)
            .collect()
            .to_arrow()
            .column("id")
            .to_pylist()
        )
        truth = _brute_force_topk(train, query, 3, "L2")
        assert ids[0] == truth[0]
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
