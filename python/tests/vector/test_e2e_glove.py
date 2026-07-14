# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""E2E: glove-200d → LakeSoul catalog write → build_partition_vector_index → reader.

Validates the full LakeSoul vector index workflow:
1. Write vectors via LakeSoul Catalog (commits to PG)
2. Build vector index via user-facing build_partition_vector_index()
3. Read with vector search, verify filtered results

Usage (local, no PG):
    python tests/vector/test_e2e_glove.py

Usage (with PG, needs docker postgres + schema init):
    LAKESOUL_PG_URL=postgres://lakesoul_test:lakesoul_test@localhost:5432/lakesoul_test \
    python tests/vector/test_e2e_glove.py --use-catalog
"""

from __future__ import annotations

import argparse
import glob
import os
import struct
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa

DATA_DIR = "/home/chenxu/program/opensource/rabitq-rs/data/glove-200d/processed"
DIM = 200


def read_fvecs(path, n=None):
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


def _make_record_batch(train: np.ndarray, dim: int, id_start: int = 0) -> pa.RecordBatch:
    schema = pa.schema([
        pa.field("id", pa.uint64(), False),
        pa.field("vec", pa.list_(pa.field("item", pa.float32()), dim), False),
    ])
    ids = pa.array(range(id_start, id_start + len(train)), type=pa.uint64())
    vec_col = pa.FixedSizeListArray.from_arrays(
        pa.array(train.flatten(), type=pa.float32()), dim
    )
    return pa.RecordBatch.from_arrays([ids, vec_col], schema=schema)


def _compute_recall(query: np.ndarray, train: np.ndarray, pred_ids: list[int], k: int) -> float:
    """Compute recall@k: fraction of predicted IDs that are in the brute-force top-k."""
    # Brute-force L2 distances from query to all train vectors
    diff = train - query  # [N, D]
    dists = np.sum(diff * diff, axis=1)  # [N]
    true_top_k = set(np.argsort(dists)[:k].tolist())
    hits = sum(1 for pid in pred_ids if pid in true_top_k)
    return hits / k


def test_e2e_glove_local_writer():
    """Test with Writer directly (no PG needed). Uses build_shard_vector_index.

    Uses hash_bucket_num=4 to verify:
    1. Multiple buckets produce multiple indexes (one per bucket)
    2. Vector search uses multiple indexes and merges results
    """
    from collections import defaultdict

    from lakesoul.vector_index import _extract_bucket_id

    train = read_fvecs(f"{DATA_DIR}/train.fvecs", 500)
    test_vecs = read_fvecs(f"{DATA_DIR}/test.fvecs", 5)
    n_train, dim = train.shape
    schema = _make_record_batch(train, dim).schema

    from lakesoul.io import IOConfig, Writer

    tmp_dir = tempfile.mkdtemp(prefix="lakesoul_e2e_")
    config = IOConfig(
        path=f"file://{tmp_dir}", schema=schema,
        primary_keys=["id"], hash_bucket_num=4,
        batch_size=8192, thread_num=2,
    )
    with Writer(config) as writer:
        writer.write(_make_record_batch(train, dim))

    result = writer.result
    assert result is not None and len(result.files) > 0
    file_paths = [f.path for f in result.files]
    print(f"[1/6] Writer produced {len(file_paths)} file(s) for 4 buckets")

    # Group files by bucket_id and build index per bucket
    bucket_files: dict[int, list[str]] = defaultdict(list)
    for fp in file_paths:
        bid = _extract_bucket_id(fp)
        bucket_files[bid].append(fp)

    n_buckets = len(bucket_files)
    assert n_buckets >= 2, f"Expected ≥2 non-empty buckets with hash_bucket_num=4, got {n_buckets}"
    print(f"[2/6] Files grouped into {n_buckets} bucket(s): "
          f"{ {b: len(fs) for b, fs in sorted(bucket_files.items())} }")

    # Build index for each bucket independently
    from lakesoul._lib.vector import build_shard_vector_index

    for bid, bfiles in sorted(bucket_files.items()):
        r = build_shard_vector_index(
            store_config={"type": "local"}, file_paths=bfiles,
            pk_column="id", vector_column="vec", dim=dim,
            nlist=16, total_bits=7, metric="L2",
        )
        assert r == "ok", f"build_shard_vector_index failed for bucket {bid}"
    print(f"[3/6] Index built for all {n_buckets} bucket(s)")

    # Verify each bucket has its own index directory with LATEST
    for bid in bucket_files:
        idx_dir = f"{tmp_dir}/_vector_index/vec/-5/{bid}/"
        idx_files = glob.glob(f"{idx_dir}**", recursive=True)
        assert any("LATEST" in f for f in idx_files), \
            f"Bucket {bid}: missing LATEST in {idx_dir}"
        print(f"      Bucket {bid}: {len(idx_files)} index file(s) at {idx_dir}")
    print(f"[4/6] {n_buckets} independent index directories verified")

    # Read with vector search — one reader per bucket, merge + re-rank in Python
    from lakesoul._lib._reader import _sync_reader as sync_reader

    query_vec = test_vecs[0]
    top_k = 3
    options = [
        ("vector_search_column", "vec"),
        ("vector_search_query", ",".join(f"{v:.6f}" for v in query_vec)),
        ("vector_search_top_k", str(top_k)),
        ("vector_search_nprobe", "4"),
    ]

    # Each reader handles one bucket, searches its own index, returns top_k candidates
    all_batches = []
    for bid, bfiles in sorted(bucket_files.items()):
        reader = sync_reader(
            batch_size=8192, thread_num=2, schema=schema,
            file_urls=bfiles, primary_keys=["id"],
            partition_info=[], oss_conf=[],
            partition_schema=None, filter=None, options=options,
        )
        all_batches.extend(list(reader))

    # Combine all bucket results
    result_table = pa.Table.from_batches(all_batches) if all_batches else schema.empty_table()
    n_candidates = result_table.num_rows
    candidate_ids = result_table.column("id").to_pylist()
    print(f"[5/6] Vector search ({n_buckets} buckets × top_k={top_k}): "
          f"{n_candidates} candidates, IDs={candidate_ids}")

    assert n_candidates > 0, "No candidates from any bucket"

    # Re-rank by exact vector distance (merge logic outside the reader)
    from lakesoul.vector_index import rerank_by_distance

    result_table = rerank_by_distance(result_table, query_vec, "vec", top_k)
    final_ids = result_table.column("id").to_pylist()
    print(f"      After re-rank: top-{len(final_ids)} IDs={final_ids}")

    assert len(final_ids) == top_k
    assert all(0 <= i < n_train for i in final_ids)

    # Verify recall against brute-force ground truth
    recall = _compute_recall(query_vec, train, final_ids, k=top_k)
    print(f"[6/6] Recall@{top_k}: {recall:.2f}")
    assert recall >= 0.5, f"Recall@{top_k} should be ≥ 0.5, got {recall:.2f}"
    print("✓ Local test PASSED (multi-bucket)")


def test_e2e_glove_catalog():
    """Test with Catalog + build_partition_vector_index (needs PG).

    Uses hash_bucket_num=4 to verify:
    1. Multiple buckets produce multiple indexes
    2. Vector search uses multiple indexes and merges results
    3. Incremental write updates all bucket indexes

    Prerequisites:
        docker run -d --name lakesoul-pg \
            -e POSTGRES_PASSWORD=lakesoul_test -e POSTGRES_USER=lakesoul_test \
            -e POSTGRES_DB=lakesoul_test -p 5432:5432 \
            swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/postgres:14.5
        ./script/meta_init_for_local_test.sh -j 2
    """
    from lakesoul import LakeSoulCatalog

    cat = LakeSoulCatalog(pg_url=os.environ.get(
        "LAKESOUL_PG_URL",
        "postgresql://lakesoul_test:lakesoul_test@localhost:5432/lakesoul_test",
    ), pg_username="lakesoul_test", pg_password="lakesoul_test")
    train = read_fvecs(f"{DATA_DIR}/train.fvecs", 500)
    test_vecs = read_fvecs(f"{DATA_DIR}/test.fvecs", 5)
    n_train, dim = train.shape
    schema = _make_record_batch(train, dim).schema
    table_name = "glove200d_e2e_test"

    # 1. Create table + write via Catalog with 4 hash buckets
    table = cat.create_table(
        table_name, path=f"file:///tmp/lakesoul_test/{table_name}",
        schema=schema, primary_keys=["id"],
        hash_bucket_num=4,
        properties={"vector_index_columns": f"vec:{dim}:64:7:L2"},
    )
    table.write_arrow(
        _make_record_batch(train, dim),
        batch_size=8192, thread_num=2,
    )
    print(f"[1/7] Table created + {n_train} rows written (4 buckets)")

    # 2. Build index via table API — should process multiple shards
    result = table.build_vector_index(
        column="vec", dim=dim,
        nlist=16, total_bits=7, metric="L2",
    )
    assert result["status"] == "ok", f"Build failed: {result}"
    n_processed = result.get("partitions_processed", result.get("shards_succeeded", "?"))
    n_total = result.get("partitions_total", result.get("shards_total", "?"))
    details = result.get("details", [])
    n_shards = sum(d.get("shards_total", 0) for d in details) if details else n_total
    assert n_shards >= 2, f"Expected ≥2 shards (buckets), got {n_shards}"
    print(f"[2/7] Index built: {n_processed}/{n_total} partitions, "
          f"{n_shards} shard(s) total")

    # 3. Read via table.scan() with vector search — each per-bucket reader
    #    searches its own index. Merge + re-rank happens in Python.
    query_vec = test_vecs[0]
    top_k = 3

    ds = table.scan().options(reader_options={
        "vector_search_column": "vec",
        "vector_search_query": ",".join(f"{v:.6f}" for v in query_vec),
        "vector_search_top_k": str(top_k),
        "vector_search_nprobe": "4",
    }).to_arrow_dataset()
    result_table = ds.scanner().to_table()

    n_candidates = result_table.num_rows
    candidate_ids = result_table.column("id").to_pylist()
    print(f"[3/7] Vector search candidates ({n_shards} shards): "
          f"{n_candidates} rows, IDs={candidate_ids}")

    assert n_candidates > 0

    # Re-rank by exact vector distance
    from lakesoul.vector_index import rerank_by_distance

    result_table = rerank_by_distance(result_table, query_vec, "vec", top_k)
    final_ids = result_table.column("id").to_pylist()
    print(f"      After re-rank: top-{len(final_ids)} IDs={final_ids}")

    assert len(final_ids) == top_k
    assert all(0 <= i < n_train for i in final_ids)

    # 4. Verify recall against brute-force ground truth
    recall = _compute_recall(query_vec, train, final_ids, k=top_k)
    print(f"[4/7] Initial search: Recall@{top_k}={recall:.2f}")
    assert recall >= 0.5, f"Recall@{top_k} should be ≥ 0.5, got {recall:.2f}"

    # 5. Incremental write + index update (should update all bucket indexes)
    more_train = read_fvecs(f"{DATA_DIR}/train.fvecs", 700)[500:]  # IDs 500-699
    batch2 = _make_record_batch(more_train, dim, id_start=500)
    table.write_arrow(batch2, batch_size=8192, thread_num=2)
    print(f"[5/7] Incremental write: {len(more_train)} vectors "
          f"(IDs 500-{500+len(more_train)-1})")

    result2 = table.build_vector_index(
        column="vec", dim=dim,
        nlist=16, total_bits=7, metric="L2",
    )
    assert result2["status"] == "ok", f"Incremental build failed: {result2}"
    details2 = result2.get("details", [])
    n_shards2 = sum(d.get("shards_total", 0) for d in details2)
    print(f"[6/7] Incremental index updated ({n_shards2} shard(s))")

    # Search with a different query, verify new vectors are reachable
    query_vec2 = test_vecs[1]
    top_k2 = 5

    ds2 = table.scan().options(reader_options={
        "vector_search_column": "vec",
        "vector_search_query": ",".join(f"{v:.6f}" for v in query_vec2),
        "vector_search_top_k": str(top_k2),
        "vector_search_nprobe": "4",
    }).to_arrow_dataset()
    result2 = ds2.scanner().to_table()

    # Re-rank by exact distance
    from lakesoul.vector_index import rerank_by_distance

    result2 = rerank_by_distance(result2, query_vec2, "vec", top_k2)
    ids2 = result2.column("id").to_pylist()
    print(f"[7/7] After incremental update: top-{len(ids2)} IDs={ids2}")

    # Verify new vectors from the incremental batch are searchable
    assert any(i >= 500 for i in ids2), \
        f"No new vectors (ID >= 500) from incremental batch in results: {ids2}"
    print(f"         New IDs from incremental batch found: {[i for i in ids2 if i >= 500]}")
    print("✓ Catalog test PASSED (multi-bucket)")

    table.drop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--use-catalog", action="store_true", help="Use Catalog + PG")
    args = parser.parse_args()

    if args.use_catalog:
        test_e2e_glove_catalog()
    else:
        test_e2e_glove_local_writer()
