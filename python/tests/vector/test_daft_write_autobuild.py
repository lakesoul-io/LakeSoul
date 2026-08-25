# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""E2E: Daft distributed write -> write_daft auto-builds the vector index.

Validates that ``table.write_daft`` (and ``LakeSoulDataTable.append``)
automatically builds/updates the IVF+RaBitQ index from the table's
``vector_index_columns`` property, using the distributed @daft.func path.

Requires a live PostgreSQL metadata store:
    docker run -d --name lakesoul-pg \
        -e POSTGRES_PASSWORD=lakesoul_test -e POSTGRES_USER=lakesoul_test \
        -e POSTGRES_DB=lakesoul_test -p 5432:5432 \
        swr.cn-southwest-2.myhuaweicloud.com/dmetasoul-repo/postgres:14.5
    ./script/meta_init_for_local_test.sh -j 2
"""

from __future__ import annotations

import glob
import os
import shutil
import struct

import daft
import numpy as np
import pyarrow as pa


DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
DIM = 200
TRAIN_PATH = os.path.join(DATA_DIR, "train_700.fvecs")
TEST_PATH = os.path.join(DATA_DIR, "test_5.fvecs")


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


def _make_record_batch(
    train: np.ndarray, dim: int, id_start: int = 0
) -> pa.RecordBatch:
    schema = pa.schema(
        [
            pa.field("id", pa.uint64(), False),
            pa.field("vec", pa.list_(pa.field("item", pa.float32()), dim), False),
        ]
    )
    ids = pa.array(range(id_start, id_start + len(train)), type=pa.uint64())
    vec_col = pa.FixedSizeListArray.from_arrays(
        pa.array(train.flatten(), type=pa.float32()), dim
    )
    return pa.RecordBatch.from_arrays([ids, vec_col], schema=schema)


def _compute_recall(
    query: np.ndarray, train: np.ndarray, pred_ids: list[int], k: int
) -> float:
    diff = train - query
    dists = np.sum(diff * diff, axis=1)
    true_top_k = set(np.argsort(dists)[:k].tolist())
    hits = sum(1 for pid in pred_ids if pid in true_top_k)
    return hits / k


def test_daft_write_auto_build() -> None:
    from lakesoul import LakeSoulCatalog
    from lakesoul.vector_index import rerank_by_distance

    train = read_fvecs(TRAIN_PATH, 500)
    test_vecs = read_fvecs(TEST_PATH, 5)
    n_train, dim = train.shape
    batch1 = _make_record_batch(train, dim)
    schema = batch1.schema
    more = read_fvecs(TRAIN_PATH, 700)[500:]  # IDs 500-699
    batch2 = _make_record_batch(more, dim, id_start=500)

    cat = LakeSoulCatalog(
        pg_url=os.environ.get(
            "LAKESOUL_PG_URL",
            "postgresql://lakesoul_test:lakesoul_test@localhost:5432/lakesoul_test",
        ),
        pg_username="lakesoul_test",
        pg_password="lakesoul_test",
    )
    table_name = "glove200d_daft_e2e"
    table_path = f"/tmp/lakesoul_test/{table_name}"

    try:
        cat.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)

    # 1. Create table + write via Daft (auto-builds the index).
    table = cat.create_table(
        table_name,
        path=f"file://{table_path}",
        schema=schema,
        primary_keys=["id"],
        hash_bucket_num=4,
        vector_index=[
            {"column": "vec", "dim": dim, "nlist": 8, "total_bits": 7, "metric": "L2"}
        ],
    )
    table.write_daft(daft.from_arrow(batch1), batch_size=8192, thread_num=2)
    print(f"[1/5] Daft wrote {n_train} rows (4 buckets)")

    # 2. Verify write_daft auto-built one index per bucket.
    latests = glob.glob(f"{table_path}/_vector_index/vec/**/LATEST", recursive=True)
    assert len(latests) >= 2, f"Expected >=2 shard indexes, got {len(latests)}"
    print(f"[2/5] Index auto-built after write_daft: {len(latests)} shard(s)")

    # 3. Vector search works (column auto-detected from properties).
    query_vec = test_vecs[0]
    top_k = 3
    result_table = (
        table.scan()
        .options(
            reader_options={
                "vector_search_query": ",".join(f"{v:.6f}" for v in query_vec),
                "vector_search_top_k": str(top_k),
                "vector_search_nprobe": "8",
            }
        )
        .to_arrow_table()
    )
    result_table = rerank_by_distance(result_table, query_vec, "vec", top_k)
    final_ids = result_table.column("id").to_pylist()
    recall = _compute_recall(query_vec, train, final_ids, k=top_k)
    print(f"[3/5] After re-rank: top-{len(final_ids)} IDs={final_ids}, Recall@{top_k}={recall:.2f}")
    assert recall >= 0.5, f"Recall@{top_k} too low: {recall:.2f}"

    # 4. Incremental Daft write auto-builds delta segments; new vectors appear.
    table.write_daft(daft.from_arrow(batch2), batch_size=8192, thread_num=2)
    query_vec2 = more[0]
    ds = (
        table.scan()
        .options(
            reader_options={
                "vector_search_query": ",".join(f"{v:.6f}" for v in query_vec2),
                "vector_search_top_k": "100",
                "vector_search_nprobe": "8",
            }
        )
        .to_arrow_table()
    )
    all_ids = ds.column("id").to_pylist()
    new_ids = [i for i in all_ids if i >= 500]
    assert len(new_ids) > 0, f"No new vectors (>=500) in {len(all_ids)} candidates"
    print(f"[4/5] Incremental delta searchable: {len(new_ids)} new ID(s)")

    # 5. auto_build_vector_index=False writes without building.
    table_name2 = "glove200d_daft_skip"
    table_path2 = f"/tmp/lakesoul_test/{table_name2}"
    try:
        cat.drop_table(table_name2, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path2, ignore_errors=True)
    table2 = cat.create_table(
        table_name2,
        path=f"file://{table_path2}",
        schema=schema,
        primary_keys=["id"],
        hash_bucket_num=4,
        vector_index=[
            {"column": "vec", "dim": dim, "nlist": 8, "total_bits": 7, "metric": "L2"}
        ],
    )
    table2.write_daft(
        daft.from_arrow(batch1),
        batch_size=8192,
        thread_num=2,
        auto_build_vector_index=False,
    )
    skip_latests = glob.glob(f"{table_path2}/_vector_index/**/LATEST", recursive=True)
    assert skip_latests == [], (
        f"auto_build_vector_index=False should not build, got {skip_latests}"
    )
    print("[5/5] auto_build_vector_index=False correctly skipped index build")

    table.drop()
    table2.drop()
    print("✓ Daft write auto-build test PASSED")


if __name__ == "__main__":
    test_daft_write_auto_build()
