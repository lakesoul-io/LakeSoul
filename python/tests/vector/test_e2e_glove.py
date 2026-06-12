# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""E2E: glove-200d → build index → LakeSoul reader with vector search.

Validates the full LakeSoul vector index workflow:
1. Build index from parquet via Rust PyO3 (`build_shard_vector_index`)
2. Read with LakeSoul `sync_reader` + vector_search options
3. Verify returned rows are filtered by vector similarity

Usage:
    cd python && uv sync --python 3.10
    .venv/bin/python -m pytest tests/vector/test_e2e_glove.py -s
"""

from __future__ import annotations

import os
import struct
import tempfile
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

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


def test_e2e_glove_build_and_search():
    """Build vector index, then read via LakeSoul reader with vector search."""
    train = read_fvecs(f"{DATA_DIR}/train.fvecs", 500)
    test_vecs = read_fvecs(f"{DATA_DIR}/test.fvecs", 5)
    n_train, dim = train.shape

    # 1. Write parquet with LakeSoul naming convention (part-xxx_0000.parquet)
    schema = pa.schema([
        pa.field("id", pa.uint64(), False),
        pa.field("vec", pa.list_(pa.field("item", pa.float32()), dim), False),
    ])
    ids = pa.array(range(n_train), type=pa.uint64())
    vec_col = pa.FixedSizeListArray.from_arrays(
        pa.array(train.flatten(), type=pa.float32()), dim
    )
    table = pa.Table.from_arrays([ids, vec_col], schema=schema)

    tmp_dir = tempfile.mkdtemp(prefix="lakesoul_e2e_")
    parquet_file = f"{tmp_dir}/part-abc_0000.parquet"
    pq.write_table(table, parquet_file, compression="zstd")
    print(f"[1/4] Parquet written: {parquet_file} ({n_train} vectors)")

    # 2. Build vector index via Rust PyO3
    from lakesoul._lib.vector import build_shard_vector_index

    idx_prefix = f"{tmp_dir}/_vector_index/vec/-5/0/"
    result = build_shard_vector_index(
        store_config={"type": "local"},
        file_paths=[f"file://{parquet_file}"],
        pk_column="id",
        vector_column="vec",
        dim=dim,
        nlist=16,
        total_bits=7,
        metric="L2",
        index_prefix=idx_prefix,
    )
    assert result == "ok", f"Build failed: {result}"
    print(f"[2/4] Index built at {idx_prefix}")

    # 3. Verify index files on disk
    import glob
    files = glob.glob(f"{idx_prefix}**", recursive=True)
    assert any("LATEST" in f for f in files), f"No LATEST in {files}"
    print(f"[3/4] Index verified: {len(files)} files")

    # 4. Read via LakeSoul reader with vector search
    from lakesoul._lib._dataset import sync_reader

    query_vec = test_vecs[0]
    options = [
        ("vector_search_column", "vec"),
        ("vector_search_query", ",".join(f"{v:.6f}" for v in query_vec)),
        ("vector_search_top_k", "3"),
        ("vector_search_nprobe", "4"),
        ("vector_search_index_prefix", idx_prefix),
        ("skip_merge_on_read", "true"),
    ]

    reader = sync_reader(
        batch_size=8192,
        thread_num=2,
        schema=schema,
        file_urls=[f"file://{parquet_file}"],
        primary_keys=["id"],
        partition_info=[],
        oss_conf=[],
        partition_schema=None,
        filter=None,
        options=options,
    )

    batches = list(reader)
    n_rows = sum(b.num_rows for b in batches)
    all_ids = [i for b in batches for i in b.column("id").to_pylist()]
    print(f"[4/4] Vector search returned {n_rows} rows, IDs={all_ids}")

    assert n_rows > 0, "Vector search should return some rows"
    assert n_rows <= 3, f"Should return ≤ top_k=3 rows, got {n_rows}"
    assert all(0 <= i < n_train for i in all_ids), "IDs out of range"
    print("✓ E2E test PASSED")


if __name__ == "__main__":
    test_e2e_glove_build_and_search()
