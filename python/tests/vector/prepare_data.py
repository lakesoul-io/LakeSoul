# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Convert fvecs/ivecs files to Parquet for LakeSoul vector index testing.

Dataset format (from http://corpus-texmex.irisa.fr/):
  *.fvecs:  [dim: i32][dim × f32] per vector
  *.ivecs:  [k: i32][k × i32] per entry

Output Parquet schema:
  id: uint64
  vec: FixedSizeList<float32, dim>
"""

from __future__ import annotations

import struct
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def read_fvecs(path: str, n: Optional[int] = None) -> np.ndarray:
    """Read .fvecs file, return (n, dim) float32 array."""
    with open(path, "rb") as f:
        dim = struct.unpack("<i", f.read(4))[0]
        f.seek(0)
        vec_size = 4 + dim * 4
        file_size = os.path.getsize(path)
        max_n = file_size // vec_size
        if n is None or n > max_n:
            n = max_n
        data = np.zeros((n, dim), dtype=np.float32)
        for i in range(n):
            d = struct.unpack("<i", f.read(4))[0]
            assert d == dim
            vec = struct.unpack(f"<{d}f", f.read(d * 4))
            data[i] = vec
        return data


def read_ivecs(path: str, n: Optional[int] = None) -> np.ndarray:
    """Read .ivecs file, return (n, k) int32 array."""
    with open(path, "rb") as f:
        k = struct.unpack("<i", f.read(4))[0]
        f.seek(0)
        entry_size = 4 + k * 4
        file_size = os.path.getsize(path)
        max_n = file_size // entry_size
        if n is None or n > max_n:
            n = max_n
        data = np.zeros((n, k), dtype=np.int32)
        for i in range(n):
            kk = struct.unpack("<i", f.read(4))[0]
            assert kk == k
            ids = struct.unpack(f"<{k}i", f.read(k * 4))
            data[i] = ids
        return data


def fvecs_to_parquet(
    fvecs_path: str,
    parquet_path: str,
    n: Optional[int] = None,
    id_offset: int = 0,
) -> None:
    """Convert .fvecs to Parquet with schema: id(u64) + vec(FixedSizeList<f32>).

    Args:
        fvecs_path: Path to input .fvecs file.
        parquet_path: Output .parquet path or directory.
        n: Number of vectors to convert (default: all).
        id_offset: Starting ID for the first vector.
    """
    vectors = read_fvecs(fvecs_path, n)
    n_vec, dim = vectors.shape

    # Build Arrow schema
    item_field = pa.field("item", pa.float32())
    list_type = pa.list_(item_field, dim)
    schema = pa.schema([
        pa.field("id", pa.uint64(), nullable=False),
        pa.field("vec", list_type, nullable=False),
    ])

    # Build columns
    ids = pa.array(range(id_offset, id_offset + n_vec), type=pa.uint64())
    vec_column = pa.FixedSizeListArray.from_arrays(
        pa.array(vectors.flatten(), type=pa.float32()), dim
    )

    table = pa.Table.from_arrays([ids, vec_column], schema=schema)
    pq.write_table(table, parquet_path, compression="zstd")
    print(f"Written {n_vec} × {dim} vectors to {parquet_path}")


if __name__ == "__main__":
    DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
    OUT_DIR = "/tmp/lakesoul_test/glove200d"

    os.makedirs(f"{OUT_DIR}/data", exist_ok=True)

    # Convert training data (700 vectors)
    fvecs_to_parquet(
        f"{DATA_DIR}/train_700.fvecs",
        f"{OUT_DIR}/data/train_700.parquet",
        n=700,
    )

    # Convert test queries (5 queries)
    fvecs_to_parquet(
        f"{DATA_DIR}/test_5.fvecs",
        f"{OUT_DIR}/data/test_5.parquet",
        n=5,
    )
