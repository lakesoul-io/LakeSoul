#!/usr/bin/env python3
"""Stream ANN-Benchmarks GloVe HDF5 into fvecs/ivecs for DataFusion SQL.

The normalized GloVe vectors use L2 distance. For unit vectors this has the
same nearest-neighbour ordering as cosine/inner product, and permits the SQL
index-only planner rule (which currently accepts L2 ordering only).
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import h5py
import numpy as np


def fvecs(handle, destination: Path, dataset: str, rows: int, block_rows: int) -> None:
    with destination.open("xb") as output:
        values = handle[dataset]
        dimension = values.shape[1]
        for start in range(0, rows, block_rows):
            block = np.asarray(
                values[start : min(rows, start + block_rows)], dtype=np.float32
            )
            norms = np.sqrt(np.einsum("ij,ij->i", block, block, dtype=np.float32))
            np.maximum(norms, np.float32(1e-12), out=norms)
            block /= norms[:, None]
            headers = np.full((len(block), 1), dimension, dtype="<i4")
            with_headers = np.concatenate((headers, block.view("<i4")), axis=1)
            with_headers.tofile(output)


def ivecs(
    source: h5py.Dataset,
    destination: Path,
    rows: int,
    top_k: int,
    block_rows: int,
) -> None:
    with destination.open("xb") as output:
        for start in range(0, rows, block_rows):
            block = np.asarray(
                source[start : min(rows, start + block_rows), :top_k], dtype="<i4"
            )
            headers = np.full((len(block), 1), top_k, dtype="<i4")
            np.concatenate((headers, block), axis=1).tofile(output)


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdf5", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--top-k", type=int, default=100)
    parser.add_argument("--block-rows", type=int, default=10_000)
    args = parser.parse_args()

    if not args.hdf5.is_file():
        parser.error(f"HDF5 input is not a file: {args.hdf5}")
    if args.output_dir.exists():
        parser.error(f"refusing to overwrite output directory: {args.output_dir}")
    args.output_dir.mkdir(parents=True)
    base_path = args.output_dir / "glove-base.fvecs"
    query_path = args.output_dir / "glove-query.fvecs"
    gt_path = args.output_dir / "glove-gt.ivecs"
    with h5py.File(args.hdf5, "r") as source:
        train = source["train"]
        test = source["test"]
        neighbors = source["neighbors"]
        if (
            args.queries < 1
            or args.queries > len(test)
            or args.queries > len(neighbors)
        ):
            parser.error("query count is outside the available test/neighbor rows")
        if args.top_k < 1 or args.top_k > neighbors.shape[1]:
            parser.error("top-k is outside the available ground-truth width")
        fvecs(source, base_path, "train", len(train), args.block_rows)
        fvecs(source, query_path, "test", args.queries, args.block_rows)
        ivecs(neighbors, gt_path, args.queries, args.top_k, args.block_rows)
        manifest = {
            "input": str(args.hdf5.resolve()),
            "input_bytes": args.hdf5.stat().st_size,
            "train_rows": len(train),
            "test_rows_used": args.queries,
            "dim": train.shape[1],
            "metric": "L2 on L2-normalized rows; angular/cosine ranking equivalent",
            "top_k": args.top_k,
            "normalization_epsilon": 1e-12,
            "outputs": {
                path.name: {"bytes": path.stat().st_size, "sha256": digest(path)}
                for path in (base_path, query_path, gt_path)
            },
        }
    (args.output_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n"
    )
    print(json.dumps(manifest, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
