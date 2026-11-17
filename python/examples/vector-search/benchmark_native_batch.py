#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors
"""Sequential, owned MinIO benchmark for Python and native SQL batch search.

Never builds Rust, adopts an existing table, changes website raw files, or falls
back to a SQL loop. The SQL contract is lakesoul_vector_search_batch[_from_table]
shared by DataFusion and LakeSoul pgwire, not the upstream pgvector extension.
Only the supervisor deletes resources after its performance processes exit.
Use --cleanup-run RUN_DIRECTORY to retry interrupted, manifest-scoped cleanup.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import json
import math
from numbers import Integral
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import socket
import subprocess
import sys
import time
import traceback
from typing import Any, Callable, Sequence
from urllib.parse import urlparse
import uuid

VERSION = "native-batch-v2"
K, SINGLE_NQ, CHUNK_NQ, EXACT_OVERFETCH = 100, 200, 100, 1
BUCKET = "lakesoul-bench2"
MARKER = ".native-batch-owner.json"
SQL_INLINE = "lakesoul_vector_search_batch"
SQL_TABLE = "lakesoul_vector_search_batch_from_table"
REPO = Path(__file__).resolve().parents[3]


class ProtocolError(RuntimeError):
    """Incomplete, malformed or incomparable measurements cannot be published."""


@dataclass(frozen=True)
class Dataset:
    filename: str
    dim: int
    rows: int
    metric: str
    normalize: bool
    nlist: int
    probes: tuple[int, ...]
    batch_nq: int = 10_000
    bits: int = 7


DATASETS = {
    "sift": Dataset("sift-128-euclidean.hdf5", 128, 1_000_000, "L2", False, 256, (64,)),
    "glove": Dataset(
        "glove-200-angular.hdf5", 200, 1_183_514, "IP", True, 1024, (512,)
    ),
    "gist": Dataset(
        "gist-960-euclidean.hdf5", 960, 1_000_000, "L2", False, 256, (64,), 1000
    ),
    "deep": Dataset(
        "deep-image-96-angular.hdf5", 96, 9_990_000, "IP", True, 1024, (32, 64)
    ),
}


def methodology() -> dict[str, Any]:
    return {
        "version": VERSION,
        "top_k": K,
        "single_nq": SINGLE_NQ,
        "batch_nq": {name: spec.batch_nq for name, spec in DATASETS.items()},
        "queries": "first real HDF5 test rows; never cycle, repeat or pad",
        "single": "200 completed ordinary single-vector API calls, no unreported warmup",
        "batch": "exactly one fully consumed native call per API/rerank/nprobe",
        "qps": "validated completed query-vector count / that call's wall seconds",
        "percentiles": "separate native calls in 100-query chunks AFTER full timing",
        "percentile_method": "chunk-normalized, not individual batch query latency",
        "timed_scope": "API execution and full transfer; preparation/validation excluded",
        "validation": "qid set, 100 unique in-range IDs, complete 1-based ranks, finite ordered SQL distances, per-query recall",
        "exact_overfetch": EXACT_OVERFETCH,
        "hash_buckets": 4,
        "cache": {"L1": "512MiB", "L2": "1GiB", "lazy_index": "auto"},
        "cache_state": "fresh process/owned empty cache per API/rerank/nprobe; singles precede batch",
        "ordering": "datasets then API/rerank/nprobe, no concurrent performance jobs",
        "sql_functions": {"inline": SQL_INLINE, "table": SQL_TABLE},
        "sql_source": "owned LakeSoul query table for full batch, inline JSON for percentile chunks",
        "sql_exact_contract": "final optional exact_overfetch literal must be supported",
        "baseline_raw": "vector-index-minio-large-512m1g-2026-11-11 (historical; raw data is no longer published with the repository)",
    }


def real_query_indices(
    dataset: str, available: int, truth_rows: int
) -> tuple[list[int], list[int]]:
    required = DATASETS[dataset].batch_nq
    if available < required or truth_rows < required:
        raise ProtocolError(
            f"{dataset} needs {required} real test/truth rows; got {available}/{truth_rows}"
        )
    return list(range(SINGLE_NQ)), list(range(required))


@dataclass
class Groups:
    qids: list[int]
    ids: list[list[int]]
    distances: list[list[float]] | None = None


def integer(value: Any, description: str) -> int:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise ProtocolError(f"{description} must be integer, got {value!r}")
    return int(value)


def sql_groups(
    rows: Sequence[Sequence[Any]], expected: Sequence[int], *, inline: bool
) -> Groups:
    """Reorder SQL rows using qid/rank, never arrival order or zip truncation."""
    source_ids = list(range(len(expected))) if inline else list(expected)
    by_qid: dict[int, dict[int, tuple[int, float]]] = {qid: {} for qid in source_ids}
    for row in rows:
        if len(row) != 4:
            raise ProtocolError("SQL rows must be (query_id, rank, id, distance)")
        qid, rank, key = (integer(row[i], ("qid", "rank", "id")[i]) for i in range(3))
        if qid not in by_qid or not 1 <= rank <= K or rank in by_qid[qid]:
            raise ProtocolError(
                f"unexpected qid or duplicate/out-of-range rank: {qid}/{rank}"
            )
        distance = float(row[3])
        if not math.isfinite(distance):
            raise ProtocolError(f"non-finite distance for query {qid}")
        by_qid[qid][rank] = (key, distance)
    ids, distances = [], []
    for qid in source_ids:
        ranked = by_qid[qid]
        if set(ranked) != set(range(1, K + 1)):
            raise ProtocolError(f"query {qid} did not return all {K} ranks")
        ids.append([ranked[rank][0] for rank in range(1, K + 1)])
        values = [ranked[rank][1] for rank in range(1, K + 1)]
        # UDTF distance is canonical: squared L2 or negative inner product.
        if any(left > right for left, right in zip(values, values[1:])):
            raise ProtocolError(f"query {qid} ranks do not order distance ascending")
        distances.append(values)
    return Groups(list(expected), ids, distances)


def validate_groups(
    groups: Groups, expected: Sequence[int], truth: Sequence[Sequence[int]], rows: int
) -> list[float]:
    expected = list(expected)
    if (
        len(set(expected)) != len(expected)
        or groups.qids != expected
        or len(groups.ids) != len(expected)
    ):
        raise ProtocolError("result qids/count differ from requested real query rows")
    if groups.distances is not None and len(groups.distances) != len(expected):
        raise ProtocolError("distance group count differs from query count")
    recalls = []
    for qid, keys in zip(expected, groups.ids):
        if not 0 <= qid < len(truth):
            raise ProtocolError(f"ground truth has no query {qid}")
        keys = [integer(key, "result id") for key in keys]
        if (
            len(keys) != K
            or len(set(keys)) != K
            or any(key < 0 or key >= rows for key in keys)
        ):
            raise ProtocolError(f"query {qid} needs {K} unique IDs in [0, {rows})")
        reference = [integer(key, "truth id") for key in truth[qid][:K]]
        if len(reference) != K or len(set(reference)) != K:
            raise ProtocolError(f"query {qid} needs {K} unique ground-truth neighbors")
        recall = len(set(keys).intersection(reference)) / K
        if not 0.0 <= recall <= 1.0:
            raise ProtocolError(f"invalid recall for query {qid}: {recall}")
        recalls.append(recall)
    return recalls


@dataclass
class Prepared:
    execute: Callable[[], Any]
    decode: Callable[[Any], Groups]


def timed(prepared: Prepared, clock: Callable[[], float]) -> tuple[Any, float]:
    start = clock()
    raw = prepared.execute()  # Exactly once; consumes every Arrow/pgwire result.
    elapsed = clock() - start
    if not math.isfinite(elapsed) or elapsed <= 0:
        raise ProtocolError("wall time must be finite and positive")
    return raw, elapsed


def percentile(values: Sequence[float], quantile: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower, fraction = math.floor(position), position % 1
    return ordered[lower] * (1 - fraction) + ordered[math.ceil(position)] * fraction


def measure(
    adapter: Any,
    dataset: str,
    truth: Sequence[Sequence[int]],
    *,
    clock: Callable[[], float] = time.perf_counter,
    save_groups: Callable[..., Any] | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Pure orchestration: fake adapters/clocks exercise the accounting in tests."""
    spec = DATASETS[dataset]
    singles, batch_qids = real_query_indices(dataset, adapter.query_count, len(truth))
    latencies, single_recalls = [], []
    single_groups = Groups([], [])
    for qid in singles:
        prepared = adapter.prepare_single(qid)
        raw, elapsed = timed(prepared, clock)
        groups = prepared.decode(raw)
        single_recalls.extend(validate_groups(groups, [qid], truth, spec.rows))
        single_groups.qids.extend(groups.qids)
        single_groups.ids.extend(groups.ids)
        latencies.append(elapsed)
        if progress and len(latencies) % 25 == 0:
            progress(f"single {len(latencies)}/{SINGLE_NQ} completed")
    single_wall = math.fsum(latencies)
    single = {
        "nq": len(single_groups.qids),
        "wall_seconds": single_wall,
        "qps": len(single_groups.qids) / single_wall,
        "first_ms": latencies[0] * 1000,
        "latencies_ms": [value * 1000 for value in latencies],
        "recall": math.fsum(single_recalls) / len(single_recalls),
        "recall_per_query": single_recalls,
    }
    for name, q in (("p50_ms", 0.50), ("p90_ms", 0.90), ("p99_ms", 0.99)):
        single[name] = percentile(single["latencies_ms"], q)
    if save_groups:
        single["result_artifact"] = save_groups("single", single_groups, single_recalls)
    single["rss_mb"] = rss_mb()
    single["server_rss_mb"] = (
        adapter.server_rss() if hasattr(adapter, "server_rss") else None
    )

    if progress:
        progress(f"full native batch start: {len(batch_qids)} real query vectors")
    prepared = adapter.prepare_batch(batch_qids, full=True)
    raw, full_wall = timed(prepared, clock)
    if progress:
        progress(f"full native batch returned in {full_wall:.3f}s; validating results")
    groups = prepared.decode(raw)
    recalls = validate_groups(groups, batch_qids, truth, spec.rows)
    batch = {
        "nq": len(groups.qids),
        "completed_vectors": len(groups.qids),
        "native_calls": 1,
        "wall_seconds": full_wall,
        "total_ms": full_wall * 1000,
        "qps": len(groups.qids) / full_wall,
        "mean_ms": full_wall * 1000 / len(groups.qids),
        "recall": math.fsum(recalls) / len(recalls),
        "recall_per_query": recalls,
    }
    if save_groups:
        batch["result_artifact"] = save_groups("batch", groups, recalls)
    batch["rss_mb"] = rss_mb()
    batch["server_rss_mb"] = (
        adapter.server_rss() if hasattr(adapter, "server_rss") else None
    )
    # These calls are deliberately AFTER the full-call elapsed time is frozen.
    samples, sample_wall, sample_calls = [], 0.0, 0
    for start in range(0, len(batch_qids), CHUNK_NQ):
        qids = batch_qids[start : start + CHUNK_NQ]
        prepared = adapter.prepare_batch(qids, full=False)
        raw, elapsed = timed(prepared, clock)
        validate_groups(prepared.decode(raw), qids, truth, spec.rows)
        sample_wall += elapsed
        sample_calls += 1
        samples.extend([elapsed * 1000 / len(qids)] * len(qids))
        if progress and (
            sample_calls % 10 == 0 or start + len(qids) == len(batch_qids)
        ):
            progress(
                f"separate percentile sampling {start + len(qids)}/{len(batch_qids)} completed"
            )
    batch["percentile_sampling"] = {
        "outside_full_timing": True,
        "native_calls": sample_calls,
        "completed_vectors": len(samples),
        "wall_seconds": sample_wall,
        "chunk": CHUNK_NQ,
        "method": "chunk-normalized",
        **{
            name: percentile(samples, q)
            for name, q in (("p50_ms", 0.50), ("p90_ms", 0.90), ("p99_ms", 0.99))
        },
    }
    return {"single": single, "batch": batch}


def dump_new(path: Path, value: Any) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def append_record(path: Path, value: Any) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def rss_mb(pid: int | None = None) -> float | None:
    try:
        text = Path(f"/proc/{pid if pid is not None else 'self'}/status").read_text()
    except OSError:
        return None
    for line in text.splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) / 1024
    return None


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_fingerprint(extension: Path, pg_binary: Path, note: str) -> dict[str, Any]:
    files = {}
    for label, path in (("python_extension", extension), ("pgwire_binary", pg_binary)):
        path = path.resolve(strict=True)
        files[label] = {
            "path": str(path),
            "size": path.stat().st_size,
            "sha256": file_hash(path),
        }
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=no"],
        cwd=REPO,
        check=True,
        text=True,
        capture_output=True,
    ).stdout
    return {
        "artifacts": files,
        "git_head": head,
        "tracked_worktree_dirty": bool(dirty),
        "declared_build_note": note,
        "build_flags_verified": False,
        "python": sys.version,
        "note": "Hashes identify actual binaries; runtime RUSTFLAGS cannot prove build flags. Runner never compiles.",
    }


def check_token(token: str) -> None:
    if re.fullmatch(r"[0-9a-f]{32}", token) is None:
        raise ProtocolError("invalid ownership token")


def verified_root(root: Path, parent: Path, token: str, dataset: str) -> Path:
    check_token(token)
    parent = parent.resolve(strict=True)
    if (
        dataset not in DATASETS
        or root.is_symlink()
        or root.resolve().parent != parent
        or root.name != f"nbatch_{token}_{dataset}"
    ):
        raise ProtocolError(f"refuse cleanup outside exact owned scratch child: {root}")
    marker = root / MARKER
    if (
        marker.is_symlink()
        or not marker.is_file()
        or json.loads(marker.read_text()).get("token") != token
    ):
        raise ProtocolError(f"ownership marker missing/mismatched: {root}")
    return root.resolve()


def remove_owned_root(root: Path, parent: Path, token: str, dataset: str) -> None:
    target = verified_root(root, parent, token, dataset)
    shutil.rmtree(target)
    if target.exists():
        raise ProtocolError(f"scratch cleanup did not finish: {target}")


def remove_owned_cache(
    cache: Path, root: Path, parent: Path, token: str, dataset: str
) -> None:
    owned = verified_root(root, parent, token, dataset)
    if (
        cache.is_symlink()
        or cache.resolve().parent != owned / "cache"
        or not re.fullmatch(
            r"(?:python|datafusion|pgwire)_(?:index|exact)_[0-9]+", cache.name
        )
    ):
        raise ProtocolError(f"refuse non-owned cache cleanup: {cache}")
    marker = cache / MARKER
    if (
        marker.is_symlink()
        or not marker.is_file()
        or json.loads(marker.read_text()).get("token") != token
    ):
        raise ProtocolError("cache ownership marker mismatch")
    shutil.rmtree(cache.resolve())
    if cache.exists():
        raise ProtocolError("cache remains after cleanup")


class OwnedObjects:
    """Injectable store interface: tests need neither PyArrow nor MinIO."""

    def __init__(self, store: Any, token: str, dataset: str):
        check_token(token)
        if dataset not in DATASETS:
            raise ProtocolError("unknown owned dataset")
        self.store, self.token = store, token
        self.prefix = f"{BUCKET}/{VERSION}/{token}/{dataset}"
        self.marker = f"{self.prefix}/{MARKER}"

    def create(self) -> None:
        if self.store.list_files(self.prefix):
            raise ProtocolError("object prefix must be empty before claiming")
        self.store.write(self.marker, json.dumps({"token": self.token}).encode())

    def cleanup(self) -> dict[str, Any]:
        if not self.store.list_files(self.prefix):
            return {
                "prefix": "s3://" + self.prefix,
                "objects_deleted": 0,
                "verified_empty": True,
                "already_absent": True,
            }
        if json.loads(self.store.read(self.marker)).get("token") != self.token:
            raise ProtocolError("S3 ownership marker mismatch")
        files = self.store.list_files(self.prefix)
        # Verify the ENTIRE listing before the first delete.
        for path in files:
            relative = path.removeprefix(self.prefix + "/")
            if (
                not path.startswith(self.prefix + "/")
                or ".." in PurePosixPath(relative).parts
            ):
                raise ProtocolError(f"object listing escaped owned prefix: {path}")
        for path in files:
            if path != self.marker:
                self.store.delete(path)
        remaining = self.store.list_files(self.prefix)
        if remaining != [self.marker]:
            raise ProtocolError(f"objects remain under owned prefix: {remaining[:5]}")
        self.store.delete(self.marker)
        if self.store.list_files(self.prefix):
            raise ProtocolError("owned object prefix not fully removed")
        return {
            "prefix": "s3://" + self.prefix,
            "objects_deleted": len(files),
            "verified_empty": True,
        }


class ArrowStore:
    def __init__(self, endpoint: str, access: str, secret: str):
        import pyarrow.fs as fs

        parsed = urlparse(endpoint)
        self.types = fs
        self.fs = fs.S3FileSystem(
            access_key=access,
            secret_key=secret,
            endpoint_override=parsed.netloc,
            scheme=parsed.scheme,
            region="us-east-1",
            allow_bucket_creation=False,
        )

    def list_files(self, prefix: str) -> list[str]:
        selector = self.types.FileSelector(
            prefix + "/", recursive=True, allow_not_found=True
        )
        return sorted(
            info.path
            for info in self.fs.get_file_info(selector)
            if info.type == self.types.FileType.File
        )

    def write(self, path: str, data: bytes) -> None:
        with self.fs.open_output_stream(path) as stream:
            stream.write(data)

    def read(self, path: str) -> bytes:
        with self.fs.open_input_file(path) as stream:
            return stream.read()

    def delete(self, path: str) -> None:
        self.fs.delete_file(path)


def worker_environment(
    cache: Path, stage: str, inherited: dict[str, str] | None = None
) -> dict[str, str]:
    env = dict(os.environ if inherited is None else inherited)
    for key in list(env):
        if key.startswith(("LAKESOUL_VECTOR_", "LAKESOUL_INDEX_")) or key in (
            "LAKESOUL_CACHE",
            "LAKESOUL_CACHE_PATH",
            "LAKESOUL_CACHE_SIZE",
            "LAKESOUL_CACHE_DIR",
            "PG_SERVICE_PID",
        ):
            env.pop(key)
    env.update(
        {
            "LAKESOUL_VECTOR_CACHE": "1" if stage == "query" else "0",
            "LAKESOUL_VECTOR_MEMORY_BYTES": "512MiB",
            "LAKESOUL_CACHE": "1",
            "LAKESOUL_CACHE_PATH": str(cache / "data"),
            "LAKESOUL_CACHE_SIZE": "1GiB",
            "LAKESOUL_VECTOR_LAZY_INDEX": "auto",
            "PYTHONUNBUFFERED": "1",
            "PYTHONPATH": str(REPO / "python/src"),
            "OMP_NUM_THREADS": "1",
            "OPENBLAS_NUM_THREADS": "1",
        }
    )
    return env


def options(config: dict[str, Any]) -> dict[str, str]:
    return {
        "fs.s3a.bucket": BUCKET,
        "fs.s3a.access.key": os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "fs.s3a.endpoint": config["endpoint"],
        "fs.s3a.path.style.access": "true",
    }


def catalog(config: dict[str, Any]) -> Any:
    from lakesoul import LakeSoulCatalog

    return LakeSoulCatalog.from_env(
        namespace="default", object_store_options=options(config)
    )


def load_queries(config: dict[str, Any]) -> tuple[Any, Any, dict[str, Any]]:
    import h5py
    import numpy as np

    spec = DATASETS[config["dataset"]]
    path = Path(config["data_dir"]) / spec.filename
    with h5py.File(path, "r") as source:
        real_query_indices(
            config["dataset"], len(source["test"]), len(source["neighbors"])
        )
        if source["train"].shape != (spec.rows, spec.dim):
            raise ProtocolError(f"unexpected train shape: {source['train'].shape}")
        queries = np.asarray(source["test"][: spec.batch_nq], dtype=np.float32)
        truth = np.asarray(source["neighbors"][: spec.batch_nq, :K], dtype=np.int64)
    if (
        queries.shape != (spec.batch_nq, spec.dim)
        or truth.shape != (spec.batch_nq, K)
        or not np.isfinite(queries).all()
    ):
        raise ProtocolError("invalid query/truth dimensions or non-finite query values")
    if spec.normalize:
        norms = np.linalg.norm(queries, axis=1, keepdims=True)
        queries = np.divide(queries, norms, out=np.zeros_like(queries), where=norms > 0)
    return (
        queries,
        truth,
        {
            "path": str(path.resolve()),
            "file_size": path.stat().st_size,
            "query_sha256": hashlib.sha256(queries.tobytes()).hexdigest(),
            "truth_sha256": hashlib.sha256(truth.tobytes()).hexdigest(),
        },
    )


def arrow_vectors(
    ids: Any, vectors: Any, dim: int, id_name: str = "id", vec_name: str = "vec"
) -> Any:
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field(id_name, pa.int64(), False),
            pa.field(vec_name, pa.list_(pa.float32(), dim), False),
        ]
    )
    return pa.RecordBatch.from_arrays(
        [
            pa.array(ids, type=pa.int64()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.reshape(-1), type=pa.float32()), dim
            ),
        ],
        schema=schema,
    )


def setup_dataset(config: dict[str, Any]) -> dict[str, Any]:
    import h5py
    import numpy as np
    import pyarrow as pa

    cat, spec = catalog(config), DATASETS[config["dataset"]]
    queries, _, fingerprint = load_queries(config)
    example = arrow_vectors(np.arange(1), queries[:1], spec.dim)
    properties = {"native_batch_owner": config["token"]}
    target = cat.create_table(
        config["table"],
        path=config["target_path"],
        schema=example.schema,
        primary_keys=["id"],
        hash_bucket_num=4,
        properties=properties,
        vector_index=[
            {
                "column": "vec",
                "dim": spec.dim,
                "nlist": spec.nlist,
                "total_bits": spec.bits,
                "metric": spec.metric,
            }
        ],
    )
    with h5py.File(Path(config["data_dir"]) / spec.filename, "r") as source:

        def batches():
            for start in range(0, spec.rows, 65_536):
                vectors = np.asarray(
                    source["train"][start : start + 65_536], dtype=np.float32
                )
                if spec.normalize:
                    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
                    vectors = np.divide(
                        vectors, norms, out=np.zeros_like(vectors), where=norms > 0
                    )
                yield arrow_vectors(
                    np.arange(start, start + len(vectors), dtype=np.int64),
                    vectors,
                    spec.dim,
                )

        start = time.perf_counter()
        target.write_arrow(
            pa.RecordBatchReader.from_batches(example.schema, batches()),
            format="vortex",
            auto_build_vector_index=False,
        )
        write_s = time.perf_counter() - start
    qbatch = arrow_vectors(
        np.arange(spec.batch_nq, dtype=np.int64),
        queries,
        spec.dim,
        "query_id",
        "query_vector",
    )
    qtable = cat.create_table(
        config["query_table"],
        path=config["query_path"],
        schema=qbatch.schema,
        primary_keys=["query_id"],
        hash_bucket_num=1,
        properties=properties,
    )
    qtable.write_arrow(qbatch, format="vortex", auto_build_vector_index=False)
    return {
        "kind": "setup",
        "write_s": write_s,
        "write_rss_mb": rss_mb(),
        "rows": spec.rows,
        "dataset_input": fingerprint,
    }


def build_index(config: dict[str, Any]) -> dict[str, Any]:
    from lakesoul import index as idx

    cat, spec = catalog(config), DATASETS[config["dataset"]]
    table = cat.table(config["table"])
    cfg = idx.config_for_column(
        table._index_configs("vector"),
        "vec",
        overrides={"nlist": spec.nlist, "total_bits": spec.bits},
    )
    start = time.perf_counter()
    result = idx.build_table_index(
        "vector",
        table_name=config["table"],
        namespace="default",
        configs=[cfg],
        store_config=idx.default_object_store_config(catalog=cat, table=table),
        rebuild=True,
    )
    if result.get("status") != "ok":
        raise ProtocolError(f"index build failed: {result!r}")
    return {
        "kind": "build",
        "build_s": time.perf_counter() - start,
        "build_rss_mb": rss_mb(),
        "nlist": spec.nlist,
        "bits": spec.bits,
    }


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_batch(
    config: dict[str, Any],
    queries: Any,
    qids: Sequence[int],
    *,
    full: bool,
    target: str,
    query_table: str,
) -> str:
    args = [sql_literal(target), "'vec'"]
    if full:
        args += [sql_literal(query_table), "'query_id'", "'query_vector'"]
    else:
        args += [
            sql_literal(
                json.dumps(
                    [queries[qid].tolist() for qid in qids],
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        ]
    args += [
        str(K),
        str(config["nprobe"]),
        sql_literal(DATASETS[config["dataset"]].metric),
        sql_literal(config["rerank"]),
    ]
    # Exact mode must not silently inherit UDTF overfetch=10 instead of the
    # original benchmark's 1. Index mode has no rerank candidate multiplier.
    if config["rerank"] == "exact":
        args.append(str(EXACT_OVERFETCH))
    return (
        "SELECT query_id, rank, id, distance FROM "
        + (SQL_TABLE if full else SQL_INLINE)
        + "("
        + ", ".join(args)
        + ")"
    )


def one_shot_sql(session: Any, sql: str) -> Any:
    # LakeSoulSession.sql currently retries empty streams with its collected API.
    # Benchmarks must observe an empty/failed result, never rerun the native call.
    batches = list(session._inner.sql_stream(sql))
    if not batches:
        return []
    import pyarrow as pa

    return pa.Table.from_batches(batches)


class Adapter:
    def __init__(self, config: dict[str, Any], queries: Any):
        self.config, self.queries, self.query_count = config, queries, len(queries)
        self.spec, self.api = DATASETS[config["dataset"]], config["api"]
        self.session = self.connection = None
        if self.api == "pgwire":
            import psycopg

            self.connection = psycopg.connect(
                host="127.0.0.1",
                port=config["port"],
                dbname="default",
                user=os.environ.get("LAKESOUL_PG_USERNAME", "lakesoul_test"),
                password=os.environ.get("LAKESOUL_PG_PASSWORD", "lakesoul_test"),
                autocommit=True,
            )
            self.cursor = self.connection.cursor()
            for key, value in (
                ("rerank", "'" + config["rerank"] + "'"),
                ("nprobe", str(config["nprobe"])),
                ("exact_overfetch", str(EXACT_OVERFETCH)),
            ):
                self.cursor.execute(f"SET lakesoul_vector_search.{key} = {value}")
        else:
            cat = catalog(config)
            self.table = cat.table(config["table"])
            self.session = cat.session(
                reuse=False,
                object_store_options=options(config),
                warehouse_prefix=f"s3://{BUCKET}/",
            )
            self.session.set_vector_search(
                rerank=config["rerank"],
                nprobe=config["nprobe"],
                exact_overfetch=EXACT_OVERFETCH,
            )

    def sql_call(self, sql: str) -> Callable[[], Any]:
        if self.api == "pgwire":

            def execute():
                self.cursor.execute(sql)
                return self.cursor.fetchall()

            return execute
        return lambda: one_shot_sql(self.session, sql)

    @staticmethod
    def sql_rows(raw: Any) -> list[Sequence[Any]]:
        if isinstance(raw, list):
            return raw
        return list(
            zip(
                *(
                    raw.column(name).to_pylist()
                    for name in ("query_id", "rank", "id", "distance")
                )
            )
        )

    def prepare_single(self, qid: int) -> Prepared:
        query = self.queries[qid].tolist()
        if self.api == "python":

            def execute():
                return (
                    self.table.search(
                        query,
                        top_k=K,
                        rerank=self.config["rerank"],
                        nprobe=self.config["nprobe"],
                        exact_overfetch=EXACT_OVERFETCH,
                        columns=["id"],
                        metric=self.spec.metric,
                        session=self.session,
                    )
                    .column("id")
                    .to_pylist()
                )
        elif self.api == "datafusion":
            sql = self.table.to_sql(
                query, top_k=K, metric=self.spec.metric, columns=["id"]
            )

            def execute():
                return self.session.sql(sql).column("id").to_pylist()
        else:
            operator = "<#>" if self.spec.metric == "IP" else "<->"
            vector = sql_literal(
                json.dumps(query, separators=(",", ":"), allow_nan=False)
            )
            run = self.sql_call(
                f'SELECT id FROM "{self.config["table"]}" ORDER BY vec {operator} {vector} LIMIT {K}'
            )

            def execute():
                return [row[0] for row in run()]

        return Prepared(execute, lambda ids: Groups([qid], [ids]))

    def prepare_batch(self, qids: Sequence[int], *, full: bool) -> Prepared:
        ids = list(qids)
        if self.api == "python":
            vectors = [self.queries[qid].tolist() for qid in ids]

            def execute():
                return self.session.batch_search(
                    self.config["table"],
                    vectors,
                    namespace="default",
                    column="vec",
                    top_k=K,
                    nprobe=self.config["nprobe"],
                    parallelism=1,
                    batch_size=0,
                    exact=self.config["rerank"] == "exact",
                    exact_overfetch=EXACT_OVERFETCH,
                    metric=self.spec.metric,
                ).ids

            return Prepared(execute, lambda raw: Groups(ids, list(raw)))
        target = (
            self.config["table"]
            if self.api == "pgwire"
            else f"lakesoul.default.{self.config['table']}"
        )
        source = (
            self.config["query_table"]
            if self.api == "pgwire"
            else f"lakesoul.default.{self.config['query_table']}"
        )
        sql = sql_batch(
            self.config, self.queries, ids, full=full, target=target, query_table=source
        )
        return Prepared(
            self.sql_call(sql),
            lambda raw: sql_groups(self.sql_rows(raw), ids, inline=not full),
        )

    def preflight(self) -> None:
        if self.api == "python":
            return
        target = (
            self.config["table"]
            if self.api == "pgwire"
            else f"lakesoul.default.{self.config['table']}"
        )
        source = (
            self.config["query_table"]
            if self.api == "pgwire"
            else f"lakesoul.default.{self.config['query_table']}"
        )
        sql = sql_batch(
            self.config,
            self.queries,
            range(self.query_count),
            full=True,
            target=target,
            query_table=source,
        )
        if self.api == "pgwire":
            self.cursor.execute("EXPLAIN " + sql)
            plan = "\n".join(str(row) for row in self.cursor.fetchall())
        else:
            plan = self.session.explain(sql, analyze=False)
        if "VectorBatchSearchExec" not in plan:
            raise ProtocolError("SQL did not plan the native batch operator")

    def server_rss(self) -> float | None:
        pid = os.environ.get("PG_SERVICE_PID")
        return rss_mb(int(pid)) if self.api == "pgwire" and pid else None

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
        if self.session is not None:
            self.session.close()


def save_result_groups(
    directory: Path, phase: str, groups: Groups, recalls: Sequence[float]
) -> dict[str, Any]:
    if len(groups.qids) != len(groups.ids) or len(recalls) != len(groups.qids):
        raise ProtocolError("cannot persist incomplete query groups or recalls")
    if groups.distances is not None and len(groups.distances) != len(groups.qids):
        raise ProtocolError("cannot persist incomplete distance groups")
    path = directory / f"{phase}.results.v2.jsonl.gz"
    with (
        path.open("xb") as raw,
        gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as zipped,
    ):
        for index, (qid, ids, recall) in enumerate(
            zip(groups.qids, groups.ids, recalls)
        ):
            row = {
                "query_id": qid,
                "ids": ids,
                "ranks": list(range(1, K + 1)),
                "recall": recall,
            }
            if groups.distances is not None:
                row["distances"] = groups.distances[index]
            zipped.write((json.dumps(row, allow_nan=False) + "\n").encode())
    return {
        "path": str(path),
        "sha256": file_hash(path),
        "query_groups": len(groups.qids),
    }


def query_worker(config: dict[str, Any], artifact_dir: Path) -> dict[str, Any]:
    queries, truth, fingerprint = load_queries(config)
    adapter = Adapter(config, queries)
    try:
        adapter.preflight()
        result = measure(
            adapter,
            config["dataset"],
            truth,
            save_groups=lambda *values: save_result_groups(artifact_dir, *values),
            progress=lambda message: print(message, file=sys.stderr, flush=True),
        )
        return {
            "kind": "query_native_batch",
            "api": config["api"],
            "rerank": config["rerank"],
            "nprobe": config["nprobe"],
            "dataset_input": fingerprint,
            **result,
        }
    finally:
        adapter.close()


def validate_config(config: dict[str, Any]) -> None:
    token, dataset = config["token"], config["dataset"]
    check_token(token)
    if dataset not in DATASETS:
        raise ProtocolError("unknown dataset")
    prefix = f"s3://{BUCKET}/{VERSION}/{token}/{dataset}"
    table = f"nb2_{token[:20]}_{dataset}"
    if (
        config["target_path"] != prefix + "/target"
        or config["query_path"] != prefix + "/queries"
        or config["table"] != table
        or config["query_table"] != table + "_queries"
    ):
        raise ProtocolError("manifest contains non-owned table or object paths")
    verified_root(
        Path(config["scratch"]), Path(config["scratch_parent"]), token, dataset
    )


def cleanup_catalog(config: dict[str, Any]) -> dict[str, Any]:
    from lakesoul.exceptions import TableNotFoundError

    cat = catalog(config)
    for key, path_key in (("query_table", "query_path"), ("table", "target_path")):
        try:
            table = cat.table(config[key])
        except TableNotFoundError:
            continue
        if (
            str(table.path).rstrip("/") != config[path_key].rstrip("/")
            or table.properties.get("native_batch_owner") != config["token"]
        ):
            raise ProtocolError(f"refuse drop of unowned table {config[key]}")
        cat.drop_table(config[key])
    for key in ("query_table", "table"):
        try:
            cat.table(config[key])
        except TableNotFoundError:
            continue
        raise ProtocolError(f"metadata cleanup incomplete: {config[key]}")
    return {
        "kind": "catalog_cleanup",
        "verified_absent": [config["query_table"], config["table"]],
    }


def process_group_alive(pid: int) -> bool:
    try:
        os.killpg(pid, 0)
        return True
    except ProcessLookupError:
        return False


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
    if process_group_alive(process.pid):
        os.killpg(process.pid, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while process_group_alive(process.pid) and time.monotonic() < deadline:
            time.sleep(0.05)
    if process_group_alive(process.pid):
        raise ProtocolError(
            f"process group {process.pid} has not exited; retain resources"
        )


def ensure_exited(directory: Path) -> None:
    for marker in directory.rglob("owned-process.json"):
        pid = json.loads(marker.read_text())["pid"]
        if process_group_alive(pid):
            raise ProtocolError(f"owned process group {pid} still active; no cleanup")


def run_child(
    command: list[str], env: dict[str, str], log_dir: Path, timeout: float
) -> None:
    log_dir.mkdir(exist_ok=False)
    with (
        (log_dir / "stdout.log").open("xb") as stdout,
        (log_dir / "stderr.log").open("xb") as stderr,
    ):
        child = subprocess.Popen(
            command,
            env=env,
            cwd=REPO,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        dump_new(log_dir / "owned-process.json", {"pid": child.pid})
        try:
            code = child.wait(timeout=timeout)
        finally:
            stop_process(child)
            dump_new(
                log_dir / "process.json",
                {
                    "pid": child.pid,
                    "returncode": child.returncode,
                    "process_group_exited": True,
                },
            )
    if code:
        raise ProtocolError(f"child exited {code}; stderr: {log_dir / 'stderr.log'}")


@contextmanager
def owned_server(config: dict[str, Any], env: dict[str, str], log_dir: Path):
    expected = config["fingerprint"]["artifacts"]["pgwire_binary"]["sha256"]
    if file_hash(Path(config["pg_binary"])) != expected:
        raise ProtocolError(
            "pgwire binary changed after fingerprinting; start a new run"
        )
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", config["port"]))
    opts = options(config)
    command = [
        config["pg_binary"],
        "--bind",
        "127.0.0.1",
        "-p",
        str(config["port"]),
        "--metrics-addr",
        f"127.0.0.1:{config['metrics_port']}",
        "--worker-threads",
        "4",
        "--warehouse-prefix",
        f"s3://{BUCKET}",
        "--s3-bucket",
        BUCKET,
        "--s3-access-key",
        opts["fs.s3a.access.key"],
        "--s3-secret-key",
        opts["fs.s3a.secret.key"],
        "--endpoint",
        config["endpoint"],
    ]
    log_dir.mkdir(exist_ok=False)
    with (
        (log_dir / "stdout.log").open("xb") as stdout,
        (log_dir / "stderr.log").open("xb") as stderr,
    ):
        server = subprocess.Popen(
            command,
            env=env,
            cwd=REPO,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        dump_new(log_dir / "owned-process.json", {"pid": server.pid})
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if server.poll() is not None:
                    raise ProtocolError(f"pgwire startup failed; see {log_dir}")
                try:
                    with socket.create_connection(
                        ("127.0.0.1", config["port"]), timeout=0.2
                    ):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise ProtocolError("owned pgwire did not become ready")
            yield server.pid
        finally:
            stop_process(server)
            dump_new(
                log_dir / "process.json",
                {
                    "pid": server.pid,
                    "returncode": server.returncode,
                    "process_group_exited": True,
                },
            )


def worker(config_path: Path, stage: str, record_path: Path) -> None:
    config = json.loads(config_path.read_text())
    validate_config(config)
    if stage in ("setup", "build", "query"):
        import lakesoul._lib as extension

        expected = config["fingerprint"]["artifacts"]["python_extension"]
        if file_hash(Path(extension.__file__)) != expected["sha256"]:
            raise ProtocolError("loaded extension differs from fingerprint")
    action = {"setup": setup_dataset, "build": build_index, "cleanup": cleanup_catalog}
    record = (
        query_worker(config, record_path.parent)
        if stage == "query"
        else action[stage](config)
    )
    dump_new(
        record_path,
        {"protocol_version": VERSION, "dataset": config["dataset"], **record},
    )


def execute_stage(
    config: dict[str, Any], stage: str, out: Path, env: dict[str, str], timeout: float
) -> dict[str, Any]:
    out.mkdir(exist_ok=False)
    cfg, record = out / "job.json", out / "record.v2.json"
    dump_new(cfg, config)
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        stage,
        "--job",
        str(cfg),
        "--record",
        str(record),
    ]
    run_child(command, env, out / "process", timeout)
    return json.loads(record.read_text())


def cleanup_dataset(
    config: dict[str, Any], out: Path, store: Any, timeout: float
) -> dict[str, Any]:
    validate_config(config)
    ensure_exited(out)
    root, parent = Path(config["scratch"]), Path(config["scratch_parent"])
    metadata = execute_stage(
        config,
        "cleanup",
        out / ("cleanup_" + uuid.uuid4().hex[:8]),
        worker_environment(root / "cleanup-cache", "cleanup"),
        timeout,
    )
    ensure_exited(out)
    objects = OwnedObjects(store, config["token"], config["dataset"]).cleanup()
    remove_owned_root(root, parent, config["token"], config["dataset"])
    return {
        "kind": "cleanup",
        "dataset": config["dataset"],
        "metadata": metadata,
        "objects": objects,
        "scratch_absent": not root.exists(),
    }


def arguments(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--datasets", nargs="+", choices=list(DATASETS), default=list(DATASETS)
    )
    parser.add_argument(
        "--apis",
        nargs="+",
        choices=["python", "datafusion", "pgwire"],
        default=["python", "datafusion", "pgwire"],
    )
    parser.add_argument(
        "--rerank", nargs="+", choices=["index", "exact"], default=["index", "exact"]
    )
    parser.add_argument(
        "--data-dir", type=Path, default=Path("/home/zhibin/ann-benchmarks")
    )
    parser.add_argument(
        "--scratch-root", type=Path, default=Path("/tmp/lakesoul-native-batch")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("/tmp/lakesoul-native-batch-results")
    )
    parser.add_argument("--endpoint", default="http://127.0.0.1:9000")
    parser.add_argument(
        "--extension", type=Path, default=REPO / "python/src/lakesoul/_lib.abi3.so"
    )
    parser.add_argument(
        "--pg-binary", type=Path, default=REPO / "rust/target/release/postgres-lakesoul"
    )
    parser.add_argument(
        "--build-note", default="unknown; identify by SHA256; runner does not compile"
    )
    parser.add_argument("--port", type=int, default=6543)
    parser.add_argument("--metrics-port", type=int, default=19090)
    parser.add_argument("--timeout-seconds", type=float, default=14_400)
    parser.add_argument(
        "--cleanup-run",
        type=Path,
        help="Retry only resources in this owned run manifest",
    )
    parser.add_argument(
        "--worker",
        choices=["setup", "build", "query", "cleanup"],
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--job", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--record", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    for name in ("datasets", "apis", "rerank"):
        if len(set(getattr(args, name))) != len(getattr(args, name)):
            parser.error(f"--{name} must not contain duplicates")
    if args.timeout_seconds <= 0 or not math.isfinite(args.timeout_seconds):
        parser.error("timeout must be finite and positive")
    return args


def supervisor(args: argparse.Namespace) -> Path:
    args.scratch_root.mkdir(parents=True, exist_ok=True)
    scratch_parent = args.scratch_root.resolve()
    with (scratch_parent / ".native-batch.lock").open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ProtocolError("another benchmark owns this scratch root") from error
        store = ArrowStore(
            args.endpoint,
            os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        if args.cleanup_run:
            out = args.cleanup_run.resolve(strict=True)
            manifest = json.loads((out / "manifest.v2.json").read_text())
            if manifest["protocol"]["version"] != VERSION:
                raise ProtocolError("cleanup requires a native-batch-v2 manifest")
            for config in manifest["datasets"]:
                if (
                    config["scratch_parent"] != str(scratch_parent)
                    or config["endpoint"] != args.endpoint
                ):
                    raise ProtocolError(
                        "cleanup must use the manifest's --scratch-root and --endpoint"
                    )
                if Path(config["scratch"]).exists():
                    append_record(
                        out / "results.v2.jsonl",
                        cleanup_dataset(config, out, store, args.timeout_seconds),
                    )
            return out
        fingerprint = build_fingerprint(args.extension, args.pg_binary, args.build_note)
        token = uuid.uuid4().hex
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        out = args.output_dir.resolve() / f"{VERSION}-{stamp}-{token}"
        out.mkdir(parents=True, exist_ok=False)
        results = out / "results.v2.jsonl"
        results.touch(exist_ok=False)
        configs = []
        for dataset in args.datasets:
            prefix = f"s3://{BUCKET}/{VERSION}/{token}/{dataset}"
            table = f"nb2_{token[:20]}_{dataset}"
            configs.append(
                {
                    "dataset": dataset,
                    "token": token,
                    "scratch_parent": str(scratch_parent),
                    "scratch": str(scratch_parent / f"nbatch_{token}_{dataset}"),
                    "table": table,
                    "query_table": table + "_queries",
                    "target_path": prefix + "/target",
                    "query_path": prefix + "/queries",
                    "endpoint": args.endpoint,
                    "data_dir": str(args.data_dir.resolve()),
                    "pg_binary": str(args.pg_binary.resolve()),
                    "port": args.port,
                    "metrics_port": args.metrics_port,
                    "fingerprint": fingerprint,
                    "settings": asdict(DATASETS[dataset]),
                }
            )
        manifest = {
            "protocol": methodology(),
            "fingerprint": fingerprint,
            "token": token,
            "datasets": configs,
        }
        dump_new(out / "manifest.v2.json", manifest)
        append_record(results, {"kind": "protocol", **manifest})
        for config in configs:
            dataset, root = config["dataset"], Path(config["scratch"])
            root.mkdir(exist_ok=False)
            dump_new(root / MARKER, {"token": token, "dataset": dataset})
            claimed = False
            try:
                OwnedObjects(store, token, dataset).create()
                claimed = True
                dataset_out = out / dataset
                dataset_out.mkdir()
                for stage in ("setup", "build"):
                    print(f"{dataset}: {stage}", file=sys.stderr, flush=True)
                    append_record(
                        results,
                        execute_stage(
                            config,
                            stage,
                            dataset_out / stage,
                            worker_environment(root / (stage + "-cache"), stage),
                            args.timeout_seconds,
                        ),
                    )
                for api in args.apis:
                    for rerank in args.rerank:
                        for nprobe in DATASETS[dataset].probes:
                            label = f"{api}_{rerank}_{nprobe}"
                            cache = root / "cache" / label
                            cache.mkdir(parents=True, exist_ok=False)
                            dump_new(cache / MARKER, {"token": token})
                            current = {
                                **config,
                                "api": api,
                                "rerank": rerank,
                                "nprobe": nprobe,
                            }
                            env = worker_environment(cache, "query")
                            print(
                                f"{dataset}: {label}; single={SINGLE_NQ}, native batch={DATASETS[dataset].batch_nq}",
                                file=sys.stderr,
                                flush=True,
                            )
                            try:
                                if api == "pgwire":
                                    with owned_server(
                                        config, env, dataset_out / (label + "-server")
                                    ) as server_pid:
                                        query_env = {
                                            **env,
                                            "PG_SERVICE_PID": str(server_pid),
                                        }
                                        record = execute_stage(
                                            current,
                                            "query",
                                            dataset_out / label,
                                            query_env,
                                            args.timeout_seconds,
                                        )
                                else:
                                    record = execute_stage(
                                        current,
                                        "query",
                                        dataset_out / label,
                                        env,
                                        args.timeout_seconds,
                                    )
                                append_record(results, record)
                            finally:
                                ensure_exited(out)
                                remove_owned_cache(
                                    cache, root, scratch_parent, token, dataset
                                )
            except BaseException as error:
                append_record(
                    results,
                    {
                        "kind": "error",
                        "dataset": dataset,
                        "error": str(error),
                        "traceback": traceback.format_exc(),
                    },
                )
                raise
            finally:
                if claimed:
                    append_record(
                        results,
                        cleanup_dataset(config, out, store, args.timeout_seconds),
                    )
                elif root.exists():
                    remove_owned_root(root, scratch_parent, token, dataset)
        return out


def main(argv: Sequence[str] | None = None) -> int:
    args = arguments(argv)
    if args.worker:
        worker(args.job, args.worker, args.record)
    else:

        def interrupted(_signum, _frame):
            raise KeyboardInterrupt("benchmark supervisor received SIGTERM")

        signal.signal(signal.SIGTERM, interrupted)
        print(supervisor(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
