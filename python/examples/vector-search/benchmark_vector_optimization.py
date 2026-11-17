#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors
"""Compare private LakeSoul A/B artifacts on one immutable index per dataset.

This program does not compile Rust or replace an installed extension. Both
variants need a private Python package, a private pgwire executable, and a
successful fresh-build receipt. Each configuration runs >=3 balanced, seeded
A/B pairs with new processes and empty owned caches. Singles are 200 ordinary
single-vector calls; each full native batch runs once (GIST 1000, others 10000).
100-query percentile calls run separately after the full-call timer stops.

Interrupted datasets retain their owned index for --resume. Completed cases are
accepted only with verified result artifacts, process exit evidence and matching
binary/index fingerprints. --cleanup-run removes only manifest-owned resources.
If a partial dataset was cleaned, its next attempt gets a new epoch/index and all
of its cases rerun; measurements from different physical indexes are never mixed.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import fcntl
import gzip
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import random
import resource
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from typing import Any, Callable, Sequence
import uuid

HELPER_PATH = Path(__file__).with_name("benchmark_native_batch.py")
_HELPER_SPEC = importlib.util.spec_from_file_location(
    "_optimization_native_batch_helpers", HELPER_PATH
)
base = importlib.util.module_from_spec(_HELPER_SPEC)
sys.modules[_HELPER_SPEC.name] = base
_HELPER_SPEC.loader.exec_module(base)
ProtocolError = base.ProtocolError
VERSION = "vector-optimization-ab-v1"
APIS = ("python", "datafusion", "pgwire")
RERANKS = ("index", "exact")


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
    ).hexdigest()


def package_hash(directory: Path) -> str:
    """Fingerprint the Python wrapper sources independently of the native lib."""
    rows = []
    for path in sorted((directory / "lakesoul").rglob("*.py")):
        if path.is_symlink():
            raise ProtocolError(f"Python package contains a source symlink: {path}")
        rows.append(
            {
                "path": path.relative_to(directory).as_posix(),
                "sha256": base.file_hash(path),
            }
        )
    if not rows:
        raise ProtocolError("private Python package has no Python wrapper sources")
    return canonical_hash(rows)


@dataclass(frozen=True)
class Variant:
    name: str
    python_package: str
    extension: str
    pg_binary: str
    build_receipt: str
    build_receipt_sha256: str
    source_tree_sha256: str
    python_sources_sha256: str
    extension_sha256: str
    pg_binary_sha256: str


def private_artifact(path: Path, forbidden: Path, label: str) -> Path:
    if path.is_symlink():
        raise ProtocolError(
            f"{label} must be a private regular artifact, not a symlink"
        )
    resolved = path.resolve(strict=True)
    if resolved == forbidden.resolve() or forbidden.resolve() in resolved.parents:
        raise ProtocolError(f"{label} must not use mutable installed/release artifacts")
    return resolved


def fingerprint_variant(
    name: str, python_package: Path, pg_binary: Path, receipt_path: Path
) -> Variant:
    package = private_artifact(
        python_package, base.REPO / "python/src", "Python package"
    )
    extension = package / "lakesoul/_lib.abi3.so"
    extension = private_artifact(
        extension, base.REPO / "python/src", "Python extension"
    )
    binary = private_artifact(
        pg_binary, base.REPO / "rust/target/release", "pgwire binary"
    )
    receipt_path = receipt_path.resolve(strict=True)
    receipt = json.loads(receipt_path.read_text())
    if (
        receipt.get("status") != "passed"
        or receipt.get("source_verified_before_after") is not True
    ):
        raise ProtocolError(
            "variant needs a passed fresh build with source verified before/after"
        )
    source_hash = receipt.get("source_tree_sha256", "")
    if len(source_hash) != 64 or any(c not in "0123456789abcdef" for c in source_hash):
        raise ProtocolError("receipt lacks a valid source-tree fingerprint")
    artifacts = {row["kind"]: row for row in receipt.get("artifacts", [])}
    if set(artifacts) != {"python", "pgwire"}:
        raise ProtocolError(
            "receipt must identify exactly the Python and pgwire artifacts"
        )
    extension_hash, binary_hash = base.file_hash(extension), base.file_hash(binary)
    for label, path, digest in (
        ("python", extension, extension_hash),
        ("pgwire", binary, binary_hash),
    ):
        if artifacts[label].get("sha256") != digest:
            raise ProtocolError(
                f"{label} artifact does not match its fresh-build receipt: {path}"
            )
    source_manifest = receipt.get("source_manifest")
    if not source_manifest:
        raise ProtocolError("fresh-build receipt must name its frozen source manifest")
    source = json.loads(Path(source_manifest).read_text())
    if source.get("source_tree_sha256") != source_hash:
        raise ProtocolError("build receipt and frozen source manifest disagree")
    expected_wrappers = {
        row["path"].removeprefix("python/src/"): row["sha256"]
        for row in source.get("source_files", [])
        if row["path"].startswith("python/src/lakesoul/")
        and row["path"].endswith(".py")
    }
    actual_wrappers = {
        path.relative_to(package).as_posix(): base.file_hash(path)
        for path in (package / "lakesoul").rglob("*.py")
    }
    if not expected_wrappers or expected_wrappers != actual_wrappers:
        raise ProtocolError(
            "private Python wrappers differ from the frozen source manifest"
        )
    return Variant(
        name,
        str(package),
        str(extension),
        str(binary),
        str(receipt_path),
        base.file_hash(receipt_path),
        source_hash,
        package_hash(package),
        extension_hash,
        binary_hash,
    )


def verify_variant(variant: dict[str, Any]) -> None:
    actual = fingerprint_variant(
        variant["name"],
        Path(variant["python_package"]),
        Path(variant["pg_binary"]),
        Path(variant["build_receipt"]),
    )
    if asdict(actual) != variant:
        raise ProtocolError(
            f"variant {variant['name']} changed after the run manifest was frozen"
        )


def vector_memory_bytes(value: str) -> int:
    import re

    match = re.fullmatch(r"([0-9]+)(B|KB|MB|GB|KiB|MiB|GiB)?", value)
    if match is None:
        raise ProtocolError(
            "vector memory must be a positive integer byte count or B/KB/MB/GB/KiB/MiB/GiB value"
        )
    multiplier = {
        None: 1,
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
    }[match[2]]
    size = int(match[1]) * multiplier
    if not 0 < size <= 2**63 - 1:
        raise ProtocolError(
            "vector memory must be positive and fit a signed 64-bit byte count"
        )
    return size


def variant_environment(
    variant: dict[str, Any],
    cache: Path,
    stage: str,
    inherited: dict[str, str] | None = None,
    *,
    vector_memory: str = "512MiB",
) -> dict[str, str]:
    # The old helper deliberately writes repo/python/src. Override LAST, for
    # every worker including setup/build/cleanup, so a requested variant cannot
    # accidentally execute the currently installed extension.
    env = base.worker_environment(cache, stage, inherited)
    env["PYTHONPATH"] = variant["python_package"]
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    if stage == "query":
        vector_memory_bytes(vector_memory)
        env["LAKESOUL_VECTOR_MEMORY_BYTES"] = vector_memory
    return env


def loaded_provenance(
    variant: dict[str, Any], *, pid: int | None = None
) -> dict[str, Any]:
    verify_variant(variant)
    import lakesoul._lib as extension

    loaded = Path(extension.__file__).resolve(strict=True)
    if (
        loaded != Path(variant["extension"])
        or base.file_hash(loaded) != variant["extension_sha256"]
    ):
        raise ProtocolError(
            "actual imported LakeSoul extension differs from the requested private variant"
        )
    result = {
        "variant": variant["name"],
        "loaded_extension": str(loaded),
        "loaded_extension_sha256": base.file_hash(loaded),
        "python_package": variant["python_package"],
        "python_sources_sha256": variant["python_sources_sha256"],
        "pg_binary": variant["pg_binary"],
        "pg_binary_sha256": variant["pg_binary_sha256"],
        "build_receipt": variant["build_receipt"],
        "build_receipt_sha256": variant["build_receipt_sha256"],
        "build_receipt_content": json.loads(Path(variant["build_receipt"]).read_text()),
        "source_tree_sha256": variant["source_tree_sha256"],
        "python": sys.version,
        "cpu_affinity": sorted(os.sched_getaffinity(0)),
        "clock_ticks_per_second": os.sysconf("SC_CLK_TCK"),
        "runtime_environment": {
            key: os.environ.get(key)
            for key in (
                "PYTHONPATH",
                "LAKESOUL_VECTOR_MEMORY_BYTES",
                "LAKESOUL_CACHE_SIZE",
                "LAKESOUL_VECTOR_LAZY_INDEX",
                "OMP_NUM_THREADS",
                "OPENBLAS_NUM_THREADS",
                "RAYON_NUM_THREADS",
            )
        },
    }
    if pid is not None:
        try:
            executable = Path(os.readlink(f"/proc/{pid}/exe")).resolve(strict=True)
        except OSError as error:
            raise ProtocolError(
                "owned pgwire server exited before provenance verification"
            ) from error
        if (
            executable != Path(variant["pg_binary"])
            or base.file_hash(executable) != variant["pg_binary_sha256"]
        ):
            raise ProtocolError(
                "running pgwire executable differs from the variant binary"
            )
        result["running_pg_executable"] = str(executable)
    return result


def schedule(
    dataset: str, apis: Sequence[str], reranks: Sequence[str], trials: int, seed: int
) -> list[dict[str, Any]]:
    if trials < 3:
        raise ProtocolError("A/B comparison requires at least three paired trials")
    cases = []
    for api in apis:
        for rerank in reranks:
            for probe in base.DATASETS[dataset].probes:
                identity = f"{seed}/{dataset}/{api}/{rerank}/{probe}"
                rng = random.Random(
                    int(hashlib.sha256(identity.encode()).hexdigest(), 16)
                )
                first = ["A", "B"] if rng.randrange(2) == 0 else ["B", "A"]
                for trial in range(trials):
                    order = first if trial % 2 == 0 else first[::-1]
                    for position, variant in enumerate(order):
                        cases.append(
                            {
                                "id": f"{api}-{rerank}-p{probe}-t{trial:02d}-{variant}",
                                "api": api,
                                "rerank": rerank,
                                "nprobe": probe,
                                "trial": trial,
                                "pair_position": position,
                                "variant": variant,
                            }
                        )
    return cases


def methodology(
    trials: int, seed: int, vector_memory: str = "512MiB"
) -> dict[str, Any]:
    vector_memory_bytes(vector_memory)
    return {
        **base.methodology(),
        "version": VERSION,
        "paired_trials": trials,
        "seed": seed,
        "cache": {
            "L1": vector_memory,
            "L1_bytes": vector_memory_bytes(vector_memory),
            "L2": "1GiB",
            "lazy_index": "auto",
        },
        "variants": "private Python packages and pgwire binaries; never swap installed artifacts",
        "index": "one owned physical target/query/index snapshot per dataset across every variant and trial",
        "ordering": "seeded balanced AB/BA pairs per API/rerank/nprobe; one measurement process at a time",
        "cache_state": "fresh process and owned empty L2 per case; host OS and MinIO caches remain shared and are not flushed",
        "results": "immutable single/full/every-percentile-chunk groups; finite ordered SQL distances are required only for full/chunk batch results",
        "single": "reuse base.Adapter.prepare_single unchanged: 200 ordinary sequential calls projecting only id, including first query and original consumption",
        "single_sql_distances": "not projected or required in timed singles; score correctness belongs to separate untimed regressions",
        "sql_batch_parallelism": "preserve session native default 0 (auto); no additional vector_batch SET statements",
        "memory": "per-phase VmHWM and ru_maxrss plus sampled Python/pgwire VmRSS; HWM is process lifetime, not a phase-local reset",
        "diagnostics": "/proc CPU ticks and local IO counters are diagnostics only, never remote MinIO IO claims",
        "resume": "only validated completed cases on the same physical-index fingerprint; partial results never count",
        "cleanup": "stop owned processes, then drop owned tables/objects/cache after all dataset cases; interrupted dataset retained for resume or explicit cleanup",
    }


def proc_sample(
    pid: int, *, read_text: Callable[[Path], str] | None = None
) -> dict[str, Any] | None:
    read = read_text or (lambda path: path.read_text())
    try:
        status = read(Path(f"/proc/{pid}/status"))
        stat = read(Path(f"/proc/{pid}/stat"))
    except (OSError, ProcessLookupError):
        return None
    fields = stat.rsplit(")", 1)[1].split()
    sample: dict[str, Any] = {
        "pid": pid,
        "start_ticks": int(fields[19]),
        "cpu_user_ticks": int(fields[11]),
        "cpu_system_ticks": int(fields[12]),
    }
    for line in status.splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmHWM"):
            sample[key + "_bytes"] = int(value.split()[0]) * 1024
    try:
        sample["io"] = {
            key: int(value)
            for key, value in (
                line.split(":", 1)
                for line in read(Path(f"/proc/{pid}/io")).splitlines()
            )
        }
    except (OSError, ValueError):
        sample["io"] = None
    return sample


class ProcessSampler:
    """Kernel HWM plus sampled RSS; identity changes are never combined."""

    def __init__(
        self,
        pid: int,
        interval: float = 0.05,
        reader: Callable[[int], Any] = proc_sample,
    ):
        self.pid, self.interval, self.reader = pid, interval, reader
        self.first = self.last = None
        self.samples = self.rss_peak = self.hwm_peak = 0
        self.identity_changed = False
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def sample(self) -> None:
        row = self.reader(self.pid)
        if row is None:
            return
        if self.first is not None and row["start_ticks"] != self.first["start_ticks"]:
            self.identity_changed = True
            return
        self.first = self.first or row
        self.last = row
        self.samples += 1
        self.rss_peak = max(self.rss_peak, row.get("VmRSS_bytes", 0))
        self.hwm_peak = max(self.hwm_peak, row.get("VmHWM_bytes", 0))

    def __enter__(self):
        self.sample()

        def poll():
            while not self._stop.wait(self.interval):
                self.sample()

        self._thread = threading.Thread(target=poll, daemon=True)
        self._thread.start()
        return self

    def finish(self) -> dict[str, Any]:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        self.sample()  # Capture VmHWM while the owned server still exists.
        first, last = self.first or {}, self.last or {}
        io_before, io_after = first.get("io"), last.get("io")
        return {
            "pid": self.pid,
            "process_start_ticks": first.get("start_ticks"),
            "samples": self.samples,
            "sampled_peak_rss_bytes": self.rss_peak or None,
            "kernel_peak_hwm_bytes": self.hwm_peak or None,
            "identity_changed": self.identity_changed,
            "cpu_user_ticks_delta": max(
                0, last.get("cpu_user_ticks", 0) - first.get("cpu_user_ticks", 0)
            ),
            "cpu_system_ticks_delta": max(
                0, last.get("cpu_system_ticks", 0) - first.get("cpu_system_ticks", 0)
            ),
            "io_delta": {
                key: max(0, io_after[key] - io_before.get(key, 0)) for key in io_after
            }
            if io_before is not None and io_after is not None
            else None,
            "scope": "local process counters only; HWM includes process lifetime; IO is not remote object-store traffic",
        }

    def __exit__(self, *_):
        self.finish()


@contextmanager
def phase_resources(server_pid: int | None):
    python = ProcessSampler(os.getpid())
    server = ProcessSampler(server_pid) if server_pid is not None else None
    result: dict[str, Any] = {}
    python.__enter__()
    if server:
        server.__enter__()
    try:
        yield result
    finally:
        result["python"] = python.finish()
        result["python"]["resource_ru_maxrss_bytes"] = (
            int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024
        )
        result["pgwire"] = server.finish() if server else None


# Reuse the original entry points and result consumption unchanged. In
# particular ordinary singles project only id, and SQL batch parallelism keeps
# its native default 0 (auto); Python's legacy parallelism=1 is not a SQL SET.
OptimizationAdapter = base.Adapter


def save_groups(
    directory: Path, phase: str, groups: Any, recalls: Sequence[float]
) -> dict[str, Any]:
    if len(groups.qids) != len(groups.ids) or len(recalls) != len(groups.qids):
        raise ProtocolError("cannot persist partial result groups")
    if groups.distances is not None and len(groups.distances) != len(groups.qids):
        raise ProtocolError("cannot persist partial distance groups")
    path = directory / f"{phase}.results.jsonl.gz"
    with (
        path.open("xb") as raw,
        gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream,
    ):
        for index, (qid, ids, recall) in enumerate(
            zip(groups.qids, groups.ids, recalls)
        ):
            row = {
                "query_id": qid,
                "ids": ids,
                "ranks": list(range(1, base.K + 1)),
                "recall": recall,
            }
            if groups.distances is not None:
                row["distances"] = groups.distances[index]
            stream.write((json.dumps(row, allow_nan=False) + "\n").encode())
    return {
        "path": str(path),
        "sha256": base.file_hash(path),
        "query_groups": len(groups.qids),
        "query_ids_sha256": canonical_hash(groups.qids),
        "distances_present": groups.distances is not None,
    }


def measure(
    adapter: Any,
    dataset: str,
    truth: Any,
    *,
    clock: Callable[[], float] = time.perf_counter,
    save: Callable[..., Any] | None = None,
    resource_scope: Callable[..., Any] = phase_resources,
    server_pid: int | None = None,
    progress: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    spec = base.DATASETS[dataset]
    singles, full_ids = base.real_query_indices(
        dataset, adapter.query_count, len(truth)
    )
    latencies, recalls = [], []
    combined = base.Groups([], [], None)
    with resource_scope(server_pid) as single_resources:
        for qid in singles:
            prepared = adapter.prepare_single(qid)
            raw, elapsed = base.timed(prepared, clock)
            groups = prepared.decode(raw)
            recalls.extend(base.validate_groups(groups, [qid], truth, spec.rows))
            combined.qids.extend(groups.qids)
            combined.ids.extend(groups.ids)
            if groups.distances is not None:
                if combined.distances is None:
                    combined.distances = []
                combined.distances.extend(groups.distances)
            latencies.append(elapsed)
            if progress and len(latencies) % 25 == 0:
                progress(f"single {len(latencies)}/{base.SINGLE_NQ}")
    wall = math.fsum(latencies)
    single = {
        "nq": len(singles),
        "completed_vectors": len(singles),
        "ordinary_single_calls": len(singles),
        "wall_seconds": wall,
        "qps": len(singles) / wall,
        "first_ms": latencies[0] * 1000,
        "latencies_ms": [v * 1000 for v in latencies],
        "recall_per_query": recalls,
        "recall": math.fsum(recalls) / len(recalls),
        "resources": single_resources,
    }
    single.update(
        {
            name: base.percentile(single["latencies_ms"], q)
            for name, q in (("p50_ms", 0.5), ("p90_ms", 0.9), ("p99_ms", 0.99))
        }
    )
    if save:
        single["result_artifact"] = save("single", combined, recalls)
    if progress:
        progress(f"full native batch {len(full_ids)} start")
    prepared = adapter.prepare_batch(full_ids, full=True)
    with resource_scope(server_pid) as batch_resources:
        raw, elapsed = base.timed(prepared, clock)
    groups = prepared.decode(raw)
    recalls = base.validate_groups(groups, full_ids, truth, spec.rows)
    batch = {
        "nq": len(full_ids),
        "completed_vectors": len(full_ids),
        "native_calls": 1,
        "wall_seconds": elapsed,
        "qps": len(full_ids) / elapsed,
        "mean_ms": elapsed * 1000 / len(full_ids),
        "recall": math.fsum(recalls) / len(recalls),
        "recall_per_query": recalls,
        "resources": batch_resources,
    }
    if save:
        batch["result_artifact"] = save("full", groups, recalls)
    if progress:
        progress(f"full native batch completed in {elapsed:.3f}s")
    samples, chunks = [], []
    with resource_scope(server_pid) as chunk_resources:
        for start in range(0, len(full_ids), base.CHUNK_NQ):
            qids = full_ids[start : start + base.CHUNK_NQ]
            prepared = adapter.prepare_batch(qids, full=False)
            raw, seconds = base.timed(prepared, clock)
            groups = prepared.decode(raw)
            recalls = base.validate_groups(groups, qids, truth, spec.rows)
            chunk = {
                "first_query_id": qids[0],
                "nq": len(qids),
                "wall_seconds": seconds,
                "recall": math.fsum(recalls) / len(recalls),
                "recall_per_query": recalls,
            }
            if save:
                chunk["result_artifact"] = save(f"chunk-{start:05d}", groups, recalls)
            chunks.append(chunk)
            samples.extend([seconds * 1000 / len(qids)] * len(qids))
            if progress and (
                len(chunks) % 10 == 0 or start + len(qids) == len(full_ids)
            ):
                progress(f"separate chunk sampling {start + len(qids)}/{len(full_ids)}")
    batch["percentile_sampling"] = {
        "outside_full_timing": True,
        "method": "chunk-normalized",
        "chunk": base.CHUNK_NQ,
        "native_calls": len(chunks),
        "completed_vectors": len(samples),
        "wall_seconds": math.fsum(c["wall_seconds"] for c in chunks),
        "chunks": chunks,
        "resources": chunk_resources,
        **{
            name: base.percentile(samples, q)
            for name, q in (("p50_ms", 0.5), ("p90_ms", 0.9), ("p99_ms", 0.99))
        },
    }
    return {"single": single, "batch": batch}


class OptimizationStore(base.ArrowStore):
    def inventory(self, prefix: str) -> list[dict[str, Any]]:
        files = self.list_files(prefix)
        infos = self.fs.get_file_info(files) if files else []
        return [
            {"path": info.path, "size": info.size, "mtime_ns": info.mtime_ns}
            for info in sorted(infos, key=lambda item: item.path)
        ]


def catalog_connection_kwargs(env: dict[str, str] | None = None) -> dict[str, Any]:
    """Parse the primary metadata URL without importing installed LakeSoul."""
    from urllib.parse import parse_qsl, unquote, urlsplit

    env = os.environ if env is None else env
    url = env.get("LAKESOUL_PG_URL", "").removeprefix("jdbc:")
    parsed = urlsplit(url)
    if (
        parsed.scheme not in ("postgres", "postgresql")
        or not parsed.hostname
        or not parsed.path.strip("/")
    ):
        raise ProtocolError(
            "catalog fingerprint needs a primary LAKESOUL_PG_URL database"
        )
    user = env.get("LAKESOUL_PG_USERNAME") or (
        unquote(parsed.username) if parsed.username else None
    )
    password = env.get("LAKESOUL_PG_PASSWORD")
    if password is None and parsed.password is not None:
        password = unquote(parsed.password)
    if not user or password is None:
        raise ProtocolError("catalog fingerprint requires PostgreSQL username/password")
    result = {
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "dbname": unquote(parsed.path.strip("/")),
        "user": user,
        "password": password,
    }
    # JDBC-only options, notably stringtype=unspecified, are not libpq options.
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        if key in (
            "sslmode",
            "sslcert",
            "sslkey",
            "sslrootcert",
            "connect_timeout",
            "application_name",
        ):
            result[key] = value
    return result


def normalized_index_prefix(path: str) -> str:
    from urllib.parse import urlsplit

    parsed = urlsplit(path)
    if parsed.scheme in ("s3", "s3a"):
        return parsed.path.lstrip("/").rstrip("/")
    return path.removeprefix("file://").rstrip("/")


def metadata_json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def catalog_control_plane_pin(
    config: dict[str, Any], *, connect: Callable[..., Any] | None = None
) -> dict[str, Any]:
    """Pin stable owner/table/partition/current-index metadata in one read-only snapshot.

    Never import lakesoul in the supervisor and never consult read/write leases
    or query counters. Shard root matching is literal and slash-delimited, not
    LIKE: table names and paths contain underscores that are SQL wildcards.
    """
    if connect is None:
        import psycopg

        connect = psycopg.connect
    kwargs = catalog_connection_kwargs()
    connection = connect(**kwargs)
    try:
        cursor = connection.cursor()
        cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        tables, partitions = [], []
        for role, name_key, path_key in (
            ("target", "table", "target_path"),
            ("queries", "query_table", "query_path"),
        ):
            cursor.execute(
                "SELECT table_id, table_namespace, table_name, table_path, properties, table_schema, partitions "
                "FROM table_info WHERE table_namespace=%s AND table_name=%s",
                ("default", config[name_key]),
            )
            rows = cursor.fetchall()
            if len(rows) != 1:
                raise ProtocolError(
                    f"expected exactly one owned {role} table metadata row"
                )
            table_id, namespace, name, path, properties, schema, partition_spec = rows[
                0
            ]
            properties = metadata_json(properties)
            if (
                namespace != "default"
                or name != config[name_key]
                or path.rstrip("/") != config[path_key].rstrip("/")
            ):
                raise ProtocolError(
                    f"{role} namespace/name/path differs from the owned manifest"
                )
            if (
                not isinstance(properties, dict)
                or properties.get("native_batch_owner") != config["token"]
            ):
                raise ProtocolError(f"{role} metadata owner token mismatch")
            cursor.execute(
                "SELECT table_id FROM table_name_id WHERE table_namespace=%s AND table_name=%s",
                (namespace, name),
            )
            name_ids = cursor.fetchall()
            cursor.execute(
                "SELECT table_id FROM table_path_id WHERE table_path=%s", (path,)
            )
            if name_ids != [(table_id,)] or cursor.fetchall() != [(table_id,)]:
                raise ProtocolError(
                    f"{role} name/path mappings disagree with its table_id"
                )
            tables.append(
                {
                    "role": role,
                    "table_id": table_id,
                    "namespace": namespace,
                    "name": name,
                    "path": path,
                    "properties": properties,
                    "schema_sha256": hashlib.sha256(schema.encode()).hexdigest(),
                    "partition_spec": partition_spec,
                }
            )
            cursor.execute(
                "SELECT DISTINCT ON (partition_desc) partition_desc, version, snapshot::text, commit_op, expression "
                "FROM partition_info WHERE table_id=%s ORDER BY partition_desc, version DESC",
                (table_id,),
            )
            partitions.append(
                {"role": role, "current": [list(row) for row in cursor.fetchall()]}
            )
        root = normalized_index_prefix(tables[0]["path"]) + "/_vector_index/vec/"
        cursor.execute(
            "SELECT sh.index_prefix, sh.shard_id, c.commit_id, c.generation, c.version, c.header, c.segments, c.mutation_head, c.mutation_records "
            "FROM index_shard sh LEFT JOIN LATERAL (SELECT commit_id, generation, version, header, segments, mutation_head, mutation_records "
            "FROM index_commit WHERE shard_id=sh.shard_id ORDER BY generation DESC, version DESC LIMIT 1) c ON true "
            "WHERE sh.kind=%s AND left(sh.index_prefix, length(%s))=%s ORDER BY sh.index_prefix",
            ("vector", root, root),
        )
        commits, seen = [], set()
        for (
            prefix,
            shard_id,
            commit_id,
            generation,
            version,
            header,
            segments,
            mutation_head,
            mutation_records,
        ) in cursor.fetchall():
            relative = prefix.removeprefix(root).rstrip("/")
            parts = relative.split("/")
            if (
                not prefix.startswith(root)
                or prefix in seen
                or len(parts) != 2
                or any(part in ("", ".", "..") for part in parts)
            ):
                raise ProtocolError(
                    "catalog shard prefix escapes or duplicates the exact owned vector root"
                )
            if parts[0] != "-5" or parts[1] not in ("0", "1", "2", "3"):
                raise ProtocolError(
                    "catalog shard does not belong to this benchmark's unpartitioned four buckets"
                )
            if commit_id is None or not header:
                raise ProtocolError(
                    "catalog shard has no current complete commit/header pin"
                )
            seen.add(prefix)
            segments = metadata_json(segments)
            if not isinstance(segments, list) or not segments:
                raise ProtocolError("catalog shard has no cumulative segment pin")
            for segment in segments:
                filename = (
                    segment.get("filename") if isinstance(segment, dict) else None
                )
                if (
                    not isinstance(filename, str)
                    or not filename
                    or Path(filename).name != filename
                    or filename in (".", "..")
                ):
                    raise ProtocolError(
                        "catalog segment filename must stay inside its exact owned shard"
                    )
                for file in segment.get("data_files", []):
                    normalized = normalized_index_prefix(file)
                    target = normalized_index_prefix(config["target_path"]) + "/"
                    if (
                        not normalized.startswith(target)
                        or ".." in Path(normalized.removeprefix(target)).parts
                    ):
                        raise ProtocolError(
                            "catalog segment references a data file outside the owned target"
                        )
            commits.append(
                {
                    "prefix": prefix,
                    "shard_id": int(shard_id),
                    "commit_id": int(commit_id),
                    "generation": int(generation),
                    "version": int(version),
                    "header_sha256": hashlib.sha256(bytes(header)).hexdigest(),
                    "segments": segments,
                    "mutation_head": mutation_head,
                    "mutation_records": int(mutation_records),
                }
            )
        cursor.execute(
            "SELECT index_prefix, state, drop_id::text FROM index_drop "
            "WHERE kind=%s AND left(index_prefix, length(%s))=%s ORDER BY index_prefix",
            ("vector", root, root),
        )
        drops = [list(row) for row in cursor.fetchall()]
        if drops:
            raise ProtocolError("owned vector index has a catalog drop tombstone")
        connection.rollback()
        identity = {key: kwargs[key] for key in ("host", "port", "dbname", "user")}
        stable = {
            "database": identity,
            "tables": tables,
            "partitions": partitions,
            "index_root": root,
            "current_commits": commits,
            "drops": drops,
        }
        return {"sha256": canonical_hash(stable), **stable}
    finally:
        connection.close()


def physical_index_fingerprint(
    store: Any,
    config: dict[str, Any],
    *,
    catalog_reader: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    prefix = f"{base.BUCKET}/{base.VERSION}/{config['token']}/{config['dataset']}"
    reader = catalog_reader or catalog_control_plane_pin
    catalog_pin = reader(config)
    inventory = store.inventory(prefix)
    if not inventory or not any("/_vector_index/" in row["path"] for row in inventory):
        raise ProtocolError("owned dataset has no physical vector index")
    manifests, files = [], {}
    for row in inventory:
        path = row["path"]
        if (
            not path.startswith(prefix + "/")
            or ".." in Path(path.removeprefix(prefix + "/")).parts
            or path in files
        ):
            raise ProtocolError(
                "physical inventory escaped or duplicated the owned object prefix"
            )
        files[path] = row
        if "/_vector_index/" in path and (
            path.endswith("/LATEST") or "/manifests/" in path
        ):
            manifests.append(
                {"path": path, "sha256": hashlib.sha256(store.read(path)).hexdigest()}
            )
    if reader(config) != catalog_pin:
        raise ProtocolError(
            "catalog control plane changed while the physical inventory was being pinned"
        )
    commits = catalog_pin.get("current_commits", [])
    if commits:
        backend = "catalog"
        for commit in commits:
            for segment in commit["segments"]:
                object_path = (
                    base.BUCKET
                    + "/"
                    + commit["prefix"].rstrip("/")
                    + "/"
                    + segment["filename"]
                )
                if (
                    object_path not in files
                    or files[object_path]["size"] != segment["file_size"]
                ):
                    raise ProtocolError(
                        "catalog segment pin is absent from or differs in size from the physical inventory"
                    )
    elif any(row["path"].endswith("/LATEST") for row in manifests) and any(
        "/manifests/" in row["path"] for row in manifests
    ):
        backend = "s3-manifest"
    else:
        raise ProtocolError(
            "physical index has neither current catalog commit pins nor complete S3 manifest pins"
        )
    data = {
        "owned_prefix": prefix,
        "storage_backend": backend,
        "inventory": inventory,
        "index_manifests": manifests,
        "catalog_control_plane": catalog_pin,
    }
    return {"sha256": canonical_hash(data), **data}


def assert_physical_index(
    store: Any, config: dict[str, Any], expected: dict[str, Any]
) -> None:
    if physical_index_fingerprint(store, config)["sha256"] != expected["sha256"]:
        raise ProtocolError(
            "physical target/query/index changed between A/B trials; comparison refused"
        )


def process_identity(pid: int) -> dict[str, Any]:
    row = proc_sample(pid)
    return {"pid": pid, "start_ticks": row["start_ticks"] if row else None}


def ensure_exited(directory: Path) -> None:
    for marker in directory.rglob("owned-process.json"):
        recorded = json.loads(marker.read_text())
        if base.process_group_alive(recorded["pid"]):
            current = proc_sample(recorded["pid"])
            if (
                current is None
                or recorded.get("start_ticks") is None
                or current["start_ticks"] == recorded["start_ticks"]
            ):
                raise ProtocolError(
                    f"owned process {recorded['pid']} has not exited; no cleanup/resume"
                )


def stop_recorded_processes(directory: Path) -> None:
    """Resume/cleanup after supervisor death without adopting unrelated PIDs."""
    for marker in directory.rglob("owned-process.json"):
        if marker.is_symlink() or directory.resolve() not in marker.resolve().parents:
            raise ProtocolError("owned process marker escaped its run directory")
        recorded = json.loads(marker.read_text())
        pid = base.integer(recorded["pid"], "owned process pid")
        if pid <= 1:
            raise ProtocolError("refuse invalid owned process identity")
        previous_exit = marker.with_name("process.json")
        if (
            previous_exit.exists()
            and json.loads(previous_exit.read_text()).get("process_group_exited")
            is True
        ):
            continue
        current = proc_sample(pid)
        if current is None or current.get("start_ticks") != recorded.get("start_ticks"):
            continue  # Original process exited; never signal a reused PID.
        if recorded.get("start_ticks") is None or os.getpgid(pid) != pid:
            raise ProtocolError("cannot prove ownership of the original process group")
        os.killpg(pid, signal.SIGTERM)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            current = proc_sample(pid)
            if current is None or current.get("start_ticks") != recorded["start_ticks"]:
                break
            time.sleep(0.05)
        else:
            current = proc_sample(pid)
            if (
                current is not None
                and current.get("start_ticks") == recorded["start_ticks"]
            ):
                os.killpg(pid, signal.SIGKILL)
        deadline = time.monotonic() + 5
        while base.process_group_alive(pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        if base.process_group_alive(pid):
            raise ProtocolError(
                "owned process group still alive after recovery; retain all dataset resources"
            )
        recovery = {
            "pid": pid,
            "start_ticks": recorded["start_ticks"],
            "process_group_exited": True,
            "reason": "recovered orphan after supervisor interruption",
        }
        base.dump_new(
            marker.with_name("recovered-" + uuid.uuid4().hex + ".json"), recovery
        )


def run_child(
    command: list[str], env: dict[str, str], log_dir: Path, timeout: float
) -> None:
    log_dir.mkdir(exist_ok=False)
    with (
        (log_dir / "stdout.log").open("xb") as stdout,
        (log_dir / "stderr.log").open("xb") as stderr,
    ):
        process = subprocess.Popen(
            command,
            env=env,
            cwd=base.REPO,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        base.dump_new(log_dir / "owned-process.json", process_identity(process.pid))
        sampler = ProcessSampler(process.pid)
        sampler.__enter__()
        code = None
        try:
            code = process.wait(timeout=timeout)
        finally:
            peaks = sampler.finish()
            base.stop_process(process)
            base.dump_new(
                log_dir / "process.json",
                {
                    "pid": process.pid,
                    "returncode": process.returncode,
                    "process_group_exited": True,
                    "resources": peaks,
                },
            )
    if code:
        raise ProtocolError(f"worker exited {code}; inspect {log_dir / 'stderr.log'}")


@contextmanager
def owned_server(
    config: dict[str, Any],
    variant: dict[str, Any],
    env: dict[str, str],
    directory: Path,
):
    verify_variant(variant)
    for port in (config["port"], config["metrics_port"]):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", port))
    options = base.options(config)
    command = [
        variant["pg_binary"],
        "--bind",
        "127.0.0.1",
        "-p",
        str(config["port"]),
        "--metrics-addr",
        f"127.0.0.1:{config['metrics_port']}",
        "--worker-threads",
        "4",
        "--warehouse-prefix",
        f"s3://{base.BUCKET}",
        "--s3-bucket",
        base.BUCKET,
        "--s3-access-key",
        options["fs.s3a.access.key"],
        "--s3-secret-key",
        options["fs.s3a.secret.key"],
        "--endpoint",
        config["endpoint"],
    ]
    directory.mkdir(exist_ok=False)
    with (
        (directory / "stdout.log").open("xb") as stdout,
        (directory / "stderr.log").open("xb") as stderr,
    ):
        process = subprocess.Popen(
            command,
            env=env,
            cwd=base.REPO,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        base.dump_new(directory / "owned-process.json", process_identity(process.pid))
        sampler = ProcessSampler(process.pid)
        sampler.__enter__()
        try:
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    raise ProtocolError("owned pgwire exited during startup")
                try:
                    with socket.create_connection(
                        ("127.0.0.1", config["port"]), timeout=0.2
                    ):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise ProtocolError("owned pgwire did not become ready")
                    time.sleep(0.1)
            yield process.pid
        finally:
            peaks = sampler.finish()  # Read kernel HWM BEFORE terminating PG.
            base.stop_process(process)
            base.dump_new(
                directory / "process.json",
                {
                    "pid": process.pid,
                    "returncode": process.returncode,
                    "process_group_exited": True,
                    "resources": peaks,
                },
            )


def worker(config_path: Path, stage: str, record_path: Path) -> None:
    config = json.loads(config_path.read_text())
    started_utc = datetime.now(timezone.utc).isoformat()
    base.validate_config(config)
    if config["harness_sha256"] != base.file_hash(Path(__file__)) or config[
        "helpers_sha256"
    ] != base.file_hash(HELPER_PATH):
        raise ProtocolError(
            "benchmark harness/helpers changed after the run was frozen"
        )
    variant = config["variant_artifact"]
    if (
        stage == "query"
        and os.environ.get("LAKESOUL_VECTOR_MEMORY_BYTES") != config["vector_memory"]
    ):
        raise ProtocolError(
            "query worker vector-memory environment differs from the frozen protocol"
        )
    server_pid = (
        int(os.environ["PG_SERVICE_PID"])
        if stage == "query" and config.get("api") == "pgwire"
        else None
    )
    provenance = loaded_provenance(variant, pid=server_pid)
    if stage == "query":
        queries, truth, dataset_input = base.load_queries(config)
        adapter = OptimizationAdapter(config, queries)
        try:
            adapter.preflight()
            measured = measure(
                adapter,
                config["dataset"],
                truth,
                server_pid=server_pid,
                save=lambda *values: save_groups(record_path.parent, *values),
                progress=lambda message: print(message, file=sys.stderr, flush=True),
            )
        finally:
            adapter.close()
        record = {
            "kind": "query_optimization",
            "case": config["case"],
            "dataset_input": dataset_input,
            "physical_index_sha256": config["physical_index_sha256"],
            **measured,
        }
    else:
        record = {
            "setup": base.setup_dataset,
            "build": base.build_index,
            "cleanup": base.cleanup_catalog,
        }[stage](config)
    # Detect artifact replacement or wrapper drift during the worker as well.
    if loaded_provenance(variant, pid=server_pid) != provenance:
        raise ProtocolError("variant changed during the measured process")
    base.dump_new(
        record_path,
        {
            "protocol_version": VERSION,
            "dataset": config["dataset"],
            "status": "complete",
            "worker_pid": os.getpid(),
            "started_utc": started_utc,
            "finished_utc": datetime.now(timezone.utc).isoformat(),
            "harness_sha256": config["harness_sha256"],
            "helpers_sha256": config["helpers_sha256"],
            "provenance": provenance,
            **record,
        },
    )


def execute_stage(
    config: dict[str, Any], stage: str, out: Path, env: dict[str, str], timeout: float
) -> dict[str, Any]:
    out.mkdir(exist_ok=False)
    job, record = out / "job.json", out / "record.json"
    base.dump_new(job, config)
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        stage,
        "--job",
        str(job),
        "--record",
        str(record),
    ]
    run_child(command, env, out / "process", timeout)
    return json.loads(record.read_text())


def result_artifacts(record: dict[str, Any]) -> list[tuple[dict[str, Any], list[int]]]:
    count = base.DATASETS[record["dataset"]].batch_nq
    rows = [
        (record["single"]["result_artifact"], list(range(base.SINGLE_NQ))),
        (record["batch"]["result_artifact"], list(range(count))),
    ]
    rows.extend(
        (
            chunk["result_artifact"],
            list(range(chunk["first_query_id"], chunk["first_query_id"] + chunk["nq"])),
        )
        for chunk in record["batch"]["percentile_sampling"]["chunks"]
    )
    return rows


def validate_complete_record(
    record: dict[str, Any],
    case: dict[str, Any],
    variant: dict[str, Any],
    physical_sha: str,
    attempt: Path,
) -> None:
    if (
        record.get("status") != "complete"
        or record.get("case") != case
        or record.get("protocol_version") != VERSION
    ):
        raise ProtocolError("partial/mismatched case cannot count as a completed trial")
    if record.get("harness_sha256") != base.file_hash(Path(__file__)) or record.get(
        "helpers_sha256"
    ) != base.file_hash(HELPER_PATH):
        raise ProtocolError("completed case used a different benchmark source protocol")
    provenance = record.get("provenance", {})
    for key in (
        "source_tree_sha256",
        "python_sources_sha256",
        "build_receipt_sha256",
        "pg_binary_sha256",
    ):
        if provenance.get(key) != variant[key]:
            raise ProtocolError(f"completed case has different {key}")
    if (
        provenance.get("loaded_extension_sha256") != variant["extension_sha256"]
        or provenance.get("loaded_extension") != variant["extension"]
    ):
        raise ProtocolError("completed case did not load the requested extension")
    if record.get("physical_index_sha256") != physical_sha:
        raise ProtocolError(
            "cannot resume cases measured on a different physical index"
        )
    full_nq = base.DATASETS[record["dataset"]].batch_nq
    for phase, count in (
        (record["single"], base.SINGLE_NQ),
        (record["batch"], full_nq),
    ):
        if phase.get("nq") != count or phase.get("completed_vectors") != count:
            raise ProtocolError("incomplete vector count cannot count as a trial")
        seconds = phase.get("wall_seconds", 0)
        if (
            not isinstance(seconds, (int, float))
            or not math.isfinite(seconds)
            or seconds <= 0
        ):
            raise ProtocolError("invalid phase wall time")
        if not math.isclose(phase.get("qps", 0), count / seconds, rel_tol=1e-10):
            raise ProtocolError(
                "QPS is not completed vector count divided by phase wall time"
            )
    if (
        record["single"].get("ordinary_single_calls") != base.SINGLE_NQ
        or record["batch"].get("native_calls") != 1
    ):
        raise ProtocolError("single/full call counts differ from the protocol")
    chunks = record["batch"]["percentile_sampling"]
    if (
        not chunks.get("outside_full_timing")
        or chunks.get("completed_vectors") != full_nq
        or chunks.get("native_calls") != full_nq // base.CHUNK_NQ
    ):
        raise ProtocolError(
            "percentile sampling is incomplete or included in full timing"
        )
    expected_starts = list(range(0, full_nq, base.CHUNK_NQ))
    if [
        chunk.get("first_query_id") for chunk in chunks.get("chunks", [])
    ] != expected_starts or any(
        chunk.get("nq") != base.CHUNK_NQ for chunk in chunks["chunks"]
    ):
        raise ProtocolError("percentile calls must cover each real query exactly once")
    if not math.isclose(
        chunks.get("wall_seconds", 0),
        math.fsum(chunk["wall_seconds"] for chunk in chunks["chunks"]),
        rel_tol=1e-10,
    ):
        raise ProtocolError(
            "percentile timer differs from its separately measured chunks"
        )
    for phase in (record["single"], record["batch"], chunks):
        measurements = phase.get("resources", {})
        python = measurements.get("python") or {}
        if (
            python.get("identity_changed")
            or not python.get("samples")
            or not python.get("kernel_peak_hwm_bytes")
            or not python.get("resource_ru_maxrss_bytes")
        ):
            raise ProtocolError(
                "completed phase lacks valid Python peak memory measurements"
            )
        if case["api"] == "pgwire":
            server = measurements.get("pgwire") or {}
            if (
                server.get("identity_changed")
                or not server.get("samples")
                or not server.get("kernel_peak_hwm_bytes")
                or not server.get("sampled_peak_rss_bytes")
            ):
                raise ProtocolError(
                    "completed pgwire phase lacks peak memory sampled while its server was alive"
                )
    for phase_index, (artifact, qids) in enumerate(result_artifacts(record)):
        path = Path(artifact["path"])
        if (
            path.is_symlink()
            or path.resolve().parent != attempt.resolve()
            or base.file_hash(path) != artifact["sha256"]
        ):
            raise ProtocolError(
                "completed case result artifact is missing/changed/outside its attempt"
            )
        if artifact.get("query_groups") != len(qids) or artifact.get(
            "query_ids_sha256"
        ) != canonical_hash(qids):
            raise ProtocolError(
                "completed artifact query counts do not match its phase"
            )
        with gzip.open(path, "rt") as stream:
            for expected, line in zip(qids, stream):
                row = json.loads(line)
                if (
                    row.get("query_id") != expected
                    or len(row.get("ids", [])) != base.K
                    or len(set(row["ids"])) != base.K
                    or row.get("ranks") != list(range(1, base.K + 1))
                ):
                    raise ProtocolError("invalid saved qid/IDs/ranks")
                if any(
                    base.integer(key, "result id") < 0
                    or key >= base.DATASETS[record["dataset"]].rows
                    for key in row["ids"]
                ):
                    raise ProtocolError("saved result ID is outside the target dataset")
                if not 0 <= row.get("recall", -1) <= 1:
                    raise ProtocolError("invalid saved recall")
                if case["api"] != "python" and phase_index > 0:
                    distances = row.get("distances", [])
                    if (
                        len(distances) != base.K
                        or any(not math.isfinite(v) for v in distances)
                        or any(a > b for a, b in zip(distances, distances[1:]))
                    ):
                        raise ProtocolError(
                            "SQL artifacts require complete finite ascending distances"
                        )
            # Do not accept zip truncation, empty files, or extra rows.
        with gzip.open(path, "rt") as stream:
            if sum(1 for _ in stream) != len(qids):
                raise ProtocolError(
                    "saved result artifact has an incomplete/extra query count"
                )


def case_completed(
    directory: Path, case: dict[str, Any], variant: dict[str, Any], physical_sha: str
) -> dict[str, Any] | None:
    completed = (
        sorted(directory.glob("attempt-*/completed.json")) if directory.exists() else []
    )
    if len(completed) > 1:
        raise ProtocolError(
            "multiple completed records for the same trial would double count it"
        )
    if not completed:
        return None
    receipt = json.loads(completed[0].read_text())
    attempt = completed[0].parent
    record_path = attempt / "query/record.json"
    if base.file_hash(record_path) != receipt["record_sha256"]:
        raise ProtocolError("completed raw record changed")
    ensure_exited(attempt)
    worker_exit = receipt.get("worker_process", {})
    if (
        worker_exit.get("process_group_exited") is not True
        or worker_exit.get("returncode") != 0
    ):
        raise ProtocolError("completed case lacks a successful joined worker exit")
    if case["api"] == "pgwire":
        server_exit = receipt.get("server_process", {})
        if server_exit.get("process_group_exited") is not True:
            raise ProtocolError(
                "completed pgwire case lacks joined server exit evidence"
            )
    record = json.loads(record_path.read_text())
    validate_complete_record(record, case, variant, physical_sha, record_path.parent)
    return record


def run_case(
    config: dict[str, Any],
    case: dict[str, Any],
    variant: dict[str, Any],
    physical: dict[str, Any],
    output: Path,
    timeout: float,
) -> dict[str, Any]:
    directory = output / "cases" / case["id"]
    directory.mkdir(parents=True, exist_ok=True)
    completed = case_completed(directory, case, variant, physical["sha256"])
    if completed is not None:
        return completed
    verify_variant(variant)
    attempt = directory / ("attempt-" + uuid.uuid4().hex)
    attempt.mkdir()
    cache = (
        Path(config["scratch"])
        / "cache"
        / f"{case['api']}_{case['rerank']}_{case['nprobe']}"
    )
    cache.mkdir(parents=True, exist_ok=False)
    base.dump_new(cache / base.MARKER, {"token": config["token"], "case": case["id"]})
    current = {
        **config,
        **{key: case[key] for key in ("api", "rerank", "nprobe")},
        "case": case,
        "variant_artifact": variant,
        "physical_index_sha256": physical["sha256"],
    }
    env = variant_environment(
        variant, cache, "query", vector_memory=config["vector_memory"]
    )
    try:
        if case["api"] == "pgwire":
            with owned_server(config, variant, env, attempt / "server") as pid:
                record = execute_stage(
                    current,
                    "query",
                    attempt / "query",
                    {**env, "PG_SERVICE_PID": str(pid)},
                    timeout,
                )
        else:
            record = execute_stage(current, "query", attempt / "query", env, timeout)
        ensure_exited(attempt)
        validate_complete_record(
            record, case, variant, physical["sha256"], attempt / "query"
        )
        receipt = {
            "case": case,
            "record_sha256": base.file_hash(attempt / "query/record.json"),
            "physical_index_sha256": physical["sha256"],
            "worker_process": json.loads(
                (attempt / "query/process/process.json").read_text()
            ),
        }
        if case["api"] == "pgwire":
            receipt["server_process"] = json.loads(
                (attempt / "server/process.json").read_text()
            )
        base.dump_new(attempt / "completed.json", receipt)
        return record
    except BaseException as error:
        base.dump_new(
            attempt / "failed.json",
            {
                "case": case,
                "error": str(error),
                "traceback": traceback.format_exc(),
                "partial_results_count_as_completed": False,
            },
        )
        raise
    finally:
        ensure_exited(attempt)
        if cache.exists():
            base.remove_owned_cache(
                cache,
                Path(config["scratch"]),
                Path(config["scratch_parent"]),
                config["token"],
                config["dataset"],
            )


def dataset_config(
    manifest: dict[str, Any], dataset: str, token: str
) -> dict[str, Any]:
    prefix = f"s3://{base.BUCKET}/{base.VERSION}/{token}/{dataset}"
    table = f"nb2_{token[:20]}_{dataset}"
    return {
        "dataset": dataset,
        "token": token,
        "scratch_parent": manifest["scratch_parent"],
        "scratch": str(Path(manifest["scratch_parent"]) / f"nbatch_{token}_{dataset}"),
        "table": table,
        "query_table": table + "_queries",
        "target_path": prefix + "/target",
        "query_path": prefix + "/queries",
        "endpoint": manifest["endpoint"],
        "data_dir": manifest["data_dir"],
        "port": manifest["port"],
        "metrics_port": manifest["metrics_port"],
        "variant_artifact": manifest["variants"]["A"],
        "vector_memory": manifest.get("vector_memory", "512MiB"),
        "harness_sha256": manifest.get(
            "harness_sha256", base.file_hash(Path(__file__))
        ),
        "helpers_sha256": manifest.get("helpers_sha256", base.file_hash(HELPER_PATH)),
    }


def cleanup_epoch(
    config: dict[str, Any], epoch: Path, store: Any, timeout: float
) -> dict[str, Any]:
    base.validate_config(config)
    stop_recorded_processes(epoch)
    ensure_exited(epoch)
    variant = config["variant_artifact"]
    cache = Path(config["scratch"]) / "cleanup-cache"
    cleanup_config = {
        **config,
        "harness_sha256": base.file_hash(Path(__file__)),
        "helpers_sha256": base.file_hash(HELPER_PATH),
    }
    metadata = execute_stage(
        cleanup_config,
        "cleanup",
        epoch / ("cleanup-" + uuid.uuid4().hex),
        variant_environment(variant, cache, "cleanup"),
        timeout,
    )
    ensure_exited(epoch)
    objects = base.OwnedObjects(store, config["token"], config["dataset"]).cleanup()
    base.remove_owned_root(
        Path(config["scratch"]),
        Path(config["scratch_parent"]),
        config["token"],
        config["dataset"],
    )
    return {
        "metadata": metadata,
        "objects": objects,
        "scratch_absent": not Path(config["scratch"]).exists(),
    }


def no_regression_decision(pairs: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """Reject observed regressions independently; gains cannot offset losses.

    All paired trials must pass for every API/mode/probe configuration. This
    has zero regression tolerance and makes no statistical-significance claim.
    A completed experiment is not an accepted improvement.
    """
    groups: dict[tuple[Any, ...], set[int]] = {}
    failures = []
    for pair in pairs:
        key = (pair["api"], pair["rerank"], pair["nprobe"])
        groups.setdefault(key, set()).add(pair["trial"])
        checks = [
            (phase + "_qps_ratio_B_over_A", False) for phase in ("single", "batch")
        ]
        checks.extend(
            (metric, True)
            for metric in (
                "single_first_ms_ratio_B_over_A",
                "single_warm_p95_ms_ratio_B_over_A",
            )
            if pair.get(metric) is not None
        )
        for metric, latency in checks:
            ratio = pair.get(metric)
            if (
                not isinstance(ratio, (int, float))
                or not math.isfinite(ratio)
                or ratio <= 0
                or (ratio > 1.0 if latency else ratio < 1.0)
            ):
                failures.append(
                    {
                        "api": key[0],
                        "rerank": key[1],
                        "nprobe": key[2],
                        "trial": pair["trial"],
                        "metric": metric,
                        "ratio_B_over_A": ratio,
                    }
                )
    enough_pairs = bool(groups) and all(len(trials) >= 3 for trials in groups.values())
    return {
        "version": "strict-observed-no-regression-v1",
        "accepted": enough_pairs and not failures,
        "minimum_paired_trials_met": enough_pairs,
        "regression_tolerance": 0.0,
        "failures": failures,
        "policy": "Every paired ordinary-single and full-batch path must pass; no cross-path, cross-configuration, or cross-trial offsetting. Available first-query and warm-P95 latency ratios must not increase.",
    }


def paired_summary(
    records: Sequence[dict[str, Any]], cases: Sequence[dict[str, Any]]
) -> dict[str, Any]:
    if len(records) != len(cases) or {row["case"]["id"] for row in records} != {
        case["id"] for case in cases
    }:
        raise ProtocolError("summary needs exactly all completed variant/trial cases")
    grouped: dict[tuple[Any, ...], dict[str, dict[str, Any]]] = {}
    for row in records:
        case = row["case"]
        key = (case["api"], case["rerank"], case["nprobe"], case["trial"])
        grouped.setdefault(key, {})[case["variant"]] = row
    pairs = []
    grouped_ratios: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for (api, rerank, probe, trial), variants in grouped.items():
        if set(variants) != {"A", "B"}:
            raise ProtocolError("incomplete A/B pair")
        pairs.append(
            {
                "api": api,
                "rerank": rerank,
                "nprobe": probe,
                "trial": trial,
                **{
                    phase + "_qps_ratio_B_over_A": variants["B"][phase]["qps"]
                    / variants["A"][phase]["qps"]
                    for phase in ("single", "batch")
                },
                **{
                    phase + "_recall_delta_B_minus_A": variants["B"][phase]["recall"]
                    - variants["A"][phase]["recall"]
                    for phase in ("single", "batch")
                },
            }
        )
        a_single, b_single = variants["A"]["single"], variants["B"]["single"]
        for metric, a, b in (
            (
                "single_first_ms_ratio_B_over_A",
                a_single.get("first_ms"),
                b_single.get("first_ms"),
            ),
            (
                "single_warm_p95_ms_ratio_B_over_A",
                base.percentile(a_single["latencies_ms"][1:], 0.95)
                if len(a_single.get("latencies_ms", [])) > 1
                else None,
                base.percentile(b_single["latencies_ms"][1:], 0.95)
                if len(b_single.get("latencies_ms", [])) > 1
                else None,
            ),
        ):
            pairs[-1][metric] = b / a if a and b else None
        for phase in ("single", "batch"):
            for process in ("python", "pgwire"):

                def peak(variant):
                    values = (
                        variants[variant][phase].get("resources", {}).get(process) or {}
                    )
                    return max(
                        values.get("kernel_peak_hwm_bytes") or 0,
                        values.get("resource_ru_maxrss_bytes") or 0,
                        values.get("sampled_peak_rss_bytes") or 0,
                    )

                a, b = peak("A"), peak("B")
                pairs[-1][phase + "_" + process + "_peak_rss_ratio_B_over_A"] = (
                    b / a if a and b else None
                )
    # Report all pairs and robust point summaries; these are not significance
    # claims and never replace the raw timings or recall measurements.
    for pair in pairs:
        grouped_ratios.setdefault(
            (pair["api"], pair["rerank"], pair["nprobe"]), []
        ).append(pair)
    aggregates = []
    for (api, rerank, probe), rows in grouped_ratios.items():
        keys = [
            key
            for key in rows[0]
            if "_ratio_B_over_A" in key or "_delta_B_minus_A" in key
        ]
        stats = {}
        for key in keys:
            values = sorted(row[key] for row in rows if row[key] is not None)
            if values:
                stats[key] = {
                    "median": base.percentile(values, 0.5),
                    "min": min(values),
                    "max": max(values),
                    "pairs": len(values),
                }
        aggregates.append(
            {
                "api": api,
                "rerank": rerank,
                "nprobe": probe,
                "paired_trials": len(rows),
                "statistics": stats,
                "no_regression": no_regression_decision(rows),
            }
        )
    return {
        "paired_results": pairs,
        "aggregates": aggregates,
        "case_count": len(records),
        "complete": True,
        "no_regression": no_regression_decision(pairs),
        "note": "Experiment completion is separate from improvement acceptance. Ratios compare the same physical index/config/trial; raw phase timings remain authoritative.",
    }


def run_dataset(
    manifest: dict[str, Any], dataset: str, run: Path, store: Any, timeout: float
) -> dict[str, Any]:
    output = run / dataset
    output.mkdir(exist_ok=True)
    summary_path = output / "dataset-complete.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())
        if not summary.get("cleanup", {}).get("scratch_absent"):
            raise ProtocolError("completed dataset lacks verified cleanup")
        base.check_token(summary["epoch"])
        epoch = output / "epochs" / summary["epoch"]
        physical = json.loads((epoch / "physical-index.json").read_text())
        if physical["sha256"] != summary["physical_index_sha256"]:
            raise ProtocolError("completed dataset physical-index provenance changed")
        for case in manifest["schedule"][dataset]:
            if (
                case_completed(
                    epoch / "cases" / case["id"],
                    case,
                    manifest["variants"][case["variant"]],
                    physical["sha256"],
                )
                is None
            ):
                raise ProtocolError(
                    "completed dataset is missing validated case evidence"
                )
        return summary
    epochs = output / "epochs"
    epochs.mkdir(exist_ok=True)
    active = []
    for epoch in sorted(epochs.iterdir()):
        config = json.loads((epoch / "dataset.json").read_text())
        if Path(config["scratch"]).exists():
            active.append((epoch, config))
    if len(active) > 1:
        raise ProtocolError("more than one live physical index exists for one dataset")
    if active:
        epoch, config = active[0]
        base.validate_config(config)
        stop_recorded_processes(epoch)
        ensure_exited(epoch)
        cache_parent = Path(config["scratch"]) / "cache"
        if cache_parent.exists():
            for cache in cache_parent.iterdir():
                base.remove_owned_cache(
                    cache,
                    Path(config["scratch"]),
                    Path(config["scratch_parent"]),
                    config["token"],
                    dataset,
                )
        if not (epoch / "physical-index.json").exists():
            # Interrupted setup/build has no comparable frozen index. Clean it
            # and start a new epoch; no completed trials from it are adopted.
            cleaned = cleanup_epoch(config, epoch, store, timeout)
            base.dump_new(
                epoch / "retired.json",
                {"reason": "interrupted setup/build", "cleanup": cleaned},
            )
            active = []
    if not active:
        token = uuid.uuid4().hex
        epoch = epochs / token
        epoch.mkdir()
        config = dataset_config(manifest, dataset, token)
        base.dump_new(epoch / "dataset.json", config)
        root = Path(config["scratch"])
        root.mkdir(exist_ok=False)
        base.dump_new(root / base.MARKER, {"token": token, "dataset": dataset})
        base.OwnedObjects(store, token, dataset).create()
        variant = manifest["variants"]["A"]
        for stage in ("setup", "build"):
            execute_stage(
                config,
                stage,
                epoch / stage,
                variant_environment(variant, root / (stage + "-cache"), stage),
                timeout,
            )
        physical = physical_index_fingerprint(store, config)
        base.dump_new(epoch / "physical-index.json", physical)
    else:
        physical = json.loads((epoch / "physical-index.json").read_text())
    records = []
    for case in manifest["schedule"][dataset]:
        assert_physical_index(store, config, physical)
        print(
            f"{dataset} {case['id']}: ordinary singles=200, full={base.DATASETS[dataset].batch_nq}",
            file=sys.stderr,
            flush=True,
        )
        record = run_case(
            config,
            case,
            manifest["variants"][case["variant"]],
            physical,
            epoch,
            timeout,
        )
        assert_physical_index(store, config, physical)
        records.append(record)
    summary = {
        "dataset": dataset,
        "epoch": config["token"],
        "physical_index_sha256": physical["sha256"],
        "results": paired_summary(records, manifest["schedule"][dataset]),
    }
    summary["cleanup"] = cleanup_epoch(config, epoch, store, timeout)
    base.dump_new(summary_path, summary)
    return summary


def cleanup_run(
    manifest: dict[str, Any], run: Path, store: Any, timeout: float
) -> None:
    for dataset in manifest["datasets"]:
        epochs = run / dataset / "epochs"
        if not epochs.exists():
            continue
        for epoch in sorted(epochs.iterdir()):
            config = json.loads((epoch / "dataset.json").read_text())
            if Path(config["scratch"]).exists():
                result = cleanup_epoch(config, epoch, store, timeout)
                base.dump_new(
                    epoch / ("manual-cleanup-" + uuid.uuid4().hex + ".json"), result
                )


def arguments(argv: Sequence[str] | None = None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=list(base.DATASETS),
        default=list(base.DATASETS),
    )
    parser.add_argument("--apis", nargs="+", choices=APIS, default=list(APIS))
    parser.add_argument("--rerank", nargs="+", choices=RERANKS, default=list(RERANKS))
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument(
        "--vector-memory",
        default="512MiB",
        help="L1 vector memory for both variants during query stages (default: 512MiB)",
    )
    parser.add_argument("--seed", type=int, default=20261114)
    parser.add_argument("--baseline-python-package", type=Path)
    parser.add_argument("--baseline-pg-binary", type=Path)
    parser.add_argument("--baseline-build-receipt", type=Path)
    parser.add_argument("--candidate-python-package", type=Path)
    parser.add_argument("--candidate-pg-binary", type=Path)
    parser.add_argument("--candidate-build-receipt", type=Path)
    parser.add_argument(
        "--data-dir", type=Path, default=Path("/home/zhibin/ann-benchmarks")
    )
    parser.add_argument(
        "--scratch-root",
        type=Path,
        default=Path("/tmp/lakesoul-vector-optimization/scratch"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp/lakesoul-vector-optimization/results"),
    )
    parser.add_argument("--endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--port", type=int, default=16543)
    parser.add_argument("--metrics-port", type=int, default=19190)
    parser.add_argument("--timeout-seconds", type=float, default=21600)
    parser.add_argument("--resume", type=Path)
    parser.add_argument("--cleanup-run", type=Path)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument(
        "--worker",
        choices=["setup", "build", "query", "cleanup"],
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--job", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--record", type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    for key in ("datasets", "apis", "rerank"):
        if len(set(getattr(args, key))) != len(getattr(args, key)):
            parser.error(f"--{key} must not contain duplicates")
    if args.trials < 3:
        parser.error("--trials must be at least 3")
    try:
        vector_memory_bytes(args.vector_memory)
    except ProtocolError as error:
        parser.error(str(error))
    if args.resume and args.cleanup_run:
        parser.error("--resume and --cleanup-run are mutually exclusive")
    if args.timeout_seconds <= 0 or not math.isfinite(args.timeout_seconds):
        parser.error("timeout must be finite and positive")
    if args.port == args.metrics_port or not all(
        0 < value < 65536 for value in (args.port, args.metrics_port)
    ):
        parser.error("pgwire and metrics ports must be distinct valid ports")
    if not (args.resume or args.cleanup_run or args.worker):
        for name in ("baseline", "candidate"):
            if any(
                getattr(args, key) is None
                for key in (
                    name + "_python_package",
                    name + "_pg_binary",
                    name + "_build_receipt",
                )
            ):
                parser.error(
                    f"new runs require all three --{name}-* artifact arguments"
                )
    return args


def supervisor(args: Any) -> Path:
    existing = args.resume or args.cleanup_run
    if existing:
        run = existing.resolve(strict=True)
        manifest = json.loads((run / "manifest.json").read_text())
        if manifest["protocol"]["version"] != VERSION:
            raise ProtocolError("resume/cleanup needs this harness protocol")
        if not args.cleanup_run and (
            manifest["harness_sha256"] != base.file_hash(Path(__file__))
            or manifest["helpers_sha256"] != base.file_hash(HELPER_PATH)
        ):
            raise ProtocolError(
                "harness/helpers changed since manifest; do not mix measurement protocols"
            )
        scratch = Path(manifest["scratch_parent"])
    else:
        scratch = args.scratch_root.resolve()
        scratch.mkdir(parents=True, exist_ok=True)
        variants = {
            name: asdict(fingerprint_variant(name, package, binary, receipt))
            for name, package, binary, receipt in (
                (
                    "A",
                    args.baseline_python_package,
                    args.baseline_pg_binary,
                    args.baseline_build_receipt,
                ),
                (
                    "B",
                    args.candidate_python_package,
                    args.candidate_pg_binary,
                    args.candidate_build_receipt,
                ),
            )
        }
        if (
            variants["A"]["extension"] == variants["B"]["extension"]
            or variants["A"]["pg_binary"] == variants["B"]["pg_binary"]
        ):
            raise ProtocolError(
                "A and B must use independent artifact paths, even for an A/A validation"
            )
        token = uuid.uuid4().hex
        run = (
            args.output_dir.resolve()
            / f"{VERSION}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{token}"
        )
        manifest = {
            "protocol": methodology(args.trials, args.seed, args.vector_memory),
            "variants": variants,
            "run_token": token,
            "vector_memory": args.vector_memory,
            "variant_build_receipts": {
                name: json.loads(Path(value["build_receipt"]).read_text())
                for name, value in variants.items()
            },
            "datasets": args.datasets,
            "scratch_parent": str(scratch),
            "data_dir": str(args.data_dir.resolve()),
            "endpoint": args.endpoint,
            "port": args.port,
            "metrics_port": args.metrics_port,
            "harness_sha256": base.file_hash(Path(__file__)),
            "helpers_sha256": base.file_hash(HELPER_PATH),
            "schedule": {
                name: schedule(name, args.apis, args.rerank, args.trials, args.seed)
                for name in args.datasets
            },
        }
        if args.plan_only:
            print(json.dumps(manifest, indent=2))
            return run
        run.mkdir(parents=True, exist_ok=False)
        base.dump_new(run / "manifest.json", manifest)
    for variant in manifest["variants"].values():
        verify_variant(variant)
    # Share the old runner's scratch-root lock as well as this protocol's lock.
    # Neither performance supervisor may run concurrently on the same root.
    with (
        (scratch / ".native-batch.lock").open("a+") as old_lock,
        (scratch / ".optimization.lock").open("a+") as lock,
    ):
        for handle in (old_lock, lock):
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise ProtocolError(
                    "another performance supervisor owns the scratch root"
                ) from error
        store = OptimizationStore(
            manifest["endpoint"],
            os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
            os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        if args.cleanup_run:
            cleanup_run(manifest, run, store, args.timeout_seconds)
            return run
        summaries = []
        try:
            for dataset in manifest["datasets"]:
                summaries.append(
                    run_dataset(manifest, dataset, run, store, args.timeout_seconds)
                )
        except BaseException as error:
            base.dump_new(
                run / ("interrupted-" + uuid.uuid4().hex + ".json"),
                {
                    "error": str(error),
                    "traceback": traceback.format_exc(),
                    "resume_command": f"{sys.executable} {Path(__file__)} --resume {run}",
                    "cleanup_command": f"{sys.executable} {Path(__file__)} --cleanup-run {run}",
                    "partial_results_count_as_completed": False,
                },
            )
            raise
        if not (run / "run-complete.json").exists():
            base.dump_new(
                run / "run-complete.json",
                {
                    "protocol": VERSION,
                    "datasets": summaries,
                    "complete": True,
                    "selected_configurations_accepted_no_regression": bool(summaries)
                    and all(
                        summary.get("results", {})
                        .get("no_regression", {})
                        .get("accepted")
                        is True
                        for summary in summaries
                    ),
                },
            )
    return run


def main(argv: Sequence[str] | None = None) -> int:
    args = arguments(argv)
    if args.worker:
        worker(args.job, args.worker, args.record)
    else:

        def interrupted(_signum, _frame):
            raise KeyboardInterrupt("optimization supervisor received SIGTERM")

        signal.signal(signal.SIGTERM, interrupted)
        print(supervisor(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
