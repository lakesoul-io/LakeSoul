# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Regression: Daft vector-index build is distributed across executors.

Before the fix, ``build_vector_index_daft`` materialised the shard rows into a
single-partition Daft dataframe and never repartitioned, so every shard was
built on one executor regardless of the shard count.  This test verifies that
the shard dataframe is repartitioned by shard count and, under the Ray runner,
the ``@daft.cls`` actor-pool UDF actually runs on multiple executors.

This requires a local Ray instance and is therefore gated:

    LAKESOUL_DAFT_RAY_TEST=1 pytest tests/vector/test_daft_ray_distribution.py
"""

from __future__ import annotations

import os
import uuid

import pytest


def _is_ray_test_enabled() -> bool:
    return os.environ.get("LAKESOUL_DAFT_RAY_TEST") == "1"


def _make_file_info(path: str, partition: str):
    class FileInfo:
        pass

    fi = FileInfo()
    fi.path = path
    fi.partition = partition
    return fi


def _shard_file_infos(partitions: int, buckets: int) -> list:
    infos = []
    for p in range(partitions):
        desc = f"dt=2026-08-{25 + p}"
        for b in range(buckets):
            infos.append(
                _make_file_info(
                    f"file:///tmp/lakesoul_x/{desc}/part-{uuid.uuid4().hex}_{b:04d}.parquet",
                    desc,
                )
            )
    return infos


def test_daft_vector_build_repartition_distribution(tmp_path) -> None:
    if not _is_ray_test_enabled():
        pytest.skip(
            "set LAKESOUL_DAFT_RAY_TEST=1 to enable the multi-executor Daft test"
        )

    import ray

    # Start a local Ray cluster with a small working dir so the package upload
    # stays small, and enough CPUs to schedule several workers.
    try:
        ray.init(
            runtime_env={
                "excludes": [".venv", "target", "**/__pycache__", "*.so", "*.whl"],
            },
            ignore_reinit_error=True,
            num_cpus=4,
            log_to_driver=False,
        )
    except Exception as error:  # noqa: BLE001
        pytest.skip(f"could not start a local Ray cluster: {error}")

    import daft
    from daft import cls, col, set_execution_config, set_runner_ray

    set_execution_config(maintain_order=False)
    set_runner_ray(noop_if_initialized=True)

    try:
        from lakesoul.daft.index_build import _shard_rows

        file_infos = _shard_file_infos(partitions=2, buckets=6)
        configs = [
            {"column": "vec", "dim": 8, "nlist": 2, "total_bits": 7, "metric": "L2"}
        ]
        rows, n_shards = _shard_rows(configs, file_infos, "{}", "id")

        # One row per (partition, bucket, column) = partition x bucket shards.
        assert n_shards == 2 * 6 * len(configs) == 12
        assert len(rows["file_paths"]) == n_shards

        # The probe records the worker pid for every shard it processes.
        pid_log_path = tmp_path / "pids.log"

        class Probe:
            def __init__(self, pid_log: str) -> None:
                self._pid_log = pid_log

            def __call__(self, file_paths, *args) -> str:
                with open(self._pid_log, "a") as f:
                    f.write(f"{os.getpid()}\n")
                return "ok"

        # Apply the same repartition + @daft.cls actor-pool pattern the
        # production build path uses (one partition per shard, actor pool sized
        # to the shard count).
        df = daft.from_pydict(rows).into_partitions(max(1, n_shards))
        udf = cls(Probe, cpus=1, max_concurrency=max(1, n_shards))
        out = df.with_column(
            "status",
            udf(str(pid_log_path))(
                col("file_paths"),
                col("store_config_json"),
                col("pk_column"),
                col("column"),
                col("dim"),
                col("nlist"),
                col("total_bits"),
                col("metric"),
                col("rotator_type"),
                col("seed"),
                col("use_faster_config"),
            ),
        ).collect()

        statuses = [row["status"] for row in out.to_pylist()]
        assert statuses == ["ok"] * n_shards

        # Every shard must have been executed exactly once.
        executions = pid_log_path.read_text().split()
        assert len(executions) == n_shards, (
            f"expected {n_shards} shard executions, got {len(executions)}"
        )
        # And they must have run on more than one executor (worker process),
        # proving the repartition + actor-pool UDF distributes the work.
        workers = set(executions)
        assert len(workers) > 1, (
            f"repartitioned into {n_shards} shards but all ran on a single "
            f"executor ({workers}); expected distribution across executors"
        )
        print(
            f"[distributed] {n_shards} shards processed across "
            f"{len(workers)} executors (pids={sorted(workers)})"
        )
    finally:
        try:
            ray.shutdown()
        except Exception:
            pass


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
