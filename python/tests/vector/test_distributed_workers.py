# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Integration test: the coordinator session executes on real workers.

Skipped unless ``LAKESOUL_WORKER_URLS`` lists running ``lakesoul-worker``
endpoints, for example::

    cargo build --release -p lakesoul-datafusion --bin lakesoul-worker
    LAKESOUL_PG_URL=... ./target/release/lakesoul-worker \\
        --bind 127.0.0.1 --port 50061 --metrics-addr 127.0.0.1:19091 &
    LAKESOUL_PG_URL=... ./target/release/lakesoul-worker \\
        --bind 127.0.0.1 --port 50062 --metrics-addr 127.0.0.1:19092 &

    LAKESOUL_WORKER_URLS='grpc://127.0.0.1:50061,grpc://127.0.0.1:50062' \\
    LAKESOUL_WORKER_METRICS='http://127.0.0.1:19091/metrics,http://127.0.0.1:19092/metrics' \\
    pytest tests/vector/test_distributed_workers.py

The test compares the coordinated query results with a single-node session and,
when ``LAKESOUL_WORKER_METRICS`` is set, checks that the workers actually
executed scan/merge streams (their ``lakesoul_execution_*`` counters grow).
"""

from __future__ import annotations

import os
import shutil
import urllib.request

import numpy as np
import pyarrow as pa
import pytest

pytest.importorskip(
    "lakesoul._lib._session",
    reason="extension built without --features ci-unify",
)

from lakesoul import LakeSoulCatalog, LakeSoulSession  # noqa: E402

WORKER_URLS = [
    url.strip()
    for url in os.environ.get("LAKESOUL_WORKER_URLS", "").split(",")
    if url.strip()
]
WORKER_METRICS = [
    url.strip()
    for url in os.environ.get("LAKESOUL_WORKER_METRICS", "").split(",")
    if url.strip()
]
pytestmark = pytest.mark.skipif(
    not WORKER_URLS,
    reason="set LAKESOUL_WORKER_URLS to run the distributed-worker integration test",
)

os.environ.setdefault("LAKESOUL_CACHE", "1")
os.environ.setdefault("LAKESOUL_CACHE_PATH", "/tmp/lakesoul_workers_cache")
os.environ.setdefault("LAKESOUL_VECTOR_BLOCK_ROWS", "256")

DIM = 8
N = 120
DAY_COUNT = 3


def _catalog() -> LakeSoulCatalog:
    return LakeSoulCatalog(
        pg_url=os.environ.get(
            "LAKESOUL_PG_URL",
            "postgresql://lakesoul_test:lakesoul_test@localhost:5432/lakesoul_test",
        ),
        pg_username=os.environ.get("LAKESOUL_PG_USERNAME", "lakesoul_test"),
        pg_password=os.environ.get("LAKESOUL_PG_PASSWORD", "lakesoul_test"),
    )


def _executed_rows(metrics_url: str) -> float:
    with urllib.request.urlopen(metrics_url, timeout=3) as response:  # noqa: S310
        text = response.read().decode("utf-8", "replace")
    total = 0.0
    for line in text.splitlines():
        if line.startswith("lakesoul_execution_output_rows_total"):
            try:
                total += float(line.rsplit(" ", 1)[-1])
            except ValueError:
                continue
    return total


def test_distributed_workers_execute_queries() -> None:
    """Coordinated search/aggregate match single-node and hit the workers."""
    catalog = _catalog()
    table_name = "py_session_worker_e2e"
    table_path = "/tmp/lakesoul_workers_e2e/table"
    shutil.rmtree("/tmp/lakesoul_workers_e2e", ignore_errors=True)
    os.makedirs("/tmp/lakesoul_workers_e2e", exist_ok=True)
    try:
        catalog.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    rng = np.random.default_rng(73)
    vectors = rng.normal(size=(N, DIM)).astype(np.float32)
    days = [f"2026-03-0{index + 1}" for index in range(DAY_COUNT)]
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), False),
            pa.field("dt", pa.string(), False),
            pa.field("vec", pa.list_(pa.field("item", pa.float32()), DIM), False),
        ]
    )
    table = catalog.create_table(
        table_name,
        path=f"file://{table_path}",
        schema=schema,
        primary_keys=["id"],
        partition_by=["dt"],
        hash_bucket_num=2,
        vector_index=[
            {
                "column": "vec",
                "dim": DIM,
                "nlist": 4,
                "total_bits": 1,
                "metric": "L2",
            }
        ],
    )
    try:
        table.write_arrow(
            pa.table(
                {
                    "id": pa.array(range(N), type=pa.int64()),
                    "dt": pa.array([days[index % DAY_COUNT] for index in range(N)]),
                    "vec": pa.FixedSizeListArray.from_arrays(
                        pa.array(vectors.reshape(-1), type=pa.float32()), DIM
                    ),
                }
            )
        )
        query = rng.normal(size=DIM).astype(np.float32)
        single = catalog.session(vector_search={"nprobe": 4, "exact_overfetch": 30})
        expected_ids = (
            table.search(query, top_k=5, session=single).column("id").to_pylist()
        )
        expected_count = (
            single.sql(f'SELECT count(*) AS cnt FROM "lakesoul".default.{table_name}')
            .column("cnt")
            .to_pylist()
        )

        before = (
            sum(_executed_rows(url) for url in WORKER_METRICS)
            if WORKER_METRICS
            else None
        )
        distributed = LakeSoulSession.from_env(
            vector_search={"nprobe": 4, "exact_overfetch": 30},
            distributed={"worker_urls": WORKER_URLS, "target_partitions": 4},
        )
        try:
            returned = (
                table.search(query, top_k=5, session=distributed)
                .column("id")
                .to_pylist()
            )
            assert len(returned) == 5, returned
            assert len(set(returned) & set(expected_ids)) >= 4, (
                returned,
                expected_ids,
            )
            counts = (
                distributed.sql(
                    f'SELECT count(*) AS cnt FROM "lakesoul".default.{table_name}'
                )
                .column("cnt")
                .to_pylist()
            )
            assert counts == expected_count, (counts, expected_count)
            grouped = distributed.sql(
                f'SELECT dt, count(*) AS cnt FROM "lakesoul".default.{table_name} '
                "GROUP BY dt ORDER BY dt"
            ).to_pydict()
            assert sorted(grouped["dt"]) == sorted(days)
            assert sum(grouped["cnt"]) == N
        finally:
            distributed.close()

        if before is not None and WORKER_METRICS:
            after = sum(_executed_rows(url) for url in WORKER_METRICS)
            assert after > before, (
                "the coordinated queries did not run on the workers: "
                f"executed-row counters stayed at {after}"
            )
    finally:
        catalog.drop_table(table_name, if_exists=True)
        shutil.rmtree("/tmp/lakesoul_workers_e2e", ignore_errors=True)
