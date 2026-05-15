# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import time

import pytest

from lakesoul.ray.read_lakesoul import read_lakesoul

from .conftest import TABLE_NAME_PART


pytestmark = pytest.mark.slow


def _measure_ray(batch_size, n_iter=3):
    times = []
    for _ in range(n_iter):
        ds = read_lakesoul(TABLE_NAME_PART, batch_size=batch_size)
        t0 = time.perf_counter()
        total = 0
        for batch in ds.iter_batches(batch_format="pyarrow"):
            total += batch.num_rows
        assert total == 20000
        times.append(time.perf_counter() - t0)
    return sorted(times)[1]  # median


def _measure_arrow(batch_size, n_iter=3):
    from lakesoul.arrow import lakesoul_dataset

    times = []
    for _ in range(n_iter):
        t0 = time.perf_counter()
        total = 0
        for batch in lakesoul_dataset(TABLE_NAME_PART, batch_size=batch_size).to_batches():
            total += batch.num_rows
        assert total == 20000
        times.append(time.perf_counter() - t0)
    return sorted(times)[1]  # median


def test_throughput_vs_arrow():
    """Ray overhead should be < 3x compared to Arrow reader."""
    ray_time = _measure_ray(4096)
    arrow_time = _measure_arrow(4096)
    overhead = ray_time / arrow_time

    print(f"\n  Ray: {ray_time:.3f}s, Arrow: {arrow_time:.3f}s, "
          f"overhead: {overhead:.2f}x")
    assert overhead < 3.0, (
        f"Ray overhead {overhead:.2f}x exceeds 3x threshold"
    )


def test_parallelism_performance():
    """Measure performance at different parallelism levels.

    In local mode, parallelism gains are limited, but higher parallelism
    should not drastically degrade performance.
    """
    from lakesoul.metadata.meta_ops import get_data_files_and_pks_by_table_name

    data_files, _ = get_data_files_and_pks_by_table_name(
        table_name=TABLE_NAME_PART,
        partitions={},
        namespace="default",
    )
    n_files = len(data_files)

    timings = {}
    for p_label, p_val in [("1", 1), ("n_files", n_files), ("2x_n_files", n_files * 2)]:
        times = []
        for _ in range(3):
            ds = read_lakesoul(TABLE_NAME_PART, batch_size=4096, parallelism=p_val)
            t0 = time.perf_counter()
            total = 0
            for batch in ds.iter_batches(batch_format="pyarrow"):
                total += batch.num_rows
            assert total == 20000
            times.append(time.perf_counter() - t0)
        timings[p_label] = sorted(times)[1]

    print(f"\n  Parallelism timings: { {k: f'{v:.3f}s' for k, v in timings.items()} }")
    # Parallel and serial should be within an order of magnitude
    assert timings["2x_n_files"] < timings["1"] * 10, (
        "Oversubscribed parallelism is drastically slower than serial"
    )
