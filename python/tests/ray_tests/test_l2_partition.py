# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from lakesoul.ray.read_lakesoul import (
    LakeSoulDatasource,
)

from .conftest import TABLE_NAME_TEST_LFS, lakesoul_ray_dataset, lakesoul_scan


def test_use_partition(ray_session):
    ds = lakesoul_ray_dataset(TABLE_NAME_TEST_LFS, partitions={"c2": "1"})
    assert ds.count() == 2
    ds = lakesoul_ray_dataset(TABLE_NAME_TEST_LFS, partitions={"c2": "2"})
    assert ds.count() == 2
    ds = lakesoul_ray_dataset(TABLE_NAME_TEST_LFS, partitions={"c2": "3"})
    assert ds.count() == 3


def test_get_read_tasks_count(ray_session):
    """get_read_tasks should return one ReadTask per data file."""
    source = LakeSoulDatasource(
        lakesoul_scan(TABLE_NAME_TEST_LFS, partitions={"c2": "1"}).to_scan_config()
    )
    tasks = source.get_read_tasks(parallelism=1)
    assert len(tasks) > 1
