# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa

from lakesoul.ray.read_lakesoul import read_lakesoul, _LakeSoulDatasourceReader

from .conftest import TABLE_NAME_PART


def test_parallelism_1(ray_session, part_arrow_table):
    ds = read_lakesoul(TABLE_NAME_PART, parallelism=1)
    assert ds.count() == part_arrow_table.num_rows

    batches = list(ds.iter_batches(batch_format="pyarrow"))
    ray_table = pa.Table.from_batches(batches)
    assert ray_table.num_rows == part_arrow_table.num_rows


def test_parallelism_default(ray_session, part_arrow_table):
    ds = read_lakesoul(TABLE_NAME_PART)
    assert ds.count() == part_arrow_table.num_rows

    batches = list(ds.iter_batches(batch_format="pyarrow"))
    ray_table = pa.Table.from_batches(batches)
    assert ray_table.num_rows == part_arrow_table.num_rows


def test_parallelism_oversubscribed(ray_session, part_arrow_table, part_data_files):
    n_files = len(part_data_files)
    ds = read_lakesoul(TABLE_NAME_PART, parallelism=n_files * 2)
    assert ds.count() == part_arrow_table.num_rows

    batches = list(ds.iter_batches(batch_format="pyarrow"))
    ray_table = pa.Table.from_batches(batches)
    assert ray_table.num_rows == part_arrow_table.num_rows


def test_schema_consistent_across_parallelism(ray_session, part_schema):
    """Schema should be invariant regardless of parallelism setting."""
    schemas = []
    for p in [1, None]:
        ds = read_lakesoul(TABLE_NAME_PART, parallelism=p)
        schemas.append(ds.schema())

    for s in schemas:
        for rf, af in zip(s, part_schema):
            assert rf.type == af.type


def test_get_read_tasks_count(ray_session, part_data_files):
    """get_read_tasks should return one ReadTask per data file."""
    reader = _LakeSoulDatasourceReader(TABLE_NAME_PART)
    tasks = reader.get_read_tasks(parallelism=1)
    assert len(tasks) == len(part_data_files)

    tasks = reader.get_read_tasks(parallelism=999)
    assert len(tasks) == len(part_data_files)


def test_read_task_schema_not_empty(ray_session):
    """Each ReadTask's BlockMetadata must have a non-empty schema."""
    reader = _LakeSoulDatasourceReader(TABLE_NAME_PART)
    tasks = reader.get_read_tasks(parallelism=1)
    for task in tasks:
        assert task.metadata.schema is not None
        assert len(task.metadata.schema) > 0
