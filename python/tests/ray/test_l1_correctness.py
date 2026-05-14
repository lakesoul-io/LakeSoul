# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa

from lakesoul.ray.read_lakesoul import read_lakesoul
from lakesoul.arrow import lakesoul_dataset

from .conftest import TABLE_NAME_PART, TABLE_NAME_TEST_LFS


def test_row_count_part(ray_session):
    ds = read_lakesoul(TABLE_NAME_PART)
    assert ds.count() == 20000


def test_row_count_test_lfs(ray_session):
    ds = read_lakesoul(TABLE_NAME_TEST_LFS)
    assert ds.count() == 1


def test_schema_vs_arrow(ray_session, part_schema):
    ray_schema = read_lakesoul(TABLE_NAME_PART).schema()
    arrow_schema = part_schema

    assert len(ray_schema) == len(arrow_schema), (
        f"Column count mismatch: {len(ray_schema)} vs {len(arrow_schema)}"
    )
    assert ray_schema.names == arrow_schema.names, (
        f"Column names differ: {ray_schema.names} vs {arrow_schema.names}"
    )

    for i, (rf, af) in enumerate(zip(ray_schema, arrow_schema)):
        assert rf.type == af.type, (
            f"Type mismatch at field [{i}] '{rf.name}': {rf.type} vs {af.type}"
        )


def test_schema_decimal_precision(ray_session, part_schema):
    """Verify decimal columns retain precision (p_retailprice is decimal128(15,2))."""
    ray_schema = read_lakesoul(TABLE_NAME_PART).schema()
    arrow_schema = part_schema

    for rf, af in zip(ray_schema, arrow_schema):
        if pa.types.is_decimal(rf.type):
            assert rf.type.precision == af.type.precision, (
                f"Decimal precision mismatch for {rf.name}"
            )
            assert rf.type.scale == af.type.scale, (
                f"Decimal scale mismatch for {rf.name}"
            )


def test_data_values_vs_arrow(ray_session, part_arrow_table):
    ray_ds = read_lakesoul(TABLE_NAME_PART)
    batches = list(ray_ds.iter_batches(batch_format="pyarrow"))
    ray_table = pa.Table.from_batches(batches)

    arrow_table = part_arrow_table
    assert ray_table.num_rows == arrow_table.num_rows

    for col_name in arrow_table.column_names:
        ray_col = ray_table.column(col_name).combine_chunks()
        arrow_col = arrow_table.column(col_name).combine_chunks()
        # Compare after sorting because Ray may reorder across parallelism
        ray_sorted = pa.compute.sort_indices(ray_col)
        arrow_sorted = pa.compute.sort_indices(arrow_col)
        assert ray_col.take(ray_sorted) == arrow_col.take(arrow_sorted), (
            f"Column '{col_name}' values differ between Ray and Arrow"
        )


def test_output_format_iter_rows(ray_session):
    ds = read_lakesoul(TABLE_NAME_PART)
    rows = list(ds.iter_rows())
    assert len(rows) == 20000
    assert set(rows[0].keys()) == set(
        read_lakesoul(TABLE_NAME_PART).schema().names
    )


def test_output_format_iter_batches(ray_session, part_schema):
    ds = read_lakesoul(TABLE_NAME_PART)
    total_rows = 0
    expected_columns = part_schema.names
    for batch in ds.iter_batches(batch_format="pyarrow"):
        assert isinstance(batch, pa.RecordBatch)
        assert batch.schema.names == expected_columns
        total_rows += batch.num_rows
    assert total_rows == 20000


def test_output_full_arrow_table(ray_session, part_arrow_table):
    ray_ds = read_lakesoul(TABLE_NAME_PART)
    batches = list(ray_ds.iter_batches(batch_format="pyarrow"))
    ray_table = pa.Table.from_batches(batches)

    assert ray_table.num_rows == part_arrow_table.num_rows
    assert ray_table.num_columns == part_arrow_table.num_columns
