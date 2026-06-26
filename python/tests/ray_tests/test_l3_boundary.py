# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import decimal

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from .conftest import (
    TABLE_NAME_PART,
    TABLE_NAME_TEST_LFS,
    lakesoul_arrow_dataset,
    lakesoul_ray_dataset,
    lakesoul_scan,
)


class TestEmptyPartition:
    def test_empty_partition_no_error(self, ray_session):
        """Reading a partition that has no data should return an empty Dataset."""
        ds = lakesoul_ray_dataset(
            TABLE_NAME_TEST_LFS,
            partitions={"c2": "no_such_value"},
        )
        assert ds.count() == 0

    def test_empty_partition_schema_present(self, ray_session):
        """Empty dataset should still have the correct schema."""
        ds = lakesoul_ray_dataset(
            TABLE_NAME_TEST_LFS,
            partitions={"c2": "no_such_value"},
        )

        assert ds.count() == 0
        assert (
            ds.schema().base_schema
            == lakesoul_arrow_dataset(
                TABLE_NAME_TEST_LFS,
                retain_partition_columns=False,
            ).schema
        )


class TestBigDecimal:
    def test_p_retailprice_precision(self, ray_session, part_arrow_table):
        """p_retailprice is decimal128(15,2). Verify no precision loss."""
        ds = lakesoul_ray_dataset(TABLE_NAME_PART)
        batches = list(ds.iter_batches(batch_format="pyarrow"))
        ray_table = pa.concat_tables(batches)

        arrow_prices = part_arrow_table.column("p_retailprice").combine_chunks()
        ray_prices = ray_table.column("p_retailprice").combine_chunks()

        assert ray_prices.type == arrow_prices.type
        assert ray_prices.type.precision == 15
        assert ray_prices.type.scale == 2

    def test_decimal_values_match(self, ray_session, part_arrow_table):
        """All decimal values should exactly match Arrow reader."""
        ds = lakesoul_ray_dataset(TABLE_NAME_PART)
        batches = list(ds.iter_batches(batch_format="pyarrow"))
        ray_table = pa.concat_tables(batches)

        for col_name in part_arrow_table.column_names:
            col_type = part_arrow_table.schema.field(col_name).type
            if pa.types.is_decimal(col_type):
                ray_col = ray_table.column(col_name).combine_chunks()
                arrow_col = part_arrow_table.column(col_name).combine_chunks()
                assert ray_col == arrow_col, (
                    f"Decimal column '{col_name}' values differ"
                )


class TestParameterBoundaries:
    def test_batch_size_1(self, ray_session):
        ds = lakesoul_ray_dataset(TABLE_NAME_PART, batch_size=1)
        assert ds.count() == 20000

    def test_batch_size_large(self, ray_session):
        ds = lakesoul_ray_dataset(TABLE_NAME_PART, batch_size=100000)
        assert ds.count() == 20000

    def test_thread_count_8(self, ray_session):
        ds = lakesoul_ray_dataset(TABLE_NAME_PART, thread_count=8)
        assert ds.count() == 20000

    def test_thread_count_0(self, ray_session):
        ds = lakesoul_ray_dataset(TABLE_NAME_PART, thread_count=0)
        assert ds.count() == 20000

    def test_thread_count_negative(self):
        """thread_count=-1 should be rejected before Ray starts read tasks."""
        with pytest.raises(ValueError, match="thread_count"):
            lakesoul_scan(TABLE_NAME_PART, thread_count=-1)


class TestFilterExpressions:
    def test_simple_filter_matches_arrow_reader(self, ray_session):
        filter = pc.field("p_size") == 50
        ds = lakesoul_ray_dataset(
            TABLE_NAME_PART,
            columns=("p_name", "p_size"),
            filter=filter,
        )

        table = _ray_to_table(ds)

        assert table.num_rows == 392
        assert table.schema.names == ["p_name", "p_size"]
        assert table.column("p_size").combine_chunks().to_pylist() == [50] * 392

    def test_compound_decimal_filter_matches_arrow_reader(self, ray_session):
        threshold = pa.array(
            [decimal.Decimal("1500.00")],
            type=pa.decimal128(15, 2),
        )[0]
        filter = (pc.field("p_retailprice") >= threshold) & (
            pc.field("p_size") == 50
        )
        ds = lakesoul_ray_dataset(
            TABLE_NAME_PART,
            columns=("p_size", "p_retailprice"),
            filter=filter,
        )

        table = _ray_to_table(ds)

        assert table.num_rows == 176
        assert table.schema.names == ["p_size", "p_retailprice"]
        assert table.column("p_size").combine_chunks().to_pylist() == [50] * 176


class TestRetainPartitionColumns:
    def test_retain_false(self, ray_session):
        ds = lakesoul_ray_dataset(TABLE_NAME_PART, retain_partition_columns=False)
        schema = ds.schema()
        # Partition columns (like p_brand suffix or any col ending
        # with typical partition names) should NOT appear.
        # We verify schema matches Arrow reader with same setting.
        arrow_schema = lakesoul_arrow_dataset(
            TABLE_NAME_PART, retain_partition_columns=False
        ).schema
        assert schema.names == arrow_schema.names

    def test_retain_true(self, ray_session):
        ds = lakesoul_ray_dataset(TABLE_NAME_PART, retain_partition_columns=True)
        schema = ds.schema()

        arrow_schema = lakesoul_arrow_dataset(
            TABLE_NAME_PART, retain_partition_columns=True
        ).schema
        assert schema.names == arrow_schema.names


class TestExceptions:
    def test_nonexistent_table(self, ray_session):
        """Reading a non-existent table should raise a clear exception."""
        with pytest.raises(Exception):
            lakesoul_ray_dataset("no_such_table_xyz_123")

    def test_ray_import_required(self):
        """Verify that importing lakesoul.ray requires ray to be installed.

        Since ray is installed in this test environment, this test confirms
        the import works. When ray is not installed, the import should fail
        with ImportError at the `import ray` line in read_lakesoul.py.
        """
        # This is a smoke test that import works in our environment.
        import lakesoul.ray  # noqa: F401


class TestTypeCoverage:
    """Verify each Arrow type LakeSoul supports is read correctly."""

    def test_part_table_types(self, ray_session, part_schema):
        """part table covers: int32 (p_size), decimal128 (p_retailprice),
        utf8 (p_name, p_brand, p_container, p_comment), int64 (p_partkey)."""
        ds = lakesoul_ray_dataset(TABLE_NAME_PART)
        schema = ds.schema().base_schema

        type_map = {}
        for f in schema:
            type_map[f.name] = f.type

        # Verify specific types exist
        assert pa.types.is_decimal(type_map["p_retailprice"])
        assert pa.types.is_integer(type_map["p_partkey"])
        assert pa.types.is_integer(type_map["p_size"])
        assert pa.types.is_large_string(type_map["p_name"]) or pa.types.is_string(
            type_map["p_name"]
        )

    def test_test_lfs_types(self, ray_session):
        """Verify test_lfs table types are preserved."""
        ds = lakesoul_ray_dataset(TABLE_NAME_TEST_LFS)
        ray_schema = ds.schema().base_schema
        arrow_schema = lakesoul_arrow_dataset(TABLE_NAME_TEST_LFS).schema

        assert len(ray_schema) == len(arrow_schema)
        for rf, af in zip(ray_schema, arrow_schema):
            assert rf.type == af.type, (
                f"Type mismatch: {rf.name}: {rf.type} vs {af.type}"
            )


def _ray_to_table(ds) -> pa.Table:
    batches = list(ds.iter_batches(batch_format="pyarrow"))
    if not batches:
        schema = getattr(ds.schema(), "base_schema", ds.schema())
        return pa.Table.from_batches([], schema=schema)
    return pa.concat_tables(batches, promote_options="default")
