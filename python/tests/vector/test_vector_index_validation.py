# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Regression: vector index configuration is validated before metadata.

create_table(vector_index=...) must reject configurations the auto-build
cannot satisfy (no primary key, non-integer id, wrong vector column type or
dimension) *before* the table metadata is created — not after a data commit
when write_arrow/write_daft auto-build reads primary_keys[0].
"""

from __future__ import annotations

import os
import shutil

import numpy as np
import pyarrow as pa
import pytest

from lakesoul import LakeSoulCatalog

DIM = 8
NAMESPACE = "default"


def _catalog() -> LakeSoulCatalog:
    return LakeSoulCatalog(
        pg_url=os.environ.get(
            "LAKESOUL_PG_URL",
            "postgresql://lakesoul_test:lakesoul_test@localhost:5432/lakesoul_test",
        ),
        pg_username="lakesoul_test",
        pg_password="lakesoul_test",
        namespace=NAMESPACE,
    )


def _table_schema(
    *,
    pk_type: pa.DataType = pa.uint64(),
    vec_type: pa.DataType | None = None,
    vec_dim: int = DIM,
) -> pa.Schema:
    if vec_type is None:
        vec_type = pa.list_(pa.field("item", pa.float32()), vec_dim)
    return pa.schema(
        [
            pa.field("id", pk_type, False),
            pa.field("vec", vec_type, False),
        ]
    )


def _random_vectors_table(n_rows: int, dim: int = DIM) -> pa.Table:
    rng = np.random.default_rng(42)
    vectors = rng.random((n_rows, dim), dtype=np.float32)
    return pa.Table.from_arrays(
        [
            pa.array(range(n_rows), type=pa.uint64()),
            pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.flatten(), type=pa.float32()), dim
            ),
        ],
        schema=_table_schema(),
    )


def _assert_not_created(cat: LakeSoulCatalog, name: str) -> None:
    assert name not in cat.list_tables(NAMESPACE), (
        f"table '{name}' was created despite invalid vector index config"
    )


def test_create_table_rejects_vector_index_without_primary_key(tmp_path) -> None:
    cat = _catalog()
    name = "vec_val_no_pk"
    path = str(tmp_path / name)
    try:
        cat.drop_table(name, NAMESPACE, if_exists=True)
        with pytest.raises(ValueError, match="primary key"):
            cat.create_table(
                name,
                path=f"file://{path}",
                schema=_table_schema(),
                primary_keys=[],
                vector_index=[{"column": "vec", "dim": DIM}],
            )
        _assert_not_created(cat, name)
    finally:
        cat.drop_table(name, NAMESPACE, if_exists=True)


def test_create_table_rejects_non_integer_primary_key(tmp_path) -> None:
    cat = _catalog()
    name = "vec_val_str_pk"
    path = str(tmp_path / name)
    schema = pa.schema(
        [
            pa.field("id", pa.string(), False),
            pa.field("vec", pa.list_(pa.field("item", pa.float32()), DIM), False),
        ]
    )
    try:
        cat.drop_table(name, NAMESPACE, if_exists=True)
        with pytest.raises(ValueError, match="UInt64 or Int64"):
            cat.create_table(
                name,
                path=f"file://{path}",
                schema=schema,
                primary_keys=["id"],
                vector_index=[{"column": "vec", "dim": DIM}],
            )
        _assert_not_created(cat, name)
    finally:
        cat.drop_table(name, NAMESPACE, if_exists=True)


def test_create_table_rejects_missing_vector_column(tmp_path) -> None:
    cat = _catalog()
    name = "vec_val_missing_col"
    path = str(tmp_path / name)
    try:
        cat.drop_table(name, NAMESPACE, if_exists=True)
        with pytest.raises(ValueError, match="not found"):
            cat.create_table(
                name,
                path=f"file://{path}",
                schema=_table_schema(),
                primary_keys=["id"],
                vector_index=[{"column": "nope", "dim": DIM}],
            )
        _assert_not_created(cat, name)
    finally:
        cat.drop_table(name, NAMESPACE, if_exists=True)


def test_create_table_rejects_dim_mismatch(tmp_path) -> None:
    cat = _catalog()
    name = "vec_val_dim_mismatch"
    path = str(tmp_path / name)
    try:
        cat.drop_table(name, NAMESPACE, if_exists=True)
        with pytest.raises(ValueError, match="dim"):
            cat.create_table(
                name,
                path=f"file://{path}",
                schema=_table_schema(),
                primary_keys=["id"],
                vector_index=[{"column": "vec", "dim": 768}],
            )
        _assert_not_created(cat, name)
    finally:
        cat.drop_table(name, NAMESPACE, if_exists=True)


def test_create_table_rejects_non_float32_vectors(tmp_path) -> None:
    cat = _catalog()
    name = "vec_val_f64"
    path = str(tmp_path / name)
    try:
        cat.drop_table(name, NAMESPACE, if_exists=True)
        with pytest.raises(ValueError, match="Float32"):
            cat.create_table(
                name,
                path=f"file://{path}",
                schema=_table_schema(
                    vec_type=pa.list_(pa.field("item", pa.float64()), DIM)
                ),
                primary_keys=["id"],
                vector_index=[{"column": "vec", "dim": DIM}],
            )
        _assert_not_created(cat, name)
    finally:
        cat.drop_table(name, NAMESPACE, if_exists=True)


def test_create_table_rejects_scalar_vector_column(tmp_path) -> None:
    cat = _catalog()
    name = "vec_val_scalar"
    path = str(tmp_path / name)
    schema = pa.schema(
        [
            pa.field("id", pa.uint64(), False),
            pa.field("vec", pa.float32(), False),
        ]
    )
    try:
        cat.drop_table(name, NAMESPACE, if_exists=True)
        with pytest.raises(ValueError, match="FixedSizeList<Float32>"):
            cat.create_table(
                name,
                path=f"file://{path}",
                schema=schema,
                primary_keys=["id"],
                vector_index=[{"column": "vec", "dim": DIM}],
            )
        _assert_not_created(cat, name)
    finally:
        cat.drop_table(name, NAMESPACE, if_exists=True)


def test_create_table_validates_properties_route(tmp_path) -> None:
    """The raw JSON properties route is validated just like vector_index=."""
    import json

    cat = _catalog()
    name = "vec_val_properties"
    path = str(tmp_path / name)
    try:
        cat.drop_table(name, NAMESPACE, if_exists=True)
        with pytest.raises(ValueError, match="primary key"):
            cat.create_table(
                name,
                path=f"file://{path}",
                schema=_table_schema(),
                primary_keys=[],
                properties={
                    "vector_index_columns": json.dumps([{"column": "vec", "dim": DIM}])
                },
            )
        _assert_not_created(cat, name)
    finally:
        cat.drop_table(name, NAMESPACE, if_exists=True)


def test_create_table_valid_config_creates_and_auto_builds(tmp_path) -> None:
    """Happy path: valid config passes validation and write auto-builds."""
    import glob

    cat = _catalog()
    name = "vec_val_ok"
    path = str(tmp_path / name)
    try:
        cat.drop_table(name, NAMESPACE, if_exists=True)
        table = cat.create_table(
            name,
            path=f"file://{path}",
            schema=_table_schema(),
            primary_keys=["id"],
            hash_bucket_num=2,
            vector_index=[{"column": "vec", "dim": DIM, "nlist": 2, "total_bits": 7}],
        )
        table.write_arrow(_random_vectors_table(16))
        assert glob.glob(f"{path}/_vector_index/vec/**/LATEST", recursive=True), (
            "valid config should auto-build indexes on write_arrow"
        )
    finally:
        try:
            cat.drop_table(name, NAMESPACE, if_exists=True)
        except Exception:
            pass
        shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
