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
from uuid import uuid4

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


def test_create_table_accepts_string_primary_key(tmp_path) -> None:
    """Index keys are arrow-Row encoded, so non-integer keys are valid."""
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
        table = cat.create_table(
            name,
            path=f"file://{path}",
            schema=schema,
            primary_keys=["id"],
            vector_index=[{"column": "vec", "dim": DIM}],
        )
        assert table is not None
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


def test_create_table_rejects_unsupported_total_bits(tmp_path) -> None:
    """Only 1/3/7-bit RaBitQ widths have an estimator kernel."""
    cat = _catalog()
    name = "vec_val_bad_bits"
    path = str(tmp_path / name)
    try:
        with pytest.raises(ValueError, match="total_bits must be 1, 3 or 7"):
            cat.create_table(
                name,
                path=f"file://{path}",
                schema=_table_schema(),
                primary_keys=["id"],
                hash_bucket_num=2,
                vector_index=[
                    {"column": "vec", "dim": DIM, "nlist": 2, "total_bits": 5}
                ],
            )
    finally:
        try:
            cat.drop_table(name, NAMESPACE, if_exists=True)
        except Exception:
            pass
        shutil.rmtree(path, ignore_errors=True)


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
        assert glob.glob(f"{path}/_vector_index/vec/**/*.seg", recursive=True), (
            "valid config should auto-build indexes on write_arrow"
        )
    finally:
        try:
            cat.drop_table(name, NAMESPACE, if_exists=True)
        except Exception:
            pass
        shutil.rmtree(path, ignore_errors=True)


@pytest.mark.parametrize("write_api", ["arrow", "daft"])
@pytest.mark.parametrize("format", ["parquet", "vortex", "vortex-compact"])
def test_sparse_write_preserves_omitted_vector_and_text_indexes(
    tmp_path, write_api: str, format: str
) -> None:
    """Omitted indexed values survive; included columns still auto-build."""
    import daft

    from lakesoul.daft import text_search, vector_search

    cat = _catalog()
    name = f"sparse_indexes_{uuid4().hex}"
    path = tmp_path / name
    vectors = _random_vectors_table(16)
    schema = pa.schema(
        [
            pa.field("id", pa.uint64(), nullable=False),
            pa.field("vec", vectors.schema.field("vec").type),
            pa.field("body", pa.string()),
            pa.field("value", pa.int64()),
        ]
    )
    initial = pa.Table.from_arrays(
        [
            vectors.column("id"),
            vectors.column("vec"),
            pa.array(["apple pie"] + ["banana bread"] * 15),
            pa.array(range(16), type=pa.int64()),
        ],
        schema=schema,
    )
    table = cat.create_table(
        name,
        path=path.as_uri(),
        schema=schema,
        primary_keys=["id"],
        hash_bucket_num=1,
        vector_index=[{"column": "vec", "dim": DIM, "nlist": 2, "total_bits": 7}],
        text_index=[{"column": "body"}],
    )

    def write(data: pa.Table):
        if write_api == "arrow":
            return table.write_arrow(data, format=format)
        return table.write_daft(daft.from_arrow(data), format=format)

    def index_files(kind: str) -> dict[str, bytes]:
        return {
            str(file.relative_to(path)): file.read_bytes()
            for file in (path / f"_{kind}_index").rglob("*")
            if file.is_file()
        }

    try:
        result = write(initial)
        assert result.files
        assert all(
            set(file.existing_columns) == set(schema.names) for file in result.files
        )
        vector_files = index_files("vector")
        text_files = index_files("text")
        assert vector_files and text_files

        result = write(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.uint64()),
                    "value": pa.array([100, 101], type=pa.int64()),
                }
            )
        )
        assert result.row_count == 2
        assert all(
            set(file.existing_columns) == {"id", "value"} for file in result.files
        )
        assert index_files("vector") == vector_files
        assert index_files("text") == text_files
        expected = initial.to_pylist()
        expected[0]["value"] = 100
        expected[1]["value"] = 101
        assert table.scan().to_arrow_table().sort_by("id").to_pylist() == expected

        nearest = (
            vector_search(
                table,
                expected[0]["vec"],
                top_k=1,
                nprobe=2,
                extra_columns=["value"],
            )
            .collect()
            .to_arrow()
        )
        assert nearest.column("id").to_pylist() == [0]
        assert nearest.column("vec").to_pylist() == [expected[0]["vec"]]
        assert nearest.column("value").to_pylist() == [100]
        matches = (
            text_search(table, "apple", extra_columns=["value"]).collect().to_arrow()
        )
        assert matches.column("id").to_pylist() == [0]
        assert matches.column("body").to_pylist() == ["apple pie"]
        assert matches.column("value").to_pylist() == [100]

        # A text-only contribution must still append its new text index delta.
        write(
            pa.table({"id": pa.array([0], type=pa.uint64()), "body": ["cherry tart"]})
        )
        assert index_files("vector") == vector_files
        assert index_files("text") != text_files
        matches = text_search(table, "cherry").collect().to_arrow()
        assert matches.column("id").to_pylist() == [0]
        assert text_search(table, "apple").collect().to_arrow().num_rows == 0
        text_files = index_files("text")

        # A vector-only contribution must likewise update its own index.
        replacement = [-2.0] * DIM
        write(
            pa.table(
                {
                    "id": pa.array([1], type=pa.uint64()),
                    "vec": pa.array([replacement], type=schema.field("vec").type),
                }
            )
        )
        assert index_files("vector") != vector_files
        assert index_files("text") == text_files
        nearest = (
            vector_search(table, replacement, top_k=1, nprobe=2).collect().to_arrow()
        )
        assert nearest.column("id").to_pylist() == [1]
        assert nearest.column("vec").to_pylist() == [replacement]
    finally:
        cat.drop_table(name, NAMESPACE, if_exists=True)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
