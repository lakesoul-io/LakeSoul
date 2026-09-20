# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Daft text search through the native Tantivy index.

Validates that a Daft query can use the text index for full-text filtering:

- ``scan.options(reader_options={...}).to_daft()`` runs the per-bucket
  Tantivy search inside the native reader;
- ``lakesoul.daft.text_search`` returns exact matches (the native reader
  verifies candidates against the current rows, so stale index entries do
  not leak);
- Chinese segmentation via jieba, updates, and validation errors.

Requires a live PostgreSQL metadata store.
"""

from __future__ import annotations

import os
import shutil

import pyarrow as pa
import pytest

from lakesoul import LakeSoulCatalog

NAMESPACE = "default"

ROWS = [
    (1, "The quick brown fox jumps over the lazy dog"),
    (2, "A fast brown fox leaps over a sleepy dog"),
    (3, "Rust systems programming with DataFusion"),
    (4, "机器学习与倒排索引"),
    (5, "全文检索和布尔查询"),
]


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


def _schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.uint64(), False),
            pa.field("body", pa.string(), False),
        ]
    )


def _rows_table(rows: list[tuple[int, str]]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([row[0] for row in rows], type=pa.uint64()),
            "body": pa.array([row[1] for row in rows], type=pa.string()),
        },
        schema=_schema(),
    )


def _create_table(name: str, *, text_index: list[dict] | None = None):
    cat = _catalog()
    table_path = f"/tmp/lakesoul_test/{name}"
    try:
        cat.drop_table(name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)
    table = cat.create_table(
        name,
        path=f"file://{table_path}",
        schema=_schema(),
        primary_keys=["id"],
        hash_bucket_num=2,
        text_index=text_index if text_index is not None else [{"column": "body"}],
    )
    return table, table_path


def _write(table, rows: list[tuple[int, str]]) -> None:
    table.write_arrow(_rows_table(rows))


def test_daft_text_search_candidates() -> None:
    """to_daft() + reader_options returns the bucket's matching rows."""
    table, table_path = _create_table("text_daft_candidates")
    try:
        _write(table, ROWS)
        result = (
            table.scan(columns=("id", "body"))
            .options(
                reader_options={
                    "text_search_query": "fox",
                    "text_search_top_k": "10",
                }
            )
            .to_daft()
            .collect()
            .to_arrow()
        )
        ids = set(result.column("id").to_pylist())
        assert ids == {1, 2}, f"unexpected hits: {ids}"
        assert "body" in result.column_names
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_text_search_api_matches_exactly() -> None:
    """text_search returns exactly the rows containing the query terms."""
    from lakesoul.daft import text_search

    table, table_path = _create_table("text_daft_api")
    try:
        _write(table, ROWS)
        result = text_search(table, "fox", top_k=10)
        collected = result.collect().to_arrow()
        assert collected.column_names == ["id", "body"], collected.column_names
        ids = set(collected.column("id").to_pylist())
        assert ids == {1, 2}, f"unexpected hits: {ids}"

        none = text_search(table, "elephant", top_k=10).collect().to_arrow()
        assert none.num_rows == 0

        chinese = text_search(table, "倒排索引", top_k=10).collect().to_arrow()
        assert chinese.column("id").to_pylist() == [4]
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_text_search_drops_stale_rows() -> None:
    """A row whose text was updated no longer matches its old terms."""
    from lakesoul.daft import text_search

    table, table_path = _create_table("text_daft_stale")
    try:
        _write(table, ROWS)
        # Upsert id=4 with unrelated text: the base split still holds the old
        # document, but the exact verification pass must drop it.
        _write(table, [(4, "全新的内容主题")])

        result = text_search(table, "倒排索引", top_k=10).collect().to_arrow()
        assert result.num_rows == 0, result.to_pylist()

        updated = text_search(table, "内容主题", top_k=10).collect().to_arrow()
        assert updated.column("id").to_pylist() == [4]
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_text_search_validation() -> None:
    from lakesoul.daft import text_search

    cat = _catalog()
    name = "text_daft_validation"
    table_path = f"/tmp/lakesoul_test/{name}"
    try:
        cat.drop_table(name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)
    schema = pa.schema(
        [
            pa.field("id", pa.uint64(), False),
            pa.field("body", pa.string(), False),
            pa.field("title", pa.string(), False),
        ]
    )
    table = cat.create_table(
        name,
        path=f"file://{table_path}",
        schema=schema,
        primary_keys=["id"],
        hash_bucket_num=2,
        text_index=[{"column": "body"}, {"column": "title"}],
    )
    try:
        with pytest.raises(ValueError, match="multiple text columns"):
            text_search(table, "fox")
        with pytest.raises(ValueError, match="not declared"):
            text_search(table, "fox", column="missing")
        with pytest.raises(ValueError, match="query must not be empty"):
            text_search(table, "  ")
        with pytest.raises(TypeError, match="query must be a string"):
            text_search(table, 42)  # type: ignore[arg-type]
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_text_index_auto_builds_on_daft_write() -> None:
    """write_daft builds the text index through the distributed UDF path."""
    import daft

    from lakesoul.daft import text_search

    table, table_path = _create_table("text_daft_write")
    try:
        dataframe = daft.from_arrow(_rows_table(ROWS))
        table.write_daft(dataframe)
        ids = set(
            text_search(table, "fox", top_k=10)
            .collect()
            .to_arrow()
            .column("id")
            .to_pylist()
        )
        assert ids == {1, 2}, f"unexpected hits: {ids}"
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_build_text_index_explicit() -> None:
    """The catalog build API creates/rebuilds the index without a write."""
    from lakesoul.daft import text_search

    table, table_path = _create_table("text_build_explicit")
    try:
        _write(table, ROWS)
        result = table.build_text_index(rebuild=True)
        assert result["status"] == "ok", result
        assert result["text_column"] == "body"
        ids = set(
            text_search(table, "fox", top_k=10)
            .collect()
            .to_arrow()
            .column("id")
            .to_pylist()
        )
        assert ids == {1, 2}, f"unexpected hits: {ids}"
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_text_index_config_is_validated_before_metadata() -> None:
    """create_table rejects unusable text indexes before creating metadata."""
    cat = _catalog()
    schema = pa.schema(
        [
            pa.field("id", pa.uint64(), False),
            pa.field("vec", pa.list_(pa.float32(), 4), False),
        ]
    )
    with pytest.raises(ValueError, match="not found in table schema"):
        cat.create_table(
            "text_invalid_column",
            path="file:///tmp/lakesoul_test/text_invalid_column",
            schema=schema,
            primary_keys=["id"],
            text_index=[{"column": "body"}],
        )
    with pytest.raises(ValueError, match="must be Utf8"):
        cat.create_table(
            "text_invalid_type",
            path="file:///tmp/lakesoul_test/text_invalid_type",
            schema=schema,
            primary_keys=["id"],
            text_index=[{"column": "vec"}],
        )
    with pytest.raises(ValueError, match="unsupported tokenizer"):
        cat.create_table(
            "text_invalid_tokenizer",
            path="file:///tmp/lakesoul_test/text_invalid_tokenizer",
            schema=_schema(),
            primary_keys=["id"],
            text_index=[{"column": "body", "tokenizer": "nope"}],
        )
