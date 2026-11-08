# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Python NativeSession + ``LakeSoulTable.search`` (S1 of the API design).

The session runs the same DataFusion engine as the SQL API, so the Python
search entry point must return the same rows as the equivalent SQL statement,
report the vector-index scan in ``EXPLAIN`` and honour filters, projections and
both rerank modes.

Requires a live PostgreSQL metadata store.  The native binding is only built
with the ``ci-unify`` feature; without it these tests are skipped.
"""

from __future__ import annotations

import os
import shutil

import numpy as np
import pyarrow as pa
import pytest

pytest.importorskip(
    "lakesoul._lib._session",
    reason="extension built without --features ci-unify",
)

from lakesoul import LakeSoulCatalog  # noqa: E402

# The exact direct path (and the native batch entry in exact mode) reads the
# stored vectors through the payload cache, which in turn needs the local
# object-store cache for the persistent key index.
os.environ.setdefault("LAKESOUL_CACHE", "1")
os.environ.setdefault("LAKESOUL_CACHE_PATH", "/tmp/lakesoul_session_cache")
os.environ.setdefault("LAKESOUL_VECTOR_RERANK_CACHE", "1")
os.environ.setdefault(
    "LAKESOUL_VECTOR_RERANK_CACHE_DIR", "/tmp/lakesoul_session_payload"
)

DIM = 8
N = 200
DISTANCE_COLUMN = "__lakesoul_vector_distance"


def _catalog() -> LakeSoulCatalog:
    return LakeSoulCatalog(
        pg_url=os.environ.get(
            "LAKESOUL_PG_URL",
            "postgresql://lakesoul_test:lakesoul_test@localhost:5432/lakesoul_test",
        ),
        pg_username=os.environ.get("LAKESOUL_PG_USERNAME", "lakesoul_test"),
        pg_password=os.environ.get("LAKESOUL_PG_PASSWORD", "lakesoul_test"),
    )


def _vectors(seed: int = 7) -> np.ndarray:
    return np.random.default_rng(seed).normal(size=(N, DIM)).astype(np.float32)


def _batch(
    vectors: np.ndarray, *, titles: bool = True, uint64_ids: bool = False
) -> pa.Table:
    columns: dict[str, pa.Array] = {
        "id": pa.array(
            range(N), type=pa.uint64() if uint64_ids else pa.int64()
        ),
        "vec": pa.FixedSizeListArray.from_arrays(
            pa.array(vectors.reshape(-1), type=pa.float32()), DIM
        ),
    }
    if titles:
        columns["title"] = pa.array([f"doc-{index}" for index in range(N)])
    return pa.table(columns)


def _create_table(
    table_name: str,
    *,
    metric: str = "L2",
    titles: bool = True,
    vectors: np.ndarray | None = None,
    uint64_ids: bool = False,
) -> tuple[LakeSoulCatalog, object, str]:
    catalog = _catalog()
    table_path = f"/tmp/lakesoul_test/{table_name}"
    try:
        catalog.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)
    vectors = _vectors() if vectors is None else vectors
    schema = _batch(vectors, titles=titles, uint64_ids=uint64_ids).schema
    table = catalog.create_table(
        table_name,
        path=f"file://{table_path}",
        schema=schema,
        primary_keys=["id"],
        hash_bucket_num=2,
        vector_index=[
            {
                "column": "vec",
                "dim": DIM,
                "nlist": 4,
                "total_bits": 1,
                "metric": metric,
            }
        ],
    )
    table.write_arrow(_batch(vectors, titles=titles, uint64_ids=uint64_ids))
    return catalog, table, table_path


def _drop(catalog: LakeSoulCatalog, table_name: str, table_path: str) -> None:
    try:
        catalog.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)


def _top_k(vectors: np.ndarray, query: np.ndarray, k: int, *, metric: str) -> list[int]:
    if metric == "L2":
        scores = ((vectors - query) ** 2).sum(axis=1)
        order = np.argsort(scores)
    else:
        scores = vectors @ query
        order = np.argsort(-scores)
    return [int(index) for index in order[:k]]


def test_session_sql_and_explain() -> None:
    """The session executes lakehouse SQL and EXPLAIN shows the vector scan."""
    table_name = "py_session_sql"
    catalog, table, table_path = _create_table(table_name)
    try:
        query = np.random.default_rng(11).normal(size=DIM).astype(np.float32)
        session = catalog.session(vector_search={"nprobe": 4, "exact_overfetch": 20})
        count = session.sql(f'SELECT count(*) AS cnt FROM "lakesoul".default.{table_name}')
        assert count.column("cnt").to_pylist() == [N]

        plan = session.explain(table.to_sql(query, top_k=5))
        assert "LakeSoulVectorSearchExec" in plan, plan
        assert "metric=L2" in plan, plan
        assert "order_by=false" in plan, plan

        # EXPLAIN ANALYZE runs the query and keeps the plan text.
        analyzed = session.explain(table.to_sql(query, top_k=5), analyze=True)
        assert "LakeSoulVectorSearchExec" in analyzed, analyzed
    finally:
        _drop(catalog, table_name, table_path)


def test_search_matches_sql_and_brute_force() -> None:
    """``table.search`` returns the SQL statement's rows (exact top-k)."""
    table_name = "py_session_search"
    vectors = _vectors()
    catalog, table, table_path = _create_table(table_name, vectors=vectors)
    try:
        query = np.random.default_rng(13).normal(size=DIM).astype(np.float32)
        session = catalog.session(vector_search={"nprobe": 4, "exact_overfetch": 30})
        expected = _top_k(vectors, query, 5, metric="L2")

        found = table.search(query, top_k=5, session=session)
        returned = found.column("id").to_pylist()
        assert len(returned) == 5
        assert len(set(returned) & set(expected)) >= 4, (returned, expected)

        # The same statement executed directly through the session agrees.
        via_sql = session.sql(table.to_sql(query, top_k=5)).column("id").to_pylist()
        assert via_sql == returned
        via_table = table.sql(table.to_sql(query, top_k=5), session=session)
        assert via_table.column("id").to_pylist() == returned

        # with_distance adds the distance column, ordered ascending for L2.
        with_distance = table.search(
            query, top_k=10, with_distance=True, session=session
        )
        assert DISTANCE_COLUMN in with_distance.column_names
        distances = with_distance.column(DISTANCE_COLUMN).to_pylist()
        assert distances == sorted(distances), distances
    finally:
        _drop(catalog, table_name, table_path)


def test_search_filter_projection_and_projection_extra_column() -> None:
    """Filters are pushed down and extra projections stay supported."""
    table_name = "py_session_filter"
    vectors = _vectors()
    catalog, table, table_path = _create_table(table_name, vectors=vectors)
    try:
        query = np.random.default_rng(17).normal(size=DIM).astype(np.float32)
        session = catalog.session(vector_search={"nprobe": 4, "exact_overfetch": 20})
        filtered = table.search(
            query, top_k=5, filter="id < 100", session=session
        )
        assert filtered.num_rows == 5
        assert all(value < 100 for value in filtered.column("id").to_pylist())
        assert "title" in filtered.column_names
        assert "vec" not in filtered.column_names

        selected = table.search(
            query, top_k=5, columns=["id", "title"], session=session
        )
        assert selected.column_names == ["id", "title"]
    finally:
        _drop(catalog, table_name, table_path)


def test_search_index_only_and_ip() -> None:
    """``rerank='index'`` does not read the vector column; IP works too."""
    table_name = "py_session_index"
    vectors = _vectors()
    catalog, table, table_path = _create_table(table_name, vectors=vectors)
    try:
        query = np.random.default_rng(19).normal(size=DIM).astype(np.float32)
        session = catalog.session(vector_search={"nprobe": 4})

        index_rows = table.search(query, top_k=5, rerank="index", session=session)
        assert index_rows.num_rows == 5
        plan = session.explain(table.to_sql(query, top_k=5))
        session.set_vector_search(rerank="index")
        plan = session.explain(table.to_sql(query, top_k=5))
        assert "order_by=true" in plan, plan
        session.set_vector_search(rerank="exact")
    finally:
        _drop(catalog, table_name, table_path)

    ip_name = "py_session_ip"
    ip_catalog, ip_table, ip_path = _create_table(
        ip_name, metric="IP", titles=False, vectors=vectors
    )
    try:
        query = np.random.default_rng(23).normal(size=DIM).astype(np.float32)
        session = ip_catalog.session(vector_search={"nprobe": 4, "exact_overfetch": 30})
        expected = _top_k(vectors, query, 5, metric="IP")
        found = ip_table.search(query, top_k=5, session=session)
        returned = found.column("id").to_pylist()
        assert len(returned) == 5
        assert len(set(returned) & set(expected)) >= 4, (returned, expected)
        plan = session.explain(ip_table.to_sql(query, top_k=5))
        assert "metric=IP" in plan, plan
    finally:
        _drop(ip_catalog, ip_name, ip_path)


def test_search_unknown_column_and_bad_dimension() -> None:
    """Parameter validation happens before any query runs."""
    table_name = "py_session_validation"
    catalog, table, table_path = _create_table(table_name)
    try:
        query = [0.0] * DIM
        with pytest.raises(ValueError):
            table.search(query, column="missing")
        with pytest.raises(ValueError):
            table.search(query[:-1])
        with pytest.raises(ValueError):
            table.search(query, top_k=0)
        with pytest.raises(TypeError):
            table.search(query, filter=42)  # type: ignore[arg-type]
        with pytest.raises(NotImplementedError):
            table.search(query, snapshot=1)
    finally:
        _drop(catalog, table_name, table_path)


def test_search_batch_matches_brute_force() -> None:
    """The native batch entry ranks like the per-query SQL path."""
    table_name = "py_session_batch"
    vectors = _vectors()
    catalog, table, table_path = _create_table(table_name, vectors=vectors)
    try:
        generator = np.random.default_rng(29)
        queries = generator.normal(size=(6, DIM)).astype(np.float32)
        session = catalog.session(vector_search={"nprobe": 4})
        result = table.search_batch(
            queries, top_k=5, exact_overfetch=30, session=session
        )
        assert len(result.ids) == 6
        for index, query in enumerate(queries):
            assert len(result.ids[index]) == 5
            expected = _top_k(vectors, query, 5, metric="L2")
            hits = len(set(result.ids[index]) & set(expected))
            assert hits >= 4, (index, result.ids[index], expected)
        arrow = result.to_arrow()
        assert arrow.num_rows == 30
        assert arrow.column_names == ["query_id", "rank", "id"]

        # The catalog convenience method delegates to the table method.
        via_catalog = catalog.search_batch(
            table_name, queries[:2], top_k=5, exact_overfetch=30, session=session
        )
        assert via_catalog.ids == result.ids[:2]
    finally:
        _drop(catalog, table_name, table_path)


def test_search_batch_falls_back_for_non_native_primary_key() -> None:
    """A non-Int64-`id` table falls back to per-query SQL, same results."""
    table_name = "py_session_batch_fallback"
    vectors = _vectors()
    catalog, table, table_path = _create_table(
        table_name, vectors=vectors, uint64_ids=True
    )
    try:
        generator = np.random.default_rng(31)
        queries = generator.normal(size=(3, DIM)).astype(np.float32)
        session = catalog.session(vector_search={"nprobe": 4, "exact_overfetch": 30})
        result = table.search_batch(queries, top_k=5, session=session)
        assert len(result.ids) == 3
        assert result.distances is not None
        for index, query in enumerate(queries):
            expected = _top_k(vectors, query, 5, metric="L2")
            hits = len(set(result.ids[index]) & set(expected))
            assert hits >= 4, (index, result.ids[index], expected)
    finally:
        _drop(catalog, table_name, table_path)


def test_output_adapters() -> None:
    """``to=`` wraps the Arrow result without rescanning the table."""
    pytest.importorskip("daft", reason="daft extra not installed")
    table_name = "py_session_adapters"
    catalog, table, table_path = _create_table(table_name)
    try:
        query = np.random.default_rng(37).normal(size=DIM).astype(np.float32)
        session = catalog.session(vector_search={"nprobe": 4, "exact_overfetch": 20})
        daft_frame = table.search(query, top_k=5, session=session, to="daft")
        collected = daft_frame.collect().to_arrow()
        assert collected.num_rows == 5
        assert "id" in collected.column_names

        daft_sql = session.sql(
            f'SELECT count(*) AS cnt FROM "lakesoul".default.{table_name}',
            to="daft",
        )
        assert daft_sql.collect().to_arrow().num_rows == 1

        with pytest.raises(ValueError):
            session.sql("SELECT 1", to="not-a-format")  # type: ignore[arg-type]
    finally:
        _drop(catalog, table_name, table_path)


def test_search_batch_validation() -> None:
    """Batch parameter validation happens before any query runs."""
    table_name = "py_session_batch_validation"
    catalog, table, table_path = _create_table(table_name)
    try:
        query = np.zeros(DIM, dtype=np.float32)
        with pytest.raises(ValueError):
            table.search_batch([query, query[:-1]])
        with pytest.raises(ValueError):
            table.search_batch([query], rerank="unknown")
        with pytest.raises(ValueError):
            table.search_batch([query], exact_overfetch=0)
        with pytest.raises(ValueError):
            table.search_batch([np.array([], dtype=np.float32)])
    finally:
        _drop(catalog, table_name, table_path)
