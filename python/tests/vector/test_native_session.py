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

from lakesoul import LakeSoulCatalog, LakeSoulSession  # noqa: E402

# The exact direct path (and the native batch entry in exact mode) reads the
# stored vectors through the on-demand decoded-block cache, which in turn needs
# the local object-store cache for the persistent key index.
os.environ.setdefault("LAKESOUL_CACHE", "1")
os.environ.setdefault("LAKESOUL_CACHE_PATH", "/tmp/lakesoul_session_cache")
os.environ.setdefault("LAKESOUL_VECTOR_BLOCK_ROWS", "256")

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
        # The session default is index-only ordering now.
        assert "order_by=true" in plan, plan

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
        session = catalog.session(
            vector_search={"nprobe": 4, "exact_overfetch": 30, "rerank": "exact"}
        )
        expected = _top_k(vectors, query, 5, metric="L2")

        found = table.search(query, top_k=5, rerank="exact", session=session)
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
            query, top_k=10, rerank="exact", with_distance=True, session=session
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
        session = ip_catalog.session(
            vector_search={"nprobe": 4, "exact_overfetch": 30, "rerank": "exact"}
        )
        expected = _top_k(vectors, query, 5, metric="IP")
        found = ip_table.search(query, top_k=5, rerank="exact", session=session)
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
        with pytest.raises(ValueError):
            table.search(query, snapshot=1, tag="v1")
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
            queries, top_k=5, rerank="exact", exact_overfetch=30, session=session
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
            table_name,
            queries[:2],
            top_k=5,
            rerank="exact",
            exact_overfetch=30,
            session=session,
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
        result = table.search_batch(
            queries, top_k=5, rerank="exact", session=session
        )
        assert len(result.ids) == 3
        assert result.distances is None
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


def _create_partitioned_table(
    table_name: str, *, metric: str = "L2"
) -> tuple[LakeSoulCatalog, object, str]:
    """Partitioned vector table: three ``dt`` partitions of 40 rows each."""
    catalog = _catalog()
    table_path = f"/tmp/lakesoul_test/{table_name}"
    try:
        catalog.drop_table(table_name, if_exists=True)
    except Exception:
        pass
    shutil.rmtree(table_path, ignore_errors=True)
    vectors = _vectors()
    days = ["2026-01-01", "2026-01-02", "2026-01-03"]
    day_values = [days[index % len(days)] for index in range(N)]
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), False),
            pa.field("dt", pa.string(), False),
            pa.field(
                "vec",
                pa.list_(pa.field("item", pa.float32()), DIM),
                False,
            ),
        ]
    )
    batch = pa.table(
        {
            "id": pa.array(range(N), type=pa.int64()),
            "dt": pa.array(day_values),
            "vec": pa.FixedSizeListArray.from_arrays(
                pa.array(vectors.reshape(-1), type=pa.float32()), DIM
            ),
        }
    )
    table = catalog.create_table(
        table_name,
        path=f"file://{table_path}",
        schema=schema,
        primary_keys=["id"],
        partition_by=["dt"],
        hash_bucket_num=1,
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
    table.write_arrow(batch)
    return catalog, table, table_path


def test_distributed_search_over_partitions() -> None:
    """Partitions are searched by several workers and merged on the driver."""
    table_name = "py_session_distributed"
    vectors = _vectors()
    catalog, table, table_path = _create_partitioned_table(table_name)
    try:
        query = np.random.default_rng(41).normal(size=DIM).astype(np.float32)
        expected = _top_k(vectors, query, 5, metric="L2")

        distributed = table.distributed(engine="threads", num_workers=3)
        assert distributed.is_distributed
        assert distributed.partition_columns == ("dt",)

        rows = distributed.search(query, top_k=5)
        returned = rows.column("id").to_pylist()
        assert len(returned) == 5, returned
        hits = len(set(returned) & set(expected))
        assert hits >= 4, (returned, expected)
        assert "dt" in rows.column_names

        with_distance = distributed.search(
            query, top_k=5, with_distance=True
        )
        distances = with_distance.column("__lakesoul_vector_distance").to_pylist()
        assert distances == sorted(distances), distances

        filtered = distributed.search(
            query, top_k=5, filter="id < 100", columns=["id", "dt"]
        )
        assert filtered.column_names == ["id", "dt"]
        assert all(value < 100 for value in filtered.column("id").to_pylist())
        assert filtered.num_rows == 5

        count = distributed.sql(
            f'SELECT count(*) AS cnt FROM "lakesoul".default.{table_name}'
        )
        assert count.column("cnt").to_pylist() == [N]
    finally:
        _drop(catalog, table_name, table_path)


def test_distributed_single_node_fallback_and_validation() -> None:
    """An unpartitioned table keeps the single-node result; bad engines fail."""
    table_name = "py_session_distributed_plain"
    catalog, table, table_path = _create_table(table_name)
    try:
        query = np.random.default_rng(43).normal(size=DIM).astype(np.float32)
        distributed = table.distributed(engine="threads", num_workers=2)
        assert not distributed.is_distributed
        via_distributed = distributed.search(query, top_k=5)
        single = table.search(query, top_k=5)
        assert via_distributed.column("id").to_pylist() == single.column("id").to_pylist()

        with pytest.raises(ValueError):
            table.distributed(engine="unknown")
        with pytest.raises(ValueError):
            table.distributed(num_workers=0)
    finally:
        _drop(catalog, table_name, table_path)


def test_distributed_ray_engine_import_error() -> None:
    """engine='ray' without ray installed raises an actionable ImportError."""
    try:
        import ray  # noqa: F401
    except ImportError:
        pass
    else:
        pytest.skip("ray is installed in this environment")
    table_name = "py_session_distributed_ray"
    catalog, table, table_path = _create_partitioned_table(table_name)
    try:
        query = np.random.default_rng(47).normal(size=DIM).astype(np.float32)
        with pytest.raises(ImportError):
            table.distributed(engine="ray").search(query, top_k=5)
    finally:
        _drop(catalog, table_name, table_path)


def test_distributed_session_configuration() -> None:
    """The coordinator session validates its worker configuration.

    Without a reachable worker fleet the session stays usable: the distributed
    planner keeps such queries on the coordinator, which is exactly the
    single-node behaviour (results unchanged).
    """
    with pytest.raises(ValueError):
        LakeSoulSession(distributed={"worker_urls": []})
    with pytest.raises(ValueError):
        LakeSoulSession(
            distributed={"worker_urls": ["grpc://127.0.0.1:59999"], "target_partitions": 1}
        )
    with pytest.raises(ValueError):
        LakeSoulSession(
            distributed={
                "worker_urls": ["grpc://127.0.0.1:59999"],
                "unknown_option": True,
            }
        )
    with pytest.raises(ValueError):
        LakeSoulSession(distributed={"worker_urls": "grpc://127.0.0.1:59999"})

    session = LakeSoulSession(
        distributed={
            "worker_urls": ["grpc://127.0.0.1:59999"],
            "target_partitions": 4,
            "fallback_to_local": True,
        }
    )
    try:
        assert session.sql("SELECT 1 AS one").to_pydict() == {"one": [1]}
    finally:
        session.close()


def test_search_pinned_snapshot_tag_and_as_of() -> None:
    """Time travel: snapshot, tag and as-of reads see the frozen rows."""
    table_name = "py_session_time_travel"
    vectors = _vectors()
    catalog, table, table_path = _create_table(table_name, vectors=vectors)
    try:
        session = catalog.session(
            vector_search={"nprobe": 4, "exact_overfetch": 20, "rerank": "exact"}
        )
        latest_before = session.sql(
            f'SELECT count(*) AS cnt FROM "lakesoul".default.{table_name}'
        )
        assert latest_before.column("cnt").to_pylist() == [N]

        snapshot_id = catalog.create_snapshot(table)
        catalog.create_tag(table, "v1", snapshot_id)
        snapshot = next(
            info
            for info in catalog.list_snapshots(table)
            if info.snapshot_id == snapshot_id
        )

        # Append rows *after* the snapshot: the pinned views must not see them.
        extra_rows = 50
        rng = np.random.default_rng(53)
        extra_vectors = rng.normal(size=(extra_rows, DIM)).astype(np.float32)
        table.write_arrow(
            pa.table(
                {
                    "id": pa.array(range(N, N + extra_rows), type=pa.int64()),
                    "title": pa.array(
                        [f"extra-{index}" for index in range(extra_rows)]
                    ),
                    "vec": pa.FixedSizeListArray.from_arrays(
                        pa.array(
                            extra_vectors.reshape(-1), type=pa.float32()
                        ),
                        DIM,
                    ),
                }
            )
        )
        latest_after = session.sql(
            f'SELECT count(*) AS cnt FROM "lakesoul".default.{table_name}'
        )
        assert latest_after.column("cnt").to_pylist() == [N + extra_rows]

        query = np.random.default_rng(59).normal(size=DIM).astype(np.float32)
        frozen_ids = {int(value) for value in table.search(
            query, top_k=50, rerank="exact", snapshot=snapshot_id, session=session
        ).column("id").to_pylist()}
        assert frozen_ids
        assert max(frozen_ids) < N, sorted(frozen_ids)[-3:]
        latest_ids = table.search(query, top_k=50, rerank="exact", session=session).column(
            "id"
        ).to_pylist()
        assert latest_ids and max(latest_ids) >= N, latest_ids[-3:]

        tag_ids = {int(value) for value in table.search(
            query, top_k=50, rerank="exact", tag="v1", session=session
        ).column("id").to_pylist()}
        assert tag_ids == frozen_ids

        as_of_ids = {int(value) for value in table.search(
            query, top_k=50, rerank="exact", as_of=int(snapshot.created_at), session=session
        ).column("id").to_pylist()}
        assert as_of_ids == frozen_ids

        frozen_count = table.sql(
            f'SELECT count(*) AS cnt FROM "lakesoul".default.{table_name}',
            snapshot=snapshot_id,
            session=session,
        )
        assert frozen_count.column("cnt").to_pylist() == [N]
        latest_count = table.sql(
            f'SELECT count(*) AS cnt FROM "lakesoul".default.{table_name}',
            session=session,
        )
        assert latest_count.column("cnt").to_pylist() == [N + extra_rows]

        # The index describes the latest files, so a pinned plan is a plain
        # merge-on-read scan with an exact Sort above it.
        alias = session.register_snapshot(
            table_name, snapshot=snapshot_id
        )
        try:
            plan = session.explain(
                f'SELECT id FROM {alias} ORDER BY array_distance(vec, '
                f"ARRAY[{', '.join('0.0' for _ in range(DIM))}]) LIMIT 5"
            )
        finally:
            session.unregister(alias)
        assert "LakeSoulVectorSearchExec" not in plan, plan
        assert "SortExec" in plan, plan

        # A pinned scan with an extra filter keeps the filter semantics.
        filtered = table.search(
            query, top_k=10, rerank="exact", filter="id < 50", snapshot=snapshot_id, session=session
        )
        assert filtered.num_rows == 10
        assert all(value < 50 for value in filtered.column("id").to_pylist())

        with pytest.raises(ValueError):
            table.search(query, snapshot=snapshot_id, tag="v1")
    finally:
        catalog.drop_tag(table, "v1")
        _drop(catalog, table_name, table_path)
