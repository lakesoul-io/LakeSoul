# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Automatic text index compaction across the Python write paths.

Every delta build appends one split; once a shard's delta history outweighs
its compacted base (``max_delta_ratio``), the shard is rebuilt from its full
active file list into a single split, dropping superseded documents.  These
tests cover the sync ``write_arrow`` path, the distributed ``write_daft``
path, the management knobs, and the manual ``build_text_index(rebuild=True)``
entry.

Requires a live PostgreSQL metadata store.
"""

from __future__ import annotations

import os
import shutil

import pyarrow as pa
import pytest

from lakesoul import LakeSoulCatalog

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


def _schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.uint64(), False),
            pa.field("body", pa.string(), False),
        ]
    )


def _table(rows: list[tuple[int, str]]) -> pa.Table:
    return pa.table(
        {
            "id": pa.array([row[0] for row in rows], type=pa.uint64()),
            "body": pa.array([row[1] for row in rows], type=pa.string()),
        },
        schema=_schema(),
    )


def _create_table(name: str, **index_params) -> tuple[object, str]:
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
        # One shard per table so the statistics are unambiguous.
        hash_bucket_num=1,
        text_index=[{"column": "body", **index_params}],
    )
    return table, table_path


def _stats(table: object) -> tuple[int, int, int, int] | None:
    from lakesoul._lib.text import text_index_stats

    files, _ = table.catalog._client.get_data_files_and_pks_by_table_name(
        table.name, namespace=table.namespace
    )
    return text_index_stats(files, "body")


def _write(table: object, rows: list[tuple[int, str]]) -> None:
    table.write_arrow(_table(rows))


def test_arrow_write_compacts_drifted_shard() -> None:
    """Repeated upserts compact the shard into a single split."""
    from lakesoul.daft import text_search

    table, table_path = _create_table("text_compact_arrow")
    try:
        _write(table, [(1, "apple pie")])
        assert _stats(table) == (1, 1, 1, 1)

        _write(table, [(1, "cherry tart")])
        assert _stats(table)[0:2] == (1, 2)

        # The third write's delta makes stale/base = (3-1)/1 > 1.0, so the
        # post-write compaction rebuilds the shard from all active files.
        _write(table, [(1, "date cake")])
        after = _stats(table)
        assert after is not None
        generation, splits, base_docs, total_docs = after
        assert generation >= 2, after
        assert splits == 1, after
        assert (base_docs, total_docs) == (1, 1), after

        ids = (
            text_search(table, "date", top_k=10)
            .collect()
            .to_arrow()
            .column("id")
            .to_pylist()
        )
        assert ids == [1]
        for stale in ("apple", "cherry"):
            matches = text_search(table, stale, top_k=10).collect().to_arrow()
            assert matches.num_rows == 0, f"'{stale}' survived compaction"
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_daft_write_compacts_drifted_shard() -> None:
    """The distributed write path compacts as well."""
    import daft

    from lakesoul.daft import text_search

    table, table_path = _create_table("text_compact_daft")
    try:
        for text in ("apple pie", "cherry tart", "date cake"):
            table.write_daft(daft.from_arrow(_table([(1, text)])))
        after = _stats(table)
        assert after is not None
        generation, splits, base_docs, total_docs = after
        assert generation >= 2, after
        assert splits == 1, after
        assert (base_docs, total_docs) == (1, 1), after

        ids = (
            text_search(table, "date", top_k=10)
            .collect()
            .to_arrow()
            .column("id")
            .to_pylist()
        )
        assert ids == [1]
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_max_delta_ratio_keeps_deltas() -> None:
    """A large ratio disables automatic compaction; exactness is preserved."""
    from lakesoul.daft import text_search

    table, table_path = _create_table("text_compact_disabled", max_delta_ratio=100.0)
    try:
        for text in ("apple pie", "cherry tart", "date cake", "elderberry pie"):
            _write(table, [(1, text)])
        stats = _stats(table)
        assert stats is not None
        generation, splits, _, total_docs = stats
        assert generation == 1, stats
        assert splits == 4, stats
        assert total_docs == 4, stats

        # Stale splits stay harmless: the exact verification drops them.
        for stale in ("apple", "cherry", "date"):
            matches = text_search(table, stale, top_k=10).collect().to_arrow()
            assert matches.num_rows == 0, f"'{stale}' leaked"
        assert text_search(table, "elderberry", top_k=10).collect().to_arrow().column(
            "id"
        ).to_pylist() == [1]
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_rebuild_mode_none_disables_automatic_compaction() -> None:
    table, table_path = _create_table("text_compact_none", rebuild_mode="none")
    try:
        for text in ("apple pie", "cherry tart", "date cake"):
            _write(table, [(1, text)])
        stats = _stats(table)
        assert stats is not None
        assert stats[0] == 1 and stats[1] == 3, stats
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_manual_rebuild_compacts() -> None:
    """build_text_index(rebuild=True) collapses the shard on demand."""
    table, table_path = _create_table("text_compact_manual", rebuild_mode="none")
    try:
        for text in ("apple pie", "cherry tart", "date cake"):
            _write(table, [(1, text)])
        assert _stats(table)[1] == 3

        table.build_text_index(rebuild=True)
        stats = _stats(table)
        assert stats is not None
        generation, splits, base_docs, total_docs = stats
        assert generation >= 2, stats
        assert splits == 1, stats
        assert (base_docs, total_docs) == (1, 1), stats
    finally:
        table.drop()
        shutil.rmtree(table_path, ignore_errors=True)


def test_invalid_management_knobs_are_rejected() -> None:
    cat = _catalog()
    for params in (
        {"rebuild_mode": "sometimes"},
        {"max_delta_ratio": 0},
        {"max_delta_ratio": "big"},
    ):
        with pytest.raises(ValueError):
            cat.create_table(
                "text_compact_invalid",
                path="file:///tmp/lakesoul_test/text_compact_invalid",
                schema=_schema(),
                primary_keys=["id"],
                text_index=[{"column": "body", **params}],
            )
