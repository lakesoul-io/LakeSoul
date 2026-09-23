# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import types

import pyarrow.fs as pafs

from lakesoul import vacuum
from lakesoul.vacuum import (
    DEFAULT_BLOB_VACUUM_INTERVAL,
    VacuumResult,
    maybe_vacuum_after_commit,
)


def _table(*, interval=None, blob=True):
    properties = {}
    if interval is not None:
        properties["blob_vacuum_interval"] = str(interval)
    return types.SimpleNamespace(
        id="t1",
        name="tbl",
        namespace="default",
        properties=properties,
        _blob_columns_option=lambda: '["payload"]' if blob else None,
    )


def _catalog(versions):
    calls = []

    def get_all_partition_info(table_id):
        calls.append(table_id)
        return [types.SimpleNamespace(version=version) for version in versions]

    catalog = types.SimpleNamespace(
        _client=types.SimpleNamespace(get_all_partition_info=get_all_partition_info),
        object_store_options={},
    )
    return catalog, calls


def _capture(monkeypatch, result=None, error=None):
    calls = []

    def fake(catalog, table, **kwargs):
        calls.append(kwargs)
        if error is not None:
            raise error
        return result or VacuumResult(dry_run=False, packs_deleted=1)

    monkeypatch.setattr(vacuum, "vacuum_blobs", fake)
    return calls


def test_triggers_on_default_interval_multiple(monkeypatch):
    calls = _capture(monkeypatch)
    catalog, _ = _catalog([DEFAULT_BLOB_VACUUM_INTERVAL])
    result = maybe_vacuum_after_commit(catalog, _table())
    assert result is not None
    assert calls == [{"older_than": vacuum.DEFAULT_OLDER_THAN, "dry_run": False}]


def test_skips_other_versions(monkeypatch):
    calls = _capture(monkeypatch)
    catalog, _ = _catalog([DEFAULT_BLOB_VACUUM_INTERVAL - 1])
    assert maybe_vacuum_after_commit(catalog, _table()) is None
    assert calls == []


def test_interval_property(monkeypatch):
    calls = _capture(monkeypatch)
    catalog, _ = _catalog([5])
    assert maybe_vacuum_after_commit(catalog, _table(interval=5)) is not None
    assert calls
    calls.clear()
    catalog, _ = _catalog([4])
    assert maybe_vacuum_after_commit(catalog, _table(interval=5)) is None
    assert calls == []


def test_zero_disables(monkeypatch):
    calls = _capture(monkeypatch)
    catalog, _ = _catalog([20])
    assert maybe_vacuum_after_commit(catalog, _table(interval=0)) is None
    assert calls == []


def test_invalid_interval_falls_back_to_default(monkeypatch):
    calls = _capture(monkeypatch)
    catalog, _ = _catalog([20])
    assert maybe_vacuum_after_commit(catalog, _table(interval="many")) is not None
    assert calls


def test_non_blob_table_skips_without_query(monkeypatch):
    calls = _capture(monkeypatch)
    catalog, queries = _catalog([20])
    assert maybe_vacuum_after_commit(catalog, _table(blob=False)) is None
    assert calls == []
    assert queries == []


def test_uses_max_version_across_partitions(monkeypatch):
    calls = _capture(monkeypatch)
    catalog, _ = _catalog([3, 20, 7])
    assert maybe_vacuum_after_commit(catalog, _table()) is not None
    assert calls


def test_failure_never_propagates(monkeypatch):
    _capture(monkeypatch, error=RuntimeError("boom"))
    catalog, _ = _catalog([20])
    assert maybe_vacuum_after_commit(catalog, _table()) is None


def test_delete_tolerates_missing_pack(monkeypatch):
    used_uri = "s3://b/t/_blob/frame/used.blob"
    monkeypatch.setattr(vacuum, "_collect", lambda c, t, f: (("f",), {used_uri}, False))
    old = types.SimpleNamespace(
        path="b/t/_blob/frame/old.blob",
        type=pafs.FileType.File,
        mtime_ns=0,
        size=10,
    )

    def delete_file(path):
        raise FileNotFoundError(path)

    filesystem = types.SimpleNamespace(delete_file=delete_file)
    monkeypatch.setattr(
        vacuum,
        "_list_packs",
        lambda t, o: (filesystem, "b/t/_blob", [old]),
    )
    result = vacuum.vacuum_blobs(
        types.SimpleNamespace(object_store_options={}),
        types.SimpleNamespace(
            _blob_columns_option=lambda: '["frame"]', path="file:///tmp/t"
        ),
        dry_run=False,
    )
    assert result.packs_deleted == 1
