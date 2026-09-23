# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import types

import pyarrow.fs as pafs
import pytest

from lakesoul import vacuum
from lakesoul.vacuum import VacuumResult, vacuum_blobs


def _table(blob_option: str | None = '["frame"]'):
    return types.SimpleNamespace(_blob_columns_option=lambda: blob_option)


def test_no_blob_columns_is_noop():
    result = vacuum_blobs(object(), _table(None))
    assert result == VacuumResult(dry_run=True)


def test_aborts_when_sidecar_missing(monkeypatch):
    monkeypatch.setattr(vacuum, "_collect", lambda c, t: (("f",), set(), True))
    result = vacuum_blobs(object(), _table())
    assert result.aborted


def test_aborts_when_live_set_changes(monkeypatch):
    calls = iter([(("a",), {"p"}, False), (("b",), {"p"}, False)])
    monkeypatch.setattr(vacuum, "_collect", lambda c, t: next(calls))
    result = vacuum_blobs(object(), _table())
    assert result.aborted


def _info(path: str, mtime_ns: int, size: int = 10):
    return types.SimpleNamespace(
        path=path, type=pafs.FileType.File, mtime_ns=mtime_ns, size=size
    )


@pytest.mark.parametrize("dry_run", [True, False])
def test_deletes_unused_old_packs(monkeypatch, dry_run):
    used_uri = "s3://b/t/_blob/frame/used.blob"
    monkeypatch.setattr(vacuum, "_collect", lambda c, t: (("f",), {used_uri}, False))
    old = _info("b/t/_blob/frame/old.blob", 0)
    young = _info("b/t/_blob/frame/young.blob", 10**30)
    used = _info("b/t/_blob/frame/used.blob", 0)
    deleted: list[str] = []
    filesystem = types.SimpleNamespace(delete_file=deleted.append)
    monkeypatch.setattr(
        vacuum,
        "_list_packs",
        lambda t, o: (filesystem, "b/t/_blob", [old, young, used]),
    )
    table = types.SimpleNamespace(
        _blob_columns_option=lambda: '["frame"]', path="s3://b/t"
    )
    catalog = types.SimpleNamespace(object_store_options={})
    result = vacuum_blobs(catalog, table, dry_run=dry_run)
    assert result.packs_total == 3
    assert result.packs_deleted == 1
    assert result.bytes_deleted == 10
    assert deleted == ([] if dry_run else ["b/t/_blob/frame/old.blob"])
