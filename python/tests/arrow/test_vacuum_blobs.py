# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import types
from pathlib import Path

import pyarrow.fs as pafs
import pytest

from lakesoul import vacuum
from lakesoul.vacuum import VacuumResult, vacuum_blobs


def _catalog():
    return types.SimpleNamespace(object_store_options={})


def _table(blob_option: str | None = '["frame"]'):
    return types.SimpleNamespace(
        _blob_columns_option=lambda: blob_option, path="file:///tmp/t"
    )


def test_no_blob_columns_is_noop():
    result = vacuum_blobs(_catalog(), _table(None))
    assert result == VacuumResult(dry_run=True)


def test_aborts_when_sidecar_missing(monkeypatch):
    monkeypatch.setattr(vacuum, "_collect", lambda c, t, f: (("f",), set(), True))
    result = vacuum_blobs(_catalog(), _table())
    assert result.aborted


def test_aborts_when_live_set_changes(monkeypatch):
    calls = iter([(("a",), {"p"}, False), (("b",), {"p"}, False)])
    monkeypatch.setattr(vacuum, "_collect", lambda c, t, f: next(calls))
    result = vacuum_blobs(_catalog(), _table())
    assert result.aborted


def test_collect_aborts_on_corrupt_sidecar(monkeypatch):
    monkeypatch.setattr(vacuum, "_live_data_files", lambda c, t: {"file"})

    def boom(path, filesystem):
        raise ValueError("invalid blobref")

    monkeypatch.setattr(vacuum, "read_blobref", boom)
    files, used, missing = vacuum._collect(object(), object(), None)
    assert files == ("file",)
    assert used == set()
    assert missing


def test_list_packs_scans_partition_dirs(tmp_path) -> None:
    table_path = tmp_path / "table"
    (table_path / "_blob" / "frame").mkdir(parents=True)
    (table_path / "_blob" / "frame" / "root.blob").write_bytes(b"r")
    (table_path / "_blob" / "frame" / "root.blobref").write_bytes(b"{}")
    partition_blob_dir = table_path / "episode_id=ep000000" / "_blob" / "data"
    partition_blob_dir.mkdir(parents=True)
    (partition_blob_dir / "part.blob").write_bytes(b"p")
    (table_path / "episode_id=ep000000" / "part-0.parquet").write_bytes(b"d")

    table = types.SimpleNamespace(path=table_path.as_uri())
    _filesystem, _base, infos = vacuum._list_packs(table, {})
    assert sorted(Path(info.path).name for info in infos) == [
        "part.blob",
        "root.blob",
    ]


def _info(path: str, mtime_ns: int, size: int = 10):
    return types.SimpleNamespace(
        path=path, type=pafs.FileType.File, mtime_ns=mtime_ns, size=size
    )


@pytest.mark.parametrize("dry_run", [True, False])
def test_deletes_unused_old_packs(monkeypatch, dry_run):
    used_uri = "s3://b/t/_blob/frame/used.blob"
    monkeypatch.setattr(vacuum, "_collect", lambda c, t, f: (("f",), {used_uri}, False))
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
    result = vacuum_blobs(_catalog(), _table(), dry_run=dry_run)
    assert result.packs_total == 3
    assert result.packs_deleted == 1
    assert result.bytes_deleted == 10
    assert deleted == ([] if dry_run else ["b/t/_blob/frame/old.blob"])
