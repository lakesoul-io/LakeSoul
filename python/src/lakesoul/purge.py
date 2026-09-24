# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Retention-aware purge of old, unreferenced table versions.

A purge only touches versions that are:

* older than the grace period (``older_than``, measured against the newest
  commit timestamp the metadata server knows),
* not pinned by any live snapshot or tag, and
* not the latest version of their partition.

Files still referenced by any live snapshot/tag or by the latest version are
never deleted, and blob packs are removed together with their data file.
"""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlparse

from .metadata.const import DaoType

if TYPE_CHECKING:
    from .catalog import LakeSoulCatalog, LakeSoulTable

__all__ = ["PurgeResult", "purge_table"]

DEFAULT_OLDER_THAN = dt.timedelta(days=1)


@dataclass(frozen=True)
class PurgeResult:
    """Outcome of a purge (or of its dry run)."""

    dry_run: bool
    commits: int
    versions: int
    files: int
    bytes: int
    packs: int


def _grace_ms(older_than: dt.timedelta | int) -> int:
    if isinstance(older_than, dt.timedelta):
        return int(older_than.total_seconds() * 1000)
    if isinstance(older_than, int):
        return older_than
    raise TypeError(
        f"older_than must be a timedelta or milliseconds, got "
        f"{type(older_than).__name__}"
    )


def _uuid_hex(uuid: Any) -> str:
    return f"{uuid.high:016x}{uuid.low:016x}"


def _filesystem(uri: str, options: dict[str, str]):
    from pyarrow.fs import FileSystem, LocalFileSystem, S3FileSystem

    parsed = urlparse(uri)
    if parsed.scheme in ("", "file"):
        path = unquote(parsed.path) if parsed.scheme == "file" else uri
        return LocalFileSystem(), path
    if parsed.scheme in ("s3", "s3a") and "fs.s3a.endpoint" in options:
        endpoint = options["fs.s3a.endpoint"]
        filesystem = S3FileSystem(
            access_key=options.get("fs.s3a.access.key"),
            secret_key=options.get("fs.s3a.secret.key"),
            endpoint_override=endpoint,
            scheme="http" if endpoint.startswith("http://") else "https",
        )
        return filesystem, f"{parsed.netloc}/{parsed.path.lstrip('/')}"
    filesystem, path = FileSystem.from_uri(uri)
    return filesystem, path


def _delete(
    uri: str, options: dict[str, str], stats: dict[str, int] | None = None
) -> None:
    filesystem, path = _filesystem(uri, options)
    try:
        info = filesystem.get_file_info(path)
        if info.type != info.type.File:
            return
        if stats is not None:
            stats["files"] += 1
            stats["bytes"] += info.size
        filesystem.delete_file(path)
    except FileNotFoundError:
        return


def purge_table(
    catalog: LakeSoulCatalog,
    table: LakeSoulTable,
    *,
    older_than: dt.timedelta | int = DEFAULT_OLDER_THAN,
    dry_run: bool = True,
) -> PurgeResult:
    """Delete unpinned versions and files older than ``older_than``."""
    client = catalog._client
    table_id = table.id
    grace_ms = _grace_ms(older_than)

    latest = list(client.get_all_partition_info(table_id))
    if not latest:
        return PurgeResult(dry_run, 0, 0, 0, 0, 0)
    server_now = max(item.timestamp for item in latest)

    pinned_commits: set[tuple[str, str]] = set()
    for snapshot in client.list_snapshots(table.name, namespace=table.namespace):
        for row in client.list_snapshot_commits(table_id, snapshot.snapshot_id):
            pinned_commits.add((row.partition_desc, _uuid_hex(row.commit_id)))

    live_commits = set(pinned_commits)
    for item in latest:
        for commit_id in item.snapshot:
            live_commits.add((item.partition_desc, _uuid_hex(commit_id)))

    live_files: set[str] = set()
    by_partition: dict[str, list[Any]] = {}
    for partition_desc, commit_id in live_commits:
        by_partition.setdefault(partition_desc, []).append(commit_id)
    for partition_desc, commit_ids in by_partition.items():
        uuids = []
        for commit_id in commit_ids:
            uuids.append((int(commit_id[:16], 16), int(commit_id[16:], 16)))
        for path in client._inner.get_data_files_of_single_partition(
            table_id, partition_desc, uuids
        ):
            live_files.add(path)

    blob_columns: list[str] = []
    blob_option = table._blob_columns_option()
    if blob_option:
        blob_columns = list(json.loads(blob_option))

    options = dict(catalog.object_store_options or {})
    stats = {"files": 0, "bytes": 0, "packs": 0}
    commits = 0
    versions = 0
    for item in latest:
        cutoff = min(server_now - grace_ms, item.timestamp - 1)
        all_versions = client.get_partition_info_by_table_id_and_desc(
            table_id, item.partition_desc
        )
        stale = [
            version
            for version in all_versions
            if version.version < item.version
            and version.timestamp <= cutoff
            and not version.pinned
        ]
        if not stale:
            continue
        versions += len(stale)
        stale_commits = {
            _uuid_hex(commit_id) for version in stale for commit_id in version.snapshot
        } - {commit_id for _, commit_id in live_commits}
        candidates: set[str] = set()
        if stale_commits:
            infos = client.list_data_commit_info(
                table_id, item.partition_desc, _uuids_of(stale_commits)
            )
            candidates = {
                op.path for info in infos for op in info.file_ops
            } - live_files
            commits += len(stale_commits)
        for path in candidates:
            if dry_run:
                filesystem, fs_path = _filesystem(path, options)
                info = filesystem.get_file_info(fs_path)
                if info.type == info.type.File:
                    stats["files"] += 1
                    stats["bytes"] += info.size
            else:
                _delete(path, options, stats)
                _delete(f"{path}.blobref", options)
                for column in blob_columns:
                    stats["packs"] += 1
                    _delete(f"{path}.{column}.blob", options, stats)
        if not dry_run:
            if stale_commits:
                client.exec_update(
                    DaoType.DeleteDataCommitInfoByTableIdAndPartitionDescAndCommitIdList,
                    [
                        table_id,
                        item.partition_desc,
                        "".join(sorted(stale_commits)),
                    ],
                )
            client.exec_update(
                DaoType.DeletePreviousVersionPartition,
                [table_id, item.partition_desc, str(cutoff)],
            )

    return PurgeResult(
        dry_run=dry_run,
        commits=commits,
        versions=versions,
        files=stats["files"],
        bytes=stats["bytes"],
        packs=stats["packs"],
    )


def _uuids_of(commit_ids: set[str]) -> list[Any]:
    from .metadata.generated.entity_pb2 import Uuid

    return [
        Uuid(high=int(commit_id[:16], 16), low=int(commit_id[16:], 16))
        for commit_id in commit_ids
    ]
