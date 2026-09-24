# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Reclaim blob packs that are no longer referenced by any live data file.

A pack survives when at least one data file reachable from the latest
partition versions or from any snapshot/tag lists it in its ``.blobref``
sidecar. Everything else is deleted once it is older than the grace period.

The live set is collected twice; if the two collections disagree (a commit
landed while scanning) or any live data file is missing its sidecar, the
vacuum aborts without deleting anything.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow.fs as pafs

from .blob import BLOB_DIR, read_blobref
from .purge import DEFAULT_OLDER_THAN, _filesystem, _grace_ms, _uuid_hex

if TYPE_CHECKING:
    from .catalog import LakeSoulCatalog, LakeSoulTable

logger = logging.getLogger(__name__)

__all__ = ["VacuumResult", "maybe_vacuum_after_commit", "vacuum_blobs"]

#: Table property that controls the automatic vacuum cadence, counted in
#: partition versions. ``0`` disables the automatic vacuum.
BLOB_VACUUM_INTERVAL_KEY = "blob_vacuum_interval"
DEFAULT_BLOB_VACUUM_INTERVAL = 20

#: Deletions require at least this much grace: packs are uploaded before the
#: data files that reference them are committed, so a shorter grace can race
#: with an in-flight write.
MIN_SAFE_GRACE = dt.timedelta(hours=1)


@dataclass(frozen=True)
class VacuumResult:
    dry_run: bool
    data_files: int = 0
    packs_total: int = 0
    packs_used: int = 0
    packs_deleted: int = 0
    bytes_deleted: int = 0
    aborted: bool = False


def _live_data_files(catalog: LakeSoulCatalog, table: LakeSoulTable) -> set[str]:
    client = catalog._client
    table_id = table.id
    latest = list(client.get_all_partition_info(table_id))
    if not latest:
        return set()

    snapshot_ids = {
        snapshot.snapshot_id
        for snapshot in client.list_snapshots(table.name, namespace=table.namespace)
    }
    for tag in client.list_tags(table.name, namespace=table.namespace):
        snapshot_id = getattr(tag, "snapshot_id", None)
        if snapshot_id is not None:
            snapshot_ids.add(snapshot_id)

    live_commits: set[tuple[str, str]] = set()
    for snapshot_id in snapshot_ids:
        for row in client.list_snapshot_commits(table_id, snapshot_id):
            live_commits.add((row.partition_desc, _uuid_hex(row.commit_id)))
    for item in latest:
        for commit_id in item.snapshot:
            live_commits.add((item.partition_desc, _uuid_hex(commit_id)))

    by_partition: dict[str, list[Any]] = {}
    for partition_desc, commit_id in live_commits:
        by_partition.setdefault(partition_desc, []).append(commit_id)
    files: set[str] = set()
    for partition_desc, commit_ids in by_partition.items():
        uuids = [
            (int(commit_id[:16], 16), int(commit_id[16:], 16))
            for commit_id in commit_ids
        ]
        for path in client._inner.get_data_files_of_single_partition(
            table_id, partition_desc, uuids
        ):
            files.add(path)
    return files


def _collect(
    catalog: LakeSoulCatalog, table: LakeSoulTable, filesystem: pafs.FileSystem
) -> tuple[tuple[str, ...], set[str], bool]:
    live = _live_data_files(catalog, table)
    used: set[str] = set()
    missing = False
    for path in sorted(live):
        try:
            packs = read_blobref(path, filesystem)
        except ValueError:
            # A corrupt sidecar is as unsafe as a missing one: abort without deleting.
            missing = True
            continue
        if packs is None:
            missing = True
            continue
        used.update(packs)
    return tuple(sorted(live)), used, missing


def _pack_key(path: str) -> str:
    return "/".join(path.rstrip("/").split("/")[-2:])


def _list_packs(
    table: LakeSoulTable, options: dict[str, str]
) -> tuple[pafs.FileSystem | None, str, list[Any]]:
    """List every pack below the table, including partition-local ``_blob`` dirs."""
    root = table.path.rstrip("/")
    filesystem, base = _filesystem(root, options)
    try:
        infos = filesystem.get_file_info(
            pafs.FileSelector(base, allow_not_found=True, recursive=True)
        )
    except FileNotFoundError:
        return filesystem, base, []
    marker = f"/{BLOB_DIR}/"
    return (
        filesystem,
        base,
        [
            info
            for info in infos
            if info.type == pafs.FileType.File
            and info.path.endswith(".blob")
            and marker in info.path
        ],
    )


def _mtime_ns(info: Any) -> int | None:
    mtime_ns = getattr(info, "mtime_ns", None)
    if mtime_ns is not None:
        return int(mtime_ns)
    mtime = getattr(info, "mtime", None)
    if mtime is None:
        return None
    if mtime.tzinfo is None:
        mtime = mtime.replace(tzinfo=dt.timezone.utc)
    return int(mtime.timestamp() * 1_000_000_000)


def vacuum_blobs(
    catalog: LakeSoulCatalog,
    table: LakeSoulTable,
    *,
    older_than: dt.timedelta | int = DEFAULT_OLDER_THAN,
    dry_run: bool = True,
    allow_short_grace: bool = False,
) -> VacuumResult:
    """Delete blob packs that no live data file references.

    ``older_than`` is a grace period (``timedelta`` or milliseconds); packs
    younger than it are never removed. With ``dry_run`` nothing is deleted
    and the result only reports what would happen.

    Writers upload packs before committing the data files that reference
    them, so a real deletion with a grace period shorter than
    :data:`MIN_SAFE_GRACE` is rejected unless ``allow_short_grace`` is set.
    """
    blob_option = table._blob_columns_option()
    if not blob_option:
        return VacuumResult(dry_run)

    if (
        not dry_run
        and not allow_short_grace
        and _grace_ms(older_than) < int(MIN_SAFE_GRACE.total_seconds() * 1000)
    ):
        raise ValueError(
            "vacuum_blobs with a grace period shorter than "
            f"{MIN_SAFE_GRACE} cannot delete safely: packs are uploaded before their"
            " data files are committed and a concurrent write could lose them; pass"
            " allow_short_grace=True to override"
        )

    options = dict(catalog.object_store_options or {})
    filesystem, _ = _filesystem(table.path, options)
    first_files, used, missing = _collect(catalog, table, filesystem)
    if missing:
        return VacuumResult(dry_run, aborted=True)
    second_files, used_again, missing = _collect(catalog, table, filesystem)
    if missing or second_files != first_files:
        return VacuumResult(dry_run, aborted=True)
    used = used | used_again

    filesystem, _base, infos = _list_packs(table, options)
    if filesystem is None:
        return VacuumResult(dry_run, aborted=True)

    used_keys = {_pack_key(path) for path in used}
    cutoff_ns = (
        dt.datetime.now(dt.timezone.utc).timestamp() * 1_000_000_000
        - _grace_ms(older_than) * 1_000_000
    )
    deleted = 0
    bytes_deleted = 0
    for info in infos:
        if _pack_key(info.path) in used_keys:
            continue
        mtime_ns = _mtime_ns(info)
        if mtime_ns is None or mtime_ns > cutoff_ns:
            continue
        deleted += 1
        bytes_deleted += info.size or 0
        if not dry_run:
            try:
                filesystem.delete_file(info.path)
            except FileNotFoundError:
                # Another writer process may have reclaimed the pack first.
                pass
    return VacuumResult(
        dry_run=dry_run,
        data_files=len(first_files),
        packs_total=len(infos),
        packs_used=len(used_keys),
        packs_deleted=deleted,
        bytes_deleted=bytes_deleted,
    )


def _vacuum_interval(table: LakeSoulTable) -> int:
    raw = dict(table.properties).get(BLOB_VACUUM_INTERVAL_KEY)
    if raw is None or str(raw).strip() == "":
        return DEFAULT_BLOB_VACUUM_INTERVAL
    try:
        value = int(str(raw).strip())
    except ValueError:
        value = -1
    if value < 0:
        logger.warning(
            "invalid %s %r on table %s.%s, using %d",
            BLOB_VACUUM_INTERVAL_KEY,
            raw,
            table.namespace,
            table.name,
            DEFAULT_BLOB_VACUUM_INTERVAL,
        )
        return DEFAULT_BLOB_VACUUM_INTERVAL
    return value


def maybe_vacuum_after_commit(
    catalog: LakeSoulCatalog,
    table: LakeSoulTable,
    partition_descs: set[str] | None = None,
) -> VacuumResult | None:
    """Reclaim blob packs after a commit when the table asks for it.

    Blob tables vacuum every ``blob_vacuum_interval`` partition versions
    (default 20, ``0`` disables it). ``partition_descs`` are the partitions
    touched by the commit; only their versions are checked so a partition
    that rests on a multiple of the interval does not trigger a vacuum on
    every write to other partitions. The cleanup never propagates failures:
    the data is already committed and the next write that crosses an
    interval retries it.
    """
    try:
        if not table._blob_columns_option():
            return None
        interval = _vacuum_interval(table)
        if interval <= 0:
            return None
        partitions = list(catalog._client.get_all_partition_info(table.id))
        if partition_descs:
            committed = [
                info for info in partitions if info.partition_desc in partition_descs
            ]
            if committed:
                partitions = committed
        version = max((int(info.version) for info in partitions), default=0)
        if version <= 0 or version % interval != 0:
            return None
        result = vacuum_blobs(
            catalog,
            table,
            older_than=DEFAULT_OLDER_THAN,
            dry_run=False,
        )
        logger.info(
            "blob vacuum for %s.%s at version %d: packs_total=%d packs_used=%d"
            " packs_deleted=%d bytes_deleted=%d aborted=%s",
            table.namespace,
            table.name,
            version,
            result.packs_total,
            result.packs_used,
            result.packs_deleted,
            result.bytes_deleted,
            result.aborted,
        )
        return result
    # Cleanup must never fail a write that is already committed.
    except Exception:
        logger.warning(
            "blob vacuum after commit failed for %s.%s",
            table.namespace,
            table.name,
            exc_info=True,
        )
        return None
