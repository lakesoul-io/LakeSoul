# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import datetime as dt
from pathlib import Path
from uuid import uuid4

import pyarrow as pa

from lakesoul import LakeSoulCatalog
from lakesoul.metadata.const import DaoType

SCHEMA = pa.schema([pa.field("id", pa.int64(), nullable=False)])
PARTITION = "-5"


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:8]}"


def _batch(value: int) -> pa.Table:
    return pa.table({"id": pa.array([value], type=pa.int64())}, schema=SCHEMA)


def _versions(catalog: LakeSoulCatalog, table_id: str):
    return [
        (item.version, item.pinned)
        for item in catalog._client.get_partition_info_by_table_id_and_desc(
            table_id, PARTITION
        )
    ]


def _uuid_hex(uuid) -> str:
    return f"{uuid.high:016x}{uuid.low:016x}"


def _make_table(catalog: LakeSoulCatalog, tmp_path: Path, name: str):
    return catalog.create_table(name, path=(tmp_path / name).as_uri(), schema=SCHEMA)


def test_purge_keeps_pinned_and_latest_versions(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("purge_pin")
    table = _make_table(catalog, tmp_path, name)

    try:
        table.write_arrow(_batch(1), format="parquet")
        snapshot_id = catalog.create_tag(name, "pin-v1")
        table.write_arrow(_batch(2), format="parquet")

        result = catalog.purge(name, older_than=dt.timedelta(seconds=0), dry_run=False)
        assert result.versions == 0
        assert [pinned for _, pinned in _versions(catalog, table.id)] == [True, False]
        assert sorted(table.scan().to_arrow_table().column("id").to_pylist()) == [1, 2]
        assert table.scan().options(snapshot=snapshot_id).to_arrow_table().column(
            "id"
        ).to_pylist() == [1]
    finally:
        catalog.drop_table(name, if_exists=True)


def test_purge_removes_unpinned_old_version(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("purge_unpin")
    table = _make_table(catalog, tmp_path, name)

    try:
        table.write_arrow(_batch(1), format="parquet")
        snapshot_id = catalog.create_tag(name, "pin-v1")
        table.write_arrow(_batch(2), format="parquet")

        assert catalog.drop_tag(name, "pin-v1")
        assert catalog.drop_snapshot(name, snapshot_id)

        result = catalog.purge(name, older_than=dt.timedelta(seconds=0), dry_run=False)
        assert result.versions == 1
        assert _versions(catalog, table.id) == [(1, False)]
        assert sorted(table.scan().to_arrow_table().column("id").to_pylist()) == [1, 2]
    finally:
        catalog.drop_table(name, if_exists=True)


def test_cleanup_sql_skips_pinned_rows(tmp_path: Path) -> None:
    catalog = LakeSoulCatalog.from_env()
    name = _table_name("purge_sql")
    table = _make_table(catalog, tmp_path, name)
    client = catalog._client

    try:
        table.write_arrow(_batch(1), format="parquet")
        snapshot_id = catalog.create_tag(name, "pin-v1")
        latest = client.get_all_partition_info(table.id)[0]
        commit_id = _uuid_hex(latest.snapshot[0])

        # Both cleanup statements must leave pinned metadata alone.
        client.exec_update(
            DaoType.DeletePreviousVersionPartition,
            [table.id, PARTITION, str(latest.timestamp + 1)],
        )
        client.exec_update(
            DaoType.DeleteDataCommitInfoByTableIdAndPartitionDescAndCommitIdList,
            [table.id, PARTITION, commit_id],
        )
        assert _versions(catalog, table.id) == [(0, True)]
        assert client.list_data_commit_info(table.id, PARTITION, list(latest.snapshot))

        assert catalog.drop_tag(name, "pin-v1")
        assert catalog.drop_snapshot(name, snapshot_id)

        # After unpinning the same statements remove the rows.
        client.exec_update(
            DaoType.DeletePreviousVersionPartition,
            [table.id, PARTITION, str(latest.timestamp + 1)],
        )
        client.exec_update(
            DaoType.DeleteDataCommitInfoByTableIdAndPartitionDescAndCommitIdList,
            [table.id, PARTITION, commit_id],
        )
        assert _versions(catalog, table.id) == []
        assert not client.list_data_commit_info(
            table.id, PARTITION, list(latest.snapshot)
        )
    finally:
        catalog.drop_table(name, if_exists=True)
