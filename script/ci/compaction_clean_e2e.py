# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Data rounds and assertions for the compaction + clean e2e script.

Run against the docker-compose environment (PG + RustFS + Flink cluster) with
the lake soul python package available. Each scenario writes several rounds so
the background NewCompactionTask compacts them and the Flink clean job removes
the retired files.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pyarrow as pa

from lakesoul import LakeSoulCatalog

S3_OPTIONS = {
    "fs.s3a.access.key": os.environ.get("RUSTFS_ACCESS_KEY", "rustfsadmin"),
    "fs.s3a.secret.key": os.environ.get("RUSTFS_SECRET_KEY", "rustfsadmin"),
    "fs.s3a.endpoint": os.environ.get("RUSTFS_ENDPOINT", "http://127.0.0.1:9000"),
    "fs.s3a.path.style.access": "true",
}
WAREHOUSE = os.environ.get("LAKESOUL_E2E_WAREHOUSE", "s3://lakesoul-test-bucket/e2e")
ROUNDS = int(os.environ.get("LAKESOUL_E2E_ROUNDS", "4"))
WAIT_SECONDS = int(os.environ.get("LAKESOUL_E2E_WAIT", "120"))


def _catalog() -> LakeSoulCatalog:
    return LakeSoulCatalog.from_env(object_store_options=S3_OPTIONS)


def _table(catalog: LakeSoulCatalog, name: str, schema: pa.Schema, **kwargs):
    catalog.drop_table(name, if_exists=True)
    return catalog.create_table(
        name,
        path=f"{WAREHOUSE}/{name}",
        schema=schema,
        properties=kwargs.pop("properties", None),
        **kwargs,
    )


def _write_rounds(table, schema: pa.Schema, rounds: int) -> None:
    for round_index in range(rounds):
        table.write_arrow(
            pa.table(
                {
                    "id": pa.array([round_index], type=pa.int64()),
                    "label": pa.array([f"round-{round_index}"], type=pa.string()),
                },
                schema=schema,
            ),
            format="parquet",
        )
        time.sleep(0.5)


def _wait_for(predicate, message: str, seconds: int = WAIT_SECONDS) -> None:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(3)
    raise AssertionError(f"timeout waiting for {message}")


def _file_exists(uri: str) -> bool:
    from pyarrow.fs import FileSystem, S3FileSystem

    parsed = urlparse(uri)
    if parsed.scheme in ("s3", "s3a"):
        filesystem = S3FileSystem(
            access_key=S3_OPTIONS["fs.s3a.access.key"],
            secret_key=S3_OPTIONS["fs.s3a.secret.key"],
            endpoint_override=S3_OPTIONS["fs.s3a.endpoint"],
            scheme="http" if S3_OPTIONS["fs.s3a.endpoint"].startswith("http://") else "https",
        )
        path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"
    else:
        filesystem, path = FileSystem.from_uri(uri)
    return filesystem.get_file_info(path).type == filesystem.get_file_info(path).type.File


def scenario_normal(catalog: LakeSoulCatalog) -> None:
    name = f"e2e_normal_{uuid4().hex[:8]}"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string()),
        ]
    )
    table = _table(catalog, name, schema)
    try:
        _write_rounds(table, schema, ROUNDS)
        client = catalog._client
        versions_before = len(
            client.get_partition_info_by_table_id_and_desc(table.id, "-5")
        )
        assert versions_before > 1, "expected several versions before cleanup"

        def compacted_and_cleaned() -> bool:
            versions = client.get_partition_info_by_table_id_and_desc(table.id, "-5")
            return len(versions) < versions_before

        _wait_for(compacted_and_cleaned, "compaction and cleanup of old versions")
        assert sorted(
            table.scan().to_arrow_table().column("id").to_pylist()
        ) == list(range(ROUNDS)), "latest data must stay readable"
        print(f"[normal] ok: {name}")
    finally:
        catalog.drop_table(name, if_exists=True)


def scenario_tag(catalog: LakeSoulCatalog) -> None:
    name = f"e2e_tag_{uuid4().hex[:8]}"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("label", pa.string()),
        ]
    )
    table = _table(catalog, name, schema)
    try:
        table.write_arrow(
            pa.table(
                {
                    "id": pa.array([0], type=pa.int64()),
                    "label": pa.array(["tagged"], type=pa.string()),
                },
                schema=schema,
            ),
            format="parquet",
        )
        snapshot_id = catalog.create_tag(name, "keep-me")
        _write_rounds(table, schema, ROUNDS)

        client = catalog._client
        commits = client.list_snapshot_commits(table.id, snapshot_id)
        pinned_files = client._inner.get_data_files_of_single_partition(
            table.id,
            commits[0].partition_desc,
            [(c.commit_id.high, c.commit_id.low) for c in commits],
        )
        assert pinned_files, "tagged snapshot must reference files"

        # After compaction + cleanup the tagged files must still exist.
        def still_readable() -> bool:
            try:
                table.scan().options(tag="keep-me").to_arrow_table()
                return True
            except Exception:
                return False

        _wait_for(still_readable, "tagged snapshot to remain readable")
        for path in pinned_files:
            assert _file_exists(path), f"tagged file was deleted: {path}"
        print(f"[tag] ok: {name}")
    finally:
        catalog.drop_table(name, if_exists=True)


def scenario_blob(catalog: LakeSoulCatalog) -> None:
    name = f"e2e_blob_{uuid4().hex[:8]}"
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("payload", pa.binary()),
        ]
    )
    table = _table(
        catalog,
        name,
        schema,
        properties={"blob_columns": json.dumps({"payload": {"mode": "external"}})},
    )
    try:
        for round_index in range(ROUNDS):
            payload = (f"payload-{round_index}-".encode() * 1024)
            table.write_arrow(
                pa.table(
                    {
                        "id": pa.array([round_index], type=pa.int64()),
                        "payload": pa.array([payload], type=pa.binary()),
                    },
                    schema=schema,
                ),
                format="parquet",
            )
            time.sleep(0.5)

        client = catalog._client
        versions_before = len(
            client.get_partition_info_by_table_id_and_desc(table.id, "-5")
        )

        def compacted() -> bool:
            return (
                len(client.get_partition_info_by_table_id_and_desc(table.id, "-5"))
                < versions_before
            )

        _wait_for(compacted, "blob table compaction")
        values = table.scan().to_arrow_table().column("payload").to_pylist()
        assert any(value for value in values), "blob payloads must stay readable"
        print(f"[blob] ok: {name}")
    finally:
        catalog.drop_table(name, if_exists=True)


def main() -> None:
    catalog = _catalog()
    scenario_normal(catalog)
    scenario_tag(catalog)
    scenario_blob(catalog)
    print("compaction clean e2e passed")


if __name__ == "__main__":
    main()
