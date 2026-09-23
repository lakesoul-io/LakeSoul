# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
"""Data rounds and assertions for the compaction + clean e2e script.

Run against the docker-compose environment (PG + RustFS + Flink cluster) with
the lakesoul python package available. Each scenario writes several rounds so
the background NewCompactionTask compacts them and the Flink clean job removes
the retired files.
"""

from __future__ import annotations

import json
import os
import time
from urllib.parse import urlparse
from uuid import uuid4

import pyarrow as pa
from lakesoul import LakeSoulCatalog
from lakesoul.blob import read_blobref
from lakesoul.vacuum import vacuum_blobs
from pyarrow.fs import FileSelector, FileType, S3FileSystem

S3_OPTIONS = {
    "fs.s3a.access.key": os.environ.get("RUSTFS_ACCESS_KEY", "rustfsadmin"),
    "fs.s3a.secret.key": os.environ.get("RUSTFS_SECRET_KEY", "rustfsadmin"),
    "fs.s3a.endpoint": os.environ.get("RUSTFS_ENDPOINT", "http://127.0.0.1:9000"),
    "fs.s3a.path.style.access": "true",
}
WAREHOUSE = os.environ.get("LAKESOUL_E2E_WAREHOUSE", "s3://lakesoul-test-bucket/e2e")
ROUNDS = int(os.environ.get("LAKESOUL_E2E_ROUNDS", "12"))
WAIT_SECONDS = int(os.environ.get("LAKESOUL_E2E_WAIT", "360"))


def _catalog() -> LakeSoulCatalog:
    return LakeSoulCatalog.from_env(object_store_options=S3_OPTIONS)


def _s3() -> S3FileSystem:
    endpoint = S3_OPTIONS["fs.s3a.endpoint"]
    return S3FileSystem(
        access_key=S3_OPTIONS["fs.s3a.access.key"],
        secret_key=S3_OPTIONS["fs.s3a.secret.key"],
        endpoint_override=endpoint,
        scheme="http" if endpoint.startswith("http://") else "https",
    )


def _list_files(uri: str) -> set[str]:
    parsed = urlparse(uri)
    path = f"{parsed.netloc}/{parsed.path.lstrip('/')}"
    infos = _s3().get_file_info(FileSelector(path, recursive=True))
    return {info.path for info in infos if info.type == FileType.File}


def _s3_path(uri: str) -> str:
    parsed = urlparse(uri)
    return f"{parsed.netloc}/{parsed.path.lstrip('/')}"


DATA_SUFFIXES = (".parquet", ".vortex")


def _data_files(files: set[str]) -> set[str]:
    return {path for path in files if path.endswith(DATA_SUFFIXES)}


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
        base = f"{WAREHOUSE}/{name}"
        before = _list_files(base)
        assert before, "expected data files after writing rounds"

        _wait_for(
            lambda: bool(before - _list_files(base)),
            "compaction and cleanup to delete old files",
        )
        assert sorted(table.scan().to_arrow_table().column("id").to_pylist()) == list(
            range(ROUNDS)
        ), "latest data must stay readable"
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
        base = f"{WAREHOUSE}/{name}"
        tagged_files = _list_files(base)
        catalog.create_tag(name, "keep-me")
        _write_rounds(table, schema, ROUNDS)
        appended_files = _list_files(base) - tagged_files

        _wait_for(
            lambda: bool(appended_files - _list_files(base)),
            "compaction and cleanup with a tag",
        )
        missing = tagged_files - _list_files(base)
        assert not missing, f"tagged files were deleted: {missing}"
        assert sorted(
            table.scan()
            .options(tag="keep-me")
            .to_arrow_table()
            .column("id")
            .to_pylist()
        ) == [0], "tagged snapshot must stay readable"
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
            payload = f"payload-{round_index}-".encode() * 4096
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

        base = f"{WAREHOUSE}/{name}"
        before = _list_files(base)
        assert before, "expected data files after writing rounds"

        _wait_for(
            lambda: bool(before - _list_files(base)),
            "blob table compaction and cleanup",
        )

        def settled() -> bool:
            files = _list_files(base)
            data_files = _data_files(files)
            return bool(data_files) and all(
                f"{path}.blobref" in files for path in data_files
            )

        _wait_for(settled, "compaction to settle with blobref sidecars")

        files = _list_files(base)
        data_files = sorted(_data_files(files))
        assert data_files, "expected live data files after compaction"
        filesystem = _s3()
        used_packs: set[str] = set()
        for path in data_files:
            sidecar = f"{path}.blobref"
            assert sidecar in files, f"missing blobref sidecar for {path}"
            packs = read_blobref(f"s3://{path}", filesystem=filesystem)
            assert packs is not None, f"unreadable blobref sidecar {sidecar}"
            used_packs.update(packs)
        assert used_packs, "compacted blob table must reference packs"
        missing = {pack for pack in used_packs if _s3_path(pack) not in files}
        assert not missing, f"referenced packs missing from storage: {missing}"
        assert all("/_blob/payload/" in pack for pack in used_packs), (
            "packs must use the shared _blob layout"
        )

        values = table.scan().to_arrow_table().column("payload").to_pylist()
        assert any(value for value in values), "blob payloads must stay readable"

        orphan = f"{_s3_path(base)}/_blob/payload/orphan-{uuid4().hex}.blob"
        with filesystem.open_output_stream(orphan) as stream:
            stream.write(b"orphan")
        assert orphan in _list_files(base), "orphan pack must be visible"

        dry = vacuum_blobs(catalog, table, older_than=0, dry_run=True)
        assert not dry.aborted, f"vacuum must not abort: {dry}"
        assert dry.packs_deleted == dry.packs_total - dry.packs_used, f"dry run {dry}"
        assert dry.packs_deleted >= 1, f"orphan pack must be reclaimable: {dry}"

        result = vacuum_blobs(catalog, table, older_than=0, dry_run=False)
        assert not result.aborted, f"vacuum must not abort: {result}"
        assert result.packs_deleted == dry.packs_deleted, (
            f"vacuum deleted {result.packs_deleted} of {dry.packs_deleted} dry-run packs"
        )
        files = _list_files(base)
        assert orphan not in files, "orphan pack must be deleted"
        for pack in used_packs:
            assert _s3_path(pack) in files, f"referenced pack deleted: {pack}"
        again = vacuum_blobs(catalog, table, older_than=0, dry_run=False)
        assert not again.aborted and again.packs_deleted == 0, (
            f"vacuum must be idempotent: {again}"
        )

        def no_orphan_sidecars() -> bool:
            current = _list_files(base)
            return not {
                path
                for path in current
                if path.endswith(".blobref") and path[: -len(".blobref")] not in current
            }

        _wait_for(no_orphan_sidecars, "clean job to delete blobref sidecars")
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
