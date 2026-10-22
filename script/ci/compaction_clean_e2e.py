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

import contextlib
import json
import os
import subprocess
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


def _psql(statement: str) -> str:
    """Run a statement in the e2e PostgreSQL container.

    The scenarios only have the python package (no PG driver); the e2e already
    runs PostgreSQL in the docker compose environment.
    """
    container = os.environ.get("LAKESOUL_E2E_PG_CONTAINER", "lakesoul-test-pg")
    result = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "psql",
            "-U",
            "lakesoul_test",
            "-d",
            "lakesoul_test",
            "-v",
            "ON_ERROR_STOP=1",
            "-tAc",
            statement,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _container_logs(container: str, tail: int = 20000) -> str:
    result = subprocess.run(
        ["docker", "logs", "--tail", str(tail), container],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout + result.stderr


def _taskmanager_logs(tail: int = 20000) -> str:
    """Aggregate the logs of all taskmanager replicas (the compose service has two)."""
    prefix = os.environ.get(
        "LAKESOUL_E2E_TM_PREFIX", "lakesoul-docker-compose-env-taskmanager"
    )
    names = subprocess.run(
        ["docker", "ps", "--filter", f"name={prefix}", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        check=False,
    ).stdout.split()
    return "\n".join(_container_logs(name, tail) for name in names)


DATA_SUFFIXES = (".parquet", ".vortex")


def _data_files(files: set[str]) -> set[str]:
    return {path for path in files if path.endswith(DATA_SUFFIXES)}


def _live_blob_state(
    filesystem, base: str
) -> tuple[set[str], list[str], set[str]] | None:
    """List the live data files and read their sidecars, or ``None`` on a race.

    The compaction and clean jobs keep rewriting and deleting files while the
    scenario runs; a file listed here can disappear before its sidecar is
    read, which simply means the caller should retry.
    """
    files = _list_files(base)
    data_files = sorted(_data_files(files))
    if not data_files:
        return None
    used_packs: set[str] = set()
    for path in data_files:
        sidecar = f"{path}.blobref"
        if sidecar not in files:
            return None
        packs = read_blobref(f"s3://{path}", filesystem=filesystem)
        if packs is None:
            return None
        used_packs.update(packs)
    return files, data_files, used_packs


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


def scenario_update_commit() -> None:
    """The clean job must keep files re-added by an UpdateCommit.

    Spark builds an update commit from the current partition files: unchanged
    files are re-added with their original paths, new files are added and
    replaced files are marked ``del``. The clean job used to delete every file
    of the older compaction commit, including the ones the update commit still
    referenced.

    Versions 0 and -1 are used on purpose so the partition_insert trigger does
    not notify the background compaction task: this scenario only exercises
    the clean job.
    """
    name = f"e2e_update_{uuid4().hex[:8]}"
    base = f"{WAREHOUSE}/{name}"
    table_id = f"table_{uuid4()}"
    filesystem = _s3()
    shared = f"{_s3_path(base)}/shared-{uuid4().hex}.parquet"
    expired = f"{_s3_path(base)}/expired-{uuid4().hex}.parquet"
    updated = f"{_s3_path(base)}/updated-{uuid4().hex}.parquet"
    for path in (shared, expired, updated):
        with filesystem.open_output_stream(path) as stream:
            stream.write(b"x")
    shared_uri = f"s3://{shared}"
    expired_uri = f"s3://{expired}"
    updated_uri = f"s3://{updated}"
    compaction_commit = str(uuid4())
    update_commit = str(uuid4())
    now_ms = int(time.time() * 1000)

    def file_ops(ops: list[tuple[str, str]]) -> str:
        values = ", ".join(
            f"ROW('{path}', '{op}', 1, 'id')::data_file_op" for path, op in ops
        )
        return f"ARRAY[{values}]"

    def insert_commit(
        commit_id: str, commit_op: str, ops: list[tuple[str, str]], ts: int
    ) -> str:
        return (
            "insert into data_commit_info(table_id, partition_desc, commit_id,"
            " file_ops, commit_op, committed, timestamp, domain) values"
            f" ('{table_id}', '-5', '{commit_id}'::uuid, {file_ops(ops)},"
            f" '{commit_op}', true, {ts}, 'public')"
        )

    def insert_partition(version: int, commit_op: str, commit_id: str, ts: int) -> str:
        return (
            "insert into partition_info(table_id, partition_desc, version,"
            " commit_op, timestamp, snapshot, domain) values"
            f" ('{table_id}', '-5', {version}, '{commit_op}', {ts},"
            f" ARRAY['{commit_id}'::uuid], 'public')"
        )

    try:
        _psql(
            "begin; "
            + insert_commit(
                compaction_commit,
                "CompactionCommit",
                [(shared_uri, "add"), (expired_uri, "add")],
                now_ms - 60_000,
            )
            + "; "
            + insert_commit(
                update_commit,
                "UpdateCommit",
                [
                    (shared_uri, "add"),
                    (updated_uri, "add"),
                    (expired_uri, "del"),
                ],
                now_ms,
            )
            + "; "
            + insert_partition(0, "UpdateCommit", update_commit, now_ms)
            + "; commit;"
        )

        # Wait until the clean job has actually processed the update commit and
        # stored its compaction state. A fixed sleep is racy: if the older
        # compaction version is processed before the broadcast state arrives,
        # the clean job falls back to its (hourly) timer and the test times
        # out. The marker is logged by CompactionBroadcastProcessFunction.
        marker = f"Received compaction state for {table_id}/-5: version=0"
        _wait_for(
            lambda: marker in _taskmanager_logs(),
            "clean job to receive the update commit",
            seconds=int(os.environ.get("LAKESOUL_E2E_UPDATE_WAIT", "180")),
        )

        _psql(
            insert_partition(-1, "CompactionCommit", compaction_commit, now_ms - 60_000)
            + ";"
        )

        def current_files() -> set[str]:
            return _list_files(base)

        _wait_for(
            lambda: _s3_path(expired_uri) not in current_files(),
            "update commit cleanup to delete the expired file",
        )
        files = current_files()
        assert _s3_path(shared_uri) in files, (
            "file re-added by the update commit must not be deleted"
        )
        assert _s3_path(updated_uri) in files, (
            "file of the update commit must not be deleted"
        )
        assert (
            _psql(
                "select count(*) from data_commit_info where"
                f" table_id = '{table_id}' and commit_id = '{compaction_commit}'"
            )
            == "0"
        ), "superseded commit metadata must be deleted"
        assert (
            _psql(
                "select count(*) from data_commit_info where"
                f" table_id = '{table_id}' and commit_id = '{update_commit}'"
            )
            == "1"
        ), "update commit metadata must stay"
        assert (
            _psql(
                "select count(*) from partition_info where"
                f" table_id = '{table_id}' and version = -1"
            )
            == "0"
        ), "superseded partition version must be deleted"
        print(f"[update-commit] ok: {name}")
    finally:
        _psql(
            f"delete from data_commit_info where table_id = '{table_id}';"
            f" delete from partition_info where table_id = '{table_id}';"
        )
        for path in (shared, expired, updated):
            with contextlib.suppress(FileNotFoundError):
                filesystem.delete_file(path)


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
        properties={
            "blob_columns": json.dumps({"payload": {"mode": "external"}}),
            "blob_vacuum_interval": "2",
        },
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
            state = _live_blob_state(filesystem, base)
            if state is None:
                stable_state["data_files"] = None
                return False
            _files, data_files, _packs = state
            previous = stable_state.get("data_files")
            stable_state["data_files"] = data_files
            return previous == data_files

        filesystem = _s3()
        stable_state: dict[str, list[str] | None] = {"data_files": None}
        _wait_for(settled, "compaction to settle with blobref sidecars")

        state = None
        for _attempt in range(20):
            state = _live_blob_state(filesystem, base)
            if state is not None:
                break
            time.sleep(3)
        assert state is not None, "could not read a stable live blob state"
        files, data_files, used_packs = state
        assert data_files, "expected live data files after compaction"
        assert used_packs, "compacted blob table must reference packs"
        missing = {pack for pack in used_packs if _s3_path(pack) not in files}
        assert not missing, f"referenced packs missing from storage: {missing}"
        assert all("/_blob/payload/" in pack for pack in used_packs), (
            "packs must use the shared _blob layout"
        )

        values = table.scan().to_arrow_table().column("payload").to_pylist()
        assert any(value for value in values), "blob payloads must stay readable"

        orphan = f"{_s3_path(base)}/_blob/payload/orphan-{uuid4().hex}.blob"
        partition_orphan = (
            f"{_s3_path(base)}/episode_id=ep000000/_blob/payload/"
            f"orphan-{uuid4().hex}.blob"
        )
        for path in (orphan, partition_orphan):
            with filesystem.open_output_stream(path) as stream:
                stream.write(b"orphan")
        files = _list_files(base)
        assert orphan in files and partition_orphan in files, (
            "orphan packs must be visible"
        )

        def vacuum_until_stable(**kwargs):
            last = None
            for _attempt in range(20):
                last = vacuum_blobs(catalog, table, **kwargs)
                if not last.aborted:
                    return last
                time.sleep(3)
            raise AssertionError(f"vacuum kept aborting: {last}")

        dry = vacuum_until_stable(older_than=0, dry_run=True)
        assert dry.packs_deleted == dry.packs_total - dry.packs_used, f"dry run {dry}"
        assert dry.packs_deleted >= 2, f"orphan packs must be reclaimable: {dry}"

        result = vacuum_until_stable(
            older_than=0, dry_run=False, allow_short_grace=True
        )
        assert result.packs_deleted == dry.packs_deleted, (
            f"vacuum deleted {result.packs_deleted} of {dry.packs_deleted} dry-run packs"
        )
        files = _list_files(base)
        assert orphan not in files, "orphan pack must be deleted"
        assert partition_orphan not in files, "partition orphan pack must be deleted"
        for pack in used_packs:
            assert _s3_path(pack) in files, f"referenced pack deleted: {pack}"

        again = None
        for _attempt in range(10):
            again = vacuum_until_stable(
                older_than=0, dry_run=False, allow_short_grace=True
            )
            if again.packs_deleted == 0:
                break
            time.sleep(3)
        assert again is not None and again.packs_deleted == 0, (
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
    scenario_update_commit()
    scenario_blob(catalog)
    print("compaction clean e2e passed")


if __name__ == "__main__":
    main()
