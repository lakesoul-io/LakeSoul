# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from compat.cases import CASES, FULL_CASES, SMOKE_CASES, CaseSpec
from compat.engines import CompatContext, Engine, TableRef, engine_registry
from compat.normalize import assert_table_matches


@dataclass(slots=True)
class MatrixRecord:
    operation: str
    case: str
    writer: str
    reader: str | None
    table_name: str
    table_path: str
    status: str
    elapsed_seconds: float
    error: str | None = None
    actual: dict | None = None
    expected: dict | None = None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    python_dir = repo_root / "python"
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = args.run_id or uuid.uuid4().hex[:10]

    ctx = CompatContext(
        repo_root=repo_root,
        python_dir=python_dir,
        output_dir=output_dir,
        storage_uri=args.storage.rstrip("/"),
        run_id=run_id,
        object_store_options=_object_store_options(args.storage),
    )
    engines = engine_registry()
    selected_writers, selected_readers = _resolve_engines(args, engines)
    selected_cases = _resolve_cases(args.cases, args.mode)
    records: list[MatrixRecord] = []
    written: dict[tuple[str, str], TableRef] = {}

    try:
        write_tasks, read_tasks = _plan_tasks(
            args.mode,
            selected_cases,
            selected_writers,
            selected_readers,
            engines,
        )

        for writer_name, case_name in write_tasks:
            case = CASES[case_name]
            writer = engines[writer_name]
            ref = ctx.table_ref(case, writer_name)
            record = _run_write(writer, case, ref, ctx)
            records.append(record)
            if record.status == "passed":
                written[(writer_name, case_name)] = ref

        for writer_name, reader_name, case_name in read_tasks:
            ref = written.get((writer_name, case_name))
            if ref is None:
                continue
            case = CASES[case_name]
            reader = engines[reader_name]
            records.append(_run_read(reader, case, ref, ctx))
    finally:
        for engine in engines.values():
            engine.close()

    manifest = {
        "run_id": run_id,
        "mode": args.mode,
        "storage": args.storage,
        "writers": selected_writers,
        "readers": selected_readers,
        "cases": selected_cases,
        "records": [asdict(record) for record in records],
    }
    manifest_file = output_dir / "manifest.json"
    manifest_file.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    failures = [record for record in records if record.status != "passed"]
    print(f"LakeSoul compatibility manifest: {manifest_file}")
    print(f"LakeSoul compatibility checks: {len(records)} run, {len(failures)} failed")
    if failures:
        for failure in failures:
            target = (
                f"{failure.writer}->{failure.reader}"
                if failure.reader is not None
                else failure.writer
            )
            print(f"FAILED {failure.operation} {target} {failure.case}: {failure.error}")
        return 1
    return 0


def _run_write(
    writer: Engine,
    case: CaseSpec,
    ref: TableRef,
    ctx: CompatContext,
) -> MatrixRecord:
    start = time.monotonic()
    try:
        writer.write_case(case, ref, ctx)
        status = "passed"
        error = None
    except Exception as exc:
        status = "failed"
        error = repr(exc)
    return MatrixRecord(
        operation="write",
        case=case.name,
        writer=writer.name,
        reader=None,
        table_name=ref.table_name,
        table_path=ref.path,
        status=status,
        elapsed_seconds=time.monotonic() - start,
        error=error,
    )


def _run_read(
    reader: Engine,
    case: CaseSpec,
    ref: TableRef,
    ctx: CompatContext,
) -> MatrixRecord:
    start = time.monotonic()
    actual = None
    expected = None
    try:
        table = reader.read_case(case, ref, ctx)
        actual, expected = assert_table_matches(
            table,
            case.expected_table,
            _visible_partition_columns(case),
        )
        status = "passed"
        error = None
    except Exception as exc:
        status = "failed"
        error = repr(exc)
    return MatrixRecord(
        operation="read",
        case=case.name,
        writer=ref.writer,
        reader=reader.name,
        table_name=ref.table_name,
        table_path=ref.path,
        status=status,
        elapsed_seconds=time.monotonic() - start,
        error=error,
        actual=actual,
        expected=expected,
    )


def _plan_tasks(
    mode: str,
    cases: list[str],
    writers: list[str],
    readers: list[str],
    engines: dict[str, Engine],
) -> tuple[list[tuple[str, str]], list[tuple[str, str, str]]]:
    writable = [name for name in writers if engines[name].can_write]
    readable = [name for name in readers if engines[name].can_read]
    if mode == "full":
        write_tasks = [(writer, case) for writer in writable for case in cases]
        read_tasks = [
            (writer, reader, case)
            for writer in writable
            for case in cases
            for reader in readable
        ]
        return write_tasks, read_tasks

    write_tasks: set[tuple[str, str]] = set()
    read_tasks: set[tuple[str, str, str]] = set()
    core_readers = [reader for reader in ("pyarrow", "spark", "datafusion") if reader in readable]

    for writer in ("spark", "pyarrow", "datafusion", "flink"):
        if writer in writable and "basic_append" in cases:
            write_tasks.add((writer, "basic_append"))
            for reader in core_readers:
                read_tasks.add((writer, reader, "basic_append"))

    if "daft" in writable:
        if "basic_append" in cases:
            write_tasks.add(("daft", "basic_append"))
            for reader in readable:
                read_tasks.add(("daft", reader, "basic_append"))
        if "pk_upsert" in cases:
            write_tasks.add(("daft", "pk_upsert"))
            for reader in core_readers:
                read_tasks.add(("daft", reader, "pk_upsert"))

    for writer in ("spark", "pyarrow"):
        if writer in writable:
            for case in ("basic_append", "partitioned_append"):
                if case in cases:
                    write_tasks.add((writer, case))
                    for reader in readable:
                        read_tasks.add((writer, reader, case))

    for writer in ("spark", "pyarrow", "datafusion"):
        if writer in writable and "pk_upsert" in cases:
            write_tasks.add((writer, "pk_upsert"))
            for reader in core_readers:
                read_tasks.add((writer, reader, "pk_upsert"))

    for writer in ("spark", "flink", "pyarrow"):
        if writer in writable and "schema_types" in cases:
            write_tasks.add((writer, "schema_types"))
            for reader in core_readers:
                read_tasks.add((writer, reader, "schema_types"))

    return sorted(write_tasks), sorted(read_tasks)


def _resolve_cases(cases_arg: str, mode: str) -> list[str]:
    if cases_arg == "smoke":
        names = list(SMOKE_CASES)
    elif cases_arg == "full":
        names = list(FULL_CASES if mode == "full" else SMOKE_CASES)
    else:
        names = [part.strip() for part in cases_arg.split(",") if part.strip()]
    unknown = sorted(set(names) - set(CASES))
    if unknown:
        raise SystemExit(f"unknown compatibility cases: {','.join(unknown)}")
    return names


def _resolve_engines(
    args: argparse.Namespace,
    engines: dict[str, Engine],
) -> tuple[list[str], list[str]]:
    if args.writers:
        writers = _split_csv(args.writers)
    else:
        writers = _default_writers(args.engines)
    if args.readers:
        readers = _split_csv(args.readers)
    else:
        readers = _default_readers(args.engines)
    unknown = sorted((set(writers) | set(readers)) - set(engines))
    if unknown:
        raise SystemExit(f"unknown compatibility engines: {','.join(unknown)}")
    return writers, readers


def _default_writers(group: str) -> list[str]:
    groups = {
        "all": ["spark", "flink", "pyarrow", "datafusion", "daft"],
        "python": ["pyarrow", "ray", "daft"],
        "jvm": ["spark", "flink"],
        "rust": ["datafusion"],
    }
    return groups[group]


def _default_readers(group: str) -> list[str]:
    groups = {
        "all": ["spark", "flink", "pyspark", "pyarrow", "datafusion", "ray", "daft"],
        "python": ["pyarrow", "pyspark", "ray", "daft"],
        "jvm": ["spark", "flink", "pyspark"],
        "rust": ["datafusion", "pyarrow"],
    }
    return groups[group]


def _split_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _visible_partition_columns(case: CaseSpec) -> tuple[str, ...]:
    if case.read_columns is None:
        return case.partition_by
    visible = set(case.read_columns)
    return tuple(column for column in case.partition_by if column in visible)


def _object_store_options(storage: str) -> dict[str, str]:
    if not storage.startswith(("s3://", "s3a://")):
        return {}
    bucket = storage.split("://", 1)[1].split("/", 1)[0]
    return {
        "fs.s3a.bucket": os.environ.get("LAKESOUL_COMPAT_S3_BUCKET", bucket),
        "fs.s3a.endpoint": os.environ.get("LAKESOUL_COMPAT_S3_ENDPOINT", "http://127.0.0.1:9000"),
        "fs.s3a.access.key": os.environ.get("LAKESOUL_COMPAT_S3_ACCESS_KEY", "rustfsadmin"),
        "fs.s3a.secret.key": os.environ.get("LAKESOUL_COMPAT_S3_SECRET_KEY", "rustfsadmin"),
        "fs.s3a.path.style.access": "true",
    }


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["smoke", "full"], default="smoke")
    parser.add_argument("--storage", default="file:///tmp/lakesoul-compat")
    parser.add_argument("--engines", choices=["all", "python", "jvm", "rust"], default="all")
    parser.add_argument("--writers", help="comma-separated writer engine override")
    parser.add_argument("--readers", help="comma-separated reader engine override")
    parser.add_argument("--cases", default="smoke", help="smoke, full, or comma-separated case names")
    parser.add_argument("--output-dir", default="/tmp/lakesoul-compat-artifacts")
    parser.add_argument("--run-id")
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[3]),
        help="LakeSoul repository root",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
