# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

import pyarrow as pa

RUN_ID = "release_legacy"
BASIC_ROWS = (
    (
        {"id": 1, "label": "alpha", "score": 1.5, "active": True, "note": "first"},
        {"id": 2, "label": "beta", "score": None, "active": False, "note": None},
    ),
    ({"id": 3, "label": "gamma", "score": -7.25, "active": None, "note": "last"},),
)
UPGRADE_ROW = (
    {
        "id": 4,
        "label": "upgrade-parquet",
        "score": 4.0,
        "active": True,
        "note": "4.0 parquet window",
    },
)
SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("label", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("active", pa.bool_()),
        pa.field("note", pa.string()),
    ]
)


def _join_uri(base: str, *parts: str) -> str:
    return "/".join([base.rstrip("/"), *(part.strip("/") for part in parts)])


def _local_path(uri: str) -> Path:
    parsed = urlparse(uri)
    if parsed.scheme not in ("", "file"):
        raise ValueError("legacy release fixtures currently require file:// storage")
    return Path(unquote(parsed.path if parsed.scheme else uri))


def _table(rows: tuple[dict, ...]) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=SCHEMA)


def _ref(storage: str, writer: str, case: str) -> dict[str, str]:
    return {
        "writer": writer,
        "case": case,
        "table_name": f"compat_{RUN_ID}_{writer}_{case}",
        "table_path": _join_uri(storage, RUN_ID, writer, case),
    }


def _record(ref: dict[str, str], files: list[str], formats: list[str]) -> dict:
    return {
        "operation": "write",
        **ref,
        "reader": None,
        "status": "passed",
        "elapsed_seconds": 0.0,
        "error": None,
        "actual": None,
        "expected": None,
        "produced_files": files,
        "physical_formats": formats,
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _files(uri: str) -> list[str]:
    root = _local_path(uri)
    return [
        str(path)
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.suffix in {".parquet", ".vortex"}
    ]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def backup_manifest(args: argparse.Namespace) -> None:
    metadata = Path(args.metadata).resolve()
    table_data = Path(args.table_data).resolve()
    metadata_checksum = _sha256(metadata)
    table_checksum = _sha256(table_data)
    backup_set_id = hashlib.sha256(
        f"{metadata_checksum}:{table_checksum}".encode()
    ).hexdigest()[:20]
    _write_json(
        Path(args.output),
        {
            "backup_set_id": backup_set_id,
            "metadata": {
                "path": str(metadata),
                "sha256": metadata_checksum,
            },
            "table_data": {
                "path": str(table_data),
                "sha256": table_checksum,
            },
        },
    )


def verify_backup(args: argparse.Namespace) -> None:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    for member in ("metadata", "table_data"):
        path = Path(manifest[member]["path"])
        actual = _sha256(path)
        expected = manifest[member]["sha256"]
        if actual != expected:
            raise RuntimeError(
                f"backup set {manifest['backup_set_id']} {member} checksum "
                f"{actual} != {expected}"
            )


def write_spark_3_0(args: argparse.Namespace) -> None:
    jar = Path(args.spark_jar).resolve()
    checksum = hashlib.sha256(jar.read_bytes()).hexdigest()
    if checksum != args.spark_jar_sha256:
        raise RuntimeError(
            f"LakeSoul 3.0 Spark JAR checksum {checksum} != {args.spark_jar_sha256}"
        )

    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("id", LongType(), nullable=False),
            StructField("label", StringType(), nullable=True),
            StructField("score", DoubleType(), nullable=True),
            StructField("active", BooleanType(), nullable=True),
            StructField("note", StringType(), nullable=True),
        ]
    )
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("LakeSoul-3.0-release-fixture")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.jars", str(jar))
        .config(
            "spark.sql.extensions",
            "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog",
        )
        .getOrCreate()
    )
    refs = {
        "parquet": _ref(args.storage, "legacy_3_0", "legacy_parquet"),
        "mixed": _ref(args.storage, "legacy_mixed", "legacy_mixed"),
    }
    try:
        for ref in refs.values():
            path = _local_path(ref["table_path"])
            if path.exists():
                shutil.rmtree(path)
        parquet = refs["parquet"]
        for index, rows in enumerate(BASIC_ROWS):
            writer = spark.createDataFrame(list(rows), schema=schema).write.format(
                "lakesoul"
            )
            if index == 0:
                writer.mode("overwrite").option(
                    "shortTableName", parquet["table_name"]
                ).save(str(_local_path(parquet["table_path"])))
            else:
                writer.mode("append").save(str(_local_path(parquet["table_path"])))

        mixed = refs["mixed"]
        (
            spark.createDataFrame(list(BASIC_ROWS[0]), schema=schema)
            .write.format("lakesoul")
            .mode("overwrite")
            .option("shortTableName", mixed["table_name"])
            .option("hashPartitions", "id")
            .option("hashBucketNum", "1")
            .save(str(_local_path(mixed["table_path"])))
        )
    finally:
        spark.stop()

    parquet_files = _files(refs["parquet"]["table_path"])
    mixed_files = _files(refs["mixed"]["table_path"])
    if not parquet_files or not mixed_files:
        raise RuntimeError("LakeSoul 3.0 writer produced no Parquet files")
    if any(Path(path).suffix != ".parquet" for path in parquet_files + mixed_files):
        raise RuntimeError("LakeSoul 3.0 fixture contains a non-Parquet file")
    _write_json(
        Path(args.output),
        {
            "run_id": RUN_ID,
            "mode": "legacy-fixture",
            "storage": args.storage,
            "writers": ["legacy_3_0"],
            "readers": [],
            "cases": ["legacy_parquet", "legacy_mixed"],
            "source_manifest": None,
            "legacy": {
                "core_tag": "v3.0.0",
                "spark_jar_sha256": checksum,
            },
            "records": [
                _record(
                    parquet,
                    parquet_files,
                    ["parquet"] * len(parquet_files),
                ),
                _record(
                    mixed,
                    mixed_files,
                    ["parquet"] * len(mixed_files),
                ),
            ],
            "refs": refs,
        },
    )


def verify_spark_3_0(args: argparse.Namespace) -> None:
    state = json.loads(Path(args.state).read_text(encoding="utf-8"))
    jar = Path(args.spark_jar).resolve()
    if _sha256(jar) != args.spark_jar_sha256:
        raise RuntimeError("LakeSoul 3.0 Spark JAR checksum changed")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("LakeSoul-3.0-release-recovery")
        .config("spark.ui.enabled", "false")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.jars", str(jar))
        .config(
            "spark.sql.extensions",
            "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog",
        )
        .getOrCreate()
    )
    try:
        rows = [
            row.asDict(recursive=True)
            for row in spark.read.format("lakesoul")
            .load(str(_local_path(state["refs"]["parquet"]["table_path"])))
            .collect()
        ]
    finally:
        spark.stop()
    expected = sorted(
        [row for batch in BASIC_ROWS for row in batch], key=lambda row: row["id"]
    )
    if sorted(rows, key=lambda row: row["id"]) != expected:
        raise RuntimeError(f"LakeSoul 3.0 restored rows {rows} != {expected}")


def _assert_result_format(result, expected: str) -> tuple[list[str], list[str]]:
    files = [file_info.path for file_info in result.files]
    formats = [
        file_info.other_info.get("physical_format", expected)
        for file_info in result.files
    ]
    if not files or set(formats) != {expected}:
        raise RuntimeError(
            f"writer result formats {sorted(set(formats))} != {[expected]}"
        )
    return files, formats


def write_upgrade_parquet(args: argparse.Namespace) -> None:
    from lakesoul import LakeSoulCatalog

    source = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    catalog = LakeSoulCatalog.from_env()
    mixed_record = next(
        record for record in source["records"] if record["case"] == "legacy_mixed"
    )
    # The upgrade appends current-format Vortex files into the table the
    # v3.0.0 writer created, so every reader exercises merge-on-read across
    # physical formats within one table.
    mixed_vortex = catalog.table(mixed_record["table_name"]).write_arrow(
        _table(BASIC_ROWS[1]), format="vortex"
    )
    vortex_files, vortex_formats = _assert_result_format(mixed_vortex, "vortex")

    before = set(_files(source["storage"]))
    mixed_result = catalog.table(mixed_record["table_name"]).write_arrow(
        _table(UPGRADE_ROW), format="parquet"
    )
    mixed_files, mixed_formats = _assert_result_format(mixed_result, "parquet")

    pk_ref = _ref(source["storage"], "upgrade_parquet", "pk_upsert")
    pk_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("value", pa.int32()),
        ]
    )
    pk_batches = (
        pa.Table.from_pylist(
            [
                {"id": 1, "name": "alice", "value": 10},
                {"id": 2, "name": "bob", "value": 20},
                {"id": 3, "name": "carol", "value": 30},
            ],
            schema=pk_schema,
        ),
        pa.Table.from_pylist(
            [
                {"id": 2, "name": "bob-updated", "value": 200},
                {"id": 4, "name": "dave", "value": 40},
            ],
            schema=pk_schema,
        ),
    )
    catalog.drop_table(pk_ref["table_name"], if_exists=True)
    pk_table = catalog.create_table(
        pk_ref["table_name"],
        path=pk_ref["table_path"],
        schema=pk_schema,
        primary_keys=("id",),
        hash_bucket_num=2,
    )
    pk_files: list[str] = []
    pk_formats: list[str] = []
    for batch in pk_batches:
        files, formats = _assert_result_format(
            pk_table.write_arrow(batch, format="parquet"), "parquet"
        )
        pk_files.extend(files)
        pk_formats.extend(formats)

    after = set(_files(source["storage"]))
    new_files = sorted(after - before)
    if not new_files or any(Path(path).suffix != ".parquet" for path in new_files):
        raise RuntimeError(
            f"Parquet-only window produced unexpected files: {new_files}"
        )

    records = [
        record for record in source["records"] if record["case"] != "legacy_mixed"
    ]
    records.append(
        _record(
            {
                **mixed_record,
                "case": "upgrade_window_mixed",
            },
            list(mixed_record.get("produced_files", [])) + vortex_files + mixed_files,
            list(mixed_record.get("physical_formats", []))
            + vortex_formats
            + mixed_formats,
        )
    )
    records.append(_record(pk_ref, pk_files, pk_formats))
    _write_json(
        Path(args.output),
        {
            **source,
            "mode": "parquet-upgrade-window",
            "cases": [
                "legacy_parquet",
                "upgrade_window_mixed",
                "pk_upsert",
            ],
            "parquet_upgrade_files": new_files,
            "records": records,
        },
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="operation", required=True)

    spark = subparsers.add_parser("spark-3.0")
    spark.add_argument("--storage", required=True)
    spark.add_argument("--spark-jar", required=True)
    spark.add_argument("--spark-jar-sha256", required=True)

    spark_verify = subparsers.add_parser("verify-spark-3.0")
    spark_verify.add_argument("--state", required=True)
    spark_verify.add_argument("--spark-jar", required=True)
    spark_verify.add_argument("--spark-jar-sha256", required=True)
    spark.add_argument("--output", required=True)

    upgrade = subparsers.add_parser("upgrade-parquet")
    upgrade.add_argument("--manifest", required=True)
    upgrade.add_argument("--output", required=True)

    backup = subparsers.add_parser("backup-manifest")
    backup.add_argument("--metadata", required=True)
    backup.add_argument("--table-data", required=True)
    backup.add_argument("--output", required=True)

    verify = subparsers.add_parser("verify-backup")
    verify.add_argument("--manifest", required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.operation == "spark-3.0":
        write_spark_3_0(args)
    elif args.operation == "verify-spark-3.0":
        verify_spark_3_0(args)
    elif args.operation == "upgrade-parquet":
        write_upgrade_parquet(args)
    elif args.operation == "backup-manifest":
        backup_manifest(args)
    else:
        verify_backup(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
