# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors

from __future__ import annotations

import datetime as dt
import glob
import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import pyarrow as pa

from compat.cases import CaseSpec
from compat.normalize import normalize_table


@dataclass(frozen=True, slots=True)
class TableRef:
    case_name: str
    writer: str
    table_name: str
    path: str


@dataclass(slots=True)
class CompatContext:
    repo_root: Path
    python_dir: Path
    output_dir: Path
    storage_uri: str
    run_id: str
    object_store_options: dict[str, str]

    def table_ref(self, case: CaseSpec, writer: str) -> TableRef:
        safe_writer = writer.replace("-", "_")
        table_name = f"compat_{self.run_id}_{safe_writer}_{case.name}"
        return TableRef(
            case_name=case.name,
            writer=writer,
            table_name=table_name,
            path=_join_uri(self.storage_uri, self.run_id, safe_writer, case.name),
        )

    def log_file(self, *parts: str) -> Path:
        log_dir = self.output_dir / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        safe = "_".join(_safe_name(part) for part in parts)
        return log_dir / f"{safe}.log"

    def sql_file(self, *parts: str) -> Path:
        sql_dir = self.output_dir / "sql"
        sql_dir.mkdir(parents=True, exist_ok=True)
        safe = "_".join(_safe_name(part) for part in parts)
        return sql_dir / f"{safe}.sql"


class Engine:
    name: str
    can_write = True
    can_read = True

    def __init__(self, name: str | None = None) -> None:
        if name is not None:
            self.name = name

    def write_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> None:
        raise NotImplementedError

    def read_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> pa.Table:
        raise NotImplementedError

    def close(self) -> None:
        return None


class PyArrowEngine(Engine):
    name = "pyarrow"

    def write_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> None:
        from lakesoul import LakeSoulCatalog

        _remove_local_table_path(ref.path)
        catalog = LakeSoulCatalog.from_env(
            object_store_options=ctx.object_store_options
        )
        catalog.drop_table(ref.table_name, if_exists=True)
        table = catalog.create_table(
            ref.table_name,
            path=ref.path,
            schema=case.schema,
            partition_by=case.partition_by,
            primary_keys=case.primary_keys,
            hash_bucket_num=_python_hash_bucket_num(case),
        )
        for batch in case.batches:
            table.write_arrow(batch)

    def read_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> pa.Table:
        from lakesoul import LakeSoulCatalog

        catalog = LakeSoulCatalog.from_env(
            object_store_options=ctx.object_store_options
        )
        scan = catalog.scan(
            ref.table_name,
            columns=case.read_columns,
            filter=_arrow_filter(case),
            retain_partition_columns=True,
        )
        return normalize_table(scan.to_arrow_table(), case.read_schema)


class SparkEngine(Engine):
    name = "spark"

    def __init__(self, name: str = "spark", *, can_write: bool = True) -> None:
        super().__init__(name)
        self.can_write = can_write
        self._spark = None

    def write_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> None:
        if not self.can_write:
            raise RuntimeError(f"{self.name} is configured as reader-only")
        from lakesoul import LakeSoulCatalog
        from lakesoul.spark import LakeSoulTable

        _remove_local_table_path(ref.path)
        LakeSoulCatalog.from_env().drop_table(ref.table_name, if_exists=True)
        spark = self._session(ctx)
        spark_path = _spark_path(ref.path)
        for index, batch in enumerate(case.batches):
            df = self._dataframe(case, batch, ctx)
            if index == 0:
                writer = (
                    df.write.format("lakesoul")
                    .mode("overwrite")
                    .option("shortTableName", ref.table_name)
                )
                if case.primary_keys:
                    writer = writer.option(
                        "hashBucketNum", str(case.hash_bucket_num)
                    )
                if case.partition_by:
                    writer = writer.option(
                        "rangePartitions", ",".join(case.partition_by)
                    )
                if case.primary_keys:
                    writer = writer.option(
                        "hashPartitions", ",".join(case.primary_keys)
                    )
                writer.save(spark_path)
            elif case.primary_keys:
                LakeSoulTable.forPath(spark, spark_path).upsert(df)
            else:
                df.write.format("lakesoul").mode("append").save(spark_path)

    def read_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> pa.Table:
        spark = self._session(ctx)
        df = spark.read.format("lakesoul").load(_spark_path(ref.path))
        for column, value in (case.read_partition_filter or {}).items():
            df = df.filter(df[column] == value)
        if case.read_columns is not None:
            df = df.select(*case.read_columns)
        return _pyspark_to_arrow(spark, df, case.read_schema)

    def close(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    def _session(self, ctx: CompatContext):
        if self._spark is not None:
            return self._spark
        from pyspark.sql import SparkSession

        jars = _spark_jars(ctx.repo_root)
        warehouse_dir = ctx.output_dir / f"{self.name}-warehouse"
        builder = (
            SparkSession.builder.appName(f"LakeSoulCompat-{self.name}")
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config(
                "spark.sql.extensions",
                "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog",
            )
            .config("spark.sql.warehouse.dir", str(warehouse_dir))
            .config("spark.jars", ",".join(jars))
        )
        for key, value in _spark_object_store_configs(ctx.object_store_options).items():
            builder = builder.config(key, value)
        self._spark = builder.getOrCreate()
        return self._spark

    def _dataframe(self, case: CaseSpec, table: pa.Table, ctx: CompatContext):
        spark = self._session(ctx)
        rows = table.to_pylist()
        for row in rows:
            for key, value in row.items():
                if isinstance(value, dt.datetime) and value.tzinfo is None:
                    row[key] = value.replace(tzinfo=dt.timezone.utc)
        return spark.createDataFrame(rows, schema=_spark_schema(case.schema))


class RayEngine(Engine):
    name = "ray"

    def __init__(self) -> None:
        self._started = False

    def write_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> None:
        import ray

        from lakesoul import LakeSoulCatalog

        self._ensure_ray(ray)
        _remove_local_table_path(ref.path)
        catalog = LakeSoulCatalog.from_env(
            object_store_options=ctx.object_store_options
        )
        catalog.drop_table(ref.table_name, if_exists=True)
        table = catalog.create_table(
            ref.table_name,
            path=ref.path,
            schema=case.schema,
            partition_by=case.partition_by,
            primary_keys=case.primary_keys,
            hash_bucket_num=_python_hash_bucket_num(case),
        )
        for batch in case.batches:
            table.write_ray(ray.data.from_arrow(batch))

    def read_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> pa.Table:
        import ray

        from lakesoul import LakeSoulCatalog

        self._ensure_ray(ray)
        scan = LakeSoulCatalog.from_env(
            object_store_options=ctx.object_store_options
        ).scan(
            ref.table_name,
            columns=case.read_columns,
            filter=_arrow_filter(case),
            retain_partition_columns=True,
        )
        dataset = scan.to_ray()
        batches = list(dataset.iter_batches(batch_format="pyarrow"))
        if not batches:
            return pa.Table.from_batches([], schema=case.read_schema)
        tables = [
            batch if isinstance(batch, pa.Table) else pa.Table.from_batches([batch])
            for batch in batches
        ]
        return normalize_table(
            pa.concat_tables(tables, promote_options="default"), case.read_schema
        )

    def close(self) -> None:
        if self._started:
            import ray

            ray.shutdown()
            self._started = False

    def _ensure_ray(self, ray_module: Any) -> None:
        if self._started:
            return
        os.environ.pop("RAY_RUNTIME_ENV_HOOK", None)
        ray_module.init(address=None, runtime_env=None, include_dashboard=False)
        self._started = True


class DaftEngine(Engine):
    name = "daft"

    def write_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> None:
        import daft

        from lakesoul import LakeSoulCatalog

        _remove_local_table_path(ref.path)
        catalog = LakeSoulCatalog.from_env(
            object_store_options=ctx.object_store_options
        )
        catalog.drop_table(ref.table_name, if_exists=True)
        table = catalog.create_table(
            ref.table_name,
            path=ref.path,
            schema=case.schema,
            partition_by=case.partition_by,
            primary_keys=case.primary_keys,
            hash_bucket_num=_python_hash_bucket_num(case),
        )
        for batch in case.batches:
            table.write_daft(daft.from_arrow(batch))

    def read_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> pa.Table:
        from lakesoul import LakeSoulCatalog

        scan = LakeSoulCatalog.from_env(
            object_store_options=ctx.object_store_options
        ).scan(
            ref.table_name,
            columns=case.read_columns,
            filter=_arrow_filter(case),
            retain_partition_columns=True,
        )
        daft_df = scan.to_daft()
        if hasattr(daft_df, "collect"):
            daft_df = daft_df.collect()
        if hasattr(daft_df, "to_arrow"):
            return normalize_table(daft_df.to_arrow(), case.read_schema)
        if hasattr(daft_df, "to_arrow_iter"):
            tables = list(daft_df.to_arrow_iter())
            if not tables:
                return pa.Table.from_batches([], schema=case.read_schema)
            return normalize_table(pa.concat_tables(tables), case.read_schema)
        raise RuntimeError(
            "Daft DataFrame does not expose a supported Arrow export API"
        )


class DataFusionEngine(Engine):
    name = "datafusion"

    def write_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> None:
        _remove_local_table_path(ref.path)
        sql_file = ctx.sql_file(self.name, "write", ref.table_name)
        statements = [
            f"DROP TABLE IF EXISTS {ref.table_name};",
            _datafusion_create_table_sql(case, ref),
        ]
        for batch in case.batches:
            statements.append(
                _insert_sql(ref.table_name, case.schema, batch, "datafusion")
            )
        sql_file.write_text("\n".join(statements) + "\n", encoding="utf-8")
        _run_command(
            _datafusion_command(ctx, sql_file),
            ctx.repo_root,
            ctx.log_file(self.name, "write", ref.table_name),
        )

    def read_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> pa.Table:
        sql_file = ctx.sql_file(self.name, "read", ref.table_name, case.name)
        sql_file.write_text(
            _select_sql(ref.table_name, case, "datafusion") + "\n", encoding="utf-8"
        )
        output = _run_command(
            _datafusion_command(ctx, sql_file),
            ctx.repo_root,
            ctx.log_file(self.name, "read", ref.table_name, case.name),
        )
        rows = _parse_datafusion_table(output, case.read_schema)
        return _table_from_rows(rows, case.read_schema)


class FlinkEngine(Engine):
    name = "flink"

    def write_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> None:
        _remove_local_table_path(ref.path)
        sql_file = ctx.sql_file(self.name, "write", ref.table_name)
        statements = [
            f"DROP TABLE IF EXISTS {ref.table_name};",
            _flink_create_table_sql(case, ref),
        ]
        for batch in case.batches:
            statements.append(_insert_sql(ref.table_name, case.schema, batch, "flink"))
        sql_file.write_text("\n".join(statements) + "\n", encoding="utf-8")
        _run_flink_sql(
            ctx, sql_file, None, ctx.log_file(self.name, "write", ref.table_name)
        )

    def read_case(self, case: CaseSpec, ref: TableRef, ctx: CompatContext) -> pa.Table:
        sql_file = ctx.sql_file(self.name, "read", ref.table_name, case.name)
        output_file = (
            ctx.output_dir / "flink-output" / f"{ref.table_name}_{case.name}.json"
        )
        output_file.parent.mkdir(parents=True, exist_ok=True)
        sql_file.write_text(
            _select_sql(ref.table_name, case, "flink") + "\n", encoding="utf-8"
        )
        _run_flink_sql(
            ctx,
            sql_file,
            output_file,
            ctx.log_file(self.name, "read", ref.table_name, case.name),
        )
        payload = json.loads(output_file.read_text(encoding="utf-8"))
        rows = [dict(zip(payload["columns"], values)) for values in payload["rows"]]
        return _table_from_rows(rows, case.read_schema)


def engine_registry() -> dict[str, Engine]:
    return {
        "pyarrow": PyArrowEngine(),
        "spark": SparkEngine("spark", can_write=True),
        "pyspark": SparkEngine("pyspark", can_write=False),
        "ray": RayEngine(),
        "daft": DaftEngine(),
        "datafusion": DataFusionEngine(),
        "flink": FlinkEngine(),
    }


def _arrow_filter(case: CaseSpec) -> Any | None:
    if not case.read_partition_filter:
        return None
    import pyarrow.compute as pc

    expression = None
    for column, value in case.read_partition_filter.items():
        current = pc.field(column) == value
        expression = current if expression is None else expression & current
    return expression


def _pyspark_to_arrow(
    spark: Any, df: Any, expected_schema: pa.Schema
) -> pa.Table:
    """Collect a PySpark DataFrame as a PyArrow Table, avoiding timestamp tz shift.

    PySpark converts java.sql.Timestamp to Python datetime via
    fromtimestamp(), which uses the OS local timezone.  We work around this
    by casting timestamp columns to string on the Spark side (Spark's
    Timestamp.toString() renders in session timezone, which is UTC for our
    harness) and parsing back to UTC datetimes in Python.
    """
    from pyspark.sql import functions as F

    timestamp_columns = [
        field.name
        for field in expected_schema
        if pa.types.is_timestamp(field.type)
    ]

    select_exprs = []
    for field in expected_schema:
        name = field.name
        if name in timestamp_columns:
            select_exprs.append(F.col(name).cast("string").alias(name))
        else:
            select_exprs.append(F.col(name))

    rows = df.select(*select_exprs).collect()
    parsed: list[dict[str, Any]] = []
    for row in rows:
        d = row.asDict(recursive=True)
        for col in timestamp_columns:
            raw = d[col]
            if raw is not None:
                d[col] = _parse_spark_timestamp_string(raw)
        parsed.append(d)
    return _table_from_rows(parsed, expected_schema)


def _parse_spark_timestamp_string(raw: str) -> dt.datetime:
    """Parse a Spark Timestamp string rendered in session timezone (UTC).

    Spark 3.3+ renders TimestampType.cast(string) as
    ``yyyy-MM-dd HH:mm:ss.SSSSSS``, omitting trailing zero fractional
    digits when they are all zero.
    """
    if "." in raw:
        return dt.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S.%f")
    return dt.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")


def _table_from_rows(rows: list[dict[str, Any]], schema: pa.Schema) -> pa.Table:
    if not rows:
        return pa.Table.from_batches([], schema=schema)
    return normalize_table(pa.Table.from_pylist(rows, schema=schema), schema)


def _join_uri(base: str, *parts: str) -> str:
    base = base.rstrip("/")
    suffix = "/".join(part.strip("/") for part in parts)
    return f"{base}/{suffix}"


def _local_path(uri: str) -> Path | None:
    parsed = urlparse(uri)
    if parsed.scheme in {"", "file"}:
        return Path(unquote(parsed.path if parsed.scheme else uri))
    return None


def _remove_local_table_path(uri: str) -> None:
    path = _local_path(uri)
    if path is not None and path.exists():
        shutil.rmtree(path)


def _spark_path(uri: str) -> str:
    path = _local_path(uri)
    return str(path) if path is not None else uri


def _safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value)


def _spark_jars(repo_root: Path) -> list[str]:
    source_dir = Path(os.environ.get("LAKESOUL_SOURCE_DIR", repo_root))
    pattern = (
        source_dir / "lakesoul-spark" / "target" / "lakesoul-spark-3.3-*-SNAPSHOT.jar"
    )
    jars = [
        path
        for path in glob.glob(str(pattern))
        if not path.endswith(("-sources.jar", "-javadoc.jar", "-tests.jar"))
    ]
    if not jars:
        raise RuntimeError(f"LakeSoul Spark jar not found with pattern {pattern}")
    return jars


def _spark_schema(schema: pa.Schema):
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    fields = []
    for field in schema:
        data_type = field.type
        if pa.types.is_int32(data_type):
            spark_type = IntegerType()
        elif pa.types.is_int64(data_type):
            spark_type = LongType()
        elif pa.types.is_float64(data_type):
            spark_type = DoubleType()
        elif pa.types.is_boolean(data_type):
            spark_type = BooleanType()
        elif pa.types.is_date32(data_type) or pa.types.is_date64(data_type):
            spark_type = DateType()
        elif pa.types.is_timestamp(data_type):
            spark_type = TimestampType()
        elif pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
            spark_type = StringType()
        else:
            raise TypeError(f"unsupported Spark compatibility type: {field}")
        fields.append(StructField(field.name, spark_type, nullable=field.nullable))
    return StructType(fields)


def _spark_object_store_configs(options: dict[str, str]) -> dict[str, str]:
    configs = {}
    for key, value in options.items():
        if key.startswith("fs."):
            configs[f"spark.hadoop.{key}"] = value
    return configs


def _sql_type(field: pa.Field, engine: str) -> str:
    data_type = field.type
    if pa.types.is_int32(data_type):
        return "INT"
    if pa.types.is_int64(data_type):
        return "BIGINT"
    if pa.types.is_float64(data_type):
        return "DOUBLE"
    if pa.types.is_boolean(data_type):
        return "BOOLEAN"
    if pa.types.is_date32(data_type) or pa.types.is_date64(data_type):
        return "DATE"
    if pa.types.is_timestamp(data_type):
        return "TIMESTAMP"
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "STRING" if engine == "flink" else "VARCHAR"
    raise TypeError(f"unsupported SQL compatibility type: {field}")


def _quote_ident(name: str, engine: str) -> str:
    return f"`{name}`" if engine == "flink" else name


def _python_hash_bucket_num(case: CaseSpec) -> int | None:
    if case.primary_keys or case.hash_bucket_num != 1:
        return case.hash_bucket_num
    return None


def _datafusion_create_table_sql(case: CaseSpec, ref: TableRef) -> str:
    columns = [
        f"{_quote_ident(field.name, 'datafusion')} {_sql_type(field, 'datafusion')}"
        + (" NOT NULL" if not field.nullable else "")
        for field in case.schema
    ]
    if case.primary_keys:
        columns.append(f"PRIMARY KEY({','.join(case.primary_keys)})")
    partition_clause = (
        f" PARTITIONED BY ({','.join(case.partition_by)})" if case.partition_by else ""
    )
    return (
        f"CREATE EXTERNAL TABLE {ref.table_name} (\n  "
        + ",\n  ".join(columns)
        + f"\n) STORED AS LAKESOUL{partition_clause} LOCATION '{ref.path}';"
    )


def _flink_create_table_sql(case: CaseSpec, ref: TableRef) -> str:
    columns = [
        f"{_quote_ident(field.name, 'flink')} {_sql_type(field, 'flink')}"
        + (" NOT NULL" if not field.nullable else "")
        for field in case.schema
    ]
    if case.primary_keys:
        keys = ",".join(_quote_ident(col, "flink") for col in case.primary_keys)
        columns.append(f"PRIMARY KEY ({keys}) NOT ENFORCED")
    partition_clause = (
        " PARTITIONED BY ("
        + ",".join(_quote_ident(col, "flink") for col in case.partition_by)
        + ")"
        if case.partition_by
        else ""
    )
    hash_bucket = (
        f",'hashBucketNum'='{case.hash_bucket_num}'"
        if case.primary_keys
        else ""
    )
    return (
        f"CREATE TABLE {ref.table_name} (\n  "
        + ",\n  ".join(columns)
        + f"\n){partition_clause} WITH ("
        + "'connector'='lakesoul',"
        + "'format'='parquet',"
        + f"'path'='{ref.path}'"
        + hash_bucket
        + ");"
    )


def _insert_sql(
    table_name: str, schema: pa.Schema, table: pa.Table, engine: str
) -> str:
    columns = ",".join(_quote_ident(name, engine) for name in schema.names)
    values = []
    for row in table.to_pylist():
        values.append(
            "("
            + ",".join(
                _sql_literal(row[name], schema.field(name).type, engine)
                for name in schema.names
            )
            + ")"
        )
    return (
        f"INSERT INTO {table_name} ({columns}) VALUES\n  " + ",\n  ".join(values) + ";"
    )


def _select_sql(table_name: str, case: CaseSpec, engine: str) -> str:
    columns = case.read_columns or tuple(case.schema.names)
    sql = (
        "SELECT "
        + ",".join(_quote_ident(col, engine) for col in columns)
        + f" FROM {table_name}"
    )
    if case.read_partition_filter:
        predicates = []
        for col, value in case.read_partition_filter.items():
            field_type = case.schema.field(col).type
            predicates.append(
                f"{_quote_ident(col, engine)} = {_sql_literal(value, field_type, engine)}"
            )
        sql += " WHERE " + " AND ".join(predicates)
    return sql + ";"


def _sql_literal(value: Any, data_type: pa.DataType, engine: str) -> str:
    if value is None:
        if engine == "flink":
            return f"CAST(NULL AS {_sql_type(pa.field('', data_type), engine)})"
        return "NULL"
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "'" + str(value).replace("'", "''") + "'"
    if pa.types.is_boolean(data_type):
        return "TRUE" if value else "FALSE"
    if pa.types.is_date32(data_type) or pa.types.is_date64(data_type):
        return f"DATE '{value.isoformat()}'"
    if pa.types.is_timestamp(data_type):
        text = (
            value.isoformat(sep=" ") if isinstance(value, dt.datetime) else str(value)
        )
        return f"TIMESTAMP '{text}'"
    return str(value)


def _coerce_value(value: Any, data_type: pa.DataType) -> Any:
    if value is None:
        return None
    if isinstance(value, str) and value == "":
        return None
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return str(value)
    if pa.types.is_int32(data_type) or pa.types.is_int64(data_type):
        return int(value)
    if pa.types.is_float64(data_type):
        return float(value)
    if pa.types.is_boolean(data_type):
        return value if isinstance(value, bool) else str(value).lower() == "true"
    if pa.types.is_date32(data_type) or pa.types.is_date64(data_type):
        return (
            value if isinstance(value, dt.date) else dt.date.fromisoformat(str(value))
        )
    if pa.types.is_timestamp(data_type):
        if isinstance(value, dt.datetime):
            return value
        return dt.datetime.fromisoformat(str(value).replace("T", " "))
    return value


def _datafusion_command(ctx: CompatContext, sql_file: Path) -> list[str]:
    cmd = [
        "cargo",
        "-q",
        "run",
        "-p",
        "lakesoul-console",
        "--",
        "--log-dir",
        str(ctx.output_dir / "datafusion-logs"),
    ]
    if ctx.storage_uri.startswith(("s3://", "s3a://")):
        cmd.extend(
            [
                "--warehouse-prefix",
                ctx.storage_uri,
                "--endpoint",
                ctx.object_store_options.get("fs.s3a.endpoint", ""),
                "--s3-bucket",
                ctx.object_store_options.get("fs.s3a.bucket", ""),
                "--s3-access-key",
                ctx.object_store_options.get("fs.s3a.access.key", ""),
                "--s3-secret-key",
                ctx.object_store_options.get("fs.s3a.secret.key", ""),
            ]
        )
    cmd.extend(["-f", str(sql_file)])
    return cmd


def _run_flink_sql(
    ctx: CompatContext,
    sql_file: Path,
    output_file: Path | None,
    log_file: Path,
) -> str:
    cmd = [
        "mvn",
        "-q",
        "-B",
        "test",
        "-pl",
        "lakesoul-flink",
        "-am",
        "-Pcross-build",
        "--file",
        "pom.xml",
        "-Dtest=CompatibilitySqlRunnerTest",
        f"-Dlakesoul.compat.sqlFile={sql_file}",
        "-Dsurefire.failIfNoSpecifiedTests=false",
        "-DargLine=--add-opens=java.base/java.nio=ALL-UNNAMED",
    ]
    if output_file is not None:
        cmd.append(f"-Dlakesoul.compat.output={output_file}")
    if ctx.storage_uri.startswith(("s3://", "s3a://")):
        cmd.extend(
            [
                "-Dlakesoul.compat.s3.bucket="
                + ctx.object_store_options.get("fs.s3a.bucket", ""),
                "-Dlakesoul.compat.s3.endpoint="
                + ctx.object_store_options.get("fs.s3a.endpoint", ""),
                "-Dlakesoul.compat.s3.accessKey="
                + ctx.object_store_options.get("fs.s3a.access.key", ""),
                "-Dlakesoul.compat.s3.secretKey="
                + ctx.object_store_options.get("fs.s3a.secret.key", ""),
                "-Dlakesoul.compat.s3.pathStyleAccess="
                + ctx.object_store_options.get("fs.s3a.path.style.access", "true"),
            ]
        )
    return _run_command(cmd, ctx.repo_root, log_file)


def _run_command(cmd: list[str], cwd: Path, log_file: Path) -> str:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    log_file.write_text(completed.stdout, encoding="utf-8")
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed with exit code {completed.returncode}: {' '.join(cmd)}; "
            f"log={log_file}"
        )
    return completed.stdout


def _parse_datafusion_table(output: str, schema: pa.Schema) -> list[dict[str, Any]]:
    table_lines = [line for line in output.splitlines() if line.startswith("|")]
    if not table_lines:
        return []
    header = [part.strip() for part in table_lines[0].strip("|").split("|")]
    rows = []
    for line in table_lines[1:]:
        values = [part.strip() for part in line.strip("|").split("|")]
        if len(values) != len(header):
            continue
        row = {}
        for name, raw in zip(header, values):
            row[name] = _coerce_value(raw, schema.field(name).type)
        rows.append(row)
    return rows
