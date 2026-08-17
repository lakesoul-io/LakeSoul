#!/usr/bin/env python3
# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MIGRATIONS_DIR = ROOT / "script" / "metadata-migrations"
MIGRATION_PATTERN = re.compile(r"^V([1-9][0-9]*)__([a-z0-9][a-z0-9_]*)\.sql$")
HISTORY_TABLE = "lakesoul_schema_migrations"
ADVISORY_LOCK_ID = 5498709836673204300


class MigrationError(Exception):
    pass


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    checksum: str
    sql: str
    path: Path


@dataclass(frozen=True)
class AppliedMigration:
    version: int
    description: str
    checksum: str


def load_migrations(directory: Path) -> list[Migration]:
    if not directory.is_dir():
        raise MigrationError(f"migration directory does not exist: {directory}")

    migrations: list[Migration] = []
    for path in sorted(directory.iterdir()):
        if path.name.startswith("."):
            continue
        match = MIGRATION_PATTERN.fullmatch(path.name)
        if not match:
            raise MigrationError(f"invalid migration filename: {path.name}")
        sql = path.read_text(encoding="utf-8")
        if not sql.strip():
            raise MigrationError(f"empty migration: {path.name}")
        if re.search(r"^\s*\\", sql, re.MULTILINE):
            raise MigrationError(
                f"psql commands are not allowed in migration: {path.name}"
            )
        if re.search(
            r"^\s*(BEGIN|COMMIT|ROLLBACK)\s*;", sql, re.IGNORECASE | re.MULTILINE
        ):
            raise MigrationError(
                f"transaction control is managed by the runner: {path.name}"
            )
        migrations.append(
            Migration(
                version=int(match.group(1)),
                description=match.group(2).replace("_", " "),
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                sql=sql,
                path=path,
            )
        )

    versions = [migration.version for migration in migrations]
    if len(versions) != len(set(versions)):
        raise MigrationError("migration versions must be unique")
    migrations.sort(key=lambda migration: migration.version)
    if not migrations:
        raise MigrationError(f"no migrations found in {directory}")
    return migrations


def normalize_database_url(value: str) -> str:
    value = value.strip()
    if not value:
        raise MigrationError("PostgreSQL URL is empty")
    if value.startswith("jdbc:"):
        value = value[5:]
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or not parsed.hostname
        or not parsed.path.strip("/")
    ):
        raise MigrationError(
            "PostgreSQL URL must identify a PostgreSQL host and database"
        )
    query = urlencode(
        [(key, item) for key, item in parse_qsl(parsed.query) if key != "stringtype"]
    )
    return urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment)
    )


def database_url(argument: str | None) -> str:
    configured = (
        argument or os.environ.get("LAKESOUL_PG_URL") or os.environ.get("DATABASE_URL")
    )
    if configured is None:
        raise MigrationError(
            "PostgreSQL URL is required; pass --database-url or set LAKESOUL_PG_URL"
        )
    return normalize_database_url(configured)


def psql_environment() -> dict[str, str]:
    environment = os.environ.copy()
    if "PGUSER" not in environment and environment.get("LAKESOUL_PG_USERNAME"):
        environment["PGUSER"] = environment["LAKESOUL_PG_USERNAME"]
    if "PGPASSWORD" not in environment and environment.get("LAKESOUL_PG_PASSWORD"):
        environment["PGPASSWORD"] = environment["LAKESOUL_PG_PASSWORD"]
    return environment


def run_psql(url: str, *, sql: str) -> str:
    command = [
        "psql",
        "-X",
        "--no-psqlrc",
        "--set=ON_ERROR_STOP=1",
        "--tuples-only",
        "--no-align",
        "--field-separator=\t",
        "--dbname",
        url,
    ]
    try:
        result = subprocess.run(
            command,
            input=sql,
            text=True,
            capture_output=True,
            env=psql_environment(),
        )
    except FileNotFoundError as error:
        raise MigrationError("psql is required to run metadata migrations") from error
    if result.returncode:
        detail = result.stderr.strip() or result.stdout.strip()
        raise MigrationError(f"psql failed: {detail}")
    return result.stdout


def history_exists(url: str) -> bool:
    output = run_psql(
        url,
        sql=f"SELECT to_regclass('public.{HISTORY_TABLE}') IS NOT NULL;\n",
    )
    return output.strip() == "t"


def load_applied(url: str) -> list[AppliedMigration]:
    if not history_exists(url):
        return []
    output = run_psql(
        url,
        sql=(
            f"SELECT version, description, checksum FROM {HISTORY_TABLE} "
            "ORDER BY version;\n"
        ),
    )
    applied: list[AppliedMigration] = []
    for line in output.splitlines():
        if not line:
            continue
        fields = line.split("\t")
        if len(fields) != 3:
            raise MigrationError("invalid migration history row returned by PostgreSQL")
        applied.append(AppliedMigration(int(fields[0]), fields[1], fields[2]))
    return applied


def validate_history(
    migrations: list[Migration],
    applied: list[AppliedMigration],
    *,
    require_current: bool,
) -> list[str]:
    available = {migration.version: migration for migration in migrations}
    errors: list[str] = []
    for record in applied:
        migration = available.get(record.version)
        if migration is None:
            errors.append(
                f"applied migration V{record.version} is absent from the repository"
            )
            continue
        if record.checksum != migration.checksum:
            errors.append(f"checksum mismatch for {migration.path.name}")
        if record.description != migration.description:
            errors.append(f"description mismatch for {migration.path.name}")
    if require_current:
        applied_versions = {record.version for record in applied}
        for migration in migrations:
            if migration.version not in applied_versions:
                errors.append(f"pending migration {migration.path.name}")
    return errors


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def migration_script(migrations: list[Migration]) -> str:
    statements = [
        "\\set ON_ERROR_STOP on",
        f"SELECT pg_advisory_lock({ADVISORY_LOCK_ID});",
        f"""CREATE TABLE IF NOT EXISTS {HISTORY_TABLE}
(
    version bigint PRIMARY KEY,
    description text NOT NULL,
    checksum char(64) NOT NULL,
    installed_at timestamptz NOT NULL DEFAULT now(),
    installed_by text NOT NULL DEFAULT current_user
);""",
    ]
    for migration in migrations:
        description = sql_literal(migration.description)
        checksum = sql_literal(migration.checksum)
        statements.extend(
            [
                f"""DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM {HISTORY_TABLE}
        WHERE version = {migration.version} AND checksum <> {checksum}
    ) THEN
        RAISE EXCEPTION 'checksum mismatch for migration V{migration.version}';
    END IF;
END
$$;""",
                f"SELECT EXISTS (SELECT 1 FROM {HISTORY_TABLE} WHERE version = {migration.version}) AS migration_applied \\gset",
                "\\if :migration_applied",
                "\\else",
                "BEGIN;",
                migration.sql.rstrip(),
                f"""INSERT INTO {HISTORY_TABLE}(version, description, checksum)
VALUES ({migration.version}, {description}, {checksum});""",
                "COMMIT;",
                "\\endif",
            ]
        )
    statements.append(f"SELECT pg_advisory_unlock({ADVISORY_LOCK_ID});")
    return "\n".join(statements) + "\n"


def check(url: str, migrations: list[Migration]) -> None:
    if not history_exists(url):
        raise MigrationError(
            f"metadata migration history table {HISTORY_TABLE} is missing"
        )
    errors = validate_history(migrations, load_applied(url), require_current=True)
    if errors:
        raise MigrationError(
            "metadata schema is not current:\n- " + "\n- ".join(errors)
        )
    print(f"Metadata schema is current at V{migrations[-1].version}.")


def migrate(url: str, migrations: list[Migration]) -> None:
    applied = load_applied(url)
    errors = validate_history(migrations, applied, require_current=False)
    if errors:
        raise MigrationError(
            "metadata migration history is invalid:\n- " + "\n- ".join(errors)
        )
    run_psql(url, sql=migration_script(migrations))
    check(url, migrations)


def status(url: str, migrations: list[Migration]) -> None:
    applied = load_applied(url)
    records = {record.version: record for record in applied}
    for migration in migrations:
        record = records.get(migration.version)
        if record is None:
            state = "pending"
        elif (
            record.checksum == migration.checksum
            and record.description == migration.description
        ):
            state = "applied"
        else:
            state = "invalid"
        print(f"V{migration.version}\t{state}\t{migration.description}")
    available = {migration.version for migration in migrations}
    for record in applied:
        if record.version not in available:
            print(f"V{record.version}\tunknown\t{record.description}")


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage LakeSoul PostgreSQL metadata migrations"
    )
    parser.add_argument("operation", choices=("migrate", "check", "status"))
    parser.add_argument(
        "--database-url", help="PostgreSQL URL; defaults to LAKESOUL_PG_URL"
    )
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=DEFAULT_MIGRATIONS_DIR,
        help="directory containing versioned SQL migrations",
    )
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        migrations = load_migrations(args.migrations_dir.resolve())
        url = database_url(args.database_url)
        if args.operation == "migrate":
            migrate(url, migrations)
        elif args.operation == "check":
            check(url, migrations)
        else:
            status(url, migrations)
    except MigrationError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
