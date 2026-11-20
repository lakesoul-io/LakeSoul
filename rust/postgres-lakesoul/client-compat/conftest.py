# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Shared fixtures for the real PostgreSQL client baseline.

The suite is deliberately driver-facing: every test goes through psycopg2,
psycopg3 or SQLAlchemy rather than the Rust test harness. It is run against a
live `postgres-lakesoul` process by `run.sh`.
"""

from __future__ import annotations

import os
import uuid
from contextlib import closing
from urllib.parse import quote

import pytest

HOST = os.environ.get("PGHOST", "127.0.0.1")
PORT = int(os.environ.get("PGPORT", "55432"))
DATABASE = os.environ.get("PGDATABASE", "default")
USER = os.environ.get("PGUSER", "lakesoul_test")
PASSWORD = os.environ.get("PGPASSWORD", "lakesoul_test")

# `run.sh` starts the server with --require-tls and exports sslmode=require.
# A manual run against a plaintext server still defaults to disable.
DEFAULT_SSLMODE = os.environ.get("PGSSLMODE", "disable")


def make_dsn(sslmode: str | None = None) -> str:
    sslmode = sslmode or DEFAULT_SSLMODE
    return (
        f"postgresql://{quote(USER, safe='')}:{quote(PASSWORD, safe='')}"
        f"@{HOST}:{PORT}/{quote(DATABASE, safe='')}"
        f"?sslmode={sslmode}&application_name=lakesoul-client-compat"
    )


def connect(driver: str, *, autocommit: bool = True, sslmode: str | None = None):
    """Open a connection through one of the configured real drivers."""
    effective = DEFAULT_SSLMODE if sslmode is None else sslmode
    dsn = os.environ.get("LAKESOUL_CLIENT_DSN")
    if dsn is None or effective != "disable":
        dsn = make_dsn(effective)
    if driver == "psycopg2":
        import psycopg2

        connection = psycopg2.connect(dsn)
        connection.autocommit = autocommit
        return connection
    if driver == "psycopg3":
        import psycopg

        return psycopg.connect(dsn, autocommit=autocommit)
    raise AssertionError(f"unknown driver {driver!r}")


def sqlstate(error: BaseException) -> str | None:
    """Return the server SQLSTATE from a psycopg2 or psycopg3 exception."""
    return getattr(error, "pgcode", None) or getattr(error, "sqlstate", None)


def unique_table(prefix: str = "client_compat") -> str:
    # PostgreSQL folds unquoted identifiers to lower case; the generated name
    # is already lower case and stays well below the 63-byte limit.
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


@pytest.fixture
def connector():
    return connect


@pytest.fixture
def pg_env():
    return {
        "PGHOST": HOST,
        "PGPORT": str(PORT),
        "PGDATABASE": DATABASE,
        "PGUSER": USER,
        "PGPASSWORD": PASSWORD,
        "PGSSLMODE": DEFAULT_SSLMODE,
    }


@pytest.fixture
def sqlalchemy_url():
    return (
        f"postgresql+psycopg2://{quote(USER, safe='')}:{quote(PASSWORD, safe='')}"
        f"@{HOST}:{PORT}/{quote(DATABASE, safe='')}?sslmode={DEFAULT_SSLMODE}"
    )


@pytest.fixture
def table_factory():
    """Create tables through psycopg2 and drop them at the end of the test."""
    names: list[str] = []

    def create(columns: str, prefix: str = "client_compat") -> str:
        name = unique_table(prefix)
        with closing(connect("psycopg2")) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {name}")
                cursor.execute(f"CREATE TABLE {name} ({columns})")
        names.append(name)
        return name

    yield create

    with closing(connect("psycopg2")) as connection:
        with connection.cursor() as cursor:
            for name in names:
                cursor.execute(f"DROP TABLE IF EXISTS {name}")


def pytest_sessionstart(session):
    """Print the exact client versions that produced this run."""
    versions = []
    try:
        import psycopg2

        versions.append(f"psycopg2={psycopg2.__version__}")
    except Exception as error:  # pragma: no cover - dependency check
        versions.append(f"psycopg2=<missing: {error}>")
    try:
        import psycopg

        versions.append(f"psycopg={psycopg.__version__}")
    except Exception as error:  # pragma: no cover - dependency check
        versions.append(f"psycopg=<missing: {error}>")
    try:
        import sqlalchemy

        versions.append(f"sqlalchemy={sqlalchemy.__version__}")
    except Exception as error:  # pragma: no cover - dependency check
        versions.append(f"sqlalchemy=<missing: {error}>")
    print("\nclient versions: " + " ".join(versions))
    print(f"server dsn: postgresql://{HOST}:{PORT}/{DATABASE}")
