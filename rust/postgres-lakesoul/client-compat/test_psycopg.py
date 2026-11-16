# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Real-client baseline for the PostgreSQL wire front end.

The tests are intentionally small and workflow-shaped: connect, introspect,
parameterized reads/writes, batches, UPSERT, transactions, errors, and the
features that import/BI tools commonly depend on. Unsupported features are
asserted to fail explicitly (`0A000`) rather than silently doing nothing.
"""

from __future__ import annotations

import io
import os
import shutil
import subprocess
from contextlib import closing
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from conftest import sqlstate

DRIVERS = ["psycopg2", "psycopg3"]


@pytest.fixture(params=DRIVERS, ids=DRIVERS)
def driver(request):
    return request.param


def rows(cursor):
    return cursor.fetchall()


def test_connect_select_and_session_identity(driver, connector):
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 AS one, 'x'::text AS label")
            assert rows(cursor) == [(1, "x")]

            cursor.execute("SELECT current_database(), current_schema()")
            database, schema = rows(cursor)[0]
            assert database
            assert schema == "public"

            # These are probes, not failures: the baseline records which
            # version string a BI tool would see.
            for sql in ("SHOW server_version", "SELECT version()"):
                try:
                    cursor.execute(sql)
                    print(f"{driver} {sql}: {rows(cursor)}")
                except Exception as error:
                    print(f"{driver} {sql}: refused ({sqlstate(error)!r})")
                    if not connection.autocommit:
                        connection.rollback()

        print(
            f"{driver}: server_version={getattr(connection, 'server_version', None) or connection.info.server_version}"
        )


def test_version_function_is_driver_compatible(connector):
    """SQLAlchemy rejects a dialect whose version() has no PostgreSQL
    prefix; keep the compatibility string and its engine suffix distinct."""
    with closing(connector("psycopg3")) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT version()")
            version = rows(cursor)[0][0]
            assert version.startswith("PostgreSQL 14.0 "), version
            assert "LakeSoul" in version, version


def test_sslmode_prefer_falls_back_to_plaintext(connector):
    """Most libpq/JDBC clients default to prefer, not disable: the server must
    answer the SSLRequest and continue when no TLS listener is configured."""
    with closing(connector("psycopg3", sslmode="prefer")) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert rows(cursor) == [(1,)]


def test_common_bi_startup_queries(connector):
    """Metabase/DBeaver emit these session statements before their metadata
    query. They must not fail just because the server has no PostgreSQL
    process behind it."""
    statements = [
        "SET extra_float_digits = 2",
        "SET application_name = 'lakesoul-client-compat'",
        "SHOW TRANSACTION ISOLATION LEVEL",
        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
        "SHOW timezone",
    ]
    with closing(connector("psycopg3")) as connection:
        with connection.cursor() as cursor:
            for sql in statements:
                cursor.execute(sql)
                if cursor.description is not None:
                    print(f"BI startup {sql}: {rows(cursor)}")

            for sql in (
                "SELECT current_setting('search_path')",
                "SELECT current_setting('server_version')",
            ):
                try:
                    cursor.execute(sql)
                    print(f"BI setting {sql}: {rows(cursor)}")
                except Exception as error:
                    print(f"BI setting {sql}: refused ({sqlstate(error)!r})")
                    if not connection.autocommit:
                        connection.rollback()


def test_parameterized_types_round_trip(driver, connector, table_factory):
    table = table_factory(
        "id bigint primary key, name text, amount numeric(20,4), "
        "day date, ts timestamptz, flag boolean"
    )
    expected_ts = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} "
                "(id, name, amount, day, ts, flag) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    1,
                    "alice",
                    Decimal("12.3400"),
                    date(2026, 1, 2),
                    expected_ts,
                    True,
                ),
            )
            cursor.execute(
                f"SELECT id, name, amount, day, ts, flag FROM {table} WHERE id = %s",
                (1,),
            )
            row = rows(cursor)[0]
            assert row[0] == 1
            assert row[1] == "alice"
            assert row[2] == Decimal("12.3400")
            assert row[3] == date(2026, 1, 2)
            timestamp = row[4]
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            assert timestamp.astimezone(timezone.utc) == expected_ts
            assert row[5] is True


def test_bytea_parameter_round_trip(driver, connector, table_factory):
    table = table_factory("id bigint primary key, payload bytea", "client_bytea")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} (id, payload) VALUES (%s, %s)",
                (1, b"\x00\x01\xff"),
            )
            cursor.execute(f"SELECT payload FROM {table} WHERE id = 1")
            assert bytes(rows(cursor)[0][0]) == b"\x00\x01\xff"

            cursor.execute(
                f"SELECT payload FROM {table} WHERE payload = %s",
                (b"\x00\x01\xff",),
            )
            assert bytes(rows(cursor)[0][0]) == b"\x00\x01\xff"

            cursor.execute(
                f"UPDATE {table} SET payload = %s WHERE id = 1",
                (b"\xde\xad\xbe\xef",),
            )
            cursor.execute(f"SELECT payload FROM {table} WHERE id = 1")
            assert bytes(rows(cursor)[0][0]) == b"\xde\xad\xbe\xef"


def test_bytea_text_literal(driver, connector):
    """The PostgreSQL hex input form is accepted for literal bytea values."""
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(r"SELECT '\x0001ff'::bytea")
            assert bytes(rows(cursor)[0][0]) == b"\x00\x01\xff"
            cursor.execute(r"SELECT '\000\001\377'::bytea")
            assert bytes(rows(cursor)[0][0]) == b"\x00\x01\xff"


def test_executemany_and_batch_insert(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_batch")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"INSERT INTO {table} (id, name) VALUES (%s, %s)",
                [(1, "a"), (2, "b"), (3, "c")],
            )
            if driver == "psycopg2":
                from psycopg2.extras import execute_values

                execute_values(
                    cursor,
                    f"INSERT INTO {table} (id, name) VALUES %s",
                    [(4, "d"), (5, "e")],
                )
            else:
                # psycopg3's executemany normally uses its pipeline path.
                cursor.executemany(
                    f"INSERT INTO {table} (id, name) VALUES (%s, %s)",
                    [(4, "d"), (5, "e")],
                )

            cursor.execute(f"SELECT id, name FROM {table} ORDER BY id")
            assert rows(cursor) == [
                (1, "a"),
                (2, "b"),
                (3, "c"),
                (4, "d"),
                (5, "e"),
            ]


def test_upsert_on_conflict_do_update(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_upsert")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} (id, name) VALUES (%s, %s)", (1, "old")
            )
            cursor.execute(
                f"INSERT INTO {table} (id, name) VALUES (%s, %s) "
                "ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
                (1, "new"),
            )
            cursor.execute(f"SELECT id, name FROM {table}")
            assert rows(cursor) == [(1, "new")]


def test_transaction_rollback_and_commit(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_tx")

    with closing(connector(driver, autocommit=False)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} (id, name) VALUES (%s, %s)", (1, "rolled")
            )
            cursor.execute(f"SELECT count(*) FROM {table}")
            assert rows(cursor) == [(1,)]
        connection.rollback()

    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {table}")
            assert rows(cursor) == [(0,)]

    with closing(connector(driver, autocommit=False)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} (id, name) VALUES (%s, %s)", (2, "kept")
            )
        connection.commit()

    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT id, name FROM {table}")
            assert rows(cursor) == [(2, "kept")]


def test_duplicate_key_sqlstate_and_recovery(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_error")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} (id, name) VALUES (%s, %s)", (1, "a"))
            with pytest.raises(Exception) as caught:
                cursor.execute(
                    f"INSERT INTO {table} (id, name) VALUES (%s, %s)", (1, "b")
                )
            assert sqlstate(caught.value) == "23505"
            cursor.execute("SELECT 1")
            assert rows(cursor) == [(1,)]


def test_catalog_introspection(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_catalog")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
            assert rows(cursor) == [(table,)]

            cursor.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s "
                "ORDER BY ordinal_position",
                (table,),
            )
            columns = rows(cursor)
            assert [column[0] for column in columns] == ["id", "name"]

            cursor.execute(
                "SELECT c.relname FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' AND c.relname = %s",
                (table,),
            )
            assert rows(cursor) == [(table,)]


def test_psql_describe_table(table_factory, pg_env):
    """psql is the libpq metadata path; its column query must remain usable."""
    table = table_factory("id bigint primary key, name text", "client_psql")
    if shutil.which("psql") is None:
        pytest.skip("psql is not installed")
    result = subprocess.run(
        ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", f"\\d {table}"],
        env={**os.environ, **pg_env},
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    assert table in result.stdout


def test_server_side_named_cursor_is_not_supported(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_cursor")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} (id, name) VALUES (%s, %s)", (1, "a"))
        try:
            with connection.cursor(name="compat_named_cursor") as cursor:
                cursor.execute(f"SELECT id FROM {table}")
                cursor.fetchall()
        except Exception as error:
            state = sqlstate(error)
            print(f"{driver} named cursor failure SQLSTATE={state!r}")
            # psycopg2 probes pg_catalog.pg_cursors before it sends DECLARE;
            # that catalog is not implemented yet and produces XX000. psycopg3
            # reaches the explicit 0A000 refusal. Both mean the workflow is
            # unavailable, but only the latter is an explicit contract.
            assert state in {"0A000", "XX000"}
        else:
            pytest.fail(
                "server-side named cursor unexpectedly succeeded; update the "
                "compatibility baseline"
            )


def test_copy_from_stdin_is_explicitly_refused(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_copy")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            try:
                if driver == "psycopg2":
                    cursor.copy_expert(
                        f"COPY {table} (id, name) FROM STDIN WITH (FORMAT csv)",
                        io.StringIO("1,alice\n"),
                    )
                else:
                    with cursor.copy(
                        f"COPY {table} (id, name) FROM STDIN WITH (FORMAT csv)"
                    ) as copy:
                        copy.write("1,alice\n")
            except Exception as error:
                assert sqlstate(error) == "0A000"
                cursor.execute("SELECT 1")
                assert rows(cursor) == [(1,)]
            else:
                pytest.fail(
                    "COPY FROM STDIN unexpectedly succeeded; update the "
                    "compatibility baseline"
                )


def test_returning_is_explicitly_refused(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_returning")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    f"INSERT INTO {table} (id, name) VALUES (%s, %s) RETURNING id",
                    (1, "a"),
                )
            except Exception as error:
                assert sqlstate(error) == "0A000"
                cursor.execute("SELECT 1")
                assert rows(cursor) == [(1,)]
            else:
                pytest.fail(
                    "INSERT ... RETURNING unexpectedly succeeded; update the "
                    "compatibility baseline"
                )


def test_sqlalchemy_select_and_write(sqlalchemy_url, table_factory):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine, text

    table = table_factory("id bigint primary key, name text", "client_sqlalchemy")
    engine = create_engine(sqlalchemy_url)
    try:
        with engine.connect() as connection:
            assert connection.execute(text("SELECT 1")).scalar() == 1
            connection.execute(
                text(f"INSERT INTO {table} (id, name) VALUES (:id, :name)"),
                {"id": 1, "name": "alice"},
            )
            connection.commit()
            assert (
                connection.execute(text(f"SELECT name FROM {table}")).scalar()
                == "alice"
            )
    finally:
        engine.dispose()
    print(f"sqlalchemy={sqlalchemy.__version__}")


def test_sqlalchemy_reflection(sqlalchemy_url, table_factory):
    """Reflection is a BI/tooling path that must see columns and the primary
    key, and report that LakeSoul has no secondary indexes or foreign keys."""
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import MetaData, Table, create_engine, inspect

    table = table_factory(
        "id bigint primary key, tenant bigint, name text", "client_reflect"
    )
    engine = create_engine(sqlalchemy_url)
    try:
        metadata = MetaData()
        reflected = Table(table, metadata, autoload_with=engine)
        assert [column.name for column in reflected.columns] == [
            "id",
            "tenant",
            "name",
        ]
        assert [column.name for column in reflected.primary_key.columns] == ["id"]
        assert reflected.primary_key.name == f"{table}_pkey"

        inspector = inspect(engine)
        assert inspector.get_indexes(table) == []
        assert inspector.get_foreign_keys(table) == []
        assert inspector.get_unique_constraints(table) == []
        assert inspector.get_check_constraints(table) == []
    finally:
        engine.dispose()
