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


def test_jsonb_parameter_and_result_round_trip(driver, connector, table_factory):
    table = table_factory("id bigint primary key, payload jsonb", "client_jsonb")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            if driver == "psycopg2":
                from psycopg2.extras import Json

                parameter = Json({"b": 2, "a": 1})
            else:
                from psycopg.types.json import Jsonb

                parameter = Jsonb({"b": 2, "a": 1})
            cursor.execute(
                f"INSERT INTO {table} (id, payload) VALUES (%s, %s)",
                (1, parameter),
            )
            connection.commit()

            cursor.execute(f"SELECT payload FROM {table} WHERE id = 1")
            assert rows(cursor)[0][0] == {"a": 1, "b": 2}

            cursor.execute(
                f"SELECT payload->>'a', payload ? 'b' FROM {table} WHERE id = 1"
            )
            assert rows(cursor)[0] == ("1", True)

            cursor.execute(f"SELECT payload->>'missing' FROM {table} WHERE id = 1")
            assert rows(cursor)[0][0] is None


def test_delete_and_truncate(driver, connector, table_factory):
    table = table_factory("id bigint primary key, val bigint", "client_delete")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"INSERT INTO {table} (id, val) VALUES (%s, %s)",
                [(1, 10), (2, 20), (3, 30)],
            )
            connection.commit()

            cursor.execute(f"DELETE FROM {table} WHERE id = %s", (2,))
            assert cursor.rowcount == 1
            cursor.execute(f"SELECT id FROM {table} ORDER BY id")
            assert rows(cursor) == [(1,), (3,)]

            cursor.execute(f"DELETE FROM {table} WHERE id = %s RETURNING id, val", (3,))
            assert rows(cursor) == [(3, 30)]
            connection.commit()

            cursor.execute(f"TRUNCATE TABLE {table}")
            cursor.execute(f"SELECT count(*) FROM {table}")
            assert rows(cursor) == [(0,)]


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

            bytea_table = table_factory(
                "id bigint primary key, payload bytea", "client_returning_bytea"
            )
            cursor.execute(
                f"INSERT INTO {bytea_table} (id, payload) VALUES (%s, %s) "
                "RETURNING payload",
                (1, b"\x00\x01\xff"),
            )
            assert bytes(rows(cursor)[0][0]) == b"\x00\x01\xff"


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


def test_session_parameters_and_catalog(driver, connector):
    """BI tools read session state before a report: the PostgreSQL version,
    `SHOW` values, `current_setting`, `pg_settings` and the role list. The
    role list used to abort the server with a stack overflow."""
    expected_user = os.environ.get("PGUSER", "lakesoul_test")
    with closing(connector(driver)) as connection:
        server_version = (
            connection.server_version
            if driver == "psycopg2"
            else connection.info.server_version
        )
        assert server_version == 140000, server_version

        with connection.cursor() as cursor:
            for sql, expected in [
                ("SHOW server_version", "14.0"),
                ("SHOW server_version_num", "140000"),
                ("SHOW standard_conforming_strings", "on"),
                ("SHOW client_encoding", "UTF8"),
                ("SHOW transaction isolation level", "read committed"),
                ("SHOW transaction_isolation", "read committed"),
                ("SELECT current_setting('server_version_num')", "140000"),
            ]:
                cursor.execute(sql)
                assert rows(cursor) == [(expected,)], (sql, rows(cursor))

            cursor.execute("SELECT current_user")
            assert rows(cursor) == [(expected_user,)]
            cursor.execute("SELECT session_user")
            assert rows(cursor) == [(expected_user,)]

            cursor.execute("SELECT set_config('application_name', 'compat', false)")
            assert rows(cursor) == [("compat",)]
            cursor.execute("SHOW application_name")
            assert rows(cursor) == [("compat",)]
            cursor.execute("SELECT current_setting('application_name')")
            assert rows(cursor) == [("compat",)]

            cursor.execute("SELECT count(*) FROM pg_catalog.pg_roles")
            assert rows(cursor)[0][0] >= 0
            cursor.execute("SELECT count(*) FROM pg_catalog.pg_settings")
            assert rows(cursor)[0][0] > 20
            cursor.execute(
                "SELECT setting FROM pg_catalog.pg_settings "
                "WHERE name = 'server_version_num'"
            )
            assert rows(cursor) == [("140000",)]

            cursor.execute(
                "SELECT to_char(TIMESTAMP '2024-01-02 03:04:05', "
                "'YYYY-MM-DD HH24:MI:SS')"
            )
            assert rows(cursor) == [("2024-01-02 03:04:05",)]
            cursor.execute("SELECT to_char(DATE '2024-01-02', 'DD Mon YYYY')")
            assert rows(cursor) == [("02 Jan 2024",)]


def test_psql_metadata_commands(table_factory, pg_env):
    r"""`\dt`, `\l` and `\dn` are the psql discovery commands BI users run
    first; they must succeed and list the created table."""
    table = table_factory("id bigint primary key, name text", "client_psql_meta")
    if shutil.which("psql") is None:
        pytest.skip("psql is not installed")
    for command in (r"\dt", r"\l", r"\dn"):
        result = subprocess.run(
            ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", command],
            env={**os.environ, **pg_env},
            text=True,
            capture_output=True,
        )
        assert result.returncode == 0, (command, result.stderr)
    result = subprocess.run(
        ["psql", "-X", "-v", "ON_ERROR_STOP=1", "-c", r"\dt"],
        env={**os.environ, **pg_env},
        text=True,
        capture_output=True,
    )
    assert table in result.stdout, result.stdout


def test_server_side_named_cursor(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_cursor")
    with closing(connector(driver, autocommit=False)) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                f"INSERT INTO {table} (id, name) VALUES (%s, %s)",
                [(1, "a"), (2, "b"), (3, "c"), (4, "d")],
            )
        connection.commit()

        with connection.cursor(name="compat_named_cursor") as cursor:
            cursor.execute(f"SELECT id, name FROM {table} ORDER BY id")
            assert cursor.fetchmany(2) == [(1, "a"), (2, "b")]
            assert cursor.fetchone() == (3, "c")
            assert cursor.fetchall() == [(4, "d")]

        # Parameters are bound in DECLARE on psycopg3 and by the client on
        # psycopg2; both must see the same rows.
        with connection.cursor(name="compat_named_param") as cursor:
            cursor.execute(
                f"SELECT id, name FROM {table} WHERE id >= %s ORDER BY id",
                (3,),
            )
            assert cursor.fetchall() == [(3, "c"), (4, "d")]

        # CLOSE drops the cursor; the name can be declared again.
        with connection.cursor(name="compat_named_cursor") as cursor:
            cursor.execute(f"SELECT id FROM {table} ORDER BY id")
            assert cursor.fetchall() == [(1,), (2,), (3,), (4,)]
        connection.commit()


def test_pg_cursors_metadata(driver, connector, table_factory):
    """Both drivers can inspect an active cursor through pg_cursors; psycopg2
    probes it before DECLARE and other tools read its PostgreSQL columns."""
    table = table_factory("id bigint primary key, name text", "client_cursor_meta")
    with closing(connector(driver, autocommit=False)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} (id, name) VALUES (1, 'a')")
        connection.commit()

        with connection.cursor(name="compat_meta_cursor") as cursor:
            cursor.execute(f"SELECT id FROM {table}")
            with connection.cursor() as plain:
                plain.execute(
                    "SELECT name, is_holdable, is_binary, is_scrollable "
                    "FROM pg_catalog.pg_cursors "
                    "WHERE name = 'compat_meta_cursor'"
                )
                assert plain.fetchall() == [("compat_meta_cursor", False, False, False)]
            assert cursor.fetchone() == (1,)
        connection.commit()


def test_copy_to_stdout(driver, connector, table_factory):
    """Both drivers export TEXT and CSV through COPY TO STDOUT, including a
    NULL and a backslash that the TEXT format must escape."""
    table = table_factory(
        "id bigint primary key, name text, amount numeric(20,4)",
        "client_copy_out",
    )
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO " + table + " (id, name, amount) VALUES "
                "(1, 'a\\tb', 12.3400), (2, NULL, -0.5000)"
            )

            text_sql = "COPY (SELECT id, name FROM " + table + " ORDER BY id) TO STDOUT"
            if driver == "psycopg2":
                text = io.StringIO()
                cursor.copy_expert(text_sql, text)
                assert cursor.rowcount == 2
                assert text.getvalue() == "1\ta\\\\tb\n2\t\\N\n"
            else:
                with cursor.copy(text_sql) as copy:
                    assert b"".join(copy) == b"1\ta\\\\tb\n2\t\\N\n"

            csv_sql = (
                "COPY (SELECT id, name FROM "
                + table
                + " ORDER BY id) TO STDOUT WITH (FORMAT csv, HEADER true)"
            )
            if driver == "psycopg2":
                csv = io.StringIO()
                cursor.copy_expert(csv_sql, csv)
                assert csv.getvalue() == "id,name\n1,a\\tb\n2,\n"
            else:
                with cursor.copy(csv_sql) as copy:
                    assert b"".join(copy) == b"id,name\n1,a\\tb\n2,\n"
                with cursor.copy(
                    "COPY (SELECT id, name FROM " + table + " WHERE id = %s) TO STDOUT",
                    (1,),
                ) as copy:
                    assert b"".join(copy) == b"1\ta\\\\tb\n"


def test_psql_copy_to(tmp_path, table_factory, pg_env, connector):
    """psql \copy TO is the interactive export path; it must write the file
    and report the row count."""
    table = table_factory("id bigint primary key, name text", "client_psql_copy_out")
    with closing(connector("psycopg2")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO " + table + " (id, name) VALUES (1, 'alice'), (2, NULL)"
            )
    if shutil.which("psql") is None:
        pytest.skip("psql is not installed")
    path = tmp_path / "copy_out.csv"
    result = subprocess.run(
        [
            "psql",
            "-X",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            f"\\copy {table} (id, name) TO '{path}' WITH (FORMAT csv, HEADER true)",
        ],
        env={**os.environ, **pg_env},
        text=True,
        capture_output=True,
    )
    assert result.returncode == 0, result.stderr
    assert "COPY 2" in result.stdout, result.stdout
    lines = path.read_text().splitlines()
    assert lines[0] == "id,name"
    assert sorted(lines[1:]) == ["1,alice", "2,"]


def test_copy_from_stdin_text_and_csv(driver, connector, table_factory):
    """Both drivers load TEXT and CSV through COPY FROM STDIN, including NULL,
    an escaped newline, a quoted comma and a quoted newline."""
    table = table_factory("id bigint primary key, name text", "client_copy")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            text = "COPY " + table + " (id, name) FROM STDIN"
            if driver == "psycopg2":
                cursor.copy_expert(
                    text, io.StringIO("1\talice\n2\t\\N\n3\tline\\nnext\n")
                )
            else:
                with cursor.copy(text) as copy:
                    copy.write("1\talice\n")
                    copy.write("2\t\\N\n")
                    copy.write("3\tline\\nnext\n")
            cursor.execute("SELECT id, name FROM " + table + " ORDER BY id")
            assert rows(cursor) == [
                (1, "alice"),
                (2, None),
                (3, "line\nnext"),
            ]

            csv = (
                "COPY "
                + table
                + " (id, name) FROM STDIN WITH (FORMAT csv, HEADER true)"
            )
            data = 'id,name\n4,"bob, jr"\n5,"line\nnext"\n6,""\n'
            if driver == "psycopg2":
                cursor.copy_expert(csv, io.StringIO(data))
            else:
                with cursor.copy(csv) as copy:
                    copy.write(data)
            cursor.execute(
                "SELECT id, name FROM " + table + " WHERE id >= 4 ORDER BY id"
            )
            assert rows(cursor) == [
                (4, "bob, jr"),
                (5, "line\nnext"),
                (6, ""),
            ]


def test_copy_driver_encoders(driver, connector, table_factory):
    """`copy_from` and `write_row` are the driver-side encoders import tools
    use; they must produce TEXT COPY that the server decodes."""
    table = table_factory("id bigint primary key, name text", "client_copy_api")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            if driver == "psycopg2":
                cursor.copy_from(
                    io.StringIO("7\teve\n8\t\\N\n"),
                    table,
                    columns=("id", "name"),
                )
            else:
                with cursor.copy("COPY " + table + " (id, name) FROM STDIN") as copy:
                    copy.write_row((7, "eve"))
                    copy.write_row((8, None))
            cursor.execute("SELECT id, name FROM " + table + " ORDER BY id")
            assert rows(cursor) == [(7, "eve"), (8, None)]


def test_copy_from_stdin_is_atomic_and_transactional(driver, connector, table_factory):
    """A failed COPY publishes none of its rows, and a successful COPY inside
    a block is staged until COMMIT."""
    table = table_factory("id bigint primary key, name text", "client_copy_tx")
    with closing(connector(driver, autocommit=False)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "INSERT INTO " + table + " (id, name) VALUES (1, 'existing')"
            )
            with pytest.raises(Exception) as info:
                if driver == "psycopg2":
                    cursor.copy_expert(
                        "COPY " + table + " (id, name) FROM STDIN",
                        io.StringIO("2\tnew\n1\tduplicate\n"),
                    )
                else:
                    with cursor.copy(
                        "COPY " + table + " (id, name) FROM STDIN"
                    ) as copy:
                        copy.write("2\tnew\n1\tduplicate\n")
            assert sqlstate(info.value) == "23505"
            connection.rollback()
            cursor.execute("SELECT count(*) FROM " + table)
            assert rows(cursor) == [(0,)]

            cursor.execute("INSERT INTO " + table + " (id, name) VALUES (10, 'ten')")
            if driver == "psycopg2":
                cursor.copy_expert(
                    "COPY " + table + " (id, name) FROM STDIN",
                    io.StringIO("11\televen\n"),
                )
            else:
                with cursor.copy("COPY " + table + " (id, name) FROM STDIN") as copy:
                    copy.write("11\televen\n")
            connection.commit()
            cursor.execute("SELECT id FROM " + table + " ORDER BY id")
            assert rows(cursor) == [(10,), (11,)]


def test_copy_aborted_by_client(connector, table_factory):
    """psycopg3 sends CopyFail when its copy block raises. The server must
    discard what was buffered, answer the failure, and keep the connection
    usable; nothing is written."""
    table = table_factory("id bigint primary key, name text", "client_copy_abort")
    with closing(connector("psycopg3")) as connection:
        observed = None
        try:
            with connection.cursor() as cursor:
                with cursor.copy("COPY " + table + " (id, name) FROM STDIN") as copy:
                    copy.write("1\tpartial\n")
                    raise RuntimeError("abort the copy")
        except RuntimeError:
            pass
        except Exception as error:  # the driver may surface the server error
            observed = sqlstate(error)
            if not connection.autocommit:
                connection.rollback()

        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM " + table)
            assert rows(cursor) == [(0,)]
            cursor.execute("SELECT 1")
            assert rows(cursor) == [(1,)]
        if observed is not None:
            assert observed == "57014", observed


def test_copy_from_stdin_must_be_alone(connector):
    """A batch cannot hand the connection to the copy sub-protocol and then
    run the statements after it; refuse it before anything executes."""
    with closing(connector("psycopg2")) as connection:
        with connection.cursor() as cursor:
            with pytest.raises(Exception) as info:
                cursor.execute("COPY client_copy_batch FROM STDIN; SELECT 1")
            assert sqlstate(info.value) == "0A000"


def test_insert_returning(driver, connector, table_factory):
    table = table_factory(
        "id bigint primary key, name text, amount numeric(20,4)",
        "client_returning",
    )
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} (id, name, amount) VALUES (%s, %s, %s) "
                "RETURNING id, name, amount",
                (1, "a", Decimal("1.5000")),
            )
            assert rows(cursor) == [(1, "a", Decimal("1.5000"))]
            assert cursor.rowcount == 1

            cursor.execute(f"INSERT INTO {table} (id) VALUES (%s) RETURNING *", (2,))
            assert rows(cursor) == [(2, None, None)]

            cursor.execute(
                f"INSERT INTO {table} (id, name) VALUES (%s, %s) "
                "RETURNING id AS new_id",
                (3, "c"),
            )
            assert rows(cursor) == [(3,)]
            assert cursor.description[0].name == "new_id"

            with pytest.raises(Exception) as caught:
                cursor.execute(
                    f"INSERT INTO {table} (id, name) VALUES (%s, %s) RETURNING id",
                    (1, "duplicate"),
                )
            assert sqlstate(caught.value) == "23505"
            cursor.execute("SELECT 1")
            assert rows(cursor) == [(1,)]

            bytea_table = table_factory(
                "id bigint primary key, payload bytea", "client_returning_bytea"
            )
            cursor.execute(
                f"INSERT INTO {bytea_table} (id, payload) VALUES (%s, %s) "
                "RETURNING payload",
                (1, b"\x00\x01\xff"),
            )
            assert bytes(rows(cursor)[0][0]) == b"\x00\x01\xff"


def test_insert_returning_unsupported_forms(driver, connector, table_factory):
    table = table_factory("id bigint primary key, name text", "client_returning_gap")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            for sql, params in [
                (
                    f"INSERT INTO {table} (id, name) VALUES (%s, %s) "
                    "ON CONFLICT (id) DO NOTHING RETURNING id",
                    (1, "a"),
                ),
                (
                    f"INSERT INTO {table} (id, name) VALUES (%s, %s) RETURNING id + 1",
                    (2, "b"),
                ),
                (
                    f"INSERT INTO {table} (id, name) SELECT 3, 'c' RETURNING id",
                    (),
                ),
            ]:
                with pytest.raises(Exception) as caught:
                    cursor.execute(sql, params)
                assert sqlstate(caught.value) == "0A000", sql
                if not connection.autocommit:
                    connection.rollback()


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


def test_sqlalchemy_insert_returning(sqlalchemy_url, table_factory):
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import MetaData, Table, create_engine, insert

    table = table_factory("id bigint primary key, name text", "client_sa_returning")
    engine = create_engine(sqlalchemy_url)
    try:
        metadata = MetaData()
        reflected = Table(table, metadata, autoload_with=engine)
        with engine.begin() as connection:
            result = connection.execute(
                insert(reflected)
                .values(id=1, name="alice")
                .returning(reflected.c.id, reflected.c.name)
            )
            assert [tuple(row) for row in result] == [(1, "alice")]
    finally:
        engine.dispose()


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
