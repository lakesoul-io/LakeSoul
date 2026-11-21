# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""SQL time travel through the real PostgreSQL drivers.

`lakesoul_table_scan(table, mode, value)` returns a per-query pinned scan.
These cases exercise the timestamp mode over the extended protocol and the
simple query path; tag and snapshot_id modes are covered by the Rust contract
test, which can build the metadata rows they need.

The driver's literal strings arrive as DataFusion literals, so the table and
timestamp are safe to inline in the statement text.
"""

from __future__ import annotations

import time
from contextlib import closing
from datetime import datetime, timezone

import pytest

from conftest import sqlstate

DRIVERS = ("psycopg2", "psycopg3")


def test_timestamp_pinned_scan_through_drivers(connector, table_factory):
    table = table_factory("id BIGINT PRIMARY KEY, val BIGINT", "compat_time_travel")
    with closing(connector("psycopg2")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} SELECT i, i * 10 FROM generate_series(1, 2) AS t(i)"
            )

    # The as-of instant is after the first commit and before the second.
    time.sleep(1)
    as_of = datetime.now(timezone.utc).isoformat()
    time.sleep(0.2)

    with closing(connector("psycopg2")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} SELECT i, i * 10 FROM generate_series(3, 4) AS t(i)"
            )

    for driver in DRIVERS:
        with closing(connector(driver)) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT id FROM {table} ORDER BY id")
                assert cursor.fetchall() == [(1,), (2,), (3,), (4,)]

                cursor.execute(
                    f"SELECT id FROM lakesoul_table_scan('{table}', 'timestamp', "
                    f"'{as_of}') ORDER BY id"
                )
                assert cursor.fetchall() == [(1,), (2,)], (
                    f"{driver} must read only the first commit at {as_of}"
                )

                # A WHERE clause is applied to the pinned rows, not to the
                # latest table view.
                cursor.execute(
                    f"SELECT id FROM lakesoul_table_scan('{table}', 'timestamp', "
                    f"'{as_of}') WHERE val > 10"
                )
                assert cursor.fetchall() == [(2,)]


def test_session_time_travel_guc(connector, table_factory):
    table = table_factory("id BIGINT PRIMARY KEY", "compat_read_snapshot")
    with closing(connector("psycopg2")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} VALUES (1), (2)")

    time.sleep(1)
    as_of = datetime.now(timezone.utc).isoformat()
    time.sleep(0.2)
    with closing(connector("psycopg2")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} VALUES (3), (4)")

    for driver in DRIVERS:
        with closing(connector(driver)) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"SET lakesoul.read.time_travel = '{as_of}'")
                cursor.execute(f"SELECT id FROM {table} ORDER BY id")
                assert cursor.fetchall() == [(1,), (2,)], driver

                cursor.execute("SHOW lakesoul.read.time_travel")
                shown = cursor.fetchone()[0]
                assert "T" in shown, shown

                cursor.execute("RESET lakesoul.read.time_travel")
                cursor.execute(f"SELECT id FROM {table} ORDER BY id")
                assert cursor.fetchall() == [(1,), (2,), (3,), (4,)], driver


def test_snapshot_tag_ddl_through_drivers(connector, table_factory):
    table = table_factory("id BIGINT PRIMARY KEY", "compat_tag_ddl")
    with closing(connector("psycopg2")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} VALUES (1), (2)")

    for driver in DRIVERS:
        with closing(connector(driver)) as connection:
            with connection.cursor() as cursor:
                cursor.execute(f"ALTER TABLE {table} CREATE TAG `v1`")
                cursor.execute(f"SHOW TAGS {table}")
                tags = cursor.fetchall()
                assert [row[0] for row in tags] == ["v1"], tags
                assert all(isinstance(value, str) for value in tags[0]), tags

                cursor.execute(
                    f"SELECT id FROM lakesoul_table_scan('{table}', 'tag', 'v1') "
                    "ORDER BY id"
                )
                assert cursor.fetchall() == [(1,), (2,)]

                cursor.execute(f"ALTER TABLE {table} DROP TAG v1")
                cursor.execute(f"SHOW TAGS {table}")
                assert cursor.fetchall() == []

                with pytest.raises(Exception) as error:
                    cursor.execute(f"ALTER TABLE {table} DROP SNAPSHOT 1")
                assert sqlstate(error.value) == "0A000", sqlstate(error.value)


def test_invalid_mode_is_refused(connector, table_factory):
    table = table_factory("id BIGINT PRIMARY KEY", "compat_time_travel")
    with closing(connector("psycopg3")) as connection:
        with connection.cursor() as cursor:
            with pytest.raises(Exception) as error:
                cursor.execute(
                    f"SELECT * FROM lakesoul_table_scan('{table}', 'bogus', 'x')"
                )
            message = str(error.value)
            assert "lakesoul_table_scan" in message, message
            # A planner error is 42601 (syntax_error) or 0A000; record the
            # actual SQLSTATE so a driver change is visible in the baseline.
            print(f"invalid mode SQLSTATE: {sqlstate(error.value)!r}")
