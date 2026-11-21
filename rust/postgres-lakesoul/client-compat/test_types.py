# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Real-driver coverage for the PostgreSQL semantic types.

The Rust contract suites prove the wire behaviour with `tokio-postgres`; this
module proves that ordinary psycopg2/psycopg3 applications can bind the same
values, read them back, and see the same type names and modifiers. Values that
LakeSoul deliberately refuses are asserted to fail with an explicit SQLSTATE.
"""

from __future__ import annotations

import uuid
from contextlib import closing
from datetime import datetime, time, timezone
from decimal import Decimal

import psycopg2
from psycopg2.extras import register_uuid

from conftest import assert_sqlstate, description, one

# psycopg2 2.9 has no built-in UUID adapter; real applications either register
# one or bind strings. Registering it exercises the typed path for both drivers.
register_uuid()

DRIVERS = ["psycopg2", "psycopg3"]


def test_uuid_scalar_round_trip(driver, connector, table_factory):
    table = table_factory("id bigint primary key, u uuid", "client_uuid")
    value = uuid.UUID("11111111-2222-3333-4444-555555555555")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} (id, u) VALUES (%s, %s)", (1, value))
            cursor.execute(f"SELECT u, u::text AS u_text FROM {table} WHERE id = 1")
            stored, text = cursor.fetchone()
            assert stored == value
            assert text == str(value)
            assert dict(description(cursor))["u"] == 2950  # pg_catalog UUID OID
            assert one(cursor, f"SELECT id FROM {table} WHERE u = %s", (value,)) == (1,)


def test_uuid_array_round_trip(driver, connector, table_factory):
    table = table_factory("id bigint primary key, ua uuid[]", "client_uuid_array")
    values = [uuid.UUID(int=1), uuid.UUID(int=2)]
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} (id, ua) VALUES (%s, %s)", (1, values))
            cursor.execute(f"SELECT ua, ua[1]::text AS first FROM {table} WHERE id = 1")
            stored, first = cursor.fetchone()
            assert stored == values
            assert first == str(values[0])
            assert dict(description(cursor))["ua"] == 2951  # UUID[] OID
            assert one(cursor, f"SELECT id FROM {table} WHERE ua = %s", (values,)) == (
                1,
            )

            cursor.execute(
                f"INSERT INTO {table} (id, ua) VALUES (%s, %s)",
                (2, [uuid.UUID(int=3), None]),
            )
            assert one(cursor, f"SELECT ua FROM {table} WHERE id = 2") == (
                [uuid.UUID(int=3), None],
            )


def test_money_round_trip(driver, connector, table_factory):
    table = table_factory("id bigint primary key, m money", "client_money")
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} (id, m) VALUES (%s, %s)", (1, Decimal("123.45"))
            )
            # The write path accepts PostgreSQL's money input syntax.
            cursor.execute(
                f"INSERT INTO {table} (id, m) VALUES (2, '$1,234.56'::money)"
            )
            cursor.execute(f"SELECT m, m::text AS m_text FROM {table} WHERE id = 1")
            stored, text = cursor.fetchone()
            assert stored == "$123.45"  # drivers have no built-in money loader
            assert text == "123.45"
            assert dict(description(cursor))["m"] == 790  # pg_catalog MONEY OID
            assert one(cursor, f"SELECT m FROM {table} WHERE id = 2") == ("$1,234.56",)
            assert one(
                cursor, f"SELECT id FROM {table} WHERE m = %s", (Decimal("123.45"),)
            ) == (1,)


def test_large_numeric_round_trip(driver, connector, table_factory):
    table = table_factory(
        "id bigint primary key, n numeric(60,10), big numeric(76,0)",
        "client_numeric",
    )
    value = Decimal("123456789012345678901234567890.1234567890")
    big = Decimal("9" * 76)
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} (id, n, big) VALUES (%s, %s, %s)",
                (1, value, big),
            )
            assert one(cursor, f"SELECT n, big FROM {table} WHERE id = 1") == (
                value,
                big,
            )
            if driver == "psycopg3":
                # psycopg2 binds Decimal with an unspecified OID; in a
                # standalone predicate the front end infers float8 and loses
                # precision. INSERT into a typed column stays exact.
                assert one(
                    cursor,
                    f"SELECT id FROM {table} WHERE n = %s::numeric(60,10)",
                    (value,),
                ) == (1,)
            cursor.execute(
                "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
                f"WHERE attrelid = '{table}'::regclass AND attname = 'n'"
            )
            assert cursor.fetchone()[0] == "numeric(60,10)"


def test_char_varchar_length_metadata(driver, connector, table_factory):
    table = table_factory(
        "id bigint primary key, v varchar(4), c char(6)", "client_length"
    )
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} (id, v, c) VALUES (1, 'abcd', 'ab')")
            assert one(cursor, f"SELECT v, c FROM {table} WHERE id = 1") == (
                "abcd",
                "ab",
            )  # char(n) padding is not emulated
            cursor.execute(
                "SELECT attname, atttypmod, format_type(atttypid, atttypmod) "
                f"FROM pg_attribute WHERE attrelid = '{table}'::regclass "
                "AND attname IN ('v', 'c') ORDER BY attname"
            )
            assert cursor.fetchall() == [
                ("c", 10, "character(6)"),
                ("v", 8, "character varying(4)"),
            ]
            try:
                cursor.execute(f"INSERT INTO {table} (id, v) VALUES (2, 'abcde')")
                raise AssertionError("over-length varchar must be refused")
            except Exception as error:
                assert_sqlstate("22001", error)


def test_time_and_timestamp_precision(driver, connector, table_factory):
    table = table_factory(
        "id bigint primary key, t time, ts3 timestamp(3), "
        "ts6 timestamp(6), tz timestamptz(0)",
        "client_time",
    )
    value = datetime(2026, 1, 2, 3, 4, 5, 123456)
    utc = value.replace(tzinfo=timezone.utc)
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} (id, t, ts3, ts6, tz) "
                "VALUES (%s, %s, %s, %s, %s)",
                (1, time(1, 2, 3, 456789), value, value, utc),
            )
            stored_time, ts3, ts6, tz = one(
                cursor, f"SELECT t, ts3, ts6, tz FROM {table} WHERE id = 1"
            )
            assert stored_time == time(1, 2, 3, 456789)
            assert ts3 == value.replace(microsecond=123000)  # truncated
            assert ts6 == value
            if tz.tzinfo is None:
                tz = tz.replace(tzinfo=timezone.utc)
            assert tz.astimezone(timezone.utc) == utc.replace(microsecond=0)
            assert dict(description(cursor))["t"] == 1083  # TIME OID


def test_array_types_and_metadata(driver, connector, table_factory):
    table = table_factory(
        "id bigint primary key, i int[], s text[], "
        "n numeric(40,5)[], v varchar(4)[], m money[]",
        "client_arrays",
    )
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} (id, i, s, n, v, m) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    1,
                    [1, 2],
                    ["a", None],
                    [Decimal("1.50000")],
                    ["ab", "cdcd"],
                    [Decimal("3.25")],
                ),
            )
            row = one(
                cursor,
                f"SELECT i, s, n, v, m FROM {table} WHERE id = 1",
            )
            assert row[0] == [1, 2]
            assert row[1] == ["a", None]
            assert row[2] == [Decimal("1.50000")]
            assert row[3] == ["ab", "cdcd"]
            # psycopg2 has no money[] loader and returns PostgreSQL's array
            # text form; psycopg3 decodes the array into strings.
            assert row[4] == (["$3.25"] if driver == "psycopg3" else "{$3.25}")
            assert [oid for _, oid in description(cursor)] == [
                1007,
                1009,
                1231,
                1015,
                791,
            ]

            cursor.execute(
                "SELECT attname, attndims, format_type(atttypid, atttypmod) "
                f"FROM pg_attribute WHERE attrelid = '{table}'::regclass "
                "AND attnum > 0 ORDER BY attnum"
            )
            assert cursor.fetchall() == [
                ("id", 0, "bigint"),
                ("i", 1, "integer[]"),
                ("s", 1, "text[]"),
                ("n", 1, "numeric(40,5)[]"),
                ("v", 1, "character varying(4)[]"),
                ("m", 1, "money[]"),
            ]
            assert one(cursor, f"SELECT s[1] FROM {table} WHERE id = 1") == ("a",)
            assert one(cursor, f"SELECT i FROM {table} WHERE i = %s", ([1, 2],)) == (
                [1, 2],
            )

            cursor.execute(f"INSERT INTO {table} (id, v) VALUES (2, ARRAY['abcd'])")
            assert one(cursor, f"SELECT v FROM {table} WHERE id = 2") == (["abcd"],)


def test_over_long_array_element_is_refused(connector, table_factory):
    table = table_factory("id bigint primary key, v varchar(4)[]", "client_array_len")
    with closing(connector("psycopg3")) as connection:
        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    f"INSERT INTO {table} (id, v) VALUES (1, ARRAY['abcde'])"
                )
                raise AssertionError("over-long array element must be refused")
            except Exception as error:
                assert_sqlstate("22001", error)


def test_unsupported_types_are_refused(connector):
    statements = [
        "CREATE TABLE client_bad_numeric (id bigint primary key, n numeric)",
        "CREATE TABLE client_bad_numeric2 (id bigint primary key, n numeric(77,2))",
        "CREATE TABLE client_bad_interval (id bigint primary key, i interval)",
        "CREATE TABLE client_bad_inet (id bigint primary key, i inet)",
        "CREATE TABLE client_bad_ts (id bigint primary key, t timestamp(5))",
        "CREATE TABLE client_bad_pk (id int[] primary key, v text)",
    ]
    with closing(connector("psycopg3")) as connection:
        with connection.cursor() as cursor:
            for sql in statements:
                try:
                    cursor.execute(sql)
                    raise AssertionError(f"must be refused: {sql}")
                except Exception as error:
                    assert_sqlstate("0A000", error)


def test_type_catalog_discovery(driver, connector):
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            assert one(cursor, "SELECT to_regtype('uuid')") == (2950,)
            assert one(cursor, "SELECT to_regtype('uuid[]')") == (2951,)
            assert one(cursor, "SELECT to_regtype('money')") == (790,)
            assert one(cursor, "SELECT to_regtype('vector')") == (16385,)
            assert one(cursor, "SELECT to_regtype('halfvec')") == (None,)
            assert one(cursor, "SELECT to_regtype('sparsevec')") == (None,)
