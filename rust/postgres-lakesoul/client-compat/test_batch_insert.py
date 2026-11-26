# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""High-volume insert shapes used by log shippers.

vector.dev's PostgreSQL sink binds a JSON array to a single statement:

    INSERT INTO t SELECT * FROM jsonb_populate_recordset(NULL::t, $1)
    [ ON CONFLICT ... ]

DataFusion has no composite type or JSON table function, so the wire frontend
rewrites that shape into a `VALUES` source with the same conflict semantics.
This module certifies the shape through real drivers, alongside multi-row
`VALUES` (which log shippers also emit) and explicit transaction batching.
"""

from __future__ import annotations

import json
import time
from contextlib import closing

from conftest import one, sqlstate, unique_table


def test_json_populate_recordset_round_trip(driver, connector, table_factory):
    table = table_factory(
        "id bigint primary key, name text, value double precision",
        "client_json_insert",
    )
    rows = [{"id": i, "name": f"n{i}", "value": i * 1.5} for i in range(1, 6)]
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {table} SELECT * FROM "
                f"jsonb_populate_recordset(NULL::{table}, %s)",
                (json.dumps(rows),),
            )
            assert cursor.rowcount == len(rows)
            cursor.execute(f"SELECT id, name, value FROM {table} ORDER BY id")
            assert cursor.fetchall() == [(i, f"n{i}", i * 1.5) for i in range(1, 6)]


def test_json_populate_conflict_and_nulls(driver, connector, table_factory):
    table = table_factory(
        "id bigint primary key, name text, value double precision",
        "client_json_conflict",
    )
    sql = (
        f"INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, %s)"
    )
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            cursor.execute(sql, (json.dumps([{"id": 1, "name": "one", "value": 1.0}]),))
            # Duplicate key is rejected by default.
            try:
                cursor.execute(
                    sql, (json.dumps([{"id": 1, "name": "dup", "value": 2.0}]),)
                )
                raise AssertionError("duplicate key must be refused")
            except Exception as error:
                assert sqlstate(error) == "23505"

            # DO NOTHING drops the duplicate and accepts a new key.
            cursor.execute(
                sql + " ON CONFLICT (id) DO NOTHING",
                (
                    json.dumps(
                        [
                            {"id": 1, "name": "dup", "value": 2.0},
                            {"id": 2, "name": "two", "value": None},
                        ]
                    ),
                ),
            )
            assert cursor.rowcount == 1
            assert one(cursor, f"SELECT name FROM {table} WHERE id = 1") == ("one",)
            assert one(cursor, f"SELECT value FROM {table} WHERE id = 2") == (None,)

            # DO UPDATE routes through the upsert planner.
            cursor.execute(
                sql + " ON CONFLICT (id) DO UPDATE SET name = excluded.name",
                (json.dumps([{"id": 1, "name": "changed", "value": 9.0}]),),
            )
            assert one(cursor, f"SELECT name FROM {table} WHERE id = 1") == ("changed",)


def test_multirow_values_round_trip(driver, connector, table_factory):
    table = table_factory(
        "id bigint primary key, name text, value double precision",
        "client_values_insert",
    )
    count = 2000
    values = ",".join(f"({i},'n{i}',{i * 1.5})" for i in range(1, count + 1))
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            start = time.monotonic()
            cursor.execute(f"INSERT INTO {table} VALUES {values}")
            assert cursor.rowcount == count
            assert one(cursor, f"SELECT count(*) FROM {table}") == (count,)
            # Keep the test quick even on slow CI; the point is correctness.
            assert time.monotonic() - start < 60
            assert one(cursor, f"SELECT name FROM {table} WHERE id = {count}") == (
                f"n{count}",
            )


def test_transaction_batches_multiple_json_inserts(driver, connector, table_factory):
    """A transaction block folds several writes into one metadata commit."""
    table = table_factory(
        "id bigint primary key, name text",
        "client_json_txn",
    )
    sql = (
        f"INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, %s)"
    )
    with closing(connector(driver, autocommit=False)) as connection:
        with connection.cursor() as cursor:
            for batch in range(3):
                rows = [
                    {"id": batch * 10 + i, "name": f"b{batch}n{i}"} for i in range(5)
                ]
                cursor.execute(sql, (json.dumps(rows),))
            connection.commit()
        with connection.cursor() as cursor:
            assert one(cursor, f"SELECT count(*) FROM {table}") == (15,)
