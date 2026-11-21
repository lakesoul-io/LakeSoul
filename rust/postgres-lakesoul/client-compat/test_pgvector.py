# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Real-client coverage for the pgvector-compatible wire type.

`vector` is reported on the wire only for columns declared in a table's
`vector_index_columns` property; `run.sh` creates that table through
`lakesoul-console` (the same path the Python SDK and DataFusion use), because
PostgreSQL `CREATE TABLE` here cannot declare a vector index column.
"""

from __future__ import annotations

import os
from contextlib import closing

from pgvector import Vector
from pgvector.psycopg import register_vector as register_vector3
from pgvector.psycopg2 import register_vector as register_vector2
from sqlalchemy import MetaData, Table, create_engine, event, text
from pgvector.sqlalchemy import Vector as SaVector

from conftest import description, one

VECTOR_TABLE = os.environ.get("CLIENT_COMPAT_VECTOR_TABLE", "client_compat_vector")


def register(driver, connection):
    if driver == "psycopg2":
        # LakeSoul reports the `vector` type but not an array type `_vector`;
        # the adapter's array registration must be turned off.
        register_vector2(connection, arrays=False)
    else:
        register_vector3(connection)


def test_vector_type_is_discoverable(driver, connector):
    with closing(connector(driver)) as connection:
        with connection.cursor() as cursor:
            assert one(cursor, "SELECT to_regtype('vector')") == (16385,)
            assert one(
                cursor, "SELECT count(*) FROM pg_type WHERE typname = 'vector'"
            ) == (1,)
            register(driver, connection)  # must not raise
            assert one(cursor, "SELECT to_regtype('halfvec')") == (None,)
            assert one(cursor, "SELECT to_regtype('sparsevec')") == (None,)


def test_vector_parameters_and_distance_operators(driver, connector):
    with closing(connector(driver)) as connection:
        register(driver, connection)
        with connection.cursor() as cursor:
            left, right = Vector([1, 2, 3]), Vector([1, 2, 3])
            assert one(
                cursor, "SELECT %s::vector(3) <-> %s::vector(3)", (left, right)
            ) == (0.0,)
            assert one(
                cursor, "SELECT %s::vector(3) <#> %s::vector(3)", (left, right)
            ) == (-14.0,)
            cosine = one(
                cursor, "SELECT %s::vector(3) <=> %s::vector(3)", (left, right)
            )[0]
            assert abs(cosine) < 1e-6


def test_vector_column_round_trip(driver, connector):
    with closing(connector(driver)) as connection:
        register(driver, connection)
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT id, vec FROM {VECTOR_TABLE} ORDER BY id")
            rows = cursor.fetchall()
            assert rows == [(1, Vector([1, 2, 3])), (2, Vector([4, 5, 6]))]
            assert dict(description(cursor))["vec"] == 16385

            assert one(
                cursor,
                f"SELECT vec FROM {VECTOR_TABLE} WHERE id = 1",
            ) == (Vector([1, 2, 3]),)
            assert one(
                cursor,
                f"SELECT id FROM {VECTOR_TABLE} ORDER BY vec <-> %s LIMIT 2",
                (Vector([1, 2, 3]),),
            ) == (1,)
            assert one(
                cursor,
                f"SELECT vec <-> %s AS distance FROM {VECTOR_TABLE} WHERE id = 1",
                (Vector([1, 2, 3]),),
            ) == (0.0,)


def test_vector_literal_and_index_session(connector):
    with closing(connector("psycopg3")) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET ivfflat.probes = 4")
            assert one(
                cursor, "SELECT '[1,2,3]'::vector(3) <-> '[1,2,3]'::vector(3)"
            ) == (0.0,)
            cursor.execute(
                "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
                f"WHERE attrelid = '{VECTOR_TABLE}'::regclass AND attname = 'vec'"
            )
            assert cursor.fetchone()[0] == "vector"


def test_sqlalchemy_vector_type_and_reflection(sqlalchemy_url):
    engine = create_engine(sqlalchemy_url)

    @event.listens_for(engine, "connect")
    def register_dbapi(dbapi_connection, _record):
        register_vector2(dbapi_connection, arrays=False)

    with engine.connect() as connection:
        reflected = Table(
            VECTOR_TABLE,
            MetaData(),
            autoload_with=connection,
        )
        assert isinstance(reflected.c.vec.type, SaVector)

        rows = connection.execute(
            reflected.select().order_by(reflected.c.id)
        ).fetchall()
        assert [list(row.vec) for row in rows] == [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]

        nearest = connection.execute(
            text(f"SELECT id FROM {VECTOR_TABLE} ORDER BY vec <-> :q LIMIT 1"),
            {"q": "[1,2,3]"},
        ).fetchall()
        assert nearest == [(1,)]
    engine.dispose()
