# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

"""Real-client coverage for the pgvector-compatible wire type.

`vector` is reported on the wire only for columns declared in a table's
`vector_index_columns` property or through a vector `CREATE INDEX`. `run.sh`
creates the fixture table through `lakesoul-console` before the server starts,
so catalog introspection never races a just-created table; the DDL tests below
create their own tables and indexes through each real client.
"""

from __future__ import annotations

import os
from contextlib import closing

import pytest
from pgvector import Vector
from pgvector.psycopg import register_vector as register_vector3
from pgvector.psycopg2 import register_vector as register_vector2
from pgvector.sqlalchemy import Vector as SaVector
from sqlalchemy import Index, MetaData, Table, create_engine, event, text

from conftest import assert_sqlstate, description, one, unique_table

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


def test_pgvector_create_table_and_index_ddl(driver, connector):
    """A driver can create, populate and index a vector(n) table end to end."""
    table = unique_table("client_pgvector_ddl")
    with closing(connector(driver)) as connection:
        register(driver, connection)
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            try:
                cursor.execute(
                    f"CREATE TABLE {table} (id bigint primary key, embedding vector(3))"
                )
                cursor.execute(
                    f"INSERT INTO {table} VALUES "
                    "(1, '[1,2,3]'::vector(3)), (2, '[4,5,6]')"
                )
                # m/ef_construction are accepted for pgvector compatibility.
                cursor.execute(
                    f"CREATE INDEX {table}_idx ON {table} USING hnsw (embedding) "
                    "WITH (m = 16, ef_construction = 64)"
                )
                assert one(
                    cursor,
                    f"SELECT id FROM {table} ORDER BY embedding <-> %s LIMIT 2",
                    (Vector([1, 2, 3]),),
                ) == (1,)
                cursor.execute(
                    "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
                    f"WHERE attrelid = '{table}'::regclass "
                    "AND attname = 'embedding'"
                )
                assert cursor.fetchone()[0] == "vector"
            finally:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")


def test_pgvector_create_table_with_index_declaration(driver, connector):
    """vector_index_columns in WITH is the SDK/DataFusion declaration shape."""
    table = unique_table("client_pgvector_decl")
    with closing(connector(driver)) as connection:
        register(driver, connection)
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            try:
                cursor.execute(
                    f"CREATE TABLE {table} "
                    "(id bigint primary key, embedding vector(3)) "
                    "WITH (vector_index_columns = "
                    '\'[{"column":"embedding","dim":3}]\')'
                )
                cursor.execute(
                    f"INSERT INTO {table} VALUES (%s, %s)", (1, Vector([1, 2, 3]))
                )
                assert one(cursor, f"SELECT embedding FROM {table} WHERE id = 1") == (
                    Vector([1, 2, 3]),
                )
                # The declaration already built the single supported index.
                try:
                    cursor.execute(
                        f"CREATE INDEX {table}_again ON {table} USING hnsw (embedding)"
                    )
                    raise AssertionError("duplicate vector index must be refused")
                except Exception as error:
                    assert_sqlstate("42710", error)
            finally:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")


def test_pgvector_index_ddl_refusals(connector):
    table = unique_table("client_pgvector_refusal")
    with closing(connector("psycopg3")) as connection:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {table}")
            cursor.execute(
                f"CREATE TABLE {table} (id bigint primary key, embedding vector(3))"
            )
            statements = [
                f"CREATE INDEX {table}_btree ON {table} USING btree (embedding)",
                f"CREATE INDEX {table}_cosine ON {table} "
                "USING hnsw (embedding vector_cosine_ops)",
                f"CREATE INDEX {table}_multi ON {table} "
                "USING hnsw (embedding, embedding)",
                f"DROP INDEX {table}_missing",
                f"ALTER TABLE {table} SET (vector_index_columns = '[]')",
            ]
            try:
                for sql in statements:
                    try:
                        cursor.execute(sql)
                        raise AssertionError(f"must be refused: {sql}")
                    except Exception as error:
                        assert_sqlstate("0A000", error)
            finally:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")


def test_sqlalchemy_vector_index_ddl(sqlalchemy_url):
    """SQLAlchemy emits CREATE INDEX ... USING hnsw; DDL needs autocommit."""
    table = unique_table("client_pgvector_sa_ddl")
    engine = create_engine(sqlalchemy_url)
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
        try:
            connection.execute(
                text(
                    f"CREATE TABLE {table} "
                    "(id integer primary key, embedding vector(3))"
                )
            )
            reflected = Table(table, MetaData(), autoload_with=connection)
            assert isinstance(reflected.c.embedding.type, SaVector)
            Index(
                f"{table}_idx", reflected.c.embedding, postgresql_using="hnsw"
            ).create(bind=connection)
            connection.execute(reflected.insert().values(id=1, embedding=[1, 2, 3]))
            nearest = connection.execute(
                text(
                    f"SELECT id FROM {table} "
                    "ORDER BY embedding <-> '[1,2,3]'::vector(3)"
                )
            ).fetchall()
            assert nearest == [(1,)]
        finally:
            connection.execute(text(f"DROP TABLE IF EXISTS {table}"))
    engine.dispose()
