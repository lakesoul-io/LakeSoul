---
sidebar_position: 5
---

# Compatibility

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul is **PostgreSQL-native, not a drop-in PostgreSQL**. The wire protocol
and the SQL surface are implemented natively, and this page records what is
covered today.

## Compatibility tiers

| Tier | Meaning | Status |
| --- | --- | --- |
| **Connectivity** | A PostgreSQL client can connect and run a query | `psql`, pgjdbc, psycopg, libpq-based tools; broader BI/import tool certification is in progress |
| **SQL surface** | The statements and semantics below | See the tables on this page |
| **Ecosystem** | Tools that rely on PostgreSQL catalog behaviour | `pg_catalog`, `information_schema`, `psql \d`; broader BI introspection is in progress |

## Supported

| Area | Statements |
| --- | --- |
| Queries | `SELECT` with joins, aggregates, windows, CTEs, `UNION`/`INTERSECT`/`EXCEPT`, subqueries |
| DDL | `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE` schema evolution (add/widen columns, `SET`/`RESET` supported options), `CREATE INDEX` for vector/text declarations, `REINDEX TABLE`, primary keys, `PARTITIONED BY`, `WITH (...)` |
| Writes | `INSERT ... VALUES` (including `RETURNING *`/column references), `INSERT ... SELECT`, `COPY ... FROM STDIN` and `COPY ... TO STDOUT` (TEXT and CSV), `UPDATE` on non-CDC primary-key tables, `DELETE` (CDC tombstone, partition clear or partition rewrite) and `TRUNCATE` (including `PARTITION (...)`), `ON CONFLICT DO NOTHING`, general `ON CONFLICT ... DO UPDATE` (partial column lists, expressions and conflict `WHERE`) |
| Subscriptions | `CREATE SUBSCRIPTION`, `ALTER SUBSCRIPTION ... ENABLE/DISABLE`, `DROP SUBSCRIPTION` for logical replication into a LakeSoul CDC table |
| Transactions | Autocommit and `READ COMMITTED`; `BEGIN`/`COMMIT`/`ROLLBACK` with deferred publication for LakeSoul writes |
| Catalog | `pg_catalog`, `information_schema`, `psql \d`/`\dt`/`\l`/`\dn`, `pg_cursors`, `pg_settings`, `pg_roles`, `current_database()`, `current_user`/`session_user`, `to_regtype`, `current_setting`/`set_config`, SQLAlchemy reflection through the `psycopg2` dialect |
| Session | `SHOW`/`SHOW ALL`, `SET TIME ZONE`, `statement_timeout`, `search_path`, `ivfflat.probes`, `RESET`, `DISCARD ALL` |
| Reporting | `date_trunc`, `date_part`/`extract`, `to_char` with PostgreSQL date/time patterns, `generate_series`, `DISTINCT ON`, `ROLLUP`/`GROUPING SETS`/`CUBE` |
| Vector | `vector(n)`, `<->`, `<#>`, `<=>`, `ORDER BY ... LIMIT k`, exact fallback, ANN |
| Full text | `text_score(column, query)` with `ORDER BY ... DESC LIMIT k` |

## Refused

These return SQLSTATE `0A000` rather than being silently ignored:

- `CREATE TABLE AS`, `CREATE TEMP TABLE`, `DELETE ... USING`
- `COPY ... TO` a file or program, `COPY ... FROM` a file or program,
  `FORMAT binary` and the COPY options listed in [COPY](copy.md)
- Writes and `COPY` inside a multi-statement simple-query batch; DDL, IVM
  maintenance, subscriptions, savepoints, a non-CDC `DELETE` rewrite and
  `TRUNCATE` inside an explicit transaction block
- `UPDATE` on CDC tables, tables without a primary key, append-only tables,
  `UPDATE ... FROM`, and assignments to primary-key or partition columns;
  `DELETE` on append-only tables and on CDC tables without a primary key
- `ON CONFLICT` on range-partitioned tables; the general `DO UPDATE` form on CDC
  tables; and changing the conflict key
- `REPEATABLE READ` and `SERIALIZABLE`
- `LIKE`, `CLONE`, `INHERITS`
- Column defaults, `UNIQUE`, `CHECK` and `FOREIGN KEY` constraints
- btree/hash/GiST/BRIN indexes, expression, multi-column and partial indexes
- `DROP INDEX`, `REINDEX INDEX/SCHEMA/DATABASE` (use the per-column
  `lakesoul_drop_vector_index` function and `REINDEX TABLE`)
- `STORED AS` / `LOCATION` in DDL (use `WITH (file_format = ...)`)
- PostgreSQL extensions, PL/pgSQL and `LISTEN`/`NOTIFY`

## SQLSTATE mapping

| SQLSTATE | Meaning |
| --- | --- |
| `0A000` | Feature not supported |
| `23505` | Duplicate key |
| `25P02` | Current transaction is aborted |
| `40001` | Serialization failure; retry |
| `42601` | Syntax error or column-count mismatch |
| `22P02` | Invalid text representation for a column type |
| `22P04` | Malformed `COPY` stream or row |
| `23502` | NOT NULL violation |
| `42703` | Undefined column |
| `42804` | Datatype mismatch |
| `42P01` | Undefined table |
| `42P07` | Duplicate table |
| `42701` | Duplicate column |
| `42P16` | Invalid table definition |
| `3F000` | Invalid schema name |
| `22023` | Invalid parameter value |
| `22007` | Invalid datetime format |
| `57014` | Query canceled (`statement_timeout`) |
| `XX000` | Internal error or unknown database at startup |

A few errors are mapped from engine errors rather than reproduced exactly. For
example, an invalid integer conversion returns `XX000` where PostgreSQL returns
`22P02`; the [SQL errors reference](../../reference/sql-errors.md) lists the
known boundaries.

## Client baseline

A real-driver baseline lives in
`rust/postgres-lakesoul/client-compat/`. It currently runs psycopg2, psycopg3,
SQLAlchemy and pgjdbc against a live endpoint and covers connect/`SELECT`,
parameterized writes including `bytea`, `COPY ... FROM STDIN` and
`COPY ... TO STDOUT` (TEXT and CSV through psycopg2, psycopg3 and pgjdbc
`CopyManager`), batches, UPSERT,
transactions, duplicate-key recovery, catalog discovery (`SHOW`, `pg_settings`,
`pg_roles`, psql `\dt`/`\l`/`\dn`), SQLAlchemy reflection, forward-only named
cursors, fetch-size cursors and Metabase-style startup statements.

The current baseline passes the core Python and JDBC workflows, including psql
`\d`, `bytea` round-trips and SQLAlchemy reflection through the `psycopg2`
dialect. Named cursors are forward-only and non-holdable; `WITH HOLD`,
`SCROLL` and backward fetches are explicitly refused. Known gaps are kept
visible rather than hidden: `RETURNING` for `INSERT ... SELECT`/`ON CONFLICT`/
`UPDATE`, and `pg_dump --data-only` (it opens a REPEATABLE READ transaction and
uses `OPERATOR(pg_catalog.=)` syntax this parser does not accept). Numeric
`to_char` patterns are not
implemented and fail explicitly; selecting `current_user` and `session_user`
in one projection is still limited by the compatibility parser's
`current_user -> session_user` rewrite, and `psql \du` still needs
`ARRAY(subquery)` support. SQLAlchemy's
`psycopg3` dialect is not part of the baseline yet because its reflection
starts nested transactions (`SAVEPOINT`), which LakeSoul does not support. See
the README in that directory for exact versions and the status of each gap.

## Version policy

The SQL surface follows the engine, not a PostgreSQL release. `version()`
returns a PostgreSQL-compatible baseline string (`PostgreSQL 14.0 (LakeSoul ...
/ DataFusion ...)`) so drivers such as SQLAlchemy can initialize their
PostgreSQL dialect. The suffix identifies the actual engine; the string is a
feature-discovery compatibility shim, not a claim of PostgreSQL 14 parity.
Treat the tables above as the contract.

## Next

- [DDL](ddl.md)
- [Insert and upsert](insert-and-upsert.md)
- [COPY](copy.md)
- [Transactions and catalog](transactions-and-catalog.md)
- [SQL errors](../../reference/sql-errors.md)
