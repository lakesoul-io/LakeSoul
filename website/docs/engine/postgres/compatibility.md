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
| **Connectivity** | A PostgreSQL client can connect and run a query | `psql`, JDBC, `pgx`, DBeaver, libpq-based tools |
| **SQL surface** | The statements and semantics below | See the tables on this page |
| **Ecosystem** | Tools that rely on PostgreSQL catalog behaviour | `pg_catalog`, `information_schema`, `psql \d`, BI tools that introspect |

## Supported

| Area | Statements |
| --- | --- |
| Queries | `SELECT` with joins, aggregates, windows, CTEs, `UNION`/`INTERSECT`/`EXCEPT`, subqueries |
| DDL | `CREATE TABLE`, `DROP TABLE`, primary keys, `PARTITIONED BY`, `WITH (...)` |
| Writes | `INSERT ... VALUES`, `INSERT ... SELECT`, `ON CONFLICT DO NOTHING`, `ON CONFLICT ... DO UPDATE SET col = EXCLUDED.col` |
| Transactions | Autocommit, `READ COMMITTED`, `BEGIN`/`COMMIT`/`ROLLBACK` for reads |
| Catalog | `pg_catalog`, `information_schema`, `psql \d`, `current_database()` |
| Session | `SET TIME ZONE`, `statement_timeout`, `search_path`, `ivfflat.probes`, `RESET`, `DISCARD ALL` |
| Vector | `vector(n)`, `<->`, `<#>`, `<=>`, `ORDER BY ... LIMIT k`, exact fallback, ANN |
| Full text | `text_score(column, query)` with `ORDER BY ... DESC LIMIT k` |

## Refused

These return SQLSTATE `0A000` rather than being silently ignored:

- `UPDATE`, `DELETE`, `TRUNCATE`, `COPY`
- `CREATE TABLE AS`, `RETURNING`
- Writes inside an explicit transaction block or a multi-statement batch
- `REPEATABLE READ` and `SERIALIZABLE`
- `CREATE TABLE ... PARTITION BY`, `LIKE`, `CLONE`, `INHERITS`
- Column defaults, `UNIQUE`, `CHECK` and `FOREIGN KEY` constraints
- `STORED AS` / `LOCATION` in DDL (use `WITH (file_format = ...)`)
- PostgreSQL extensions, PL/pgSQL and `LISTEN`/`NOTIFY`
- `ON CONFLICT DO UPDATE` with expressions over the old row

## SQLSTATE mapping

| SQLSTATE | Meaning |
| --- | --- |
| `0A000` | Feature not supported |
| `23505` | Duplicate key |
| `25P02` | Current transaction is aborted |
| `40001` | Serialization failure; retry |
| `42601` | Syntax error or column-count mismatch |
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

## Version policy

The SQL surface follows the engine, not a PostgreSQL release. The endpoint does
not advertise a `server_version` that implies PostgreSQL feature parity. Treat
the tables above as the contract.

## Next

- [DDL](ddl.md)
- [Insert and upsert](insert-and-upsert.md)
- [Transactions and catalog](transactions-and-catalog.md)
- [SQL errors](../../reference/sql-errors.md)
