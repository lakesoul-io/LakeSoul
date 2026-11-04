---
sidebar_position: 4
---

# Transactions and catalog

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Transactions

The endpoint runs at **autocommit** with **READ COMMITTED** visibility. Each
statement sees the latest committed snapshot; a statement that writes commits
before it returns.

- `BEGIN` / `COMMIT` / `ROLLBACK` are accepted for read-only work.
- Writes inside an explicit transaction block are refused with `0A000`.
- `REPEATABLE READ`, `SERIALIZABLE` and their `SET` forms are refused with
  `0A000`.
- A failed transaction returns `25P02` for subsequent statements until
  `ROLLBACK`. `COMMIT` inside a failed block ends the transaction as a rollback,
  matching PostgreSQL.
- `statement_timeout` cancels a running query with `57014`; the connection
  remains usable.

Multi-statement simple-query batches execute the reads before a refused write.
Do not rely on a batch being atomic: it is not.

## Catalog

A PostgreSQL **database** maps to exactly one LakeSoul **namespace**. Inside a
connection:

- User tables live in the `public` schema.
- `current_database()` reports the namespace name.
- `pg_catalog` and `information_schema` expose the tables, columns and types
  visible in that namespace.
- `psql \d`, `\dt` and `\dn` work.
- Other namespaces appear in `pg_database`, but their tables are not readable
  from this connection. Cross-database reads and writes are refused.

Connecting to a database that is not a LakeSoul namespace fails at startup with
`FATAL XX000`. The quick start uses the `default` namespace created by
`script/meta_init.sql`.

`search_path` does not bypass the database boundary; schemas other than `public`
are not a second namespace.

## Session settings

The endpoint accepts and applies:

| Setting | Effect |
| --- | --- |
| `SET TIME ZONE` / `SET timezone` | Session time zone for timestamp rendering and parsing |
| `SET statement_timeout` | Per-statement budget |
| `SET search_path` | Accepted; does not cross the database boundary |
| `SET ivfflat.probes` | ANN probe count for vector scans |
| `RESET ALL` / `DISCARD ALL` | Restore the connection defaults |

Unsupported settings return `42704`. `SET LOCAL` and `SET GLOBAL` for the time
zone return `0A000`.

## Next

- [Compatibility](compatibility.md)
- [Operations](operations.md)
