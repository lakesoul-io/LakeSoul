---
sidebar_position: 4
---

# Transactions and catalog

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Transactions

The endpoint supports **autocommit** and explicit transaction blocks with
**READ COMMITTED** visibility. A write inside a block writes its files but
publishes no metadata until `COMMIT`; other connections see nothing before the
commit. `COMMIT` publishes every staged write of every table in one metadata
transaction. `ROLLBACK` deletes the staged files.

- `BEGIN` / `COMMIT` / `ROLLBACK` are accepted. Reads inside a block see the
  published snapshot plus that block's staged writes.
- `CREATE TABLE`, `DROP TABLE` and `ALTER TABLE` are staged in a block and
  published by `COMMIT` in the same metadata transaction as the block's data
  writes. A peer sees nothing before the commit, and `ROLLBACK` leaves no table
  or file behind. A `CREATE TABLE` is immediately usable by later statements
  of the same block.
- `SAVEPOINT`, `RELEASE SAVEPOINT` and `ROLLBACK TO SAVEPOINT` are supported.
  A savepoint captures the block's staged data and table DDL;
  `ROLLBACK TO SAVEPOINT` discards the changes after it and recovers a failed
  transaction.
- `CREATE INDEX`, `REINDEX`, `TRUNCATE`, IVM maintenance, subscriptions, users
  and grants inside a block are still refused with `0A000`. A non-CDC `DELETE`
  rewrite is also refused. `COPY ... FROM STDIN` is a write: it is staged in
  the block and published by `COMMIT`.
- `REPEATABLE READ`, `SERIALIZABLE` and their `SET` forms are refused with
  `0A000`.
- A failed transaction returns `25P02` for subsequent statements until
  `ROLLBACK` or a successful `ROLLBACK TO SAVEPOINT`. `COMMIT` inside a failed
  block ends the transaction as a rollback, matching PostgreSQL.
- A write or table DDL in a multi-statement simple-query batch is still
  refused: those statements are not run in one transaction yet. Split them
  into separate messages or use an explicit block.
- `statement_timeout` cancels a running query with `57014`; the connection
  remains usable.
- `idle_in_transaction_session_timeout` is not implemented.

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
