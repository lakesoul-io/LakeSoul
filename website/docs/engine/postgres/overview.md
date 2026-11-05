---
sidebar_position: 1
sidebar_label: Overview
---

# PostgreSQL front end

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`postgres-lakesoul` is the LakeSoul engine speaking the PostgreSQL wire protocol.
Point `psql`, JDBC, `pgx` or a BI tool at it and the statements execute in the
lakehouse engine over object storage.

This is not a PostgreSQL extension. There is no Postgres process executing the
query, and no data is copied into Postgres heap tables. The metadata database is
separate infrastructure.

## Start it

```bash
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix file:///tmp/lakesoul/warehouse
```

See [Quick start: the PostgreSQL endpoint](../../get-started/quickstart-postgres.md)
for the metadata database and the first table.

## What works

| Area | Support |
| --- | --- |
| **Connectivity** | PostgreSQL wire protocol, simple and extended query protocol, `pg_catalog`, `psql \d` |
| **DDL** | `CREATE TABLE`, `DROP TABLE`, primary keys, `PARTITIONED BY`, `WITH (...)` options |
| **Writes** | `INSERT ... VALUES`, `INSERT ... SELECT`, `ON CONFLICT DO NOTHING`, `ON CONFLICT ... DO UPDATE SET col = EXCLUDED.col` (every non-key column) |
| **Queries** | The DataFusion SQL surface, including joins, aggregates, windows, CTEs and set operations |
| **Search** | pgvector-compatible vector operators, `text_score` for BM25 |
| **Incremental SQL** | `INSERT ... SELECT` maintains its target when the session enables it |
| **Transactions** | Autocommit; `READ COMMITTED`; failed-transaction recovery |
| **Isolation** | One database maps to one LakeSoul namespace; user tables live in `public` |

## What does not

`UPDATE`, `DELETE`, `COPY`, `RETURNING`, `CREATE TABLE AS`, `TRUNCATE`, non-
`READ COMMITTED` isolation, and writes inside an explicit transaction block are
refused with SQLSTATE `0A000`. See the
[compatibility matrix](compatibility.md) for the full list.

## Next

- [DDL](ddl.md)
- [Insert and upsert](insert-and-upsert.md)
- [Transactions and catalog](transactions-and-catalog.md)
- [Compatibility](compatibility.md)
- [Operations](operations.md)
