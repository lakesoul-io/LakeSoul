---
sidebar_position: 1
sidebar_label: Overview
---

# Engine overview

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

One engine provides four capabilities over the same tables and the same snapshot.

| Capability | What it gives you | Start here |
| --- | --- | --- |
| **PostgreSQL front end** | `psql`, JDBC, `pgx`, `pg_catalog`, DDL, `INSERT`, upserts and queries executed by the engine | [PostgreSQL](postgres/overview.md) |
| **Incremental SQL** | `INSERT ... SELECT` that reads only new commits and writes only changed rows | [Incremental SQL](incremental-sql/overview.md) |
| **Vector search** | pgvector-style SQL over lake-resident IVF + RaBitQ indexes | [Vector search](vector-search/overview.md) |
| **Full-text search** | BM25 ranking over Tantivy indexes in the same engine | [Full-text search](full-text-search/overview.md) |

## One snapshot, several readers

The PostgreSQL endpoint, the Python SDK and the connectors all read the committed
snapshot. A change committed through one path is visible to the others; there is
no replication step between SQL, search and Python.

## Boundaries

- The engine executes SQL over object storage; it does not require a JVM.
- The PostgreSQL front end is native, not a drop-in PostgreSQL. See the
  [compatibility matrix](postgres/compatibility.md).
- Incremental SQL is a query execution mode, not a separate object type. You run
  an `INSERT ... SELECT`; the engine decides what changed.
- Spark, Flink and Presto are optional connectors. The engine does not depend on
  them.

## Next

- [PostgreSQL front end](postgres/overview.md)
- [Incremental SQL](incremental-sql/overview.md)
- [Vector search](vector-search/overview.md)
- [Full-text search](full-text-search/overview.md)
