---
sidebar_position: 1
---

# What is LakeSoul?

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul is an open lakehouse engine. It turns plain object storage into a
transactional lakehouse, and it speaks PostgreSQL.

The catalog, the commit protocol, the file IO, the execution engine and the
indexes are one Rust codebase. There is no separate catalog service, no JVM
required to read the data, and no second copy of the data for search or AI.

## What it does

- **PostgreSQL front end.** `CREATE TABLE`, `INSERT ... ON CONFLICT` and queries
  run in the engine over object storage. `psql`, JDBC, `pgx` and `pg_catalog`
  work; there is no Postgres extension and no CDC pipeline in the path.
- **Incremental SQL.** An `INSERT ... SELECT` can maintain its target from the
  table's own commit log, so each run reads only the commits since its cursor and
  writes only the changed rows.
- **Vector and full-text search.** pgvector-compatible SQL over IVF + RaBitQ indexes,
  and BM25 full-text search over Tantivy indexes, both living next to the data
  and maintained on write.
- **Python-native reads.** The Python SDK reads object storage directly through
  the Rust IO layer, so Daft, Ray and PyTorch can shard a scan without a SQL
  gateway.

## Where it fits

A table format describes how files are laid out. It has no engine, no indexes and
no incremental state. A database gives you an engine, but you adopt its storage.

LakeSoul is the engine in between: your object storage, in open files, with a
stateless compute layer on top.

```
   PostgreSQL                 Arrow                       Python
   psql / JDBC / pgx          Spark · Flink · Presto      Daft · Ray · PyTorch
        │                     DuckDB · Daft                    │
        └──────────────┬───────────────┬───────────────────────┘
                       ▼               ▼
              ┌──────────────────────────────────┐
              │          LakeSoul engine         │
              │  catalog · commit protocol       │
              │  native IO · MOR · compaction    │
              │  indexes · incremental SQL       │
              └────────────────┬─────────────────┘
                               ▼
                 object storage (S3 / OSS / HDFS)
                      + PostgreSQL metadata
```

## Three interfaces

| Interface | For | What you get |
| --- | --- | --- |
| **PostgreSQL** | SQL, BI, applications | Wire protocol and SQL dialect, `psql`, JDBC, `pgx`, pgvector-compatible operators |
| **Python** | AI and data science | Daft, Ray, PyTorch, Pandas and DuckDB reading object storage directly |
| **Connectors** | Existing compute stacks | Spark, Flink, Presto and other engines, through the same Rust IO path |

## What it is not

- It is not a drop-in PostgreSQL. The wire protocol and the SQL surface are
  implemented natively, and the [compatibility matrix](../engine/postgres/compatibility.md)
  tracks what is covered.
- It is not a table format. There is an engine to run.
- It does not own your storage. The files stay in your bucket, in open formats.

## Next

- [Quick start: the PostgreSQL endpoint](quickstart-postgres.md)
- [Quick start: Python](quickstart-python.md)
- [Architecture](architecture.md)
- [Engine overview](../engine/overview.md)
