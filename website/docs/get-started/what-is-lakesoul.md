---
sidebar_position: 1
---

# What is LakeSoul?

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Why LakeSoul

The default answer to agent data infrastructure is to assemble a platform: a
warehouse for analytics, a CDC pipeline for freshness, a vector database for
retrieval, an OLTP database for state, and ETL to keep them consistent. Each
system is another API for the agent to learn and another copy of the data to
reconcile; a closed platform adds a lock-in you cannot undo.

LakeSoul takes another path. Agents already have PostgreSQL clients, so LakeSoul
puts an open lakehouse behind the PostgreSQL wire protocol: federated queries
over data where it lives, CDC subscriptions for freshness, vector and structured
search in one plan, incremental SQL, and the same tables from a single process to
a PB-scale distributed cluster. The data stays in your object store, in Parquet
and Vortex, under Apache 2.0.

An agent is one kind of PostgreSQL client. The same endpoint serves `psql`, BI
tools, applications and notebooks; no separate API or data copy is required for
an agent to use it. The catalog, commit protocol, file IO, execution engine and
indexes are one Rust codebase; reading the data does not require a JVM, and
search and AI do not need a second copy.

## What it does

- **PostgreSQL front end.** `CREATE TABLE`, `INSERT ... ON CONFLICT`, `UPDATE`,
  `COPY ... FROM STDIN`, `RETURNING` and explicit transaction blocks run in the
  engine over object storage. `psql`, JDBC, `pgx`, SQLAlchemy reflection and
  `pg_catalog` work; there is no Postgres extension and no CDC pipeline in the
  path.
- **Federated query.** Foreign tables through ADBC expose PostgreSQL, MySQL,
  SQLite, DuckDB, BigQuery, Snowflake and Trino tables; joins with local tables
  run in one DataFusion plan. External Parquet, CSV and JSON files on S3 are
  foreign tables too.
- **CDC subscriptions.** `CREATE SUBSCRIPTION` replicates PostgreSQL logical
  replication or MySQL binlog changes into LakeSoul CDC tables, with managed
  snapshots, schema evolution and multi-table subscriptions.
- **Incremental SQL.** An `INSERT ... SELECT` can maintain its target from the
  table's own commit log, so each run reads only the commits since its cursor and
  writes only the changed rows.
- **Vector and full-text search.** pgvector-compatible SQL over IVF + RaBitQ
  indexes, and BM25 full-text search over Tantivy indexes, both living next to the
  data and maintained on write.
- **Python-native reads.** The Python SDK reads object storage directly through
  the Rust IO layer, so Daft, Ray and PyTorch can shard a scan without a SQL
  gateway.
- **Distributed execution.** A coordinator plans across `lakesoul-worker`
  processes, with Kubernetes discovery; the same tables scale from a single
  process to a PB-scale cluster. Spark, Flink and Presto connect through the same
  Rust IO path.

## Where it fits

A table format describes how files are laid out. It has no engine, no indexes and
no incremental state. A database gives you an engine, but you adopt its storage.

LakeSoul is the engine in between: your object storage, in open files, with a
stateless compute layer on top. The
[architecture and table format](../engine/architecture.md) page describes the
catalog, commit protocol and storage layout.

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
| **PostgreSQL** | SQL, BI, applications | Wire protocol and SQL dialect, `psql`, JDBC, `pgx`, SQLAlchemy reflection, pgvector-compatible operators |
| **Python** | AI and data science | Daft, Ray, PyTorch, Pandas and DuckDB reading object storage directly |
| **Connectors** | Existing compute stacks | Spark, Flink, Presto and other engines, through the same Rust IO path |

## Limitations

- Not a drop-in PostgreSQL. The wire protocol and SQL surface are implemented
  natively; the [compatibility matrix](../engine/postgres/compatibility.md)
  records the exact coverage.
- It reads operational databases through foreign tables and CDC subscriptions;
  it does not replace the OLTP database.
- The metadata catalog requires PostgreSQL and is part of the deployment.
- The table format is not a public specification; other engines read through
  LakeSoul connectors or native IO.
- PB-scale deployment requires distributed workers or a Spark/Flink cluster;
  capacity depends on the object store and the compute attached to it.

## Next

- [Quick start: the PostgreSQL endpoint](quickstart-postgres.md)
- [Quick start: Python](quickstart-python.md)
- [Architecture](architecture.md)
- [Engine overview](../engine/overview.md)
