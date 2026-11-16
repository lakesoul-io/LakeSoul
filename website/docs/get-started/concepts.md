---
sidebar_position: 4
sidebar_label: Concepts
---

# Concepts

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Table, namespace, partition

A LakeSoul **table** lives in a **namespace**. The PostgreSQL endpoint maps a
database to a namespace, and user tables are exposed in the `public` schema.

A table can have:

- **Range partitions**, declared with `PARTITIONED BY (col, ...)`.
- **Hash buckets**, declared with `WITH (hash_bucket_num = n)`.
- **A primary key**, which enables upsert and merge-on-read.

A table without a primary key is append-only.

## Commit and snapshot

Every write publishes a **commit** that records the files it added or removed.
Readers open a **snapshot**: a consistent view of the table at one commit. The
catalog and the commit records live in PostgreSQL; the data files live in object
storage.

Snapshots enable time travel, rollback and incremental reads.

## Merge-on-read and compaction

A primary-key write does not rewrite existing files. It writes a **delta file**
with the new rows and their change kind. A read merges the delta files with the
base files and returns one row per key.

**Compaction** merges delta files into larger base files, in size tiers. It runs
in the background and does not block readers.

## Indexes

Vector and full-text indexes are table properties. They are stored under the
table path, built per (partition, bucket) shard, and maintained as rows are
written.

- **Vector**: IVF + RaBitQ, queried with pgvector-compatible operators.
- **Full text**: Tantivy BM25, queried with `text_match` and `text_score`.

## Incremental SQL

A table's commits are also a changelog. Incremental SQL keeps a cursor per target
and, on each `INSERT ... SELECT`, reads only the commits after that cursor and
writes only the changed rows.

## Two PostgreSQLs

1. The **metadata store** holds the catalog and commit records. It is
   infrastructure; users do not query tables through it.
2. The **PostgreSQL endpoint** (`postgres-lakesoul`) speaks the PostgreSQL wire
   protocol to users and executes SQL in the engine.

Keep them on separate ports in local setups.

## Next

- [Architecture](architecture.md)
- [PostgreSQL front end](../engine/postgres/overview.md)
- [Incremental SQL](../engine/incremental-sql/overview.md)
