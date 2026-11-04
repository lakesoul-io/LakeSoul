<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

# Roadmap

LakeSoul is an open lakehouse engine: a catalog, a commit protocol, a native IO
layer and a vectorized execution engine in one Rust codebase, reachable over
PostgreSQL, Arrow and Python.

This page is the public plan, organized by **what becomes possible for a user**
rather than by component. Anything in *Now* has an owner; anything in *Next* has a
design sketch; anything in *Later* is a direction we want feedback on.

- [`good first issue`](https://github.com/lakesoul-io/LakeSoul/labels/good%20first%20issue)
  is the best place to start contributing.
- Open a [discussion](https://github.com/lakesoul-io/LakeSoul/discussions) to argue
  with anything below. The roadmap changes when the arguments do.

## Now

- **Complete the PostgreSQL write surface.** `UPDATE`, `DELETE`, `COPY`,
  `RETURNING`, expression assignments in `ON CONFLICT DO UPDATE`, and writes inside
  a transaction block.
- **pgvector coverage.** Complete the operator, cast and index-option surface, and
  document exactly what is covered.
- **Incremental views GA.** More query shapes, a `CREATE MATERIALIZED VIEW` /
  `REFRESH` surface, and cursor-aware garbage collection for sources with a TTL.
- **Python direct read GA.** Sharding, prefetch and snapshot semantics documented
  and benchmarked for Daft, Ray and PyTorch.

## Next

- **Distributed incremental maintenance.** Push IVM refresh and compaction to the
  distributed executor; today `--ivm` and distributed mode are mutually exclusive.
- **Secondary indexes in Spark and Flink.** The index framework and the vector and
  full-text kinds exist; the JVM connectors do not use them yet.
- **External catalog interoperability.** Let engines that do not link the native
  library read the same tables, without a custom connector.

## Later

- **More index kinds**, driven by what users actually ask for.
- **An embodied dataset API.** First-class episodes, frames and tensors instead of
  modelling them as wide rows.
- **Metadata cache.** Reduce the per-query metadata round trips without weakening
  snapshot semantics.

## Shipped

### 2026

- **PostgreSQL front end.** Wire protocol and extended query protocol; `CREATE
  TABLE`, `DROP TABLE` and partitioned DDL; `INSERT` with `ON CONFLICT DO NOTHING`
  and `ON CONFLICT ... DO UPDATE SET col = EXCLUDED.col`; `pg_catalog`, `psql \d`,
  namespace-to-database mapping.
- **Incremental views through PostgreSQL.** `SET lakesoul.ivm.enabled = on` makes
  `INSERT ... SELECT` maintain the target from the source changelog, with atomic
  rebuilds and per-view leases.
- **Incremental materialized views.** Replay-safe refresh covering aggregates,
  distinct aggregates, windows, top-k, joins, set operations and correlated
  aggregates, with `ivm.epochs` bookkeeping.
- **Multimodality.** [Vortex](https://github.com/vortex-data/vortex) and
  `vortex-compact` as the default layout, [Daft](https://github.com/Eventual-Inc/Daft)
  integration, blob references, ANN search with upserts, BM25 full-text search
  with upserts.
- **Performance.** 2× faster merge-on-read with window-sliding merge (full and
  partial); 50% lower memory in the primary-key writer via spill-sort; disk LRU
  cache for object store; [Apache Gluten](https://github.com/apache/gluten) and
  [Velox](https://github.com/facebookincubator/velox) integration; up to 100×
  faster partition pruning; optional routing of read-only metadata queries to a
  PostgreSQL standby; secondary index framework.
- **Maintenance.** Leveled compaction; async cleanup (vacuum) via Flink CDC on a
  PostgreSQL replication slot.
- **Security.** S3 proxy with table-level RBAC verification.

### Earlier

- **Data science and AI** — native Python reader without PySpark; PyTorch Dataset
  and distributed training; Ray and Daft support.
- **Metadata** — multi-level partitioning (multiple range partitions and at most
  one hash partition); concurrent writes with automatic conflict resolution; MVCC
  with read isolation; two-stage commit through PostgreSQL transactions; schema
  evolution (add and drop columns).
- **Table operations** — LSM-tree style upsert for hash-partitioned tables;
  merge-on-read for upsert delta files; copy-on-write update for non-hash-
  partitioned tables; automatic disaggregated compaction service.
- **Data warehousing** — CDC ingestion with automatic DDL sync; snapshot query;
  incremental query; incremental streaming source; Flink stream/batch source;
  multi-workspace and RBAC.
- **Spark integration** — Table/DataFrame API; SQL support with catalog (except
  upsert); shuffle/join elimination on primary keys; Merge UDF; Merge Into SQL
  with primary-key match (merge-on-read).
- **Flink integration and CDC ingestion** — Table API; batch/stream sink and
  source; changelog stream semantics; exactly-once source and sink; Flink CDC with
  automatic DDL sync, automatic table creation, and multiple source tables with
  different schemas.
- **Hive integration** — export to Hive partitions after compaction; Apache Kyuubi
  (Hive JDBC) integration.
- **Realtime data warehousing** — CDC ingestion; time travel (snapshot read);
  snapshot rollback; global compaction service; MPP engine integration (Presto,
  Presto native execution with Velox, Apache Doris).
- **Cloud and native IO** — object store IO optimization; native vectorized
  merge-on-read; multi-layer storage classes with local-disk data cache.
