---
sidebar_position: 5
---

# Architecture

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul has three layers: clients at the top, one engine in the middle, and your
storage at the bottom.

```
┌──────────────────────────────────────────────────────────────┐
│  psql · JDBC · BI tools      Python SDK · Daft · Ray · Torch │
└───────────────┬──────────────────────────┬───────────────────┘
                │ PG wire                  │ native IO / Arrow
┌───────────────▼──────────────────────────▼───────────────────┐
│                        LakeSoul engine                       │
│  PostgreSQL front end   ·   incremental SQL                  │
│  vector + full-text indexes   ·   vectorized execution       │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│  object storage (S3 / OSS / HDFS)  +  PostgreSQL metadata    │
└──────────────────────────────────────────────────────────────┘
```

## Engine components

| Component | Responsibility |
| --- | --- |
| **Catalog and commit protocol** | Tables, namespaces, snapshots and atomic commits, stored in PostgreSQL |
| **Native IO** | Parquet, Vortex and Vortex Compact; merge-on-read for primary-key tables; object-store cache |
| **Execution** | DataFusion planning and vectorized execution, plus LakeSoul planner rules |
| **PostgreSQL front end** | Wire protocol, `pg_catalog`, DDL, `INSERT`, query sessions and limits |
| **Incremental SQL** | Consumes the source commit log and writes only changed rows to the target |
| **Vector and full-text indexes** | IVF + RaBitQ and Tantivy BM25, maintained on write and pushed into the scan |
| **Compaction** | Disaggregated, size-tiered merge of delta files |

## Two PostgreSQL roles

The name "PostgreSQL" appears twice, in different places:

1. **The metadata store.** LakeSoul uses a PostgreSQL database for its catalog and
   commit records. This is infrastructure; users do not connect to it to query
   tables.
2. **The PostgreSQL endpoint.** `postgres-lakesoul` speaks the PostgreSQL wire
   protocol to users. It is the LakeSoul engine, not the metadata database.

Keep the two on separate ports in local setups. The quick start uses `5432` for
metadata and `5433` for the endpoint.

## Compute and storage

- **Storage is yours.** Table data lives in your bucket, in open formats, with
  the commit metadata in PostgreSQL.
- **Compute is stateless.** A `postgres-lakesoul` process can be restarted or
  replaced without moving data. With distributed workers configured, queries are
  planned across them.
- **One snapshot.** The PostgreSQL endpoint, the Python SDK and the connectors
  read the same committed snapshot. There is no second copy for search or AI.

## Next

- [Engine overview](../engine/overview.md)
- [Deep dive: engine architecture and table format](../engine/architecture.md)
- [PostgreSQL front end](../engine/postgres/overview.md)
- [Incremental SQL](../engine/incremental-sql/overview.md)
- [Vector search](../engine/vector-search/overview.md)
