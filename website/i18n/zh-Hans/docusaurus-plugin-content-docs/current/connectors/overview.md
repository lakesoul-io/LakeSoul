---
sidebar_position: 1
---

# Connectors

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

Connectors are optional. The engine speaks PostgreSQL and exposes a Python SDK;
the pages in this section are for teams that already run Spark, Flink, Presto or
another engine and want it to read or write the same tables.

| Connector | Version | Read | Write | Interface |
| --- | --- | --- | --- | --- |
| [Spark](spark/getting-started.mdx) | 3.5 | Batch | Batch | Java / Python / Scala / SQL |
| [Flink](flink/getting-started.mdx) | 1.20 | Streaming | Streaming | Java / SQL |
| [Presto](presto.md) | 0.296 | Batch | — | SQL |
| [Doris](doris.md) | — | Batch | — | SQL |
| [Kyuubi](kyuubi.md) | — | Batch | — | Hive JDBC |
| [Hive export](hive.md) | — | Batch | Export | Hive |
| [Elasticsearch gateway](elasticsearch-gateway.md) | — | Search | — | HTTP / Elasticsearch API |

## What stays the same

Every connector uses the same Rust IO layer, the same catalog and the same
snapshot. A table written by Spark is readable from `psql` and from Python without
a copy or a sync job.

## What connectors are for

- Running an existing Spark or Flink pipeline against LakeSoul tables.
- Reading LakeSoul from a Presto or Doris cluster.
- Exposing tables to Hive-based tools.
- Serving an Elasticsearch-compatible search API.

## Next

- [Spark](spark/getting-started.mdx)
- [Flink](flink/getting-started.mdx)
- [Presto](presto.md)
