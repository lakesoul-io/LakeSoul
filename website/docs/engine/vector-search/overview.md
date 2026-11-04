---
sidebar_position: 1
sidebar_label: Overview
---

# Vector search

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul answers pgvector-style queries over ANN indexes that live in the same
object storage as the table. The index is maintained as data is written; there is
no separate vector database to operate.

## What you get

- **SQL surface.** `vector` inputs, `<->`, `<#>` and `<=>`, `ORDER BY ... LIMIT k`
  and `SET ivfflat.probes`.
- **Lake-resident index.** An IVF + RaBitQ index stored next to the table data,
  with the same commit and snapshot semantics.
- **Incremental maintenance.** New rows update the index; a shard is rebuilt only
  when its deltas make the centroid stale.
- **Hybrid search.** Structured filters and ANN in one plan, with candidate
  overfetch for filtered queries.
- **Same engine.** Vector search joins, aggregates and filters with ordinary SQL;
  full-text search is available in the same query.

## How a vector table is created

Vector columns and indexes are declared through the Python SDK, not through
PostgreSQL DDL:

```python
catalog.create_table(
    name="documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,          # embedding is Float32, dim 768
    primary_keys=["id"],
    vector_index=[{"column": "embedding", "dim": 768, "metric": "L2"}],
)
```

The PostgreSQL endpoint and the Python SDK then query the same table.

## Distance metrics

| Operator | Distance | Index |
| --- | --- | --- |
| `<->` | L2 (Euclidean) | Yes |
| `<#>` | Negative inner product | Yes |
| `<=>` | Cosine | Exact evaluation only |

The ANN index supports L2 and inner product. Cosine search is available and
correct, but it does not use the index; normalize vectors and use `<#>` when you
need indexed cosine similarity.

## Next

- [SQL reference](sql-reference.md)
- [Index management](index-management.md)
- [Hybrid search](hybrid-search.md)
- [Benchmarks](benchmarks.md)
