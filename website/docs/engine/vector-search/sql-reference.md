---
sidebar_position: 2
---

# Vector SQL reference

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Vector inputs

A query vector can be a string literal or an array:

```sql
embedding <-> '[0.12, 0.98, 0.31]'
embedding <-> ARRAY[0.12, 0.98, 0.31]
```

A parameter works in prepared statements:

```sql
SELECT id FROM documents ORDER BY embedding <-> $1 LIMIT 10;
```

The column is `Float32` with a fixed dimension. The dimension must match the
query vector; a mismatch is an error.

## Operators

| Operator | Returns | Index |
| --- | --- | --- |
| `<->` | Euclidean distance (square root) | Yes |
| `<#>` | Negative inner product | Yes |
| `<=>` | Cosine distance | Exact only |

`<#>` returns the negative inner product so that ascending order matches
pgvector's ordering. Cosine is evaluated exactly; see
[Distance metrics](overview.md#distance-metrics).

## Top-k queries

```sql
SELECT id, body, embedding <-> '[0.12, 0.98, 0.31]' AS distance
FROM documents
ORDER BY embedding <-> '[0.12, 0.98, 0.31]'
LIMIT 10;
```

The planner recognises the `ORDER BY <distance expression> LIMIT k` shape and
pushes the top-k into the ANN scan. For inner product, order descending:

```sql
SELECT id FROM documents
ORDER BY embedding <#> $1 DESC
LIMIT 10;
```

`LIMIT` is required for the index path. Without it the query falls back to a full
distance computation.

## Probes

```sql
SET ivfflat.probes = 16;
SHOW ivfflat.probes;
```

`ivfflat.probes` maps to the number of IVF lists probed. The setting is
connection-local, defaults to a small value, and is validated (`0` is rejected).
Higher values trade latency for recall.

## Reranking

By default the scan fetches candidates from the index and recomputes their exact
distance before returning them. To use the index's approximate score directly:

```sql
SET lakesoul_vector_search.rerank = 'index';
```

Exact reranking is the safer default; index-only reranking is faster but returns
approximate distances.

## Exact fallback

Without a usable index, or for a metric the index cannot serve (`<=>`), the
engine computes the distance exactly over the scan. Results are correct; latency
grows with the number of rows.

## Combining with filters

Filters in the same query are applied with candidate overfetch. See
[Hybrid search](hybrid-search.md).

## Type notes

- `vector(n)` is the column type used by the Python SDK.
- A `CAST(... AS vector(n))` in a query is supported for constructing a vector
  value.
- The PostgreSQL front end does not create `vector` columns itself; create the
  table and its index through the Python SDK.

## Next

- [Index management](index-management.md)
- [Hybrid search](hybrid-search.md)
- [Benchmarks](benchmarks.md)
