---
sidebar_position: 4
sidebar_label: 混合检索
---

# Hybrid search

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

Structured filters and vector similarity run in one plan:

```sql
SELECT id, title, embedding <-> $1 AS distance
FROM documents
WHERE tenant_id = 'acme'
  AND category = 'contract'
ORDER BY embedding <-> $1
LIMIT 10;
```

The filter is evaluated on the scan; the ANN index supplies candidates. The
engine overfetches candidates when a filter is present, then applies the filter
and keeps the top `k`.

## Overfetch and reranking

- Without a filter, the scan fetches roughly `2 × k` candidates.
- With a filter, it fetches roughly `10 × k` candidates, because some candidates
  will be filtered out.
- By default, candidate distances are recomputed exactly before the result is
  returned. `SET lakesoul_vector_search.rerank = 'index'` uses the index score
  instead.

## Selectivity

A highly selective filter can return fewer than `k` rows if the candidate pool
does not contain enough matching rows. Options:

- Raise `ivfflat.probes` to scan more lists.
- Express the filter so it prunes partitions or buckets.
- Fall back to an exact scan for very selective predicates.

The engine does not silently change the query shape; check `EXPLAIN` to see
whether the ANN path was used.

## Choosing a metric

- Use `<->` (L2) when the embedding model was trained with Euclidean distance.
- Use `<#>` (negative inner product) for maximum inner product search.
- For cosine similarity, normalize vectors at write time and use `<#>` so the
  query can use the index. `<=>` is evaluated exactly.

## Next

- [Vector SQL reference](sql-reference.md)
- [Index management](index-management.md)
- [Full-text search](../full-text-search/overview.md)
