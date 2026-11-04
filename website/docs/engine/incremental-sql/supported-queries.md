---
sidebar_position: 3
---

# Supported queries

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

Incremental SQL maintains a query over keyed (primary-key) sources. The shapes
below are maintained incrementally; a shape outside the list falls back to a full
recompute, and an append-only source is rejected for an incremental run.

## Projection, filter, distinct

| Shape | Notes |
| --- | --- |
| `SELECT [expr AS c, ...] FROM src [WHERE p]` | The filter may compare against an uncorrelated scalar subquery |
| `SELECT DISTINCT c, ... FROM src` | Maintains one row per distinct tuple |
| `SELECT DISTINCT ON (g) g, v FROM src [ORDER BY ...]` | The source primary key breaks ties deterministically |

## Aggregates

| Shape | Notes |
| --- | --- |
| `SUM`, `COUNT(*)`, `COUNT(v)`, `AVG` | With or without `GROUP BY`; `HAVING` supported |
| `MIN`, `MAX` | With or without `GROUP BY` |
| `COUNT(DISTINCT v)`, `SUM(DISTINCT v)` | Multi-column distinct requires a `GROUP BY` |
| `VAR_SAMP`, `VAR_POP`, `STDDEV_SAMP`, `STDDEV_POP`, `STDDEV` | |
| `MEDIAN` | |
| `BOOL_AND`, `BOOL_OR` | |
| `APPROX_DISTINCT`, `APPROX_PERCENTILE_CONT`, `APPROX_MEDIAN` | Approximate results |
| `STRING_AGG`, `ARRAY_AGG` | `ORDER BY` inside the aggregate is supported |
| `BIT_AND`, `BIT_OR`, `BIT_XOR` | |
| `CORR`, `COVAR_SAMP`, `COVAR_POP`, `REGR_*` | |
| `PERCENTILE_CONT` | |
| Mixed aggregates in one `GROUP BY` | Any mix of the supported functions |

`GROUPING SETS`, `ROLLUP` and `CUBE` are expanded to their member sets. `SUM`,
`COUNT` and `AVG` keep the incremental layout; other mixes are recomputed per
set.

## Windows and top-k

| Shape | Notes |
| --- | --- |
| `ROW_NUMBER() OVER (PARTITION BY p ORDER BY o)` | Maintained by recomputing the affected partitions |
| `SELECT ... FROM (SELECT ..., ROW_NUMBER() ... AS rn) t WHERE rn <= k` | Top-k per partition |
| `SELECT ... FROM src ORDER BY o LIMIT k` | Global top-k; `OFFSET` shifts the window |

Ties at the boundary are broken by the source primary keys.

## Joins

| Shape | Notes |
| --- | --- |
| Inner equi-join | Both sides keyed; keys may have different names; multiple payload columns |
| `LEFT JOIN` with a right side keyed by the join keys | Lookup join |
| Multi-way inner joins (3–32 sources) | Optional side filters and cross-source conditions |
| Left-deep `[LEFT] JOIN` chains | Each step keyed by its join keys |
| `CROSS JOIN` | Both sides keyed |
| `LEFT`, `RIGHT`, `FULL` outer equi-joins | Unmatched identities are nullable |
| `IS NOT DISTINCT FROM` join keys | Matches NULL with NULL for plain columns |

## Set operations

| Shape | Notes |
| --- | --- |
| `UNION ALL` | Merges sources with the same schema |
| `UNION` | Deduplicated |
| `INTERSECT`, `EXCEPT` | Both `DISTINCT` and `ALL` variants |
| `INTERSECT ALL`, `EXCEPT ALL` | Per-tuple match counts |

## Semi/anti and correlated shapes

| Shape | Notes |
| --- | --- |
| `[NOT] EXISTS`, `IN`, `op ANY`, `op ALL` | Semi/anti joins, optional side filters |
| Correlated scalar aggregate in the select list | `COUNT` family keeps empty-set semantics |
| Correlated scalar filter (`WHERE v > (SELECT AVG(v) ...)`) | The subquery tables are watched alongside the source |

## Unsupported

- Append-only sources for incremental runs. Use a keyed source, or let the shape
  fall back to a full recompute.
- A `SELECT` that changes the target's row identity without a primary key on the
  target.
- A shape outside the list above. The engine reports
  `unsupported incremental view shape` and does not write a partial result.

## Next

- [Consistency and limits](consistency-and-limits.md)
- [Getting started](getting-started.md)
