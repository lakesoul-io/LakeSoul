---
sidebar_position: 7
sidebar_label: DELETE and TRUNCATE
---

# DELETE and TRUNCATE

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`DELETE` and `TRUNCATE` publish new metadata versions. They never delete an
object synchronously: the old commits and files stay in place, so a pinned
snapshot can still read the pre-delete relation, and GC reclaims unreferenced
objects later.

## DELETE

```sql
DELETE FROM orders WHERE id = 42;
DELETE FROM events WHERE day = '2026-01-01';   -- clears whole partitions
DELETE FROM orders WHERE id = 42 RETURNING id, amount;
```

How the rows are removed depends on the table:

| Table | Representation |
| --- | --- |
| CDC table with a primary key | A tombstone row: the change column becomes `'delete'` |
| Non-CDC table, predicate only names range-partition columns | An empty snapshot for the matching partitions |
| Non-CDC table, any other predicate | The affected partitions are rewritten without the matching rows |
| Table without a primary key | The whole table or the affected partitions are rewritten |
| `appendOnly` table | Refused with `0A000` |
| CDC table without a primary key | Refused with `0A000` |

The non-CDC rewrite writes the current merge-on-read relation to new files and
replaces the affected partitions with one `UpdateCommit`. The cost is the size
of those partitions, not the number of deleted rows. A partition-only
predicate (`day = ...`, `part IN (...)`) avoids the rewrite entirely.

`DELETE ... RETURNING` returns the deleted rows. The result is collected before
the metadata commit and is subject to the server's result limits.

### Boundaries

- `DELETE ... USING`, `ORDER BY`, `LIMIT` and multi-table deletes are refused
  with `0A000` (Phase 1).
- A non-CDC rewrite is refused inside an explicit transaction block in Phase 1:
  its `UpdateCommit` publication is not staged yet. A CDC tombstone is an
  ordinary append and is staged normally, including read-your-writes.
- A write in a multi-statement simple-query message is refused; split the
  statements or use an explicit block.
- The target may not be a foreign table, an external-file table, a subscription
  target or an IVM internal table.

## TRUNCATE

```sql
TRUNCATE TABLE orders;
TRUNCATE TABLE orders PARTITION (day = '2026-01-01');
TRUNCATE TABLE orders, order_items;            -- one metadata transaction
TRUNCATE TABLE IF EXISTS staging;              -- missing table: success
```

`TRUNCATE` publishes an empty snapshot for every selected partition. The
partition version advances and its so-far-visible commits are no longer in the
snapshot; the commit history and files remain available to time travel and are
removed by GC after the retention window.

- `PARTITION (...)` accepts `column = value` terms and must name every range
  partition column. It is a LakeSoul extension, not PostgreSQL syntax.
- Multiple tables are published in one metadata transaction: either every
  selected partition is cleared or none is.
- `ONLY`, `CASCADE` and `RESTART IDENTITY` are accepted as no-ops (`NOTICE` is
  not emitted): LakeSoul has no inheritance, foreign keys or sequences.
- A missing table is `42P01` unless `IF EXISTS` is given.

`TRUNCATE` is refused inside an explicit transaction block in Phase 1, for the
same reason as a rewrite `DELETE`.

## Time travel, indexes and GC

- Old files are not removed by these statements. A pinned snapshot still reads
  the pre-delete relation; the clean service removes files that no snapshot or
  tag pins after the retention window.
- The vector and text search paths compare a shard's indexed files with the
  current active files. A DELETE/TRUNCATE changes that set, so a stale shard is
  rejected and the query falls back to the exact scan. The default rerank mode
  therefore never returns a deleted row.
- After a non-CDC DELETE rewrite, the server rebuilds pending vector/text index
  shards for the rewritten files. A failed build is logged and leaves the
  search path on the exact fallback; the data is already published.
