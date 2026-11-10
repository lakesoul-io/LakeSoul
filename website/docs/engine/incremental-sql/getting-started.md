---
sidebar_position: 2
---

# Incremental SQL: getting started

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 1. Start the endpoint with incremental SQL enabled

```bash
cargo run -p postgres-lakesoul -- --port 5433 --ivm \
  --warehouse-prefix file:///tmp/lakesoul/warehouse
```

`--ivm` enables the `INSERT ... SELECT` maintenance path. Without it, the
statement is a normal insert.

## 2. Create a target table

The target needs a primary key, the CDC change column `rowKinds`, and
`use_cdc = true`:

```sql
CREATE TABLE daily_revenue (
  day     date not null,
  region  text not null,
  revenue numeric(20,4),
  "rowKinds" text,
  PRIMARY KEY (day, region)
) PARTITIONED BY (day)
  WITH (hash_bucket_num = 4, use_cdc = true);
```

- The primary key identifies a target row; the engine writes delete markers for
  rows that disappear.
- `"rowKinds"` is the change column. Quote it to preserve the mixed case; the
  front end folds unquoted identifiers to lower case.
- `use_cdc = true` enables the change stream the target writes retractions to.
  Without it, a run cannot remove rows that disappear from the source.
- The engine adds its epoch column to the target schema if it is missing.

If the source is range-partitioned, a run detects which source partitions have
new commits and reads only those partitions. The target can be range-partitioned
too: a run replaces only the partitions it owns and leaves the others alone.

## 3. Run the query

```sql
SET lakesoul.ivm.enabled = on;

INSERT INTO daily_revenue (day, region, revenue)
SELECT date_trunc('day', ts) AS day,
       region,
       sum(amount) AS revenue
FROM orders
GROUP BY 1, 2;
```

The first run **bootstraps** the target: it computes the query over the current
source state and writes the full result.

Later runs read only the source commits after the last cursor and write only the
changed rows. The statement is idempotent for a given source state.

## 4. Keep the session setting

`lakesoul.ivm.enabled` is session-local and defaults to off. Set it on every
connection that runs incremental SQL. `INSERT ... VALUES` is always a normal
insert, regardless of the setting.

## 5. Re-run after source changes

There is no scheduler and no refresh command. Run the same `INSERT ... SELECT`
when you want the target to catch up; the engine decides what changed.

If the query definition changes, the next run rebuilds the target from the full
source in one atomic commit. Readers do not see an empty table during the
rebuild.

## Requirements

- The target must be a keyed, CDC-enabled table: primary key, `rowKinds`, and
  `use_cdc = true`.
- Sources must be keyed tables. A source whose deletes must be visible also
  needs `use_cdc = true` and a change column. Append-only sources are rejected
  for incremental runs; a shape that cannot be maintained incrementally falls
  back to a full recompute.
- The statement must run in autocommit. Explicit transactions and multi-statement
  batches are refused.
- Distributed execution is supported when every worker is built with the IVM
  feature and the same protocol generation. The coordinator still owns source
  windows, epochs and file commits; workers perform the scan/lookup/write
  stages.

## Next

- [Supported queries](supported-queries.md)
- [Consistency and limits](consistency-and-limits.md)
