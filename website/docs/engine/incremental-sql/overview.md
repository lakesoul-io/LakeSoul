---
sidebar_position: 1
sidebar_label: Overview
---

# Incremental SQL

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

Incremental SQL runs an `INSERT ... SELECT` over the source table's commit log.
The first run builds the target; later runs read only the commits since the last
cursor and write only the rows that changed.

The result is identical to running the query over the full source. The difference
is the work: a run over 10 new rows touches 10 rows, not the whole table.

## What it is

- A **statement**, not an object. You run `INSERT INTO target SELECT ...`; there
  is no separate definition to create, no scheduled job, and no refresh command.
- **Change-driven.** The engine reads the source table's commits, folds them into
  the query, and writes the delta to the target.
- **Partition-aware.** A run finds the source partitions whose commits changed and
  reads only their new data; the target is updated only in the partitions it owns.
- **CDC-enabled target.** The target is a keyed table with `use_cdc = true` and
  the `rowKinds` change column, so a run can write retractions for rows that
  disappear.
- **SQL-level.** It maintains joins, aggregates, windows and set operations, not
  just a storage-level delta.

## How a run works

```
source commits ──► changelog window ──► query delta ──► target delta
                        │
                        └── cursor advances only after the target commit
```

1. The engine reads the source commits after the target's last cursor.
2. It computes the rows the query would change.
3. It commits the target delta and advances the cursor in the same step.
4. A retry replays the same window; already-written rows are skipped.

The first run **bootstraps** the target from the current source state. If the
query definition changes, or the target schema changes, the engine rebuilds the
target from the full source in one atomic commit.

## When to use it

- A dashboard table that must follow a changing source without a full rebuild.
- A join or aggregate that is expensive to recompute end to end.
- A pipeline that already writes through the engine and wants the derived table
  to follow automatically.

For a one-off transformation, a normal `INSERT ... SELECT` is simpler.

## Next

- [Getting started](getting-started.md)
- [Supported queries](supported-queries.md)
- [Consistency and limits](consistency-and-limits.md)
