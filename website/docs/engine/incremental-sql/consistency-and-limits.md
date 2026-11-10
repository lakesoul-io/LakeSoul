---
sidebar_position: 4
---

# Consistency and limits

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Replay safety

Every consumed source window is recorded in the engine's epoch table, keyed by
the target. A retry of the same run sees the recorded window and skips the rows
that were already written. The target commit and the cursor advance happen
together.

A run is either fully applied or not applied at all. A failed run leaves the
target at its previous committed state.

## Rebuilds

A rebuild recomputes the target from the full source and publishes it as one
atomic commit. It does not truncate the user-visible target first, so readers
never observe an empty table during a rebuild.

A rebuild happens when:

- the query definition changes;
- the target schema changes in a way the engine cannot reconcile;
- the target has no usable state (for example, after it was dropped and
  recreated).

## Concurrency

- Two runs against the same target are serialized by a per-target lease. The
  second waits or fails instead of interleaving writes.
- A data commit written by another engine in a partition the run owns is detected
  before the run and again before its commit; the run fails instead of silently
  overwriting it. Compaction commits are allowed.
- Ordinary writes to other partitions can proceed concurrently.

## Partitions

A run detects the source partitions whose commits changed and reads only their
new data. The target is replaced only in the partitions it owns; other target
partitions are left untouched.

## Retention

A run needs the source history between its cursor and the present. Do not enable
`partition.ttl`, `compaction.ttl` or `dataExpiredTime` cleanup on tables consumed
by incremental SQL: deleting the history makes the cursor unreadable.

If history is missing, the run fails; the next run rebuilds from the current
source state.

## Limits

| Limit | Behaviour |
| --- | --- |
| **Autocommit only** | Explicit transactions and multi-statement batches are refused |
| **`INSERT ... SELECT` only** | `INSERT ... VALUES` is always a normal insert |
| **Keyed sources** | A source without a primary key is rejected |
| **Keyed target** | The target needs a primary key, `rowKinds`, and `use_cdc = true` |
| **Distributed execution** | `--ivm` can run with distributed workers; every worker must be built with the IVM feature and the same protocol generation. The coordinator resolves source windows and commits the files workers write. |
| **Supported shapes** | A shape outside the list reports `unsupported incremental view shape` and writes nothing |

## Failure modes

| Symptom | Cause |
| --- | --- |
| `source table ... has no primary key` | The source is append-only; use a keyed source |
| `unsupported incremental view shape` | The query shape is outside the supported list |
| `target table ... needs the internal rowKinds/__ivm_epoch columns` | The target is missing `rowKinds` (or the engine could not add the epoch column) |
| `target table ... needs a primary key` | The target is not a keyed table |
| A concurrent-writer error | Another engine wrote the target scope during the run; retry |

## Next

- [Supported queries](supported-queries.md)
- [Getting started](getting-started.md)
