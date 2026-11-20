---
sidebar_position: 10
sidebar_label: Time travel
---

# Time travel (`lakesoul_table_scan`)

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

The PostgreSQL front end exposes a table function for pinning one scan of a
LakeSoul table to an earlier state:

```sql
SELECT * FROM lakesoul_table_scan('events', 'timestamp', '2026-11-20T10:00:00+08:00');
```

```text
lakesoul_table_scan(table, mode, value)
```

All three arguments must be string literals. `mode` selects how `value` is
interpreted:

| mode | value | Meaning |
| --- | --- | --- |
| `timestamp` | RFC3339 timestamp with an explicit offset | Read the latest partition versions committed at or before that instant |
| `tag` | tag name | Read the snapshot a tag pins |
| `snapshot_id` (alias `version`) | integer snapshot id | Read the snapshot frozen by `table_snapshot` |

Examples:

```sql
-- State at a point in time
SELECT count(*) FROM lakesoul_table_scan('events', 'timestamp', '2026-11-20T10:00:00+08:00');

-- A named snapshot and an id from table_snapshot
SELECT * FROM lakesoul_table_scan('events', 'tag', 'release-2026-11');
SELECT * FROM lakesoul_table_scan('events', 'snapshot_id', '42');

-- Ordinary SQL applies on top of the pinned scan
SELECT user_id, sum(amount)
FROM lakesoul_table_scan('events', 'tag', 'release-2026-11')
WHERE amount > 0
GROUP BY user_id;
```

## Semantics

- **Per query, not per session.** The function returns a pinned view of one
  table; nothing is stored in the session. A plain `SELECT` in the same session
  still reads the latest commit.
- **`timestamp` is "at or before".** For each partition the server picks the
  latest version whose `partition_info.timestamp` is `<=` the given instant.
  The timestamp is the wall-clock time the partition version was committed;
  partitions updated by one atomic commit share the same PostgreSQL
  transaction time. An RFC3339 string with an explicit offset is required
  (`+08:00`, `Z`, …); a naive string without an offset is refused.
- **Snapshot and tag reads use the metadata rows** in `table_snapshot`,
  `snapshot_commit` and `table_snapshot_tag`. This front end has no SQL DDL to
  create them yet; they are created by the metadata tooling and the Flink
  clean job. `timestamp` needs no such preparation.
- **Pinned reads do not use vector or text index pushdown.** Those indexes
  describe the latest files, so the scan falls back to the regular
  merge-on-read path. Expect it to be slower than an equivalent current-state
  scan.
- **Reads and writes are independent.** Reading an old version and writing the
  result to another table or partition is an ordinary query and is not
  refused. The target table's own write path still uses its current commit.
- **Visibility is unchanged.** The table is resolved through the connection's
  catalog, so RBAC domain and ACL rules apply to `lakesoul_table_scan` exactly
  as they do to a plain `SELECT`. Foreign tables are not supported.
- **Retention matters.** A pinned read needs its historical `partition_info`
  rows and commit files to still exist. Tags pin commits for the clean job via
  the `pinned` flag; an untagged snapshot may be reclaimed once it is old
  enough.

## Error cases

A missing or hidden table, an unknown mode, a malformed timestamp, a
non-integer snapshot id, or non-literal arguments are all planning-time
errors prefixed with `lakesoul_table_scan:`. For example:

```text
ERROR:  lakesoul_table_scan: timestamp must be an RFC3339 string with an explicit offset
```

## Related

- [DELETE and TRUNCATE](delete-and-truncate.md) — old commits stay readable.
- [Authentication and RBAC](authentication.md) — domain and table visibility.
