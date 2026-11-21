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
  `snapshot_commit` and `table_snapshot_tag`. You can create them from SQL (see
  [Managing snapshots and tags](#managing-snapshots-and-tags)); the Python
  catalog offers the same operations. `timestamp` needs no such preparation.
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

## Session-wide default

A connection can pin every local table scan with one setting:

```sql
SET lakesoul.read.time_travel = '2026-11-20T10:00:00+08:00';
SELECT * FROM events;          -- as of that instant
SELECT * FROM lakesoul_table_scan('events', 'timestamp', '2026-11-21T00:00:00Z');  -- explicit pin wins

RESET lakesoul.read.time_travel;   -- or SET … = DEFAULT
```

- **No snapshot or tag is needed.** Each partition resolves to its latest
  version committed at or before the instant, exactly like the `timestamp`
  mode of `lakesoul_table_scan`.
- If a table has **no** version at or before the instant, the scan fails with
  `table "…" has no version committed at or before …` instead of returning
  zero rows. Partitions created after the instant are simply omitted.
- A table that does have older versions but whose query filter matches nothing
  is an ordinary empty result, not an error.
- The snapshot is uniform: `UPDATE` and `DELETE` targets read the same pinned
  rows a `SELECT` sees, not the latest commit. A row written after the instant
  is invisible to them. Writes remain available — `INSERT … SELECT` can copy
  pinned rows elsewhere. Because a new commit is published on top of current
  state, writing a row that also changed after the instant can overwrite that
  later change; use a narrow `WHERE` when that matters.
- An explicit `lakesoul_table_scan` pin takes precedence over the session
  default.
- Only local LakeSoul tables are pinned; federated foreign tables have no time
  travel and keep reading their current state.
- The value must be an RFC3339 string with an explicit offset; `SHOW` reports
  it normalized to UTC. `SET LOCAL` is refused (the setting is
  connection-local and not reverted by `COMMIT`/`ROLLBACK`). `RESET ALL` and
  `DISCARD ALL` clear it.
- The setting is per connection, so a pool must set it on the connections that
  need it.

## Managing snapshots and tags

A **snapshot** is an immutable, table-wide freeze of the partition versions
and commit ids that were current at a point in time. Its numeric `snapshot_id`
is the "version" used by `lakesoul_table_scan`. A **tag** is a readable name
attached to one snapshot. Creating one freezes and names it in a single
statement:

```sql
ALTER TABLE events CREATE TAG `EOW-01`;                                       -- freeze now
ALTER TABLE events CREATE TAG `v1` AS OF timestamp '2026-11-20T10:00:00+08:00';
ALTER TABLE events CREATE TAG `copy` AS OF VERSION 7;                          -- name an existing version

SHOW TAGS events;                -- tag, version, created_at, snapshot_created_at, description
ALTER TABLE events DROP TAG `v1`;
```

- `AS OF timestamp` freezes the latest partition version committed at or before
  that instant, so the tag reads exactly the state at that time. `AS OF VERSION`
  requires an existing snapshot id of that table. Omitting `AS OF` freezes the
  current state.
- `SHOW TAGS` returns **all columns as text**; timestamps are RFC3339 strings.
  Its output is produced by the `lakesoul_tags('table')` table function, which
  you can also query directly.
- `DROP TAG` removes the tag and, when no other tag or sample manifest
  references that snapshot, the snapshot itself (unpinning its commits for
  normal GC). If it is shared, only the tag is removed and a `NOTICE` explains
  why the version was kept.
- Snapshot versions are immutable: there is deliberately **no** standalone
  `DROP SNAPSHOT`/`DROP VERSION` statement (`0A000`). Tag DDL is refused inside
  an explicit transaction block, like other DDL.
- RBAC/ACL visibility applies: a hidden table cannot be tagged or listed.

The same operations are available in Python:

```python
catalog.create_tag("events", "EOW-01")                 # freeze now + tag
catalog.create_tag("events", "v1", as_of=as_of_ms)      # freeze at a Unix-ms instant + tag
catalog.create_tag("events", "copy", snapshot=7)        # name an existing version
catalog.list_tags("events")
catalog.drop_tag("events", "v1")                        # tag only; snapshot stays
```

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
