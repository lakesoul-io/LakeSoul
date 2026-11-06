---
sidebar_position: 7
---

# PostgreSQL subscriptions

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`postgres-lakesoul` can consume a PostgreSQL logical replication publication and
write the changes into a pre-created LakeSoul CDC table. The subscription is
defined from the PostgreSQL endpoint: `CREATE SUBSCRIPTION` validates the source
and target, stores the definition and progress in the metadata PostgreSQL, and
starts a supervised local subscriber process.

This is logical replication into LakeSoul, not a PostgreSQL extension and not a
separate CDC cluster.

## At a glance

| Piece | Behaviour |
| --- | --- |
| Source | PostgreSQL 14+, logical WAL, one `public` table, `REPLICA IDENTITY FULL`, matching primary key |
| Target | Pre-created LakeSoul CDC table in the connection's database, unpartitioned, empty when the subscription is created |
| Initial data | Managed snapshot; the target stays empty until one atomic commit publishes the snapshot |
| Changes | `INSERT`, `UPDATE` and `DELETE`; full rows plus the CDC change column |
| Lifecycle | `CREATE`, `ALTER ... ENABLE/DISABLE`, `DROP`; SQL-created subscriptions resume after an endpoint restart |
| State | Subscription metadata and progress in the metadata PostgreSQL; data progress in target commit ids |

One subscription maps one source table to one target table. A target table is
reserved to one subscription.

## Source setup

Enable logical WAL on the source PostgreSQL:

```conf
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

Restart PostgreSQL after changing `wal_level`. Create the table, publication and
replica identity:

```sql
CREATE TABLE public.users (
  id         bigint PRIMARY KEY,
  name       text NOT NULL,
  amount     numeric(20,4),
  updated_at timestamptz
);

ALTER TABLE public.users REPLICA IDENTITY FULL;

CREATE PUBLICATION app_pub
  FOR TABLE public.users
  WITH (publish = 'insert,update,delete');
```

The source user needs `REPLICATION` and `SELECT` on the table. Use a dedicated
replication user with only the privileges it needs.

The source must be a primary. A hot standby cannot create the logical slot
this subscriber needs, and an independent standby snapshot cannot be handed
off to the primary slot without an LSN-alignment protocol.

The source table must be an ordinary table in the `public` schema. Set
`max_slot_wal_keep_size` to a value that fits the WAL the subscription may need
to retain, and run large initial copies outside peak hours. Snapshot readers
set `statement_timeout = 0`, `DateStyle = ISO`, `TimeZone = UTC` and
`bytea_output = hex` on their own connections.

## Target setup

Create the target table through the PostgreSQL endpoint. The database you
connect to is the target LakeSoul namespace:

```sql
CREATE TABLE users (
  id         bigint PRIMARY KEY,
  name       text NOT NULL,
  amount     numeric(20,4),
  updated_at timestamptz,
  "rowKinds" text
) WITH (
  use_cdc = true,
  lakesoul_cdc_change_column = 'rowKinds',
  hash_bucket_num = 4
);
```

The target's non-CDC columns must match the source columns by name and compatible
type, the primary key must match, and the table must not be range-partitioned.
The table must be empty when a SQL-created subscription is created. Keep the CDC
column out of the source; the subscriber fills it.

## Create a subscription

```sql
CREATE SUBSCRIPTION users_from_app
CONNECTION 'postgresql://user:pass@source:5432/app'
PUBLICATION app_pub
WITH (
  source_table         = 'public.users',
  target_table         = 'users',
  snapshot             = 'managed',
  slot_name            = 'users_from_app',
  primary_key          = 'id',
  commit_rows          = 50000,
  commit_interval_ms   = 1000,
  snapshot_batch_rows  = 50000,
  snapshot_batch_bytes = 67108864,
  snapshot_parallelism = 4,
  snapshot_memory_bytes = 1073741824
);
```

| Option | Default | Meaning |
| --- | --- | --- |
| `source_table` | required | Source table, `public.users` or `users` |
| `target_table` | required | Bare target table name in the current database |
| `snapshot` | `auto` | `auto` and `managed` copy the initial table; `none` skips the copy and starts from the slot's current position |
| `slot_name` | subscription name | Replication slot name; must not already exist |
| `primary_key` | inferred | Source/target primary-key columns; inferred from the target table when omitted |
| `commit_rows` | `50000` | Flush a streaming batch after this many distinct keys |
| `commit_interval_ms` | `1000` | Maximum delay before flushing a streaming batch |
| `snapshot_batch_rows` | `50000` | Rows per snapshot file batch |
| `snapshot_batch_bytes` | `67108864` | Bytes per snapshot file batch |
| `snapshot_parallelism` | `4` | Parallel COPY readers; every reader imports the same exported snapshot |
| `snapshot_memory_bytes` | `1073741824` | Process-wide DataFusion memory pool shared by snapshot writers; `0` is unbounded |

`CONNECTION` is an ordinary libpq-style connection string. The subscriber adds
the replication parameter internally.

The initial snapshot splits the source table into CTID ranges, runs one COPY
reader per range, and lets every reader write its own files. All readers import
the same exported snapshot, so the parallel copy still has one consistent point.
Writers share `snapshot_memory_bytes` instead of each taking an independent
budget. The target stays empty until all readers finish and one commit publishes
the complete snapshot.

`CREATE SUBSCRIPTION` validates the source and target before writing metadata. It
returns after the local subscriber process has started; the initial snapshot
continues in the background. A SQL-created subscription requires an empty target
for every snapshot mode. `snapshot = none` on an empty target starts from the
slot's current position and only applies subsequent changes.

The target namespace is the database in the connection, and the target table is
always `public.<table>`.

## Check progress

Inspect the subscription state in the metadata PostgreSQL:

```sql
-- Run against the metadata database, not the endpoint.
SELECT subscription_id, state, last_error
FROM subscription.subscriptions;

SELECT subscription_id, snapshot_state, snapshot_lsn, data_lsn, state
FROM subscription.tables;
```

`state` moves through `starting`, `snapshotting`, `streaming`, `stopped`,
`disabled` and `error`. `snapshot_state` is `pending`, `copying`, `done` or
`failed`.

The subscriber process writes to a per-subscription log. Configure the log
directory with `--subscription-log-dir` when starting the endpoint; if it is not
set, `LAKESOUL_LOG_DIR` is used, otherwise a directory under the system temporary
directory is used.

## Lifecycle

```sql
ALTER SUBSCRIPTION users_from_app DISABLE;
ALTER SUBSCRIPTION users_from_app ENABLE;

DROP SUBSCRIPTION users_from_app;
DROP SUBSCRIPTION IF EXISTS users_from_app;
```

- `DISABLE` stops the subscriber process but keeps its replication slot. A long
  pause therefore retains WAL on the source.
- `ENABLE` starts the process again from the persisted watermark.
- `DROP` stops the process, drops the source replication slot, and removes the
  subscription metadata.
- SQL-created subscriptions that are enabled are started again when the endpoint
  starts.
- A child that exits unexpectedly is recorded as `error`; it is not retried in a
  loop. `ENABLE` or an endpoint restart starts it again.

A subscription created with the CLI is not auto-started by the endpoint, so a
manually launched subscriber is not duplicated.

## How it works

1. `CREATE SUBSCRIPTION` validates the source publication, source table,
   `REPLICA IDENTITY FULL`, primary key and column types, then validates the
   target CDC table.
2. The definition and zero progress are written to
   `subscription.subscriptions`, `subscription.tables` and
   `subscription.progress`.
3. The endpoint starts a local child process:

   ```text
   postgres-lakesoul --subscribe --subscription-id <name>
   ```

4. For `auto` and `managed`, the child opens the slot with an exported snapshot,
   splits the table into CTID ranges, and runs one COPY reader per range. Every
   reader imports the same snapshot and writes files in batches; the writers
   share one bounded memory pool. After every reader finishes, all files are
   published in one atomic LakeSoul commit at the consistent point. Readers
   therefore see either an empty target or the complete snapshot.
5. After the handoff, changes are accumulated per source transaction. A batch is
   written and published with one LakeSoul commit. The commit id records the
   source LSN, so replaying from an older slot position skips data that was
   already applied.
6. The metadata progress additionally records transactions that touched no
   subscribed table.

## Source types

The subscriber supports these PostgreSQL source types:

`boolean`, `smallint`, `integer`, `bigint`, `real`, `double precision`,
`numeric(p,s)`, `text`, `varchar`, `char`, `name`, `bytea`, `date`, `time`,
`timestamp`, `timestamptz`, `uuid`, `json` and `jsonb`.

Target columns use the matching LakeSoul/Arrow types and the same column names.
The CDC change column is an extra target column.

## CLI mode

For manual operation, the same subscriber can be started directly:

```bash
postgres-lakesoul --subscribe \
  --source-url 'postgresql://user:pass@source:5432/app' \
  --publication app_pub \
  --slot users_from_app \
  --source-table public.users \
  --target-table users \
  --target-namespace default \
  --primary-key id \
  --snapshot managed \
  --snapshot-parallelism 4 \
  --snapshot-memory-bytes 1073741824
```

`--snapshot` accepts `auto`, `managed` and `none`. `--snapshot-parallelism`
sets the number of COPY readers, and `--snapshot-memory-bytes` bounds the
DataFusion pool shared by their writers (`0` is unbounded). `--dry-run` validates the
source, target and types without creating a slot or writing subscription
metadata. CLI-created subscriptions are stored in the metadata database but are
not auto-started by the endpoint.

## Security note

For SQL-created subscriptions, the source connection string, including its
credentials, is stored in the metadata PostgreSQL so the supervisor can restart
the child. Protect the metadata database and its backups, and use a
least-privilege source user.

## Next

- [Quick start: the PostgreSQL endpoint](../../get-started/quickstart-postgres.md)
- [DDL](ddl.md)
- [Insert and upsert](insert-and-upsert.md)
- [Operations](operations.md)
- [Metadata store](../../operations/metadata-postgres.md)
