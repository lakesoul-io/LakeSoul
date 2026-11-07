# PostgreSQL logical-replication subscriber (P0)

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`postgres-lakesoul --subscribe` consumes a PostgreSQL publication and writes
the changes into a pre-created LakeSoul CDC table. This is a P0 for validating
the WAL → Arrow → LakeSoul batch path and its replay semantics, not a complete
PostgreSQL subscription implementation.

## Scope

| Dimension | P0 behaviour |
| --- | --- |
| Process | One subscriber process per subscription; a subscription maps one or more source tables |
| Source | PostgreSQL 14, `public` schema only |
| Target | Pre-created LakeSoul tables; no auto-create, no schema evolution; namespace defaults to the session database |
| Target partitioning | Unpartitioned (hash buckets only) |
| Replica identity | `REPLICA IDENTITY FULL` required |
| Writes | INSERT / UPDATE / DELETE; full rows plus `rowKinds` |
| TOAST | Supported; unchanged values in an UPDATE are filled from the full old tuple |
| Initial load | Managed snapshot per table; each table commits independently |
| pg_dump | Next phase; schema fields are reserved, implementation is not present |
| TRUNCATE | Rejected at startup and at runtime |
| SQL surface | CLI plus `CREATE/DROP/ALTER SUBSCRIPTION` (single `source_table`/`target_table` or `TABLES (...)`) on the PG wire server |
| Publishing | Not supported |
| Transactions | One LakeSoul commit per table per batch; no cross-table atomicity |
| Concurrency | One writer assumed; no lease |

### Why FULL

With `REPLICA IDENTITY FULL`, DELETE and key-changing UPDATE carry a complete
old row, including unchanged TOAST values, so the sink can write a full delete
tombstone without padding NULLs or splitting the writer schema. `DEFAULT` would
only carry the old key and cannot supply unchanged TOAST values.

### Unchanged TOAST

With `REPLICA IDENTITY FULL`, PostgreSQL logs the complete old row
(`toast_flatten_tuple`) and the subscriber can recover unchanged TOAST values.
The new tuple still sends `'u'` for an externally stored value that did not
change; `pgoutput` does not repeat the payload. `RowData` drops the `u` column,
so `parse_update` starts from the old tuple's full values and overwrites only
the columns the new tuple actually carries. A missing column is accepted only
when the source column is TOASTable; any other missing column is a protocol or
schema error. INSERT and DELETE still require complete tuples.

This is the same trade-off Debezium documents: `REPLICA IDENTITY DEFAULT`
cannot supply unchanged TOAST values and needs a placeholder or a racy
out-of-band reselect; `FULL` avoids both at the cost of larger WAL records.

## Source setup

The source needs logical WAL and a publication that does not publish truncate:

```sql
-- postgresql.conf / ALTER SYSTEM; restart afterwards
-- wal_level = logical
-- max_replication_slots >= 1
-- max_wal_senders >= 1
-- max_slot_wal_keep_size = '1GB'   -- protect the source disk

CREATE TABLE public.users (
  id bigint PRIMARY KEY,
  name text NOT NULL,
  amount numeric(20,4),
  updated_at timestamptz
);

ALTER TABLE public.users REPLICA IDENTITY FULL;

CREATE PUBLICATION lakesoul_pub
  FOR TABLE public.users
  WITH (publish = 'insert,update,delete');
```

The source user needs `REPLICATION` plus `SELECT` on the table.

## Target setup

The target namespace must already exist. A target must be empty for a managed
snapshot; `snapshot = none` accepts a pre-initialized target. Create it through
the PostgreSQL front end:

```sql
CREATE TABLE users (
  id bigint PRIMARY KEY,
  name text NOT NULL,
  amount numeric(20,4),
  updated_at timestamptz,
  "rowKinds" text
) WITH (
  use_cdc = true,
  lakesoul_cdc_change_column = 'rowKinds',
  hash_bucket_num = '4'
);
```

The target's non-CDC columns must match the source by name and compatible
type, the primary key must match, and the table must not be range-partitioned.

## Run

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test

postgres-lakesoul --subscribe \
  --source-url 'postgresql://lakesoul_test:lakesoul_test@127.0.0.1:5432/pg_source' \
  --publication lakesoul_pub \
  --slot lakesoul_slot \
  --source-table public.users \
  --target-table users \
  --target-namespace pg_source \
  --primary-key id \
  --commit-rows 50000 \
  --commit-interval-ms 1000 \
  --snapshot auto \
  --snapshot-batch-rows 50000 \
  --snapshot-batch-bytes 67108864 \
  --snapshot-parallelism 4 \
  --snapshot-memory-bytes 1073741824
```

Multi-table streaming uses repeated `--table` mappings. Managed snapshots are
supported per table; all targets must be empty when the snapshot starts:

```bash
postgres-lakesoul --subscribe \
  --source-url 'postgresql://lakesoul_test:lakesoul_test@127.0.0.1:5432/pg_source' \
  --publication lakesoul_pub \
  --slot lakesoul_multi \
  --target-namespace pg_source \
  --table public.users=users \
  --table public.orders=analytics.orders \
  --snapshot managed
```

`--dry-run` validates the source, target and types and prints the plan without
creating a slot or touching the subscription metadata. `--max-transactions N`
stops after N new source transactions (useful for tests). When running next to
the wire server, pass a distinct `--metrics-addr`; both processes otherwise
default to `127.0.0.1:19090`.

## SQL-managed subscriptions

The wire server can own the subscriber process. A target namespace defaults to
the database the client connected to; `namespace.table` selects another
LakeSoul namespace. Target tables must already exist.

```sql
CREATE SUBSCRIPTION sub_users
CONNECTION 'postgresql://user:pass@source:5432/app'
PUBLICATION app_pub
WITH (
  source_table        = 'public.users',
  target_table        = 'users',
  snapshot            = 'managed',   -- auto | managed | none
  slot_name           = 'sub_users', -- optional, defaults to the subscription name
  primary_key         = 'id',        -- optional, inferred from the target table
  commit_rows         = 50000,
  commit_interval_ms  = 1000,
  snapshot_batch_rows = 50000,
  snapshot_batch_bytes = 67108864,
  snapshot_parallelism = 4,
  snapshot_memory_bytes = 1073741824
);

ALTER SUBSCRIPTION sub_users DISABLE;
ALTER SUBSCRIPTION sub_users ENABLE;
DROP SUBSCRIPTION [IF EXISTS] sub_users;
```

`source_table`, `target_table`, `publication` and `CONNECTION` are required for
the single-table form. `snapshot` defaults to `auto`; a managed snapshot
requires an empty target. Multi-table form:

```sql
CREATE SUBSCRIPTION app_all
CONNECTION 'postgresql://user:pass@source:5432/app'
PUBLICATION app_pub
TABLES (
  public.users    TO users,
  public.orders   TO analytics.orders
)
WITH (
  snapshot = 'managed',
  commit_rows = 50000
);
```

Multi-table streaming uses one slot and one process. Each table commits
independently; there is no cross-table atomicity. Managed snapshots run per
table from a temporary slot, so each table has its own snapshot LSN and a
completed table is not copied again after a restart. Leave one extra slot in
`max_replication_slots` for the per-table temporary slot. The server validates
every source, target, primary key, publication and source slot before writing
metadata. It then starts a child process:

```text
postgres-lakesoul --subscribe --subscription-id <name> --no-metrics
```

The child loads the definition from the metadata PostgreSQL and runs the same
snapshot/streaming state machine as the CLI. `ALTER DISABLE` stops the child but
keeps its slot (long pauses therefore retain WAL), `ENABLE` starts it again from
the persisted watermark, and `DROP` stops the child, drops the source
replication slot, and removes the metadata rows. The server resumes enabled
SQL-created subscriptions at startup (CLI-created rows are not auto-started,
so a manually launched subscriber is never duplicated) and stops its children
on SIGINT/SIGTERM; on Linux a parent-death signal prevents orphans if the
server is killed abruptly. A child that crashes is recorded as `error`; it is
not retried in a loop, so `ENABLE` or a server restart starts it again.

Subscription DDL is accepted through both the simple and extended query
protocols. It cannot be combined with other statements in one simple-query
batch and cannot run inside an explicit transaction. `ALTER`/`DROP` only
operate on SQL-created subscriptions; rows created by the CLI remain managed
manually.

The source connection string is stored in
`subscription.subscriptions.source_url` so a later server restart can start the
child without repeating the command line. It contains source credentials;
protect the metadata database accordingly.

## Initial snapshot

`--snapshot` selects the initial-load strategy:

- `auto` (default): run a managed snapshot when the target is empty and no
  completed snapshot exists; otherwise resume streaming.
- `managed`: require/perform the managed snapshot; a non-empty target without a
  completed snapshot is rejected.
- `none`: do not copy initial data. The target is assumed to be initialized
  already (or intentionally should contain only future changes).

The managed snapshot uses pg-walstream's exported snapshot:

1. create the slot with `EXPORT_SNAPSHOT` and get the consistent point `S`;
2. split the table into `--snapshot-parallelism` CTID ranges (TID Range Scans
   on PostgreSQL 14+); every reader imports the same exported snapshot;
3. each reader runs `COPY`, parses rows into the same Arrow schema, and writes
   files in batches (`--snapshot-batch-rows`, default 50,000, or
   `--snapshot-batch-bytes`, default 64MB), but does **not** commit them;
4. all writers share one DataFusion memory pool bounded by
   `--snapshot-memory-bytes` (default 1 GiB; 0 is unbounded);
5. after every reader is done, hand the stream back and commit every snapshot
   file with one `commit_data_commit_infos` call and `commit_id(S)`;
6. update `data_lsn = S`, `processed_lsn = S`, `snapshot_state = done`, then
   start streaming from `S`.

The target table is therefore empty to readers until the final commit, and a
failed snapshot leaves an empty target (only unreferenced files remain). If the
copy or commit fails, the next `auto`/`managed` start drops the stale slot and
copies again. If the process crashes after the final commit but before
`snapshot_state=done` is persisted, the next start recognizes
`target commit watermark == snapshot_lsn` and marks the snapshot done instead of
copying again.

The source's COPY holds an exported REPEATABLE READ snapshot: it pins `xmin`
and holds `ACCESS SHARE` locks for the duration. Run large snapshots off-peak
and set `max_slot_wal_keep_size` high enough for the WAL generated during the
copy. Source session settings `statement_timeout=0`, `DateStyle=ISO`,
`TimeZone=UTC` and `bytea_output=hex` are set by each snapshot reader.

## Subscription state

The definition and progress live in the metadata PostgreSQL under the
`subscription` schema. Production deployments apply
`script/metadata-migrations/V4000002__subscription.sql`; the worker also runs
the same idempotent DDL at startup for local development and tests. This is a
pre-release schema: a database created before the latest columns were added
must drop and recreate the `subscription` schema; no migration compatibility is
provided.

| Table | Purpose |
| --- | --- |
| `subscription.subscriptions` | slot, publication, source URL, state (`starting`/`snapshotting`/`streaming`/`stopped`/`disabled`/`error`), `enabled`, batch settings, supervisor pid/log, last error |
| `subscription.tables` | source table + relation OID, target namespace/table + table id, primary key, `data_lsn`, `snapshot_state`, `snapshot_mode`, `snapshot_lsn`, snapshot timestamps |
| `subscription.progress` | subscription-level `processed_lsn`, including transactions that touched no mapped table |

The target table's commit ids remain the authoritative data watermark; the
metadata rows add identity checks and record idle progress. `data_lsn` is
updated after the data commit, then `processed_lsn`, and only then is the slot
acked. If the process crashes between the data commit and the progress update,
the target commit ids still cover the data; if the metadata is restored older
than the target, the target commit watermark wins. If the target is restored
older than `tables.data_lsn`, startup fails instead of silently skipping data.

A different `--source-table`, recreated source table (relation OID), or
recreated target table (LakeSoul table id) is rejected. Both CLI and
SQL-created subscriptions store the source URL in
`subscription.subscriptions.source_url`; the supervisor uses it to restart a
child without repeating the command line.

## Batch, commit and replay

- Changes are accumulated per source transaction. A batch keeps at most one row
  per primary key; the last change wins.
- A streaming batch flushes when `commit_rows` keys are pending or its estimated
  buffered values reach 64 MiB, whichever comes first. The byte cap keeps a
  batch of large TOAST values from pinning unbounded memory.
- A batch is published with one `commit_data_commit_infos` call per table. The
  commit id is `high = 0x4c53504700000000`, `low = last commit_lsn` in the batch.
- On startup, the latest partition snapshot of the target is scanned for that
  magic; its maximum `low` is the durable watermark. Transactions at or below
  the watermark are skipped, so replaying from an older slot position is a
  no-op for already-applied data.
- `subscription.progress.processed_lsn` additionally records progress through
  transactions that touch no subscribed table.
- When no managed snapshot runs (for example `--snapshot none`), the initial
  slot is created with `snapshot = nothing`; the subscriber starts from the
  slot's current position. The managed snapshot uses `EXPORT_SNAPSHOT` as
  described above.

## Verified P0 behaviour

The local PostgreSQL 14 smoke test covered:

- insert, update of ordinary columns, update of the primary key (old key
  tombstone + new key row), delete, delete + reinsert;
- batching several source transactions into one LakeSoul commit;
- `kill -9` mid-stream, restart from the metadata progress, and final state
  equality with the source;
- restart with metadata progress behind the target commit watermark, using the
  target commit-id watermark;
- recreated source/target table and mismatched state identity rejected;
- managed snapshot of 50,000 rows: the target stayed empty during the copy and
  exposed all 50,000 rows only after the atomic final commit;
- insert/update/delete issued while the snapshot was copying were applied after
  the handoff, with final target/source equality;
- `kill -9` during the snapshot left the target empty; restart dropped the stale
  slot and copied again, including the concurrent changes;
- a non-empty target was rejected by a new `managed` subscription; changing an
  existing subscription from `managed` to `none` was rejected;
- `--snapshot none` started from the slot's current position and captured only
  subsequent changes;
- `CREATE SUBSCRIPTION` through psql (simple query) and psycopg (extended
  query), including the managed snapshot, metadata row and child process;
- `ALTER SUBSCRIPTION DISABLE/ENABLE` stopped and restarted the child without
  losing the watermark, and a server SIGTERM stopped the child cleanly;
- `DROP SUBSCRIPTION` stopped the child, dropped the slot and removed the
  metadata;
- duplicate subscription, transaction-block, mixed-batch and non-empty-target
  requests were rejected with their PostgreSQL SQLSTATEs;
- TOAST merge: a 20,000-byte externally stored `payload` followed by an update
  to a different column keeps the payload; updating the payload, changing the
  primary key, and deleting the row also round-trip correctly;
- startup rejection of a publication that publishes truncate and of a source
  table without `REPLICA IDENTITY FULL`.

## Known limitations

- `rowKinds` is currently visible in `SELECT *` and in the PG catalog.
- `pg_walstream` is pinned to `0.10.0`; minor releases have changed public APIs.
  tokio is 1.54.1 and pingora is 0.9.1, which contains the zero-capacity
  `BufReader` fix for tokio 1.54.
- A subscription can map multiple source tables (one slot/process). Each table
  has its own snapshot/commit; there is no cross-table atomicity. The publication
  may contain unmapped tables, whose events are ignored.
- pgdump mode is reserved but not implemented; the managed snapshot is
  parallel across CTID ranges but still holds one exported snapshot (xmin and
  ACCESS SHARE locks) for the duration.
- SQL-created subscriptions assume one server instance; there is no cross-node
  lease. The source URL is plaintext in the metadata database.
- No DDL/schema evolution, no truncate, no range partitioning, no cross-table
  atomic commit, no HA lease.
- Binary pgoutput, arrays/enums/domains/intervals, and PG 15+ column lists/row
  filters are not covered.
