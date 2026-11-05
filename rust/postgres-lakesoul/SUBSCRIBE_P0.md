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
| Process | One subscriber process, one source table per process |
| Source | PostgreSQL 14, `public` schema only |
| Target | Pre-created LakeSoul table; no auto-create, no schema evolution |
| Target partitioning | Unpartitioned (hash buckets only) |
| Replica identity | `REPLICA IDENTITY FULL` required |
| Writes | INSERT / UPDATE / DELETE; full rows plus `rowKinds` |
| TOAST | Not supported; an unchanged-TOAST marker (`u`) stops the worker |
| Initial load | Managed snapshot; target stays empty until one atomic final commit |
| pg_dump | Next phase; schema fields are reserved, implementation is not present |
| TRUNCATE | Rejected at startup and at runtime |
| SQL surface | CLI only; no `CREATE SUBSCRIPTION` |
| Publishing | Not supported |
| Transactions | One LakeSoul commit per table per batch; no cross-table atomicity |
| Concurrency | One writer assumed; no lease |

### Why FULL

With `REPLICA IDENTITY FULL`, DELETE and key-changing UPDATE carry a complete
non-TOAST old row, so the sink can write a full delete tombstone without padding
NULLs or splitting the writer schema. `DEFAULT` would only carry the old key.

### Why `u` is an error

`REPLICA IDENTITY FULL` does **not** make TOAST values available. PostgreSQL 14's
`logicalrep_write_tuple()` writes `'u'` for every externally stored TOAST datum
in both the old and new tuple; `ReorderBufferToastReplace()` only fills the new
tuple when the TOAST chunks arrived in the same transaction. A missing column in
`RowData` is therefore an unchanged TOAST value, and P0 fails instead of writing
NULL over the existing value. Partial-column writes (or a read-modify-write per
key) are the next phase.

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

The target namespace must already exist and the table must be empty when the
slot is created. Create it through the PostgreSQL front end:

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
  --snapshot-batch-bytes 67108864
```

`--dry-run` validates the source, target and types and prints the plan without
creating a slot or touching the subscription metadata. `--max-transactions N`
stops after N new source transactions (useful for tests). When running next to
the wire server, pass a distinct `--metrics-addr`; both processes otherwise
default to `127.0.0.1:19090`.

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
2. `COPY` the published table and parse rows into the same Arrow schema;
3. write files in batches (`--snapshot-batch-rows`, default 50,000, or
   `--snapshot-batch-bytes`, default 64MB) but do **not** commit them;
4. after the copy is complete, hand the stream back and commit every snapshot
   file with one `commit_data_commit_infos` call and `commit_id(S)`;
5. update `data_lsn = S`, `processed_lsn = S`, `snapshot_state = done`, then
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
`TimeZone=UTC` and `bytea_output=hex` are currently operator requirements.

## Subscription state

The definition and progress live in the metadata PostgreSQL under the
`subscription` schema. Production deployments apply
`script/metadata-migrations/V4000002__subscription.sql`; the worker also runs
the same idempotent DDL at startup for local development and tests.

| Table | Purpose |
| --- | --- |
| `subscription.subscriptions` | slot, publication, state (`init`/`streaming`/`stopped`/`error`), last error |
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
recreated target table (LakeSoul table id) is rejected. The source URL and its
password are not stored in the metadata; they remain CLI/environment input.

## Batch, commit and replay

- Changes are accumulated per source transaction. A batch keeps at most one row
  per primary key; the last change wins.
- A batch is published with one `commit_data_commit_infos` call per table. The
  commit id is `high = 0x4c53504700000000`, `low = last commit_lsn` in the batch.
- On startup, the latest partition snapshot of the target is scanned for that
  magic; its maximum `low` is the durable watermark. Transactions at or below
  the watermark are skipped, so replaying from an older slot position is a
  no-op for already-applied data.
- `subscription.progress.processed_lsn` additionally records progress through
  transactions that touch no subscribed table.
- The initial slot is created with `snapshot = nothing`; the subscriber starts
  from the slot's current position.

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
- TOAST guard: a 9,600-byte incompressible `payload` followed by an update to a
  different column stops the worker with the `u` error instead of writing NULL;
- startup rejection of a publication that publishes truncate and of a source
  table without `REPLICA IDENTITY FULL`.

## Known limitations

- `rowKinds` is currently visible in `SELECT *` and in the PG catalog.
- `pg_walstream` is pinned to `0.10.0`; minor releases have changed public APIs.
  tokio is 1.54.1 and pingora is 0.9.1, which contains the zero-capacity
  `BufReader` fix for tokio 1.54.
- Only one source table per process; the publication may contain other tables,
  whose events are ignored.
- pgdump mode is reserved but not implemented; the managed snapshot is
  single-threaded and holds the exported snapshot (xmin and ACCESS SHARE locks).
- No DDL/schema evolution, no truncate, no range partitioning, no multi-table
  coordination, no HA lease.
- Binary pgoutput, arrays/enums/domains/intervals, and PG 15+ column lists/row
  filters are not covered.
