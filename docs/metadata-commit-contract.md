# LakeSoul Metadata Commit Contract

Every metadata commit — whether issued by the JVM coordinator
(`DBManager.commitData` through the JDBC DAOs), the JVM adapter for the native
boundary (`DBManager.commitData` with `NATIVE_METADATA_UPDATE_ENABLED = true`),
or the direct Rust client (`MetaDataClient::commit_data`) — follows the same
partition-version and snapshot transition rules described here. The rules are
executed by one authoritative state machine per client boundary: Rust owns
conflict and retry semantics for the native path; the JVM coordinator owns them
for the JDBC path. The cross-language conformance suites verify that both
coordinators produce the same abstract state for the same commit trace.

## Outcomes

`commit_data` resolves into one of three outcomes:

| Outcome | Meaning |
|---|---|
| committed | At least one new partition version was published. |
| already committed | Every data commit carried by the request is already visible in the partition heads. |
| superseded | A concurrent update/compaction made the publication obsolete (compaction only). |

A conflict — a concurrent publication raced for the same partition version — is
raised as an explicit conflict result after the retry budget is exhausted. A
rolled-back or zero-row publication is never reported as success: the DAO layer
returns the published row count (`0` on conflict, never negative), and the
coordinator converts `0` into a conflict and rebuilds.

The JVM adapter maps the native outcome codes `1 = committed`, `2 = already
committed`, `3 = superseded`; conflicts surface as errors that carry the
message `Metadata commit conflict`.

## Partition version model

- A partition that has never been published is modeled as version `-1`
  (`PartitionInfo` row absent; JVM `getOrCreateCurPartitionInfo` seeds `-1` as
  well). The first published version is `0`; every publication writes
  `current version + 1`.
- Reads carry the partition head the caller observed; a commit whose read
  version is stale is resolved against the commits that landed in between, not
  silently overwritten.
- New partition and data-commit rows take the table's domain from its
  `table_info` row (JVM `getTableDomain`, native `get_table_domain`); tables
  created by path have no `table_name_id` row.

## Per-operation snapshot transitions

For each partition of a commit, the new head row is

```
version = current version + 1
commit_op = this operation
expression = caller's expression
snapshot = per operation below
```

| Operation | Snapshot rule |
|---|---|
| Append / Merge | current snapshot extended by the commit's data commits, unconditionally. The coordinator performs no head-op compatibility decision: the writer layer (Spark, Flink) owns table-type restrictions such as rejecting a second append on a hash-keyed table. Merge after append, append after delete, and append onto update/compaction heads are all legal here. |
| Update | replaces the snapshot with the commit's data commits. If the read version is stale: an intervening update, or a second compaction in the window, is a hard conflict (at any read version); a *single, deduplicated* compaction in the window is merged over; otherwise the snapshot is rebased over commits the caller could not observe. |
| Compaction | replaces the snapshot with the commit's data commits when the read version is current. If an update landed since the read, the compaction lives on and the partition is skipped (no new version). Otherwise the snapshot is rebased over unseen appends. |
| Delete | clears the snapshot and bumps the version. A delete for a partition whose current head is missing (or that the caller did not observe) is skipped entirely — the partition keeps its head, and no data commit of that delete is marked published. |

Skipping means the partition is not part of the publication, so the data
commits it carried stay uncommitted. Both coordinators apply exactly this
skip-aware commit-id set (`DBManager.compactionConflict` removes skipped
entries from `snapshotList`; the Rust coordinator collects commit ids only from
published partitions before sending them through the typed publication
request).

## Idempotency

Before planning, both coordinators check whether *every* data commit carried by
the request is already committed (`data_commit_info.committed = true`). The
check translates into `already committed` when

- the operation is an append or a merge (which can never re-assert a rewrite),
  or
- every partition of the request has a head newer than the version the caller
  read (a late replay; a request without read info counts as late). An update,
  compaction, or delete that reads the *current* version deliberately
  re-asserts a rewrite — e.g., the incremental-view maintenance baseline
  refresh — and is planned normally.

So an idempotent replay cannot mint a duplicate version, while an intentional
rewrite of a current head can.

## Publication protocol

One partition commit publishes the real partition rows and marks the data
commits they make visible in one PostgreSQL transaction, addressed by the typed
publication request

```
message PartitionCommitEntry {
  PartitionInfo partition_info = 1;
  repeated Uuid commit_id = 2;
}

message PartitionInfoCommit {
  repeated PartitionCommitEntry partitions = 1;
}
```

carried as `JniWrapper.partition_info_commit = 11`. Commit ids are scoped per
partition: Spark streaming (`getCommitIdByBatchIdAndQueryId`) and Flink
(`LakeSoulSinkCommitter`) reuse one commit id across every partition of a
batch, so a flat id list could not address rows of skipped partitions nor
publish a shared id once per partition. The former "append one synthetic empty
`PartitionInfo` and let the consumer pop it as a container" convention is
retired from the native DAO contract on both sides: a real partition can no
longer be mistaken for control data.

Per transaction:

1. Insert one `partition_info` row per real partition; any failure rolls the
   transaction back. A duplicate-key or serialization failure indicates a
   racing publication for the same partition version.
2. For each partition entry, for each of its commit ids, `update
   data_commit_info set committed = true where table_id = $1 and
   partition_desc = $2 and commit_id = $3` must update exactly one row;
   otherwise the transaction rolls back with an internal error (the pending
   commit row must have been staged before the publication, as every writer
   flow does).
3. On commit-phase races the transaction reports zero published rows so the
   coordinator rebuilds from the fresh partition heads.

## Table metadata mutations

`commit_data` applies the caller's table metadata (short name when present,
properties) exactly once per logical commit — and identically on both
coordinators (`DBManager.commitData` lines for `updateTableShortName` /
`updateTableProperties`, mirrored by
`MetaDataClient::update_table_short_name` / `update_table_properties`).

## Ownership and conformance

- `NATIVE_METADATA_UPDATE_ENABLED = true`: JVM is an adapter; the native
  metadata client runs the entire state machine (partition preconditions,
  snapshot transitions, retries within `MAX_COMMIT_ATTEMPTS = 5` with
  exponential backoff, table metadata mutations).
- `NATIVE_METADATA_UPDATE_ENABLED = false`: the JVM coordinator drives the
  JDBC DAOs; the retry budget is `DBConfig.MAX_COMMIT_ATTEMPTS = 5` after a
  `partition_info` insert conflict.
- The Rust FFI exposes `commit_data` with outcome encoding above; the JDBC DAOs
  keep the numeric published-row-count contract.

Same-trace conformance is executable:

- `rust/lakesoul-metadata/tests/commit_occ.rs` — direct Rust client: concurrent
  appends, compaction merging, update merging, stale update/compaction
  conflicts, idempotent replay, multi-partition publication.
- `lakesoul-common/src/test/java/.../CommitConformanceTest.java` — the same
  traces through the JVM coordinator with JDBC DAOs.
- `lakesoul-common/src/test/java/.../CommitNativeConformanceTest.java` — the
  same traces through the JVM adapter into the native coordinator, verified
  against JDBC-read state.
