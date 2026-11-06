---
sidebar_position: 2
sidebar_label: Architecture & format
---

# Architecture and table format

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

This page is about the layer below the SQL surface: how commits, metadata and
data files are arranged so that one table supports row-level updates, incremental
reads, incremental compute and search. For the 30-second overview, see
[Architecture](../get-started/architecture.md).

## Design goals

- **Object storage is the source of truth.** Data is durable and cheap there;
  compute is stateless and can be replaced without moving files.
- **Row-level changes do not rewrite the table.** An update writes a delta file,
  not a new copy of the table.
- **The commit log is the changelog.** Incremental reads and incremental compute
  consume the same commits that made the data visible.
- **Indexes live with the data.** Vector and full-text indexes are part of the
  table lifecycle, not a second system to synchronize.
- **The physical format is a choice.** A table can move between formats without
  rewriting history.

## Three planes

```
┌──────────────────────────────────────┐
│ Control plane — PostgreSQL           │
│ catalog · partition versions         │
│ data commits · snapshots · indexes   │
└──────────────────┬───────────────────┘
                   │ commit / resolve
┌──────────────────▼───────────────────┐        ┌──────────────────────────┐
│ Data plane — object storage          │◄───────│ Compute plane            │
│ base + delta files                   │  read  │ DataFusion · PG frontend │
│ vector + text index segments         │        │ Python SDK · connectors  │
└──────────────────────────────────────┘        └──────────────────────────┘
```

The control plane is a database, not a set of manifest files. That choice is
what makes partition-version conflict detection, row-level merge-on-read and
index leases possible; it also means PostgreSQL is part of the deployment.

## Metadata model

| Table | Responsibility |
| --- | --- |
| `namespace` | Namespace registry |
| `table_info` | Table path, Arrow schema, partition columns, properties |
| `table_name_id` / `table_path_id` | Name and path lookup to `table_id` |
| `partition_info` | Per-partition version, snapshot (`commit_id[]`), commit op |
| `data_commit_info` | File operations of one commit, and whether it is published |
| `table_snapshot` / `snapshot_commit` / `table_snapshot_tag` | Table-level snapshots and tags |
| `index_shard` / `index_commit` / `index_lease` | Secondary-index control plane |

Three concepts carry most of the design:

- **Partition version.** A partition is the unit of concurrency. `partition_info`
  stores a monotonically increasing `version` per partition; a commit compares
  the version it read with the current head.
- **Snapshot.** `partition_info.snapshot` is an array of `commit_id`s. A read
  resolves exactly that set of commits, not “the latest files”.
- **Commit.** `data_commit_info` records the `file_ops` of a commit and a
  `committed` flag. A commit becomes visible when its partition publishes a new
  version and marks its data commits published in the same transaction.

## Commit protocol

```
writer:
  write immutable files ──► object storage
  commit_data(partition, read_version, file_ops, commit_op)
        │
        ▼
  PostgreSQL transaction:
    insert partition_info (version + 1, snapshot = commit ids)
    update data_commit_info set committed = true
        │
        ▼
  duplicate key / stale version = conflict ──► resolve, retry or fail
```

The new partition version and the data commits it references are published in
**one PostgreSQL transaction**. A racing writer loses on the partition-version
unique key, not on a table-wide lock. A replay whose commits are already
published returns `already committed`; a stale read is resolved against the
commits that landed in between.

Snapshot transitions differ by operation:

| Operation | Snapshot rule |
| --- | --- |
| Append / merge | Extend the current snapshot with the new commit ids |
| Update | Replace the snapshot; a stale read is rebased or reported as a conflict |
| Compaction | Replace the snapshot when the read is current; skip the partition if an update landed |
| Delete | Clear the snapshot and bump the version |

Compaction is triggered by a PostgreSQL `NOTIFY` when a partition has accumulated
enough delta versions; readers do not wait for it.

## Table format

### Base files and delta files

A primary-key write appends **delta files** and records them as `add` operations
in `data_commit_info.file_ops`. A delete records a `delete` operation, or a
tombstone row in the CDC change column. A read collects the file operations of
its snapshot and merges them by primary key: the latest commit wins, and
tombstones disappear after the merge.

`file_exist_cols` records which columns a file actually contains. That is what
makes **partial-column upserts** possible: a delta that carries only `(id, score)`
does not overwrite `name` from an older file; a missing column is “not
contributed”, not NULL.

`compaction` merges delta files into larger base files in size tiers and publishes
a `CompactionCommit`. It reduces read amplification without blocking writers or
readers.

### Primary key, partitions and CDC

- **Hash buckets.** `hash_bucket_num` splits a primary-key table into buckets;
  each bucket is an independent merge unit.
- **Range partitions.** `PARTITIONED BY` adds a range dimension above the hash
  buckets. Reads prune partitions and buckets; a write touches one partition.
- **CDC change column.** `rowKinds` (or a column named by
  `lakesoul_cdc_change_column`) records insert/delete semantics. It is also what
  lets a target table retract rows in incremental SQL.

### Pluggable physical formats

`file_format` is a **per-write** choice, not a table-wide lock:

| Value | Use it when |
| --- | --- |
| `parquet` | Interoperability, rollback, or an external reader is required |
| `vortex` | Read performance and random row access matter most |
| `vortex-compact` | Default; smallest files, with Zstd/PCodec-class encodings |

One snapshot can mix Parquet and Vortex files. Readers group files by format and
build one logical scan; merge-on-read, primary-key resolution, partition pruning
and snapshot semantics are format-independent. See
[Physical file formats](../operations/physical-file-formats.md) for the full
matrix and the upgrade boundary.

### Vortex: random access and multimodal data

Vortex is a compressed columnar format that operates directly on encoded arrays,
with projection and predicate pushdown. Ordinary Vortex tables keep the Vortex
writer defaults: 8192-row blocks and a 1 MiB data-block target, which preserves
compression. The columns declared in a table's `vector_index_columns` property
instead default to 1024-row blocks and a 128 KiB target, because a vector search
fetches a scattered candidate set from those wide columns. That layout is per
column and configurable per table; the other columns keep the writer defaults.

Vortex also carries large binary payloads. Video frames, robot episodes and
tensors live as blob columns beside their metadata; `BlobRef` and
`materialize_blob` keep the reference and the payload in the same snapshot. An
embodied dataset does not need a second object store for media and a third system
for metadata.

### Primary-key locator: point lookups

`pk = value` and `pk IN (...)` do not scan the table. Each data file has a
**sorted per-file primary-key index**:

- keys are encoded with `arrow-row` (the same order-preserving encoding the
  writer uses), so a lookup returns a contiguous row range;
- the index is built lazily and stored in the object-store disk cache as an
  `mmap`ed file, shared by queries in the process;
- data files are immutable: a rewrite or compaction produces a new file and a
  new locator, so entries never go stale;
- a composite key supports a **prefix** lookup (`g = 'x'` on `(g, value)`).

```
pk = 42
  ├─ per-file sorted PK index (mmap, object-store cache)
  ├─ candidate row ranges
  │    ├─ Vortex: read the rows directly by row index
  │    └─ Parquet: row selection + page index, skip unrelated pages
  └─ merge-on-read over those rows only, latest version wins
```

The locator deliberately returns a **superset** of matching rows: it uses key
prefixes only, because dropping a payload-matching row could drop the latest
version of a key during merge-on-read. The correctness of the merge does not
depend on the locator being exact.

Both Vortex and Parquet are supported; Vortex reads are direct row-index reads,
while Parquet uses row selection and the page index. This is the difference
between “scan every delta file and merge” and “locate the rows, then merge”.

## Read paths

| Path | Resolution | Used by |
| --- | --- | --- |
| Snapshot read | `partition_info.snapshot` → `data_commit_info.file_ops` → merge-on-read | SQL queries, time travel |
| Incremental read | Commit range in `data_commit_info` → changelog | Spark/Flink incremental query, incremental SQL |
| Index scan | `index_commit` generation/version → index segments | Vector and full-text search |

## Secondary indexes as part of the format

The index control plane shares the metadata database:

- `index_shard` identifies a shard by `(kind, index_prefix)`;
- `index_commit` stores an immutable `(generation, version)` with the index
  header and segment list; a full rebuild publishes a new generation, an
  incremental flush publishes `version + 1`;
- `index_lease` keeps a generation alive while a reader loads it, so garbage
  collection cannot delete a file in use.

Index data lives in object storage next to the table. Vector indexes are
IVF + RaBitQ; text indexes are Tantivy BM25. Both are maintained after the data
commit, and both are queried by the same engine that reads the table. See
[Vector index management](vector-search/index-management.md) and
[Text index management](full-text-search/index-management.md).

## Incremental compute on the commit log

A table's commit sequence is already a changelog. Incremental SQL keeps a cursor
per target, consumes only the commits after it, detects the partitions that
changed, and writes only the affected rows. The same commits that made the data
visible to a snapshot read are the input to the incremental computation; there is
no separate CDC pipeline for writes that went through the engine.

See [Incremental SQL](incremental-sql/overview.md).

## Why not just Iceberg?

Iceberg is an open specification for table layout: manifests, snapshots,
partition evolution and broad engine interoperability. It is a good solution to
that problem, and LakeSoul does not try to replace the specification.

LakeSoul is solving a different layer: an engine that runs on object storage and
needs row-level updates, a consumable changelog, indexes and a PostgreSQL
endpoint in the same system. Put differently:

| | Iceberg | LakeSoul |
| --- | --- | --- |
| Metadata | Manifest and snapshot files | Partition versions and commit rows in PostgreSQL |
| Row-level update | Delete files, merged by an external engine | Primary-key delta files + merge-on-read |
| Incremental changelog | Snapshot diff computed by an external engine | Commit log consumed directly |
| Secondary index | External system | Index catalog in the engine |
| Physical format | Parquet / ORC / Avro | Parquet / Vortex / Vortex Compact, per write |
| Multimodal payload | Binary columns; no blob semantics | Vortex blob columns + `BlobRef` |
| Point lookup | Delete files + engine scan; no primary-key locator | Per-file sorted locator, direct row ranges |
| Commit concurrency | Catalog / object-store coordination | Partition-version CAS in a PostgreSQL transaction |

These capabilities are not part of the Iceberg specification. You can build them
around Iceberg — delete files, an external index service, a CDC pipeline and a
separate vector database — but then you are operating several systems and
reconciling them yourself. LakeSoul makes them one design.

**Iceberg is the better choice** when you need a passive table format that many
engines can read without adopting a particular engine. **LakeSoul is the better
choice** when you need an engine that performs row-level updates, incremental
compute and search on object storage, and you are willing to run its catalog.

The cost is real: the catalog depends on PostgreSQL, reads need the LakeSoul
engine or its native reader, the format is not a public specification, and Vortex
is newer than Parquet with fewer external readers. Those are deliberate
trade-offs, not omissions.

## Design boundaries

- Metadata is served from PostgreSQL; the catalog is part of the deployment.
- External catalog interoperability is on the roadmap; today, other engines read
  through LakeSoul's connectors or native IO.
- Vortex is the default for new writes, but Parquet remains the interoperability
  and rollback path. LakeSoul 3.x cannot read Vortex; see the
  [compatibility reference](../reference/compatibility.md).
- Incremental SQL, vector search and full-text search have their own limits;
  this page describes the shared storage design, not every feature boundary.

## Next

- [Engine overview](overview.md)
- [PostgreSQL front end](postgres/overview.md)
- [Incremental SQL](incremental-sql/overview.md)
- [Vector search](vector-search/overview.md)
- [Physical file formats](../operations/physical-file-formats.md)
