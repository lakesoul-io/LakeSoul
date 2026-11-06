# LakeSoul 4.0.0 Release Notes

LakeSoul `4.0.0` is a major Core release. It changes the default physical file
format, metadata representation, connector coordinates, native ABI, runtime
baselines, and several user-visible semantics. Read the
[upgrade and recovery guide](upgrade-4.0.0.md) before upgrading any existing
deployment.

LakeSoul Core and the Python package have independent release versions. The
Core release tag is `v4.0.0`; the compatible Python release is `2.0.0` with tag
`py-v2.0.0`.

## Highlights

- Vortex Compact is the default format for new writes.
- Readers can scan Parquet, Vortex, and Vortex Compact files in one snapshot.
- Metadata stores the Arrow schema as Arrow IPC alongside the existing JSON
  schema.
- Spark moves to `3.5.8`, Flink to `1.20.0`, Flink CDC to `3.5.0`, and Presto
  to `0.296`.
- Connector Maven coordinates now encode the engine compatibility series in
  the `artifactId`.
- Official native artifacts support Linux x86_64 GNU only.
- Spark Gluten integration is available as a GitHub Release Preview.

## Breaking changes

### Default physical format

`vortex-compact` is the default format for `4.0.0` writes. LakeSoul `3.0.x`
cannot read Vortex or Vortex Compact files. During an upgrade, force every
writer to Parquet until the deployment has passed its acceptance checks and
restoring the pre-upgrade backup is no longer required.

The exact point of no return is the first successful metadata commit that
makes a Vortex or Vortex Compact file part of a visible table snapshot.
Creating an uncommitted temporary file does not cross this boundary.

### Metadata Arrow schema

The `4.0.0` metadata migration adds these nullable columns to `table_info`:

```sql
table_schema_arrow_ipc bytea
table_schema_arrow_ipc_json_hash text
```

`table_schema_arrow_ipc` stores the Arrow schema in IPC form.
`table_schema_arrow_ipc_json_hash` associates that representation with the
existing JSON `table_schema`. Existing rows may keep both new values as `NULL`
until a `4.0.0` client writes or refreshes their Arrow schema. The JSON schema
column remains present.

The migration also applies:

```sql
ALTER TABLE data_commit_info REPLICA IDENTITY FULL;
```

Apply the versioned, idempotent migration with `script/metadata_migrate.py`
before starting any `4.0.0` process. The runner records the migration version,
description, checksum, installation time, and database role in
`lakesoul_schema_migrations`; it rejects an applied migration whose recorded
description or checksum no longer matches the repository.

### Maven coordinates

Starting with `4.0.0`, Maven `<version>` represents the LakeSoul Core version
only. Runtime and Scala compatibility versions are encoded in `artifactId`.
The old connector coordinates and the new coordinates must not coexist on one
classpath.

| Old example | `4.0.0` coordinate |
|---|---|
| `com.dmetasoul:lakesoul-spark:3.5-3.0.0` | `com.dmetasoul:lakesoul-spark-3.5_2.12:4.0.0` |
| `com.dmetasoul:lakesoul-flink:1.20-3.0.0` | `com.dmetasoul:lakesoul-flink-1.20_2.12:4.0.0` |
| `com.dmetasoul:lakesoul-presto:0.29-3.0.0` | `com.dmetasoul:lakesoul-presto-0.296:4.0.0` |

No relocation artifacts are published for the old names.

### Range-partitioned overwrite

For a range-partitioned table, Spark overwrite without `replaceWhere` now
expires only range partitions present in the new files. Range partitions
absent from the input remain visible. In `3.0.0`, the same operation expired
all existing files.

Jobs that used overwrite without a predicate as a table-wide truncate must
explicitly truncate or drop the table, or use an appropriate validated
`replaceWhere` predicate.

### Flink CDC cutover

Recovery of a Flink CDC `3.0` savepoint in Flink CDC `3.5` is not supported by
the `4.0.0` upgrade path. Use a stopped cutover with source retention or an
independently verified replay or backfill procedure.

### Presto behavior

Presto now defaults `case-sensitive-name-matching` to `false`. Identifiers are
normalized to lower case and resolved without regard to case. Lookup fails
when multiple physical schemas or tables differ only by case. Set the property
to `true` when exact physical-name matching is required.

Arrow timestamps, including timezone-annotated fields, map to Presto
`TIMESTAMP`. In `3.0.0`, timezone-annotated fields were exposed as
`TIMESTAMP WITH TIME ZONE`. Microsecond values are truncated to Presto's
millisecond representation, and the Arrow timezone annotation is not
preserved on this path.

### Native ABI and deployment

The `4.0.0` Java JARs and embedded native libraries are one matching set.
Mixing `3.x` and `4.0.0` connector JARs, native libraries, coordinators,
workers, writers, or metadata processes is unsupported. Upgrade by stopping
the complete deployment and replacing every component together.

## Non-rollback boundaries

In-place rollback and mixed-version rolling rollback from `4.0.0` to `3.x`
are unsupported after either boundary:

1. the first visible metadata commit containing a Vortex or Vortex Compact
   file;
2. the first incompatible metadata migration applied by this or a later
   `4.x` release.

Before either boundary, create and restore-test one consistent backup pair
containing PostgreSQL metadata and table data from the same quiesced point.
After a boundary, recovery to `3.x` requires restoring both halves of that
pair. Replacing binaries, restoring only PostgreSQL, restoring only table
files, or rewriting selected Vortex files to Parquet is not rollback. When
backup restoration is not selected, use a forward fix on `4.0.x`.

## Published coordinates and assets

### Maven Central GA

```text
com.dmetasoul:lakesoul-parent:4.0.0
com.dmetasoul:lakesoul-common:4.0.0
com.dmetasoul:lakesoul-io-java:4.0.0
com.dmetasoul:lakesoul-spark-3.5_2.12:4.0.0
com.dmetasoul:lakesoul-flink-1.20_2.12:4.0.0
com.dmetasoul:lakesoul-presto-0.296:4.0.0
```

### GitHub Release

```text
lakesoul-spark-3.5_2.12-4.0.0.jar
lakesoul-flink-1.20_2.12-4.0.0.jar
lakesoul-presto-0.296-4.0.0.jar
lakesoul-spark-gluten-3.5_2.12-4.0.0.jar  # Preview
lakesoul-4.0.0-src.tar.gz
SHA256SUMS
SHA256SUMS.asc
SBOM.spdx.json
```

`lakesoul-common` and `lakesoul-io-java` are available from Maven Central and
are not duplicated as GitHub Release assets. Rust native libraries are
embedded in the JVM artifacts and are not published independently.

## Compatibility and runtime baselines

The [user-facing compatibility matrix](../../website/docs/01-Getting%20Started/04-compatibility.md)
distinguishes supported baselines from versions exercised by release CI.

| Component | `4.0.0` baseline | Release status |
|---|---|---|
| Spark | Spark `3.5.8`, Scala `2.12.15`, Java 11 build/runtime baseline | GA |
| Flink | Flink `1.20.0`, Scala `2.12`, Java 11 or later | GA |
| Flink CDC | `3.5.0` | GA; `3.0` savepoint reuse unsupported |
| Presto | Presto `0.296`, Java 17 | GA |
| PostgreSQL | PostgreSQL 14 or later; release CI uses `14.5` | GA |
| Python | LakeSoul Python `2.0.0`, Python `3.10+`, PyArrow `>=16,<21` | Independent release |
| Native platform | Linux x86_64 GNU, `x86_64-unknown-linux-gnu` | GA |
| Gluten | Spark `3.5.8`, Scala `2.12`, Gluten `1.6.0` | Preview |

## Native platform support

Official `4.0.0` artifacts support only:

| Platform | Platform ID | Rust target | Support level |
|---|---|---|---|
| Linux x86_64 GNU | `linux-x86_64` | `x86_64-unknown-linux-gnu` | GA Production |

Linux aarch64, Linux musl, macOS, Windows, and 32-bit platforms are
unsupported. The release does not publish platform-specific connector JARs or
provide official source-build support for those platforms.

## Gluten Preview

The preview artifact is:

```text
com.dmetasoul:lakesoul-spark-gluten-3.5_2.12:4.0.0
```

It targets Spark `3.5.8`, Scala `2.12`, Gluten `1.6.0`, and Linux x86_64 GNU.
It is distributed as a GitHub Release JAR, is not published to Maven Central,
and does not block Core GA. Preview means its public dependency chain and
production support contract are not yet at GA level; validate it independently
before production use.

## Known issues and limitations

- Only Linux x86_64 GNU has official native support.
- LakeSoul `3.x` cannot read Vortex or Vortex Compact files written by
  `4.0.0`.
- In-place and rolling rollback after a point of no return are unsupported.
- Flink CDC `3.0` savepoints are not supported for recovery into Flink CDC
  `3.5`.
- Presto timestamp mapping loses Arrow timezone annotations and truncates
  microsecond values to milliseconds.
- Case-insensitive Presto lookup fails for physical names that differ only by
  case.
- Gluten support is Preview and the artifact is available only from the
  GitHub Release.
- Mixed-version Spark, Flink, Presto, JAR, native-library, writer, and metadata
  deployments are unsupported.

## Upgrade and recovery

The supported production path is a cold upgrade from the verified `3.0.0`
baseline. Stop all LakeSoul activity, create and restore-test the metadata and
table-data backup pair, apply metadata DDL, replace every runtime, and retain a
Parquet-only upgrade window before enabling Vortex Compact.

Follow the complete [LakeSoul 4.0.0 Upgrade and Recovery Guide](upgrade-4.0.0.md);
do not infer rollback support from snapshot time travel or mixed-format read
support.
