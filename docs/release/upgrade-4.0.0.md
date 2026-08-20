# LakeSoul 4.0.0 Upgrade and Recovery Guide

This guide defines the supported upgrade path to LakeSoul Core `4.0.0`, the required pre-upgrade checks, the rollback boundaries, and the tested recovery procedure. Read it together with the [LakeSoul 4.0.0 release guide](release-4.0.0.md).

LakeSoul `4.0.0` is a cold upgrade. Mixed `3.x`/`4.0.0` writers, mixed connector JARs and native libraries, rolling rollback, and in-place rollback after a point of no return are unsupported.

## 1. Supported upgrade paths

| Source | Target | Support status | Required path |
|---|---|---|---|
| `3.0.0` | `4.0.0` | Supported cold upgrade | Stop all LakeSoul activity, create and restore-test one consistent backup pair, migrate metadata, replace every runtime together, and keep all writers on Parquet during the upgrade window. |
| Other `3.0.x` versions | `4.0.0` | Not verified | Upgrade to the verified `3.0.0` baseline or validate the exact source version with the complete `4.0.0` release gates before production use. |
| `2.x` and earlier | `4.0.0` | No direct upgrade | Upgrade through a supported `3.0.0` deployment and verify it before starting this procedure. |
| `4.0.x` | Later `4.0.x` | Forward-fix path | Follow the target patch release notes; do not infer downgrade compatibility. |
| Any newer release | `4.0.0` | Unsupported downgrade | Restore a backup created by the older release instead. |

The release migration gate verifies the `v3.0.0` metadata schema against the `4.0.0` schema and verifies PostgreSQL dump/restore. It does not establish compatibility for every historical `3.x` schema.

Flink CDC `3.0` savepoint recovery into Flink CDC `3.5` has not been verified. Reusing such a savepoint is not a supported part of this upgrade. Plan a stopped cutover with source retention or another independently tested replay/backfill procedure.

The supported native release platform is Linux x86_64 GNU. Linux aarch64, Linux musl, macOS, Windows, and 32-bit deployments are outside the `4.0.0` Core release support boundary.

## 2. Required runtime and dependency changes

### 2.1 Runtime baselines

| Component | `4.0.0` baseline |
|---|---|
| Spark | Spark `3.5.8`, Scala `2.12.15` |
| Flink | Flink `1.20.0`, Scala `2.12`, Java 11 or later |
| Flink CDC | Flink CDC `3.5.0` |
| Presto | Presto `0.296`, Java 17 |
| Native libraries | Linux `x86_64-unknown-linux-gnu` |

Presto must run on Java 17. Do not reuse a Java 11 Presto coordinator or worker process, and do not run mixed old/new coordinators and workers during the upgrade.

### 2.2 Maven coordinates

Starting with `4.0.0`, the Maven version is only the LakeSoul Core version. Spark, Flink, Presto, and Scala compatibility versions are encoded in the `artifactId`.

Replace old connector coordinates as follows:

| Old example | `4.0.0` coordinate |
|---|---|
| `com.dmetasoul:lakesoul-spark:3.5-3.0.0` | `com.dmetasoul:lakesoul-spark-3.5_2.12:4.0.0` |
| `com.dmetasoul:lakesoul-flink:1.20-3.0.0` | `com.dmetasoul:lakesoul-flink-1.20_2.12:4.0.0` |
| `com.dmetasoul:lakesoul-presto:0.29-3.0.0` | `com.dmetasoul:lakesoul-presto-0.296:4.0.0` |

The complete Maven Central GA set is:

```text
com.dmetasoul:lakesoul-parent:4.0.0
com.dmetasoul:lakesoul-common:4.0.0
com.dmetasoul:lakesoul-io-java:4.0.0
com.dmetasoul:lakesoul-spark-3.5_2.12:4.0.0
com.dmetasoul:lakesoul-flink-1.20_2.12:4.0.0
com.dmetasoul:lakesoul-presto-0.296:4.0.0
```

`lakesoul-spark-gluten-3.5_2.12:4.0.0` is a GitHub Release Preview, not a Maven Central GA artifact. `4.0.0` does not publish relocation artifacts for the old connector names. Remove the old JARs from application images, Spark `--jars`, Flink `lib`, and Presto plugin directories; do not place old and new artifacts on the same classpath.

## 3. Behavior changes

### 3.1 Physical format default

The `4.0.0` writer default is `vortex-compact`. LakeSoul `3.0.x` cannot read Vortex files. Keep every `4.0.0` writer on Parquet until the upgrade has passed all acceptance checks and rollback to the pre-upgrade backup is no longer required.

Readers can group and read Parquet, Vortex, and Vortex Compact files in one snapshot. That read capability does not make a `3.0.x` rollback safe after a Vortex commit.

### 3.2 Metadata Arrow schema

`script/meta_init.sql` adds these nullable columns to `table_info`:

```sql
table_schema_arrow_ipc bytea
table_schema_arrow_ipc_json_hash text
```

Existing rows may keep both values as `NULL` until a `4.0.0` client writes or refreshes the Arrow schema. The JSON `table_schema` remains present. The same migration also sets:

```sql
ALTER TABLE data_commit_info REPLICA IDENTITY FULL;
```

The versioned migration in `script/metadata-migrations/` is additive and idempotent: it uses `ADD COLUMN IF NOT EXISTS`, preserves existing metadata rows, and is exercised by the `v3.0.0` migration gate. `script/metadata_migrate.py` records its version, description, SHA-256 checksum, installation time, and database role in `lakesoul_schema_migrations`. An applied migration whose checksum or description no longer matches the repository fails validation.

Apply and check the DDL before deploying or starting any `4.0.0` binary. The runner requires an explicit PostgreSQL URL and holds a PostgreSQL advisory lock, so concurrent deployment processes cannot apply the same migration independently. After the DDL is applied, keep all `3.x` processes stopped even though these particular additions are backward-tolerant; mixed-version operation is not supported.

### 3.3 Range-partitioned overwrite

In `3.0.0`, Spark `SaveMode.Overwrite` without `replaceWhere` expired every existing file, including files in range partitions absent from the input.

In `4.0.0`:

- a non-range-partitioned overwrite still replaces all table files;
- a range-partitioned overwrite without `replaceWhere` expires only the range partitions present in the newly written files;
- untouched range partitions remain visible;
- an overwrite with `replaceWhere` continues to validate the predicate against the new files and expires files selected by that predicate.

Example: if a table contains range partitions `id=2` and `id=4`, and an overwrite writes `id=1`, `id=2`, and `id=3`, `4.0.0` replaces `id=2`, adds `id=1` and `id=3`, and preserves `id=4`. Jobs that relied on overwrite-without-a-predicate to truncate every range partition must explicitly truncate/drop the table or use an explicit, validated `replaceWhere` operation appropriate to their partition layout.

Review all uses of:

```scala
df.write.format("lakesoul").mode("overwrite")
```

and:

```sql
INSERT OVERWRITE TABLE ...
```

before upgrading. Treat any job whose input omits existing range partitions as behavior-changing.

## 4. Pre-upgrade checks

Complete and record every check before stopping the old deployment.

### 4.1 Inventory and runtime checks

1. Record the exact LakeSoul JAR names, native library versions, Spark/Flink/Presto versions, Java versions, PostgreSQL version, warehouse URI, object-store settings, and active table namespaces.
2. Verify the target hosts are Linux x86_64 GNU.
3. Verify Spark uses `3.5.8` and Scala `2.12.15`.
4. Verify Flink uses `1.20.0`, Flink CDC uses `3.5.0`, and its JVM is Java 11 or later.
5. Verify Presto `0.296` will use Java 17.
6. Remove every old Maven coordinate and every duplicate LakeSoul JAR from deployment manifests.
7. Identify all writers, streaming queries, CDC jobs, compaction tasks, cleanup tasks, and metadata-management processes. The backup cannot begin until all of them are stopped.
8. Review every range-partitioned overwrite for the semantic change in [Section 3.3](#33-range-partitioned-overwrite).
9. Confirm enough PostgreSQL and table-storage capacity for an immutable backup plus an isolated restore.

Useful runtime commands include:

```bash
java -version
spark-submit --version
flink --version
psql "$LAKESOUL_PG_URL" -c 'select version();'
```

Use the JDBC-to-`psql` connection form appropriate to the deployment; do not pass a JDBC URL directly to `psql`.

### 4.2 Metadata checks

Before migration, capture row counts and the current schema:

```bash
psql "$PGURI" -v ON_ERROR_STOP=1 -c "
select 'namespace', count(*) from namespace
union all select 'table_info', count(*) from table_info
union all select 'table_name_id', count(*) from table_name_id
union all select 'table_path_id', count(*) from table_path_id
union all select 'partition_info', count(*) from partition_info
union all select 'data_commit_info', count(*) from data_commit_info
union all select 'global_config', count(*) from global_config
order by 1;"

psql "$PGURI" -v ON_ERROR_STOP=1 -c "
select table_name, column_name, data_type, is_nullable
from information_schema.columns
where table_schema = 'public'
order by table_name, ordinal_position;"
```

Investigate missing table/path mappings, partitions whose commits are absent, and referenced data files that do not exist before taking the backup. An upgrade is not a repair mechanism for a pre-existing inconsistent table.

### 4.3 Quiescence check

Stop all LakeSoul writers and maintenance services. Cancel or drain Spark streaming, Flink/Flink CDC, compaction, cleanup, and ingestion jobs. Block new application deployments and metadata changes. Confirm no process can commit while the PostgreSQL and table-data backups are taken.

The required backup is a pair from this single quiesced point:

```text
pre-upgrade PostgreSQL metadata backup
+
pre-upgrade table-data backup
```

Backups taken while commits are still running, or metadata and table-data backups from different points in time, are invalid for rollback.

## 5. Parquet-only upgrade window

Apply the setting to every writer. One unconfigured writer can cross the Vortex point of no return for its table.

### 5.1 Spark

Set the session-wide default before creating any writer:

```sql
SET spark.dmetasoul.lakesoul.native.io.physical_format=parquet;
```

For defense in depth, also set the per-write option:

```scala
df.write
  .format("lakesoul")
  .option("file_format", "parquet")
  .mode("append")
  .save(path)
```

The per-write `file_format` option takes precedence over the Hadoop job option, which takes precedence over the Spark SQL default.

### 5.2 Flink and Flink CDC

Set `file_format` on every LakeSoul sink table:

```sql
CREATE TABLE target (
  id BIGINT,
  data STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'lakesoul',
  'file_format' = 'parquet'
);
```

For statement-level dynamic options, use the same key:

```sql
INSERT INTO target /*+ OPTIONS('file_format'='parquet') */
SELECT id, data FROM source;
```

Verify generated or synchronized Flink CDC sink DDL also contains `'file_format'='parquet'`; setting it only on manually created tables is insufficient.

### 5.3 Python, Ray, and Daft

Pass `format="parquet"` explicitly to every write API:

```python
table.write_arrow(arrow_table, format="parquet")
table.write_ray(ray_dataset, format="parquet")
table.write_daft(daft_dataframe, format="parquet")
```

Do not rely on the default; the default is `vortex-compact`.

### 5.4 Window exit check

Before allowing Vortex Compact, verify that every file committed since the upgrade started is Parquet and that Spark, Flink, Presto, and the required Python paths can read representative append, partition-filter, and primary-key/upsert tables. Retain the pre-upgrade backup pair after the window ends according to the production retention policy.

## 6. Verified backup procedure

The `4.0.0` release gate verifies a PostgreSQL custom-format dump together with a table-file archive, destroys both working copies, restores both, and reads the restored Parquet, Vortex, and Vortex Compact tables. Production backups must provide the same properties for the deployment's storage system.

### 6.1 PostgreSQL metadata backup

With all LakeSoul activity stopped:

```bash
mkdir -p "$BACKUP_DIR"
pg_dump "$PGURI" \
  --format=custom \
  --file="$BACKUP_DIR/lakesoul-metadata.dump"
pg_restore --list "$BACKUP_DIR/lakesoul-metadata.dump" \
  > "$BACKUP_DIR/lakesoul-metadata.contents"
sha256sum "$BACKUP_DIR/lakesoul-metadata.dump" \
  > "$BACKUP_DIR/lakesoul-metadata.dump.sha256"
```

Save the pre-upgrade row counts and schema output beside the dump.

Restore into an isolated database; listing the dump is not sufficient verification:

```bash
createdb "$RESTORE_DB"
pg_restore \
  --exit-on-error \
  --dbname="$RESTORE_DB" \
  "$BACKUP_DIR/lakesoul-metadata.dump"
```

Run the row-count and schema queries from [Section 4.2](#42-metadata-checks) against the isolated database and compare them with the captured pre-upgrade results.

### 6.2 Table-data backup

Use an immutable, storage-native snapshot when available:

- S3-compatible storage: enable versioning and record the version/snapshot boundary, or copy every referenced object to a separate immutable bucket or prefix;
- HDFS: create and retain an HDFS snapshot of every LakeSoul warehouse path;
- local/shared filesystem: create a filesystem snapshot or a complete archive while the deployment is quiesced.

The release gate's local-filesystem equivalent is:

```bash
tar -C "$WAREHOUSE_PARENT" \
  -cf "$BACKUP_DIR/lakesoul-table-data.tar" \
  "$WAREHOUSE_NAME"
sha256sum "$BACKUP_DIR/lakesoul-table-data.tar" \
  > "$BACKUP_DIR/lakesoul-table-data.tar.sha256"
```

The backup must include every object referenced by `data_commit_info` and `partition_info`, including data retained for snapshots required by the recovery policy. Record object paths, object versions where available, object count, total bytes, and checksums or provider ETags. A copy of only the newest visible Parquet files is not a valid LakeSoul backup.

### 6.3 Restore verification

Restore the table data to an isolated bucket, prefix, HDFS snapshot clone, or filesystem path. Point the isolated metadata restore at the corresponding restored paths without modifying production metadata. Verify at least:

- representative non-partitioned and range-partitioned tables;
- partition pruning/filter reads;
- primary-key merge/upsert results;
- snapshot data retained by the recovery policy;
- Spark, Flink, Presto, and required Python readers;
- object count and checksums against the backup manifest.

A backup is verified only after this isolated metadata-and-table-data restore succeeds. Record the dump checksum, table-data snapshot/version identifier, restore location, verification commands, results, and approver as one backup-set record.

## 7. Points of no return

### 7.1 First committed Vortex Compact write

The exact boundary named for `4.0.0` is the first successful metadata commit that makes a `.vortex` file written as `vortex-compact` part of a visible table snapshot. A commit written with the non-compact `vortex` mode is equally unreadable by `3.0.x` and crosses the same boundary. Creation of an uncommitted temporary/orphan file does not change a table snapshot, but it must still be cleaned before any recovery decision.

After this commit:

- `3.0.x` cannot read the affected snapshot;
- replacing `4.0.0` binaries with `3.0.x` binaries is not rollback;
- mixed-version rolling rollback is unsupported;
- rewriting selected Vortex files to Parquet is a forward data conversion, not in-place rollback;
- recovery to `3.0.x` requires restoration of both members of the same pre-upgrade backup pair.

Operationally, treat enabling any writer's Vortex setting as approval to cross this boundary; do not wait for an incident to determine which table committed first.

### 7.2 First incompatible metadata migration

The exact boundary is the successful commit of the first metadata migration that drops, renames, changes the type or meaning of a `3.0.x`-required column/type/function, or otherwise makes a `3.0.x` metadata client unable to perform its normal reads and writes.

The additive Arrow columns and `REPLICA IDENTITY FULL` statement in `V4000000__core_4_0_0.sql` do not meet that definition and therefore do not cross this boundary. If a deployment applies additional, site-specific, or later release-candidate DDL that does meet the definition, that DDL commit is the boundary.

After an incompatible migration:

- do not start a `3.x` metadata client against the migrated database;
- restoring only table files is invalid;
- reversing individual DDL statements in the live database is unsupported;
- mixed-version rolling rollback is unsupported;
- recovery to `3.0.x` requires restoration of the matching pre-upgrade PostgreSQL dump and table-data backup.

## 8. Upgrade procedure

1. Complete all checks in [Section 4](#4-pre-upgrade-checks).
2. Stop every writer, reader service that mutates metadata, compaction job, cleanup job, CDC job, and scheduled deployment. Keep them stopped.
3. Create one quiesced PostgreSQL and table-data backup pair using [Section 6](#6-verified-backup-procedure).
4. Restore both backups into an isolated environment and complete the verification reads. Do not continue with an unverified backup.
5. Apply and validate the `4.0.0` metadata migration before deploying any `4.0.0` binary. `LAKESOUL_PG_URL` may be a JDBC PostgreSQL URL; the runner removes the JDBC-only `stringtype` parameter for `psql`:

   ```bash
   export LAKESOUL_PG_URL='jdbc:postgresql://metadata.example:5432/lakesoul?stringtype=unspecified'
   export LAKESOUL_PG_USERNAME='lakesoul'
   export LAKESOUL_PG_PASSWORD='...'
   python script/metadata_migrate.py status
   python script/metadata_migrate.py migrate
   python script/metadata_migrate.py check
   ```

   `migrate` creates the migration-history table, serializes migrators with an advisory lock, applies each pending migration transactionally, and writes its history row in the same transaction. A failed DDL statement therefore cannot produce a successful history record. `check` must report the schema current before the binary deployment proceeds.

6. Verify the Arrow columns and replica identity:

   ```bash
   psql "$PGURI" -v ON_ERROR_STOP=1 -c "
   select column_name, data_type
   from information_schema.columns
   where table_name = 'table_info'
     and column_name in (
       'table_schema_arrow_ipc',
       'table_schema_arrow_ipc_json_hash'
     )
   order by column_name;

   select relreplident
   from pg_class
   where relname = 'data_commit_info';"
   ```

   The two Arrow columns must be present and `data_commit_info.relreplident` must be `f` (`FULL`).

7. Replace all LakeSoul JARs and both native libraries as one set. Do not leave a `3.x` connector or native library in any process image.
8. Configure every writer for Parquet using [Section 5](#5-parquet-only-upgrade-window).
9. Start `4.0.0` readers first and validate representative tables. Then start controlled Parquet-only writers and validate commits, partition filters, primary-key results, and the changed overwrite behavior.
10. Start remaining Parquet-only workloads only after the controlled validation succeeds. Do not restore a Flink CDC `3.0` savepoint into Flink CDC `3.5` as part of the supported procedure.
11. Keep the deployment in the Parquet-only window until the release acceptance criteria are signed off.
12. Crossing the Vortex point of no return requires an explicit operational approval. Remove the Parquet override only after the owner accepts that recovery to `3.0.x` now means restoring the pre-upgrade backup pair.

## 9. Recovery procedures

Choose exactly one recovery model. Stop all writers and maintenance tasks before either procedure.

### 9.1 Restore the pre-upgrade backup pair

Use this path to return to `3.0.0`, including after a point of no return:

1. Stop and fence every `4.0.0` writer, CDC job, compaction task, cleanup task, and metadata client.
2. Preserve the failed `4.0.0` database and warehouse for incident analysis; do not overwrite the only copy.
3. Create a new database and a new storage location.
4. Restore the pre-upgrade table-data backup to the new location.
5. Restore the matching PostgreSQL dump to the new database.
6. Update restored table paths only through a separately tested recovery procedure if the storage URI changed. Never combine metadata from one backup set with table files from another.
7. Deploy the exact pre-upgrade `3.0.0` JARs, native libraries, runtime configuration, and credentials against the restored pair.
8. Run the same isolated restore verification used to approve the backup, then perform application acceptance checks.
9. Switch production traffic only after validation. Retain the failed environment until the incident owner approves disposal.

This is disaster recovery from a backup, not in-place rollback. Data committed after the pre-upgrade quiescence point is intentionally absent unless it is replayed through a separately verified application process.

### 9.2 Apply a `4.0.x` forward fix

Use this path when retaining post-upgrade commits is more important than returning to `3.0.0`:

1. Stop affected writers and preserve the current PostgreSQL database and table data.
2. Create and restore-test a new backup pair of the current `4.0.x` state before applying the fix.
3. Review the target `4.0.x` patch release for metadata and file-format instructions.
4. Apply only the documented patch migration or repair procedure.
5. Deploy matching patch-version JARs and native libraries as one set.
6. Validate metadata, Parquet/Vortex reads, affected writes, and application invariants before resuming traffic.

A forward fix may include a documented data rewrite or metadata repair. It does not restore `3.x` compatibility and must not be described as rollback.

## 10. Unsupported rollback patterns

The following procedures are unsupported after either point of no return and are not supported as rolling-upgrade techniques before it:

- replacing `4.0.0` JARs with `3.x` JARs in place;
- running `3.x` and `4.0.0` writers against the same table or metadata database;
- rolling back only some Spark executors, Flink task managers, or Presto coordinators/workers;
- restoring PostgreSQL without the matching table-data backup;
- restoring table data without the matching PostgreSQL backup;
- mixing backup sets from different timestamps;
- deleting Vortex files and continuing with old metadata;
- reversing incompatible DDL manually in the live production database;
- reusing an unverified Flink CDC `3.0` savepoint with Flink CDC `3.5`.

When in doubt, keep all writers stopped and choose either the complete pre-upgrade backup restoration or a documented `4.0.x` forward fix.
