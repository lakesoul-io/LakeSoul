# Physical File Formats

LakeSoul 4.0 introduces Vortex as a native physical file format and changes the default for new writes to `vortex-compact`. LakeSoul tables remain logical tables managed by LakeSoul metadata: the physical format is selected for each write, so one table snapshot may reference Parquet and Vortex files at the same time.

:::important LakeSoul 4.0 default
All LakeSoul 4.0 NativeIO writers default to `vortex-compact`. LakeSoul 3.x cannot read either Vortex variant. Read [File and upgrade compatibility](04-compatibility.md#file-and-upgrade-compatibility) before enabling Vortex during an upgrade.
:::

## Format comparison

| Write value | File extension | Compression strategy | Choose it when |
|---|---|---|---|
| `vortex-compact` | `.vortex` | Vortex Compact; LakeSoul enables the compact BtrBlocks strategy, which uses aggressive encodings including Zstd for binary data and PCodec for numeric data | Minimum storage and object-store transfer are more important than maximum scan speed. This is the LakeSoul 4.0 default. |
| `vortex` | `.vortex` | Standard Vortex writer strategy | Read performance matters more than the smallest possible file, and every reader is LakeSoul 4.x or another compatible Vortex reader. Benchmark against `vortex-compact` for the workload. |
| `parquet` | `.parquet` | Parquet with Zstd compression in LakeSoul NativeIO | Parquet interoperability is required, or a 4.0 upgrade must retain the pre-Vortex rollback path. |

`vortex` and `vortex-compact` are not two file specifications. Both produce Vortex files with the `.vortex` extension; the value selects the writer's compression strategy. A filename alone cannot distinguish the two strategies.

Vortex is a compressed columnar format designed to operate directly on encoded arrays. LakeSoul enables Vortex projection and predicate pushdown. See the [Vortex file-format documentation](https://docs.vortex.dev/concepts/file-format.html) for the underlying layout and compression model.

## Read behavior

LakeSoul 4.0 readers detect physical formats from file paths, group files by format and object store, and build one logical scan. As a result:

- existing Parquet data does not need to be rewritten before LakeSoul 4.0 can read it;
- later writes may use Vortex while older Parquet files remain visible;
- Merge-on-Read, primary-key resolution, partition pruning, and snapshot semantics are LakeSoul behaviors and remain in effect across mixed-format files;
- direct reads of the physical files outside LakeSoul bypass LakeSoul metadata and Merge-on-Read semantics.

Mixed-format read support is not mixed-version runtime support. Do not run LakeSoul 3.x and 4.0 writers, readers, connector JARs, or native libraries together.

## Select a format

### Spark

Set the session default:

```sql
SET spark.dmetasoul.lakesoul.native.io.physical_format=vortex-compact;
```

Override one write with `file_format`:

```scala
df.write
  .format("lakesoul")
  .option("file_format", "parquet")
  .mode("append")
  .save(path)
```

Accepted values are `parquet`, `vortex`, and `vortex-compact`. The per-write option takes precedence over the Hadoop job option, which takes precedence over the Spark SQL default.

### Flink and Flink CDC

Set `file_format` on the LakeSoul sink table:

```sql
CREATE TABLE target (
  id BIGINT,
  data STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'lakesoul',
  'file_format' = 'vortex-compact'
);
```

A statement-level dynamic option can override it:

```sql
INSERT INTO target /*+ OPTIONS('file_format'='parquet') */
SELECT id, data FROM source;
```

For generated Flink CDC sink DDL, configure the generated tables as well; changing only manually created tables does not affect generated sinks.

### Python, Ray, and Daft

The Python SDK uses `vortex-compact` when `format` is omitted:

```python
# Default: vortex-compact
arrow_result = table.write_arrow(arrow_table)
table.write_ray(ray_dataset)
daft_result = table.write_daft(daft_dataframe)

# Explicit alternatives
parquet_result = table.write_arrow(arrow_table, format="parquet")
vortex_result = table.write_arrow(arrow_table, format="vortex")
compact_result = table.write_arrow(arrow_table, format="vortex-compact")
```

The low-level `lakesoul.io.Writer` accepts the same three values through `IOConfig(format=...)`.

## Upgrade safety

When upgrading from LakeSoul 3.0 to 4.0, force **every** Spark, Flink, CDC, Python, Ray, Daft, compaction, and maintenance writer to `parquet` until acceptance checks pass and restoring the pre-upgrade backup is no longer required.

The rollback boundary is the first successful metadata commit that makes a Vortex file visible in a table snapshot. After that point, LakeSoul 3.x cannot read the table state. An uncommitted temporary `.vortex` file does not cross the boundary.

See the [LakeSoul 4.0 compatibility matrix](04-compatibility.md) and [4.0 upgrade and recovery guide](https://github.com/lakesoul-io/LakeSoul/blob/v4.0.0/docs/release/upgrade-4.0.0.md) for the supported cutover procedure.
