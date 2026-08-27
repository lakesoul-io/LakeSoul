# Core catalog and table IO

`LakeSoulCatalog` is the main Python entry point. It owns the metadata connection and returns `LakeSoulTable` and `LakeSoulScan` objects.

## Load a table

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(namespace="default")
table = catalog.table("events")

print(table.name)
print(table.schema)
print(table.partition_by)
print(table.primary_keys)
```

`catalog.table()` raises `TableNotFoundError` when the table does not exist. `catalog.list_tables()` can be used for discovery.

## Create a table

A table requires a PyArrow schema and a storage path. Partition and primary-key columns must exist in the schema.

```python
import pyarrow as pa

from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
schema = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("event_date", pa.string()),
        pa.field("value", pa.float64()),
    ]
)

table = catalog.create_table(
    "events",
    path="file:///tmp/lakesoul/events",
    schema=schema,
    partition_by=("event_date",),
    primary_keys=("id",),
    hash_bucket_num=4,
)
```

For local development, `path` may be a local path instead of an object-store URI.

## Build a lazy scan

A scan records partition pruning, projection, filtering, batching, and optional distributed sharding. It does not read data until converted or iterated.

```python
import pyarrow.compute as pc

scan = (
    catalog.scan("events", partitions={"event_date": "2026-08-27"})
    .select("id", "value")
    .filter(pc.field("value") >= 50)
    .options(batch_size=4096, thread_count=4)
)

for batch in scan.to_batches():
    print(batch.to_pydict())
```

The equivalent options can be supplied directly to `catalog.scan()` or `table.scan()`. Use `scan.shard(rank, world_size)` when the caller owns distributed rank assignment.

`LakeSoulScan` is immutable: `select()`, `filter()`, `with_partitions()`, `shard()`, and `options()` return a new scan.

## Read as PyArrow

Choose a materialization method according to the expected data size:

```python
# Lazy PyArrow Dataset
arrow_dataset = scan.to_arrow_dataset()

# Streaming RecordBatchReader
reader = scan.to_reader()

# Iterable of RecordBatch objects
batches = scan.to_batches()

# Materialize the complete result in memory
arrow_table = scan.to_arrow_table()
```

Prefer `to_reader()` or `to_batches()` for large results. `to_arrow_table()` loads the complete scan result into memory.

## Write PyArrow data

`LakeSoulTable.write_arrow()` accepts a `pyarrow.RecordBatch`, `pyarrow.Table`, or `pyarrow.RecordBatchReader`. It writes data files and commits them to LakeSoul metadata before returning.

```python
import pyarrow as pa

rows = pa.table(
    {
        "id": [1, 2],
        "event_date": ["2026-08-27", "2026-08-27"],
        "value": [52.0, 81.5],
    },
    schema=table.schema,
)

result = table.write_arrow(rows)
print(result.row_count)
print(result.files)
```

The default physical format is `vortex-compact`. Pass `format="vortex"` or `format="parquet"` to select another strategy. See [Physical File Formats](../../01-Getting%20Started/05-physical-file-formats.md) for the tradeoffs, mixed-format behavior, and upgrade boundary.

## Object-store options

Object-store settings can be attached to the catalog and overridden for one scan or write:

```python
catalog = LakeSoulCatalog.from_env(
    object_store_options={
        "fs.s3a.endpoint": "http://localhost:9000",
        "fs.s3a.access.key": "rustfsadmin",
        "fs.s3a.secret.key": "rustfsadmin",
        "fs.s3a.path.style.access": "true",
    }
)

scan = catalog.scan(
    "events",
    object_store_options={"fs.s3a.endpoint": "http://rustfs:9000"},
)
```

Catalog options are inherited by tables and scans. Per-operation values take precedence.

## Drop a table

```python
table.drop()

# Suppress only the table-not-found error
catalog.drop_table("events", if_exists=True)
```
