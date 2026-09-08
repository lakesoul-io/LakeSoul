# LakeSoul Python SDK

The `lakesoul` package provides Python APIs for LakeSoul metadata, native IO,
and integrations with common data and machine-learning frameworks.

## Features

- Discover, create, load, and drop LakeSoul tables through `LakeSoulCatalog`.
- Build immutable, lazy scans with partition pruning, column projection, row
  filtering, batching, and distributed sharding.
- Read LakeSoul's primary-key merge-on-read results through PyArrow, Pandas,
  DuckDB, PyTorch, Hugging Face Datasets, Ray Data, or Daft.
- Write PyArrow data locally or use Ray Data and Daft for distributed writes.
- Write Parquet, Vortex, and Vortex Compact files and commit them to LakeSoul
  metadata.

Python 3.10 or later is required.

```bash
pip install lakesoul

# Install only the integrations required by your application.
pip install 'lakesoul[pandas,duckdb,torch,datasets,ray,daft]'
```

## Read APIs

Start from a catalog or a loaded table. A `LakeSoulScan` is lazy: data is read
only when one of its output methods is called.

```python
import pyarrow.compute as pc

from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
scan = (
    catalog.scan("events", partitions={"event_date": "2026-09-08"})
    .select("id", "value")
    .filter(pc.field("value") >= 50)
    .options(batch_size=4096, thread_count=4)
)
```

### Scan creation and configuration

| API | Purpose |
| --- | --- |
| `LakeSoulCatalog.scan(name, ...)` | Load a table and create a lazy scan. |
| `LakeSoulCatalog.dataset(name, ...)` | Create a scan and return its PyArrow Dataset directly. |
| `LakeSoulTable.scan(...)` | Create a lazy scan from an already loaded table. |
| `LakeSoulScan.select(*columns)` | Select result columns. |
| `LakeSoulScan.filter(expression)` | Apply a PyArrow Dataset filter expression. |
| `LakeSoulScan.with_partitions(...)` | Restrict the scan to partition values. |
| `LakeSoulScan.shard(rank, world_size)` | Assign one distributed scan shard. |
| `LakeSoulScan.options(...)` | Configure batching, threads, partition columns, object storage, and native reader options. |

### Read outputs

| API | Result |
| --- | --- |
| `LakeSoulScan.to_arrow_dataset()` | Lazy `pyarrow.dataset.Dataset`. |
| `LakeSoulScan.to_reader()` | Streaming `pyarrow.RecordBatchReader`. |
| `LakeSoulScan.to_batches()` | Iterator of `pyarrow.RecordBatch` objects. |
| `LakeSoulScan.to_arrow_table()` | Materialized `pyarrow.Table`; use only when the result fits in memory. |
| `LakeSoulScan.to_ray()` | Lazy Ray Data `Dataset`. |
| `LakeSoulScan.to_daft()` | Lazy Daft `DataFrame`. |
| `LakeSoulScan.to_torch()` | PyTorch `IterableDataset`; automatically uses an initialized distributed rank. |
| `LakeSoulScan.to_huggingface()` | Hugging Face streaming `IterableDataset`. |

Pandas and DuckDB use the Arrow outputs:

```python
pandas_dataframe = scan.to_arrow_table().to_pandas()

arrow_dataset = scan.to_arrow_dataset()
# DuckDB can query arrow_dataset directly by its Python variable name.
```

The adapter modules also expose `lakesoul.ray.read_lakesoul(scan)`,
`lakesoul.daft.read_lakesoul(scan)`, and
`lakesoul.huggingface.from_lakesoul(scan)`.

## Write APIs

Use the methods on an existing `LakeSoulTable` when the produced files must be
committed to LakeSoul metadata.

| API | Input | Behavior |
| --- | --- | --- |
| `LakeSoulTable.write_arrow(data, ...)` | `pyarrow.RecordBatch`, `pyarrow.Table`, or `pyarrow.RecordBatchReader` | Writes files, commits metadata, and returns `WriteResult`. |
| `LakeSoulTable.write_ray(dataset, ...)` | Ray Data `Dataset` | Runs distributed Ray write tasks and commits after all tasks succeed. |
| `LakeSoulTable.write_daft(dataframe, ...)` | Daft `DataFrame` | Uses Daft's distributed sink, commits metadata, and returns `WriteResult`. |

```python
import pyarrow as pa

table = catalog.table("events")
result = table.write_arrow(
    pa.table(
        {
            "id": [1, 2],
            "event_date": ["2026-09-08", "2026-09-08"],
            "value": [52.0, 81.5],
        },
        schema=table.schema,
    )
)
print(result.row_count)
print(result.files)
```

Equivalent integration entry points are also available:

| API | Equivalent table method |
| --- | --- |
| `lakesoul.ray.write_lakesoul(dataset, table, ...)` | `table.write_ray(dataset, ...)` |
| `ray_dataset.write_lakesoul(table, ...)` after importing `lakesoul.ray` | `table.write_ray(ray_dataset, ...)` |
| `lakesoul.daft.write_lakesoul(dataframe, table, ...)` | `table.write_daft(dataframe, ...)` |
| `daft_dataframe.write_lakesoul(table, ...)` after importing `lakesoul.daft` | `table.write_daft(daft_dataframe, ...)` |

### Low-level native writer

`lakesoul.io.Writer` writes data files but does **not** publish them to the
LakeSoul metadata service. Use it only when the caller owns the commit step.

```python
from lakesoul.io import IOConfig, Writer

config = IOConfig(path="./output", schema=table.schema)
with Writer(config) as writer:
    writer.write(table_data)

result = writer.result
```

The writer accepts `pyarrow.RecordBatch`, `pyarrow.Table`, and
`pyarrow.RecordBatchReader`. `Writer.finish()` returns `WriteResult`;
`Writer.abort()` discards pending work and closes the writer.
