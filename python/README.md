# LakeSoul Python Support

## Native Writer

The native writer accepts PyArrow batches, tables, and record batch readers. It
writes data files and returns their metadata; publishing those files to the
LakeSoul metadata service is a separate operation.

```python
import pyarrow as pa

from lakesoul.io import IOConfig, Writer

table = pa.table({"id": [1, 2], "value": ["a", "b"]})
config = IOConfig(
    path="./output",
    schema=table.schema,
    format="parquet",
)

with Writer(config) as writer:
    writer.write(table)

print(writer.result.files)
```

## Ray Writer

Importing `lakesoul.ray` registers a Pythonic write method on Ray datasets. The
target LakeSoul table must already exist. Data files are written by Ray tasks,
then committed to LakeSoul metadata by the driver after every task succeeds.

```python
import ray
import lakesoul.ray

dataset = ray.data.from_items(
    [
        {"id": 1, "value": "a"},
        {"id": 2, "value": "b"},
    ]
)
dataset.write_lakesoul(
    "target_table",
    namespace="default",
    format="parquet",
)
```
