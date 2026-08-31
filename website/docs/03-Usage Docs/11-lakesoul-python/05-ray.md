# Ray Data

The Ray adapter maps LakeSoul scan partitions to Ray Data read tasks and writes Ray blocks through LakeSoul's native writer.

Install the optional dependency:

```bash
pip install 'lakesoul[ray]'
```

The examples below use the `events` table created and populated in [Core catalog and table IO](02-core-api.md).

## Read a LakeSoul table

`LakeSoulScan.to_ray()` returns a `ray.data.Dataset`:

```python
import ray

from lakesoul import LakeSoulCatalog

ray.init()
catalog = LakeSoulCatalog.from_env()
ray_dataset = (
    catalog.scan(
        "events",
        partitions={"event_date": "2026-08-27"},
        columns=["id", "value"],
    )
    .to_ray()
)

print(ray_dataset.count())
for batch in ray_dataset.iter_batches(batch_format="pyarrow"):
    print(batch.to_pydict())
```

LakeSoul resolves the table snapshot and merge-on-read plan first. Each non-empty LakeSoul scan partition becomes a Ray read task.

Importing `lakesoul.ray` also registers `ray.data.read_lakesoul`. The registered function accepts a `LakeSoulScan`:

```python
import ray.data
import lakesoul.ray

scan = catalog.scan("events")
ray_dataset = ray.data.read_lakesoul(scan)
```

Prefer `scan.to_ray()` in new code for consistency with the other SDK adapters.

## Write a Ray Dataset

The target LakeSoul table must already exist and its schema must match the Ray Dataset:

```python
import pyarrow as pa
import ray.data

table = catalog.table("events")
arrow_rows = pa.table(
    {
        "id": [1, 2],
        "event_date": ["2026-08-27", "2026-08-27"],
        "value": [52.0, 81.5],
    },
    schema=table.schema,
)
rows = ray.data.from_arrow(arrow_rows)
table.write_ray(rows)
```
Alternatively, importing `lakesoul.ray` registers `Dataset.write_lakesoul`:

```python
import lakesoul.ray

rows.write_lakesoul(table)
```

Both calls default to `vortex-compact`. Pass `format="vortex"` or `format="parquet"` when required; see [Physical File Formats](../../01-Getting%20Started/05-physical-file-formats.md).

Ray tasks write the data files. The driver commits the files to LakeSoul metadata only after the tasks complete successfully.
