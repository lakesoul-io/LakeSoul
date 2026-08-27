# Daft

The Daft adapter exposes LakeSoul as a native Daft data source and sink. LakeSoul resolves metadata and scan partitions; Daft schedules the distributed read or write tasks.

Install the optional dependency:

```bash
pip install 'lakesoul[daft]'
```

The examples below use the `events` table created and populated in [Core catalog and table IO](02-core-api.md).

## Read a LakeSoul table

`LakeSoulScan.to_daft()` returns a lazy `daft.DataFrame`:

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
dataframe = (
    catalog.scan(
        "events",
        partitions={"event_date": "2026-08-27"},
        columns=["id", "value"],
    )
    .to_daft()
)

result = dataframe.where(dataframe["value"] >= 50).collect()
result.show()
```

Projection and partition pruning configured on `LakeSoulScan` are preserved when the Daft source is created. Additional Daft operations remain lazy until the DataFrame is executed.

## Write a Daft DataFrame

The target table must already exist and its schema must match the DataFrame output:

```python
import daft

rows = daft.from_pydict(
    {
        "id": [1, 2],
        "event_date": ["2026-08-27", "2026-08-27"],
        "value": [52.0, 81.5],
    }
)

table = catalog.table("events")
result = table.write_daft(rows)
print(result.row_count)
```

Importing `lakesoul.daft` also registers `DataFrame.write_lakesoul`. The registered method accepts a `LakeSoulTable` handle:

```python
import lakesoul.daft

result = rows.write_lakesoul(table)
```

Both calls default to `vortex-compact`. Pass `format="vortex"` or `format="parquet"` when required; see [Physical File Formats](../../01-Getting%20Started/05-physical-file-formats.md).

The Daft tasks use LakeSoul's native writer. The sink commits the produced files to LakeSoul metadata after the distributed write completes.
