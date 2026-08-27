# PyArrow, Pandas, and DuckDB

PyArrow is the SDK's common interchange layer. A single `LakeSoulScan` can stream Arrow batches, materialize a table for Pandas, or expose an Arrow Dataset to DuckDB.

Install the required optional dependencies:

```bash
pip install 'lakesoul[pandas,duckdb]'
```

The examples below use the `events` table created and populated in [Core catalog and table IO](02-core-api.md).

## PyArrow Dataset

```python
import pyarrow.compute as pc

from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
scan = catalog.scan(
    "events",
    partitions={"event_date": "2026-08-27"},
    columns=["id", "value"],
    filter=pc.field("value") >= 50,
)
dataset = scan.to_arrow_dataset()

for batch in dataset.to_batches():
    print(batch.to_pydict())
```

The scan carries partition pruning into the LakeSoul scan plan. Projection and supported PyArrow expressions are applied by the Arrow scanner.

You can also configure projection and filtering on the returned Dataset:

```python
scanner = dataset.scanner(
    columns=["id"],
    filter=pc.field("value") >= 50,
)
table = scanner.to_table()
```

## Pandas

Convert only when the result fits in memory:

```python
dataframe = scan.to_arrow_table().to_pandas()
```

For larger tables, process `scan.to_batches()` incrementally instead of constructing one Pandas DataFrame for the complete table.

## DuckDB

DuckDB can query the PyArrow Dataset returned by LakeSoul:

```python
import duckdb

lake_dataset = scan.to_arrow_dataset()
connection = duckdb.connect()
result = connection.sql(
    "SELECT id, value FROM lake_dataset WHERE value >= 75"
)

print(result.fetchall())
```

DuckDB resolves `lake_dataset` from the Python scope. Keep the variable alive while executing the query.

This integration uses DuckDB's Arrow Dataset support; it is not a LakeSoul-specific DuckDB connector. LakeSoul performs table snapshot resolution and merge-on-read, while DuckDB executes the SQL query over the resulting Arrow Dataset.
