# LakeSoul Python SDK

The `lakesoul` package is LakeSoul's Python SDK. It provides one catalog API for table metadata and IO, then exposes adapters that hand the same LakeSoul scan to PyArrow, Pandas, DuckDB, PyTorch, Hugging Face Datasets, Ray Data, and Daft.

Use the Python SDK when a Python application needs to:

- discover, create, load, or drop LakeSoul tables;
- read a table lazily with partition, column, and row pruning;
- write PyArrow data and commit the resulting files to LakeSoul metadata;
- pass a configured scan to a supported data or machine-learning framework.

## Requirements and installation

The current Python package requires Python 3.10 or later.

Install the core SDK:

```bash
pip install lakesoul
```

Install only the adapters required by your application:

```bash
pip install 'lakesoul[pandas]'
pip install 'lakesoul[duckdb]'
pip install 'lakesoul[torch]'
pip install 'lakesoul[datasets]'
pip install 'lakesoul[ray]'
pip install 'lakesoul[daft]'
```

To install every optional integration:

```bash
pip install 'lakesoul[all]'
```

## Configure metadata access

Start a LakeSoul environment as described in [Set up a local environment](../../01-Getting%20Started/01-setup-local-env.md), then configure the metadata connection:

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://localhost:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME='lakesoul_test'
export LAKESOUL_PG_PASSWORD='lakesoul_test'
```

`LakeSoulCatalog.from_env()` reads these variables:

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
print(catalog.list_namespaces())
print(catalog.list_tables())
```

Applications may instead pass PostgreSQL settings explicitly:

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog(
    pg_url="postgresql://localhost:5432/lakesoul_test",
    pg_username="lakesoul_test",
    pg_password="lakesoul_test",
    namespace="default",
)
```

Do not embed production credentials in source code. Use environment variables or the secret-management mechanism of the deployment platform.

## SDK model

The public API has three main objects:

- `LakeSoulCatalog`: metadata connection and entry point for namespaces and tables;
- `LakeSoulTable`: a loaded table, including schema, partitioning, and write operations;
- `LakeSoulScan`: an immutable, lazy read configuration that can be refined and converted to an ecosystem-specific object.

```text
LakeSoulCatalog
    | table("events") / scan("events")
    v
LakeSoulTable ---- write_arrow / write_ray / write_daft
    |
    | scan(partitions=..., columns=..., filter=...)
    v
LakeSoulScan
    |-- to_arrow_dataset / to_arrow_table / to_batches
    |-- to_torch / to_huggingface
    |-- to_ray
    `-- to_daft
```

Continue with [Core catalog and table IO](02-core-api.md), then choose the integration used by your application:

- [PyArrow, Pandas, and DuckDB](03-arrow-pandas-duckdb.md)
- [PyTorch and Hugging Face Datasets](04-pytorch-huggingface.md)
- [Ray Data](05-ray.md)
- [Daft](06-daft.md)

For Spark SQL and DataFrame usage, use the separate [Spark setup guide](../02-setup-spark.md) and [Spark API documentation](../03-spark-api-docs.md).

More runnable examples are available under [`python/examples`](https://github.com/lakesoul-io/LakeSoul/tree/main/python/examples).
