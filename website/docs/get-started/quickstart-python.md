---
sidebar_position: 3
---

# Quick start: Python

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

The Python SDK reads and writes LakeSoul tables through the native Rust IO layer.
It does not go through the PostgreSQL endpoint, so a scan can be sharded across
Daft, Ray or PyTorch workers without a SQL gateway.

## Install

```bash
pip install lakesoul
```

Install only the integrations you need:

```bash
pip install 'lakesoul[pandas,duckdb,torch,datasets,ray,daft]'
```

Python 3.10 or later is required.

## Point it at metadata

The SDK uses the same metadata environment variables as the engine:

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
```

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(namespace="default")
print(catalog.list_tables())
```

## Read

A `LakeSoulScan` is lazy: no data is read until an output method is called.

```python
import pyarrow.compute as pc

scan = (
    catalog.scan("orders")
    .select("id", "region", "amount")
    .filter(pc.field("amount") >= 100)
)

table = scan.to_arrow_table()   # materialized pyarrow.Table
dataset = scan.to_arrow_dataset()  # lazy pyarrow.dataset.Dataset
```

The same scan can be handed to a data or ML framework:

```python
scan.to_daft()    # Daft DataFrame
scan.to_ray()     # Ray Data Dataset
scan.to_torch()   # PyTorch IterableDataset
scan.to_huggingface()
```

`scan.shard(rank, world_size)` assigns one shard to a worker when you drive the
distribution yourself.

## Write

Load a table, then write Arrow data through the native writer:

```python
import pyarrow as pa

table = catalog.table("orders")
result = table.write_arrow(
    pa.table(
        {
            "id": [2, 3],
            "region": ["EMEA", "AMER"],
            "amount": [88.50, 120.00],
        },
        schema=table.schema,
    )
)
print(result.row_count, result.files)
```

For primary-key tables, writes are sparse upserts: supply the primary-key and
partition columns, and the columns you want to change.

`table.write_ray(dataset)` and `table.write_daft(dataframe)` commit after all
distributed write tasks succeed.

## Create a table

```python
import pyarrow as pa

catalog.create_table(
    name="events",
    path="s3://my-bucket/warehouse/events",
    schema=pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("event_date", pa.string()),
        pa.field("value", pa.float64()),
    ]),
    primary_keys=["id"],
    partition_by=["event_date"],
    hash_bucket_num=16,
)
```

Vector and text indexes are declared on the same call:

```python
catalog.create_table(
    name="documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,
    primary_keys=["id"],
    vector_index=[{"column": "embedding", "dim": 768, "metric": "L2"}],
    text_index=[{"column": "body"}],
)
```

`embedding` must be a `Float32` column with the declared dimension. Index metrics
are `L2` and `IP`; cosine search is evaluated exactly.

## Next

- [Python SDK overview](../python/overview.md)
- [Core API](../python/core-api.md)
- [PyTorch and Hugging Face](../python/pytorch-huggingface.md)
- [Vector search from Python](../python/vector-search.md)
