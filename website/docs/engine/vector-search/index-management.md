---
sidebar_position: 3
---

# Vector index management

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

Vector indexes are declared and built through the Python SDK. The index lives
under the table path, in the same object store as the data, and is maintained as
rows are written.

## Declare an index

```python
catalog.create_table(
    name="documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,
    primary_keys=["id"],
    vector_index=[{
        "column": "embedding",
        "dim": 768,
        "metric": "L2",
        "nlist": 256,
    }],
)
```

The configuration is stored on the table; `write_arrow` then builds or updates
the index after the data commit.

| Key | Default | Meaning |
| --- | --- | --- |
| `column` | required | `Float32` column with the vector |
| `dim` | required | Vector dimension |
| `metric` | `L2` | `L2` or `IP` (inner product) |
| `nlist` | `256` | IVF cluster count |
| `total_bits` | `7` | RaBitQ bits |
| `rotator_type` | `FhtKac` | `FhtKac` or `Matrix` |
| `seed` | `42` | Rotation seed |
| `use_faster_config` | `True` | Faster quantization |

The table needs an `Int64` or `UInt64` primary key.

## Build or rebuild

For an existing table, build the index directly:

```python
from lakesoul.vector_index import build_table_vector_index

build_table_vector_index(
    table_name="documents",
    namespace="default",
    vector_column="embedding",
    dim=768,
    metric="L2",
    nlist=256,
)
```

Per partition:

```python
from lakesoul.vector_index import build_partition_vector_index

build_partition_vector_index(
    table_name="documents",
    namespace="default",
    partition_desc="range=2026-01-01",
    vector_column="embedding",
    dim=768,
    metric="L2",
)
```

Use `rebuild=True` to retrain the IVF centroids over all active data files. This
is the right operation after the data distribution drifts; without it, new rows
are appended as delta segments and the existing centroids are reused.

## Shards

The index is built per (partition, hash bucket) shard. Each shard has its own
centroids and RaBitQ codes; a query probes the shards that can contain matching
rows and merges the candidates.

`store_config` accepts object store credentials; when omitted, the SDK reads the
`LAKESOUL_OBJECT_STORE_*` environment variables.

## Maintenance

- After an `INSERT` or a Python write, the index is updated. Index maintenance
  runs after the metadata commit; a failed index build returns an error but does
  not roll back the data.
- A shard accumulates immutable delta segments. When the deltas make the
  centroid stale, the shard is rebuilt from its data files.
- `rebuild=True` forces a full rebuild of every shard.

## Next

- [Vector SQL reference](sql-reference.md)
- [Hybrid search](hybrid-search.md)
- [Benchmarks](benchmarks.md)
