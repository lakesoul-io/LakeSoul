---
sidebar_position: 1
sidebar_label: Quick start
---

# Vector search quick start

LakeSoul keeps the vector index next to the table data. The index is an
**IVF + RaBitQ** structure stored under `{table_path}/_vector_index/...` in the
same object store as the rows, with the same commits and snapshots as the
table. There is no separate vector database to operate.

- **Lake-resident.** The index is a secondary index on a LakeSoul table; data
  files stay ordinary columnar files.
- **Incremental.** A write appends immutable delta segments to the affected
  shards; a shard is re-clustered from all of its data files only when its
  accumulated deltas make the centroids stale.
- **Two search orders.** `rerank="exact"` re-ranks the candidates by the stored
  vectors; `rerank="index"` (the default) orders them by the quantized index
  distance and never reads the vector column.
- **pgvector-compatible SQL.** The PostgreSQL endpoint answers `vector`
  literals, `<->`, `<#>`, `<=>` and `SET ivfflat.probes` over the same tables.

This page takes you from an empty metadata database to searchable rows in about
ten minutes. The [API reference](api.md), [configuration](configuration.md) and
[benchmarks](benchmarks.md) go deeper.

## Before you start

- **Python 3.11+** with the LakeSoul SDK: `pip install lakesoul`.
- **PostgreSQL metadata** reachable from the client through `LAKESOUL_PG_URL`,
  `LAKESOUL_PG_USERNAME` and `LAKESOUL_PG_PASSWORD`.
- **An object store** for the table data. A local directory (`file:///...`) is
  enough for a first run; S3/MinIO credentials are read from
  `LAKESOUL_OBJECT_STORE_*` or passed explicitly to the catalog.
- A table design with a **primary key** (`Int64` or `UInt64`) and a **Float32
  vector column** whose declared `dim` matches the column length.

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME='lakesoul_test'
export LAKESOUL_PG_PASSWORD='lakesoul_test'
```

## Create a table with a vector column

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(namespace="default")

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("title", pa.string()),
    pa.field("embedding", pa.list_(pa.field("item", pa.float32()), 8), nullable=False),
])

table = catalog.create_table(
    "docs",
    path="file:///tmp/lakesoul-docs",
    schema=schema,
    primary_keys=["id"],
    hash_bucket_num=2,
    vector_index=[{
        "column": "embedding",
        "dim": 8,
        "nlist": 2,
        "total_bits": 7,
        "metric": "L2",
    }],
)
```

`vector_index` is serialized into the `vector_index_columns` table property.
The declaration is validated before any metadata is written: the table must have
a primary key (the example uses a single `Int64` `id`), the column must be
`Float32`, `dim` must match its length, and `total_bits` must be `1`, `3` or
`7`. Pass a list of entries to declare several vector columns. All parameters
and their defaults are listed in
[configuration](configuration.md#index-build-parameters).

Use `path="s3://bucket/prefix/docs"` for object storage. The index is written
next to the data under that prefix.

## Write rows

```python
import numpy as np

rows = pa.table({
    "id": pa.array([1, 2, 3, 4], type=pa.int64()),
    "title": pa.array(["cat", "kitten", "airplane", "car"]),
    "embedding": pa.FixedSizeListArray.from_arrays(
        pa.array(
            np.array([
                [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            ], dtype=np.float32).reshape(-1),
            type=pa.float32(),
        ),
        8,
    ),
})

table.write_arrow(rows)   # commit data files, then maintain the index
```

The write commits data files first and then builds or updates the index for the
affected `(partition, hash bucket)` shards; a failed index build does not roll
back the committed data. For bulk loads, write with
`auto_build_vector_index=False` and build once at the end:

```python
for batch in batches:
    table.write_arrow(batch, auto_build_vector_index=False)
table.build_vector_index()
```

## Search

```python
query = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

# Exact re-rank: candidates from the index, ordered by the stored vectors.
exact = table.search(query, top_k=3, rerank="exact")
print(exact.to_pydict())

# Index-only: ordered by the quantized index distance, no vector-column reads.
approx = table.search(query, top_k=3, rerank="index")
print(approx.column("id").to_pylist())

# Batch: many queries share one snapshot, index load and ranking pass.
batch = table.search_batch(
    [query, [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]],
    top_k=3,
)
print(batch.ids)
```

`table.search` is a lakehouse query: it follows the table snapshot,
merge-on-read semantics and partition pruning, and uses the vector index when
the index can serve the query. Results are ordered by distance — L2 ascending,
inner product descending. Increase `nprobe=` (clusters probed) for higher
recall at higher latency.

## The same query in SQL

In-process DataFusion through the Python session:

```python
session = catalog.session(vector_search={"nprobe": 2})

sql = """
SELECT id, title,
       array_distance(embedding, ARRAY[1,0,0,0,0,0,0,0]) AS distance
FROM "lakesoul"."default"."docs"
ORDER BY array_distance(embedding, ARRAY[1,0,0,0,0,0,0,0])
LIMIT 3
"""
print(session.sql(sql).to_pydict())
print(session.explain(sql))   # look for LakeSoulVectorSearchExec
```

Through the `postgres-lakesoul` endpoint, the same query uses pgvector syntax:

```sql
SET ivfflat.probes = 2;

SELECT id, title
FROM docs
ORDER BY embedding <-> '[1,0,0,0,0,0,0,0]'
LIMIT 3;
```

`<->` maps to `array_distance`, `<#>` to the negative inner product and `<=>`
to cosine distance (evaluated exactly, without the index). `ivfflat.probes` is
the same session option as `lakesoul_vector_search.nprobe`. The endpoint is
read-only: create and write tables through the SDK or the other engines.

## Maintain the index

```python
table.rebuild_vector_index()                  # re-cluster shards from their data
table.drop_vector_index()                     # drop the index; the data stays
table.recreate_vector_index(column="embedding", dim=8, nlist=2)
```

`rebuild_vector_index()` re-reads all active data files of each shard, trains
fresh k-means centroids and publishes a new index generation atomically —
searches keep being served while the rebuild runs. `drop_vector_index()`
removes only the derived index, never data files, partitions or snapshots.
Both accept `column=` and `partition_desc=`.

## Clean up

```python
catalog.drop_table("docs", if_exists=True)
```

## Next

- [Architecture and caching](architecture.md) — how the index, query path and the three-tier cache fit together.

- [API reference](api.md) — the complete Python, SQL, pgwire and Rust surface.
- [Configuration](configuration.md) — index build parameters, cache tiers and
  runtime environment variables.
- [Benchmarks](benchmarks.md) — measured recall and throughput.
