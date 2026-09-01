# Vector Search

LakeSoul provides vector similarity search built on an **IVF+RaBitQ** index. The index is stored on the same object store as the table data, is built incrementally as data is written, and is queried through the normal scan API with a query vector.

The Python SDK supports:

- declaring vector columns at table creation;
- automatically building/updating the index after `write_arrow` and `write_daft`;
- approximate nearest-neighbour (ANN) search through `table.scan()` with `reader_options`;
- exact re-ranking of the candidate rows with `rerank_by_distance()`.

## Declare vector columns at table creation

Pass `vector_index=` to `create_table` to declare one or more vector columns. The configuration is stored in the `vector_index_columns` table property as JSON.

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()

schema = pa.schema(
    [
        pa.field("id", pa.uint64(), False),
        pa.field("vec", pa.list_(pa.field("item", pa.float32()), 768), False),
    ]
)

table = catalog.create_table(
    "doc_embeddings",
    path="s3://bucket/path/doc_embeddings",
    schema=schema,
    primary_keys=["id"],
    hash_bucket_num=16,
    vector_index=[
        {
            "column": "vec",
            "dim": 768,
            "nlist": 256,
            "total_bits": 7,
            "metric": "L2",
        }
    ],
)
```

Each entry accepts the following fields:

| Field | Description | Default |
|-------|-------------|---------|
| `column` | Vector column name (required) | - |
| `dim` | Vector dimension (required) | - |
| `nlist` | Number of IVF clusters | 256 |
| `total_bits` | RaBitQ total bits, 1-16 | 7 |
| `metric` | Distance metric, `"L2"` or `"IP"` | `"L2"` |
| `rotator_type` | Rotation, `"FhtKac"` or `"Matrix"` | `"FhtKac"` |
| `seed` | Random seed | 42 |
| `use_faster_config` | Fast-quantization mode | `true` |

Multiple vector columns are supported by passing a list of entries; each column gets its own index.

The configuration is validated when the table is created, before any metadata is written:

- the table must define a primary key, and it must be `UInt64` or `Int64` (the index maps search results to primary key values);
- each vector column must exist and be `FixedSizeList<Float32>` or `List<Float32>`;
- the declared `dim` must match the `FixedSizeList` size.

You may instead pass the same configuration through the raw property:

```python
import json

table = catalog.create_table(
    "doc_embeddings",
    path="s3://bucket/path/doc_embeddings",
    schema=schema,
    primary_keys=["id"],
    properties={
        "vector_index_columns": json.dumps(
            [{"column": "vec", "dim": 768, "nlist": 256, "metric": "L2"}]
        )
    },
)
```

## Automatic index build on write

When a table has vector index properties, `write_arrow` and `write_daft` build or update the index automatically after the files are committed. The native builder detects the existing manifest and writes **delta segments** for the new vectors only, so repeated writes are incremental.

```python
table.write_arrow(batch1)  # first write: builds the base index
table.write_arrow(batch2)  # second write: incremental delta update
```

Pass `auto_build_vector_index=False` to skip the index build for a particular write:

```python
table.write_arrow(batch, auto_build_vector_index=False)
```

`write_daft` builds the index in a distributed manner: the new files are grouped by `(partition, hash bucket)` and each shard is built through a Daft `@daft.cls` actor-pool UDF, with the shard dataframe repartitioned by shard count. Tune the per-shard CPU budget with `vector_index_cpus`:

```python
import daft

rows = daft.from_arrow(batch)
table.write_daft(rows, vector_index_cpus=1)
```

For tables created before this feature existed, or when you want to rebuild from the committed files, call `build_vector_index` explicitly. Parameters default to the table properties and can be overridden:

```python
table.build_vector_index()                      # all columns, params from properties
table.build_vector_index(column="vec", nlist=64)  # override nlist
```

## Vector search

Vector search is triggered through the scan API by setting `reader_options`. Each per-bucket reader runs the ANN search against the matching index and returns the candidate ids; merge the candidates across buckets and re-rank them with exact distances using `rerank_by_distance`.

```python
query = [0.1, 0.2, ...]  # a 768-dim query vector, must match the index dimension

result_table = (
    table.scan()
    .options(
        reader_options={
            "vector_search_query": ",".join(f"{v:.6f}" for v in query),
            "vector_search_top_k": "10",
            "vector_search_nprobe": "64",
        }
    )
    .to_arrow_table()
)

from lakesoul.vector_index import rerank_by_distance

top_k = rerank_by_distance(result_table, query, "vec", 10)
print(top_k.column("id").to_pylist())
```

Supported `reader_options`:

| Key | Description | Default |
|-----|-------------|---------|
| `vector_search_query` | Query vector, comma-separated `f32` values (required) | - |
| `vector_search_column` | Vector column to search | auto-detected when the table has one vector column |
| `vector_search_top_k` | Candidate count per bucket | 10 |
| `vector_search_nprobe` | Number of IVF clusters to probe | 64 |

The vector column and the metric are read from the table properties, so for a single vector column you only need to provide the query. When the table has multiple vector columns, set `vector_search_column` explicitly.

`rerank_by_distance(table, query, vector_column, top_k, metric="L2")` computes the exact L2 or inner-product distance on the candidate rows and returns a `pyarrow.Table` with the true top-`k` rows. Pass `metric="IP"` for inner product tables.

## Vector search through Daft

The same candidate filtering applies when the scan is read through Daft: each per-bucket Daft task runs the ANN search against that bucket's index, so only candidate rows leave the native reader. `lakesoul.daft.vector_search` runs the whole pipeline lazily and distributed — it filters candidates by the index, re-ranks them by exact distance inside Daft, and returns the global top-`k`:

```python
from lakesoul.daft import vector_search

df = vector_search(
    table,                 # LakeSoulTable or LakeSoulScan
    query,                 # query vector, same dimension as the index
    top_k=10,
    nprobe=64,
    # column="vec",        # required only when multiple columns are indexed
    # metric="L2",         # defaults to the table property
    extra_columns=["ts"],  # additional columns to return
)

result = df.collect().to_arrow()
print(result.column("id").to_pylist())  # nearest first, global top-10
```

`vector_search` accepts either a table or a configured scan. When a scan is passed, its partition pruning and runtime options are kept — for example, searching only one range partition. The primary key and the vector column are always read; the returned rows are ordered by distance (nearest first) and the distance column itself is not included.

`scan.to_daft()` with `reader_options` remains available for callers that only need the per-bucket candidates, e.g. to run their own merge logic.

## How it works

- Each table keeps one index per vector column at `{table_path}/_vector_index/{column}/{partition}/{bucket}/`.
- The vector index shard identity is `(partition_desc, hash_bucket_id)`: files from different range partitions are never merged into one shard, so partitioned tables get a separate index per partition.
- Indexes are stored as immutable segments; incremental writes append delta segments without rewriting the base index, and the manifest is updated with a compare-and-swap for concurrent safety.
- See the [core catalog and table IO](02-core-api.md) guide for the general `create_table`, `write_arrow`, and `scan` APIs used above.
