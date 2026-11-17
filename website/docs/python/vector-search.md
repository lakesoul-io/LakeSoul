---
sidebar_position: 7
---

# Vector Search

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul's Python SDK can declare a vector column on a table, maintain an
**IVF + RaBitQ** index as data is written, and run approximate nearest-neighbour
search through the normal scan API. The index lives in the same object store as
the table data and follows the table's snapshot and merge-on-read semantics.

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(namespace="default")

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("title", pa.string()),
    pa.field("vec", pa.list_(pa.field("item", pa.float32()), 768), nullable=False),
])

table = catalog.create_table(
    "doc_embeddings",
    path="s3://bucket/path/doc_embeddings",
    schema=schema,
    primary_keys=["id"],
    vector_index=[{"column": "vec", "dim": 768, "metric": "L2"}],
)

table.write_arrow(rows)          # commits data and maintains the index

hits = table.search(query, top_k=10, rerank="exact")
print(hits.to_pydict())
```

- `create_table(..., vector_index=[...])` stores the configuration in the
  `vector_index_columns` property; `write_arrow` and `write_daft` then build or
  incrementally update the index for the affected shards.
- `table.search` returns an Arrow table ordered by distance. The default
  `rerank="index"` never reads the vector column; `rerank="exact"` re-ranks the
  candidates against the stored vectors. `table.search_batch` runs many queries
  against one snapshot.
- `table.build_vector_index()`, `table.rebuild_vector_index()` and
  `table.drop_vector_index()` maintain the derived index without touching table
  data.

The full Python, SQL and pgwire surface is in the
[vector-search API reference](../engine/vector-search/api.md); index parameters
and caching are in [configuration](../engine/vector-search/configuration.md) and
measured performance is in [benchmarks](../engine/vector-search/benchmarks.md).
For a step-by-step introduction, start with the
[vector-search quick start](../engine/vector-search/quickstart.md).
