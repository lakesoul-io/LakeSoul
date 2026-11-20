---
sidebar_position: 2
---

# Vector search API

This page is the reference for the LakeSoul vector-search surface. The index is
an IVF + RaBitQ secondary index on a LakeSoul table; rows remain in the table's
ordinary data files and are read through merge-on-read. The same table can be
queried from Python, from DataFusion SQL, and from the pgvector-compatible
`postgres-lakesoul` endpoint.

Start with the [quick start](quickstart.md) if you have not created a table yet.
Index parameters and cache configuration live in
[configuration](configuration.md); measured performance is in
[benchmarks](benchmarks.md).

## Python API

### Catalog and table handles

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(
    namespace="default",
    object_store_options=None,   # defaults to LAKESOUL_OBJECT_STORE_*
)
```

`catalog.table(name, namespace=None)` returns a `LakeSoulTable` handle. The
catalog also exposes convenience wrappers: `catalog.search(table, query, ...)`,
`catalog.search_batch(table, queries, ...)` and
`catalog.drop_vector_index(table, column=None, ...)`.

Useful table properties:

| Attribute | Meaning |
| --- | --- |
| `table.vector_index_columns` | Names of the declared vector columns |
| `table.primary_keys` / `table.hash_bucket_num` | Index shard key and bucket count |
| `table.path` | Table root; the index lives under `{path}/_vector_index/` |
| `table.schema` / `table.properties` | Arrow schema and raw table properties |

### Create a table with vector indexes

```python
table = catalog.create_table(
    "documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,          # embedding is Float32, dim 768
    primary_keys=["id"],
    hash_bucket_num=4,
    vector_index=[{
        "column": "embedding",
        "dim": 768,
        "nlist": 256,
        "total_bits": 7,
        "metric": "L2",
    }],
)
```

`create_table` validates the declaration before writing any metadata:

- the table must have a primary key (the index maps candidate ids back to
  primary-key values; the native batch path additionally requires a single
  `Int64` column named `id`);
- every vector column must exist and hold `Float32` values;
- for a `FixedSizeList` column, the declared `dim` must equal the list size;
- `total_bits` must be `1`, `3` or `7`.

Pass a list of entries to declare several vector columns; each gets its own
index and its own build parameters. The raw form is the table property
`vector_index_columns`, a JSON array of the same entries. It can also be set
through `properties={"vector_index_columns": json.dumps([...])}`.

A table can also be created from SQL (see [SQL DDL](#creating-tables-in-sql)).

### Write and automatic index maintenance

```python
table.write_arrow(batch)                 # commit data, then update the index
table.write_arrow(batch, auto_build_vector_index=False)   # data only
```

`write_arrow` and `write_daft` build or update the index after the data commit
for the affected `(partition, hash bucket)` shards. The native builder detects
the existing manifest and appends **delta segments** only; it never rewrites
the base index for an incremental write.

- A failed index build does not roll back the committed data; the index is a
  derived object.
- For bulk imports, write with `auto_build_vector_index=False` and call
  `table.build_vector_index()` once at the end.
- `write_daft` builds shards in parallel through a Daft actor-pool UDF; tune the
  per-shard CPU budget with `table.write_daft(rows, vector_index_cpus=2)`.
- Tables whose property `index_maintenance` is `"deferred"` commit data only;
  build the missing shards later with `table.build_vector_index()` (or the Rust
  `build_pending_indices` entry point).

After many writes the IVF centroids can become stale. With the default
`rebuild_mode: "auto"` the writers that run automatic maintenance (the native
Rust/DataFusion write path and the connectors) rebuild a shard from all of its
data files once any of its clusters accumulates
`delta_vectors / base_vectors > max_delta_ratio`. The Python SDK's own writes
always append delta segments; call `table.rebuild_vector_index()` explicitly to
re-train from Python.

### Index maintenance

| Call | Purpose |
| --- | --- |
| `table.build_vector_index(column=None, dim=None, nlist=None, total_bits=None, metric=None, rotator_type=None, seed=None, use_faster_config=None, partition_desc=None, partitions=None)` | Build a missing index or update it incrementally |
| `table.rebuild_vector_index(column=None, partition_desc=None, partitions=None)` | Re-cluster shards from all active data files |
| `table.drop_vector_index(column=None, *, partition_desc=None, partitions=None, keep_declaration=None, purge_files=True, dry_run=False, if_exists=True, grace_seconds=0.0, wait_for_cleanup=True)` | Drop the derived index |
| `table.declare_vector_index(configs)` | Re-add or replace declarations after a drop |
| `table.recreate_vector_index(column, dim, nlist=1024, total_bits=7, metric="L2", rotator_type="FhtKac", seed=42, use_faster_config=True, ...)` | Declare + rebuild |
| `table.gc_dropped_vector_index_objects()` | Sweep objects left by a failed or deferred drop; tombstones are kept |
| `lakesoul.vector_index.delete_by_primary_keys(table_name, namespace, vector_column, keys)` | Hide rows from the index without rewriting segments |

`build_vector_index` and `rebuild_vector_index` accept a single
`partition_desc` (for example `"range=2024-01-01"`) or `partitions={"range":
"2024-01-01"}`; omit them to process every partition. A rebuild re-reads all
active data files, runs fresh k-means and publishes a new index generation.

For background jobs, the module-level functions accept the parameters
explicitly:

```python
from lakesoul.vector_index import build_partition_vector_index, build_table_vector_index

build_partition_vector_index(
    table_name="documents", namespace="default",
    partition_desc="range=2024-01-01",
    vector_column="embedding", dim=768,
    nlist=256, total_bits=7, metric="L2",
    rebuild=False,              # True = re-cluster this partition
)

build_table_vector_index(
    table_name="documents", namespace="default",
    vector_column="embedding", dim=768, rebuild=False,
)
```

`store_config` selects the object store for the build
(`{"type": "s3", "bucket": ..., "region": ..., "access_key_id": ...,
"secret_access_key": ...}`); when omitted, the SDK reads the
`LAKESOUL_OBJECT_STORE_*` variables.

`delete_by_primary_keys` writes index tombstones for the given primary keys. It
does **not** delete data rows: delete or update the rows in the table as well,
otherwise a later rebuild re-indexes them from the data files.

#### Drop semantics and the drop report

`drop_vector_index` removes the derived index and leaves data files, partitions,
schema, data commits, CDC tombstones and snapshots untouched. By default the
column declaration is removed from `vector_index_columns`, so later writes do
not rebuild it. Pass `keep_declaration=True` to drop only the physical index and
keep it rebuildable. A partition-scoped drop requires
`keep_declaration=True`.

The call returns a report:

```python
{
    "table_name": "documents", "namespace": "default",
    "columns": ["embedding"], "partitions": ["-5"],
    "shards_dropped": 4, "objects_deleted": 1024, "bytes_deleted": 162010176,
    "declaration_removed": True, "dry_run": False, "epoch": 3,
    "drop_id": "c1b8...", "cleanup": "tombstone_retained",
}
```

| `cleanup` | Meaning |
| --- | --- |
| `completed` | Objects deleted, declaration kept |
| `tombstone_retained` | Objects deleted, declaration removed, tombstone kept |
| `deferred` | Control plane removed; a background job continues object cleanup |

`wait_for_cleanup=False` returns as soon as the control plane is removed and
reports `cleanup="deferred"` plus the `drop_id` of the background job.
`grace_seconds` delays object deletion so readers holding the old commit can
finish. `dry_run=True` reports what would be dropped without changing anything.

#### Rust and DataFusion entry points

```rust
use lakesoul_datafusion::vector_index::{
    DropVectorIndexOptions, VectorIndexGcOptions, drop_vector_index, gc_vector_index,
};

let report = drop_vector_index(
    &client, "documents", "default", &object_store_options,
    DropVectorIndexOptions {
        columns: Some(vec!["embedding".into()]),
        purge_files: true,
        ..Default::default()
    },
).await?;

// grace_seconds: 3600, keep_generations: 1, drop_orphan_shards: true
gc_vector_index(&client, "documents", "default", &object_store_options,
                &VectorIndexGcOptions::default()).await?;
```

`lakesoul_datafusion::index_maintenance::build_pending_indices(client,
table_name, namespace)` builds the shards of a table whose writes deferred index
maintenance. `LakeSoulTable::drop_vector_index(options)` and
`LakeSoulTable::rebuild_vector_index()` are the table-level wrappers.

### Search

```python
result = table.search(
    query,                    # query vector, dimension must match the index
    top_k=10,                 # rows returned
    column=None,              # required when the table has several vector columns
    metric=None,              # "L2" or "IP"; defaults to the index metric
    rerank="index",           # "index" (default) or "exact"
    nprobe=64,                # IVF clusters probed per bucket
    exact_overfetch=None,     # candidates per bucket in exact mode (top_k * this)
    filter=None,              # SQL predicate, e.g. "lang = 'zh'"
    columns=None,             # projection; defaults to every non-vector column
    with_distance=False,      # also return __lakesoul_vector_distance
)
```

Notes:

- Results are ordered by distance: L2 ascending, inner product descending.
- With `rerank="index"` the scan orders candidates by the quantized index
  distance and never reads the stored vector column. With `rerank="exact"` the
  candidates are re-ranked against the stored vectors; `exact_overfetch`
  controls how many candidates are read per bucket (default 10).
- `columns` keeps the fast path: only the rows the limit can return have their
  payload columns fetched (see [deferred payload](#hybrid-search-and-filters)).
- `filter` is pushed down with the scan. Partition predicates prune files;
  other predicates are evaluated on candidates with iterative widening.
- `session=` runs the query on an existing `LakeSoulSession` (see
  [Sessions and output adapters](#sessions-and-output-adapters)).
- `to=` converts the result (`"pyarrow"`, `"arrow"`, `"daft"`, `"ray"`,
  `"duckdb"`).

Pinned reads (`snapshot=`, `tag=` or `as_of=`; at most one) use the table's
time-travel view. The vector index describes the latest files, so pinned reads
skip it and evaluate distances exactly; results stay correct but slower.

### Batch search

```python
batch = table.search_batch(
    [query_vec_1, query_vec_2],
    top_k=10,
    column=None,
    metric=None,
    rerank="index",       # or "exact"
    nprobe=64,
    exact_overfetch=None, # default 10 for the native path
    batch_size=1024,      # queries per native snapshot batch
    parallelism=1,
)
print(batch.ids)          # one primary-key list per query
arrow = batch.to_arrow()  # (query_id, rank, id[, distance])
```

`search_batch` prefers the native snapshot batch: every bucket's index shard is
resolved once and the ranking runs natively for the whole call. The native entry
requires a single `Int64` primary key named `id`; exact mode additionally
requires proven snapshot coverage, an available decoded-block cache and per-file
index ranks. When the table cannot use it, the table method falls back to one
SQL search per query — identical ids, just slower. Call
`session.batch_search(...)` directly to use the native entry and surface its
errors instead of falling back.

### Low-level scan and candidate re-ranking

For callers that need the per-bucket candidates (to merge several sources
themselves), set the reader options on a scan:

```python
candidates = (
    table.scan()
    .options(reader_options={
        "vector_search_column": "embedding",
        "vector_search_query": ",".join(f"{v:.6f}" for v in query),
        "vector_search_top_k": "100",   # per shard
        "vector_search_nprobe": "64",
        "vector_search_metric": "L2",
    })
    .to_arrow_table()
)

from lakesoul.vector_index import rerank_by_distance
top_k = rerank_by_distance(candidates, query, "embedding", 10, metric="L2")
```

| Reader option | Default | Meaning |
| --- | --- | --- |
| `vector_search_column` | required | Vector column to search |
| `vector_search_query` | required | Comma-separated `f32` query vector |
| `vector_search_top_k` | `10` | Candidates per shard (not a global top-k) |
| `vector_search_nprobe` | `64` | IVF clusters probed per bucket |
| `vector_search_metric` | table metric | `L2` or `IP` |
| `vector_search_parallelism` | `1` | Clusters probed concurrently |
| `vector_search_order_by` | `false` | Order by the index distance; adds `__lakesoul_vector_distance` (L2) |
| `vector_search_requests` | – | JSON list of several column queries (`[{"column","query","top_k","weight","metric"}]`); candidate sets are intersected |

`rerank_by_distance(table, query, vector_column, top_k, metric="L2")` computes
exact L2 or inner-product distances over the candidate rows and returns the true
top-k of those candidates.

### Daft integration

`lakesoul.daft.vector_search` and `multi_vector_search` return lazy Daft
DataFrames and run the same index fast path inside the Daft pipeline:

```python
from lakesoul.daft import vector_search, multi_vector_search

df = vector_search(
    table,                    # LakeSoulTable or LakeSoulScan
    query,
    top_k=10,
    nprobe=64,
    extra_columns=["title"],  # optional payload columns
    rerank="exact",           # "index" (default) or "exact"
)

fused = multi_vector_search(
    table,
    {"text_vec": text_query, "image_vec": image_query},
    top_k=10,
    weights={"text_vec": 0.7, "image_vec": 0.3},
    rerank="index",
    with_distance=True,
)
fused.collect().to_arrow()
```

`vector_search` accepts a table or a configured scan (its partition pruning and
runtime options are kept) and returns rows nearest first; the distance column is
not included unless `with_distance=True`. `multi_vector_search` requires a row
to be a candidate in every queried column's index and ranks by the weighted sum
of the per-column distances; all queried columns must use the same metric, and
index-only fusion is L2-only.

### Sessions and output adapters

```python
session = catalog.session(vector_search={"nprobe": 64, "rerank": "index"})

session.set_vector_search(nprobe=128)          # nprobe, rerank, parallelism,
                                               # deferred_payload, exact_overfetch
print(session.sql(sql).to_pydict())
print(session.explain(sql, verbose=True))
```

A `LakeSoulSession` is an in-process read-only DataFusion engine. Reusing the
catalog's session (the default) keeps the metadata, index and block caches warm
across statements. `session.batch_search(...)` is the native batch entry.

## SQL surface

### Creating tables in SQL

DataFusion SQL can create a table and declare its vector indexes in the same
statement:

```sql
CREATE EXTERNAL TABLE "lakesoul".default.documents (
    id        BIGINT NOT NULL PRIMARY KEY,
    title     VARCHAR,
    embedding FLOAT[] NOT NULL
) STORED AS LAKESOUL
LOCATION 's3://my-bucket/warehouse/documents'
OPTIONS (
    'vector_index_columns'
        '[{"column":"embedding","dim":768,"nlist":256,"total_bits":7,"metric":"L2"}]',
    'hashBucketNum' '4'
);
```

`vector_index_columns` is a JSON array, so several vector columns can be
declared at once. `hashBucketNum` selects the hash bucket count; the index is
written to `{LOCATION}/_vector_index/{column}/{partition}/{bucket}/`.

The PostgreSQL endpoint additionally accepts `vector(n)` columns,
`CREATE INDEX ... USING hnsw|ivfflat|gin`, `REINDEX TABLE` and deferred
maintenance; see [Vector and text index DDL](../postgres/index-ddl.md).

### Distance expressions and top-k pushdown

In DataFusion SQL the query vector is an `ARRAY[...]` literal and the distance
is computed by the functions below. The pgvector `vector(n)` type, string
literals and distance operators are provided by the PostgreSQL endpoint (see
[pgwire compatibility](#postgresql-pgwire-compatibility)).

| Expression | Meaning | Index |
| --- | --- | --- |
| `array_distance(vec, ARRAY[...])` | L2 (Euclidean) distance, ascending | Yes |
| `inner_product(vec, ARRAY[...])` (alias `dot_product`) | Inner product; order descending for nearest | Yes |
| `cosine_distance(vec, ARRAY[...])` | Cosine distance | No — exact scan |

```sql
SELECT id, title,
       array_distance(embedding, ARRAY[0.12, 0.98, 0.31]) AS distance
FROM "lakesoul"."default".documents
ORDER BY array_distance(embedding, ARRAY[0.12, 0.98, 0.31])
LIMIT 10;
```

The optimizer recognises `ORDER BY <distance expression> LIMIT k` and pushes
the top-k into the ANN scan; `LIMIT` is required for the index path. For inner
product, order descending:

```sql
SELECT id FROM documents
ORDER BY inner_product(embedding, ARRAY[...]) DESC
LIMIT 10;
```

`EXPLAIN` shows the pushed plan as
`LakeSoulVectorSearchExec(vec=embedding, top_k=10, metric=L2, order_by=false)`.
Without a usable index, or for an unsupported shape or metric, the engine falls
back to an exact scan: results stay correct, latency grows with the row count.

### Session settings

```sql
SET lakesoul_vector_search.nprobe = 64;
SET lakesoul_vector_search.rerank = 'index';      -- or 'exact'
SET lakesoul_vector_search.parallelism = 4;
SET lakesoul_vector_search.deferred_payload = false;
SET lakesoul_vector_search.exact_overfetch = 10;
```

| Setting | Default | Meaning |
| --- | --- | --- |
| `lakesoul_vector_search.nprobe` | `64` | IVF clusters probed per bucket |
| `lakesoul_vector_search.rerank` | `index` | `index` orders by the quantized distance; `exact` re-ranks candidates by the stored vectors |
| `lakesoul_vector_search.parallelism` | `1` | Clusters probed concurrently; results are unchanged |
| `lakesoul_vector_search.deferred_payload` | `true` | Two-phase read of payload columns |
| `lakesoul_vector_search.exact_overfetch` | `10` | Candidates per bucket in exact mode (`top_k * this`) |

These settings are connection-local. The `postgres-lakesoul` endpoint also
accepts pgvector's name for the probe count, `SET ivfflat.probes = N`.

### Multiple indexed columns in SQL

A weighted sum of `array_distance` calls is recognised as a multi-column search:

```sql
SELECT id
FROM documents
ORDER BY 0.7 * array_distance(text_vec, ARRAY[...])
       + 0.3 * array_distance(image_vec, ARRAY[...])
LIMIT 10;
```

Each column is searched independently, candidates are intersected, and rows are
ranked by the weighted sum. Every term must be an `array_distance` over a
declared vector column and all terms must share the metric; with
`lakesoul_vector_search.rerank = 'index'` the fused index distance is used (L2
only). `EXPLAIN VERBOSE` shows the fused node as
`LakeSoulVectorSearchExec(vec=text_vec+image_vec, ...)`.

### Native batch SQL

The table functions `lakesoul_vector_search_batch` and
`lakesoul_vector_search_batch_from_table` run many queries in one SQL request
over one table snapshot and one set of read leases, with bounded parallelism.
Both are registered in DataFusion and in `postgres-lakesoul`.

```sql
-- Inline JSON: query_id is the array position, starting at 0.
SELECT query_id, rank, id, distance
FROM lakesoul_vector_search_batch(
    'lakesoul.default.documents',   -- pgwire: public.documents
    'embedding',
    '[[0.1,0.2,0.3],[0.4,0.5,0.6]]',
    10,        -- top_k
    64,        -- nprobe (default 64)
    'L2',      -- auto (default), L2, IP
    'index',   -- index (default) or exact
    10         -- exact_overfetch (default 10)
)
ORDER BY query_id, rank;

-- Query table: many queries without embedding them in the SQL text.
SELECT query_id, rank, id, distance
FROM lakesoul_vector_search_batch_from_table(
    'lakesoul.default.documents', 'embedding',
    'lakesoul.default.query_vectors', 'qid', 'qvec',
    100, 64, 'L2', 'exact', 1
)
ORDER BY query_id, rank;
```

Output schema (fixed): `query_id BIGINT`, `rank INTEGER` (from 1), `id BIGINT`,
`distance DOUBLE PRECISION` (smaller is closer; L2 is Euclidean, IP is the
negative inner product). The stream is unordered; add `ORDER BY query_id,
rank`. The functions accept constant arguments only — no `$1` vector
placeholders; use the query-table form for large inputs.

Limits: at most 65,536 queries per request, input vectors at most 64 MiB, and a
single query window of at most 256 queries. Tune the native path per session:

```sql
SET lakesoul_vector_batch.query_window = 256;
SET lakesoul_vector_batch.parallelism = 0;      -- 0 = process default
SET lakesoul_vector_batch.io_concurrency = 8;
SET lakesoul_vector_batch.memory_bytes = 134217728;
```

The native batch entry requires a single `Int64` primary key named `id`, an
index that covers the snapshot exactly, and no CDC/mutation chain; when it
cannot prove that, it rejects the request instead of silently degrading.
Different range partitions may contain the same `id`; they are distinct rows
and are not de-duplicated by id.

### Dropping an index from SQL

SQL uses a scalar DDL function, not `ALTER TABLE`:

```sql
SELECT lakesoul_drop_vector_index(
    'documents',  -- table
    'default',    -- namespace
    'embedding',  -- column
    false,        -- keep_declaration
    true,         -- purge_files
    0.0,          -- grace seconds
    true,         -- wait_for_cleanup
    false         -- dry_run
);
```

It returns one JSON string with the same fields as the Python report.
Destructive calls require `LAKESOUL_ALLOW_INDEX_DDL=1` in the server
environment; without it only `dry_run=true` is allowed. SQL access is also
subject to the PostgreSQL connection's permissions.

## PostgreSQL (pgwire) compatibility

`postgres-lakesoul` serves the pgvector SQL dialect over LakeSoul tables.
Tables can be created, written and indexed through the endpoint. The DDL side —
`vector(n)`, `vector_index_columns`, `CREATE INDEX`, `ALTER TABLE` declarations,
`REINDEX TABLE` and deferred maintenance — is collected in
[Vector and text index DDL](../postgres/index-ddl.md).

```python
import psycopg

conn = psycopg.connect("host=127.0.0.1 port=5433 user=lakesoul_test dbname=default")
# 5433 is the port postgres-lakesoul was started with (see the PostgreSQL quick start)
conn.execute("SET ivfflat.probes = 64")
rows = conn.execute(
    "SELECT id FROM documents ORDER BY embedding <-> '[1,0,0,0]' LIMIT 3"
).fetchall()
```

Supported: the `vector(n)` type and `'[0.1,0.2]'` literals (also as prepared
parameters, `$1::vector(n)`, with `register_vector`; a `CAST(... AS vector(n))`
constructs a vector value in a query), the operators `<->` (L2), `<#>` (negative inner product) and `<=>` (cosine distance), and the
connection-local `SET`/`SHOW`/`RESET ivfflat.probes` setting, which maps to
`lakesoul_vector_search.nprobe`. Only L2 and inner product use the index —
`<=>` is evaluated exactly. The native batch functions are available with the
same names; in a pgwire session, refer to tables as `public.<table>`.

## Hybrid search and filters

Structured filters and vector similarity run in one plan:

```sql
SELECT id, title, embedding <-> $1 AS distance
FROM documents
WHERE tenant_id = 'acme' AND category = 'contract'
ORDER BY embedding <-> $1
LIMIT 10;
```

The ANN index supplies candidates, the filter is evaluated on them, and the
engine overfetches to compensate:

- In index mode the scan fetches roughly `2 × k` candidates per shard; with a
  filter it fetches roughly `10 × k`, because some candidates are dropped.
- In exact mode it fetches `top_k × exact_overfetch` per bucket (default
  `10 ×`) and re-ranks them by the stored vectors; the SQL path uses at least
  100 candidates per shard.
- A very selective filter can return fewer than `k` rows when the candidate
  pool does not contain `k` matching rows. Raise `nprobe`, prune partitions or
  buckets in the predicate, or fall back to an exact scan. The engine does not
  silently change the query shape — check `EXPLAIN`.

Payload columns are read in a second pass when the plan can keep the limit:
the scan ranks candidates on the primary key and vector columns, keeps the rows
the `LIMIT` can return, and only then fetches their payload columns. A filter
above the scan disables that second pass and reads each candidate once. Set
`lakesoul_vector_search.deferred_payload = false` to turn the second pass off
globally.

### Choosing a metric

- Use L2 (`array_distance`, `<->`) when the embedding model was trained with
  Euclidean distance.
- Use inner product (`inner_product`, `<#>`) for maximum-inner-product search;
  order the SQL expression descending.
- For cosine similarity, normalize vectors at write time and search with inner
  product so the index can be used; `cosine_distance` / `<=>` are evaluated
  exactly and do not use the index.

## Partitions and hash buckets

The index is built per `(partition_desc, hash_bucket_id)` shard. Each shard keeps
its own IVF centroids and RaBitQ codes, and a query probes only the shards that
can contain matching rows:

- Range partitions are never merged into one shard; a partitioned table gets a
  separate index per partition. Rebuild and drop accept `partition_desc=` to
  target one partition.
- `hash_bucket_num` (default 4) splits each partition into hash buckets that are
  probed in parallel. Choose 4–16 for large tables and 1–2 for small tables.
- A bucket may contain several data files; the native exact batch supports
  multiple files per bucket when it can prove index coverage of the snapshot.

## Supported types and limits

| Item | Supported |
| --- | --- |
| Primary key | Required. A single `Int64` column named `id` is required by the native batch path and other native fast paths; other primary-key shapes use the generic scan path |
| Vector column (Python SDK) | `FixedSizeList<Float32, dim>` or `List<Float32>`; `dim` must match |
| Vector column (Rust/DataFusion IO) | `Float16`, `Float32` or `Float64` values are widened to `Float32` for the index |
| Metrics | `L2` and `IP` use the index; cosine (`<=>`, `cosine_distance`) is evaluated exactly |
| `total_bits` | `1`, `3` or `7` |
| `nlist` | IVF cluster count, default 256; larger values build slower and need higher `nprobe` |
| `nprobe` | Default 64; raise it for recall, lower it for latency |
| `top_k` | Global top-k after merging buckets; ties are not ordered deterministically |
| Query vector | The dimension must match the index dimension; a mismatch is an error |

Results may contain fewer than `k` rows after filtering, deletes or weak ANN
recall. Pinned (time-travel) reads bypass the index. ANN search is approximate;
there is no GPU path and no cross-table ANN.

## Index lifecycle, leases and tombstones

Vector indexes are derived objects with their own control plane and object
files; they can be built late, rebuilt or dropped without affecting data
correctness.

- **Generation.** A build publishes a new index generation through the metadata
  catalog; the manifest is updated atomically (compare-and-swap) and readers
  pick up the latest generation on their next statement. A rebuild publishes a
  fresh generation and the old one is garbage-collected later.
- **Drop tombstone.** Dropping with `keep_declaration=False` records an
  `index_drop` tombstone per prefix. It implies `IndexDropped` for the builder,
  so a stale writer cannot recreate the index, and data writes keep succeeding.
  `declare_vector_index`, `recreate_vector_index` and
  `rebuild_vector_index()` clear the tombstone; `gc_dropped_vector_index_objects`
  only sweeps objects and keeps the tombstone.
- **Epoch and caches.** Every drop or re-declare bumps the table property
  `vector_index_epoch`. The local metadata cache, snapshot cache, L1 index cache
  and lazy-shard cache are invalidated for the affected prefixes. Other nodes
  observe the new epoch within `LAKESOUL_METADATA_TTL_MS` (when enabled).
- **Read leases.** A query holds leases that pin the index commit and the data
  files it reads, so cleanup cannot delete them underneath it. Reader leases
  expire after `LAKESOUL_VECTOR_INDEX_LEASE_TTL_SECS` (default 300 s). The
  native batch path uses a request-level `index_read_lease` with a 120 s TTL
  renewed every 30 s; cancellation or a failed renewal cancels the request.
  Physical object cleanup waits for active leases or the grace period.
- **Garbage collection.** After each write the engine may delete superseded
  generations older than `gc_grace_seconds` (default 3600) while keeping
  `gc_keep_generations` (default 1). Write-path GC is amortized every
  `LAKESOUL_INDEX_GC_EVERY` writes (default 16) unless the grace period is 0.
- **Table drop.** `drop_table` cleans the index control plane (shard, commit,
  lease and drop rows); it does not delete the table's object-store directory.

Semantics of index maintenance failures: a failed index build leaves the data
committed and the index unchanged; queries fall back to an exact scan for the
files the index does not cover.

## Next

- [Configuration](configuration.md) — index parameters, cache tiers and
  environment variables.
- [Benchmarks](benchmarks.md) — measured recall and throughput.
- [Quick start](quickstart.md) — an end-to-end example.
