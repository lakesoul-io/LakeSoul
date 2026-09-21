# Text Search

LakeSoul provides full-text search built on a **Tantivy** index. The index is stored on the same object store as the table data, is built incrementally as data is written, and is queried through the scan API, through Daft, or through the DataFusion SQL engine.

Because a row may be updated after it was indexed, the index can contain stale versions of it. Every search therefore runs an exact verification pass over the **current** rows after merge-on-read, so the rows returned are always true matches.

The Python SDK supports:

- declaring text columns at table creation;
- automatically building/updating the index — and compacting drifted shards — after `write_arrow` and `write_daft`;
- exact full-text filtering through `table.scan()` with `reader_options`;
- text search through `lakesoul.daft.text_search`;
- manual (re)building with `build_text_index()`.

## Declare text columns at table creation

Pass `text_index=` to `create_table` to declare one or more text columns. The configuration is stored in the `text_index_columns` table property as JSON.

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()

schema = pa.schema(
    [
        pa.field("id", pa.uint64(), False),
        pa.field("body", pa.string(), False),
    ]
)

table = catalog.create_table(
    "documents",
    path="s3://bucket/path/documents",
    schema=schema,
    primary_keys=["id"],
    hash_bucket_num=16,
    text_index=[
        {
            "column": "body",
            "tokenizer": "jieba",
        }
    ],
)
```

Each entry accepts the following fields:

| Field | Description | Default |
|-------|-------------|---------|
| `column` | Text column name (required) | - |
| `tokenizer` | Analyzer: `"jieba"`, `"default"`, `"en_stem"`, `"whitespace"`, or `"raw"` | `"jieba"` |
| `with_positions` | Index term positions so phrase queries work; disabling shrinks the index but drops phrase support | `true` |
| `stored` | Also store the original text in the index (needed for snippets/highlighting, not for search) | `false` |
| `rebuild_mode` | Compaction policy: `"auto"` rebuilds a shard once its delta history outweighs its compacted base; `"none"` only appends delta splits | `"auto"` |
| `max_delta_ratio` | Compaction trigger: `delta_documents / base_documents` of a shard | `1.0` |

`jieba` is Chinese word segmentation followed by lowercasing, so mixed Chinese/English text matches case-insensitively. The other analyzers are Tantivy's built-ins (`default` tokenizes on word boundaries and lowercases, `en_stem` additionally applies an English stemmer).

Multiple text columns are supported by passing a list of entries; each column gets its own index.

The configuration is validated when the table is created, before any metadata is written:

- the table must define a primary key, and it must be `UInt64` or `Int64` (the index maps search results to primary key values);
- each text column must exist and be `Utf8`, `LargeUtf8`, or `Utf8View`;
- the tokenizer must be one of the supported names;
- `rebuild_mode` must be `"auto"` or `"none"`, and `max_delta_ratio` must be positive.

You may instead pass the same configuration through the raw property:

```python
import json

table = catalog.create_table(
    "documents",
    path="s3://bucket/path/documents",
    schema=schema,
    primary_keys=["id"],
    properties={
        "text_index_columns": json.dumps(
            [{"column": "body", "tokenizer": "jieba", "with_positions": True}]
        )
    },
)
```

### Compaction policy (folding delta splits)

Every write appends one split per shard; superseded and deleted rows stay in the older splits until the shard is rebuilt. Two fields control how this is handled:

- `rebuild_mode`: `"auto"` (default) or `"none"`. In `"auto"` mode, after a write's delta build the shard is rebuilt from **all** of its active data files into a single fresh split once `(total_documents - base_documents) / base_documents > max_delta_ratio`, where the base is the split published by the last rebuild. Set it to `"none"` to only append deltas.
- `max_delta_ratio`: the threshold (default `1.0`, meaning "about as many delta documents as the base holds"). Compaction amortizes its own cost: writing roughly as much data as the base holds triggers the next rebuild.

```python
text_index=[
    {
        "column": "body",
        "rebuild_mode": "auto",   # "auto" (default) or "none"
        "max_delta_ratio": 1.0,   # compact when delta/base exceeds this
    }
]
```

Stale splits are harmless for correctness — the exact verification pass drops their rows — and the rebuild removes them for good: splits are written with Tantivy `delete_term`, so only the newest version of each primary key survives in the new split.

## Automatic index build on write

When a table has text index properties, `write_arrow` and `write_daft` build or update the index automatically after the files are committed, and compact the shards that drifted. Each write appends a **delta split** for the new files, so repeated writes are incremental.

```python
table.write_arrow(batch1)  # first write: builds the base index
table.write_arrow(batch2)  # second write: incremental delta split
```

Pass `auto_build_index=False` to skip the index work for a particular write:

```python
table.write_arrow(batch, auto_build_index=False)
```

`write_daft` builds the index in a distributed manner: the new files are grouped by `(partition, hash bucket)` and each shard is built through a Daft `@daft.cls` actor-pool UDF. Compaction of drifted shards then runs in the driver process. The skip flag keeps its legacy name there:

```python
table.write_daft(rows, auto_build_vector_index=False)
```

`write_ray` does **not** build secondary indexes. Call `build_text_index()` explicitly after a Ray write, or write through `write_arrow`/`write_daft`.

For tables created before this feature existed, or to build the index without writing, call `build_text_index()`. Parameters default to the table properties and can be overridden:

```python
table.build_text_index()                          # all configured columns, properties' params
table.build_text_index(column="body")             # a single text column
table.build_text_index(column="body", tokenizer="en_stem")
table.build_text_index(partition_desc="range=2024-01-01")
table.build_text_index(column="body", partitions={"range": "2024-01-01"})
```

Pass `rebuild=True` to rebuild each shard from scratch — all of its active data files re-read into one fresh split instead of a delta — which is also the way to compact on demand:

```python
table.build_text_index(rebuild=True)
```

## Text search through the scan API

Text search is triggered through the scan API by setting `reader_options`:

```python
matches = (
    table.scan()
    .options(
        reader_options={
            "text_search_query": "quick fox",
            "text_search_top_k": "10",
        }
    )
    .to_arrow_table()
)
print(matches.column("id").to_pylist())
```

Supported `reader_options`:

| Key | Description | Default |
|-----|-------------|---------|
| `text_search_query` | Query string (required) | - |
| `text_search_column` | Text column to search | auto-detected when the table has one text column |
| `text_search_top_k` | Candidate count per shard (bucket) | 10 |
| `text_search_scores` | Expose the BM25 score of each verified row in the reserved `__lakesoul_text_score` column; requesting scores also enables the verification pass | `false` |
| `text_search_verify` | Run the exact verification pass over the current rows | `true` |

Each per-bucket reader searches its own index and keeps its top-`top_k` candidates; the native reader then verifies every candidate against the current row text after merge-on-read, so updated or deleted rows never leak into the result. The result is the set of exact matches among the candidates, bounded by `top_k × number_of_shards`; the scan itself does not order them. Set `text_search_scores=true` to also read each row's BM25 score (as computed by the bucket's index) in the reserved `__lakesoul_text_score` column — `lakesoul.daft.text_search` uses it to rank results globally.

The text column is read from the table properties, so for a single text column you only need to provide the query. When the table has multiple text columns, set `text_search_column` explicitly.

## Text search through Daft

`lakesoul.daft.text_search` wraps the same candidate scan into a lazy Daft pipeline:

```python
from lakesoul.daft import text_search

df = text_search(
    table,                  # LakeSoulTable or LakeSoulScan
    "quick fox",            # query string
    top_k=10,               # rows to return globally, best first
    # column="body",        # required only when multiple columns are indexed
    # with_score=True,      # also return the BM25 score column
    extra_columns=["ts"],   # additional columns to return
)

result = df.collect().to_arrow()
print(result.column("id").to_pylist())  # best-scoring first
```

`text_search` accepts either a table or a configured scan. When a scan is passed, its partition pruning and runtime options are kept — for example, searching only one range partition. The primary key and the text column are always read. It returns the global top-`top_k` exact matches, best BM25 score first: each task searches its bucket's index for a candidate set (sized from `top_k` with over-fetch so verification cannot leave the result short), the native reader verifies the candidates against the current rows, and the candidates are ranked by score and truncated to `top_k`. The scores are dropped unless `with_score=True`.

Ranking compares each bucket's BM25 statistics, so ordering across buckets is approximate — the same trade-off as a default Elasticsearch search across shards.

`scan.to_daft()` with `reader_options` remains available for callers that want to run their own merge logic.

## Text search through SQL (DataFusion)

The LakeSoul DataFusion session registers a boolean `text_match(column, query)` UDF, so text search is also available to SQL:

```sql
SELECT id, body
FROM documents
WHERE text_match(body, 'quick fox')
LIMIT 10;
```

With a finite `LIMIT`, the planner pushes the search down to the text index (`EXPLAIN` shows `LakeSoulTextSearchExec`) and evaluates the `text_match` predicate exactly above the scan, which removes stale candidates. Without a `LIMIT`, there is no candidate pushdown: the query falls back to a full scan and evaluates `text_match` exactly over every row.

`ORDER BY text_score(column, query) DESC` adds relevance ranking; the scan then returns the global top-`LIMIT` rows by BM25 score:

```sql
SELECT id, body
FROM documents
WHERE text_match(body, 'quick fox')
ORDER BY text_score(body, 'quick fox') DESC
LIMIT 10;
```

`text_score` is only supported in `ORDER BY` together with a `text_match` filter and a `LIMIT`; it cannot be projected into the `SELECT` list yet (use the scan API with `text_search_scores` to read scores). As with Daft, ranking across shards uses per-shard BM25 statistics and is approximate.

`text_match` works on tables without a text index too, again as an exact full-scan predicate. When several `text_match` terms are combined with `AND`, only the first one is pushed down as an index search; all of them are still evaluated exactly.

The query string is parsed as a Tantivy query: whitespace-separated terms are OR-combined, quoted phrases (`"quick fox"`) require `with_positions=true`, and boolean operators (`AND`, `OR`, `NOT`, parentheses) are supported. Chinese text is segmented with jieba.

## Query syntax and analyzers

- Query strings are parsed with Tantivy's query parser on the indexed text field.
- Whitespace-separated terms are OR-combined; `AND`/`OR`/`NOT` and parentheses refine the query.
- Quoted phrases match consecutive positions; they need `with_positions=true` (the default). With positions disabled, phrase syntax cannot match.
- The analyzer is fixed at index time by `tokenizer`. The `jieba` analyzer segments Chinese words and lowercases the tokens; `default`/`en_stem` are Tantivy's word-boundary analyzers. There is no query-time analyzer override — searching a jieba index with the query string is always consistent with how the text was indexed.

## How it works

- Each table keeps one index per text column at `{table_path}/_text_index/{column}/{partition}/{bucket}/`.
- The shard identity is `(partition_desc, hash_bucket_id)`: files from different range partitions are never merged into one shard, so partitioned tables get a separate index per partition.
- A split is one immutable Tantivy index bundled into a single object (`{split_id}.split`): the Tantivy files followed by a JSON footer with per-file offsets and CRCs. Readers materialize splits into a local cache and open them read-only.
- A search collects the per-split BM25 top-k and merges them by primary key (keeping the best score); the reader injects the candidate ids as a `pk IN (...)` filter, and the verification pass re-checks each candidate's current text — after merge-on-read resolved row versions and dropped CDC delete tombstones — before the rows are returned.
- The BM25 score of every candidate travels with it and is attached to the rows that pass verification, so `text_search_scores` and the Daft ranking compare the same scores that selected the candidates.
- Index maintenance is best-effort after a committed write: it never blocks the data write, and a failed build or compaction leaves the previous index (and correctness) intact.
- The index directory follows the data lifecycle: automatic GC removes generations that have been superseded for longer than `gc_grace_seconds`, keeping `gc_keep_generations` generations.

## Limitations

- Relevance ranking compares per-shard BM25 statistics (splits are built and scored per `(partition, bucket)`), so cross-shard ordering is approximate — the same trade-off as a default Elasticsearch search across shards. Global statistics are future work.
- `text_score` can order results but cannot be projected into the `SELECT` list yet; read scores through `text_search_scores` or Daft's `with_score=True`.
- Multiple text columns require an explicit `text_search_column`/`column`.
- Phrase queries require `with_positions=true` (the default); with positions disabled only term/boolean queries are available.
- `write_ray` does not build or update secondary indexes; use `build_text_index()`.
- The index is currently reachable from the Python SDK/Daft and from the DataFusion SQL engine; Spark/Flink integration is not exposed yet.
