# Native Batch Query (Current Implementation)

This document keeps the early design and the later change log; some of the "current limitations" described below describe their respective historical stage. The authoritative interface and boundaries of native batch SQL are this section and [Native SQL Batch](../../python/examples/vector-search/native-batch-sql.md).

DataFusion and LakeSoul pgwire register the same set of table functions, so a single SQL statement can search multiple queries, using a shared snapshot, cluster/FastScan blocks, and a bounded parallel kernel:

```sql
-- Target and query tables are created by the existing write paths; pgwire stays read-only.
SELECT query_id, rank, id, distance
FROM lakesoul_vector_search_batch_from_table(
  'public.items', 'embedding',
  'public.query_vectors', 'qid', 'qvec',
  100, 64, 'L2', 'exact', 1
)
ORDER BY query_id, rank;

-- A small number of queries can use inline JSON; query_id is the 0-based array position.
SELECT query_id, rank, id, distance
FROM lakesoul_vector_search_batch(
  'public.items', 'embedding', '[[0.1,0.2,0.3],[0.4,0.5,0.6]]',
  10, 64, 'L2', 'index'
)
ORDER BY query_id, rank;
```

- Returns `query_id BIGINT, rank INTEGER, id BIGINT, distance FLOAT8`; rank starts at 1. L2 returns Euclidean distance and IP returns the negative inner product; smaller is better for both.
- `exact` re-ranks ANN candidates by reading their original vectors and does not promise exact kNN over the whole table; the new native batch supports both L2 and IP.
- The target currently requires an Int64 single-column primary key `id`, complete index coverage, no CDC/mutation chain, and no time travel. The query table must have a unique, non-null Int64 qid and a Float32 list vector. The same id in different range partitions still denotes different target rows, so there is no table-wide false ID de-duplication.
- Parameters must be constants; these two table functions do not accept dynamic `$1` vector parameters. Use a query table for large numbers of vectors. This boundary changes neither the prepared-parameter support of the existing single-vector query nor its plain scan path when no index exists.
- `SET lakesoul_vector_batch.query_window='256'` controls the window of at most 256 queries, automatically filling across small Arrow input batches; `parallelism`, `io_concurrency`, and `memory_bytes` can also be set. The temporary input/working/output budget is capped at 128 MiB, with at most 65536 queries and 64 MiB of effective input per request.
- A request pins the target snapshot and a renewable `index_read_lease`; a logical drop prevents new batches from using that index, and physical cleanup waits for existing readers. Client cancellation or lease expiry propagates into the actual work, which retains protection until it stops.

`EXPLAIN` shows `VectorBatchSearchExec`, and `EXPLAIN ANALYZE` reports metrics such as window/query and cluster/block reuse. The final PG session test verified that 1,000 queries run in 4 windows sized `[256,256,256,232]`; real pgwire batch queries over both the simple and extended protocols match the small-dataset baseline. See the [Notebook](../../python/examples/vector-search/tutorial/native_batch_sql.ipynb) for usage and [Vector SQL and Batch API](../../website/docs/engine/vector-search/api.md) for the validation scope. This is a LakeSoul pgwire extension, not the batch-query feature of the upstream pgvector extension, and not a serial SQL loop over single queries.

---

## Recommendation

Split the work into two stages:

1. **Support pgvector query syntax first**: `<->`, `<#>`, `vector(n)`, and `'[1,2,3]'`. Compute distances exactly when no usable index is available.
2. **Then optimize `ORDER BY distance LIMIT k` using LakeSoul's IVF+RaBitQ search**.

Do not use string replacement in `postgres-lakesoul` or store pgvector parameters in a shared `SessionContext`. Put syntax support and plan optimization in `lakesoul-datafusion`. Keep `postgres-lakesoul` responsible for the PostgreSQL wire protocol, `CREATE EXTENSION`, `SET ivfflat.probes`, and catalog compatibility.

---

# 1. Key Limitations in the Current Code

## 1.1 The PostgreSQL Server Is Not Yet Connected to LakeSoul

`rust/postgres-lakesoul/src/main.rs:75-78` currently creates a plain DataFusion session:

```rust
let conf = SessionConfig::new().with_information_schema(true);
let ctx = SessionContext::new_with_config(conf);
setup_pg_catalog(&ctx, "datafusion", am)?;
```

It does not call:

```rust
create_lakesoul_session_ctx(...)
```

So before adding vector syntax, make the server use:

```text
LakeSoulCatalog
LakeSoulTableProvider
LakeSoulQueryPlanner
LakeSoul RuntimeEnv/ObjectStore
```

The catalog name passed to `setup_pg_catalog` should also use the standardized lowercase name:

```rust
setup_pg_catalog(&ctx, "lakesoul", am)?;
```

---

## 1.2 SQL Parser Support for pgvector

I tested the following SQL using the repository's current `sqlparser 0.61`:

| Syntax | Parse result | AST operator or node |
|---|---|---|
| `embedding <-> '[1,2,3]'` | Parses | `LtDashGt` |
| `embedding <#> '[1,2,3]'` | Parses | `Custom("<#>")` |
| `embedding <=> '[1,2,3]'` | Parses, but conflicts | `Spaceship` |
| `embedding <+> '[1,2,3]'` | **Fails to parse** | None |
| `embedding vector(3)` | Parses | `DataType::Custom("vector", ["3"])` |
| `CREATE EXTENSION vector` | Parses | `CreateExtension` |
| `CREATE INDEX ... USING ivfflat` | Parses | `CreateIndex` |

The biggest pitfall is `<=>`. DataFusion currently maps `BinaryOperator::Spaceship` to:

```rust
Operator::IsNotDistinctFrom
```

This is null-safe equality, whereas pgvector defines `<=>` as cosine distance. Intercept it before DataFusion's default operator conversion; otherwise the query can run and return completely incorrect results.

`<+>` needs support at the tokenizer or parser level. An ordinary `ExprPlanner` runs too late to handle it.

---

## 1.3 Existing Vector Search Is Not on the DataFusion SQL Path

The existing vector search path is:

```text
LakeSoulReader::start()
  → inject_vector_search_filter()
  → search_matching_shards()
  → candidate IDs
  → PK filter
```

Locations:

- `rust/lakesoul-io/src/reader.rs:148-340`
- `rust/lakesoul-io/src/vector/search.rs`

SQL queries take a different path:

```text
LakeSoulTableProvider::scan()
  → list_files_for_scan()
  → DataSourceExec
  → MergeParquetExec
```

Location:

- `rust/lakesoul-datafusion/src/datasource/table_provider.rs:701-918`

This path does not create a `LakeSoulReader`, so setting `vector_search_*` options at the SQL layer will not trigger the existing vector index search.

Simply mapping `embedding <-> '[1,2,3]'` to the `vector_search_query` option is therefore not enough. Integrate index search into `LakeSoulTableProvider::scan` or add a dedicated logical or physical node.

---

## 1.4 The Existing Search API Is Not Sufficient for SQL Top-K

`search_matching_shards()` currently:

- Uses only the first index prefix.
- Returns only `Vec<u64>`, discarding scores.
- Searches only one bucket; callers must merge results across buckets.
- Ignores the `_metric` argument and uses the metric from the index.
- Returns no IDs if the index is missing.
- Does not verify that the index covers the current LakeSoul snapshot.

Using it directly for SQL would cause several serious problems.

### A Missing Index Incorrectly Returns No Rows

The current behavior is:

```text
missing index
  → empty IDs
  → Boolean(false) filter
  → zero rows
```

This is incorrect for SQL. If the index is missing or incomplete, fall back to an exact scan rather than returning an empty result.

### L2 Scores Do Not Match pgvector's Return Value

RaBitQ currently returns squared L2 distance:

```rust
l2_distance_sqr(...)
```

pgvector's `<->` returns Euclidean distance, which requires taking the square root. The ranking is the same, but the index score cannot be returned directly as the SQL `distance` value.

### Inner Product Uses a Different Sign

The current index's `Metric::InnerProduct` returns the ordinary inner product. pgvector's `<#>` returns its **negative** because PostgreSQL index scans primarily operate in ascending order.

Use the index only to select candidates. The final SQL plan should still compute the exact pgvector distance on candidate rows, sort them, and apply the `LIMIT`.

> **Already implemented (R68)**: `<#>` now uses the index end to end, including index-only ordering.
> - `datafusion-postgres` rewrites `<#>` into `-inner_product(vec, q)` ASC; the pushdown rule
>   (`vector_search_rule.rs`) recognizes `Expr::Negative` and normalizes it into the canonical
>   "positive weight + IP descending" form, so the fused physical plan that "drops the Sort and
>   emits rows directly in index order" holds for IP as well;
> - Single-column IP allows index-only (multi-column fused ordering still only supports L2, because
>   multi-column fused distance is currently defined for L2 only);
> - `exact` mode re-ranks exactly by `-dot` (the IP branch of `accumulate_exact_row`), producing
>   results consistent with `inner_product`.
>
> Measured (GloVe-1.18M×200d, 4 buckets, bits=1, top_k=100, 200 real queries, TTL=200ms, IP index):
>
> | Query form | Mode | QPS | Recall@100 |
> |---|---|---:|---:|
> | `ORDER BY vec <#> '[...]'` | exact | **590.8** | 0.7006 |
> | Same as above | index-only (np16) | **779.1** | 0.4395 |
> | `ORDER BY inner_product(vec, ARRAY[...]) DESC` | exact / index-only | 341.4 / 398.2 | 0.7006 / 0.4395 |
>
> Both forms return **identical results row by row** (the id lists are the same for 200/200 queries);
> `<#>` goes through a string literal, using 200 fewer SQL tokens than `ARRAY[...]`, hence the higher
> throughput. Remaining limitations: `exact` in the batch entry point (`vector_batch.rs`) still only
> supports L2; index-only for multi-column fused ordering still only supports L2.

---

# 2. Recommended Architecture

## Layer 1: DataFusion Expression Compatibility

Add the following under `rust/lakesoul-datafusion/src/vector/`:

```text
vector/
├── expr_planner.rs
├── type_planner.rs
├── functions.rs
└── optimizer.rs
```

### `PgVectorExprPlanner`

Implement DataFusion 54's:

```rust
ExprPlanner::plan_binary_op
```

Use these mappings:

| pgvector operator | DataFusion expression | Initial support |
|---|---|---|
| `<->` | `pgvector_l2_distance(left, right)` | Exact + ANN |
| `<#>` | `pgvector_negative_inner_product(left, right)` | Exact + ANN |
| `<=>` | `pgvector_cosine_distance(left, right)` | Exact first |
| `<+>` | `pgvector_l1_distance(left, right)` | Exact after the parser fix |
| `<~>` | Hamming | Not yet supported |
| `<%>` | Jaccard | Not yet supported |

Use distinct `pgvector_*` UDF names rather than rewriting directly to `array_distance`. That lets the optimizer reliably recognize vector Top-K queries without accidentally optimizing ordinary array calculations.

The UDFs can reuse DataFusion's existing functions:

- `array_distance`
- `inner_product`
- `cosine_distance`

These functions coerce `FixedSizeList` to `List`. L1 distance needs a small Arrow kernel.

### Vector Input

Support both forms:

```sql
embedding <-> '[1,2,3]'
embedding <-> ARRAY[1,2,3]
```

Use a common internal type:

```rust
FixedSizeList<Float32, dimension>
```

Parse text vectors strictly:

- Require surrounding `[...]`.
- Require the dimension to match the column.
- Require every element to be a finite `f32`.
- Reject empty elements, NaN, Infinity, and overflow.
- Parse each literal once during planning, not once per row.
- Keep prepared statement parameters as scalars rather than expanding them into arrays repeated for every row.

---

## Layer 2: The `vector(n)` Type

Add `PgVectorTypePlanner`:

```text
vector(768)
  → Arrow FixedSizeList<Float32, 768>
```

This matches the existing index builder's requirement:

```rust
DataType::FixedSizeList(_, dim)
```

See `rust/lakesoul-vector/src/reader.rs:88-120`.

Recommended rules:

- Table column definitions must use `vector(n)` with `n > 0`.
- `vector` without a dimension is allowed only in transient expressions, not in declarations of indexable LakeSoul columns.
- `vector(n)` defines the storage type. It **does not create an index automatically**.
- Indexes are still managed through `vector_index_columns` or, later, `CREATE INDEX`.

### Combining Type Planners

`setup_pg_catalog()` installs its own `PgOidTypePlanner`. A DataFusion `SessionState` supports only one `TypePlanner`, so calling `with_type_planner()` twice does not combine them.

Use a composite planner:

```rust
struct LakeSoulPgTypePlanner {
    vector: PgVectorTypePlanner,
    postgres: PgOidTypePlanner,
}
```

Dispatch as follows:

```text
vector / pg_catalog.vector
  → PgVectorTypePlanner

All other types
  → PgOidTypePlanner
```

Install the composite planner after `setup_pg_catalog()`; otherwise PostgreSQL catalog initialization will overwrite the vector planner.

---

## Layer 3: Recognize Top-K Queries and Use the Index

Add a logical optimizer rule that matches this plan shape:

```text
Limit(k)
  └─ Sort(
       pgvector_l2_distance(embedding, constant_query) ASC
     )
       └─ Filter(optional)
          └─ TableScan(LakeSoulTableProvider)
```

Convert it into a scan with query-local settings:

```rust
struct VectorSearchSpec {
    column: String,
    query: Arc<[f32]>,
    metric: Metric,
    limit: usize,
    candidate_k: usize,
    nprobe: usize,
}
```

Do not store these settings in:

- Global environment variables.
- A shared `SessionContext`.
- The persistent `LakeSoulTableProvider.io_config`.
- Table metadata.

A PostgreSQL server runs multiple queries concurrently. Mutating a shared provider or session can cause one query to use another query's vector.

### Optimize Only When All of These Conditions Hold

1. The query involves a single LakeSoul table.
2. `ORDER BY` uses a supported vector distance.
3. The ordering is ascending (`ASC`).
4. The query has a finite `LIMIT`.
5. The left operand is a column declared as a vector index column on that table.
6. The query vector is a literal or a bound prepared statement parameter.
7. The operator's metric matches the index manifest's metric.
8. The index covers the current query snapshot.

Otherwise, run an exact full-table scan.

For example, the following cannot be pushed down directly to ANN:

```sql
ORDER BY embedding <-> query_table.embedding
```

The right operand is not a query constant, so this requires a join and exact execution.

---

## Layer 4: Scan Candidates and Rerank Exactly

Recommended execution flow:

```text
Identify range partition filters
        │
        ▼
List files for the current LakeSoul snapshot
        │
        ▼
Group by partition_desc + bucket_id
        │
        ▼
Search each shard index concurrently
        │
        ▼
Merge and deduplicate candidate PKs
        │
        ▼
Push PK IN (...) into the LakeSoul merge-on-read scan
        │
        ▼
Read complete rows at their current versions
        │
        ▼
Compute exact distances with the UDF
        │
        ▼
Global Sort + Limit
```

Keep the original `Sort + Limit` so the final stages can:

- Rerank candidates using exact distances.
- Compute the global Top-K across shards.
- Take the square root for L2 distance.
- Apply the negative sign for `<#>`.
- Use current vectors for updated rows instead of old vectors from the index.

Use `Expr::InList` for the candidate filter rather than building thousands of nested conditions:

```text
id = 1 OR id = 2 OR id = 3 ...
```

The existing `LakeSoulReader::inject_vector_search_filter()` builds an `OR` chain. Do not reuse that approach for SQL.

---

# 3. Index Freshness Is a P0 Before Production

The current index path is just:

```text
table/_vector_index/column/partition/bucket/LATEST
```

SQL queries also need to know which LakeSoul data commits or files the manifest covers.

Add the following to the manifest:

```rust
struct LakeSoulVectorSnapshot {
    table_id: String,
    partition_desc: String,
    bucket_id: u32,
    vector_column: String,
    covered_file_ops: Vec<FileIdentity>,
    metric: Metric,
    dimension: usize,
}
```

Pin a single LakeSoul snapshot for each query, then check index coverage:

| Index state | Behavior |
|---|---|
| Index fully covers the snapshot | ANN |
| Index is missing | Exact scan |
| Index is behind, and new files can be identified | Index candidates plus an exact scan of new files |
| Manifest is corrupt or the metric does not match | Return a clear error or fall back according to configuration |
| Index references deleted files | Treat it as stale; do not return an empty result |

Otherwise, compaction, updates, and delays in automatic index building can silently cause rows to be missed.

---

# 4. PostgreSQL Wire Layer Responsibilities

Implement these in `rust/postgres-lakesoul`, not in the RaBitQ core.

## `CREATE EXTENSION vector`

In the first version, `PgVectorQueryHook` can intercept:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

Because support is already compiled into the server, the hook can return success. It must accept only `vector` and continue to report other extensions as unsupported.

Full compatibility also requires exposing entries in these virtual system catalogs:

```text
pg_extension
pg_type
pg_proc
pg_operator
pg_opclass
```

Many pgvector clients run the following query and assume pgvector is not installed if it returns no row:

```sql
SELECT typname, oid FROM pg_type WHERE typname = 'vector';
```

## `SET ivfflat.probes`

Map the setting as follows:

```text
ivfflat.probes → nprobe
```

Store it in connection-local state rather than modifying a shared `SessionContext`.

The current `datafusion-postgres::SetShowHook` recognizes only a limited set of configuration options. Put the custom vector hook before it:

```text
PgVectorHook
CursorStatementHook
SetShowHook
TransactionStatementHook
```

`serve()` currently shares a single `Arc<SessionContext>`. Before implementing connection-local `nprobe`, complete the per-connection session factory described in `plan.md`. Alternatively, use a fixed server-wide `nprobe` default in the first version.

## PostgreSQL Custom Type

`arrow-pg` currently encodes:

```rust
FixedSizeList<Float32>
```

as:

```text
float4[]
```

It does not use pgvector's `vector` OID.

Distinguish between two levels of support:

### Syntax Compatibility MVP

- Input: text such as `'[1,2,3]'` or a `float4[]` array.
- Output: `float4[]`.
- SQL operators work.
- Standard psql and JDBC clients work.
- Registration with pgvector-specific drivers is not guaranteed.

### Full pgvector Driver Compatibility

Also required:

- A fixed or dynamically assigned `vector` type OID.
- An entry for that OID in `pg_type`.
- PostgreSQL wire decoders for text and binary vectors.
- Text and binary vector encoders for Arrow `FixedSizeList<Float32>`.
- Prepared statement parameter type inference.
- Support for `$1` sent by a pgvector driver with the custom OID.

This is a separate layer of work that SQL parsing alone cannot address.

---

# 5. Handling `CREATE INDEX USING ivfflat`

The official syntax is:

```sql
CREATE INDEX ON items
USING ivfflat (embedding vector_l2_ops)
WITH (lists = 100);
```

The current parser accepts it, but DataFusion drops important fields when converting it into a `LogicalPlan`:

- The operator class, such as `vector_l2_ops`.
- The `WITH (lists = 100)` clause.

Read the original `Statement::CreateIndex` in `QueryHook` rather than relying on DataFusion's `CreateIndex` logical plan.

Possible mappings for later versions:

| pgvector syntax | LakeSoul behavior |
|---|---|
| `USING ivfflat` | IVF+RaBitQ index |
| `vector_l2_ops` | `Metric::L2` |
| `vector_ip_ops` | `Metric::InnerProduct` |
| `WITH (lists=N)` | `nlist=N` |
| `SET ivfflat.probes=N` | `nprobe=N` |
| `USING hnsw` | Explicitly reject |
| `vector_cosine_ops` | Reject for now |
| `vector_l1_ops` | Reject for now |

LakeSoul actually uses **IVF+RaBitQ**, not pgvector's native IVFFlat. `USING ivfflat` can be accepted as a SQL compatibility alias, but both the documentation and `pg_indexes` must disclose the actual implementation rather than imply the algorithms are identical.

Index build orchestration currently lives mainly in Python. For the PostgreSQL server to execute `CREATE INDEX`, it needs a Rust coordinator:

```text
Update vector_index_columns
  → enumerate partitions and buckets
  → VectorShardIndexBuilder
  → publish manifests
  → commit index state after success
```

Keep the first version read-only and leave `CREATE INDEX` disabled.

---

# 6. Recommended Implementation Order

## PR 1: Exact Query Syntax

Support:

```sql
SELECT id,
       embedding <-> '[3,1,2]' AS distance
FROM items
ORDER BY embedding <-> '[3,1,2]'
LIMIT 10;
```

Scope:

- Switch the PostgreSQL server to `create_lakesoul_session_ctx`.
- Add `PgVectorExprPlanner`.
- Support `<->` and `<#>`.
- Support vector text literals.
- Add exact distance UDFs.
- Validate dimensions, null handling, and finite values.
- Do not use indexes.

This is the smallest end-to-end implementation, and it works correctly even when no index exists.

## PR 2: ANN Query Optimization

- `VectorSearchSpec`.
- Top-K logical optimizer rule.
- Concurrent search across shards.
- Candidate PK `IN` filter.
- Exact reranking.
- Metric validation.
- Fallback when an index is missing.

## PR 3: Index and Snapshot Correctness

- Record LakeSoul snapshot coverage in the manifest.
- Detect stale indexes.
- Add hybrid scans for new files.
- Maintain consistency across updates, deletes, and compaction.

## PR 4: PostgreSQL Session Syntax

- `CREATE EXTENSION vector`.
- `SET/SHOW ivfflat.probes`.
- Connection-local state.
- `pg_extension` and basic pgvector catalog entries.

## PR 5: DDL and Wire Types

- `vector(n)` DDL.
- `CREATE INDEX USING ivfflat`.
- The `vector` OID.
- Binary prepared statement parameters.
- Compatibility with pgvector-python and JDBC adapters.

Leave `<=>`, `<+>`, HNSW, halfvec, sparsevec, and bit vectors until last. The current index layer does not support them.

---

# 7. Validation Matrix

At a minimum, cover the following.

### Parsing and Exact Semantics

```sql
embedding <-> '[...]'
embedding <#> '[...]'
embedding <=> '[...]'
embedding <+> '[...]'
```

Check:

- Expected results for valid inputs.
- NULLs.
- Dimension mismatches.
- NaN and Infinity.
- Literals and prepared statement parameters.
- `<#>` returns the negative inner product.
- `<->` returns Euclidean distance, not squared distance.

### ANN Correctness

- Single shard.
- Multiple buckets.
- Multiple range partitions.
- Global Top-K.
- `WHERE` with Top-K.
- Duplicate PKs and version merging.
- Updated vectors.
- Deleted rows.
- New data committed before the index is updated.
- Missing or corrupt manifests.
- An L2 operator against an inner-product index.
- Exact reranking of ANN results.

### PostgreSQL Protocol

- Simple queries.
- Extended queries.
- `$1` parameters.
- psql.
- JDBC.
- psycopg.
- pgvector-python `register_vector()`.
- Text and binary result formats.

---

# 8. Top Risks by Priority

| Priority | Risk |
|---|---|
| P0 | A missing or stale index is treated as an empty result, silently omitting rows |
| P0 | SQL scans bypass `LakeSoulReader`, so existing vector options have no effect |
| P0 | Candidates are not merged globally across shards |
| P0 | Storing the query vector or `nprobe` in a shared `SessionContext` causes settings to leak between concurrent queries |
| P0 | DataFusion treats `<=>` as null-safe equality |
| P1 | Squared L2 distance is returned directly as pgvector's `<->` result |
| P1 | The inner-product score is not negated to match pgvector |
| P1 | Arrow results are encoded as `float4[]`, causing pgvector driver registration to fail |
| P1 | Only the first `UInt64` or `Int64` primary key column is supported; composite or string primary keys cannot be indexed |
| P2 | The `CREATE INDEX` operator class and `WITH` parameters are lost in the DataFusion plan |
| P2 | `<+>` fails to parse in the current `sqlparser` |

---

## End-to-End Data Flow

```text
PG SQL
  │
  ▼
PostgresCompatibilityParser
  │
  ├─ <-> → LtDashGt
  ├─ <#> → Custom("<#>")
  └─ <=> → Spaceship
  │
  ▼
PgVectorExprPlanner
  │
  ▼
pgvector_* distance UDF
  │
  ▼
LogicalPlan
  │
  ▼
VectorTopKOptimizerRule
  │
  ├─ No usable, complete index ─────┐
  │                                 │
  └─ Complete index available       │
       │                            │
       ▼                            │
  Concurrent ANN by partition       │
  and bucket                        │
       │                            │
       ▼                            │
  Candidate PK set                  │
       │                            │
       └──────────────┬─────────────┘
                      ▼
             LakeSoulTableProvider
                      │
                      ▼
          DataSourceExec + MergeParquetExec
                      │
                      ▼
               Exact distance
                      │
                      ▼
               Global Sort + Limit
                      │
                      ▼
              Arrow → PG wire
```

Start with **PR 1**. It establishes the boundaries between the parser, types, UDFs, prepared statement parameters, and the PostgreSQL wire protocol before adding ANN. Starting with the index would mix syntax issues with snapshot correctness issues.

Reference: [Official pgvector query syntax and operators](https://github.com/pgvector/pgvector#querying).

---

# 9. Cache and Performance (Whole-File Payloads Removed)

Whole-file payloads have been removed entirely. The `<->` exact re-rank in `postgres-lakesoul` now
relies on the on-demand decoded-block cache provided by `LAKESOUL_CACHE`:

```bash
export LAKESOUL_CACHE=1
export LAKESOUL_CACHE_PATH=/data/lakesoul/cache
export LAKESOUL_CACHE_SIZE=8GiB
export LAKESOUL_VECTOR_BLOCK_ROWS=256
export LAKESOUL_VECTOR_BLOCK_CACHE_BYTES=268435456
```

If `LAKESOUL_CACHE` is not set at service startup, a WARN is logged: exact queries remain correct
but fall back to the reader path (noticeably slower). See
[Configuration](../../website/docs/engine/vector-search/configuration.md) and
[Benchmark](../../website/docs/engine/vector-search/benchmarks.md) for the single-query and batch
measurements of exact search.

The historical R58 pgvector QPS table (exact 261–598 QPS) was measured under the removed whole-file
payload mode and no longer reflects the current version; do not use that table for capacity
planning. index-only (`rerank='index'`) does not read the vector column, is unaffected by that
removal, and remains the highest-throughput PG wire path.

---

# 10. Implementation Status Addendum (Driver Type Detection / Zero-Change pgvector)

`postgres-lakesoul` already supports the type detection that pgvector clients need for
"zero-change" usage:

- **`to_regtype(text)`** (`src/pg_compat.rs`): resolves a type name to an OID and returns
  `Int32` (the same type as `pg_type.oid`), returning `NULL` for unknown names (PostgreSQL
  semantics). It covers `vector` (16385, i.e. `PG_VECTOR_TYPE_OID`), `bit` (1560, which pgvector's
  `register_vector` also probes), and common built-in types.
  This lets psycopg's `TypeInfo.fetch('vector')` inside `pgvector.psycopg.register_vector()`
  (whose SQL is `... WHERE t.oid = to_regtype($1)`) return type info normally.
- **RowDescription / the `vector` type in pg_type**: a LakeSoul session marks columns that declare a
  vector index with `pg.vector` via `LakeSoulVectorFieldMetadata`, and `datafusion-postgres` uses
  that to expose a `vector` row in `pg_catalog.pg_type` and to return the vector OID (16385) in the
  RowDescription of query results; text/binary encoding and decoding are handled by the pgvector
  feature of `arrow-pg`.
- **Parameterization**: `$n::vector(n)` and bare `$n` parameters of the extended protocol are bound
  and decoded as the vector type; numpy arrays and `pgvector.Vector` can both be passed directly as
  query parameters, and vector columns in query results are decoded into `Vector` objects by
  pgvector's loader.

Verification (2026-11, measured locally):

```text
psycopg + pgvector.psycopg.register_vector() → PASS
SELECT id FROM docs ORDER BY embedding <-> %s LIMIT 3   (numpy parameter) → [(1,), (2,), (6,)]
SELECT embedding FROM docs WHERE id = 1 → pgvector.Vector([1.0, 0.0, ...])
SELECT %s::vector(8) <-> %s::vector(8) → 0.0
```

Rust tests: `to_regtype_resolves_pgvector_type_for_drivers` (simple + extended
protocol), `pgvector_prepared_parameter_binds_a_vector`,
`pgvector_results_report_the_vector_type`, and others — 7 pgvector-related cases pass in total.

Still uncovered (keeping the read-only design): SQL-level `PREPARE/EXECUTE`, the actual semantics
of `hnsw.ef_search`, multi-statement transaction snapshots (`BEGIN` pinning a LakeSoul snapshot),
and production-grade auth/TLS.
