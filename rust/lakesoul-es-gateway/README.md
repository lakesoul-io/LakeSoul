# lakesoul-es-gateway: Elasticsearch-compatible API over LakeSoul

A small HTTP gateway that speaks the Elasticsearch REST contract a RAG
client (e.g. WeKnora) uses, backed by LakeSoul tables with Tantivy text and
IVF+RaBitQ vector indexes.

Indexes are **pre-declared** in a TOML configuration; the gateway provisions
each declared index as a LakeSoul table with a fixed document schema and
rejects undeclared indexes.  Document writes go through the LakeSoul upsert
path, so the text/vector indexes are built synchronously before the request
returns (writes are immediately searchable).

## Status

Implemented:

- `GET /` (version/product info), `HEAD/PUT /{index}`, `GET /{index}/_mapping`
- `POST /{index}/_doc` (v8), `PUT /{index}/_doc/{id}/_create` (v7),
  `PUT/POST /{index}/_doc/{id}`
- `POST /{index}/_bulk` (NDJSON, `index`/`create` actions, both v7 `_id` and
  v8 server-generated forms)
- `POST /{index}/_delete_by_query` (terms/term/bool filters → CDC tombstones)
- `POST /{index}/_update_by_query` (the four fixed WeKnora scripts: enable /
  disable / set tag / move knowledge base)
- `POST/GET /{index}/_search`:
  - keyword search (`match` on the content column) with BM25 `_score`, a
    global relevance order, exact verification against the current rows
    (stale candidates are dropped) and `size`/`from`;
  - `bool.filter`/`bool.must`/`bool.must_not` (terms/term), where a missing
    `is_enabled` counts as enabled;
  - `_source` includes/excludes (the v8 search excludes `embedding`) and the
    filter-only copy path with full `_source` including embeddings;
- `X-Elastic-Product: Elasticsearch` on every response

Vector search (`script_score`/cosine) lands in the next pull request.

## Configuration

```toml
[server]
listen = "0.0.0.0:9200"
# Reported by GET /; 8.x enables the v8 (keyword+vector) client paths,
# 7.x binds the client to keyword-only retrieval.
version = "8.19.6"

[lakesoul]
namespace = "default"
# Base path for tables without an explicit per-index `path`.
warehouse_prefix = "s3://bucket/lakesoul"
# Optional S3 credentials (otherwise the environment is used).
# endpoint = "http://minio:9000"
# s3_bucket = "bucket"
# s3_access_key = "..."
# s3_secret_key = "..."

[defaults]
hash_bucket_num = 4
tokenizer = "jieba"
with_positions = true

[[indexes]]
name = "weknora"          # Elasticsearch index name
table = "weknora_docs"    # LakeSoul table (defaults to the index name)
dim = 1536                # embedding dimension; omit for keyword-only
# path = "s3://bucket/lakesoul/weknora_docs"
```

Metadata comes from the standard `LAKESOUL_PG_URL` / `LAKESOUL_PG_USERNAME` /
`LAKESOUL_PG_PASSWORD` environment variables.

## Run

```bash
cargo run -p lakesoul-es-gateway -- --config lakesoul-es-gateway.toml
```

## Document model

Every document is written with the WeKnora fields: `content`, `source_id`,
`source_type` (integer), `chunk_id`, `knowledge_id`, `knowledge_base_id`,
`tag_id`, `embedding` (array of floats), `is_enabled`, `is_recommended`.
Embeddings are L2-normalized on write and searched with the inner-product
index, which is cosine similarity for unit vectors.

## Limits

- Delete/update resolve their terms filters with a scan of the table; there
  is no secondary index on the filter columns yet.
- `number_of_shards` / `number_of_replicas` from `PUT /{index}` are ignored:
  the bucket count is fixed at provisioning time.
- Basic auth and per-user RBAC are not implemented yet.
