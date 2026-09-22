# WeKnora ES driver harness

Replays the Elasticsearch request sequence of
[WeKnora](https://github.com/Tencent/WeKnora)'s v8 retriever driver
(`internal/application/repository/retriever/elasticsearch/v8`) against a
LakeSoul ES gateway, using the same Go client library
(`github.com/elastic/go-elasticsearch/v8`) and the same request shapes.  It
surfaces wire-level incompatibilities in seconds, without deploying the full
WeKnora stack (app + frontend + docreader + PostgreSQL + Redis + an embedding
provider).

Embeddings are synthetic deterministic unit vectors, so no embedding API is
needed for this harness.

## What it covers

| Step | WeKnora driver call |
|------|---------------------|
| client info | typed-client version / `X-Elastic-Product` handshake |
| `indices.exists` + create if missing | `createIndexIfNotExists` (settings: shards/replicas) |
| `indices.get_mapping` | `detectFieldTypes`: asserts `chunk_id` maps to `keyword`, so the driver uses bare field names instead of `chunk_id.keyword` |
| index one document | `Save` (chunk edit/re-index path) |
| bulk create | `BatchSave` |
| keywords retrieve | `bool.filter` (nested `getBaseConds`) + `match content`, `_source.excludes` |
| vector retrieve | `script_score` with `Math.max(cosineSimilarity(...), 0.0)`, `min_score`, `_source.excludes` |
| `update_by_query` enable/disable | `UpdateChunkEnabledStatus` |
| `update_by_query` tag | `BatchUpdateChunkTagID` (`params.tag_id`) |
| `update_by_query` move | `MoveKnowledgeIndices` (`knowledge_base_id = params.target; tag_id = ''`, `refresh=true`); asserts `total == updated`, no timeouts/conflicts/failures |
| copy indices paging | `CopyIndices`: filter-only `from`/`size` pages of 5, re-index into another knowledge base |
| `delete_by_query` | `DeleteByChunkIDList` (`terms chunk_id`) |

Keyword hits are also asserted to carry a positive `_score` in descending
order, because the WeKnora clients dereference `_score` and use hit order for
rank fusion.

## Run

```sh
# 1. Gateway (PostgreSQL metadata required)
cargo build --release -p lakesoul-es-gateway
cat > /tmp/weknora-harness.toml <<'EOF'
[server]
listen = "127.0.0.1:9200"
version = "8.19.6"

[lakesoul]
namespace = "default"
provision_on_start = true

[defaults]
hash_bucket_num = 4
tokenizer = "jieba"

[[indexes]]
name = "WeKnora"
table = "weknora_smoke"
path = "file:///tmp/weknora-harness-data"
dim = 1024
EOF
LAKESOUL_PG_URL=... ./rust/target/release/lakesoul-es-gateway --config /tmp/weknora-harness.toml

# 2. Harness (Go 1.22+)
go run ./script/weknora-es-harness \
    --addr http://127.0.0.1:9200 --index WeKnora --dim 1024
```

`--dim` must match the `dim` configured on the index; the synthetic embeddings
use that dimension.

`--client v7` replays the same sequence through `esapi`
(`github.com/elastic/go-elasticsearch/v7`), which is what WeKnora's v7
retriever driver uses.

## Current status

- v8 typed client: all steps pass.
- v7 esapi client: all steps pass.
- Full WeKnora v0.8.0 stack (docker compose, `RETRIEVE_DRIVER=elasticsearch_v8`,
  DashScope `text-embedding-v4`): document upload, parse/chunk/embed,
  hybrid (BM25 + vector RRF) retrieval, chunk enable/disable, reparse move
  and delete all pass.  Two client-side issues were found and are documented
  on the website page:
  - v0.8.0 sends the unclamped `cosineSimilarity` script (the gateway now
    accepts both spellings);
  - v0.8.0's `CopyIndices` drops `is_enabled`, so `reuse_vectors` moves and
    KB clones land disabled in **any** Elasticsearch (use `mode=reparse`).
