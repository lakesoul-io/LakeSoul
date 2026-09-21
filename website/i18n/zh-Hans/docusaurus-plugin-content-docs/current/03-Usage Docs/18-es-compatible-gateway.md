# Elasticsearch 兼容 API 网关

LakeSoul 提供 `lakesoul-es-gateway`：一个小型 HTTP 服务，对外暴露检索类客户端使用的 Elasticsearch REST 接口，底层是带 Tantivy 全文索引与 IVF+RaBitQ 向量索引的 LakeSoul 表。客户端继续使用其 Elasticsearch 驱动，只需把地址指向网关：

- 关键词检索走文本索引，返回 BM25 分数；
- 向量检索走向量索引，使用余弦相似度；
- 文档写入、delete-by-query 与部分更新映射为 LakeSoul 的 upsert（merge-on-read + CDC tombstone），索引同步维护——写请求返回时文档即可检索。

索引需要在 TOML 配置中**预先声明**；网关在启动时完成建表，未声明的 index 会被拒绝。

## 快速开始

```bash
cargo build --release -p lakesoul-es-gateway
```

`lakesoul-es-gateway.toml`：

```toml
[server]
listen = "0.0.0.0:9200"
version = "8.19.6"        # GET / 上报；7.x 会让客户端仅使用关键词能力

[lakesoul]
namespace = "default"
warehouse_prefix = "s3://bucket/lakesoul"
# 也可通过环境变量提供 S3 凭据
# endpoint = "http://minio:9000"
# s3_bucket = "bucket"
# s3_access_key = "..."
# s3_secret_key = "..."

[defaults]
hash_bucket_num = 4
tokenizer = "jieba"
nprobe = 64

[[indexes]]
name = "my_index"          # Elasticsearch index 名
table = "my_docs"          # LakeSoul 表名（默认与 index 同名）
dim = 1536                 # embedding 维度；省略则仅关键词
```

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
./target/release/lakesoul-es-gateway --config lakesoul-es-gateway.toml
```

## 文档模型

每个文档使用固定 schema 存储：`content`（全文列）、`source_id`、`source_type`、`chunk_id`、`knowledge_id`、`knowledge_base_id`、`tag_id`、`embedding`、`is_enabled`、`is_recommended`。内部主键由网关生成，并以字符串 `_id` 返回。embedding 写入时做 L2 归一化，索引使用内积度量，因此索引相似度即余弦。

## 支持的接口

| 接口 | 行为 |
|------|------|
| `GET /` | 版本与集群信息；`version.number` 决定客户端方言（7.x 仅关键词，否则 v8）。 |
| `HEAD /{index}`、`PUT /{index}` | 已声明 index 存在；`PUT` 校验声明并忽略 `number_of_shards`/`number_of_replicas`。 |
| `GET /{index}/_mapping` | 固定 mapping，过滤字段均为 `keyword`。 |
| `POST /{index}/_doc`、`PUT /{index}/_doc/{id}/_create`、`PUT/POST /{index}/_doc/{id}` | 单文档 upsert。 |
| `POST /{index}/_bulk` | NDJSON `index`/`create` 动作（兼容 v7 带 `_id` 与 v8 服务端生成两种形式）。 |
| `POST /{index}/_delete_by_query` | `terms`/`term`/`bool` 过滤；命中的行改写为 CDC tombstone。 |
| `POST /{index}/_update_by_query` | 固定脚本：启停、打标签、移动知识库。 |
| `POST/GET /{index}/_search` | 关键词、向量与 filter-only 查询。 |
| `POST /{index}/_refresh` | 空操作：写入立即可见。 |

所有响应都带 `X-Elastic-Product: Elasticsearch`，错误使用 `{"error":{"type","reason"},"status"}` 结构。

## 检索语义

关键词检索（对 content 列的 `match`）按 BM25 返回带数值 `_score` 的命中，并在当前行上做精确校验，因此被更新或删除的文档不会漏出。跨 hash bucket 的排序使用各自索引的统计量——与 Elasticsearch 默认的跨 shard 检索是同一取舍；RRF 融合由客户端完成。

向量检索支持客户端发送的 `script_score` 形式：

```json
{"query":{"script_score":{
   "query":{"bool":{"filter":[{"term":{"knowledge_base_id":"kb1"}}]}},
   "script":{"source":"Math.max(cosineSimilarity(params.query_vector, 'embedding'), 0.0)",
             "params":{"query_vector":[0.1, 0.2]}},
   "min_score":0.5}},
 "size":10,"_source":{"excludes":["embedding"]}}
```

候选来自向量索引，网关对当前行重新计算精确余弦并 clamp 到 `[0, 1]`（与脚本的 `Math.max` 一致），`min_score` 过滤 clamp 后的分数。filter-only 查询（无评分子句）作为普通扫描执行并返回完整 `_source`，对应索引复制路径。

## 限制

- delete/update 通过表扫描解析过滤条件；只有主键有快速路径。
- 建索引时忽略 `number_of_shards`/`number_of_replicas`；分桶数与 `nprobe` 来自网关配置。
- 未实现鉴权、RBAC、高亮、聚合与 `sort`。
- `_search` 支持的查询子集为 `match`、`terms`、`term`、`bool.filter/must/must_not` 以及带 `cosineSimilarity` 的 `script_score`。
