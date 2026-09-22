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

## 性能调参

每个 hash bucket 是独立的数据文件与索引 shard，因此 `hash_bucket_num` 是写入并行度与单请求开销之间的主要取舍。用同一批 20,000 篇 MS MARCO 文档、每批 500 条写入实测：

| Buckets | 批量写入 | 检索 p50 | 检索 QPS | Recall@100 |
|--------:|---------:|---------:|---------:|-----------:|
| 4 | 约 1,200 docs/s | 约 237 ms | 约 5 | 85.7% |
| 1 | 约 2,900 docs/s | 约 76 ms | 约 15 | 85.7% |

建议**每个分区从 1 个 bucket 起步**，只有并发写入需要更高并行度时再增加。其他参数：

- **`nprobe`**（向量，默认 64）——每个 shard 探测的聚类数；降低可减延迟，提高可增召回。
- **写路径索引 GC**——默认 `gc_grace_seconds = 3600` 时新写入不可能产生可回收文件，因此 GC 已摊销：默认每 16 次写入执行一次，可用 `LAKESOUL_INDEX_GC_EVERY` 调整（设为 `1` 恢复旧的每次写入都执行；grace 为 `0` 时始终每次执行）。
- **`index_build`**——`inline`（默认）在写入返回前完成索引增量构建；`deferred` 只提交数据，由后台任务按 `index_build_interval_secs`（默认 15 秒）构建未入索引的 shard。存在积压期间，检索会精确扫描尚未入索引的数据文件，因此写入立即可检索，代价是读取积压数据；在 20K 文档、1 bucket、每批 500 条的实测中，deferred 写入约 12,900 docs/s（inline 约 3,100 docs/s），recall@100 完全一致；存在积压时检索 p50 约 160 ms，后台追平后约 46 ms。该模式是 provision 时写入的表属性；已有表保持 inline，配置不一致时网关会输出警告。
- **分阶段计时**——启动网关时设置 `LAKESOUL_ES_GATEWAY_TIMING=1` 与 `RUST_LOG=lakesoul_es_gateway::timing=info`，即可按请求打印写（`parse`、`upsert`）与检索（`files`、`resolve`、`lease`、`shard_search`、`fetch`、`verify`）各阶段耗时。

## 限制

- delete/update 通过表扫描解析过滤条件；只有主键有快速路径。
- 建索引时忽略 `number_of_shards`/`number_of_replicas`；分桶数与 `nprobe` 来自网关配置。
- 未实现鉴权、RBAC、高亮、聚合与 `sort`。
- `_search` 支持的查询子集为 `match`、`terms`、`term`、`bool.filter/must/must_not` 以及带 `cosineSimilarity` 的 `script_score`。

## 对接 WeKnora

[WeKnora](https://github.com/Tencent/WeKnora) 通过其 Elasticsearch v7/v8 向量库驱动访问网关，因此 `RETRIEVE_DRIVER=elasticsearch_v8`（或 `elasticsearch_v7`）无需改动客户端即可对接 LakeSoul。

1. **网关**——按 WeKnora 使用的 embedding 维度声明索引：

   ```toml
   [server]
   listen = "0.0.0.0:9200"        # 容器经 host.docker.internal 访问
   version = "8.19.6"

   [lakesoul]
   namespace = "default"
   provision_on_start = true

   [defaults]
   hash_bucket_num = 4
   tokenizer = "jieba"

   [[indexes]]
   name = "WeKnora"               # ELASTICSEARCH_INDEX
   table = "weknora_docs"
   path = "file:///data/weknora_docs"
   dim = 1024                     # 必须与 embedding 模型维度一致
   ```

2. **WeKnora `.env`**——把驱动指向网关并配置 embedding 模型（任意 OpenAI 兼容端点，如百炼）：

   ```sh
   RETRIEVE_DRIVER=elasticsearch_v8
   ELASTICSEARCH_ADDR=http://host.docker.internal:9200
   ELASTICSEARCH_INDEX=WeKnora

   EMBEDDING_PROVIDER=openai
   EMBEDDING_MODEL_NAME=text-embedding-v4
   EMBEDDING_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
   EMBEDDING_API_KEY=sk-...

   # 若 Docker daemon 会给容器注入 HTTP(S)_PROXY，必须把网关排除：
   # 否则驱动发往 host.docker.internal 的请求会被代理拦截并返回 502。
   NO_PROXY=localhost,127.0.0.1,host.docker.internal,.aliyuncs.com
   no_proxy=localhost,127.0.0.1,host.docker.internal,.aliyuncs.com
   ```

   宿主 80 端口被占用时需给 `FRONTEND_PORT` 指定空闲端口。

3. **API 流程**——创建与网关 `dim` 相同维度的 embedding 模型，用它创建知识库，上传文档后检索：

   ```sh
   curl -X POST "$WEKNORA/api/v1/models" -H "Authorization: Bearer $TOKEN"      -d '{"name":"text-embedding-v4","type":"Embedding","source":"remote",
          "parameters":{"base_url":"https://dashscope.aliyuncs.com/compatible-mode/v1",
                        "api_key":"sk-...","provider":"aliyun",
                        "embedding_parameters":{"dimension":1024}}}'
   curl -X POST "$WEKNORA/api/v1/knowledge-bases" -H "Authorization: Bearer $TOKEN"      -d '{"name":"docs","embedding_model_id":"<model id>"}'
   curl -X POST "$WEKNORA/api/v1/knowledge-search" -H "Authorization: Bearer $TOKEN"      -d '{"query":"全文检索","knowledge_base_id":"<kb id>"}'
   ```

   检索响应融合关键词（BM25）与向量（script_score）两路结果，二者都经网关访问同一张 LakeSoul 表。

### 驱动回放工具

`script/weknora-es-harness/` 用相同的 Go 客户端回放驱动的请求序列，无需部署 WeKnora：

```sh
go run ./script/weknora-es-harness --client v8 --addr http://127.0.0.1:9200     --index WeKnora --dim 1024
go run ./script/weknora-es-harness --client v7 --addr http://127.0.0.1:9200     --index WeKnora --dim 1024
```

覆盖版本握手、按 mapping 判定 `.keyword`、单条/批量写入、关键词与向量检索、四个固定 `update_by_query` 脚本、索引拷贝分页与 `delete_by_query`，并断言驱动会解引用的响应字段（`_score`、`total == updated`、无冲突与失败）。

### WeKnora v0.8.0 已知问题

- **未加限幅的 `script_score`**：v0.8.0 镜像发送的是 `cosineSimilarity(params.query_vector, 'embedding')`，没有当前 `main` 分支的 `Math.max(..., 0.0)` 包裹。网关两种写法都接受，并按 `[0,1]` 限幅。
- **`CopyIndices` 丢失 `is_enabled`**：v0.8.0 的拷贝路径构造文档时未带 `is_enabled`，因此 `reuse_vectors` 迁移与知识库克隆的数据会以**禁用**状态落库并被检索过滤掉——在真实 Elasticsearch 上同样如此。迁移请使用 `mode=reparse`；克隆库的 chunk 需要在上游修复前手动重新启用。
