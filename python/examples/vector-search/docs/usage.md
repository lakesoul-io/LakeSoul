# LakeSoul 向量检索：使用说明

本文介绍如何在 LakeSoul 中声明、构建、查询和维护向量索引。

- 架构与概念见 [architecture.md](architecture.md)
- 文档首页见 [README.md](README.md)
- 性能/排障见 [performance.md](performance.md)

---

## 1. 前置条件

1. **主键**：表必须有主键（单列或复合）。索引 key 是主键列的 arrow-Row 编码。
2. **向量列**：
   - 类型：`FixedSizeList` / `List` / `LargeList` 的浮点数组；
   - Python SDK 要求 `Float32`；Rust/DataFusion 还接受 `Float16`/`Float64`；
   - `FixedSizeList` 的长度必须等于配置的 `dim`。
3. **索引参数**：
   - `total_bits` 当前只支持 `1`、`3`、`7`；
   - `metric` 支持 `L2` 和 `IP`（`INNERPRODUCT`）。
4. **元数据**：PostgreSQL 可访问（catalog）。
5. **对象存储**：本地 / S3 / S3A 等，reader 和 builder 使用同一套凭据。

---

## 2. 声明向量索引

### 2.1 SQL（DataFusion）

```sql
CREATE EXTERNAL TABLE "lakesoul".default.docs (
    id        BIGINT NOT NULL PRIMARY KEY,
    title     VARCHAR,
    embedding FLOAT[] NOT NULL
) STORED AS LAKESOUL
LOCATION 's3://my-bucket/tables/docs'
OPTIONS (
    'vector_index_columns'
        '[{"column":"embedding","dim":768,"nlist":256,"total_bits":7,"metric":"L2"}]',
    'hashBucketNum' '4'
);
```

说明：

- `vector_index_columns` 是 JSON 数组，可以声明多个向量列；
- `hashBucketNum` 决定 hash bucket 数，索引按 bucket 分片；
- `LOCATION` 指向表根目录；索引会写到
  `{LOCATION}/_vector_index/{column}/{partition}/{bucket}/`。

### 2.2 Python

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(namespace="default")

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("title", pa.string()),
    pa.field("embedding", pa.list_(pa.float32(), 768), nullable=False),
])

table = catalog.create_table(
    "docs",
    path="s3://my-bucket/tables/docs",
    schema=schema,
    primary_keys=["id"],
    vector_index=[{
        "column": "embedding",
        "dim": 768,
        "nlist": 256,
        "total_bits": 7,
        "metric": "L2",
    }],
)
```

`create_table` 会：

1. 校验主键、向量列类型、`dim`、`total_bits`；
2. 把配置序列化到 `vector_index_columns` 表属性；
3. 之后 `write_arrow` 自动构建/更新索引。

### 2.3 多列

```python
vector_index=[
    {"column": "text_vec",  "dim": 768, "nlist": 256, "metric": "L2"},
    {"column": "image_vec", "dim": 512, "nlist": 128, "metric": "L2"},
]
```

SQL：

```sql
OPTIONS (
  'vector_index_columns'
    '[{"column":"text_vec","dim":768,"nlist":256,"metric":"L2"},
      {"column":"image_vec","dim":512,"nlist":128,"metric":"L2"}]'
)
```

多列查询见第 5.3 节。

---

## 3. 数据写入

### 3.1 Python

```python
batch = pa.record_batch(
    [
        pa.array([1, 2, 3], type=pa.int64()),
        pa.array(["a", "b", "c"]),
        pa.array([[0.1] * 768, [0.2] * 768, [0.3] * 768],
                 type=pa.list_(pa.float32(), 768)),
    ],
    schema=schema,
)
table.write_arrow(batch)
```

写入后：

- 数据文件提交到 metadata；
- 默认自动对受影响的 `(partition, bucket)` 构建/更新索引；
- 如果表属性 `index_maintenance = 'deferred'`，只提交数据，索引由后台任务构建；
- 也可以显式关闭本次写入的索引构建：

```python
table.write_arrow(batch, auto_build_vector_index=False)
```

之后用 `table.build_vector_index()` 或 `build_pending_indices` 补齐。

### 3.2 SQL

```sql
INSERT INTO "lakesoul".default.docs
VALUES (1, 'a', ARRAY[0.1, 0.2, /* ... */]);
```

> 具体数组字面量语法取决于 DataFusion 版本；批量导入建议使用 Python/SDK。

### 3.3 Daft

```python
from lakesoul.daft import write_lakesoul

write_lakesoul(df, table)                  # 写入并自动构建索引
write_lakesoul(df, table, auto_build_vector_index=False)  # 只写数据
```

---

## 4. 索引构建与维护

### 4.1 自动构建

默认行为：

```text
写入 commit
  → auto_build_vector_index
       ├─ 无 index commit → fresh build
       ├─ 有 index commit → delta build
       └─ drift 超阈值   → rebuild
```

### 4.2 延迟构建

建表时：

```sql
OPTIONS ('index_maintenance' 'deferred')
```

或 Python：

```python
table = catalog.create_table(..., properties={"index_maintenance": "deferred"})
```

之后手动构建：

```python
table.build_vector_index()                       # 所有分区、所有配置列
table.build_vector_index(column="embedding")     # 单列
table.build_vector_index(partition_desc="dt=2024-01-01")
table.build_vector_index(partitions={"dt": "2024-01-01"})
```

Rust 侧：

```rust
lakesoul_datafusion::index_maintenance::build_pending_indices(
    client, table_name, namespace,
).await?;
```

### 4.3 模块级构建 API

除了表方法，也可以直接调用模块函数（适合后台任务/调度器）：

```python
from lakesoul.vector_index import (
    build_partition_vector_index,
    build_table_vector_index,
)

# 单个分区
build_partition_vector_index(
    table_name="docs",
    namespace="default",
    partition_desc="dt=2024-01-01",
    vector_column="embedding",
    dim=768,
    nlist=256,
    total_bits=7,
    metric="L2",
    rebuild=False,          # True = 全量重建
)

# 全表
build_table_vector_index(
    table_name="docs",
    namespace="default",
    vector_column="embedding",
    dim=768,
    rebuild=False,
)
```

### 4.4 手动 reindex / rebuild

```python
# 增量更新（新文件 → delta）
table.build_vector_index()

# 全量重建（重新训练 centroid，新 generation）
table.rebuild_vector_index()
table.rebuild_vector_index(column="embedding")
table.rebuild_vector_index(partition_desc="dt=2024-01-01")
```

什么时候 rebuild：

- 数据分布明显漂移；
- 增量 delta 很多，召回下降；
- 大量 upsert/delete 后，mutation 状态变大；
- 升级旧索引后，需要给所有 segment 打 `birth_version`。

### 4.5 删除索引中的 key

```python
from lakesoul.vector_index import delete_by_primary_keys

delete_by_primary_keys(
    table_name="docs",
    namespace="default",
    vector_column="embedding",
    keys=pa.table({"id": pa.array([1, 2, 3], type=pa.int64())}),
)
```

注意事项：

- 只写 **索引 tombstone**，不删除数据行；
- 必须同时删除/更新表数据，否则 rebuild 会重新索引这些行；
- 删除不存在的 key 也会写 tombstone，不报错；
- 该 API 会在每个 partition、每个 shard 上各提交一次。

### 4.6 GC

```rust
use lakesoul_datafusion::vector_index::{VectorIndexGcOptions, gc_vector_index};

let options = VectorIndexGcOptions {
    grace_seconds: 3600,
    keep_generations: 1,
    drop_orphan_shards: true,
};
let report = gc_vector_index(
    &client,
    "docs",
    "default",
    &object_store_options,   // HashMap<String, String>
    &options,
).await?;
```

默认值：

- `grace_seconds = 3600`
- `keep_generations = 1`
- `drop_orphan_shards = true`

写入路径也会按 `LAKESOUL_INDEX_GC_EVERY`（默认 16 次写）摊销执行 GC。

---

### 4.7 删除索引（drop_vector_index）

完整说明见 [drop-index.md](drop-index.md)。

```python
# 删除并移除声明；tombstone 保留，防止旧 writer 重建
table.drop_vector_index(column="embedding")

# 删除但保留声明，之后可 rebuild
table.drop_vector_index(column="embedding", keep_declaration=True)
table.rebuild_vector_index(column="embedding")

# 默认删除后重建：重新声明 + 全量 rebuild
table.recreate_vector_index(
    column="embedding",
    dim=768,
    nlist=1024,
    total_bits=7,
    metric="L2",
)

# 异步清理对象
result = table.drop_vector_index(
    column="embedding",
    keep_declaration=True,
    grace_seconds=300,
    wait_for_cleanup=False,
)
# result["cleanup"] == "deferred"
# result["drop_id"] 标识后台任务

# 失败/崩溃后清对象；保留 tombstone
table.gc_dropped_vector_index_objects()
```

语义：

- 只删索引，不删数据文件、分区、schema、commit、快照；
- 删除声明后可以通过 `declare_vector_index` / `recreate_vector_index` 重建；
- 索引维护失败不会让数据写入失败（`IndexDropped` 被视为 no-op）。

## 5. 查询

### 5.1 SQL：exact（默认）

```sql
SELECT id, title
FROM "lakesoul".default.docs
ORDER BY array_distance(embedding, ARRAY[0.1, 0.2, /* ... 768 个 */])
LIMIT 10;
```

执行过程：

1. 规则匹配 `Limit → Sort → TableScan`；
2. scan 切换为向量索引候选；
3. 每个 bucket 返回 `top_k * 10`（最少 100）候选；
4. 如果启用 `deferred_payload`（默认），执行节点先按精确/索引距离排序，
   只保留 `keep_rows`（约 `max(limit, top_k) * 2 + 16`）再交给上层；
5. `Sort` 对候选行读向量列，算精确距离；
6. `Limit` 取全局 top-k。

### 5.2 SQL：index-only

```sql
SET lakesoul_vector_search.rerank = 'index';

SELECT id
FROM "lakesoul".default.docs
ORDER BY array_distance(embedding, ARRAY[...])
LIMIT 10;
```

特点：

- 不读向量列；
- 由 `LakeSoulVectorSearchExec` 按索引量化距离排序；
- 仅支持 L2；
- 距离是量化的，召回/排序与精确值可能有差异。

### 5.3 SQL：内积与多列

```sql
-- 内积：降序
SELECT id
FROM docs
ORDER BY inner_product(embedding, ARRAY[...]) DESC
LIMIT 10;

-- 多列加权融合（各列 metric 必须一致；index-only 仅 L2）
SELECT id
FROM docs
ORDER BY 0.7 * array_distance(text_vec, ARRAY[...])
       + 0.3 * array_distance(image_vec, ARRAY[...])
LIMIT 10;
```

多列时：

- 每个列独立 ANN；
- 候选取**交集**；
- exact 模式由上层 Sort 算加权精确距离；
- index-only 模式由 reader 算加权索引距离。

### 5.4 SQL：确认是否走索引

```sql
EXPLAIN
SELECT id FROM docs
ORDER BY array_distance(embedding, ARRAY[...])
LIMIT 10;
```

期望看到：

```text
LakeSoulVectorSearchExec(vec=embedding, top_k=10, metric=L2, order_by=false)
```

如果看到普通 `TableScan` + `Sort`，说明没有下推，常见原因见第 10 节。

### 5.5 Python/Daft：高层 API

```python
from lakesoul.daft import vector_search, multi_vector_search

# 单列，exact
df = vector_search(table, query, top_k=10, nprobe=64)
rows = df.collect().to_arrow()

# index-only
df = vector_search(table, query, top_k=10, rerank="index")

# 多列
df = multi_vector_search(
    table,
    {"text_vec": q1, "image_vec": q2},
    top_k=10,
    weights={"text_vec": 0.7, "image_vec": 0.3},
)
```

参数：

| 参数 | 说明 |
|---|---|
| `top_k` | 全局返回条数 |
| `nprobe` | 每个 shard 探测的 cluster 数 |
| `column` | 向量列；单列索引可省略 |
| `metric` | `L2` / `IP`；默认取表属性 |
| `extra_columns` | 额外返回列 |
| `rerank` | `exact` / `index` |
| `parallelism` | 每 reader 并行探测的 cluster 组数 |
| `with_distance` | 是否返回 `vector_distance` 列 |

### 5.6 Python：NativeSession（湖仓 SQL + 快路径检索）

`LakeSoulSession` 是进程内 DataFusion 会话（`ci-unify` 构建特性），Python 的向量检索
就是它上面的一条 SQL；`table.search` 只是语法糖，因此自动获得 filter 扩探、任意投影、
多版本/CDC/分区语义与索引快路径。

```python
from lakesoul import LakeSoulCatalog, LakeSoulSession

with LakeSoulSession.from_env(vector_search={"nprobe": 48}) as session:
    # 1) 任意湖仓 SQL（可 join/聚合/过滤）
    rows = session.sql('''SELECT id, title FROM "lakesoul".default.docs
                         WHERE lang = 'zh'
                         ORDER BY array_distance(embedding, ARRAY[0.1, 0.2, /*...*/])
                         LIMIT 10''')
    # 2) 单查询检索（等价 SQL，由 session 执行）
    docs = LakeSoulCatalog.from_env().table("docs")
    top = docs.search(query_vec, top_k=10, filter="lang = 'zh'",
                      columns=["id", "title"], with_distance=True,
                      session=session)
    # 3) 确认走了索引
    print(session.explain(docs.to_sql(query_vec, top_k=10)))
    # 4) 索引排序（不读向量列，近似、更快）
    fast = docs.search(query_vec, top_k=10, rerank="index", session=session)
```

#### 批量、适配器与分布式

```python
# 批量：优先原生快照批量（单 Int64 `id` 主键），否则逐条 SQL 回退
batch = docs.search_batch(queries, top_k=100, exact_overfetch=10, session=session)
ids = batch.ids                      # 每个 query 一个主键列表
arrow = batch.to_arrow()             # (query_id, rank, id[, distance])

# 输出适配器（只包装 Arrow 结果，不会重新扫描湖仓）
daft_df = docs.search(query_vec, top_k=10, to="daft", session=session)
duck = session.sql("SELECT ...", to="duckdb")

# 分布式 driver 归并：按 range partition 分给多个 worker，driver 合并 top-k
dist = docs.distributed(engine="threads", num_workers=8)   # 或 "ray"/"dask"
rows = dist.search(query_vec, top_k=10, filter="lang = 'zh'")
info = dist.sql("SELECT lang, count(*) FROM ... GROUP BY lang")   # 当前由 driver 单机计划

# 分布式 coordinator（需要部署 lakesoul-worker 集群）
session = LakeSoulSession.from_env(distributed={
    "worker_urls": ["grpc://worker1:50051", "grpc://worker2:50051"],
    "target_partitions": 8,
})
```

#### 时间旅行（快照 / tag / as-of）

```python
snapshot_id = catalog.create_snapshot(table)        # 冻结当前各分区版本
catalog.create_tag(table, "v1", snapshot_id)        # 给快照打 tag

# 1) 检索固定快照（结果=该时刻的行，merge-on-read 语义不变）
old_rows = table.search(query_vec, top_k=10, snapshot=snapshot_id, session=session)
tag_rows = table.search(query_vec, top_k=10, tag="v1", session=session)
time_rows = table.search(query_vec, top_k=10, as_of=snapshot.created_at, session=session)

# 2) 任意湖仓 SQL 也支持固定版本（本表引用会自动改写到时间旅行视图）
count = table.sql('SELECT count(*) FROM "lakesoul".default.docs',
                  snapshot=snapshot_id, session=session)

# 3) 或手动注册/注销一个时间旅行视图，供多条 SQL 共享
alias = session.register_snapshot("docs", tag="v1")
try:
    session.sql(f"SELECT * FROM {alias} WHERE lang = 'zh' LIMIT 5")
finally:
    session.unregister(alias)
```

固定快照的读取**不使用向量/文本索引**（索引描述的是最新文件），因此走普通 merge-on-read
扫描 + 精确距离排序：结果正确，延迟高于最新版本检索。一次只能指定 snapshot/tag/as_of 之一。

`search` 支持 `column/top_k/metric/rerank/nprobe/exact_overfetch/filter/columns/with_distance/snapshot/tag/as_of`；
`to_sql()` 返回实际执行的 SQL，便于审计或在其它 DataFusion 前端复用。
`snapshot=`/`tag=` 需要原生扫描器的快照读取能力（当前未开放，传入会报 `NotImplementedError`）。

### 5.7 Python：低层 scan（拿候选）

```python
candidates = (
    table.scan()
    .options(reader_options={
        "vector_search_column": "embedding",
        "vector_search_query": ",".join(f"{v:.6f}" for v in query),
        "vector_search_top_k": "100",     # 每个 shard 的候选数
        "vector_search_nprobe": "64",
        "vector_search_metric": "L2",
    })
    .to_daft()
    .collect()
    .to_arrow()
)
```

注意：

- 返回的是**每个 bucket 的候选行**，不是全局 top-k；
- 需要自己按距离排序/截断；
- 如果设置 `vector_search_order_by=true`，会多出
  `__lakesoul_vector_distance` 列。

### 5.7 Native Rust Reader

Native Reader 需要调用方先解析 shard：

```rust
// 伪代码：完整实现可参考 python/src/index.rs 的 resolve_index_shards
let client = MetaDataClient::from_env().await?;
let catalog = client.vector_index_catalog();
let view = catalog.resolve(index_prefix).await?.unwrap();

let resolved = ResolvedIndex {
    kind: IndexKind::Vector,
    index_prefix: index_prefix.to_string(),
    commit_id: view.commit_id,
    generation: view.generation,
    version: view.version,
    header: view.header.clone(),
    segments: serde_json::to_value(&segment_entries)?, // SegmentEntry 数组
    mutation_head: view.mutation_head.clone(),
    mutation_records: view.mutation_records,
};

let config = LakeSoulIOConfigBuilder::new()
    .with_files(files)
    .with_prefix(table_prefix)
    .with_primary_keys(vec!["id".to_string()])
    .with_schema(schema)
    .with_option("vector_search_column", "embedding")
    .with_option("vector_search_query", query_csv)
    .with_option("vector_search_top_k", "100")
    .with_option("vector_search_nprobe", "64")
    .with_resolved_index_shards(vec![resolved])
    .build();

let mut reader = LakeSoulReader::new(config)?;
reader.start().await?;
while let Some(batch) = reader.next_rb().await {
    let batch = batch?;
    // 处理候选行
}
```

### 5.8 ES Gateway（可选）

如果启用了 ES 兼容网关，可以用 `knn`：

```json
POST /docs/_search
{
  "size": 10,
  "knn": {
    "field": "embedding",
    "query_vector": [0.1, 0.2, 0.3],
    "k": 10,
    "num_candidates": 100
  }
}
```

说明：

- 网关要求该列已在表的 `vector_index_columns` 中声明；
- `knn` 的候选由 `search_resolved_shard` 产生，再按网关的评分规则重排；
- 网关还支持 `script_score`（cosine）形式，内部会归一化向量并做精确重排；
- 具体 metric 兼容性以网关实现和表属性为准，建议先用 `knn` 验证。

---

## 6. 配置参考

### 6.1 表属性 `vector_index_columns`

| 字段 | 默认 | 说明 |
|---|---|---|
| `column` | 必填 | 向量列名 |
| `dim` | 必填 | 向量维度 |
| `nlist` | 256 | IVF cluster 数；实际 clamp 到 `min(nlist, N)` |
| `total_bits` | 7 | RaBitQ 总位数，只支持 1/3/7 |
| `metric` | `L2` | `L2` / `IP` |
| `rotator_type` | `FhtKac` | `FhtKac` / `Matrix` |
| `seed` | 42 | 随机种子 |
| `use_faster_config` | true | 快速量化（精度损失 <1%） |
| `rebuild_mode` | `auto` | `auto` / `none` |
| `max_delta_ratio` | 1.0 | 任一 cluster 的 delta/base 超过该值触发 rebuild |
| `gc_enabled` | true | 写入后是否执行 GC |
| `gc_grace_seconds` | 3600 | GC grace |
| `gc_keep_generations` | 1 | 保留的 generation 数 |

### 6.2 Reader options

| Key | 默认 | 说明 |
|---|---|---|
| `vector_search_column` | 必填 | 向量列 |
| `vector_search_query` | 必填 | 逗号分隔的 f32 |
| `vector_search_top_k` | 10 | **每个 shard** 的候选数 |
| `vector_search_nprobe` | 64 | 探测 cluster 数 |
| `vector_search_parallelism` | 1 | 并行探测组数 |
| `vector_search_metric` | L2 | 仅用于选择/校验，header 是权威 |
| `vector_search_order_by` | false | 输出 `__lakesoul_vector_distance` |
| `vector_search_requests` | — | 多列 JSON 数组 |

多列 JSON 示例：

```json
[
  {"column":"text_vec","query":"0.1,0.2","top_k":100,"weight":0.7},
  {"column":"image_vec","query":"0.3,0.4","top_k":100,"weight":0.3}
]
```

### 6.3 SQL session 配置

```sql
SET lakesoul_vector_search.nprobe = 128;
SET lakesoul_vector_search.rerank = 'index';
SET lakesoul_vector_search.parallelism = 4;
SET lakesoul_vector_search.deferred_payload = false;
```

| Key | 默认 | 说明 |
|---|---|---|
| `nprobe` | 64 | 探测 cluster 数 |
| `rerank` | `exact` | `exact` / `index` |
| `parallelism` | 1 | 并行探测组数 |
| `deferred_payload` | true | 两阶段读 payload |

### 6.4 环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `LAKESOUL_INDEX_CACHE_BYTES` | 512 MiB | 已加载索引的进程缓存 |
| `LAKESOUL_VECTOR_INDEX_CACHE_BYTES` | 同上 | 旧变量名 |
| `LAKESOUL_VECTOR_INDEX_LEASE_TTL_SECS` | 300 | reader lease TTL |
| `LAKESOUL_INDEX_GC_EVERY` | 16 | 每 N 次写执行一次摊销 GC |
| `LAKESOUL_CACHE` | — | pk locator 使用的本地磁盘缓存 |
| `LAKESOUL_VECTOR_SEARCH_PROFILE` | — | 打印每个 bucket 的耗时 |
| `LAKESOUL_READER_PLAN` | — | 打印 reader 物理计划 |
| `LAKESOUL_PK_PROFILE` | — | 打印 pk locator 耗时 |

---

## 7. 参数调优

### 7.1 `nlist`

- 越大：每个 cluster 越小，检索越快，但训练/构建越慢，且需要更多 nprobe 才能覆盖；
- 经验：从 `256` 开始，数据量到千万级可考虑 `1024`；
- 实际会被 clamp 到 `min(nlist, N)`。

### 7.2 `total_bits`

- `7`（默认）：召回最好，存储/计算最大；
- `3`：内存/带宽更小，召回下降；
- `1`：最省，召回最低；
- 只支持 1/3/7。

### 7.3 `nprobe`

- 默认 64；
- 召回优先：128 / 256；
- 延迟优先：16 / 32；
- 和 `nlist` 一起调：`nprobe / nlist` 大致对应扫描比例。

### 7.4 `top_k` 与 overfetch

- SQL exact 模式：每 shard `top_k * 10`（最少 100），全局 Sort 再截断；
- SQL index-only：每 shard `top_k * 2`（有 filter 时 10x）；
- Python exact：每 shard `top_k`；
- Python index：每 shard `max(top_k*2, top_k+8)`。

候选预算不足会导致：

- 过滤/删除后结果少于 k；
- 召回下降。

### 7.5 `parallelism`

- 默认 1（串行探测 cluster）；
- 增大可降低单查询延迟，但会占用更多线程；
- 高并发场景保持 1，避免线程争用。

### 7.6 缓存

- 索引缓存默认 512 MiB；
- 低于索引大小时，每次查询都会重新加载，延迟显著上升；
- 大索引可调大 `LAKESOUL_INDEX_CACHE_BYTES`，或减少 shard 数。

### 7.7 rebuild

- `rebuild_mode=auto` + `max_delta_ratio` 控制自动 rebuild；
- 高频 upsert/delete 后，mutation key 集合增长，建议定期手动 rebuild；
- 数据分布漂移后 rebuild 能恢复召回。

---

## 8. 完整示例

### 8.1 SQL

```sql
-- 1. 建表
CREATE EXTERNAL TABLE "lakesoul".default.docs (
    id        BIGINT NOT NULL PRIMARY KEY,
    title     VARCHAR,
    embedding FLOAT[] NOT NULL
) STORED AS LAKESOUL
LOCATION 's3://my-bucket/tables/docs'
OPTIONS (
    'vector_index_columns'
        '[{"column":"embedding","dim":768,"nlist":256,"total_bits":7,"metric":"L2"}]',
    'hashBucketNum' '4'
);

-- 2. 写入（略；建议用 SDK 批量写）

-- 3. 查询
SELECT id, title
FROM "lakesoul".default.docs
ORDER BY array_distance(embedding, ARRAY[0.1, 0.2, /* ... */])
LIMIT 10;

-- 4. 确认计划
EXPLAIN
SELECT id FROM "lakesoul".default.docs
ORDER BY array_distance(embedding, ARRAY[0.1, 0.2, /* ... */])
LIMIT 10;

-- 5. index-only
SET lakesoul_vector_search.rerank = 'index';
SELECT id
FROM "lakesoul".default.docs
ORDER BY array_distance(embedding, ARRAY[0.1, 0.2, /* ... */])
LIMIT 10;
```

### 8.2 Python

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog
from lakesoul.daft import vector_search

catalog = LakeSoulCatalog.from_env(namespace="default")
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("title", pa.string()),
    pa.field("embedding", pa.list_(pa.float32(), 768), nullable=False),
])

table = catalog.create_table(
    "docs",
    path="s3://my-bucket/tables/docs",
    schema=schema,
    primary_keys=["id"],
    vector_index=[{
        "column": "embedding", "dim": 768,
        "nlist": 256, "total_bits": 7, "metric": "L2",
    }],
)

table.write_arrow(batch)

df = vector_search(table, query, top_k=10, nprobe=64)
rows = df.collect().to_arrow()

# 维护
table.build_vector_index()
table.rebuild_vector_index()
```

完整可运行示例见 [tutorial/README.md](../tutorial/README.md)：包含建表、写入、
exact / index-only 检索，以及 MinIO 端到端示例。

---

## 9. 常见错误

| 错误 | 原因 | 处理 |
|---|---|---|
| `a vector index requires an id column` | 没有主键 | 建表时指定 `primary_keys` / `PRIMARY KEY` |
| `vector index column '...' not found` | 列名拼错 | 检查 schema |
| `dim ... does not match schema FixedSizeList size` | `dim` 与列长度不一致 | 修正 `dim` |
| `total_bits must be 1, 3 or 7` | 传了其他位宽 | 改为 1/3/7 |
| `vector column ... must hold Float32` | Python SDK 类型限制 | 用 Float32 或改 Rust 路径 |
| `query vector dimension mismatch` | 查询向量长度不对 | 检查 query 长度 |
| `vector index at '...' was not resolved by the caller` | 索引未建/未 resolve | 先构建索引，或检查 shard 前缀 |
| `key layout changed` | 主键 schema 变了 | rebuild 索引 |
| 查询结果少于 k | 候选不足/被过滤 | 增大 `top_k`、`nprobe`、overfetch |
| `EXPLAIN` 没有 `LakeSoulVectorSearchExec` | 未下推 | 检查 SQL 形态、索引声明、metric、PK |

更多排查见 [performance.md](performance.md)。
