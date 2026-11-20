---
sidebar_position: 2
sidebar_label: API 参考
---

# 向量检索 API 参考

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

本文是 LakeSoul 向量检索的完整接口说明：Python SDK、DataFusion SQL、pgwire（PostgreSQL 兼容）与 Rust。索引构建参数的默认值与缓存配置见[配置](configuration.md)，10 分钟上手见[快速开始](quickstart.md)。

## 入口概览

| 入口 | 适合场景 | 入口对象 |
|---|---|---|
| Python | 数据工程、Notebook、脚本 | `LakeSoulCatalog` / `LakeSoulTable` / `LakeSoulSession` |
| SQL / DataFusion | 数仓 SQL、报表、BI | `LakeSoulSession.sql` / DataFusion `SessionContext` |
| pgwire | 存量 pgvector 应用 | `postgres-lakesoul` + `psycopg` |
| Rust | 引擎集成、服务端 | `lakesoul-datafusion` / `lakesoul-io` |

四个入口查到的是**同一张表、同一份快照**：向量索引只负责召回候选主键，行内容、upsert/delete 可见性和分区裁剪仍由 LakeSoul 的 merge-on-read 与表提交语义决定。

## 1. Python API

### 1.1 Catalog 与建表

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(
    namespace="default",                 # 默认 namespace
    object_store_options=None,           # S3/MinIO 时传对象存储选项
)
table = catalog.table("docs")            # 加载已有表
```

`create_table` 的签名（关键字参数）：

```python
table = catalog.create_table(
    name,                                # 表名
    *,
    path,                                # 表根目录（file:// / s3://）
    schema,                              # pyarrow.Schema
    namespace=None,
    partition_by=(),                     # 分区列
    primary_keys=(),                     # 主键列（必须有）
    hash_bucket_num=None,                # hash bucket 数
    properties=None,                     # 表属性
    vector_index=None,                   # 向量索引声明（list[dict]）
    index_configs=None,                  # 索引种类 -> 声明的通用写法
    domain="public",
)
```

建表时校验：

- 表必须有主键，类型为 `Int64` 或 `UInt64`；
- 每个向量列必须存在，Python SDK 要求类型为 `FixedSizeList<Float32>`；
- 声明的 `dim` 必须与 `FixedSizeList` 长度一致；
- `total_bits` 只支持 `1` / `3` / `7`，`metric` 支持 `L2` / `IP`。

```python
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("title", pa.string()),
    pa.field("embedding", pa.list_(pa.field("item", pa.float32()), 768), nullable=False),
])

table = catalog.create_table(
    "docs",
    path="s3://my-bucket/tables/docs",
    schema=schema,
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

`vector_index=` 只是 `index_configs={"vector": ...}` 的简写；声明会序列化为表属性 `vector_index_columns`（JSON 数组），之后写入自动维护索引。也可以直接写属性：

```python
import json
table = catalog.create_table(
    "docs",
    path="s3://my-bucket/tables/docs",
    schema=schema,
    primary_keys=["id"],
    properties={
        "vector_index_columns": json.dumps(
            [{"column": "embedding", "dim": 768, "nlist": 256, "metric": "L2"}]
        )
    },
)
```

索引构建参数的完整字段与默认值见[配置](configuration.md#索引构建参数)。

表信息属性：

```python
table.path                # 表根目录
table.primary_keys        # 主键列
table.hash_bucket_num     # hash bucket 数
table.vector_index_columns# 已声明向量列
```

### 1.2 写入

```python
table.write_arrow(batch)                                    # 提交数据 + 增量维护索引
table.write_arrow(batch, auto_build_vector_index=False)     # 只写数据
table.write_daft(daft_df, vector_index_cpus=1)              # Daft 分布式写入与建索引
```

- 数据文件和索引文件都不可变；更新通过追加 + merge-on-read 完成；
- 默认在数据提交后自动增量更新索引（只索引本次新文件，不重写历史 segment）；
- 若表属性 `index_maintenance = 'deferred'`，写入只提交数据，索引由后台任务构建；
- SQL `INSERT` / Spark / Flink 写入同一张表时，由引擎在提交后执行同样的自动维护。

### 1.3 构建、重建与声明

```python
# 补建/增量构建：参数默认取表属性，显式参数覆盖
table.build_vector_index(
    column=None,             # 向量列；None = 所有声明列
    dim=None, nlist=None, total_bits=None,
    metric=None, rotator_type=None, seed=None, use_faster_config=None,
    partition_desc=None,     # "range=2024-01-01"，只构建单个分区
    partitions=None,         # 分区映射简写
)

# 全量重建：重读 shard 的全部活跃数据文件、重训 IVF 质心、发布新 generation
table.rebuild_vector_index(
    column=None,
    partition_desc=None,
    partitions=None,
)
```

`rebuild_vector_index` 不受 `rebuild_mode` 限制，适用于：

- 数据分布明显漂移、召回下降；
- 增量 delta 很多，索引打开/合并变慢；
- 大量 upsert/delete 后 mutation 链变大；
- 调整 `nlist` / `total_bits` / `metric` 后重新训练。

重建期间查询服务不受影响：新 generation 原子发布，reader 在解析到新 commit 时切换；旧 generation 由 GC 回收。

模块级构建 API（适合后台任务/调度器）：

```python
from lakesoul.vector_index import (
    build_partition_vector_index,
    build_table_vector_index,
)

build_partition_vector_index(
    table_name="docs", namespace="default",
    partition_desc="dt=2024-01-01",
    vector_column="embedding", dim=768,
    nlist=256, total_bits=7, metric="L2",
    rotator_type="FhtKac", seed=42, use_faster_config=True,
    store_config=None,       # 对象存储凭据；None 时读 LAKESOUL_OBJECT_STORE_*
    rebuild=False,           # True = 全量重建
)

build_table_vector_index(
    table_name="docs", namespace="default",
    vector_column="embedding", dim=768,
    rebuild=False,
)
```

重新声明与重建已删除的索引：

```python
table.declare_vector_index([{                 # 写回 vector_index_columns，并清除 drop tombstone
    "column": "embedding", "dim": 768, "nlist": 1024, "metric": "L2",
}])
table.recreate_vector_index(                  # = declare + rebuild
    column="embedding", dim=768, nlist=1024,
    total_bits=7, metric="L2", rotator_type="FhtKac",
    seed=42, use_faster_config=True,
    partition_desc=None, partitions=None,
)
```

### 1.4 删除索引与 GC

```python
result = table.drop_vector_index(
    column="embedding",          # None = 所有声明列
    partition_desc=None,         # 只删一个分区
    partitions=None,             # 分区映射简写
    keep_declaration=None,       # 默认：整表删除时移除声明，分区级删除时保留
    purge_files=True,            # 是否删除 _vector_index/ 对象
    dry_run=False,
    if_exists=True,
    grace_seconds=0.0,           # 对象删除保护期
    wait_for_cleanup=True,       # False = 后台清理，立即返回
)

# 分区级删除（强制保留声明）
table.drop_vector_index(column="embedding", partition_desc="dt=2024-01-01")

# 删除但保留声明，之后可 rebuild
table.drop_vector_index(column="embedding", keep_declaration=True)
table.rebuild_vector_index(column="embedding")

# 失败/崩溃后清理对象；保留 tombstone，幂等
table.gc_dropped_vector_index_objects()

# Catalog 入口
catalog.drop_vector_index("docs", column="embedding", keep_declaration=True)
```

返回字典：

```python
{
    "table_name": "docs",
    "namespace": "default",
    "columns": ["embedding"],
    "partitions": ["-5"],
    "shards_dropped": 4,
    "objects_deleted": 1024,
    "bytes_deleted": 162010176,
    "declaration_removed": True,
    "dry_run": False,
    "epoch": 3,
    "drop_id": "c1b8...",
    "cleanup": "tombstone_retained",
}
```

`cleanup` 取值：

| 值 | 含义 |
|---|---|
| `completed` | 对象清理完成（声明保留） |
| `tombstone_retained` | 对象清理完成，声明已移除，tombstone 保留防旧 writer 重建 |
| `deferred` | 控制面已删除，后台任务继续清理对象；`drop_id` 标识任务 |

只删索引中的部分 key（不删除数据行）：

```python
from lakesoul.vector_index import delete_by_primary_keys

delete_by_primary_keys(
    table_name="docs",
    namespace="default",
    vector_column="embedding",
    keys=pa.table({"id": pa.array([1, 2, 3], type=pa.int64())}),
    store_config=None,
)
```

> **注意**：`delete_by_primary_keys` 只写索引 tombstone，不删除表数据行。必须同时删除/更新数据，否则 `rebuild_vector_index()` 会从数据文件重新把这些行索引回来。该 API 会在每个分区、每个 shard 上各提交一次。

`drop_table` 会级联清理 `index_shard / index_commit / index_lease / index_drop` 控制面，避免同一路径重建表继承旧索引；对象文件仍由表目录清理/维护任务处理。

### 1.5 查询

`table.search` 的完整参数：

```python
result = table.search(
    query,                    # 查询向量（维度须与索引一致）
    column=None,              # 多列时必填；单列自动识别
    top_k=10,
    metric=None,              # "L2" / "IP"；默认取索引 metric
    rerank="index",           # "index"（默认，不读向量列）/ "exact"（精确重排）/ "auto"
    nprobe=None,              # 每个 bucket 探测的 cluster 数；默认取会话设置
    exact_overfetch=None,     # exact 模式每 bucket 候选倍数；默认 10
    filter=None,              # SQL 谓词，如 "lang = 'zh'"
    columns=None,             # 投影；默认返回所有非向量列
    with_distance=False,      # 是否返回 __lakesoul_vector_distance
    session=None,             # 复用会话（推荐）
    snapshot=None, tag=None, as_of=None,   # 固定快照/标签/时间点，三选一
    to=None,                  # 输出适配器："daft" 等
)
```

- 结果按距离排序：L2 升序、IP 降序；`top_k` 是跨 bucket 合并后的全局 top-k；
- `snapshot` / `tag` / `as_of` 固定快照读取**不使用向量索引**（索引描述最新文件），走普通 merge-on-read 扫描 + 精确距离排序，结果正确但延迟更高；
- `table.to_sql(query, top_k=10)` 返回实际执行的 SQL，便于审计或换前端复用。

批量查询：

```python
batch = table.search_batch(
    queries,                  # (nq, dim)
    column=None,
    top_k=10,
    metric=None,
    rerank="index",           # "index" / "exact" / "auto"
    nprobe=None,
    exact_overfetch=None,
    batch_size=1024,
    parallelism=1,            # 单 shard 内 cluster 探测并行度
    session=None,
)
batch.ids                     # 每个 query 一个主键列表
batch.to_arrow()              # (query_id, rank, id[, distance])
```

`search_batch` 优先走原生快照批量入口（整批 query 只解析一次 shard、只做一轮排序）；当表不满足原生入口条件（单 `Int64` 主键且列名 `id`；exact 模式还要求覆盖性证明、`LAKESOUL_CACHE` decoded-block 缓存和每个文件的 index rank）时，自动退回逐 query SQL，结果 id 一致但更慢。

直接使用原生入口（不静默回退，错误直接暴露）：

```python
session = catalog.session()
result = session.batch_search(
    "docs", queries,
    namespace="default",
    column="embedding",       # 必填
    top_k=10, nprobe=64, parallelism=1,
    batch_size=0,             # 0 = 使用 API 默认
    exact=False,
    exact_overfetch=10,
    metric="L2",
)
```

会话 SQL 与设置：

```python
session = catalog.session(vector_search={"nprobe": 64, "rerank": "exact"})
session.sql("SET lakesoul_vector_search.nprobe = 128")
session.set_vector_search(nprobe=128, rerank="index", exact_overfetch=10)
session.explain(sql)          # 检查是否出现 LakeSoulVectorSearchExec
session.sql(sql).to_pydict()
```

低层 scan（拿每个 bucket 的候选，自己做合并）：

```python
candidates = (
    table.scan()
    .options(reader_options={
        "vector_search_column": "embedding",
        "vector_search_query": ",".join(f"{v:.6f}" for v in query),
        "vector_search_top_k": "100",       # 每个 shard 的候选数
        "vector_search_nprobe": "64",
        "vector_search_parallelism": "1",
        "vector_search_metric": "L2",
        "vector_search_order_by": "false",  # true 时输出 __lakesoul_vector_distance
    })
    .to_arrow_table()
)
top_k = rerank_by_distance(candidates, query, "embedding", 10, metric="L2")
```

Reader options：

| Key | 默认 | 说明 |
|---|---|---|
| `vector_search_column` | 必填 | 向量列 |
| `vector_search_query` | 必填 | 逗号分隔的 float32 查询向量 |
| `vector_search_top_k` | 10 | **每个 shard** 的候选数 |
| `vector_search_nprobe` | 64 | 探测 cluster 数 |
| `vector_search_parallelism` | 1 | 并行探测组数 |
| `vector_search_metric` | L2 | 仅用于选择/校验，索引 header 是权威 |
| `vector_search_order_by` | false | 输出 `__lakesoul_vector_distance` |
| `vector_search_requests` | — | 多列 JSON 数组（见多列章节） |

Daft 侧的高层检索：

```python
from lakesoul.daft import vector_search, multi_vector_search

df = vector_search(table, query, top_k=10, nprobe=64)        # exact 重排（默认）
df = vector_search(table, query, top_k=10, rerank="index")    # index-only
df = multi_vector_search(
    table, {"text_vec": q1, "image_vec": q2},
    top_k=10, weights={"text_vec": 0.7, "image_vec": 0.3},
)
rows = df.collect().to_arrow()
```

参数：`top_k`、`nprobe`、`column`（单列可省略）、`metric`（默认取表属性）、`extra_columns`、`rerank`、`parallelism`、`with_distance`。

## 2. SQL（DataFusion）

### 2.1 建表声明

表和索引通过 SQL `CREATE EXTERNAL TABLE` 的 `OPTIONS` 声明（与 Spark / Flink 大小写写法一致）：

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

- `vector_index_columns` 是 JSON 数组，可以一次声明多个向量列；
- `hashBucketNum` 决定 hash bucket 数，索引按 bucket 分片；
- 索引写入 `{LOCATION}/_vector_index/{column}/{partition}/{bucket}/`；
- 也可以用 Python 建表，两种方式创建的表互相可见、共享同一套索引；
- PostgreSQL 端点还接受 `vector(n)` 列、`CREATE INDEX ... USING
  hnsw|ivfflat|gin`、`REINDEX TABLE` 和 deferred 维护，详见
  [向量与文本索引 DDL](../postgres/index-ddl.md)。

### 2.2 向量类型与字面量

查询向量支持字符串字面量、数组和 cast，`$n` 参数用于预处理语句（pgwire）：

```sql
embedding <-> '[0.12, 0.98, 0.31]'          -- 字符串字面量
embedding <-> ARRAY[0.12, 0.98, 0.31]       -- 数组
CAST('[0.12, 0.98, 0.31]' AS vector(3))     -- 显式类型
SELECT id FROM docs ORDER BY embedding <-> $1 LIMIT 10;   -- 预处理参数
```

列是浮点向量（Python SDK 建表为 `FixedSizeList<Float32>`；SQL 建表可用 `FLOAT[]`，维度由索引配置的 `dim` 声明），查询向量维度必须与 `dim` 一致，否则报错（pgwire 的 `$1` / 字符串字面量也会按 `vector(n)` 或对侧列维度校验，不会只比较共享前缀）。

### 2.3 距离操作符与函数

| 写法 | 返回 | 走索引 | 排序方向 |
|---|---|---|---|
| `<->`（pgwire）/ `array_distance(a, b)`（DataFusion） | 欧氏距离（开平方） | 是（L2） | 升序 |
| `<#>`（pgwire）/ `inner_product(a, b)`（DataFusion） | **负**内积 | 是（IP） | 升序（等价于内积降序） |
| `<=>`（pgwire） | Cosine 距离 | 否，仅精确计算 | 升序 |

`<#>` 返回负内积，因此升序排列与 pgvector 的排序一致。Cosine 查询结果正确但不走索引；需要带索引的 cosine 相似度时，写入时归一化向量并使用 `<#>`。

### 2.4 Top-k 查询形状

规划器识别 `ORDER BY <距离表达式> LIMIT k` 形状，把 top-k 下推到 ANN 扫描：

```sql
-- exact 重排（默认是按索引距离排序；这里显式要求精确重排）
SET lakesoul_vector_search.rerank = 'exact';

SELECT id, title,
       array_distance(embedding, ARRAY[0.1, 0.2, /* ... */]) AS distance
FROM "lakesoul".default.docs
ORDER BY array_distance(embedding, ARRAY[0.1, 0.2, /* ... */])
LIMIT 10;

EXPLAIN
SELECT id FROM "lakesoul".default.docs
ORDER BY array_distance(embedding, ARRAY[0.1, 0.2, /* ... */])
LIMIT 10;
-- 期望出现：LakeSoulVectorSearchExec(vec=embedding, top_k=10, metric=L2, order_by=false)
```

内积用降序：

```sql
SELECT id FROM "lakesoul".default.docs
ORDER BY inner_product(embedding, ARRAY[0.1, 0.2, /* ... */]) DESC
LIMIT 10;
```

多列加权融合（各列 metric 必须一致）：

```sql
SELECT id FROM "lakesoul".default.docs
ORDER BY 0.7 * array_distance(text_vec, ARRAY[...])
       + 0.3 * array_distance(image_vec, ARRAY[...])
LIMIT 10;
```

- 索引路径需要 `LIMIT`；没有 `LIMIT` 时回退完整距离计算；
- 如果看到普通 `TableScan` + `Sort`，说明没有下推（见[支持类型与限制](#7-支持类型与限制)的常见错误表）。

### 2.5 会话设置

```sql
SET lakesoul_vector_search.nprobe = 128;
SET lakesoul_vector_search.rerank = 'index';        -- 默认值
SET lakesoul_vector_search.exact_overfetch = 10;
SET lakesoul_vector_search.parallelism = 4;
SET lakesoul_vector_search.deferred_payload = false;
```

| Key | 默认 | 说明 |
|---|---|---|
| `lakesoul_vector_search.nprobe` | 64 | 每个 shard 探测的 cluster 数 |
| `lakesoul_vector_search.rerank` | `index` | `index`：按索引量化距离排序、不读向量列；`exact`：候选读原始向量精确重排 |
| `lakesoul_vector_search.exact_overfetch` | 10 | exact 模式每 bucket 候选倍数（`top_k * this`） |
| `lakesoul_vector_search.parallelism` | 1 | 并行探测的 cluster 组数（1 = 串行） |
| `lakesoul_vector_search.deferred_payload` | true | 两阶段读 payload：先排序保留候选，再读其余列 |

进程级覆盖：`LAKESOUL_VECTOR_EXACT_OVERFETCH` 可覆盖 exact 模式的候选倍数；`LAKESOUL_VECTOR_SEARCH_PROFILE` 打开每个 bucket 的耗时日志（排障用）。

pgwire 的 pgvector 设置见 [pgwire 兼容](#3-pgwire-兼容)。

### 2.6 原生批量 SQL 函数

DataFusion 与 LakeSoul pgwire 共享同一套实现，计划中显示 `VectorBatchSearchExec`，一次调用完成有界并行的原生批检索（复用快照、cluster 和 FastScan block，不是逐条 SQL 循环）：

```sql
-- 输入来自表
SELECT query_id, rank, id, distance
FROM lakesoul_vector_search_batch_from_table(
  'lakesoul.default.docs', 'embedding',
  'lakesoul.default.query_vectors', 'qid', 'qvec',
  10, 64, 'L2', 'index'
)
ORDER BY query_id, rank;

-- 输入来自 JSON 字面量
SELECT * FROM lakesoul_vector_search_batch(
  'lakesoul.default.docs', 'embedding',
  '[[0.1,0.2],[0.3,0.4]]', 10, 64, 'L2', 'index'
);
```

参数（位置参数，全部必须是字面量）：

| 位置 | `lakesoul_vector_search_batch` | `lakesoul_vector_search_batch_from_table` |
|---|---|---|
| 1 | `target`：表标识 | `target`：表标识 |
| 2 | `column`：向量列 | `column`：向量列 |
| 3 | `queries_json`：JSON 数组（`[[f32,...], ...]`） | `query_table`：查询表标识 |
| 4 | `k` | `qid_col`：查询 id 列（非空唯一 Int64） |
| 5 | （可选）`nprobe`，默认 64 | `qvec_col`：查询向量列（float32 列表） |
| 6 | （可选）`metric`：`auto` / `L2` / `IP`，默认 `auto` | `k` |
| 7 | （可选）`rerank`：`index` / `exact`，默认 `index` | （可选）`nprobe`，默认 64 |
| 8 | （可选）`exact_overfetch`，默认 10 | （可选）`metric` / `rerank` / `exact_overfetch`（依次） |

输出列：`query_id`、`rank`、`id`、`distance`。限制：

- 参数不支持 placeholder / 动态表达式，必须是字面量；
- `queries_json` 不超过 64 MiB 且最多 65,536 个 query；
- 表必须有单个 `Int64` 主键且列名为 `id`；当前索引 commit 必须覆盖快照的全部文件、无 mutation chain、无 CDC 列、向量列为定长 `Float32`；不满足时明确报错（不会静默回退）；
- 执行预算用 `SET lakesoul_vector_batch.*` 调整：

| Key | 默认 | 说明 |
|---|---|---|
| `lakesoul_vector_batch.query_window` | 256 | 每个原生窗口的 query 数（1–256） |
| `lakesoul_vector_batch.parallelism` | 0 | 每个窗口的 CPU worker；0 = 进程 CPU 上限 |
| `lakesoul_vector_batch.io_concurrency` | 8 | 并发 object/block 读（1–64） |
| `lakesoul_vector_batch.memory_bytes` | 134217728 | 单次批量瞬时内存上限（1 B–128 MiB） |

### 2.7 删除索引 SQL 函数

```sql
SELECT lakesoul_drop_vector_index(
    'docs', 'default', 'embedding',
    false,   -- keep_declaration
    true,    -- purge_files
    0.0,     -- grace seconds
    true,    -- wait_for_cleanup
    false    -- dry_run
);
```

- 返回一行 JSON 字符串（字段与 Python 返回一致）；
- 破坏性调用需要 `LAKESOUL_ALLOW_INDEX_DDL=1`；未设置时只允许 `dry_run=true`，SQL 入口仍受 PostgreSQL 连接自身权限限制；
- 这是标量 DDL 函数，不是 `ALTER TABLE ... DROP VECTOR INDEX`。

## 3. pgwire 兼容

`postgres-lakesoul` 提供 pgvector 风格的 SQL，可以通过它建表、写入和建索引。
DDL 部分（`vector(n)`、`vector_index_columns`、`CREATE INDEX`、`ALTER TABLE`
声明、`REINDEX TABLE` 和 deferred 维护）统一见
[向量与文本索引 DDL](../postgres/index-ddl.md)。查询侧支持 `<->` / `<#>` / `<=>`
距离操作符、`'[...]'` 字符串字面量、`$1` 预处理参数、
`SET ivfflat.probes` / `SHOW ivfflat.probes`，以及上面提到的
`lakesoul_vector_search*` 函数与会话设置。

```python
import psycopg
from pgvector.psycopg import register_vector

conn = psycopg.connect("host=127.0.0.1 port=6543 user=lakesoul_test dbname=default")
register_vector(conn)

conn.execute("SET ivfflat.probes = 64")
rows = conn.execute(
    "SELECT id FROM docs ORDER BY embedding <-> '[0.1, 0.2, 0.3, 0.4]' LIMIT 10"
).fetchall()

# 内积：pgwire 的 <#> 返回负内积，升序即“内积最大”
conn.execute("SELECT id FROM docs ORDER BY embedding <#> $1 LIMIT 10", (vec,))
conn.execute("SELECT vec <=> %s FROM docs WHERE id = 7", (unit_vec,))
```

- `ivfflat.probes` 映射到每个 shard 探测的 cluster 数（与 `lakesoul_vector_search.nprobe` 指向同一选项），**连接本地**，默认 64，`0` 会被拒绝；
- `SET lakesoul_vector_search.rerank = 'exact'` 等设置同样生效；
- pgwire 支持建表、写入和索引 DDL；具体语法和限制见
  [向量与文本索引 DDL](../postgres/index-ddl.md) 以及 PostgreSQL 兼容矩阵；
- 表解析用当前数据库 + `public` schema（如 `public.docs`）；索引下推要求该表已声明向量索引。

## 4. 混合检索与过滤

结构化过滤和向量相似度在同一个计划里执行，ANN 索引提供候选，过滤在扫描/候选阶段求值：

```sql
SELECT id, title, embedding <-> $1 AS distance
FROM documents
WHERE tenant_id = 'acme' AND category = 'contract'
ORDER BY embedding <-> $1
LIMIT 10;
```

```python
result = table.search(query, top_k=10, filter="lang = 'zh'")
```

候选预算（每 shard）：

| 入口 / 模式 | 无过滤 | 有过滤 |
|---|---|---|
| SQL exact | `max(10 × k, 100)` | `max(10 × k, 100)` |
| SQL index-only | `2 × k` | `10 × k` |
| Python exact | `k` | `k` |
| Python index-only | `max(2 × k, k + 8)` | 同左 |

过滤条件选择性很高时，候选池里可能没有足够的匹配行，返回结果会少于 `k`。可选做法：

- 提高 `nprobe`（探测更多 cluster）；
- 让过滤条件裁剪分区或 hash bucket（分区列、`<`/`>` 等可下推谓词）；
- 对高选择性谓词回退到精确扫描（结果正确，延迟随行数增长）。

引擎不会静默改变查询形状；用 `EXPLAIN` 确认是否走了 ANN 路径。

## 5. 多列

声明多个向量列，每个列构建独立的索引：

```python
vector_index=[
    {"column": "text_vec",  "dim": 768, "nlist": 256, "metric": "L2"},
    {"column": "image_vec", "dim": 512, "nlist": 128, "metric": "L2"},
]
```

```sql
OPTIONS (
  'vector_index_columns'
    '[{"column":"text_vec","dim":768,"nlist":256,"metric":"L2"},
      {"column":"image_vec","dim":512,"nlist":128,"metric":"L2"}]'
)
```

查询时每个列独立做 ANN，候选取**交集**，再按加权距离排序：

- exact 模式：由上层 `Sort` 对候选行计算加权精确距离；
- index 模式：由 reader 计算加权索引距离，**仅支持 L2**；
- 各列的 metric 必须一致；多列同样受 SQL `ORDER BY ... LIMIT k` 形状约束。

Python 多列检索通过 Daft 的 `multi_vector_search` 或低层 `vector_search_requests`：

```json
[
  {"column":"text_vec","query":"0.1,0.2","top_k":100,"weight":0.7},
  {"column":"image_vec","query":"0.3,0.4","top_k":100,"weight":0.3}
]
```

## 6. 分区与 hash bucket

索引 shard 的身份是 `(partition_desc, hash_bucket_id)`：

- 每个 range 分区、每个 hash bucket 各自有独立的 IVF 质心和 RaBitQ 编码；
- 不同分区的文件绝不会合并到同一 shard；
- 分区表每个分区都有独立索引，可以按分区构建/重建/删除；
- `hash_bucket_num`（SQL：`hashBucketNum`）决定分桶数：大表常用 4–16、小表 1–2，bucket 之间可并行构建与探测；
- 查询按分区裁剪 + bucket 定位，只加载/探测相关 shard，最后跨 bucket 合并全局 top-k。

索引目录：

```text
{table_path}/_vector_index/{column}/{partition}/{bucket}/
```

## 7. 支持类型与限制

### 7.1 类型与参数

| 项目 | 支持范围 |
|---|---|
| 主键 | 必须有主键；索引 key 是主键列的 Arrow-Row 编码（单列或复合）；原生 batch 要求单个 `Int64` 且列名 `id` |
| 向量列 | Python SDK 要求 `FixedSizeList<Float32>`；Rust/DataFusion 读取路径还接受 `List` / `LargeList` 浮点数组以及 `Float16` / `Float64` 的部分路径；exact 的 block 解码只支持固定长度向量列，变长/复杂类型回退 reader |
| `dim` | 必须与 `FixedSizeList` 长度一致 |
| `metric` | `L2`、`IP`（`INNERPRODUCT`）；cosine 不走索引 |
| `total_bits` | `1` / `3` / `7` |
| 索引模式 | exact（候选精确重排）与 index-only（量化距离，仅 L2） |

### 7.2 限制

- **不做通用向量数据库**：没有跨表全局 ANN、没有 GPU 检索、不保证 100% 召回；
- **SQL 形状受限**：只识别 `ORDER BY <距离> LIMIT k` 等模式；`cosine_distance`、无 `LIMIT`、索引不可用时会回退精确扫描；
- **索引异步构建**：写入提交成功即返回；构建完成前查询精确扫描未覆盖文件，结果正确但更慢；
- **固定快照/时间旅行**不使用索引（索引描述最新文件），走精确扫描；
- **删除索引**不影响数据正确性，只是变慢；`drop_table` 只级联清索引控制面，不删对象文件；
- **多节点可见性**受 `LAKESOUL_METADATA_TTL_MS` 和本地 snapshot cache 影响（见[配置](configuration.md)）；
- 变长/复杂类型的向量列在 exact 路径会回退 reader。

### 7.3 常见错误

| 错误 / 现象 | 原因 | 处理 |
|---|---|---|
| `a vector index requires an id column` | 没有主键 | 建表时指定 `primary_keys` / `PRIMARY KEY` |
| `vector index column '...' not found` | 列名拼错 | 检查 schema |
| `dim ... does not match schema FixedSizeList size` | `dim` 与列长度不一致 | 修正 `dim` |
| `total_bits must be 1, 3 or 7` | 传了其他位宽 | 改为 1/3/7 |
| `vector column ... must hold Float32` | Python SDK 类型限制 | 用 `Float32` 或改 Rust 路径 |
| `query vector dimension mismatch` | 查询向量长度不对 | 检查 query 长度 |
| `vector index at '...' was not resolved by the caller` | 索引未建/未解析 | 先构建索引，或检查 shard 前缀 |
| `no vector_index_columns property` | 表未声明索引，或 drop 时移除了声明 | 重新声明 + rebuild |
| `EXPLAIN` 没有 `LakeSoulVectorSearchExec` | 未下推 | 检查 SQL 形状、索引声明、metric、主键 |
| 结果少于 `top_k` | 过滤后候选不足，或 ANN 召回不足 | 增大 `nprobe` / overfetch / 放宽过滤 |

## 8. 读租约与 tombstone 生命周期

索引控制面在 PostgreSQL 里是几张独立的表：`index_shard`（unique `(kind, index_prefix)`）、`index_commit`（含 mutation 链）、`index_lease`、`index_read_lease`（读租约）、`index_drop`（drop tombstone）。

**读租约（read lease）**

- 交互式单查的 reader lease 默认 TTL 300 秒，可用 `LAKESOUL_VECTOR_INDEX_LEASE_TTL_SECS` 覆盖；
- 原生 batch / 快照固定读取使用独立的 `index_read_lease`：TTL 120 秒、每 30 秒续租；续租失败会取消 batch，不允许失去保护后继续输出；
- 已运行的原生 batch 请求用读租约固定索引 commit 和原始数据文件，**逻辑 drop 不会级联删除这些租约**。

**drop 与 tombstone**

- `drop_vector_index` 只删除派生索引：不改 Parquet/Vortex 数据文件、分区、schema、数据 commit、CDC 墓碑和快照；
- 默认从 `vector_index_columns` 移除声明；`keep_declaration=True` 时保留声明但把 `vector_index_available` 置为 `false`，planner 自动回退普通扫描，`rebuild` / `declare` 会重新置为 `true`；
- 每次 drop / re-declare 递增表属性 `vector_index_epoch`；本进程的 metadata cache、snapshot cache（key 含 epoch）、L1 index cache、lazy shard cache 都会失效；其他节点在下次刷新 table_info 后看到新 epoch；
- `index_drop` tombstone 阻止旧 writer 把索引重建回来：`ensure_shard` 看到 tombstone 返回 `IndexDropped`，`commit_vector_build` 直接 no-op；数据写入不会因为索引 drop 失败；
- `purge_files=True` 时物理清理等待 `grace_seconds` 与活跃读租约释放；`wait_for_cleanup=False` 时控制面立即删除、返回 `cleanup="deferred"` 和 `drop_id`，后台任务继续清理对象；进程退出后状态保留，可再次调用 GC；
- 显式重建与物理清理通过同一 prefix 锁协调，清理发现原 drop 已被重建取代时拒绝删除新对象；
- 状态机：`tombstoned → control_plane_deleted → objects_pending → tombstone_retained / completed`（失败为 `objects_failed`，可由 GC 重试）；
- `gc_dropped_vector_index_objects()` 只删除对象、保留 tombstone，可重复执行，幂等；
- `drop_table` 级联清理索引控制面（`index_shard / index_commit / index_lease / index_drop`），但不删除对象文件。

**索引文件 GC**

写入提交后，被取代且超过宽限期（`gc_grace_seconds`，默认 1 小时）且没有活跃租约的 generation 会被清理；写入路径按 `LAKESOUL_INDEX_GC_EVERY`（默认 16 次写）摊销执行 GC；也可以显式调用 Rust `gc_vector_index` API 按需清理。

## 9. Rust API（简要）

```rust
use lakesoul_datafusion::vector_index::{
    DropVectorIndexOptions, gc_vector_index, drop_vector_index,
};

let report = drop_vector_index(
    &client, "docs", "default", &object_store_options,
    DropVectorIndexOptions {
        columns: Some(vec!["embedding".into()]),
        keep_declaration: false,
        purge_files: true,
        grace: std::time::Duration::from_secs(300),
        wait_for_cleanup: true,
        ..Default::default()
    },
).await?;
```

表级封装：`lakesoul_datafusion::lakesoul_table::LakeSoulTable` 提供 `rebuild_vector_index()` / `drop_vector_index(options)` / `gc_vector_index(options)` 等。

底层 crate 分工：

- `lakesoul-vector`：IVF / RaBitQ、segment、mutation、commit；
- `lakesoul-io`：shard builder、search、cache、pk locator；
- `lakesoul-metadata`：index catalog、租约、GC。
