---
sidebar_position: 1
sidebar_label: 快速开始
---

# 向量检索快速开始

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul 的向量检索不是独立的向量数据库，而是**表上的二级索引**：IVF + RaBitQ 索引与表数据放在同一个对象存储上（表目录下的 `_vector_index/`），随写入增量维护；查询走常规 LakeSoul 查询引擎，用兼容 pgvector 的 SQL 或 Python SDK。索引只负责召回候选主键，最终行内容仍由表数据的 merge-on-read、CDC 删除墓碑和快照语义决定。

本文用 10 分钟跑通：建表 → 写入 → 查询（Python / SQL / pgwire）→ 维护。完整 API 见 [API 参考](api.md)，索引参数与缓存配置见[配置](configuration.md)，性能数字见[性能基准](benchmarks.md)。

## 前置条件

- **PostgreSQL 元数据库**：LakeSoul 的 catalog 存在 PG 中，需要可连接的测试库；
- **对象存储**：本地路径即可；S3 / MinIO 通过对象存储选项传入（见文末）；
- **Python SDK**：`pip install lakesoul`，或本地 `uv run maturin develop`；另外需要 `pyarrow`；
- **主键**：表必须有主键，索引 key 是主键列的 Arrow-Row 编码。

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME='lakesoul_test'
export LAKESOUL_PG_PASSWORD='lakesoul_test'
```

本机可以用 `./script/meta_init_for_local_test.sh -j 1` 初始化这个测试库（默认用户、密码、库名均为 `lakesoul_test`）。

## 1. 建表并声明向量列

向量列必须是 `Float32` 的定长列表，`dim` 必须与列长度一致。`vector_index=` 里的配置会序列化写入表属性 `vector_index_columns`，之后每次写入都会自动维护索引。

```python
import numpy as np
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(namespace="default")

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("title", pa.string(), nullable=False),
    pa.field("embedding", pa.list_(pa.field("item", pa.float32()), 8), nullable=False),
])

table = catalog.create_table(
    "docs",
    path="file:///tmp/lakesoul_docs_quickstart",
    schema=schema,
    primary_keys=["id"],
    hash_bucket_num=2,
    vector_index=[{
        "column": "embedding",
        "dim": 8,
        "nlist": 2,
        "total_bits": 7,
        "metric": "L2",
    }],
)
```

`hash_bucket_num` 是索引分片数（每个 hash bucket 一个 shard，可并行构建和探测）：小示例用 2 即可，大表常用 4–16。索引按 `(分区, hash bucket)` 分片，详见 [API 参考](api.md#6-分区与-hash-bucket)。

## 2. 写入数据

```python
rows = pa.table({
    "id": pa.array([1, 2, 3, 4], type=pa.int64()),
    "title": pa.array(["cat", "kitten", "airplane", "car"]),
    "embedding": pa.FixedSizeListArray.from_arrays(
        pa.array(np.array([
            [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0],
        ], dtype=np.float32).reshape(-1), type=pa.float32()),
        8,
    ),
})

table.write_arrow(rows)  # 提交数据，并增量维护索引
```

索引构建是异步的：数据提交成功即返回，索引只追加不可变的 delta segment，不重写历史。构建完成前，查询会对未被索引覆盖的文件做精确扫描，结果仍然正确，只是更慢。大批量导入时可以关掉自动构建，最后统一补建：

```python
table.write_arrow(rows, auto_build_vector_index=False)
table.build_vector_index()  # 一次性补建历史数据
```

## 3. Python 查询

`catalog.session(...)` 复用同一个 DataFusion 会话，元数据、索引和 block 缓存会跨查询保持温热。两种排序：

- `rerank="index"`（默认）：按索引量化距离排序，**不读向量列**，更快但近似；
- `rerank="exact"`：索引召回候选后读原始向量做精确重排，召回更稳，成本更高。

```python
session = catalog.session(vector_search={"nprobe": 2, "exact_overfetch": 2})
q = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

approx = table.search(q, top_k=3, session=session)               # index-only
exact = table.search(q, top_k=3, rerank="exact", session=session) # 精确重排
print(exact.to_pydict())

# 批量：一次调用多个 query，复用同一份快照/索引
batch = table.search_batch([q, [0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]],
                           top_k=3, session=session)
print(batch.ids)          # 每个 query 一个主键列表
print(batch.to_arrow())   # (query_id, rank, id[, distance])
```

结果顺序：L2 距离升序、内积降序；`top_k` 是跨 bucket 合并后的全局 top-k。`with_distance=True` 会在结果里多返回 `__lakesoul_vector_distance` 列（exact 为精确距离，index 为量化距离）。查询失败或结果不足的常见原因见 [API 参考](api.md#7-支持类型与限制)。

## 4. 相同查询的 SQL / pgwire 形式

Python 会话本身就是 DataFusion SQL：`table.search` 等价于 `ORDER BY array_distance(...) LIMIT k`。可以直接写 SQL，也可以在同一会话里用 `EXPLAIN` 确认下推：

```python
session.sql("SET lakesoul_vector_search.nprobe = 2")
session.sql("SET lakesoul_vector_search.rerank = 'exact'")

sql = """
SELECT id, title,
       array_distance(embedding, ARRAY[1,0,0,0,0,0,0,0]) AS distance
FROM "lakesoul"."default"."docs"
ORDER BY array_distance(embedding, ARRAY[1,0,0,0,0,0,0,0])
LIMIT 3
"""
print(session.sql(sql).to_pydict())
print(session.explain(sql))
```

`EXPLAIN` 输出里出现 `LakeSoulVectorSearchExec` 表示 top-k 已下推到向量索引；如果只看到普通 `TableScan` + `Sort`，说明没有下推（表未声明索引、SQL 形状不匹配、索引不可用或 metric 不一致）。

如果启用了 PostgreSQL 兼容服务（`postgres-lakesoul`，默认只读），同一张表可以用 pgvector 语法查询：

```python
import psycopg
from pgvector.psycopg import register_vector

conn = psycopg.connect("host=127.0.0.1 port=6543 user=lakesoul_test dbname=default")
register_vector(conn)
conn.execute("SET ivfflat.probes = 2")
rows = conn.execute(
    "SELECT id FROM docs ORDER BY embedding <-> '[1,0,0,0,0,0,0,0]' LIMIT 3"
).fetchall()
```

pgwire 支持 `vector` 类型、`<->` / `<#>` / `<=>` 三个距离操作符、`$1` 参数和 `SET ivfflat.probes`；建表、写入仍由 Python / SQL / Spark / Flink 完成。详见 [API 参考](api.md#3-pgwire-兼容)。

## 5. 最小可用清单

1. 表必须有**主键**（`primary_keys` / `PRIMARY KEY`），类型 `Int64` 或 `UInt64`；
2. 向量列必须是 `Float32` 定长列表，且 `dim` 与列长度一致；
3. `total_bits` 当前支持 `1` / `3` / `7`，`metric` 支持 `L2` 与 `IP`；
4. 写入后等待索引构建，或手动调用 `table.build_vector_index()` 补建；
5. SQL 的排序方向必须匹配 metric：L2 升序、IP 降序；
6. 需要高召回时用 `rerank="exact"`，需要高吞吐时保持默认的 `index-only`。

## 6. 维护：删除 / 重建（一行示例）

```python
table.rebuild_vector_index()                    # 全量重建：重训质心、发布新 generation
table.drop_vector_index(column="embedding")     # 删除索引，不删除数据行
table.recreate_vector_index(column="embedding", dim=8, nlist=2)  # 重新声明 + 全量重建
```

索引是派生数据：删除索引不影响数据文件、分区、schema、commit 和快照，查询会自动回退普通扫描，只是变慢。`drop_vector_index` 的参数、tombstone、读租约和异步清理语义见 [API 参考](api.md#14-删除索引与-gc)。

## 7. 使用 S3 / MinIO

本地路径不需要额外配置；S3 / MinIO 需要传入对象存储选项（endpoint、bucket、凭据），建表时 `path` 用 `s3://` 前缀：

```python
OPTIONS = {
    "fs.s3a.bucket": "your-bucket",
    "fs.s3a.endpoint": "http://127.0.0.1:9000",
    "fs.s3a.access.key": "minioadmin",
    "fs.s3a.secret.key": "minioadmin",
    "fs.s3a.path.style.access": "true",
}
catalog = LakeSoulCatalog.from_env(namespace="default", object_store_options=OPTIONS)
# catalog.create_table("docs", path="s3://your-bucket/docs", ...)
```

索引与数据文件写入同一个桶、同一套凭据；reader 和 builder 必须能访问同一位置。

## 下一步

- [架构与缓存](architecture.md) — 索引、查询路径与三级缓存如何配合。

- [API 参考](api.md)：Python / SQL / pgwire / Rust 的完整接口，索引构建、删除、GC 与读租约语义；
- [配置](configuration.md)：索引构建参数与 L1/L2 缓存、block 调优、元数据 TTL；
- [性能基准](benchmarks.md)：index-only 与 exact、单查与 batch 的实测数字；
- Python SDK 页面：[Python 向量检索](../../python/vector-search.md)；
- PostgreSQL 兼容服务：[PostgreSQL 前端](../postgres/overview.md)。
