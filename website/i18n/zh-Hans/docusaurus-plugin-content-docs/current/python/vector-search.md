---
sidebar_position: 7
---

# 向量检索

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul Python SDK 在表上声明和维护 **IVF + RaBitQ** 向量索引：索引与表数据存放在同一对象存储上（`_vector_index/`），随写入增量维护，检索走 Lakesoul 的 DataFusion 查询引擎，并与分区裁剪、merge-on-read、CDC 删除墓碑共享同一份快照。

完整文档：

- [向量检索快速开始](../engine/vector-search/quickstart.md)：建表 → 写入 → Python / SQL / pgwire 查询；
- [API 参考](../engine/vector-search/api.md)：`table.search` / `search_batch`、构建、删除、GC 与读租约语义；
- [配置](../engine/vector-search/configuration.md)：索引参数与 L1/L2 缓存；
- [性能基准](../engine/vector-search/benchmarks.md)：index-only / exact、单查 / batch 实测。

## 示例：建表、写入、检索

```python
import pyarrow as pa
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(namespace="default")

# 1. 建表时声明向量列（Float32 定长列表，dim 必须与列长度一致）
schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("title", pa.string()),
    pa.field("embedding", pa.list_(pa.field("item", pa.float32()), 768), nullable=False),
])
table = catalog.create_table(
    "documents",
    path="s3://my-bucket/warehouse/documents",
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

# 2. 写入：数据提交后自动增量维护索引
table.write_arrow(batch)

# 3. 检索：默认 index-only（不读向量列，更快）；rerank="exact" 做精确重排
session = catalog.session(vector_search={"nprobe": 64})
approx = table.search(query, top_k=10, session=session)
exact = table.search(query, top_k=10, rerank="exact", session=session)

# 4. 批量检索
batch = table.search_batch([query1, query2], top_k=10, session=session)
print(batch.ids)
```

## 常用维护接口

```python
table.build_vector_index()                     # 历史数据补建 / 增量构建
table.rebuild_vector_index()                   # 全量重建（重训质心、新 generation）
table.drop_vector_index(column="embedding")    # 删除索引，不删除数据
table.recreate_vector_index(column="embedding", dim=768)  # 重新声明 + 重建
table.gc_dropped_vector_index_objects()        # 清理已 drop 索引的对象（幂等）
```

索引构建参数（`nlist` / `total_bits` / `metric` / `rebuild_mode` / `max_delta_ratio` 等）的默认值、缓存与调优见[配置](../engine/vector-search/configuration.md)；建表、写入、scan 等通用 API 见[核心 API](./core-api.md)。
