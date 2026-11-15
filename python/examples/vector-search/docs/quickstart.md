# Quickstart

10 分钟跑通：建表 → 写数据 → 自动建索引 → Python / SQL 查询。

## 1. 前置条件

- 已安装 LakeSoul Python 包（`pip install lakesoul` 或本地 `uv run maturin develop`）；
- 有可连接的 PostgreSQL 元数据库；
- 本地路径即可，不需要对象存储。

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME='lakesoul_test'
export LAKESOUL_PG_PASSWORD='lakesoul_test'
```

## 2. 建表并声明向量索引

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

`vector_index` 会写入表属性 `vector_index_columns`，后续写入自动维护索引。

## 3. 写入数据

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

table.write_arrow(rows)  # 提交数据 + 自动增量构建索引
```

## 4. Python 查询

```python
session = catalog.session(vector_search={"nprobe": 2, "exact_overfetch": 2})
q = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

# 精确重排：更适合要求高准确率的场景
exact = table.search(q, top_k=3, rerank="exact", session=session)
print(exact.to_pydict())

# index-only：不读向量列，更快
approx = table.search(q, top_k=3, rerank="index", session=session)
print(approx.to_pydict())
```

## 5. SQL 查询

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

`EXPLAIN` 中出现 `LakeSoulVectorSearchExec` 表示下推成功。

## 6. 最小可用清单

1. 表必须有**主键**；
2. 向量列必须是 `float32` 定长列表，且 `dim` 与配置一致；
3. `total_bits` 当前常用 `1` / `3` / `7`；
4. 写入后等待索引构建，或手动 `build_vector_index()`；
5. SQL 排序方向必须匹配 metric：L2 升序，IP 降序。

## 7. 完整示例与教程

- [tutorial/README.md](../tutorial/README.md)：按使用方式拆分的完整教程；
- [minio_end_to_end.ipynb](../tutorial/minio_end_to_end.ipynb)：MinIO 端到端；
- [existing_table_vector_index.ipynb](../tutorial/existing_table_vector_index.ipynb)：已有表补建索引；
- [drop_vector_index.ipynb](../tutorial/drop_vector_index.ipynb)：删除索引。

## 8. 清理

```python
catalog.drop_table("docs", if_exists=True)
```

## 使用 S3 / MinIO

```python
OPTIONS = {
    "fs.s3a.bucket": "your-bucket",
    "fs.s3a.endpoint": "http://127.0.0.1:9000",
    "fs.s3a.access.key": "minioadmin",
    "fs.s3a.secret.key": "minioadmin",
    "fs.s3a.path.style.access": "true",
}
catalog = LakeSoulCatalog.from_env(namespace="default", object_store_options=OPTIONS)
# create_table(path="s3://your-bucket/docs", ...)
```

下一篇：[Roadmap & Changelog](roadmap-and-changelog.md)。
