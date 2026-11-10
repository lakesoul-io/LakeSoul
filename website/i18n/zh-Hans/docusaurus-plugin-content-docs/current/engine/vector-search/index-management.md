---
sidebar_position: 3
sidebar_label: 索引管理
---

# 向量索引管理

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

向量索引通过 Python SDK 声明和构建。索引存放在表路径下、与数据同一个对象存储，并随写入维护。

## 声明索引

```python
catalog.create_table(
    name="documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,
    primary_keys=["id"],
    vector_index=[{
        "column": "embedding",
        "dim": 768,
        "metric": "L2",
        "nlist": 256,
    }],
)
```

配置存在表属性上；`write_arrow` 在数据提交后构建或更新索引。

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `column` | 必填 | 存向量的 `Float32` 列 |
| `dim` | 必填 | 向量维度 |
| `metric` | `L2` | `L2` 或 `IP`（内积） |
| `nlist` | `256` | IVF 聚类数 |
| `total_bits` | `7` | RaBitQ 位数 |
| `rotator_type` | `FhtKac` | `FhtKac` 或 `Matrix` |
| `seed` | `42` | 旋转种子 |
| `use_faster_config` | `True` | 更快的量化配置 |

表需要 `Int64` 或 `UInt64` 主键。

## 构建或重建

对已有表直接构建索引：

```python
from lakesoul.vector_index import build_table_vector_index

build_table_vector_index(
    table_name="documents",
    namespace="default",
    vector_column="embedding",
    dim=768,
    metric="L2",
    nlist=256,
)
```

按分区：

```python
from lakesoul.vector_index import build_partition_vector_index

build_partition_vector_index(
    table_name="documents",
    namespace="default",
    partition_desc="range=2026-01-01",
    vector_column="embedding",
    dim=768,
    metric="L2",
)
```

`rebuild=True` 会基于全部活跃数据文件重新训练 IVF 质心。数据分布漂移后应使用它；否则新行会作为 delta segment 追加，沿用已有质心。

## Shard

索引按（分区，哈希 bucket）shard 构建。每个 shard 有自己的质心和 RaBitQ 编码；查询探测可能包含匹配行的 shard，再合并候选。

`store_config` 接受对象存储凭证；省略时 SDK 读取 `LAKESOUL_OBJECT_STORE_*` 环境变量。

## 维护

- `INSERT` 或 Python 写入后，索引会更新。索引维护在元数据提交之后运行；索引构建失败会返回错误，但不回滚数据。
- shard 会累积不可变的 delta segment。delta 让质心失效时，shard 会从数据文件重建。
- `rebuild=True` 强制重建所有 shard。

## 下一步

- [向量 SQL 参考](sql-reference.md)
- [混合检索](hybrid-search.md)
- [Benchmark](benchmarks.md)
