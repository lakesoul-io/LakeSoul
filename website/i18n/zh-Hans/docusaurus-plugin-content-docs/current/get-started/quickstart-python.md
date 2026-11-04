---
sidebar_position: 3
---

# 快速开始：Python

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

Python SDK 通过原生 Rust IO 层读写 LakeSoul 表，不经过 PostgreSQL 端点。因此一次扫描可以分片到 Daft、Ray 或 PyTorch worker，而不需要 SQL 网关。

## 安装

```bash
pip install lakesoul
```

只装需要的集成：

```bash
pip install 'lakesoul[pandas,duckdb,torch,datasets,ray,daft]'
```

需要 Python 3.10 或更高版本。

## 指向元数据

SDK 和引擎使用同一组元数据环境变量：

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
```

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env(namespace="default")
print(catalog.list_tables())
```

## 读取

`LakeSoulScan` 是惰性的：调用输出方法之前不会读数据。

```python
import pyarrow.compute as pc

scan = (
    catalog.scan("orders")
    .select("id", "region", "amount")
    .filter(pc.field("amount") >= 100)
)

table = scan.to_arrow_table()      # 物化的 pyarrow.Table
dataset = scan.to_arrow_dataset()  # 惰性的 pyarrow.dataset.Dataset
```

同一个 scan 可以交给数据或 ML 框架：

```python
scan.to_daft()    # Daft DataFrame
scan.to_ray()     # Ray Data Dataset
scan.to_torch()   # PyTorch IterableDataset
scan.to_huggingface()
```

需要自己驱动分布时，用 `scan.shard(rank, world_size)` 给每个 worker 分配一个分片。

## 写入

```python
import pyarrow as pa

table = catalog.table("orders")
result = table.write_arrow(
    pa.table(
        {
            "id": [2, 3],
            "region": ["EMEA", "AMER"],
            "amount": [88.50, 120.00],
        },
        schema=table.schema,
    )
)
print(result.row_count, result.files)
```

主键表使用稀疏 upsert：提供主键、分区列和要修改的列即可。

`table.write_ray(dataset)` 和 `table.write_daft(dataframe)` 会在所有分布式写入任务成功后提交。

## 建表

```python
import pyarrow as pa

catalog.create_table(
    name="events",
    path="s3://my-bucket/warehouse/events",
    schema=pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("event_date", pa.string()),
        pa.field("value", pa.float64()),
    ]),
    primary_keys=["id"],
    partition_by=["event_date"],
    hash_bucket_num=16,
)
```

向量和全文索引在同一次调用里声明：

```python
catalog.create_table(
    name="documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,
    primary_keys=["id"],
    vector_index=[{"column": "embedding", "dim": 768, "metric": "L2"}],
    text_index=[{"column": "body"}],
)
```

`embedding` 必须是 `Float32` 列，维度与声明一致。索引 metric 为 `L2` 或 `IP`；cosine 走精确计算。

## 下一步

- [Python SDK 概览](../python/overview.md)
- [核心 API](../python/core-api.md)
- [PyTorch 与 Hugging Face](../python/pytorch-huggingface.md)
- [用 Python 做向量检索](../python/vector-search.md)
