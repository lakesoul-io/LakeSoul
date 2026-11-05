---
sidebar_position: 1
sidebar_label: 概览
---

# 向量检索

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul 在表数据所在的对象存储上维护 ANN 索引，用兼容 pgvector 的 SQL 查询。这里的兼容指的是 PostgreSQL wire protocol 和 SQL 语法：`vector` 类型、距离操作符（`<->`、`<#>`、`<=>`）以及 `SET ivfflat.probes`。索引随写入维护，不需要另起一套向量数据库。

## 能获得什么

- **SQL 表面。** `vector` 输入、`<->`、`<#>`、`<=>`、`ORDER BY ... LIMIT k` 和 `SET ivfflat.probes`。
- **湖上索引。** IVF + RaBitQ 索引与表数据同置，共享同一套提交和快照语义。
- **增量维护。** 新行写入会更新索引；只有 delta 让质心失效时，才重建某个 shard。
- **混合检索。** 结构化过滤与 ANN 在同一个计划里，过滤查询会做候选 overfetch。
- **同一个引擎。** 向量检索可以和普通 SQL 的 join、聚合、过滤组合；全文检索也在同一个查询里。

## 向量表怎么建

向量列和索引通过 Python SDK 声明，不走 PostgreSQL DDL：

```python
catalog.create_table(
    name="documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,          # embedding 是 Float32，维度 768
    primary_keys=["id"],
    vector_index=[{"column": "embedding", "dim": 768, "metric": "L2"}],
)
```

之后 PostgreSQL 端点和 Python SDK 查询同一张表。

## 距离度量

| 操作符 | 距离 | 索引 |
| --- | --- | --- |
| `<->` | L2（欧氏距离） | 是 |
| `<#>` | 负内积 | 是 |
| `<=>` | Cosine | 仅精确计算 |

ANN 索引支持 L2 和内积。Cosine 查询可用且结果正确，但不走索引；需要带索引的 cosine 相似度时，写入时归一化向量并用 `<#>`。

## 下一步

- [SQL 参考](sql-reference.md)
- [索引管理](index-management.md)
- [混合检索](hybrid-search.md)
- [Benchmark](benchmarks.md)
