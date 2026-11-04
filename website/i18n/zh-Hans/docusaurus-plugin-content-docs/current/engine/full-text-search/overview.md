---
sidebar_position: 1
sidebar_label: 概览
---

# 全文检索

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul 在表数据旁边维护 Tantivy 全文索引，用 BM25 排序。索引随写入维护，top-k 查询由索引回答，而不是全表扫描。

## 能获得什么

- **`text_match(column, query)`** 作为过滤谓词。
- **`text_score(column, query)`** 放在 `ORDER BY` 里做 BM25 排序。
- **Top-k 下推**：`WHERE text_match(...) ORDER BY text_score(...) DESC LIMIT k` 从索引返回全局 top-k。
- **同一个引擎**：全文结果可以和向量检索、普通 SQL 的 join 和聚合组合。

## 示例

```sql
SELECT id, title, body
FROM documents
WHERE text_match(body, 'lakehouse')
ORDER BY text_score(body, 'lakehouse') DESC
LIMIT 10;
```

## 创建索引

文本索引通过 Python SDK 在表上声明：

```python
catalog.create_table(
    name="documents",
    path="s3://my-bucket/warehouse/documents",
    schema=documents_schema,
    primary_keys=["id"],
    text_index=[{"column": "body", "tokenizer": "jieba"}],
)
```

之后 PostgreSQL 端点用 `text_match` 和 `text_score` 查询这张表。

## 下一步

- [文本 SQL 参考](sql-reference.md)
- [索引管理](index-management.md)
- [Benchmark](benchmarks.md)
