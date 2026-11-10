---
sidebar_position: 4
sidebar_label: 混合检索
---

# 混合检索

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

结构化过滤和向量相似度在同一个计划里执行：

```sql
SELECT id, title, embedding <-> $1 AS distance
FROM documents
WHERE tenant_id = 'acme'
  AND category = 'contract'
ORDER BY embedding <-> $1
LIMIT 10;
```

过滤在扫描上求值；ANN 索引提供候选。存在过滤条件时，引擎会 overfetch 候选，再应用过滤并保留前 `k` 行。

## Overfetch 与重排

- 没有过滤时，扫描大约取 `2 × k` 个候选。
- 有过滤时，大约取 `10 × k` 个候选，因为部分候选会被过滤掉。
- 默认会在返回前重算候选的精确距离。`SET lakesoul_vector_search.rerank = 'index'` 改用索引分数。

## 选择性

过滤条件选择性很高时，如果候选池里没有足够的匹配行，返回结果可能少于 `k` 行。可选做法：

- 提高 `ivfflat.probes`，扫描更多 list。
- 让过滤条件能够裁剪分区或 bucket。
- 对高选择性谓词回退到精确扫描。

引擎不会静默改变查询形状；用 `EXPLAIN` 确认是否走了 ANN 路径。

## 选择 metric

- embedding 模型按欧氏距离训练时用 `<->`（L2）。
- 最大内积搜索用 `<#>`（负内积）。
- 要 cosine 相似度，写入时归一化向量并用 `<#>`，这样查询能使用索引。`<=>` 走精确计算。

## 下一步

- [向量 SQL 参考](sql-reference.md)
- [索引管理](index-management.md)
- [全文检索](../full-text-search/overview.md)
