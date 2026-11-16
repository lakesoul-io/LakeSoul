---
sidebar_position: 2
---

# 向量 SQL 参考

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 向量输入

查询向量可以写成字符串字面量或数组：

```sql
embedding <-> '[0.12, 0.98, 0.31]'
embedding <-> ARRAY[0.12, 0.98, 0.31]
```

预处理语句里可以用参数：

```sql
SELECT id FROM documents ORDER BY embedding <-> $1 LIMIT 10;
```

列是 `Float32`，维度固定。查询向量维度必须一致，否则报错。

## 操作符

| 操作符 | 返回 | 索引 |
| --- | --- | --- |
| `<->` | 欧氏距离（开平方） | 是 |
| `<#>` | 负内积 | 是 |
| `<=>` | Cosine 距离 | 仅精确计算 |

`<#>` 返回负内积，这样升序排列与 pgvector 的排序一致。Cosine 走精确计算，见[距离度量](overview.md#距离度量)。

## Top-k 查询

```sql
SELECT id, body, embedding <-> '[0.12, 0.98, 0.31]' AS distance
FROM documents
ORDER BY embedding <-> '[0.12, 0.98, 0.31]'
LIMIT 10;
```

规划器识别 `ORDER BY <距离表达式> LIMIT k` 形状，把 top-k 下推到 ANN 扫描。内积用降序：

```sql
SELECT id FROM documents
ORDER BY embedding <#> $1 DESC
LIMIT 10;
```

索引路径需要 `LIMIT`。没有 `LIMIT` 时回退到完整距离计算。

## Probes

```sql
SET ivfflat.probes = 16;
SHOW ivfflat.probes;
```

`ivfflat.probes` 映射到探测的 IVF list 数。设置是连接本地的，默认值较小，`0` 会被拒绝。值越大，延迟越高、召回越好。

## 重排

默认情况下，扫描从索引取候选，再在返回前重算精确距离。要直接使用索引的近似分数：

```sql
SET lakesoul_vector_search.rerank = 'index';
```

精确重排是更安全的默认值；只用索引分数更快，但返回的是近似距离。

## 精确回退

没有可用索引，或 metric 索引无法服务（`<=>`）时，引擎在扫描上精确计算距离。结果正确，延迟随行数增长。

## 与过滤组合

同一个查询里的过滤会配合候选 overfetch。见[混合检索](hybrid-search.md)。

## 类型说明

- `vector(n)` 是 Python SDK 使用的列类型。
- 查询里可以用 `CAST(... AS vector(n))` 构造向量值。
- PostgreSQL 前端本身不创建 `vector` 列；表和索引通过 Python SDK 创建。

## 下一步

- [索引管理](index-management.md)
- [混合检索](hybrid-search.md)
- [Benchmark](benchmarks.md)
