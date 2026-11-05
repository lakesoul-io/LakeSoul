---
sidebar_position: 2
sidebar_label: SQL 参考
---

# 文本 SQL 参考

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 函数

| 函数 | 返回 | 用途 |
| --- | --- | --- |
| `text_match(column, query)` | Boolean | 过滤谓词 |
| `text_score(column, query)` | BM25 分数 | 只能放在 `ORDER BY` |

`text_match` 对列求值查询。`text_score` 提供排序分数；目前不能投影到结果集里。

## Top-k 查询

```sql
SELECT id, title
FROM documents
WHERE text_match(body, 'lakehouse')
ORDER BY text_score(body, 'lakehouse') DESC
LIMIT 10;
```

规划器识别这个模式，向文本索引请求全局 top-k。索引路径需要 `LIMIT`。

## 只过滤、不排序

```sql
SELECT id, title
FROM documents
WHERE text_match(body, 'lakehouse');
```

没有 `ORDER BY text_score(...)` 时，谓词在扫描上精确求值。结果正确，但不走 top-k 路径。

## 规则

- `text_score` 必须出现在 `ORDER BY ... DESC` 里；不支持投影它。
- 没有有限 `LIMIT` 时，规划器回退到精确扫描。
- 列必须通过 Python SDK 声明了文本索引。
- 查询串是标量表达式；支持参数。
- 多个 `text_match` 谓词按普通布尔过滤求值。

## 分词器

索引按列选择一个分词器：

| 分词器 | 行为 |
| --- | --- |
| `jieba` | 中文分词（默认） |
| `default` | Tantivy 默认分词器 |
| `en_stem` | 英文词干 |
| `whitespace` | 按空白切分 |
| `raw` | 整个值作为一个 token |

分词器在创建索引时固定。

## 下一步

- [索引管理](index-management.md)
- [Benchmark](benchmarks.md)
- [向量检索](../vector-search/overview.md)
