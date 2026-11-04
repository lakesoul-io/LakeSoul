---
sidebar_position: 3
sidebar_label: 支持的查询
---

# 支持的查询

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

增量 SQL 在主键源表上维护查询。下面这些形状会增量维护；列表之外的形状回退到全量重算，append-only 源会被拒绝。

## 投影、过滤、去重

| 形状 | 说明 |
| --- | --- |
| `SELECT [expr AS c, ...] FROM src [WHERE p]` | 过滤条件可以与不相关标量子查询比较 |
| `SELECT DISTINCT c, ... FROM src` | 每个 distinct 元组维护一行 |
| `SELECT DISTINCT ON (g) g, v FROM src [ORDER BY ...]` | 源表主键确定性地打破平局 |

## 聚合

| 形状 | 说明 |
| --- | --- |
| `SUM`、`COUNT(*)`、`COUNT(v)`、`AVG` | 带或不带 `GROUP BY`；支持 `HAVING` |
| `MIN`、`MAX` | 带或不带 `GROUP BY` |
| `COUNT(DISTINCT v)`、`SUM(DISTINCT v)` | 多列 distinct 需要 `GROUP BY` |
| `VAR_SAMP`、`VAR_POP`、`STDDEV_SAMP`、`STDDEV_POP`、`STDDEV` | |
| `MEDIAN` | |
| `BOOL_AND`、`BOOL_OR` | |
| `APPROX_DISTINCT`、`APPROX_PERCENTILE_CONT`、`APPROX_MEDIAN` | 近似结果 |
| `STRING_AGG`、`ARRAY_AGG` | 支持聚合内的 `ORDER BY` |
| `BIT_AND`、`BIT_OR`、`BIT_XOR` | |
| `CORR`、`COVAR_SAMP`、`COVAR_POP`、`REGR_*` | |
| `PERCENTILE_CONT` | |
| 同一个 `GROUP BY` 里混用聚合 | 支持函数可以任意组合 |

`GROUPING SETS`、`ROLLUP`、`CUBE` 会展开成成员集合。`SUM`、`COUNT`、`AVG` 保持增量布局；其他组合按集合重算。

## 窗口与 top-k

| 形状 | 说明 |
| --- | --- |
| `ROW_NUMBER() OVER (PARTITION BY p ORDER BY o)` | 重算受影响的分区 |
| `SELECT ... FROM (SELECT ..., ROW_NUMBER() ... AS rn) t WHERE rn <= k` | 每个分区的 top-k |
| `SELECT ... FROM src ORDER BY o LIMIT k` | 全局 top-k；`OFFSET` 移动窗口 |

边界平局由源表主键打破。

## Join

| 形状 | 说明 |
| --- | --- |
| 内等值 join | 两侧都是主键表；键名可以不同；支持多个 payload 列 |
| 右侧按 join 键建主键的 `LEFT JOIN` | Lookup join |
| 3–32 个源表的多路内 join | 可选的侧过滤和跨源条件 |
| 左深 `[LEFT] JOIN` 链 | 每一步按自己的 join 键建主键 |
| `CROSS JOIN` | 两侧都是主键表 |
| `LEFT`、`RIGHT`、`FULL` 外等值 join | 未匹配的身份可为 NULL |
| `IS NOT DISTINCT FROM` join 键 | 普通列支持 NULL 匹配 NULL |

## 集合运算

| 形状 | 说明 |
| --- | --- |
| `UNION ALL` | 合并同 schema 的源 |
| `UNION` | 去重 |
| `INTERSECT`、`EXCEPT` | `DISTINCT` 和 `ALL` 变体 |
| `INTERSECT ALL`、`EXCEPT ALL` | 按元组匹配计数 |

## 半连接/反连接与相关形状

| 形状 | 说明 |
| --- | --- |
| `[NOT] EXISTS`、`IN`、`op ANY`、`op ALL` | 半连接/反连接，可选侧过滤 |
| select 列表里的相关标量聚合 | `COUNT` 家族保持空集语义 |
| 相关标量过滤（`WHERE v > (SELECT AVG(v) ...)`） | 子查询涉及的表会与源表一起被监听 |

## 不支持

- Append-only 源用于增量运行。改用主键源，或让形状回退到全量重算。
- 改变目标行身份、但目标表没有主键的 `SELECT`。
- 列表之外的形状。引擎会报告 `unsupported incremental view shape`，不会写入部分结果。

## 下一步

- [一致性与限制](consistency-and-limits.md)
- [上手](getting-started.md)
