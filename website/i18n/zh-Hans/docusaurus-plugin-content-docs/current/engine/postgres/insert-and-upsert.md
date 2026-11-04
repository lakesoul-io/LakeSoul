---
sidebar_position: 3
sidebar_label: 写入与 Upsert
---

# 写入与 Upsert

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

PostgreSQL 前端支持 `INSERT ... VALUES` 和 `INSERT ... SELECT`。两者都通过原生 LakeSoul sink 写入并立即提交。

## 普通插入

```sql
INSERT INTO orders (id, day, region, amount)
VALUES (1, DATE '2026-01-01', 'APAC', 42.00);
```

在主键表上，普通插入是**只插入**：重复主键会以 SQLSTATE `23505` 拒绝。需要 upsert 时用 `ON CONFLICT`。

`INSERT` 省略的列会写入 `NULL`，不是稀疏 upsert。需要保留值的列都要显式列出。

## Upsert

```sql
INSERT INTO orders (id, day, region, amount)
VALUES (1, DATE '2026-01-01', 'APAC', 45.00)
ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount;
```

支持两种冲突动作：

| 子句 | 行为 |
| --- | --- |
| `ON CONFLICT (key) DO NOTHING` | 丢弃与已有主键冲突的行 |
| `ON CONFLICT (key) DO UPDATE SET col = EXCLUDED.col` | 用传入行替换整行 |

`DO UPDATE` 只接受 `column = EXCLUDED.column` 形式。对旧行做表达式（`SET amount = orders.amount + EXCLUDED.amount`）需要读改写，会被拒绝。

## 从查询插入

```sql
INSERT INTO daily_orders (day, region, amount)
SELECT day, region, sum(amount)
FROM orders
GROUP BY 1, 2;
```

没有开启增量 SQL 时，这是一次普通插入。开启后，同一条语句会基于源表 changelog 维护目标表。见[增量 SQL](../incremental-sql/overview.md)。

## 事务

显式事务块内和多语句 simple-query batch 内的写入会被拒绝。LakeSoul 立即提交，后续的 `ROLLBACK` 或 batch 中失败语句无法撤销已写入的行。

`BEGIN` / `COMMIT` 可以用于只读操作；写入放在 autocommit 语句里。

## 错误

| SQLSTATE | 含义 |
| --- | --- |
| `23505` | 普通插入遇到重复主键 |
| `40001` | 与并发写者竞争失败；重试该语句 |
| `42601` | 源列与目标列对不上 |
| `42804` | 表达式无法赋值转换到目标列 |
| `22P02` | 字符串字面量无法转换为目标类型 |
| `0A000` | `RETURNING`、`ON CONFLICT` 表达式赋值，或事务块内写入 |

不支持 `RETURNING`：sink 在行写完后提交，没有可返回的行流。

## 索引维护

如果表声明了向量或全文索引，索引会在元数据提交后更新。索引构建失败时语句返回错误，但写入的行已经提交且可见。

## 下一步

- [事务与目录](transactions-and-catalog.md)
- [增量 SQL](../incremental-sql/getting-started.md)
- [向量检索](../vector-search/overview.md)
