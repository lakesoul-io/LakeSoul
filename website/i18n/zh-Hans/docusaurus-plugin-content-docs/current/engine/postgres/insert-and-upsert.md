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
ON CONFLICT (id) DO UPDATE
SET day = EXCLUDED.day,
    region = EXCLUDED.region,
    amount = EXCLUDED.amount;
```

支持两种冲突动作：

| 子句 | 行为 |
| --- | --- |
| `ON CONFLICT (key) DO NOTHING` | 丢弃与已有主键冲突的行 |
| `ON CONFLICT (key) DO UPDATE SET col = EXCLUDED.col` | 替换该行；必须给每个非主键列赋值 |

`DO UPDATE` 只接受 `column = EXCLUDED.column` 形式，而且赋值列表必须覆盖**每一个非主键列**。`INSERT` 必须列出所有表列，或省略列清单。对旧行做表达式（`SET amount = orders.amount + EXCLUDED.amount`）需要读改写，会被拒绝。

范围分区表不支持 `ON CONFLICT`：LakeSoul 的唯一键是分区本地的。

## 从查询插入

```sql
INSERT INTO daily_orders (day, region, amount)
SELECT day, region, sum(amount)
FROM orders
GROUP BY 1, 2;
```

`INSERT ... SELECT` 也可以 upsert，列规则相同：

```sql
INSERT INTO orders (id, day, region, amount)
SELECT id, day, region, amount
FROM staging_orders
ON CONFLICT (id) DO UPDATE
SET day = EXCLUDED.day,
    region = EXCLUDED.region,
    amount = EXCLUDED.amount;
```

没有开启增量 SQL 时，这是一次普通插入。开启后，同一条语句会基于源表 changelog 维护目标表。见[增量 SQL](../incremental-sql/overview.md)。

## 事务

显式事务块内的普通 `INSERT` 和 `ON CONFLICT DO NOTHING` 会先 staged，由 `COMMIT` 发布；`ROLLBACK` 会删除它们。通用 `ON CONFLICT ... DO UPDATE` 仍使用自己的发布路径，在事务块内以 `0A000` 拒绝。多语句 simple-query batch 内的写入也会被拒绝。

`BEGIN` / `COMMIT` 可以用于只读操作；写入放在 autocommit 语句里。

## 错误

| SQLSTATE | 含义 |
| --- | --- |
| `23505` | 普通插入遇到重复主键 |
| `40001` | 与并发写者竞争失败；重试该语句 |
| `42601` | 源列与目标列对不上 |
| `42804` | 表达式无法赋值转换到目标列 |
| `22P02` | 字符串字面量无法转换为目标类型 |
| `0A000` | `RETURNING` 搭配 `ON CONFLICT` 或非 `VALUES` 源、分区表或 CDC 表上的 `ON CONFLICT`、赋值列表缺少非主键列，或事务块内的通用 `DO UPDATE` |

`INSERT ... VALUES ... RETURNING` 会返回插入的行，支持 `RETURNING *`、列引用和列别名；表达式、`INSERT ... SELECT ... RETURNING`、`ON CONFLICT ... RETURNING` 和 `UPDATE ... RETURNING` 暂不支持。foreign table 和 external-file table 上的 `RETURNING` 也会被拒绝。

## 索引维护

如果表声明了向量或全文索引，索引会在元数据提交后更新。索引构建失败时语句返回错误，但写入的行已经提交且可见。

## 下一步

- [事务与目录](transactions-and-catalog.md)
- [增量 SQL](../incremental-sql/getting-started.md)
- [向量检索](../vector-search/quickstart.md)
