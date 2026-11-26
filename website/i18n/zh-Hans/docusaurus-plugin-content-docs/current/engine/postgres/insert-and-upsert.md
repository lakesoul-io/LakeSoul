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
| `0A000` | `RETURNING` 搭配 `ON CONFLICT`、`RETURNING` 表达式、分区表或 CDC 表上的 `ON CONFLICT`、赋值列表缺少非主键列，或事务块内的通用 `DO UPDATE` |

`INSERT ... VALUES ... RETURNING`、`INSERT ... SELECT ... RETURNING` 和
`UPDATE ... RETURNING` 会返回写入所消费的行，支持 `RETURNING *`、列引用和列别名；
表达式和 `ON CONFLICT ... RETURNING` 暂不支持。foreign table 和 external-file
table 上的 `RETURNING` 也会被拒绝。如果 prepared write 的参数出现在 `SELECT` 源或
`UPDATE` 赋值中，必须给它们显式类型（`$1::integer`）或使用驱动的 typed-prepare
接口；只有 `INSERT ... VALUES` 会从目标列推断参数类型。易失的源表达式会被求值两次
（一次生成返回行、一次执行写入），因此会被拒绝。

## 索引维护

如果表声明了向量或全文索引，索引会在元数据提交后更新。索引构建失败时语句返回错误，但写入的行已经提交且可见。

## DELETE 与 TRUNCATE

行表示、分区快速路径和事务边界见 [DELETE 与 TRUNCATE](delete-and-truncate.md)。

## 从 JSON 批量写入

vector.dev 等日志采集器会把一批数据绑定为一个 JSONB 参数，使用如下语句形态：

```sql
INSERT INTO orders
SELECT * FROM jsonb_populate_recordset(NULL::orders, $1)
ON CONFLICT (id) DO UPDATE
SET day = EXCLUDED.day,
    region = EXCLUDED.region,
    amount = EXCLUDED.amount;
```

`$1` 是一个**对象数组**，每个对象对应一行。对象的键映射到列名（会尝试小写匹配），
缺失的键写入 `NULL`。也可以传入数组的数组，此时按位置映射到 `INSERT` 列清单中
指定的列。`DO NOTHING` 与 `DO UPDATE` 两种冲突处理与普通 `VALUES` 插入行为一致。

```json
[
  {"id": 1, "day": "2026-01-01", "region": "APAC", "amount": 42.00},
  {"id": 2, "day": "2026-01-02", "region": "EMEA", "amount": 51.50}
]
```

该函数只在 `INSERT ... SELECT * FROM jsonb_populate_recordset(NULL::table, $n)`
这种形态下被识别，参数必须是 JSONB 标量。它不是通用的表函数，不能用于任意
`SELECT`；这种形态也不支持 `RETURNING`。非法 JSON 以及按位置映射时长度不符的行
分别以 `22P02` / `42601` 拒绝。

## 大批量写入建议

每条 `INSERT` 或 `COPY` 语句对应一次提交，客户端在元数据提交完成后才收到确认。
通过 PG wire 高吞吐写入时：

- **按批发送，不要逐行写**。把多行放进一个 `VALUES` 列表、按上面的方式绑定一个
  JSON 数组，或使用 [`COPY ... FROM STDIN`](copy.md)。使用 vector.dev 的 PostgreSQL
  sink 时，调大 sink 的 batch size，让一条语句携带更多行。
- 语句形态允许时，把多条写放进 `BEGIN ... COMMIT`：暂存写入会在 `COMMIT` 时一起
  发布，减少元数据往返。通用 `DO UPDATE` 仍须在 autocommit 下执行。
- 一批数据会按它触及的**哈希桶和 range 分区**各生成一个文件，而不是每批一个文件。
  根据读取并行度选择 `hash_bucket_num` 和 range 分区；写入密集的表不要设置过大的
  桶数。
- 写入**同一张表、同一个分区**的并发客户端会在该分区的元数据版本上串行。普通
  `INSERT` / `UPDATE` 竞争失败时返回 `40001`，客户端需要重试。`ON CONFLICT DO UPDATE`
  会在服务端重试发布：可兼容的提交（compaction、append、merge）会 rebase 到新快照，
  期间出现 update 或 delete 则报冲突。
- 很多客户端并发写入时，尽量分散到不同分区或不同表，避免都去竞争同一个版本槽。
  服务端的提交重试次数有上限，客户端仍应处理好最终的 `40001` 并重试语句。
- 长期运行的写入仍依赖后台 compaction 服务来合并各批次产生的文件。

## 下一步

- [事务与目录](transactions-and-catalog.md)
- [COPY](copy.md)
- [增量 SQL](../incremental-sql/getting-started.md)
- [向量检索](../vector-search/quickstart.md)
