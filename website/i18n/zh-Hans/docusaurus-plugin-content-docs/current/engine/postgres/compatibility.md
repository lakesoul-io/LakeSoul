---
sidebar_position: 5
sidebar_label: 兼容性
---

# 兼容性

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul 是 **PostgreSQL-native，不是 drop-in 的 PostgreSQL**。wire protocol 和 SQL 表面是原生实现的，这一页记录当前覆盖范围。

## 兼容层级

| 层级 | 含义 | 状态 |
| --- | --- | --- |
| **连接性** | PostgreSQL 客户端可以连接并执行查询 | `psql`、JDBC、`pgx`、DBeaver、libpq 工具 |
| **SQL 表面** | 下面列出的语句和语义 | 见本页表格 |
| **生态** | 依赖 PostgreSQL catalog 行为的工具 | `pg_catalog`、`information_schema`、`psql \d`、做内省的 BI 工具 |

## 支持

| 领域 | 语句 |
| --- | --- |
| 查询 | 带 join、聚合、窗口、CTE、`UNION`/`INTERSECT`/`EXCEPT`、子查询的 `SELECT` |
| DDL | `CREATE TABLE`、`DROP TABLE`、主键、`PARTITIONED BY`、`WITH (...)` |
| 写入 | `INSERT ... VALUES`、`INSERT ... SELECT`、`ON CONFLICT DO NOTHING`、`ON CONFLICT ... DO UPDATE SET col = EXCLUDED.col`（覆盖每个非主键列） |
| 订阅 | `CREATE SUBSCRIPTION`、`ALTER SUBSCRIPTION ... ENABLE/DISABLE`、`DROP SUBSCRIPTION`，把源 PostgreSQL 逻辑复制到 LakeSoul CDC 表 |
| 事务 | Autocommit、`READ COMMITTED`、只读的 `BEGIN`/`COMMIT`/`ROLLBACK` |
| 目录 | `pg_catalog`、`information_schema`、`psql \d`、`current_database()` |
| 会话 | `SET TIME ZONE`、`statement_timeout`、`search_path`、`ivfflat.probes`、`RESET`、`DISCARD ALL` |
| 向量 | `vector(n)`、`<->`、`<#>`、`<=>`、`ORDER BY ... LIMIT k`、精确回退、ANN |
| 全文 | `text_score(column, query)`，配合 `ORDER BY ... DESC LIMIT k` |

## 拒绝

以下操作返回 SQLSTATE `0A000`，而不是被静默忽略：

- `UPDATE`、`DELETE`、`TRUNCATE`、`COPY`
- `CREATE TABLE AS`、`RETURNING`
- 显式事务块或多语句 batch 内的写入
- `REPEATABLE READ` 和 `SERIALIZABLE`
- `LIKE`、`CLONE`、`INHERITS`
- 列默认值、`UNIQUE`、`CHECK`、`FOREIGN KEY` 约束
- DDL 里的 `STORED AS` / `LOCATION`（改用 `WITH (file_format = ...)`）
- PostgreSQL 扩展、PL/pgSQL、`LISTEN`/`NOTIFY`
- 对旧行做表达式的 `ON CONFLICT DO UPDATE`

## SQLSTATE 映射

| SQLSTATE | 含义 |
| --- | --- |
| `0A000` | 功能不支持 |
| `23505` | 重复主键 |
| `25P02` | 当前事务已中止 |
| `40001` | 序列化失败；重试 |
| `42601` | 语法错误或列数不匹配 |
| `42703` | 列不存在 |
| `42804` | 类型不匹配 |
| `42P01` | 表不存在 |
| `42P07` | 表已存在 |
| `42701` | 列重复 |
| `42P16` | 表定义非法 |
| `3F000` | schema 名非法 |
| `22023` | 参数值非法 |
| `22007` | 日期时间格式非法 |
| `57014` | 查询被取消（`statement_timeout`） |
| `XX000` | 内部错误，或启动时 database 不存在 |

少数错误是从引擎错误映射来的，而不是精确复现。例如非法整数转换返回 `XX000`，而 PostgreSQL 返回 `22P02`。已知边界见 [SQL 错误参考](../../reference/sql-errors.md)。

## 版本策略

SQL 表面跟随引擎，而不是某个 PostgreSQL 版本。端点不会宣告一个暗示 PostgreSQL 功能对齐的 `server_version`。以上表格就是契约。

## 下一步

- [DDL](ddl.md)
- [写入与 Upsert](insert-and-upsert.md)
- [事务与目录](transactions-and-catalog.md)
- [SQL 错误](../../reference/sql-errors.md)
