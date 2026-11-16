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
| **连接性** | PostgreSQL 客户端可以连接并执行查询 | `psql`、pgjdbc、psycopg、libpq 工具；更广泛的 BI/导入工具认证仍在进行 |
| **SQL 表面** | 下面列出的语句和语义 | 见本页表格 |
| **生态** | 依赖 PostgreSQL catalog 行为的工具 | `pg_catalog`、`information_schema`、`psql \d`；更广泛的 BI introspection 仍在进行 |

## 支持

| 领域 | 语句 |
| --- | --- |
| 查询 | 带 join、聚合、窗口、CTE、`UNION`/`INTERSECT`/`EXCEPT`、子查询的 `SELECT` |
| DDL | `CREATE TABLE`、`DROP TABLE`、`ALTER TABLE` schema evolution（加列/加宽列、`SET`/`RESET` 支持的选项）、主键、`PARTITIONED BY`、`WITH (...)` |
| 写入 | `INSERT ... VALUES`、`INSERT ... SELECT`、非 CDC 主键表上的 `UPDATE`、`ON CONFLICT DO NOTHING`、通用 `ON CONFLICT ... DO UPDATE`（部分列、表达式和 conflict `WHERE`） |
| 订阅 | `CREATE SUBSCRIPTION`、`ALTER SUBSCRIPTION ... ENABLE/DISABLE`、`DROP SUBSCRIPTION`，把源 PostgreSQL 逻辑复制到 LakeSoul CDC 表 |
| 事务 | Autocommit、`READ COMMITTED`；LakeSoul 写入使用 deferred publication 的 `BEGIN`/`COMMIT`/`ROLLBACK` |
| 目录 | `pg_catalog`、`information_schema`、`psql \d`、`current_database()` |
| 会话 | `SET TIME ZONE`、`statement_timeout`、`search_path`、`ivfflat.probes`、`RESET`、`DISCARD ALL` |
| 向量 | `vector(n)`、`<->`、`<#>`、`<=>`、`ORDER BY ... LIMIT k`、精确回退、ANN |
| 全文 | `text_score(column, query)`，配合 `ORDER BY ... DESC LIMIT k` |

## 拒绝

以下操作返回 SQLSTATE `0A000`，而不是被静默忽略：

- `DELETE`、`TRUNCATE`、`COPY`、`CREATE TABLE AS`、`CREATE TEMP TABLE`、`RETURNING`
- 多语句 simple-query batch 内的写入；显式事务块内的 DDL、`COPY`、IVM 维护、
  订阅和 savepoint
- CDC 表、无主键表、append-only 表上的 `UPDATE`，`UPDATE ... FROM`，以及更新
  主键或分区列
- 分区表上的 `ON CONFLICT`；CDC 表的通用 `DO UPDATE`；修改冲突键
- `REPEATABLE READ` 和 `SERIALIZABLE`
- `LIKE`、`CLONE`、`INHERITS`
- 列默认值、`UNIQUE`、`CHECK`、`FOREIGN KEY` 约束
- DDL 里的 `STORED AS` / `LOCATION`（改用 `WITH (file_format = ...)`）
- PostgreSQL 扩展、PL/pgSQL、`LISTEN`/`NOTIFY`

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

## 客户端基线

真实驱动基线位于 `rust/postgres-lakesoul/client-compat/`。它当前对运行中的端点
执行 psycopg2、psycopg3、SQLAlchemy 和 pgjdbc，覆盖连接/`SELECT`、参数化写入、
batch、UPSERT、事务、重复键恢复、catalog 发现、fetch-size cursor 和 Metabase
风格的启动语句。

核心 Python 和 JDBC 工作流已通过基线，包括 psql `\d`。已知缺口会显式保留，
而不是隐藏：SQLAlchemy reflection、psycopg2 `bytea` 参数、`COPY FROM STDIN`、
服务端 named cursor 和 `RETURNING`。具体版本和每个缺口的状态见该目录的
README。

## 版本策略

SQL 表面跟随引擎，而不是某个 PostgreSQL 版本。`version()` 返回一个 PostgreSQL
兼容的基线字符串（`PostgreSQL 14.0 (LakeSoul ... / DataFusion ...)`），以便
SQLAlchemy 等驱动初始化 PostgreSQL dialect。后缀标明实际引擎；该字符串只是
功能发现兼容层，不表示完整 PostgreSQL 14 对齐。以上表格就是契约。

## 下一步

- [DDL](ddl.md)
- [写入与 Upsert](insert-and-upsert.md)
- [事务与目录](transactions-and-catalog.md)
- [SQL 错误](../../reference/sql-errors.md)
