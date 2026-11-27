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
| DDL | `CREATE TABLE`、`DROP TABLE`、`ALTER TABLE` schema evolution（加列/加宽列、`SET`/`RESET` 支持的选项）、向量/文本索引的 `CREATE INDEX`、`REINDEX TABLE`、主键、`PARTITIONED BY`、`WITH (...)` |
| 写入 | `INSERT ... VALUES` 与 `INSERT ... SELECT`（均支持 `RETURNING *`/列引用）、`COPY ... FROM STDIN`（TEXT、CSV 和 binary）和 `COPY ... TO STDOUT`（TEXT 和 CSV）、非 CDC 主键表上的 `UPDATE`、`DELETE`（CDC tombstone、分区清空或分区重写）和 `TRUNCATE`（含 `PARTITION (...)`）、`ON CONFLICT DO NOTHING`、通用 `ON CONFLICT ... DO UPDATE`（部分列、表达式和 conflict `WHERE`） |
| 订阅 | `CREATE SUBSCRIPTION`、`ALTER SUBSCRIPTION ... ENABLE/DISABLE`、`DROP SUBSCRIPTION`，把源 PostgreSQL 逻辑复制到 LakeSoul CDC 表 |
| 事务 | Autocommit、`READ COMMITTED`；LakeSoul 写入使用 deferred publication 的 `BEGIN`/`COMMIT`/`ROLLBACK` |
| 目录 | `pg_catalog`、`information_schema`、`psql \d`/`\dt`/`\l`/`\dn`、`pg_cursors`、`pg_settings`、`pg_roles`、`current_database()`、`current_user`/`session_user`、`to_regtype`、`current_setting`/`set_config`、`psycopg2` dialect 下的 SQLAlchemy reflection |
| 会话 | `SHOW`/`SHOW ALL`、`SET TIME ZONE`、`statement_timeout`、`search_path`、`ivfflat.probes`、`RESET`、`DISCARD ALL` |
| 报表 | `date_trunc`、`date_part`/`extract`、PostgreSQL 日期/时间 pattern 的 `to_char`、`generate_series`、`DISTINCT ON`、`ROLLUP`/`GROUPING SETS`/`CUBE` |
| 向量 | `vector(n)`、`<->`、`<#>`、`<=>`、`ORDER BY ... LIMIT k`、精确回退、ANN |
| 全文 | `text_score(column, query)`，配合 `ORDER BY ... DESC LIMIT k` |

## 拒绝

以下操作返回 SQLSTATE `0A000`，而不是被静默忽略：

- `CREATE TABLE AS`、`CREATE TEMP TABLE`、`DELETE ... USING`
- `COPY ... TO` 文件或程序、`COPY ... FROM` 文件或程序、导出的 `FORMAT binary`、
  binary 数组/vector 值，以及 [COPY](copy.md) 中列出的选项
- 多语句 simple-query batch 内的写入、`COPY` 和表 DDL；显式事务块内的
  `CREATE INDEX`、`REINDEX`、IVM 维护、订阅、非 CDC `DELETE` 重写和 `TRUNCATE`
- CDC 表、无主键表、append-only 表上的 `UPDATE`，`UPDATE ... FROM`，以及更新
  主键或分区列；append-only 表和无主键 CDC 表上的 `DELETE`
- 分区表上的 `ON CONFLICT`；CDC 表的通用 `DO UPDATE`；修改冲突键
- `REPEATABLE READ` 和 `SERIALIZABLE`
- `LIKE`、`CLONE`、`INHERITS`
- 列默认值、`UNIQUE`、`CHECK`、`FOREIGN KEY` 约束
- btree/hash/GiST/BRIN 索引、表达式/多列/部分索引
- `DROP INDEX`、`REINDEX INDEX/SCHEMA/DATABASE`（改用
  `lakesoul_drop_vector_index` 和 `REINDEX TABLE`）
- DDL 里的 `STORED AS` / `LOCATION`（改用 `WITH (file_format = ...)`）
- PostgreSQL 扩展、PL/pgSQL、`LISTEN`/`NOTIFY`

## 已知限制

- 普通查询里的 `INTERSECT ALL` / `EXCEPT ALL` 目前按成员语义执行，而不是 SQL 多重集计数：只要某行在右侧出现，`INTERSECT ALL` 会保留它在左侧的全部重复，`EXCEPT ALL` 会删掉它的全部左侧重复；PostgreSQL 返回 `min(m, n)` 和 `max(m - n, 0)` 份。`DISTINCT` 变体不受影响。见 [#1320](https://github.com/lakesoul-io/LakeSoul/issues/1320)。由 IVM 维护的视图（`INSERT INTO ... SELECT ... INTERSECT ALL` / `EXCEPT ALL`）会正确维护按元组计数；需要精确的多重集语义时可以使用它。

## SQLSTATE 映射

| SQLSTATE | 含义 |
| --- | --- |
| `0A000` | 功能不支持 |
| `23505` | 重复主键 |
| `25P02` | 当前事务已中止 |
| `40001` | 序列化失败；重试 |
| `42601` | 语法错误或列数不匹配 |
| `22P02` | 列类型的文本表示非法 |
| `22P04` | `COPY` 数据流或行格式错误 |
| `23502` | NOT NULL 违反 |
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
执行 psycopg2、psycopg3、SQLAlchemy 和 pgjdbc，覆盖连接/`SELECT`、参数化写入
（包括 `bytea`）、`COPY ... FROM STDIN`（TEXT/CSV/binary）和 `COPY ... TO STDOUT`
（psycopg2、psycopg3 和 pgjdbc `CopyManager` 的 TEXT/CSV）、batch、UPSERT、事务、重复键恢复、
catalog 发现
（`SHOW`、`pg_settings`、`pg_roles`、psql `\dt`/`\l`/`\dn`）、SQLAlchemy
reflection、forward-only named cursor、fetch-size cursor 和 Metabase 风格的启动
语句。

核心 Python 和 JDBC 工作流已通过基线，包括 psql `\d`、`bytea` 往返和
`psycopg2` dialect 下的 SQLAlchemy reflection。Named cursor 目前只支持
forward-only、非 holdable；`WITH HOLD`、`SCROLL` 和 backward fetch 会显式拒绝。
已知缺口会显式保留，而不是隐藏：`ON CONFLICT DO NOTHING` 上的 `RETURNING` 以及
`RETURNING` 表达式，以及 `pg_dump --data-only`（它使用 REPEATABLE READ 事务和
`OPERATOR(pg_catalog.=)` 语法，当前 parser 不接受）。数字 `to_char`
pattern 未实现，会显式报错；在同一个 projection 里同时选择 `current_user` 和
`session_user` 仍受兼容 parser 的 `current_user -> session_user` 重写限制；
`psql \du` 仍需要 `ARRAY(subquery)` 支持。SQLAlchemy
的 `psycopg3` dialect 尚未纳入基线；它的 reflection 现在走已支持的 `SAVEPOINT`
路径，但其余 reflection 查询仍有缺口。具体版本和每个缺口的状态见该目录的
README。

## 版本策略

SQL 表面跟随引擎，而不是某个 PostgreSQL 版本。`version()` 返回一个 PostgreSQL
兼容的基线字符串（`PostgreSQL 14.0 (LakeSoul ... / DataFusion ...)`），以便
SQLAlchemy 等驱动初始化 PostgreSQL dialect。后缀标明实际引擎；该字符串只是
功能发现兼容层，不表示完整 PostgreSQL 14 对齐。以上表格就是契约。

## 下一步

- [DDL](ddl.md)
- [写入与 Upsert](insert-and-upsert.md)
- [DELETE 与 TRUNCATE](delete-and-truncate.md)
- [COPY](copy.md)
- [事务与目录](transactions-and-catalog.md)
- [SQL 错误](../../reference/sql-errors.md)
