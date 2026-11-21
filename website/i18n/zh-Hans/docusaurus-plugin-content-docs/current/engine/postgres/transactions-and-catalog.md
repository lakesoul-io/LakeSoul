---
sidebar_position: 4
sidebar_label: 事务与目录
---

# 事务与目录

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 事务

端点支持 **autocommit** 和显式事务块，可见性为 **READ COMMITTED**。事务块内的写入会先写文件，但直到 `COMMIT` 才发布元数据；其他连接在提交前看不到任何数据。`COMMIT` 在一个元数据事务中发布所有表的 staged 写入；`ROLLBACK` 删除 staged 文件。

- 支持 `BEGIN` / `COMMIT` / `ROLLBACK`。块内读取会看到已发布快照加上本块已 staged 的写入。
- `CREATE TABLE`、`DROP TABLE`、`ALTER TABLE` 会在块内 staged，并在 `COMMIT`
  时与数据写入在同一个元数据事务中发布。提交前其他连接看不到；`ROLLBACK`
  不会留下表或文件。`CREATE TABLE` 之后的同块语句可以立即使用新表。
- 支持 `SAVEPOINT`、`RELEASE SAVEPOINT`、`ROLLBACK TO SAVEPOINT`。savepoint
  会记录本块已 staged 的数据和表 DDL；`ROLLBACK TO SAVEPOINT` 丢弃其后的修改，
  并可从失败事务中恢复。
- 事务块内的 `CREATE INDEX`、`REINDEX`、`TRUNCATE`、IVM 维护、订阅、用户和
  GRANT 仍以 `0A000` 拒绝。`COPY ... FROM STDIN` 是写入：它会在块内 staged，
  并由 `COMMIT` 发布。
- `REPEATABLE READ`、`SERIALIZABLE` 及其 `SET` 形式会以 `0A000` 拒绝。
- 失败事务之后的语句返回 `25P02`，直到 `ROLLBACK` 或成功的
  `ROLLBACK TO SAVEPOINT`。失败块里的 `COMMIT` 会以回滚结束事务，与 PostgreSQL 一致。
- CDC `DELETE` 是 tombstone append，会在块内正常 staged；块内读取通过
  read-your-writes 看到删除效果。多语句 simple-query batch 内的写入和表 DDL
  仍被拒绝：这些语句还没有运行在同一个事务中。请把它们拆成多条消息，或使用
  显式事务块。
- `statement_timeout` 会以 `57014` 取消正在运行的查询；连接仍可继续使用。
- 未实现 `idle_in_transaction_session_timeout`。

## 目录

一个 PostgreSQL **database** 精确映射到一个 LakeSoul **namespace**。在一个连接里：

- 用户表位于 `public` schema。
- `current_database()` 返回 namespace 名。
- `pg_catalog` 和 `information_schema` 暴露该 namespace 内可见的表、列和类型。
- `psql \d`、`\dt`、`\dn` 可用。
- 其他 namespace 会出现在 `pg_database`，但它们的表不能从这个连接读取。跨库读写会被拒绝。

连接一个不是 LakeSoul namespace 的 database，会在启动时以 `FATAL XX000` 失败。快速开始使用 `script/meta_init.sql` 创建的 `default` namespace。

`search_path` 不能绕过 database 边界；`public` 之外的 schema 不是第二个 namespace。

## 会话设置

端点接受并应用：

| 设置 | 作用 |
| --- | --- |
| `SET TIME ZONE` / `SET timezone` | 时间戳渲染和解析的会话时区 |
| `SET statement_timeout` | 每条语句的预算 |
| `SET search_path` | 接受；不跨越 database 边界 |
| `SET ivfflat.probes` | 向量扫描的 ANN probe 数 |
| `RESET ALL` / `DISCARD ALL` | 恢复连接默认值 |

不支持的设置返回 `42704`。时区的 `SET LOCAL` 和 `SET GLOBAL` 返回 `0A000`。

## 下一步

- [兼容性](compatibility.md)
- [运维](operations.md)
