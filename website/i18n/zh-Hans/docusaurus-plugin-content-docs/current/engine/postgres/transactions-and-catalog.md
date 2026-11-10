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

端点运行在 **autocommit** 和 **READ COMMITTED** 可见性下。每条语句看到最新的已提交快照；写入语句在返回前完成提交。

- `BEGIN` / `COMMIT` / `ROLLBACK` 可以用于只读操作。
- 显式事务块内的写入会以 `0A000` 拒绝。
- `REPEATABLE READ`、`SERIALIZABLE` 及其 `SET` 形式会以 `0A000` 拒绝。
- 失败事务之后的语句返回 `25P02`，直到 `ROLLBACK`。失败块里的 `COMMIT` 会以回滚结束事务，与 PostgreSQL 一致。
- `statement_timeout` 会以 `57014` 取消正在运行的查询；连接仍可继续使用。

多语句 simple-query batch 会先执行读语句，再在写语句处拒绝。不要依赖 batch 的原子性：它不是原子的。

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
