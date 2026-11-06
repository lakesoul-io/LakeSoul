---
sidebar_position: 1
sidebar_label: 概览
---

# PostgreSQL 前端

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`postgres-lakesoul` 是说 PostgreSQL wire protocol 的 LakeSoul 引擎。把 `psql`、JDBC、`pgx` 或 BI 工具接到它上面，语句就在湖仓引擎里、在对象存储上执行。

它不是 PostgreSQL 扩展。没有 Postgres 进程执行查询，也没有数据被复制进 Postgres 堆表；元数据库是独立的基础设施。

## 启动

```bash
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix file:///tmp/lakesoul/warehouse
```

元数据库和第一张表见[快速开始：PostgreSQL 端点](../../get-started/quickstart-postgres.md)。

## 支持范围

| 领域 | 支持 |
| --- | --- |
| **连接** | PostgreSQL wire protocol、simple 与 extended query protocol、`pg_catalog`、`psql \d` |
| **DDL** | `CREATE TABLE`、`DROP TABLE`、主键、`PARTITIONED BY`、`WITH (...)` 选项 |
| **写入** | `INSERT ... VALUES`、`INSERT ... SELECT`、`ON CONFLICT DO NOTHING`、`ON CONFLICT ... DO UPDATE SET col = EXCLUDED.col`（覆盖每个非主键列） |
| **查询** | DataFusion SQL 表面，包括 join、聚合、窗口、CTE 和集合运算 |
| **检索** | 兼容 pgvector 的向量操作符、BM25 的 `text_score` |
| **增量 SQL** | 会话开启后，`INSERT ... SELECT` 维护目标表 |
| **订阅** | `CREATE/DROP/ALTER SUBSCRIPTION` 把一张源 PostgreSQL 表复制到 LakeSoul CDC 表 |
| **事务** | Autocommit；`READ COMMITTED`；失败事务恢复 |
| **目录** | 一个 database 映射一个 LakeSoul namespace；用户表在 `public` |

## 不支持

`UPDATE`、`DELETE`、`COPY`、`RETURNING`、`CREATE TABLE AS`、`TRUNCATE`、非 `READ COMMITTED` 隔离级别，以及显式事务块里的写入，都会以 SQLSTATE `0A000` 被拒绝。完整列表见[兼容性矩阵](compatibility.md)。

## 下一步

- [DDL](ddl.md)
- [写入与 Upsert](insert-and-upsert.md)
- [事务与目录](transactions-and-catalog.md)
- [PostgreSQL 订阅](subscription.md)
- [兼容性](compatibility.md)
- [运维](operations.md)
