---
sidebar_position: 1
---

# 什么是 LakeSoul？

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul 是一个开放的湖仓引擎。它把普通对象存储变成带事务的湖仓，并且说 PostgreSQL。

catalog、提交协议、文件 IO、执行引擎和索引都在同一套 Rust 代码里。没有独立的 catalog 服务，读取数据不需要 JVM，检索和 AI 也不需要第二份数据。

## 它能做什么

- **PostgreSQL 前端。** `CREATE TABLE`、`INSERT ... ON CONFLICT` 和查询都在引擎里执行，直接落在对象存储上。`psql`、JDBC、`pgx` 和 `pg_catalog` 都可用；路径上没有 Postgres 扩展，也没有 CDC 管道。
- **增量 SQL。** 一条 `INSERT ... SELECT` 可以基于表自己的 commit log 维护目标表：每次运行只读取游标之后的提交，只写入发生变化的行。
- **向量与全文检索。** pgvector 风格的 SQL 查的是 IVF + RaBitQ 索引，BM25 全文检索查的是 Tantivy 索引；两者都与数据同置，写入时维护。
- **Python 原生读取。** Python SDK 通过 Rust IO 层直接读取对象存储，Daft、Ray、PyTorch 可以分片扫描，不需要 SQL 网关。

## 它在什么位置

表格式解决的是「文件怎么摆」，它没有引擎、没有索引、没有增量状态。数据库给你一个引擎，但你要采用它的存储。

LakeSoul 是中间那层引擎：存储还是你的对象存储，文件仍是开放格式，上面是一层无状态的计算。

```
   PostgreSQL                 Arrow                       Python
   psql / JDBC / pgx          Spark · Flink · Presto      Daft · Ray · PyTorch
        │                     DuckDB · Daft                    │
        └──────────────┬───────────────┬───────────────────────┘
                       ▼               ▼
              ┌──────────────────────────────────┐
              │          LakeSoul 引擎           │
              │  catalog · 提交协议              │
              │  native IO · MOR · compaction    │
              │  索引 · 增量 SQL                 │
              └────────────────┬─────────────────┘
                               ▼
                 对象存储（S3 / OSS / HDFS）
                      + PostgreSQL 元数据
```

## 三种接入方式

| 接入方式 | 面向 | 提供 |
| --- | --- | --- |
| **PostgreSQL** | SQL / BI / 应用 | 协议与 SQL 方言、`psql`、JDBC、`pgx`、pgvector 风格操作符 |
| **Python** | AI 与数据科学 | Daft、Ray、PyTorch、Pandas、DuckDB 直接读取对象存储 |
| **连接器** | 已有计算栈 | Spark、Flink、Presto 等，共用同一条 Rust IO 路径 |

## 它不是什么

- 不是 drop-in 的 PostgreSQL。协议和 SQL 表面是原生实现的，覆盖范围见[兼容性矩阵](../engine/postgres/compatibility.md)。
- 不是表格式。这里有一个能跑、能改的引擎。
- 不拥有你的存储。文件留在你的 bucket 里，保持开放格式。

## 下一步

- [快速开始：PostgreSQL 端点](quickstart-postgres.md)
- [快速开始：Python](quickstart-python.md)
- [架构](architecture.md)
- [引擎概览](../engine/overview.md)
