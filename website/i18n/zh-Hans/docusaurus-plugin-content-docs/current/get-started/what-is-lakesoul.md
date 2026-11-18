---
sidebar_position: 1
---

# 什么是 LakeSoul？

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 为什么做 LakeSoul

传统的数据平台是很多系统的拼接：分析用数仓，新鲜度用 CDC 管道，检索用向量库，状态用 OLTP 库，再用 ETL 把它们对齐。每多一个系统，Agent 就多学一套 API，数据也多一份需要对齐的副本；闭源平台还会把数据锁住。

LakeSoul 选择另一条路。Agent 通常已经具备 PostgreSQL 客户端，LakeSoul 通过 PostgreSQL wire protocol 提供一个开放湖仓：联邦查询可以直接访问数据所在的位置，CDC 订阅保持数据新鲜，向量与结构化检索进入同一个执行计划，增量 SQL 维护派生表，同一批表从单机平滑扩展到 PB 级分布式集群。数据保留在你的对象存储中，以 Parquet 和 Vortex 文件保存，代码使用 Apache 2.0 许可。

Agent 只是 PostgreSQL 客户端的一种。同一个端点同时服务 `psql`、BI 工具、应用和 notebook；Agent 使用它不需要额外的 API，也不需要复制一份数据。catalog、提交协议、文件 IO、执行引擎和索引都在同一套 Rust 代码里；读取数据不需要 JVM，检索和 AI 也不需要第二份数据。

## 它能做什么

- **PostgreSQL 前端。** `CREATE TABLE`、`INSERT ... ON CONFLICT`、`UPDATE`、`COPY ... FROM STDIN`、`RETURNING` 和显式事务块都在引擎里执行，直接落在对象存储上。`psql`、JDBC、`pgx`、SQLAlchemy reflection 和 `pg_catalog` 可用；路径上没有 Postgres 扩展，也没有 CDC 管道。
- **联邦查询。** 通过 ADBC foreign table 访问 PostgreSQL、MySQL、SQLite、DuckDB、BigQuery、Snowflake 和 Trino 中的表；与本地表在同一个 DataFusion 计划里 join。S3 上的 Parquet、CSV 和 JSON 文件也可以作为 foreign table 查询。
- **CDC 订阅。** `CREATE SUBSCRIPTION` 把 PostgreSQL logical replication 或 MySQL binlog 的变更写入 LakeSoul CDC 表，支持受管快照、schema evolution 和多表订阅。
- **增量 SQL。** 一条 `INSERT ... SELECT` 可以基于表自己的 commit log 维护目标表：每次运行只读取游标之后的提交，只写入发生变化的行。
- **向量与全文检索。** 兼容 pgvector 的 SQL 查的是 IVF + RaBitQ 索引，BM25 全文检索查的是 Tantivy 索引；两者都与数据同置，写入时维护。
- **Python 原生读取。** Python SDK 通过 Rust IO 层直接读取对象存储，Daft、Ray、PyTorch 可以分片扫描，不需要 SQL 网关。
- **分布式执行。** coordinator 在多个 `lakesoul-worker` 进程之间规划任务，并支持 Kubernetes discovery；同一批表从单进程扩展到 PB 级集群。Spark、Flink、Presto 通过同一条 Rust IO 路径接入。
- **认证与权限。** 客户端通过用户名和密码连接；每条连接都被限制在该用户被授予的权限内。可以在同一份 catalog 上，为不同的 workload 或 Agent 启动不同的计算集群。

## 它在什么位置

表格式解决的是「文件怎么摆」，它没有引擎、没有索引、没有增量状态。数据库给你一个引擎，但你要采用它的存储。

LakeSoul 是中间那层引擎：存储还是你的对象存储，文件仍是开放格式，上面是一层无状态的计算。[架构与表格式](../engine/architecture.md)描述了 catalog、提交协议和存储布局。

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
| **PostgreSQL** | SQL / BI / 应用 | wire protocol 与 SQL 方言、`psql`、JDBC、`pgx`、SQLAlchemy reflection、兼容 pgvector 的操作符 |
| **Python** | AI 与数据科学 | Daft、Ray、PyTorch、Pandas、DuckDB 直接读取对象存储 |
| **连接器** | 已有计算栈 | Spark、Flink、Presto 等，共用同一条 Rust IO 路径 |

## 限制

- 不是 drop-in 的 PostgreSQL。wire protocol 和 SQL 表面是原生实现的，具体覆盖范围见[兼容性矩阵](../engine/postgres/compatibility.md)。
- 它通过 foreign table 和 CDC 订阅读取业务数据库，不替代 OLTP 数据库。
- 元数据 catalog 依赖 PostgreSQL，是部署的一部分。
- 表格式不是公开规范；其他引擎通过 LakeSoul 连接器或 native IO 读取。
- PB 级部署需要分布式 worker 或 Spark/Flink 集群；容量取决于对象存储和挂载的计算资源。

## 下一步

- [快速开始：PostgreSQL 端点](quickstart-postgres.md)
- [快速开始：Python](quickstart-python.md)
- [架构](architecture.md)
- [引擎概览](../engine/overview.md)
