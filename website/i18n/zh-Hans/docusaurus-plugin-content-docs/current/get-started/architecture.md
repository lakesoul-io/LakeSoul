---
sidebar_position: 5
---

# 架构

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul 分三层：上面是客户端，中间是一个引擎，底下是你的存储。

```
┌──────────────────────────────────────────────────────────────┐
│  psql · JDBC · BI 工具       Python SDK · Daft · Ray · Torch │
└───────────────┬──────────────────────────┬───────────────────┘
                │ PG wire                  │ native IO / Arrow
┌───────────────▼──────────────────────────▼───────────────────┐
│                        LakeSoul 引擎                         │
│  PostgreSQL 前端   ·   增量 SQL                              │
│  向量 + 全文索引   ·   向量化执行                             │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│  对象存储（S3 / OSS / HDFS）  +  PostgreSQL 元数据            │
└──────────────────────────────────────────────────────────────┘
```

## 引擎组件

| 组件 | 职责 |
| --- | --- |
| **Catalog 与提交协议** | 表、namespace、快照和原子提交，存放在 PostgreSQL |
| **Native IO** | Parquet、Vortex、Vortex Compact；主键表 merge-on-read；对象存储缓存 |
| **执行** | DataFusion 计划与向量化执行，加上 LakeSoul 的 planner 规则 |
| **PostgreSQL 前端** | wire protocol、`pg_catalog`、DDL、`INSERT`、会话与限制 |
| **增量 SQL** | 消费源表 commit log，只写变化的行 |
| **向量与全文索引** | IVF + RaBitQ 与 Tantivy BM25，写入时维护并下推到扫描 |
| **Compaction** | 存算分离的 size-tiered delta 文件合并 |

## 两个 PostgreSQL

“PostgreSQL” 会出现在两个不同的位置：

1. **元数据存储。** LakeSoul 用 PostgreSQL 存 catalog 和提交记录。它是基础设施，用户不直接连它查表。
2. **PostgreSQL 端点。** `postgres-lakesoul` 对用户说 PostgreSQL 协议。它是 LakeSoul 引擎，不是元数据库。

本地环境把两者放在不同端口：元数据用 `5432`，端点用 `5433`。

## 计算与存储

- **存储是你的。** 表数据在你的 bucket 里，保持开放格式；提交元数据在 PostgreSQL。
- **计算无状态。** `postgres-lakesoul` 进程可以重启或替换，不需要搬数据。配置分布式 worker 后，查询会跨 worker 计划。
- **同一份快照。** PostgreSQL 端点、Python SDK 和连接器读取同一个已提交快照。检索和 AI 没有第二份数据。

## 下一步

- [引擎概览](../engine/overview.md)
- [PostgreSQL 前端](../engine/postgres/overview.md)
- [增量 SQL](../engine/incremental-sql/overview.md)
- [向量检索](../engine/vector-search/overview.md)
