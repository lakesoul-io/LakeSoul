---
sidebar_position: 1
sidebar_label: 概览
---

# 引擎概览

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

一个引擎，在同一批表、同一份快照上提供四种能力。

| 能力 | 提供什么 | 从这里开始 |
| --- | --- | --- |
| **PostgreSQL 前端** | `psql`、JDBC、`pgx`、`pg_catalog`、DDL、`INSERT`、upsert 和查询，由引擎执行 | [PostgreSQL](postgres/overview.md) |
| **增量 SQL** | `INSERT ... SELECT` 只读新提交、只写变化的行 | [增量 SQL](incremental-sql/overview.md) |
| **向量检索** | 湖上 IVF + RaBitQ 索引上的 兼容 pgvector 的 SQL | [向量检索](vector-search/overview.md) |
| **全文检索** | 同一引擎里的 Tantivy BM25 排序 | [全文检索](full-text-search/overview.md) |

## 一份快照，多个读者

PostgreSQL 端点、Python SDK 和连接器读取同一个已提交快照。一个路径提交的变更，对其他路径立即可见；SQL、检索和 Python 之间没有复制步骤。

## 边界

- 引擎在对象存储上执行 SQL，不需要 JVM。
- PostgreSQL 前端是原生实现，不是 drop-in 的 PostgreSQL。见[兼容性矩阵](postgres/compatibility.md)。
- 增量 SQL 是一种查询执行方式，不是独立对象。你运行一条 `INSERT ... SELECT`，引擎决定变化了什么。
- Spark、Flink、Presto 是可选连接器，引擎不依赖它们。

## 下一步

- [架构与表格式](architecture.md)
- [PostgreSQL 前端](postgres/overview.md)
- [增量 SQL](incremental-sql/overview.md)
- [向量检索](vector-search/overview.md)
- [全文检索](full-text-search/overview.md)
