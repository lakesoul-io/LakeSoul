---
sidebar_position: 4
sidebar_label: 核心概念
---

# 核心概念

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 表、namespace、分区

LakeSoul 的**表**属于一个 **namespace**。PostgreSQL 端点把 database 映射到 namespace，用户表暴露在 `public` schema 下。

一张表可以有：

- **范围分区**，用 `PARTITIONED BY (col, ...)` 声明。
- **哈希桶**，用 `WITH (hash_bucket_num = n)` 声明。
- **主键**，用于 upsert 和 merge-on-read。

没有主键的表是 append-only 的。

## 提交与快照

每次写入发布一个**提交**，记录它新增或删除了哪些文件。读者打开一个**快照**：表在某个提交上的一致视图。catalog 和提交记录在 PostgreSQL；数据文件在对象存储。

快照支持时间旅行、回滚和增量读取。

## Merge-on-read 与 compaction

主键写入不重写现有文件，而是写一个**delta 文件**，包含新行及其变更类型。读取时把 delta 文件与 base 文件合并，每个主键返回一行。

**Compaction** 按大小分层把 delta 文件合并成更大的 base 文件。它在后台运行，不阻塞读者。

## 索引

向量和全文索引是表的属性。它们存放在表路径下，按（分区，bucket）shard 构建，随写入维护。

- **向量**：IVF + RaBitQ，用 pgvector 风格操作符查询。
- **全文**：Tantivy BM25，用 `text_match` 和 `text_score` 查询。

## 增量 SQL

表的提交序列也是一份 changelog。增量 SQL 为每个目标表保存游标；每次 `INSERT ... SELECT` 只读取游标之后的提交，只写入发生变化的行。

## 两个 PostgreSQL

1. **元数据存储**保存 catalog 和提交记录。它是基础设施，用户不通过它查表。
2. **PostgreSQL 端点**（`postgres-lakesoul`）对用户说 PostgreSQL 协议，并在引擎里执行 SQL。

本地环境把它们放在不同端口。

## 下一步

- [架构](architecture.md)
- [PostgreSQL 前端](../engine/postgres/overview.md)
- [增量 SQL](../engine/incremental-sql/overview.md)
