---
sidebar_position: 1
sidebar_label: 概览
---

# 连接器

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

连接器是可选的。引擎本身说 PostgreSQL，并提供 Python SDK；这一节面向已经在用 Spark、Flink、Presto 或其他引擎、希望它们读写同一批表的团队。

| 连接器 | 版本 | 读取 | 写入 | 接口 |
| --- | --- | --- | --- | --- |
| [Spark](spark/getting-started.mdx) | 3.5 | 批量 | 批量 | Java / Python / Scala / SQL |
| [Flink](flink/getting-started.mdx) | 1.20 | 流式 | 流式 | Java / SQL |
| [Presto](presto.md) | 0.296 | 批量 | — | SQL |
| [Doris](doris.md) | — | 批量 | — | SQL |
| [Kyuubi](kyuubi.md) | — | 批量 | — | Hive JDBC |
| [Hive 导出](hive.md) | — | 批量 | 导出 | Hive |
| [Elasticsearch 网关](elasticsearch-gateway.md) | — | 检索 | — | HTTP / Elasticsearch API |

## 不变的部分

每个连接器都使用同一套 Rust IO 层、同一个 catalog 和同一份快照。Spark 写入的表，可以从 `psql` 和 Python 直接读取，不需要复制或同步任务。

## 连接器用来做什么

- 让已有的 Spark 或 Flink 管道读写 LakeSoul 表。
- 从 Presto 或 Doris 集群读取 LakeSoul。
- 把表暴露给 Hive 生态工具。
- 提供 Elasticsearch 兼容的检索 API。

## 下一步

- [Spark](spark/getting-started.mdx)
- [Flink](flink/getting-started.mdx)
- [Presto](presto.md)
