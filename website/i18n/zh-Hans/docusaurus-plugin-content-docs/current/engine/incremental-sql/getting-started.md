---
sidebar_position: 2
sidebar_label: 上手
---

# 增量 SQL：上手

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 1. 以增量 SQL 启动端点

```bash
cargo run -p postgres-lakesoul -- --port 5433 --ivm \
  --warehouse-prefix file:///tmp/lakesoul/warehouse
```

`--ivm` 开启 `INSERT ... SELECT` 的维护路径。不加它时，这条语句就是普通插入。

## 2. 创建目标表

目标表需要主键、CDC 变更列 `rowKinds`，以及 `use_cdc = true`：

```sql
CREATE TABLE daily_revenue (
  day     date not null,
  region  text not null,
  revenue numeric(20,4),
  "rowKinds" text,
  PRIMARY KEY (day, region)
) WITH (hash_bucket_num = 4, use_cdc = true);
```

- 主键标识目标行；对于消失的行，引擎会写删除标记。
- `"rowKinds"` 是变更列。加引号保留大小写；未加引号的标识符会被前端折叠成小写。
- `use_cdc = true` 开启目标表写入撤回记录所需的变更流。没有它，运行无法删除源表里消失的行。
- 如果目标 schema 缺少 epoch 列，引擎会自动加上。

目标表也可以用 `PARTITIONED BY` 做范围分区。一次运行只替换它拥有的分区，其他分区保持不变。

## 3. 运行查询

```sql
SET lakesoul.ivm.enabled = on;

INSERT INTO daily_revenue (day, region, revenue)
SELECT date_trunc('day', ts) AS day,
       region,
       sum(amount) AS revenue
FROM orders
GROUP BY 1, 2;
```

第一次运行会 **bootstrap** 目标表：对当前源表状态计算查询并写入完整结果。

之后的运行只读取上次游标之后的源表提交，只写入发生变化的行。对于给定的源表状态，这条语句是幂等的。

## 4. 保持会话设置

`lakesoul.ivm.enabled` 是会话本地的，默认关闭。每个执行增量 SQL 的连接都要设置它。无论该设置如何，`INSERT ... VALUES` 始终是普通插入。

## 5. 源表变化后重跑

没有调度器，也没有 refresh 命令。需要目标表追上时，重新运行同一条 `INSERT ... SELECT`；引擎决定变化了什么。

如果查询定义变化，下一次运行会从完整源表重建目标表，并以一次原子提交发布。读者不会在重建期间看到空表。

## 要求

- 目标表必须是主键、CDC 表：主键 + `rowKinds` + `use_cdc = true`。
- 源表必须是主键表。需要让删除可见的源表也要开启 `use_cdc = true` 并声明变更列。Append-only 源会被拒绝；无法增量维护的形状会回退到全量重算。
- 语句必须在 autocommit 下运行。显式事务和多语句 batch 会被拒绝。
- v1 中增量 SQL 与分布式执行互斥。

## 下一步

- [支持的查询](supported-queries.md)
- [一致性与限制](consistency-and-limits.md)
