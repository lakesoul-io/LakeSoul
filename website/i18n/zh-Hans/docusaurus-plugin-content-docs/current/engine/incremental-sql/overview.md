---
sidebar_position: 1
sidebar_label: 概览
---

# 增量 SQL

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

增量 SQL 让一条 `INSERT ... SELECT` 消费源表的 commit log：第一次运行构建目标表，之后每次只读取游标之后的提交，只写入发生变化的行。

结果与在完整源表上执行同一个查询一致，区别是工作量：10 行新数据只碰 10 行，而不是整张表。

## 它是什么

- 一个**语句**，不是一个对象。你运行 `INSERT INTO target SELECT ...`；没有单独的定义要创建，没有调度任务，也没有 refresh 命令。
- **变更驱动。** 引擎读取源表的提交，把它折叠进查询，再把 delta 写到目标表。
- **分区感知。** 一次运行会找出有新提交的源分区，只读取这些分区的新数据；目标表也只更新它拥有的分区。
- **CDC 目标表。** 目标表是带 `use_cdc = true` 和 `rowKinds` 变更列的主键表，这样运行才能为消失的行写入撤回记录。
- **SQL 级。** 它维护 join、聚合、窗口和集合运算，而不只是存储层的 delta。

## 一次运行怎么发生

```
源表提交 ──► changelog 窗口 ──► 查询 delta ──► 目标表 delta
                  │
                  └── 目标提交成功后才推进游标
```

1. 引擎读取目标表上次游标之后的源表提交。
2. 计算查询会改变的行。
3. 提交目标表 delta，并在同一步推进游标。
4. 重试会重放同一个窗口，已写入的行会被跳过。

第一次运行会从当前源表状态 **bootstrap** 目标表。如果查询定义变化，或目标表 schema 变化，引擎会从完整源表重建，并以一次原子提交发布。

## 什么时候用

- 看板表要跟随变化的源表，但不想全量重算。
- join 或聚合端到端重算太贵。
- 管道已经通过引擎写入，希望派生表自动跟随。

一次性转换用普通的 `INSERT ... SELECT` 更简单。

## 下一步

- [上手](getting-started.md)
- [支持的查询](supported-queries.md)
- [一致性与限制](consistency-and-limits.md)
