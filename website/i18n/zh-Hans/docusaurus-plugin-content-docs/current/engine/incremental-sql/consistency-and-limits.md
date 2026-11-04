---
sidebar_position: 4
sidebar_label: 一致性与限制
---

# 一致性与限制

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 重放安全

每个消费的源窗口都会记录在引擎的 epoch 表里，按目标表索引。同一次运行重试时，引擎会看到已记录的窗口，跳过已经写入的行。目标提交和游标推进一起发生。

一次运行要么完整生效，要么不生效。失败的运行会把目标表留在上一次已提交状态。

## 重建

重建会从完整源表重新计算目标，并以一次原子提交发布。它不会先 truncate 用户可见的目标表，所以读者不会在重建期间看到空表。

以下情况会触发重建：

- 查询定义变化；
- 目标表 schema 发生引擎无法协调的变化；
- 目标表没有可用状态（例如被删除后重建）。

## 并发

- 同一目标表上的两次运行由 per-target lease 串行化。第二次会等待或失败，不会交错写入。
- 其他引擎在本次运行拥有的分区里写入的数据提交，会在运行前和提交前被检测到；运行失败，而不是静默覆盖。Compaction 提交被放行。
- 其他分区上的普通写入可以并发进行。

## 保留期

一次运行需要游标到当前之间的源表历史。不要对增量 SQL 消费的表开启 `partition.ttl`、`compaction.ttl` 或 `dataExpiredTime` 清理：删除历史会让游标不可读。

如果历史缺失，运行会失败；下一次运行会从当前源表状态重建。

## 限制

| 限制 | 行为 |
| --- | --- |
| **只支持 autocommit** | 显式事务和多语句 batch 会被拒绝 |
| **只支持 `INSERT ... SELECT`** | `INSERT ... VALUES` 始终是普通插入 |
| **源表必须有主键** | 没有主键的源会被拒绝 |
| **目标表必须有主键** | 目标需要主键和 `rowKinds` |
| **v1 单机** | `--ivm` 与分布式执行互斥 |
| **支持的形状** | 列表之外的形状报告 `unsupported incremental view shape`，不写入任何内容 |

## 失败模式

| 现象 | 原因 |
| --- | --- |
| `source table ... has no primary key` | 源表是 append-only；改用主键源 |
| `unsupported incremental view shape` | 查询形状不在支持列表内 |
| `target table ... needs the internal rowKinds/__ivm_epoch columns` | 目标缺少 `rowKinds`（或引擎无法添加 epoch 列） |
| `target table ... needs a primary key` | 目标不是主键表 |
| 并发写者错误 | 运行期间其他引擎写入了目标范围；重试 |

## 下一步

- [支持的查询](supported-queries.md)
- [上手](getting-started.md)
