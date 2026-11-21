---
sidebar_position: 10
sidebar_label: 时间旅行
---

# 时间旅行（`lakesoul_table_scan`）

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

PostgreSQL 前端提供了一个表函数，把某张 LakeSoul 表的一次扫描固定到历史状态：

```sql
SELECT * FROM lakesoul_table_scan('events', 'timestamp', '2026-11-20T10:00:00+08:00');
```

```text
lakesoul_table_scan(table, mode, value)
```

三个参数都必须是字符串字面量。`mode` 决定 `value` 的含义：

| mode | value | 含义 |
| --- | --- | --- |
| `timestamp` | 带显式时区的 RFC3339 时间戳 | 读取在该时刻或之前提交的每个分区的最新版本 |
| `tag` | tag 名称 | 读取某个 tag 固定的 snapshot |
| `snapshot_id`（别名 `version`） | 整数 snapshot id | 读取 `table_snapshot` 记录的 snapshot |

示例：

```sql
-- 某个时间点的状态
SELECT count(*) FROM lakesoul_table_scan('events', 'timestamp', '2026-11-20T10:00:00+08:00');

-- 命名 snapshot 和 table_snapshot 里的 id
SELECT * FROM lakesoul_table_scan('events', 'tag', 'release-2026-11');
SELECT * FROM lakesoul_table_scan('events', 'snapshot_id', '42');

-- 普通 SQL 可以直接作用在固定视图上
SELECT user_id, sum(amount)
FROM lakesoul_table_scan('events', 'tag', 'release-2026-11')
WHERE amount > 0
GROUP BY user_id;
```

## 语义

- **每次查询生效，不是会话级。** 函数只返回一张表的固定视图，不写任何会话状态；同一会话里的普通 `SELECT` 仍然读最新提交。
- **`timestamp` 是“不晚于”。** 对每个分区，服务端选择 `partition_info.timestamp <=` 给定时刻的最新版本。这个时间戳是分区版本提交时的墙钟时间；同一次原子提交更新的多个分区共享同一个 PostgreSQL 事务时间。必须提供带显式偏移的 RFC3339 字符串（`+08:00`、`Z` 等），不带偏移的字符串会被拒绝。
- **snapshot / tag 读取依赖元数据行**，来自 `table_snapshot`、`snapshot_commit` 和 `table_snapshot_tag`。可以直接用 SQL 创建它们（见[管理 snapshot 与 tag](#管理-snapshot-与-tag)），Python catalog 也提供相同操作。`timestamp` 不需要预先准备。
- **固定读不使用向量/文本索引下推。** 这些索引描述的是最新文件，所以扫描会退回常规的 merge-on-read 路径，通常比等价的当前状态查询慢。
- **读写互不影响。** 读旧版本再把结果写到其他表或分区是普通查询，不会被拒绝；目标表自身的写入仍基于它当前的提交。
- **可见性不变。** 表通过连接所属的 catalog 解析，因此 RBAC 域和 ACL 规则对 `lakesoul_table_scan` 与普通 `SELECT` 完全一致。不支持外部表。
- **注意保留策略。** 固定读需要历史的 `partition_info` 行和 commit 文件仍然存在。tag 通过 `pinned` 标记让 clean job 保留对应 commit；没有 tag 的 snapshot 在足够旧之后可能被回收。

## 会话级默认值

一条设置即可固定整个连接的所有本地表扫描：

```sql
SET lakesoul.read.time_travel = '2026-11-20T10:00:00+08:00';
SELECT * FROM events;          -- 读取该时刻的状态
SELECT * FROM lakesoul_table_scan('events', 'timestamp', '2026-11-21T00:00:00Z');  -- 显式 pin 优先

RESET lakesoul.read.time_travel;   -- 或 SET … = DEFAULT
```

- **不需要预先创建 snapshot 或 tag**。每个分区取该时刻或之前提交的最新版本，语义与 `lakesoul_table_scan` 的 `timestamp` 模式一致。
- 如果某张表在该时刻或之前**没有任何版本**，扫描直接报错 `table "…" has no version committed at or before …`，而不是返回 0 行。该时刻之后才创建的分区会被省略。
- 表里有旧版本、只是查询的过滤条件没有命中，属于正常的空结果，不报错。
- **快照语义统一**：`UPDATE` / `DELETE` 的目标表读取的是同一个固定视图，而不是最新提交——该时刻之后写入的行对它们不可见；`INSERT … SELECT` 也照常从固定视图拷贝到别的表。注意新提交是在当前状态之上发布的，如果某行的“后续改动”和固定视图里的行同时命中，写回去会覆盖那个后续改动，必要时请收窄 `WHERE`。
- 显式的 `lakesoul_table_scan` pin 优先于会话默认值。
- 只作用于本地 LakeSoul 表；联邦外部表没有 time travel，继续读当前状态。
- 值必须是带显式偏移的 RFC3339；`SHOW` 会显示为规范化后的 UTC。`SET LOCAL` 被拒绝（该设置是连接级、不会随 `COMMIT`/`ROLLBACK` 回滚）。`RESET ALL` 和 `DISCARD ALL` 会清除它。
- 设置按连接生效，连接池需要在需要的连接上分别设置。

## 管理 snapshot 与 tag

**snapshot** 是对整张表在某个时刻的分区版本和 commit 的不可变冻结，它的数字 `snapshot_id` 就是 `lakesoul_table_scan` 里的“version”。**tag** 是给 snapshot 起的可读名字。创建时一步完成冻结和命名：

```sql
ALTER TABLE events CREATE TAG `EOW-01`;                                       -- 冻结当前状态
ALTER TABLE events CREATE TAG `v1` AS OF timestamp '2026-11-20T10:00:00+08:00';
ALTER TABLE events CREATE TAG `copy` AS OF VERSION 7;                          -- 给已有版本起名

SHOW TAGS events;                -- tag, version, created_at, snapshot_created_at, description
ALTER TABLE events DROP TAG `v1`;
```

- `AS OF timestamp` 冻结该时刻或之前最新提交的分区版本，tag 读到的正是那个时间点的状态；`AS OF VERSION` 要求 id 是该表已有的 snapshot。不写 `AS OF` 则冻结当前状态。
- `SHOW TAGS` 的**所有列都是文本**，时间是 RFC3339 字符串。它实际上由表函数 `lakesoul_tags('table')` 生成，也可以直接查询。
- `DROP TAG` 删除 tag；当没有其他 tag 或样本 manifest 引用该 snapshot 时，一并删除 snapshot（unpin 其 commit 供正常 GC）。若被共享，则只删 tag 并返回 `NOTICE` 说明保留原因。
- snapshot 版本不可变：刻意**不提供**独立的 `DROP SNAPSHOT` / `DROP VERSION` 语句（`0A000`）。tag DDL 和其他 DDL 一样，不允许在显式事务块中执行。
- RBAC/ACL 可见性同样生效：不可见的表无法打 tag，也不会出现在列表里。

Python 提供相同操作：

```python
catalog.create_tag("events", "EOW-01")                 # 冻结当前 + tag
catalog.create_tag("events", "v1", as_of=as_of_ms)      # 冻结 Unix 毫秒时刻 + tag
catalog.create_tag("events", "copy", snapshot=7)        # 给已有版本起名
catalog.list_tags("events")
catalog.drop_tag("events", "v1")                        # 只删 tag；snapshot 保留
```

## 错误情况

表不存在或不可见、mode 未知、时间戳格式错误、snapshot id 不是整数、参数不是字面量，都会在计划阶段报错，并以 `lakesoul_table_scan:` 开头，例如：

```text
ERROR:  lakesoul_table_scan: timestamp must be an RFC3339 string with an explicit offset
```

## 相关

- [DELETE 与 TRUNCATE](delete-and-truncate.md) — 旧 commit 仍然可读。
- [认证与 RBAC](authentication.md) — 域与表的可见性。
