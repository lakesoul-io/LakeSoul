---
sidebar_position: 7
sidebar_label: PostgreSQL 订阅
---

# PostgreSQL 订阅

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`postgres-lakesoul` 可以消费 PostgreSQL 逻辑复制 publication，把变更写入预先创建的 LakeSoul CDC 表。订阅通过 PostgreSQL 端点定义：`CREATE SUBSCRIPTION` 校验源库和目标表，把定义和进度写入元数据 PostgreSQL，并拉起受监管的本地订阅进程。

这是把逻辑复制写入 LakeSoul，不是 PostgreSQL 扩展，也不是另一套 CDC 集群。

## 概览

| 项目 | 行为 |
| --- | --- |
| 源库 | PostgreSQL 14+、logical WAL、一张 `public` 表、`REPLICA IDENTITY FULL`、主键匹配 |
| 目标 | 当前连接 database 下预先创建的 LakeSoul CDC 表，不分区，创建订阅时必须为空 |
| 初始数据 | 受管快照；在最终一次原子提交发布快照之前，目标表保持为空 |
| 变更 | `INSERT`、`UPDATE`、`DELETE`；完整行加 CDC 变更列 |
| 生命周期 | `CREATE`、`ALTER ... ENABLE/DISABLE`、`DROP`；SQL 创建的订阅会在端点重启后恢复 |
| 状态 | 订阅定义和进度在元数据 PostgreSQL；数据进度记录在目标表 commit id 中 |

一个订阅对应一张源表和一张目标表。一张目标表只能被一个订阅使用。

## 源库配置

在源 PostgreSQL 开启逻辑 WAL：

```conf
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

修改 `wal_level` 后重启 PostgreSQL。创建表、publication 和 replica identity：

```sql
CREATE TABLE public.users (
  id         bigint PRIMARY KEY,
  name       text NOT NULL,
  amount     numeric(20,4),
  updated_at timestamptz
);

ALTER TABLE public.users REPLICA IDENTITY FULL;

CREATE PUBLICATION app_pub
  FOR TABLE public.users
  WITH (publish = 'insert,update,delete');
```

源库用户需要 `REPLICATION` 权限和表的 `SELECT` 权限。建议使用只具备所需权限的专用复制用户。

源库必须是主库。从库无法创建本订阅所需的 logical slot，而从库自己的一致性快照也无法在没有 LSN 对齐协议的情况下接到主库 slot 上。

源表必须是 `public` schema 下的普通表。把 `max_slot_wal_keep_size` 设置为能容纳订阅可能保留的 WAL 的大小，并尽量在业务低峰执行大表初始拷贝。快照 reader 会在自己的连接上设置 `statement_timeout = 0`、`DateStyle = ISO`、`TimeZone = UTC` 和 `bytea_output = hex`。

## 目标表配置

通过 PostgreSQL 端点创建目标表。连接使用的 database 就是目标 LakeSoul namespace：

```sql
CREATE TABLE users (
  id         bigint PRIMARY KEY,
  name       text NOT NULL,
  amount     numeric(20,4),
  updated_at timestamptz,
  "rowKinds" text
) WITH (
  use_cdc = true,
  lakesoul_cdc_change_column = 'rowKinds',
  hash_bucket_num = 4
);
```

目标表的非 CDC 列要和源列同名、类型兼容，主键要和源表一致，且不能是 range partition 表。SQL 创建订阅时目标表必须为空。CDC 列不需要出现在源表中，由订阅端填写。

## 创建订阅

```sql
CREATE SUBSCRIPTION users_from_app
CONNECTION 'postgresql://user:pass@source:5432/app'
PUBLICATION app_pub
WITH (
  source_table         = 'public.users',
  target_table         = 'users',
  snapshot             = 'managed',
  slot_name            = 'users_from_app',
  primary_key          = 'id',
  commit_rows          = 50000,
  commit_interval_ms   = 1000,
  snapshot_batch_rows  = 50000,
  snapshot_batch_bytes = 67108864,
  snapshot_parallelism = 4,
  snapshot_memory_bytes = 1073741824
);
```

| 选项 | 默认值 | 含义 |
| --- | --- | --- |
| `source_table` | 必填 | 源表，`public.users` 或 `users` |
| `target_table` | 必填 | 当前 database 下的裸表名 |
| `snapshot` | `auto` | `auto`、`managed` 会拷贝初始表；`none` 跳过拷贝，从 slot 当前位点开始 |
| `slot_name` | 订阅名 | replication slot 名称，不能已存在 |
| `primary_key` | 自动推断 | 源/目标主键列；省略时从目标表推断 |
| `commit_rows` | `50000` | 流式批次达到多少个不同 key 后提交 |
| `commit_interval_ms` | `1000` | 流式批次最长等待时间 |
| `snapshot_batch_rows` | `50000` | 快照文件批次行数 |
| `snapshot_batch_bytes` | `67108864` | 快照文件批次字节数 |
| `snapshot_parallelism` | `4` | 并行 COPY reader 数；所有 reader 导入同一个 exported snapshot |
| `snapshot_memory_bytes` | `1073741824` | 快照 writer 共享的进程级 DataFusion 内存池；`0` 表示不限制 |

`CONNECTION` 是普通的 libpq 风格连接串；订阅端会在内部补上 replication 参数。

初始快照按 CTID 把源表切成多个范围，每个范围一个 COPY reader，每个 reader 自己写文件。所有 reader 导入同一个 exported snapshot，因此并行拷贝仍然只有一个一致点。writer 共享 `snapshot_memory_bytes`，而不是各自占用一份预算。目标表在所有 reader 完成、一次 commit 发布完整快照之前保持为空。

`CREATE SUBSCRIPTION` 会先校验源库和目标表，再写元数据。命令在本地订阅进程启动后返回，初始快照在后台继续。SQL 创建的订阅在任何 snapshot 模式下都要求目标表为空；空目标上的 `snapshot = none` 从 slot 当前位点开始，只应用后续变更。

目标 namespace 就是连接使用的 database，目标表固定是 `public.<table>`。

## 查看进度

在元数据 PostgreSQL 中查看订阅状态：

```sql
-- 在元数据库执行，不要连 PostgreSQL 端点。
SELECT subscription_id, state, last_error
FROM subscription.subscriptions;

SELECT subscription_id, snapshot_state, snapshot_lsn, data_lsn, state
FROM subscription.tables;
```

`state` 会经历 `starting`、`snapshotting`、`streaming`、`stopped`、`disabled` 和 `error`。`snapshot_state` 为 `pending`、`copying`、`done` 或 `failed`。

订阅进程会写每个订阅自己的日志。启动端点时用 `--subscription-log-dir` 配置日志目录；未设置时使用 `LAKESOUL_LOG_DIR`，再没有就使用系统临时目录下的目录。

## 生命周期

```sql
ALTER SUBSCRIPTION users_from_app DISABLE;
ALTER SUBSCRIPTION users_from_app ENABLE;

DROP SUBSCRIPTION users_from_app;
DROP SUBSCRIPTION IF EXISTS users_from_app;
```

- `DISABLE` 停止订阅进程，但保留 replication slot。长时间暂停会继续在源库保留 WAL。
- `ENABLE` 从持久化 watermark 继续运行。
- `DROP` 停止进程、删除源库 replication slot，并删除订阅元数据。
- SQL 创建且处于启用状态的订阅会在端点重启后自动拉起。
- 子进程异常退出会记录为 `error`，不会无限重试；执行 `ENABLE` 或重启端点会再次启动。

用 CLI 创建的订阅不会被端点自动拉起，因此手动启动的订阅进程不会被重复启动。

## 工作方式

1. `CREATE SUBSCRIPTION` 校验源 publication、源表、`REPLICA IDENTITY FULL`、主键和列类型，再校验目标 CDC 表。
2. 把定义和零进度写入 `subscription.subscriptions`、`subscription.tables` 和 `subscription.progress`。
3. 端点启动本地子进程：

   ```text
   postgres-lakesoul --subscribe --subscription-id <name>
   ```

4. 对于 `auto` 和 `managed`，子进程用导出的快照打开 slot，按 CTID 把表切成多个范围，每个范围一个 COPY reader。所有 reader 导入同一个快照并按批次写文件，writer 共享一个有界内存池；全部完成后，在一致点用一次原子 LakeSoul commit 发布所有快照文件。读请求因此只会看到空表或完整快照。
5. 切换完成后，变更按源事务累积；一个批次用一次 LakeSoul commit 写入和发布。commit id 记录源 LSN，从更早的 slot 位点重放时会跳过已应用的数据。
6. 元数据进度还会记录没有触及订阅表的事务。

## 源类型

订阅支持以下 PostgreSQL 源类型：

`boolean`、`smallint`、`integer`、`bigint`、`real`、`double precision`、`numeric(p,s)`、`text`、`varchar`、`char`、`name`、`bytea`、`date`、`time`、`timestamp`、`timestamptz`、`uuid`、`json` 和 `jsonb`。

目标列使用对应的 LakeSoul/Arrow 类型和相同列名；CDC 变更列是目标表额外的一列。

## CLI 模式

需要手动运行时，也可以直接启动订阅进程：

```bash
postgres-lakesoul --subscribe \
  --source-url 'postgresql://user:pass@source:5432/app' \
  --publication app_pub \
  --slot users_from_app \
  --source-table public.users \
  --target-table users \
  --target-namespace default \
  --primary-key id \
  --snapshot managed \
  --snapshot-parallelism 4 \
  --snapshot-memory-bytes 1073741824
```

`--snapshot` 支持 `auto`、`managed` 和 `none`。`--snapshot-parallelism` 设置 COPY reader 数，`--snapshot-memory-bytes` 限制 writer 共享的 DataFusion 内存池（`0` 表示不限制）。`--dry-run` 只校验源库、目标表和类型，不创建 slot，也不写订阅元数据。CLI 创建的订阅会记录在元数据库中，但不会由端点自动拉起。

## 安全说明

SQL 创建的订阅会把源连接串（包含凭据）写入元数据 PostgreSQL，以便 supervisor 重启子进程。请保护元数据库及其备份，并为源库使用最小权限的复制用户。

## 下一步

- [快速开始：PostgreSQL 端点](../../get-started/quickstart-postgres.md)
- [DDL](ddl.md)
- [写入与 Upsert](insert-and-upsert.md)
- [运维](operations.md)
- [元数据存储](../../operations/metadata-postgres.md)
