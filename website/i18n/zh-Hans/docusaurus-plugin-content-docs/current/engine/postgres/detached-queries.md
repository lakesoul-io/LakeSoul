---
sidebar_position: 8
sidebar_label: 后台查询
---

# 后台查询

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

全量 ETL 或大表聚合经常比提交它的客户端连接活得更久。PostgreSQL 的异步命令
API 帮不上忙：`PQsendQuery` 只是让客户端 I/O 不阻塞，结果流仍绑定在 `PGconn`
上，PostgreSQL 没有可以给另一条连接使用的 query handle。

`postgres-lakesoul` 在 SQL 层补上了这一层。**后台查询**是一个脚本：只提交一次，
返回持久的 query id，之后独立于连接运行；任意新连接都可以轮询、读取结果或取消。

## 提交

```sql
SELECT lakesoul_submit(
  $job$
    SET lakesoul.ivm.enabled = on;
    INSERT INTO silver.orders
      SELECT * FROM bronze.orders WHERE dt = '2026-11-09';
    SELECT count(*) AS written FROM silver.orders;
  $job$,
  'orders-2026-11-09',   -- 可选幂等键
  7200000,               -- 可选超时，单位毫秒
  true                   -- 服务重启后是否重跑，默认 true
) AS query_id;
```

一个脚本可以包含多条 `SELECT`、`INSERT ... SELECT` 和 `SET`。`SET` 对同一个
job 会话中它之后的语句生效，和 psql 脚本一致：支持 `SET search_path`、
`SET TIME ZONE`、`SET statement_timeout`、`SET lakesoul.ivm.enabled` 以及普通
DataFusion 选项。事务控制、DDL、`SET LOCAL`/`SET GLOBAL` 和 `SET ROLE` 会被拒绝。

第二个参数是幂等键。同一个键重复提交会返回已有 query id，而不是再跑一遍；如果
提交响应在断线中丢了，客户端也可以用它找回作业。上一个 server 进程用过的 key
不能复用，需要换一个新的 key。

提交本身是控制动作，可以出现在事务块里（psycopg、SQLAlchemy 默认会隐式开启事
务），但作业不会随该事务回滚。

## 轮询

```sql
SELECT query_id, state, attempt, rows_produced, result_sets,
       error_sqlstate, error_message
FROM lakesoul_query_status('7c9e8f3a-...');

SELECT * FROM lakesoul_queries() WHERE state = 'running';

-- 等待终态；超时不会取消作业
SELECT lakesoul_wait_query('7c9e8f3a-...', 60000);
```

状态包括 `queued`、`running`、`succeeded`、`failed`、`canceled` 和 `lost`。
运行中的作业通过**从头重跑**来跨 server 重启存活：server 启动时重新排队自己未
完成的作业，runner 领取时递增 `attempt`，超过 `retry_on_restart` 次数后标记
`lost`。已完成的结果不需要重跑：Parquet 文件和 schema 记录在元数据库中，重启后
在 24 小时保留期内仍可读取。

因为重跑是从头开始，如果上一次尝试已经提交，`INSERT ... SELECT` 是
**at-least-once**。对不能重复写入的目标，提交时传 `retry_on_restart => false`
（或第四个位置参数 `false`），或者改用 upsert、IVM、分区覆盖等幂等写法。

query id 作为客户端参数时，用 `lakesoul_queries()` 过滤：

```sql
SELECT state FROM lakesoul_queries() WHERE query_id = $1;
```

`lakesoul_query_status(...)`、`lakesoul_query_result(...)` 等结果函数需要字面量
query id，因为它们在规划阶段就要确定结果 schema；UUID 直接内联是安全的。

## 结果

`INSERT ... SELECT` 走正常写入路径，整条语句一次 commit，不产生行结果；状态里给
出最后一条 command tag 和写入行数。

脚本中的每个 `SELECT` 产生一个独立结果集，每个结果集是一个 Parquet 文件，路径为
`<warehouse>/_lakesoul_jobs/<query_id>/attempt-<n>/stmt-<i>/part-0.parquet`。
读取**最后一个**结果集：

```sql
SELECT * FROM lakesoul_query_result('7c9e8f3a-...');
SELECT * FROM lakesoul_query_result('7c9e8f3a-...', 100000, 10000);
```

第二、三个参数是 offset 和 limit，默认最多 1,000,000 行。

包含多个 `SELECT` 的脚本可以逐个读取：

```sql
SELECT * FROM lakesoul_query_result_sets('7c9e8f3a-...');
-- set_index | statement_index | rows | bytes | expires_at

SELECT * FROM lakesoul_query_result_set('7c9e8f3a-...', 0);
SELECT * FROM lakesoul_query_result_set('7c9e8f3a-...', 1, 100000, 10000);
```

`lakesoul_query_result` 返回最后一个结果集，方便单 SELECT 的常见场景。整个脚本的
spool 默认上限 1 GiB，超过时以 SQLSTATE `54000` 失败，并提示改用 `INSERT ...
SELECT` 写入目标表。结果默认保留 24 小时，作业记录保留 3 天。

硬崩溃可能在 job 目录下留下未完成的 multipart part；已完成的结果文件由保留期清理
任务删除，未完成的 multipart 上传应通过对象存储的 lifecycle 规则中止。

## 取消

```sql
SELECT lakesoul_cancel_query('7c9e8f3a-...');
```

取消和 wire 路径的 `CancelRequest` 一样是协作式的：查询在批次之间检查 token。
流式查询会很快停下；CPU 密集算子会在下次让出时停下。`psql` 的 Ctrl-C 只会取消
当前轮询语句，不会取消后台作业。

## 限制

- 作业使用提交者的身份、database、`search_path`、时区和 IVM 开关；
  `statement_timeout` 不继承，可在脚本里 `SET`。
- 作业内部的 `INSERT` 仍拒绝事务块；作业语句本身是 autocommit。
- 服务端同时运行的作业数默认 4，暂不限制单用户并发。
- 队列在 server 进程内。每个 pg-wire cluster 只负责自己接受的作业；如果监听地址
  在重启间可能变化，用 `--job-cluster-id` 固定 cluster 身份。已完成的结果可以被
  共享同一 warehouse 的任意进程读取。

## 配置

| 参数 | 环境变量 | 默认值 |
| --- | --- | --- |
| `--max-jobs` | `LAKESOUL_PG_MAX_JOBS` | `4`（`0` 表示不限） |
| `--job-result-max-mib` | `LAKESOUL_PG_JOB_RESULT_MAX_MIB` | `1024` |
| `--job-result-ttl-hours` | `LAKESOUL_PG_JOB_RESULT_TTL_HOURS` | `24` |
| `--job-record-ttl-hours` | `LAKESOUL_PG_JOB_RECORD_TTL_HOURS` | `72` |
| `--job-cluster-id` | `LAKESOUL_PG_JOB_CLUSTER_ID` | `<bind>:<port>` |
