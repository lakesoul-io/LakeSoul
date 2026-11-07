<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

<img src='https://github.com/lakesoul-io/artwork/blob/main/horizontal/color/LakeSoul_Horizontal_Color.svg' alt="LakeSoul" height='200'>

<img src='https://github.com/lfai/artwork/blob/main/other/lfaidata-project-badge/sandbox/color/lfaidata-project-badge-sandbox-color.svg' alt="LF AI & Data Sandbox Project" height='180'>

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![PG wire](https://img.shields.io/badge/PG%20wire-DDL%20%2B%20INSERT%20%2B%20query-green.svg)](#postgres-%E5%8E%9F%E7%94%9F)
[![pgvector](https://img.shields.io/badge/pgvector-compatible%20SQL-purple.svg)](#%E5%90%91%E9%87%8F%E5%8E%9F%E7%94%9F)
[![Python](https://img.shields.io/badge/Python-3.10%2B-yellow.svg)](#python-%E5%8E%9F%E7%94%9F)
[![Daft](https://img.shields.io/badge/Daft-multimodal-orange.svg)](#python-%E5%8E%9F%E7%94%9F)
[![Release](https://img.shields.io/github/v/release/lakesoul-io/LakeSoul?include_prereleases&sort=semver)](https://github.com/lakesoul-io/LakeSoul/releases)

[English](README.md)

# LakeSoul

**Postgres 原生。Vector 原生。Python 原生。**<br>
一个开放的湖仓引擎 —— 共享 catalog、ACID upsert、SQL 查询、增量计算、向量检索与 Python SDK，纯 Rust 实现，运行在你的对象存储之上。

把已经在用的工具接上来 —— `psql`、Python、兼容 pgvector 的 SQL —— 底下是一个增量湖仓。

[快速开始](#快速开始) · [架构](#架构) · [文档](https://lakesoul-io.github.io/zh-Hans/docs/engine/overview)

## Postgres 原生

把任意 PostgreSQL 客户端接到 LakeSoul。`CREATE TABLE`、`INSERT ... ON CONFLICT` 和查询都在湖仓引擎里执行，直接落在对象存储上 —— 不是 sidecar，也不走 CDC 管道。

```sql
CREATE TABLE orders (
  id     bigint primary key,
  region text,
  amount numeric(20,4)
) WITH (hash_bucket_num = 8);

INSERT INTO orders VALUES (1, 'APAC', 42.00)
ON CONFLICT (id) DO UPDATE
SET region = EXCLUDED.region,
    amount = EXCLUDED.amount;

SELECT region, sum(amount) FROM orders GROUP BY 1;
```

wire protocol、扩展查询协议、`pg_catalog` 和 `psql \d` 都可用。但这不是 drop-in 的 PostgreSQL：`UPDATE`、`DELETE`、`COPY`、`RETURNING` 以及非 `READ COMMITTED` 隔离级别会返回 `0A000`，写入必须在 autocommit 下执行。

### PostgreSQL 订阅

同一个端点也可以订阅源 PostgreSQL，把变更持续写入 LakeSoul CDC 表。用 SQL 定义订阅；端点会校验 publication 和目标表，执行受管初始快照，并拉起受监管的本地订阅进程。

```sql
CREATE SUBSCRIPTION users_from_app
CONNECTION 'postgresql://user:pass@source:5432/app'
PUBLICATION app_pub
WITH (
  source_table     = 'public.users',
  target_table     = 'users',
  snapshot         = 'managed',
  auto_create      = true,
  schema_evolution = 'auto'
);
```

源库需要开启 logical WAL、创建 publication，并设置 `REPLICA IDENTITY FULL`；目标端是可以按源 schema 自动创建或预先创建的 LakeSoul CDC 表；managed 快照要求目标表为空。源端 schema 变化时，订阅端会自动追加 nullable 列并安全放宽类型。初始拷贝按 CTID 范围并行执行（`snapshot_parallelism`、`snapshot_memory_bytes`）。用 `ALTER SUBSCRIPTION ... DISABLE/ENABLE` 和 `DROP SUBSCRIPTION` 管理进程。一个订阅可以映射多张源表；每张表独立 snapshot 和 commit。完整配置和 SQL 参考见 [PostgreSQL 订阅](https://lakesoul-io.github.io/zh-Hans/docs/engine/postgres/subscription)。

## 高效的 SQL 增量计算

开启增量 SQL 后，从 `psql` 发出的一条 `INSERT ... SELECT` 会以增量方式更新目标表：每次执行只读取游标之后的提交，只写入发生变化的行，结果与全量重算一致。

```sql
-- 目标表是带 CDC 变更列 "rowKinds" 的主键表
-- 这个开关为当前会话开启增量 SQL
SET lakesoul.ivm.enabled = on;

INSERT INTO daily_revenue
SELECT date_trunc('day', ts) AS day, region, sum(amount)
FROM orders
GROUP BY 1, 2;
```

- SQL 级增量，而不只是存储层的增量读写
- 可安全重放：每个消费窗口都有记录，重试会跳过已写入的数据

## 向量原生

兼容 pgvector 的 SQL，查的是和数据放在一起的 ANN 索引。新行写入会更新索引，不重建，也不需要再运维一套系统。

```sql
-- documents.embedding 是 vector(768)
SELECT id, body, embedding <-> '[0.12, 0.98, ...]' AS distance
FROM documents
WHERE tenant_id = 'acme' AND category = 'contract'
ORDER BY embedding <-> '[0.12, 0.98, ...]'
LIMIT 10;
```

- 对象存储上的 IVF + RaBitQ 索引，写入时自动维护
- 结构化过滤与 ANN 在同一个执行计划里
- 同一个引擎里还提供 BM25 全文检索（`text_score`）

## Python 原生

Python SDK 通过 Rust IO 层直接读取对象存储。没有网关，没有 JDBC 跳转 —— Daft 和 Ray 把读取分片到多个 worker，PyTorch 拿到数据集，Pandas 和 DuckDB 消费 Arrow。

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
scan = catalog.scan("documents").select("id", "body")
table = scan.to_arrow_table()   # 或 .to_daft()、.to_ray()、.to_torch()
```

BI 看板查询的那张表，就是训练任务读取的那张表。视频帧、机器人 episode 和 tensor 以 blob 形式与元数据同置。

## 快速开始

### 方式 A —— PostgreSQL 端点

```bash
# 1. 元数据库
docker run -d --name lakesoul-pg -p 5432:5432 \
  -e POSTGRES_USER=lakesoul_test -e POSTGRES_PASSWORD=lakesoul_test \
  -e POSTGRES_DB=lakesoul_test postgres:14.5
docker cp script/meta_init.sql lakesoul-pg:/
docker exec -i lakesoul-pg sh -c \
  "PGPASSWORD=lakesoul_test psql -U lakesoul_test -f meta_init.sql"

# 2. PostgreSQL 端点：database "default" 即 LakeSoul namespace
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix file:///tmp/lakesoul/warehouse

# 3. 用任意 PostgreSQL 客户端连接
psql "postgresql://lakesoul_test:lakesoul_test@127.0.0.1:5433/default"
```

```sql
CREATE TABLE t (id bigint primary key, v text);
INSERT INTO t VALUES (1, 'hello');
SELECT * FROM t;
```

### 方式 B —— Python

```bash
pip install lakesoul
```

```python
from lakesoul import LakeSoulCatalog

catalog = LakeSoulCatalog.from_env()
print(catalog.scan("t").to_arrow_table().num_rows)
```

对象存储配置与完整的本地环境搭建见[文档](https://lakesoul-io.github.io/zh-Hans/docs/get-started/quickstart-postgres)。

## 架构

```
┌──────────────────────────────────────────────────────────────┐
│  psql · JDBC · BI 工具       Python SDK · Daft · Ray · Torch │
└───────────────┬──────────────────────────┬───────────────────┘
                │ PG wire                  │ native IO / Arrow
┌───────────────▼──────────────────────────▼───────────────────┐
│                        LakeSoul 引擎                         │
│  PostgreSQL 前端   ·   增量 SQL                              │
│  向量 + 全文索引   ·   向量化执行                            │
└───────────────────────────────┬──────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────┐
│  对象存储（S3 / OSS / HDFS）  +  PostgreSQL 元数据           │
└──────────────────────────────────────────────────────────────┘
```

- **PostgreSQL 前端** —— wire protocol、`pg_catalog`、DDL 与 `INSERT`，由引擎执行，而不是由 Postgres 扩展执行
- **增量 SQL** —— 每次执行消费表自己的 commit log，只写变化的行，可安全重放
- **向量与全文索引** —— IVF + RaBitQ 与 Tantivy BM25，写入时自动维护
- **Native IO** —— 对象存储上的 Parquet、Vortex、Vortex Compact，主键表走 merge-on-read

## 与其他方案对比

表格式只描述文件怎么摆；数据库自带引擎，但存储也归它。LakeSoul 两者都不是：catalog、存储格式、执行引擎和索引都在同一套代码里，而数据仍然留在你的对象存储，文件保持开放。

| | Postgres 扩展 | Postgres + 列存镜像 | 托管 Postgres | LakeSoul |
| --- | --- | --- | --- | --- |
| **数据在哪** | 你的 Postgres 表，或经 FDW 访问 Iceberg | Postgres WAL 加一份列存副本 | 厂商的存储 | **你的对象存储，开放文件** |
| **在哪执行** | Postgres 进程内 | Postgres 与镜像 | 厂商的服务 | **湖仓引擎** |
| **写入模型** | Postgres MVCC | 逻辑解码写入镜像 | Postgres MVCC | **主键 upsert + merge-on-read** |
| **增量计算** | — | — | — | **基于 commit log 的增量 SQL** |
| **向量索引** | 堆表上的 pgvector | — | pgvector | **湖上索引，写入时维护** |
| **可自托管** | 是 | 是 | 否 | **是，Apache 2.0** |

这里比的是架构类别，不是厂商评分：对应的例子是 pg_duckdb 与 pg_lake、pg_mooncake、Lakebase。带日期和来源的详细对比放在文档里。

## 接入你的技术栈

- **PostgreSQL 客户端** —— `psql`、pgAdmin、DBeaver、JDBC、libpq
- **Python** —— Pandas、PyArrow、DuckDB、Daft、Ray、PyTorch、Hugging Face Datasets
- **引擎** —— Spark 3.5、Flink 1.20、Presto 0.296
- **BI** —— Superset、Metabase，以及任何支持 PostgreSQL 的工具

## 社区

- [Discord](https://discord.gg/WJrHKq4BPf)：提问与设计讨论
- [GitHub Issues](https://github.com/lakesoul-io/LakeSoul/issues)：缺陷与需求；想开始贡献可以找 `good first issue`
- [社区准则](dev/community/guideline-cn.md)
- 邮件列表：[lakesoul-technical-discuss@lists.lfaidata.foundation](mailto:lakesoul-technical-discuss@lists.lfaidata.foundation)
- 路线图：[ROADMAP.md](ROADMAP.md)

LakeSoul 是 [LF AI & Data](https://lfaidata.foundation/) 的 sandbox 项目。

## 开源协议

Apache License 2.0，见 [LICENSE](LICENSE)。
