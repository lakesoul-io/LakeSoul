---
sidebar_position: 2
---

# 快速开始：PostgreSQL 端点

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

这一页启动 LakeSoul 引擎，通过 PostgreSQL wire protocol 暴露出来，并用 `psql` 跑第一条 `CREATE TABLE` / `INSERT` / `SELECT`。

## 前置条件

- 一个 PostgreSQL 14+ 数据库，作为 LakeSoul 元数据存储。快速开始用 Docker。
- Rust 工具链和原生编译依赖（`protoc`、Clang、`flatc`）。如果 `cargo build` 失败，见[开发环境](../development/environment.md)。
- 连接机器上有 `psql`。

## 1. 启动元数据库

LakeSoul 把表和提交元数据存在 PostgreSQL 里。这个数据库**不是**用户连接的端点，而是元数据存储。

```bash
docker run -d --name lakesoul-pg -p 5432:5432 \
  -e POSTGRES_USER=lakesoul_test \
  -e POSTGRES_PASSWORD=lakesoul_test \
  -e POSTGRES_DB=lakesoul_test \
  postgres:14.5

docker cp script/meta_init.sql lakesoul-pg:/
docker exec -i lakesoul-pg sh -c \
  "PGPASSWORD=lakesoul_test psql -U lakesoul_test -f meta_init.sql"
```

schema 会创建 `default` namespace。如果你已经有 PostgreSQL，直接对它执行 `script/meta_init.sql`。

## 2. 启动端点

在仓库根目录执行：

```bash
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix file:///tmp/lakesoul/warehouse
```

端点监听 `127.0.0.1:5433`，默认连接 `127.0.0.1:5432` 上的 `lakesoul_test` / `lakesoul_test`。元数据库在别处时用环境变量覆盖：

```bash
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
```

`CREATE TABLE` 需要 `--warehouse-prefix`：它是新表数据的写入位置，可以是本地路径（`file:///...`），也可以是对象存储前缀（`s3://bucket/warehouse`）。

## 3. 连接

在 PostgreSQL 里，database 映射到一个 LakeSoul namespace。快速开始使用 `meta_init.sql` 创建的 `default`：

```bash
psql "postgresql://lakesoul_test:lakesoul_test@127.0.0.1:5433/default"
```

任何 PostgreSQL 客户端都可以：`psql`、JDBC、`pgx`、DBeaver 或 BI 工具。

## 4. 建表、写入、查询

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

`hash_bucket_num` 控制主键表的哈希分区数。`numeric` 必须显式写出精度和小数位；不带参数的 `numeric` 会被拒绝。

`psql \d orders` 和 `pg_catalog` 查询也都可以用。

## 5. 下一步

- [DDL](../engine/postgres/ddl.md) —— 表选项、类型、分区，以及会被拒绝的子句。
- [写入与 Upsert](../engine/postgres/insert-and-upsert.md) —— `ON CONFLICT` 语义、重复键和写入路径。
- [增量 SQL](../engine/incremental-sql/getting-started.md) —— 用 `INSERT ... SELECT` 基于 commit log 维护目标表。
- [PostgreSQL 订阅](../engine/postgres/subscription.md) —— 把源 PostgreSQL publication 复制到 LakeSoul CDC 表。
- [向量检索](../engine/vector-search/overview.md) —— 湖上 ANN 索引上的 兼容 pgvector 的 SQL。

## 对象存储

当 warehouse 前缀在 S3 或兼容 S3 的存储上时，传入对象存储凭证：

```bash
cargo run -p postgres-lakesoul -- --port 5433 \
  --warehouse-prefix s3://lakesoul-test-bucket/warehouse \
  --endpoint http://127.0.0.1:9000 \
  --s3-bucket lakesoul-test-bucket \
  --s3-access-key rustfsadmin \
  --s3-secret-key rustfsadmin
```

需要 virtual-hosted 请求的存储加 `--s3-virtual-host-style`。

## 常见问题

- **`FATAL: database ... does not exist`** —— 连接一个已存在的 namespace。快速开始用 `default`；其他 namespace 通过管理 API 或 Python catalog 创建。
- **`CREATE TABLE needs a configured warehouse`** —— 启动时加 `--warehouse-prefix`。
- **Connection refused** —— 检查 `--bind` 和 `--port`；默认绑定 `127.0.0.1`。
