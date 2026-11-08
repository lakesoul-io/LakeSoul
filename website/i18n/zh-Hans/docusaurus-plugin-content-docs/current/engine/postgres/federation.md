# PostgreSQL 外部表（ADBC）

LakeSoul 的 PostgreSQL 端点可以把其他数据库中的表映射成当前 LakeSoul namespace 下的只读外部表。查询通过 [ADBC](https://arrow.apache.org/adbc/) driver 读取远端表，并可以与 LakeSoul 本地表在同一个 DataFusion 计划中 join。

端点把 server、user mapping 和 foreign table 保存在元数据 PostgreSQL 的 `federation` schema。查询计划只携带 server key，用户名密码保留在元数据中，由 coordinator 或分布式 worker 解析。projection、filter 和 limit 的下推使用 server 上记录的 SQL dialect。

## 安装 ADBC driver

使用 [`dbc`](https://docs.columnar.tech/dbc/) 为每种源数据库安装 driver：

```bash
dbc install postgresql
dbc install mysql
dbc install sqlite
```

driver 会安装到用户 driver 目录，并以名称加载。也可以通过 `ADBC_DRIVER_PATH` 指定其他位置。可能执行 foreign scan 的每个 `lakesoul-worker` 都需要安装相同的 driver。

## 创建 server 和 user mapping

`adbc_fdw` 是通用 wrapper；`postgres_fdw` 作为兼容别名保留，默认使用 `postgresql` ADBC driver。

```sql
CREATE EXTENSION adbc_fdw;

CREATE SERVER app_source
FOREIGN DATA WRAPPER adbc_fdw
OPTIONS (
  driver 'postgresql',
  uri 'postgresql://postgres.example.com:5432/app'
);

CREATE USER MAPPING FOR CURRENT_USER
SERVER app_source
OPTIONS (
  user 'lakesoul_reader',
  password '...'
);
```

dialect 在创建 server 时确定：会根据 ADBC driver 自动推导，也可以通过 `dialect` 选项显式指定：

| Driver | 默认 dialect |
|---|---|
| `postgresql` | `postgres` |
| `mysql` | `mysql` |
| `sqlite` | `sqlite` |
| `duckdb` | `duckdb` |
| `bigquery` | `bigquery` |
| `snowflake` | `snowflake` |
| `trino` | `generic` |
| 其他 | 必须提供 `dialect` |

支持的 `dialect` 值：`postgres`、`mysql`、`sqlite`、`duckdb`、`bigquery`、`snowflake`、`generic`。

```sql
CREATE SERVER app_mysql
FOREIGN DATA WRAPPER adbc_fdw
OPTIONS (
  driver 'mysql',
  uri 'mysql://mysql.example.com:3306/app',
  dialect 'mysql'
);
```

`uri` 是通用的连接配置方式。PostgreSQL server 也可以使用 `host`、`port`、`dbname`，以及 user mapping 中的 `user`/`password`。FDW 保留选项之外的其他选项会作为 ADBC 原生 database option 传给 driver。

密码保存在元数据 PostgreSQL 中。请使用权限最小的专用 reader，并保护元数据库；凭据加密后续再引入。

## 创建外部表

```sql
CREATE FOREIGN TABLE public.remote_users (
  id      bigint,
  name    text,
  amount  numeric(20,4)
)
SERVER app_source
OPTIONS (
  schema_name 'public',
  table_name  'users'
);
```

列定义使用和 `CREATE TABLE` 相同的 PostgreSQL → Arrow 类型映射，列名和兼容类型必须与远端表一致。之后可以直接查询，也可以和 LakeSoul 表 join：

```sql
SELECT l.id, l.name, r.amount
FROM local_users l
JOIN remote_users r USING (id);
```

## 清理

```sql
DROP FOREIGN TABLE public.remote_users;
DROP USER MAPPING FOR CURRENT_USER SERVER app_source;
DROP SERVER app_source CASCADE;
```

有 foreign table 依赖时，`DROP SERVER` 不带 `CASCADE` 会被拒绝。`DROP FOREIGN TABLE` 只删除本地元数据，不会删除远端表。

## 分布式 worker

一个 foreign scan 目前是一个 partition。distributed planner 把 stage 路由到 worker 时，worker 需要相同的 server 定义。启动 `lakesoul-worker` 时连接元数据 PostgreSQL：

```bash
lakesoul-worker \
  --warehouse-prefix s3://bucket/warehouse \
  --port 50051
```

并设置 `LAKESOUL_PG_URL`、`LAKESOUL_PG_USERNAME`、`LAKESOUL_PG_PASSWORD`。worker 启动时会加载 federation user mapping，并按 server 和 mapping key 注册 ADBC pool（mapping key 是查询用户，共享 mapping 则为 `PUBLIC`）。

coordinator 使用 server 上保存的 dialect 生成远端 SQL；worker 只执行序列化后的 SQL。ADBC scan 目前不会拆成多个 task。如果整个计划的所有 stage 都只有一个 task，查询会在 coordinator 上执行，因为 `datafusion-distributed` 会消除两个单 task stage 之间的网络边界。如果 foreign scan 属于包含多 task stage 的更大计划，例如与需要多个 task 扫描的 LakeSoul 表 join，scan stage 会被发送到 worker，worker 根据自己的 federation 注册表解析 server 和 user mapping key。把单张 foreign table 拆成多个并行 scan task 是后续优化。

## 当前限制

- 只读：foreign table 上的 `INSERT`、`UPDATE`、`DELETE` 尚未实现。
- `IMPORT FOREIGN SCHEMA` 尚未实现，需要显式创建每张 foreign table。
- 支持的 DDL：`CREATE/DROP SERVER`、`CREATE/DROP USER MAPPING`、`CREATE/DROP FOREIGN TABLE`、`CREATE/DROP EXTENSION adbc_fdw`（或 `postgres_fdw`）。
- 暂不支持 `ALTER SERVER`、`ALTER FOREIGN TABLE`、`GRANT` 和 FDW catalog view。
- foreign table 声明的 schema 就是类型契约。源库中无法用端点的 Arrow/PostgreSQL 类型表示的类型，需要显式 cast 或声明兼容列。
- 同一个查询中的多张 foreign table 使用各自连接，不共享远端事务快照。
