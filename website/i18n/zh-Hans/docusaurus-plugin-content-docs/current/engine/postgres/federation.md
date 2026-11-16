# PostgreSQL 外部表（ADBC）

LakeSoul 的 PostgreSQL 端点可以把其他数据库中的表映射成当前 LakeSoul namespace 下的外部表。查询通过 [ADBC](https://arrow.apache.org/adbc/) driver 读取远端表，并可以与 LakeSoul 本地表在同一个 DataFusion 计划中 join。`INSERT INTO` 通过同一个 driver 写入。

端点把 server、user mapping 和 foreign table 保存在元数据 PostgreSQL 的 `federation` schema。对于 ADBC 表，查询计划只携带 server key，用户名密码保留在元数据中，由 coordinator 或分布式 worker 解析。外部文件 scan plan 会携带 S3 store 配置（包括凭据），worker 在执行时注册 store；详见下文“S3 外部文件”一节。projection、filter 和 limit 的下推使用 server 上记录的 SQL dialect。

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

## 写入外部表

写入只支持追加。为了让重跑和未来 IVM 的重复投递保持幂等，需要在外部表上声明远端 key 和冲突策略：

```sql
CREATE FOREIGN TABLE public.remote_users (
  id      bigint,
  name    text,
  amount  numeric(20,4)
)
SERVER app_source
OPTIONS (
  schema_name 'public',
  table_name  'users',
  primary_key 'id',
  on_conflict 'update'       -- 或 'nothing'
);
```

之后普通的 `INSERT` 会在数据 worker 上执行：

```sql
INSERT INTO remote_users VALUES (1, 'alice', 10.0), (2, 'bob', 20.0);
```

`on_conflict 'update'` 在 PostgreSQL/SQLite 上生成 `ON CONFLICT (id) DO UPDATE`，在 MySQL 上生成 `ON DUPLICATE KEY UPDATE`；`on_conflict 'nothing'` 会忽略已存在的 key。重复执行同一语句会收敛，不会产生重复行。`write_batch_size`（默认 1024）控制单条生成 SQL 的行数。使用 `on_conflict 'update'` 时，源数据同一条语句内不能重复出现同一个冲突 key；PostgreSQL 会按“同一行被影响两次”拒绝，因此需要先在源端去重。

写入采用 at-least-once + 每 batch 提交，没有跨 worker 的全局事务。某个 task 失败时，已经提交的 batch 会保留；只有在目标表存在声明的唯一 key 且冲突策略为 `update` 或 `nothing` 时，重跑才是安全的。普通 append 不提供幂等保证。

写 stage 在 worker 上执行，而不是 coordinator：输入会做 hash repartition，行数汇总回到 coordinator。配置了 worker 时至少使用两个写 task，单 worker 部署也一样；没有 distributed resolver 时在本地执行。

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

并设置 `LAKESOUL_PG_URL`、`LAKESOUL_PG_USERNAME`、`LAKESOUL_PG_PASSWORD`。worker 启动时会加载 federation user mapping，并按 server 和 mapping key 注册 ADBC pool（mapping key 是查询用户，共享 mapping 则为 `PUBLIC`）。请在启动 worker 之前创建 server 和 user mapping，或者在变更后重启 worker；worker 不会轮询注册表。

coordinator 使用 server 上保存的 dialect 生成远端 SQL；worker 只执行序列化后的 SQL。一个 foreign scan 是一个 partition，因此如果整个计划的所有 stage 都只有一个 task，查询会在 coordinator 上执行，因为 `datafusion-distributed` 会消除两个单 task stage 之间的网络边界。如果 foreign scan 属于包含多 task stage 的更大计划，例如与需要多个 task 扫描的 LakeSoul 表 join，scan stage 会被发送到 worker，worker 根据自己的 federation 注册表解析 server 和 user mapping key。把单张 foreign table 拆成多个并行 scan task 是后续优化。

外部表写入的形态相反：写 stage 会做 hash repartition，并且在配置了 worker 时始终在 worker 上执行。每个 task 独立提交自己的 batch，没有跨 task 的全局事务。

## 当前限制

- 只实现了 `INSERT`；foreign table 上的 `UPDATE`、`DELETE` 尚未实现。
- 不支持 `INSERT ... ON CONFLICT`、`INSERT OVERWRITE` 和 `RETURNING`；请在 foreign table 上配置 `primary_key` 和 `on_conflict`。
- 写入是 at-least-once + 每 batch 提交，没有跨 worker task 的全局事务；失败语句可能留下已提交的 batch。幂等需要唯一 key 和 `on_conflict`。
- SQLite 同时只允许一个 writer；并行写 task 可能报 `database is locked`。需要并行写入时请使用服务端数据库。
- `IMPORT FOREIGN SCHEMA` 尚未实现，需要显式创建每张 foreign table。
- 支持的 DDL：`CREATE/DROP SERVER`、`CREATE/DROP USER MAPPING`、`CREATE/DROP FOREIGN TABLE`、`CREATE/DROP EXTENSION adbc_fdw`（或 `postgres_fdw`）。
- 暂不支持 `ALTER SERVER`、`ALTER FOREIGN TABLE`、`GRANT` 和 FDW catalog view。
- foreign table 声明的 schema 就是类型契约。源库中无法用端点的 Arrow/PostgreSQL 类型表示的类型，需要显式 cast 或声明兼容列。
- 同一个查询中的多张 foreign table 使用各自连接，不共享远端事务快照。

## S3 外部文件（`external_file_wrapper`）

`external_file_wrapper` 使用 DataFusion `ListingTable` 把 S3 前缀暴露为外表。
表的 `path` 直接写完整 S3 URL，server 只保存 S3 连接配置：

```sql
CREATE EXTENSION external_file_wrapper;

CREATE SERVER s3_server
FOREIGN DATA WRAPPER external_file_wrapper
OPTIONS (
  endpoint 'http://rustfs:9000',
  access_key_id 'rustfsadmin',
  secret_access_key 'rustfsadmin',
  virtual_host_style 'false'
);

CREATE FOREIGN TABLE public.events (
  id          bigint,
  event_time  timestamp,
  payload     text
)
SERVER s3_server
OPTIONS (
  path   's3://lakehouse/external/events/',
  format 'parquet'
);
```

`path` 必须是完整的 `s3://` 或 `s3a://` URL（`s3a://` 会规范化为 `s3://`）。
以 `/` 结尾表示目录，应指向直接包含数据文件的目录；不以 `/` 结尾表示单个对象。`format` 支持
`parquet`、`csv`、`json`；单文件可以根据扩展名省略 `format`。

该 wrapper 使用 server-level 凭据，不接受 `CREATE USER MAPPING` 的 options。
S3-compatible 服务通常需要 `virtual_host_style 'false'`（path-style 访问）。
支持 `endpoint`、`access_key_id`/`ak`、`secret_access_key`/`sk`、
`session_token`、`region` 和 `allow_http`。凭据保存在元数据 PostgreSQL 中，需要保护该数据库；
生产环境优先使用短期凭据或实例/IAM role。

CSV/JSON 常用选项：

```sql
CREATE FOREIGN TABLE public.csv_events (
  id bigint,
  name text
)
SERVER s3_server
OPTIONS (
  path        's3://lakehouse/external/csv/',
  format      'csv',
  has_header  'true',
  delimiter   ','
);

CREATE FOREIGN TABLE public.json_events (
  id bigint,
  name text
)
SERVER s3_server
OPTIONS (
  path       's3://lakehouse/external/json/',
  format     'json',       -- 默认 newline-delimited JSON
  json_array 'false'
);
```

列定义就是 schema contract，不会从文件自动推断。Parquet 缺失的声明列读为
NULL，兼容类型会做转换；CSV 按列位置解析，因此声明顺序必须和文件一致，
`has_header` 只负责跳过表头；JSON 按字段名匹配。P0 不支持嵌套 Parquet/JSON
类型和 Hive partition 列。

同一目录可以包含多种格式，但一个 foreign table 只能选择一种 `format`/扩展名，
需要按格式分别建表。外表支持 append-only 的 `INSERT INTO`，见下文“向外部文件
append”。

外部文件扫描参与分布式执行。scan plan 会携带 S3 store 配置（包括凭据），
worker 在执行前注册 object store，不需要在启动时加载 federation 元数据。
由于凭据会进入 plan payload，coordinator 和 worker 之间应使用 TLS 或可信网络，
并避免把 plan payload 打到日志里。coordinator 和 worker 必须使用相同的分布式
协议版本。

P0 每个外部扫描是一个分布式 task；文件级并行扫描是后续优化。多个 server 可以
指向同一个 bucket，但 object store registry 按 bucket 记录，同 bucket 使用不同
凭据会互相覆盖；目前应保证一个 bucket 只有一套凭据。

其他 P0 限制：

- 只支持 S3 / S3-compatible；不暴露本地文件和 HDFS。
- 未实现 `IMPORT FOREIGN SCHEMA` 和 `ALTER FOREIGN TABLE`；使用显式列定义，
  修改 path/format 需要 drop 后重建。
- 外部文件是不可变输入，没有 snapshot、事务或 time travel；查询期间不要原地
  覆盖文件。
- 空前缀是合法的，会返回空表。

## 向外部文件 append

外部文件外表支持 append-only 的 `INSERT INTO`：

```sql
INSERT INTO public.events
SELECT id, event_time, payload FROM local_events;

INSERT INTO public.events VALUES (1, now(), 'raw');
```

目标 `path` 必须是目录（以 `/` 结尾）；向单个对象写入会被拒绝。每个输入 partition
对应一个 writer task，DataFusion 的 file sink 会在目标目录下写入新的、文件名唯一的
文件。`VALUES` 只有一个输入 partition，因此写一个文件；分布式 `SELECT` 可能写
多个文件。

没有事务、原子性或回滚：

- 语句失败时，已经完成的 task 写入的文件会保留；
- 成功 task 写入的文件在语句结束前就可能被读取；
- 重试同一语句会再次 append，可能产生重复数据；
- 不做清理，也不做 compaction。

不支持 `INSERT OVERWRITE`、`INSERT ... ON CONFLICT`、`UPDATE`、`DELETE` 和
`RETURNING`。目标格式由 foreign table 固定（`parquet`、`csv` 或 `json`）；
JSON 输出是 newline-delimited。write plan 会携带和 scan 相同的 server-level
S3 store 配置，worker 在执行时注册 object store。凭据进入 plan 的安全权衡，以及
同 bucket 多套凭据互相覆盖的限制，同样适用于写入。

目标必须是非分区目录；P0/P1 不写 Hive-style partition 列。INSERT 不会产生
LakeSoul commit，因此没有 snapshot、time travel，也没有跨文件的原子可见性。
