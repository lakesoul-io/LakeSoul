# PostgreSQL 表级 ACL 设计

Status: implemented in PR #1316; this note records the model and the decisions behind it.

## 1. 目标

在现有 domain 隔离之上，增加表级 ACL，使 LakeSoul metadata（PostgreSQL）
的直接访问路径（Spark / Flink / Python / 任意代码）和 PG wire 路径都能执行
同一套权限规则。

核心约束：

- **domain 是硬租户边界**。没有授权时，跨 domain 的 namespace 和表完全不可见，
  返回“不存在”，不是“无权限”。
- **ACL 是叠加，不是替代**。同 domain 的默认权限由新模型决定，ACL 只做进一步
  收紧或显式授权。
- **任意代码可以直连 PostgreSQL metadata**，因此直接访问路径必须由数据库
  RLS 执行 ACL，不能只在 JVM/Rust 应用层检查。
- **PG wire 的客户端不是 metadata DB 用户**，所以 PG wire 的客户端 ACL 在应用层
  执行；数据库连接本身仍受它所使用的 PG 角色/domain 的 RLS 限制。
- **S3 proxy 是直接访问路径的存储层闸门**。多租户环境中，Spark/Python 等可执行
  代码的作业必须通过 S3 proxy sidecar 访问对象存储，否则仅靠 metadata RLS
  无法阻止代码直接读取文件。

## 2. 权限模型

### 2.1 新默认

| 主体 / 表 | 权限 |
| --- | --- |
| owner（`table_info.creator`） | rw + DDL |
| space admin（该 domain 的 admin） | domain 内所有表 rw + DDL |
| 同 domain 其他用户，无 ACL | **ro** |
| 同 domain 用户，ACL `rw` | rw |
| 同 domain 用户，ACL `ro` | ro |
| 同 domain 用户，ACL `none` | 不可见 |
| 跨 domain，ACL `ro` | 可见、只读 |
| 跨 domain，无 ACL | 不可见 |
| 跨 domain，ACL `rw` | 授权 API 拒绝；历史数据按 ro |
| `public` / `lake-public` 表 | rw；完全忽略 ACL/domain |

namespace：

| 主体 | 权限 |
| --- | --- |
| owner / space admin | rw + 创建/删除 namespace |
| 同 domain 用户，无 ACL | rw：可列目录、可建表 |
| 同 domain 用户，ACL `ro` | 可列目录、可读表，不可建表 |
| 同 domain 用户，ACL `none` | 不可见 |
| 跨 domain，任意表被授权 | namespace 可见，只读；不可建表 |
| 跨 domain，无授权 | 不可见 |
| `public` namespace | rw：所有 domain 可列、可建表 |

说明：

- 表的 `domain` 与 namespace 的 `domain` 独立。`public` namespace 中的表若由
  team A 创建，其 `table.domain = team_a`，仍按 team A 的规则；只有
  `table.domain in ('public', 'lake-public')` 才是全局开放。
- 同 domain 默认 ro 是新行为。存量表通过迁移脚本一次性写入 domain rw 授权，
  保持升级前现有表的可写行为；升级后新建表使用新默认。
- `public` / `lake-public` 表不参与 ACL。授权 API 应拒绝为它们写 ACL 行。
- 无授权 = `42P01`（不可见）。可见但只读时写 = `42501`。

### 2.2 解析顺序

对表：

1. `table.domain in ('public', 'lake-public')` -> `rw`
2. `user == table.creator` -> `rw`（owner）
3. `user` 是 `table.domain` 的 space admin -> `rw`
4. exact user ACL -> 值（跨 domain `rw` 降为 `ro`）
5. group ACL（后续版本）-> 值
6. domain grant ACL -> 值
7. 同 domain -> `ro`
8. 其他 -> `none`

对 namespace：

1. `namespace.domain in ('public', 'lake-public')` -> `rw`
2. `user == namespace.creator` -> `rw`
3. `user` 是 `namespace.domain` 的 space admin -> `rw`
4. exact user / domain namespace ACL -> 值
5. 同 domain -> `rw`
6. 跨 domain 且该 namespace 下至少一张表可见 -> `ro`
7. 其他 -> `none`

`effective = min(namespace_privilege, table_privilege)`，`rw > ro > none`。
namespace 的 `ro` 是写的上限：即使表 ACL 为 `rw`，namespace 为 `ro` 时也不能写。

## 3. 元数据模型

### 3.1 `table_acl`

```sql
create table table_acl (
  table_id       text not null references table_info(table_id) on delete cascade,
  grantee_domain text not null,
  grantee_kind   text not null, -- 'user' | 'group' | 'domain'
  grantee        text not null, -- user id / group name / '*' for domain grant
  privilege      text not null, -- 'ro' | 'rw' | 'none'
  granted_by     text not null,
  updated_at     timestamptz not null default now(),
  primary key (table_id, grantee_domain, grantee_kind, grantee)
);

create index table_acl_grantee_idx
  on table_acl (grantee_domain, grantee_kind, grantee, privilege, table_id);
```

`grantee_kind = 'domain'` 表示“table.domain 下所有用户”；一行即可表达
存量表的 domain rw 授权。`grantee_kind = 'user'` 的 `grantee_domain` 是被授权者
所在 domain，用于解决不同 domain 同名用户。

### 3.2 `namespace_acl`

同 `table_acl`，主键为 `(namespace, grantee_domain, grantee_kind, grantee)`，
外键引用 `namespace(namespace) on delete cascade`。跨 domain 只允许表级授权，
namespace ACL 只用于同 domain 的精细控制。

### 3.3 `domain_admin`

```sql
create table domain_admin (
  domain    text not null,
  user_name text not null,
  primary key (domain, user_name)
);
```

一个 domain 多行，支持多个 space admin。迁移时从旧的 `_admins` PG role 成员
回填；新控制台/管理 API 直接维护该表。直接连接路径的 RLS 同时兼容旧的
`pg_has_role(user, domain || '_admins', 'member')`。

### 3.4 `creator`

`table_info` 和 `namespace` 增加 `creator text default current_user`。

- 直接连接路径：默认 `current_user`，即 job 用户，owner 正确。
- PG wire：建表时显式写客户端 JWT 用户，不能使用 `DEFAULT current_user`
  （那会是 cluster 的 metadata 用户）。
- 存量表：迁移脚本只在列不存在时添加并把存量行置为该 domain 的 admin；
  已有列不修改。由于存量表同时获得 domain rw 授权，数据读写不依赖 creator；
  DDL 由 owner 或 space admin 执行。

## 4. RLS 实现

### 4.1 SQL helper

`meta_rbac_init.sql` 创建以下 `STABLE SECURITY DEFINER` 函数，固定
`search_path`，只返回 `ro/rw/none`：

- `lakesoul_table_privilege(table_id text, user_name text) returns text`
  (PL/pgSQL role-derived domains, used by RLS)
- `lakesoul_table_privilege(table_id text, user_name text, domain text)`
  (explicit domain, used by the S3 proxy)
- `lakesoul_table_privilege_for_domains(table_id text, user_name text, domains text[])`
- `lakesoul_table_privilege_by_path(path text, user_name text, domain text)`
- `lakesoul_namespace_privilege(namespace text, user_name text) returns text`
- `lakesoul_is_domain_admin(domain text, user_name text) returns boolean`
- `lakesoul_current_domains(user_name text) returns text[]`

`SECURITY DEFINER` 让函数可以读 `table_info`、`table_acl`、`domain_admin`，
不受这些表自身 RLS 的递归影响。函数的 owner 是执行 RBAC 初始化脚本的管理员；
`search_path` 固定为 `pg_catalog, public`，`EXECUTE` 只授予 metadata 角色。

### 4.2 policy

直接连接路径下，`current_user` 是 job 用户：

- `namespace`：SELECT 要求 namespace privilege != none；INSERT 允许同 domain
  用户创建；UPDATE/DELETE 仅 creator 或 space admin。
- `table_info`：SELECT 要求 table privilege != none；INSERT 要求 namespace
  privilege = rw，且 `creator = current_user OR space admin`；UPDATE/DELETE
  仅 creator 或 space admin。
- `table_name_id` / `table_path_id`：SELECT 通过 `table_id` 的 table privilege；
  INSERT/UPDATE/DELETE 要求 table privilege = rw。
- `partition_info` / `data_commit_info`：SELECT 通过 `table_id` 的 table
  privilege；写要求 rw。
- `table_acl` / `namespace_acl`：SELECT 允许所有 metadata 用户（PG ACL 目录
  本来可读）；写仅 table/namespace 的 space admin。ACL 写策略通过 helper
  判断，避免 domain admin 修改其他 domain 的授权。
- `domain_admin`：SELECT 允许所有 metadata 用户；写仅 super admin。

旧的 `read_policy` / `domain_only_policy` 必须先 `DROP POLICY IF EXISTS`，
不能只追加 ACL policy：PostgreSQL 多个 PERMISSIVE policy 是 OR，旧 policy
存在会让 ACL 失效。

### 4.3 可见性

- 无授权：`42P01`，不暴露表存在。
- 表可见但只读：写操作 `42501`。
- 跨 domain 被授权 ro：在可见 namespace 中列出该表，只列授权表，不列同
  namespace 下其他未授权表。
- `SHOW DATABASES`：同 domain / public / 该 namespace 下至少一张表对当前用户
  可见时才显示。

## 5. PG wire 应用层

PG wire cluster 的 metadata 连接使用 cluster 服务账号；客户端 JWT 用户不是
DB 用户。因此：

- 客户端 ACL 在应用层解析，与应用层 catalog 过滤一致。
- cluster metadata 账号仍需受 RLS/domain 约束；写入的 domain 为 cluster 启动
  domain。
- 客户端权限不能超过 cluster 权限：`effective = cluster 权限 ∩ 客户端 ACL`。
- 建表 domain = cluster domain；`creator` = 客户端 JWT 用户。
- 跨 domain 只读：客户端通过 ACL 解析，不依赖连接的是哪个 cluster；cluster
  metadata 账号必须能读到跨 domain 的 metadata/ACL（catalog reader / owner），
  这些账号是受信任的服务账号，客户端不会拿到它们的凭据。应用层仍是最终闸门。
  S3 proxy sidecar 的 metadata 账号同理，或使用 ACL 函数（函数存在时 Rust
  解析器优先调用）。
- 跨 domain 读写请求：写直接 `42501`；`public` 表例外，public 完全开放。
- 未配置 `--rbac` 时保持现有行为，不启用 ACL。

实现：

- `lakesoul-metadata::acl::AclView` 从 PostgreSQL 读取 namespace/table/ACL/
  domain_admin，解析成每用户的可见视图。
- `PgSession` 持有 `AclEntry`；每个语句前由 `AclRefreshHook` 按 TTL 刷新，
  并更新 `PgDatabaseCatalogList` 的可见 database。新建连接强制刷新一次，
  看到最近的 grant/revoke；已有连接按 5 秒 TTL 刷新。
- 共享 cluster 可通过 `--rbac-domain` / `LAKESOUL_PG_RBAC_DOMAIN` 固定写入
  domain；未设置时沿用 token 的 `group`。跨 domain 客户端只能读。
- 新表或另一个引擎刚创建的表不在缓存视图时，catalog/写检查按单表 live
  查询一次，不用等整张 ACL 视图刷新。
- `PgPublicSchema` 用 `AclView` 过滤 `table_names`/`table`/`table_exist`/
  `deregister_table`。
- INSERT/UPDATE/COPY FROM/DDL hook 在解析目标表后检查写权限；只读目标返回
  `42501`，不可见目标返回 `42P01`。
- DDL 建表检查 namespace 写权限，写入 `creator = 客户端用户`，domain 使用
  cluster domain。

### 5.1 SQL GRANT/REVOKE

pg-wire 在应用层实现表级 `GRANT` / `REVOKE` 子集，不追求完整 PostgreSQL：

```sql
GRANT { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | ALL }
  ON [TABLE] table [, ...] TO grantee [, ...];

REVOKE { SELECT | INSERT | UPDATE | DELETE | TRUNCATE | ALL }
  ON [TABLE] table [, ...] FROM grantee [, ...];
```

- grantee 可以是同 domain 用户名、`domain.user`（跨 domain，只读）或
  `PUBLIC`（写入 table 所在 domain 的 domain grant）。
- `SELECT` -> `ro`；`INSERT/UPDATE/DELETE/TRUNCATE/ALL` -> `rw`。跨 domain
  的写权限直接拒绝（`0A000`）。
- `REVOKE` 写权限把 `rw` 降为 `ro`；`REVOKE SELECT`/`ALL` 对同 domain 用户
  写显式 `none`，对跨 domain 用户删除 ACL 行。
- owner 或 domain admin 才能执行；不可见表返回 `42P01`，可见但无管理权返回
  `42501`；public 表不接受 ACL。
- `ON ALL TABLES IN SCHEMA public` 枚举当前 namespace 的表，public 表跳过；
  任一非 public 表不可管理则整条语句失败。
- 语句在 metadata 中单事务提交，但 pg-wire 事务块内不支持（与 DDL 一致）。
- 不支持角色成员、列级权限、`WITH GRANT OPTION`、group ACL；ACL 变更当前
  连接立即生效，其他连接和 S3 proxy 在 5 秒 TTL 内生效。
- `information_schema` / `pg_class.relacl` / `psql \dp` 暂不反射 ACL。

实现位于 `rust/postgres-lakesoul/src/grant.rs`，ACL 写入通过
`lakesoul-metadata::acl::apply_acl_changes` 在一个事务里完成；它按
`table_id` 排序并 `FOR SHARE` 锁表，避免与 `DROP TABLE` 竞态或批量语句死锁。

## 6. S3 proxy

S3 proxy 对表路径的访问增加 ACL 检查：

- `GET`/`HEAD`/list -> 需要 `ro` 或 `rw`。
- `PUT`/`POST`/`DELETE` -> 需要 `rw`。
- 跨 domain ro 允许读，拒绝写。
- `public` / `lake-public` 表直接允许读写。
- 非表路径保持现有行为：读拒绝，写/列目录放行。
- ACL 查询通过 `lakesoul_table_privilege_by_path(path, user)` 或 Rust
  `AclView` 完成；缓存要有短 TTL 或显式失效。

## 7. 迁移

### 7.1 新部署

`script/meta_init.sql` 直接包含最新模型：

- `table_acl`
- `namespace_acl`
- `domain_admin`
- `table_info.creator` / `namespace.creator`
- 索引与外键

`script/meta_rbac_init.sql` 只包含最新的角色、函数和 RLS policy。

### 7.2 存量部署

单独脚本 `script/migrate_acl.sql`，不混入 `meta_init.sql` /
`meta_rbac_init.sql`：

1. 创建缺失的 ACL/domain_admin 表和索引；缺失 `creator` 列时添加，并把存量
   行置为该 table/namespace domain 的 admin；已有 `creator` 列不修改。核心
   schema（ACL 表、creator、domain_admin）同时写入 `meta_init.sql` 和 release
   migration，保证走自动 migration 的部署也有对象；本脚本只负责存量数据。
2. 为每张非 public/lake-public 存量表插入一条 domain rw 授权：

   ```sql
   insert into table_acl(table_id, grantee_domain, grantee_kind, grantee,
                         privilege, granted_by)
   select table_id, domain, 'domain', '*', 'rw', 'acl-migration'
   from table_info
   where domain not in ('public', 'lake-public')
   on conflict do nothing;
   ```

3. 从旧的 `_admins` PG role 成员回填 `domain_admin`。
4. 安装/替换 ACL 版 RLS policy（或提示重新执行更新后的
   `meta_rbac_init.sql`）。
5. 输出迁移的表数量、admin 数量。

迁移幂等；可重复执行。

## 8. 测试

- metadata：`AclView` 解析矩阵（owner/admin/同 domain/跨 domain/public/none/
  namespace ro 上限）、`creator`、domain grant、public 跳过 ACL。
- PG wire：`acl_contract.rs` 覆盖同 domain ro/rw/none、跨 domain ro/写拒绝、
  public 开放、SHOW DATABASES/TABLES、`42P01` vs `42501`、owner/domain admin、
  creator=客户端用户、跨 domain 授权不依赖 cluster、ACL 变更后的刷新。
- SQL GRANT/REVOKE：`acl_contract.rs` 覆盖 GRANT/REVOKE 的读写、跨 domain、
  PUBLIC、ALL TABLES、非 owner、public 表、事务块和扩展协议；
  `lakesoul-metadata/tests/acl_mutation.rs` 覆盖原子性与折叠语义。
- 回归：现有 `rbac_contract`、`session_contract`、`sql_compat`、`copy_contract`
  等。测试 harness 的 out-of-band 表需要补 domain rw ACL，模拟旧表。
- S3 proxy：单元测试 path ACL 读写判断；跨 domain ro 读通过、写 403。
- 迁移：迁移脚本在含存量表/无 creator 列的库上幂等执行。

## 9. 边界

- v1 不做 group ACL（表结构预留 `grantee_kind`）。
- SQL `GRANT/REVOKE` 只覆盖表级子集；namespace ACL 仍由管理 API/直接元数据
  操作维护。角色成员、列级权限、`WITH GRANT OPTION` 不做。
- v1 不做列级权限、用户自定义 RLS、`SET ROLE`。
- HDFS 存储层授权暂不做；对象存储必须通过 S3 proxy 访问。
- `public` / `lake-public` 表完全开放读写；DDL 仍由 creator / space admin
  控制（避免任意用户 DROP 共享表）。
- 同 domain 的 namespace 默认可建表（rw）；表的默认是 owner rw、其他人 ro。
  存量表迁移为 domain rw，升级后新表才使用新默认。
