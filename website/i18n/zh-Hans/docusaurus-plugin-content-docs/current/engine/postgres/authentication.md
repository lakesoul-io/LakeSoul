---
sidebar_position: 9
sidebar_label: 认证与权限
---

# 认证与权限

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

`postgres-lakesoul` 提供 PostgreSQL wire 协议服务。本页只介绍该服务自身的认证与授权：如何启动服务、如何用 LakeSoul token 连接，以及域隔离、表级 `GRANT` / `REVOKE` 和 `CREATE USER` 的行为。

这些控制背后的租户模型——域是什么、metadata 超级管理员是谁、如何创建域和工作空间管理员——见[工作空间与 RBAC](../../operations/workspace-and-rbac.md)。新部署请先按那一页完成域和管理员初始化。

## 概念

**域（工作空间，domain）**：租户边界。每个 namespace、表、partition 和 snapshot 都属于且只属于一个域；一条连接看不到另一个域的对象。

**工作空间管理员（`domain_admin`）**：管理一个域的用户。可以建表、读写域内所有表、授予表权限；不能创建另一个工作空间管理员。

**普通用户**：域内没有管理员角色的成员。可以在已有 namespace 中建表；默认对其他用户的表只读，可以被额外授权。

**metadata 超级管理员**：拥有 LakeSoul metadata 数据库的 PostgreSQL 账号，负责执行初始化脚本。它不是 LakeSoul 客户端账号，凭据不能交给普通用户。

**`public` / `lake-public`**：对所有连接可见的共享域。

## 启动服务

服务读取的 metadata 连接配置与其他 LakeSoul 服务一致：

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test

postgres-lakesoul --bind 0.0.0.0 --port 5432 \
  --warehouse-prefix file:///data/lakesoul/warehouse
```

默认模式下信任启动时的 `user`，每条连接都能看到所有 namespace 和表。三个参数增加认证和租户隔离：

| 参数 | 环境变量 | 含义 |
| --- | --- | --- |
| `--jwt-auth` | `LAKESOUL_PG_JWT_AUTH` | 要求每条连接用明文密码方式提交 LakeSoul JWT，并只接受该 JWT |
| `--rbac` | `LAKESOUL_PG_RBAC` | 把每条连接限制在 token 的 `group` 所表示的域内；需要 `--jwt-auth` |
| `--rbac-domain` | `LAKESOUL_PG_RBAC_DOMAIN` | 固定共享 cluster 的写入域；需要 `--rbac` |

要求 token：

```sh
postgres-lakesoul ... --jwt-auth
```

要求 token 并隔离租户：

```sh
postgres-lakesoul ... --jwt-auth --rbac
```

固定共享 cluster 的写入域：

```sh
postgres-lakesoul ... --jwt-auth --rbac --rbac-domain team_a
```

## Token

这里用的就是 Flight server 和 S3 proxy 接受的那枚 token：HS256，用 metadata client secret 签名，claims 为 `{sub, group, exp}`。

- `sub` 必须等于连接的用户名；签给其他用户的 token 会被拒绝。
- `group` 是连接所属的 LakeSoul 域，只有开 `--rbac` 时才据此限制可见性。
- `exp` 是 Unix 时间戳；校验时额外允许 60 秒的容忍窗口。

签名密钥不是配置项，而是从 metadata 凭据推导出来的，与所有 LakeSoul 服务一致：

```
secret = md5("!@" + LAKESOUL_PG_USERNAME + "#" + LAKESOUL_PG_PASSWORD + "&*")
```

在持有这些配置的机器上签发：

```sh
export LAKESOUL_PG_URL='jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified'
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test

postgres-lakesoul token --user alice --group team_a --ttl 1h
```

`token` 只输出 token 本身。在 RBAC 部署中它会先查询 `domain_user` 用户目录，拒绝为不存在的用户签发；没有该表的旧 metadata 库仍保持离线签发行为。`--ttl` 接受数字加可选单位 `s`、`m`、`h`、`d`（如 `90`、`15m`、`1h`、`7d`），省略单位表示秒。

## 连接

```sh
PGPASSWORD=$(postgres-lakesoul token --user alice --group team_a --ttl 1h) \
  psql -h 127.0.0.1 -p 5432 -U alice -d default
```

驱动用的是同一个字段：token 就是连接的密码。libpq 放在 `PGPASSWORD` 或连接串的 `password=…` 里，JDBC 放在 `password` 属性里。连接的用户名必须是 token 的 `sub`。

## 被拒绝时客户端看到什么

没有 token、token 格式错误或签名不对、token 已过期、token 签给了其他用户，都会得到 `FATAL 28P01` 并断开连接。客户端不会被告知具体是哪种原因。服务端记录原因并计数：

| 日志 `reason` | 原因 |
| --- | --- |
| `missing_token` | 密码为空 |
| `invalid_token` | 格式错误、签名不对或缺少 claim |
| `expired` | `exp` 已过（超出 60 秒容忍窗口） |
| `user_mismatch` | token 签给了其他用户 |
| `unknown_user` | 用户不在 RBAC 用户目录中，或已被禁用 |

计数器为 `lakesoul_auth_failures_total{reason}`，在 `--metrics-addr` 上暴露。服务端同时输出 `token authentication failed` 日志，带对端地址和详情；客户端只会看到通用的密码失败。

token 通过之前不会创建会话，也不会占用连接名额。

## 域隔离

开 `--rbac` 后，token 的 `group` 成为连接的域。规则与 `script/meta_rbac_init.sql` 对 JVM 引擎施加的行级安全一致，但没有角色继承：一个 token 只带一个 group。

连接能看到自己的域，以及 `public` 和 `lake-public`；表级 ACL 还可以跨域授予只读访问，未授权的表对它不存在：

| 路径 | 行为 |
| --- | --- |
| 连接其他域的 namespace | 启动失败：`database "…" does not exist`；有表授权时 namespace 以只读可见 |
| 对其他域已授权表的 `SELECT` / `COPY TO` | 允许；无授权则为 `42P01` |
| 对只读表的 `INSERT` / `UPDATE` / `COPY FROM` | `42501` |
| 非 owner/空间管理员的 `ALTER` / `DROP` | `42501` |
| `pg_class`、`pg_tables`、`pg_database` | 只列出可见对象 |
| `CREATE TABLE` | 表域为 cluster 写入域（`--rbac-domain` 或 token group）；`creator` 为连接用户 |
| 后台查询（detached job） | 在提交者的域内执行，并按该域存储 |
| 订阅、IVM 维护的 `INSERT ... SELECT` | 以 `42501` 拒绝；它们会绕过域过滤写表 |

token 的域在启动时确定，`SET` / `RESET` 无法修改。共享 cluster 的写入域由 `--rbac-domain` 固定；其他域的客户端可以读取被授权的表，但不能写入或创建私有表。

## 表级 ACL

域隔离是外层边界。在域内部，每张表有 owner 和可选的授权：

- 建表者（`table_info.creator`）是 owner，可以读写、修改、删除该表。
- 表所在域的空间管理员可以读写域内所有表。
- 同域其他用户默认只读。
- 跨域用户默认看不到表；owner 或空间管理员可以授予 `ro`，跨域写权限会被拒绝。
- 显式 `none` 会对同域用户隐藏该表。
- `public` / `lake-public` 域的表完全开放，忽略表 ACL。

新表默认 owner 可读写、同域其他人只读。metadata 侧的 ACL 对象和策略（包括存量表的 `script/migrate_acl.sql` 一次性升级）在启用多租户时安装，见[工作空间与 RBAC](../../operations/workspace-and-rbac.md#启用多租户)。

服务接受 PostgreSQL `GRANT` / `REVOKE` 的表级子集：

```sql
GRANT SELECT ON t TO bob;
GRANT INSERT, UPDATE ON t TO bob;
GRANT SELECT ON t TO team_b.carol;          -- 跨域，只读
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bob;
REVOKE INSERT ON t FROM bob;
REVOKE ALL ON t FROM bob;
```

- `SELECT` 映射为 `ro`；`INSERT`、`UPDATE`、`DELETE`、`TRUNCATE`、`ALL` 映射为 `rw`。写权限隐含读权限。
- 裸用户名属于表所在域；`domain.user` 是 LakeSoul 的跨域**只读**授权扩展；跨域的 `ALL` 或写权限会被拒绝。
- `PUBLIC` 写入域级授权（`grantee_kind = 'domain'`）。`REVOKE ALL ... FROM PUBLIC` 会让表只对 owner 和空间管理员可见。
- `REVOKE` 写权限会降为 `ro`；`REVOKE SELECT` / `ALL` 会给同域用户写显式 `none`，从而隐藏该表。
- owner 或空间管理员才能授权。不可见的表返回 `42P01`；可见但无管理权返回 `42501`；public 表不接受 ACL。
- grantee 名字按原样保存。RBAC 部署还维护 `lakesoul_user` / `domain_user` 全局用户目录，供 `CREATE USER` 和 token 校验使用；仍然可以先授权、后建用户。
- `ON ALL TABLES IN SCHEMA public` 只覆盖当前 database（即当前 namespace）中的表。
- `GRANT` / `REVOKE` 立即提交，和 DDL 一样不支持在事务块内执行。当前连接立即生效；其他连接和 S3 proxy 在 5 秒 ACL 缓存内生效。ACL 视图刷新后如果可见权限发生变化，连接上的 prepared statement 会在执行前重新 plan，避免 revoke 后旧 plan 继续读表。
- 角色成员、列级权限、`WITH GRANT OPTION`、group ACL，以及 `information_schema` / `psql \dp` 反射暂不支持。

## 创建用户

空间管理员可以在自己的域内创建普通用户。服务端会创建真实 PostgreSQL 登录角色，GRANT `<domain>` 和 `<domain>_users`，并在 token 签发和 `--rbac` 连接使用的用户目录中登记；不会授予 `<domain>_admins`，因此空间管理员不能创建其他空间管理员。

```sql
CREATE USER alice WITH PASSWORD 'secret';
CREATE USER IF NOT EXISTS alice WITH PASSWORD 'secret';
```

- `PASSWORD` 必须显式给出，且必须是字符串字面量；不会生成随机密码。
- 已存在且由 LakeSoul 管理的角色在 `IF NOT EXISTS` 下只补当前域成员关系，不改密码；未登记的 role 会被拒绝。
- `public` / `lake-public` 不接受租户管理员创建用户。
- 需要创建另一个空间管理员时，由 metadata 超级管理员执行 `script/meta_rbac_add_user.sql` 并设置 `is_admin=true`，见[工作空间与 RBAC](../../operations/workspace-and-rbac.md#创建工作空间管理员)。
- `ALTER USER` / `DROP USER` 暂未实现。

pg-wire 的 metadata 账号需要 `CREATEROLE` 以及对 domain role 的 ADMIN OPTION。签发 token 的账号还需要 `domain_user` 的 `SELECT` 权限。创建后即可为该用户签发 token。

## Federation ACL

federation server 和 foreign table 使用与普通表相同的域默认规则：同域默认只读，public 对所有域只读；creator 和 domain admin 可写。`CREATE SERVER` 的创建者成为 owner；`CREATE FOREIGN TABLE` 需要 server 写权限和当前 namespace 写权限；server/foreign table 的 DDL、user mapping 和 DML 只有 creator/domain admin 可以执行。跨域 foreign table 不可见。

`--rbac` 会把域写进每个后台查询（`job.queries.domain`），`job_key` 的唯一索引也包含域。已有的 4.x 元数据库必须先迁移，服务端才能提交作业：

```sh
python3 script/metadata_migrate.py migrate
```

## Token 有效期

服务端只在握手时校验一次 token。已经建立的连接在 token 过期后仍可继续使用；没有中途重新认证，也没有在 `exp` 之前使 token 失效的手段。因此：

- 一条长连接无论 TTL 多短，都能一直用到断开。
- 新连接——包括连接池回收物理连接后新建的那条——如果配置里还是旧 token，一旦过期就会被拒绝。
- 没有吊销列表，`exp` 是 token 失效的唯一方式。轮换 metadata 密码会同时更换签名密钥，让所有 LakeSoul 服务的所有 token 一起失效；服务端只在启动时读取密钥，因此需要重启才能使用新密钥。

如果进程寿命超过一个 token 的有效期，应改为**每条物理连接现签一张**（在驱动的密码来源外面包一层），或者把 `--ttl` 设得比进程寿命更长。否则启动时签的那张过期后，连接池会出现间歇性的 `28P01`：池里空闲的旧连接还能用，新建的连接却认证不过。

## 安全说明

- token 以明文密码形式传输。请在服务端前面终止 TLS，或只在可信网络内使用。
- HS256 是对称密钥：任何能读到 metadata 凭据的服务，都能为任意用户和 group 签出 token。域隔离的强度取决于持有该凭据的最弱组件。
- `public` 和 `lake-public` 的对象对所有连接可见，并且（与 JVM 引擎的行级安全一致）可写。

## 下一步

- [工作空间与 RBAC](../../operations/workspace-and-rbac.md)
- [运维](operations.md)
- [后台查询](detached-queries.md)
- [Federation](federation.md)
