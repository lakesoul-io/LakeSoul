---
sidebar_position: 9
sidebar_label: 认证与域隔离
---

# 认证与域隔离

<!--
SPDX-FileCopyrightText: 2026 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

PostgreSQL wire 服务默认不认证：信任启动时的 `user`，每条连接都能看到所有 namespace 和表。两个参数改变这一点：

| 参数 | 环境变量 | 含义 |
| --- | --- | --- |
| `--jwt-auth` | `LAKESOUL_PG_JWT_AUTH` | 要求每条连接用明文密码方式提交 LakeSoul JWT，并只接受该 JWT |
| `--rbac` | `LAKESOUL_PG_RBAC` | 把每条连接限制在 token 的 `group` 所表示的域内；需要 `--jwt-auth` |

两个参数都不开时，启动和可见性与之前完全一致。

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

`token` 只输出 token 本身，且不建立 metadata 连接，所以 metadata 库不可用时也能签发。`--ttl` 接受数字加可选单位 `s`、`m`、`h`、`d`（如 `90`、`15m`、`1h`、`7d`），省略单位表示秒。

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

计数器为 `lakesoul_auth_failures_total{reason}`，在 `--metrics-addr` 上暴露。服务端同时输出 `token authentication failed` 日志，带对端地址和详情；客户端只会看到通用的密码失败。

token 通过之前不会创建会话，也不会占用连接名额。

## 域隔离

开 `--rbac` 后，token 的 `group` 成为连接的域。规则与 `script/meta_rbac_init.sql` 对 JVM 引擎施加的行级安全一致，但没有角色继承：一个 token 只带一个 group。

连接能看到自己的域，以及 `public` 和 `lake-public`；其余对象对它不存在：

| 路径 | 行为 |
| --- | --- |
| 连接其他域的 namespace | 启动失败：`database "…" does not exist` |
| 对其他域的表执行 `SELECT`、`INSERT`、`UPDATE`、`ALTER`、`DROP` | `42P01`，在共享 namespace 里同样如此 |
| `pg_class`、`pg_tables`、`pg_database` | 只列出可见对象 |
| `CREATE TABLE` | 表的元数据行带上创建者的域 |
| 后台查询（detached job） | 在提交者的域内执行，并按该域存储 |
| 订阅、IVM 维护的 `INSERT ... SELECT` | 以 `42501` 拒绝；它们会绕过域过滤写表 |

域在启动时确定，`SET`/`RESET` 无法修改。需要多个域的用户按域各开一条连接。

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

- [运维](operations.md)
- [后台查询](detached-queries.md)
- [工作空间与 RBAC](../../operations/workspace-and-rbac.md)
