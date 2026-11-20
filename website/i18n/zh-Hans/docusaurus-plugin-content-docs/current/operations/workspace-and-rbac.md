---
sidebar_position: 6
---

# 多租户功能：工作空间和角色权限控制

:::tip
从 2.4 起支持。
:::

## 概述

LakeSoul 支持多个工作空间（domain，也称域）。每个空间有自己的 namespace、表、partition、snapshot 和物理数据；一个空间的用户无法读取或修改另一个空间的数据。

实现上，LakeSoul 使用 PostgreSQL 的 Role 和 Row Level Security（RLS，行级安全策略）隔离元数据，并结合存储权限隔离物理数据。本页介绍租户模型和部署操作：启用多租户、创建工作空间和管理员、创建用户。pg-wire 服务的 token 认证、表级 ACL 和 SQL `CREATE USER` 见[认证与权限](../engine/postgres/authentication.md)。

这种隔离对提交到集群的 SQL/Java/Scala/Python 作业都有效，用户无法通过提交代码访问其他空间的元数据和物理数据。传统 Hadoop 体系中的 Ranger 只能保护发往 HiveServer 的请求，很难阻止集群上的代码直接访问 Hive 元数据。

### 角色说明

| 角色 | 说明 |
| --- | --- |
| metadata 超级管理员 | 拥有 LakeSoul metadata 数据库的 PostgreSQL 账号。负责创建角色、开启 RLS 和执行初始化脚本。它不是 LakeSoul 客户端账号，凭据不能交给普通用户。 |
| 工作空间管理员（`domain_admin`） | `<workspace>_admins` 的成员。可以创建/删除 namespace、读写空间内所有表，并可以创建普通用户；不能创建另一个工作空间管理员。 |
| 普通用户 | `<workspace>_users` 的成员。可以在已有 namespace 中建表；默认对其他用户的表只读，具体受表级 ACL 约束。 |
| `public` / `lake-public` | 对所有用户可见的共享空间。 |

每个用户有一个 PostgreSQL role，可以是多个工作空间的成员。直连 Spark/Flink/Python 作业通过 `LAKESOUL_CURRENT_DOMAIN`（或 `lakesoul.current.domain`）选择工作空间，metadata 连接使用真实用户和该 domain。pg-wire / Flight 连接通过 token 的 `group` claim 选择；这些服务持有共享的 privileged metadata 账号，因此必须在应用层用 session user/group 重新检查 ACL。`CREATE USER` 和 `GRANT` 的授权主体也是 session user 及其 token group，而不是 cluster write domain。

## 隔离机制

元数据侧，LakeSoul 使用 PostgreSQL Role 和 Row Level Security 策略。每个用户用自己的 PostgreSQL role 连接 metadata 库，RLS 把该 role 限制在所属工作空间的行上：其他空间的 namespace、表、partition、snapshot 和 data commit 既不可读也不可写。

HDFS 物理数据侧，LakeSoul 在建表时按创建者和工作空间设置 namespace/表目录的 user、group 和目录权限，配合 Hadoop 的用户组模型实现文件隔离。

对象存储侧，[S3 proxy](#与存储系统的权限集成)对 S3 请求执行同一套表 ACL。

## 工作空间、默认角色和库表关系

每个工作空间都有一个管理员角色 `<workspace>_admins` 和一个普通用户角色 `<workspace>_users`。namespace 所属的工作空间由创建它的管理员决定；namespace 和表在创建后不能更换工作空间。

## 启用多租户

默认情况下多租户是关闭的。全新部署直接使用 `script/meta_init.sql` 的 schema。要启用角色和 RLS，由 **metadata 超级管理员**执行：

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_init.sql lakesoul_test
```

连接 PostgreSQL 的账号需要能创建 Role、开启 RLS。请按实际部署调整 host、port、user 和 database。

如果部署中已有使用旧 ACL 模型的表，先执行 ACL 迁移，让这些表保持原来的读写行为：

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/migrate_acl.sql lakesoul_test
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_init.sql lakesoul_test
```

## 创建工作空间

工作空间由 metadata 超级管理员通过 `script/meta_rbac_add_domain.sql` 创建：

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_add_domain.sql \
  -v domain="'test_workspace'"
```

`-v domain` 参数是工作空间名。脚本会创建 `<workspace>`、`<workspace>_admins`、`<workspace>_users` 三个 PostgreSQL role，并登记到 domain registry。

## 创建工作空间管理员

metadata 超级管理员用同一个用户脚本、设置 `is_admin=true` 创建工作空间的第一个管理员：

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_add_user.sql \
  -v domain="'test_workspace'" \
  -v user="'admin1'" \
  -v is_admin='true'
```

脚本会在需要时创建 PostgreSQL role，授予工作空间和 `<workspace>_admins` 角色，写入 `domain_admin` 行，并打印生成的 PostgreSQL 密码。请保存该密码，用户连接 metadata 库时需要使用。对已有用户重复执行脚本不会重置密码。

工作空间管理员不能通过 pg-wire 的 `CREATE USER` 创建另一个工作空间管理员；需要由 metadata 超级管理员执行本脚本。

## 创建普通用户

普通用户用同一个脚本、设置 `is_admin=false` 创建：

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/meta_rbac_add_user.sql \
  -v domain="'test_workspace'" \
  -v user="'user1'" \
  -v is_admin='false'
```

脚本会授予 `<workspace>` 和 `<workspace>_users`，并把用户登记到用户目录。生成的密码只打印一次，用户连接 metadata 库时需要使用。

工作空间有管理员之后，普通用户也可以由该管理员通过 pg-wire 创建：

```sql
CREATE USER user2 WITH PASSWORD 'secret';
```

SQL 行为、密码要求和限制见[创建用户](../engine/postgres/authentication.md#创建用户)。

## 配置用户访问 LakeSoul

使用用户自己的 PostgreSQL 用户名和密码建立 metadata 连接，参考[元数据配置](metadata-postgres.md)。同时设置工作空间：

```sh
export LAKESOUL_CURRENT_DOMAIN='test_workspace'
```

或 JVM 属性：

```properties
lakesoul.current.domain=test_workspace
```

设置后，Spark/Flink/Presto/Python 作业会被限制在该工作空间内，只能读写该空间的元数据。

## 与存储系统的权限集成

在 HDFS 集群中，还需要隔离物理数据。LakeSoul 在建 namespace 或表时，会把目录的 user 和 group 设置为当前用户和工作空间：namespace 目录权限为 `drwx-rwx-___`，表目录权限为 `drwx-r_x-___`。目前 Hadoop 存储主要支持 HDFS；S3 等对象存储的目录隔离由下面的 S3 proxy 提供。

在多租户 Hadoop 环境中，建议在作业提交客户机上为每个用户分配 Linux 用户，并与 Hadoop 用户组同步。为每个用户设置 LakeSoul metadata 连接的环境变量（例如写入 `~/.bashrc`）。除集群管理员外，其他用户不应在客户机上拥有 sudo 权限。

如果使用 Zeppelin 等开发环境，可以参考
[User Impersonation](https://zeppelin.apache.org/docs/0.10.0/usage/interpreter/user_impersonation.html)。
用户创建 Notebook 时，Zeppelin 会通过 `sudo -u` 切换到对应的 Linux 用户；其他工具也可以实现类似机制。

对于 S3 等对象存储，LakeSoul 提供 `S3 Proxy` 做目录级权限隔离。它采用 sidecar 转发模式，拦截 S3 HTTP 请求，结合当前用户判断是否有权访问目标表。sidecar 需要一个 catalog-reader 的 metadata 账号来评估跨空间授权。

## 存量部署

已有 4.x 部署先执行 metadata 迁移：

```sh
python3 script/metadata_migrate.py migrate
```

然后从现有的 `<workspace>_users` / `<workspace>_admins` role 成员关系回填工作空间注册表和 pg-wire 用户目录：

```sh
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test \
  -f script/migrate_users.sql lakesoul_test
```

之后以 `--rbac` 启动 pg-wire 时，不在目录中的用户 token 会被拒绝。

## 表级访问控制

工作空间隔离是外层边界；表级 ACL 在空间内部进一步控制访问（owner 可写、同空间默认只读、跨空间只读授权）。具体规则、`GRANT` / `REVOKE` 示例和 SQL `CREATE USER` 见[认证与权限](../engine/postgres/authentication.md#表级-acl)。
