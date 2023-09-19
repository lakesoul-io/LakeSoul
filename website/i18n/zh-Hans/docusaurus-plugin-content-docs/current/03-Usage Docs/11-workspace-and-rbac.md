# 多租户功能：工作空间和角色权限控制

:::tip
从 2.4 起支持。
:::
LakeSoul 支持多个工作空间划分，每个空间可以有多个用户，分为管理员和普通用户两种角色。不同工作空间的数据（包括元数据、物理数据）均是隔离的。

实现原理上，LakeSoul 使用了 PostgreSQL 的 Role 和 Row Level Security Policies （RLS 行级别安全策略），实现了元数据隔离。

多租户情况下，为每个用户分配一个 PG 的用户，通过 RLS 的控制，该用户仅能读写其所属工作空间的元数据（包括 namespace/table/partition/snapshot），而无法读取/修改其他工作空间的元数据。配合 hadoop 的用户组，可以实现 HDFS 的权限隔离。

**值得一提的是，LakeSoul 的权限隔离，对于提交到集群上的 SQL/Java/Scala/Python 作业均是有效的，用户无法通过代码访问到其他空间的元数据和物理数据。而传统上 Hadoop 体系的 Ranger ，仅能够对提交到 HiveServer 的请求实现元数据隔离，而很难防止用户通过提交代码到集群上直接访问到 Hive 元数据。**

## 工作空间，默认角色和库表的关系
每个工作空间中，都会有管理员（admins）角色和普通用户（users）角色。其中仅有管理员可以创建、删除 namespace(database)。而管理员、用户均可在该空间下的 namespace 中创建并读写表。每个 namespace 在创建时，当前操作的管理员所属的空间，就是该 namespace 所属的空间。namespace 和表所属的工作空间，创建后不可修改。

## 启用多工作空间和角色权限控制功能
默认情况下，LakeSoul 并不会开启该功能。可以通过执行 `script/meta_rbac_init.sql` 启用该功能：
```shell
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_rbac_init.sql lakesoul_test
```
这里连接 PG 的用户需要是管理员用户，具有创建 Role、开启 RLS 的权限。可以根据实际 PG 的部署修改（包括最后的 database 名）。

## 创建工作空间
LakeSoul 提供了创建工作空间的 PLSQL 脚本，位于 `script/meta_rbac_add_domain.sql`，执行方法：
```shell
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_rbac_add_domain.sql -v domain="'test_workspace'"
```
该脚本需要 PG 管理员帐号执行。其中 -v domain 参数，为要创建的工作空间名。

## 创建工作空间中的用户
执行：
```shell
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_rbac_add_user.sql -v domain="'test_workspace'" -v user="'admin1'" -v is_admin='true'
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_rbac_add_user.sql -v domain="'test_workspace'" -v user="'user1'" -v is_admin='false'
```
该脚本需要 PG 管理员帐号执行。其中 domain 参数指定该用户所属的工作空间，user 参数为用户名，is_admin 参数配置是否是管理员。执行这个脚本，会为该用户分配一个具有对应工作空间和角色的 PG 用户，并生成一个 PG 连接的随机密码。后续该用户需要使用该用户名和密码作为 LakeSoul 元数据连接的参数。每个用户的密码需要自行妥善保管，不可泄漏给他人。

## 用户访问 LakeSoul
参考 [元数据配置](01-setup-meta-env.md) 中的方法，使用用户自身的用户名、密码，建立与 LakeSoul 元数据库的连接。

同时，还需要增加一个额外的配置项来指定工作空间。可以用环境变量 `LAKESOUL_CURRENT_DOMAIN` 或 JVM 属性 `lakesoul.current.domain` 来传递该用户的工作空间。设置这些参数后，Spark/Flink/Presto/Python 的作业即可实现工作空间划分和元数据隔离。

## 与 Hadoop 用户组配合使用
在集群中，通常还需要隔离 HDFS 上的物理数据。LakeSoul 会自动在建表时，将 namespace 和表目录的用户组设置为当前用户和工作空间。并将 namespace 目录权限设置为 `drwx-rwx-___`，将表目录权限设置为 `drwx-r_x-___` （目前仅支持HDFS。S3的支持会在未来提供）。

在多租户 Hadoop 环境中，建议在作业提交客户机上，为每个用户分配 Linux 用户，与 Hadoop 用户组同步。并为每个用户设置好 LakeSoul 元数据连接的环境变量(可以添加到每个用户的 `~/.bashrc` 中)。除集群管理员外，其他用户在客户机上不应有 sudo 权限。

如果使用 Zeppelin 等开发环境，可以参考 Zeppelin 的 [User Impersonation](https://zeppelin.apache.org/docs/0.10.0/usage/interpreter/user_impersonation.html) 功能，在每个用户创建 Notebook 时，自动进行 Linux 用户的切换(通过 `sudo -u`)。其他开发工具也可以参考实现类似的机制。