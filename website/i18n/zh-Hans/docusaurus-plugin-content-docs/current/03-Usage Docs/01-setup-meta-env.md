# 配置 LakeSoul 元数据库

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 使用配置文件
LakeSoul 使用 `LAKESOUL_HOME` (大小写均可) 环境变量或者 `lakesoul_home` JVM Property （只能全小写）来定位元数据库的配置文件，配置文件中主要包含 PostgreSQL DB 的连接信息。一个示例配置文件:
```ini
lakesoul.pg.driver=com.lakesoul.shaded.org.postgresql.Driver
lakesoul.pg.url=jdbc:postgresql://localhost:5432/lakesoul_test?stringtype=unspecified
lakesoul.pg.username=lakesoul_test
lakesoul.pg.password=lakesoul_test
```
以上配置值也是各个配置项的默认值。如果参考快速开始文档启动的元数据库，在本地测试中可以不用做以上配置，就能正常访问。

生产环境可以根据数据库部署的实际情况来配置。

如果找不到上述环境变量或 JVM Property，则会分别查找 `LAKESOUL_PG_DRIVER`、`LAKESOUL_PG_URL`、`LAKESOUL_PG_USERNAME`、`LAKESOUL_PG_PASSWORD` 这几个环境变量作为配置的值。

如果还查找不到配置，则会分别查找 `lakesoul.pg.dirver`、`lakesoul.pg.url`、`lakesoul.pg.username`、`lakesoul.pg.password` 这几个 JVM Property 的值，如果一个配置项都没有配置，则使用其默认值。

:::caution
在 2.0.1 及之前版本，只支持通过 `lakesoul_home` （全小写）环境变量查找配置文件。
:::

:::caution
自 2.1.0 起，LakeSoul Spark 和 Flink 的 jar 包通过 shade 方式打包了 Postgres Driver，Driver 的名字是 `com.lakesoul.shaded.org.postgresql.Driver`，而在 2.0.1 版本之前，Driver 还没有 shaded 打包，名字是 `org.postgresql.Driver`。
:::

在使用 LakeSoul 之前还需要初始化元数据表结构：
```bash
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_init.sql
```
`meta_init.sql` 文件在代码库的 `script` 目录下。