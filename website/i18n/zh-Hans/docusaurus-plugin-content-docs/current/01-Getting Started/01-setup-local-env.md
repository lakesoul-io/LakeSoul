# 搭建本地测试环境

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 启动一个 PostgreSQL 数据库
可以通过docker使用下面命令快速搭建一个pg数据库：
```bash
docker run -d --name lakesoul-test-pg -p5432:5432 -e POSTGRES_USER=lakesoul_test -e POSTGRES_PASSWORD=lakesoul_test -e POSTGRES_DB=lakesoul_test -d swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/postgres:14.5
```

## PG 数据库初始化
在 LakeSoul 代码库目录下执行：
```bash
PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_init.sql
```

## 安装 Spark 环境
由于 Apache Spark 官方的下载安装包不包含 hadoop-cloud 以及 AWS S3 等依赖，我们提供了一个 Spark 安装包，其中包含了 hadoop cloud 、s3 等必要的依赖：https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/spark/spark-3.3.2-bin-hadoop3.tgz

```bash
wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/spark/spark-3.3.2-bin-hadoop3.tgz
tar xf spark-3.3.2-bin-hadoop3.tgz
export SPARK_HOME=${PWD}/spark-3.3.2-bin-hadoop3
```

:::tip
如果是生产部署，推荐下载不打包 Hadoop 的 Spark 安装包：

https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-without-hadoop.tgz

并参考 https://spark.apache.org/docs/latest/hadoop-provided.html 这篇文档使用集群环境中的 Hadoop 依赖和配置。
:::

LakeSoul 发布 jar 包可以从 GitHub Releases 页面下载：https://github.com/lakesoul-io/LakeSoul/releases 。下载后请将 Jar 包放到 Spark 安装目录下的 jars 目录中：
```bash
wget https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-spark-2.4.0-spark-3.3.jar -P $SPARK_HOME/jars
```

如果访问 Github 有问题，也可以从如下链接下载：https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/lakesoul-spark-2.4.0-spark-3.3.jar

:::tip
从 2.1.0 版本起，LakeSoul 自身的依赖已经通过 shade 方式打包到一个 jar 包中。之前的版本是多个 jar 包以 tar.gz 压缩包的形式发布。
:::

## 启动 spark-shell 进行测试

### 首先为 LakeSoul 增加 PG 数据库配置
默认情况下，pg数据库连接到本地数据库，配置信息如下：
```txt
lakesoul.pg.driver=com.lakesoul.shaded.org.postgresql.Driver
lakesoul.pg.url=jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified
lakesoul.pg.username=lakesoul_test
lakesoul.pg.password=lakesoul_test
```

自定义 PG 数据库配置信息，需要在程序启动前增加一个环境变量 `lakesoul_home`，将配置文件信息引入进来。假如 PG 数据库配置信息文件路径名为：/opt/soft/pg.property，则在程序启动前需要添加这个环境变量：
```bash
export lakesoul_home=/opt/soft/pg.property
```

用户可以在这里自定义数据库配置信息，这样用户自定义 PG DB 的配置信息就会在 Spark 作业中生效。

### 进入 Spark 安装目录，启动 spark 交互式 shell：
  ```shell
  ./bin/spark-shell --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf spark.sql.defaultCatalog=lakesoul
  ```

## Spark 作业 LakeSoul 相关参数设置
可以将以下配置添加到 spark-defaults.conf 或者 Spark Session Builder 部分。

|Key | Value
|---|---|
spark.sql.extensions | com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension
spark.sql.catalog.lakesoul | org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
spark.sql.defaultCatalog | lakesoul