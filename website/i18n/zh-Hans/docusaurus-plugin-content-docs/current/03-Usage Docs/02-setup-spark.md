# 设置 Spark/Flink 工程/作业

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Spark 工程和作业的设置
### Spark 版本
LakeSoul 目前支持 Spark 3.3 + Scala 2.12.

### 设置 Spark Shell (包括 pyspark shell 和 spark sql shell)
使用 `spark-shell`、`pyspark` 或者 `spark-sql` 交互式查询, 需要添加 LakeSoul 的依赖和配置，有两种方法：

#### 使用 `--packages` 传 Maven 仓库和包名
```bash
spark-shell --packages com.dmetasoul:lakesoul-spark:2.4.0-spark-3.3
```

#### 使用打包好的 LakeSoul 包
可以从 [Releases](https://github.com/lakesoul-io/LakeSoul/releases) 页面下载已经打包好的 LakeSoul Jar 包。
下载 jar 并传给 `spark-submit` 命令：
```bash
spark-submit --jars "lakesoul-spark-2.4.0-spark-3.3.jar"
```

#### 直接将 Jar 包放在 Spark 环境中
可以将 Jar 包下载后，放在 $SPARK_HOME/jars 中。

Jar 包可以从 Github Release 页面下载：https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-spark-2.4.0-spark-3.3.jar

或者从国内地址下载：https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/lakesoul-spark-2.4.0-spark-3.3.jar

### 设置 Java/Scala 项目
增加以下 Maven 依赖项:
```xml
<dependency>
    <groupId>com.dmetasoul</groupId>
    <artifactId>lakesoul-spark</artifactId>
    <version>2.4.0-spark-3.3</version>
</dependency>
```

### 为 Spark 作业设置 `lakesoul_home` 环境变量
为了能够正确连接 LakeSoul 元数据库，Spark 使用 local 或 client 模式时，driver 是在本地运行，这时可以直接在 shell 中 export 环境变量：
```bash
export lakesoul_home=/path/to/lakesoul.properties
```

Spark 使用 cluster 模式时，driver 也运行在集群上，根据集群部署方式为 `spark-submit` 命令增加参数：
- 对于 Hadoop Yarn 集群, 增加命令行参数 `--conf spark.yarn.appMasterEnv.lakesoul_home=lakesoul.properties --files /path/to/lakesoul.properties`；
- 对于 K8s 集群，增加命令行参数 `--conf spark.kubernetes.driverEnv.lakesoul_home=lakesoul.properties --files /path/to/lakesoul.properties` to `spark-submit` command.

### 设置 Spark SQL Extension
LakeSoul 通过 Spark SQL Extension 机制来实现一些查询计划改写的扩展，需要为 Spark 作业增加以下配置：
```properties
spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension
```

### 设置 Spark 的 Catalog
LakeSoul 实现了 Spark 3 的 CatalogPlugin 接口，可以作为独立的 Catalog 插件让 Spark 加载。在 Spark 作业中增加如下配置：

```properties
spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
```

该配置增加了一个名为 `lakesoul` 的 Catalog。为了方便 SQL 中使用，也可以将该 Catalog 设置为默认的 Catalog：

```properties
spark.sql.defaultCatalog=lakesoul
```

通过如上配置，默认会通过 LakeSoul Catalog 来查找所有 database 和表。如果需要同时访问 Hive 等外部 catalog，需要在表名前加上对应 catalog 名字。例如在 Spark 中启用 Hive 作为 Session Catalog，则访问 Hive 表时需要加上 `spark_catalog` 前缀。

:::tip
在 2.0.1 及之前版本，LakeSoul 只实现了 Session Catalog 接口，只能通过 `spark.sql.catalog.spark_catalog=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog` 的方式来设置。但是由于 Spark 在 3.3 之前，Session Catalog 对 DataSource V2 表的支持不全，从 2.1.0 起 LakeSoul 的 Catalog 更改为非 session 的实现。

从 2.1.0 起，你仍然可以将 LakeSoul 设置为 Session Catalog，即设置名为 `spark_catalog` ，但是这样就无法再访问到 Hive 表。
:::

### 设置 Spark 的 SessionCatalog
如果不需要访问 Hive，也可以将 LakeSoul 直接设置为 SessionCatalog：
```properties
# 设置 LakeSoul 为 session catalog
spark.sql.catalog.spark_catalog org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
# 禁用 hive
spark.sql.catalogImplementation in-memory
```

### 在建表时不指定 provider/format (即不写 using/stored as) 的情况下默认使用 LakeSoul
```properties
spark.sql.legacy.createHiveTableByDefault false
spark.sql.sources.default lakesoul
```

## Flink 工程/作业的配置

### 所需的 Flink 版本
目前 2.4 版本，支持 Flink 1.17。

### 为 Flink 设置元数据数据库连接

在 `$FLINK_HOME/conf/flink-conf.yaml` 中添加如下配置：
```yaml
containerized.master.env.LAKESOUL_PG_DRIVER：com.lakesoul.shaded.org.postgresql.Driver
containerized.master.env.LAKESOUL_PG_USERNAME: root
containerized.master.env.LAKESOUL_PG_PASSWORD: root
containerized.master.env.LAKESOUL_PG_URL: jdbc:postgresql://localhost:5432/test_lakesoul_meta?stringtype=未指定
containerized.taskmanager.env.LAKESOUL_PG_DRIVER：com.lakesoul.shaded.org.postgresql.Driver
containerized.taskmanager.env.LAKESOUL_PG_USERNAME: root
containerized.taskmanager.env.LAKESOUL_PG_PASSWORD: root
containerized.taskmanager.env.LAKESOUL_PG_URL: jdbc:postgresql://localhost:5432/test_lakesoul_meta?stringtype=未指定
```

请注意，需要同时设置 master 和 taskmanager 环境变量。

:::tip
Postgres 数据库的连接信息、用户名和密码需要根据实际部署进行修改。
:::

::: caution
注意，如果使用 Session 方式启动作业，即以客户端的方式将作业提交给 Flink Standalone Cluster，作为客户端的 `flink run` 不会读取上面的配置，所以需要单独配置环境变量， 即：

```bash
export LAKESOUL_PG_DRIVER=com.lakesoul.shaded.org.postgresql.Driver
export LAKESOUL_PG_URL=jdbc:postgresql://localhost:5432/test_lakesoul_meta?stringtype=unspecified
export LAKESOUL_PG_USERNAME=root
export LAKESOUL_PG_PASSWORD=root
```
::::

:::tip
LakeSoul 需要使用相对多一些的堆外内存，建议适当增加 Task Manager 的堆外内存配置，例如：
```yaml
taskmanager.memory.task.off-heap.size: 3000m
```
:::


### 添加 LakeSoul Jar 到 Flink 部署的目录
从以下地址下载 LakeSoul Flink Jar：https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-flink-2.4.0-flink-1.17.jar

并将 jar 文件放在 `$FLINK_HOME/lib` 下。在此之后，您可以像往常一样启动 flink 会话集群或应用程序。

:::tip
如果需要访问 S3，还需要下载与 Flink 版本对应的 `[flink-s3-hadoop](https://mvnrepository.com/artifact/org.apache.flink/flink-s3-fs-hadoop)`，并放到 `$FLINK_HOME/lib` 目录下。

如果需要访问 Hadoop 环境，可以声明 Hadoop Classpath 环境变量：
```bash
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
```
具体可以参考：[Flink on Hadoop](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/resource-providers/yarn/)
:::

### 在你的 Java 项目中添加 LakeSoul Flink Maven 依赖

将以下内容添加到项目的 pom.xml
```xml
<dependency>
     <groupId>com.dmetasoul</groupId>
     <artifactId>lakesoul</artifactId>
     <version>2.4.0-flink-1.17</version>
</dependency>
```