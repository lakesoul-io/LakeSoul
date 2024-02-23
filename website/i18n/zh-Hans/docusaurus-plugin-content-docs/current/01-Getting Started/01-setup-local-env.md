# 快速搭建运行环境

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 1. 在 Linux 本地搭建测试环境
将数据存储在本地磁盘上，只需要一行命令启动 PostgreSQL 数据库即可在单机环境快速体验 LakeSoul 的功能。
### 1.1 启动一个 PostgreSQL 数据库
可以通过docker使用下面命令快速搭建一个pg数据库：
```bash
docker run -d --name lakesoul-test-pg -p5432:5432 -e POSTGRES_USER=lakesoul_test -e POSTGRES_PASSWORD=lakesoul_test -e POSTGRES_DB=lakesoul_test -d swr.cn-north-4.myhuaweicloud.com/dmetasoul-repo/postgres:14.5
```

### 1.2 PG 数据库初始化
在 LakeSoul 代码库目录下执行：
```bash
## 将初始化脚本copy到容器中
docker cp script/meta_init.sql lakesoul-test-pg:/

## 执行初始化命令
docker exec -i lakesoul-test-pg sh -c "PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f meta_init.sql"
```

### 1.3 安装 Spark 环境
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
wget https://github.com/lakesoul-io/LakeSoul/releases/download/vVAR::VERSION/lakesoul-spark-spark-3.3-VAR::VERSION.jar -P $SPARK_HOME/jars
```

如果访问 Github 有问题，也可以从如下链接下载：https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/lakesoul-spark-spark-3.3-VAR::VERSION.jar

:::tip
从 2.1.0 版本起，LakeSoul 自身的依赖已经通过 shade 方式打包到一个 jar 包中。之前的版本是多个 jar 包以 tar.gz 压缩包的形式发布。
:::

#### 1.3.1 启动 spark-shell 进行测试

#### 首先为 LakeSoul 增加 PG 数据库配置
默认情况下，pg数据库连接到本地数据库，配置信息如下：
```properties
lakesoul.pg.driver=com.lakesoul.shaded.org.postgresql.Driver
lakesoul.pg.url=jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified
lakesoul.pg.username=lakesoul_test
lakesoul.pg.password=lakesoul_test
```

自定义 PG 数据库配置信息，需要在程序启动前增加一个环境变量 `lakesoul_home`，将配置文件信息引入进来。假如 PG 数据库配置信息文件路径名为：/opt/soft/pg.property，则在程序启动前需要添加这个环境变量：
```shell
export lakesoul_home=/opt/soft/pg.property
```

用户可以在这里自定义数据库配置信息，这样用户自定义 PG DB 的配置信息就会在 Spark 作业中生效。

#### 1.3.2 进入 Spark 安装目录，启动 spark 交互式 shell：
  ```shell
  ./bin/spark-shell --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf spark.sql.defaultCatalog=lakesoul
  ```

#### 1.3.3 将数据写入对象存储服务
需要添加对象存储 access key, secret key 和 endpoint 等信息
  ```shell
  ./bin/spark-shell --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf spark.sql.defaultCatalog=lakesoul --conf spark.hadoop.fs.s3a.access.key=XXXXXX --conf spark.hadoop.fs.s3a.secret.key=XXXXXX --conf spark.hadoop.fs.s3a.endpoint=XXXXXX --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
  ```

如果是 Minio 等兼容 S3 的存储服务，还需要添加 `--conf spark.hadoop.fs.s3a.path.style.access=true`。

#### Spark 作业 LakeSoul 相关参数设置
可以将以下配置添加到 spark-defaults.conf 或者 Spark Session Builder 部分。

|Key | Value
|---|---|
spark.sql.extensions | com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension
spark.sql.catalog.lakesoul | org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
spark.sql.defaultCatalog | lakesoul

### 1.4 Flink 本地环境搭建
以当前发布最新版本为例，LakeSoul Flink jar 包下载地址为：https://github.com/lakesoul-io/LakeSoul/releases/download/vVAR::VERSION/lakesoul-flink-flink-1.17-VAR::VERSION.jar

最新版本支持 flink 集群为1.17，Flink jar下载地址为：https://dlcdn.apache.org/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz

#### 1.4.1 启动Flink SQL shell
在创建好 pg 数据库和 `lakesoul_home` 配置文件后，通过以下方式可以进入 SQL Client 客户端，将LakeSoul Flink jar放在 FLink 目录下，
进入 Flink 安装目录，执行以下命令：
```shell
# 启动 flink 集群
export lakesoul_home=/opt/soft/pg.property && ./bin/start-cluster.sh

# 启动 flink sql client
export lakesoul_home=/opt/soft/pg.property && ./bin/sql-client.sh embedded -j lakesoul-flink-flink-1.17-VAR::VERSION.jar
```

#### 1.4.2 将数据写入对象存储服务
需要在配置文件 flink-conf.yaml 添加 access key, secret key 和 endpoint 等配置。
```yaml
s3.access-key: XXXXXX
s3.secret-key: XXXXXX
s3.endpoint: XXXXXX
```
如果是 Minio 等兼容 S3 的存储服务，还需要添加：
```yaml
s3.path.style.access: true
```

将flink-s3-fs-hadoop.jar 和 flink-shaded-hadoop-2-uber-2.6.5-10.0.jar 放到 Flink/lib 下
flink-s3-fs-hadoop.jar 下载地址为：https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.2/flink-s3-fs-hadoop-1.17.2.jar
flink-shaded-hadoop-2-uber-2.6.5-10.0.jar 下载地址为：https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.6.5-10.0/flink-shaded-hadoop-2-uber-2.6.5-10.0.jar

## 2. 在 Hadoop、Spark 和 FLink 集群环境下运行
在 hadoop 集群中应用 LakeSoul 服务，只需要将相关配置信息假如环境变量中以及 Spark、FLink 集群配置中即可。Spark 环境可以参考 [1.3](#13-安装-spark-环境) 安装部署，Flink 环境可以参考 [1.4](#14-flink-本地环境搭建) 安装部署。Spark、Flink 的环境推荐不包含 Hadoop 依赖，通过 `SPARK_DIST_CLASSPATH`、`HADOOP_CLASSPATH` 环境变量引入 Hadoop 环境，避免对 Hadoop 版本的强依赖。

具体配置方法如下：

### 2.1 在 Spark 配置文件 `$SPARK_HOME/conf/spark-defaults.conf` 添加如下信息
```properties
spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension
spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
spark.sql.defaultCatalog=lakesoul

spark.yarn.appMasterEnv.LAKESOUL_PG_DRIVER=com.lakesoul.shaded.org.postgresql.Driver
spark.yarn.appMasterEnv.LAKESOUL_PG_URL=jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified
spark.yarn.appMasterEnv.LAKESOUL_PG_USERNAME=lakesoul_test
spark.yarn.appMasterEnv.LAKESOUL_PG_PASSWORD=lakesoul_test
```

### 2.2 在Flink 配置文件 `$FLINK_HOME/conf/flink-conf.yaml` 添加如下信息
```yaml
containerized.master.env.LAKESOUL_PG_DRIVER: com.lakesoul.shaded.org.postgresql.Driver
containerized.master.env.LAKESOUL_PG_USERNAME: postgres
containerized.master.env.LAKESOUL_PG_PASSWORD: postgres123
containerized.master.env.LAKESOUL_PG_URL: jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified
containerized.taskmanager.env.LAKESOUL_PG_DRIVER: com.lakesoul.shaded.org.postgresql.Driver
containerized.taskmanager.env.LAKESOUL_PG_USERNAME: lakesoul_test
containerized.taskmanager.env.LAKESOUL_PG_PASSWORD: lakesoul_test
containerized.taskmanager.env.LAKESOUL_PG_URL: jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified
```

:::tip
以上配置中，LakeSoul 的 PG 数据库连接地址、用户名、密码，需要根据 PostgreSQL 具体的部署做相应的修改。
:::

:::tip
如果需要在 Hadoop 环境中访问 S3 等对象存储，也需要添加 `spark.hadoop.fs.s3a.*`(Spark)、`s3.*`(Flink) 等相应配置项。
* Spark 环境中需要 `hadoop-aws-$hadoop_version.jar` 及其依赖(建议放在 `$SPARK_HOME/jars` 目录下)。可以从 https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws 这里根据 Hadoop 版本下载，与 Spark 版本无关。同时还需要下载 `aws-java-sdk-bundle`：https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-bundle
* Flink 环境中需要 `flink-s3-hadoop-$flink_version.jar`(建议放在 `$FLINK_HOME/plugins/s3` 目录下)。可以从 https://mvnrepository.com/artifact/org.apache.flink/flink-s3-fs-hadoop 这里根据 Flink 版本下载(LakeSoul 目前最新版本对 Flink 版本要求是 1.17)。
:::

### 2.3 在客户端机器上配置全局环境变量信息
这里需要用到变量信息写到一个 env.sh 文件中，这里 Hadoop 版本为 3.1.4.0-315，Spark 版本为 spark-3.3.2， Flink 版本为 flink-1.17.2，Hadoop 环境可以根据实际情况配置。如果客户机上已经有默认的 Hadoop 环境变量配置，则前面 Hadoop 的变量可以省去：
```shell
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="/usr/hdp/3.1.4.0-315/hadoop"
export HADOOP_HDFS_HOME="/usr/hdp/3.1.4.0-315/hadoop-hdfs"
export HADOOP_MAPRED_HOME="/usr/hdp/3.1.4.0-315/hadoop-mapreduce"
export HADOOP_YARN_HOME="/usr/hdp/3.1.4.0-315/hadoop-yarn"
export HADOOP_LIBEXEC_DIR="/usr/hdp/3.1.4.0-315/hadoop/libexec"
export HADOOP_CONF_DIR="/usr/hdp/3.1.4.0-315/hadoop/conf"

export SPARK_HOME=/usr/hdp/spark-3.3.2-bin-without-hadoop-ddf
export SPARK_CONF_DIR=/home/lakesoul/lakesoul_hadoop_ci/LakeSoul-main/LakeSoul/script/benchmark/hadoop/spark-conf

export FLINK_HOME=/opt/flink-1.17.2
export FLINK_CONF_DIR=/opt/flink-1.17.2/conf
export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$FLINK_HOME/bin:$JAVA_HOME/bin:$PATH
export HADOOP_CLASSPATH=$(hadoop classpath)
export SPARK_DIST_CLASSPATH=$HADOOP_CLASSPATH
export LAKESOUL_PG_DRIVER=com.lakesoul.shaded.org.postgresql.Driver
export LAKESOUL_PG_URL=jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified
export LAKESOUL_PG_USERNAME=lakesoul_test
export LAKESOUL_PG_PASSWORD=lakesoul_test
```
配置好如上信息后，执行以下命令，然后便可以在客户端将 LakeSoul 任务提交到 yarn 集群上运行
```shell
source env.sh
```

## 3. 使用 Docker Compose 搭建本地集群环境

### 3.1 Docker Compose 文件
我们提供了 docker compose 环境方便快速启动一个本地的 PostgreSQL 服务和一个 MinIO S3 存储服务。Docker Compose 环境可以在代码库中找到：[lakesoul-docker-compose-env](https://github.com/lakesoul-io/LakeSoul/tree/main/docker/lakesoul-docker-compose-env).

### 3.2 安装 Docker Compose
安装 Docker Compose 可以参考 Docker 官方文档：[Install Docker Engine](https://docs.docker.com/engine/install/)

### 3.3 启动 Docker Compose 环境
启动 Docker Compose 环境，执行以下命令：
```bash
cd docker/lakesoul-docker-compose-env/
docker compose up -d
```
然后可以使用 `docker compose ps` 命令来检查服务状态是否是 `running`. PostgreSQL 服务会自动初始化好 LakeSoul 需要的 database 和 表结构。MinIO 服务会创建一个公共读写的桶。PostgreSQL 的用户名、密码、DB名字、MinIO 的桶名可以在 `docker-compose.yml` 文件中修改。

### 3.4 在 Docker Compose 环境中运行 LakeSoul 测试
#### 3.4.1 准备 LakeSoul 配置文件
```ini title="lakesoul.properties"
lakesoul.pg.driver=com.lakesoul.shaded.org.postgresql.Driver
lakesoul.pg.url=jdbc:postgresql://lakesoul-docker-compose-env-lakesoul-meta-db-1:5432/lakesoul_test?stringtype=unspecified
lakesoul.pg.username=lakesoul_test
lakesoul.pg.password=lakesoul_test
```
#### 3.4.2 准备 Spark 镜像
可以使用 bitnami Spark 镜像：
```bash
docker pull bitnami/spark:3.3.1
```

#### 3.4.3 启动 Spark Shell
```bash
docker run --net lakesoul-docker-compose-env_default --rm -ti \
    -v $(pwd)/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties \
    --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 \
    spark-shell \
    --packages com.dmetasoul:lakesoul-spark:2.4.0-spark-3.3 \
    --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension \
    --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog \
    --conf spark.sql.defaultCatalog=lakesoul \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.buffer.dir=/opt/spark/work-dir/s3a \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider
```

#### 3.4.4 执行 LakeSoul Scala API
```scala
val tablePath= "s3://lakesoul-test-bucket/test_table"
val df = Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date","id","name")
df.write
  .mode("append")
  .format("lakesoul")
  .option("rangePartitions","date")
  .option("hashPartitions","id")
  .option("hashBucketNum","2")
  .save(tablePath)
```

#### 3.4.5 检查数据是否成功写入
可以打开链接 http://127.0.0.1:9001/buckets/lakesoul-test-bucket/browse/ 查看数据是否已经成功写入。
MinIO console 的登录用户名密码是 minioadmin1:minioadmin1。

### 3.5 清理元数据表和 MinIO 桶
清理元数据表内容:
```bash
docker exec -ti lakesoul-docker-compose-env-lakesoul-meta-db-1 psql -h localhost -U lakesoul_test -d lakesoul_test -f /meta_cleanup.sql
```
清理 MinIO 桶内容:
```bash
docker run --net lakesoul-docker-compose-env_default --rm -t bitnami/spark:3.3.1 aws --no-sign-request --endpoint-url http://minio:9000 s3 rm --recursive s3://lakesoul-test-bucket/
```

### 3.6 停止 Docker Compose 环境
```bash
cd docker/lakesoul-docker-compose-env/
docker compose stop
docker compose down
```
