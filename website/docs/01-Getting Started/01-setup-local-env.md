# Quick Setup Environment

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 1. Set up a test environment in the Linux local file system
To store data on local disk, only a PostgreSQL database is required.

### 1.1 Start A Local PostgreSQL DB
The quickest way to start a pg DB is via docker container:
```shell
docker run -d --name lakesoul-test-pg -p5432:5432 -e POSTGRES_USER=lakesoul_test -e POSTGRES_PASSWORD=lakesoul_test -e POSTGRES_DB=lakesoul_test -d postgres:14.5
```

### 1.2 PG Database Initialization

Init PG database of LakeSoul using `script/meta_init.cql`.
Execute code blow in the LakeSoul base directory:
```
docker cp script/meta_init.sql lakesoul-test-pg:/

docker exec -i lakesoul-test-pg sh -c "PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f meta_init.sql"
```

### 1.3 Lakesoul PG Database Configuration Description:
By default, the PG database is connected to the local database. The configuration information is as follows,
```txt
lakesoul.pg.driver=com.lakesoul.shaded.org.postgresql.Driver
lakesoul.pg.url=jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified
lakesoul.pg.username=lakesoul_test
lakesoul.pg.password=lakesoul_test
```

LakeSoul supports to customize PG database configuration information. Add an environment variable `lakesoul_home` before starting the Spark program to include the configuration file information.

For example, the PG database configuration information file path name is: `/opt/soft/pg.property`, you need to add this environment variable before the program starts:
```
export lakesoul_home=/opt/soft/pg.property
```

You can put customized database configuration information in this file.

### 1.4 Install an Apache Spark environment
You could download spark distribution from https://spark.apache.org/downloads.html, and please choose spark version 3.3.0 or above. Note that the official package from Apache Spark does not include hadoop-cloud component. We provide a Spark package with Hadoop cloud dependencies, download it from https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/spark/spark-3.3.2-bin-hadoop3.tgz.

After unpacking spark package, you could find LakeSoul distribution jar from https://github.com/lakesoul-io/LakeSoul/releases. Download the jar file put it into `jars` directory of your spark environment.

```bash
wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/spark/spark-3.3.2-bin-hadoop-3.tgz
tar xf spark-3.3.2-bin-hadoop-3.tgz
export SPARK_HOME=${PWD}/spark-3.3.2-bin-hadoop3
wget https://github.com/lakesoul-io/LakeSoul/releases/download/vVAR::VERSION/lakesoul-spark-VAR::VERSION-spark-3.3.jar -P $SPARK_HOME/jars
```

:::tip
For production deployment on Hadoop, it's recommended to use spark release without bundled hadoop:

https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-without-hadoop.tgz

Refer to https://spark.apache.org/docs/latest/hadoop-provided.html on how to setup hadoop classpath.
:::

:::tip
Since 2.1.0, LakeSoul package all its dependencies into one single jar via maven shade plugin. Before that all jars were packaged into one tar.gz file.
:::

#### 1.4.1 Start spark-shell for testing LakeSoul
cd into the spark installation directory, and start an interactive spark-shell:
```shell
./bin/spark-shell --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf spark.sql.defaultCatalog=lakesoul
```

#### 1.4.2 Write data to object storage service
It is necessary to add information such as object storage access key, secret key and endpoint.
  ```shell
  ./bin/spark-shell --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf spark.sql.defaultCatalog=lakesoul --conf spark.hadoop.fs.s3a.access.key=XXXXXX --conf spark.hadoop.fs.s3a.secret.key=XXXXXX --conf spark.hadoop.fs.s3a.endpoint=XXXXXX --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
  ```

If it is a storage service compatible with S3 such as Minio, you also need to add `--conf spark.hadoop.fs.s3a.path.style.access=true`.
  
#### LakeSoul Spark Conf Parameters
Before start to use Lakesoul, we should add some paramethers in `spark-defaults.conf` or `Spark Session Builder`。

| Key | Value | Description |
|---|---|---|
spark.sql.extensions | com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension | extention name for spark sql
spark.sql.catalog.lakesoul | org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog | plug in LakeSoul's catalog
spark.sql.defaultCatalog | lakesoul | set default catalog for spark

### 1.5 Setup Flink environment
Download LakeSoul Flink jar: https://github.com/lakesoul-io/LakeSoul/releases/download/vVAR::VERSION/lakesoul-flink-1.17-VAR::VERSION.jar

Download Flink: https://dlcdn.apache.org/flink/flink-1.17.2/flink-1.17.2-bin-scala_2.12.tgz

#### 1.5.1 Start Flink SQL shell
After creating the pg database and `lakesoul_home` configuration file, place the LakeSoul Flink jars in the FLink directory.
Enter the Flink installation directory and execute the following command:
```shell
export lakesoul_home=/opt/soft/pg.property && ./bin/start-cluster.sh

export lakesoul_home=/opt/soft/pg.property && ./bin/sql-client.sh embedded -j lakesoul-flink-1.17-VAR::VERSION.jar
```

#### 1.5.2 Write data to object storage service
Access key, Secret key and Endpoint information need to be added to the Flink configuration file flink-conf.yaml
```yaml
s3.access-key: XXXXXX
s3.secret-key: XXXXXX
s3.endpoint: XXXXXX
```
If it is a storage service compatible with S3 such as Minio, you also need to add:
```yaml
s3.path.style.access: true
```

And place flink-s3-fs-hadoop.jar and flink-shaded-hadoop-2-uber-2.6.5-10.0.jar under Flink/lib
Download flink-s3-fs-hadoop.jar: https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.17.2/flink-s3-fs-hadoop-1.17.2.jar
Download flink-shaded-hadoop-2-uber-2.6.5-10.0.jar: https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.6.5-10.0/flink-shaded-hadoop-2-uber-2.6.5-10.0.jar

## 2. Start on Hadoop, Spark and Flink cluster environments
To deploy LakeSoul in a Hadoop cluster, you only need to add the relevant configuration information to the environment variables and Spark and FLink cluster configurations. For the Spark environment, please refer to [1.3](#13-Installation-spark-environment) for installation and deployment. For the Flink environment, please refer to [1.4](#14-flink-Local Environment Construction) for installation and deployment. It is recommended that the environments of Spark and Flink do not include Hadoop dependencies. Use the `SPARK_DIST_CLASSPATH` and `HADOOP_CLASSPATH` environment variables to introduce the Hadoop environment to avoid dependence on the Hadoop version.

The detailed configurations are as follows:

### 2.1 Add the following information to the Spark configuration file spark-defaults.conf
```properties
spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension
spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
spark.sql.defaultCatalog=lakesoul

spark.yarn.appMasterEnv.LAKESOUL_PG_DRIVER=com.lakesoul.shaded.org.postgresql.Driver
spark.yarn.appMasterEnv.LAKESOUL_PG_URL=jdbc:postgresql://127.0.0.1:5432/lakesoul_test?stringtype=unspecified
spark.yarn.appMasterEnv.LAKESOUL_PG_USERNAME=lakesoul_test
spark.yarn.appMasterEnv.LAKESOUL_PG_PASSWORD=lakesoul_test
```

### 2.2 Add the following information to the Flink configuration file flink-conf.yaml
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

In the above configurations, LakeSoul's PG URL connection address, user name, and password need to be modified accordingly according to the specific deployment of PostgreSQL.

### 2.3 Configuration Hadoop Environment
Configure global environment variable information on the client machine. Here you need to write the variable information into an env.sh file. 
Here the Hadoop version is 3.1.4.0-315, the Spark version is spark-3.3.2, and the Flink version is flink-1.17.2. Change Hadoop environment variables according to your Hadoop deployment. If your environment has been pre-configured with Hadoop, you can omit those Hadoop related envs.

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
After configuring the above information, execute the following command, and then you can submit the LakeSoul task to the yarn cluster for running on the client
```shell
source env.sh
```

## 3. Use Docker Compose to Start a Local Cluster
### 3.1 Docker Compose Files
We provide a docker compose env to quickly start a local PostgreSQL service and a MinIO S3 Storage service. The docker compose env is located under [lakesoul-docker-compose-env](https://github.com/lakesoul-io/LakeSoul/tree/main/docker/lakesoul-docker-compose-env).

### 3.2 Install Docker Compose
To install docker compose, please refer to [Install Docker Engine](https://docs.docker.com/engine/install/)

### 3.3 Start docker compose
To start the docker compose env, cd into the docker compose env dir, and execute the command:
```bash
cd docker/lakesoul-docker-compose-env/
docker compose up -d
```
Then use `docker compose ps` to check both services' statuses are `running(healthy)`. The PostgreSQL service would automatically setup the database and tables required by LakeSoul Meta. And the MinIO service would setup a public bucket. You can change the user, password, database name and MinIO bucket name accordingly in the `docker-compose.yml` file.

### 3.4 Run LakeSoul Tests in Docker Compose Env
#### 3.4.1 Prepare LakeSoul Properties File
```ini title="lakesoul.properties"
lakesoul.pg.driver=com.lakesoul.shaded.org.postgresql.Driver
lakesoul.pg.url=jdbc:postgresql://lakesoul-docker-compose-env-lakesoul-meta-db-1:5432/lakesoul_test?stringtype=unspecified
lakesoul.pg.username=lakesoul_test
lakesoul.pg.password=lakesoul_test
```
####  3.4.2 Prepare Spark Image
You could use bitnami's Spark 3.3 docker image with packaged hadoop denendencies:
```bash
docker pull bitnami/spark:3.3.1
```

####  3.4.3 Start Spark Shell
```bash
docker run --net lakesoul-docker-compose-env_default --rm -ti \
    -v $(pwd)/lakesoul.properties:/opt/spark/work-dir/lakesoul.properties \
    --env lakesoul_home=/opt/spark/work-dir/lakesoul.properties bitnami/spark:3.3.1 \
    spark-shell \
    --packages com.dmetasoul:lakesoul-spark:spark-3.3-VAR::VERSION \
    --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension \
    --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog \
    --conf spark.sql.defaultCatalog=lakesoul \
    --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.buffer.dir=/opt/spark/work-dir/s3a \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider
```

#### 3.4.4 Execute LakeSoul Scala APIs
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

#### 3.4.5 Verify Data Written Successfully
Open link http://127.0.0.1:9001/buckets/lakesoul-test-bucket/browse/ in your browser to verify that LakeSoul table has been written to MinIO successfully.
Use minioadmin1:minioadmin1 to login into MinIO's console.

### 3.5 Cleanup Meta Tables and MinIO Bucket
To cleanup all contents in LakeSoul meta tables, execute:
```bash
docker exec -ti lakesoul-docker-compose-env-lakesoul-meta-db-1 psql -h localhost -U lakesoul_test -d lakesoul_test -f /meta_cleanup.sql
```
To cleanup all contents in MinIO bucket, execute:
```bash
docker run --net lakesoul-docker-compose-env_default --rm -t bitnami/spark:3.3.1 aws --no-sign-request --endpoint-url http://minio:9000 s3 rm --recursive s3://lakesoul-test-bucket/
```

### 3.6 Shutdown Docker Compose Env
```bash
cd docker/lakesoul-docker-compose-env/
docker compose stop
docker compose down
```