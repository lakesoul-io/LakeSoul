# Setup a Local Environment

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Start A Local PostgreSQL DB
The quickest way to start a pg DB is via docker container:
```shell
docker run -d --name lakesoul-test-pg -p5432:5432 -e POSTGRES_USER=lakesoul_test -e POSTGRES_PASSWORD=lakesoul_test -e POSTGRES_DB=lakesoul_test -d postgres:14.5
```

## PG Database Initialization

Init PG database of LakeSoul using `script/meta_init.cql`.

  ```
  PGPASSWORD=lakesoul_test psql -h localhost -p 5432 -U lakesoul_test -f script/meta_init.sql
  ```

## Lakesoul PG Database Configuration Description:
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

## Install an Apache Spark environment
You could download spark distribution from https://spark.apache.org/downloads.html, and please choose spark version 3.3.0 or above. Note that the official package from Apache Spark does not include hadoop-cloud component. We provide a Spark package with Hadoop cloud dependencies, download it from https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/spark/spark-3.3.2-bin-hadoop3.tgz.

After unpacking spark package, you could find LakeSoul distribution jar from https://github.com/lakesoul-io/LakeSoul/releases. Download the jar file put it into `jars` directory of your spark environment.

```bash
wget https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/spark/spark-3.3.2-bin-hadoop-3.tgz
tar xf spark-3.3.2-bin-hadoop-3.tgz
export SPARK_HOME=${PWD}/spark-3.3.2-bin-hadoop3
wget https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-spark-2.4.0-spark-3.3.jar -P $SPARK_HOME/jars
```

:::tip
For production deployment on Hadoop, it's recommended to use spark release without bundled hadoop:

https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-without-hadoop.tgz

Refer to https://spark.apache.org/docs/latest/hadoop-provided.html on how to setup hadoop classpath.
:::

:::tip
Since 2.1.0, LakeSoul package all its dependencies into one single jar via maven shade plugin. Before that all jars were packaged into one tar.gz file.
:::

## Start spark-shell for testing LakeSoul
cd into the spark installation directory, and start an interactive spark-shell:
```shell
./bin/spark-shell --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf spark.sql.defaultCatalog=lakesoul
```
  
## LakeSoul Spark Conf Parameters
Before start to use Lakesoul, we should add some paramethers in `spark-defaults.conf` or `Spark Session Builder`。

| Key | Value | Description |
|---|---|---|
spark.sql.extensions | com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension | extention name for spark sql
spark.sql.catalog.lakesoul | org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog | plug in LakeSoul's catalog
spark.sql.defaultCatalog | lakesoul | set default catalog for spark