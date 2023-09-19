# Setup Your Spark and Flink Project/Job

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Setup Spark Project or Job

### Required Spark Version
LakeSoul is currently available with Scala version 2.12 and Spark version 3.3.

### Setup (Py)Spark Shell or Spark SQL Shell
To use `spark-shell`, `pyspark` or `spark-sql` shells, you should include LakeSoul's dependencies. There are two approaches to achieve this.

#### Use Maven Coordinates via --packages
```bash
spark-shell --packages com.dmetasoul:lakesoul-spark:2.4.0-spark-3.3
```

#### Use Local Packages
You can find the LakeSoul packages from our release page: [Releases](https://github.com/lakesoul-io/LakeSoul/releases).
Download the jar file and pass it to `spark-submit`.
```bash
spark-submit --jars "lakesoul-spark-2.4.0-spark-3.3.jar"
```

Or you could directly put the jar into `$SPARK_HOME/jars`

### Setup Java/Scala Project for Spark
Include maven dependencies in your project:
```xml
<dependency>
    <groupId>com.dmetasoul</groupId>
    <artifactId>lakesoul</artifactId>
    <version>2.4.0-spark-3.3</version>
</dependency>
```

### Pass `lakesoul_home` Environment Variable to Your Spark Job
If you are using Spark's local or client mode, you could just export the env var in your shell:
```bash
export lakesoul_home=/path/to/lakesoul.properties
```

If you are using Spark's cluster mode, in which the driver would also be scheduled into Yarn or K8s cluster, you can setup the driver's env:
- For Hadoop Yarn, pass `--conf spark.yarn.appMasterEnv.lakesoul_home=lakesoul.properties --files /path/to/lakesoul.properties` to `spark-submit` command;
- For K8s, pass `--conf spark.kubernetes.driverEnv.lakesoul_home=lakesoul.properties --files /path/to/lakesoul.properties` to `spark-submit` command.

### Set Spark SQL Extension
LakeSoul implements some query plan rewriting extensions through the Spark SQL Extension mechanism, and the following configuration needs to be added to the Spark job:
```properties
spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension
```

### Set Spark's Catalog
LakeSoul implements the CatalogPlugin interface of Spark 3, which can be loaded by Spark as an independent Catalog plugin. Add the following configuration to the Spark job:

```properties
spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
```

This configuration adds a Catalog called `lakesoul`. For convenience in SQL, you can also set the Catalog as the default Catalog:

```properties
spark.sql.defaultCatalog=lakesoul
```

With the above configuration, all databases and tables will be found through LakeSoul Catalog by default. If you need to access external catalogs such as Hive at the same time, you need to add the corresponding catalog name before the table name. For example, if Hive is enabled as the Session Catalog in Spark, the `spark_catalog` prefix needs to be added when accessing the Hive table.

:::tip
In versions 2.0.1 and earlier, LakeSoul only implements the Session Catalog interface, which can only be set through `spark.sql.catalog.spark_catalog=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog`. However, before Spark 3.3, the Session Catalog did not fully support the DataSource V2 table. From 2.1.0 onwards, LakeSoul's Catalog has been changed to a non-session implementation.

From 2.1.0 onwards, you can still set LakeSoul as Session Catalog, which is called `spark_catalog`, but you can no longer access Hive tables.
:::

### Set Spark's SessionCatalog
If you don't need to access Hive, you can also set LakeSoul directly as SessionCatalog:
```properties
# Set LakeSoul as session catalog
spark.sql.catalog.spark_catalog org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog
# disable hive
spark.sql.catalogImplementation in-memory
```

### Use LakeSoul by default when the provider/format is not specified (without using/stored as clause) when creating the table
```properties
spark.sql.legacy.createHiveTableByDefault false
spark.sql.sources.default lakesoul
```

## Setup Flink Project or Job

### Required Flink Version
Since 2.4.0, Flink version 1.17 is supported.

### Setup Metadata Database Connection for Flink

Add the following configuration to `$FLINK_HOME/conf/flink-conf.yaml`:
```yaml
containerized.master.env.LAKESOUL_PG_DRIVER: com.lakesoul.shaded.org.postgresql.Driver
containerized.master.env.LAKESOUL_PG_USERNAME: root
containerized.master.env.LAKESOUL_PG_PASSWORD: root
containerized.master.env.LAKESOUL_PG_URL: jdbc:postgresql://localhost:5432/test_lakesoul_meta?stringtype=unspecified
containerized.taskmanager.env.LAKESOUL_PG_DRIVER: com.lakesoul.shaded.org.postgresql.Driver
containerized.taskmanager.env.LAKESOUL_PG_USERNAME: root
containerized.taskmanager.env.LAKESOUL_PG_PASSWORD: root
containerized.taskmanager.env.LAKESOUL_PG_URL: jdbc:postgresql://localhost:5432/test_lakesoul_meta?stringtype=unspecified
```

Note that both the master and taskmanager environment variables need to be set.

:::tip
The connection information, username and password of the Postgres database need to be modified according to the actual deployment.
:::

:::caution
Note that if you use Session mode to start a job, that is, submit the job to Flink Standalone Cluster as a client, `flink run` as a client will not read the above configuration, so you need to configure the environment variables separately, namely:

```bash
export LAKESOUL_PG_DRIVER=com.lakesoul.shaded.org.postgresql.Driver
export LAKESOUL_PG_URL=jdbc:postgresql://localhost:5432/test_lakesoul_meta?stringtype=unspecified
export LAKESOUL_PG_USERNAME=root
export LAKESOUL_PG_PASSWORD=root
````
:::

:::tip
If you need to access S3, you also need to download `[flink-s3-hadoop](https://mvnrepository.com/artifact/org.apache.flink/flink-s3-fs-hadoop)` corresponding to the Flink version, and put to the `$FLINK_HOME/lib` directory.

If access to the Hadoop environment is required, the Hadoop Classpath environment variable can be declared:
```bash
export HADOOP_CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath`
```
For details, please refer to: [Flink on Hadoop](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/resource-providers/yarn/)
:::

:::tip
LakeSoul may use extra amount of off-heap memory, consider to increase the off heap memory size for task manager:
```yaml
taskmanager.memory.task.off-heap.size: 3000m
```
:::

### Add LakeSoul Jar to Flink's directory
Download LakeSoul Flink Jar from: https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-flink-2.4.0-flink-1.17.jar

And put the jar file under `$FLINK_HOME/lib`. After this, you could start flink session cluster or application as usual.

### Add LakeSoul Flink Maven Dependency in Your Java Project

Add the following to your project's pom.xml
```xml
<dependency>
    <groupId>com.dmetasoul</groupId>
    <artifactId>lakesoul</artifactId>
    <version>2.4.0-flink-1.17</version>
</dependency>
```