# Setup Your Spark and Flink Project/Job

## Required Spark Version
LakeSoul is currently available with Scala version 2.12 and Spark version 3.3.

## Setup (Py)Spark Shell or Spark SQL Shell
To use `spark-shell`, `pyspark` or `spark-sql` shells, you should include LakeSoul's dependencies. There are two approaches to achieve this.

### Use Maven Coordinates via --packages
```bash
spark-shell --packages com.dmetasoul:lakesoul-spark:2.3.0-spark-3.3
```

### Use Local Packages
You can find the LakeSoul packages from our release page: [Releases](https://github.com/lakesoul-io/LakeSoul/releases).
Download the jar file and pass it to `spark-submit`.
```bash
spark-submit --jars "lakesoul-spark-2.3.0-spark-3.3.jar"
```

Or you could directly put the jar into `$SPARK_HOME/jars`

## Setup Java/Scala Project for Spark
Include maven dependencies in your project:
```xml
<dependency>
    <groupId>com.dmetasoul</groupId>
    <artifactId>lakesoul</artifactId>
    <version>2.3.0-spark-3.3</version>
</dependency>
```

## Pass `lakesoul_home` Environment Variable to Your Spark Job
If you are using Spark's local or client mode, you could just export the env var in your shell:
```bash
export lakesoul_home=/path/to/lakesoul.properties
```

If you are using Spark's cluster mode, in which the driver would also be scheduled into Yarn or K8s cluster, you can setup the driver's env:
- For Hadoop Yarn, pass `--conf spark.yarn.appMasterEnv.lakesoul_home=lakesoul.properties --files /path/to/lakesoul.properties` to `spark-submit` command;
- For K8s, pass `--conf spark.kubernetes.driverEnv.lakesoul_home=lakesoul.properties --files /path/to/lakesoul.properties` to `spark-submit` command.

## Required Flink Version
Currently Flink 1.14 is supported.

## Setup Metadata Database Connection for Flink

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
LakeSoul may use extra amount of off-heap memory, consider to increase the off heap memory size for task manager:
```yaml
taskmanager.memory.task.off-heap.size: 3000m
```
:::

## Add LakeSoul Jar to Flink's directory
Download LakeSoul Flink Jar from: https://github.com/lakesoul-io/LakeSoul/releases/download/v2.3.0/lakesoul-flink-2.3.0-flink-1.14.jar

And put the jar file under `$FLINK_HOME/lib`. After this, you could start flink session cluster or application as usual.

## Add LakeSoul Flink Maven Dependency in Your Java Project

Add the following to your project's pom.xml
```xml
<dependency>
    <groupId>com.dmetasoul</groupId>
    <artifactId>lakesoul</artifactId>
    <version>2.3.0-flink-1.14</version>
</dependency>
```