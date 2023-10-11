# Use Kyuubi JDBC to Access Lakesoul's Table

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

:::tip
Available since version 2.4.
:::

LakeSoul implements Flink/Spark Connector.We could use Spark/Flink SQL queries towards Lakesoul by using kyuubi.


## Requirements

|Components | Version|
|-----------|--------|
| Kyuubi | 1.8  |
| Spark  | 3.3  |
| Flink  | 1.17 |
| LakeSoul | 2.4.0 |
| Java     | 1.8 |

The operating environment is Linux, and Spark, Flink, and Kyuubi have been installed. It is recommended to use Hadoop Yarn to run the Kyuubi Engine. Also, you could start local spark/flink cluster.

[Deploy Kyuubi engines on Yarn](https://kyuubi.readthedocs.io/en/v1.7.3/deployment/engine_on_yarn.html).

## Flink SQL Access Lakesoul's Table

### 1. Dependencies

Download LakeSoul Flink Jar from: https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-flink-2.4.0-flink-1.17.jar

And put the jar file under `$FLINK_HOME/lib`.

### 2. Configurations

Please set the PG parameters related to Lakesoul according to this document: 
[Setup Metadata Database Connection for Flink](02-setup-spark.md#setup-metadata-database-connection-for-flink)

 After this, you could start flink session cluster or application as usual.

### 3. LakeSoul Operations

Use Kyuubi beeline to connect Flink SQL Engine:

```bash
$KYUUBI_HOME/bin/beeline -u 'jdbc:hive2://localhost:10009/default;user=admin;?kyuubi.engine.type=FLINK_SQL'
```
Flink SQL Access Lakesoul : 

```SQL
create catalog lakesoul with('type'='lakesoul');
use catalog lakesoul;
use `default`;

create table if not exists test_lakesoul_table_v1 (`id` INT, name STRING, score INT,`date` STRING,region STRING, PRIMARY KEY (`id`,`name`) NOT ENFORCED ) PARTITIONED BY (`region`,`date`) WITH ( 'connector'='lakeSoul', 'use_cdc'='true','format'='lakesoul', 'path'='hdfs:///lakesoul-test-bucket/default/test_lakesoul_table_v1/', 'hashBucketNum'='4');

insert into `lakesoul`.`default`.test_lakesoul_table_v1 values (1,'AAA', 100, '2023-05-11', 'China');
insert into `lakesoul`.`default`.test_lakesoul_table_v1 values (2,'BBB', 100, '2023-05-11', 'China');
insert into `lakesoul`.`default`.test_lakesoul_table_v1 values (3,'AAA', 98, '2023-05-10', 'China');

select * from `lakesoul`.`default`.test_lakesoul_table_v1 limit 1;

drop table `lakesoul`.`default`.test_lakesoul_table_v1;
```
You could replace the location schema from  `hdfs://` to `file://` .

More details about Flink SQL with LakeSoul refer to : [Flink Lakesoul Connector](./06-flink-lakesoul-connector.md) 

## Spark SQL Access Lakesoul's Table

### 1. Dependencies

Download LakeSoul Spark Jar from: https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-spark-2.4.0-spark-3.3.jar

And put the jar file under `$SPARK_HOME/jars`. 

### 2. Configurations
1. Please set the PG parameters related to Lakesoul according to this document: 
[Setup Metadata Database Connection for Spark](02-setup-spark.md#pass-lakesoul_home-environment-variable-to-your-spark-job)
2. Add spark conf to `$SPARK_CONF_DIR/spark-defaults.conf`

    ```
    spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension

    spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

    spark.sql.defaultCatalog=lakesoul

    spark.sql.caseSensitive=false
    spark.sql.legacy.parquet.nanosAsLong=false
    ```

### 3. LakeSoul Operations
Use Kyuubi beeline to connect Spark SQL Engine:

```bash
$KYUUBI_HOME/bin/beeline -u 'jdbc:hive2://localhost:10009/default;user=admin;?kyuubi.engine.type=SPARK_SQL'
```
Spark SQL Access Lakesoul : 

```SQL
use default;

create table if not exists test_lakesoul_table_v2 (id INT, name STRING, score INT, date STRING,region STRING) USING lakesoul PARTITIONED BY (region,date) LOCATION 'hdfs:///lakesoul-test-bucket/default/test_lakesoul_table_v2/' TBLPROPERTIES( 'hashPartitions'='id,name', 'use_cdc'='true', 'hashBucketNum'='4');

insert into test_lakesoul_table_v2 values (1,'AAA', 100, '2023-05-11', 'China');
insert into test_lakesoul_table_v2 values (2,'BBB', 100, '2023-05-11', 'China');
insert into test_lakesoul_table_v2 values (3,'AAA', 98, '2023-05-10', 'China');

select * from test_lakesoul_table_v2 limit 1;

drop table test_lakesoul_table_v2;
```
You could replace the location schema from  `hdfs://` to `file://` .

More details about Spark SQL with LakeSoul refer to : [Operate LakeSoulTable by Spark SQL](./03-api-docs.md#7-operate-lakesoultable-by-spark-sql) 