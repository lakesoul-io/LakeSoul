# 使用Kyuubi JDBC访问Lakesoul表

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

:::tip
从 2.4 版本起提供。
:::

LakeSoul实现了Flink/Spark Connector。我们可以通过Kyuubi使用Spark/Flink SQL来查询访问Lakesoul.


## 环境要求

|组件 | 版本|
|-----------|--------|
| Kyuubi | 1.8  |
| Spark  | 3.3  |
| Flink  | 1.17 |
| LakeSoul | 2.4.0 |
| Java     | 1.8 |

运行环境为Linux环境，并已安装Spark, Flink, Kyuubi，推荐Kyuubi Engine以Hadoop Yarn作为执行环境，当然也可以本地启动Spark/Flink Local集群。

可参考Kyuubi安装文档: [Deploy Kyuubi engines on Yarn](https://kyuubi.readthedocs.io/en/v1.7.3/deployment/engine_on_yarn.html).


## Flink SQL访问Lakesoul表

### 1. 依赖

下载LakeSoul Flink Jar: https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-flink-2.4.0-flink-1.17.jar

将该jar拷贝至 `$FLINK_HOME/lib`.

### 2. Flink配置项

请根据如下文档来设置连接LakeSoul元数据库需要的PG参数: 
[Setup Metadata Database Connection for Flink](02-setup-spark.md#setup-metadata-database-connection-for-flink)

在此之后，您可以像往常一样启动 Flink Session集群或Flink Application。

### 3. 操作LakeSoul 

使用Kyuubi beeline来连接Flink SQL Engine:

```bash
$KYUUBI_HOME/bin/beeline -u 'jdbc:hive2://localhost:10009/default;user=admin;?kyuubi.engine.type=FLINK_SQL'
```

使用Flink SQL操作LakeSoul: 

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
可以将数据存储路径中的 `hdfs://` 替换为 `file://` 。

详细的Flink SQL操作Lakesoul内容参阅 : [Flink Lakesoul Connector](./06-flink-lakesoul-connector.md) 

## Spark SQL访问Lakesoul表

### 1. 依赖

下载LakeSoul Spark Jar: https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-spark-2.4.0-spark-3.3.jar

将该jar拷贝至 `$SPARK_HOME/jars`. 

### 2. Spark配置项
1. 请根据如下文档来配置连接LakeSoul元数据需要的PG参数: 
[Setup Metadata Database Connection for Spark](02-setup-spark.md#pass-lakesoul_home-environment-variable-to-your-spark-job)

2. Add spark conf to `$SPARK_CONF_DIR/spark-defaults.conf`

    ```
    spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension

    spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog

    spark.sql.defaultCatalog=lakesoul

    spark.sql.caseSensitive=false
    spark.sql.legacy.parquet.nanosAsLong=false
    ```

### 3. 操作LakeSoul

使用Kyuubi beeline来连接Spark SQL Engine:

```bash
$KYUUBI_HOME/bin/beeline -u 'jdbc:hive2://localhost:10009/default;user=admin;?kyuubi.engine.type=SPARK_SQL'
```

Spark SQL访问Lakesoul: 

```SQL
use default;

create table if not exists test_lakesoul_table_v2 (id INT, name STRING, score INT, date STRING,region STRING) USING lakesoul PARTITIONED BY (region,date) LOCATION 'hdfs:///lakesoul-test-bucket/default/test_lakesoul_table_v2/' TBLPROPERTIES( 'hashPartitions'='id,name', 'use_cdc'='true', 'hashBucketNum'='4');

insert into test_lakesoul_table_v2 values (1,'AAA', 100, '2023-05-11', 'China');
insert into test_lakesoul_table_v2 values (2,'BBB', 100, '2023-05-11', 'China');
insert into test_lakesoul_table_v2 values (3,'AAA', 98, '2023-05-10', 'China');

select * from test_lakesoul_table_v2 limit 1;

drop table test_lakesoul_table_v2;
```

可以将数据存储路径中的`hdfs://`替换为`file://` 。

详细的Spark SQL操作Lakesoul的内容参阅 : [Operate LakeSoulTable by Spark SQL](./03-api-docs.md#7-operate-lakesoultable-by-spark-sql) 