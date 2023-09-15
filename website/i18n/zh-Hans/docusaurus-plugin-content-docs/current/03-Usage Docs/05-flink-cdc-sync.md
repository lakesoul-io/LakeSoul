# LakeSoul Flink CDC 整库千表同步

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul 自 2.1.0 版本起，实现了 Flink CDC Sink，能够支持 Table API 及 SQL （单表），以及 Stream API （整库多表）。目前支持的上游数据源为 MySQL（5.6-8.0）

## 主要功能特点

在 Stream API 中，LakeSoul Sink 主要功能点有：
* 支持整库千表（不同 schema）在同一个 Flink 作业中实时 CDC 同步，不同表会自动写入 LakeSoul 对应表名中
* 支持 Schema 变更(DDL)自动同步到 LakeSoul，下游读取自动兼容新旧数据（目前支持增删列以及数值类型增加精度）；
* 支持运行过程中上游数据库中新建表自动感知，在 LakeSoul 中自动建表；
* 支持严格一次（Exactly Once）语义，即使 Flink 作业发生 Failover，能够保证数据不丢不重；
* 提供 Flink 命令行启动入口类，支持指定库名、表名黑名单、并行度等参数；

## 命令行使用方法
### 1. 下载 LakeSoul Flink Jar
可以在 LakeSoul Release 页面下载：https://github.com/lakesoul-io/LakeSoul/releases/download/v2.4.0/lakesoul-flink-2.4.0-flink-1.17.jar。

如果访问 Github 有问题，也可以通过这个链接下载：https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/lakesoul-flink-2.4.0-flink-1.17.jar。

目前支持的 Flink 版本为 1.17。

### 2. 启动 Flink 作业

#### 2.1 增加 LakeSoul 元数据库配置
在 `$FLINK_HOME/conf/flink-conf.yaml` 中增加如下配置：
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
注意这里 master 和 taskmanager 的环境变量都需要设置。

:::tip
Postgres 数据库的连接信息、用户名密码需要根据实际情况修改。
:::

:::caution
注意如果使用 Session 模式来启动作业，即将作业以 client 方式提交到 Flink Standalone Cluster，则 `flink run` 作为 client，是不会读取上面配置，因此需要再单独配置环境变量，即：
```bash
export LAKESOUL_PG_DRIVER=com.lakesoul.shaded.org.postgresql.Driver
export LAKESOUL_PG_URL=jdbc:postgresql://localhost:5432/test_lakesoul_meta?stringtype=unspecified
export LAKESOUL_PG_USERNAME=root
export LAKESOUL_PG_PASSWORD=root
```
:::

#### 2.2 启动同步作业
```bash
bin/flink run -c org.apache.flink.lakesoul.entry.MysqlCdc \
    lakesoul-flink-2.4.0-flink-1.17.jar \
    --source_db.host localhost \
    --source_db.port 3306 \
    --source_db.db_name default \
    --source_db.user root \
    --source_db.password root \
    --source.parallelism 4 \
    --sink.parallelism 4 \
    --server_time_zone=Asia/Shanghai
    --warehouse_path s3://bucket/lakesoul/flink/data \
    --flink.checkpoint s3://bucket/lakesoul/flink/checkpoints \
    --flink.savepoint s3://bucket/lakesoul/flink/savepoints
```

必填参数说明：

| 参数      | 含义                                                                        | 取值说明    |
|----------------|--------------------------------------------------------------------------------------|---------------------------------------------|
| -c      | 任务运行main函数入口类                                                                        | org.apache.flink.lakesoul.entry.MysqlCdc    |
| 主程序包           | 任务运行jar包                                                                     | lakesoul-flink-2.4.0-flink-1.17.jar                  |
| --source_db.host      | MySQL 数据库的地址                                                     |                                             |
| --source_db.port      | MySQL 数据库的端口                                                     |                                             |
| --source_db.user      | MySQL 数据库的用户名                                                     |                                             |
| --source_db.password      | MySQL 数据库的密码                                                     |                                             |
| --source.parallelism      | 单表读取任务并行度，影响数据读取速度，值越大对 MySQL 压力越大   | 可以根据 MySQL 的写入 QPS 来调整并行度 |
| --sink.parallelism        | 单表写任务并行度，同时也是LakeSoul表主键分片的个数。影响入湖数据落地速度。值越大，小文件数越多，影响后续读取性能；值越小对写任务压力越大，发生数据倾斜可能性越大 | 可以根据最大表的数据量进行调整。一般建议一个并行度（主键分片）管理不超过1千万行数据。 |
| --warehouse_path    | 数据存储路径前缀（hdfs需要带上集群前缀）  | LakeSoul 会将对应表数据写入到 ${warehouse_path}/database_name/table_name/ 目录下         |
| --flink.savepoint    | Flink savepoint路径（hdfs需要带上集群前缀） |                                             |
| --flink.checkpoint   | Flink checkpoint路径（hdfs需要带上集群前缀）|                                             |

其余 Flink 参数，如 job manager、task manager 的 CPU、内存、Slots 等，也需要根据具体情况设置。

可选参数说明：

| 参数                     | 含义说明                         | 参数填写格式                                   |
|--------------------------|------------------------------|------------------------------------------|
| --source_db.exclude_tables | 不需要同步的数据表名列表，表名之间用逗号分隔，默认为空  | --source_db.exclude_tables test_1,test_2 |
| --job.checkpoint_mode      | 数据同步方式，默认是 EXACTLY_ONCE       | --job.checkpoint_mode AT_LEAST_ONCE      |
| --job.checkpoint_interval  | checkpoint 存储间隔，单位 ms，默认值为10分钟 | --job.checkpoint_interval 1200000        |
| --server_time_zone=Asia/Shanghai  | MySQL 服务端时区，Flink 端默认为 "Asia/Shanghai" | 参考 [JDK ZoneID 文档](https://docs.oracle.com/javase/8/docs/api/java/time/ZoneId.html) |

## LakeSoul Flink CDC Sink 作业执行流程

LakeSoul Flink 作业启动后初始化阶段，首先会读取配置的 MySQL DB 中的所有表（排除掉不需要同步的表）。对每一个表，首先判断在 LakeSoul 中是否存在，如果不存在则自动创建一个 LakeSoul 表，其 Schema 与 MySQL 对应表一致。

完成初始化后，会读取所有表的 CDC Stream，以 Upsert 的方式写入到对应的各个 LakeSoul 表中。

在同步期间如果 MySQL 表发生 DDL Schema 变更，则该变更也会同样应用到对应的 LakeSoul 表中。

## LakeSoul Flink CDC Sink 严格一次语义保证
LakeSoul Flink CDC Sink 在作业运行过程中会自动保存相关状态，在 Flink 作业发生 Failover 时能够将状态恢复并重新写入，因此数据不会丢失。

LakeSoul 写入时，在两个部分保证写入的幂等性：
1. Stage 文件 Commit 时，与 Flink File Sink 一致，通过文件系统 rename 操作的原子性，来保证 staging 文件写入到最终的路径。因为 rename 是原子的，Failover 之后不会发生重复写入或缺失的情况。
2. LakeSoul 元数据提交时，会首先记录文件路径，在更新 snapshot 时会通过事务标记该文件已提交。Failover 后，通过判断一个文件是否已经提交，可以保证提交的幂等性。

综上，LakeSoul Flink CDC Sink 通过状态恢复保证数据不丢失，通过提交幂等性保证数据不重复，实现了严格一次（Exactly Once）语义保证。

## MySQL 与 LakeSoul 的类型映射关系

由于 MySQL、Spark、Parquet 等数据类型不完全相同，LakeSoul 在同步时做了如下映射关系（不在表中的类型目前暂不支持）：

|MySQL 类型 | Spark 类型类名 |
|-|-|
|BOOLEAN, BOOL|org.apache.spark.sql.types.DataTypes.BooleanType|
|BIT(1)|org.apache.spark.sql.types.DataTypes.BooleanType|
|BIT(>1)|org.apache.spark.sql.types.DataTypes.BinaryType|
|TINYINT|org.apache.spark.sql.types.DataTypes.IntegerType|
|SMALLINT[(M)]|org.apache.spark.sql.types.DataTypes.IntegerType|
|MEDIUMINT[(M)]|org.apache.spark.sql.types.DataTypes.IntegerType|
|INT, INTEGER[(M)]|org.apache.spark.sql.types.DataTypes.IntegerType|
|BIGINT[(M)]|org.apache.spark.sql.types.DataTypes.LongType|
|REAL[(M,D)]|org.apache.spark.sql.types.DataTypes.FloatType|
|FLOAT[(M,D)]|org.apache.spark.sql.types.DataTypes.DoubleType|
|DOUBLE[(M,D)]|org.apache.spark.sql.types.DataTypes.DoubleType|
|CHAR(M)]|org.apache.spark.sql.types.DataTypes.StringType|
|VARCHAR(M)]|org.apache.spark.sql.types.DataTypes.StringType|
|BINARY(M)]|org.apache.spark.sql.types.DataTypes.BinaryType|
|VARBINARY(M)]|org.apache.spark.sql.types.DataTypes.BinaryType|
|TINYBLOB|org.apache.spark.sql.types.DataTypes.BinaryType|
|TINYTEXT|org.apache.spark.sql.types.DataTypes.StringType|
|BLOB|org.apache.spark.sql.types.DataTypes.BinaryType|
|TEXT|org.apache.spark.sql.types.DataTypes.StringType|
|MEDIUMBLOB|org.apache.spark.sql.types.DataTypes.BinaryType|
|MEDIUMTEXT|org.apache.spark.sql.types.DataTypes.StringType|
|LONGBLOB|org.apache.spark.sql.types.DataTypes.BinaryType|
|LONGTEXT|org.apache.spark.sql.types.DataTypes.StringType|
|JSON|org.apache.spark.sql.types.DataTypes.StringType|
|ENUM|org.apache.spark.sql.types.DataTypes.StringType|
|SET|org.apache.spark.sql.types.DataTypes.StringType|
|YEAR[(2\|4)]|org.apache.spark.sql.types.DataTypes.IntegerType|
|TIMESTAMP[(M)]|org.apache.spark.sql.types.DataTypes.TimestampType|
|DATE|org.apache.spark.sql.types.DataTypes.DateType|
|TIME[(M)]|org.apache.spark.sql.types.DataTypes.LongType|
|DATETIME[(M)]|org.apache.spark.sql.types.DataTypes.TimestampType|
|NUMERIC[(M[,D])],DECIMAL[(M[,D])]|_if decimal.handling.mode=precise_ <br />&emsp;org.apache.spark.sql.types.DecimalType(M,D) <br />if _decimal.handling.mode=string_<br />   &emsp;                 org.apache.spark.sql.types.DataTypes.StringType<br />_if decimal.handling.mode=doulbe_ <br />&emsp;org.apache.spark.sql.types.DataTypes.DoubleType|
|GEOMETRY, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION, POINT|暂不支持|

Spark 中的类型，在 Spark SQL 中的类型名，可以在 [Spark Data Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) 文档中查找对应关系。

## 注意事项
1. MySQL 中的表必须存在主键，无主键表目前暂不支持；
2. DDL 变更目前支持在最后增加列，或删除中间某一列；新增列的默认值目前只支持 `null`，LakeSoul 读取旧数据时会自动对该列补 `null` 值；删除的列，LakeSoul 读取时会自动过滤该列；
3. MySQL的TIME类型对应LakeSoul中LongType类型，因为Spark中没有TIME数据类型且Debezium在解析Time类型时，解析为当前值距离00:00:00经过的微秒数，所以这里和Debezium保持一致；
4. MySQL中TIMESTAMP和DATETIME类型，在LakeSoul中会以UTC时区值进行存储，避免出现时区解析问题；在读的时候只需要指定时区便可按指定时区正确解析读出。所以在FLINK CDC任务启动时需要正确填写server_time_zone参数。