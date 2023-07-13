# Kafka 多 topic 数据入 LakeSoul 教程

通过 LakeSul Kafka Stream 将 Kafka 中的数据同步到 LakeSul 非常方便。

LakeSoul Kafka Stream 可以支持自动创建表，自动识别新 topic，exactly-once 语义、自动为表添加分区等功能。

LakeSoul Kafka Stream 主要使用 Spark Structured Streaming 来实现数据同步功能。

在本教程中，我们将充分演示如何将 topic 中的数据同步到 LakeSoul，包括自动创建表、标识新主题和其他操作。

:::caution
使用 LakeSoul Kafka Stream需要以下条件之一：

1. topic 中的数据为 json 格式;
2. Kafka 集群带有 Schema Registry 服务.
:::

## 1. 准备环境

你可以编译 LakeSoul 项目以获取 LakeSoul Kafka Stream jar, 或者可以通过 https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/lakesoul-kafka-stream-3.3.tar.gz 来获取 LakeSoul Kafka Stream 以及其他任务运行依赖的jar包。

下载后解压 tar 包，然后将 jar 包放入 $SPARK_HOME/jars 目录下，或者在提交任务时添加依赖的jar，比如通过 --jars。

## 2. 启动 LakeSoul Kafka Stream 任务

1. 任务启动时通过 `lakesoul_home` 环境变量添加元数据库信息. 这部分请参考 [搭建本地测试环境](../01-Getting%20Started/01-setup-local-env.md)
2. 提交任务。 你需要按顺序填写一些参数，以确保任务能够准确运行。参数描述如下：

参数说明:

| 参数顺序   | 含义                         | 是否必要 | 取值示例                     |
|--------|----------------------------|------|--------------------------|
| 第一个    | Kafka bootstrap.servers    | 必要   | localhost:9092           |
| 第二个    | Topic 的正则表达式               | 必要   | test.*                   |
| 第三个    | 数据存储路径前缀                   | 必要   | /tmp/lakesoul/data       |
| 第四个    | 任务 checkpoint 路径           | 必要   | /tmp/lakesoul/checkpoint |
| 第五个    | LakeSoul 中数据库名             | 必要   | kafka                    |
| 第六个    | 数据从topic最新还是最早位置开始同步       | 必要   | 'earliest' 或者 'latest'   |
| 第七个    | 是否为表添加分区                   | 必要   | 'true' 或者 'false'        |
| 第八个    | Schema Registry 服务连接地址     | 不必要  | http://localhost:8081    |

:::tip
如果第七个参数设置(为表添加分区): true，则将为所有表自动创建名为 "lakeoul_dt" 的分区字段，值为 "yyyyMMddHH" 格式的时间戳。
:::

## 3. 任务流程示例

1. 假设 Kafka 集群已经存在。在这里，通过 Docker Compose 运行 Kafka 集群。然后创建一个名为 "test" 的主题并向其中写入一些数据。 Kafka bootstrap.servers: localhost:9092.

```bash
# 创建 topic 'test'
bin# ./kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

# 查看 topic 列表
bin# ./kafka-topics.sh  --list --bootstrap-server localhost:9092
test

# 向 名为 'test' 的topic中写入一些数据
bin# ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
>{"before":{"id":1,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":1}
>{"before":{"id":2,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":2}
>{"before":{"id":3,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":3}
>{"before":{"id":4,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":4}
>{"before":{"id":5,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":5}
```

2. 提交 Kafka Stream 任务. 将上述下载的依赖 jars 放到 $SPARK_HOME/jars 目录下.

```bash
export lakesoul_home=./pg.properties && ./bin/spark-submit \
--class org.apache.spark.sql.lakesoul.kafka.KafkaStream \
--driver-memory 4g \
--executor-memory 4g \
--master local[4] \
./jars/lakesoul-spark-2.3.0-spark-3.3.jar \
localhost:9092 test.* /tmp/kafka/data /tmp/kafka/checkpoint/ kafka earliest false
```

3. 通过 spark-shell 查看写入 LakeSoul 中的数据 

```bash
scala> import com.dmetasoul.lakesoul.tables.LakeSoulTable
import com.dmetasoul.lakesoul.tables.LakeSoulTable

scala> val tablepath="/tmp/kafka/data/kafka/test"
tablepath: String = /tmp/kafka/data/kafka/test

scala> val lake = LakeSoulTable.forPath(tablepath)
lake: com.dmetasoul.lakesoul.tables.LakeSoulTable = com.dmetasoul.lakesoul.tables.LakeSoulTable@585a2ad9

scala> lake.toDF.show(false)
+----------------------------------+----------------------------------+---+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+-------------+
|after                             |before                            |op |source                                                                                                                                                                                                                                                                                                 |transaction|ts_ms        |
+----------------------------------+----------------------------------+---+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+-------------+
|{"id":2,"rangeid":2,"value":"sms"}|{"id":1,"rangeid":2,"value":"sms"}|c  |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|1          |1644461444777|
|{"id":2,"rangeid":2,"value":"sms"}|{"id":3,"rangeid":2,"value":"sms"}|c  |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|3          |1644461444777|
|{"id":2,"rangeid":2,"value":"sms"}|{"id":5,"rangeid":2,"value":"sms"}|c  |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|5          |1644461444777|
|{"id":2,"rangeid":2,"value":"sms"}|{"id":2,"rangeid":2,"value":"sms"}|c  |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|2          |1644461444777|
|{"id":2,"rangeid":2,"value":"sms"}|{"id":4,"rangeid":2,"value":"sms"}|c  |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|4          |1644461444777|
+----------------------------------+----------------------------------+---+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+-------------+
```

4. 创建新的 topic 'test_1' 并向其中写入一些数据

```bash
# 创建 topic 'test_1' 
bin# ./kafka-topics.sh --create --topic test_1 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

# 查看 topic list
bin# ./kafka-topics.sh  --list --bootstrap-server localhost:9092
test
test_1

# 向 topic 'test_1' 中写入一些数据
bin# ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test_1
>{"before":{"id":1,"rangeid":2,"value":"sms"},"after":{"id":1,"rangeid":1,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":1}
>{"before":{"id":2,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":2}
>{"before":{"id":3,"rangeid":2,"value":"sms"},"after":{"id":3,"rangeid":3,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":3}
>{"before":{"id":4,"rangeid":2,"value":"sms"},"after":{"id":4,"rangeid":4,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":4}
>{"before":{"id":5,"rangeid":2,"value":"sms"},"after":{"id":5,"rangeid":5,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":5}
```

5. 创建新的 topic 后，数据同步到 Lakesoul 需要5分钟时间。5分钟后查看 LakeSoul 中的数据

```bash
scala> val tablepath_1="/tmp/kafka/data/kafka/test_1"
tablepath_1: String = /tmp/kafka/data/kafka/test_1

scala> val lake_1 = LakeSoulTable.forPath(tablepath_1)
lake: com.dmetasoul.lakesoul.tables.LakeSoulTable = com.dmetasoul.lakesoul.tables.LakeSoulTable@43900101

lake_1.toDF.show(false)
+----------------------------------+----------------------------------+----+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+-------------+
|after                             |before                            |op  |source                                                                                                                                                                                                                                                                                                 |transaction|ts_ms        |
+----------------------------------+----------------------------------+----+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+-------------+
|{"id":2,"rangeid":2,"value":"sms"}|{"id":2,"rangeid":2,"value":"sms"}|c   |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|2          |1644461444777|
|{"id":1,"rangeid":1,"value":"sms"}|{"id":1,"rangeid":2,"value":"sms"}|c   |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|1          |1644461444777|
|{"id":4,"rangeid":4,"value":"sms"}|{"id":4,"rangeid":2,"value":"sms"}|c   |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|4          |1644461444777|
|{"id":3,"rangeid":3,"value":"sms"}|{"id":3,"rangeid":2,"value":"sms"}|c   |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|3          |1644461444777|
|{"id":5,"rangeid":5,"value":"sms"}|{"id":5,"rangeid":2,"value":"sms"}|c   |{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null}|5          |1644461444777|
+----------------------------------+----------------------------------+----+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------+-------------+
```

6. 如果 Kafka 集群使用 Schema Registry服务，提交任务时参数最后需要填写 schema registry 服务地址。

```bash
export lakesoul_home=./pg.properties && ./bin/spark-submit \
--class org.apache.spark.sql.lakesoul.kafka.KafkaStream \
--driver-memory 4g \
--executor-memory 4g \
--master local[4] \
./jars/lakesoul-spark-2.3.0-spark-3.3.jar \
localhost:9092 test.* /tmp/kafka/data /tmp/kafka/checkpoint/ kafka earliest false http://localhost:8081
```