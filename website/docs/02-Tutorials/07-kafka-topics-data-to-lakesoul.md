# Multiple Kafka Topics Data to LakeSoul Tutorial

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

It is very convenient to synchronize the data in Kafka to LakeSoul by LakeSoul Kafka Stream.

LakeSoul Kafka Stream can support automatic table creation, automatic identification of new topics, exactly once
semantics, add partition for table etc.

LakeSoul Kafka Stream mainly use Spark Structured Streaming to realize data synchronization function.

In this tutorial, we fully demonstrate synchronizing data in topics to LakeSoul, including automatic table creation,
identifier new topics and other operations.

:::caution 
Using LakeSoul Kafka Stream requires one of the following conditions:
1. the data in topic is json string;
2. Kafka cluster with Schema Registry service.
:::

## 1. Prepare the environment

You can compile the LakeSoul project to obtain LakeSoul Kafka Stream jar, or get LakeSoul Kafka Stream jar and others  related jars from https://dmetasoul-bucket.obs.cn-southwest-2.myhuaweicloud.com/releases/lakesoul/lakesoul-kafka-stream-3.3.tar.gz.

After decompression, put the jar package into $SPARK_HOME/jars directory or config dependent jars with --jars or other
methods when submitting tasks.

## 2. Start LakeSoul Kafka Stream Task

1. Pass `lakesoul_home` environment variable to your job. For detailed documentation, please refer
   to [Setup a local test environment](../01-Getting%20Started/01-setup-local-env.md)
2. Submit task. you need to fill in some parameters in sequence in order to ensure that the task can run accurately. The parameters are described as follows:

Description of required parameters:

| Parameter Order | Meaning                          | required | Value Example            |
|-----------------|----------------------------------|----------|--------------------------|
| First           | Kafka bootstrap.servers          | true     | localhost:9092           |
| Second          | Topic regular expression         | true     | test.*                   |
| Third           | Data storage path prefix         | true     | /tmp/lakesoul/data       |
| Fourth          | Task checkpoint path             | true     | /tmp/lakesoul/checkpoint |
| Fifth           | Database name in LakeSoul        | true     | kafka                    |
| Sixth           | data sync beginning offset       | true     | 'earliest' or 'latest'   |
| Seventh         | whether add partition for tables | true     | 'true' or 'false'        |
| Eighth          | Schema Registry Url              | false    | http://localhost:8081    |

:::tip
If set the Seventh parameter (add partition for tables): true, then partition field named 'lakesoul_dt' will be created automatically for all tables, and the value is timestamp with 'yyyyMMddHH' format.
:::

## 3. Example of task process

1. Suppose the Kafka cluster already exists. Here, run Kafka cluster through Docker Compose. Then create a topic named 'test' and write some data to it. Kafka bootstrap.servers: localhost:9092.
```bash
# create topic 'test'
bin# ./kafka-topics.sh --create --topic test --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

# show topic list
bin# ./kafka-topics.sh  --list --bootstrap-server localhost:9092
test

# write data to test 'topic'
bin# ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
>{"before":{"id":1,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":1}
>{"before":{"id":2,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":2}
>{"before":{"id":3,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":3}
>{"before":{"id":4,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":4}
>{"before":{"id":5,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":5}
```

2. Submit Kafka Stream task. Put the above dependent jars to $SPARK_HOME/jars directory here.

```bash
export lakesoul_home=./pg.properties && ./bin/spark-submit \
--class org.apache.spark.sql.lakesoul.kafka.KafkaStream \
--driver-memory 4g \
--executor-memory 4g \
--master local[4] \
./jars/lakesoul-spark-2.3.0-spark-3.3.jar \
localhost:9092 test.* /tmp/kafka/data /tmp/kafka/checkpoint/ kafka earliest false
```

3. Select data in LakeSoul by spark-shell.

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

4. Add new topic 'test_1' and write some data to it.

```bash
# create topic 'test_1' 
bin# ./kafka-topics.sh --create --topic test_1 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4

# show topic list
bin# ./kafka-topics.sh  --list --bootstrap-server localhost:9092
test
test_1

# write data to topic 'test_1'
bin# ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test_1
>{"before":{"id":1,"rangeid":2,"value":"sms"},"after":{"id":1,"rangeid":1,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":1}
>{"before":{"id":2,"rangeid":2,"value":"sms"},"after":{"id":2,"rangeid":2,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":2}
>{"before":{"id":3,"rangeid":2,"value":"sms"},"after":{"id":3,"rangeid":3,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":3}
>{"before":{"id":4,"rangeid":2,"value":"sms"},"after":{"id":4,"rangeid":4,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":4}
>{"before":{"id":5,"rangeid":2,"value":"sms"},"after":{"id":5,"rangeid":5,"value":"sms"},"source":{"version":"1.8.0.Final","connector":"mysql","name":"cdcserver","ts_ms":1644461444000,"snapshot":"false","db":"cdc","sequence":null,"table":"sms","server_id":529210004,"gtid":"de525a81-57f6-11ec-9b60-fa163e692542:1621099","file":"binlog.000033","pos":54831329,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1644461444777,"transaction":5}
```

5. The new topic data synchronization take 5 minutes. Check the 'test_1' data after 5 minutes.

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

6. Submitting task with schema registry url parameter if Kafka cluster operated with Schema Registry service.

```bash
export lakesoul_home=./pg.properties && ./bin/spark-submit \
--class org.apache.spark.sql.lakesoul.kafka.KafkaStream \
--driver-memory 4g \
--executor-memory 4g \
--master local[4] \
./jars/lakesoul-spark-2.3.0-spark-3.3.jar \
localhost:9092 test.* /tmp/kafka/data /tmp/kafka/checkpoint/ kafka earliest false http://localhost:8081
```