# Spark 快照相关功能用法教程

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul 使用快照的方式来记录每一次更新的文件集合，并在元数据中生成一个新的版本号。历史的快照版本如果没有被清理，则也可以通过 LakeSoul API 进行读取、回滚和清理等操作。由于快照版本是内部的机制，为了使用方便，LakeSoul 提供了基于时间戳的快照管理 API。

Flink 中使用 SQL 进行快照读取可以参考 [Flink Connector](../03-Usage%20Docs/06-flink-lakesoul-connector.md)。

## 快照读
在某些情况下，可能会需要查询一张表某个分区在之前某个时间点的快照数据，也称为 Time Travel。LakeSoul 执行读取某个时间点的快照的方式：
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
// 读取 'date=2022-01-02' 分区在时间戳小于等于并最接近 '2022-01-01 15:15:15'时的数据
val lakeSoulTable = LakeSoulTable.forPathSnapshot(tablePath, "date=2022-01-02", "2022-01-01 15:15:15")
```

## 快照回滚
某些时候由于新写入的数据有误，需要回滚到某个历史快照版本。使用 LakeSoul 执行快照回滚到某个时间点之前的方式：
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//将分区为'2021-01-02'的数据回滚到时间戳小于等于并最接近'2022-01-01 15:15:15'时的数据信息
lakeSoulTable.rollbackPartition("date='2022-01-02'", "2022-01-01 15:15:15")
```
回滚操作本身会创建一个新的快照版本，而其他的版本快照以及数据不会被删除。

## 快照清理
历史的快照如果已经不需要了，例如已经执行过 Compaction，可以调用清理方法清理某个时间点之前的快照数据：
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//将分区为'date=2022-01-02'，且时间早于"2022-01-01 15:15:15"之前的元数据和存储数据进行清理
lakeSoulTable.cleanupPartitionData("date='2022-01-02'", "2022-01-01 15:15:15")
```
