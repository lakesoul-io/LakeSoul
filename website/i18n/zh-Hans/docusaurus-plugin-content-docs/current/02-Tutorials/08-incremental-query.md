# 增量查询功能教程

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul提供基于时间戳的增量查询 API，方便用户获取自给定时间戳以后新增的数据流。用户通过指定起始时间戳和结束时间戳，可以查询这一时间范围内的增量数据，如果未指定结束时间戳，则查询起始时间到当前最新时间的增量数据。

LakeSoul共支持四种commit操作：mergeCommit；appendCommit；compactCommit；updateCommit，对于update操作由于历史数据每次合并会生成新文件，无法获取增量文件，因此不支持增量查询。

可选参数及含义

```Scala
1.分区信息
option(LakeSoulOptions.PARTITION_DESC, "range=range1")
option(LakeSoulOptions.HASH_PARTITIONS, "hash")
option(LakeSoulOptions.HASH_BUCKET_NUM, "2")
如果未指定分区信息，则默认针对所有分区进行增量查询，如果没有range，则必须指定hash
2.起始和结束时间戳
option(LakeSoulOptions.READ_START_TIME, "2022-01-01 15:15:15")
option(LakeSoulOptions.READ_END_TIME, "2022-01-01 20:15:15")
3.时区信息
option(LakeSoulOptions.TIME_ZONE,"Asia/Sahanghai")
如果不指定时间戳的时区信息，则默认为按本机时区处理
4.读类型
option(LakeSoulOptions.READ_TYPE, "incremental")
可以指定增量读"incremental"，快照读"snapshot"，不指定默认全量读。
```



## 增量读

支持简单的upsert场景和CDC场景下的增量读，有两种方式，一种是通过调用LakeSoulTable.forPath()函数进行查询，另一种是通过spark.read指定选项进行增量读，可以获得指定分区在起止时间范围内的增量数据，获取的增量数据时间区间为前闭后开。

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
// 针对给定range和时间戳，进行增量读,incremental表示增量读类型
// 例如读取range1分区以上海时区为标准在2023-01-01 15:15:00到2023-01-01 15:20:00时间范围内的增量数据
// 第一种方式，通过forPathIncremental进行增量读，不指定分区则输入""，不输入时区参数则默认使用本机系统时区
val lake1 = LakeSoulTable.forPathIncremental(tablePath, "range=range1", "2023-01-01 15:15:00", "2023-01-01 15:20:00")
val lake2 = LakeSoulTable.forPathIncremental(tablePath, "range=range1", "2023-01-01 15:15:00", "2023-01-01 15:20:00","Asia/Shanghai")

// 第二种方式，通过spark.read指定选项进行增量读
val lake3 = spark.read.format("lakesoul")
  .option(LakeSoulOptions.PARTITION_DESC, "range=range1")
  .option(LakeSoulOptions.READ_START_TIME, "2023-01-01 15:15:00")
  .option(LakeSoulOptions.READ_END_TIME, "2023-01-01 15:20:00")
  .option(LakeSoulOptions.TIME_ZONE,"Asia/Shanghai")
  .option(LakeSoulOptions.READ_TYPE, "incremental")
  .load(tablePath)
```

## 流式读

LakeSoul支持 Spark Structured Streaming read，流式读基于增量查询，通过spark.readStream指定选项进行流式读，可以获得实时数据流中指定分区下每一批次更新的增量数据。指定的起始时间需要早于实时数据的摄入时间。

```Scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()
val tablePath = "s3a://bucket-name/table/path/is/also/table/name"

// 通过spark.readStream指定选项进行流式读，读取range1分区以上海时区为标准在2023-01-01 15:00:00及之后的增量数据，每1秒触发一次读取，将结果输出到控制台
spark.readStream.format("lakesoul")
  .option(LakeSoulOptions.PARTITION_DESC, "range=range1")
  .option(LakeSoulOptions.READ_START_TIME, "2022-01-01 15:00:00")
  .option(LakeSoulOptions.TIME_ZONE,"Asia/Shanghai")
  .option(LakeSoulOptions.READ_TYPE, "incremental")
  .load(tablePath)
  .writeStream.format("console")
  .trigger(Trigger.ProcessingTime(1000))
  .start()
  .awaitTermination()
```

## python接口教程

将LakeSoul/python/lakesoul文件夹放入spark/python/pyspark中，通过提供pyspark.lakesoul模块，实现快照读、增量读和流式读的python API

```Python
# 使用spark 3.3.x版本运行pyspark测试
from pyspark.lakesoul.tables import LakeSoulTable
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Stream Test") \
    .master('local[4]') \
    .config("spark.ui.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "5") \
    .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension") \
    .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog") \
    .config("spark.sql.defaultCatalog", "lakesoul") \
    .config("spark.sql.warehouse.dir", "/tmp/testPyspark") \
    .getOrCreate()
tablePath = "s3a://bucket-name/table/path/is/also/table/name"
    
df = spark.createDataFrame([('hash1', 11),('hash2', 44),('hash3', 55)],["key","value"])
# upsert 要求必须指定hashPartition，可以不指定rangePartition
df.write.format("lakesoul")
    .mode("append")
    .option("hashPartitions", "key")
    .option("hashBucketNum", "2")
    .option("shortTableName", "tt")
    .save(tablePath)
lake = LakeSoulTable.forPath(spark, tablePath)
df_upsert = spark.createDataFrame([('hash5', 100)],["key","value"])
# 通过upsert产生增量数据，用于测试
lake.upsert(df_upsert)


#快照读的两种方法,forPathSnapshot省略输入时区参数，则默认使用本机系统时区
lake = spark.read.format("lakesoul")
    .option("readendtime", "2023-02-28 14:45:00")
    .option("readtype", "snapshot")
    .load(tablePath) 
lake = LakeSoulTable.forPathSnapshot(spark,tablePath,"","2023-02-28 14:45:00","Asia/Shanghai")

#增量读的两种方法，forPathIncremental省略输入时区参数，则默认使用本机系统时区
lake = spark.read.format("lakesoul")
    .option("readstarttime", "2023-02-28 14:45:00")
    .option("readendtime", "2023-02-28 14:50:00")
    .option("timezone","Asia/Shanghai")
    .option("readtype", "incremental")
    .load(tablePath) 
lake = LakeSoulTable.forPathIncremental(spark,tablePath,"","2023-02-28 14:45:00","2023-02-28 14:50:00","Asia/Shanghai")

#流式读，需要开两个pyspark窗口，一个用于修改数据产生多版本数据，一个用于执行流式读
spark.readStream.format("lakesoul")
    .option("readstarttime", "2023-02-28 14:45:00")
    .option("timezone","Asia/Shanghai")
    .option("readtype", "incremental")
    .load(tablePath)
    .writeStream.format("console")
    .trigger(processingTime='2 seconds')
    .start()
    .awaitTermination()
```
