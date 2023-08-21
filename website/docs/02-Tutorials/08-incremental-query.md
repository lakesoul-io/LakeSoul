# Incremental Query Function Tutorial

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul provides a timestamp-based incremental query API to facilitate users to obtain data streams added after a given timestamp. Users can query the incremental data within this time range by specifying the start timestamp and the end timestamp. If the end timestamp is not specified, the incremental data from the start time to the current latest time will be queried.

LakeSoul supports a total of four commit operations: mergeCommit; appendCommit; compactCommit; updateCommit. For update operations, it is difficult to obtain incremental files because historical data is merged and new files are generated each time, so incremental queries are not supported.

Optional parameters and their meanings

```scala
// 1. Partition information
option(LakeSoulOptions.PARTITION_DESC, "range=range1")
option(LakeSoulOptions. HASH_PARTITIONS, "hash")
option(LakeSoulOptions. HASH_BUCKET_NUM, "2")
// If no partition information is specified, incremental query will be performed for all partitions by default, if there is no range, hash must be specified
// 2. Start and end timestamps
option(LakeSoulOptions. READ_START_TIME, "2022-01-01 15:15:15")
option(LakeSoulOptions. READ_END_TIME, "2022-01-01 20:15:15")
// 3. Time zone information
option(LakeSoulOptions.TIME_ZONE,"Asia/Sahanghai")
// If the time zone information of the timestamp is not specified, it will be processed according to the user's local time zone by default
4. Read type
option(LakeSoulOptions. READ_TYPE, "incremental")
// You can specify incremental read "incremental", snapshot read "snapshot", and do not specify the default full read.
```

## Incremental Read

LakeSoul supports incremental read for both upsert-only table and CDC table. There are two ways. One is to query by calling the LakeSoulTable.forPath() function, and the other is to perform incremental reads by specifying options in `spark.read` and `spark.readStream`. You can get Incremental data of the specified partition within the start and end time range, and the time interval of the acquired incremental data is closed before and opened after.

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
   .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
   .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
// Incremental read for a given range and timestamp, incremental means incremental read type
// For example, read the incremental data of the range1 partition in the time range from 2023-01-01 15:15:00 to 2023-01-01 15:20:00 based on the Shanghai time zone
// The first way is to perform incremental reading through forPathIncremental, if you do not specify a partition, enter "", if you do not enter a time zone parameter, the local system time zone is used by default
val lake1 = LakeSoulTable.forPathIncremental(tablePath, "range=range1", "2023-01-01 15:15:00", "2023-01-01 15:20:00", "Asia/Shanghai")

// The second way is to perform incremental reading by specifying the option of spark.read
val lake2 = spark.read.format("lakesoul")
   .option(LakeSoulOptions.PARTITION_DESC, "range=range1")
   .option(LakeSoulOptions.READ_START_TIME, "2023-01-01 15:15:00")
   .option(LakeSoulOptions.READ_END_TIME, "2023-01-01 15:20:00")
   .option(LakeSoulOptions.TIME_ZONE,"Asia/Shanghai")
   .option(LakeSoulOptions.READ_TYPE, "incremental")
   .load(tablePath)
```

## Streaming Read

LakeSoul supports Spark Structured Streaming read. Streaming read is based on incremental query. Through spark.readStream specified options for streaming read, you can obtain the incremental data updated in each batch under the specified partition in the real-time data stream. The specified start time needs to be earlier than the ingestion time of the real-time data.

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession. builder. master("local")
   .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
   .getOrCreate()
val tablePath = "s3a://bucket-name/table/path/is/also/table/name"

// Use spark.readStream to specify options for streaming reading, read the incremental data of the range1 partition at 2023-01-01 15:00:00 and later based on the Shanghai time zone, trigger a read every 1 second, and save the results output to the console
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

## Python Interface Tutorial

First put LakeSoul/python/lakesoul folder into `$SPARK_HOME/python/pyspark`, which provides the pyspark.lakesoul module to with the python API of snapshot reading, incremental reading and streaming reading. Then execute in the command line:
```bash
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
```

```python
# Run pyspark tests using spark 3.3.x version
from pyspark.lakesoul.tables import LakeSoulTable
from pyspark.sql import SparkSession

spark = SparkSession.builder \
     .appName("Stream Test") \
     .master('local[4]') \
     .config("spark.ui.enabled", "false") \
     .config("spark.sql.shuffle.partitions", "5") \
     .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")\
     .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog") \
     .config("spark.sql.defaultCatalog", "lakesoul") \
     .config("spark.sql.warehouse.dir", "/tmp/testPyspark") \
     .getOrCreate()
tablePath = "s3a://bucket-name/table/path/is/also/table/name"
    
df = spark.createDataFrame([('hash1', 11),('hash2', 44),('hash3', 55)],["key","value"])
# upsert requires that hashPartition must be specified, and rangePartition may not be specified
df.write.format("lakesoul")
     .mode("append")
     .option("hashPartitions", "key")
     .option("hashBucketNum", "2")
     .option("shortTableName", "tt")
     .save(tablePath)
lake = LakeSoulTable.forPath(spark, tablePath)
df_upsert = spark.createDataFrame([('hash5', 100)],["key","value"])
# Generate incremental data through upsert for testing
lake.upsert(df_upsert)


#Two methods of snapshot reading, if forPathSnapshot omits the input time zone parameter, the local system time zone is used by default
lake = spark.read.format("lakesoul")
     .option("readendtime", "2023-02-28 14:45:00")
     .option("readtype", "snapshot")
     .load(tablePath)
lake = LakeSoulTable.forPathSnapshot(spark,tablePath,"","2023-02-28 14:45:00","Asia/Shanghai")

#Two methods of incremental reading, if forPathIncremental omits the input time zone parameter, the local system time zone is used by default
lake = spark.read.format("lakesoul")
     .option("readstarttime", "2023-02-28 14:45:00")
     .option("readendtime", "2023-02-28 14:50:00")
     .option("timezone","Asia/Shanghai")
     .option("readtype", "incremental")
     .load(tablePath)
lake = LakeSoulTable.forPathIncremental(spark,tablePath,"","2023-02-28 14:45:00","2023-02-28 14:50:00","Asia/Shanghai")

#Streaming reading, you need to open two pyspark windows, one for modifying data to generate multi-version data, and one for performing streaming reading
spark.readStream.format("lakesoul")
     .option(
```