[CN Doc](README-CN.md)

# LakeSoul
LakeSoul is a unified streaming and batch table storage solution built on top of the Apache Spark engine by the [DMetaSoul](https://www.dmetasoul.com) team, and supports scalable metadata management, ACID transactions, efficient and flexible upsert operation, schema evolution, and streaming & batch unification.

LakeSoul specializes in row and column level incremental upserts, high concurrent write, and bulk scan for data on cloud storage. The cloud native computing and storage separation architecture makes deployment very simple, while supporting huge amounts of data at lower cost.

To be specific, LakeSoul has the following characteristics:

  - Elastic framework: The computing and storage is completely separated. Without the need for fixed nodes and disks, the computing and storage has its own elastic capacity, and a lot of optimization for the cloud storage has done, like concurrency consistency in the object storage, incremental update and etc. With LakeSoul, there is no need to maintain fixed storage nodes, and the cost of object storage on cloud is only 1/10 of local disk, which greatly reduces storage and operation costs.
  - Efficient and scalable metadata management: LakeSoul uses Cassandra to manage metadata, which can efficiently handle modification on metadata and support multiple concurrent writes. It solves the problem of slow metadata parsing after long running in data Lake systems such as Delta Lake which use files to maintain metadata, and can only be written at a single point.
  - ACID transactions: Undo and Redo mechanism ensures that the committing are transactional and users will never see inconsistent data. 
  - Multi-level partitioning and efficient upsert: LakeSoul supports range and hash partitioning, and a flexible upsert operation at row and column level. The upsert data are stored as delta files, which greatly improves the efficiency and concurrency of writing data, and the optimized merge scan provides efficient MergeOnRead performance.
  - Streaming and batch unification: Streaming Sink is supported in LakeSoul, which can handle streaming data ingesting, historical data filling in batch, interactive query and other scenarios simultaneously.
  - Schema evolution: Users can add new fields at any time and quickly populate the new fields with historical data.


Application scenarios of LakeSoul:
  - Incremental data need to be written efficiently in large batches in real time, as well as concurrent updates at the row or column level.
  - Detailed query and update on a large time range with huge amount historical data, while hoping to maintain a low cost
  - The query is not fixed, and the resource consumption changes greatly, which is expected that the computing resources can be flexible and scalable independently
  - High concurrent writes are required, and metadata is too large for Delta Lake to meet performance requirements.
  - For data updates to primary keys, Hudi's MergeOnRead does not meet update performance requirements.



# Comparison of Data Lake Solutions
|   | Delta Lake | Hudi | Iceberg | LakeSoul |
| ----- | :----- | :----- | :----- | :----- | 
| Table Mode | CopyOnWrite | CopyOnWrite and MergeOnRead | only delete supports MergeOnRead | **CopyOnWrite and MergeOnRead** |
| Metadata Store | meta files on table path | meta files on table path | meta files on table path | **Cassandra** |
| Metadata Scalability | low | low | low | **high** |
| Read | partition pruning by meta files | partition pruning by meta files | metadata is richer and supports fine-grained pruning at file level | **partition pruning by meta data** |
| Concurrent Write | support | only support one writer, optimistic concurrency is still in the experimental stage | support | **support, the unique upsert mechanism turns updates into append, greatly improved concurrent write performance** |
| Streaming and Batch Unification | support | don't support concurrent write | support | **support** |
| Row Level Upsert | overwrite row-related files | support MergeOnRead, will be compacted at next snapshot | overwrite row-related files | **support MergeOnRead, only write the data to be updated** |
| Column Level upsert | overwrite all files in partition | support MergeOnRead, will overwrite all files in the partition at next snapshot | overwrite all files in partition | **support MergeOnRead, can last any time until user executes compaction, and optimized merge scan provides efficient read performance** |
| Bucket Join | nonsupport | nonsupport | nonsupport, bucket rules can be specified in partitioning spec, but there is no implementation yet | **support** |


# Performance Comparison Between LakeSoul and Iceberg
In above data Lake solutions, Delta Lake open source version exists to feed commercial version, and the community is inactive. Hudi focuses on the streaming scenario, and does not support concurrent write, as a data lake solution, its application scenario is too narrow. Iceberg, with its elegant abstract design and active community, as well as more and more new features as the version is updated, is one of the best data lake solution at present, so we choose Iceberg to compare performance.

![performance comparison](doc/performance_comparison.png)

# Maven
LakeSoul is only available with Scala version 2.12.

```xml
<dependency>
    <groupId>com.dmetasoul</groupId>
    <artifactId>lakesoul</artifactId>
    <version>1.0.0</version>
</dependency>
```

# Usage

## 1. Create and Write LakeSoulTable

### 1.1 Table Name

The table name in LakeSoul is a path, and the path where the data is stored is the table name.

When Dataframe.write(or writeStream) is called to write data to LakeSoulTable, a new table will automatically created using the storage path if the table does not exist.


### 1.2 Metadata Management
LakeSoul manages metadata through cassandra, so it can process metadata efficiently, and the meta cluster can be easily scaled up in the cloud.

You need to specify the cassandra cluster address when you use LakeSoul, which can be configured in spark environment or can be specified when submitting job.


### 1.3 Partition
LakeSoulTable can be partitioned in two ways, range and hash, and they can be used at the same time.
  - Range partition is a common time-based table partition. Data files of different partitions are stored in different partition paths.
  - To use a hash partition, you must specify both the hash primary key fields and the hash bucket num. The hash bucket num is used to hash the hash primary key fields.
  - If you specify both range partition and hash partition, each range partition will have the same hash key written to file with the same bucket id.
  - When partitioning is specified, data written to LakeSoulTable must contain partitioning fields.

Depending on the specific scenario, you can choose to use a range partition, a hash partition, or both. When a hash partition is specified, the data in LakeSoulTable will be unique by the primary key, which is the hash partition field + range partition field (if any).

When a hash partition is specified, LakeSoulTable supports upsert operations, where writing to data in APPEND mode is disabled, and the `lakeSoulTable.upsert()` method can be used instead.

### 1.4 Code Examples
```scala
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  //cassandra host
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()
import spark.implicits._

val df = Seq(("2021-01-01",1,"rice"),("2021-01-01",2,"bread")).toDF("date","id","name")
val tablePath = "s3a://bucket-name/table/path/is/also/table/name"

//create table
//spark batch
df.write
  .mode("append")
  .format("lakesoul")
  .option("rangePartitions","date")
  .option("hashPartitions","id")
  .option("hashBucketNum","2")
  .save(tablePath)
//spark streaming
import org.apache.spark.sql.streaming.Trigger
val readStream = spark.readStream.parquet("inputPath")
val writeStream = readStream.writeStream
  .outputMode("append")
  .trigger(Trigger.ProcessingTime("1 minutes"))
  .format("lakesoul")
  .option("rangePartitions","date")
  .option("hashPartitions","id")
  .option("hashBucketNum", "2")
  .option("checkpointLocation", "s3a://bucket-name/checkpoint/path")
  .start(tablePath)
writeStream.awaitTermination()

//for existing table, it no longer need to specify partition information when writing data
//equivalent to INSERT OVERWRITE PARTITION, if you do not specify option replaceWhere, the entire table will be overwritten
df.write
  .mode("overwrite")
  .format("lakesoul")
  .option("replaceWhere","date='2021-01-01'")
  .save(tablePath)

```

## 2. Read LakeSoulTable
You can read data by Spark API or building LakeSoulTable, Spark SQL is also supported, see [8. Operate LakeSoulTable by Spark SQL](#8-operate-lakeSoultable-by-spark-sql)

### 2.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()
val tablePath = "s3a://bucket-name/table/path/is/also/table/name"

//by spark api
val df1 = spark.read.format("lakesoul").load(tablePath)

//by LakeSoulTableRel
val df2 = LakeSoulTable.forPath(tablePath).toDF

```

## 3. Upsert LakeSoulTable

### 3.1 Batch
Upsert is supported when hash partitioning has been specified.

MergeOnRead is used by default, upsert data is written as delta files. LakeSoul provides efficient upsert and merge scan performance.

Parameter `spark.dmetasoul.lakesoul.deltaFile.enabled` can be set to `false` to use CopyOnWrite mode, eventually merged data will be generated after upsert, but this mode is not recommended, because it has poor performance and low concurrent.

#### 3.1.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()
import spark.implicits._

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"

val lakeSoulTable = LakeSoulTable.forPath(tablePath)
val extraDF = Seq(("2021-01-01",3,"chicken")).toDF("date","id","name")

lakeSoulTable.upsert(extraDF)
```

### 3.2 Streaming Support
In streaming, when outputMode is complete, each write will overwrite all previous data.
  
When outputMode is append or update, if hash partition is specified, each write is treated as an upsert, if data with the same primary key exists at read time, the latest value of the same key overrides the previous one. Update mode is available only if hash partition is specified.  
Duplicate data is allowed if no hash partitioning is used. 

## 4. Update LakeSoulTable
LakeSoul supports update operations, which are performed by specifying the condition and the field Expression that needs to be updated. There are several ways to perform update, see annotations in `LakeSoulTable`.

### 4.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)
import org.apache.spark.sql.functions._

//update(condition, set)
lakeSoulTable.update(col("date") > "2021-01-01", Map("data" -> lit("2021-01-02")))

```

## 5. Delete Data
LakeSoul supports delete operation to delete data that meet the conditions. Conditions can be any field, and if no condition is specified, all data in table will be deleted.

### 5.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//delete data that meet the condition
lakeSoulTable.delete("date='2021-01-01'")
//delete full table data
lakeSoulTable.delete()
```

## 6. Compaction
Upsert will generates delta files, which can affect read efficiency when delta files num become too large, in this time, compaction can be performed to merge files.

When compaction is performed to the full table, you can set conditions for compaction, only range partitions that meet the conditions will perform compaction.

Conditions to trigger compaction:
1. The last modification time for a range partition is before `spark.dmetasoul.lakesoul.compaction.interval` (ms), default is 12 hours
2. Delta file num exceeds `spark.dmetasoul.lakesoul.deltaFile.max.num`, default is 5

### 6.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//compaction on the specified partition
lakeSoulTable.compaction("date='2021-01-01'")
//compAction on all partitions of the table
lakeSoulTable.compaction()
//compaction on all partitions, but only partitions meet the conditions will be performed
lakeSoulTable.compaction(false)

```

## 7. Operate LakeSoulTable by Spark SQL 
Spark SQL is supported to read and write LakeSoulTable. To use it, you need to set `spark.sql.catalog.spark_catalog` to `org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog`.

Note:
  - Insert into statement turns `autoMerge` on by default
  - Spark SQL does not support to set hash partition while creating a LakeSoulTable
  - Cannot perform INSERT INTO on a hash partitioned table, use `lakeSoulTable.upsert()` instead
  - Some Spark SQL statements are not supported, see `org.apache.spark.sql.lakesoul.rules.LakeSoulUnsupportedOperationsCheck`

### 7.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoulsoul.catalog.LakeSoulCatalog")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
spark.range(10).createOrReplaceTempView("tmpView")

//write
spark.sql(s"insert overwrite table lakesoul.`$tablePath` partition (date='2021-01-01') select id from tmpView") 
//INSERT INTO cannot be used on a hash partitioned table, use `lakeSoulTable.upsert()` instead
spark.sql(s"insert into lakesoul.`$tablePath` select * from tmpView")

//read
spark.sql(s"select * from lakesoul.`$tablePath`").show()

```

## 8. Operator on Hash Primary Keys
When hash partition is specified, the data in each range partition is partitioned according to the hash primary key and the partitioned data is ordered. Therefore, there is no need to do shuffle and sort when some operators perform on hash primary key.

LakeSoul currently supports optimization of join, intersect, and except, and more operators will be supported in the future.

### 8.1 Join on Hash Keys
Scenarios:
  - Shuffle and sort are not required when data from different partitions of the same table is joined on the hash keys
  - If two different tables have the same hash field type and number of fields, and the same hash bucket num, there is no need to shuffle and sort when they are joined  on the hash keys

The test result shows that the efficiency of optimized join is 2.5 times that of Iceberg.

### 8.2 Intersect/Except on Hash Keys
Scenarios:
  - Intersect/Except on hash keys for different partitions of the same table does not require shuffle, sort, and distinct
  - Intersect/Except on hash keys for different tables that have the same type and number of hash keys, and the same hash bucket num, there is no need to shuffle, sort, and distinct 

In a range partition, the hash primary keys are unique, so the results of intersect or except are not repeated, so the subsequent operations do not need to deduplicate again. For example, you can directly `count` the number of data, without the need for `count distinc`.

The test result shows that the efficiency of optimized intersect and except is 3.9 times and 3.6 times that of Iceberg respectively.

### 8.3 Code Examples
```scala
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoulsoul.catalog.LakeSoulCatalog")
  .getOrCreate()
import spark.implicits._


val df1 = Seq(("2021-01-01",1,1,"rice"),("2021-01-02",2,2,"bread")).toDF("date","id1","id2","name")
val df2 = Seq(("2021-01-01",1,1,2.7),("2021-01-02",2,2,1.3)).toDF("date","id3","id4","price")

val tablePath1 = "s3a://bucket-name/table/path/is/also/table/name/1"
val tablePath2 = "s3a://bucket-name/table/path/is/also/table/name/2"

df1.write
  .mode("append")
  .format("lakesoul")
  .option("rangePartitions","date")
  .option("hashPartitions","id1,id2")
  .option("hashBucketNum","2")
  .save(tablePath1)
df2.write
  .mode("append")
  .format("lakesoul")
  .option("rangePartitions","date")
  .option("hashPartitions","id3,id4")
  .option("hashBucketNum","2")
  .save(tablePath2)


//join on hash keys without shuffle and sort
//different range partitions for the same table
spark.sql(
  s"""
    |select t1.*,t2.* from
    | (select * from lakesoul.`$tablePath1` where date='2021-01-01') t1
    | join 
    | (select * from lakesoul.`$tablePath1` where date='2021-01-02') t2
    | on t1.id1=t2.id1 and t1.id2=t2.id2
  """.stripMargin)
    .show()
//different tables with the same hash setting
spark.sql(
  s"""
    |select t1.*,t2.* from
    | (select * from lakesoul.`$tablePath1` where date='2021-01-01') t1
    | join 
    | (select * from lakesoul.`$tablePath2` where date='2021-01-01') t2
    | on t1.id1=t2.id3 and t1.id2=t2.id4
  """.stripMargin)
  .show()

//intersect/except on hash keys without shuffle,sort and distinct
//different range partitions for the same table
spark.sql(
  s"""
    |select count(1) from 
    | (select id1,id2 from lakesoul.`$tablePath1` where date='2021-01-01'
    |  intersect
    | select id1,id2 from lakesoul.`$tablePath1` where date='2021-01-02') t
  """.stripMargin)
  .show()
//different tables with the same hash setting
spark.sql(
  s"""
    |select count(1) from 
    | (select id1,id2 from lakesoul.`$tablePath1` where date='2021-01-01'
    |  intersect
    | select id3,id4 from lakesoul.`$tablePath2` where date='2021-01-01') t
  """.stripMargin)
  .show()

```

## 9. Schema Evolution
LakeSoul supports Schema Evolution, new columns allowed to be added (partitioning fields cannot be modified). When a new column is added and the existing data is read, the new column will be NULL. You can fill the new columns by upsert operation.

### 9.1 Merge Schema
Specify `mergeSchema` to `true` or enable `autoMerge` to merge the schema when writing data. The new schema is the union of table schema and the current written data schema.

### 9.2 Code Examples
```scala
df.write
  .mode("append")
  .format("lakesoul")
  .option("rangePartitions","date")
  .option("hashPartitions","id")
  .option("hashBucketNum","2")
  //first way
  .option("mergeSchema","true")
  .save(tablePath)
  
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  //second way
  .config("spark.dmetasoul.lakesoul.schema.autoMerge.enabled", "true")
  .getOrCreate()
```

## 10. Drop Partition
Drop a partition, also known as drop range partition, does not actually delete the data files. You can use the Cleanup operation to cleanup stale data.

### 10.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//drop the specified range partition
lakeSoulTable.dropPartition("date='2021-01-01'")

```

## 11. Drop Table
Drop table will directly deletes all the metadata and files.

### 11.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//drop table
lakeSoulTable.dropTable()

```