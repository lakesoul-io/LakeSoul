# Spark API Docs

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 1. Create and Write LakeSoulTable

### 1.1 Table Name

The table name in LakeSoul can be a path, and the directory where the data is stored is the table name of LakeSoulTable. At the same time a table can have a table name to help remember, or to access in SQL, that is not a string in the form of a path.

When calling the Dataframe.write.save method to write data to LakeSoulTable, if the table does not exist, a new table will be automatically created using the storage path, but there is no table name by default, and can only be accessed through the path. You can add `option("shortTableName" , "table_name")` option to set the table name.

Through DataFrame.write.saveAsTable, a table will be created, which can be accessed by the table name. The default path is `spark.sql.warehouse.dir`/current_database/table_name, which can be accessed by the path or table name later. To customize the table path, you can add `option("path", "s3://bucket/...")` option.

When creating a table through SQL, the table name can be a path or a table name, and the path must be an absolute path. If it is a table name, the rules of the path are consistent with the above Dataframe.write.saveAsTable, which can be set through the LOCATION clause in `CREATE TABLE` SQL. For how to create a primary key partition table in SQL, you can refer to [7. Use Spark SQL to operate LakeSoul table](#7-operate-lakesoultable-by-spark-sql)


### 1.2 Metadata Management
LakeSoul manages metadata through external database, so it can process metadata efficiently, and the meta cluster can be easily scaled up in the cloud.

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
You can read data by Spark API or building LakeSoulTable, Spark SQL is also supported, see [7. Operate LakeSoulTable by Spark SQL](#7-operate-lakeSoultable-by-spark-sql)

### 2.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
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
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)
import org.apache.spark.sql.functions._

//update(condition, set)
lakeSoulTable.update(col("date") > "2021-01-01", Map("date" -> lit("2021-01-02")))

```

## 5. Delete Data
LakeSoul supports delete operation to delete data that meet the conditions. Conditions can be any field, and if no condition is specified, all data in table will be deleted.

### 5.1 Code Examples

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
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

### 6.2 Compaction And Load Partition to Hive
Since version 2.0, LakeSoul supports load partition into Hive after compaction.

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
val lakeSoulTable = LakeSoulTable.forName("lakesoul_test_table")
lakeSoulTable.compaction("date='2021-01-01'", "spark_catalog.default.hive_test_table")
```

**Note** If `lakesoul` has been set as default catalog, Hive tables should be referenced with `spark_catalog` prefix.

## 7. Operate LakeSoulTable by Spark SQL 

LakeSoul supports Spark SQL to read and write data. When using it, you need to set `spark.sql.catalog.lakesoul` to `org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog`. At the same time, you can also set LakeSoul as the default Catalog, that is, add the `spark.sql.defaultCatalog=lakesoul` configuration item.
What has to be aware of is:
   - The `insert into` function cannot be performed on a hash partitioned table, please use the `MERGE INTO` SQL syntax;

### 7.1 Code Examples

#### 7.1.1 DDL SQL

```sql
# To create a primary key table, you need to set the primary key name and the number of hash buckets through TBLPROPERTIES, if not set, it is a non-primary key table
# To create a primary key CDC table, you need to add the table attribute `'lakesoul_cdc_change_column'='change_kind'`, please refer to [LakeSoul CDC table](../03-Usage%20Docs/04-cdc-ingestion-table.mdx)
CREATE TABLE default.table_name (id string, date string, data string) USING lakesoul
    PARTITIONED BY (date)
    LOCATION 's3://bucket/table_path'
    TBLPROPERTIES(
      'hashPartitions'='id',
      'hashBucketNum'='2')
```

It also supports adding or deleting columns using ALTER TABLE. This part has the same syntax as Spark SQL and does not support changing the column type.

#### 7.1.2 DML SQL

```sql
# INSERT INTO
insert overwrite/into table default.table_name partition (date='2021-01-01') select id from tmpView

# MERGE INTO
# For primary key tables, Upsert can be implemented through the `Merge Into` statement
# Currently does not support MATCHED/NOT MATCHED conditional statements in Merge Into
# The ON clause can only contain expressions with equal primary keys. Non-primary key column joins are not supported, and non-equal expressions are not supported
MERGE INTO default.`table_name` AS t USING source_table AS s
    ON t.hash = s.hash
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
```

**Notice**:
* The database (namespace) name can be added before the table name, the default is the database name of the current `USE`, and the `default` if the `USE database` has not been executed
* The table path can be set using the LOCATION clause or the `path` table property, if no path is set, it defaults to `spark.sql.warehouse.dir`/database_name/table_name/
* You can use the table path to read and write a LakeSoul table. In SQL, the table name part needs to be written as lakesoul.default.`table_path`

## 8. Operator on Hash Primary Keys
When hash partition is specified, the data in each range partition is partitioned according to the hash primary key and the partitioned data is ordered. Therefore, there is no need to do shuffle and sort when some operators perform on hash primary key.

LakeSoul currently supports optimization of join, intersect, and except, and more operators will be supported in the future.

### 8.1 Join on Hash Keys
Scenarios:
  - Shuffle and sort are not required when data from different partitions of the same table is joined on the hash keys
  - If two different tables have the same hash field type and number of fields, and the same hash bucket num, there is no need to shuffle and sort when they are joined  on the hash keys

### 8.2 Intersect/Except on Hash Keys
Scenarios:
  - Intersect/Except on hash keys for different partitions of the same table does not require shuffle, sort, and distinct
  - Intersect/Except on hash keys for different tables that have the same type and number of hash keys, and the same hash bucket num, there is no need to shuffle, sort, and distinct 

In a range partition, the hash primary keys are unique, so the results of intersect or except are not repeated, so the subsequent operations do not need to deduplicate again. For example, you can directly `count` the number of data, without the need for `count distinc`.

### 8.3 Code Examples
```scala
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
  .config("spark.sql.defaultCatalog", "lakesoul")
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
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//drop table
lakeSoulTable.dropTable()

```