# Spark API 文档

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 1. 创建和写入 LakeSoulTable

### 1.1 Table Name

LakeSoul 中表名可以是一个路径，数据存储的目录就是 LakeSoulTable 的表名。同时一个表可以有一个表名帮助记忆，或在SQL中访问，即不是路径形式的一个字符串。

当调用 Dataframe.write.save 方法向 LakeSoulTable 写数据时，若表不存在，则会使用存储路径自动创建新表，但是默认没有表名，只能通过路径访问，可以通过添加 `option("shortTableName", "table_name")` 选项来设置表名。

通过 DataFrame.write.saveAsTable，会创建表，可以通过表名访问，路径默认为 `spark.sql.warehouse.dir`/current_database/table_name，后续可以通过路径或表名访问。如需自定义表路径，则可以加上 `option("path", "s3://bucket/...")` 选项。

通过 SQL 建表时，表名可以是路径或一个表名，路径必须是绝对路径。如果是表名，则路径的规则和上面 Dataframe.write.saveAsTable 一致，可以在 `CREATE TABLE` SQL 中通过 LOCATION 子句设置。关于如何在 SQL 中创建主键分区表，可以参考 [7. 使用 Spark SQL 操作 LakeSoul 表](#7-使用-spark-sql-操作-lakesoultable)

### 1.2 元数据管理
LakeSoul 通过数据是管理 meta 数据，因此可以高效的处理元数据，并且 meta 集群可以很方便的在云上进行扩容。

### 1.3 Partition
LakeSoulTable 有两种分区方式，分别是 range 分区和 hash 分区，可以两种分区同时使用。

  - range 分区即通常的基于时间的表分区，不同分区的数据文件存储在不同的分区路径下；
  - 使用 hash 分区，必须同时指定 hash 分区主键字段和 hash bucket num，在写数据时，会根据 bucket num 对 hash 主键字段值进行散列，取模后相同数据会写到同一个文件，文件内部根据 hash 字段值升序排列；
  - 若同时指定了 range 分区和 hash 分区，则每个 range 分区内，hash 值相同的数据会写到同一个文件里;
  - 指定分区后，写入 LakeSoulTable 的数据必须包含分区字段。

可以根据具体场景选择使用 range 分区或 hash 分区，或者同时使用两者。当指定 hash 分区后，LakeSoulTable 的数据将根据主键唯一，主键字段为 hash 分区字段 + range 分区字段（如果存在）。

当指定 hash 分区时，LakeSoulTable 支持 upsert 操作 (scala/sql)，此时 append 模式写数据被禁止，可以使用 `LakeSoulTable.upsert()` 方法或者 `MERGE INTO` SQL 语句。

### 1.4 代码示例
```scala
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  // 使用 SQL 功能还需要增加以下两个配置项
  .config("spark.sql.catalog.lakesoul", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
  .config("spark.sql.defaultCatalog", "lakesoul")
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

//对于已存在的表，写数据时不需要再指定分区信息
//相当于 insert overwrite partition，如果不指定 replaceWhere，则会重写整张表
df.write
  .mode("overwrite")
  .format("lakesoul")
  .option("replaceWhere","date='2021-01-01'")
  .save(tablePath)

```

## 2. Read LakeSoulTable
可以通过 Spark read api 或者构建 LakeSoulTable 来读取数据，LakeSoul 也支持通过 Spark SQL 读取数据，详见 [7. 使用 Spark SQL 操作 LakeSoulTable](#7-使用-spark-sql-操作-lakesoultable)

### 2.1 代码示例

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()
val tablePath = "s3a://bucket-name/table/path/is/also/table/name"

//方法一
val df1 = spark.read.format("lakesoul").load(tablePath)

//方法二
val df2 = LakeSoulTable.forPath(tablePath).toDF

```

## 3. Upsert LakeSoulTable

### 3.1 Batch
当 LakeSoulTable 使用 hash 分区时，支持 upsert 功能。  

默认情况下使用 MergeOnRead 模式，upsert 数据以 delta file 的形式写入表路径，LakeSoul 提供了高效的 upsert 和 merge scan 性能。  

#### 3.1.1 代码示例

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

### 3.2 Streaming 支持
流式场景中，若 outputMode 为 complete，则每次写数据都会 overwrite 之前的数据。  

当 outputMode 为 append 或 update 时，如果指定了 hash 分区，则每次写入数据视为进行一次 upsert 更新，读取时如果存在相同主键的数据，同一字段的最新值会覆盖之前的值。仅当指定 hash 分区时，update outputMode 可用。   
若未使用 hash 分区，则允许存在重复数据。  

## 4. Update LakeSoulTable
LakeSoul 支持 update 操作，通过指定条件和需要更新的字段 expression 来执行。有多种方式可以执行 update，详见 `LakeSoulTable` 注释。

### 4.1 代码示例

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
lakeSoulTable.update(col("date") > "2021-01-01", Map("data" -> lit("2021-01-02")))

```

## 5. Delete Data
LakeSoul 支持 delete 操作删除符合条件的数据，条件可以是任意字段，若不指定条件，则会删除全表数据。

### 5.1 代码示例

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//删除符合条件的数据
lakeSoulTable.delete("date='2021-01-01'")
//删除全表数据
lakeSoulTable.delete()
```

## 6. Compaction
执行 upsert 会生成 delta 文件，当 delta 文件过多时，会影响读取效率，此时可以执行 compaction 合并文件。  

当执行全表 compaction 时，可以给 compaction 设置条件，只有符合条件的 range 分区才会执行 compaction 操作。  

### 6.1 代码示例

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//对指定分区执行 compaction 操作
lakeSoulTable.compaction("date='2021-01-01'")
//对全表所有分区执行 compaction 操作
lakeSoulTable.compaction()
//对全表所有分区执行 compaction 操作，会检测是否符合执行 compaction 的条件，只有符合条件的才会执行
lakeSoulTable.compaction(false)
```

### 6.2 Compaction 后挂载到 Hive Meta
自 2.0 版本起，LakeSoul 支持将 Compaction 后的目录路径，挂载到指定的 Hive 表，并保持所有 Range 分区名不变和自定义分区名功能。该功能可以方便下游一些只能支持访问 Hive 的系统读取到 LakeSoul 的数据。更推荐的方式是通过 Kyuubi 来支持 Hive JDBC，这样可以直接使用 Hive JDBC 调用 Spark 引擎来访问 LakeSoul 表，包括 Merge on Read 读取

要使用 LakeSoul 导出分区到 Hive Meta 的功能，保持 hive 分区名不变，可以执行如下 Compaction 调用：
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
val lakeSoulTable = LakeSoulTable.forName("lakesoul_test_table")
lakeSoulTable.compaction("date='2021-01-01'", "spark_catalog.default.hive_test_table")
```
自定义 hive 分区名，可以执行如下 Compaction 调用：
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
val lakeSoulTable = LakeSoulTable.forName("lakesoul_test_table")
lakeSoulTable.compaction("date='2021-01-02'", "spark_catalog.default.hive_test_table", "date='20210102'")
```

**注意** 如果将 LakeSoul Catalog 设置为了 Spark 默认 Catalog，则 Hive 表名前面需要加上 `spark_catalog`。

## 7. 使用 Spark SQL 操作 LakeSoulTable
LakeSoul 支持 Spark SQL 读写数据，使用时需要设置 `spark.sql.catalog.lakesoul` 为 `org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog`。同时也可以将 LakeSoul 设置为默认 Catalog，即增加 `spark.sql.defaultCatalog=lakesoul` 配置项。
需要注意的是：
  - 不能对 hash 分区的表执行 `insert into` 功能，请使用 `MERGE INTO` SQL 语法；

### 7.1 代码示例

#### 7.1.1 DDL SQL

```sql
# 创建主键表，需要通过 TBLPROPERTIES 设置主键名和哈希分桶数，没有设置则为非主键表
# 创建主键CDC表，需要增加表属性 `'lakesoul_cdc_change_column'='change_kind'`，具体请参考 [LakeSoul CDC 表](../03-Usage%20Docs/04-cdc-ingestion-table.mdx)
CREATE TABLE default.table_name (id string, date string, data string) USING lakesoul
    PARTITIONED BY (date)
    LOCATION 's3://bucket/table_path'
    TBLPROPERTIES(
      'hashPartitions'='id',
      'hashBucketNum'='2')
```

同时也支持使用 ALTER TABLE 增加或删除列，该部分与 Spark SQL 语法相同，暂不支持修改列的类型。

#### 7.1.2 DML SQL

```sql
# INSERT INTO
insert overwrite/into table default.table_name partition (date='2021-01-01') select id from tmpView

# MERGE INTO
# 对主键表，可以通过 `Merge Into` 语句来实现 Upsert
# 暂不支持 Merge Into 中 MATCHED/NOT MATCHED 带条件的语句
# ON 子句只能包含主键相等的表达式，不支持非主键列连接，不支持非相等表达式
MERGE INTO default.`table_name` AS t USING source_table AS s
    ON t.hash = s.hash
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
```

**注意**：
* 表名前可以添加 database(namespace) 名，默认为当前 `USE` 的 database 名，没有执行过 `USE database` 则为 `default`
* 可以使用 LOCATION 子句或 `path` 表属性来设置表路径，如果没有设置路径，则默认为 `spark.sql.warehouse.dir`/database_name/table_name
* 可以使用表路径来读写一个 LakeSoul 表，在 SQL 中表名部分需要写成 lakesoul.default.`table_path`

## 8. Operator on Hash Primary Keys
指定 hash 分区后，LakeSoul 各 range 分区内的数据根据 hash 主键字段分片且分片数据有序，因此部分算子作用于 hash 主键字段时，无需 shuffle 和 sort。  
 
LakeSoul 目前支持 join、intersect 和 except 算子的优化，后续将支持更多算子。


### 8.1 Join on Hash Keys
支持的场景：
  - 对于同一张表，不同分区的数据根据 hash 字段进行 join 时，无需 shuffle 和 sort
  - 若两张不同表的 hash 字段类型和字段数量相同，且 hash bucket 数量相同，它们之间根据 hash 字段进行 join 时，也无需 shuffle 和 sort

### 8.2 Intersect/Except on Hash Keys
支持的场景：
  - 对同一张表不同分区的 hash 字段执行 intersect/except 时，无需 shuffle、sort 和 distinct
  - 对两张不同的表，若它们拥有相同的 hash 字段类型和字段数量且 hash bucket 数量相同，对 hash 字段执行 intersect/except 时，无需 shuffle、sort 和 distinct

range 分区内，hash 主键字段值是唯一的，因此 intersect 或 except 的结果是不重复的，后续操作不需要再次去重，例如可以直接 `count` 获取不重复数据的数量，无需 `count distinct`。

### 8.3 代码示例
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
//相同表的不同 range 分区
spark.sql(
  s"""
    |select t1.*,t2.* from
    | (select * from lakesoul.`$tablePath1` where date='2021-01-01') t1
    | join 
    | (select * from lakesoul.`$tablePath1` where date='2021-01-02') t2
    | on t1.id1=t2.id1 and t1.id2=t2.id2
  """.stripMargin)
    .show()
//相同 hash 设置的不同表
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
//相同表的不同 range 分区
spark.sql(
  s"""
    |select count(1) from 
    | (select id1,id2 from lakesoul.`$tablePath1` where date='2021-01-01'
    |  intersect
    | select id1,id2 from lakesoul.`$tablePath1` where date='2021-01-02') t
  """.stripMargin)
  .show()
//相同 hash 设置的不同表
spark.sql(
  s"""
    |select count(1) from 
    | (select id1,id2 from lakesoul.`$tablePath1` where date='2021-01-01'
    |  intersect
    | select id3,id4 from lakesoul.`$tablePath2` where date='2021-01-01') t
  """.stripMargin)
  .show()

```

## 9. Schema 演进
LakeSoul 支持 schema 演进功能，可以新增列 (分区字段无法修改)。新增列后，读取现有数据，该新增列会是 NULL。你可以通过使用 upsert 功能，为现有数据追加该新列。

### 9.1 Merge Schema
在写数据时指定 `mergeSchema` 为 `true`，或者启用 `autoMerge` 来 merge schema，新的 schema 为表原本 schema 和当前写入数据 schema 的并集。  

### 9.2 代码示例
```scala
df.write
  .mode("append")
  .format("lakesoul")
  .option("rangePartitions","date")
  .option("hashPartitions","id")
  .option("hashBucketNum","2")
  //方式一
  .option("mergeSchema","true")
  .save(tablePath)
  
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  //方式二
  .config("spark.dmetasoul.lakesoul.schema.autoMerge.enabled", "true")
  .getOrCreate()
```

## 10. Drop Partition
删除分区，也就是删除 range 分区，实际上并不会真正删掉数据文件，可以使用 cleanup 功能清理失效数据

### 10.1 代码示例

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//删除指定 range 分区
lakeSoulTable.dropPartition("date='2021-01-01'")

```

## 11. Drop Table
删除表会直接删除表的所有 meta 数据和文件数据

### 11.1 代码示例

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//删除表
lakeSoulTable.dropTable()

```