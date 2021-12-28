# LakeSoul
LakeSoul 是由 [DMetaSoul](https://www.dmetasoul.com) 研发的构建于 Apache Spark 引擎之上的流批一体表存储框架，具备高可扩展的元数据管理、ACID 事务、高效灵活的 upsert 操作、Schema 演进和批流一体化处理。

LakeSoul 专门为数据湖云存储之上的数据进行行、列级别增量更新、高并发入库、批量扫描读取做了大量优化。云原生计算存储分离的架构使得部署非常简单，同时可以以很低的成本支撑极大的数据量。具体来说，LakeSoul 有如下几个特点：

  - 弹性架构：计算存储完全分离，不需要固定节点和磁盘，计算存储各自弹性扩容。并且针对云存储做了大量优化，在对象存储上实现了并发一致性、增量更新等功能；使用 LakeSoul 不需要维护固定的存储节点，云上对象存储的成本只有本地磁盘的 1/10，极大地降低了存储成本和运维成本；
  - 高效可扩展的元数据管理：LakeSoul 使用 Cassandra 数据库来管理文件元数据，可以高效的处理元数据的修改，并能够支持多并发写入，解决了 Hive、Delta Lake 等元数据层的性能瓶颈，长时间运行后元数据解析缓慢以及只能单点写入的痛点；
  - ACID 事务：undo 和 redo 机制保证了提交的事务性，用户不会看到不一致数据；多个并发写入、读取都能保证一致性；
  - 多级分区模式和高效灵活的 upsert 操作：LakeSoul 支持 range 和 hash 分区，通过灵活的 upsert 功能，支持行、列级别的增、删、改等更新操作，将 upsert 数据以 delta file 的形式保存，大幅提高了写数据效率和并发性，而优化过的 merge scan 提供了高效的 MergeOnRead 读取性能；
  - 批流一体：LakeSoul 支持 streaming sink，可以同时处理流式数据摄入和历史数据批量回填、交互式查询等场景；
  - Schema 演进：可以随时新增字段，并为新字段快速填充历史数据。

LakeSoul 的适用场景：
  - 新增数据需要高效实时大批量写入，同时需要行、列级别的并发增量更新的场景；
  - 历史数据存储量很大，并且需要对大跨度时间范围做明细查询、修改，同时希望保持较低成本的场景；
  - 查询请求不固定，资源消耗变化较大，希望计算资源能够独立弹性伸缩的场景；
  - 需要多并发写，同时文件数量多，Delta Lake 元数据更新无法满足性能要求的场景；
  - 针对主键进行数据更新，Hudi 的 MergeOnRead 无法满足更新性能要求的场景。

# 流批一体开源方案对比
|   | Delta Lake | Hudi | Iceberg | LakeSoul |
| ----- | :----- | :----- | :----- | :----- | 
| 表模式 | CopyOnWrite | CopyOnWrite 和 MergeOnRead | Delete 操作使用 MergeOnRead，其他功能使用 CopyOnWrite | **CopyOnWrite 和 MergeOnRead** |
| 元数据存储 | 表路径 meta 文件 | 表路径 meta 文件 | 表路径 meta 文件 | **Cassandra** |
| 元数据扩展性 | 较弱 | 较弱 | 较弱 | **极强** |
| 查询 | 通过 meta 文件进行分区裁剪 | 通过 meta 文件进行分区裁剪 | 通过 meta 文件进行裁剪，元数据更丰富，支持文件级别的细粒度裁剪 | **通过 meta 数据进行分区裁剪** |
| 并发写 | 支持 | 默认的 MVCC 并发控制只支持单数据源写入，Optimistic Concurrency 还在实验阶段，且需要 Zookeeper 或者 HiveMetastore 来获取锁 | 支持 | **支持，独特的 upsert 机制将更新变成 append，大幅提高了并发写性能** |
| 批流一体 | 支持 | 不支持批流并发写 | 支持 | **支持** |
| 行式 upsert | 重写行相关数据文件 | 支持 MergeOnRead，会在下个快照版本被 compaction | 重写行相关数据文件 | **支持 MergeOnRead，仅写待更新字段的数据** |
| 列式 upsert | 重写分区内所有数据文件 | 支持 MergeOnRead，但此场景会在下一个快照版本重写所有分区内数据文件 | 重写分区内所有数据文件 | **支持 MergeOnRead，可以持续任意时间，直到用户执行 compaction，优化过的 merge scan 提供了高效的读性能** |
| bucket join | 不支持 | 不支持 | 不支持，分区规范中可以指定 bucket 规则，但暂无相关实现 | **支持** |


# LakeSoul 和 Iceberg 的性能对比
在上述数据湖方案中，Delta Lake 开源版作为商业版的引流而存在，社区活跃度不高，Hudi 专注于流式场景，且不支持批流并发写，作为数据湖方案应用场景太窄。而 Iceberg 拥有优雅的抽象设计和活跃的社区，以及随着版本更新越来越多的新特性，是当前最好的数据湖方案之一，因此我们选择 Iceberg 作为性能比较对象。

![performance comparison](doc/performance_comparison.png)

# 在你的 Spark 项目中引入依赖
需要使用 Scala 2.12 来支持 LakeSoul。
```xml
<dependency>
    <groupId>com.dmetasoul</groupId>
    <artifactId>lakesoul</artifactId>
    <version>1.0.0</version>
</dependency>
```

# 使用文档

## 1. 创建和写入 LakeSoulTable

### 1.1 Table Name

LakeSoul 中表名是一个路径，数据存储的目录就是 LakeSoulTable 的表名。  

当调用 Dataframe.write(writeStream) 方法向 LakeSoulTable 写数据时，若表不存在，则会使用存储路径自动创建新表。

### 1.2 元数据管理
LakeSoul 通过 Cassandra 管理 meta 数据，因此可以高效的处理元数据，并且 meta 集群可以很方便的在云上进行扩容。

在使用时需要指定 Cassandra 集群地址，可以配置到 Spark 环境中，也可以在作业运行时指定。

### 1.3 Partition
LakeSoulTable 有两种分区方式，分别是 range 分区和 hash 分区，可以两种分区同时使用。

  - range 分区即通常的基于时间的表分区，不同分区的数据文件存储在不同的分区路径下；
  - 使用 hash 分区，必须同时指定 hash 分区主键字段和 hash bucket num，在写数据时，会根据 bucket num 对 hash 主键字段值进行散列，取模后相同数据会写到同一个文件，文件内部根据 hash 字段值升序排列；
  - 若同时指定了 range 分区和 hash 分区，则每个 range 分区内，hash 值相同的数据会写到同一个文件里;
  - 指定分区后，写入 LakeSoulTable 的数据必须包含分区字段。

可以根据具体场景选择使用 range 分区或 hash 分区，或者同时使用两者。当指定 hash 分区后，LakeSoulTable 的数据将根据主键唯一，主键字段为 hash 分区字段 + range 分区字段（如果存在）。

当指定 hash 分区时，LakeSoulTable 支持 upsert 操作，此时 append 模式写数据被禁止，可以使用 `LakeSoulTable.upsert()` 方法来代替。

### 1.4 代码示例
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

//对于已存在的表，写数据时不需要再指定分区信息
//相当于 insert overwrite partition，如果不指定 replaceWhere，则会重写整张表
df.write
  .mode("overwrite")
  .format("lakesoul")
  .option("replaceWhere","date='2021-01-01'")
  .save(tablePath)

```

## 2. Read LakeSoulTable
可以通过 Spark read api 或者构建 LakeSoulTable 来读取数据，LakeSoul 也支持通过 Spark SQL 读取数据，详见 [8. 使用 Spark SQL 操作 LakeSoulTable](#8-使用-spark-sql-操作-LakeSoulTable)

### 2.1 代码示例

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
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

可以通过设置参数 `spark.dmetasoul.lakesoul.deltaFile.enabled` 为 `false` 开启 CopyOnWrite 模式，每次 upsert 都生成最终合并数据，但不建议这么做，因为写效率很差且并发度较低。

#### 3.1.1 代码示例

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
LakeSoul 支持 delete 操作删除符合条件的数据，条件可以是任意字段，若不指定条件，则会删除全表数据。

### 5.1 代码示例

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
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

触发 compaction 的条件：
1. range 分区最后一次修改时间在设置的 `spark.dmetasoul.lakesoul.compaction.interval` (ms) 之前，默认是 12 小时
2. range 分区 upsert 产生的 delta file num 超过了设置的 `spark.dmetasoul.lakesoul.deltaFile.max.num`，默认是 5

### 6.1 代码示例

```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
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

## 7. 使用 Spark SQL 操作 LakeSoulTable
LakeSoul 支持 Spark SQL 读写数据，使用时需要设置 `spark.sql.catalog.spark_catalog` 为 `org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog`。  
需要注意的是：
  - insert into 语句会默认开启 `autoMerge` 功能；
  - 建表语句中指定的分区为 range 分区，暂不支持通过 Spark SQL 在建表时设置 hash 分区；
  - 不能对 hash 分区的表执行 `insert into` 功能，请使用 `lakeSoulTable.upsert()` 方法；
  - lakeSoulTable 暂不支持部分 Spark SQL 语句，详见 `org.apache.spark.sql.lakesoul.rules.LakeSoulUnsupportedOperationsCheck`；

### 7.1 代码示例

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

//写数据
spark.sql(s"insert overwrite table lakesoul.`$tablePath` partition (date='2021-01-01') select id from tmpView") 
//insert into 不能对 hash 分区表使用，请使用 LakeSoulTableRel.upsert() 方法
spark.sql(s"insert into lakesoul.`$tablePath` select * from tmpView")

//读数据
spark.sql(s"select * from lakesoul.`$tablePath`").show()
```

## 8. Operator on Hash Primary Keys
指定 hash 分区后，LakeSoul 各 range 分区内的数据根据 hash 主键字段分片且分片数据有序，因此部分算子作用于 hash 主键字段时，无需 shuffle 和 sort。  
 
LakeSoul 目前支持 join、intersect 和 except 算子的优化，后续将支持更多算子。


### 8.1 Join on Hash Keys
支持的场景：
  - 对于同一张表，不同分区的数据根据 hash 字段进行 join 时，无需 shuffle 和 sort
  - 若两张不同表的 hash 字段类型和字段数量相同，且 hash bucket 数量相同，它们之间根据 hash 字段进行 join 时，也无需 shuffle 和 sort

测试结果显示，优化后 join 算子的效率是 Iceberg 的2.5倍。

### 8.2 Intersect/Except on Hash Keys
支持的场景：
  - 对同一张表不同分区的 hash 字段执行 intersect/except 时，无需 shuffle、sort 和 distinct
  - 对两张不同的表，若它们拥有相同的 hash 字段类型和字段数量且 hash bucket 数量相同，对 hash 字段执行 intersect/except 时，无需 shuffle、sort 和 distinct

range 分区内，hash 主键字段值是唯一的，因此 intersect 或 except 的结果是不重复的，后续操作不需要再次去重，例如可以直接 `count` 获取不重复数据的数量，无需 `count distinct`。

测试结果显示，优化后 intersect 和 except 算子的效率分别是 Iceberg 的3.9倍和3.6倍。 

### 8.3 代码示例
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
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
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
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
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
  .config("spark.dmetasoul.lakesoul.meta.host", "cassandra_host")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable.forPath(tablePath)

//删除表
lakeSoulTable.dropTable()

```