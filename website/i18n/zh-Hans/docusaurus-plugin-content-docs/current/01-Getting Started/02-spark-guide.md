# Spark 快速开始

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## 配置
要在Spark中使用LakeSoul，请首先配置[Spark Catalog](01-setup-local-env.mdx)。LakeSoul使用Apache Spark的DataSourceV2 API来实现数据源和目录。此外，LakeSoul还提供了 Scala 的表API，以扩展LakeSoul数据表的功能。


### Spark 3 Support Matrix

LakeSoul | Spark Version
--- | ---
2.2.x-2.4.x |3.3.x
2.0.x-2.1.x| 3.1.x

### Spark Shell/SQL

使用`LakeSoulSparkSessionExtension` sql扩展来运行spark-shell/spark-sql。

<Tabs>
  <TabItem value="Spark SQL" label="Spark SQL" default>

```bash
spark-sql --conf spark.sql.extensions=com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension --conf spark.sql.catalog.lakesoul=org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog --conf spark.sql.defaultCatalog=lakesoul
```

  </TabItem>

  <TabItem value="Scala" label="Scala">

```bash
spark-shell 
```
</TabItem>

</Tabs>

### 依赖配置

然后，我们导入LakeSoul依赖项并设置Spark配置，后续的使用案例都将依赖这里的配置。

<Tabs>
  <TabItem value="Spark SQL" label="Spark SQL" default>

```sql
// Spark SQL
// Required dependencies has been imported last paragraph
```

  </TabItem>

  <TabItem value="Scala" label="Scala">

```scala
// Scala
import org.apache.spark.sql.SparkSession
import spark.implicits._
import com.dmetasoul.lakesoul.tables.LakeSoulTable

val spark = SparkSession.builder()
    .master("local")
    .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
    .config("spark.sql.defaultCatalog", "lakesoul")
    .getOrCreate()
    
```

</TabItem>

</Tabs>


## 创建命名空间
首先，为LakeSoul表创建一个namespace，如果不创建将使用默认的namespace，LakeSoul Catalog的默认namespace是`default`。

```sql
// Spark SQL
CREATE NAMESPACE lakesoul_namespace;
USE lakesoul_namespace
```

## 创建表
使用`USING lakesoul`的子句创建一个分区的LakeSoul表

<Tabs>
  <TabItem value="Spark SQL" label="Spark SQL" default>

```sql
// Spark SQL
CREATE TABLE lakesoul_table (id BIGINT, date STRING, data STRING) 
USING lakesoul 
PARTITIONED BY (date) 
LOCATION 'file:/tmp/lakesoul_namespace/lakesoul_table'

```
  </TabItem>

  <TabItem value="Scala" label="Scala">

```scala
// scala
// First commit will auto-initialize the table
```
</TabItem>

</Tabs>

### 哈希分区表
在LakeSoul中，带有主键的表被定义为哈希分区表。使用USING lakesoul子句，并结合TBLPROPERTIES设置（其中'hashPartitions'指定以逗号分隔的主键列表，'hashBucketNum'指定哈希桶的大小），可以创建一个哈希分区的LakeSoul表。

```sql
// Spark SQL
CREATE TABLE lakesoul_hash_table (id BIGINT NOT NULL, date STRING, name STRING) 
USING lakesoul 
PARTITIONED BY (date) 
LOCATION 'file:/tmp/lakesoul_namespace/lakesoul_hash_table' 
TBLPROPERTIES ( 
  'hashPartitions'='id',
  'hashBucketNum'='2')
```

### CDC表
哈希分区的LakeSoul表具有可选的数据变更捕获（CDC）功能，能够记录数据的变化。要创建支持CDC的LakeSoul表，可以在哈希分区表的DDL语句中添加额外的`TBLPROPERTIES`设置，指定`'lakesoul_cdc_change_column'`属性。这个属性定义了一个隐式列，帮助表有效地处理CDC信息，从而实现对数据变更的精确追踪和管理。

```sql
// Spark SQL
CREATE TABLE lakesoul_cdc_table (id BIGINT NOT NULL, date STRING, name STRING) 
USING lakesoul 
PARTITIONED BY (date) 
LOCATION 'file:/tmp/lakesoul_namespace/lakesoul_cdc_table' 
TBLPROPERTIES( 
  'hashPartitions'='id',
  'hashBucketNum'='2',
  'lakesoul_cdc_change_column' = 'op'
)
```

## 数据插入/合并

要使用Spark SQL向非哈希分区表写入数据，请使用`INSERT INTO`语句。

要使用DataFrame向表写入数据，请使用`DataFrameWriterV2` API。如果这是对该表的第一次写入，它还将自动创建相应的LakeSoul表。
<Tabs>
  <TabItem value="Spark SQL" label="Spark SQL" default>

```sql
// Spark SQL
INSERT INTO TABLE lakesoul_table VALUES (1, '2024-01-01', 'Alice'), (2, '2024-01-01', 'Bob'), (1, "2024-01-02", "Cathy")
```
  </TabItem>

  <TabItem value="Scala" label="Scala">

```scala
// Scala
val data: DataFrame = Seq((1, "2024-01-01", "Alice"), (2, "2024-01-01", "Bob"), (1, "2024-01-02", "Cathy"))
              .toDF("id", "date", "name")
data.write.format("lakesoul").insertIno("lakesoul_table")
```
  </TabItem>

</Tabs>

要使用Spark SQL向哈希分区表写入数据，请使用`Merge INTO`语句。

要使用DataFrame向哈希分区表写入数据，请使用`LakeSoulTable`的`upsert` API。


<Tabs>
  <TabItem value="Spark SQL" label="Spark SQL" default>

```sql
// Spark SQL
// Create source_view
CREATE OR REPLACE VIEW spark_catalog.default.source_view (id , date, data)
AS SELECT (1 as `id`, '2024-01-01' as `date`, 'data' as `data`)

// Merge source_view Into lakesoul_hash_table

MERGE INTO lakesoul_hash_table AS t 
USING spark_catalog.default.source_view AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

  </TabItem>

  <TabItem value="Scala" label="Scala">

```scala
// Scala
val tablePath = "file:/tmp/lakesoul_namespace/lakesoul_hash_table"

// Init hash table with first dataframe
val df = Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value")
val writer = df.write.format("lakesoul").mode("overwrite")

writer
    .option("rangePartitions", rangePartition.mkString(","))
    .option("hashPartitions", hashPartition.mkString(","))
    .option("hashBucketNum", hashBucketNum)
    .save(tablePath)

// merge the second dataframe into hash table using LakeSoulTable upsert API
val dfUpsert = Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value")
LakeSoulTable.forPath(tablePath).upsert(dfUpsert)

```
</TabItem>

</Tabs>



## 数据更新
LakeSoul表可以通过DataFrame或使用标准的`UPDATE`语句进行更新。要使用DataFrame更新表中的数据，请使用`LakeSoulTable`的`updateExpr` API。


<Tabs>
  <TabItem value="Spark SQL" label="Spark SQL" default>


```sql
// Spark SQL
UPDATE table_namespace.table_name SET name = "David" WHERE id = 2
```
  </TabItem>

  <TabItem value="Scala" label="Scala">

  ```scala
  // Scala
val tablePath = "file:/tmp/lakesoul_namespace/lakesoul_table"
LakeSoulTable.forPath(tablePath).updateExpr("id = 2", Seq("name"->"David").toMap)
  ```
  </TabItem>

</Tabs>


## 数据删除
LakeSoul表可以通过DataFrame或使用标准的`DELETE`语句来删除记录。要使用DataFrame从表中删除数据，请使用`LakeSoulTable`的`delete` API。


<Tabs>
  <TabItem value="Spark SQL" label="Spark SQL" default>

```sql
// Spark SQL
DELETE FROM lakesoul_table WHERE id =1
```

  </TabItem>

  <TabItem value="Scala" label="Scala">

```scala
// Scala
val tablePath = "file:/tmp/lakesoul_namespace/lakesoul_table"
LakeSoulTable.forPath(tablePath).delete("id = 1 or id =2")
```

</TabItem>

</Tabs>

## 数据查询

LakeSoul表可以使用DataFrame或Spark SQL进行查询。

<Tabs>
  <TabItem value="Spark SQL" label="Spark SQL" default>

```sql
// Spark SQL
SELECT * FROM lakesoul_table
```

  </TabItem>

  <TabItem value="Scala" label="Scala">

```scala
// Scala

// query data with DataFrameReader API
val tablePath = "file:/tmp/lakesoul_namespace/lakesoul_table"
spark.read.format("lakesoul").load(tablePath)

// query data with LakeSoulTable API
val tablePath = "file:/tmp/lakesoul_namespace/lakesoul_table"
LakeSoulTable.forPath(tablePath).toDF

val tableName = "lakesoul_table"
LakeSoulTable.forName(tableName).toDF
```

</TabItem>

</Tabs>

## Time Travel查询
LakeSoul支持Time Travel查询，可以查询历史上任何时间点的表或两个提交时间之间的更改数据。

```scala
// Scala
val tablePath = "file:/tmp/lakesoul_namespace/lakesoul_cdc_table"
Seq(("range1", "hash1", "insert"), ("range2", "hash2", "insert"), ("range3", "hash2", "insert"), ("range4", "hash2", "insert"), ("range4", "hash4", "insert"), ("range3", "hash3", "insert"))
    .toDF("range", "hash", "op")
    .write
    .mode("append")
    .format("lakesoul")
    .option("rangePartitions", "range")
    .option("hashPartitions", "hash")
    .option("hashBucketNum", "2")
    .option("shortTableName", "lakesoul_cdc_table")
    .option("lakesoul_cdc_change_column", "op")
    .save(tablePath)
// record the version of 1st commit 
val versionA: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis)


val lakeTable = LakeSoulTable.forPath(tablePath)
lakeTable.upsert(Seq(("range1", "hash1-1", "delete"), ("range2", "hash2-10", "delete"))
.toDF("range", "hash", "op"))
// record the version of 2nd commit 
val versionB: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis)

lakeTable.upsert(Seq(("range1", "hash1-13", "insert"), ("range2", "hash2-13", "update"))
.toDF("range", "hash", "op"))
lakeTable.upsert(Seq(("range1", "hash1-15", "insert"), ("range2", "hash2-15", "update"))
.toDF("range", "hash", "op"))
// record the version of 3rd,4th commits 
val versionC: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(System.currentTimeMillis)

```

### 全量查询
```scala
// Scala
spark.sql("SELECT * FROM lakesoul_cdc_table")
```

### 快照查询
LakeSoul支持快照查询，可用于查询历史上某一时间点的表数据。


```scala
// Scala
spark.read.format("lakesoul")
    .option(LakeSoulOptions.PARTITION_DESC, "range=range2")
    .option(LakeSoulOptions.READ_END_TIME, versionB)
    .option(LakeSoulOptions.READ_TYPE, ReadType.SNAPSHOT_READ)
    .load(tablePath)
```

### 增量查询
LakeSoul支持增量查询，可获得在起始时间和结束时间之间发生更改的数据记录。

```scala
// Scala
spark.read.format("lakesoul")
    .option(LakeSoulOptions.PARTITION_DESC, "range=range1")
    .option(LakeSoulOptions.READ_START_TIME, versionA)
    .option(LakeSoulOptions.READ_END_TIME, versionB)
    .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
    .load(tablePath)
```

## 更多案例
接下来，您可以在[Spark API文档](../03-Usage%20Docs/03-api-docs.md)中了解更多关于在Spark中使用LakeSoul表的案例。
