# Spark Guide

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

## Setup

To use LakeSoul in Spark, first configure [Spark catalogs](02-docker-compose.mdx). LakeSoul uses Apache Spark’s DataSourceV2 API for data source and catalog implementations. Moreover, LakeSoul provides scala table API to extend the capability of LakeSoul table.

### Spark 3 Support Matrix

LakeSoul | Spark Version
--- | ---
2.2.x-2.4.x |3.3.x
2.0.x-2.1.x| 3.1.x

### Spark Shell/SQL

Run spark-shell/spark-sql with the `LakeSoulSparkSessionExtension` sql extension.
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

### Setup project

Then, we do imports LakeSoul dependencies and setup spark config for following cases.

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


## Create Namespace
First, create a namespace for LakeSoul table, default namespace of LakeSoul Catalog is `default`.
```sql
// Spark SQL
CREATE NAMESPACE lakesoul_namespace;
USE lakesoul_namespace
```

## Create Table
Create a partitioned LakeSoul table with the clause `USING lakesoul`:

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

### Hash Partitioned Table
In LakeSoul, a table with primary keys is defined as a hash-partitioned table. To create such a table, use the `USING lakesoul` clause and specify the `TBLPROPERTIES` setting, where `'hashPartitions'` designates a comma-separated list of primary key column names, and `'hashBucketNum'` determines the size or number of hash buckets.

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

### CDC Table
Optionally, a hash-partitioned LakeSoul table has the capability to record Change Data Capture (CDC) data, enabling the tracking of data modifications. To create a LakeSoul table with CDC support, one can utilize the DDL statement for a hash-partitioned LakeSoul table and include an additional `TBLPROPERTIES` setting specifying the `'lakesoul_cdc_change_column'` attribute. This attribute introduces an implicit column that assists the table in efficiently handling CDC information, ensuring precise tracking and management of data changes.

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

## Insert/Merge Data

To append new data to a non-hash-partitioned table using Spark SQL, use INSERT INTO.

To append new data to a table using DataFrame, use `DataFrameWriterV2` API. If this is the first write of the table, it will also auto-create the corresponding LakeSoul table. 

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

To append new data to a hash-partitioned table using Spark SQL, use Merge INTO.

To append new data to a hash-partitioned table using DataFrame, use `LakeSoulTable` upsert API.


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



## Update Data
LakeSoul tables can be updated by a DataFrame or using a standard `UPDATE` statement.
To update data to a table using DataFrame, use `LakeSoulTable` updateExpr API.

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


## Delete Data
LakeSoul tables can be removes the records by a DataFrame or using a standard `DELETE` statement.
To delete data to a table using DataFrame, use `LakeSoulTable` `delete` API.

<Tabs>
  <TabItem value="Spark SQL" label="Spark SQL" default>

```sql
// Spark SQL
DELETE FROM lakesoul.lakesoul_namespace.tbl  WHERE key =1
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

## Query Data

LakeSoul tables can be queried using a DataFrame or Spark SQL.

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

## Time Travel Query
LakeSoul supports time travel query to query the table at any point-in-time in history or the changed data between two commit time. 

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

### Complete Query
```scala
// Scala
spark.sql("SELECT * FROM lakesoul_cdc_table")
```

### Snapshot Query
LakeSoul supports snapshot query for query the table at a point-in-time in history.
```scala
// Scala
spark.read.format("lakesoul")
    .option(LakeSoulOptions.PARTITION_DESC, "range=range2")
    .option(LakeSoulOptions.READ_END_TIME, versionB)
    .option(LakeSoulOptions.READ_TYPE, ReadType.SNAPSHOT_READ)
    .load(tablePath)
```

### Incremental Query
LakeSoul supports incremental query to obtain a set of records that changed between a start and end time.

```scala
// Scala
spark.read.format("lakesoul")
    .option(LakeSoulOptions.PARTITION_DESC, "range=range1")
    .option(LakeSoulOptions.READ_START_TIME, versionA)
    .option(LakeSoulOptions.READ_END_TIME, versionB)
    .option(LakeSoulOptions.READ_TYPE, ReadType.INCREMENTAL_READ)
    .load(tablePath)
```

## Next steps
Next, you can learn more usage cases about LakeSoul tables in Spark at [Spark API docs](../03-Usage%20Docs/03-api-docs.md).
