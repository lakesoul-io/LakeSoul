# 数据更新 (Upsert) 和 Merge UDF 使用教程

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul可以支持对已经入湖的数据做部分字段更新功能，而不必将整张数据表全部覆盖重写，避免这种繁重且浪费资源的操作。

举个例子一张表数据信息如下，id为主键（即hashPartitions），目前需要根据主键字段，对phone_number做字段修改处理。

| id  | name | phone_number | address   | job   | company   |
|-----|------|--------------|-----------|-------|-----------|
| 1   | Jake | 13700001111  | address_1 | job_1 | company_2 |
| 2   | Make | 13511110000  | address_2 | job_2 | company_2 |

可以使用 upsert 来实现对任意行中任意一个字段的更新。upsert需要包含主键 (id) 和需要修改的 address 信息，再次读取整张表数据 address 便可展示为修改后的字段信息。

```scala
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()
import spark.implicits._

val df = Seq(("1", "Jake", "13700001111", "address_1", "job_1", "company_1"),("2", "Make", "13511110000", "address_2", "job_2", "company_2"))
  .toDF("id", "name", "phone_number", "address", "job", "company")
val tablePath = "s3a://bucket-name/table/path/is/also/table/name"

df.write
  .mode("append")
  .format("lakesoul")
  .option("hashPartitions","id")
  .option("hashBucketNum","2")
  .save(tablePath)

val lakeSoulTable = LakeSoulTable.forPath(tablePath)
val extraDF = Seq(("1", "address_1_1")).toDF("id","address")
lakeSoulTable.upsert(extraDF)
lakeSoulTable.toDF.show()

/**
 *  result:
 *  +---+----+------------+-----------+-----+---------+
 *  | id|name|phone_number|    address|  job|  company|
 *  +---+----+------------+-----------+-----+---------+
 *  |  1|Jake| 13700001111|address_1_1|job_1|company_1|
 *  |  2|Make| 13511110000|  address_2|job_2|company_2|
 *  +---+----+------------+-----------+-----+---------+
 */
```

## 自定义 merge 合并逻辑
LakeSoul 默认 merge 规则，即数据更新后取最后一条记录作为该字段数据 (org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.DefaultMergeOp)。在此基础上，LakeSoul 内置扩展了几种数据 merge 逻辑，对 Int/Long 字段做加和 merge(MergeOpInt/MergeOpLong)、对非空字段更新 (MergeNonNullOp)、以","拼接字符串 merge 方式。

下面以对非空字段更新 (MergeNonNullOp) 为例，借用上面表格数据样例。数据写入时同样以 upsert 方式进行更新写入，然后在数据读取时需要注册 merger 逻辑，然后进行读取即可。
```scala
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeNonNullOp
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()
import spark.implicits._

val df = Seq(("1", "Jake", "13700001111", "address_1", "job_1", "company_1"),("2", "Make", "13511110000", "address_2", "job_2", "company_2"))
  .toDF("id", "name", "phone_number", "address", "job", "company")

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"

df.write
  .mode("append")
  .format("lakesoul")
  .option("hashPartitions","id")
  .option("hashBucketNum","2")
  .save(tablePath)

val lakeSoulTable = LakeSoulTable.forPath(tablePath)
val extraDF = Seq(("1", "null", "13100001111", "address_1_1", "job_1_1", "company_1_1"),("2", "null", "13111110000", "address_2_2", "job_2_2", "company_2_2"))
  .toDF("id", "name", "phone_number", "address", "job", "company")

new MergeNonNullOp().register(spark, "NotNullOp")
lakeSoulTable.toDF.show()
lakeSoulTable.upsert(extraDF)
lakeSoulTable.toDF.withColumn("name", expr("NotNullOp(name)")).show()

/**
 *  result
 *  +---+----+------------+-----------+-------+-----------+
 *  | id|name|phone_number|    address|    job|    company|
 *  +---+----+------------+-----------+-------+-----------+
 *  |  1|Jake| 13100001111|address_1_1|job_1_1|company_1_1|
 *  |  2|Make| 13111110000|address_2_2|job_2_2|company_2_2|
 *  +---+----+------------+-----------+-------+-----------+
 */
```

用户也可以通过自定义 MergeOperator (实现 `trait org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator`) 来自定义 Merge 时的逻辑，能够灵活地实现数据高效入湖。