# Upsert Data and Merge UDF Tutorial

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul can support the function of updating some fields of the data that has entered the lake, without having to overwrite the entire data table, so as to avoid this heavy and resource wasting operation.

For example, the data information of a table is as follows. The ID is the primary key (i.e. hashPartitions). At present, it is necessary to check the data of phone according to the primary key field_ Number to modify the field.

| id  | name | phone_number | address   | job   | company   |
|-----|------|--------------|-----------|-------|-----------|
| 1   | Jake | 13700001111  | address_1 | job_1 | company_2 |
| 2   | Make | 13511110000  | address_2 | job_2 | company_2 |

Upsert can be used to update and overwrite existing fields. Upsert operation needs to include the primary key (e.g. id) and other fields (e.g. address) to be modified. Reading the address of the whole table data again can display the modified field information.

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

## Customize Merge Logic
The essence of the field update supported by LakeSoul is to follow the default merge rule of LakeSoul, that is, after data is upserted, the latest record is taken as the changed field data (see `org.apache.spark.sql.execution.datasources.v2.merge.request.batch.merge_operator.DefaultMergeOp`). On this basis, LakeSoul has several built-int merge operators, including adding and merging Int/Long fields (MergeOpInt/MergeOpLong), updating no empty fields (MergeNonNullOp), and merging strings with ",".

The following is an example of updating no null fields (MergeNonNullOp), borrowing the above table data sample. When data is written, it is also updated and written in the upsert mode. When reading data, you need to register the merge logic and then read.

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

You could also define your own merge logic via implementing the trait `org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator` to achieve efficient data update.