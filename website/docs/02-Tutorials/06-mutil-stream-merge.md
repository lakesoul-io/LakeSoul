# Multi Stream Merge to Build Wide Table Tutorial

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

To build wide table, traditional data warehouse or ETL uses multi tables join according to the primary and foreign key. When there is a large amount of data or multiple joins are required, there will be problems such as low efficiency, large memory consumption, and even OOM. In addition, the Shuffle process takes up most of the data exchange time, and is inefficient. LakeSoul has supported upsert with merge operator, which can be used to implement multi stream merge in realtime, and avoid the above problems by eliminating join and shuffle. The following is a specific example of this scenario.

Suppose there are data of the following streams, A, B, C and D. The data contents of each stream are as follows:

A:

| Ip      | sy  | us  |
|---------|-----|-----|
| 1.1.1.1 | 30  | 40  |

B:

| Ip      | free | cache |
|---------|------|-------|
| 1.1.1.1 | 1677 | 455   |


C:

| Ip      | level | des    |
|---------|-------|--------|
| 1.1.1.2 | error | killed |

D:

| Ip      | qps | tps |
|---------|-----|-----|
| 1.1.1.1 | 30  | 40  |

Finally, a large wide table needs to be formed, and the four tables need to be consolidated and displayed as follows:

| IP      | sy   | us   | free | cache | level | des    | qps  | tps  |
|---------|------|------|------|-------|-------|--------|------|------|
| 1.1.1.1 | 30   | 40   | 1677 | 455   | null  | null   | 30   | 40   |
| 1.1.1.2 | null | null | null | null  | error | killed | null | null |

Traditionally, to perform the above operations, four tables need to be joined three times according to the primary key (IP). The writing method is as follows:

```sql
Select 
       A.IP as IP,  
       A.sy as sy, 
       A.us as us, 
       B.free as free, 
       B.cache as cache, 
       C.level as level, 
       C.des as des, 
       D.qps as qps, 
       D.tps as tps 
from A join B on A.IP = B.IP 
    join C on C.IP = A.IP 
    join D on D.IP = A.IP.
```

LakeSoul supports multi stream merge with different schemas (same primary keys should exist), and can automatically extend the schema of the table according to the primary key from multiple streams. If the newly written data field does not exist in the original table, it will automatically extend the table schema. The non-existent field is null by default. Therefore, the same resulting data can be achieved by writing each stream data to LakeSoul through upsert without table join. The above process code is implemented as follows:

```scala
import org.apache.spark.sql._
val spark = SparkSession.builder.master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .config("spark.dmetasoul.lakesoul.schema.autoMerge.enabled", "true")
  .getOrCreate()
import spark.implicits._

val df1 = Seq(("1.1.1.1", 30, 40)).toDF("IP", "sy", "us")
val df2 = Seq(("1.1.1.1", 1677, 455)).toDF("IP", "free", "cache")
val df3 = Seq(("1.1.1.2", "error", "killed")).toDF("IP", "level", "des")
val df4 = Seq(("1.1.1.1", 30, 40)).toDF("IP", "qps", "tps")

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"

df1.write
  .mode("append")
  .format("lakesoul")
  .option("hashPartitions","IP")
  .option("hashBucketNum","2")
  .save(tablePath)

val lakeSoulTable = LakeSoulTable.forPath(tablePath)

lakeSoulTable.upsert(df2)
lakeSoulTable.upsert(df3)
lakeSoulTable.upsert(df4)
lakeSoulTable.toDF.show()

/**
 *  result
 *  |  IP   |  sy|  us|free|cache|level|   des| qps| tps|
 *  +-------+----+----+----+-----+-----+------+----+----+
 *  |1.1.1.2|null|null|null| null|error|killed|null|null|
 *  |1.1.1.1|  30|  40|1677|  455| null|  null|  30|  40|
 *  +-------+----+----+----+-----+-----+------+----+----+
 */

```
