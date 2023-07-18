# 多流合并构建宽表教程

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

为构建宽表，传统数仓的 ETL 在做多表关联时，需要根据主外键多次 join，然后构建一个大宽表。当数据量较多或需要多次join时，会有效率低下，内存消耗大，容易 OOM 等问题，且 Shuffle 过程占据大部分数据交换时间，效率也很低下。LakeSoul 支持对数据进行 Upsert，并支持自定义 MergeOperator 功能，可以避免上述存在的问题，不必Join即可得到合并结果。下面针对这一场景具体举例进行说明。

假设有以下几个流的数据，A、B、C和D，各个流数据内容如下：

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

最后需要形成一张大宽表，将四张表进行合并展示，如下：

| IP      | sy   | us   | free | cache | level | des    | qps  | tps  |
|---------|------|------|------|-------|-------|--------|------|------|
| 1.1.1.1 | 30   | 40   | 1677 | 455   | null  | null   | 30   | 40   |
| 1.1.1.2 | null | null | null | null  | error | killed | null | null |

us | free | cache | level | des | qps | tps |
---|--|--|--|--|--|--|
40 |1677 |455 |null |null |30 |40|

传统意义上进行上述操作，需要将四张表根据主键（IP）进行三次join，写法如下：

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
LakeSoul 支持多流合并，多个流可以有不同的 Schema （需要有相同主键）。LakeSoul 可以做到自动扩展 Schema，若新写入的数据字段在原表中未存在，则会自动扩展表 schema，不存在的字段默认为null处理。通过使用 LakeSoul 多流合并功能，结合 LakeSoul 独特的 MergeOperator 功能，通过 upsert 将数据写入 LakeSoul 后，不需要 join，即可读取到拼接好的宽表。上述过程代码实现如下：

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
展示效果
|     IP|  sy|  us|free|cache|level|   des| qps| tps|
+-------+----+----+----+-----+-----+------+----+----+
|1.1.1.2|null|null|null| null|error|killed|null|null|
|1.1.1.1|  30|  40|1677|  455| null|  null|  30|  40|
+-------+----+----+----+-----+-----+------+----+----+
 */

```