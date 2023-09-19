# LakeSoul 表生命周期自动维护和冗余数据自动清理

:::tip
该功能于 2.4.0 版本起提供
:::

在数据仓库中，通常会需要设置表的数据的生命周期，从而达到节省空间，降低成本的目的。

另一方面，对于实时更新的表，还会存在冗余数据。冗余数据是指，每次执行 compaction 操作时，都会生成新的 compaction 文件，新 compaction 文件包含所有的历史数据，此时所有的历史 compaction 文件便可以视为冗余数据。

同时，对于一个在持续更新和 compaction 操作的表数据。如果用户只关心近期某个时间断的数据变更情况。此时用户可以选择清理某个 compaction 之前的所有数据，这样会保留一份全量数据并且支持用户从近期某个时间断增量读和快照读。

## 手工清理旧 compaction 数据
用户在执行 compactition 操作时，可以开启开关 cleanOldCompaction=true,清理旧compaction文件数据。默认为false。
```scala
    LakeSoulTable.forPath(tablePath).compaction(true)
```

## 自动清理过期数据和冗余数据
用户可以配置下面两个表属性：
1. `partition.ttl` 表示分区过期时间，粒度(天)  
2. `compaction.ttl` 表示冗余数据过期时间，粒度(天)  

### 配置分区生命周期 `partition.ttl` 表属性
对表分区的过期时间，假设用户配置为365天，那么该表中，如果某个分区的最近commit记录已经过期，那么删除该分区数据。特别的，如果该表的所有分区都过期，那么相当于执行 truncate 功能。

### 配置冗余数据自动清理周期 `compaction.ttl` 表属性
清理分区冗余数据。假设冗余清理是3天，那么找到3天前的最近一次compaction，删除它之前的数据。这样的目的是3天内的快照读、增量读都是有效的。

### 配置示例

用户可以通过以下方式设置 `partition.ttl`和 `compaction.ttl` 表属性。

#### 通过数据写入时指定配置 
```scala
    val df = Seq(("2021-01-01", 1, "rice"), ("2021-01-01", 2, "bread")).toDF("date", "id", "name")
    val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
    df.write
      .mode("append")
      .format("lakesoul")
      .option("rangePartitions", "date")
      .option("hashPartitions", "id")
      .option("partition.ttl",365)
      .option("compaction.ttl",3)
      .option("hashBucketNum", "2")
      .save(tablePath)
```
也可以在 `CREATE TABLE` SQL 语句中，在 `TBLPROPERTIES` 里添加属性配置（Flink 中是在 `WITH` 后添加属性配置）。示例：
```sql
-- For Spark
CREATE TABLE table (id INT, data STRING) USING lakesoul
TBLPROPERTIES ('partition.ttl'='365', 'compaction.ttl'='7')

-- For Flink
create table `lakesoul`.`default`.test_table (`id` INT, data STRING,
  PRIMARY KEY (`id`,`name`) NOT ENFORCED)
WITH (
    'connector'='lakesoul',
    'hashBucketNum'='4',
    'use_cdc'='true',
    'partition.ttl'='365',
    'compaction.ttl'='7',
    'path'='file:///tmp/lakesoul/flink/sink/test');
```

### 通过 API 来增加或修改该配置
```scala
    LakeSoulTable.forPath(tablePath).setPartitionTtl(128).setCompactionTtl(10)
```

同时，可以通过tableAPI来取消该配置
```scala
    LakeSoulTable.forPath(tablePath).cancelPartitionTtl()
    LakeSoulTable.forPath(tablePath).cancelCompactionTtl()
```
为 Spark 作业配置 LakeSoul 元数据库连接，详细说明可以参考 [LakeSoul设置 Spark 工程/作业](../03-Usage%20Docs/02-setup-spark.md) ；

### 执行自动清理所有表的过期数据的作业
LakeSoul 提供了一个清理过期数据的 Spark 作业实现，会扫描元数据中所有过期的分区并执行清理，使用者可以天级别定时调度这个任务来达到清理目的。

本地启动 Spark 清理命令：
```shell
./bin/spark-submit \
    --name clean_redundant_data \
    --master yarn  \
    --deploy-mode cluster \
    --executor-memory 3g \
    --executor-cores 1 \
    --num-executors 20 \
    --class com.dmetasoul.lakesoul.spark.clean.CleanExpiredData \
    jars/lakesoul-spark-2.4.0-spark-3.3.jar 

```
:::tip
上述清理任务是对 LakeSoul 所有表生效。
:::