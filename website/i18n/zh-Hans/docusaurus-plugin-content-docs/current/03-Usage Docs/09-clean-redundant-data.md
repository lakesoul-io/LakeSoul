# LakeSoul 冗余数据清除方法

:::tip
该功能于 2.4.0 版本起提供
:::

这里冗余数据是指，每次执行compaction操作时，都会生成一个新的compaction文件，新compaction文件包含所有的历史数据，此时所有的历史compaction文件便可以视为冗余数据。
同时，对于一个在持续更新和compaction操作的表数据。如果用户只关心近期某个时间断的数据变更情况。此时用户可以选择清理某个compaction之前的所有数据，这样会保留一份全量数据并且支持用户从近期某个时间断增量读和快照读。

## 清理旧compaction
用户在执行compactition操作时，可以开启开关cleanOldCompaction=true,清理旧compaction文件数据。默认为false。
```scala
    LakeSoulTable.forPath(tablePath).compaction(true)
```

##清理过期数据
用户可以配置下面两个属性   
partition.ttl表示用户配置的整表过期时间，粒度(天)  
compaction.ttl表示用户配置的冗余数据过期时间，粒度(天)  

1，配置partition.ttl
对表分区的过期时间  
假设用户配置为365天，那么该表中，如果某个分区的最近commit记录已经过期，
那么删除该分区数据。特别的，如果该表的所有分区都过期，那么相当于执行truncate功能。

2，配置compaction.ttl  
清理分区冗余数据
假设冗余清理是3天，那么找到3天前的最近一次compaction，删除它之前的数据。
这样的目的是3天内的快照读、增量读都是有效的。


用户可以通过以下方式设置partition.ttl和compaction.ttl  
1，通过数据写入lakesoul时指定:  

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
2,对已经创建了的表:  
    可以通过tableAPI来增加或修改该配置
```scala
    LakeSoulTable.forPath(tablePath).setPartitionTtl(128).setCompactionTtl(10)
```

同时，可以通过tableAPI来取消该配置
```scala
    LakeSoulTable.forPath(tablePath).cancelPartitionTtl()
    LakeSoulTable.forPath(tablePath).cancelCompactionTtl()
```
为 Spark 作业配置 LakeSoul 元数据库连接，详细说明可以参考 [LakeSoul设置 Spark 工程/作业](../03-Usage%20Docs/02-setup-spark.md) ；


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
    jars/lakesoul-spark-2.4.0-spark-3.3-SNAPSHOT.jar 

```
:::tip
上述清理操作是对lakesoul所有表生效。
:::