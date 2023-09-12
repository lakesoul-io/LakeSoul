# LakeSoul Methods for Redundant Data Removal

:::tip
The feature is available starting from version 2.4.0
:::

Here, redundant data refers to the fact that every time a compaction operation is executed, a new compaction file is generated, which contains all historical data. At this point, all historical compaction files can be considered redundant data. Additionally, for a table that continuously updates and undergoes compaction operations, if a user is only interested in recent data changes within a certain time frame, the user can choose to clean up all data before a specific compaction. This way, one can retain a full set of data and support incremental reads and snapshot reads from a specific recent time period.
## clean old compaction
Users can enable the 'cleanOldCompaction' switch when performing a compaction operation to clean up old compaction file data. It is set to 'false' by default.
```scala
    LakeSoulTable.forPath(tablePath).compaction(true)
```

##Clean up expired data
Users can configure the following two properties:  

partition.ttl represents the user-configured expiration time for table partitions, with a granularity in days.
compaction.ttl represents the user-configured expiration time for redundant data, with a granularity in days.

1,config partition.ttl  
The expiration time for the entire table  
Assuming a user configuration of 365 days, for a table, if the most recent commit record in a partition has expired, then delete the data in that partition. In particular, if all partitions of the table have expired, it is equivalent to performing the truncate function.

2,config compaction.ttl
Cleaning redundant partition data  
Assuming a redundancy cleanup period of 3 days, locate the most recent compaction operation that occurred exactly 3 days ago and delete data preceding it. The purpose of this is to ensure the effectiveness of snapshot and incremental reads within a 3-day timeframe.


You can set partition.ttl and compaction.ttl in the following ways:
1, You can specify this when writing data to Lakesoul.

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
2, For tables that have already been created, you can use the table API to add or modify this configuration.
```scala
    LakeSoulTable.forPath(tablePath).setPartitionTtl(128).setCompactionTtl(10)
```

Meanwhile, you can cancel this configuration through the table API.
```scala
    LakeSoulTable.forPath(tablePath).cancelPartitionTtl()
    LakeSoulTable.forPath(tablePath).cancelCompactionTtl()
```
Setup metadata connection for LakeSoul. For detailed documentation, please refer
to [Setup Spark Job](../03-Usage%20Docs/02-setup-spark.md)

Local Spark Cleanup Commands:
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
The above cleaning operation applies to all tables in Lakesoul.
:::