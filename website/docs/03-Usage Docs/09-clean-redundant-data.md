# LakeSoul Table Lifecycle Automatic Maintenance and Redundant Data Automatic Cleaning

:::tip
This feature is available starting from version 2.4.0
:::

In a data warehouse, it is usually necessary to set the life cycle of table data to save space and reduce costs.

On the other hand, for tables that are updated in real time, there will also be redundant data. Redundant data means that every time a compaction operation is performed, a new compaction file will be generated. The new compaction file contains all historical data. At this time, all historical compaction files can be regarded as redundant data.

At the same time, for a table data that is continuously updated and compaction operated. If the user only cares about the data changes at a certain time recently. At this time, users can choose to clean up all data before a certain compaction, which will retain a full copy of the data and support users' incremental and snapshot reads from a recent time.

## Manually Clean Up Old Compaction Data
When performing a compactition operation, users can turn on the switch cleanOldCompaction=true to clean up old compaction file data. Default is false.
```scala
     LakeSoulTable.forPath(tablePath).compaction(true)
```

## Automatically Clean Up Expired Data and Redundant Data
Users can configure the following two table properties:
1. `partition.ttl` represents the partition expiration time, unit is days.
2. `compaction.ttl` represents the expiration time of redundant data, unit is days.

### Configure Partition Lifecycle via `partition.ttl` table property
As for the expiration time of table partitions, assuming that the user configures it to 365 days, then in the table, if the latest commit record of a partition has expired, the partition data will be deleted. In particular, if all partitions of the table are expired, it is equivalent to executing the truncate function.

### Configure Lifecycle of Redundant Data via `compaction.ttl` table property
Clean up redundant data in partitions. Assume that the redundant cleanup lasts for 3 days, then find the latest compaction 3 days ago and delete the data before it. The purpose is that snapshot reads and incremental reads within 3 days are all valid.

### Configuration Examples

Users can set `partition.ttl` and `compaction.ttl` in the following ways.

#### Specify configuration when writing through data
```scala
     val df = Seq(("2021-01-01", 1, "rice"), ("2021-01-01", 2, "bread")).toDF("date", "id", "name ")
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
You can also add property configuration in `TBLPROPERTIES` in the `CREATE TABLE` SQL statement (in Flink, property configuration is added after `WITH`). Examples:
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

### Add or modify the configuration through API
```scala
     LakeSoulTable.forPath(tablePath).setPartitionTtl(128).setCompactionTtl(10)
```

At the same time, the configuration can be canceled through tableAPI
```scala
     LakeSoulTable.forPath(tablePath).cancelPartitionTtl()
     LakeSoulTable.forPath(tablePath).cancelCompactionTtl()
```
Configure the LakeSoul metabase connection for Spark jobs. For detailed instructions, please refer to [LakeSoul Setup Spark Project/Job](../03-Usage%20Docs/02-setup-spark.md);

### Execute a job that automatically cleans expired data in all tables
LakeSoul provides a Spark job implementation for cleaning expired data. It will scan all expired partitions in the metadata and perform cleaning. Users can schedule this task regularly at the daily level to achieve the purpose of cleaning.

Start the Spark cleanup command locally:
```shell
./bin/spark-submit \
     --name clean_redundant_data \
     --master yarn \
     --deploy-mode cluster \
     --executor-memory 3g \
     --executor-cores 1 \
     --num-executors 20 \
     --class com.dmetasoul.lakesoul.spark.clean.CleanExpiredData \
     jars/lakesoul-spark-2.4.0-spark-3.3.jar

```
:::tip
The above cleaning tasks are effective for all LakeSoul tables.
:::