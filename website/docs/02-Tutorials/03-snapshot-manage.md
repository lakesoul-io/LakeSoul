# Snapshot Related API Usage Tutorial

<!--
SPDX-FileCopyrightText: 2023 LakeSoul Contributors

SPDX-License-Identifier: Apache-2.0
-->

LakeSoul uses snapshots to record each updated file set and generate a new version number in the metadata. If the historical snapshot version has not been cleaned up, it can also be read, rolled back and cleaned up through the LakeSoul API. Since the snapshot version is an internal mechanism, LakeSoul provides a timestamp-based snapshot management API for convenience.

For snapshot read in Flink SQL, please refer to [Flink Connector](../03-Usage%20Docs/06-flink-lakesoul-connector.md).

## Snapshot Read
In some cases, it may be necessary to query the snapshot data of a partition of a table at a previous point in time, also known as Time Travel. The way LakeSoul performs reading a snapshot at a point in time:
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession. builder. master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
// Read the data of the 'date=2022-01-02' partition when the timestamp is less than or equal to and closest to '2022-01-01 15:15:15'
val lakeSoulTable = LakeSoulTable.forPathSnapshot(tablePath, "date=2022-01-02", "2022-01-01 15:15:15")
```

## Snapshot Rollback
Sometimes due to errors in newly written data, it is necessary to roll back to a certain historical snapshot version. How to perform a snapshot rollback to a point in time using LakeSoul:
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession. builder. master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable. forPath(tablePath)

//Rollback metadata and storage data partitioned as '2021-01-02' when the timestamp is less than or equal to and the closest to '2022-01-01 15:15:15'
lakeSoulTable.rollbackPartition("date='2022-01-02'", "2022-01-01 15:15:15")
```
The rollback operation itself will create a new snapshot version, while other version snapshots and data will not be deleted.

## Snapshot Cleanup
If the historical snapshot is no longer needed, for example, Compaction has been executed, you can call the cleanup method to clean up the snapshot data before a certain point in time:
```scala
import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql._
val spark = SparkSession. builder. master("local")
  .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
  .getOrCreate()

val tablePath = "s3a://bucket-name/table/path/is/also/table/name"
val lakeSoulTable = LakeSoulTable. forPath(tablePath)

//Clean up metadata and storage data partitioned as 'date=2022-01-02' and earlier than "2022-01-01 15:15:15"
lakeSoulTable.cleanupPartitionData("date='2022-01-02'", "2022-01-01 15:15:15")
```
