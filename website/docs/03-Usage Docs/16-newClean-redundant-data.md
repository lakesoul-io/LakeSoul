# LakeSoul Table Lifecycle Automatic Maintenance and Redundant Data Automatic Cleanup

:::tip
This feature is available in version 3.0.0 and above.
:::

In data warehouses, it is often necessary to define the lifecycle of table data in order to save storage space and reduce costs.

On the other hand, for real-time updating tables, redundant data may exist. Redundant data refers to the fact that every time a **compaction** operation is executed, a new compaction file is generated. The new compaction file contains all historical data, and at this point, all previous compaction files can be considered redundant.

Meanwhile, for a table that is continuously updated and compacted, if the user only cares about data changes within a recent time range, they can choose to clean up all data before a specific compaction. This way, one full snapshot of the data will be preserved, while still allowing users to perform incremental reads and snapshot queries from the recent time range onward.

Before version 3.0.0, the automatic cleanup service was scheduled to run daily, scanning all metadata to find expired files and then deleting them. This caused a high instantaneous load on the metadata service.  
Starting from version 3.0.0, the cleanup service has been completely reimplemented as an **asynchronous, real-time cleanup mechanism**. By consuming metadata change events through CDC combined with Flink’s timer mechanism, the new design achieves asynchronous cleanup with higher efficiency and significantly reduces metadata service load.

## Run Flink Cleanup Job Locally

```shell
./bin/flink run \
-c org.apache.flink.lakesoul.entry.clean.NewCleanJob \
lakesoul-flink-1.20-3.0.0-SNAPSHOT.jar \
--source_db.dbName lakesoul_test \
--source_db.user lakesoul_test \
--source_db.host localhost \
--source_db.port 5432 \
--source_db.password lakesoul_test \
--slotName flink  \
--plugName pgoutput \
--url jdbc:postgresql://localhost:5432/lakesoul_test \
--ontimer_interval "1" \
--dataExpiredTime "5"
```

## Parameter Configuration

| Parameter Name                | Required | Description                                                      |
|---------------------|----------|---------------------------------------------------------|
| --source_db.dbName  | yes      | PostgreSQL database name                                                 |
| --source_db.user    | yes      | PostgreSQL username                                                |
| --source_db.host    | yes      | PostgreSQL host                                              |
| --source_db.port    | yes      | Database port                                                   |
| --source_db.password | yes      | Database password                                                    |
| --slotName          | yes      | Logical replication slot name                                                    |
| --plugName          | yes      | Logical replication plugin name                                                    |
| --url               | yes      | JDBC URL of the PostgreSQL database                                                  |
| --ontimer_interval  | no       | Timer trigger interval (in minutes), default is 5 minutes                                    |
| --dataExpiredTime   | no       | Data expiration time (in days), default is 3 days                                     |
| --source.parallelism | no       | Source reading parallelism, default is 1                                           |
| --targetTableName   | no       | Specify a table for cleanup, e.g., public.testTable. Multiple tables can be separated by commas. If not specified, all tables will be included in cleanup. |

